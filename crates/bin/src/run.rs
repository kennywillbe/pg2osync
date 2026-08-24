//! The main mode: bootstrap, consistent backfill, live streaming.

use anyhow::{Context as _, Result};
use pg2osync_core::event::{ChangeEvent, RowKind, TransactionBoundary};
use pg2osync_core::lsn::Lsn;
use pg2osync_core::sink::Sink;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, watch};

use crate::config::AppConfig;

/// Column metadata needed to rebuild documents outside the WAL stream.
#[derive(Clone)]
struct ColMeta {
    name: String,
    type_oid: u32,
    is_pk: bool,
}

async fn columns_of(
    client: &tokio_postgres::Client,
    qualified_table: &str,
) -> Result<Vec<ColMeta>> {
    let rows = client
        .query(
            r#"
            SELECT a.attname AS name,
                   a.atttypid::int4 AS oid,
                   EXISTS(
                       SELECT 1 FROM pg_index i
                       WHERE i.indrelid = a.attrelid
                         AND i.indisprimary
                         AND a.attnum = ANY(i.indkey)
                   ) AS is_pk
            FROM pg_attribute a
            WHERE a.attrelid = ($1::text)::regclass
              AND a.attnum > 0
              AND NOT a.attisdropped
            ORDER BY a.attnum
            "#,
            &[&qualified_table],
        )
        .await?;
    Ok(rows
        .into_iter()
        .map(|r| ColMeta {
            name: r.get("name"),
            // atttypid arrives via text cast above
            type_oid: r.get::<_, i32>(1) as u32,
            is_pk: r.get("is_pk"),
        })
        .collect())
}

/// COPY-style unescape for PostgreSQL TEXT format field values.
fn unescape_copy(field: &str) -> Option<Vec<u8>> {
    if field == "\\N" {
        return None;
    }
    let mut out = Vec::with_capacity(field.len());
    let mut chars = field.chars();
    while let Some(c) = chars.next() {
        if c != '\\' {
            out.extend_from_slice(c.to_string().as_bytes());
            continue;
        }
        match chars.next() {
            Some('t') => out.push(b'\t'),
            Some('n') => out.push(b'\n'),
            Some('r') => out.push(b'\r'),
            Some('\\') => out.push(b'\\'),
            Some(other) => {
                out.extend_from_slice(other.to_string().as_bytes());
            }
            None => {}
        }
    }
    Some(out)
}

#[allow(clippy::too_many_arguments)]
pub async fn run_pipeline(
    cfg: AppConfig,
    source_url: String,
    target_password: Option<String>,
    shutdown_rx: watch::Receiver<bool>,
    durable: pg2osync_engine::mapping::DurableLsn,
) -> Result<()> {
    let engine_shutdown = shutdown_rx.clone();
    use pg2osync_source::runner::{WalSource, WalSourceConfig};

    // --- connections & bootstrap -------------------------------------------------
    let (admin, admin_conn) = tokio_postgres::connect(&source_url, tokio_postgres::NoTls)
        .await
        .context("cannot connect to source")?;
    tokio::spawn(async move {
        let _ = admin_conn.await;
    });

    let mut children_map: HashMap<(String, String), Vec<pg2osync_source::children::ChildSpec>> =
        HashMap::new();
    let mut child_parents_map: HashMap<(String, String), (String, String)> = HashMap::new();
    let mut parent_pk_columns: HashMap<(String, String), String> = HashMap::new();
    for tbl in cfg.sync.values() {
        let mut parts = tbl.table.splitn(2, '.');
        let (ps, pt) = (
            parts.next().unwrap_or_default().to_string(),
            parts.next().unwrap_or_default().to_string(),
        );
        if !tbl.primary_key.as_ref().is_some_and(|k| !k.is_empty()) {
            // pk detection happens later via catalog; placeholder "id" keeps
            // refetch queries working for the common case
        }
        parent_pk_columns.insert(
            (ps.clone(), pt.clone()),
            tbl.primary_key.clone().unwrap_or_else(|| "id".into()),
        );
        for cj in &tbl.children {
            let spec = pg2osync_source::children::ChildSpec::new(
                &cj.table,
                &cj.field,
                &cj.foreign_key,
                &tbl.primary_key.clone().unwrap_or_else(|| "id".into()),
            )?;
            let cparts: Vec<&str> = cj.table.splitn(2, '.').collect();
            child_parents_map.insert(
                (cparts[0].to_string(), cparts[1].to_string()),
                (ps.clone(), pt.clone()),
            );
            children_map
                .entry((ps.clone(), pt.clone()))
                .or_default()
                .push(spec);
        }
    }

    let tables: Vec<String> = cfg.sync.values().map(|t| t.table.clone()).collect();
    // child tables join the publication or their changes never reach us
    let mut tables = tables;
    for tbl in cfg.sync.values() {
        for cj in &tbl.children {
            if !tables.contains(&cj.table) {
                tables.push(cj.table.clone());
            }
        }
    }
    let url = url::Url::parse(&source_url)?;
    let src_cfg = WalSourceConfig {
        host: url.host_str().unwrap_or("localhost").into(),
        port: url.port().unwrap_or(5432),
        user: url.username().into(),
        password: url.password().unwrap_or_default().into(),
        database: url.path().trim_start_matches('/').to_string(),
        slot_name: cfg.source.slot_name.clone(),
        publication: cfg.source.publication.clone(),
        tables: tables.clone(),
        start_lsn: None,
        durable: None,
        admin_url: Some(source_url.clone()),
        children: children_map.clone(),
        child_parents: child_parents_map.clone(),
        parent_pk_columns,
    };
    let source = WalSource::new(src_cfg.clone());
    source.bootstrap(&admin).await?;
    // MySQL source bootstrap happens inside stream(); PG-specific checks here
    // deletes on CHILD tables carry no FK under the default replica identity;
    // warn upfront — runtime still errors precisely if a delete loses its way
    for tbl in cfg.sync.values() {
        for cj in &tbl.children {
            let mut cp = cj.table.splitn(2, '.');
            let cs = cp.next().unwrap_or("public");
            let ct = cp.next().unwrap_or_default();
            let ci = pg2osync_source::catalog::table_info(&admin, cs, ct)
                .await
                .with_context(|| format!("cannot inspect child table {}", cj.table))?;
            if ci.relreplident != 'f' {
                tracing::warn!(target: "pg2osync::run",
                    "child table {} has REPLICA IDENTITY '{}': child DELETE refresh will fail.                      Recommended: ALTER TABLE {} REPLICA IDENTITY FULL",
                    cj.table, ci.relreplident, cj.table);
            }
        }
    }

    // --- sink & mapping ----------------------------------------------------------
    let transform_pairs = cfg.sync.values().filter_map(|tbl| {
        let mut parts = tbl.table.splitn(2, '.');
        let schema = parts.next()?.to_string();
        let table = parts.next()?.to_string();
        if tbl.transform.is_empty() {
            return None;
        }
        let rules: HashMap<String, pg2osync_engine::mapping::TransformOp> = tbl
            .transform
            .iter()
            .filter_map(|(c, op)| TransformOp::parse(op).map(|o| (c.clone(), o)))
            .collect();
        Some(((schema, table), rules))
    });
    let transforms = pg2osync_engine::mapping::Transforms::from_pairs(transform_pairs);
    use pg2osync_engine::mapping::TransformOp;
    let mapping_pairs = cfg.sync.iter().map(|(key, tbl)| {
        let mut parts = tbl.table.splitn(2, '.');
        let schema = parts.next().unwrap_or_default().to_string();
        let table = parts.next().unwrap_or_default().to_string();
        ((schema, table), tbl.index_name(key))
    });
    let mapping = pg2osync_engine::mapping::TableMapping::from_pairs(mapping_pairs);

    let api_key = cfg
        .target
        .api_key_env
        .as_ref()
        .and_then(|k| std::env::var(k).ok());
    let sink: Arc<dyn Sink> = match cfg.target.flavor.as_str() {
        "elasticsearch" => Arc::new(pg2osync_sink::elasticsearch::ElasticsearchSink::new(
            pg2osync_sink::elasticsearch::ElasticsearchSinkConfig {
                url: cfg.target.url.clone(),
                username: cfg.target.username.clone(),
                password: target_password,
                api_key,
                tls_verify: cfg.target.tls_verify,
            },
        )?),
        "meilisearch" => Arc::new(pg2osync_sink::meilisearch::MeilisearchSink::new(
            pg2osync_sink::meilisearch::MeilisearchSinkConfig {
                url: cfg.target.url.clone(),
                api_key: api_key.or(target_password),
                state_dir: cfg.target.state_dir.clone(),
            },
        )?),
        _ => Arc::new(pg2osync_sink::OpenSearchSink::new(
            pg2osync_sink::OpenSearchSinkConfig {
                url: cfg.target.url.clone(),
                username: cfg.target.username.clone(),
                password: target_password,
                tls_verify: cfg.target.tls_verify,
                serverless: cfg.target.serverless,
            },
        )?),
    };
    let index_specs: Vec<pg2osync_core::sink::IndexSpec> = cfg
        .sync
        .iter()
        .map(|(k, t)| pg2osync_core::sink::IndexSpec {
            name: t.index_name(k),
        })
        .collect();
    sink.ensure_ready(&index_specs).await?;

    let start_lsn =
        pg2osync_source::catalog::confirmed_flush_lsn(&admin, &cfg.source.slot_name).await?;
    let mut resume_from = sink.read_checkpoint().await?;
    // A checkpoint older than the slot's replay position is unusable: streaming
    // starts at the slot position, so anything between them would be lost.
    // The safe response is a full backfill (at-least-once makes it harmless).
    if let (Some(cp), Some(conf)) = (resume_from, start_lsn)
        && cp < conf
    {
        tracing::warn!(target: "pg2osync::run",
                "checkpoint {cp} predates slot position {conf}; forcing full backfill to avoid gaps");
        resume_from = None;
    }
    match &resume_from {
        Some(lsn) => {
            tracing::info!(target: "pg2osync::run", "checkpoint found at {lsn}; skipping backfill")
        }
        None => {
            tracing::info!(target: "pg2osync::run", "no usable checkpoint; running full backfill")
        }
    }

    // --- pipeline channels -------------------------------------------------------
    let (events_tx, events_rx) = mpsc::channel::<ChangeEvent>(10_000);
    let (ack_tx, _ack_rx) = watch::channel(None);

    let metrics = Arc::new(pg2osync_engine::metrics::Metrics::default());
    if cfg.metrics.enabled {
        let bind = cfg.metrics.bind.clone();
        let m = metrics.clone();
        tokio::spawn(async move { pg2osync_engine::metrics::serve(&bind, m).await });
    }
    let ctx = Arc::new(pg2osync_engine::PipelineCtx {
        sink: sink.clone(),
        mapping,
        transforms,
        cfg: cfg.engine.clone(),
        ack_tx,
        metrics,
    });

    let engine_handle = {
        let slot = cfg.source.slot_name.clone();
        let pub_name = cfg.source.publication.clone();
        tokio::spawn(async move {
            pg2osync_engine::run(events_rx, ctx, slot, pub_name, durable, engine_shutdown).await
        })
    };

    // --- backfill ------------------------------------------------------------------
    if resume_from.is_none() {
        // Snapshot AFTER slot creation: overlap becomes harmless duplicates,
        // gaps are impossible under at-least-once semantics (Spike B).
        // A dedicated connection holds the repeatable-read snapshot so the
        // admin connection stays free for catalog work.
        let (mut reader_conn, reader_conn_bg) =
            tokio_postgres::connect(&source_url, tokio_postgres::NoTls).await?;
        tokio::spawn(async move {
            let _ = reader_conn_bg.await;
        });
        let tx = reader_conn.transaction().await?;
        tx.execute(
            "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY",
            &[],
        )
        .await?;
        let bf_tx = events_tx.clone();

        for tbl in cfg.sync.values() {
            let cols = columns_of(&admin, &tbl.table).await?;
            let select_cols: Vec<String> = cols
                .iter()
                .map(|c| format!("{}::text", pg_quote_ident(&c.name)))
                .collect();
            let sql = format!(
                "COPY (SELECT {} FROM {}) TO STDOUT (FORMAT text)",
                select_cols.join(", "),
                tbl.table
            );
            let col_meta = cols.clone();

            let started = std::time::Instant::now();
            let mut count: u64 = 0;
            let copy_stream = tx.copy_out(&sql).await?;
            use futures::StreamExt;
            let mut lines = String::new();
            let mut stream = std::pin::pin!(copy_stream);
            while let Some(chunk) = stream.next().await {
                let chunk: bytes::Bytes = chunk.context("copy stream failed")?;
                lines.push_str(&String::from_utf8_lossy(&chunk));
                while let Some(pos) = lines.find('\n') {
                    let line: String = lines.drain(..pos + 1).collect();
                    let line = line.trim_end_matches('\n');
                    if line.is_empty() {
                        continue;
                    }
                    count += 1;
                    let fields: Vec<Option<Vec<u8>>> = split_copy_line(line)
                        .iter()
                        .map(|f| unescape_copy(f))
                        .collect();
                    let change = build_backfill_change(&tbl.table, &col_meta, &fields)?;
                    if bf_tx.send(change).await.is_err() {
                        anyhow::bail!("engine closed during backfill");
                    }
                    if count.is_multiple_of(5000) {
                        // synthetic commit boundaries give the engine periodic
                        // flush points during very large backfills
                        let _ = bf_tx
                            .send(ChangeEvent::Transaction(TransactionBoundary::Commit {
                                lsn: Lsn(0),
                                commit_ts_micros: 0,
                            }))
                            .await;
                    }
                }
            }
            // final boundary marks the whole backfill as one logical txn
            let _ = bf_tx
                .send(ChangeEvent::Transaction(TransactionBoundary::Commit {
                    lsn: Lsn(0),
                    commit_ts_micros: 0,
                }))
                .await;
            let secs = started.elapsed().as_secs_f64();
            tracing::info!(target: "pg2osync::run",
                "backfilled {} in {:.1}s (~{:.0} rows/s)", tbl.table, secs, count as f64 / secs);
        }
        drop(tx); // release snapshot
    }

    // --- streaming -----------------------------------------------------------------
    let result = if cfg.source.mode == "poll" {
        if resume_from.is_none() {
            tracing::info!(target: "pg2osync::run", "poll mode: backfill complete");
        }
        let tables = cfg
            .sync
            .values()
            .map(|t| (t.table.clone(), cfg.source.poll_column.clone()))
            .collect();
        let mut poll =
            pg2osync_source::poll::PollSource::new(pg2osync_source::poll::PollSourceConfig {
                url: source_url.clone(),
                tables,
                interval_secs: cfg.source.poll_interval_secs,
            });
        poll.stream(events_tx, shutdown_rx).await
    } else {
        let mut source = WalSource::new(src_cfg);
        source.stream(events_tx, shutdown_rx).await
    };

    let _ = engine_handle.await;
    result
}

fn pg_quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

fn split_copy_line(line: &str) -> Vec<String> {
    // tabs separate fields; escaped tabs inside values were encoded as \t
    let mut fields = vec![];
    let mut cur = String::new();

    let mut esc = false;
    for c in line.chars() {
        if esc {
            cur.push('\\');
            cur.push(c);
            esc = false;
            continue;
        }
        match c {
            '\\' => esc = true,
            '\t' => fields.push(std::mem::take(&mut cur)),
            other => cur.push(other),
        }
    }
    fields.push(cur);
    fields
}

fn build_backfill_change(
    qualified_table: &str,
    cols: &[ColMeta],
    fields: &[Option<Vec<u8>>],
) -> Result<ChangeEvent> {
    let (schema, table) = qualified_table
        .split_once('.')
        .unwrap_or(("public", qualified_table));
    let mut doc = serde_json::Map::new();
    let mut pk_map = serde_json::Map::new();
    let mut scalar_pk: Option<serde_json::Value> = None;
    for (i, meta) in cols.iter().enumerate() {
        let v = pg2osync_source::typemap::convert(
            meta.type_oid,
            fields.get(i).and_then(|f| f.as_deref()),
        )
        .map_err(|e| anyhow::anyhow!("column {}: {e}", meta.name))?;
        if meta.is_pk {
            scalar_pk.get_or_insert(v.clone());
            pk_map.insert(meta.name.clone(), v.clone());
        }
        doc.insert(meta.name.clone(), v);
    }
    let pk: serde_json::Value = if pk_map.len() == 1 {
        scalar_pk.expect("single pk inserted")
    } else {
        serde_json::Value::Object(pk_map)
    };
    Ok(ChangeEvent::Row(pg2osync_core::event::RowChange {
        schema: schema.to_string(),
        table: table.to_string(),
        kind: RowKind::Insert {
            pk,
            doc: serde_json::Value::Object(doc),
        },
    }))
}
