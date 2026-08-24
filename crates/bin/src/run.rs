//! Pipeline orchestration: bootstrap, initial load, live streaming.
//!
//! Everything source-specific stays behind `SourceKind`; the engine and sink
//! wiring below is identical for every source.

use anyhow::{Context as _, Result, bail};
use pg2osync_core::checkpoint::{Checkpoint, SOURCE_MYSQL, SOURCE_POSTGRES, StreamId};
use pg2osync_core::event::ChangeEvent;
use pg2osync_core::lsn::Lsn;
use pg2osync_core::sink::{IndexSpec, Sink};
use pg2osync_engine::mapping::{
    DurableLsn, Projection, Projections, TableMapping, TransformOp, Transforms,
};
use pg2osync_engine::{PipelineCtx, PositionRenderer};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, watch};

use crate::backfill::split_qualified;
use crate::config::AppConfig;

/// How far the CLI takes the pipeline.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    /// Create source-side objects and target indices, then exit.
    Bootstrap,
    /// Initial load plus continuous streaming.
    Run,
}

/// Channel depth between source and engine. Bounded channels are the whole
/// backpressure mechanism: a slow sink must stall the source, not buffer.
const EVENT_CHANNEL_DEPTH: usize = 10_000;

#[allow(clippy::too_many_arguments)]
pub async fn run_pipeline(
    cfg: AppConfig,
    source_url: String,
    admin_url: String,
    target_password: Option<String>,
    shutdown_rx: watch::Receiver<bool>,
    durable: DurableLsn,
    mode: Mode,
) -> Result<()> {
    let sink = build_sink(&cfg, target_password)?;
    let index_specs: Vec<IndexSpec> = cfg
        .sync
        .iter()
        .map(|(k, t)| IndexSpec {
            name: t.index_name(k),
        })
        .collect();

    match cfg.source.flavor.as_str() {
        "mysql" => {
            run_mysql(
                cfg,
                source_url,
                sink,
                index_specs,
                shutdown_rx,
                durable,
                mode,
            )
            .await
        }
        "postgres" | "postgresql" => {
            run_postgres(
                cfg,
                source_url,
                admin_url,
                sink,
                index_specs,
                shutdown_rx,
                durable,
                mode,
            )
            .await
        }
        other => bail!("unsupported source.flavor {other:?}; expected \"postgres\" or \"mysql\""),
    }
}

// ---------------------------------------------------------------- shared wiring

pub fn build_sink(cfg: &AppConfig, target_password: Option<String>) -> Result<Arc<dyn Sink>> {
    let api_key = cfg
        .target
        .api_key_env
        .as_ref()
        .and_then(|k| std::env::var(k).ok());
    let retry = pg2osync_sink::RetryPolicy {
        max_attempts: cfg.engine.retry_max.max(1),
        base_backoff_ms: cfg.engine.retry_backoff_ms.max(1),
    };
    let sink: Arc<dyn Sink> = match cfg.target.flavor.as_str() {
        "elasticsearch" => Arc::new(pg2osync_sink::elasticsearch::ElasticsearchSink::new(
            pg2osync_sink::elasticsearch::ElasticsearchSinkConfig {
                url: cfg.target.url.clone(),
                username: cfg.target.username.clone(),
                password: target_password,
                api_key,
                tls_verify: cfg.target.tls_verify,
                retry,
            },
        )?),
        "meilisearch" => Arc::new(pg2osync_sink::meilisearch::MeilisearchSink::new(
            pg2osync_sink::meilisearch::MeilisearchSinkConfig {
                url: cfg.target.url.clone(),
                api_key: api_key.or(target_password),
                state_dir: cfg.target.state_dir.clone(),
            },
        )?),
        "opensearch" => Arc::new(pg2osync_sink::OpenSearchSink::new(
            pg2osync_sink::OpenSearchSinkConfig {
                url: cfg.target.url.clone(),
                username: cfg.target.username.clone(),
                password: target_password,
                tls_verify: cfg.target.tls_verify,
                serverless: cfg.target.serverless,
                retry,
            },
        )?),
        other => bail!(
            "unsupported target.flavor {other:?}; expected \"opensearch\", \
             \"elasticsearch\" or \"meilisearch\""
        ),
    };
    Ok(sink)
}

fn table_mapping(cfg: &AppConfig) -> TableMapping {
    TableMapping::from_pairs(cfg.sync.iter().map(|(key, tbl)| {
        let (schema, table) = split_qualified(&tbl.table);
        ((schema.to_string(), table.to_string()), tbl.index_name(key))
    }))
}

fn projections(cfg: &AppConfig) -> Projections {
    Projections::from_pairs(cfg.sync.values().filter_map(|tbl| {
        let (schema, table) = split_qualified(&tbl.table);
        let rule = match (&tbl.columns, tbl.exclude_columns.is_empty()) {
            (Some(cols), _) => Projection::Include(cols.clone()),
            (None, false) => Projection::Exclude(tbl.exclude_columns.clone()),
            (None, true) => return None,
        };
        Some(((schema.to_string(), table.to_string()), rule))
    }))
}

fn transforms(cfg: &AppConfig) -> Result<Transforms> {
    let mut pairs = Vec::new();
    for (key, tbl) in &cfg.sync {
        if tbl.transform.is_empty() {
            continue;
        }
        let (schema, table) = split_qualified(&tbl.table);
        let mut rules = HashMap::new();
        for (col, op) in &tbl.transform {
            let parsed = TransformOp::parse(op).with_context(|| {
                format!("[sync.{key}.transform] {col} = {op:?} is not \"hash\" or \"redact\"")
            })?;
            rules.insert(col.clone(), parsed);
        }
        pairs.push(((schema.to_string(), table.to_string()), rules));
    }
    Ok(Transforms::from_pairs(pairs))
}

/// Build the engine context and start the metrics endpoint.
fn pipeline_ctx(cfg: &AppConfig, sink: Arc<dyn Sink>) -> Result<Arc<PipelineCtx>> {
    let metrics = Arc::new(pg2osync_engine::metrics::Metrics::default());
    if cfg.metrics.enabled {
        let bind = cfg.metrics.bind.clone();
        let m = metrics.clone();
        tokio::spawn(async move { pg2osync_engine::metrics::serve(&bind, m).await });
    }
    let (ack_tx, _ack_rx) = watch::channel(None);
    Ok(Arc::new(PipelineCtx {
        sink,
        mapping: table_mapping(cfg),
        projections: projections(cfg),
        transforms: transforms(cfg)?,
        cfg: cfg.engine.clone(),
        ack_tx,
        metrics,
    }))
}

/// A stored checkpoint is only usable when it belongs to this exact stream.
fn usable_checkpoint(stored: Option<Checkpoint>, expected: &StreamId) -> Option<Checkpoint> {
    let stored = stored?;
    if stored.stream.source != expected.source || stored.stream.stream != expected.stream {
        tracing::warn!(target: "pg2osync::run",
            "checkpoint belongs to {}/{} but this run is {}/{}; ignoring it and \
             running a full initial load",
            stored.stream.source, stored.stream.stream, expected.source, expected.stream);
        return None;
    }
    Some(stored)
}

// ------------------------------------------------------------------- PostgreSQL

#[allow(clippy::too_many_arguments)]
async fn run_postgres(
    cfg: AppConfig,
    source_url: String,
    admin_url: String,
    sink: Arc<dyn Sink>,
    index_specs: Vec<IndexSpec>,
    shutdown_rx: watch::Receiver<bool>,
    durable: DurableLsn,
    mode: Mode,
) -> Result<()> {
    use pg2osync_source::runner::{WalSource, WalSourceConfig};

    let polling = cfg.source.mode == "poll";
    let tls = cfg.tls_settings(&source_url)?;
    tracing::info!(target: "pg2osync::run", "source sslmode={}", tls.mode.as_str());
    let admin = tls
        .connect(&admin_url)
        .await
        .context("cannot connect to source PostgreSQL")?;

    let children = child_specs(&cfg)?;
    let mut tables: Vec<String> = cfg.sync.values().map(|t| t.table.clone()).collect();
    // child tables must join the publication or their changes never reach us
    for tbl in cfg.sync.values() {
        for child in &tbl.children {
            if !tables.contains(&child.table) {
                tables.push(child.table.clone());
            }
        }
    }

    let src_cfg: WalSourceConfig = wal_config(
        &cfg,
        &source_url,
        &admin_url,
        &tables,
        &children,
        &durable,
        &tls,
    )?;
    let source = WalSource::new(src_cfg.clone());

    if !polling {
        source.bootstrap(&admin).await?;
        warn_on_child_replica_identity(&cfg, &admin).await?;
    }
    sink.ensure_ready(&index_specs).await?;

    if mode == Mode::Bootstrap {
        println!("✓ source objects and target indices are ready");
        if !polling {
            println!(
                "  publication: {}\n  slot: {}",
                cfg.source.publication, cfg.source.slot_name
            );
        }
        return Ok(());
    }

    let stream_id = StreamId {
        source: SOURCE_POSTGRES.into(),
        stream: cfg.source.slot_name.clone(),
        publication: cfg.source.publication.clone(),
    };
    let render: PositionRenderer = Arc::new(|token| Lsn(token).to_string());

    // Poll mode has no source position to resume from, so a leftover WAL
    // checkpoint would skip rows changed while the process was down.
    let stored = if polling {
        None
    } else {
        usable_checkpoint(sink.read_checkpoint().await?, &stream_id)
    };
    let mut resume_from = stored.map(|c| Lsn(c.token));
    if !polling {
        let slot_lsn =
            pg2osync_source::catalog::confirmed_flush_lsn(&admin, &cfg.source.slot_name).await?;
        // A checkpoint behind the slot's replay position is unusable: streaming
        // resumes at the slot position, so the gap between them would be lost.
        if let (Some(cp), Some(slot)) = (resume_from, slot_lsn)
            && cp < slot
        {
            tracing::warn!(target: "pg2osync::run",
                "checkpoint {cp} predates slot position {slot}; running a full \
                 initial load to avoid a gap");
            resume_from = None;
        }
    }
    match &resume_from {
        Some(lsn) => {
            tracing::info!(target: "pg2osync::run", "resuming from checkpoint {lsn}")
        }
        None => tracing::info!(target: "pg2osync::run", "no usable checkpoint; initial load"),
    }

    let (events_tx, events_rx) = mpsc::channel::<ChangeEvent>(EVENT_CHANNEL_DEPTH);
    let ctx = pipeline_ctx(&cfg, sink)?;
    let engine = spawn_engine(
        events_rx,
        ctx,
        stream_id,
        render,
        durable,
        shutdown_rx.clone(),
    );

    if resume_from.is_none() {
        crate::backfill::run(&cfg, &admin_url, &tls, &admin, &children, events_tx.clone()).await?;
    }

    let result = if polling {
        let mut poll =
            pg2osync_source::poll::PollSource::new(poll_config(&cfg, &source_url, tls.clone())?);
        poll.stream(events_tx, shutdown_rx).await
    } else {
        let mut source = WalSource::new(src_cfg);
        source.stream(events_tx, shutdown_rx).await
    };
    let _ = engine.await;
    result
}

#[allow(clippy::too_many_arguments)]
fn wal_config(
    cfg: &AppConfig,
    source_url: &str,
    admin_url: &str,
    tables: &[String],
    children: &HashMap<(String, String), Vec<pg2osync_source::children::ChildSpec>>,
    durable: &DurableLsn,
    tls: &pg2osync_source::tls::TlsSettings,
) -> Result<pg2osync_source::runner::WalSourceConfig> {
    let url = url::Url::parse(source_url).context("source url is not a valid URL")?;
    let mut child_parents = HashMap::new();
    let mut parent_pk_columns = HashMap::new();
    for tbl in cfg.sync.values() {
        let (ps, pt) = split_qualified(&tbl.table);
        parent_pk_columns.insert(
            (ps.to_string(), pt.to_string()),
            tbl.primary_key.clone().unwrap_or_else(|| "id".into()),
        );
        for child in &tbl.children {
            let (cs, ct) = split_qualified(&child.table);
            child_parents.insert(
                (cs.to_string(), ct.to_string()),
                (ps.to_string(), pt.to_string()),
            );
        }
    }
    Ok(pg2osync_source::runner::WalSourceConfig {
        host: url.host_str().unwrap_or("localhost").into(),
        port: url.port().unwrap_or(5432),
        user: url.username().into(),
        password: url.password().unwrap_or_default().into(),
        database: url.path().trim_start_matches('/').to_string(),
        slot_name: cfg.source.slot_name.clone(),
        publication: cfg.source.publication.clone(),
        tables: tables.to_vec(),
        start_lsn: None,
        // Feedback to PostgreSQL is clamped to this: acknowledging beyond the
        // durable checkpoint lets PG recycle WAL we have not indexed yet, which
        // loses data on crash-restart.
        durable: Some(durable.0.clone()),
        admin_url: Some(admin_url.to_string()),
        tls: tls.clone(),
        children: children.clone(),
        child_parents,
        parent_pk_columns,
    })
}

fn poll_config(
    cfg: &AppConfig,
    source_url: &str,
    tls: pg2osync_source::tls::TlsSettings,
) -> Result<pg2osync_source::poll::PollSourceConfig> {
    Ok(pg2osync_source::poll::PollSourceConfig {
        url: source_url.to_string(),
        tables: cfg
            .sync
            .values()
            .map(|t| pg2osync_source::poll::PollTable {
                qualified: t.table.clone(),
                poll_column: t
                    .poll_column
                    .clone()
                    .unwrap_or_else(|| cfg.source.poll_column.clone()),
                pk_columns: vec![t.primary_key.clone().unwrap_or_else(|| "id".into())],
            })
            .collect(),
        interval_secs: cfg.source.poll_interval_secs,
        page_size: cfg.source.poll_page_size,
        tls,
    })
}

fn child_specs(
    cfg: &AppConfig,
) -> Result<HashMap<(String, String), Vec<pg2osync_source::children::ChildSpec>>> {
    let mut map: HashMap<_, Vec<_>> = HashMap::new();
    for tbl in cfg.sync.values() {
        let (schema, table) = split_qualified(&tbl.table);
        for child in &tbl.children {
            let spec = pg2osync_source::children::ChildSpec::new(
                &child.table,
                &child.field,
                &child.foreign_key,
                &tbl.primary_key.clone().unwrap_or_else(|| "id".into()),
            )?;
            map.entry((schema.to_string(), table.to_string()))
                .or_default()
                .push(spec);
        }
    }
    Ok(map)
}

/// Deletes on a child table carry no foreign key under the default replica
/// identity, so the parent cannot be located. Warn before it happens.
async fn warn_on_child_replica_identity(
    cfg: &AppConfig,
    admin: &tokio_postgres::Client,
) -> Result<()> {
    for tbl in cfg.sync.values() {
        for child in &tbl.children {
            let (schema, table) = split_qualified(&child.table);
            let info = pg2osync_source::catalog::table_info(admin, schema, table)
                .await
                .with_context(|| format!("cannot inspect child table {}", child.table))?;
            if info.relreplident != 'f' {
                tracing::warn!(target: "pg2osync::run",
                    "child table {} has REPLICA IDENTITY '{}': DELETEs on it cannot \
                     refresh the parent document. Run: ALTER TABLE {} REPLICA IDENTITY FULL",
                    child.table, info.relreplident, child.table);
            }
        }
    }
    Ok(())
}

// ------------------------------------------------------------------------ MySQL

#[allow(clippy::too_many_arguments)]
async fn run_mysql(
    cfg: AppConfig,
    source_url: String,
    sink: Arc<dyn Sink>,
    index_specs: Vec<IndexSpec>,
    shutdown_rx: watch::Receiver<bool>,
    durable: DurableLsn,
    mode: Mode,
) -> Result<()> {
    use pg2osync_source_mysql::catalog as mysql_catalog;
    use pg2osync_source_mysql::runner::MySqlSource;

    if cfg.sync.values().any(|t| !t.children.is_empty()) {
        bail!("nested children are not supported for the MySQL source yet");
    }
    let src_cfg = mysql_config(&cfg, &source_url)?;
    let source = MySqlSource::new(src_cfg);
    let mut admin = source.admin_connection().await?;
    source.bootstrap(&mut admin).await?;
    sink.ensure_ready(&index_specs).await?;

    if mode == Mode::Bootstrap {
        println!("✓ MySQL prerequisites met and target indices are ready");
        return Ok(());
    }

    let stream_id = StreamId {
        source: SOURCE_MYSQL.into(),
        stream: cfg.source.server_id.to_string(),
        publication: String::new(),
    };
    let stored = usable_checkpoint(sink.read_checkpoint().await?, &stream_id);
    let resume = stored.and_then(|c| mysql_catalog::parse_position(&c.position));
    let (file_prefix, _) = match &resume {
        Some((file, _)) => {
            mysql_catalog::split_binlog_file(file).unwrap_or_else(|| ("binlog".to_string(), 0))
        }
        None => {
            let (file, _) = mysql_catalog::master_position(&mut admin).await?;
            mysql_catalog::split_binlog_file(&file).unwrap_or_else(|| ("binlog".to_string(), 0))
        }
    };
    let render: PositionRenderer = {
        let prefix = file_prefix.clone();
        Arc::new(move |token| mysql_catalog::position_text(&prefix, token))
    };

    let (events_tx, events_rx) = mpsc::channel::<ChangeEvent>(EVENT_CHANNEL_DEPTH);
    let ctx = pipeline_ctx(&cfg, sink)?;
    let engine = spawn_engine(
        events_rx,
        ctx,
        stream_id,
        render,
        durable,
        shutdown_rx.clone(),
    );

    let mut src_cfg = mysql_config(&cfg, &source_url)?;
    match resume {
        Some((file, pos)) => {
            tracing::info!(target: "pg2osync::run", "resuming binlog from {file}@{pos}");
            src_cfg.start_file = Some(file);
            src_cfg.start_pos = pos;
        }
        None => {
            tracing::info!(target: "pg2osync::run", "no usable checkpoint; initial load");
            let start = source.snapshot(&mut admin, &events_tx).await?;
            tracing::info!(target: "pg2osync::run",
                "snapshot complete; streaming from {}@{}", start.file, start.pos);
            src_cfg.start_file = Some(start.file);
            src_cfg.start_pos = start.pos;
        }
    }

    let mut streaming = MySqlSource::new(src_cfg);
    let result = streaming.stream(events_tx, shutdown_rx).await;
    let _ = engine.await;
    result
}

fn mysql_config(
    cfg: &AppConfig,
    source_url: &str,
) -> Result<pg2osync_source_mysql::runner::MySqlSourceConfig> {
    let url = url::Url::parse(source_url).context("source url is not a valid URL")?;
    let tables = cfg
        .sync
        .values()
        .map(|t| {
            let (schema, table) = split_qualified(&t.table);
            (schema.to_string(), table.to_string())
        })
        .collect();
    Ok(pg2osync_source_mysql::runner::MySqlSourceConfig {
        host: url.host_str().unwrap_or("localhost").into(),
        port: url.port().unwrap_or(3306),
        user: percent_decode(url.username()),
        password: percent_decode(url.password().unwrap_or_default()),
        server_id: cfg.source.server_id,
        tables,
        start_file: None,
        start_pos: 0,
    })
}

/// URL-encoded credentials are common in connection strings; MySQL auth needs
/// the decoded bytes.
fn percent_decode(value: &str) -> String {
    let bytes = value.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            let hex = std::str::from_utf8(&bytes[i + 1..i + 3]).unwrap_or("");
            if let Ok(b) = u8::from_str_radix(hex, 16) {
                out.push(b);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn spawn_engine(
    events_rx: mpsc::Receiver<ChangeEvent>,
    ctx: Arc<PipelineCtx>,
    stream_id: StreamId,
    render: PositionRenderer,
    durable: DurableLsn,
    shutdown: watch::Receiver<bool>,
) -> tokio::task::JoinHandle<Result<(), pg2osync_core::CoreError>> {
    tokio::spawn(async move {
        pg2osync_engine::run(events_rx, ctx, stream_id, render, durable, shutdown).await
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn credentials_are_percent_decoded() {
        assert_eq!(percent_decode("p%40ss%3Aword"), "p@ss:word");
        assert_eq!(percent_decode("plain"), "plain");
        assert_eq!(percent_decode("trailing%"), "trailing%");
    }

    #[test]
    fn checkpoints_from_another_stream_are_rejected() {
        let expected = StreamId {
            source: SOURCE_POSTGRES.into(),
            stream: "slot_a".into(),
            publication: "pub".into(),
        };
        let stored = Checkpoint {
            stream: StreamId {
                source: SOURCE_POSTGRES.into(),
                stream: "slot_b".into(),
                publication: "pub".into(),
            },
            token: 42,
            position: "0/2A".into(),
        };
        assert!(usable_checkpoint(Some(stored.clone()), &expected).is_none());
        let mut same = stored;
        same.stream.stream = "slot_a".into();
        assert!(usable_checkpoint(Some(same), &expected).is_some());
    }
}
