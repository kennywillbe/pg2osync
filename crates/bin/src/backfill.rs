//! Initial load for the PostgreSQL source.
//!
//! A table is read in primary-key ranges, each range in its own short
//! transaction, rather than all tables inside one long-lived snapshot. What
//! makes that safe is not snapshot consistency: it is that the replication slot
//! exists *before* the first range is read and nothing advances it during the
//! load, so streaming afterwards resumes from a position that predates every
//! range. Anything a range missed or read stale is still in the WAL and is
//! replayed onto an idempotent write.
//!
//! Two conditions that argument depends on, both already true here: writes are
//! whole-document upserts keyed by the row's primary key, and an update whose
//! TOASTed columns arrive as markers is completed from the stored document
//! before it is written — otherwise a replayed update would erase a value a
//! range had read correctly.
//!
//! Ranges are read unordered on purpose. `ORDER BY pk LIMIT n` forbids a bitmap
//! heap scan, so a row-estimate miss degrades to a sort per chunk, and index
//! order costs random heap access on any key that is not physically correlated.
//! `WHERE pk >= a AND pk < b` leaves the planner free to read the heap in
//! physical order.

use anyhow::{Context as _, Result, bail};
use pg2osync_core::checkpoint::StreamId;
use pg2osync_core::event::{ChangeEvent, RowKind, TransactionBoundary};
use pg2osync_core::load::{LoadCursor, LoadProgress, LoadScope, load_progress_key};
use pg2osync_core::lsn::Lsn;
use pg2osync_source::children::ChildSpec;
use std::collections::HashMap;
use tokio::sync::mpsc::Sender;

use crate::config::AppConfig;

/// Column metadata needed to rebuild documents outside the WAL stream.
#[derive(Clone)]
pub struct ColMeta {
    pub name: String,
    pub type_oid: u32,
    pub is_pk: bool,
}

/// Rows between synthetic commit boundaries. Large tables would otherwise
/// buffer entirely in the engine before the first flush.
const ROWS_PER_BOUNDARY: u64 = 5_000;

/// Enough sampled rows per boundary that an uneven key distribution produces
/// uneven ranges rather than empty ones.
const SAMPLE_ROWS_PER_BOUNDARY: f64 = 20.0;

/// How long to wait before looking at the slot again while it is under
/// pressure. The stream is draining WAL the whole time, so this resolves on its
/// own; the wait only has to be short enough not to waste the room it frees.
const SLOT_RECHECK: std::time::Duration = std::time::Duration::from_millis(500);

pub async fn columns_of(
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
            type_oid: r.get::<_, i32>(1) as u32,
            is_pk: r.get("is_pk"),
        })
        .collect())
}

/// Build the `COPY` statement for one table, embedding each child collection
/// as a pre-aggregated JSON column.
///
/// Aggregating child rows once and hash-joining beats fetching them per parent
/// by orders of magnitude, and unlike a correlated lateral it does not depend
/// on the child's foreign key being indexed — which a general-purpose tool
/// cannot assume.
#[allow(clippy::too_many_arguments)]
fn copy_statement(
    qualified_table: &str,
    cols: &[ColMeta],
    children: &[ChildSpec],
    soft_delete: Option<&str>,
    range: &KeyRange,
    pk_column: Option<&str>,
    filter: Option<&str>,
    table_filter: Option<&pg2osync_core::filter::Filter>,
) -> String {
    let mut selected: Vec<String> = cols
        .iter()
        .map(|c| format!("p.{}::text", quote_ident(&c.name)))
        .collect();
    let mut joins = String::new();
    let mut totals: Vec<String> = Vec::new();

    for (i, child) in children.iter().enumerate() {
        let alias = format!("c{i}");
        selected.push(format!("COALESCE({alias}.agg, '[]'::jsonb)::text"));
        // The total the source holds, so a capped array can say what it left
        // out. Without it the load embeds a short array that claims to be the
        // whole collection while a streamed re-fetch of the same parent says
        // otherwise — the two paths disagreeing where nobody looks.
        totals.push(format!("COALESCE({alias}.total, 0)::text"));
        // the aggregation itself comes from the source crate, so the array this
        // embeds is the same one a streamed re-fetch embeds — order, cap and all.
        // Two builders would disagree the moment either changed.
        //
        // the key is compared in its own type: a ::text cast on either side
        // makes the index unusable and turns this into a sequential scan
        joins.push_str(&format!(
            " LEFT JOIN ({agg}) {alias} ON {alias}.k = p.{parent_key}",
            agg = pg2osync_source::children::agg_subquery(child, None),
            parent_key = quote_ident(&child.parent_column),
        ));
    }

    let mut conditions: Vec<String> = Vec::new();
    if let Some(key) = pk_column {
        let predicate = range.predicate(&format!("p.{}", quote_ident(key)));
        if !predicate.is_empty() {
            conditions.push(predicate);
        }
    }
    // a row that is already deleted has no business being indexed and then
    // deleted again on the first poll cycle
    if let Some(predicate) = soft_delete {
        conditions.push(format!("NOT ({predicate})"));
    }
    // The table's own row filter, the same predicate the engine evaluates on
    // every streamed row. Pushed down here so the load never reads, ships and
    // indexes a row it would delete on arrival.
    if let Some(filter) = table_filter {
        conditions.push(format!("({})", filter.to_sql(&pg_dialect())));
    }
    // an operator's own predicate, parenthesised so it cannot rearrange the
    // conditions it is joined to
    if let Some(predicate) = filter {
        conditions.push(format!("({predicate})"));
    }
    let filter = if conditions.is_empty() {
        String::new()
    } else {
        format!(" WHERE {}", conditions.join(" AND "))
    };
    selected.extend(totals);
    format!(
        "COPY (SELECT {} FROM {} p{}{}) TO STDOUT (FORMAT text)",
        selected.join(", "),
        qualify(qualified_table),
        joins,
        filter
    )
}

/// One primary-key range to read, as already-quoted SQL literals.
///
/// `None` on either side means the range is open there, so a table read in one
/// piece is a single range open at both ends — exactly the statement this code
/// issued before it could chunk at all.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyRange {
    pub from: Option<String>,
    pub to: Option<String>,
}

impl KeyRange {
    /// The predicate for this range, or the empty string when it covers
    /// everything.
    fn predicate(&self, quoted_key: &str) -> String {
        match (&self.from, &self.to) {
            (None, None) => String::new(),
            (Some(from), None) => format!("{quoted_key} >= {from}"),
            (None, Some(to)) => format!("{quoted_key} < {to}"),
            (Some(from), Some(to)) => format!("{quoted_key} >= {from} AND {quoted_key} < {to}"),
        }
    }
}

/// Turn a list of interior boundaries into the ranges between them.
fn ranges_from_bounds(bounds: &[String]) -> Vec<KeyRange> {
    if bounds.is_empty() {
        return vec![KeyRange {
            from: None,
            to: None,
        }];
    }
    let mut ranges = Vec::with_capacity(bounds.len() + 1);
    ranges.push(KeyRange {
        from: None,
        to: Some(bounds[0].clone()),
    });
    for pair in bounds.windows(2) {
        ranges.push(KeyRange {
            from: Some(pair[0].clone()),
            to: Some(pair[1].clone()),
        });
    }
    ranges.push(KeyRange {
        from: Some(bounds[bounds.len() - 1].clone()),
        to: None,
    });
    ranges
}

/// Work out where to split a table, or decide not to split it.
///
/// The boundaries come from the database rather than from arithmetic on the
/// keys, so they work for any orderable key type and follow the actual
/// distribution. They are sampled rather than exact: uneven ranges cost a
/// little throughput, and a full pass to make them exact would cost more than
/// chunking saves.
async fn key_bounds(
    client: &tokio_postgres::Client,
    qualified_table: &str,
    cols: &[ColMeta],
    rows_per_chunk: i64,
) -> Result<Vec<String>> {
    let whole: Vec<String> = Vec::new();

    // Sampled over the whole table even when the load reads a filtered one:
    // that yields uneven ranges, never wrong ones, and sampling under the
    // predicate would cost a second scan to save nothing.
    // A composite key would need a row-constructor comparison, which is right
    // for PostgreSQL and pathological on MySQL; until that is worth branching
    // for, such a table is read in one piece as before. So is an append-only
    // table, which has no key to cut by at all.
    let pk: Vec<&ColMeta> = cols.iter().filter(|c| c.is_pk).collect();
    let [key] = pk.as_slice() else {
        return Ok(whole);
    };

    // reltuples is an estimate maintained by ANALYZE, and -1 means never
    // analysed. Reading it costs nothing, which is the point: deciding whether
    // to chunk must not itself scan the table.
    let estimate: f64 = client
        .query_one(
            "SELECT reltuples::float8 FROM pg_class WHERE oid = ($1::text)::regclass",
            &[&qualified_table],
        )
        .await
        .context("cannot estimate table size")?
        .get(0);
    if estimate < rows_per_chunk as f64 {
        return Ok(whole);
    }

    let chunks = (estimate / rows_per_chunk as f64).ceil() as i64;
    let boundaries = chunks - 1;
    if boundaries < 1 {
        return Ok(whole);
    }
    let fractions: Vec<f64> = (1..=boundaries).map(|i| i as f64 / chunks as f64).collect();

    // Sample a fraction of pages rather than reading the table: SYSTEM is the
    // cheap sampling method, and its page-level bias only makes the ranges less
    // even, which they already are.
    let percent =
        ((boundaries as f64 * SAMPLE_ROWS_PER_BOUNDARY / estimate) * 100.0).clamp(0.5, 100.0);
    let sql = format!(
        "SELECT quote_literal(v::text) FROM (              SELECT unnest(percentile_disc($1::float8[]) WITHIN GROUP (ORDER BY {key})) AS v              FROM {table} TABLESAMPLE SYSTEM ($2::float8)          ) s WHERE v IS NOT NULL",
        key = quote_ident(&key.name),
        table = qualify(qualified_table),
    );
    let rows = client
        .query(&sql, &[&fractions, &percent])
        .await
        .with_context(|| format!("cannot sample key boundaries of {qualified_table}"))?;

    // The sample can repeat a boundary where keys are dense; duplicates would
    // produce empty ranges, which are harmless but pointless.
    let mut bounds: Vec<String> = rows.iter().map(|r| r.get::<_, String>(0)).collect();
    bounds.dedup();
    if bounds.is_empty() {
        return Ok(whole);
    }
    tracing::debug!(target: "pg2osync::backfill",
        "{qualified_table}: ~{estimate:.0} rows, reading in {} range(s)", bounds.len() + 1);
    Ok(bounds)
}

/// A child collection reaches the document as one JSON column, so it can ride
/// along in the same `COPY` as an ordinary column of type `jsonb`.
fn child_column(child: &ChildSpec) -> ColMeta {
    const JSONB_OID: u32 = 3802;
    ColMeta {
        name: child.field.clone(),
        type_oid: JSONB_OID,
        is_pk: false,
    }
}

/// Stream every configured table into the engine channel.
#[allow(clippy::too_many_arguments)]
pub async fn run(
    cfg: &AppConfig,
    source_url: &str,
    tls: &pg2osync_source::tls::TlsSettings,
    admin: &tokio_postgres::Client,
    children: &HashMap<(String, String), Vec<ChildSpec>>,
    tx: Sender<ChangeEvent>,
    sink: &dyn pg2osync_core::sink::Sink,
    stream: &StreamId,
    mut load_done: tokio::sync::watch::Receiver<u64>,
    scope: &LoadScope,
) -> Result<()> {
    // A dedicated connection, but no transaction spanning the load: each range
    // is its own statement and its own snapshot. Holding one open for the whole
    // load pins the xmin horizon, so autovacuum cannot clean anything that died
    // after it started — minutes are fine, an hour on a busy table is not.
    // One connection per reader, and they outlive the table loop: opening them
    // per wave would pay a handshake for every few hundred milliseconds of work.
    // At the default width this is the single connection it always was.
    let mut readers = Vec::new();
    for _ in 0..cfg.source.load_workers.max(1) {
        readers.push(
            pg2osync_source::tls::connect(tls, source_url)
                .await
                .context("backfill connection failed")?,
        );
    }
    if readers.len() > 1 {
        tracing::info!(target: "pg2osync::backfill",
            "reading the load with {} connections", readers.len());
    }
    // One counter for the whole load: a mark only has to be increasing, and a
    // single sequence keeps the wait condition a comparison.
    let mut mark: u64 = 0;
    let mut progress_keys: Vec<String> = Vec::new();

    for tbl in cfg.sync.values() {
        if !scope.covers(&tbl.table) {
            continue;
        }
        let (schema, table) = split_qualified(&tbl.table);
        let mut cols = columns_of(admin, &tbl.table).await?;
        let child_specs: &[ChildSpec] = children
            .get(&(schema.to_string(), table.to_string()))
            .map(Vec::as_slice)
            .unwrap_or_default();

        for child in child_specs {
            // shadowing a real column would produce a document that quietly
            // disagrees with the row it came from
            let mut claimed = vec![child.field.clone()];
            if child.max_rows.is_some() {
                // a capped collection also writes what it left out
                claimed.push(child.truncated_field());
                claimed.push(child.total_field());
            }
            for name in claimed {
                if cols.iter().any(|c| c.name == name) {
                    bail!(
                        "[sync] child field {name:?} collides with a column of {}; \
                         choose another field name",
                        tbl.table
                    );
                }
            }
            cols.push(child_column(child));
        }
        // The column list is pinned here and reused for every range. A column
        // added mid-load would otherwise leave earlier ranges shaped one way
        // and later ranges another, and no WAL event repairs that: ADD COLUMN
        // with a constant default does not rewrite existing rows.
        let data_cols = &cols[..cols.len() - child_specs.len()];
        let pk_column = cols
            .iter()
            .filter(|c| c.is_pk)
            .map(|c| c.name.clone())
            .collect::<Vec<_>>();
        let pk_column = (pk_column.len() == 1).then(|| pk_column[0].clone());

        let progress_key = load_progress_key(stream, &tbl.table);
        let stored = if scope.resumable {
            progress_keys.push(progress_key.clone());
            sink.read_state(&progress_key)
                .await?
                .as_ref()
                .and_then(LoadProgress::from_doc)
        } else {
            None
        };
        let mut progress = match stored {
            Some(p) if p.finished => {
                tracing::info!(target: "pg2osync::backfill",
                    "{} was fully loaded by an earlier run; skipping", tbl.table);
                continue;
            }
            Some(p) => p,
            None => {
                let progress = LoadProgress {
                    cursor: LoadCursor::Ranges {
                        boundaries: key_bounds(
                            admin,
                            &tbl.table,
                            &cols,
                            cfg.source.load_chunk_rows.max(1),
                        )
                        .await?,
                        done: 0,
                    },
                    finished: false,
                };
                if scope.resumable {
                    sink.write_state(&progress_key, &progress.to_doc()).await?;
                }
                progress
            }
        };
        let LoadCursor::Ranges { boundaries, done } = &progress.cursor else {
            bail!(
                "load progress for {} was written by a loader that cuts the table \
                 differently; delete {progress_key} from the target to start over",
                tbl.table
            );
        };
        let (ranges, first) = (ranges_from_bounds(boundaries), *done);
        if first > 0 {
            tracing::info!(target: "pg2osync::backfill",
                "resuming the load of {} after {first} of {} range(s)",
                tbl.table, ranges.len());
        }

        let started = std::time::Instant::now();
        let mut count: u64 = 0;
        let plan = RangePlan {
            schema,
            table,
            qualified: &tbl.table,
            cols: &cols,
            data_cols,
            child_specs,
            soft_delete: tbl.soft_delete.as_deref(),
            pk_column: pk_column.as_deref(),
            append_only: tbl.append_only,
            filter: scope.filter.as_deref(),
            table_filter: scope.table_filter(&tbl.table),
        };
        // Ranges are read in waves, one per reader, with a barrier at the end of
        // each. A free-running pool would be faster by the width of the skew
        // between ranges and wrong in two ways: the engine forgets its record of
        // stream-removed keys on every load mark, which is only safe while
        // nothing from before that mark is still in flight; and progress is a
        // count of *leading* ranges written, which out-of-order completion
        // cannot advance. A wave satisfies both by construction — it is
        // contiguous, and it is finished before its mark is sent.
        for wave_start in (first..ranges.len()).step_by(readers.len()) {
            let wave = &ranges[wave_start..(wave_start + readers.len()).min(ranges.len())];
            // Between waves, never inside one. A `COPY` paused mid-stream would
            // hold its snapshot open for as long as the pause lasts, which is
            // the long transaction this design exists to avoid — and a range is
            // under a second's work at measured rates, so waiting for one to
            // finish costs nothing worth having.
            wait_for_slot_room(admin, &cfg.source.slot_name).await?;
            let read = wave
                .iter()
                .zip(readers.iter())
                .map(|(range, reader)| read_range(&plan, reader, range, &tx));
            for rows in futures::future::try_join_all(read).await? {
                count += rows;
            }
            // Strict order: rows, then the mark, then the progress document. A
            // crash anywhere in it can only lose the wave and redo it, which an
            // idempotent write makes free — the reverse order would claim a
            // range that was never written.
            mark += 1;
            tx.send(ChangeEvent::LoadMark(mark))
                .await
                .map_err(|_| anyhow::anyhow!("engine closed during backfill"))?;
            load_done
                .wait_for(|written| *written >= mark)
                .await
                .map_err(|_| anyhow::anyhow!("engine stopped before the range was written"))?;
            if let LoadCursor::Ranges { done, .. } = &mut progress.cursor {
                *done = wave_start + wave.len();
            }
            if scope.resumable {
                sink.write_state(&progress_key, &progress.to_doc()).await?;
            }
        }
        progress.finished = true;
        if scope.resumable {
            sink.write_state(&progress_key, &progress.to_doc()).await?;
        }
        let secs = started.elapsed().as_secs_f64();
        tracing::info!(target: "pg2osync::backfill",
            "read {} rows from {} in {:.1}s (~{:.0} rows/s) over {} range(s)",
            count, tbl.table, secs, count as f64 / secs.max(f64::EPSILON), ranges.len());
    }
    // Nothing left to resume, so nothing should claim otherwise on the next
    // start. Left behind, a finished document is merely a wasted read.
    for key in progress_keys {
        if let Err(e) = sink.clear_state(&key).await {
            tracing::warn!(target: "pg2osync::backfill",
                "could not remove load progress {key}: {e}");
        }
    }
    Ok(())
}

/// Everything about a table that every range of it shares.
///
/// Pinned once per table rather than per range: a column added mid-load would
/// otherwise leave earlier ranges shaped one way and later ranges another.
struct RangePlan<'a> {
    schema: &'a str,
    table: &'a str,
    qualified: &'a str,
    cols: &'a [ColMeta],
    data_cols: &'a [ColMeta],
    child_specs: &'a [ChildSpec],
    soft_delete: Option<&'a str>,
    pk_column: Option<&'a str>,
    /// Declared keyless: its rows go out with no key, and the engine files
    /// them under a hash of their content.
    append_only: bool,
    filter: Option<&'a str>,
    table_filter: Option<&'a pg2osync_core::filter::Filter>,
}

/// Read one range on one connection, and return how many rows it produced.
///
/// Everything a range needs is either in the plan or read on its own
/// connection, so several of these run at once without sharing anything but the
/// channel they send to — which is safe because ranges cover disjoint keys and
/// every document carries the position it was read at.
async fn read_range(
    plan: &RangePlan<'_>,
    reader: &tokio_postgres::Client,
    range: &KeyRange,
    tx: &Sender<ChangeEvent>,
) -> Result<u64> {
    // The position read before the range becomes the version of every document
    // the range produces. A change committed after this point necessarily has a
    // higher position, so it wins at the target whichever order the two arrive
    // in — which is what allows the copy to run beside the stream instead of
    // before it. It is a version only: the range's rows still never advance the
    // checkpoint.
    let chunk_lsn = current_lsn(reader).await?;
    let sql = copy_statement(
        plan.qualified,
        plan.data_cols,
        plan.child_specs,
        plan.soft_delete,
        range,
        plan.pk_column,
        plan.filter,
        plan.table_filter,
    );
    let copy_stream = reader.copy_out(&sql).await?;
    use futures::StreamExt;
    let mut count: u64 = 0;
    // A cursor rather than draining each line off the front: draining from the
    // front shifts everything behind it, so a network chunk full of short rows
    // paid one memmove of the whole chunk per row. Measured no faster on a load
    // this size — the target's indexing rate dominates — but the quadratic
    // shape is gone and there is one allocation less per row.
    let mut pending: Vec<u8> = Vec::new();
    // One line per collection for the whole range rather than per row: a range
    // where every parent has a duplicate would otherwise be one line each.
    let mut collections: Vec<Collection<'_>> = plan
        .child_specs
        .iter()
        .map(|spec| Collection {
            spec,
            duplicates: pg2osync_core::children::Duplicates::default(),
        })
        .collect();
    let mut stream = std::pin::pin!(copy_stream);
    while let Some(chunk) = stream.next().await {
        let chunk: bytes::Bytes = chunk.context("copy stream failed")?;
        pending.extend_from_slice(&chunk);
        let mut consumed = 0usize;
        while let Some(nl) = pending[consumed..].iter().position(|&b| b == b'\n') {
            let line = &pending[consumed..consumed + nl];
            consumed += nl + 1;
            if line.is_empty() {
                continue;
            }
            count += 1;
            // child collections arrive as ordinary jsonb columns of this very
            // row, so nothing extra is fetched per parent
            let change = {
                let text = String::from_utf8_lossy(line);
                let fields: Vec<Option<Vec<u8>>> =
                    split_copy_line(&text).iter().map(unescape_copy).collect();
                build_change(
                    plan.schema,
                    plan.table,
                    plan.cols,
                    &fields,
                    chunk_lsn,
                    &mut collections,
                    plan.append_only,
                )?
            };
            if tx.send(ChangeEvent::Row(change)).await.is_err() {
                bail!("engine closed during backfill");
            }
            if count.is_multiple_of(ROWS_PER_BOUNDARY) {
                send_boundary(tx).await?;
            }
        }
        // one compaction per network chunk, not one per row
        pending.drain(..consumed);
    }
    for collection in &collections {
        if let Some(message) = collection.duplicates.message(collection.spec) {
            tracing::warn!(target: "pg2osync::backfill", "{message}");
        }
    }
    Ok(count)
}

/// Hold the copy back while the replication slot is beyond the WAL the server
/// promised to keep.
///
/// The copy competes with the stream for the same target, and the stream is
/// what releases WAL: if the copy takes all the throughput, retention grows
/// until PostgreSQL invalidates the slot, which forces exactly the full reload
/// this is all built to avoid. So the copy yields, and the source's own
/// `wal_status` decides when — no threshold of ours to tune or get wrong.
async fn wait_for_slot_room(client: &tokio_postgres::Client, slot_name: &str) -> Result<()> {
    let mut waited = std::time::Duration::ZERO;
    loop {
        let Some(pressure) = pg2osync_source::catalog::slot_pressure(client, slot_name).await?
        else {
            // no slot: poll mode, where there is no WAL to retain
            return Ok(());
        };
        if pressure.lost() {
            bail!(
                "replication slot {slot_name:?} has been invalidated (wal_status = lost);                  the load cannot continue and streaming cannot resume from it. Raise                  max_slot_wal_keep_size, then start again"
            );
        }
        if !pressure.straining() {
            if !waited.is_zero() {
                tracing::info!(target: "pg2osync::backfill",
                    "slot {slot_name} back within its WAL budget after {:.1}s", waited.as_secs_f64());
            }
            return Ok(());
        }
        if waited.is_zero() {
            tracing::warn!(target: "pg2osync::backfill",
                "pausing the load: slot {slot_name} is at wal_status = {} with {} retained; the stream gets the throughput until it recovers",
                pressure.wal_status,
                pressure
                    .retained_bytes
                    .map(|b| format!("{b} bytes"))
                    .unwrap_or_else(|| "an unknown amount".into()));
        }
        tokio::time::sleep(SLOT_RECHECK).await;
        waited += SLOT_RECHECK;
    }
}

/// The source's current position, for versioning the rows a range is about to
/// produce.
async fn current_lsn(client: &tokio_postgres::Client) -> Result<Option<u64>> {
    let row = client
        .query_one("SELECT pg_current_wal_lsn()::text", &[])
        .await
        .context("cannot read the current WAL position")?;
    let text: String = row.get(0);
    match text.parse::<Lsn>() {
        Ok(lsn) => Ok(Some(lsn.0)),
        // Unversioned rows still load correctly; they just lose the protection
        // against a concurrent change, so the fact has to be visible.
        Err(_) => {
            tracing::warn!(target: "pg2osync::backfill",
                "cannot read the WAL position ({text:?}); loaded rows will be unversioned");
            Ok(None)
        }
    }
}

/// A boundary carrying `Lsn(0)`: it flushes the batch without advancing the
/// checkpoint, because backfill rows have no WAL position of their own.
async fn send_boundary(tx: &Sender<ChangeEvent>) -> Result<()> {
    tx.send(ChangeEvent::Transaction(TransactionBoundary::Commit {
        lsn: Lsn(0),
        commit_ts_micros: 0,
    }))
    .await
    .map_err(|_| anyhow::anyhow!("engine closed during backfill"))
}

/// One collection as a range reads it: its spec, and what it has seen of it.
///
/// Held together rather than in two parallel slices, so a range cannot count one
/// collection's duplicates against another's name.
struct Collection<'a> {
    spec: &'a ChildSpec,
    duplicates: pg2osync_core::children::Duplicates,
}

fn build_change(
    schema: &str,
    table: &str,
    cols: &[ColMeta],
    fields: &[Option<Vec<u8>>],
    version: Option<u64>,
    collections: &mut [Collection<'_>],
    append_only: bool,
) -> Result<pg2osync_core::event::RowChange> {
    let mut doc = serde_json::Map::new();
    let mut pk_map = serde_json::Map::new();
    for (i, meta) in cols.iter().enumerate() {
        let value = pg2osync_source::typemap::convert(
            meta.type_oid,
            fields.get(i).and_then(|f| f.as_deref()),
        )
        .map_err(|e| anyhow::anyhow!("column {}: {e}", meta.name))?;
        if meta.is_pk && !append_only {
            pk_map.insert(meta.name.clone(), value.clone());
        }
        doc.insert(meta.name.clone(), value);
    }
    // An append-only row carries no key even when the table happens to have
    // one: the engine mints its id from the document, the same way the
    // stream's rows get theirs, so the two paths cannot file one row twice.
    let pk = match pk_map.len() {
        0 if append_only => serde_json::Value::Null,
        0 => bail!(
            "table {schema}.{table} has no primary key; pg2osync needs one to \
             derive a stable document id, or `append_only = true` on its [sync] \
             section to index its rows as they arrive"
        ),
        1 => pk_map.into_iter().next().expect("single entry").1,
        _ => serde_json::Value::Object(pk_map),
    };
    // The collection totals ride at the end of the row, one per child in
    // configuration order. Shaping goes through the core helper so a loaded
    // document and a streamed one cannot disagree about what a collection is.
    let mut doc = serde_json::Value::Object(doc);
    for (nth, collection) in collections.iter_mut().enumerate() {
        let child = collection.spec;
        let array = doc
            .get(&child.field)
            .cloned()
            .unwrap_or_else(|| serde_json::Value::Array(Vec::new()));
        // A row without its total column is one nothing can call cut short,
        // so the array stands as the whole collection.
        let total = match fields.get(cols.len() + nth).and_then(|f| f.as_deref()) {
            Some(raw) => String::from_utf8_lossy(raw).parse().unwrap_or(0),
            None => array.as_array().map(Vec::len).unwrap_or(0) as i64,
        };
        let applied = pg2osync_core::children::apply_collection(&mut doc, child, array, total);
        collection.duplicates.record(child, &pk, applied.matched);
    }
    Ok(pg2osync_core::event::RowChange {
        schema: schema.to_string(),
        table: table.to_string(),
        kind: RowKind::Insert { pk, doc },
        version,
    })
}

/// COPY TEXT field unescaping; `\N` is the NULL marker.
fn unescape_copy(field: &String) -> Option<Vec<u8>> {
    if field == "\\N" {
        return None;
    }
    let mut out = Vec::with_capacity(field.len());
    let mut chars = field.chars();
    while let Some(c) = chars.next() {
        if c != '\\' {
            let mut buf = [0u8; 4];
            out.extend_from_slice(c.encode_utf8(&mut buf).as_bytes());
            continue;
        }
        match chars.next() {
            Some('t') => out.push(b'\t'),
            Some('n') => out.push(b'\n'),
            Some('r') => out.push(b'\r'),
            Some('\\') => out.push(b'\\'),
            Some(other) => {
                let mut buf = [0u8; 4];
                out.extend_from_slice(other.encode_utf8(&mut buf).as_bytes());
            }
            None => {}
        }
    }
    Some(out)
}

fn split_copy_line(line: &str) -> Vec<String> {
    let mut fields = vec![];
    let mut cur = String::new();
    let mut escaped = false;
    for c in line.chars() {
        if escaped {
            cur.push('\\');
            cur.push(c);
            escaped = false;
            continue;
        }
        match c {
            '\\' => escaped = true,
            '\t' => fields.push(std::mem::take(&mut cur)),
            other => cur.push(other),
        }
    }
    fields.push(cur);
    fields
}

pub fn split_qualified(qualified: &str) -> (&str, &str) {
    qualified.split_once('.').unwrap_or(("public", qualified))
}

fn qualify(qualified: &str) -> String {
    let (s, t) = split_qualified(qualified);
    format!("{}.{}", quote_ident(s), quote_ident(t))
}

/// How the COPY statement spells a filter: the table is aliased `p` and child
/// aggregates are joined beside it, so a bare column could be shadowed by a
/// join's `k`, `agg` or `total`. Strings double their quotes and nothing else,
/// which is PostgreSQL's rule under `standard_conforming_strings`.
fn pg_dialect() -> pg2osync_core::filter::SqlDialect<'static> {
    fn ident(name: &str) -> String {
        format!("p.{}", quote_ident(name))
    }
    pg2osync_core::filter::SqlDialect {
        quote_ident: &ident,
        quote_str: &pg_quote_str,
    }
}

/// The same for a statement with no alias, such as reconcile's key read or
/// the startup probe.
pub(crate) fn pg_dialect_bare() -> pg2osync_core::filter::SqlDialect<'static> {
    pg2osync_core::filter::SqlDialect {
        quote_ident: &quote_ident,
        quote_str: &pg_quote_str,
    }
}

fn pg_quote_str(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

#[cfg(test)]
mod tests {
    #[test]
    fn one_boundary_makes_two_ranges_that_meet_without_overlapping() {
        let ranges = ranges_from_bounds(&["'500'".into()]);
        assert_eq!(
            ranges,
            vec![
                KeyRange {
                    from: None,
                    to: Some("'500'".into())
                },
                KeyRange {
                    from: Some("'500'".into()),
                    to: None
                },
            ]
        );
    }

    #[test]
    fn the_ends_stay_open_so_no_row_falls_outside_every_range() {
        let ranges = ranges_from_bounds(&["'10'".into(), "'20'".into(), "'30'".into()]);
        assert_eq!(ranges.len(), 4);
        assert!(
            ranges.first().expect("first").from.is_none(),
            "no lower bound"
        );
        assert!(ranges.last().expect("last").to.is_none(), "no upper bound");
        // each range starts where the previous one ended: half-open, so a key
        // equal to a boundary belongs to exactly one range
        for pair in ranges.windows(2) {
            assert_eq!(pair[0].to, pair[1].from);
        }
    }

    #[test]
    fn no_boundaries_means_one_range_over_everything() {
        assert_eq!(ranges_from_bounds(&[]), vec![whole()]);
        assert_eq!(whole().predicate("\"id\""), "", "and no predicate at all");
    }

    #[test]
    fn a_range_predicate_is_closed_below_and_open_above() {
        let range = KeyRange {
            from: Some("'10'".into()),
            to: Some("'20'".into()),
        };
        assert_eq!(
            range.predicate("p.\"id\""),
            "p.\"id\" >= '10' AND p.\"id\" < '20'"
        );
        let first = KeyRange {
            from: None,
            to: Some("'10'".into()),
        };
        assert_eq!(first.predicate("p.\"id\""), "p.\"id\" < '10'");
        let last = KeyRange {
            from: Some("'20'".into()),
            to: None,
        };
        assert_eq!(last.predicate("p.\"id\""), "p.\"id\" >= '20'");
    }

    #[test]
    fn a_range_and_a_soft_delete_filter_both_apply() {
        let sql = copy_statement(
            "public.users",
            &[col("id")],
            &[],
            Some("deleted_at IS NOT NULL"),
            &KeyRange {
                from: Some("'1'".into()),
                to: Some("'9'".into()),
            },
            Some("id"),
            None,
            None,
        );
        assert!(sql.contains("p.\"id\" >= '1' AND p.\"id\" < '9'"), "{sql}");
        assert!(sql.contains("NOT (deleted_at IS NOT NULL)"), "{sql}");
        assert_eq!(sql.matches("WHERE").count(), 1, "one WHERE, not two: {sql}");
    }

    #[test]
    fn a_composite_key_reads_the_table_in_one_piece() {
        // a row-constructor comparison is right for PostgreSQL and pathological
        // on MySQL, so chunking waits until that is worth branching for
        let mut a = col("a");
        let mut b = col("b");
        a.is_pk = true;
        b.is_pk = true;
        let sql = copy_statement("public.t", &[a, b], &[], None, &whole(), None, None, None);
        assert!(!sql.contains("WHERE"), "{sql}");
    }

    /// The range that covers the whole table — what an unchunked read uses.
    fn whole() -> KeyRange {
        KeyRange {
            from: None,
            to: None,
        }
    }

    #[test]
    fn a_table_filter_is_pushed_down_beside_the_range_and_the_soft_delete() {
        let filter =
            pg2osync_core::filter::Filter::parse("status = 'active' AND dec > 10").expect("valid");
        let sql = copy_statement(
            "public.t",
            &[col("id")],
            &[],
            Some("deleted_at IS NOT NULL"),
            &KeyRange {
                from: Some("'5'".into()),
                to: None,
            },
            Some("id"),
            Some("tenant_id = 42"),
            Some(&filter),
        );
        assert!(
            sql.contains(
                "WHERE p.\"id\" >= '5' AND NOT (deleted_at IS NOT NULL) \
                 AND ((p.\"status\" = 'active' AND p.\"dec\" > 10)) AND (tenant_id = 42)"
            ),
            "one WHERE, every condition ANDed, the filter aliased and quoted: {sql}"
        );
    }

    #[test]
    fn a_soft_deleted_row_is_not_loaded_only_to_be_deleted_later() {
        let sql = copy_statement(
            "public.users",
            &[col("id")],
            &[],
            Some("deleted_at IS NOT NULL"),
            &whole(),
            None,
            None,
            None,
        );
        assert!(sql.contains("WHERE NOT (deleted_at IS NOT NULL)"), "{sql}");
    }

    #[test]
    fn a_resnapshot_filter_joins_the_conditions_already_there() {
        let sql = copy_statement(
            "public.users",
            &[col("id")],
            &[],
            Some("deleted_at IS NOT NULL"),
            &whole(),
            None,
            Some("tenant_id = 42"),
            None,
        );
        assert!(
            sql.contains("WHERE NOT (deleted_at IS NOT NULL) AND (tenant_id = 42)"),
            "{sql}"
        );
    }

    use super::*;

    #[test]
    fn copy_line_splitting_respects_escapes() {
        let fields = split_copy_line("1\\tx\ta\\nb\t\\N");
        assert_eq!(fields, vec!["1\\tx", "a\\nb", "\\N"]);
        assert_eq!(unescape_copy(&fields[0]), Some(b"1\tx".to_vec()));
        assert_eq!(unescape_copy(&fields[2]), None);
    }

    fn child(field: &str, table: &str, fk: &str, parent_key: &str) -> ChildSpec {
        ChildSpec::new(table, field, fk, parent_key).expect("qualified")
    }

    fn col(name: &str) -> ColMeta {
        ColMeta {
            name: name.into(),
            type_oid: 25,
            is_pk: false,
        }
    }

    #[test]
    fn a_table_without_children_reads_straight_through() {
        let sql = copy_statement(
            "public.users",
            &[col("id"), col("name")],
            &[],
            None,
            &whole(),
            None,
            None,
            None,
        );
        assert_eq!(
            sql,
            concat!(
                "COPY (SELECT p.\"id\"::text, p.\"name\"::text ",
                "FROM \"public\".\"users\" p) TO STDOUT (FORMAT text)"
            )
        );
    }

    #[test]
    fn a_child_collection_becomes_one_aggregated_column() {
        let sql = copy_statement(
            "public.customers",
            &[col("id")],
            &[child("orders", "public.orders", "customer_id", "id")],
            None,
            &whole(),
            None,
            None,
            None,
        );
        assert!(
            sql.contains("COALESCE(c0.agg, '[]'::jsonb)::text"),
            "a parent with no children must still get an empty array: {sql}"
        );
        assert!(
            sql.contains("PARTITION BY \"customer_id\""),
            "grouped by the foreign key: {sql}"
        );
        assert!(
            sql.contains("c0.k = p.\"id\""),
            "the key is compared in its own type, or the index goes unused: {sql}"
        );
        assert!(
            !sql.contains("::text = "),
            "no cast may appear on either side of the join: {sql}"
        );
    }

    #[test]
    fn the_load_and_the_stream_embed_the_same_array() {
        // The one thing that must not drift: if these two ever build the
        // aggregation separately, a re-snapshot changes documents for no reason
        // and nothing says so.
        let mut spec = child("orders", "public.orders", "customer_id", "id");
        spec.order_by = vec!["id".into()];
        spec.max_rows = Some(500);
        let sql = copy_statement(
            "public.customers",
            &[col("id")],
            std::slice::from_ref(&spec),
            None,
            &whole(),
            None,
            None,
            None,
        );
        assert!(
            sql.contains(&pg2osync_source::children::agg_subquery(&spec, None)),
            "the load embeds the source crate's own aggregation verbatim: {sql}"
        );
        assert!(sql.contains("ORDER BY \"id\""), "ordered: {sql}");
        assert!(sql.contains("rn <= 500"), "capped: {sql}");

        // and the projection rides along, because it lives in that aggregation
        spec.exclude_columns = vec!["internal_notes".into()];
        let projected = copy_statement(
            "public.customers",
            &[col("id")],
            std::slice::from_ref(&spec),
            None,
            &whole(),
            None,
            None,
            None,
        );
        assert!(
            projected.contains(&pg2osync_source::children::agg_subquery(&spec, None))
                && projected.contains("ARRAY['internal_notes']::text[]"),
            "the load cannot embed a different element from the re-fetch: {projected}"
        );
    }

    #[test]
    fn several_collections_each_get_their_own_join() {
        let sql = copy_statement(
            "public.customers",
            &[col("id")],
            &[
                child("orders", "public.orders", "customer_id", "id"),
                child("tickets", "support.tickets", "cust", "id"),
            ],
            None,
            &whole(),
            None,
            None,
            None,
        );
        assert!(sql.contains("c0.k = p.\"id\""), "{sql}");
        assert!(sql.contains("c1.k = p.\"id\""), "{sql}");
        assert!(sql.contains("\"support\".\"tickets\""), "{sql}");
        assert_eq!(sql.matches("LEFT JOIN").count(), 2);
    }

    #[test]
    fn identifiers_are_quoted_everywhere_they_appear() {
        let sql = copy_statement(
            "public.we\"ird",
            &[col("od\"d")],
            &[child("kids", "public.ch\"ild", "fk\"y", "pk\"y")],
            None,
            &whole(),
            None,
            None,
            None,
        );
        assert!(sql.contains("\"we\"\"ird\""), "{sql}");
        assert!(sql.contains("\"od\"\"d\""), "{sql}");
        assert!(sql.contains("\"fk\"\"y\""), "{sql}");
    }

    #[test]
    fn a_one_to_one_child_loads_as_an_object_and_counts_a_duplicate() {
        let mut spec = child("profile", "public.profiles", "user_id", "id");
        spec.single = true;
        let pk = ColMeta {
            name: "id".into(),
            type_oid: 23,
            is_pk: true,
        };
        let load = |agg: &str, total: &str| {
            let fields: Vec<Option<Vec<u8>>> = [b"1".to_vec(), agg.into(), total.into()]
                .into_iter()
                .map(Some)
                .collect();
            let mut collections = vec![Collection {
                spec: &spec,
                duplicates: pg2osync_core::children::Duplicates::default(),
            }];
            let mut change = build_change(
                "public",
                "users",
                &[pk.clone(), child_column(&spec)],
                &fields,
                None,
                &mut collections,
                false,
            )
            .expect("a row with one collection");
            let doc = change
                .doc_mut()
                .expect("an insert carries its document")
                .clone();
            (doc, collections.remove(0).duplicates.message(&spec))
        };

        let (one, quiet) = load(r#"[{"bio": "hi"}]"#, "1");
        assert_eq!(one["profile"], serde_json::json!({"bio": "hi"}));
        assert!(one.get("profile_truncated").is_none());
        assert!(quiet.is_none());

        let (none, _) = load("[]", "0");
        assert_eq!(
            none["profile"],
            serde_json::json!(null),
            "the key stays, the value is null"
        );

        let (two, loud) = load(r#"[{"id": 4}, {"id": 9}]"#, "2");
        assert_eq!(
            two["profile"],
            serde_json::json!({"id": 4}),
            "the lowest key stands"
        );
        assert!(
            loud.expect("a second row is reported")
                .contains("public.profiles"),
            "the load says which collection matched twice"
        );
    }

    #[test]
    fn multibyte_values_survive_unescaping() {
        assert_eq!(
            unescape_copy(&"çğü".to_string()),
            Some("çğü".as_bytes().to_vec())
        );
    }
}
