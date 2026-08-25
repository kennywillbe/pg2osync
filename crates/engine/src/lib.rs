//! The multi-task pipeline:
//!
//! source task ──events──► engine task ──batches──► sink task
//!                              │                     │
//!                              └── checkpoint ◄── acks (watch)
//!
//! Rows buffer until their COMMIT boundary (never partially indexed), are
//! flushed in bounded batches, and acknowledged upstream only after the
//! checkpoint is durable.

pub mod api;
pub mod http;
pub mod mapping;
pub mod metrics;

use crate::mapping::TableMapping;
use pg2osync_core::checkpoint::{Checkpoint, StreamId};
use pg2osync_core::error::CoreError;
use pg2osync_core::event::{ChangeEvent, RowKind, TransactionBoundary};
use pg2osync_core::lsn::Lsn;
use pg2osync_core::sink::{DocumentOp, LsnOp, Sink, SinkAck};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use tokio::sync::{mpsc, watch};

/// `[engine]` config section.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct EngineConfig {
    /// Rows per sink request.
    pub batch_size: usize,
    /// Approximate byte ceiling per sink request; whichever limit is reached
    /// first splits the batch.
    pub batch_max_bytes: usize,
    /// Warning threshold for a single open transaction. Exceeding it means the
    /// transaction is split, so the sink briefly holds a partial transaction.
    pub txn_buffer_cap_mb: usize,
    pub retry_max: u32,
    pub retry_backoff_ms: u64,
    pub checkpoint_interval_ms: u64,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            batch_size: 500,
            batch_max_bytes: 10 * 1024 * 1024,
            txn_buffer_cap_mb: 256,
            retry_max: 10,
            retry_backoff_ms: 500,
            checkpoint_interval_ms: 500,
        }
    }
}

/// Render a primary-key JSON value into the OpenSearch `_id` string.
///
/// Scalars map directly; composite keys become a deterministic `col=val`
/// list so the same row always yields the same id.
pub fn pk_to_id(pk: &Value) -> String {
    match pk {
        Value::Null => "__null__".into(),
        Value::String(s) => s.clone(),
        Value::Number(n) => n.to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Object(map) => {
            let mut pairs: Vec<String> = map
                .iter()
                .map(|(k, v)| format!("{k}={}", scalar_display(v)))
                .collect();
            pairs.sort();
            pairs.join(",")
        }
        other => other.to_string(),
    }
}

fn scalar_display(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

/// Shared durable position: how far ahead the source may acknowledge.
#[derive(Clone, Default)]
pub struct DurableLsn(pub Arc<std::sync::atomic::AtomicU64>);

impl DurableLsn {
    pub fn store(&self, lsn: Lsn) {
        self.0.store(lsn.0, std::sync::atomic::Ordering::SeqCst);
    }
}

/// Work handed to the sink task, in the order the source produced it.
///
/// TRUNCATE travels through the same channel as writes: executing it directly
/// would let writes still queued ahead of it land afterwards and resurrect
/// documents the source has already dropped.
enum SinkCommand {
    Write(Vec<LsnOp>),
    /// Clearing an index, carrying the position it happened at so a versioned
    /// target can order it against the writes around it.
    Truncate(String, Option<u64>),
    /// A commit whose rows were already handed over — an empty transaction, or
    /// one whose row count was an exact multiple of the batch size. The
    /// position still has to be acknowledged, and it travels through this
    /// channel so it lands *after* the writes it belongs behind.
    Position(Lsn),
}

/// Renders an engine position token into the source's own textual form.
///
/// Injected by the binary so the engine can persist a resumable checkpoint
/// without knowing whether the token is a WAL LSN or a binlog offset.
pub type PositionRenderer = Arc<dyn Fn(u64) -> String + Send + Sync>;

/// The inverse: turns a position a caller supplies back into an ordering token.
///
/// Returns `None` when the text does not belong to this source's position
/// space, so a caller pasting a MySQL coordinate at a PostgreSQL pipeline gets
/// a clear rejection rather than a nonsensical wait.
pub type PositionParser = Arc<dyn Fn(&str) -> Option<u64> + Send + Sync>;

/// Runtime handles shared by all pipeline tasks.
pub struct PipelineCtx {
    pub sink: Arc<dyn Sink>,
    pub mapping: TableMapping,
    pub projections: crate::mapping::Projections,
    pub transforms: crate::mapping::Transforms,
    pub cfg: EngineConfig,
    /// Updated by the sink task after every successful flush.
    pub ack_tx: watch::Sender<Option<Lsn>>,
    pub metrics: Arc<crate::metrics::Metrics>,
}

/// Run the engine + sink + checkpoint side of the pipeline.
///
/// Returns a fatal error when the pipeline must halt (permanent document
/// rejection — correctness-first failure policy).
pub async fn run(
    mut events: mpsc::Receiver<ChangeEvent>,
    ctx: Arc<PipelineCtx>,
    stream: StreamId,
    render_position: PositionRenderer,
    durable: crate::mapping::DurableLsn,
    mut shutdown: watch::Receiver<bool>,
) -> Result<(), CoreError> {
    let (batch_tx, batch_rx) = mpsc::channel::<SinkCommand>(64);
    let (ckpt_done_tx, ckpt_done_rx) = watch::channel::<Option<Lsn>>(None);

    let sink_task = tokio::spawn(sink_loop(
        batch_rx,
        ctx.sink.clone(),
        ctx.ack_tx.clone(),
        ckpt_done_tx.clone(),
        ctx.metrics.clone(),
    ));

    // checkpoint loop: persist acked LSN periodically; only after a successful
    // persist does `durable` advance, which is what gates source acknowledgment
    let ckpt_sink = ctx.sink.clone();
    let ckpt_stream = stream;
    let ckpt_render = render_position;
    let ckpt_metrics = ctx.metrics.clone();
    let ckpt_interval = ctx.cfg.checkpoint_interval_ms.max(100);
    let ckpt_task = tokio::spawn(async move {
        let mut last_persisted: Option<Lsn> = None;
        let mut ckpt_done_rx = ckpt_done_rx;
        loop {
            // plain polling keeps every value cloned out of the guard before
            // any await point (watch guards are not Send across awaits)
            tokio::time::sleep(std::time::Duration::from_millis(ckpt_interval)).await;
            if ckpt_done_rx.has_changed().is_err() {
                break;
            }
            let new_lsn = *ckpt_done_rx.borrow_and_update();
            if last_persisted == new_lsn {
                continue;
            }
            let Some(lsn) = new_lsn else { continue };
            let checkpoint = Checkpoint {
                stream: ckpt_stream.clone(),
                token: lsn.0,
                position: ckpt_render(lsn.0),
            };
            match ckpt_sink.write_checkpoint(&checkpoint).await {
                Ok(()) => {
                    // `durable` gates what the source may acknowledge, so it
                    // must advance only after the checkpoint is persisted
                    durable.store(lsn);
                    ckpt_metrics.set_confirmed_position(lsn.0);
                    last_persisted = new_lsn;
                }
                Err(e) => {
                    tracing::warn!(target: "pg2osync::checkpoint", "checkpoint persist failed: {e}");
                }
            }
        }
    });

    let mut txn_buffer: Vec<LsnOp> = Vec::new();
    let mut txn_bytes: usize = 0;
    // an event pulled off the channel while gathering rows, to be handled on
    // the next turn rather than dropped
    let mut deferred: Option<ChangeEvent> = None;
    // the position the open transaction commits at, which is the document
    // version for every row it produces
    let mut version: Option<u64> = None;
    // when the current run of coalesced transactions started, so a busy stream
    // cannot postpone a flush indefinitely
    let mut coalescing_since: Option<std::time::Instant> = None;
    let mut break_err: Option<CoreError> = None;
    let txn_cap_bytes = ctx.cfg.txn_buffer_cap_mb.saturating_mul(1024 * 1024);
    let mut cap_warned = false;

    let result = loop {
        let ev = match deferred.take() {
            Some(ev) => ev,
            None => tokio::select! {
                ev = events.recv() => match ev {
                    Some(ev) => ev,
                    None => break Ok(()),
                },
                _ = shutdown.changed() => {
                    break Ok(());
                }
            },
        };
        tracing::trace!(target: "pg2osync::engine", "engine got event");
        match ev {
            ChangeEvent::Transaction(TransactionBoundary::Begin { lsn }) => {
                tracing::debug!(target: "pg2osync::engine", "BEGIN at {lsn}");
                // every document this transaction writes is versioned by the
                // position it becomes visible at, so a write carrying an older
                // position can never overwrite a newer one at the target
                version = Some(lsn.0);
            }
            ChangeEvent::Transaction(TransactionBoundary::Commit {
                lsn,
                commit_ts_micros,
            }) => {
                // end-to-end latency: PG commit wall clock -> now. PG's time
                // base is 2000-01-01; 946684800 = seconds to Unix epoch.
                if commit_ts_micros > 0 {
                    let committed_unix_ms = commit_ts_micros as u64 / 1000 + 946_684_800_000;
                    let now_ms = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_millis() as u64;
                    if now_ms > committed_unix_ms {
                        ctx.metrics.record_latency(now_ms - committed_unix_ms);
                    }
                }
                // backfill boundaries carry Lsn(0): their rows have no WAL
                // position, so they must NEVER advance ack/checkpoint state
                let backfill_boundary = lsn.0 == 0;
                if !backfill_boundary {
                    ctx.metrics.set_current_position(lsn.0);
                }
                if !txn_buffer.is_empty() {
                    if !backfill_boundary && let Some(last) = txn_buffer.last_mut() {
                        last.lsn = lsn;
                    }
                    // Hold the buffer when another event is already waiting:
                    // a commit is what forces a batch, so a stream of
                    // single-row transactions would otherwise cost one request
                    // per row. Only whole transactions accumulate — the ops of
                    // an open one are still in the buffer either way — and the
                    // batch's highest LSN stays the last commit in it, so an
                    // ack can never run past a transaction that was not fully
                    // written.
                    let waiting = match events.try_recv() {
                        Ok(next) => {
                            deferred = Some(next);
                            true
                        }
                        Err(_) => false,
                    };
                    let overdue = coalescing_since.is_some_and(|since: std::time::Instant| {
                        since.elapsed() >= COALESCE_WINDOW
                    });
                    if waiting && !overdue {
                        coalescing_since.get_or_insert_with(std::time::Instant::now);
                    } else {
                        txn_bytes = 0;
                        coalescing_since = None;
                        if batch_tx
                            .send(SinkCommand::Write(std::mem::take(&mut txn_buffer)))
                            .await
                            .is_err()
                        {
                            break Err(CoreError::Other("batch channel closed".into()));
                        }
                    }
                } else if !backfill_boundary
                    && batch_tx.send(SinkCommand::Position(lsn)).await.is_err()
                {
                    break Err(CoreError::Other("batch channel closed".into()));
                }
            }
            ChangeEvent::Row(row) => {
                // Take every row already waiting in the channel, not just this
                // one: rows that need a read of the target can then be read in
                // one request instead of one round-trip each. Nothing is waited
                // for, so this costs no latency when rows arrive alone.
                let mut rows = vec![row];
                while rows.len() < ctx.cfg.batch_size {
                    match events.try_recv() {
                        Ok(ChangeEvent::Row(next)) => rows.push(next),
                        Ok(other) => {
                            deferred = Some(other);
                            break;
                        }
                        Err(_) => break,
                    }
                }
                ctx.metrics.incr_event_by("row", rows.len() as u64);
                let completions =
                    match fetch_completions(&rows, &ctx.mapping, ctx.sink.as_ref(), &ctx.metrics)
                        .await
                    {
                        Ok(map) => map,
                        Err(e) => break Err(e),
                    };

                for row in &rows {
                    let index = ctx.mapping.index_for(&row.schema, &row.table);
                    let previous = completion_id(&row.kind)
                        .and_then(|id| completions.get(&(index.to_string(), id)))
                        .and_then(Option::as_ref);
                    let ops = match materialize(
                        index,
                        (&row.schema, &row.table),
                        &row.kind,
                        &ctx.projections,
                        &ctx.transforms,
                        previous,
                        version,
                    ) {
                        Ok(ops) => ops,
                        Err(e) => {
                            break_err = Some(e);
                            break;
                        }
                    };
                    txn_bytes += ops.iter().map(op_size).sum::<usize>();
                    txn_buffer.extend(ops);
                    if txn_bytes > txn_cap_bytes && !cap_warned {
                        cap_warned = true;
                        tracing::warn!(target: "pg2osync::engine",
                            "open transaction exceeds txn_buffer_cap_mb ({} MB); it will be \
                             split across sink requests",
                            ctx.cfg.txn_buffer_cap_mb);
                    }
                    if txn_buffer.len() >= ctx.cfg.batch_size
                        || txn_bytes >= ctx.cfg.batch_max_bytes
                    {
                        // oversized transaction split: safe because every op is
                        // idempotent and the commit LSN lands on the final piece
                        if batch_tx
                            .send(SinkCommand::Write(std::mem::take(&mut txn_buffer)))
                            .await
                            .is_err()
                        {
                            break_err = Some(CoreError::Other("batch channel closed".into()));
                            break;
                        }
                        txn_bytes = 0;
                    }
                }
                if let Some(e) = break_err.take() {
                    break Err(e);
                }
            }
            ChangeEvent::TableTruncated { schema, table } => {
                ctx.metrics.incr_event("truncate");
                let Some(index) = ctx.mapping.opt_index_for(&schema, &table) else {
                    continue;
                };
                let index = index.to_string();
                // rows buffered before the TRUNCATE belong before it
                if !txn_buffer.is_empty()
                    && batch_tx
                        .send(SinkCommand::Write(std::mem::take(&mut txn_buffer)))
                        .await
                        .is_err()
                {
                    break Err(CoreError::Other("batch channel closed".into()));
                }
                txn_bytes = 0;
                if batch_tx
                    .send(SinkCommand::Truncate(index, version))
                    .await
                    .is_err()
                {
                    break Err(CoreError::Other("batch channel closed".into()));
                }
            }
        }
    };

    drop(batch_tx);
    let _ = sink_task.await;
    ckpt_task.abort();
    result
}

#[allow(clippy::too_many_arguments)]
async fn sink_loop(
    mut commands: mpsc::Receiver<SinkCommand>,
    sink: Arc<dyn Sink>,
    ack_tx: watch::Sender<Option<Lsn>>,
    ckpt_done_tx: watch::Sender<Option<Lsn>>,
    metrics: Arc<crate::metrics::Metrics>,
) {
    while let Some(command) = commands.recv().await {
        let wrote = matches!(command, SinkCommand::Write(_));
        let result = match command {
            SinkCommand::Write(batch) => sink.write(batch).await.map(Some),
            SinkCommand::Position(lsn) => Ok(Some(SinkAck { max_lsn: lsn })),
            SinkCommand::Truncate(index, version) => {
                match sink.truncate_index(&index, version).await {
                    Ok(()) => {
                        tracing::info!(target: "pg2osync::sink",
                        "index {index} cleared after TRUNCATE");
                        Ok(None)
                    }
                    Err(e) => Err(e),
                }
            }
        };
        match result {
            Ok(Some(ack)) => {
                if wrote {
                    metrics.batches_flushed.fetch_add(1, Ordering::Relaxed);
                }
                // zero-position acks come from initial-load batches;
                // acknowledging them would corrupt the source position chain
                if ack.max_lsn.0 > 0 {
                    ack_tx.send_replace(Some(ack.max_lsn));
                    ckpt_done_tx.send_replace(Some(ack.max_lsn));
                }
            }
            Ok(None) => {}
            Err(CoreError::DocumentRejected {
                index,
                doc_id,
                reason,
            }) => {
                metrics.sink_errors_total.fetch_add(1, Ordering::Relaxed);
                tracing::error!(target: "pg2osync::sink",
                    "halting pipeline: permanent rejection {reason} for {index}/{doc_id}");
                return;
            }
            Err(e) => {
                metrics.sink_errors_total.fetch_add(1, Ordering::Relaxed);
                tracing::error!(target: "pg2osync::sink", "sink failed permanently: {e}");
                return;
            }
        }
    }
}

/// Rough serialized size of one operation, used only to cap batch size.
///
/// Re-serializing the document to measure it exactly would double the work for
/// no benefit: the limit exists to keep requests under the sink's own ceiling.
fn op_size(op: &LsnOp) -> usize {
    const OVERHEAD: usize = 64;
    match &op.op {
        DocumentOp::Upsert { index, id, doc, .. } => {
            index.len() + id.len() + estimate_json(doc) + OVERHEAD
        }
        DocumentOp::Delete { index, id, .. } => index.len() + id.len() + OVERHEAD,
    }
}

fn estimate_json(value: &Value) -> usize {
    match value {
        Value::Null => 4,
        Value::Bool(_) => 5,
        Value::Number(_) => 8,
        Value::String(s) => s.len() + 2,
        Value::Array(items) => 2 + items.iter().map(estimate_json).sum::<usize>(),
        Value::Object(map) => {
            2 + map
                .iter()
                .map(|(k, v)| k.len() + 4 + estimate_json(v))
                .sum::<usize>()
        }
    }
}

/// Read the stored documents a group of rows needs to complete unchanged
/// TOASTed columns, one request per index rather than one per row.
async fn fetch_completions(
    rows: &[pg2osync_core::event::RowChange],
    mapping: &crate::mapping::TableMapping,
    sink: &dyn Sink,
    metrics: &crate::metrics::Metrics,
) -> Result<HashMap<(String, String), Option<Value>>, CoreError> {
    let mut wanted: HashMap<String, Vec<String>> = HashMap::new();
    for row in rows {
        let Some(id) = completion_id(&row.kind) else {
            continue;
        };
        let index = mapping.index_for(&row.schema, &row.table).to_string();
        let ids = wanted.entry(index).or_default();
        // the same row updated twice in one group needs one read, not two
        if !ids.contains(&id) {
            ids.push(id);
        }
    }
    let mut out = HashMap::new();
    for (index, ids) in wanted {
        metrics
            .toast_readbacks_total
            .fetch_add(ids.len() as u64, std::sync::atomic::Ordering::Relaxed);
        let docs = sink.get_documents(&index, &ids).await?;
        for (id, doc) in ids.into_iter().zip(docs) {
            out.insert((index.clone(), id), doc);
        }
    }
    Ok(out)
}

/// How long a committed transaction may wait for company before its batch is
/// written anyway.
///
/// Short enough not to show up next to the end-to-end latency the pipeline
/// already reports (p50 1 ms), long enough that a thousand commits a second
/// arrive ten to a batch instead of one.
const COALESCE_WINDOW: std::time::Duration = std::time::Duration::from_millis(10);

/// The document id whose stored copy completes this change, if one is needed.
///
/// A row whose key changed is still filed under the old id, so completing from
/// the new one would write the unchanged columns as null.
fn completion_id(kind: &RowKind) -> Option<String> {
    let RowKind::Update {
        pk,
        previous_pk,
        unchanged_toast_columns,
        ..
    } = kind
    else {
        return None;
    };
    if unchanged_toast_columns.is_empty() {
        return None;
    }
    let id = pk_to_id(pk);
    Some(match previous_pk.as_ref().map(pk_to_id) {
        Some(previous) if previous != id => previous,
        _ => id,
    })
}

/// Convert one row change into document operations, completing unchanged-TOAST
/// columns from the previously indexed document when needed.
///
/// `previous` is the document already in the target, which the caller fetches
/// when `completion_id` asked for one. Doing it here would mean one round-trip
/// per row in the middle of the pipeline; measured on 20k updates to a table
/// with an 8 kB TOASTed column, that was the difference between 1,800 and
/// 4,400 rows per second.
fn materialize<'a>(
    index: &'a str,
    table: (&'a str, &'a str),
    kind: &RowKind,
    projections: &crate::mapping::Projections,
    transforms: &crate::mapping::Transforms,
    previous: Option<&Value>,
    version: Option<u64>,
) -> Result<Vec<LsnOp>, CoreError> {
    // PENDING_LSN is overwritten by the commit handler before any ack can
    // reference it: rows never leave the buffer without their commit attached.
    const PENDING_LSN: Lsn = Lsn(0);
    let mk = |op| {
        vec![LsnOp {
            lsn: PENDING_LSN,
            op,
        }]
    };
    match kind {
        RowKind::Insert { pk, doc } => {
            let mut doc = doc.clone();
            projections.apply(table.0, table.1, &mut doc);
            transforms.apply(table.0, table.1, &mut doc);
            Ok(mk(DocumentOp::Upsert {
                index: index.into(),
                id: pk_to_id(pk),
                doc,
                version,
            }))
        }
        RowKind::Update {
            pk,
            previous_pk,
            doc,
            unchanged_toast_columns,
        } => {
            let id = pk_to_id(pk);
            // A changed key means the row moved to a different document. The
            // old one still holds the previous version and has to be removed,
            // or nothing will ever collect it.
            let moved_from = previous_pk
                .as_ref()
                .map(pk_to_id)
                .filter(|previous| previous != &id);

            let mut doc = doc.clone();
            if !unchanged_toast_columns.is_empty()
                && let Some(Value::Object(prev_map)) = previous
                && let Value::Object(doc_map) = &mut doc
            {
                for col in unchanged_toast_columns {
                    if let Some(v) = prev_map.get(col) {
                        doc_map.insert(col.clone(), v.clone());
                    }
                }
            }
            projections.apply(table.0, table.1, &mut doc);
            transforms.apply(table.0, table.1, &mut doc);

            let mut ops = mk(DocumentOp::Upsert {
                index: index.into(),
                id,
                doc,
                version,
            });
            // write first, delete second: a crash between them leaves a
            // duplicate that the replay repairs, where the reverse order would
            // leave a gap that nothing repairs
            if let Some(previous) = moved_from {
                ops.push(LsnOp {
                    lsn: PENDING_LSN,
                    op: DocumentOp::Delete {
                        index: index.into(),
                        id: previous,
                        version,
                    },
                });
            }
            Ok(ops)
        }
        RowKind::Delete { pk } => Ok(mk(DocumentOp::Delete {
            index: index.into(),
            id: pk_to_id(pk),
            version,
        })),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn scalar_keys_render_as_plain_ids() {
        assert_eq!(pk_to_id(&json!(42)), "42");
        assert_eq!(pk_to_id(&json!("u-1")), "u-1");
        assert_eq!(pk_to_id(&json!(true)), "true");
        assert_eq!(pk_to_id(&Value::Null), "__null__");
    }

    #[test]
    fn composite_keys_are_order_independent() {
        let a = pk_to_id(&json!({"tenant": "acme", "id": 7}));
        let b = pk_to_id(&json!({"id": 7, "tenant": "acme"}));
        assert_eq!(a, b, "the same row must always yield the same id");
        assert_eq!(a, "id=7,tenant=acme");
    }

    #[test]
    fn string_key_parts_are_not_quoted() {
        // a quoted part would change the id of every existing document
        assert_eq!(pk_to_id(&json!({"a": "x", "b": "y"})), "a=x,b=y");
    }

    #[test]
    fn defaults_are_production_sane() {
        let cfg = EngineConfig::default();
        assert!(cfg.batch_size > 0);
        assert!(cfg.checkpoint_interval_ms > 0);
        assert!(cfg.retry_max > 0);
    }
}

#[cfg(test)]
mod pipeline_tests {
    use super::*;
    use pg2osync_core::checkpoint::{SOURCE_POSTGRES, StreamId};
    use pg2osync_core::event::{RowChange, RowKind};
    use pg2osync_core::sink::{Health, IndexSpec};
    use serde_json::json;
    use std::sync::Mutex;

    /// Records what the pipeline asks of a sink, so ordering and checkpoint
    /// behaviour can be asserted without a live cluster.
    #[derive(Default)]
    struct RecordingSink {
        events: Mutex<Vec<String>>,
        checkpoints: Mutex<Vec<Checkpoint>>,
    }

    impl RecordingSink {
        fn events(&self) -> Vec<String> {
            self.events.lock().expect("not poisoned").clone()
        }
        fn last_checkpoint(&self) -> Option<Checkpoint> {
            self.checkpoints
                .lock()
                .expect("not poisoned")
                .last()
                .cloned()
        }
    }

    #[async_trait::async_trait]
    impl Sink for RecordingSink {
        async fn ensure_ready(&self, _tables: &[IndexSpec]) -> Result<(), CoreError> {
            Ok(())
        }

        async fn get_documents(
            &self,
            _index: &str,
            ids: &[String],
        ) -> Result<Vec<Option<Value>>, CoreError> {
            self.events
                .lock()
                .expect("not poisoned")
                .push(format!("read({})", ids.join(",")));
            Ok(ids
                .iter()
                .map(|id| Some(json!({"id": id, "bio": "stored"})))
                .collect())
        }

        async fn write(&self, batch: Vec<LsnOp>) -> Result<SinkAck, CoreError> {
            let max_lsn = batch.last().expect("engine never sends empty batches").lsn;
            let rendered: Vec<String> = batch
                .iter()
                .map(|op| match &op.op {
                    DocumentOp::Upsert { id, version, .. } => match version {
                        Some(v) => format!("upsert:{id}@{v}"),
                        None => format!("upsert:{id}"),
                    },
                    DocumentOp::Delete { id, .. } => format!("delete:{id}"),
                })
                .collect();
            self.events
                .lock()
                .expect("not poisoned")
                .push(format!("write[{}]", rendered.join(" ")));
            Ok(SinkAck { max_lsn })
        }

        async fn refresh(&self, _indices: &[String]) -> Result<(), CoreError> {
            self.events
                .lock()
                .expect("not poisoned")
                .push("refresh".to_string());
            Ok(())
        }

        async fn truncate_index(
            &self,
            index: &str,
            _version: Option<u64>,
        ) -> Result<(), CoreError> {
            self.events
                .lock()
                .expect("not poisoned")
                .push(format!("truncate({index})"));
            Ok(())
        }

        async fn write_checkpoint(&self, checkpoint: &Checkpoint) -> Result<(), CoreError> {
            self.checkpoints
                .lock()
                .expect("not poisoned")
                .push(checkpoint.clone());
            Ok(())
        }

        async fn read_checkpoint(
            &self,
            _stream: &pg2osync_core::checkpoint::StreamId,
        ) -> Result<Option<Checkpoint>, CoreError> {
            Ok(self.last_checkpoint())
        }

        async fn health(&self) -> Result<Health, CoreError> {
            Ok(Health::Up)
        }
    }

    fn row(id: i64) -> ChangeEvent {
        ChangeEvent::Row(RowChange {
            schema: "public".into(),
            table: "users".into(),
            kind: RowKind::Insert {
                pk: json!(id),
                doc: json!({"id": id}),
            },
        })
    }

    fn begin(lsn: u64) -> ChangeEvent {
        ChangeEvent::Transaction(TransactionBoundary::Begin { lsn: Lsn(lsn) })
    }

    fn commit(lsn: u64) -> ChangeEvent {
        ChangeEvent::Transaction(TransactionBoundary::Commit {
            lsn: Lsn(lsn),
            commit_ts_micros: 0,
        })
    }

    /// Drives the engine over a fixed event script and returns the sink.
    async fn run_script(batch_size: usize, script: Vec<ChangeEvent>) -> Arc<RecordingSink> {
        let sink = Arc::new(RecordingSink::default());
        let (events_tx, events_rx) = mpsc::channel(1024);
        let (ack_tx, _ack_rx) = watch::channel(None);
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let ctx = Arc::new(PipelineCtx {
            sink: sink.clone(),
            mapping: TableMapping::from_pairs([(
                ("public".to_string(), "users".to_string()),
                "users".to_string(),
            )]),
            projections: crate::mapping::Projections::default(),
            transforms: crate::mapping::Transforms::default(),
            cfg: EngineConfig {
                batch_size,
                // a short interval keeps the test fast; the loop only persists
                // when the acknowledged position actually moved
                checkpoint_interval_ms: 100,
                ..EngineConfig::default()
            },
            ack_tx,
            metrics: Arc::new(crate::metrics::Metrics::default()),
        });
        let stream = StreamId {
            source: SOURCE_POSTGRES.into(),
            stream: "slot".into(),
            publication: "pub".into(),
        };
        let render: PositionRenderer = Arc::new(|token| Lsn(token).to_string());
        let engine = tokio::spawn(run(
            events_rx,
            ctx,
            stream,
            render,
            crate::mapping::DurableLsn::default(),
            shutdown_rx,
        ));
        for event in script {
            events_tx.send(event).await.expect("engine alive");
        }
        // let the checkpoint loop observe the final acknowledged position
        tokio::time::sleep(std::time::Duration::from_millis(400)).await;
        drop(events_tx);
        engine.await.expect("task joined").expect("engine ran");
        sink
    }

    #[tokio::test]
    async fn transaction_flushes_at_its_commit() {
        let sink = run_script(500, vec![row(1), row(2), commit(0x100)]).await;
        assert_eq!(sink.events(), vec!["write[upsert:1 upsert:2]"]);
        let ckpt = sink.last_checkpoint().expect("checkpoint persisted");
        assert_eq!(ckpt.token, 0x100);
        assert_eq!(ckpt.position, "0/100", "position is rendered by the source");
    }

    #[tokio::test]
    async fn a_transaction_versions_its_documents_by_the_position_it_commits_at() {
        // pgoutput reports the commit position at BEGIN, so the version is known
        // before the rows arrive. It is what stops a stale write from
        // overwriting a newer document at the target.
        let sink = run_script(10, vec![begin(0x200), row(1), row(2), commit(0x200)]).await;
        assert_eq!(sink.events(), vec!["write[upsert:1@512 upsert:2@512]"]);
    }

    #[tokio::test]
    async fn coalescing_transactions_into_one_batch_does_not_blur_their_versions() {
        let sink = run_script(
            10,
            vec![
                begin(0x100),
                row(1),
                commit(0x100),
                begin(0x300),
                row(2),
                commit(0x300),
            ],
        )
        .await;
        // whole transactions share a batch, and each row still carries the
        // position of the transaction it belongs to
        assert_eq!(sink.events(), vec!["write[upsert:1@256 upsert:2@768]"]);
    }

    #[tokio::test]
    async fn rows_with_no_transaction_of_their_own_go_unversioned() {
        // the initial load sends rows without a BEGIN, because it has no
        // position to offer yet
        let sink = run_script(10, vec![row(1), commit(0)]).await;
        assert_eq!(sink.events(), vec!["write[upsert:1]"]);
    }

    #[tokio::test]
    async fn commit_with_an_exhausted_buffer_still_checkpoints() {
        // batch_size 2 with exactly 2 rows leaves the buffer empty at COMMIT.
        // Without a position marker through the sink channel the checkpoint
        // would stall here, and PostgreSQL would retain WAL forever.
        let sink = run_script(2, vec![row(1), row(2), commit(0x200)]).await;
        assert_eq!(sink.events(), vec!["write[upsert:1 upsert:2]"]);
        assert_eq!(
            sink.last_checkpoint().map(|c| c.token),
            Some(0x200),
            "the commit position must be checkpointed even with nothing buffered"
        );
    }

    #[tokio::test]
    async fn empty_transaction_advances_the_position() {
        let sink = run_script(500, vec![commit(0x300)]).await;
        assert!(sink.events().is_empty(), "nothing to write");
        assert_eq!(sink.last_checkpoint().map(|c| c.token), Some(0x300));
    }

    #[tokio::test]
    async fn truncate_lands_after_the_writes_it_follows() {
        let sink = run_script(
            500,
            vec![
                row(1),
                commit(0x400),
                ChangeEvent::TableTruncated {
                    schema: "public".into(),
                    table: "users".into(),
                },
                row(2),
                commit(0x500),
            ],
        )
        .await;
        assert_eq!(
            sink.events(),
            vec!["write[upsert:1]", "truncate(users)", "write[upsert:2]"],
            "a write queued before the truncate must not survive it"
        );
    }

    #[tokio::test]
    async fn initial_load_boundaries_never_advance_the_checkpoint() {
        // Lsn(0) marks rows that have no source position of their own
        let sink = run_script(500, vec![row(1), commit(0)]).await;
        assert_eq!(sink.events(), vec!["write[upsert:1]"]);
        assert!(
            sink.last_checkpoint().is_none(),
            "a positionless batch must never be checkpointed"
        );
    }

    #[tokio::test]
    async fn oversized_transaction_is_split_but_keeps_its_commit_position() {
        let mut script: Vec<ChangeEvent> = (1..=5).map(row).collect();
        script.push(commit(0x600));
        let sink = run_script(2, script).await;
        assert_eq!(
            sink.events(),
            vec![
                "write[upsert:1 upsert:2]",
                "write[upsert:3 upsert:4]",
                "write[upsert:5]"
            ],
            "splitting is expected; every op is idempotent"
        );
        assert_eq!(sink.last_checkpoint().map(|c| c.token), Some(0x600));
    }

    fn moved(from: i64, to: i64, toast: &[&str]) -> ChangeEvent {
        ChangeEvent::Row(RowChange {
            schema: "public".into(),
            table: "users".into(),
            kind: RowKind::Update {
                pk: json!(to),
                previous_pk: Some(json!(from)),
                doc: json!({"id": to}),
                unchanged_toast_columns: toast.iter().map(|c| c.to_string()).collect(),
            },
        })
    }

    #[tokio::test]
    async fn a_moved_row_is_written_at_its_new_id_and_removed_from_the_old_one() {
        let sink = run_script(500, vec![moved(1, 2, &[]), commit(0x700)]).await;
        assert_eq!(
            sink.events(),
            vec!["write[upsert:2 delete:1]"],
            "write first, delete second: a crash between them leaves a duplicate, \
             not a gap"
        );
    }

    #[tokio::test]
    async fn an_update_that_keeps_its_key_deletes_nothing() {
        let unchanged = ChangeEvent::Row(RowChange {
            schema: "public".into(),
            table: "users".into(),
            kind: RowKind::Update {
                pk: json!(7),
                previous_pk: Some(json!(7)),
                doc: json!({"id": 7}),
                unchanged_toast_columns: vec![],
            },
        });
        let sink = run_script(500, vec![unchanged, commit(0x800)]).await;
        assert_eq!(sink.events(), vec!["write[upsert:7]"]);
    }

    #[tokio::test]
    async fn a_moved_row_completes_toast_from_the_document_it_left_behind() {
        // the stored document is still under the old id; reading the new one
        // would find nothing and write the unchanged column as null
        let sink = run_script(500, vec![moved(1, 2, &["bio"]), commit(0x900)]).await;
        assert_eq!(
            sink.events(),
            vec!["read(1)", "write[upsert:2 delete:1]"],
            "completion must read where the document actually is"
        );
    }

    #[tokio::test]
    async fn unmapped_tables_are_ignored_for_truncate() {
        let sink = run_script(
            500,
            vec![ChangeEvent::TableTruncated {
                schema: "public".into(),
                table: "not_synced".into(),
            }],
        )
        .await;
        assert!(sink.events().is_empty());
    }
}
