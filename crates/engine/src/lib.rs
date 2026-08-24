//! The multi-task pipeline (ADR #17):
//!
//! source task ──events──► engine task ──batches──► sink task
//!                              │                     │
//!                              └── checkpoint ◄── acks (watch)
//!
//! Rows buffer until their COMMIT boundary (never partially indexed), are
//! flushed in bounded batches, and acknowledged upstream only after the
//! checkpoint is durable.

pub mod mapping;
pub mod metrics;

use crate::mapping::TableMapping;
use pg2osync_core::error::CoreError;
use pg2osync_core::event::{ChangeEvent, RowKind, TransactionBoundary};
use pg2osync_core::lsn::Lsn;
use pg2osync_core::sink::{DocumentOp, LsnOp, Sink};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use tokio::sync::{mpsc, watch};

/// `[engine]` config section.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct EngineConfig {
    pub batch_size: usize,
    pub batch_max_bytes: usize,
    pub flush_interval_ms: u64,
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
            flush_interval_ms: 1000,
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

/// Runtime handles shared by all pipeline tasks.
pub struct PipelineCtx {
    pub sink: Arc<dyn Sink>,
    pub mapping: TableMapping,
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
    slot_name: String,
    publication: String,
    durable: crate::mapping::DurableLsn,
    mut shutdown: watch::Receiver<bool>,
) -> Result<(), CoreError> {
    let (batch_tx, batch_rx) = mpsc::channel::<Vec<LsnOp>>(64);
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
    let ckpt_slot = slot_name.clone();
    let ckpt_pub = publication;
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
            if last_persisted == new_lsn || new_lsn.is_none() {
                continue;
            }
            match ckpt_sink
                .write_checkpoint(&ckpt_slot, &ckpt_pub, new_lsn.expect("checked"))
                .await
            {
                Ok(()) => {
                    durable.store(new_lsn.expect("checked"));
                    last_persisted = new_lsn;
                }
                Err(e) => {
                    tracing::warn!(target: "pg2osync::checkpoint", "checkpoint persist failed: {e}");
                }
            }
        }
    });

    let mut txn_buffer: Vec<LsnOp> = Vec::new();

    let result = loop {
        let ev = tokio::select! {
            ev = events.recv() => match ev {
                Some(ev) => ev,
                None => break Ok(()),
            },
            _ = shutdown.changed() => {
                break Ok(());
            }
        };
        tracing::trace!(target: "pg2osync::engine", "engine got event");
        match ev {
            ChangeEvent::Transaction(TransactionBoundary::Begin) => {
                tracing::debug!(target: "pg2osync::engine", "BEGIN");
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
                if !txn_buffer.is_empty() {
                    if !backfill_boundary && let Some(last) = txn_buffer.last_mut() {
                        last.lsn = lsn;
                    }
                    if batch_tx
                        .send(std::mem::take(&mut txn_buffer))
                        .await
                        .is_err()
                    {
                        break Err(CoreError::Other("batch channel closed".into()));
                    }
                } else if !backfill_boundary {
                    // empty transaction still advances the safe position
                    ctx.ack_tx.send_replace(Some(lsn));
                }
            }
            ChangeEvent::Row(row) => {
                ctx.metrics.incr_event("row");
                tracing::debug!(target: "pg2osync::engine", "ROW {}.{}", row.schema, row.table);
                let index = ctx.mapping.index_for(&row.schema, &row.table);
                let ops = match materialize(
                    index,
                    (&row.schema, &row.table),
                    &row.kind,
                    &ctx.transforms,
                    ctx.sink.as_ref(),
                )
                .await
                {
                    Ok(ops) => ops,
                    Err(e) => break Err(e),
                };
                txn_buffer.extend(ops);
                if txn_buffer.len() >= ctx.cfg.batch_size {
                    // oversized transaction split: safe because every op is
                    // idempotent and the commit LSN lands on the final piece
                    if batch_tx
                        .send(std::mem::take(&mut txn_buffer))
                        .await
                        .is_err()
                    {
                        break Err(CoreError::Other("batch channel closed".into()));
                    }
                }
            }
            ChangeEvent::TableTruncated { schema, table } => {
                if let Some(index) = ctx.mapping.opt_index_for(&schema, &table) {
                    if let Err(e) = ctx.sink.truncate_index(index).await {
                        break Err(e);
                    }
                    tracing::info!(target: "pg2osync::engine", "index {index} cleared after TRUNCATE");
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
    mut batches: mpsc::Receiver<Vec<LsnOp>>,
    sink: Arc<dyn Sink>,
    ack_tx: watch::Sender<Option<Lsn>>,
    ckpt_done_tx: watch::Sender<Option<Lsn>>,
    metrics: Arc<crate::metrics::Metrics>,
) {
    while let Some(batch) = batches.recv().await {
        match sink.write(batch).await {
            Ok(ack) => {
                metrics.batches_flushed.fetch_add(1, Ordering::Relaxed);
                // zero-LSN acks come from backfill batches; acknowledging them
                // would corrupt the WAL position chain
                if ack.max_lsn.0 > 0 {
                    ack_tx.send_replace(Some(ack.max_lsn));
                    ckpt_done_tx.send_replace(Some(ack.max_lsn));
                }
            }
            Err(CoreError::DocumentRejected {
                index,
                doc_id,
                reason,
            }) => {
                tracing::error!(target: "pg2osync::sink",
                    "halting pipeline: permanent rejection {reason} for {index}/{doc_id}");
                return;
            }
            Err(e) => {
                tracing::error!(target: "pg2osync::sink", "sink failed permanently: {e}");
                return;
            }
        }
    }
}

/// Convert one row change into document operations, completing unchanged-TOAST
/// columns from the previously indexed document when needed (ADR #19a).
async fn materialize<'a>(
    index: &'a str,
    table: (&'a str, &'a str),
    kind: &RowKind,
    transforms: &crate::mapping::Transforms,
    sink: &dyn Sink,
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
            transforms.apply(table.0, table.1, &mut doc);
            Ok(mk(DocumentOp::Upsert {
                index: index.into(),
                id: pk_to_id(pk),
                doc,
            }))
        }
        RowKind::Update {
            pk,
            doc,
            unchanged_toast_columns,
        } => {
            let id = pk_to_id(pk);
            let mut doc = doc.clone();
            if !unchanged_toast_columns.is_empty() {
                let prev = sink
                    .get_documents(index, std::slice::from_ref(&id))
                    .await?
                    .into_iter()
                    .flatten()
                    .next();
                if let Some(Value::Object(prev_map)) = prev
                    && let Value::Object(doc_map) = &mut doc
                {
                    for col in unchanged_toast_columns {
                        if let Some(v) = prev_map.get(col) {
                            doc_map.insert(col.clone(), v.clone());
                        }
                    }
                }
            }
            Ok(mk(DocumentOp::Upsert {
                index: index.into(),
                id,
                doc,
            }))
        }
        RowKind::Delete { pk } => Ok(mk(DocumentOp::Delete {
            index: index.into(),
            id: pk_to_id(pk),
        })),
    }
}
