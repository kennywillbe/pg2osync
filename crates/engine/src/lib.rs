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
    /// How many write requests may be open against the target at once.
    ///
    /// One request at a time is what the initial load is actually limited by:
    /// measured on a laptop stack, a single open request tops out around 52,000
    /// documents a second whatever its size, while four reach 114,000. Raising
    /// this multiplies the load placed on the target, so it stays at one until
    /// an operator measures their own.
    pub write_concurrency: usize,
    /// Warning threshold for a single open transaction. Exceeding it means the
    /// transaction is split, so the sink briefly holds a partial transaction.
    pub txn_buffer_cap_mb: usize,
    pub retry_max: u32,
    pub retry_backoff_ms: u64,
    pub checkpoint_interval_ms: u64,
    /// What to do about a document the target will never accept.
    #[serde(default)]
    pub on_permanent_rejection: RejectionPolicy,
    /// How many documents may be quarantined before the pipeline halts anyway.
    ///
    /// One malformed row should not stop replication; a mapping that refuses a
    /// whole table should. Counted against what the target actually holds, so a
    /// restart does not hand the budget back.
    #[serde(default = "default_max_rejects")]
    pub max_rejects: u64,
}

/// What to do when the target permanently refuses a document.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RejectionPolicy {
    /// Stop. Nothing is skipped and nothing is lost, at the cost of replication
    /// for every table until someone fixes the mapping.
    #[default]
    Halt,
    /// Record the document with its position and carry on. Trades the
    /// no-partial-transactions guarantee, per document, for availability.
    Quarantine,
}

fn default_max_rejects() -> u64 {
    100
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            batch_size: 500,
            batch_max_bytes: 10 * 1024 * 1024,
            write_concurrency: 1,
            txn_buffer_cap_mb: 256,
            retry_max: 10,
            retry_backoff_ms: 500,
            checkpoint_interval_ms: 500,
            on_permanent_rejection: RejectionPolicy::Halt,
            max_rejects: default_max_rejects(),
        }
    }
}

pub use crate::mapping::pk_to_id;

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
    /// Everything before this mark has been handed to the sink; the sink task
    /// publishes it once written, which is what the initial load waits on.
    LoadMark(u64),
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
    /// Target field names; applied after everything else has shaped the document.
    pub renames: crate::mapping::Renames,
    /// Configured document ids; a table with no entry keeps `pk_to_id`.
    pub id_templates: crate::mapping::IdTemplates,
    /// Tables whose rows fan out into one document per array element.
    pub fan_outs: crate::mapping::FanOuts,
    pub cfg: EngineConfig,
    /// Updated by the sink task after every successful flush.
    pub ack_tx: watch::Sender<Option<Lsn>>,
    /// Highest initial-load mark whose rows are durably written. The load
    /// records its progress behind this and nothing else.
    pub load_done_tx: watch::Sender<u64>,
    pub metrics: Arc<crate::metrics::Metrics>,
}

/// Run the engine + sink + checkpoint side of the pipeline.
///
/// Returns a fatal error when the pipeline must halt (permanent document
/// rejection — correctness-first failure policy).
pub async fn run(
    mut events: mpsc::Receiver<ChangeEvent>,
    mut copy: mpsc::Receiver<ChangeEvent>,
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
        ctx.load_done_tx.clone(),
        ctx.metrics.clone(),
        ctx.cfg.on_permanent_rejection,
        ctx.cfg.max_rejects,
        ctx.cfg.write_concurrency,
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
    // when the current run of coalesced transactions started, so a busy stream
    // cannot postpone a flush indefinitely
    let mut coalescing_since: Option<std::time::Instant> = None;
    let mut break_err: Option<CoreError> = None;
    // A transaction is open from its BEGIN until its COMMIT. While one is, no
    // other producer's boundary may flush the buffer, or the transaction would
    // reach the target in two pieces.
    let mut stream_txn_open = false;
    // A load mark that arrived mid-transaction, released once the transaction
    // it interrupted has been handed over.
    let mut pending_mark: Option<u64> = None;
    let txn_cap_bytes = ctx.cfg.txn_buffer_cap_mb.saturating_mul(1024 * 1024);
    let mut cap_warned = false;
    // A closed channel yields None immediately, which would spin the select; a
    // flag disables its branch instead. The loop ends when both are closed.
    let mut events_open = true;
    let mut copy_open = true;
    // What the stream has removed while a copy row for the same key may still be
    // in flight behind it. A versioned delete leaves a tombstone rather than
    // nothing, and the target keeps that tombstone only for `index.gc_deletes`
    // (60s by default); once it is gone `external_gte` accepts *any* version,
    // including one below the delete's. So a copy row starved behind a busy
    // stream for longer than that would resurrect the document. These say no
    // without asking the target to make a comparison it cannot make.
    let mut superseded: HashMap<(String, String), u64> = HashMap::new();
    let mut cleared: HashMap<String, u64> = HashMap::new();

    let result = loop {
        let ev = match deferred.take().or_else(|| try_next(&mut events, &mut copy)) {
            Some(ev) => ev,
            None => {
                let mut got = None;
                while got.is_none() {
                    tokio::select! {
                        biased;
                        // The stream comes first, always. A copy chunk can wait
                        // a moment; the source retains WAL until the stream is
                        // consumed, so the stream cannot.
                        ev = events.recv(), if events_open => match ev {
                            Some(ev) => got = Some(ev),
                            None => {
                                events_open = false;
                                if !copy_open {
                                    break;
                                }
                            }
                        },
                        ev = copy.recv(), if copy_open => match ev {
                            Some(ev) => got = Some(ev),
                            None => {
                                // the load is over, so there is nothing left for
                                // the window to protect
                                copy_open = false;
                                superseded.clear();
                                cleared.clear();
                                if !events_open {
                                    break;
                                }
                            }
                        },
                        _ = shutdown.changed() => break,
                    }
                }
                match got {
                    Some(ev) => ev,
                    None => break Ok(()),
                }
            }
        };
        tracing::trace!(target: "pg2osync::engine", "engine got event");
        match ev {
            ChangeEvent::Transaction(TransactionBoundary::Begin { lsn }) => {
                tracing::debug!(target: "pg2osync::engine", "BEGIN at {lsn}");
                stream_txn_open = true;
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
                    stream_txn_open = false;
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
                    let waiting = match try_next(&mut events, &mut copy) {
                        Some(next) => {
                            deferred = Some(next);
                            true
                        }
                        None => false,
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
                // An empty buffer here means everything before the held mark is
                // on its way to the sink, which is the condition it waits for.
                if txn_buffer.is_empty()
                    && !stream_txn_open
                    && let Some(mark) = pending_mark.take()
                    && batch_tx.send(SinkCommand::LoadMark(mark)).await.is_err()
                {
                    break Err(CoreError::Other("batch channel closed".into()));
                }
            }
            ChangeEvent::LoadMark(mark) => {
                // The window closes here. The load sends a chunk's rows, then
                // this mark, then *waits* for the sink to report it before
                // reading the next chunk — so by the time the mark arrives every
                // row of that chunk has been materialised and none of the next
                // one can exist yet. Nothing is in flight for a remembered
                // delete to outrank, which is what keeps this bounded by one
                // chunk rather than by the whole load. A loader that sent the
                // next chunk before its mark was confirmed would silently
                // reopen the window.
                superseded.clear();
                cleared.clear();
                if stream_txn_open {
                    pending_mark = Some(mark);
                    continue;
                }
                if !txn_buffer.is_empty() {
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
                if batch_tx.send(SinkCommand::LoadMark(mark)).await.is_err() {
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
                    match try_next(&mut events, &mut copy) {
                        Some(ChangeEvent::Row(next)) => rows.push(next),
                        Some(other) => {
                            deferred = Some(other);
                            break;
                        }
                        None => break,
                    }
                }
                ctx.metrics.incr_event_by("row", rows.len() as u64);
                let rules = Rules {
                    projections: &ctx.projections,
                    transforms: &ctx.transforms,
                    renames: &ctx.renames,
                    id_templates: &ctx.id_templates,
                    fan_outs: &ctx.fan_outs,
                };
                let completions = match fetch_completions(
                    &rows,
                    &ctx.mapping,
                    ctx.sink.as_ref(),
                    &ctx.metrics,
                    &rules,
                )
                .await
                {
                    Ok(map) => map,
                    Err(e) => break Err(e),
                };

                for row in &rows {
                    // Defence in depth rather than a second filter: the source
                    // drops what it cannot map, and a panic in a worker thread
                    // is the worst way to find out it missed something — it
                    // takes the process down and every reconnect repeats it.
                    let Some(index) = ctx.mapping.opt_index_for(&row.schema, &row.table) else {
                        tracing::error!(target: "pg2osync::engine",
                            "no index is configured for {}.{}; its rows are being dropped",
                            row.schema, row.table);
                        continue;
                    };
                    let previous = completion_id(&row.kind, &rules, (&row.schema, &row.table))
                        .and_then(|id| completions.get(&(index.to_string(), id)))
                        .and_then(Option::as_ref);
                    let ops = match materialize(
                        index,
                        (&row.schema, &row.table),
                        &row.kind,
                        &rules,
                        previous,
                        row.version,
                    ) {
                        Ok(ops) => ops,
                        Err(e) => {
                            break_err = Some(e);
                            break;
                        }
                    };
                    // A write the stream has already superseded is dropped
                    // here rather than sent for the target to refuse: by the
                    // time it lands the tombstone that would refuse it may be
                    // gone. Strictly greater, because on PostgreSQL a delete and
                    // a re-insert of one key inside one transaction share the
                    // commit position, and the re-insert has to survive.
                    //
                    // Applied to every row, not only copied ones, because it
                    // cannot fire on a streamed row: within a stream a later
                    // event always carries a higher position.
                    let ops: Vec<LsnOp> = ops
                        .into_iter()
                        .filter(|op| match &op.op {
                            DocumentOp::Upsert {
                                index,
                                id,
                                version: Some(version),
                                ..
                            } => {
                                // the empty check comes first so an ordinary row
                                // never pays for building a lookup key
                                let dead = (!superseded.is_empty()
                                    && superseded
                                        .get(&(index.clone(), id.clone()))
                                        .is_some_and(|removed| removed > version))
                                    || cleared.get(index.as_str()).is_some_and(|at| at > version);
                                if dead {
                                    ctx.metrics.incr_event("superseded");
                                    tracing::debug!(target: "pg2osync::engine",
                                    "dropping {index}/{id} at {version}: the stream \
                                     removed it at a later position");
                                }
                                !dead
                            }
                            _ => true,
                        })
                        .collect();
                    // Remembered only while a load is running: with no copy
                    // row to outrank there is nothing for this to protect.
                    if copy_open {
                        for op in &ops {
                            if let DocumentOp::Delete {
                                index,
                                id,
                                version: Some(version),
                            } = &op.op
                            {
                                let entry = superseded
                                    .entry((index.clone(), id.clone()))
                                    .or_insert(*version);
                                *entry = (*entry).max(*version);
                            }
                        }
                    }
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
            ChangeEvent::TableTruncated {
                schema,
                table,
                version,
            } => {
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
                // A truncate removes every key at once, so one floor per index
                // says what a per-key entry would: anything copied from before
                // it is gone. Its tombstones expire like any other delete's.
                if copy_open && let Some(version) = version {
                    let at = cleared.entry(index.clone()).or_insert(version);
                    *at = (*at).max(version);
                }
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

/// Whatever is already waiting, stream before copy.
///
/// Used everywhere the engine gathers more work without waiting for it, so a
/// batch fills from both producers instead of one row at a time from the copy.
fn try_next(
    events: &mut mpsc::Receiver<ChangeEvent>,
    copy: &mut mpsc::Receiver<ChangeEvent>,
) -> Option<ChangeEvent> {
    events.try_recv().or_else(|_| copy.try_recv()).ok()
}

#[allow(clippy::too_many_arguments)]
async fn sink_loop(
    mut commands: mpsc::Receiver<SinkCommand>,
    sink: Arc<dyn Sink>,
    ack_tx: watch::Sender<Option<Lsn>>,
    ckpt_done_tx: watch::Sender<Option<Lsn>>,
    load_done_tx: watch::Sender<u64>,
    metrics: Arc<crate::metrics::Metrics>,
    policy: RejectionPolicy,
    max_rejects: u64,
    write_concurrency: usize,
) {
    // What the target already holds, so the budget survives a restart instead of
    // being handed back every time the pipeline comes up.
    let quarantined = match policy {
        RejectionPolicy::Quarantine => match sink.list_rejects(0).await {
            Ok((_, total)) => total,
            Err(e) => {
                tracing::error!(target: "pg2osync::sink",
                    "cannot read the quarantine store, so its limit cannot be honoured: {e}");
                return;
            }
        },
        RejectionPolicy::Halt => 0,
    };
    if quarantined > 0 {
        tracing::warn!(target: "pg2osync::sink",
            "{quarantined} document(s) already quarantined of a limit of {max_rejects}; \
             `pg2osync rejects` lists them");
    }
    let mut acks = Acks {
        sink: sink.as_ref(),
        ack_tx: &ack_tx,
        ckpt_done_tx: &ckpt_done_tx,
        metrics: metrics.as_ref(),
        policy,
        max_rejects,
        quarantined,
    };
    // Write requests are the only command that may be open concurrently, and
    // they are still completed in submission order: a position is acknowledged
    // only once every batch before it is durable, and nothing after a failure
    // is acknowledged at all. Everything else is a barrier, so the order a
    // truncate or a load mark has against the writes around it is unchanged.
    let concurrency = write_concurrency.max(1);
    let mut inflight: std::collections::VecDeque<tokio::task::JoinHandle<_>> =
        std::collections::VecDeque::new();
    let mut halted = false;
    loop {
        // Nothing more can start while the window is full, so waiting on the
        // oldest write is the only thing left to do.
        let full = inflight.len() >= concurrency;
        let command = if full || inflight.is_empty() {
            if full {
                if !take_one(&mut inflight, &mut acks).await {
                    halted = true;
                    break;
                }
                continue;
            }
            match commands.recv().await {
                Some(command) => command,
                None => break,
            }
        } else {
            tokio::select! {
                biased;
                // A finished write first: it frees a slot and releases the
                // position it carries without waiting for whatever the engine
                // sends next. Taking the outcome here rather than dropping the
                // branch and reading the handle again is what makes this sound:
                // a handle that has already yielded its result panics if it is
                // awaited a second time, and because this branch is biased
                // first it can only be dropped while it is *not* ready, which
                // leaves the handle in the queue untouched.
                joined = async { inflight.front_mut().expect("not empty").await } => {
                    inflight.pop_front();
                    if !finish(joined, &mut acks).await {
                        halted = true;
                        break;
                    }
                    continue;
                }
                command = commands.recv() => match command {
                    Some(command) => command,
                    None => break,
                },
            }
        };
        let result = match command {
            SinkCommand::Write(batch) => {
                let sink = sink.clone();
                // Spawned rather than merely awaited concurrently, so building
                // one request's body does not hold up sending another: past the
                // target's own limit that serialization is the next cost.
                inflight.push_back(tokio::spawn(
                    async move { sink.write(batch).await.map(Some) },
                ));
                continue;
            }
            SinkCommand::Position(lsn) => {
                if !drain(&mut inflight, &mut acks).await {
                    halted = true;
                    break;
                }
                Ok(Some(SinkAck::written(lsn)))
            }
            SinkCommand::LoadMark(mark) => {
                // reached only after every write queued ahead of it succeeded,
                // which is exactly what the load needs to know
                if !drain(&mut inflight, &mut acks).await {
                    halted = true;
                    break;
                }
                load_done_tx.send_replace(mark);
                Ok(None)
            }
            SinkCommand::Truncate(index, version) => {
                if !drain(&mut inflight, &mut acks).await {
                    halted = true;
                    break;
                }
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
        if !acks.apply(result, false).await {
            halted = true;
            break;
        }
    }
    if halted {
        // The requests still open were already sent, so nothing is lost by
        // dropping them — but leaving them running would let a write land after
        // the pipeline decided to stop.
        for handle in &inflight {
            handle.abort();
        }
        return;
    }
    // The engine is done sending, so what is still open is the last of the
    // work: dropping it here would discard writes that have already happened.
    drain(&mut inflight, &mut acks).await;
}

/// Requests open against the target, oldest first.
type Inflight =
    std::collections::VecDeque<tokio::task::JoinHandle<Result<Option<SinkAck>, CoreError>>>;

/// Wait for the oldest open write and account for it.
///
/// Returns whether the pipeline may carry on.
async fn take_one(inflight: &mut Inflight, acks: &mut Acks<'_>) -> bool {
    let Some(handle) = inflight.pop_front() else {
        return true;
    };
    finish(handle.await, acks).await
}

/// Account for one write that has finished, however it finished.
///
/// Returns whether the pipeline may carry on.
async fn finish(
    joined: Result<Result<Option<SinkAck>, CoreError>, tokio::task::JoinError>,
    acks: &mut Acks<'_>,
) -> bool {
    match joined {
        Ok(result) => acks.apply(result, true).await,
        Err(e) => {
            acks.metrics
                .sink_errors_total
                .fetch_add(1, Ordering::Relaxed);
            tracing::error!(target: "pg2osync::sink", "the sink task did not finish: {e}");
            false
        }
    }
}

/// Wait for every open write, in the order they were sent.
async fn drain(inflight: &mut Inflight, acks: &mut Acks<'_>) -> bool {
    while !inflight.is_empty() {
        if !take_one(inflight, acks).await {
            return false;
        }
    }
    true
}

/// What one finished sink command means for the position, the quarantine budget
/// and whether the pipeline carries on.
///
/// Held together rather than passed apart because the order of its steps is the
/// contract: a refused document is filed before the position covering it is
/// acknowledged, and a failure acknowledges nothing.
struct Acks<'a> {
    sink: &'a dyn Sink,
    ack_tx: &'a watch::Sender<Option<Lsn>>,
    ckpt_done_tx: &'a watch::Sender<Option<Lsn>>,
    metrics: &'a crate::metrics::Metrics,
    policy: RejectionPolicy,
    max_rejects: u64,
    quarantined: u64,
}

impl Acks<'_> {
    /// Returns whether the pipeline may carry on.
    async fn apply(&mut self, result: Result<Option<SinkAck>, CoreError>, wrote: bool) -> bool {
        let (metrics, sink, policy, max_rejects) =
            (self.metrics, self.sink, self.policy, self.max_rejects);
        match result {
            Ok(Some(ack)) => {
                if wrote {
                    metrics.batches_flushed.fetch_add(1, Ordering::Relaxed);
                }
                if !ack.rejected.is_empty() {
                    let first = &ack.rejected[0];
                    if policy == RejectionPolicy::Halt {
                        metrics.sink_errors_total.fetch_add(1, Ordering::Relaxed);
                        tracing::error!(target: "pg2osync::sink",
                            "halting pipeline: permanent rejection {} for {}/{}. Set \
                             on_permanent_rejection = \"quarantine\" to record it and carry on",
                            first.reason, first.index, first.doc_id);
                        return false;
                    }
                    // Already spent: halt without recording more. The batch is
                    // not acknowledged either, so these documents are replayed
                    // once the mapping is fixed or the limit raised — nothing is
                    // lost by declining to file them now.
                    if self.quarantined >= max_rejects {
                        metrics.sink_errors_total.fetch_add(1, Ordering::Relaxed);
                        tracing::error!(target: "pg2osync::sink",
                            "halting pipeline: {} quarantined documents are already at \
                             the max_rejects limit of {max_rejects}; fix the mapping and replay \
                             them with `pg2osync rejects --replay`",
                            self.quarantined);
                        return false;
                    }
                    // Recorded *before* the position is acknowledged. The
                    // checkpoint may only pass a document that was written or
                    // durably filed as refused; if this fails the batch goes
                    // unacknowledged and the source replays it.
                    if let Err(e) = sink.quarantine(&ack.rejected).await {
                        metrics.sink_errors_total.fetch_add(1, Ordering::Relaxed);
                        tracing::error!(target: "pg2osync::sink",
                            "halting pipeline: {} document(s) were refused and could not be \
                             quarantined, so their position must not be acknowledged: {e}",
                            ack.rejected.len());
                        return false;
                    }
                    self.quarantined += ack.rejected.len() as u64;
                    metrics
                        .rejected_total
                        .fetch_add(ack.rejected.len() as u64, Ordering::Relaxed);
                    tracing::warn!(target: "pg2osync::sink",
                        "quarantined {}/{} ({}); {} of {max_rejects} used",
                        first.index, first.doc_id, first.reason, self.quarantined);
                    if self.quarantined >= max_rejects {
                        tracing::error!(target: "pg2osync::sink",
                            "halting pipeline: {} quarantined documents reached the \
                             max_rejects limit of {max_rejects}. One bad row is worth carrying \
                             on past; this many means something systematic",
                            self.quarantined);
                        return false;
                    }
                }
                // zero-position acks come from initial-load batches;
                // acknowledging them would corrupt the source position chain
                if ack.max_lsn.0 > 0 {
                    self.ack_tx.send_replace(Some(ack.max_lsn));
                    self.ckpt_done_tx.send_replace(Some(ack.max_lsn));
                }
                true
            }
            Ok(None) => true,
            Err(e) => {
                metrics.sink_errors_total.fetch_add(1, Ordering::Relaxed);
                tracing::error!(target: "pg2osync::sink", "sink failed permanently: {e}");
                false
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
    rules: &Rules<'_>,
) -> Result<HashMap<(String, String), Option<Value>>, CoreError> {
    let mut wanted: HashMap<String, Vec<String>> = HashMap::new();
    for row in rows {
        let Some(id) = completion_id(&row.kind, rules, (&row.schema, &row.table)) else {
            continue;
        };
        let Some(index) = mapping.opt_index_for(&row.schema, &row.table) else {
            continue;
        };
        let ids = wanted.entry(index.to_string()).or_default();
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

/// The per-table rules that decide what a row becomes in the target:
/// projection, transforms, field renames, the document id, and whether one
/// row fans out into many documents. Everything that mints an `_id` goes
/// through here.
pub struct Rules<'a> {
    pub projections: &'a crate::mapping::Projections,
    pub transforms: &'a crate::mapping::Transforms,
    pub renames: &'a crate::mapping::Renames,
    pub id_templates: &'a crate::mapping::IdTemplates,
    pub fan_outs: &'a crate::mapping::FanOuts,
}

/// The document id for the row state described by `doc`, or by `pk` alone
/// for a delete. Identity renders from the RAW values — before projections
/// and before transforms — and a missing or NULL column is an error naming
/// the table and column: an id that quietly changed would strand the
/// documents the row already owns.
fn derived_id(
    table: (&str, &str),
    pk: &Value,
    doc: Option<&Value>,
    before: Option<&Value>,
    templates: &crate::mapping::IdTemplates,
) -> Result<String, CoreError> {
    let halt = |e: String| CoreError::Other(format!("{}.{}: {e}", table.0, table.1));
    let Some(template) = templates.for_table(table.0, table.1) else {
        return Ok(pk_to_id(pk));
    };
    if let Some(doc) = doc {
        return template.render(doc).map_err(halt);
    }
    // A key-only event. The before-image, when the source carries one, is
    // the row exactly as the target last saw it, so it renders the id the
    // current document is filed under. Without it the key alone has to be
    // enough: `is_pk_only` is decided from the *configured* key, which can
    // claim a single column where the real key is composite, so it is used
    // only once the before-image is ruled out — and the startup check has
    // already refused any template the catalogue's key cannot render.
    if let Some(before) = before {
        return template.render(before).map_err(halt);
    }
    if template.is_pk_only() || pk.is_object() {
        return template.render_from_pk(pk).map_err(halt);
    }
    Err(halt(
        "the configured id needs columns outside the primary key, but this event \
         carries no before-image; the table needs REPLICA IDENTITY FULL"
            .into(),
    ))
}

/// The document id whose stored copy completes this change, if one is needed.
///
/// A row whose key changed is still filed under the old id, so completing from
/// the new one would write the unchanged columns as null. The old id is what
/// the template rendered for the row's previous state: for a key-only
/// template that is the previous key, otherwise the before-image.
fn completion_id(kind: &RowKind, rules: &Rules<'_>, table: (&str, &str)) -> Option<String> {
    let RowKind::Update {
        pk,
        previous_pk,
        unchanged_toast_columns,
        before,
        ..
    } = kind
    else {
        return None;
    };
    if unchanged_toast_columns.is_empty() {
        return None;
    }
    let template = rules.id_templates.for_table(table.0, table.1);
    let Some(template) = template else {
        let id = pk_to_id(pk);
        return Some(match previous_pk.as_ref().map(pk_to_id) {
            Some(previous) if previous != id => previous,
            _ => id,
        });
    };
    if template.is_pk_only() {
        return template
            .render_from_pk(previous_pk.as_ref().unwrap_or(pk))
            .ok();
    }
    // the row's previous document carries the before-image's id, when there
    // is one; without it there is nothing to complete against and the write
    // path halts on its own terms
    before
        .as_ref()
        .and_then(|b| template.render(b).ok())
        .or_else(|| template.render_from_pk(pk).ok())
}

/// Convert one row change into document operations, completing unchanged-TOAST
/// columns from the previously indexed document when needed.
///
/// `previous` is the document already in the target, which the caller fetches
/// when `completion_id` asked for one. Doing it here would mean one round-trip
/// per row in the middle of the pipeline; measured on 20k updates to a table
/// with an 8 kB TOASTed column, that was the difference between 1,800 and
/// 4,400 rows per second.
fn materialize(
    index: &str,
    table: (&str, &str),
    kind: &RowKind,
    rules: &Rules<'_>,
    previous: Option<&Value>,
    version: Option<u64>,
) -> Result<Vec<LsnOp>, CoreError> {
    // PENDING_LSN is overwritten by the commit handler before any ack can
    // reference it: rows never leave the buffer without their commit attached.
    const PENDING_LSN: Lsn = Lsn(0);
    let mk = |op| LsnOp {
        lsn: PENDING_LSN,
        op,
    };
    let upsert = |id: String, doc: Value| {
        mk(DocumentOp::Upsert {
            index: index.into(),
            id,
            doc,
            version,
        })
    };
    let delete = |id: String| {
        mk(DocumentOp::Delete {
            index: index.into(),
            id,
            version,
        })
    };
    let halt = |e: String| CoreError::Other(format!("{}.{}: {e}", table.0, table.1));
    // Identity renders from the row's RAW values, so every derivation below
    // reads the document before projections and transforms touch it; renames
    // run last of all, so nothing but the target ever sees the new names.
    let fan = rules.fan_outs.for_table(table.0, table.1);
    let shape = |base: &str, doc: &Value| -> Result<Vec<(String, Value)>, CoreError> {
        match fan {
            None => Ok(vec![(base.to_string(), doc.clone())]),
            Some(rule) => crate::mapping::fan_out_docs(rule, base, doc).map_err(halt),
        }
    };
    let finish = |docs: Vec<(String, Value)>| -> Vec<LsnOp> {
        docs.into_iter()
            .map(|(id, mut doc)| {
                rules.projections.apply(table.0, table.1, &mut doc);
                rules.transforms.apply(table.0, table.1, &mut doc);
                rules.renames.apply(table.0, table.1, &mut doc);
                upsert(id, doc)
            })
            .collect()
    };
    match kind {
        RowKind::Insert { pk, doc } => {
            let base = derived_id(table, pk, Some(doc), None, rules.id_templates)?;
            Ok(finish(shape(&base, doc)?))
        }
        RowKind::Update {
            pk,
            previous_pk,
            doc,
            unchanged_toast_columns,
            before,
        } => {
            let mut doc = doc.clone();
            if !unchanged_toast_columns.is_empty()
                && let Some(Value::Object(prev_map)) = previous
                && let Value::Object(doc_map) = &mut doc
            {
                for col in unchanged_toast_columns {
                    // the stored document carries the target name; the one
                    // being built is still in source names, renames run last
                    let stored = rules.renames.target_name(table.0, table.1, col);
                    if let Some(v) = prev_map.get(stored) {
                        doc_map.insert(col.clone(), v.clone());
                    }
                }
            }
            let before = before.as_ref();
            let base = derived_id(table, pk, Some(&doc), before, rules.id_templates)?;
            let new_docs = shape(&base, &doc)?;
            let mut ops = finish(new_docs.clone());
            // write first, delete second: a crash between them leaves a
            // duplicate that the replay repairs, where the reverse order would
            // leave a gap that nothing repairs
            if let Some(rule) = fan {
                // the diff against the before-image: every document the row
                // owned that its new state no longer produces is removed. The
                // startup check required a before-image for fanned tables, so
                // its absence means a source that broke its promise rather
                // than a delete nobody asked for; lingering stale documents
                // are the reconcile tool's to find.
                if let Some(before) = before {
                    let old_base = derived_id(
                        table,
                        previous_pk.as_ref().unwrap_or(pk),
                        None,
                        Some(before),
                        rules.id_templates,
                    )?;
                    let held: std::collections::HashSet<&str> =
                        new_docs.iter().map(|(id, _)| id.as_str()).collect();
                    for (id, _) in crate::mapping::fan_out_docs(rule, &old_base, before)
                        .map_err(|e| halt(format!("before-image of a fanned row: {e}")))?
                    {
                        if !held.contains(id.as_str()) {
                            ops.push(delete(id));
                        }
                    }
                }
            } else {
                // A changed key means the row moved to a different document.
                // The old one still holds the previous version and has to be
                // removed, or nothing will ever collect it.
                let moved_from = match rules.id_templates.for_table(table.0, table.1) {
                    None => previous_pk.as_ref().map(pk_to_id),
                    Some(t) if t.is_pk_only() => {
                        previous_pk.as_ref().and_then(|p| t.render_from_pk(p).ok())
                    }
                    Some(t) => before.and_then(|b| t.render(b).ok()),
                }
                .filter(|previous| previous != &base);
                if let Some(previous) = moved_from {
                    ops.push(delete(previous));
                }
            }
            Ok(ops)
        }
        RowKind::Delete { pk, before } => {
            let before = before.as_ref();
            let base = derived_id(table, pk, None, before, rules.id_templates)?;
            match fan {
                None => Ok(vec![delete(base)]),
                Some(rule) => {
                    let row = before.ok_or_else(|| {
                        halt(
                            "a fanned row's delete needs its before-image, which this event \
                             does not carry; the table needs REPLICA IDENTITY FULL"
                                .into(),
                        )
                    })?;
                    Ok(crate::mapping::fan_out_docs(rule, &base, row)
                        .map_err(halt)?
                        .into_iter()
                        .map(|(id, _)| delete(id))
                        .collect())
                }
            }
        }
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
        /// Document ids the target refuses permanently.
        refuse: Mutex<Vec<String>>,
        /// Whether recording a refusal fails, which must stop the pipeline
        /// without acknowledging.
        quarantine_fails: bool,
        /// What the store already held before this run.
        held: u64,
        /// Every upserted document, so a test can look at what was written
        /// and not only at which ids the event log names.
        docs: Mutex<Vec<(String, Value)>>,
        /// What a read-back returns beyond the id; unset keeps the fixed
        /// `bio: stored` shape the older tests rely on.
        stored: Mutex<Option<Value>>,
    }

    impl RecordingSink {
        fn events(&self) -> Vec<String> {
            self.events.lock().expect("not poisoned").clone()
        }
        fn doc(&self, id: &str) -> Option<Value> {
            self.docs
                .lock()
                .expect("not poisoned")
                .iter()
                .rev()
                .find(|(written, _)| written == id)
                .map(|(_, doc)| doc.clone())
        }
        fn store(&self, doc: Value) {
            *self.stored.lock().expect("not poisoned") = Some(doc);
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
            let stored = self.stored.lock().expect("not poisoned").clone();
            Ok(ids
                .iter()
                .map(|id| {
                    let mut doc = stored.clone().unwrap_or_else(|| json!({"bio": "stored"}));
                    doc["id"] = json!(id);
                    Some(doc)
                })
                .collect())
        }

        async fn write(&self, batch: Vec<LsnOp>) -> Result<SinkAck, CoreError> {
            let max_lsn = batch.last().expect("engine never sends empty batches").lsn;
            let refuse = self.refuse.lock().expect("not poisoned").clone();
            let rejected: Vec<pg2osync_core::sink::Rejection> = batch
                .iter()
                .filter(|op| {
                    let id = match &op.op {
                        DocumentOp::Upsert { id, .. } | DocumentOp::Delete { id, .. } => id,
                    };
                    refuse.contains(id)
                })
                .map(|op| {
                    let (index, doc_id) = match &op.op {
                        DocumentOp::Upsert { index, id, .. }
                        | DocumentOp::Delete { index, id, .. } => (index.clone(), id.clone()),
                    };
                    pg2osync_core::sink::Rejection {
                        index,
                        doc_id,
                        reason: "refused by the test target".into(),
                        lsn: op.lsn,
                        op: op.op.clone(),
                    }
                })
                .collect();
            self.docs
                .lock()
                .expect("not poisoned")
                .extend(batch.iter().filter_map(|op| match &op.op {
                    DocumentOp::Upsert { id, doc, .. } => Some((id.clone(), doc.clone())),
                    DocumentOp::Delete { .. } => None,
                }));
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
            Ok(SinkAck { max_lsn, rejected })
        }

        fn can_quarantine(&self) -> bool {
            true
        }

        async fn quarantine(
            &self,
            rejected: &[pg2osync_core::sink::Rejection],
        ) -> Result<(), CoreError> {
            if self.quarantine_fails {
                return Err(CoreError::Sink("quarantine store unavailable".into()));
            }
            let ids: Vec<String> = rejected.iter().map(|r| r.doc_id.clone()).collect();
            self.events
                .lock()
                .expect("not poisoned")
                .push(format!("quarantine[{}]", ids.join(" ")));
            Ok(())
        }

        async fn list_rejects(
            &self,
            _limit: usize,
        ) -> Result<(Vec<pg2osync_core::sink::StoredReject>, u64), CoreError> {
            Ok((Vec::new(), self.held))
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
        row_at(id, None)
    }

    fn row_at(id: i64, version: Option<u64>) -> ChangeEvent {
        ChangeEvent::Row(RowChange {
            schema: "public".into(),
            table: "users".into(),
            kind: RowKind::Insert {
                pk: json!(id),
                doc: json!({"id": id}),
            },
            version,
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
        drive(batch_size, script).await.0
    }

    /// As `run_script`, also reporting the highest load mark reported written.
    async fn drive(batch_size: usize, script: Vec<ChangeEvent>) -> (Arc<RecordingSink>, u64) {
        drive_split(batch_size, script, Vec::new()).await
    }

    /// As `drive`, with a second script on the copy channel. Both are queued
    /// before the engine starts, so the order it drains them in is the engine's
    /// choice and not a race — which is what makes a test about precedence, or
    /// about one channel outranking the other, mean anything.
    async fn drive_split(
        batch_size: usize,
        script: Vec<ChangeEvent>,
        copy_script: Vec<ChangeEvent>,
    ) -> (Arc<RecordingSink>, u64) {
        drive_sink(
            Arc::new(RecordingSink::default()),
            EngineConfig {
                batch_size,
                checkpoint_interval_ms: 100,
                ..EngineConfig::default()
            },
            script,
            copy_script,
        )
        .await
    }

    /// As `drive_split`, over a sink and a configuration the caller chose — for
    /// the tests that are about what the pipeline does when a write is refused.
    async fn drive_sink(
        sink: Arc<RecordingSink>,
        cfg: EngineConfig,
        script: Vec<ChangeEvent>,
        copy_script: Vec<ChangeEvent>,
    ) -> (Arc<RecordingSink>, u64) {
        let (events_tx, events_rx) = mpsc::channel(1024);
        let (copy_tx, copy_rx) = mpsc::channel(1024);
        let (ack_tx, _ack_rx) = watch::channel(None);
        let (load_done_tx, load_done_rx) = watch::channel(0u64);
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let ctx = Arc::new(PipelineCtx {
            sink: sink.clone(),
            mapping: TableMapping::from_pairs([(
                ("public".to_string(), "users".to_string()),
                "users".to_string(),
            )]),
            projections: crate::mapping::Projections::default(),
            transforms: crate::mapping::Transforms::default(),
            renames: crate::mapping::Renames::default(),
            id_templates: crate::mapping::IdTemplates::default(),
            fan_outs: crate::mapping::FanOuts::default(),
            cfg,
            ack_tx,
            load_done_tx,
            metrics: Arc::new(crate::metrics::Metrics::default()),
        });
        let stream = StreamId {
            source: SOURCE_POSTGRES.into(),
            stream: "slot".into(),
            publication: "pub".into(),
        };
        let render: PositionRenderer = Arc::new(|token| Lsn(token).to_string());
        for event in copy_script {
            copy_tx.send(event).await.expect("copy channel open");
        }
        drop(copy_tx);
        // Queued, not sent after the engine starts: the sender is still held, so
        // the engine cannot finish early, and it sees both channels already
        // populated on its first turn.
        for event in script {
            events_tx.send(event).await.expect("channel has room");
        }
        let engine = tokio::spawn(run(
            events_rx,
            copy_rx,
            ctx,
            stream,
            render,
            crate::mapping::DurableLsn::default(),
            shutdown_rx,
        ));
        // let the checkpoint loop observe the final acknowledged position
        tokio::time::sleep(std::time::Duration::from_millis(400)).await;
        drop(events_tx);
        engine.await.expect("task joined").expect("engine ran");
        let mark = *load_done_rx.borrow();
        (sink, mark)
    }

    #[tokio::test]
    async fn change_events_are_drained_before_copy_rows() {
        // Both are already queued, so this is the engine's choice: the source
        // retains WAL until the stream is consumed, and a copy chunk can wait.
        let (sink, _) = drive_split(
            500,
            vec![row_at(1, Some(0x100)), commit(0x100)],
            vec![row_at(2, None), row_at(3, None), ChangeEvent::LoadMark(1)],
        )
        .await;
        assert_eq!(
            sink.events(),
            vec!["write[upsert:1@256 upsert:2 upsert:3]"],
            "the streamed row is written first even though the copy queued first"
        );
    }

    #[tokio::test]
    async fn a_load_mark_waits_for_the_transaction_it_landed_inside() {
        // The load runs beside the stream, so its marks arrive at arbitrary
        // points. Acting on one mid-transaction would flush half a transaction
        // to the target, which is the one thing the buffer exists to prevent.
        let (sink, mark) = drive(
            500,
            vec![
                begin(0x100),
                row(1),
                ChangeEvent::LoadMark(1),
                row(2),
                commit(0x100),
            ],
        )
        .await;
        assert_eq!(sink.events(), vec!["write[upsert:1 upsert:2]"]);
        assert_eq!(mark, 1, "the mark is still reported, just not early");
    }

    #[tokio::test]
    async fn a_load_mark_is_reported_only_after_the_rows_before_it() {
        let (sink, mark) = drive(500, vec![row(1), ChangeEvent::LoadMark(7)]).await;
        assert_eq!(sink.events(), vec!["write[upsert:1]"]);
        assert_eq!(mark, 7);
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
    async fn a_document_is_written_at_the_version_its_row_carries() {
        // the version stops a stale write from overwriting a newer document at
        // the target, so it has to reach the sink unchanged
        let sink = run_script(10, vec![row_at(1, Some(0x200)), commit(0x200)]).await;
        assert_eq!(sink.events(), vec!["write[upsert:1@512]"]);
    }

    #[tokio::test]
    async fn coalescing_into_one_batch_does_not_blur_versions() {
        let sink = run_script(
            10,
            vec![
                row_at(1, Some(0x100)),
                commit(0x100),
                row_at(2, Some(0x300)),
                commit(0x300),
            ],
        )
        .await;
        assert_eq!(sink.events(), vec!["write[upsert:1@256 upsert:2@768]"]);
    }

    #[tokio::test]
    async fn interleaved_producers_keep_their_own_versions() {
        // a copy row landing between a transaction's BEGIN and its rows must
        // not take that transaction's position, nor lend it its own: this is
        // what makes copying beside the stream safe
        let sink = run_script(
            10,
            vec![
                begin(0x900),
                row_at(1, Some(0x100)),
                row_at(2, Some(0x900)),
                commit(0x900),
            ],
        )
        .await;
        assert_eq!(sink.events(), vec!["write[upsert:1@256 upsert:2@2304]"]);
    }

    #[tokio::test]
    async fn rows_with_no_position_go_unversioned() {
        // a polling source reconstructs state and has no log position to offer
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
                    version: None,
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
                before: None,
            },
            version: None,
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
                before: None,
            },
            version: None,
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

    fn deleted_at(id: i64, version: u64) -> ChangeEvent {
        ChangeEvent::Row(RowChange {
            schema: "public".into(),
            table: "users".into(),
            kind: RowKind::Delete {
                pk: json!(id),
                before: None,
            },
            version: Some(version),
        })
    }

    fn truncated_at(version: u64) -> ChangeEvent {
        ChangeEvent::TableTruncated {
            schema: "public".into(),
            table: "users".into(),
            version: Some(version),
        }
    }

    #[tokio::test]
    async fn a_copy_row_the_stream_has_already_deleted_is_never_written() {
        // The target cannot be trusted to refuse this. A versioned delete leaves
        // a tombstone, the tombstone lives for index.gc_deletes (60s), and once
        // it is gone external_gte accepts any version at all — so a copy row
        // starved behind a busy stream for longer than that would put the
        // document back. Verified against a real target: at gc_deletes = 1s the
        // same write is a 409 immediately and a `created` two seconds later.
        let (sink, _) = drive_split(
            500,
            vec![deleted_at(1, 0x200), commit(0x200)],
            vec![row_at(1, Some(0x100)), ChangeEvent::LoadMark(1)],
        )
        .await;
        assert_eq!(
            sink.events(),
            vec!["write[delete:1]"],
            "the delete stands and the stale copy row is gone"
        );
    }

    #[tokio::test]
    async fn a_copy_row_read_after_the_delete_still_lands() {
        // The rule is a comparison, not a blocklist: a chunk that read the key
        // after it was deleted and found it there again is telling the truth.
        let (sink, _) = drive_split(
            500,
            vec![deleted_at(1, 0x100), commit(0x100)],
            vec![row_at(1, Some(0x200)), ChangeEvent::LoadMark(1)],
        )
        .await;
        assert_eq!(sink.events(), vec!["write[delete:1 upsert:1@512]"]);
    }

    #[tokio::test]
    async fn a_streamed_reinsert_after_a_delete_survives() {
        // Why the rule needs no idea which channel a row came from: within a
        // stream a later event always carries a higher position, so it can never
        // fire on one.
        let sink = run_script(
            500,
            vec![deleted_at(1, 0x100), row_at(1, Some(0x200)), commit(0x200)],
        )
        .await;
        assert_eq!(sink.events(), vec!["write[delete:1 upsert:1@512]"]);
    }

    #[tokio::test]
    async fn a_delete_and_a_reinsert_sharing_one_commit_both_stand() {
        // PostgreSQL versions every row of a transaction with its commit
        // position, so these two are equal. Comparing with >= instead of >
        // would drop the re-insert and lose the row.
        let sink = run_script(
            500,
            vec![
                begin(0x300),
                deleted_at(1, 0x300),
                row_at(1, Some(0x300)),
                commit(0x300),
            ],
        )
        .await;
        assert_eq!(sink.events(), vec!["write[delete:1 upsert:1@768]"]);
    }

    #[tokio::test]
    async fn a_copy_row_from_before_a_truncate_is_dropped() {
        // A truncate removes every key at once, and clears the index with
        // versioned deletes whose tombstones expire like any other.
        let (sink, _) = drive_split(
            500,
            vec![truncated_at(0x200)],
            vec![
                row_at(1, Some(0x100)),
                row_at(2, Some(0x300)),
                ChangeEvent::LoadMark(1),
            ],
        )
        .await;
        assert_eq!(
            sink.events(),
            vec!["truncate(users)", "write[upsert:2@768]"],
            "the row read before the truncate goes, the one read after stays"
        );
    }

    #[tokio::test]
    async fn a_load_mark_ends_the_window_it_closed() {
        // The load waits for each mark before reading the next chunk, so once a
        // mark arrives nothing from the chunk it belongs to is still in flight
        // and the next chunk's rows must not be judged against it.
        let (sink, _) = drive_split(
            500,
            vec![deleted_at(1, 0x200), commit(0x200)],
            vec![
                ChangeEvent::LoadMark(1),
                row_at(1, Some(0x100)),
                ChangeEvent::LoadMark(2),
            ],
        )
        .await;
        assert_eq!(
            sink.events(),
            vec!["write[delete:1]", "write[upsert:1@256]"],
            "the mark cleared the window, so the later chunk is untouched"
        );
    }

    fn refusing(ids: &[&str]) -> RecordingSink {
        RecordingSink {
            refuse: Mutex::new(ids.iter().map(|s| (*s).to_string()).collect()),
            ..RecordingSink::default()
        }
    }

    fn quarantining(max_rejects: u64) -> EngineConfig {
        EngineConfig {
            batch_size: 500,
            checkpoint_interval_ms: 100,
            on_permanent_rejection: RejectionPolicy::Quarantine,
            max_rejects,
            ..EngineConfig::default()
        }
    }

    #[tokio::test]
    async fn a_refused_document_halts_by_default_and_acknowledges_nothing() {
        let (sink, _) = drive_sink(
            Arc::new(refusing(&["2"])),
            EngineConfig {
                batch_size: 500,
                checkpoint_interval_ms: 100,
                ..EngineConfig::default()
            },
            vec![row(1), row(2), commit(0x100)],
            vec![],
        )
        .await;
        assert_eq!(
            sink.last_checkpoint(),
            None,
            "a position may never pass a document that was neither written nor recorded"
        );
    }

    #[tokio::test]
    async fn quarantine_records_before_it_acknowledges() {
        // The order is the whole correctness argument: the checkpoint may only
        // advance past a refused document once the target holds it.
        let (sink, _) = drive_sink(
            Arc::new(refusing(&["2"])),
            quarantining(100),
            vec![row(1), row(2), commit(0x100)],
            vec![],
        )
        .await;
        assert_eq!(
            sink.events(),
            vec!["write[upsert:1 upsert:2]", "quarantine[2]"],
            "the write is reported, then the refusal is filed"
        );
        assert_eq!(
            sink.last_checkpoint().map(|c| c.token),
            Some(0x100),
            "and only then does the position move"
        );
    }

    #[tokio::test]
    async fn a_quarantine_that_fails_halts_without_acknowledging() {
        let sink = RecordingSink {
            quarantine_fails: true,
            ..refusing(&["2"])
        };
        let (sink, _) = drive_sink(
            Arc::new(sink),
            quarantining(100),
            vec![row(1), row(2), commit(0x100)],
            vec![],
        )
        .await;
        assert_eq!(
            sink.last_checkpoint(),
            None,
            "nowhere to record it means the source has to send it again"
        );
    }

    #[tokio::test]
    async fn the_limit_stops_a_mapping_that_refuses_everything() {
        let (sink, _) = drive_sink(
            Arc::new(refusing(&["1", "2", "3"])),
            quarantining(2),
            vec![
                row(1),
                commit(0x100),
                row(2),
                commit(0x200),
                row(3),
                commit(0x300),
            ],
            vec![],
        )
        .await;
        // The three transactions coalesce into one batch, so all three refusals
        // arrive together: every one is recorded first — losing them is the one
        // thing the limit must not cause — and only then does the limit halt.
        assert_eq!(
            sink.events(),
            vec!["write[upsert:1 upsert:2 upsert:3]", "quarantine[1 2 3]"]
        );
        assert_eq!(
            sink.last_checkpoint(),
            None,
            "the batch that hit the limit is not acknowledged either"
        );
    }

    #[tokio::test]
    async fn the_budget_counts_what_the_target_already_holds() {
        // Otherwise a crash loop hands the whole budget back on every restart.
        let sink = RecordingSink {
            held: 2,
            ..refusing(&["1"])
        };
        let (sink, _) = drive_sink(
            Arc::new(sink),
            quarantining(2),
            vec![row(1), commit(0x100)],
            vec![],
        )
        .await;
        assert_eq!(
            sink.last_checkpoint(),
            None,
            "already at the limit before this run started"
        );
    }

    #[tokio::test]
    async fn a_refused_delete_keeps_its_operation() {
        let (sink, _) = drive_sink(
            Arc::new(refusing(&["7"])),
            quarantining(100),
            vec![deleted_at(7, 0x100), commit(0x100)],
            vec![],
        )
        .await;
        assert_eq!(sink.events(), vec!["write[delete:7]", "quarantine[7]"]);
    }

    #[tokio::test]
    async fn unmapped_tables_are_ignored_for_truncate() {
        let sink = run_script(
            500,
            vec![ChangeEvent::TableTruncated {
                schema: "public".into(),
                table: "not_synced".into(),
                version: None,
            }],
        )
        .await;
        assert!(sink.events().is_empty());
    }

    /// A sink that holds every write open until the test lets it finish, which
    /// is the only way to observe how many the writer keeps open at once.
    struct GatedSink {
        /// Writes whose position is at or below this may return.
        release: watch::Sender<u64>,
        open: std::sync::atomic::AtomicUsize,
        peak: std::sync::atomic::AtomicUsize,
        /// Positions in the order the writes finished.
        finished: Mutex<Vec<u64>>,
        /// A position the target refuses, to test what is acknowledged after it.
        fail_at: Option<u64>,
    }

    impl GatedSink {
        fn new(fail_at: Option<u64>) -> Arc<Self> {
            Arc::new(Self {
                release: watch::channel(0).0,
                open: std::sync::atomic::AtomicUsize::new(0),
                peak: std::sync::atomic::AtomicUsize::new(0),
                finished: Mutex::new(Vec::new()),
                fail_at,
            })
        }

        fn peak(&self) -> usize {
            self.peak.load(Ordering::SeqCst)
        }

        fn finished(&self) -> Vec<u64> {
            self.finished.lock().expect("not poisoned").clone()
        }

        /// Wait until `n` writes are open at once, so an assertion about
        /// concurrency does not race the tasks it is about.
        async fn await_open(&self, n: usize) {
            tokio::time::timeout(std::time::Duration::from_secs(5), async {
                while self.open.load(Ordering::SeqCst) < n {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("writes opened");
        }
    }

    #[async_trait::async_trait]
    impl Sink for GatedSink {
        async fn ensure_ready(&self, _tables: &[IndexSpec]) -> Result<(), CoreError> {
            Ok(())
        }

        async fn get_documents(
            &self,
            _index: &str,
            ids: &[String],
        ) -> Result<Vec<Option<Value>>, CoreError> {
            Ok(ids.iter().map(|_| None).collect())
        }

        async fn write(&self, batch: Vec<LsnOp>) -> Result<SinkAck, CoreError> {
            let lsn = batch.last().expect("nonempty").lsn;
            let open = self.open.fetch_add(1, Ordering::SeqCst) + 1;
            self.peak.fetch_max(open, Ordering::SeqCst);
            let mut release = self.release.subscribe();
            loop {
                if *release.borrow_and_update() >= lsn.0 {
                    break;
                }
                release.changed().await.expect("gate open");
            }
            self.open.fetch_sub(1, Ordering::SeqCst);
            self.finished.lock().expect("not poisoned").push(lsn.0);
            if self.fail_at == Some(lsn.0) {
                return Err(CoreError::Sink("refused".into()));
            }
            Ok(SinkAck::written(lsn))
        }

        async fn refresh(&self, _indices: &[String]) -> Result<(), CoreError> {
            Ok(())
        }

        async fn truncate_index(
            &self,
            _index: &str,
            _version: Option<u64>,
        ) -> Result<(), CoreError> {
            Ok(())
        }

        async fn write_checkpoint(&self, _checkpoint: &Checkpoint) -> Result<(), CoreError> {
            Ok(())
        }

        async fn read_checkpoint(
            &self,
            _stream: &pg2osync_core::checkpoint::StreamId,
        ) -> Result<Option<Checkpoint>, CoreError> {
            Ok(None)
        }

        async fn health(&self) -> Result<Health, CoreError> {
            Ok(Health::Up)
        }
    }

    /// One batch carrying a single upsert at `lsn`, which is what the writer
    /// acknowledges the position from.
    fn batch_at(lsn: u64) -> Vec<LsnOp> {
        vec![LsnOp {
            lsn: Lsn(lsn),
            op: DocumentOp::Upsert {
                index: "users".into(),
                id: lsn.to_string(),
                doc: json!({"id": lsn}),
                version: Some(lsn),
            },
        }]
    }

    /// The writer on its own, so the tests are about the write window and not
    /// about how the engine happens to batch.
    struct Writer {
        commands: mpsc::Sender<SinkCommand>,
        acked: watch::Receiver<Option<Lsn>>,
        load_done: watch::Receiver<u64>,
        task: tokio::task::JoinHandle<()>,
    }

    fn writer(sink: Arc<GatedSink>, concurrency: usize) -> Writer {
        let (commands, commands_rx) = mpsc::channel(64);
        let (ack_tx, acked) = watch::channel(None);
        let (ckpt_done_tx, _ckpt_done_rx) = watch::channel(None);
        let (load_done_tx, load_done) = watch::channel(0u64);
        let task = tokio::spawn(sink_loop(
            commands_rx,
            sink,
            ack_tx,
            ckpt_done_tx,
            load_done_tx,
            Arc::new(crate::metrics::Metrics::default()),
            RejectionPolicy::Halt,
            100,
            concurrency,
        ));
        Writer {
            commands,
            acked,
            load_done,
            task,
        }
    }

    #[tokio::test]
    async fn write_concurrency_is_how_many_requests_stay_open() {
        let sink = GatedSink::new(None);
        let writer = writer(sink.clone(), 3);
        for lsn in [100, 200, 300] {
            writer
                .commands
                .send(SinkCommand::Write(batch_at(lsn)))
                .await
                .expect("writer running");
        }
        sink.await_open(3).await;
        assert_eq!(sink.peak(), 3, "three requests open against the target");
        sink.release.send_replace(300);
        drop(writer.commands);
        writer.task.await.ok();
    }

    #[tokio::test]
    async fn one_request_at_a_time_is_still_one_request_at_a_time() {
        let sink = GatedSink::new(None);
        let writer = writer(sink.clone(), 1);
        for lsn in [100, 200] {
            writer
                .commands
                .send(SinkCommand::Write(batch_at(lsn)))
                .await
                .expect("writer running");
        }
        sink.await_open(1).await;
        // Nothing may follow the first while it is unfinished, which is the
        // default the whole change leaves untouched.
        assert_eq!(sink.peak(), 1);
        sink.release.send_replace(200);
        drop(writer.commands);
        writer.task.await.ok();
    }

    #[tokio::test]
    async fn a_slow_write_holds_back_the_position_of_the_ones_behind_it() {
        let sink = GatedSink::new(None);
        let writer = writer(sink.clone(), 3);
        for lsn in [100, 200, 300] {
            writer
                .commands
                .send(SinkCommand::Write(batch_at(lsn)))
                .await
                .expect("writer running");
        }
        sink.await_open(3).await;
        // Every position that is ever acknowledged, so the assertion is about
        // the sequence rather than about whichever value happened to be last.
        let seen = Arc::new(Mutex::new(Vec::new()));
        let collector = {
            let seen = seen.clone();
            let mut acked = writer.acked.clone();
            tokio::spawn(async move {
                while acked.changed().await.is_ok() {
                    if let Some(lsn) = *acked.borrow_and_update() {
                        seen.lock().expect("not poisoned").push(lsn.0);
                    }
                }
            })
        };
        // The gate opens for all three at once, so the writes finish as the
        // runtime pleases; the positions must not.
        sink.release.send_replace(300);
        drop(writer.commands);
        writer.task.await.ok();
        collector.abort();
        let seen = seen.lock().expect("not poisoned").clone();
        assert_eq!(
            seen.last(),
            Some(&300),
            "the whole window is durable, so the newest position is safe"
        );
        assert!(
            seen.windows(2).all(|pair| pair[0] < pair[1]),
            "a position may never go backwards: {seen:?}"
        );
        assert_eq!(
            sink.finished().len(),
            3,
            "all three writes ran, concurrently"
        );
    }

    #[tokio::test]
    async fn a_load_mark_waits_for_every_write_before_it() {
        let sink = GatedSink::new(None);
        let mut writer = writer(sink.clone(), 3);
        for lsn in [100, 200] {
            writer
                .commands
                .send(SinkCommand::Write(batch_at(lsn)))
                .await
                .expect("writer running");
        }
        writer
            .commands
            .send(SinkCommand::LoadMark(7))
            .await
            .expect("writer running");
        sink.await_open(2).await;
        assert_eq!(
            *writer.load_done.borrow_and_update(),
            0,
            "the mark is what says the chunk is durable, so it cannot pass the writes"
        );
        sink.release.send_replace(200);
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            writer.load_done.changed(),
        )
        .await
        .expect("the mark arrives")
        .expect("channel open");
        assert_eq!(*writer.load_done.borrow_and_update(), 7);
        drop(writer.commands);
        writer.task.await.ok();
    }

    #[tokio::test]
    async fn nothing_behind_a_refused_write_is_acknowledged() {
        let sink = GatedSink::new(Some(200));
        let mut writer = writer(sink.clone(), 3);
        for lsn in [100, 200, 300] {
            writer
                .commands
                .send(SinkCommand::Write(batch_at(lsn)))
                .await
                .expect("writer running");
        }
        sink.await_open(3).await;
        sink.release.send_replace(300);
        writer.task.await.ok();
        assert_eq!(
            *writer.acked.borrow_and_update(),
            Some(Lsn(100)),
            "300 was written but sits behind a failure, so its position must not pass"
        );
    }

    /// As `drive_sink`, with derived ids and fan-out rules configured for
    /// `public.users` → `users`. Everything else is the default pipeline.
    async fn drive_rules(
        ids: crate::mapping::IdTemplates,
        fan: crate::mapping::FanOuts,
        script: Vec<ChangeEvent>,
    ) -> Arc<RecordingSink> {
        drive_rules_with(ids, fan, crate::mapping::Renames::default(), script).await
    }

    async fn drive_rules_with(
        ids: crate::mapping::IdTemplates,
        fan: crate::mapping::FanOuts,
        renames: crate::mapping::Renames,
        script: Vec<ChangeEvent>,
    ) -> Arc<RecordingSink> {
        drive_rules_at(
            500,
            ids,
            fan,
            renames,
            Arc::new(RecordingSink::default()),
            script,
        )
        .await
    }

    async fn drive_rules_at(
        batch_size: usize,
        ids: crate::mapping::IdTemplates,
        fan: crate::mapping::FanOuts,
        renames: crate::mapping::Renames,
        sink: Arc<RecordingSink>,
        script: Vec<ChangeEvent>,
    ) -> Arc<RecordingSink> {
        let (events_tx, events_rx) = mpsc::channel(1024);
        let (copy_tx, copy_rx) = mpsc::channel(1024);
        drop(copy_tx);
        let (ack_tx, _ack_rx) = watch::channel(None);
        let (load_done_tx, _load_done_rx) = watch::channel(0u64);
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let ctx = Arc::new(PipelineCtx {
            sink: sink.clone(),
            mapping: TableMapping::from_pairs([(
                ("public".to_string(), "users".to_string()),
                "users".to_string(),
            )]),
            projections: crate::mapping::Projections::default(),
            transforms: crate::mapping::Transforms::default(),
            renames,
            id_templates: ids,
            fan_outs: fan,
            cfg: EngineConfig {
                batch_size,
                checkpoint_interval_ms: 100,
                ..EngineConfig::default()
            },
            ack_tx,
            load_done_tx,
            metrics: Arc::new(crate::metrics::Metrics::default()),
        });
        let stream = StreamId {
            source: SOURCE_POSTGRES.into(),
            stream: "slot".into(),
            publication: "pub".into(),
        };
        let render: PositionRenderer = Arc::new(|token| Lsn(token).to_string());
        for event in script {
            events_tx.send(event).await.expect("channel has room");
        }
        let engine = tokio::spawn(run(
            events_rx,
            copy_rx,
            ctx,
            stream,
            render,
            crate::mapping::DurableLsn::default(),
            shutdown_rx,
        ));
        drop(events_tx);
        engine.await.expect("task joined").expect("engine ran");
        sink
    }

    fn users_ids(spec: &str, pk_columns: &[&str]) -> crate::mapping::IdTemplates {
        let pk: Vec<String> = pk_columns.iter().map(|s| s.to_string()).collect();
        crate::mapping::IdTemplates::from_pairs([(
            ("public".to_string(), "users".to_string()),
            crate::mapping::IdTemplate::parse(spec, &pk).expect("valid template"),
        )])
    }

    fn users_fan(field: &str, spec: &str) -> crate::mapping::FanOuts {
        crate::mapping::FanOuts::from_pairs([(
            ("public".to_string(), "users".to_string()),
            crate::mapping::FanOut {
                field: field.into(),
                id: crate::mapping::IdTemplate::parse(spec, &[]).expect("valid template"),
            },
        )])
    }

    fn row_doc(id: i64, doc: Value) -> ChangeEvent {
        ChangeEvent::Row(RowChange {
            schema: "public".into(),
            table: "users".into(),
            kind: RowKind::Insert { pk: json!(id), doc },
            version: None,
        })
    }

    fn users_renames(pairs: &[(&str, &str)]) -> crate::mapping::Renames {
        crate::mapping::Renames::from_pairs([(
            ("public".to_string(), "users".to_string()),
            crate::mapping::Rename {
                columns: pairs
                    .iter()
                    .map(|(from, to)| (from.to_string(), to.to_string()))
                    .collect(),
                nested: Default::default(),
            },
        )])
    }

    #[tokio::test]
    async fn a_renamed_field_is_written_under_its_target_name() {
        let sink = drive_rules_with(
            users_ids("user-{id}", &["id"]),
            crate::mapping::FanOuts::default(),
            users_renames(&[("email", "contact")]),
            vec![
                row_doc(7, json!({"id": 7, "email": "a@x.io"})),
                commit(0x100),
            ],
        )
        .await;
        assert_eq!(
            sink.events(),
            vec!["write[upsert:user-7]"],
            "identity is untouched"
        );
        assert_eq!(
            sink.doc("user-7"),
            Some(json!({"id": 7, "contact": "a@x.io"})),
            "the document carries the target name and not the source one"
        );
    }

    #[tokio::test]
    async fn toast_completion_reads_the_stored_document_by_its_target_name() {
        // the target holds the column under its renamed key; a completion that
        // looked it up by the source name would find nothing and write null
        let sink = Arc::new(RecordingSink::default());
        sink.store(json!({"about": "stored"}));
        let sink = drive_rules_at(
            500,
            crate::mapping::IdTemplates::default(),
            crate::mapping::FanOuts::default(),
            users_renames(&[("bio", "about")]),
            sink,
            vec![moved(1, 2, &["bio"]), commit(0x900)],
        )
        .await;
        assert_eq!(sink.events(), vec!["read(1)", "write[upsert:2 delete:1]"]);
        assert_eq!(
            sink.doc("2"),
            Some(json!({"id": 2, "about": "stored"})),
            "the unchanged column is completed and stored under its target name again"
        );
    }

    #[tokio::test]
    async fn a_scalar_fan_out_element_can_be_renamed_too() {
        let sink = drive_rules_with(
            users_ids("user-{id}", &["id"]),
            users_fan("tags", "user-{id}-{tags}"),
            users_renames(&[("tags", "tag")]),
            vec![row_doc(7, json!({"id": 7, "tags": ["a"]})), commit(0x100)],
        )
        .await;
        assert_eq!(sink.events(), vec!["write[upsert:user-7-a]"]);
        assert_eq!(
            sink.doc("user-7-a"),
            Some(json!({"id": 7, "tag": "a"})),
            "fan-out lands the element under the source name, the rename moves it after"
        );
    }

    #[tokio::test]
    async fn a_configured_id_is_rendered_from_the_row_before_projection() {
        let sink = drive_rules(
            users_ids("user-{id}", &["id"]),
            Default::default(),
            vec![
                row_doc(7, json!({"id": 7, "email": "a@x.io"})),
                commit(0x100),
            ],
        )
        .await;
        assert_eq!(sink.events(), vec!["write[upsert:user-7]"]);
    }

    #[tokio::test]
    async fn a_row_without_an_id_config_is_filed_under_its_key_alone() {
        // the regression that keeps every existing index byte-identical: the
        // default derivation is untouched by the feature
        let sink = drive_rules(
            Default::default(),
            Default::default(),
            vec![row_doc(7, json!({"id": 7})), commit(0x100)],
        )
        .await;
        assert_eq!(sink.events(), vec!["write[upsert:7]"]);
    }

    #[tokio::test]
    async fn a_null_in_an_id_column_halts_the_pipeline() {
        let (events_tx, events_rx) = mpsc::channel(4);
        let (_copy_tx, copy_rx) = mpsc::channel(4);
        let (ack_tx, _ack_rx) = watch::channel(None);
        let (load_done_tx, _load_done_rx) = watch::channel(0u64);
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let sink = Arc::new(RecordingSink::default());
        let ctx = Arc::new(PipelineCtx {
            sink,
            mapping: TableMapping::from_pairs([(
                ("public".to_string(), "users".to_string()),
                "users".to_string(),
            )]),
            projections: crate::mapping::Projections::default(),
            transforms: crate::mapping::Transforms::default(),
            renames: crate::mapping::Renames::default(),
            id_templates: users_ids("user-{tenant}", &["tenant"]),
            fan_outs: Default::default(),
            cfg: EngineConfig::default(),
            ack_tx,
            load_done_tx,
            metrics: Arc::new(crate::metrics::Metrics::default()),
        });
        events_tx
            .send(row_doc(7, json!({"id": 7, "tenant": null})))
            .await
            .unwrap();
        let error = run(
            events_rx,
            copy_rx,
            ctx,
            StreamId {
                source: SOURCE_POSTGRES.into(),
                stream: "slot".into(),
                publication: "pub".into(),
            },
            Arc::new(|token| Lsn(token).to_string()),
            crate::mapping::DurableLsn::default(),
            shutdown_rx,
        )
        .await
        .expect_err("a NULL where an id needs a value must stop the pipeline");
        let message = error.to_string();
        assert!(
            message.contains("public.users")
                && message.contains("tenant")
                && message.contains("NULL"),
            "the halt must name the table and the column: {message}"
        );
        drop(events_tx);
    }

    #[tokio::test]
    async fn a_key_only_id_renders_deletes_from_the_key_alone() {
        // Phase 1's promise: no before-image needed when the id names only
        // the key, because a delete's key carries everything there is
        let delete = ChangeEvent::Row(RowChange {
            schema: "public".into(),
            table: "users".into(),
            kind: RowKind::Delete {
                pk: json!(42),
                before: None,
            },
            version: Some(0x100),
        });
        let sink = drive_rules(
            users_ids("user-{id}", &["id"]),
            Default::default(),
            vec![delete, commit(0x100)],
        )
        .await;
        assert_eq!(sink.events(), vec!["write[delete:user-42]"]);
    }

    #[tokio::test]
    async fn a_delete_outside_the_key_renders_from_the_before_image() {
        let delete = ChangeEvent::Row(RowChange {
            schema: "public".into(),
            table: "users".into(),
            kind: RowKind::Delete {
                pk: json!(42),
                before: Some(json!({"id": 42, "tenant": "acme"})),
            },
            version: Some(0x100),
        });
        let sink = drive_rules(
            users_ids("{tenant}-user-{id}", &["id"]),
            Default::default(),
            vec![delete, commit(0x100)],
        )
        .await;
        assert_eq!(sink.events(), vec!["write[delete:acme-user-42]"]);
    }

    #[tokio::test]
    async fn an_update_that_moves_a_derived_id_removes_the_document_it_left() {
        // the id is derived from a column outside the key, so only the
        // before-image says what the row used to be called
        let update = ChangeEvent::Row(RowChange {
            schema: "public".into(),
            table: "users".into(),
            kind: RowKind::Update {
                pk: json!(7),
                previous_pk: None,
                doc: json!({"id": 7, "tenant": "globex"}),
                unchanged_toast_columns: vec![],
                before: Some(json!({"id": 7, "tenant": "acme"})),
            },
            version: Some(0x200),
        });
        let sink = drive_rules(
            users_ids("{tenant}-user-{id}", &["id"]),
            Default::default(),
            vec![update, commit(0x200)],
        )
        .await;
        assert_eq!(
            sink.events(),
            vec!["write[upsert:globex-user-7@512 delete:acme-user-7]"],
            "write the new name first, remove the old one second"
        );
    }

    #[tokio::test]
    async fn a_key_only_template_renders_the_same_id_with_or_without_the_before_image() {
        // the before-image is preferred wherever it exists, so a key-only id
        // must not depend on which one it rendered from: the regression is
        // that the two derivations agree, and that neither invents a new name
        // when an unrelated column changes
        let delete = ChangeEvent::Row(RowChange {
            schema: "public".into(),
            table: "users".into(),
            kind: RowKind::Delete {
                pk: json!(42),
                before: Some(json!({"id": 42, "email": "gone@x.io"})),
            },
            version: Some(0x100),
        });
        let sink = drive_rules(
            users_ids("user-{id}", &["id"]),
            Default::default(),
            vec![delete, commit(0x100)],
        )
        .await;
        assert_eq!(sink.events(), vec!["write[delete:user-42]"]);
    }

    #[tokio::test]
    async fn a_fanned_row_becomes_one_document_per_element() {
        let sink = drive_rules(
            users_ids("user-{id}", &["id"]),
            users_fan("tags", "user-{id}-{tags}"),
            vec![
                row_doc(7, json!({"id": 7, "tags": ["a", "b"]})),
                commit(0x100),
            ],
        )
        .await;
        assert_eq!(
            sink.events(),
            vec!["write[upsert:user-7-a upsert:user-7-b]"]
        );
    }

    #[tokio::test]
    async fn a_fanned_update_removes_dropped_elements_and_keeps_the_rest() {
        let update = ChangeEvent::Row(RowChange {
            schema: "public".into(),
            table: "users".into(),
            kind: RowKind::Update {
                pk: json!(7),
                previous_pk: None,
                doc: json!({"id": 7, "tags": ["a", "c"]}),
                unchanged_toast_columns: vec![],
                before: Some(json!({"id": 7, "tags": ["a", "b"]})),
            },
            version: Some(0x200),
        });
        let sink = drive_rules(
            users_ids("user-{id}", &["id"]),
            users_fan("tags", "user-{id}-{tags}"),
            vec![update, commit(0x200)],
        )
        .await;
        assert_eq!(
            sink.events(),
            vec!["write[upsert:user-7-a@512 upsert:user-7-c@512 delete:user-7-b]"],
            "kept and new elements are written, the dropped one is removed"
        );
    }

    #[tokio::test]
    async fn a_fanned_delete_removes_every_element_document() {
        let delete = ChangeEvent::Row(RowChange {
            schema: "public".into(),
            table: "users".into(),
            kind: RowKind::Delete {
                pk: json!(7),
                before: Some(json!({"id": 7, "tags": ["a", "b"]})),
            },
            version: Some(0x300),
        });
        let sink = drive_rules(
            users_ids("user-{id}", &["id"]),
            users_fan("tags", "user-{id}-{tags}"),
            vec![delete, commit(0x300)],
        )
        .await;
        assert_eq!(
            sink.events(),
            vec!["write[delete:user-7-a delete:user-7-b]"]
        );
    }

    #[tokio::test]
    async fn a_fanned_row_with_a_null_array_still_owns_one_document() {
        let sink = drive_rules(
            users_ids("user-{id}", &["id"]),
            users_fan("tags", "user-{id}-{tags}"),
            vec![row_doc(7, json!({"id": 7, "tags": null})), commit(0x100)],
        )
        .await;
        assert_eq!(sink.events(), vec!["write[upsert:user-7]"]);
    }

    #[tokio::test]
    async fn a_fanned_update_that_empties_the_array_leaves_nothing_behind() {
        let update = ChangeEvent::Row(RowChange {
            schema: "public".into(),
            table: "users".into(),
            kind: RowKind::Update {
                pk: json!(7),
                previous_pk: None,
                doc: json!({"id": 7, "tags": []}),
                unchanged_toast_columns: vec![],
                before: Some(json!({"id": 7, "tags": ["a"]})),
            },
            version: Some(0x200),
        });
        let sink = drive_rules(
            users_ids("user-{id}", &["id"]),
            users_fan("tags", "user-{id}-{tags}"),
            vec![update, commit(0x200)],
        )
        .await;
        assert_eq!(sink.events(), vec!["write[delete:user-7-a]"]);
    }
}
