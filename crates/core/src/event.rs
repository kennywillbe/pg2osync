//! The change-event vocabulary shared by source and engine.
//!
//! These are the ONLY types the engine uses to understand what happened in
//! the source database. Nothing here mentions PostgreSQL specifics — table
//! identity is a logical (schema, name) pair resolved by the source crate.

use crate::lsn::Lsn;
use serde_json::Value;

/// Boundary signals emitted around committed transactions.
///
/// The engine must treat rows as invisible until their `Commit` arrives:
/// buffering until COMMIT is a correctness invariant, not an optimization.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionBoundary {
    /// `lsn` is the position this transaction will commit at, which pgoutput
    /// reports up front. It marks the transaction open: while one is, nothing
    /// may flush a batch, or a concurrent producer's boundary would split it.
    Begin { lsn: Lsn },
    /// `lsn` is the commit LSN; it is the highest position that may be
    /// acknowledged to the source once this transaction's rows are durable.
    /// `commit_ts_micros` is microseconds since 2000-01-01 (PG epoch), or 0
    /// when unavailable (backfill boundaries).
    Commit { lsn: Lsn, commit_ts_micros: i64 },
}

/// A single committed row change.
#[derive(Debug, Clone, PartialEq)]
pub struct RowChange {
    pub schema: String,
    pub table: String,
    pub kind: RowKind,
    /// The source position at which this change became visible, carried on the
    /// row rather than inferred from the surrounding boundaries: two producers
    /// may feed the engine at once, so ambient state would attribute one's
    /// position to the other's rows. Deliberately not the checkpoint token —
    /// the target versions documents by it, nothing acknowledges it.
    pub version: Option<u64>,
}

impl RowChange {
    pub fn pk(&self) -> &Value {
        match &self.kind {
            RowKind::Insert { pk, .. }
            | RowKind::Update { pk, .. }
            | RowKind::Delete { pk, .. } => pk,
        }
    }

    /// Mutable access to the document for source-side enrichment (nested
    /// children). Deletes have no document.
    pub fn doc_mut(&mut self) -> Option<&mut Value> {
        match &mut self.kind {
            RowKind::Insert { doc, .. } | RowKind::Update { doc, .. } => Some(doc),
            RowKind::Delete { .. } => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum RowKind {
    Insert {
        pk: Value,
        doc: Value,
    },
    /// `unchanged_toast_columns` lists columns whose values PG omitted because
    /// they are large, externally stored and unchanged since the last write
    /// The engine must complete them from the previously indexed
    /// document before writing; until then `doc` is partial.
    Update {
        pk: Value,
        /// The key this row had before the update, when the source knows it.
        /// `None` means unknown, not unchanged — a source that cannot observe
        /// the old row reports `None` and moves go undetected there.
        previous_pk: Option<Value>,
        doc: Value,
        unchanged_toast_columns: Vec<String>,
        /// The row's before-image document, when the source can provide one
        /// (PostgreSQL under REPLICA IDENTITY FULL, MySQL under
        /// binlog_row_image = FULL). A derived document id that references
        /// columns outside the key is rendered from it, which is what lets an
        /// update that moved the id delete the document the row used to own.
        before: Option<Value>,
    },
    Delete {
        pk: Value,
        /// The row's before-image where the source carries one; the same role
        /// as on `Update`, and required for deleting derived ids.
        before: Option<Value>,
    },
}

/// Everything the engine consumes. Deliberately tiny; sources grow, engine does not.
#[derive(Debug, Clone, PartialEq)]
pub enum ChangeEvent {
    Transaction(TransactionBoundary),
    Row(RowChange),
    /// A request from the initial load to be told, through the pipeline's load
    /// channel, when everything sent before it is durably written. Progress is
    /// only recorded behind one, so a crash loses forward progress and never
    /// claims a range that was not written.
    LoadMark(u64),
    /// A load is about to send rows down this channel.
    ///
    /// Sent before the first of them, because what it opens is the window in
    /// which the engine remembers what the stream has removed: a copied row
    /// starved behind a busy stream must lose to a delete that came after it,
    /// and the engine can only know to remember that delete if it already
    /// knows a copied row may be on its way.
    LoadStarted,
    /// The load has sent everything it is going to send.
    ///
    /// The copy channel closing used to say this, and cannot any more: the
    /// channel stays open for the streaming attempt's life so a reload can
    /// read a table added to a running pipeline down it. This says the load is
    /// over without saying the channel is.
    LoadFinished,
    /// A source table whose columns changed under the running pipeline, with
    /// `detail` naming what was added, removed or retyped.
    ///
    /// It carries no position and no data on purpose: applying the change is
    /// refused, so the engine counts it, says so and drops it. Nothing about
    /// the stream may advance on one.
    SchemaDrift {
        schema: String,
        table: String,
        detail: String,
    },
    /// TRUNCATE on a source table; target index content must be cleared.
    /// `version` is the position it happened at, as for a row.
    TableTruncated {
        schema: String,
        table: String,
        version: Option<u64>,
    },
}
