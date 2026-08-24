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
    Begin,
    /// `lsn` is the commit LSN; it is the highest position that may be
    /// acknowledged to the source once this transaction's rows are durable.
    /// `commit_ts_micros` is microseconds since 2000-01-01 (PG epoch), or 0
    /// when unavailable (backfill boundaries).
    Commit {
        lsn: Lsn,
        commit_ts_micros: i64,
    },
}

/// A single committed row change.
#[derive(Debug, Clone, PartialEq)]
pub struct RowChange {
    pub schema: String,
    pub table: String,
    pub kind: RowKind,
}

impl RowChange {
    pub fn pk(&self) -> &Value {
        match &self.kind {
            RowKind::Insert { pk, .. } | RowKind::Update { pk, .. } | RowKind::Delete { pk } => pk,
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
    },
    Delete {
        pk: Value,
    },
}

/// Everything the engine consumes. Deliberately tiny; sources grow, engine does not.
#[derive(Debug, Clone, PartialEq)]
pub enum ChangeEvent {
    Transaction(TransactionBoundary),
    Row(RowChange),
    /// TRUNCATE on a source table; target index content must be cleared.
    TableTruncated {
        schema: String,
        table: String,
    },
}
