//! The `Sink` trait and its data types.
//!
//! Contract (Liskov): every implementation must preserve
//! - idempotent upserts by document `_id`
//! - at-least-once delivery of every `LsnOp` accepted into `write`
//! - acks only for operations that are durably visible to search

use crate::error::CoreError;
use crate::lsn::Lsn;
use serde_json::Value;

/// A document operation paired with the source position it came from.
#[derive(Debug, Clone, PartialEq)]
pub struct LsnOp {
    pub lsn: Lsn,
    pub op: DocumentOp,
}

#[derive(Debug, Clone, PartialEq)]
pub enum DocumentOp {
    Upsert {
        index: String,
        id: String,
        doc: Value,
    },
    Delete {
        index: String,
        id: String,
    },
}

/// Highest LSN known to be durably written after a successful `write`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SinkAck {
    pub max_lsn: Lsn,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Health {
    Up,
    Down(String),
}

/// Static description of one target index; passed before backfill starts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexSpec {
    pub name: String,
}

#[async_trait::async_trait]
pub trait Sink: Send + Sync {
    /// Create/verify indices, mappings, aliases. Called once before backfill.
    async fn ensure_ready(&self, tables: &[IndexSpec]) -> Result<(), CoreError>;

    /// Fetch current documents by id. Used by the engine to complete documents
    /// containing unchanged-TOAST markers.
    async fn get_documents(
        &self,
        index: &str,
        ids: &[String],
    ) -> Result<Vec<Option<Value>>, CoreError>;

    /// Write a batch. On success the ack reports the highest LSN in the batch;
    /// the engine may only checkpoint up to that LSN.
    async fn write(&self, batch: Vec<LsnOp>) -> Result<SinkAck, CoreError>;

    /// Clear all documents of an index after a source-side TRUNCATE.
    async fn truncate_index(&self, index: &str) -> Result<(), CoreError>;

    /// Persist the pipeline checkpoint (source position) durably.
    async fn write_checkpoint(
        &self,
        slot_name: &str,
        publication: &str,
        lsn: Lsn,
    ) -> Result<(), CoreError>;

    /// Read the last persisted checkpoint; None before the first persist.
    async fn read_checkpoint(&self) -> Result<Option<Lsn>, CoreError>;

    /// Cheap health probe for /status and reconnect logic.
    async fn health(&self) -> Result<Health, CoreError>;
}
