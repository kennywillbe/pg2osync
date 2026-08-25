//! The `Sink` trait and its data types.
//!
//! Contract (Liskov): every implementation must preserve
//! - idempotent upserts by document `_id`
//! - at-least-once delivery of every `LsnOp` accepted into `write`
//! - acks only for operations that are durably visible to search

use crate::checkpoint::Checkpoint;
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
    /// Mapping to create the index with, when the operator configured one.
    /// Never applied to an index that already exists: changing a mapping in
    /// place is refused by the target for anything that matters, and doing it
    /// implicitly would be a reindex nobody asked for.
    pub mapping: Option<Value>,
}

/// Index settings put aside while an initial load runs, so they can be put
/// back afterwards. Opaque to the engine: only the sink knows what it saved.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BulkLoadSettings(pub Vec<(String, Option<String>, Option<String>)>);

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

    /// Make everything written so far visible to search.
    ///
    /// A write that the sink has accepted is not necessarily searchable yet;
    /// engines that refresh on an interval need to be asked. Implementations
    /// where acceptance already implies visibility do nothing here.
    async fn refresh(&self, indices: &[String]) -> Result<(), CoreError>;

    /// Relax the settings that only cost during a bulk load, returning what
    /// they were.
    ///
    /// An index that refreshes every second and writes replicas while millions
    /// of rows land is doing work nobody is waiting for: nothing searches an
    /// index that is still being filled. Targets with no such settings do
    /// nothing here.
    async fn begin_bulk_load(
        &self,
        _indices: &[String],
    ) -> Result<BulkLoadSettings, CoreError> {
        Ok(BulkLoadSettings::default())
    }

    /// Put back what `begin_bulk_load` set aside.
    async fn end_bulk_load(&self, _saved: &BulkLoadSettings) -> Result<(), CoreError> {
        Ok(())
    }

    /// One page of `(document id, key value)` from an index, ordered by
    /// `key_field`, starting after `after`.
    ///
    /// Used to walk an index against its source. Targets that cannot page an
    /// index in key order say so rather than returning a partial answer, since
    /// a caller acting on this deletes things.
    async fn scan_keys(
        &self,
        _index: &str,
        _key_field: &str,
        _after: Option<&Value>,
        _size: usize,
    ) -> Result<Vec<(String, Value)>, CoreError> {
        Err(CoreError::Sink(
            "this target cannot page an index in key order".into(),
        ))
    }

    /// Persist the pipeline checkpoint durably.
    async fn write_checkpoint(&self, checkpoint: &Checkpoint) -> Result<(), CoreError>;

    /// Read the last persisted checkpoint; None before the first persist.
    async fn read_checkpoint(&self) -> Result<Option<Checkpoint>, CoreError>;

    /// Cheap health probe for /status and reconnect logic.
    async fn health(&self) -> Result<Health, CoreError>;
}
