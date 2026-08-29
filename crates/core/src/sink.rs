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
        /// Which shard holds this document. `None` is the target's own rule,
        /// the document's id — which is what every document that is not a
        /// join child uses, so nothing about an ordinary pipeline changes.
        routing: Option<String>,
        doc: Value,
        /// Source position this version of the row became visible at, used as
        /// the target's external document version. `None` where the source has
        /// no position to offer — poll mode, and the initial load until it
        /// carries one.
        ///
        /// It is deliberately separate from `LsnOp::lsn`: that one is the
        /// position that may be acknowledged, and an initial-load row must
        /// never advance it while still needing a version.
        version: Option<u64>,
    },
    Delete {
        index: String,
        id: String,
        routing: Option<String>,
        version: Option<u64>,
    },
    /// Every child document of one parent, removed at the parent's own
    /// position, after the parent itself.
    ///
    /// Not a bulk action: the children have to be found before they can be
    /// deleted, which is why this is an operation of its own rather than a
    /// list of ids the engine could have built — the engine does not know
    /// which children the target holds.
    DeleteChildren {
        index: String,
        /// The join field the relation is declared in.
        field: String,
        /// The *parent's* relation name. The children are identified through
        /// it rather than by naming each child type, because one parent may
        /// have several and the sink is told about none of them.
        parent_name: String,
        /// The parent document's id, which is also the routing.
        parent_id: String,
        version: Option<u64>,
    },
}

/// What came back from a `write`: how far it got, and what the target refused.
#[derive(Debug, Clone, PartialEq)]
pub struct SinkAck {
    /// Highest LSN known to be durably written.
    pub max_lsn: Lsn,
    /// Documents the target will never accept. Reported rather than raised so
    /// the caller can decide between halting and quarantining — and so that
    /// *every* rejection in a batch is visible, not just the first.
    pub rejected: Vec<Rejection>,
}

impl SinkAck {
    /// The ordinary outcome: everything in the batch was accepted.
    pub fn written(max_lsn: Lsn) -> Self {
        Self {
            max_lsn,
            rejected: Vec::new(),
        }
    }
}

/// One document the target refused permanently, with everything needed to
/// record it and to submit it again later.
#[derive(Debug, Clone, PartialEq)]
pub struct Rejection {
    pub index: String,
    pub doc_id: String,
    pub reason: String,
    /// Where in the source this document came from. Recorded so a quarantined
    /// document can still be accounted for against a position.
    pub lsn: Lsn,
    /// The operation itself, which is what makes a replay possible at all.
    pub op: DocumentOp,
}

/// A rejection read back out of the target's store, with the id it is filed
/// under so it can be cleared once replayed.
#[derive(Debug, Clone, PartialEq)]
pub struct StoredReject {
    pub id: String,
    pub rejection: Rejection,
    /// When it was quarantined, epoch seconds, as the target recorded it.
    pub at_epoch: u64,
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

    /// Fetch current documents by `(id, routing)`. Used by the engine to
    /// complete documents containing unchanged-TOAST markers. A join child
    /// lives on its parent's shard, so reading it back needs the routing the
    /// write used.
    async fn get_documents(
        &self,
        index: &str,
        ids: &[(String, Option<String>)],
    ) -> Result<Vec<Option<Value>>, CoreError>;

    /// Write a batch. On success the ack reports the highest LSN in the batch;
    /// the engine may only checkpoint up to that LSN.
    async fn write(&self, batch: Vec<LsnOp>) -> Result<SinkAck, CoreError>;

    /// Clear all documents of an index after a source-side TRUNCATE.
    ///
    /// `version` is the position the truncate happened at. A target that
    /// versions its documents must clear them *at* that position: anything
    /// written before it has to lose, and anything written after it has to
    /// survive, including a row re-inserted moments later.
    ///
    /// `only` narrows the clear to documents whose `field` equals `value` —
    /// how one relation of a join pair is cleared without touching the other
    /// half of the index it shares. Kept as a field/value pair rather than a
    /// query so the caller never writes the target's query language.
    async fn truncate_index(
        &self,
        index: &str,
        version: Option<u64>,
        only: Option<(&str, &str)>,
    ) -> Result<(), CoreError>;

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
    async fn begin_bulk_load(&self, _indices: &[String]) -> Result<BulkLoadSettings, CoreError> {
        Ok(BulkLoadSettings::default())
    }

    /// Put back what `begin_bulk_load` set aside.
    async fn end_bulk_load(&self, _saved: &BulkLoadSettings) -> Result<(), CoreError> {
        Ok(())
    }

    /// One page of `(document id, key value, routing)` from an index, ordered
    /// by `key_field`, starting after `after`. `only` narrows the page to
    /// documents whose `field` equals `value` — how one table's documents are
    /// picked out of an index two tables share. Kept as a field/value pair
    /// rather than a query so the caller never writes the target's query
    /// language.
    ///
    /// Used to walk an index against its source. Targets that cannot page an
    /// index in key order say so rather than returning a partial answer, since
    /// a caller acting on this deletes things.
    async fn scan_keys(
        &self,
        _index: &str,
        _key_field: &str,
        _only: Option<(&str, &str)>,
        _after: Option<&Value>,
        _size: usize,
    ) -> Result<Vec<(String, Value, Option<String>)>, CoreError> {
        Err(CoreError::Sink(
            "this target cannot page an index in key order".into(),
        ))
    }

    /// Point `alias` at `index`, removing it from wherever it was, in one
    /// step.
    ///
    /// The swap has to be atomic: a reader that resolves the alias between a
    /// remove and an add gets an error, and this is the moment a zero-downtime
    /// reindex exists to avoid.
    async fn switch_alias(&self, _alias: &str, _index: &str) -> Result<(), CoreError> {
        Err(CoreError::Sink(
            "this target has no aliases to switch".into(),
        ))
    }

    /// Whether this target decides between two writes of one document by the
    /// version they carry rather than by which arrived last.
    ///
    /// This is what makes more than one write request safe to have open at
    /// once: two in-flight requests can land in either order, and only a target
    /// that compares versions still ends up holding the later document. A
    /// target without versions would keep whichever request happened to finish
    /// second, so concurrency is refused there at startup instead of quietly
    /// reordering writes.
    fn orders_by_version(&self) -> bool {
        true
    }

    /// Whether this target can durably record a rejected document.
    ///
    /// Checked at startup so a pipeline configured to quarantine against a
    /// target that cannot fails immediately rather than at the first bad
    /// document.
    fn can_quarantine(&self) -> bool {
        false
    }

    /// Record documents the target refused, durably, before their position is
    /// acknowledged.
    ///
    /// The default is an error and not success on purpose: a silent no-op here
    /// is indistinguishable from losing the document, which is the whole failure
    /// this exists to prevent.
    async fn quarantine(&self, _rejected: &[Rejection]) -> Result<(), CoreError> {
        Err(CoreError::Sink(
            "this target cannot record a rejected document; \
             set on_permanent_rejection = \"halt\""
                .into(),
        ))
    }

    /// Quarantined documents, newest first, with the total held.
    ///
    /// The total comes back with the page so a caller can bound itself against
    /// the whole store without asking twice.
    async fn list_rejects(&self, _limit: usize) -> Result<(Vec<StoredReject>, u64), CoreError> {
        Err(CoreError::Sink(
            "this target holds no quarantined documents".into(),
        ))
    }

    /// Forget one quarantined document, once it has been dealt with.
    async fn clear_reject(&self, _id: &str) -> Result<(), CoreError> {
        Err(CoreError::Sink(
            "this target holds no quarantined documents".into(),
        ))
    }

    /// Read a small named state document from the target.
    ///
    /// Initial-load progress lives here for the reason the checkpoint does:
    /// only the target is visible to both the process that stopped and the one
    /// that takes over. A target with nowhere to keep it answers `None`, which
    /// costs a reload and never a gap.
    async fn read_state(&self, _key: &str) -> Result<Option<Value>, CoreError> {
        Ok(None)
    }

    /// Persist a named state document durably.
    async fn write_state(&self, _key: &str, _doc: &Value) -> Result<(), CoreError> {
        Ok(())
    }

    /// Remove a named state document; absent is success.
    async fn clear_state(&self, _key: &str) -> Result<(), CoreError> {
        Ok(())
    }

    /// Persist the pipeline checkpoint durably.
    async fn write_checkpoint(&self, checkpoint: &Checkpoint) -> Result<(), CoreError>;

    /// Read the last persisted checkpoint for one stream; None before the
    /// first persist.
    async fn read_checkpoint(
        &self,
        stream: &crate::checkpoint::StreamId,
    ) -> Result<Option<Checkpoint>, CoreError>;

    /// Cheap health probe for /status and reconnect logic.
    async fn health(&self) -> Result<Health, CoreError>;
}
