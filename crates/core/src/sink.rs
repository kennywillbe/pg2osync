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
        /// The target's ingest pipeline this document goes through, named by
        /// the section that produced it. Carried on the operation rather than
        /// looked up by index because two sections can share one index and
        /// still want different pipelines — and because a quarantined
        /// document has to be replayed through the same one.
        pipeline: Option<String>,
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
    /// An index name, or — when `pattern` — the glob the rows of a templated
    /// table render into.
    pub name: String,
    /// Mapping to create the index with, when the operator configured one.
    /// Never applied to an index that already exists: changing a mapping in
    /// place is refused by the target for anything that matters, and doing it
    /// implicitly would be a reindex nobody asked for.
    pub mapping: Option<Value>,
    /// Whether `name` is a glob rather than an index. `ensure_ready` creates
    /// nothing for one; it records the glob and the mapping so a name a row
    /// renders can be created when the first document for it is written.
    pub pattern: bool,
}

/// Whether `name` is one of the indices `pattern` claims. `*` stands for any
/// run of characters; a pattern with no `*` matches only itself, so a fixed
/// index is the same rule rather than a second branch.
pub fn index_matches_pattern(pattern: &str, name: &str) -> bool {
    let Some((head, tail)) = pattern.split_once('*') else {
        return pattern == name;
    };
    let Some(mut rest) = name.strip_prefix(head) else {
        return false;
    };
    // Each literal between two stars is matched at its leftmost occurrence:
    // whatever a later, further-right match would leave for the star before
    // it, the star after it can absorb instead, so nothing is lost by being
    // greedy. The last literal has no star after it and must end the name.
    let mut literals = tail.split('*').peekable();
    while let Some(literal) = literals.next() {
        if literals.peek().is_none() {
            return rest.ends_with(literal);
        }
        let Some(at) = rest.find(literal) else {
            return false;
        };
        rest = &rest[at + literal.len()..];
    }
    true
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
    ///
    /// `index` may be a glob for a templated table — the `pattern` of its
    /// `IndexSpec`, covering every index its rows rendered — since the source
    /// truncated the table, not one of the indices. A target that cannot
    /// resolve a glob refuses templated specs in `ensure_ready` instead of
    /// clearing the wrong thing here.
    async fn truncate_index(
        &self,
        index: &str,
        version: Option<u64>,
        only: Option<(&str, &str)>,
    ) -> Result<(), CoreError>;

    /// Take a retry budget the operator changed while the pipeline was
    /// running.
    ///
    /// Default is to ignore it: a target whose client does not retry — or
    /// retries on its own terms — has nothing here to change, and saying so by
    /// doing nothing is better than a second knob that means nothing.
    fn set_retry_policy(
        &self,
        _max_attempts: u32,
        _base_backoff_ms: u64,
        _max_elapsed_ms: Option<u64>,
    ) {
    }

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

    /// How many documents `index` holds, or `None` when there is no such
    /// index.
    ///
    /// `None` rather than zero because the two answers lead different places:
    /// an index that was never created is a mistake to report, and an empty
    /// one is a count to compare.
    async fn count_documents(&self, _index: &str) -> Result<Option<u64>, CoreError> {
        Err(CoreError::Sink(
            "this target cannot count the documents in an index".into(),
        ))
    }

    /// Whether an index of this name is already there.
    ///
    /// Asked before a rebuild names one: finding the collision after a
    /// multi-hour load would throw the load away.
    async fn index_exists(&self, _name: &str) -> Result<bool, CoreError> {
        Err(CoreError::Sink(
            "this target cannot say whether an index exists".into(),
        ))
    }

    /// Remove an index and everything in it; absent is success.
    ///
    /// Only ever called on an index the caller named explicitly — the old one
    /// a rebuild replaced — never inferred from a pattern.
    async fn delete_index(&self, _name: &str) -> Result<(), CoreError> {
        Err(CoreError::Sink("this target cannot delete an index".into()))
    }

    /// Point `alias` at `index`, removing it from wherever it was, in one
    /// step.
    ///
    /// The swap has to be atomic: a reader that resolves the alias between a
    /// remove and an add gets an error, and this is the moment a zero-downtime
    /// reindex exists to avoid.
    ///
    /// What is contracted is that outcome, not the mechanism. A target with an
    /// alias namespace moves a pointer inside it. A target without one — where
    /// the name readers use *is* an index — reaches the same end by exchanging
    /// the contents of the two names, which leaves `index` holding what `alias`
    /// held. So a caller keeping the previous documents as a rollback must look
    /// for them under whichever name the target left them in, and must not
    /// assume `index` still holds what was just written into it.
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

    /// Whether the target has an ingest pipeline of this name, so `validate`
    /// can refuse a configuration naming one that does not exist before the
    /// first batch is refused for it.
    ///
    /// The default says yes: a target with no ingest pipelines never gets a
    /// configured one past config load, so it is never asked.
    async fn has_pipeline(&self, _name: &str) -> Result<bool, CoreError> {
        Ok(true)
    }

    /// Whether `name` is an alias rather than an index of that name.
    ///
    /// Asked by `validate` when `require_alias` is set, so a section still
    /// pointing at the raw index a rebuild replaced is named before the first
    /// batch is refused for it rather than after.
    ///
    /// The default says yes for the reason `has_pipeline`'s does: a target
    /// with no alias namespace never gets `require_alias` past config load, so
    /// it is never asked.
    async fn is_alias(&self, _name: &str) -> Result<bool, CoreError> {
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::index_matches_pattern;

    #[test]
    fn a_pattern_without_a_star_matches_only_itself() {
        assert!(index_matches_pattern("events", "events"));
        assert!(!index_matches_pattern("events", "events-1"));
        assert!(!index_matches_pattern("events-1", "events"));
    }

    #[test]
    fn a_trailing_star_claims_every_name_with_the_prefix() {
        assert!(index_matches_pattern("events-*", "events-acme"));
        assert!(index_matches_pattern("events-*", "events-"));
        assert!(!index_matches_pattern("events-*", "orders-acme"));
    }

    #[test]
    fn a_leading_star_claims_every_name_with_the_suffix() {
        assert!(index_matches_pattern("*-events", "acme-events"));
        assert!(!index_matches_pattern("*-events", "acme-orders"));
    }

    #[test]
    fn a_star_in_the_middle_claims_a_prefix_and_a_suffix_at_once() {
        assert!(index_matches_pattern(
            "events-*-archive",
            "events-2024-archive"
        ));
        assert!(index_matches_pattern("*-*-x", "a-b-c-x"));
        assert!(!index_matches_pattern(
            "events-*-archive",
            "events-2024-live"
        ));
        assert!(!index_matches_pattern(
            "events-*-archive",
            "orders-2024-archive"
        ));
    }

    #[test]
    fn a_lone_star_claims_everything() {
        assert!(index_matches_pattern("*", "anything"));
        assert!(index_matches_pattern("*", ""));
    }

    #[test]
    fn a_name_shorter_than_the_literals_is_not_claimed() {
        assert!(!index_matches_pattern("events-*", "event"));
        assert!(!index_matches_pattern("events-*-archive", "events-"));
        // the two literals may not share the one character the name has
        assert!(!index_matches_pattern("a*a", "a"));
    }
}
