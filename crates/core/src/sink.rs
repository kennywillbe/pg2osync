//! The `Sink` trait and its data types.
//!
//! Contract (Liskov): every implementation must preserve
//! - idempotent upserts by document `_id`
//! - at-least-once delivery of every `LsnOp` accepted into `write`
//! - acks only for operations that are durably visible to search

use crate::checkpoint::Checkpoint;
use crate::error::CoreError;
use crate::position::Position;
use serde_json::Value;

/// A document operation paired with the source position it came from.
#[derive(Debug, Clone, PartialEq)]
pub struct LsnOp {
    /// `None` for an operation that may not be acknowledged: a row with no
    /// position of its own, or one inside a transaction whose commit position
    /// rides on its last operation only.
    pub position: Option<Position>,
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
        /// It is deliberately separate from `LsnOp::position`: that one is the
        /// position that may be acknowledged, and an initial-load row must
        /// never advance it while still needing a version.
        version: Option<u64>,
        /// The target's ingest pipeline this document goes through, named by
        /// the section that produced it. Carried on the operation rather than
        /// looked up by index because two sections can share one index and
        /// still want different pipelines — and because a quarantined
        /// document has to be replayed through the same one.
        pipeline: Option<String>,
        /// The alias the index should carry if this write is the one that
        /// creates it, rendered from the same row as the index. Carried on
        /// the operation because only the row knows the values: the name
        /// alone cannot be split back into them once a template has two
        /// placeholders. An index that already exists is left as it is.
        alias: Option<String>,
    },
    Delete {
        index: String,
        id: String,
        routing: Option<String>,
        version: Option<u64>,
        /// The same as on `Upsert`, and for the same reason: a versioned
        /// delete into a missing index makes the target create that index,
        /// bare, so a delete can be the write that creates one too.
        alias: Option<String>,
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
    /// The batch's last position, once it is durably written; `None` when the
    /// batch ended on an operation that may not be acknowledged.
    pub max_position: Option<Position>,
    /// Documents the target will never accept. Reported rather than raised so
    /// the caller can decide between halting and quarantining — and so that
    /// *every* rejection in a batch is visible, not just the first.
    pub rejected: Vec<Rejection>,
}

impl SinkAck {
    /// The ordinary outcome: everything in the batch was accepted.
    pub fn written(max_position: Option<Position>) -> Self {
        Self {
            max_position,
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
    /// Where in the source this document came from, when it came from a
    /// position at all. Recorded so a quarantined document can still be
    /// accounted for against one.
    pub position: Option<Position>,
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

/// Whether some name is claimed by both `a` and `b`: what two templates have to
/// be compared by, since a row's values can turn either one's placeholder into
/// the other's literals. A side with no `*` is a single name and goes through
/// `index_matches_pattern`. Two sides with a star each overlap exactly when
/// their heads agree as far as the shorter one goes, and so do their tails:
/// the longer head, then every middle literal of both, then the longer tail is
/// a name both match, each star absorbing what its side does not spell out.
pub fn index_patterns_overlap(a: &str, b: &str) -> bool {
    let (Some((a_head, a_tail)), Some((b_head, b_tail))) = (head_and_tail(a), head_and_tail(b))
    else {
        return index_matches_pattern(a, b) || index_matches_pattern(b, a);
    };
    (a_head.starts_with(b_head) || b_head.starts_with(a_head))
        && (a_tail.ends_with(b_tail) || b_tail.ends_with(a_tail))
}

/// The literal before the first `*` and the one after the last, or `None`
/// for a pattern that is a single name.
fn head_and_tail(pattern: &str) -> Option<(&str, &str)> {
    Some((pattern.split_once('*')?.0, pattern.rsplit_once('*')?.1))
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

    /// Write a batch. On success the ack reports the batch's last position;
    /// the engine may only checkpoint up to that position.
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

    /// The open indices `pattern` claims, by `index_matches_pattern`'s rule,
    /// sorted. A pattern that matches nothing is an empty list, not an error.
    ///
    /// How a templated table is walked against its source: its documents are
    /// spread over every index its rows rendered, and only the target knows
    /// which those are. Only names the pattern itself claims are returned —
    /// an alias that happens to match must not bring in an index the
    /// template never wrote — because a caller acting on this deletes things.
    /// Targets that cannot list their indices say so rather than returning a
    /// partial answer.
    async fn list_indices(&self, _pattern: &str) -> Result<Vec<String>, CoreError> {
        Err(CoreError::Sink(
            "this target cannot list the indices a pattern matches".into(),
        ))
    }

    /// Every alias whose name `pattern` claims, by `index_matches_pattern`'s
    /// rule, as one `(alias, index)` pair per index holding it, sorted. A
    /// pattern that matches nothing is an empty list, not an error.
    ///
    /// How a switch of a templated table finds the aliases it leaves where
    /// they are: only the target knows which tenants' aliases exist, and a
    /// tenant missing from the generation switched to keeps its alias on
    /// another one. Re-checked by name for the reason `list_indices` is.
    async fn list_aliases(&self, _pattern: &str) -> Result<Vec<(String, String)>, CoreError> {
        Err(CoreError::Sink(
            "this target cannot list the aliases a pattern matches".into(),
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

    /// Point each alias at exactly the indices it is paired with, in one
    /// atomic step for all of them.
    ///
    /// A templated table has one alias per rendered index, and moving them
    /// one `switch_alias` at a time would leave readers split between two
    /// generations for as long as the loop runs. An alias is removed from
    /// every index that holds it and is not paired with it, never from one it
    /// is also being added to, and an alias paired with several indices ends
    /// up on all of them. The default refuses rather than looping, because a
    /// loop is not atomic and a caller relying on this cannot tell.
    async fn switch_aliases(&self, _pairs: &[(String, String)]) -> Result<(), CoreError> {
        Err(CoreError::Sink(
            "this target cannot switch several aliases in one step".into(),
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

    /// Whether a truncate at a position clears what was written before it and
    /// keeps what was written after.
    ///
    /// Deliberately a different question from `orders_by_version`: recording
    /// the position of every document it holds is enough to clear at one, and
    /// that is a comparison against what is stored rather than a race between
    /// two writes in flight. A target that can do the first and not the second
    /// answers yes here and no there.
    fn truncates_at_a_position(&self) -> bool {
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

    /// The raw position text of a checkpoint 1.x left for `stream`, wherever
    /// 1.x kept it; `None` when there is none, or only a current one.
    ///
    /// Never resumed from: 1.x ordered positions by one source's integer. It is
    /// read so a source can carry over what must not go backwards — a MySQL
    /// version generation — before the first current checkpoint replaces it.
    /// The default, for a target 1.x never stored a checkpoint in, has none.
    async fn read_legacy_position_text(
        &self,
        _stream: &crate::checkpoint::StreamId,
    ) -> Result<Option<String>, CoreError> {
        Ok(None)
    }

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
    use super::{index_matches_pattern, index_patterns_overlap};

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

    #[test]
    fn two_patterns_overlap_when_a_row_could_render_one_name_into_both() {
        for (a, b, overlap) in [
            ("events-*", "*-errors", true),
            ("events-*", "events-*-*", true),
            ("events-*", "events-archive", true),
            ("events", "events", true),
            ("events-*", "orders-*", false),
            ("a-*", "ab-*", false),
            ("*-a", "*-b", false),
            ("events-*", "events", false),
            ("a*a", "a", false),
            ("events", "orders", false),
        ] {
            assert_eq!(index_patterns_overlap(a, b), overlap, "{a} vs {b}");
            assert_eq!(index_patterns_overlap(b, a), overlap, "{b} vs {a}");
        }
    }

    /// The overlap rule against its definition: a name both patterns match,
    /// searched for among every short name. A witness is never longer than
    /// the literals of both patterns together, so the search is complete.
    #[test]
    fn two_patterns_overlap_exactly_when_some_name_matches_both() {
        fn words(alphabet: &[char], max: usize) -> Vec<String> {
            let mut all = vec![String::new()];
            let mut last = vec![String::new()];
            for _ in 0..max {
                last = last
                    .iter()
                    .flat_map(|w| alphabet.iter().map(move |c| format!("{w}{c}")))
                    .collect();
                all.extend(last.iter().cloned());
            }
            all
        }
        let patterns = words(&['a', 'b', '*'], 4);
        let names = words(&['a', 'b'], 8);
        for a in &patterns {
            for b in &patterns {
                let witnessed = names
                    .iter()
                    .any(|n| index_matches_pattern(a, n) && index_matches_pattern(b, n));
                assert_eq!(index_patterns_overlap(a, b), witnessed, "{a:?} vs {b:?}");
            }
        }
    }
}
