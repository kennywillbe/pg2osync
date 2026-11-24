//! The pipeline checkpoint: where a sink records how far the source was read.
//!
//! The engine orders positions by their bytes and never interprets them, so
//! one checkpoint document serves every source kind. `position` is the source's
//! own textual form — the only value a source needs to resume, and the place for
//! anything that has no byte order, like a GTID set — and `source` tells the
//! resuming process how to parse it.

use crate::position::Position;

/// Source-kind discriminator stored in the checkpoint document.
pub const SOURCE_POSTGRES: &str = "postgres";
pub const SOURCE_MYSQL: &str = "mysql";

/// Immutable identity of the stream a checkpoint belongs to.
///
/// A checkpoint written for a different stream must never be used to resume:
/// the position space of a PostgreSQL slot and a MySQL binlog are unrelated.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamId {
    /// `postgres` or `mysql`.
    pub source: String,
    /// PostgreSQL: replication slot name. MySQL: `server_id`.
    pub stream: String,
    /// PostgreSQL: publication name. MySQL: empty.
    pub publication: String,
}

/// One persisted checkpoint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Checkpoint {
    pub stream: StreamId,
    /// What the engine compares: the source's position as ordered bytes.
    pub ordering: Position,
    /// Source-specific textual position (`"0/1B4F2A8"`, `"binlog.000004:1234"`).
    pub position: String,
}

/// Proof that a pipeline is running a stream, kept beside its checkpoint.
///
/// Only whether it changes means anything: a reader compares two reads taken
/// some refresh intervals apart, so a stopped pipeline's lease, which never
/// changes again, says nothing about the present, and no clock is compared.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Lease {
    pub stream: StreamId,
    /// Unique to one process. A restart counts from the start again, and the
    /// token is what keeps that from reading as the same lease.
    pub run: String,
    /// Rises on every refresh.
    pub counter: u64,
    /// How often the holder refreshes, so a reader waits long enough for this
    /// holder rather than for its own idea of the interval.
    pub interval_ms: u64,
    /// Who holds it, for the human reading a refusal; may be empty.
    pub holder: String,
    /// When it was last refreshed, epoch seconds by the holder's clock. Shown,
    /// never compared.
    pub refreshed_at_epoch: u64,
}

impl Lease {
    /// Whether `later` shows a pipeline alive since `earlier` was read.
    ///
    /// One that cleared its lease in between stopped cleanly, so only a lease
    /// that is there now and differs is evidence.
    pub fn moved(earlier: Option<&Lease>, later: Option<&Lease>) -> bool {
        match (earlier, later) {
            (_, None) => false,
            (None, Some(_)) => true,
            (Some(a), Some(b)) => a.run != b.run || a.counter != b.counter,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lease(run: &str, counter: u64) -> Lease {
        Lease {
            stream: StreamId {
                source: SOURCE_MYSQL.into(),
                stream: "7".into(),
                publication: String::new(),
            },
            run: run.into(),
            counter,
            interval_ms: 5_000,
            holder: "a".into(),
            refreshed_at_epoch: 1,
        }
    }

    #[test]
    fn a_lease_that_did_not_change_is_no_evidence() {
        let a = lease("r1", 4);
        assert!(!Lease::moved(Some(&a), Some(&a)));
        assert!(!Lease::moved(None, None));
    }

    #[test]
    fn a_refresh_a_restart_or_a_new_holder_is_a_live_pipeline() {
        let a = lease("r1", 4);
        assert!(Lease::moved(Some(&a), Some(&lease("r1", 5))));
        assert!(Lease::moved(Some(&a), Some(&lease("r2", 4))));
        assert!(Lease::moved(None, Some(&a)));
    }

    #[test]
    fn only_the_time_changing_is_not_a_refresh() {
        let a = lease("r1", 4);
        let b = Lease {
            refreshed_at_epoch: 99,
            ..a.clone()
        };
        assert!(!Lease::moved(Some(&a), Some(&b)));
    }

    #[test]
    fn a_lease_cleared_in_between_is_a_clean_stop() {
        assert!(!Lease::moved(Some(&lease("r1", 4)), None));
    }
}
