//! Adapter layer between the `pgwire-replication` transport and core types.
//!
//! The transport is swappable by design: nothing outside this module
//! may name pgwire-replication types.

use crate::lsn::Lsn;
use crate::pgoutput;
use std::collections::HashMap;

/// Relation metadata resolved from RELATION messages, keyed by relid.
///
/// PG re-sends RELATION after every relcache invalidation, so entries are
/// upserted on observation rather than created once.
#[derive(Debug, Clone, Default)]
pub struct RelationRegistry {
    relations: HashMap<u32, RelationInfo>,
}

#[derive(Debug, Clone)]
pub struct RelationInfo {
    pub schema: String,
    pub name: String,
    /// Columns marked as part of the replica identity; empty when the table
    /// has REPLICA IDENTITY NOTHING.
    pub pk_columns: Vec<String>,
}

impl RelationRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn observe(&mut self, rel: &pgoutput::Relation) {
        self.relations.insert(
            rel.rel_id,
            RelationInfo {
                schema: rel.schema.clone(),
                name: rel.name.clone(),
                pk_columns: rel
                    .columns
                    .iter()
                    .filter(|c| c.in_replica_identity)
                    .map(|c| c.name.clone())
                    .collect(),
            },
        );
    }

    pub fn get(&self, rel_id: u32) -> Option<&RelationInfo> {
        self.relations.get(&rel_id)
    }
}

/// Convert a transport LSN into this source's own LSN.
pub fn to_core_lsn(lsn: pgwire_replication::Lsn) -> Lsn {
    Lsn(lsn.0)
}

/// The position a stream boundary may acknowledge, or `None` for a zero LSN.
///
/// Zero names no WAL record; the engine used to skip it as its positionless
/// sentinel, and as a real position it would sort below everything and move
/// the acknowledged position backwards.
pub fn stream_position(lsn: pgwire_replication::Lsn) -> Option<pg2osync_core::Position> {
    (lsn.0 != 0).then(|| to_core_lsn(lsn).to_position())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registry_upserts_on_repeated_relation_messages() {
        let mut reg = RelationRegistry::new();
        let rel = pgoutput::Relation {
            rel_id: 16385,
            schema: "public".into(),
            name: "users".into(),
            replica_identity: pgoutput::ReplicaIdentity::Default,
            columns: vec![
                pgoutput::RelationColumn {
                    name: "id".into(),
                    type_oid: 20,
                    typmod: -1,
                    in_replica_identity: true,
                },
                pgoutput::RelationColumn {
                    name: "email".into(),
                    type_oid: 25,
                    typmod: -1,
                    in_replica_identity: false,
                },
            ],
        };
        reg.observe(&rel);
        assert_eq!(reg.get(16385).unwrap().pk_columns, vec!["id".to_string()]);
        // same relid observed again with different columns must replace, not duplicate
        let mut changed = rel.clone();
        changed.columns[0].in_replica_identity = false;
        reg.observe(&changed);
        assert!(reg.get(16385).unwrap().pk_columns.is_empty());
    }

    #[test]
    fn a_zero_lsn_is_no_position() {
        // a zero used to be the positionless sentinel; acknowledging one would
        // move the acknowledged position back to the start of the WAL
        assert_eq!(stream_position(pgwire_replication::Lsn(0)), None);
        assert_eq!(
            stream_position(pgwire_replication::Lsn(0x1B4_F2A8)),
            Some(Lsn(0x1B4_F2A8).to_position())
        );
    }

    #[test]
    fn lsn_conversion_is_lossless() {
        let raw = pgwire_replication::Lsn(0x1B4_F2A8);
        assert_eq!(to_core_lsn(raw).to_string(), "0/1B4F2A8");
    }
}
