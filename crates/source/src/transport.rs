//! Adapter layer between the `pgwire-replication` transport and core types.
//!
//! The transport is swappable by design (ADR #5): nothing outside this module
//! may name pgwire-replication types.

use crate::pgoutput;
use pg2osync_core::Lsn;
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

/// Convert a transport LSN into the shared core LSN.
pub fn to_core_lsn(lsn: pgwire_replication::Lsn) -> Lsn {
    Lsn(lsn.0)
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
    fn lsn_conversion_is_lossless() {
        let raw = pgwire_replication::Lsn(0x1B4_F2A8);
        assert_eq!(to_core_lsn(raw).to_string(), "0/1B4F2A8");
    }
}
