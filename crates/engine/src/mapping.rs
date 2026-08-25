//! Logical table → index mapping and the shared durable-LSN cell.

use std::collections::HashMap;
use std::sync::Arc;

#[derive(Clone, Default)]
pub struct DurableLsn(pub Arc<std::sync::atomic::AtomicU64>);

impl DurableLsn {
    pub fn store(&self, lsn: pg2osync_core::Lsn) {
        self.0.store(lsn.0, std::sync::atomic::Ordering::SeqCst);
    }
    pub fn load(&self) -> Option<pg2osync_core::Lsn> {
        let v = self.0.load(std::sync::atomic::Ordering::SeqCst);
        (v > 0).then_some(pg2osync_core::Lsn(v))
    }
}

/// Per-table column projection from `[sync.x] columns` / `exclude_columns`.
///
/// Applied to every document regardless of source and code path (backfill,
/// streaming, poll), so an excluded column can never reach a sink.
#[derive(Debug, Clone, Default)]
pub struct Projections {
    map: HashMap<(String, String), Projection>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Projection {
    /// Keep only these columns, in whatever order the document has them.
    Include(Vec<String>),
    /// Keep everything except these columns.
    Exclude(Vec<String>),
}

impl Projections {
    pub fn from_pairs(pairs: impl IntoIterator<Item = ((String, String), Projection)>) -> Self {
        Self {
            map: pairs.into_iter().collect(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    /// Drop columns the configuration excludes, in place.
    pub fn apply(&self, schema: &str, table: &str, doc: &mut serde_json::Value) {
        let Some(rule) = self.map.get(&(schema.to_string(), table.to_string())) else {
            return;
        };
        let Some(obj) = doc.as_object_mut() else {
            return;
        };
        match rule {
            Projection::Include(keep) => obj.retain(|k, _| keep.iter().any(|c| c == k)),
            Projection::Exclude(drop) => obj.retain(|k, _| !drop.iter().any(|c| c == k)),
        }
    }
}

/// Per-table column transformations from `[sync.x.transform]`.
#[derive(Debug, Clone, Default)]
pub struct Transforms {
    /// (schema, table) -> (column -> operation)
    map: HashMap<(String, String), HashMap<String, TransformOp>>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum TransformOp {
    Hash,
    Redact,
}

impl TransformOp {
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "hash" => Some(Self::Hash),
            "redact" => Some(Self::Redact),
            _ => None,
        }
    }
}

impl Transforms {
    pub fn from_pairs(
        pairs: impl IntoIterator<Item = ((String, String), HashMap<String, TransformOp>)>,
    ) -> Self {
        Self {
            map: pairs.into_iter().collect(),
        }
    }

    pub fn for_table(&self, schema: &str, table: &str) -> Option<&HashMap<String, TransformOp>> {
        self.map.get(&(schema.to_string(), table.to_string()))
    }

    /// Apply configured transforms in place to a document.
    pub fn apply(&self, schema: &str, table: &str, doc: &mut serde_json::Value) {
        let Some(rules) = self.for_table(schema, table) else {
            return;
        };
        let Some(doc_map) = doc.as_object_mut() else {
            return;
        };
        for (col, op) in rules {
            if let Some(v) = doc_map.get_mut(col) {
                if v.is_null() {
                    continue;
                }
                *v = match op {
                    TransformOp::Redact => serde_json::Value::String("***".into()),
                    TransformOp::Hash => {
                        use sha2::{Digest, Sha256};
                        let mut hasher = Sha256::new();
                        hasher.update(v.to_string().as_bytes());
                        let h = hasher.finalize();
                        serde_json::Value::String(
                            h.iter().map(|b| format!("{b:02x}")).collect::<String>()[..16]
                                .to_string(),
                        )
                    }
                };
            }
        }
    }
}

/// Maps `(schema, table)` to the target index name, from `[sync.*]` config.
#[derive(Debug, Clone, Default)]
pub struct TableMapping {
    map: HashMap<(String, String), String>,
}

impl TableMapping {
    pub fn from_pairs(pairs: impl IntoIterator<Item = ((String, String), String)>) -> Self {
        Self {
            map: pairs.into_iter().collect(),
        }
    }

    pub fn opt_index_for(&self, schema: &str, table: &str) -> Option<&str> {
        self.map
            .get(&(schema.to_string(), table.to_string()))
            .map(String::as_str)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn include_keeps_only_listed_columns() {
        let p = Projections::from_pairs([(
            ("public".into(), "users".into()),
            Projection::Include(vec!["id".into(), "email".into()]),
        )]);
        let mut doc = json!({"id": 1, "email": "a@b.c", "secret": "x"});
        p.apply("public", "users", &mut doc);
        assert_eq!(doc, json!({"id": 1, "email": "a@b.c"}));
    }

    #[test]
    fn exclude_drops_listed_columns_and_ignores_other_tables() {
        let p = Projections::from_pairs([(
            ("public".into(), "users".into()),
            Projection::Exclude(vec!["password_hash".into()]),
        )]);
        let mut doc = json!({"id": 1, "password_hash": "$2b$"});
        p.apply("public", "users", &mut doc);
        assert_eq!(doc, json!({"id": 1}));

        let mut other = json!({"password_hash": "kept"});
        p.apply("public", "orders", &mut other);
        assert_eq!(other, json!({"password_hash": "kept"}));
    }

    #[test]
    fn transforms_redact_and_hash_but_leave_nulls() {
        let rules = HashMap::from([
            ("email".to_string(), TransformOp::Redact),
            ("ssn".to_string(), TransformOp::Hash),
            ("phone".to_string(), TransformOp::Redact),
        ]);
        let t = Transforms::from_pairs([(("public".into(), "users".into()), rules)]);
        let mut doc = json!({"email": "a@b.c", "ssn": "123", "phone": null});
        t.apply("public", "users", &mut doc);
        assert_eq!(doc["email"], json!("***"));
        assert_eq!(
            doc["ssn"].as_str().map(str::len),
            Some(16),
            "hash is truncated to a stable width"
        );
        assert!(doc["phone"].is_null(), "null carries no value to mask");
    }

    #[test]
    fn hashing_is_deterministic() {
        let rules = HashMap::from([("v".to_string(), TransformOp::Hash)]);
        let t = Transforms::from_pairs([(("s".into(), "t".into()), rules)]);
        let mut a = json!({"v": "same"});
        let mut b = json!({"v": "same"});
        t.apply("s", "t", &mut a);
        t.apply("s", "t", &mut b);
        assert_eq!(a, b);
    }

    #[test]
    fn durable_lsn_reports_none_until_set() {
        let d = DurableLsn::default();
        assert!(d.load().is_none());
        d.store(pg2osync_core::Lsn(42));
        assert_eq!(d.load(), Some(pg2osync_core::Lsn(42)));
    }

    #[test]
    fn unmapped_tables_have_no_index() {
        let m = TableMapping::from_pairs([(("public".into(), "users".into()), "users_v1".into())]);
        assert_eq!(m.opt_index_for("public", "users"), Some("users_v1"));
        assert_eq!(m.opt_index_for("public", "orders"), None);
    }
}
