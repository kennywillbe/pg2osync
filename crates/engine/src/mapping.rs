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

    /// Target index; unmapped tables are ignored upstream, so this only sees
    /// configured tables — but stay defensive with a derived name.
    pub fn index_for(&self, schema: &str, table: &str) -> &str {
        self.opt_index_for(schema, table)
            .expect("unmapped table reached the engine; source filter is broken")
    }

    pub fn opt_index_for(&self, schema: &str, table: &str) -> Option<&str> {
        self.map
            .get(&(schema.to_string(), table.to_string()))
            .map(String::as_str)
    }
}
