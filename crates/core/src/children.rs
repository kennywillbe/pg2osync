//! What a nested child collection is, independent of where it is read from.
//!
//! Children are resolved on the source side so the engine stays
//! source-agnostic: a parent document simply arrives with extra array fields.
//! The reading is therefore per source — a dialect, a client and a row-to-JSON
//! conversion each — but three things must not be: what a collection is called,
//! when a document is allowed to claim it holds all of one, and how a group of
//! changed rows collapses to the parents it affects.
//!
//! Those live here so the two sources cannot answer them differently. A second
//! implementation of the truncation rule in particular would be invisible until
//! somebody compared a MySQL document against a PostgreSQL one.

use crate::event::RowChange;
use serde_json::Value;
use std::collections::{HashMap, HashSet};

/// One configured `[[sync.x.children]]` entry, fully qualified.
#[derive(Debug, Clone)]
pub struct ChildSpec {
    pub schema: String,
    pub table: String,
    pub field: String,
    pub foreign_key: String,
    /// Parent column the foreign key references — the parent's key.
    pub parent_column: String,
    /// The child's own primary key, resolved from the catalogue at startup.
    ///
    /// Without an order the array is a set in arbitrary order, so the initial
    /// load and a streamed re-fetch can embed the same children differently and
    /// a re-snapshot rewrites documents for no reason. With `max_rows` it
    /// matters more than cosmetically: two runs would keep *different* children.
    pub order_by: Vec<String>,
    /// How many children to embed, or all of them.
    pub max_rows: Option<u32>,
}

impl ChildSpec {
    pub fn new(
        qualified_child: &str,
        field: &str,
        foreign_key: &str,
        parent_column: &str,
    ) -> Result<Self, crate::error::CoreError> {
        let (schema, table) = qualified_child.split_once('.').ok_or_else(|| {
            crate::error::CoreError::Other(format!(
                "child table {qualified_child:?} must be schema-qualified"
            ))
        })?;
        Ok(Self {
            schema: schema.into(),
            table: table.into(),
            field: field.into(),
            foreign_key: foreign_key.into(),
            parent_column: parent_column.into(),
            order_by: Vec::new(),
            max_rows: None,
        })
    }

    pub fn qualified(&self) -> String {
        format!("{}.{}", self.schema, self.table)
    }

    /// The field naming how many children the source actually has, present only
    /// on a document whose array was cut short.
    pub fn total_field(&self) -> String {
        format!("{}_total", self.field)
    }

    /// The field saying the array is not the whole collection.
    pub fn truncated_field(&self) -> String {
        format!("{}_truncated", self.field)
    }
}

/// Put one collection on a parent document, saying so if it is not all of it.
///
/// `total` is what the source holds before any cap. A consumer cannot otherwise
/// tell a short array from a complete one, and handing over part of a collection
/// as if it were the whole thing is worse than either embedding everything or
/// refusing to.
///
/// Returns whether anything was left out, so the caller can count it.
pub fn apply_collection(doc: &mut Value, spec: &ChildSpec, array: Value, total: i64) -> bool {
    let Value::Object(map) = doc else {
        return false;
    };
    let embedded = array.as_array().map(Vec::len).unwrap_or(0) as i64;
    map.insert(spec.field.clone(), array);
    let cut = total > embedded;
    if cut {
        map.insert(spec.truncated_field(), Value::Bool(true));
        map.insert(spec.total_field(), Value::from(total));
    }
    cut
}

/// Past this many embedded rows, say so.
///
/// OpenSearch's own `index.mapping.nested_objects.limit` default: the point where
/// an unbounded array stops being slow and starts being refused, because every
/// element of a `nested` field becomes a hidden Lucene sub-document.
pub const UNBOUNDED_ARRAY_WARNING: i64 = 10_000;

/// How a parent key is matched against what a batched read returned.
///
/// Both sides render the key as JSON, so a number and its text form cannot
/// disagree about whether they are the same key.
pub fn key_lookup(key: &Value) -> String {
    key.to_string()
}

/// Rows held back until their children can be resolved for the whole group.
///
/// A parent row keeps its decoded change, because its document comes from the
/// replication log and only the arrays are missing. A child row keeps nothing but
/// the parent key it names, deduplicated — which is why a transaction touching a
/// thousand children of one parent holds one key rather than a thousand rows, and
/// writes one document rather than a thousand identical ones.
#[derive(Default)]
pub struct Pending {
    pub parents: HashMap<(String, String), Vec<RowChange>>,
    pub named: HashMap<(String, String), Vec<Value>>,
    seen: HashSet<(String, String, String)>,
}

impl Pending {
    pub fn hold_parent(&mut self, table: (String, String), change: RowChange) {
        self.parents.entry(table).or_default().push(change);
    }

    pub fn name_parent(&mut self, table: (String, String), key: Value) {
        let id = (table.0.clone(), table.1.clone(), key_lookup(&key));
        if self.seen.insert(id) {
            self.named.entry(table).or_default().push(key);
        }
    }

    pub fn is_empty(&self) -> bool {
        self.parents.is_empty() && self.named.is_empty()
    }

    /// How much is held, for the cap that keeps one enormous transaction from
    /// living entirely in memory.
    pub fn len(&self) -> usize {
        self.parents.values().map(Vec::len).sum::<usize>()
            + self.named.values().map(Vec::len).sum::<usize>()
    }

    /// Every parent table this group touches, from either direction.
    pub fn tables(&self) -> Vec<(String, String)> {
        self.parents
            .keys()
            .chain(self.named.keys())
            .cloned()
            .collect::<HashSet<_>>()
            .into_iter()
            .collect()
    }

    /// Take one table's held rows and named keys, resolving them together.
    pub fn take(&mut self, table: &(String, String)) -> (Vec<RowChange>, Vec<Value>) {
        (
            self.parents.remove(table).unwrap_or_default(),
            self.named.remove(table).unwrap_or_default(),
        )
    }

    pub fn clear_seen(&mut self) {
        self.seen.clear();
    }
}

/// Which named parent keys still need reading.
///
/// A key a parent row in this group already carries needs no re-read: that row's
/// document came from the replication log, which is the fresher of the two and
/// saves a query. A parent *deleted* in the group suppresses it as well — the
/// delete is what removes the document, and re-reading the key would find
/// nothing.
pub fn keys_needing_refetch(rows: &[RowChange], named: Vec<Value>) -> Vec<Value> {
    let covered: HashSet<String> = rows.iter().map(|r| key_lookup(r.pk())).collect();
    named
        .into_iter()
        .filter(|k| !covered.contains(&key_lookup(k)))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::RowKind;
    use serde_json::json;

    fn spec() -> ChildSpec {
        ChildSpec::new("public.orders", "orders", "customer_id", "id").expect("qualified")
    }

    fn parent_row(id: i64) -> RowChange {
        RowChange {
            schema: "public".into(),
            table: "customers".into(),
            kind: RowKind::Insert {
                pk: json!(id),
                doc: json!({"id": id}),
            },
            version: None,
        }
    }

    fn table() -> (String, String) {
        ("public".to_string(), "customers".to_string())
    }

    #[test]
    fn a_complete_collection_says_nothing_extra() {
        let mut doc = json!({"id": 1});
        assert!(!apply_collection(&mut doc, &spec(), json!([{"id": 9}]), 1));
        assert_eq!(doc["orders"], json!([{"id": 9}]));
        assert!(doc.get("orders_truncated").is_none());
        assert!(doc.get("orders_total").is_none());
    }

    #[test]
    fn a_cut_collection_says_how_much_it_is_missing() {
        let mut doc = json!({"id": 1});
        assert!(apply_collection(&mut doc, &spec(), json!([{"id": 9}]), 500));
        assert_eq!(doc["orders_truncated"], json!(true));
        assert_eq!(doc["orders_total"], json!(500));
    }

    #[test]
    fn many_children_of_one_parent_hold_one_key() {
        // The whole point of holding the key rather than the row: 500 child rows
        // on one parent must not become 500 queries and 500 identical documents.
        let mut pending = Pending::default();
        for _ in 0..500 {
            pending.name_parent(table(), json!(7));
        }
        assert_eq!(pending.len(), 1);
    }

    #[test]
    fn the_same_key_on_two_parent_tables_is_two_keys() {
        let mut pending = Pending::default();
        pending.name_parent(("public".into(), "customers".into()), json!(1));
        pending.name_parent(("public".into(), "invoices".into()), json!(1));
        assert_eq!(pending.len(), 2, "different documents, same key value");
        assert_eq!(pending.tables().len(), 2);
    }

    #[test]
    fn a_parent_row_in_the_group_saves_the_re_read() {
        let needed = keys_needing_refetch(&[parent_row(1)], vec![json!(1), json!(2)]);
        assert_eq!(needed, vec![json!(2)]);
    }

    #[test]
    fn a_deleted_parent_still_suppresses_the_re_read() {
        let deleted = RowChange {
            schema: "public".into(),
            table: "customers".into(),
            kind: RowKind::Delete { pk: json!(9) },
            version: None,
        };
        assert!(keys_needing_refetch(&[deleted], vec![json!(9)]).is_empty());
    }

    #[test]
    fn taking_a_table_resolves_both_directions_together() {
        let mut pending = Pending::default();
        pending.hold_parent(table(), parent_row(1));
        pending.name_parent(table(), json!(2));
        let (rows, named) = pending.take(&table());
        assert_eq!(rows.len(), 1);
        assert_eq!(named, vec![json!(2)]);
        assert!(pending.is_empty(), "nothing left once it is taken");
    }
}
