//! Which tables a running stream admits, and the per-table facts its decoder
//! needs to file their rows.
//!
//! One structure rather than three fields on each source's configuration,
//! because a reload that admitted a table before the decoder knew its key
//! would file that table's rows under the wrong id — and nothing downstream
//! could tell. Swapping the set as a whole makes that state unreachable.
//!
//! The lock is a plain `std::sync::RwLock`: every reader clones the snapshot
//! out of it in a few instructions and none of them holds it across an await.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, PoisonError, RwLock};

/// The tables one source streams, as one consistent set.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TableSet {
    /// `(schema, table)`, in configuration order.
    pub tables: Vec<(String, String)>,
    /// Each table's key columns as the catalogue reports them. Under REPLICA
    /// IDENTITY FULL pgoutput flags every column as identity, and only this
    /// says which of them a document is actually filed under.
    pub key_columns: HashMap<(String, String), Vec<String>>,
    /// Tables declared `append_only`: their inserts carry no key, and an
    /// update or delete on one is an error rather than a document nothing
    /// can find.
    pub append_only: HashSet<(String, String)>,
}

impl TableSet {
    pub fn contains(&self, schema: &str, table: &str) -> bool {
        self.tables.iter().any(|(s, t)| s == schema && t == table)
    }

    /// Schema-qualified names, which is how the source's catalogue statements
    /// and the publication both spell a table.
    pub fn qualified(&self) -> Vec<String> {
        self.tables
            .iter()
            .map(|(s, t)| format!("{s}.{t}"))
            .collect()
    }
}

/// A [`TableSet`] a running stream reads and a reload replaces.
///
/// Cloning shares: the streaming attempt, its loader and the reload task all
/// hold the same set, so a table added to it is admitted by whichever of them
/// looks next.
#[derive(Debug, Clone, Default)]
pub struct SharedTables(Arc<RwLock<Arc<TableSet>>>);

impl SharedTables {
    pub fn new(set: TableSet) -> Self {
        Self(Arc::new(RwLock::new(Arc::new(set))))
    }

    /// The set as it is now. Held by the caller, so what it decides is decided
    /// against one version rather than against a set changing under it.
    pub fn snapshot(&self) -> Arc<TableSet> {
        self.0
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    pub fn contains(&self, schema: &str, table: &str) -> bool {
        self.snapshot().contains(schema, table)
    }

    /// Replace the set with an edited copy of it.
    ///
    /// Copy-on-write rather than mutation in place: readers hold an `Arc` of
    /// the old set for as long as they are deciding, so a swap can never show
    /// one of them half an edit.
    pub fn edit(&self, change: impl FnOnce(&mut TableSet)) {
        let mut guard = self.0.write().unwrap_or_else(PoisonError::into_inner);
        let mut next = (**guard).clone();
        change(&mut next);
        *guard = Arc::new(next);
    }

    /// Admit `(schema, table)`, with everything the decoder needs for it.
    pub fn add(&self, schema: &str, table: &str, key_columns: Vec<String>, append_only: bool) {
        let key = (schema.to_string(), table.to_string());
        self.edit(|set| {
            if !set.contains(schema, table) {
                set.tables.push(key.clone());
            }
            set.key_columns.insert(key.clone(), key_columns);
            if append_only {
                set.append_only.insert(key);
            } else {
                set.append_only.remove(&key);
            }
        });
    }

    /// Stop admitting `(schema, table)`, and forget what was known about it.
    pub fn remove(&self, schema: &str, table: &str) {
        let key = (schema.to_string(), table.to_string());
        self.edit(|set| {
            set.tables.retain(|entry| entry != &key);
            set.key_columns.remove(&key);
            set.append_only.remove(&key);
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn set_of(tables: &[(&str, &str)]) -> TableSet {
        TableSet {
            tables: tables
                .iter()
                .map(|(s, t)| (s.to_string(), t.to_string()))
                .collect(),
            ..Default::default()
        }
    }

    #[test]
    fn a_table_added_to_one_handle_is_admitted_by_every_other() {
        let stream = SharedTables::new(set_of(&[("public", "users")]));
        let reload = stream.clone();
        assert!(!stream.contains("public", "orders"));
        reload.add("public", "orders", vec!["id".into()], false);
        assert!(stream.contains("public", "orders"));
        assert_eq!(
            stream.snapshot().key_columns.get(&k("orders")),
            Some(&vec!["id".to_string()])
        );
    }

    #[test]
    fn a_snapshot_taken_before_a_swap_is_the_set_it_was_taken_from() {
        let tables = SharedTables::new(set_of(&[("public", "users")]));
        let deciding = tables.snapshot();
        tables.add("public", "orders", Vec::new(), false);
        assert!(
            !deciding.contains("public", "orders"),
            "a reader must never see half an edit"
        );
    }

    #[test]
    fn removing_a_table_forgets_what_was_known_about_it() {
        let tables = SharedTables::new(set_of(&[("public", "users")]));
        tables.add("public", "events", Vec::new(), true);
        tables.remove("public", "events");
        let set = tables.snapshot();
        assert!(!set.contains("public", "events"));
        assert!(set.append_only.is_empty());
        assert!(!set.key_columns.contains_key(&k("events")));
        assert_eq!(set.qualified(), vec!["public.users".to_string()]);
    }

    fn k(table: &str) -> (String, String) {
        ("public".to_string(), table.to_string())
    }
}
