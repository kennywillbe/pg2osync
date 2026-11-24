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

/// The junction table a many-to-many child is reached through.
///
/// It holds the pair and nothing else: the child rows are what gets embedded,
/// and the junction contributes no field to the document.
#[derive(Debug, Clone)]
pub struct Through {
    pub schema: String,
    pub table: String,
    /// Junction column referencing the CHILD's primary key.
    pub through_key: String,
    /// The child's own primary key, resolved from the catalogue at startup.
    ///
    /// A through child needs exactly one, because it is both what the join
    /// matches `through_key` against and what a changed child row is looked
    /// back up by.
    pub child_key: String,
}

impl Through {
    pub fn new(qualified: &str, through_key: &str) -> Result<Self, crate::error::CoreError> {
        let (schema, table) = qualified.split_once('.').ok_or_else(|| {
            crate::error::CoreError::Other(format!(
                "through table {qualified:?} must be schema-qualified"
            ))
        })?;
        Ok(Self {
            schema: schema.into(),
            table: table.into(),
            through_key: through_key.into(),
            child_key: String::new(),
        })
    }

    pub fn qualified(&self) -> String {
        format!("{}.{}", self.schema, self.table)
    }
}

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
    /// The only child columns to embed, or all of them.
    ///
    /// Projection happens in the read rather than after it, so the initial load
    /// and a streamed re-fetch cannot embed different shapes.
    pub columns: Option<Vec<String>>,
    /// Child columns to leave out. Mutually exclusive with `columns`.
    pub exclude_columns: Vec<String>,
    /// The relation is one-to-one: the field holds the element itself.
    ///
    /// Unwrapped here rather than in the read, so both sources keep exactly one
    /// aggregation builder and the initial load and a streamed re-fetch cannot
    /// produce different shapes.
    pub single: bool,
    /// The junction a many-to-many relation is reached through.
    ///
    /// With it set, `foreign_key` is a column of the junction rather than of
    /// the child: the junction is what carries the parent's key.
    pub through: Option<Through>,
    /// Collections embedded inside each element of this one, one level deep.
    ///
    /// Each one's `parent_column` is this child's own single-column key,
    /// resolved from the catalogue, because that is what every element is
    /// matched by when the second level is attached.
    pub children: Vec<ChildSpec>,
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
            columns: None,
            exclude_columns: Vec::new(),
            single: false,
            through: None,
            children: Vec::new(),
        })
    }

    pub fn qualified(&self) -> String {
        format!("{}.{}", self.schema, self.table)
    }

    /// Whether this collection reads a table, as either of the two it watches.
    ///
    /// A through collection is fed by two tables — the junction carries the
    /// parent's key, the child carries what is embedded — and a streamed row of
    /// either one has to find its way back to this spec.
    pub fn reads(&self, schema: &str, table: &str) -> bool {
        (self.schema == schema && self.table == table)
            || self
                .through
                .as_ref()
                .is_some_and(|t| t.schema == schema && t.table == table)
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

    /// The column each element of this collection is matched to its own
    /// children by, when it has any.
    ///
    /// A through child's key was resolved onto its junction; a direct one's is
    /// the single-column primary key the order is taken from.
    pub fn element_key_column(&self) -> Option<&str> {
        if self.children.is_empty() {
            return None;
        }
        match &self.through {
            Some(through) => Some(through.child_key.as_str()),
            None => match self.order_by.as_slice() {
                [key] => Some(key.as_str()),
                _ => None,
            },
        }
    }
}

/// Where a streamed row of a watched table belongs among a parent's
/// collections.
#[derive(Debug, Clone, Copy)]
pub enum Route<'a> {
    /// A first-level collection, or its junction.
    Child(&'a ChildSpec),
    /// A second-level collection: its rows name an element of `parent`, not a
    /// document.
    Grandchild {
        parent: &'a ChildSpec,
        spec: &'a ChildSpec,
    },
}

/// Find the collection a table feeds among one parent's collections.
///
/// The first level is asked first only for determinism: configuration refuses
/// a table that appears twice across sections and depths, so at most one
/// answer exists. Matching a second-level table by name rather than through
/// [`ChildSpec::reads`] is deliberate, because it is that spec's own
/// `foreign_key` the row is read by, never its parent's.
pub fn route<'a>(specs: &'a [ChildSpec], schema: &str, table: &str) -> Option<Route<'a>> {
    if let Some(spec) = specs.iter().find(|s| s.reads(schema, table)) {
        return Some(Route::Child(spec));
    }
    specs.iter().find_map(|parent| {
        parent
            .children
            .iter()
            .find(|g| g.schema == schema && g.table == table)
            .map(|spec| Route::Grandchild { parent, spec })
    })
}

/// The name an element carries its own key under until its children are
/// attached.
///
/// Read beside the element rather than out of it, because `columns` or
/// `exclude_columns` may leave the key out of what the document embeds; it is
/// removed again by [`attach_nested`] whatever happens, so no document ever
/// carries it.
pub const ELEMENT_KEY: &str = "__pg2osync_element_key";

/// Every element key one embedded collection holds, as read for its children.
///
/// A `single` collection is the element itself, or null; an array is any
/// number of them. Both are walked, since a one-to-one intermediate can still
/// have a collection of its own.
pub fn element_keys(collection: &Value) -> Vec<Value> {
    elements(collection)
        .filter_map(|e| e.get(ELEMENT_KEY).cloned())
        .filter(|k| !k.is_null())
        .collect()
}

fn elements(collection: &Value) -> Box<dyn Iterator<Item = &Value> + '_> {
    match collection {
        Value::Array(rows) => Box::new(rows.iter()),
        Value::Object(_) => Box::new(std::iter::once(collection)),
        _ => Box::new(std::iter::empty()),
    }
}

fn elements_mut(collection: &mut Value) -> Box<dyn Iterator<Item = &mut Value> + '_> {
    match collection {
        Value::Array(rows) => Box::new(rows.iter_mut()),
        Value::Object(_) => Box::new(std::iter::once(collection)),
        _ => Box::new(std::iter::empty()),
    }
}

/// What one second-level collection turned into across a group, so the
/// caller can say it once rather than per element.
#[derive(Debug, Default, Clone)]
pub struct NestedTally {
    /// Elements whose array was cut by `max_rows`.
    pub cut: usize,
    /// The element holding the most rows, and how many.
    pub largest: Option<(Value, i64)>,
    pub duplicates: Duplicates,
}

impl NestedTally {
    /// The line a cut second level logs, or nothing when no element was cut.
    ///
    /// Worded here so both sources say it the same way: one line per
    /// collection per group, naming the worst element rather than each one.
    pub fn cut_message(&self, via: &ChildSpec, grand: &ChildSpec) -> Option<String> {
        let (key, total) = self.largest.as_ref().filter(|_| self.cut > 0)?;
        Some(format!(
            "{} element(s) of {} embed only max_rows of {}, and say so in {} and {}; \
             the largest is {} row {key} with {total} rows",
            self.cut,
            via.field,
            grand.qualified(),
            grand.truncated_field(),
            grand.total_field(),
            via.qualified()
        ))
    }
}

/// The distinct element keys across a group of one collection's values, in
/// the order first seen.
///
/// One question per key rather than per element: a many-to-many intermediate
/// row sits in every document that links it, and asking for it once per
/// document would read its second level that many times.
pub fn distinct_element_keys<'a>(collections: impl IntoIterator<Item = &'a Value>) -> Vec<Value> {
    let mut seen = HashSet::new();
    let mut keys = Vec::new();
    for collection in collections {
        for key in element_keys(collection) {
            if seen.insert(key_lookup(&key)) {
                keys.push(key);
            }
        }
    }
    keys
}

/// Put each of `spec`'s second-level collections on every element of one
/// embedded collection, then drop the element key.
///
/// `fetched` holds, per second-level collection in `spec.children` order, the
/// arrays and totals the source read keyed by [`key_lookup`] of an element key.
/// The shaping is [`apply_collection`]'s, so a second-level array's
/// `_truncated`/`_total` land inside the element it belongs to exactly as a
/// first-level one's land on the document.
pub fn attach_nested(
    collection: &mut Value,
    spec: &ChildSpec,
    fetched: &[HashMap<String, (Value, i64)>],
    tallies: &mut [NestedTally],
) {
    for element in elements_mut(collection) {
        let Value::Object(map) = element else {
            continue;
        };
        let key = map.remove(ELEMENT_KEY).unwrap_or(Value::Null);
        for ((grand, by_key), tally) in spec.children.iter().zip(fetched).zip(tallies.iter_mut()) {
            let (array, total) = by_key
                .get(&key_lookup(&key))
                .cloned()
                .unwrap_or((Value::Array(Vec::new()), 0));
            let applied = apply_collection(element, grand, array, total);
            if applied.truncated {
                tally.cut += 1;
            }
            tally.duplicates.record(grand, &key, applied.matched);
            if tally.largest.as_ref().is_none_or(|(_, most)| total > *most) {
                tally.largest = Some((key.clone(), total));
            }
        }
    }
}

/// How many nested objects one document can hold under `spec`, or `None`
/// when a level is unbounded.
///
/// Only meaningful for a chain with a second level: every first-level element
/// is one object and carries up to the product of the caps below it.
pub fn nested_object_bound(spec: &ChildSpec) -> Option<u64> {
    let width = |s: &ChildSpec| -> Option<u64> {
        if s.single {
            Some(1)
        } else {
            s.max_rows.map(u64::from)
        }
    };
    let per_element = spec
        .children
        .iter()
        .try_fold(1u64, |acc, g| Some(acc.saturating_add(width(g)?)))?;
    Some(width(spec)?.saturating_mul(per_element))
}

/// What one collection turned into, so the caller can summarise a batch.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Applied {
    /// The array is not the whole collection, and the document says so.
    pub truncated: bool,
    /// How many child rows matched this parent, before any unwrapping.
    pub matched: usize,
}

/// The value one collection lands as, and how many rows it was made of.
///
/// A `single` collection is the element itself — `null` when the parent has no
/// child — because a one-to-one relation that reads as `profile[0].bio` forces
/// every query and every mapping to carry an index that is always zero.
fn shape_collection(spec: &ChildSpec, array: Value) -> (Value, usize) {
    let matched = array.as_array().map(Vec::len).unwrap_or(0);
    if !spec.single {
        return (array, matched);
    }
    // The read orders by the child's key, so the element is the lowest-keyed
    // row: a re-snapshot keeps the same one.
    let element = match array {
        Value::Array(rows) => rows.into_iter().next().unwrap_or(Value::Null),
        other => other,
    };
    (element, matched)
}

/// Put one collection on a parent document, saying so if it is not all of it.
///
/// `total` is what the source holds before any cap. A consumer cannot otherwise
/// tell a short array from a complete one, and handing over part of a collection
/// as if it were the whole thing is worse than either embedding everything or
/// refusing to. A `single` collection is never capped — `max_rows` is refused
/// with it — so it writes neither field.
pub fn apply_collection(doc: &mut Value, spec: &ChildSpec, array: Value, total: i64) -> Applied {
    let Value::Object(map) = doc else {
        return Applied::default();
    };
    let (value, matched) = shape_collection(spec, array);
    map.insert(spec.field.clone(), value);
    let truncated = !spec.single && total > matched as i64;
    if truncated {
        map.insert(spec.truncated_field(), Value::Bool(true));
        map.insert(spec.total_field(), Value::from(total));
    }
    Applied { truncated, matched }
}

/// Parents whose one-to-one collection matched more than once, in one batch.
///
/// A second row does not fail the run: a duplicate that exists for the length of
/// a migration must not halt an index. It is not silent either, so this collects
/// the batch into the one line the truncation warnings are shaped like — the
/// collection, how many parents, and the worst of them.
#[derive(Debug, Default, Clone)]
pub struct Duplicates {
    parents: usize,
    largest: Option<(Value, usize)>,
}

impl Duplicates {
    /// Note what one parent matched. Only a `single` collection can have a row
    /// too many; for any other one every row is embedded.
    pub fn record(&mut self, spec: &ChildSpec, key: &Value, matched: usize) {
        if !spec.single || matched < 2 {
            return;
        }
        self.parents += 1;
        if self
            .largest
            .as_ref()
            .is_none_or(|(_, most)| matched > *most)
        {
            self.largest = Some((key.clone(), matched));
        }
    }

    /// The line the batch logs, or nothing when every parent had at most one.
    pub fn message(&self, spec: &ChildSpec) -> Option<String> {
        let (key, most) = self.largest.as_ref()?;
        Some(format!(
            "{} document(s) embed only the lowest-keyed row of {}, which single = true \
             declares one-to-one; the largest is parent {key} with {most} matching rows",
            self.parents,
            spec.qualified()
        ))
    }
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

/// Child keys still to be turned into parent keys: the parent table and the
/// first-level field they belong to, and the distinct keys.
///
/// Two kinds of row land here and resolve the same way at commit: a changed
/// row of a many-to-many child, whose parents the junction knows, and a
/// changed second-level row, whose root the intermediate row knows.
pub type ChildKeys = (((String, String), String), Vec<Value>);

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
    through: HashMap<((String, String), String), Vec<Value>>,
    seen: HashSet<(String, String, String)>,
    seen_by_child: HashSet<((String, String), String, String)>,
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

    /// A child key that names no parent by itself: a many-to-many child's own
    /// key, whose parents the junction knows, or the intermediate key a
    /// second-level row names, whose root that intermediate row knows.
    ///
    /// Deduplicated the same way a named parent is, per collection, so a
    /// transaction touching one child a thousand times asks about one key.
    pub fn name_by_child(&mut self, table: (String, String), field: &str, child_key: Value) {
        let id = (table.clone(), field.to_string(), key_lookup(&child_key));
        if self.seen_by_child.insert(id) {
            self.through
                .entry((table, field.to_string()))
                .or_default()
                .push(child_key);
        }
    }

    /// Every collection's distinct child keys, to resolve to parents.
    ///
    /// Taken all at once and before anything else, because resolving them is
    /// what turns them into named parents — after which the group is read
    /// exactly as a group of changed junction rows would have been.
    pub fn take_by_child(&mut self) -> Vec<ChildKeys> {
        self.through.drain().collect()
    }

    pub fn is_empty(&self) -> bool {
        self.parents.is_empty() && self.named.is_empty() && self.through.is_empty()
    }

    /// How much is held, for the cap that keeps one enormous transaction from
    /// living entirely in memory.
    pub fn len(&self) -> usize {
        self.parents.values().map(Vec::len).sum::<usize>()
            + self.named.values().map(Vec::len).sum::<usize>()
            + self.through.values().map(Vec::len).sum::<usize>()
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
        self.seen_by_child.clear();
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

    fn single_spec() -> ChildSpec {
        let mut spec =
            ChildSpec::new("public.profiles", "profile", "customer_id", "id").expect("qualified");
        spec.single = true;
        spec
    }

    fn lines_with_options() -> ChildSpec {
        let mut lines = ChildSpec::new("public.lines", "lines", "order_id", "id").expect("q");
        lines.order_by = vec!["line_id".into()];
        let mut options =
            ChildSpec::new("public.options", "options", "line_id", "line_id").expect("q");
        options.max_rows = Some(1);
        lines.children.push(options);
        lines
    }

    #[test]
    fn a_second_level_lands_inside_the_element_it_belongs_to() {
        let lines = lines_with_options();
        let mut doc = json!({"id": 1});
        apply_collection(
            &mut doc,
            &lines,
            json!([
                {ELEMENT_KEY: 10, "sku": "a"},
                {ELEMENT_KEY: 11, "sku": "b"},
                {ELEMENT_KEY: 12, "sku": "c"}
            ]),
            3,
        );
        assert_eq!(
            element_keys(&doc["lines"]),
            vec![json!(10), json!(11), json!(12)]
        );
        let fetched = vec![HashMap::from([
            (key_lookup(&json!(10)), (json!([{"color": "red"}]), 3)),
            (key_lookup(&json!(11)), (json!([{"color": "blue"}]), 1)),
        ])];
        let mut tallies = vec![NestedTally::default()];
        attach_nested(&mut doc["lines"], &lines, &fetched, &mut tallies);
        assert_eq!(
            doc["lines"],
            json!([
                {"sku": "a", "options": [{"color": "red"}],
                 "options_truncated": true, "options_total": 3},
                {"sku": "b", "options": [{"color": "blue"}]},
                {"sku": "c", "options": []}
            ]),
            "the cap is reported inside the element it cut, the key is gone, \
             and an element with no rows still holds the field"
        );
        assert!(doc.get("options_truncated").is_none(), "not on the root");
        assert_eq!(tallies[0].cut, 1);
        assert_eq!(tallies[0].largest, Some((json!(10), 3)));
    }

    #[test]
    fn an_element_shared_by_two_documents_is_asked_for_once() {
        let first = json!([{ELEMENT_KEY: 7}, {ELEMENT_KEY: 8}]);
        let second = json!([{ELEMENT_KEY: 8}, {ELEMENT_KEY: null}]);
        let single = json!({ELEMENT_KEY: 9});
        assert_eq!(
            distinct_element_keys([&first, &second, &single, &Value::Null]),
            vec![json!(7), json!(8), json!(9)]
        );
    }

    #[test]
    fn a_cut_second_level_says_so_once_naming_the_worst_element() {
        let lines = lines_with_options();
        let options = &lines.children[0];
        let mut tally = NestedTally::default();
        assert!(tally.cut_message(&lines, options).is_none());
        tally.largest = Some((json!(10), 3));
        assert!(
            tally.cut_message(&lines, options).is_none(),
            "a largest element that was not cut is not news"
        );
        tally.cut = 2;
        let message = tally.cut_message(&lines, options).expect("cut");
        assert!(message.starts_with("2 element(s) of lines"), "{message}");
        assert!(
            message.contains("options_truncated and options_total"),
            "{message}"
        );
        assert!(
            message.contains("public.lines row 10 with 3 rows"),
            "{message}"
        );
    }

    #[test]
    fn a_one_to_one_intermediate_holds_its_collection_too() {
        let mut lines = lines_with_options();
        lines.single = true;
        lines.children[0].max_rows = None;
        let mut doc = json!({"id": 1});
        apply_collection(&mut doc, &lines, json!([{ELEMENT_KEY: 10, "sku": "a"}]), 1);
        let fetched = vec![HashMap::from([(
            key_lookup(&json!(10)),
            (json!([{"color": "red"}, {"color": "blue"}]), 2),
        )])];
        attach_nested(
            &mut doc["lines"],
            &lines,
            &fetched,
            &mut [NestedTally::default()],
        );
        assert_eq!(
            doc["lines"],
            json!({"sku": "a", "options": [{"color": "red"}, {"color": "blue"}]})
        );

        // a parent with no intermediate row has nothing to attach to
        let mut empty = json!({"id": 2});
        apply_collection(&mut empty, &lines, json!([]), 0);
        assert!(element_keys(&empty["lines"]).is_empty());
        attach_nested(
            &mut empty["lines"],
            &lines,
            &fetched,
            &mut [NestedTally::default()],
        );
        assert_eq!(empty["lines"], json!(null));
    }

    #[test]
    fn a_second_level_table_is_routed_by_its_own_name() {
        let mut through = ChildSpec::new("public.tags", "tags", "user_id", "id").expect("q");
        through.through = Some(Through::new("public.user_tag", "tag_id").expect("q"));
        let specs = vec![through, lines_with_options()];
        assert!(matches!(
            route(&specs, "public", "user_tag"),
            Some(Route::Child(s)) if s.field == "tags"
        ));
        match route(&specs, "public", "options") {
            Some(Route::Grandchild { parent, spec }) => {
                assert_eq!(parent.field, "lines");
                assert_eq!(spec.foreign_key, "line_id", "read by its own key");
            }
            other => panic!("a grandchild routes through its parent: {other:?}"),
        }
        assert!(route(&specs, "public", "nobody").is_none());
    }

    #[test]
    fn the_nested_object_bound_multiplies_down_a_chain() {
        let mut lines = lines_with_options();
        lines.max_rows = Some(100);
        lines.children[0].max_rows = Some(200);
        assert_eq!(nested_object_bound(&lines), Some(100 * 201));
        lines.children[0].max_rows = None;
        assert_eq!(nested_object_bound(&lines), None, "an unbounded level");
        lines.children[0].single = true;
        assert_eq!(nested_object_bound(&lines), Some(200));
        lines.max_rows = None;
        assert_eq!(nested_object_bound(&lines), None);
    }

    #[test]
    fn only_an_intermediate_has_an_element_key() {
        let mut lines = lines_with_options();
        assert_eq!(lines.element_key_column(), Some("line_id"));
        lines.order_by.push("other".into());
        assert_eq!(lines.element_key_column(), None, "composite has none");
        assert_eq!(spec().element_key_column(), None, "no children, no key");
    }

    #[test]
    fn a_complete_collection_says_nothing_extra() {
        let mut doc = json!({"id": 1});
        let applied = apply_collection(&mut doc, &spec(), json!([{"id": 9}]), 1);
        assert!(!applied.truncated);
        assert_eq!(applied.matched, 1);
        assert_eq!(doc["orders"], json!([{"id": 9}]));
        assert!(doc.get("orders_truncated").is_none());
        assert!(doc.get("orders_total").is_none());
    }

    #[test]
    fn a_cut_collection_says_how_much_it_is_missing() {
        let mut doc = json!({"id": 1});
        assert!(apply_collection(&mut doc, &spec(), json!([{"id": 9}]), 500).truncated);
        assert_eq!(doc["orders_truncated"], json!(true));
        assert_eq!(doc["orders_total"], json!(500));
    }

    #[test]
    fn a_one_to_one_child_is_the_element_itself() {
        let mut doc = json!({"id": 1});
        let applied = apply_collection(&mut doc, &single_spec(), json!([{"bio": "hi"}]), 1);
        assert_eq!(doc["profile"], json!({"bio": "hi"}));
        assert_eq!(applied.matched, 1);
        assert!(!applied.truncated);
        assert!(doc.get("profile_truncated").is_none());
        assert!(doc.get("profile_total").is_none());
    }

    #[test]
    fn a_missing_one_to_one_child_is_null_under_its_own_name() {
        // the field is present either way, so a query for it need not know
        // whether the parent happens to have a child
        let mut doc = json!({"id": 1});
        assert_eq!(
            apply_collection(&mut doc, &single_spec(), json!([]), 0).matched,
            0
        );
        assert_eq!(doc["profile"], json!(null));
        assert!(doc.as_object().expect("object").contains_key("profile"));
    }

    #[test]
    fn a_second_matching_row_is_counted_not_chosen_between() {
        let spec = single_spec();
        let mut doc = json!({"id": 1});
        let applied = apply_collection(&mut doc, &spec, json!([{"id": 4}, {"id": 9}]), 2);
        assert_eq!(doc["profile"], json!({"id": 4}), "the lowest key stands");
        assert_eq!(applied.matched, 2);
        assert!(doc.get("profile_truncated").is_none(), "not a cap");

        let mut tally = Duplicates::default();
        tally.record(&spec, &json!(1), applied.matched);
        tally.record(&spec, &json!(2), 5);
        tally.record(&spec, &json!(3), 1);
        let message = tally.message(&spec).expect("two parents matched twice");
        assert!(message.contains("2 document(s)"), "{message}");
        assert!(message.contains("public.profiles"), "{message}");
        assert!(
            message.contains("parent 2 with 5"),
            "the worst offender names itself: {message}"
        );
    }

    #[test]
    fn an_array_collection_never_reports_a_duplicate() {
        let mut tally = Duplicates::default();
        tally.record(&spec(), &json!(1), 500);
        assert!(tally.message(&spec()).is_none());
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
    fn many_changed_children_of_one_collection_hold_one_key_each() {
        // A row of a through collection's child table names no parent, so it is
        // held under its own key until the junction is asked at commit — and a
        // transaction touching one author a hundred times asks about one.
        let mut pending = Pending::default();
        for _ in 0..100 {
            pending.name_by_child(table(), "authors", json!(7));
        }
        pending.name_by_child(table(), "authors", json!(8));
        // the same key on another collection of the same parent is another
        // question, and another junction to ask it of
        pending.name_by_child(table(), "editors", json!(7));
        assert_eq!(pending.len(), 3);
        assert!(!pending.is_empty(), "nothing has been resolved yet");

        let mut taken = pending.take_by_child();
        taken.sort_by_key(|((_, field), _)| field.clone());
        assert_eq!(taken.len(), 2, "one lookup per collection");
        assert_eq!(taken[0].0.1, "authors");
        assert_eq!(taken[0].1, vec![json!(7), json!(8)]);
        assert_eq!(taken[1].1, vec![json!(7)]);
        assert!(pending.is_empty(), "taking them is what resolves them");
    }

    #[test]
    fn a_parent_named_through_both_paths_is_read_once() {
        // The junction row and the child row of the same relation land in one
        // transaction all the time; what the resolved child key merges into is
        // the same named set, so the parent is still read once.
        let mut pending = Pending::default();
        pending.name_parent(table(), json!(1));
        pending.name_by_child(table(), "authors", json!(9));
        for (_, child_keys) in pending.take_by_child() {
            assert_eq!(child_keys, vec![json!(9)]);
            // what the junction lookup answers, merged in as any other key
            pending.name_parent(table(), json!(1));
        }
        let (_, named) = pending.take(&table());
        assert_eq!(named, vec![json!(1)]);
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
            kind: RowKind::Delete {
                pk: json!(9),
                before: None,
            },
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
