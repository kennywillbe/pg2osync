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

/// A configured document id: literals plus `{column}` placeholders, e.g.
/// `tenant-{tenant_id}-{id}`.
///
/// Identity renders from the row's RAW values — before projections and
/// before transforms — because it is a property of the row, not of the
/// projected document.
#[derive(Debug, Clone, PartialEq)]
pub struct IdTemplate {
    parts: Vec<IdPart>,
    /// Whether every placeholder names a primary-key column. A pk-only
    /// template can render a delete's id from the key tuple alone, which is
    /// all a PostgreSQL delete carries under a non-FULL replica identity;
    /// anything else needs the row's before-image.
    pk_only: bool,
}

#[derive(Debug, Clone, PartialEq)]
enum IdPart {
    Literal(String),
    Column(String),
}

impl IdTemplate {
    /// Parse and grammar-check the template. The primary-key columns decide
    /// whether the template can be rendered from a bare key.
    pub fn parse(spec: &str, pk_columns: &[String]) -> Result<Self, String> {
        let mut parts: Vec<IdPart> = Vec::new();
        let mut literal = String::new();
        let mut chars = spec.char_indices().peekable();
        while let Some((_, c)) = chars.next() {
            match c {
                '{' => {
                    let mut name = String::new();
                    loop {
                        match chars.next() {
                            None => return Err("unbalanced '{' in id template".into()),
                            Some((_, '}')) => break,
                            Some((_, '{')) => return Err("nested '{' in id template".into()),
                            Some((_, inner)) => name.push(inner),
                        }
                    }
                    if name.is_empty() {
                        return Err("empty placeholder {} in id template".into());
                    }
                    let valid = name.starts_with(|c: char| c.is_ascii_alphabetic() || c == '_')
                        && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_');
                    if !valid {
                        return Err(format!(
                            "placeholder '{{{name}}}' must name a column: [A-Za-z_][A-Za-z0-9_]*"
                        ));
                    }
                    if !literal.is_empty() {
                        parts.push(IdPart::Literal(std::mem::take(&mut literal)));
                    }
                    parts.push(IdPart::Column(name));
                }
                '}' => return Err("unbalanced '}' in id template".into()),
                c => literal.push(c),
            }
        }
        if !literal.is_empty() {
            parts.push(IdPart::Literal(literal));
        }
        if parts.is_empty() {
            return Err("id template is empty".into());
        }
        let columns: Vec<&str> = Self::column_names(&parts);
        let pk_only = columns.iter().all(|c| pk_columns.iter().any(|p| p == c));
        Ok(Self { parts, pk_only })
    }

    fn column_names(parts: &[IdPart]) -> Vec<&str> {
        let mut names: Vec<&str> = Vec::new();
        for part in parts {
            if let IdPart::Column(name) = part {
                let name = name.as_str();
                if !names.contains(&name) {
                    names.push(name);
                }
            }
        }
        names
    }

    /// Every column the template references, in first-occurrence order.
    pub fn columns(&self) -> Vec<&str> {
        Self::column_names(&self.parts)
    }

    pub fn is_pk_only(&self) -> bool {
        self.pk_only
    }

    /// Render the id from a row's raw document. A column the row does not
    /// carry, or that is NULL, is an error: an id that silently changed shape
    /// would strand the document the row already owns.
    pub fn render(&self, doc: &serde_json::Value) -> Result<String, String> {
        let map = doc
            .as_object()
            .ok_or("id template needs a row document, not a bare value")?;
        let mut out = String::new();
        for part in &self.parts {
            match part {
                IdPart::Literal(s) => out.push_str(s),
                IdPart::Column(c) => match map.get(c) {
                    None => return Err(format!("column {c} is missing from the row")),
                    Some(serde_json::Value::Null) => return Err(format!("column {c} is NULL")),
                    Some(v) => out.push_str(&scalar_display(v)),
                },
            }
        }
        Ok(out)
    }

    /// Render the id from a primary-key value alone: a scalar for a
    /// single-column key, or the `col → value` object for a composite one.
    /// A scalar binds to its only placeholder *when that placeholder is the
    /// key* — which is what `pk_only` records — because nothing in a bare
    /// value names the column it came from.
    pub fn render_from_pk(&self, pk: &serde_json::Value) -> Result<String, String> {
        match pk {
            serde_json::Value::Object(map) => {
                let doc = serde_json::Value::Object(map.clone());
                self.render(&doc)
            }
            serde_json::Value::Null => Err("the key tuple is NULL".into()),
            scalar => {
                let columns = self.columns();
                if columns.len() != 1 {
                    return Err(format!(
                        "id template names {} columns but the key is a single value",
                        columns.len()
                    ));
                }
                if !self.pk_only {
                    return Err(format!(
                        "column {} is not part of the key, so a bare key cannot render the id",
                        columns[0]
                    ));
                }
                let doc = serde_json::json!({ columns[0]: scalar });
                self.render(&doc)
            }
        }
    }
}

/// Per-table document-id templates from `[sync.x] id`, keyed (schema, table)
/// like the other per-table rules.
#[derive(Debug, Clone, Default)]
pub struct IdTemplates {
    map: HashMap<(String, String), IdTemplate>,
}

impl IdTemplates {
    pub fn from_pairs(pairs: impl IntoIterator<Item = ((String, String), IdTemplate)>) -> Self {
        Self {
            map: pairs.into_iter().collect(),
        }
    }

    pub fn for_table(&self, schema: &str, table: &str) -> Option<&IdTemplate> {
        self.map.get(&(schema.to_string(), table.to_string()))
    }
}

/// Render a primary-key JSON value into the OpenSearch `_id` string.
///
/// Scalars map directly; composite keys become a deterministic `col=val`
/// list so the same row always yields the same id.
pub fn pk_to_id(pk: &serde_json::Value) -> String {
    match pk {
        serde_json::Value::Null => "__null__".into(),
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::Bool(b) => b.to_string(),
        serde_json::Value::Object(map) => {
            let mut pairs: Vec<String> = map
                .iter()
                .map(|(k, v)| format!("{k}={}", scalar_display(v)))
                .collect();
            pairs.sort();
            pairs.join(",")
        }
        other => other.to_string(),
    }
}

pub(crate) fn scalar_display(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

/// Per-table fan-out rules from `[sync.x.fan_out]`: one row's JSON array
/// column becomes one document per element.
#[derive(Debug, Clone, Default)]
pub struct FanOuts {
    map: HashMap<(String, String), FanOut>,
}

/// One array column and the id its element documents are filed under.
#[derive(Debug, Clone)]
pub struct FanOut {
    pub field: String,
    pub id: IdTemplate,
}

impl FanOuts {
    pub fn from_pairs(pairs: impl IntoIterator<Item = ((String, String), FanOut)>) -> Self {
        Self {
            map: pairs.into_iter().collect(),
        }
    }

    pub fn for_table(&self, schema: &str, table: &str) -> Option<&FanOut> {
        self.map.get(&(schema.to_string(), table.to_string()))
    }
}

/// The documents one row expands to: `(id, doc)` per element, in array order.
///
/// A NULL array column keeps the row a single parent document under the base
/// id — an array that is absent is "nothing to index", an array that is NULL
/// is "a row with no children", and only the second reading has a document to
/// show for the row. An empty or missing array emits nothing. Element
/// documents are the parent-minus-array doc merged with the element, element
/// fields winning on name collision; a scalar element lands under the array's
/// own field name.
///
/// Ids render from the merged RAW docs, like every other id: identity is a
/// property of the row (and now of its element), not of the projected
/// document.
pub fn fan_out_docs(
    rule: &FanOut,
    base_id: &str,
    doc: &serde_json::Value,
) -> Result<Vec<(String, serde_json::Value)>, String> {
    let parent = doc
        .as_object()
        .ok_or("fan-out needs a row document, not a bare value")?;
    let items = match parent.get(&rule.field) {
        None => return Ok(Vec::new()),
        Some(serde_json::Value::Null) => return Ok(vec![(base_id.to_string(), doc.clone())]),
        Some(serde_json::Value::Array(items)) => items,
        Some(_) => {
            return Err(format!(
                "fan_out column {} is neither an array nor NULL",
                rule.field
            ));
        }
    };
    let mut out = Vec::with_capacity(items.len());
    for item in items {
        let mut child = parent.clone();
        child.remove(&rule.field);
        match item {
            serde_json::Value::Object(fields) => {
                for (k, v) in fields {
                    child.insert(k.clone(), v.clone());
                }
            }
            scalar => {
                child.insert(rule.field.clone(), scalar.clone());
            }
        }
        let child_doc = serde_json::Value::Object(child);
        out.push((rule.id.render(&child_doc)?, child_doc));
    }
    Ok(out)
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

    fn pk(spec: &str, pk_columns: &[&str]) -> IdTemplate {
        IdTemplate::parse(
            spec,
            &pk_columns.iter().map(|s| s.to_string()).collect::<Vec<_>>(),
        )
        .expect("valid template")
    }

    #[test]
    fn a_template_renders_from_the_row_its_given() {
        let t = pk("tenant-{tenant_id}-{id}", &["id", "tenant_id"]);
        assert!(t.is_pk_only());
        let doc = json!({"id": 7, "tenant_id": "acme", "email": null});
        assert_eq!(t.render(&doc).expect("renders"), "tenant-acme-7");
    }

    #[test]
    fn string_parts_are_unquoted_and_other_scalars_keep_their_json_form() {
        let t = pk("k-{ref}-{n}-{b}", &["ref"]);
        let doc = json!({"ref": "x y", "n": 1.5, "b": true});
        assert_eq!(t.render(&doc).expect("renders"), "k-x y-1.5-true");
    }

    #[test]
    fn a_null_or_missing_id_column_is_an_error_naming_the_column() {
        let t = pk("{tenant_id}-{id}", &["id"]);
        assert!(
            !t.is_pk_only(),
            "tenant_id is not the key, so a bare key cannot render this"
        );
        let err = t.render(&json!({"tenant_id": null, "id": 1})).unwrap_err();
        assert!(err.contains("tenant_id") && err.contains("NULL"), "{err}");
        let err = t.render(&json!({"id": 1})).unwrap_err();
        assert!(
            err.contains("tenant_id") && err.contains("missing"),
            "{err}"
        );
    }

    #[test]
    fn the_grammar_rejects_malformed_templates() {
        let bad = ["a-{", "a-}", "{}", "a-{ }", "a-{1x}", "a-{b-{c}}", ""];
        for spec in bad {
            assert!(
                IdTemplate::parse(spec, &[]).is_err(),
                "{spec:?} must not parse"
            );
        }
        assert!(IdTemplate::parse("plain-id", &[]).is_ok(), "literals only");
    }

    #[test]
    fn a_key_only_template_renders_from_a_bare_key() {
        let t = pk("user-{id}", &["id"]);
        assert_eq!(t.render_from_pk(&json!(42)).expect("scalar key"), "user-42");
        let c = pk("{tenant}-{id}", &["id", "tenant"]);
        assert_eq!(
            c.render_from_pk(&json!({"tenant": "acme", "id": 7}))
                .expect("composite key"),
            "acme-7"
        );
    }

    #[test]
    fn a_template_outside_the_key_refuses_a_bare_scalar_key() {
        // binding a key value to a placeholder that names another column
        // would invent an id, and the row's real document would be stranded
        let t = pk("ticket-{tag}", &["id"]);
        assert!(t.render_from_pk(&json!(42)).is_err());
    }

    #[test]
    fn fan_out_merges_elements_into_the_parent_minus_the_array() {
        let rule = FanOut {
            field: "tags".into(),
            id: pk("t-{id}-{tags}", &[]),
        };
        let doc = json!({"id": 1, "title": "x", "tags": ["a", "b"]});
        let docs = fan_out_docs(&rule, "t-1", &doc).expect("fans out");
        assert_eq!(
            docs,
            vec![
                (
                    "t-1-a".to_string(),
                    json!({"id": 1, "title": "x", "tags": "a"})
                ),
                (
                    "t-1-b".to_string(),
                    json!({"id": 1, "title": "x", "tags": "b"})
                ),
            ]
        );
    }

    #[test]
    fn fan_out_object_elements_win_their_names_and_the_array_is_gone() {
        let rule = FanOut {
            field: "items".into(),
            id: pk("o-{order_id}-{sku}", &[]),
        };
        let doc = json!({"order_id": 9, "items": [{"sku": "A", "qty": 2}, {"sku": "B", "qty": 1}], "title": "t"});
        let docs = fan_out_docs(&rule, "o-9", &doc).expect("fans out");
        assert_eq!(docs.len(), 2);
        assert_eq!(docs[0].0, "o-9-A");
        assert_eq!(
            docs[0].1,
            json!({"order_id": 9, "sku": "A", "qty": 2, "title": "t"})
        );
    }

    #[test]
    fn fan_out_null_keeps_one_parent_and_empty_or_missing_emits_nothing() {
        let rule = FanOut {
            field: "tags".into(),
            id: pk("t-{id}-{tags}", &[]),
        };
        let docs = fan_out_docs(&rule, "t-1", &json!({"id": 1, "tags": null})).expect("null");
        assert_eq!(
            docs,
            vec![("t-1".to_string(), json!({"id": 1, "tags": null}))]
        );
        assert!(
            fan_out_docs(&rule, "t-1", &json!({"id": 1, "tags": []}))
                .expect("empty")
                .is_empty()
        );
        assert!(
            fan_out_docs(&rule, "t-1", &json!({"id": 1}))
                .expect("missing")
                .is_empty()
        );
    }

    #[test]
    fn fan_out_refuses_a_column_that_holds_anything_else() {
        let rule = FanOut {
            field: "tags".into(),
            id: pk("t-{id}", &[]),
        };
        let err = fan_out_docs(&rule, "t-1", &json!({"id": 1, "tags": "a,b"})).unwrap_err();
        assert!(err.contains("tags"), "{err}");
    }
}
