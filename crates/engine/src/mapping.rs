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

#[derive(Debug, Clone, PartialEq)]
pub enum TransformOp {
    Hash,
    Redact,
    /// A string that holds a JSON document becomes that document.
    Json,
    /// A delimited string becomes an array of its trimmed, non-empty pieces.
    Split {
        by: String,
    },
    /// A string that holds a number becomes a JSON number.
    Number,
    /// A string in `from` (strptime syntax) becomes an ISO 8601 string.
    Date {
        from: String,
    },
}

impl TransformOp {
    /// The name the configuration knows this by, for a message that has to
    /// say which operation was asked for.
    pub fn name(&self) -> &'static str {
        match self {
            Self::Hash => "hash",
            Self::Redact => "redact",
            Self::Json => "json",
            Self::Split { .. } => "split",
            Self::Number => "number",
            Self::Date { .. } => "date",
        }
    }
}

/// What one operation did to one value.
///
/// `AlreadyShaped` is what makes the reshaping ops idempotent: a value that
/// is already what the op produces is not a failure, so a replayed row
/// reports nothing and the counter stays a signal rather than a hum.
enum Applied {
    Converted(serde_json::Value),
    AlreadyShaped,
    Unconvertible,
}

fn apply_op(op: &TransformOp, v: &serde_json::Value) -> Applied {
    use serde_json::Value;
    match op {
        TransformOp::Redact => Applied::Converted(Value::String("***".into())),
        TransformOp::Hash => {
            use sha2::{Digest, Sha256};
            let mut hasher = Sha256::new();
            hasher.update(v.to_string().as_bytes());
            let h = hasher.finalize();
            Applied::Converted(Value::String(
                h.iter().map(|b| format!("{b:02x}")).collect::<String>()[..16].to_string(),
            ))
        }
        // A JSON column legitimately holds a bare number or bool, and json /
        // jsonb columns already arrive parsed, so anything but a string is
        // taken to be the document itself rather than counted against the op.
        TransformOp::Json => match v {
            Value::String(s) => {
                serde_json::from_str(s).map_or(Applied::Unconvertible, Applied::Converted)
            }
            _ => Applied::AlreadyShaped,
        },
        // `by` is never empty here: the configuration refuses it, and an
        // empty pattern would split between every character.
        TransformOp::Split { by } => match v {
            Value::String(s) => Applied::Converted(Value::Array(
                s.split(by.as_str())
                    .map(str::trim)
                    .filter(|piece| !piece.is_empty())
                    .map(|piece| Value::String(piece.to_string()))
                    .collect(),
            )),
            Value::Array(_) => Applied::AlreadyShaped,
            _ => Applied::Unconvertible,
        },
        // Integers first: 9007199254740993 is exact as an i64 and lossy as a
        // double, and money is why these arrive as strings to begin with.
        // `from_f64` refuses what Rust parses but JSON cannot hold (NaN, inf,
        // 1e400), so those count as unconvertible instead of panicking.
        TransformOp::Number => match v {
            Value::String(s) => {
                let s = s.trim();
                if let Ok(i) = s.parse::<i64>() {
                    Applied::Converted(Value::from(i))
                } else if let Some(n) = s.parse::<f64>().ok().and_then(serde_json::Number::from_f64)
                {
                    Applied::Converted(Value::Number(n))
                } else {
                    Applied::Unconvertible
                }
            }
            Value::Number(_) => Applied::AlreadyShaped,
            _ => Applied::Unconvertible,
        },
        // Each of chrono's parsers demands exactly the fields its type has: a
        // format carrying an offset parses only into DateTime, a date-only
        // format only into NaiveDate. Most specific first, so a value that
        // carries an offset keeps it instead of having it silently dropped.
        // This op is not idempotent (an ISO value will not parse through
        // `%d/%m/%Y`), but no value meets it twice: a completed TOAST column
        // is skipped, and probing for "already ISO" would hide a wrong `from`.
        TransformOp::Date { from } => match v {
            Value::String(s) => {
                let s = s.trim();
                if let Ok(dt) = chrono::DateTime::<chrono::FixedOffset>::parse_from_str(s, from) {
                    Applied::Converted(Value::String(dt.to_rfc3339()))
                } else if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(s, from) {
                    Applied::Converted(Value::String(dt.format("%Y-%m-%dT%H:%M:%S").to_string()))
                } else if let Ok(d) = chrono::NaiveDate::parse_from_str(s, from) {
                    Applied::Converted(Value::String(d.format("%Y-%m-%d").to_string()))
                } else {
                    Applied::Unconvertible
                }
            }
            _ => Applied::Unconvertible,
        },
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
        // the caller has nowhere to report to; the engine uses apply_except
        self.apply_except(schema, table, doc, &[]);
    }

    /// The same, leaving `shaped` columns alone: a value completed from the
    /// stored document was transformed when it was first written, and a hash
    /// of a hash would never match what a fresh write of the row produces.
    ///
    /// Returns the columns whose value the operation could not convert. Those
    /// values are written exactly as they arrived: the target's mapping is the
    /// arbiter of what it will hold, and halting the pipeline on one row — or
    /// nulling the field — would cost more than it saves.
    pub fn apply_except<'a>(
        &'a self,
        schema: &str,
        table: &str,
        doc: &mut serde_json::Value,
        shaped: &[String],
    ) -> Vec<&'a str> {
        let mut left = Vec::new();
        let Some(rules) = self.for_table(schema, table) else {
            return left;
        };
        let Some(doc_map) = doc.as_object_mut() else {
            return left;
        };
        for (col, op) in rules {
            if shaped.contains(col) {
                continue;
            }
            if let Some(v) = doc_map.get_mut(col) {
                // NULL carries no value to reshape
                if v.is_null() {
                    continue;
                }
                match apply_op(op, v) {
                    Applied::Converted(new) => *v = new,
                    Applied::AlreadyShaped => {}
                    Applied::Unconvertible => left.push(col.as_str()),
                }
            }
        }
        left
    }
}

/// Per-table field renames from `[sync.x.fields]` and the `fields` of each
/// `[[sync.x.children]]`.
///
/// Applied last — after projection and transforms — so every other rule keeps
/// naming the column as the source knows it, and identity, projection and
/// transforms never have to know a rename exists.
#[derive(Debug, Clone, Default)]
pub struct Renames {
    map: HashMap<(String, String), Rename>,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct Rename {
    /// Source column → target field on the document itself.
    pub columns: HashMap<String, String>,
    /// Child field on the parent document → (child column → target field),
    /// applied to every object of that embedded array.
    pub nested: HashMap<String, HashMap<String, String>>,
}

impl Renames {
    pub fn from_pairs(pairs: impl IntoIterator<Item = ((String, String), Rename)>) -> Self {
        Self {
            map: pairs.into_iter().collect(),
        }
    }

    /// The name `col` of `schema.table` is stored under in the target — the
    /// column itself when nothing renames it. TOAST completion reads the
    /// stored document, which is the one place the target name leaks back
    /// into a pipeline that otherwise thinks in source names.
    pub fn target_name<'a>(&'a self, schema: &str, table: &str, col: &'a str) -> &'a str {
        self.map
            .get(&(schema.to_string(), table.to_string()))
            .and_then(|r| r.columns.get(col))
            .map_or(col, String::as_str)
    }

    /// Rename in place: the document's own fields, then the objects inside
    /// each embedded child array.
    pub fn apply(&self, schema: &str, table: &str, doc: &mut serde_json::Value) {
        let Some(rule) = self.map.get(&(schema.to_string(), table.to_string())) else {
            return;
        };
        let Some(obj) = doc.as_object_mut() else {
            return;
        };
        rename_keys(obj, &rule.columns);
        for (field, columns) in &rule.nested {
            if let Some(serde_json::Value::Array(rows)) = obj.get_mut(field) {
                for row in rows.iter_mut().filter_map(serde_json::Value::as_object_mut) {
                    rename_keys(row, columns);
                }
            }
        }
    }
}

/// Every renamed key leaves the object before any lands under its new name,
/// so a swap is well defined and a rename onto a surviving field is the one
/// deterministic thing it can be: the renamed value wins.
fn rename_keys(
    obj: &mut serde_json::Map<String, serde_json::Value>,
    map: &HashMap<String, String>,
) {
    let moved: Vec<(&String, serde_json::Value)> = map
        .iter()
        .filter_map(|(from, to)| obj.remove(from).map(|v| (to, v)))
        .collect();
    for (to, v) in moved {
        obj.insert(to.clone(), v);
    }
}

/// Per-table fields that come from no column, from `[sync.x.constants]`.
///
/// The values arrive rendered: `{schema}`/`{table}` were resolved once at
/// startup, so nothing per row parses a template and the engine only inserts
/// what it was handed. Applied after projection, transforms and renames —
/// a constant is not a column, so `columns` would otherwise strip it.
#[derive(Debug, Clone, Default)]
pub struct Constants {
    map: HashMap<(String, String), HashMap<String, serde_json::Value>>,
}

impl Constants {
    pub fn from_pairs(
        pairs: impl IntoIterator<Item = ((String, String), HashMap<String, serde_json::Value>)>,
    ) -> Self {
        Self {
            map: pairs.into_iter().collect(),
        }
    }

    /// Add the section's constants in place; a field of the same name is
    /// overwritten, which is the one deterministic reading of a collision
    /// the configuration checks could not see.
    pub fn apply(&self, schema: &str, table: &str, doc: &mut serde_json::Value) {
        let Some(fields) = self.map.get(&(schema.to_string(), table.to_string())) else {
            return;
        };
        let Some(obj) = doc.as_object_mut() else {
            return;
        };
        for (name, value) in fields {
            obj.insert(name.clone(), value.clone());
        }
    }
}

/// Per-table row filters from `[sync.x] where`.
///
/// The same predicate the initial load pushed into its query, evaluated here
/// for every streamed and polled row — a stream has no query to push it into,
/// and a row that has left the filter has to become a delete rather than
/// simply stop arriving.
#[derive(Debug, Clone, Default)]
pub struct Filters {
    map: HashMap<(String, String), pg2osync_core::filter::Filter>,
}

impl Filters {
    pub fn from_pairs(
        pairs: impl IntoIterator<Item = ((String, String), pg2osync_core::filter::Filter)>,
    ) -> Self {
        Self {
            map: pairs.into_iter().collect(),
        }
    }

    pub fn for_table(&self, schema: &str, table: &str) -> Option<&pg2osync_core::filter::Filter> {
        self.map.get(&(schema.to_string(), table.to_string()))
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
    fn a_column_already_shaped_is_left_alone() {
        let t = Transforms::from_pairs([(
            ("public".into(), "users".into()),
            HashMap::from([("ssn".to_string(), TransformOp::Hash)]),
        )]);
        let mut doc = json!({"ssn": "already-a-digest"});
        t.apply_except("public", "users", &mut doc, &["ssn".to_string()]);
        assert_eq!(doc, json!({"ssn": "already-a-digest"}));
    }

    fn users_transforms(pairs: &[(&str, TransformOp)]) -> Transforms {
        Transforms::from_pairs([(
            ("public".into(), "users".into()),
            pairs
                .iter()
                .map(|(col, op)| (col.to_string(), op.clone()))
                .collect(),
        )])
    }

    #[test]
    fn json_parses_a_string_and_leaves_objects_and_garbage_alone() {
        let t = users_transforms(&[
            ("payload", TransformOp::Json),
            ("already", TransformOp::Json),
            ("bare", TransformOp::Json),
            ("broken", TransformOp::Json),
        ]);
        let mut doc = json!({
            "payload": "{\"k\": 1}",
            "already": {"k": 2},
            "bare": 3,
            "broken": "not json",
        });
        let left = t.apply_except("public", "users", &mut doc, &[]);
        assert_eq!(doc["payload"], json!({"k": 1}));
        assert_eq!(
            doc["already"],
            json!({"k": 2}),
            "a parsed column is the document"
        );
        assert_eq!(doc["bare"], json!(3));
        assert_eq!(doc["broken"], json!("not json"), "left as it arrived");
        assert_eq!(left, vec!["broken"], "only garbage counts");
    }

    #[test]
    fn split_trims_and_drops_empty_pieces() {
        let by = || TransformOp::Split { by: ",".into() };
        let t = users_transforms(&[
            ("tags", by()),
            ("none", by()),
            ("done", by()),
            ("num", by()),
        ]);
        let mut doc = json!({"tags": "a, b ,,c", "none": "", "done": ["x"], "num": 4});
        let left = t.apply_except("public", "users", &mut doc, &[]);
        assert_eq!(doc["tags"], json!(["a", "b", "c"]));
        assert_eq!(doc["none"], json!([]), "an empty string names no pieces");
        assert_eq!(doc["done"], json!(["x"]));
        assert_eq!(doc["num"], json!(4));
        assert_eq!(left, vec!["num"]);
    }

    #[test]
    fn number_parses_integers_and_floats_and_leaves_the_rest() {
        let t = users_transforms(&[
            ("int", TransformOp::Number),
            ("big", TransformOp::Number),
            ("float", TransformOp::Number),
            ("nan", TransformOp::Number),
            ("huge", TransformOp::Number),
            ("word", TransformOp::Number),
            ("done", TransformOp::Number),
        ]);
        let mut doc = json!({
            "int": " 42 ", "big": "9007199254740993", "float": "99.99",
            "nan": "NaN", "huge": "1e400", "word": "abc", "done": 7,
        });
        let mut left = t.apply_except("public", "users", &mut doc, &[]);
        left.sort_unstable();
        assert!(
            doc["int"].is_i64(),
            "an integer stays an integer, not a double"
        );
        assert_eq!(doc["big"], json!(9007199254740993i64), "exact past 2^53");
        assert_eq!(doc["float"], json!(99.99));
        assert_eq!(
            doc["nan"],
            json!("NaN"),
            "JSON has no NaN; the value is left, not nulled"
        );
        assert_eq!(doc["huge"], json!("1e400"));
        assert_eq!(doc["word"], json!("abc"));
        assert_eq!(doc["done"], json!(7));
        assert_eq!(left, vec!["huge", "nan", "word"]);
    }

    #[test]
    fn date_normalizes_to_iso_8601_by_trying_offset_then_naive_then_date() {
        let t = users_transforms(&[
            (
                "day",
                TransformOp::Date {
                    from: "%d/%m/%Y".into(),
                },
            ),
            (
                "at",
                TransformOp::Date {
                    from: "%Y-%m-%d %H:%M:%S".into(),
                },
            ),
            (
                "zoned",
                TransformOp::Date {
                    from: "%Y-%m-%dT%H:%M:%S%z".into(),
                },
            ),
            (
                "bad",
                TransformOp::Date {
                    from: "%d/%m/%Y".into(),
                },
            ),
            (
                "num",
                TransformOp::Date {
                    from: "%d/%m/%Y".into(),
                },
            ),
        ]);
        let mut doc = json!({
            "day": "01/03/2024", "at": "2024-03-01 10:00:00",
            "zoned": "2024-03-01T10:00:00+0200", "bad": "nope", "num": 5,
        });
        let mut left = t.apply_except("public", "users", &mut doc, &[]);
        left.sort_unstable();
        assert_eq!(doc["day"], json!("2024-03-01"));
        assert_eq!(doc["at"], json!("2024-03-01T10:00:00"));
        assert_eq!(
            doc["zoned"],
            json!("2024-03-01T10:00:00+02:00"),
            "the offset is kept"
        );
        assert_eq!(doc["bad"], json!("nope"));
        assert_eq!(left, vec!["bad", "num"]);
    }

    #[test]
    fn a_null_is_left_alone_by_every_op_and_counts_for_none() {
        let t = users_transforms(&[
            ("a", TransformOp::Hash),
            ("b", TransformOp::Redact),
            ("c", TransformOp::Json),
            ("d", TransformOp::Split { by: ",".into() }),
            ("e", TransformOp::Number),
            ("f", TransformOp::Date { from: "%Y".into() }),
        ]);
        let mut doc = json!({"a": null, "b": null, "c": null, "d": null, "e": null, "f": null});
        let left = t.apply_except("public", "users", &mut doc, &[]);
        assert_eq!(
            doc,
            json!({"a": null, "b": null, "c": null, "d": null, "e": null, "f": null})
        );
        assert!(left.is_empty());
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

    fn users_renames(columns: &[(&str, &str)], nested: &[(&str, &[(&str, &str)])]) -> Renames {
        let pairs = |m: &[(&str, &str)]| -> HashMap<String, String> {
            m.iter()
                .map(|(a, b)| (a.to_string(), b.to_string()))
                .collect()
        };
        Renames::from_pairs([(
            ("public".into(), "users".into()),
            Rename {
                columns: pairs(columns),
                nested: nested
                    .iter()
                    .map(|(field, m)| (field.to_string(), pairs(m)))
                    .collect(),
            },
        )])
    }

    #[test]
    fn renames_move_top_level_fields_and_leave_other_tables_alone() {
        let r = users_renames(&[("usr_nm", "username")], &[]);
        let mut doc = json!({"usr_nm": "alice", "id": 1});
        r.apply("public", "users", &mut doc);
        assert_eq!(doc, json!({"username": "alice", "id": 1}));

        let mut other = json!({"usr_nm": "alice"});
        r.apply("public", "orders", &mut other);
        assert_eq!(other, json!({"usr_nm": "alice"}));
    }

    #[test]
    fn renames_reach_into_every_object_of_a_child_array() {
        let r = users_renames(&[], &[("orders", &[("total", "amount")])]);
        let mut doc = json!({
            "id": 1,
            "orders": [{"total": 1}, {"total": 2}, null],
            "orders_total": 3,
            "orders_truncated": true,
        });
        r.apply("public", "users", &mut doc);
        assert_eq!(
            doc,
            json!({
                "id": 1,
                "orders": [{"amount": 1}, {"amount": 2}, null],
                "orders_total": 3,
                "orders_truncated": true,
            }),
            "objects are renamed, a null element and the cap fields are not"
        );
    }

    #[test]
    fn a_rename_after_a_transform_keeps_the_transformed_value() {
        let t = Transforms::from_pairs([(
            ("public".into(), "users".into()),
            HashMap::from([("email".to_string(), TransformOp::Redact)]),
        )]);
        let r = users_renames(&[("email", "contact")], &[]);
        let mut doc = json!({"email": "a@b.c"});
        t.apply("public", "users", &mut doc);
        r.apply("public", "users", &mut doc);
        assert_eq!(doc, json!({"contact": "***"}));
    }

    #[test]
    fn swapped_renames_are_well_defined() {
        let r = users_renames(&[("a", "b"), ("b", "a")], &[]);
        let mut doc = json!({"a": 1, "b": 2});
        r.apply("public", "users", &mut doc);
        assert_eq!(doc, json!({"a": 2, "b": 1}));
    }

    #[test]
    fn target_name_is_identity_when_unmapped() {
        let r = users_renames(&[("bio", "about")], &[]);
        assert_eq!(r.target_name("public", "users", "bio"), "about");
        assert_eq!(r.target_name("public", "users", "id"), "id");
        assert_eq!(r.target_name("public", "orders", "bio"), "bio");
    }

    fn users_constants(pairs: &[(&str, serde_json::Value)]) -> Constants {
        Constants::from_pairs([(
            ("public".into(), "users".into()),
            pairs
                .iter()
                .map(|(k, v)| (k.to_string(), v.clone()))
                .collect(),
        )])
    }

    #[test]
    fn constants_are_added_and_leave_other_tables_alone() {
        let c = users_constants(&[("entity", json!("user")), ("rank", json!(3))]);
        let mut doc = json!({"id": 1});
        c.apply("public", "users", &mut doc);
        assert_eq!(doc, json!({"id": 1, "entity": "user", "rank": 3}));

        let mut other = json!({"id": 1});
        c.apply("public", "orders", &mut other);
        assert_eq!(other, json!({"id": 1}));
    }

    #[test]
    fn a_constant_wins_over_a_field_of_the_same_name() {
        let c = users_constants(&[("entity", json!("user"))]);
        let mut doc = json!({"entity": "row"});
        c.apply("public", "users", &mut doc);
        assert_eq!(doc, json!({"entity": "user"}));
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
