//! Logical table → index mapping and the shared durable-LSN cell.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::pseudonym::PseudonymKey;

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
    /// Environment variable name -> the key it held, for every `pseudonym`.
    /// `with_keys` is the only way in, and it refuses a set that does not
    /// cover every rule.
    keys: HashMap<String, PseudonymKey>,
}

/// A `pseudonym` rule whose `key_env` no entry of the key ring names.
#[derive(Debug, thiserror::Error)]
#[error("{schema}.{table}.{column}: pseudonym key_env={key_env:?} has no value")]
pub struct MissingKey {
    pub schema: String,
    pub table: String,
    pub column: String,
    pub key_env: String,
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
    /// A value whose text form is a key of `map` becomes that key's label.
    Lookup {
        map: std::collections::BTreeMap<String, String>,
        default: Option<String>,
    },
    /// A value becomes a deterministic, reversible token under the key named
    /// by `key_env`, scoped so the token cannot be replayed elsewhere.
    ///
    /// The key material is deliberately absent: this enum is `Debug` and
    /// `Clone`, and it lives in rule maps that are cloned per table and
    /// printed when a configuration is dumped.
    Pseudonym {
        key_env: String,
        /// The associated data. `None` means the column's own
        /// `schema.table.column`, which two columns that must join have to
        /// override with the same explicit label.
        scope: Option<String>,
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
            Self::Lookup { .. } => "lookup",
            Self::Pseudonym { .. } => "pseudonym",
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
    /// Unconvertible, but with a value to write in place of the original: a
    /// `lookup` miss that has a `default`. Still counted — the dictionary did
    /// not know the value, and that is what the counter exists to make visible.
    Defaulted(serde_json::Value),
    /// Counted like `Unconvertible`, but the value does not survive: a
    /// protective op that cannot render its input must not publish it.
    Refused,
}

/// What one op needs to know beyond the value: the key ring, and where the
/// value came from, which is the default scope of a pseudonym.
struct OpCtx<'a> {
    keys: &'a HashMap<String, PseudonymKey>,
    schema: &'a str,
    table: &'a str,
    column: &'a str,
}

fn apply_op(op: &TransformOp, v: &serde_json::Value, ctx: &OpCtx<'_>) -> Applied {
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
        // The dictionary is closed and its keys are strings, so a value is
        // matched by its text form: a JSON number 1 and the string "1" are the
        // same key, and a bool matches through "true"/"false". An array or an
        // object has no such form and simply misses. Like `date`, this op is
        // not idempotent — a label is rarely a key of the same map — and for
        // the same reason it never meets a value twice: a completed TOAST
        // column is skipped.
        TransformOp::Lookup { map, default } => {
            let key = match v {
                Value::String(s) => Some(s.clone()),
                Value::Number(n) => Some(n.to_string()),
                Value::Bool(b) => Some(b.to_string()),
                _ => None,
            };
            match key.and_then(|k| map.get(&k)) {
                Some(label) => Applied::Converted(Value::String(label.clone())),
                None => match default {
                    Some(d) => Applied::Defaulted(Value::String(d.clone())),
                    None => Applied::Unconvertible,
                },
            }
        }
        // `with_keys` refuses a rule whose key is absent, so the lookup here
        // cannot miss; if it ever did, the honest answer is to redact rather
        // than index the value the op exists to hide.
        TransformOp::Pseudonym { key_env, scope } => {
            let Some(key) = ctx.keys.get(key_env) else {
                return Applied::Refused;
            };
            let default_scope;
            let scope = match scope {
                Some(s) => s.as_str(),
                None => {
                    default_scope = format!("{}.{}.{}", ctx.schema, ctx.table, ctx.column);
                    default_scope.as_str()
                }
            };
            // A number and a bool are rendered, not stringified as JSON the
            // way `hash` does it: a bigint primary key and the foreign key
            // pointing at it have to produce the same token, and one of the
            // two may well arrive as a string.
            let rendered = match v {
                Value::String(s) => s.clone(),
                Value::Number(n) => n.to_string(),
                Value::Bool(b) => b.to_string(),
                // An array or an object has no single value to tokenise, and
                // indexing it as it arrived would publish the PII.
                _ => return Applied::Refused,
            };
            key.token(scope, rendered.as_bytes())
                .map_or(Applied::Refused, |t| Applied::Converted(Value::String(t)))
        }
    }
}

impl Transforms {
    pub fn from_pairs(
        pairs: impl IntoIterator<Item = ((String, String), HashMap<String, TransformOp>)>,
    ) -> Self {
        Self {
            map: pairs.into_iter().collect(),
            keys: HashMap::new(),
        }
    }

    /// Attach the key ring, refusing any `pseudonym` it does not cover.
    ///
    /// Refusing here rather than at the row is the point: a key missing at run
    /// time leaves only bad options, and the least bad of them — redacting the
    /// column of every row — is a silent, total data loss that a start-up
    /// error is strictly better than.
    pub fn with_keys(mut self, keys: HashMap<String, PseudonymKey>) -> Result<Self, MissingKey> {
        for ((schema, table), rules) in &self.map {
            for (column, op) in rules {
                if let TransformOp::Pseudonym { key_env, .. } = op
                    && !keys.contains_key(key_env)
                {
                    return Err(MissingKey {
                        schema: schema.clone(),
                        table: table.clone(),
                        column: column.clone(),
                        key_env: key_env.clone(),
                    });
                }
            }
        }
        self.keys = keys;
        Ok(self)
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
    /// Returns the columns whose value the operation could not convert. A
    /// reshaping op leaves those values exactly as they arrived: the target's
    /// mapping is the arbiter of what it will hold, and halting the pipeline
    /// on one row — or nulling the field — would cost more than it saves. A
    /// protective op (`pseudonym`) is the exception and writes `***`, because
    /// publishing what it was asked to hide is worse than losing it.
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
                let ctx = OpCtx {
                    keys: &self.keys,
                    schema,
                    table,
                    column: col,
                };
                match apply_op(op, v, &ctx) {
                    Applied::Converted(new) => *v = new,
                    Applied::AlreadyShaped => {}
                    Applied::Unconvertible => left.push(col.as_str()),
                    Applied::Defaulted(new) => {
                        *v = new;
                        left.push(col.as_str());
                    }
                    Applied::Refused => {
                        *v = serde_json::Value::String("***".into());
                        left.push(col.as_str());
                    }
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
    /// applied to every object of that embedded array, or to the element
    /// itself when the child is `single`.
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
            match obj.get_mut(field) {
                Some(serde_json::Value::Array(rows)) => {
                    for row in rows.iter_mut().filter_map(serde_json::Value::as_object_mut) {
                        rename_keys(row, columns);
                    }
                }
                // a `single` child is the element itself, and an absent one is
                // null: the rename has to reach the object either way
                Some(serde_json::Value::Object(row)) => rename_keys(row, columns),
                _ => {}
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

/// One-to-one children whose element is lifted onto the parent document
/// instead of nested under a field, from `flatten = true`.
///
/// Applied after the renames, so what is lifted carries the names the child's
/// own `fields` chose — and before the constants, which are written last
/// whatever else the document holds. Nothing here has to decide a winner: a
/// lifted name that anything else on the document claims is refused at
/// configuration time.
#[derive(Debug, Clone, Default)]
pub struct Flattens {
    map: HashMap<(String, String), Vec<String>>,
}

impl Flattens {
    pub fn from_pairs(pairs: impl IntoIterator<Item = ((String, String), Vec<String>)>) -> Self {
        Self {
            map: pairs.into_iter().collect(),
        }
    }

    /// Lift each named child onto the document in place.
    ///
    /// A parent with no child row has nothing to lift, and the fields are
    /// absent rather than null: the source read no row, and a row of nulls
    /// would claim it read one.
    pub fn apply(&self, schema: &str, table: &str, doc: &mut serde_json::Value) {
        let Some(fields) = self.map.get(&(schema.to_string(), table.to_string())) else {
            return;
        };
        let Some(obj) = doc.as_object_mut() else {
            return;
        };
        for field in fields {
            // the field goes either way: a flattened child names nothing on
            // the document, not even when it matched nothing
            if let Some(serde_json::Value::Object(element)) = obj.remove(field) {
                obj.extend(element);
            }
        }
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

/// Per-table ingest pipelines from `[sync.x] pipeline`.
///
/// Keyed by table rather than by index because the pipeline is the section's
/// choice: two sections writing one index may each name their own, and the
/// name rides on the operation, so the sink never has to know which section a
/// document came from.
#[derive(Debug, Clone, Default)]
pub struct Pipelines {
    map: HashMap<(String, String), String>,
}

impl Pipelines {
    pub fn from_pairs(pairs: impl IntoIterator<Item = ((String, String), String)>) -> Self {
        Self {
            map: pairs.into_iter().collect(),
        }
    }

    pub fn for_table(&self, schema: &str, table: &str) -> Option<&str> {
        self.map
            .get(&(schema.to_string(), table.to_string()))
            .map(String::as_str)
    }
}

/// Per-table routing columns from `[sync.x] routing`.
///
/// Keyed by table for the same reason pipelines are: routing is the section's
/// choice, and it rides on the operation, so the sink never has to know which
/// section a document came from.
#[derive(Debug, Clone, Default)]
pub struct Routings {
    map: HashMap<(String, String), RoutingColumn>,
}

/// The column whose value decides a document's shard.
#[derive(Debug, Clone)]
pub struct RoutingColumn {
    /// Column on this table holding the routing value.
    pub column: String,
    /// Whether `column` is part of this table's configured key, and so
    /// readable from a key-only delete event. The same declaration
    /// `JoinParent::key_column` makes, and the same startup check makes it
    /// true.
    pub key_column: bool,
}

impl Routings {
    pub fn from_pairs(pairs: impl IntoIterator<Item = ((String, String), RoutingColumn)>) -> Self {
        Self {
            map: pairs.into_iter().collect(),
        }
    }

    pub fn for_table(&self, schema: &str, table: &str) -> Option<&RoutingColumn> {
        self.map.get(&(schema.to_string(), table.to_string()))
    }
}

impl RoutingColumn {
    /// The routing this row's documents are written under.
    ///
    /// Reads the RAW row, like identity does: a projection must not be able to
    /// move a document to another shard. An empty value is an error rather
    /// than "no routing": the target rejects an empty routing outright, and
    /// silently writing the document to its default shard would hide half a
    /// tenant somewhere no routed query looks.
    pub fn render(&self, raw: &serde_json::Value) -> Result<String, String> {
        let map = raw
            .as_object()
            .ok_or("a routing column needs a row document, not a bare value")?;
        match map.get(&self.column) {
            None => Err(format!("column {} is missing from the row", self.column)),
            Some(serde_json::Value::Null) => Err(format!(
                "column {} is NULL, so the document has no routing",
                self.column
            )),
            Some(value) => self.usable(scalar_display(value)),
        }
    }

    /// The routing a key-only event's document is filed under. The same order
    /// and the same reasoning as a derived id: the before-image is the row
    /// exactly as the target last saw it; without one the key has to carry the
    /// routing column, either as a member of a composite key or as the key
    /// itself, which `key_column` is the startup check's promise of.
    pub fn render_from_key(
        &self,
        before: Option<&serde_json::Value>,
        pk: &serde_json::Value,
    ) -> Result<String, String> {
        if let Some(before) = before {
            return self.render(before);
        }
        if let Some(value) = pk.as_object().and_then(|map| map.get(&self.column)) {
            return match value {
                serde_json::Value::Null => Err(format!(
                    "column {} is NULL, so the document has no routing",
                    self.column
                )),
                value => self.usable(scalar_display(value)),
            };
        }
        if self.key_column && !pk.is_object() {
            return self.usable(scalar_display(pk));
        }
        Err(format!(
            "a routed row's delete needs its {}, but this event carries no before-image; \
             the table needs REPLICA IDENTITY FULL",
            self.column
        ))
    }

    fn usable(&self, routing: String) -> Result<String, String> {
        if routing.is_empty() {
            return Err(format!(
                "column {} is empty, so the document has no routing",
                self.column
            ));
        }
        Ok(routing)
    }
}

/// Tables declared `[sync.x] append_only = true`: they have no key the
/// pipeline may address a row by, so a row is only ever inserted and is filed
/// under a hash of its content unless the section configures an id.
#[derive(Debug, Clone, Default)]
pub struct AppendOnly {
    tables: HashSet<(String, String)>,
}

impl FromIterator<(String, String)> for AppendOnly {
    fn from_iter<I: IntoIterator<Item = (String, String)>>(pairs: I) -> Self {
        Self {
            tables: pairs.into_iter().collect(),
        }
    }
}

impl AppendOnly {
    pub fn contains(&self, schema: &str, table: &str) -> bool {
        self.tables
            .contains(&(schema.to_string(), table.to_string()))
    }
}

/// The document id of a row that has no key: a hash of the row itself.
///
/// Hashed from the RAW document, like every other identity, and over its
/// canonical JSON: the workspace does not enable serde_json's `preserve_order`,
/// so object keys are sorted and the same row serialises the same whichever
/// path delivered it — a COPY or load, the WAL or binlog, a poll — which is
/// what lets a replayed row land on the document it already is. 32 hex
/// characters is 128 bits, enough that two distinct rows never collide in
/// practice, and short enough to read in an index.
pub fn content_id(doc: &serde_json::Value) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(doc.to_string().as_bytes());
    hasher
        .finalize()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect::<String>()[..32]
        .to_string()
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

    /// Every literal between the placeholders, in order.
    pub fn literals(&self) -> Vec<&str> {
        self.parts
            .iter()
            .filter_map(|part| match part {
                IdPart::Literal(s) => Some(s.as_str()),
                IdPart::Column(_) => None,
            })
            .collect()
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

/// The grammar an index name has to satisfy, in one place: config checks a
/// fixed name at load, and a name a row rendered is checked where it is
/// rendered — the only thing that can be done about a bad one there is halt.
pub fn check_index_name(name: &str) -> Result<(), String> {
    if !name.chars().next().is_some_and(|c| c.is_ascii_lowercase()) {
        return Err("must start with a lowercase letter".into());
    }
    if !name
        .chars()
        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_' || c == '-')
    {
        return Err("may only contain lowercase [a-z0-9_-]".into());
    }
    // OpenSearch rejects names starting with '_'; dot-prefix is reserved for
    // system indices. Kept as its own rule so the reason is on record even
    // though the first check already turns both away.
    if name.starts_with('_') || name.starts_with('.') {
        return Err("must not start with '_' or '.'".into());
    }
    Ok(())
}

/// Where one table's documents go: a fixed index, or one the row renders.
///
/// A per-row index is the identity problem again — a column that decides the
/// name is a column that can change, and the document then lives in the old
/// index — so it is the same template type, rendered from the same raw row.
#[derive(Debug, Clone, PartialEq)]
pub enum IndexTarget {
    Static(String),
    /// The spec as written is kept because a rendered name that is not a
    /// legal index has to say which template produced it.
    Template {
        spec: String,
        template: IdTemplate,
    },
}

impl IndexTarget {
    /// The index for the row `doc` describes. A rendered name is
    /// grammar-checked here, so no caller can forget to.
    pub fn render(&self, doc: &serde_json::Value) -> Result<String, String> {
        match self {
            Self::Static(name) => Ok(name.clone()),
            Self::Template { spec, template } => {
                Self::usable(spec, template, template.render(doc)?)
            }
        }
    }

    /// The index from a primary-key value alone, under the same rule
    /// `IdTemplate::render_from_pk` applies.
    pub fn render_from_pk(&self, pk: &serde_json::Value) -> Result<String, String> {
        match self {
            Self::Static(name) => Ok(name.clone()),
            Self::Template { spec, template } => {
                Self::usable(spec, template, template.render_from_pk(pk)?)
            }
        }
    }

    fn usable(spec: &str, template: &IdTemplate, name: String) -> Result<String, String> {
        check_index_name(&name).map_err(|why| {
            format!(
                "the index template {spec:?} rendered {name:?} from {}, which is not a usable \
                 index name: {why}",
                template.columns().join(", ")
            )
        })?;
        Ok(name)
    }

    /// Whether a bare key can render this. A fixed index needs nothing at all.
    pub fn is_pk_only(&self) -> bool {
        match self {
            Self::Static(_) => true,
            Self::Template { template, .. } => template.is_pk_only(),
        }
    }

    /// The wildcard a TRUNCATE of this table has to clear: each placeholder
    /// becomes `*`, adjacent stars collapse. `events-{tenant}` -> `events-*`.
    /// A fixed index is its own pattern, so one rule serves both.
    pub fn pattern(&self) -> String {
        match self {
            Self::Static(name) => name.clone(),
            Self::Template { template, .. } => {
                let mut out = String::new();
                for part in &template.parts {
                    match part {
                        IdPart::Literal(s) => out.push_str(s),
                        IdPart::Column(_) if out.ends_with('*') => {}
                        IdPart::Column(_) => out.push('*'),
                    }
                }
                out
            }
        }
    }

    /// The index as the configuration wrote it.
    pub fn spec(&self) -> &str {
        match self {
            Self::Static(name) => name,
            Self::Template { spec, .. } => spec,
        }
    }
}

impl From<String> for IndexTarget {
    fn from(name: String) -> Self {
        Self::Static(name)
    }
}

impl From<&str> for IndexTarget {
    fn from(name: &str) -> Self {
        Self::Static(name.to_string())
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
    /// The separator a delimited string column is cut on. Unset means the
    /// column already holds an array.
    pub by: Option<String>,
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
///
/// With `by` set the column is a delimited string and fan-out cuts it here,
/// on the raw row, with the same trimming and empty-dropping the `split`
/// transform documents — the column type is what the two rules differ on, so
/// each is refused where the other applies.
pub fn fan_out_docs(
    rule: &FanOut,
    base_id: &str,
    doc: &serde_json::Value,
) -> Result<Vec<(String, serde_json::Value)>, String> {
    let parent = doc
        .as_object()
        .ok_or("fan-out needs a row document, not a bare value")?;
    let items: std::borrow::Cow<'_, [serde_json::Value]> = match (parent.get(&rule.field), &rule.by)
    {
        (None, _) => return Ok(Vec::new()),
        (Some(serde_json::Value::Null), _) => {
            return Ok(vec![(base_id.to_string(), doc.clone())]);
        }
        (Some(serde_json::Value::Array(items)), None) => std::borrow::Cow::Borrowed(items),
        (Some(serde_json::Value::String(s)), Some(by)) => std::borrow::Cow::Owned(
            s.split(by.as_str())
                .map(str::trim)
                .filter(|piece| !piece.is_empty())
                .map(|piece| serde_json::Value::String(piece.to_string()))
                .collect(),
        ),
        (Some(serde_json::Value::Array(_)), Some(by)) => {
            return Err(format!(
                "fan_out column {} holds an array, but by {by:?} splits a delimited string; \
                 remove by",
                rule.field
            ));
        }
        (Some(_), Some(_)) => {
            return Err(format!(
                "fan_out column {} is neither a string nor NULL, and by splits a string",
                rule.field
            ));
        }
        (Some(_), None) => {
            return Err(format!(
                "fan_out column {} is neither an array nor NULL",
                rule.field
            ));
        }
    };
    let mut out = Vec::with_capacity(items.len());
    for item in items.iter() {
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

/// Per-table join rules from `[sync.x.join]`: the join field a table's
/// documents are filed under and, for a child, the parent that holds them.
#[derive(Debug, Clone, Default)]
pub struct Joins {
    map: HashMap<(String, String), JoinRule>,
}

/// One table's place in a join field.
#[derive(Debug, Clone)]
pub struct JoinRule {
    /// The join field's name in the target document.
    pub field: String,
    /// This table's relation name inside it.
    pub name: String,
    /// Unset on the parent, whose documents keep the target's own routing.
    pub parent: Option<JoinParent>,
}

/// Where a child's parent is found: the column that names it, and how that
/// value becomes the parent's document id — which is also the child's routing.
#[derive(Debug, Clone)]
pub struct JoinParent {
    /// Column on this (child) table holding the parent's key.
    pub column: String,
    /// The parent table's relation name; the cascade is scoped by it.
    pub name: String,
    /// The parent section's id rule, applied to `column`'s value. It has to
    /// be the parent's own rule and nothing else, or the two documents never
    /// meet.
    pub id: ParentId,
    /// Whether `column` is part of this table's configured key, and so
    /// readable from a key-only delete event. The same declaration
    /// `IdTemplate::pk_only` makes, and the same startup check makes it true.
    pub key_column: bool,
    /// Whether the parent is the fanned element rather than a column of the
    /// row. `column` is then the fan-out field, which every element document
    /// carries in the array's place, so the parent is read per element.
    pub element: bool,
}

/// How a foreign-key value becomes the parent's document id.
#[derive(Debug, Clone)]
pub enum ParentId {
    /// The parent configures no `id`; its documents are filed under `pk_to_id`.
    Key,
    /// A key-only `id` template. One naming anything else is refused at config
    /// load: the child carries one column and could not render it.
    Template(IdTemplate),
}

impl ParentId {
    pub fn render(&self, key: &serde_json::Value) -> Result<String, String> {
        match self {
            Self::Key => Ok(pk_to_id(key)),
            Self::Template(template) => template.render_from_pk(key),
        }
    }
}

impl Joins {
    pub fn from_pairs(pairs: impl IntoIterator<Item = ((String, String), JoinRule)>) -> Self {
        Self {
            map: pairs.into_iter().collect(),
        }
    }

    pub fn for_table(&self, schema: &str, table: &str) -> Option<&JoinRule> {
        self.map.get(&(schema.to_string(), table.to_string()))
    }
}

impl JoinRule {
    /// The value the join field takes in this row's document, and the routing
    /// the document is written under. A parent is its bare relation name and
    /// keeps the target's own routing; a child names its parent, and lives on
    /// the parent's shard.
    ///
    /// Reads the RAW row, like identity does: a projection must not be able
    /// to move a document to another shard. A fanned row's element document
    /// is that same raw row with the element in the array's place, which is
    /// what an element parent reads its value from.
    pub fn routing_for_doc(
        &self,
        raw: &serde_json::Value,
    ) -> Result<(serde_json::Value, Option<String>), String> {
        let Some(parent) = &self.parent else {
            return Ok((serde_json::Value::String(self.name.clone()), None));
        };
        let map = raw
            .as_object()
            .ok_or("a join child needs a row document, not a bare value")?;
        let parent_id = match map.get(&parent.column) {
            None => {
                return Err(format!("column {} is missing from the row", parent.column));
            }
            Some(serde_json::Value::Null) => {
                return Err(format!(
                    "column {} is NULL, so the parent document has no name",
                    parent.column
                ));
            }
            Some(key) => parent.id.render(key)?,
        };
        let value = serde_json::json!({ "name": self.name, "parent": parent_id });
        Ok((value, Some(parent_id)))
    }

    /// The routing a key-only event's document is filed under. The same order
    /// and the same reasoning as a derived id: the before-image is the row
    /// exactly as the target last saw it; without one the key has to carry the
    /// parent column, either as a member of a composite key or as the key
    /// itself, which `key_column` is the startup check's promise of.
    pub fn routing_for_key(
        &self,
        before: Option<&serde_json::Value>,
        pk: &serde_json::Value,
    ) -> Result<Option<String>, String> {
        let Some(parent) = &self.parent else {
            return Ok(None);
        };
        // An element parent gives one row as many parents as it has elements,
        // and a key names none of them: the fanned paths route each element
        // document from the element itself instead.
        if parent.element {
            return Err(format!(
                "the parent of {} is the fanned element, which a key alone does not carry",
                self.name
            ));
        }
        if let Some(before) = before {
            return self.routing_for_doc(before).map(|(_, routing)| routing);
        }
        if let Some(key) = pk.as_object().and_then(|map| map.get(&parent.column)) {
            return parent.id.render(key).map(Some);
        }
        if parent.key_column && !pk.is_object() {
            return parent.id.render(pk).map(Some);
        }
        Err(format!(
            "a join child's delete needs its {}, but this event carries no before-image;              the table needs REPLICA IDENTITY FULL",
            parent.column
        ))
    }
}

/// Maps `(schema, table)` to where its documents go, from `[sync.*]` config.
#[derive(Debug, Clone, Default)]
pub struct TableMapping {
    map: HashMap<(String, String), IndexTarget>,
    /// Indices more than one table writes to. A TRUNCATE of one of them must
    /// not clear the index: the other tables' documents are in it, and nothing
    /// in the source would ever put them back. Only fixed names count: a
    /// template claims a namespace of its own, which config refuses to share.
    shared: HashSet<String>,
}

impl TableMapping {
    pub fn from_pairs<T: Into<IndexTarget>>(
        pairs: impl IntoIterator<Item = ((String, String), T)>,
    ) -> Self {
        let mut map = HashMap::new();
        let mut seen = HashSet::new();
        let mut shared = HashSet::new();
        for (table, target) in pairs {
            let target = target.into();
            if let IndexTarget::Static(index) = &target
                && !seen.insert(index.clone())
            {
                shared.insert(index.clone());
            }
            map.insert(table, target);
        }
        Self { map, shared }
    }

    pub fn target_for(&self, schema: &str, table: &str) -> Option<&IndexTarget> {
        self.map.get(&(schema.to_string(), table.to_string()))
    }

    pub fn is_shared(&self, index: &str) -> bool {
        self.shared.contains(index)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn a_content_id_depends_on_the_row_and_not_on_key_order() {
        let a = content_id(&json!({"kind": "login", "at": "t1"}));
        let b = content_id(&json!({"at": "t1", "kind": "login"}));
        assert_eq!(a, b, "the same row must hash the same on every path");
        assert_ne!(a, content_id(&json!({"at": "t2", "kind": "login"})));
        assert_eq!(a.len(), 32);
        assert!(a.chars().all(|c| c.is_ascii_hexdigit()));
    }

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
            ("g", lookup(&[("1", "active")], Some("unknown"))),
            ("h", pseudonym(None)),
        ])
        .with_keys(key_ring())
        .expect("the ring covers the rule");
        let mut doc = json!({
            "a": null, "b": null, "c": null, "d": null, "e": null, "f": null, "g": null,
            "h": null,
        });
        let left = t.apply_except("public", "users", &mut doc, &[]);
        assert_eq!(
            doc,
            json!({
                "a": null, "b": null, "c": null, "d": null, "e": null, "f": null, "g": null,
                "h": null,
            }),
            "neither a lookup default nor a pseudonym displaces a NULL"
        );
        assert!(left.is_empty());
    }

    fn lookup(entries: &[(&str, &str)], default: Option<&str>) -> TransformOp {
        TransformOp::Lookup {
            map: entries
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            default: default.map(str::to_string),
        }
    }

    #[test]
    fn lookup_matches_a_value_by_its_text_form() {
        let dict = || {
            lookup(
                &[("1", "active"), ("2", "suspended"), ("true", "yes")],
                None,
            )
        };
        let t = users_transforms(&[
            ("num", dict()),
            ("str", dict()),
            ("flag", dict()),
            ("arr", dict()),
        ]);
        let mut doc = json!({"num": 1, "str": "2", "flag": true, "arr": ["1"]});
        let left = t.apply_except("public", "users", &mut doc, &[]);
        assert_eq!(doc["num"], json!("active"), "a number matches its digits");
        assert_eq!(doc["str"], json!("suspended"));
        assert_eq!(doc["flag"], json!("yes"));
        assert_eq!(doc["arr"], json!(["1"]), "an array has no text form");
        assert_eq!(left, vec!["arr"]);
    }

    #[test]
    fn a_lookup_miss_keeps_the_value_or_takes_the_default_and_counts_either_way() {
        let t = users_transforms(&[
            ("kept", lookup(&[("1", "active")], None)),
            ("labelled", lookup(&[("1", "active")], Some("unknown"))),
            ("hit", lookup(&[("1", "active")], Some("unknown"))),
        ]);
        let mut doc = json!({"kept": "9", "labelled": "9", "hit": "1"});
        let mut left = t.apply_except("public", "users", &mut doc, &[]);
        left.sort_unstable();
        assert_eq!(doc["kept"], json!("9"), "no default: indexed as it arrived");
        assert_eq!(doc["labelled"], json!("unknown"));
        assert_eq!(doc["hit"], json!("active"));
        assert_eq!(
            left,
            vec!["kept", "labelled"],
            "a default is still a value the dictionary did not know"
        );
    }

    const KEY_VAR: &str = "PG2OSYNC_TEST_KEY";
    const KEY_HEX: &str = "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f\
                           202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f";

    fn key_ring() -> HashMap<String, PseudonymKey> {
        HashMap::from([(
            KEY_VAR.to_string(),
            PseudonymKey::from_hex(KEY_HEX).expect("a valid key"),
        )])
    }

    fn pseudonym(scope: Option<&str>) -> TransformOp {
        TransformOp::Pseudonym {
            key_env: KEY_VAR.to_string(),
            scope: scope.map(str::to_string),
        }
    }

    fn keyed_users(pairs: &[(&str, TransformOp)]) -> Transforms {
        users_transforms(pairs)
            .with_keys(key_ring())
            .expect("the ring covers every rule")
    }

    #[test]
    fn a_pseudonym_is_a_token_rather_than_the_value() {
        let t = keyed_users(&[("email", pseudonym(None))]);
        let mut doc = json!({"email": "alice@example.com"});
        let left = t.apply_except("public", "users", &mut doc, &[]);
        let token = doc["email"].as_str().expect("a string");
        assert_ne!(token, "alice@example.com");
        assert!(left.is_empty());
        // 16-byte synthetic IV plus the 17-byte plaintext, base64url unpadded
        assert_eq!(token.len(), 44);
    }

    #[test]
    fn a_number_and_a_bool_are_rendered_before_being_tokenised() {
        let t = keyed_users(&[("n", pseudonym(Some("s"))), ("b", pseudonym(Some("s")))]);
        let mut doc = json!({"n": 123, "b": true});
        assert!(t.apply_except("public", "users", &mut doc, &[]).is_empty());

        let mut text = json!({"n": "123", "b": "true"});
        assert!(t.apply_except("public", "users", &mut text, &[]).is_empty());
        assert_eq!(
            doc, text,
            "a bigint key and a textual foreign key must agree"
        );
    }

    #[test]
    fn an_array_or_an_object_is_redacted_and_counted() {
        let t = keyed_users(&[("tags", pseudonym(None)), ("meta", pseudonym(None))]);
        let mut doc = json!({"tags": ["a", "b"], "meta": {"k": "v"}});
        let mut left = t.apply_except("public", "users", &mut doc, &[]);
        left.sort_unstable();
        assert_eq!(doc, json!({"tags": "***", "meta": "***"}));
        assert_eq!(left, vec!["meta", "tags"]);
    }

    #[test]
    fn the_default_scope_separates_two_columns_of_one_value() {
        let t = keyed_users(&[("a", pseudonym(None)), ("b", pseudonym(None))]);
        let mut doc = json!({"a": "same", "b": "same"});
        t.apply("public", "users", &mut doc);
        assert_ne!(doc["a"], doc["b"]);
    }

    #[test]
    fn one_explicit_scope_makes_a_foreign_key_join() {
        let scope = Some("public.users.id");
        let users = Transforms::from_pairs([(
            ("public".into(), "users".into()),
            HashMap::from([("id".to_string(), pseudonym(scope))]),
        )])
        .with_keys(key_ring())
        .expect("keyed");
        let orders = Transforms::from_pairs([(
            ("public".into(), "orders".into()),
            HashMap::from([("user_id".to_string(), pseudonym(scope))]),
        )])
        .with_keys(key_ring())
        .expect("keyed");

        let mut user = json!({"id": 7});
        users.apply("public", "users", &mut user);
        let mut order = json!({"user_id": 7});
        orders.apply("public", "orders", &mut order);
        assert_eq!(user["id"], order["user_id"]);
    }

    #[test]
    fn with_keys_refuses_a_rule_whose_key_is_absent() {
        let err = users_transforms(&[("email", pseudonym(None))])
            .with_keys(HashMap::new())
            .expect_err("no key, no rules");
        assert_eq!(err.column, "email");
        assert_eq!(err.key_env, KEY_VAR);
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
    fn renames_reach_into_a_one_to_one_child_object() {
        let r = users_renames(&[], &[("profile", &[("bio", "about")])]);
        let mut doc = json!({"id": 1, "profile": {"bio": "hi"}});
        r.apply("public", "users", &mut doc);
        assert_eq!(doc, json!({"id": 1, "profile": {"about": "hi"}}));

        // an absent one-to-one child is null, and a rename has nothing to move
        let mut none = json!({"id": 1, "profile": null});
        r.apply("public", "users", &mut none);
        assert_eq!(none, json!({"id": 1, "profile": null}));
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

    fn users_flattens(fields: &[&str]) -> Flattens {
        Flattens::from_pairs([(
            ("public".into(), "users".into()),
            fields.iter().map(|f| f.to_string()).collect(),
        )])
    }

    #[test]
    fn a_flattened_child_lands_at_the_top_level_under_its_renamed_names() {
        let r = users_renames(&[], &[("company", &[("customer_name", "company_name")])]);
        let f = users_flattens(&["company"]);
        let mut doc = json!({"id": 1, "company": {"customer_name": "acme"}});
        r.apply("public", "users", &mut doc);
        f.apply("public", "users", &mut doc);
        assert_eq!(doc, json!({"id": 1, "company_name": "acme"}));

        let mut other = json!({"id": 1, "company": {"customer_name": "acme"}});
        f.apply("public", "orders", &mut other);
        assert_eq!(
            other,
            json!({"id": 1, "company": {"customer_name": "acme"}}),
            "another table's document keeps its shape"
        );
    }

    #[test]
    fn a_parent_with_no_child_row_carries_none_of_the_lifted_fields() {
        let f = users_flattens(&["company"]);
        let mut doc = json!({"id": 1, "company": null});
        f.apply("public", "users", &mut doc);
        assert_eq!(
            doc,
            json!({"id": 1}),
            "the field the child arrived under goes too"
        );

        let mut never = json!({"id": 1});
        f.apply("public", "users", &mut never);
        assert_eq!(never, json!({"id": 1}));
    }

    #[test]
    fn two_flattened_children_both_reach_the_document() {
        let f = users_flattens(&["company", "plan"]);
        let mut doc = json!({"id": 1, "company": {"name": "acme"}, "plan": {"tier": "gold"}});
        f.apply("public", "users", &mut doc);
        assert_eq!(doc, json!({"id": 1, "name": "acme", "tier": "gold"}));
    }

    #[test]
    fn a_flattened_child_lifts_only_what_the_projection_left() {
        // the read projects, so the element holds the chosen columns and the
        // lift carries exactly those
        let f = users_flattens(&["company"]);
        let mut doc = json!({"id": 1, "company": {"customer_name": "acme"}});
        f.apply("public", "users", &mut doc);
        assert_eq!(doc, json!({"id": 1, "customer_name": "acme"}));
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
    fn a_pipeline_belongs_to_its_table_alone() {
        let p = Pipelines::from_pairs([(
            ("public".to_string(), "users".to_string()),
            "embed-users".to_string(),
        )]);
        assert_eq!(p.for_table("public", "users"), Some("embed-users"));
        assert_eq!(p.for_table("public", "orders"), None);
        assert_eq!(p.for_table("other", "users"), None);
        assert_eq!(Pipelines::default().for_table("public", "users"), None);
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
        let m = TableMapping::from_pairs([(("public".into(), "users".into()), "users_v1")]);
        assert_eq!(
            m.target_for("public", "users"),
            Some(&IndexTarget::Static("users_v1".into()))
        );
        assert_eq!(m.target_for("public", "orders"), None);
    }

    #[test]
    fn a_mapping_knows_which_indices_more_than_one_table_feeds() {
        let m = TableMapping::from_pairs([
            (("public".into(), "users".into()), "u"),
            (("public".into(), "orders".into()), "u"),
            (("public".into(), "carts".into()), "c"),
        ]);
        assert!(m.is_shared("u"));
        assert!(!m.is_shared("c"));
        assert!(!m.is_shared("nowhere"));
    }

    fn indexed(spec: &str, pk_columns: &[&str]) -> IndexTarget {
        IndexTarget::Template {
            spec: spec.into(),
            template: pk(spec, pk_columns),
        }
    }

    #[test]
    fn a_template_is_never_a_shared_index() {
        // two templates that happen to render the same name are refused at
        // config; the mapping only ever counts what it can see, fixed names
        let m = TableMapping::from_pairs([
            (("public".into(), "events".into()), indexed("e-{t}", &[])),
            (("public".into(), "audits".into()), indexed("e-{t}", &[])),
        ]);
        assert!(!m.is_shared("e-x"));
        assert!(!m.is_shared("e-*"));
    }

    #[test]
    fn a_static_target_renders_its_own_name() {
        let t = IndexTarget::from("users");
        assert_eq!(t.render(&json!({"id": 1})).expect("fixed"), "users");
        assert_eq!(t.render_from_pk(&json!(1)).expect("fixed"), "users");
        assert!(t.is_pk_only(), "a bare key is more than enough");
        assert_eq!(t.spec(), "users");
        assert_eq!(
            IndexTarget::from("u".to_string()),
            IndexTarget::Static("u".into())
        );
    }

    #[test]
    fn a_templated_target_renders_from_the_raw_row() {
        let t = indexed("events-{tenant}", &["id"]);
        assert!(!t.is_pk_only());
        assert_eq!(
            t.render(&json!({"id": 7, "tenant": "acme"}))
                .expect("renders"),
            "events-acme"
        );
        assert_eq!(t.spec(), "events-{tenant}");
        let keyed = indexed("shard-{id}", &["id"]);
        assert!(keyed.is_pk_only());
        assert_eq!(
            keyed.render_from_pk(&json!(7)).expect("bare key"),
            "shard-7"
        );
    }

    #[test]
    fn a_rendered_name_that_is_not_a_usable_index_is_an_error() {
        let t = indexed("events-{tenant}", &[]);
        let err = t.render(&json!({"tenant": "ACME"})).unwrap_err();
        assert!(
            err.contains("events-{tenant}")
                && err.contains("events-ACME")
                && err.contains("tenant"),
            "names the template, what it rendered and from which column: {err}"
        );
        assert!(err.contains("lowercase"), "{err}");
        let leading = indexed("{tenant}-events", &[]);
        assert!(leading.render(&json!({"tenant": "_x"})).is_err());
        assert!(
            leading.render(&json!({"tenant": ""})).is_err(),
            "an empty value leaves a name that starts with '-'"
        );
        assert!(
            leading.render_from_pk(&json!({"tenant": "X"})).is_err(),
            "the key path is checked too"
        );
    }

    #[test]
    fn a_null_in_an_index_column_is_an_error() {
        let t = indexed("events-{tenant}", &[]);
        let err = t.render(&json!({"tenant": null})).unwrap_err();
        assert!(err.contains("tenant") && err.contains("NULL"), "{err}");
    }

    #[test]
    fn a_templates_pattern_replaces_every_placeholder_with_a_star() {
        assert_eq!(indexed("events-{month}", &[]).pattern(), "events-*");
        assert_eq!(indexed("{a}-{b}-x", &[]).pattern(), "*-*-x");
        assert_eq!(
            indexed("{a}{b}-x", &[]).pattern(),
            "*-x",
            "adjacent placeholders are one star"
        );
    }

    #[test]
    fn a_fixed_index_is_its_own_pattern() {
        assert_eq!(IndexTarget::from("users").pattern(), "users");
    }

    #[test]
    fn literals_are_reported_in_order() {
        assert_eq!(pk("a-{x}-b{y}c", &[]).literals(), vec!["a-", "-b", "c"]);
        assert!(pk("{x}{y}", &[]).literals().is_empty());
        assert_eq!(pk("plain", &[]).literals(), vec!["plain"]);
    }

    #[test]
    fn an_index_name_is_checked_against_one_grammar() {
        let cases = [
            ("users", None),
            ("events-2024_01", None),
            ("Users", Some("must start with a lowercase letter")),
            ("", Some("must start with a lowercase letter")),
            ("_hidden", Some("must start with a lowercase letter")),
            (".system", Some("must start with a lowercase letter")),
            ("events-ACME", Some("may only contain lowercase [a-z0-9_-]")),
            ("a b", Some("may only contain lowercase [a-z0-9_-]")),
        ];
        for (name, expected) in cases {
            assert_eq!(
                check_index_name(name).err().as_deref(),
                expected,
                "{name:?}"
            );
        }
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
            by: None,
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
            by: None,
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
            by: None,
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
            by: None,
            id: pk("t-{id}", &[]),
        };
        let err = fan_out_docs(&rule, "t-1", &json!({"id": 1, "tags": "a,b"})).unwrap_err();
        assert!(err.contains("tags"), "{err}");
    }

    #[test]
    fn a_delimited_column_is_split_with_the_transforms_semantics() {
        let rule = FanOut {
            field: "member_ids".into(),
            by: Some(",".into()),
            id: pk("n-{id}-{member_ids}", &[]),
        };
        let docs = fan_out_docs(&rule, "n-1", &json!({"id": 1, "member_ids": "7, 12 ,31"}))
            .expect("splits");
        assert_eq!(
            docs.iter().map(|(id, _)| id.as_str()).collect::<Vec<_>>(),
            vec!["n-1-7", "n-1-12", "n-1-31"],
            "pieces are trimmed, and each is an element of its own"
        );
        assert_eq!(docs[0].1, json!({"id": 1, "member_ids": "7"}));
        assert_eq!(
            fan_out_docs(&rule, "n-2", &json!({"id": 2, "member_ids": "9"}))
                .expect("one piece")
                .len(),
            1,
            "a list of one is one element, not a special case"
        );
        for empty in ["", " ", ",", " , "] {
            assert!(
                fan_out_docs(&rule, "n-3", &json!({"id": 3, "member_ids": empty}))
                    .expect("empty")
                    .is_empty(),
                "{empty:?} names no member"
            );
        }
        assert_eq!(
            fan_out_docs(&rule, "n-4", &json!({"id": 4, "member_ids": null})).expect("null"),
            vec![("n-4".to_string(), json!({"id": 4, "member_ids": null}))],
            "a NULL list is still a row with no members"
        );
    }

    #[test]
    fn by_and_an_array_column_are_two_answers_to_one_question() {
        let rule = FanOut {
            field: "tags".into(),
            by: Some(",".into()),
            id: pk("t-{id}-{tags}", &[]),
        };
        let err = fan_out_docs(&rule, "t-1", &json!({"id": 1, "tags": ["a", "b"]})).unwrap_err();
        assert!(err.contains("tags") && err.contains("by"), "{err}");
        let err = fan_out_docs(&rule, "t-1", &json!({"id": 1, "tags": 7})).unwrap_err();
        assert!(err.contains("tags"), "{err}");
    }

    #[test]
    fn an_element_parent_files_every_element_under_itself() {
        let fan = FanOut {
            field: "member_ids".into(),
            by: Some(",".into()),
            id: pk("n-{id}-{member_ids}", &[]),
        };
        let rule = JoinRule {
            field: "relation".into(),
            name: "member".into(),
            parent: Some(JoinParent {
                column: "member_ids".into(),
                element: true,
                name: "note".into(),
                id: ParentId::Template(pk("note-{id}", &["id"])),
                key_column: false,
            }),
        };
        let docs =
            fan_out_docs(&fan, "n-1", &json!({"id": 1, "member_ids": "7,12"})).expect("fans out");
        let placed: Vec<(serde_json::Value, Option<String>)> = docs
            .iter()
            .map(|(_, doc)| rule.routing_for_doc(doc).expect("element parent"))
            .collect();
        assert_eq!(
            placed,
            vec![
                (
                    json!({"name": "member", "parent": "note-7"}),
                    Some("note-7".to_string())
                ),
                (
                    json!({"name": "member", "parent": "note-12"}),
                    Some("note-12".to_string())
                ),
            ],
            "each element document names and routes to its own parent"
        );
        // a key names none of the parents a fanned row has
        let err = rule.routing_for_key(None, &json!(1)).unwrap_err();
        assert!(err.contains("element"), "{err}");
    }

    fn parent_rule() -> JoinRule {
        JoinRule {
            field: "relation".into(),
            name: "customer".into(),
            parent: None,
        }
    }

    fn child_rule(id: ParentId, key_column: bool) -> JoinRule {
        JoinRule {
            field: "relation".into(),
            name: "order".into(),
            parent: Some(JoinParent {
                column: "customer_id".into(),
                element: false,
                name: "customer".into(),
                id,
                key_column,
            }),
        }
    }

    #[test]
    fn a_join_parent_is_its_bare_name_and_keeps_the_targets_routing() {
        let rule = parent_rule();
        let (value, routing) = rule
            .routing_for_doc(&json!({"id": 1, "name": "acme"}))
            .expect("parent");
        assert_eq!(value, json!("customer"));
        assert_eq!(routing, None);
        assert_eq!(rule.routing_for_key(None, &json!(1)).expect("parent"), None);
    }

    #[test]
    fn a_join_child_names_its_parent_and_is_routed_to_it() {
        let rule = child_rule(ParentId::Key, false);
        let (value, routing) = rule
            .routing_for_doc(&json!({"id": 7, "customer_id": 1}))
            .expect("child");
        assert_eq!(value, json!({"name": "order", "parent": "1"}));
        assert_eq!(routing.as_deref(), Some("1"));
    }

    #[test]
    fn a_parent_id_renders_with_the_parents_own_rule() {
        // the child holds one value; it has to land on the id the parent's
        // section renders for itself, or the two documents never meet
        assert_eq!(ParentId::Key.render(&json!(1)).expect("key"), "1");
        let template = ParentId::Template(pk("customer-{id}", &["id"]));
        assert_eq!(template.render(&json!(1)).expect("template"), "customer-1");
        let (value, routing) = child_rule(template, false)
            .routing_for_doc(&json!({"id": 7, "customer_id": 1}))
            .expect("child");
        assert_eq!(value["parent"], json!("customer-1"));
        assert_eq!(routing.as_deref(), Some("customer-1"));
    }

    #[test]
    fn a_null_or_missing_parent_column_names_the_column() {
        let rule = child_rule(ParentId::Key, false);
        let null = rule
            .routing_for_doc(&json!({"id": 7, "customer_id": null}))
            .unwrap_err();
        assert!(
            null.contains("customer_id") && null.contains("NULL"),
            "{null}"
        );
        let missing = rule.routing_for_doc(&json!({"id": 7})).unwrap_err();
        assert!(
            missing.contains("customer_id") && missing.contains("missing"),
            "{missing}"
        );
    }

    #[test]
    fn a_key_only_event_routes_from_the_before_image_or_the_key() {
        let rule = child_rule(ParentId::Template(pk("customer-{id}", &["id"])), false);
        assert_eq!(
            rule.routing_for_key(Some(&json!({"id": 7, "customer_id": 1})), &json!(7))
                .expect("before-image"),
            Some("customer-1".into()),
            "the before-image is the row as the target last saw it"
        );
        assert_eq!(
            rule.routing_for_key(None, &json!({"customer_id": 2, "seq": 3}))
                .expect("composite key"),
            Some("customer-2".into()),
            "a composite key carrying the parent column needs nothing else"
        );
        let keyed = child_rule(ParentId::Key, true);
        assert_eq!(
            keyed.routing_for_key(None, &json!(5)).expect("bare key"),
            Some("5".into()),
            "a bare key binds to the parent column when that column is the key"
        );
    }

    #[test]
    fn a_key_only_event_outside_the_key_needs_a_before_image() {
        let rule = child_rule(ParentId::Key, false);
        let err = rule.routing_for_key(None, &json!(7)).unwrap_err();
        assert!(
            err.contains("customer_id") && err.contains("REPLICA IDENTITY FULL"),
            "{err}"
        );
    }

    fn routing_column(key_column: bool) -> RoutingColumn {
        RoutingColumn {
            column: "tenant".into(),
            key_column,
        }
    }

    #[test]
    fn a_routing_column_renders_from_the_raw_row() {
        let rule = routing_column(false);
        assert_eq!(
            rule.render(&json!({"id": 7, "tenant": "acme"}))
                .expect("routing"),
            "acme"
        );
        assert_eq!(
            rule.render(&json!({"id": 7, "tenant": 42}))
                .expect("scalar"),
            "42",
            "a non-string routing value is displayed like an id part"
        );
    }

    #[test]
    fn a_null_missing_or_empty_routing_column_halts() {
        let rule = routing_column(false);
        let null = rule.render(&json!({"id": 7, "tenant": null})).unwrap_err();
        assert!(null.contains("tenant") && null.contains("NULL"), "{null}");
        let missing = rule.render(&json!({"id": 7})).unwrap_err();
        assert!(
            missing.contains("tenant") && missing.contains("missing"),
            "{missing}"
        );
        let empty = rule.render(&json!({"id": 7, "tenant": ""})).unwrap_err();
        assert!(
            empty.contains("tenant") && empty.contains("empty"),
            "{empty}"
        );
    }

    #[test]
    fn a_key_only_event_takes_its_routing_from_the_before_image_or_the_key() {
        let rule = routing_column(false);
        assert_eq!(
            rule.render_from_key(Some(&json!({"id": 7, "tenant": "acme"})), &json!(7))
                .expect("before-image"),
            "acme",
            "the before-image is the row as the target last saw it"
        );
        assert_eq!(
            rule.render_from_key(None, &json!({"tenant": "globex", "seq": 3}))
                .expect("composite key"),
            "globex",
            "a composite key carrying the routing column needs nothing else"
        );
        let keyed = routing_column(true);
        assert_eq!(
            keyed
                .render_from_key(None, &json!("acme"))
                .expect("bare key"),
            "acme",
            "a bare key binds to the routing column when that column is the key"
        );
    }

    #[test]
    fn a_key_only_event_outside_the_key_needs_a_before_image_for_its_routing() {
        let err = routing_column(false)
            .render_from_key(None, &json!(7))
            .unwrap_err();
        assert!(
            err.contains("tenant") && err.contains("REPLICA IDENTITY FULL"),
            "{err}"
        );
    }
}
