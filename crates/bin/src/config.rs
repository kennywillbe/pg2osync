//! Config loading and validation.
//!
//! Secrets are env-first: `*_env` keys are resolved at load time;
//! plain-text secrets in the file are accepted but warn deprecated.

use anyhow::{Context, Result};
use pg2osync_core::sink::index_matches_pattern;
use pg2osync_engine::mapping::{IdTemplate, IndexTarget, TransformOp, check_index_name};
use serde::Deserialize;
use std::collections::BTreeMap;
use std::path::Path;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AppConfig {
    pub source: SourceConfig,
    pub target: TargetConfig,
    pub sync: BTreeMap<String, TableSync>,
    #[serde(default)]
    pub engine: pg2osync_engine::EngineConfig,
    #[serde(default)]
    pub metrics: MetricsConfig,
    #[serde(default)]
    pub api: ApiConfig,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceConfig {
    /// "wal" (default) | "poll" — ignored for MySQL (always binlog)
    #[serde(default = "default_mode")]
    pub mode: String,
    /// "postgres" (default) | "mysql"
    #[serde(default = "default_flavor")]
    pub flavor: String,
    /// MySQL/MariaDB only: unique server_id for binlog dump
    #[serde(default = "default_server_id")]
    pub server_id: u32,
    /// poll mode: default timestamp column, overridable per table
    #[serde(default = "default_poll_column")]
    pub poll_column: String,
    #[serde(default = "default_poll_interval")]
    pub poll_interval_secs: u64,
    /// poll mode: rows fetched per table per cycle
    #[serde(default = "default_poll_page_size")]
    pub poll_page_size: i64,
    pub url: Option<String>,
    pub url_env: Option<String>,
    /// libpq spelling: disable | prefer | require | verify-ca | verify-full.
    /// Falls back to the URL's own `sslmode`, then to `prefer`.
    #[serde(default)]
    pub sslmode: Option<String>,
    /// PEM bundle of trusted roots for the verifying modes.
    #[serde(default)]
    pub sslrootcert: Option<String>,
    /// PEM client certificate chain presented to the server (mTLS).
    #[serde(default)]
    pub sslcert: Option<String>,
    /// PEM private key for `sslcert`; PKCS#8, RSA (PKCS#1) or EC (SEC1).
    #[serde(default)]
    pub sslkey: Option<String>,
    /// Nested-child queries use a dedicated connection; defaults to url.
    #[serde(default)]
    pub admin_url_env: Option<String>,
    /// Consecutive stream failures tolerated before the process gives up and
    /// hands over to whatever supervises it. Zero exits on the first failure.
    #[serde(default = "default_reconnect_max")]
    pub reconnect_max: u32,
    #[serde(default = "default_reconnect_backoff")]
    pub reconnect_backoff_ms: u64,
    /// Rows one range of the initial load should cover.
    ///
    /// A range is one statement and cannot be interrupted, so this is also how
    /// often the load can look at anything — the slot's WAL budget included.
    /// The default is a measurement, not a convention: ~50,000 rows is under a
    /// second of work at observed rates, which is the same granularity
    /// pt-online-schema-change aims for with its 0.5s chunk target.
    #[serde(default = "default_load_chunk_rows")]
    pub load_chunk_rows: i64,
    /// How many ranges of the initial load are read at once, each on its own
    /// connection.
    ///
    /// One is the default because the read was never the constraint: a single
    /// `COPY` hands over rows more than twenty times faster than the pipeline
    /// indexes them. It becomes worth raising only once the write path is
    /// concurrent enough that this process's own parsing is the limit — and it
    /// multiplies the read load on the operator's database, which is not a
    /// default anyone should inherit unmeasured. PostgreSQL only.
    #[serde(default = "default_load_workers")]
    pub load_workers: usize,
    #[serde(default = "default_slot_name")]
    pub slot_name: String,
    #[serde(default = "default_publication")]
    pub publication: String,
}

fn default_mode() -> String {
    "wal".into()
}

fn default_poll_column() -> String {
    "updated_at".into()
}

fn default_poll_interval() -> u64 {
    30
}

fn default_poll_page_size() -> i64 {
    5000
}

fn default_flavor() -> String {
    "postgres".into()
}

fn default_server_id() -> u32 {
    424242
}

fn default_reconnect_max() -> u32 {
    10
}

fn default_reconnect_backoff() -> u64 {
    1000
}

fn default_load_chunk_rows() -> i64 {
    50_000
}

fn default_load_workers() -> usize {
    1
}

fn default_slot_name() -> String {
    "pg2osync".into()
}

fn default_publication() -> String {
    "pg2osync_pub".into()
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TargetConfig {
    pub url: String,
    /// "opensearch" (default) | "elasticsearch" | "meilisearch"
    #[serde(default = "default_target_flavor")]
    pub flavor: String,
    pub username: Option<String>,
    pub password: Option<String>,
    pub password_env: Option<String>,
    pub api_key_env: Option<String>,
    #[serde(default = "default_true")]
    pub tls_verify: bool,
    /// meilisearch only: checkpoint fallback directory
    #[serde(default = "default_state_dir")]
    pub state_dir: String,
}

fn default_target_flavor() -> String {
    "opensearch".into()
}

fn default_state_dir() -> String {
    "./.pg2osync-state".into()
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct MetricsConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "default_bind")]
    pub bind: String,
    /// Environment variable holding a bearer token required on /metrics.
    /// Unset serves the exposition to anything that can reach the port.
    pub token_env: Option<String>,
}

fn default_bind() -> String {
    "127.0.0.1:9100".into()
}

/// The read-your-writes endpoint. Off by default: it is a surface applications
/// call, not an operational one, so opening a port is the operator's decision.
#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ApiConfig {
    pub enabled: bool,
    pub bind: String,
    /// Environment variable holding a bearer token required on every request.
    pub token_env: Option<String>,
}

impl Default for ApiConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            bind: "127.0.0.1:9101".into(),
            token_env: None,
        }
    }
}

// derive(Default) would set enabled=false, silently disabling metrics
impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            bind: default_bind(),
            token_env: None,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TableSync {
    /// Schema-qualified table name, e.g. "public.users".
    pub table: String,
    pub index: Option<String>,
    pub primary_key: Option<String>,
    /// The table has no key the pipeline may address a row by: it is synced
    /// insert-only, each row filed under a hash of its content unless `id`
    /// says otherwise, and an UPDATE or DELETE on it halts the pipeline.
    #[serde(default)]
    pub append_only: bool,
    /// Derived document id: literals plus `{column}` placeholders, e.g.
    /// `tenant-{tenant_id}-{id}`. Unset keeps the primary key as the id.
    #[serde(default)]
    pub id: Option<String>,
    /// One row to many documents: fan an array column out into one document
    /// per element.
    #[serde(default)]
    pub fan_out: Option<FanOut>,
    /// This table's place in a join field shared with another section of the
    /// same index.
    #[serde(default)]
    pub join: Option<JoinSpec>,
    #[serde(default)]
    pub columns: Option<Vec<String>>,
    #[serde(default)]
    pub exclude_columns: Vec<String>,
    /// poll mode: overrides `[source] poll_column` for this table
    #[serde(default)]
    pub poll_column: Option<String>,
    /// SQL predicate marking a row as deleted, e.g. `deleted_at IS NOT NULL`.
    /// Poll mode turns a matching row into a delete; the initial load skips it.
    #[serde(default)]
    pub soft_delete: Option<String>,
    /// Restricted SQL predicate deciding which rows belong in the index, e.g.
    /// `status = 'active' AND deleted_at IS NULL`. The initial load pushes it
    /// into its query; the engine evaluates it again on every streamed row,
    /// which is what makes a row that leaves the filter a delete.
    #[serde(default, rename = "where")]
    pub filter: Option<String>,
    /// The target's ingest pipeline every document of this section goes
    /// through, e.g. one that computes a vector field. Named here rather than
    /// implemented here: the target already owns the model, and the document
    /// still takes the one write path. OpenSearch and Elasticsearch only.
    #[serde(default)]
    pub pipeline: Option<String>,
    /// Column whose value routes this section's documents, e.g. a tenant id,
    /// so one tenant's documents share a shard. Co-location only: routing
    /// does not name the document, and the column stays an ordinary field.
    /// OpenSearch and Elasticsearch only.
    #[serde(default)]
    pub routing: Option<String>,
    /// Column transformations: `email = "redact"` for an operation without a
    /// parameter, `tags = { op = "split", by = "," }` for one with.
    #[serde(default)]
    pub transform: std::collections::HashMap<String, TransformSpec>,
    /// Target field names, source column → field. Applied after every other
    /// rule, so the rest of this section keeps naming source columns.
    #[serde(default)]
    pub fields: std::collections::HashMap<String, String>,
    /// Fields that come from no column: literal values added last, after
    /// projection, transforms and renames. `{schema}`/`{table}` in a string
    /// render once at startup.
    #[serde(default)]
    pub constants: std::collections::HashMap<String, Constant>,
    /// One-to-many children embedded as JSON arrays (single level, 0.3).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub children: Vec<ChildJoin>,
    /// JSON file holding the mapping to create this index with, resolved
    /// relative to the config file. Applied only when the index does not
    /// exist; an existing index is compared against it, never altered.
    #[serde(default)]
    pub mapping_file: Option<String>,
    /// The parsed contents of `mapping_file`. Read once at load so a missing
    /// or malformed file fails before anything connects.
    #[serde(skip)]
    pub mapping: Option<serde_json::Value>,
}

/// A value for a field that comes from no column. Scalars only: an object or
/// an array as a constant is a document shape nobody asked for, and a TOML
/// datetime has no unambiguous target type.
#[derive(Debug, Clone, PartialEq)]
pub enum Constant {
    Str(String),
    Int(i64),
    Float(f64),
    Bool(bool),
}

/// Hand-written rather than `#[serde(untagged)]`: the untagged form reports
/// "did not match any variant" without naming the key, and a datetime reaches
/// it as a map with a private marker, which reads as nonsense. A visitor lets
/// serde say which key held what, and what was expected instead.
impl<'de> Deserialize<'de> for Constant {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Scalar;
        impl serde::de::Visitor<'_> for Scalar {
            type Value = Constant;
            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("a string, integer, float or boolean")
            }
            fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<Constant, E> {
                Ok(Constant::Str(v.to_owned()))
            }
            fn visit_string<E: serde::de::Error>(self, v: String) -> Result<Constant, E> {
                Ok(Constant::Str(v))
            }
            fn visit_i64<E: serde::de::Error>(self, v: i64) -> Result<Constant, E> {
                Ok(Constant::Int(v))
            }
            fn visit_u64<E: serde::de::Error>(self, v: u64) -> Result<Constant, E> {
                i64::try_from(v)
                    .map(Constant::Int)
                    .map_err(|_| E::custom("integer is too large for an i64"))
            }
            fn visit_f64<E: serde::de::Error>(self, v: f64) -> Result<Constant, E> {
                Ok(Constant::Float(v))
            }
            fn visit_bool<E: serde::de::Error>(self, v: bool) -> Result<Constant, E> {
                Ok(Constant::Bool(v))
            }
        }
        deserializer.deserialize_any(Scalar)
    }
}

impl Constant {
    /// The JSON the document carries. `{schema}` and `{table}` are the only
    /// placeholders, rendered here from the section's table so the engine
    /// never sees a template. A string without a brace is taken verbatim,
    /// which is what keeps `note = ""` a value rather than a grammar error.
    pub fn render(
        &self,
        schema: &str,
        table: &str,
    ) -> std::result::Result<serde_json::Value, String> {
        match self {
            Self::Int(v) => Ok(serde_json::json!(v)),
            Self::Float(v) => Ok(serde_json::json!(v)),
            Self::Bool(v) => Ok(serde_json::json!(v)),
            Self::Str(s) if !s.contains('{') => Ok(serde_json::json!(s)),
            Self::Str(s) => {
                let template = IdTemplate::parse(s, &[]).map_err(|e| e.to_string())?;
                if let Some(name) = template
                    .columns()
                    .into_iter()
                    .find(|c| *c != "schema" && *c != "table")
                {
                    return Err(format!(
                        "placeholder {{{name}}} is not one of {{schema}}/{{table}}"
                    ));
                }
                template
                    .render(&serde_json::json!({ "schema": schema, "table": table }))
                    .map(serde_json::Value::String)
            }
        }
    }
}

/// One entry of `[sync.x.transform]`: the operation's name alone, or a table
/// naming it with its parameter.
#[derive(Debug, Clone, PartialEq)]
pub enum TransformSpec {
    Op(String),
    Table(TransformTable),
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TransformTable {
    pub op: String,
    #[serde(default)]
    pub by: Option<String>,
    #[serde(default)]
    pub from: Option<String>,
}

/// Hand-written for the same reason `Constant` is: an untagged enum reports
/// "did not match any variant" and swallows the table's own error, which is
/// the one that names a misspelt key.
impl<'de> Deserialize<'de> for TransformSpec {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Spec;
        impl<'de> serde::de::Visitor<'de> for Spec {
            type Value = TransformSpec;
            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str(
                    "a transform name like \"redact\", or a table like { op = \"split\", by = \",\" }",
                )
            }
            fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<TransformSpec, E> {
                Ok(TransformSpec::Op(v.to_owned()))
            }
            fn visit_string<E: serde::de::Error>(self, v: String) -> Result<TransformSpec, E> {
                Ok(TransformSpec::Op(v))
            }
            fn visit_map<A: serde::de::MapAccess<'de>>(
                self,
                map: A,
            ) -> Result<TransformSpec, A::Error> {
                TransformTable::deserialize(serde::de::value::MapAccessDeserializer::new(map))
                    .map(TransformSpec::Table)
            }
        }
        deserializer.deserialize_any(Spec)
    }
}

impl TransformSpec {
    /// The engine operation this names, or why it is not one. Both the
    /// startup check and the run-time build go through here, so one grammar
    /// decides what a transform is.
    pub fn parse(&self) -> std::result::Result<TransformOp, String> {
        let (op, by, from) = match self {
            Self::Op(op) => (op.as_str(), None, None),
            Self::Table(t) => (t.op.as_str(), t.by.as_deref(), t.from.as_deref()),
        };
        let refuse = |param: &str| format!("{op:?} takes no {param:?}");
        match op {
            "hash" | "redact" | "json" | "number" => {
                if by.is_some() {
                    return Err(refuse("by"));
                }
                if from.is_some() {
                    return Err(refuse("from"));
                }
                Ok(match op {
                    "hash" => TransformOp::Hash,
                    "redact" => TransformOp::Redact,
                    "json" => TransformOp::Json,
                    _ => TransformOp::Number,
                })
            }
            "split" => {
                if from.is_some() {
                    return Err(refuse("from"));
                }
                match by {
                    None => Err("split needs \"by\": { op = \"split\", by = \",\" }".into()),
                    Some("") => Err("split needs a non-empty \"by\"".into()),
                    Some(by) => Ok(TransformOp::Split { by: by.to_string() }),
                }
            }
            "date" => {
                if by.is_some() {
                    return Err(refuse("by"));
                }
                match from {
                    None => {
                        Err("date needs \"from\": { op = \"date\", from = \"%d/%m/%Y\" }".into())
                    }
                    Some("") => Err("date needs a non-empty \"from\"".into()),
                    Some(from) => Ok(TransformOp::Date {
                        from: from.to_string(),
                    }),
                }
            }
            other => Err(format!(
                "{other:?} is not a transform; expected one of \"hash\", \"redact\", \"json\", \
                 \"split\", \"number\", \"date\""
            )),
        }
    }
}

/// One array column fanned out into one document per element. `id` is the
/// template element documents are filed under; it renders from the merged
/// child document (parent-minus-array plus element), so its placeholders may
/// name parent columns as well as fields of the element.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FanOut {
    pub field: String,
    pub id: String,
}

/// This section's place in a join field: its relation name, and — for a
/// child — the column holding its parent's key.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JoinSpec {
    /// The join field, as the parent's mapping declares it.
    pub field: String,
    /// This section's relation name inside that field.
    pub name: String,
    /// Column on THIS table holding the parent's key. Its presence is what
    /// makes this section a child; its absence makes it the parent.
    #[serde(default)]
    pub parent: Option<String>,
}

/// A child table joined into the parent document.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChildJoin {
    /// Schema-qualified child table, e.g. "public.orders".
    pub table: String,
    /// Field name on the parent document holding the nested array.
    pub field: String,
    /// Column holding the parent's key: on the CHILD table, or — where
    /// `through` is set — on the junction, which is what carries it there.
    pub foreign_key: String,
    /// Schema-qualified junction table of a many-to-many relation.
    ///
    /// The child rows are still what gets embedded; the junction only says
    /// which parent each of them belongs to, and contributes no field.
    #[serde(default)]
    pub through: Option<String>,
    /// Junction column referencing the CHILD's primary key.
    #[serde(default)]
    pub through_key: Option<String>,
    /// How many children to embed. Unset embeds all of them.
    ///
    /// Unset by default because a cap loses data and the target already has the
    /// bound that matters: past `index.mapping.nested_objects.limit` (10,000 by
    /// default) OpenSearch refuses a document whose field is mapped `nested`,
    /// which is reported and quarantined rather than lost. Set this to trade a
    /// complete array for a bounded document — a document whose array was cut
    /// says so in `<field>_truncated` and `<field>_total`.
    #[serde(default)]
    pub max_rows: Option<u32>,
    /// Target field names inside the embedded array, child column → field.
    #[serde(default)]
    pub fields: std::collections::HashMap<String, String>,
    /// The only child columns to embed. Unset embeds all of them.
    ///
    /// Projected in the read, so the initial load and a streamed re-fetch
    /// cannot embed different shapes. Mutually exclusive with
    /// `exclude_columns`, as on the parent section.
    #[serde(default)]
    pub columns: Option<Vec<String>>,
    /// Child columns to leave out of the embedded element.
    #[serde(default)]
    pub exclude_columns: Vec<String>,
    /// The relation is one-to-one: embed the element itself, not an array.
    ///
    /// `null` when the parent has no child. Refused with `max_rows`: a cap on a
    /// relation declared one-to-one contradicts it.
    #[serde(default)]
    pub single: bool,
}

impl ChildJoin {
    /// Every name this child writes on the parent document: the array, and
    /// the two fields a capped array reports itself with.
    ///
    /// A one-to-one child writes neither of those: it cannot be capped, so
    /// claiming the names would refuse configuration that nothing collides with.
    fn claimed_fields(&self) -> Vec<String> {
        if self.single {
            return vec![self.field.clone()];
        }
        vec![
            self.field.clone(),
            format!("{}_truncated", self.field),
            format!("{}_total", self.field),
        ]
    }
}

impl TableSync {
    /// The index as written: a name, or the template a row renders one from.
    /// Every message names this rather than a rendered name, because the
    /// spec is what the operator can find in the file.
    pub fn index_name(&self, key: &str) -> String {
        self.index.clone().unwrap_or_else(|| key.to_string())
    }

    /// Whether the index is chosen per row. Only `index` can be a template:
    /// a section key with a brace is a name, and the grammar refuses it.
    pub fn is_templated(&self) -> bool {
        self.index.as_deref().is_some_and(|s| s.contains('{'))
    }

    /// The parsed target, grammar-checked. `pk_columns` decides whether a
    /// delete's bare key can render it, exactly as for `id`.
    ///
    /// The grammar lives here rather than in `validate` because `run` does
    /// not go through `validate`: the engine's table map is built from the
    /// same call, so a bad template is refused on every path.
    pub fn index_target(
        &self,
        key: &str,
        pk_columns: &[String],
    ) -> std::result::Result<IndexTarget, String> {
        let spec = self.index_name(key);
        if !self.is_templated() {
            check_index_name(&spec).map_err(|e| format!("index {spec:?} {e}"))?;
            return Ok(IndexTarget::Static(spec));
        }
        let template = IdTemplate::parse(&spec, pk_columns)
            .map_err(|e| format!("index {spec:?} is not a usable index template: {e}"))?;
        // A TRUNCATE clears every index the template can render, which for a
        // template without a literal is every index there is.
        if template.literals().is_empty() {
            return Err(format!(
                "index {spec:?} is all placeholders, so a TRUNCATE of {} could only be \
                 applied by clearing every index; give the template a fixed prefix or suffix",
                self.table
            ));
        }
        // The literals are checked where a rendered name would carry them:
        // each placeholder stands in with one legal character, so only the
        // literals can fail, and a leading placeholder is left to the row —
        // the first character of `{tenant}-events` is checked where it is
        // rendered, and halts there.
        let stand_in: serde_json::Map<String, serde_json::Value> = template
            .columns()
            .into_iter()
            .map(|col| (col.to_string(), serde_json::Value::String("x".into())))
            .collect();
        let sample = template
            .render(&serde_json::Value::Object(stand_in))
            .map_err(|e| format!("index {spec:?} is not a usable index template: {e}"))?;
        check_index_name(&sample).map_err(|e| format!("index {spec:?} {e}"))?;
        Ok(IndexTarget::Template { spec, template })
    }
}

impl AppConfig {
    /// Indices more than one `[sync.*]` section writes to.
    ///
    /// Sharing is allowed but never inherited: the sections that share an
    /// index each declare an `id` or a join, so identity is a statement rather
    /// than an accident of two tables both having a row 1.
    pub fn shared_indexes(&self) -> std::collections::HashSet<String> {
        let mut seen = std::collections::HashSet::new();
        let mut shared = std::collections::HashSet::new();
        for (key, tbl) in &self.sync {
            let index = tbl.index_name(key);
            if !seen.insert(index.clone()) {
                shared.insert(index);
            }
        }
        shared
    }

    /// Whether every section writing `index` is part of a join pair, whose
    /// documents carry the relation that tells one table's from another's.
    pub fn is_join_index(&self, index: &str) -> bool {
        let mut members = self
            .sync
            .iter()
            .filter(|(key, tbl)| tbl.index_name(key) == index)
            .peekable();
        members.peek().is_some() && members.all(|(_, tbl)| tbl.join.is_some())
    }
}

#[derive(Debug)]
pub struct ResolvedSecrets {
    pub source_url: String,
    /// Separate connection for catalog and nested-child queries; defaults to
    /// `source_url`. A dedicated admin user keeps replication privileges apart.
    pub admin_url: String,
    pub target_password: Option<String>,
    pub warnings: Vec<String>,
}

impl AppConfig {
    pub fn load(path: &Path) -> Result<Self> {
        let raw = std::fs::read_to_string(path)
            .with_context(|| format!("cannot read config file {}", path.display()))?;
        let mut cfg: AppConfig =
            toml::from_str(&raw).with_context(|| format!("invalid TOML in {}", path.display()))?;
        let base = path.parent().unwrap_or_else(|| Path::new("."));
        for (key, table) in cfg.sync.iter_mut() {
            let Some(file) = &table.mapping_file else {
                continue;
            };
            let full = base.join(file);
            let text = std::fs::read_to_string(&full).with_context(|| {
                format!("[sync.{key}] cannot read mapping_file {}", full.display())
            })?;
            let mapping: serde_json::Value = serde_json::from_str(&text)
                .with_context(|| format!("[sync.{key}] {} is not valid JSON", full.display()))?;
            if !mapping.is_object() {
                anyhow::bail!("[sync.{key}] {} must hold a JSON object", full.display());
            }
            table.mapping = Some(mapping);
        }
        cfg.validate()?;
        Ok(cfg)
    }

    /// Structural validation. Connection checks happen in `validate` command;
    /// this function never touches the network.
    pub fn validate(&self) -> Result<()> {
        const SOURCE_FLAVORS: [&str; 3] = ["postgres", "postgresql", "mysql"];
        const TARGET_FLAVORS: [&str; 3] = ["opensearch", "elasticsearch", "meilisearch"];
        const SOURCE_MODES: [&str; 2] = ["wal", "poll"];

        if !SOURCE_FLAVORS.contains(&self.source.flavor.as_str()) {
            anyhow::bail!(
                "[source] flavor {:?} is not one of {SOURCE_FLAVORS:?}",
                self.source.flavor
            );
        }
        if !TARGET_FLAVORS.contains(&self.target.flavor.as_str()) {
            anyhow::bail!(
                "[target] flavor {:?} is not one of {TARGET_FLAVORS:?}",
                self.target.flavor
            );
        }
        if !SOURCE_MODES.contains(&self.source.mode.as_str()) {
            anyhow::bail!(
                "[source] mode {:?} is not one of {SOURCE_MODES:?}",
                self.source.mode
            );
        }
        if let Some(sslmode) = &self.source.sslmode {
            pg2osync_source::tls::SslMode::parse(sslmode)
                .context("[source] sslmode is not a libpq ssl mode")?;
        }
        // the pairing is checked here as well as in `resolve` so a config
        // review catches it without a filesystem the reviewer may not have
        match (&self.source.sslcert, &self.source.sslkey) {
            (Some(_), None) => {
                anyhow::bail!("[source] sslcert needs sslkey; set both or neither")
            }
            (None, Some(_)) => {
                anyhow::bail!("[source] sslkey needs sslcert; set both or neither")
            }
            _ => {}
        }
        if self.engine.load_max_rows_per_sec == Some(0) {
            anyhow::bail!(
                "[engine] load_max_rows_per_sec = 0 would stop the load rather than slow it; \
                 leave the option unset, which means unlimited"
            );
        }
        if self.source.flavor == "mysql" && self.source.mode == "poll" {
            anyhow::bail!(
                "[source] mode = \"poll\" is PostgreSQL-only; MySQL always reads the binlog"
            );
        }
        // Whether the sections sharing an index may do so is a question about
        // the group, not about either section, so the groups are checked after
        // every section has been.
        let mut by_index: BTreeMap<String, Vec<(&str, &TableSync)>> = BTreeMap::new();
        // A template is a claim on a whole namespace, which is a question
        // about every other section's index, so the targets are kept for a
        // pass over the pairs.
        let mut targets: Vec<(&str, IndexTarget)> = Vec::with_capacity(self.sync.len());
        if self.sync.is_empty() {
            anyhow::bail!("no [sync.*] sections: nothing to synchronize");
        }
        for (key, tbl) in &self.sync {
            if !is_qualified_table(&tbl.table) {
                anyhow::bail!(
                    "[sync.{key}] table {:?} must be schema-qualified (e.g. \"public.users\")",
                    tbl.table
                );
            }
            let index = tbl.index_name(key);
            // the key columns do not matter to the grammar; whether a bare key
            // can render the template is the startup check's question
            let target = tbl
                .index_target(key, &[])
                .map_err(|e| anyhow::anyhow!("[sync.{key}] {e}"))?;
            by_index
                .entry(index.clone())
                .or_default()
                .push((key.as_str(), tbl));
            if let Some(join) = &tbl.join {
                if join.field.is_empty() {
                    anyhow::bail!("[sync.{key}.join] field must not be empty");
                }
                if join.name.is_empty() {
                    anyhow::bail!("[sync.{key}.join] name must not be empty");
                }
                if join.parent.as_deref() == Some("") {
                    anyhow::bail!("[sync.{key}.join] parent must not be empty");
                }
                if tbl.fan_out.is_some() {
                    anyhow::bail!(
                        "[sync.{key}] fan_out and join cannot be combined: every element of a \
                         fanned row would need its own routing, and they would all be filed \
                         under one parent"
                    );
                }
                if join.parent.is_some() && tbl.mapping_file.is_some() {
                    anyhow::bail!(
                        "[sync.{key}] a join child must not set mapping_file: the parent's \
                         mapping creates index {index}, and it is the one that declares the \
                         join field"
                    );
                }
                if self.target.flavor == "meilisearch" {
                    anyhow::bail!(
                        "[sync.{key}] join is an OpenSearch and Elasticsearch data model, which \
                         Meilisearch has no equivalent for; remove join for this target"
                    );
                }
                if tbl
                    .columns
                    .as_ref()
                    .is_some_and(|cols| cols.contains(&join.field))
                {
                    anyhow::bail!(
                        "[sync.{key}] columns lists {}, which is the join field, not a column",
                        join.field
                    );
                }
                if let Some(child) = tbl
                    .children
                    .iter()
                    .find(|child| child.claimed_fields().contains(&join.field))
                {
                    anyhow::bail!(
                        "[sync.{key}] {} is the join field and also the field of child {}",
                        join.field,
                        child.table
                    );
                }
            }
            if let Some(pipeline) = &tbl.pipeline {
                if pipeline.is_empty() {
                    anyhow::bail!("[sync.{key}] pipeline must not be empty");
                }
                if self.target.flavor == "meilisearch" {
                    anyhow::bail!(
                        "[sync.{key}] pipeline is an OpenSearch and Elasticsearch feature, which \
                         Meilisearch has no equivalent for; remove pipeline for this target"
                    );
                }
            }
            if let Some(routing) = &tbl.routing {
                if routing.is_empty() {
                    anyhow::bail!("[sync.{key}] routing must not be empty");
                }
                if tbl.join.is_some() {
                    anyhow::bail!(
                        "[sync.{key}] routing and join cannot be combined: a join child is \
                         already routed to its parent, and a second rule would move it off \
                         the shard its parent is on"
                    );
                }
                if self.target.flavor == "meilisearch" {
                    anyhow::bail!(
                        "[sync.{key}] routing is an OpenSearch and Elasticsearch feature, which \
                         Meilisearch has no equivalent for; remove routing for this target"
                    );
                }
            }
            if tbl.columns.is_some() && !tbl.exclude_columns.is_empty() {
                anyhow::bail!("[sync.{key}] columns and exclude_columns are mutually exclusive");
            }
            // append_only says there is no key; everything below addresses a
            // row, an element or a parent by one
            if tbl.append_only {
                if tbl.primary_key.is_some() {
                    anyhow::bail!("[sync.{key}] append_only and primary_key contradict each other");
                }
                if tbl.fan_out.is_some() {
                    anyhow::bail!(
                        "[sync.{key}] fan_out needs a key: the documents a row fans out into \
                         are found again by the row's key, which an append-only table does \
                         not have"
                    );
                }
                if tbl.join.is_some() {
                    anyhow::bail!(
                        "[sync.{key}] join needs a key: a parent is routed to by its key and a \
                         child names its parent by it, which an append-only table does not have"
                    );
                }
                if !tbl.children.is_empty() {
                    anyhow::bail!(
                        "[sync.{key}] [[children]] needs a key: a child row names the parent \
                         document to re-read by its key, which an append-only table does not have"
                    );
                }
                if tbl.soft_delete.is_some() {
                    anyhow::bail!(
                        "[sync.{key}] soft_delete needs a key to delete by, which an append-only \
                         table does not have"
                    );
                }
            }
            if tbl.columns.as_ref().is_some_and(|c| c.is_empty()) {
                anyhow::bail!("[sync.{key}] columns must not be empty");
            }
            // well-formedness only; whether a placeholder names a real column
            // is a `validate` question, because only the catalogue knows
            if let Some(spec) = &tbl.id {
                pg2osync_engine::mapping::IdTemplate::parse(spec, &[]).map_err(|e| {
                    anyhow::anyhow!("[sync.{key}] id {spec:?} is not a usable id: {e}")
                })?;
            }
            if let Some(fan) = &tbl.fan_out {
                if self.source.mode == "poll" {
                    anyhow::bail!(
                        "[sync.{key}] fan_out needs the replication log: a poll cycle \
                         cannot see which elements left an array"
                    );
                }
                if !tbl.children.is_empty() {
                    anyhow::bail!(
                        "[sync.{key}] fan_out and [[children]] both decide what a row's \
                         array becomes; configuring them together has no coherent meaning"
                    );
                }
                if fan.field.is_empty() {
                    anyhow::bail!("[sync.{key}.fan_out] field must not be empty");
                }
                // every document a row fans out into goes to one index, so the
                // template renders from the row, where the array is a value
                // no name can be made of
                if let IndexTarget::Template { spec, template } = &target
                    && template.columns().contains(&fan.field.as_str())
                {
                    anyhow::bail!(
                        "[sync.{key}] index {spec:?} names the fan_out field {}; every document \
                         a row produces goes to one index, so the index is chosen from the row \
                         and not from an element",
                        fan.field
                    );
                }
                // identity must see the array raw: a projection that cuts it
                // would leave fan-out with nothing to expand
                if tbl
                    .columns
                    .as_ref()
                    .is_some_and(|cols| cols.iter().any(|c| c == &fan.field))
                    || tbl.exclude_columns.iter().any(|c| c == &fan.field)
                {
                    anyhow::bail!(
                        "[sync.{key}.fan_out] field {:?} also appears in columns/exclude_columns; \
                         projection would cut the array before identity and fan-out see it",
                        fan.field
                    );
                }
                // element ids render from the merged child document, whose
                // element fields are not table columns: grammar only here
                pg2osync_engine::mapping::IdTemplate::parse(&fan.id, &[]).map_err(|e| {
                    anyhow::anyhow!(
                        "[sync.{key}.fan_out] id {:?} is not a usable id: {e}",
                        fan.id
                    )
                })?;
            }
            if let Some(spec) = &tbl.filter {
                pg2osync_core::filter::Filter::parse(spec).map_err(|e| {
                    anyhow::anyhow!(
                        "[sync.{key}] where {spec:?}: {e}\n{}",
                        pg2osync_core::filter::SUPPORTED
                    )
                })?;
            }
            for (col, spec) in &tbl.transform {
                spec.parse()
                    .map_err(|e| anyhow::anyhow!("[sync.{key}.transform] {col}: {e}"))?;
                // fan-out reads the raw row, before transforms: a split on the
                // array column would never reach it, and a scalar element
                // would be re-split afterwards
                if tbl.fan_out.as_ref().is_some_and(|fan| &fan.field == col) {
                    anyhow::bail!(
                        "[sync.{key}.transform] {col} is the fan_out field; fan-out reads the \
                         raw row, so a transform there never reaches it"
                    );
                }
            }
            check_field_map(&format!("sync.{key}.fields"), &tbl.fields)?;
            for (col, target) in &tbl.fields {
                // a rename is a promise that the column reaches the target;
                // projection dropping it would break that promise silently
                if tbl.exclude_columns.contains(col) {
                    anyhow::bail!(
                        "[sync.{key}.fields] {col} is renamed but also excluded; \
                         an excluded column never reaches the target"
                    );
                }
                if let Some(cols) = &tbl.columns
                    && !cols.contains(col)
                {
                    anyhow::bail!(
                        "[sync.{key}.fields] {col} is renamed but not in columns; \
                         it would never reach the target"
                    );
                }
                if let Some(cols) = &tbl.columns
                    && cols.contains(target)
                    && !tbl.fields.contains_key(target)
                {
                    anyhow::bail!(
                        "[sync.{key}.fields] {col} = {target:?} would overwrite column {target}"
                    );
                }
            }
            // the join field is not a column either: a rename onto it would be
            // buried, and a rename of it would name a column the target sees
            // under the join field's shape, not the row's
            if let Some(name) = tbl.join.as_ref().map(|join| &join.field)
                && (tbl.fields.contains_key(name) || tbl.fields.values().any(|t| t == name))
            {
                anyhow::bail!(
                    "[sync.{key}.fields] {name:?} is the join field; the join field is \
                     written last and would bury the renamed column"
                );
            }
            for child in &tbl.children {
                // the child's field is not a column, so a parent rename that
                // names it would either do nothing or bury the array
                for name in &child.claimed_fields() {
                    if tbl.fields.contains_key(name) || tbl.fields.values().any(|t| t == name) {
                        anyhow::bail!(
                            "[sync.{key}.fields] {name:?} is the field of child {}; \
                             rename the child's columns in its own fields instead",
                            child.table
                        );
                    }
                }
                check_field_map(
                    &format!("sync.{key}.children({}).fields", child.table),
                    &child.fields,
                )?;
                let child_key = format!("sync.{key}.children({})", child.table);
                if child.columns.is_some() && !child.exclude_columns.is_empty() {
                    anyhow::bail!(
                        "[{child_key}] columns and exclude_columns are mutually exclusive"
                    );
                }
                if child.columns.as_ref().is_some_and(|c| c.is_empty()) {
                    anyhow::bail!("[{child_key}] columns must not be empty");
                }
                match (&child.through, &child.through_key) {
                    (Some(through), Some(_)) => {
                        if !is_qualified_table(through) {
                            anyhow::bail!(
                                "[{child_key}] through table {through:?} must be \
                                 schema-qualified"
                            );
                        }
                        // the junction is a third table by definition: pointed
                        // at either end of the relation it joins nothing
                        if through == &child.table || through == &tbl.table {
                            anyhow::bail!(
                                "[{child_key}] through {through:?} is the same table as the \
                                 {} it would join; a junction is a table of its own",
                                if through == &child.table {
                                    "child"
                                } else {
                                    "parent"
                                }
                            );
                        }
                    }
                    (Some(_), None) => anyhow::bail!(
                        "[{child_key}] through needs through_key: the junction column \
                         referencing the child's primary key"
                    ),
                    (None, Some(_)) => anyhow::bail!(
                        "[{child_key}] through_key needs through: without a junction there \
                         is nothing for it to name"
                    ),
                    (None, None) => {}
                }
                if child.single && child.max_rows.is_some() {
                    anyhow::bail!(
                        "[{child_key}] single and max_rows contradict each other: a relation \
                         declared one-to-one has nothing to cap"
                    );
                }
                for (col, target) in &child.fields {
                    // a rename is a promise that the column reaches the target;
                    // projection dropping it would break that promise silently
                    if child.exclude_columns.contains(col) {
                        anyhow::bail!(
                            "[{child_key}.fields] {col} is renamed but also excluded; \
                             an excluded column never reaches the target"
                        );
                    }
                    if let Some(cols) = &child.columns
                        && !cols.contains(col)
                    {
                        anyhow::bail!(
                            "[{child_key}.fields] {col} is renamed but not in columns; \
                             it would never reach the target"
                        );
                    }
                    if let Some(cols) = &child.columns
                        && cols.contains(target)
                        && !child.fields.contains_key(target)
                    {
                        anyhow::bail!(
                            "[{child_key}.fields] {col} = {target:?} would overwrite column \
                             {target}"
                        );
                    }
                }
            }
            // qualification was checked at the top of this loop
            let (schema, table) = tbl.table.split_once('.').unwrap_or((&tbl.table, ""));
            let mut names: Vec<&String> = tbl.constants.keys().collect();
            names.sort();
            for name in names {
                if name.is_empty() {
                    anyhow::bail!("[sync.{key}.constants] a constant name must not be empty");
                }
                tbl.constants[name]
                    .render(schema, table)
                    .map_err(|e| anyhow::anyhow!("[sync.{key}.constants] {name}: {e}"))?;
                // a constant is written last, so any name that still reaches
                // the target would be buried by it; a rename *key* is fine,
                // that column leaves the document before constants run
                if tbl.fields.values().any(|t| t == name) {
                    anyhow::bail!(
                        "[sync.{key}.constants] {name} is also the target of a rename; \
                         the constant would bury the renamed column"
                    );
                }
                if tbl.columns.as_ref().is_some_and(|cols| cols.contains(name))
                    && !tbl.fields.contains_key(name)
                {
                    anyhow::bail!("[sync.{key}.constants] {name} would overwrite column {name}");
                }
                if let Some(child) = tbl
                    .children
                    .iter()
                    .find(|child| child.claimed_fields().contains(name))
                {
                    anyhow::bail!(
                        "[sync.{key}.constants] {name} is the field of child {}",
                        child.table
                    );
                }
                if tbl.fan_out.as_ref().is_some_and(|fan| &fan.field == name) {
                    anyhow::bail!(
                        "[sync.{key}.constants] {name} is the fan_out field; a scalar element \
                         lands under that name and the constant would bury it"
                    );
                }
                if tbl.join.as_ref().is_some_and(|join| &join.field == name) {
                    anyhow::bail!("[sync.{key}.constants] {name} is the join field");
                }
            }
            // an excluded column silently dropped from the key would produce
            // colliding document ids
            if let Some(pk) = &tbl.primary_key
                && tbl.exclude_columns.contains(pk)
            {
                anyhow::bail!("[sync.{key}] primary_key {pk:?} cannot be in exclude_columns");
            }
            for child in &tbl.children {
                if !is_qualified_table(&child.table) {
                    anyhow::bail!(
                        "[sync.{key}] child table {:?} must be schema-qualified",
                        child.table
                    );
                }
            }
            targets.push((key.as_str(), target));
        }
        check_junctions(self)?;
        check_template_claims(self, &targets)?;
        for (index, members) in &by_index {
            check_index_group(index, members)?;
        }
        for (key, table) in &self.sync {
            // the predicate is evaluated by the database inside the poll query;
            // WAL mode has no query to put it in, and sees a soft delete as the
            // ordinary UPDATE it is
            if table.soft_delete.is_some() && self.source.mode != "poll" {
                anyhow::bail!(
                    "[sync.{key}] soft_delete only applies in poll mode; \
                     WAL mode already propagates deletes"
                );
            }
        }

        Ok(())
    }

    /// Resolve env-var indirections; returns deprecation warnings for any
    /// plain-text secret found in the file.
    pub fn resolve_secrets(&self) -> Result<ResolvedSecrets> {
        let mut warnings = Vec::new();

        let source_url = match (&self.source.url_env, &self.source.url) {
            (Some(env_key), _) => std::env::var(env_key).map_err(|_| {
                anyhow::anyhow!("source.url_env={env_key:?} is set but variable is missing")
            })?,
            (None, Some(url)) => {
                warnings.push(
                    "source.url contains credentials in plain text; prefer source.url_env (deprecated)"
                        .into(),
                );
                url.clone()
            }
            (None, None) => anyhow::bail!("either source.url or source.url_env is required"),
        };

        let target_password = match (&self.target.password_env, &self.target.password) {
            (Some(env_key), _) => Some(std::env::var(env_key).map_err(|_| {
                anyhow::anyhow!("target.password_env={env_key:?} is set but variable is missing")
            })?),
            (None, Some(pw)) => {
                warnings.push(
                    "target.password contains a plain-text secret; prefer target.password_env (deprecated)"
                        .into(),
                );
                Some(pw.clone())
            }
            (None, None) => None,
        };

        let admin_url = match &self.source.admin_url_env {
            Some(env_key) => std::env::var(env_key).map_err(|_| {
                anyhow::anyhow!("source.admin_url_env={env_key:?} is set but variable is missing")
            })?,
            None => source_url.clone(),
        };

        Ok(ResolvedSecrets {
            source_url,
            admin_url,
            target_password,
            warnings,
        })
    }
}

impl AppConfig {
    /// Effective TLS settings for a resolved source URL.
    pub fn tls_settings(&self, source_url: &str) -> Result<pg2osync_source::tls::TlsSettings> {
        Ok(pg2osync_source::tls::TlsSettings::resolve(
            source_url,
            pg2osync_source::tls::ConfiguredTls {
                sslmode: self.source.sslmode.as_deref(),
                sslrootcert: self.source.sslrootcert.as_deref(),
                sslcert: self.source.sslcert.as_deref(),
                sslkey: self.source.sslkey.as_deref(),
            },
        )?)
    }
}

impl SourceConfig {
    pub fn reconnect_policy(&self) -> pg2osync_source::reconnect::ReconnectPolicy {
        pg2osync_source::reconnect::ReconnectPolicy {
            max_attempts: self.reconnect_max,
            base_backoff_ms: self.reconnect_backoff_ms.max(1),
        }
    }
}

fn is_qualified_table(name: &str) -> bool {
    let parts: Vec<&str> = name.split('.').collect();
    parts.len() == 2 && parts.iter().all(|p| !p.is_empty())
}

/// One junction table belongs to one relation.
///
/// The streamed row of a junction is resolved through a single mapping from its
/// table to the parent it feeds, so two sections naming the same junction with
/// different parents — or the same parent through a different `foreign_key` —
/// describe something the stream cannot represent: one of the two would win and
/// the other would go stale without a word.
fn check_junctions(cfg: &AppConfig) -> Result<()> {
    let mut seen: std::collections::HashMap<&str, (&str, &str, &str)> =
        std::collections::HashMap::new();
    for (key, tbl) in &cfg.sync {
        for child in &tbl.children {
            let Some(through) = &child.through else {
                continue;
            };
            let entry = (tbl.table.as_str(), child.foreign_key.as_str(), key.as_str());
            match seen.get(through.as_str()) {
                Some((table, foreign_key, first))
                    if (*table, *foreign_key) != (entry.0, entry.1) =>
                {
                    anyhow::bail!(
                        "[sync.{key}] junction {through} is already used by [sync.{first}]                          for {table} through {foreign_key}; one junction table names one                          parent, or a streamed junction row cannot say which"
                    );
                }
                Some(_) => {}
                None => {
                    seen.insert(through.as_str(), entry);
                }
            }
        }
    }
    Ok(())
}

/// A template is a claim on a whole namespace, so it cannot also be a shared
/// index, and nothing else may sit inside what its TRUNCATE would clear.
fn check_template_claims(cfg: &AppConfig, targets: &[(&str, IndexTarget)]) -> Result<()> {
    for (key, target) in targets {
        let IndexTarget::Template { spec, .. } = target else {
            continue;
        };
        if let Some((other, _)) = targets
            .iter()
            .find(|(k, _)| k != key && cfg.sync[*k].index_name(k) == *spec)
        {
            anyhow::bail!(
                "[sync.{key}] index {spec:?} is a template and is also fed by [sync.{other}]; a \
                 template claims every index it can render, which is not something two tables \
                 can share"
            );
        }
        let pattern = target.pattern();
        if let Some((other, _)) = targets
            .iter()
            .find(|(k, t)| k != key && index_matches_pattern(&pattern, &t.pattern()))
        {
            anyhow::bail!(
                "[sync.{key}] index {spec:?} claims {pattern:?}, which also matches {:?} of \
                 [sync.{other}]; a TRUNCATE of {} would clear that index too",
                cfg.sync[*other].index_name(other),
                cfg.sync[*key].table
            );
        }
    }
    Ok(())
}

/// Whether the sections writing one index may share it: either as a join
/// pair, or with an explicit id on each. A section alone in its index is only
/// refused when it is a child, whose parent then exists nowhere.
fn check_index_group(index: &str, members: &[(&str, &TableSync)]) -> Result<()> {
    let [(key, single)] = members else {
        return check_shared_index(index, members);
    };
    if let Some(column) = single.join.as_ref().and_then(|join| join.parent.as_deref()) {
        anyhow::bail!(
            "[sync.{key}] join names a parent through {column}, but no other [sync.*] section \
             writes index {index:?} as its parent"
        );
    }
    Ok(())
}

/// An index built before pg2osync is usually a union of several tables. What
/// made that unsafe was `_id` inherited from each table's own key: two tables
/// with a row 1 are one document. An explicit `id` on every section sharing
/// the index is the declaration that makes a collision the operator's choice,
/// so the only ones left are the ones written down.
fn check_plain_shared_index(index: &str, members: &[(&str, &TableSync)]) -> Result<()> {
    if let Some((key, _)) = members.iter().find(|(_, tbl)| tbl.id.is_none()) {
        anyhow::bail!(
            "[sync.{key}] two tables map to the same index {index:?}; give each an explicit \
             id template so their documents cannot collide, or declare a join pair"
        );
    }
    let described: Vec<&str> = members
        .iter()
        .filter(|(_, tbl)| tbl.mapping_file.is_some())
        .map(|(key, _)| *key)
        .collect();
    if let [first, second, ..] = described.as_slice() {
        anyhow::bail!(
            "[sync.{second}] index {index:?} is also described by [sync.{first}]; an index is \
             created once, so at most one of the sections feeding it may set mapping_file"
        );
    }
    Ok(())
}

/// A join pair: every section declares the same join field, exactly one is
/// the parent, and each has a relation name of its own.
fn check_shared_index(index: &str, members: &[(&str, &TableSync)]) -> Result<()> {
    let Some((joined_key, _)) = members.iter().find(|(_, tbl)| tbl.join.is_some()) else {
        return check_plain_shared_index(index, members);
    };
    let mut joined = Vec::with_capacity(members.len());
    for (key, tbl) in members {
        let Some(join) = &tbl.join else {
            // A join field is read on every document of the index, so a table
            // writing there without one leaves documents no relation names.
            anyhow::bail!(
                "[sync.{key}] writes index {index:?} without join, but [sync.{joined_key}] \
                 declares one; every section sharing a join field's index must be part of \
                 the pair"
            );
        };
        joined.push((*key, *tbl, join));
    }
    // the slice is non-empty: a group exists because a section put itself in it
    let Some((first_key, _, first)) = joined.first() else {
        return Ok(());
    };
    for (key, _, join) in &joined {
        if join.field != first.field {
            anyhow::bail!(
                "[sync.{key}.join] field {:?} disagrees with [sync.{first_key}.join] field {:?}; \
                 every section writing to index {index:?} must name the same join field",
                join.field,
                first.field
            );
        }
    }
    let parents: Vec<(&str, &TableSync)> = joined
        .iter()
        .filter(|(_, _, join)| join.parent.is_none())
        .map(|(key, tbl, _)| (*key, *tbl))
        .collect();
    let (parent_key, parent) = match parents.as_slice() {
        [] => anyhow::bail!(
            "index {index:?} has no join parent: every section writing to it names a parent \
             column, so nothing writes the parent documents. Remove parent from the section \
             that holds them"
        ),
        [(a, _), (b, _), ..] => anyhow::bail!(
            "[sync.{a}] and [sync.{b}] are both the join parent of index {index:?}; exactly one \
             section may omit parent"
        ),
        [one] => *one,
    };
    for (i, (a, _, join)) in joined.iter().enumerate() {
        if let Some((b, _, _)) = joined[i + 1..].iter().find(|(_, _, o)| o.name == join.name) {
            anyhow::bail!(
                "[sync.{a}] and [sync.{b}] both use the join name {:?}; each section of index \
                 {index:?} needs its own relation name",
                join.name
            );
        }
    }
    // the child holds one column and renders the parent's id from it alone,
    // so the parent's id may name nothing but the key
    if let Some(spec) = &parent.id {
        let pk = vec![parent.primary_key.clone().unwrap_or_else(|| "id".into())];
        let template = pg2osync_engine::mapping::IdTemplate::parse(spec, &pk).map_err(|e| {
            anyhow::anyhow!("[sync.{parent_key}] id {spec:?} is not a usable id: {e}")
        })?;
        if !template.is_pk_only()
            && let Some(column) = joined
                .iter()
                .find_map(|(_, _, join)| join.parent.as_deref())
        {
            anyhow::bail!(
                "[sync.{parent_key}] id {spec:?} names a column outside the primary key, so a \
                 join child cannot compute the parent's document id from its own {column} \
                 column. Give the parent an id that names only its key"
            );
        }
    }
    Ok(())
}

/// The shape checks a rename map needs regardless of what it applies to.
/// A target that is another key of the same map is allowed: that column is
/// itself renamed away, so nothing is overwritten.
fn check_field_map(
    section: &str,
    fields: &std::collections::HashMap<String, String>,
) -> Result<()> {
    let mut by_target: std::collections::HashMap<&str, &str> = std::collections::HashMap::new();
    let mut keys: Vec<&String> = fields.keys().collect();
    keys.sort();
    for col in keys {
        let target = &fields[col];
        if col.is_empty() || target.is_empty() {
            anyhow::bail!("[{section}] a column name and its target must not be empty");
        }
        // an option that does nothing implies a guarantee it does not give
        if col == target {
            anyhow::bail!("[{section}] {col} renames a column to itself");
        }
        if let Some(other) = by_target.insert(target, col) {
            anyhow::bail!("[{section}] {other} and {col} would both be stored as {target:?}");
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(toml_str: &str) -> Result<AppConfig> {
        let cfg: AppConfig = toml::from_str(toml_str)?;
        cfg.validate()?;
        Ok(cfg)
    }

    const MINIMAL: &str = r#"
[source]
url = "postgres://u:p@localhost/db"
[target]
url = "http://localhost:9200"
[sync.users]
table = "public.users"
"#;

    #[test]
    fn minimal_config_defaults_to_postgres_and_opensearch() {
        let cfg = parse(MINIMAL).expect("valid");
        assert_eq!(cfg.source.flavor, "postgres");
        assert_eq!(cfg.target.flavor, "opensearch");
        assert_eq!(cfg.sync["users"].index_name("users"), "users");
        assert!(cfg.metrics.enabled, "metrics must default to on");
    }

    #[test]
    fn a_load_rate_limit_of_zero_is_refused() {
        let capped =
            parse(&MINIMAL.replace("[target]", "[engine]\nload_max_rows_per_sec = 50\n[target]"))
                .expect("a ceiling an operator named is valid");
        assert_eq!(capped.engine.load_max_rows_per_sec, Some(50));
        assert_eq!(
            parse(MINIMAL).expect("valid").engine.load_max_rows_per_sec,
            None,
            "unset means unlimited"
        );
        let message = refused(
            &MINIMAL.replace("[target]", "[engine]\nload_max_rows_per_sec = 0\n[target]"),
            "zero is not a rate",
        );
        assert!(
            message.contains("unset") && message.contains("unlimited"),
            "the refusal says what to write instead: {message}"
        );
    }

    #[test]
    fn unknown_flavors_and_modes_are_rejected() {
        assert!(parse(&MINIMAL.replace("[source]", "[source]\nflavor = \"mongodb\"")).is_err());
        assert!(parse(&MINIMAL.replace("[target]", "[target]\nflavor = \"solr\"")).is_err());
        assert!(parse(&MINIMAL.replace("[source]", "[source]\nmode = \"trigger\"")).is_err());
        assert!(
            parse(&MINIMAL.replace("[source]", "[source]\nflavor = \"mysql\"\nmode = \"poll\""))
                .is_err(),
            "poll mode is PostgreSQL-only"
        );
    }

    #[test]
    fn a_client_certificate_needs_both_halves() {
        let fixtures = format!("{}/../tls/tests/fixtures", env!("CARGO_MANIFEST_DIR"));
        assert!(
            parse(&MINIMAL.replace(
                "[source]",
                &format!("[source]\nsslcert = \"{fixtures}/client.crt\"")
            ))
            .is_err(),
            "sslcert alone must be refused"
        );
        assert!(
            parse(&MINIMAL.replace(
                "[source]",
                &format!("[source]\nsslkey = \"{fixtures}/pkcs8.key\"")
            ))
            .is_err(),
            "sslkey alone must be refused"
        );

        let cfg = parse(&MINIMAL.replace(
            "[source]",
            &format!(
                "[source]\nsslcert = \"{fixtures}/client.crt\"\nsslkey = \"{fixtures}/pkcs8.key\""
            ),
        ))
        .expect("both halves are valid");
        let tls = cfg
            .tls_settings("postgres://u:p@localhost/db")
            .expect("resolves");
        assert_eq!(
            tls.client_cert,
            Some(format!("{fixtures}/client.crt").into())
        );
        assert_eq!(tls.client_key, Some(format!("{fixtures}/pkcs8.key").into()));
    }

    #[test]
    fn index_names_and_duplicates_are_validated() {
        assert!(parse(&MINIMAL.replace("table = \"public.users\"", "table = \"users\"")).is_err());
        assert!(parse(&format!("{MINIMAL}index = \"_bad\"\n")).is_err());
        assert!(parse(&format!("{MINIMAL}index = \"Users\"\n")).is_err());
    }

    #[test]
    fn a_fixed_index_is_a_static_target() {
        let cfg = parse(&format!("{MINIMAL}index = \"users_v2\"\n")).expect("valid");
        let tbl = &cfg.sync["users"];
        assert!(!tbl.is_templated());
        assert_eq!(
            tbl.index_target("users", &[]),
            Ok(IndexTarget::Static("users_v2".into()))
        );
        assert_eq!(
            cfg.sync["users"].index_name("users"),
            "users_v2",
            "the name as written is what messages show"
        );
    }

    #[test]
    fn a_per_row_index_template_is_accepted() {
        let cfg = parse(&format!("{MINIMAL}index = \"events-{{tenant}}\"\n")).expect("valid");
        let tbl = &cfg.sync["users"];
        assert!(tbl.is_templated());
        let target = tbl
            .index_target("users", &["id".to_string()])
            .expect("a template with a literal prefix");
        assert!(matches!(&target, IndexTarget::Template { spec, .. } if spec == "events-{tenant}"));
        assert_eq!(target.pattern(), "events-*");
        assert_eq!(
            tbl.index_name("users"),
            "events-{tenant}",
            "index_name keeps the spec as written"
        );
        parse(&format!("{MINIMAL}index = \"{{tenant}}-events\"\n"))
            .expect("a leading placeholder is checked where the row renders it");
    }

    #[test]
    fn an_index_template_of_only_placeholders_is_refused() {
        for spec in ["{tenant}", "{tenant}{region}"] {
            let message = refused(
                &format!("{MINIMAL}index = \"{spec}\"\n"),
                "a template without a literal",
            );
            assert!(
                message.contains("all placeholders") && message.contains("public.users"),
                "{message}"
            );
        }
    }

    #[test]
    fn an_index_template_literal_follows_the_index_grammar() {
        let message = refused(
            &format!("{MINIMAL}index = \"Events-{{tenant}}\"\n"),
            "an uppercase leading literal",
        );
        assert!(
            message.contains(
                "[sync.users] index \"Events-{tenant}\" must start with a lowercase letter"
            ),
            "{message}"
        );
        let message = refused(
            &format!("{MINIMAL}index = \"{{tenant}}-Events\"\n"),
            "an uppercase literal after a placeholder",
        );
        assert!(
            message.contains("may only contain lowercase [a-z0-9_-]"),
            "{message}"
        );
        let message = refused(
            &format!("{MINIMAL}index = \"events-{{\"\n"),
            "an unbalanced brace",
        );
        assert!(
            message.contains("is not a usable index template"),
            "{message}"
        );
    }

    #[test]
    fn an_index_template_cannot_also_be_shared() {
        let message = refused(
            &UNION.replace("index = \"same\"", "index = \"events-{tenant}\""),
            "two sections on one template",
        );
        assert!(
            message.contains(
                "[sync.orders] index \"events-{tenant}\" is a template and is also fed by \
                 [sync.users]"
            ),
            "{message}"
        );
    }

    #[test]
    fn an_index_template_may_not_claim_another_sections_index() {
        let toml_str = format!(
            "{MINIMAL}index = \"events-{{tenant}}\"\n[sync.archive]\ntable = \"public.archive\"\n\
             index = \"events-archive\"\n"
        );
        let message = refused(&toml_str, "a fixed index inside the template's pattern");
        assert!(
            message.contains(
                "[sync.users] index \"events-{tenant}\" claims \"events-*\", which also matches \
                 \"events-archive\" of [sync.archive]"
            ) && message.contains("TRUNCATE of public.users"),
            "{message}"
        );
        let toml_str = format!(
            "{MINIMAL}index = \"events-{{tenant}}\"\n[sync.archive]\ntable = \"public.archive\"\n\
             index = \"events-{{tenant}}-{{year}}\"\n"
        );
        let message = refused(&toml_str, "a template inside another template's pattern");
        assert!(message.contains("claims \"events-*\""), "{message}");
        parse(&format!(
            "{MINIMAL}index = \"events-{{tenant}}\"\n[sync.archive]\ntable = \"public.archive\"\n\
             index = \"archive\"\n"
        ))
        .expect("an index outside the pattern is untouched by the claim");
    }

    #[test]
    fn an_index_template_may_not_name_the_fan_out_field() {
        let message = refused(
            &format!(
                "{MINIMAL}index = \"u-{{tags}}\"\n[sync.users.fan_out]\nfield = \"tags\"\n\
                 id = \"user-{{id}}-{{tag}}\"\n"
            ),
            "the index chosen from the array",
        );
        assert!(
            message.contains("[sync.users] index \"u-{tags}\" names the fan_out field tags"),
            "{message}"
        );
        parse(&format!(
            "{MINIMAL}index = \"u-{{tenant}}\"\n[sync.users.fan_out]\nfield = \"tags\"\n\
             id = \"user-{{id}}-{{tag}}\"\n"
        ))
        .expect("a template over a row column composes with fan-out");
    }

    /// Two plain sections into one index, each holding a place for its id.
    const UNION: &str = r#"
[source]
url = "postgres://u:p@localhost/db"
[target]
url = "http://localhost:9200"
[sync.orders]
table = "public.orders"
index = "same"
id = "order-{id}"
[sync.users]
table = "public.users"
index = "same"
id = "user-{id}"
"#;

    fn refused(toml_str: &str, why: &str) -> String {
        parse(toml_str)
            .err()
            .unwrap_or_else(|| panic!("must be refused: {why}"))
            .to_string()
    }

    #[test]
    fn an_index_two_tables_share_needs_an_explicit_id_on_each() {
        let neither = UNION
            .replace("id = \"order-{id}\"\n", "")
            .replace("id = \"user-{id}\"\n", "");
        let message = refused(&neither, "two tables in one index without ids");
        assert!(
            message.contains("[sync.orders]") && message.contains("explicit id template"),
            "the refusal names the first section without an id: {message}"
        );
        let one = UNION.replace("id = \"user-{id}\"\n", "");
        let message = refused(&one, "an id on only one of the two");
        assert!(
            message.contains("[sync.users]") && message.contains("explicit id template"),
            "{message}"
        );
    }

    #[test]
    fn two_tables_may_share_an_index_once_each_declares_its_id() {
        let cfg = parse(UNION).expect("both sections declare their identity");
        assert_eq!(cfg.sync["orders"].index_name("orders"), "same");
        assert_eq!(cfg.sync["users"].index_name("users"), "same");
        parse(&UNION.replace(
            "index = \"same\"\nid = \"user-{id}\"",
            "index = \"same\"\nid = \"user-{id}\"\nmapping_file = \"m.json\"",
        ))
        .expect("one section may describe the index");
    }

    #[test]
    fn only_one_section_may_describe_a_shared_index() {
        let both = UNION.replace(
            "index = \"same\"",
            "index = \"same\"\nmapping_file = \"m.json\"",
        );
        let message = refused(&both, "two mapping files for one index");
        assert!(
            message.contains("[sync.users] index \"same\" is also described by [sync.orders]")
                && message.contains("mapping_file"),
            "{message}"
        );
    }

    #[test]
    fn a_mixed_join_and_plain_group_is_refused() {
        let mixed = UNION.replace(
            "id = \"order-{id}\"",
            "id = \"order-{id}\"\n[sync.orders.join]\nfield = \"relation\"\nname = \"order\"",
        );
        let message = refused(&mixed, "a join parent beside a plain section");
        assert!(
            message.contains("[sync.users] writes index \"same\" without join")
                && message.contains("[sync.orders]"),
            "{message}"
        );
    }

    #[test]
    fn shared_indexes_names_only_the_indices_more_than_one_table_feeds() {
        let cfg = parse(&format!(
            "{UNION}[sync.carts]\ntable = \"public.carts\"\nindex = \"alone\"\n"
        ))
        .expect("loads");
        let shared = cfg.shared_indexes();
        assert!(shared.contains("same"));
        assert!(!shared.contains("alone"));
        assert_eq!(shared.len(), 1);
        assert!(
            !cfg.is_join_index("same"),
            "two plain sections are not a pair"
        );
        assert!(!cfg.is_join_index("alone"));
        assert!(
            !cfg.is_join_index("nowhere"),
            "an index nothing writes is not a join index"
        );
    }

    #[test]
    fn transform_specs_are_parsed_and_their_parameters_checked() {
        let cfg = parse(&format!(
            "{MINIMAL}[sync.users.transform]\nemail = \"redact\"\nprice = \"number\"\n\
             payload = {{ op = \"json\" }}\ntags = {{ op = \"split\", by = \",\" }}\n\
             born = {{ op = \"date\", from = \"%d/%m/%Y\" }}\n"
        ))
        .expect("both forms load");
        let t = &cfg.sync["users"].transform;
        assert_eq!(t["email"].parse(), Ok(TransformOp::Redact));
        assert_eq!(t["payload"].parse(), Ok(TransformOp::Json));
        assert_eq!(t["tags"].parse(), Ok(TransformOp::Split { by: ",".into() }));
        assert_eq!(
            t["born"].parse(),
            Ok(TransformOp::Date {
                from: "%d/%m/%Y".into()
            })
        );

        let refused = [
            ("tags = { op = \"split\" }", "split without by"),
            (
                "tags = { op = \"split\", by = \"\" }",
                "split with an empty by",
            ),
            ("born = { op = \"date\" }", "date without from"),
            (
                "born = { op = \"date\", from = \"\" }",
                "date with an empty from",
            ),
            (
                "email = { op = \"hash\", by = \",\" }",
                "a parameter hash does not take",
            ),
            (
                "tags = { op = \"split\", by = \",\", from = \"x\" }",
                "a parameter split does not take",
            ),
            ("price = 3", "neither a name nor a table"),
        ];
        for (line, why) in refused {
            assert!(
                parse(&format!("{MINIMAL}[sync.users.transform]\n{line}\n")).is_err(),
                "{why}"
            );
        }
        let err = parse(&format!(
            "{MINIMAL}[sync.users.transform]\ntags = {{ op = \"split\", by = \",\", nope = 1 }}\n"
        ))
        .expect_err("an unknown key is refused");
        assert!(
            format!("{err:#}").contains("nope"),
            "the error names the key, which is what the hand-written visitor is for: {err:#}"
        );
        assert!(
            parse(&format!(
                "{MINIMAL}[sync.users.fan_out]\nfield = \"tags\"\nid = \"u-{{id}}-{{tags}}\"\n\
                 [sync.users.transform]\ntags = {{ op = \"split\", by = \",\" }}\n"
            ))
            .is_err(),
            "a transform on the fan_out field never reaches fan-out"
        );
    }

    #[test]
    fn a_where_predicate_is_parsed_and_its_grammar_bounded() {
        parse(&format!(
            "{MINIMAL}where = \"status = 'active' AND tenant IN ('eu','us') AND deleted_at IS NULL\"\n"
        ))
        .expect("the supported subset loads");
        let err = parse(&format!("{MINIMAL}where = \"status LIKE 'a%'\"\n"))
            .expect_err("LIKE is outside the subset");
        let text = format!("{err:#}");
        assert!(text.contains("LIKE"), "names the operator: {text}");
        assert!(
            text.contains("supported:"),
            "lists what is supported: {text}"
        );
        parse(&format!(
            "{}where = \"tenant = 'eu'\"\nsoft_delete = \"deleted_at IS NOT NULL\"\n",
            MINIMAL.replace(
                "[source]",
                "[source]\nmode = \"poll\"\npoll_column = \"updated_at\""
            )
        ))
        .expect("where and soft_delete compose in poll mode");
    }

    #[test]
    fn transforms_and_projections_are_checked() {
        assert!(
            parse(&format!(
                "{MINIMAL}[sync.users.transform]\nemail = \"encrypt\"\n"
            ))
            .is_err(),
            "unknown transform op"
        );
        assert!(
            parse(&format!(
                "{MINIMAL}columns = [\"id\"]\nexclude_columns = [\"x\"]\n"
            ))
            .is_err(),
            "columns and exclude_columns are mutually exclusive"
        );
        assert!(
            parse(&format!(
                "{MINIMAL}primary_key = \"id\"\nexclude_columns = [\"id\"]\n"
            ))
            .is_err(),
            "the key column cannot be excluded"
        );
    }

    #[test]
    fn fields_are_validated_against_projection_and_each_other() {
        let refused = [
            ("[sync.users.fields]\n\"\" = \"x\"\n", "empty column"),
            ("[sync.users.fields]\nname = \"\"\n", "empty target"),
            (
                "[sync.users.fields]\nname = \"name\"\n",
                "a column renamed to itself",
            ),
            (
                "[sync.users.fields]\na = \"x\"\nb = \"x\"\n",
                "two columns stored under one name",
            ),
            (
                "exclude_columns = [\"secret\"]\n[sync.users.fields]\nsecret = \"s\"\n",
                "an excluded column cannot be renamed",
            ),
            (
                "columns = [\"id\"]\n[sync.users.fields]\nname = \"n\"\n",
                "a column outside the projection cannot be renamed",
            ),
            (
                "columns = [\"id\", \"email\"]\n[sync.users.fields]\nid = \"email\"\n",
                "a target that is a surviving column would overwrite it",
            ),
            (
                "[sync.users.fields]\nname = \"orders\"\n[[sync.users.children]]\ntable = \"public.orders\"\nfield = \"orders\"\nforeign_key = \"user_id\"\n",
                "a target that is a child field",
            ),
            (
                "[sync.users.fields]\norders = \"o\"\n[[sync.users.children]]\ntable = \"public.orders\"\nfield = \"orders\"\nforeign_key = \"user_id\"\n",
                "a key that is a child field",
            ),
            (
                "[[sync.users.children]]\ntable = \"public.orders\"\nfield = \"orders\"\nforeign_key = \"user_id\"\n[sync.users.children.fields]\ntotal = \"total\"\n",
                "a child column renamed to itself",
            ),
            (
                "[[sync.users.children]]\ntable = \"public.orders\"\nfield = \"orders\"\nforeign_key = \"user_id\"\n[sync.users.children.fields]\na = \"x\"\nb = \"x\"\n",
                "two child columns stored under one name",
            ),
        ];
        for (extra, why) in refused {
            assert!(parse(&format!("{MINIMAL}{extra}")).is_err(), "{why}");
        }

        parse(&format!(
            "{MINIMAL}[sync.users.fields]\na = \"b\"\nb = \"a\"\n"
        ))
        .expect("a swap renames both columns away, so neither is overwritten");
        parse(&format!(
            "{MINIMAL}[sync.users.fields]\nemail = \"contact\"\n[sync.users.transform]\nemail = \"redact\"\n"
        ))
        .expect("transforms keep naming the source column");
        let cfg = parse(&format!(
            "{MINIMAL}[[sync.users.children]]\ntable = \"public.orders\"\nfield = \"orders\"\nforeign_key = \"user_id\"\n[sync.users.children.fields]\ntotal = \"amount\"\n"
        ))
        .expect("a child rename parses");
        assert_eq!(
            cfg.sync["users"].children[0].fields["total"], "amount",
            "the child's fields attach to that child"
        );
    }

    #[test]
    fn a_many_to_many_child_names_its_junction_and_both_of_its_columns() {
        const M2M: &str = "[[sync.users.children]]\ntable = \"public.tags\"\n\
                           field = \"tags\"\nforeign_key = \"user_id\"\n";
        let refused = [
            (
                format!("{M2M}through = \"public.user_tag\"\n"),
                "a junction with no column pointing at the child",
            ),
            (
                format!("{M2M}through_key = \"tag_id\"\n"),
                "a junction column with no junction",
            ),
            (
                format!("{M2M}through = \"user_tag\"\nthrough_key = \"tag_id\"\n"),
                "an unqualified junction",
            ),
            (
                format!("{M2M}through = \"public.tags\"\nthrough_key = \"tag_id\"\n"),
                "the child as its own junction",
            ),
            (
                format!("{M2M}through = \"public.users\"\nthrough_key = \"tag_id\"\n"),
                "the parent as the junction",
            ),
            (
                format!(
                    "{M2M}through = \"public.user_tag\"\nthrough_key = \"tag_id\"\n\
                     [sync.other]\ntable = \"public.posts\"\n\
                     [[sync.other.children]]\ntable = \"public.tags\"\nfield = \"tags\"\n\
                     foreign_key = \"post_id\"\nthrough = \"public.user_tag\"\n\
                     through_key = \"tag_id\"\n"
                ),
                "one junction naming two parents: a streamed junction row could \
                 not say which",
            ),
        ];
        for (extra, why) in refused {
            assert!(parse(&format!("{MINIMAL}{extra}")).is_err(), "{why}");
        }

        let cfg = parse(&format!(
            "{MINIMAL}{M2M}through = \"public.user_tag\"\nthrough_key = \"tag_id\"\n\
             max_rows = 5\nsingle = false\n"
        ))
        .expect("a many-to-many child parses, cap and all");
        let child = &cfg.sync["users"].children[0];
        assert_eq!(child.through.as_deref(), Some("public.user_tag"));
        assert_eq!(child.through_key.as_deref(), Some("tag_id"));
        assert_eq!(
            child.foreign_key, "user_id",
            "the foreign key keeps its name and changes its home"
        );
        parse(&format!(
            "{MINIMAL}[[sync.users.children]]\ntable = \"public.tags\"\nfield = \"tag\"\n\
             foreign_key = \"user_id\"\nthrough = \"public.user_tag\"\n\
             through_key = \"tag_id\"\nsingle = true\n"
        ))
        .expect("a one-to-one relation may still be recorded in a junction");
    }

    #[test]
    fn a_child_collection_projects_its_own_columns() {
        const CHILD: &str = "[[sync.users.children]]\ntable = \"public.orders\"\n\
                             field = \"orders\"\nforeign_key = \"user_id\"\n";
        let refused = [
            (
                "columns = [\"id\"]\nexclude_columns = [\"note\"]\n",
                "a projection is one list or the other",
            ),
            ("columns = []\n", "an empty projection embeds nothing"),
            (
                "exclude_columns = [\"note\"]\n[sync.users.children.fields]\nnote = \"n\"\n",
                "an excluded child column cannot be renamed",
            ),
            (
                "columns = [\"id\"]\n[sync.users.children.fields]\nnote = \"n\"\n",
                "a child column outside the projection cannot be renamed",
            ),
            (
                "columns = [\"id\", \"note\"]\n[sync.users.children.fields]\nid = \"note\"\n",
                "a target that is a surviving child column would overwrite it",
            ),
        ];
        for (extra, why) in refused {
            assert!(parse(&format!("{MINIMAL}{CHILD}{extra}")).is_err(), "{why}");
        }

        let cfg = parse(&format!(
            "{MINIMAL}{CHILD}exclude_columns = [\"internal_notes\"]\n"
        ))
        .expect("a child exclusion parses");
        assert_eq!(
            cfg.sync["users"].children[0].exclude_columns,
            vec!["internal_notes".to_string()],
            "the exclusion attaches to that child, not to the parent"
        );
        assert!(cfg.sync["users"].exclude_columns.is_empty());
    }

    #[test]
    fn a_one_to_one_child_embeds_the_element_and_claims_only_its_field() {
        const CHILD: &str = "[[sync.users.children]]\ntable = \"public.profiles\"\n\
                             field = \"profile\"\nforeign_key = \"user_id\"\nsingle = true\n";
        assert!(
            parse(&format!("{MINIMAL}{CHILD}max_rows = 5\n")).is_err(),
            "a relation declared one-to-one has nothing to cap"
        );

        let cfg = parse(&format!(
            "{MINIMAL}{CHILD}[sync.users.children.fields]\nbio = \"about\"\n"
        ))
        .expect("a one-to-one child renames its own columns like any other");
        assert!(cfg.sync["users"].children[0].single);

        // no array means no cap, so the two names a cap writes are free
        parse(&format!(
            "{MINIMAL}{CHILD}[sync.users.constants]\nprofile_total = \"v\"\n"
        ))
        .expect("a single child does not claim profile_total");
        assert!(
            parse(&format!(
                "{MINIMAL}{CHILD}[sync.users.constants]\nprofile = \"v\"\n"
            ))
            .is_err(),
            "the field itself is still claimed"
        );
    }

    #[test]
    fn constants_are_scalar_and_checked_against_the_document_shape() {
        const CHILD: &str = "[[sync.users.children]]\ntable = \"public.orders\"\nfield = \"orders\"\nforeign_key = \"user_id\"\n";
        let refused = [
            ("[sync.users.constants]\n\"\" = \"x\"\n", "empty name"),
            (
                "[sync.users.constants]\ntags = [\"a\"]\n",
                "an array is not a scalar",
            ),
            (
                "[sync.users.constants]\nnested = { a = 1 }\n",
                "a table is not a scalar",
            ),
            (
                "[sync.users.constants]\nwhen = 2026-08-29\n",
                "a datetime is not a scalar",
            ),
            (
                "[sync.users.constants]\norigin = \"{nope}\"\n",
                "an unknown placeholder",
            ),
            (
                "[sync.users.constants]\norigin = \"{\"\n",
                "a malformed template",
            ),
            (
                "[sync.users.fields]\nname = \"x\"\n[sync.users.constants]\nx = \"v\"\n",
                "a rename target",
            ),
            (
                "columns = [\"id\", \"name\"]\n[sync.users.constants]\nname = \"v\"\n",
                "a surviving column",
            ),
            (
                &format!("{CHILD}[sync.users.constants]\norders = \"v\"\n"),
                "a child field",
            ),
            (
                &format!("{CHILD}[sync.users.constants]\norders_total = \"v\"\n"),
                "a child's cap field",
            ),
            (
                "[sync.users.fan_out]\nfield = \"tags\"\nid = \"u-{id}-{tags}\"\n[sync.users.constants]\ntags = \"v\"\n",
                "the fan_out field",
            ),
        ];
        for (extra, why) in refused {
            assert!(parse(&format!("{MINIMAL}{extra}")).is_err(), "{why}");
        }

        let cfg = parse(&format!(
            "{MINIMAL}[sync.users.constants]\nentity = \"user\"\norigin = \"{{schema}}.{{table}}\"\nrank = 3\nactive = true\nnote = \"\"\n"
        ))
        .expect("scalars and the two placeholders load");
        let constants = &cfg.sync["users"].constants;
        assert_eq!(constants["rank"], Constant::Int(3));
        assert_eq!(constants["active"], Constant::Bool(true));
        assert_eq!(
            constants["origin"],
            Constant::Str("{schema}.{table}".into()),
            "rendering is the run's job, the config keeps the template"
        );
        parse(&format!(
            "{MINIMAL}[sync.users.fields]\nname = \"n\"\n[sync.users.constants]\nname = \"v\"\n"
        ))
        .expect("a renamed column leaves its name free");
    }

    #[test]
    fn placeholders_render_from_the_section_table() {
        assert_eq!(
            Constant::Str("{schema}.{table}".into()).render("public", "users"),
            Ok(serde_json::json!("public.users"))
        );
        assert_eq!(
            Constant::Int(3).render("public", "users"),
            Ok(serde_json::json!(3))
        );
        assert_eq!(
            Constant::Str("".into()).render("public", "users"),
            Ok(serde_json::json!("")),
            "an empty constant is a value, not a template"
        );
        let err = Constant::Str("{nope}".into())
            .render("public", "users")
            .expect_err("only schema and table render");
        assert!(err.contains("nope"), "{err}");
    }

    #[test]
    fn an_id_template_is_parsed_and_grammar_checked() {
        let cfg = parse(&format!("{MINIMAL}id = \"user-{{id}}\"\n")).expect("valid");
        assert_eq!(cfg.sync["users"].id.as_deref(), Some("user-{id}"));
        for bad in ["user-{", "user-}", "user-{}", "user-{1x}", ""] {
            assert!(
                parse(&format!("{MINIMAL}id = \"{bad}\"\n")).is_err(),
                "{bad:?} must not load as an id"
            );
        }
    }

    #[test]
    fn fan_out_combinations_are_rejected_and_the_shape_accepted() {
        let ok = format!(
            "{MINIMAL}id = \"user-{{id}}\"\n[sync.users.fan_out]\nfield = \"tags\"\nid = \"user-{{id}}-{{tag}}\"\n"
        );
        assert!(parse(&ok).is_ok(), "a well-formed fan_out loads");
        let poll = r#"[source]
url = "postgres://u:p@localhost/db"
mode = "poll"
[target]
url = "http://localhost:9200"
[sync.users]
table = "public.users"
[sync.users.fan_out]
field = "tags"
id = "user-{id}-{tag}"
"#;
        assert!(parse(poll).is_err(), "fan_out needs the replication log");
        let children = format!(
            "{MINIMAL}[[sync.users.children]]\ntable = \"public.orders\"\nfield = \"orders\"\nforeign_key = \"customer_id\"\n[sync.users.fan_out]\nfield = \"tags\"\nid = \"user-{{id}}\"\n"
        );
        assert!(parse(&children).is_err(), "fan_out and children collide");
        let projected = format!(
            "{MINIMAL}exclude_columns = [\"tags\"]\n[sync.users.fan_out]\nfield = \"tags\"\nid = \"user-{{id}}\"\n"
        );
        assert!(
            parse(&projected).is_err(),
            "projection must not cut the array before fan-out"
        );
        let included = format!(
            "{MINIMAL}columns = [\"id\", \"tags\"]\n[sync.users.fan_out]\nfield = \"tags\"\nid = \"user-{{id}}\"\n"
        );
        assert!(
            parse(&included).is_err(),
            "an array named in columns is the same collision"
        );
        let bad_id = format!("{MINIMAL}[sync.users.fan_out]\nfield = \"tags\"\nid = \"user-{{\"\n");
        assert!(parse(&bad_id).is_err(), "the element id is grammar-checked");
    }

    /// A well-formed join pair; each refusal below is one edit away from it.
    const PAIR: &str = r#"
[source]
url = "postgres://u:p@localhost/db"
[target]
url = "http://localhost:9200"
[sync.customers]
table = "public.customers"
index = "shop"
id = "customer-{id}"
[sync.customers.join]
field = "relation"
name = "customer"
[sync.orders]
table = "public.orders"
index = "shop"
id = "order-{id}"
[sync.orders.join]
field = "relation"
name = "order"
parent = "customer_id"
"#;

    #[test]
    fn an_append_only_section_loads_and_everything_needing_a_key_is_refused() {
        let base = MINIMAL.replace("table = \"public.users\"", "table = \"public.events\"");
        let ok = format!("{base}append_only = true\n");
        let cfg = parse(&ok).expect("a keyless table is a valid section");
        assert!(cfg.sync["users"].append_only);
        parse(&format!(
            "{ok}id = \"{{event_id}}\"\nindex = \"events-{{kind}}\"\nwhere = \"kind <> 'noise'\"\n\
             [sync.users.fields]\nat = \"ts\"\n"
        ))
        .expect("an id, an index template, a filter and a rename address no row by key");

        let message = refused(
            &format!("{ok}primary_key = \"kind\"\n"),
            "a key on a keyless table",
        );
        assert_eq!(
            message,
            "[sync.users] append_only and primary_key contradict each other"
        );
        let message = refused(
            &format!("{ok}[sync.users.fan_out]\nfield = \"tags\"\nid = \"{{tag}}\"\n"),
            "fan_out on a keyless table",
        );
        assert!(message.contains("fan_out needs a key"), "{message}");
        let message = refused(
            &format!("{ok}[sync.users.join]\nfield = \"rel\"\nname = \"event\"\n"),
            "join on a keyless table",
        );
        assert!(message.contains("join needs a key"), "{message}");
        let message = refused(
            &format!(
                "{ok}[[sync.users.children]]\ntable = \"public.notes\"\nfield = \"notes\"\n\
                 foreign_key = \"event_id\"\n"
            ),
            "children on a keyless table",
        );
        assert!(message.contains("[[children]] needs a key"), "{message}");
        let message = refused(
            &format!(
                "{}soft_delete = \"deleted_at IS NOT NULL\"\n",
                ok.replace(
                    "url = \"postgres://u:p@localhost/db\"",
                    "url = \"postgres://u:p@localhost/db\"\nmode = \"poll\""
                )
            ),
            "soft_delete on a keyless table",
        );
        assert!(
            message.contains("soft_delete needs a key to delete by"),
            "{message}"
        );
    }

    #[test]
    fn a_join_pair_validates_and_so_does_a_parent_on_its_own() {
        let cfg = parse(PAIR).expect("a well-formed pair loads");
        assert_eq!(
            cfg.sync["orders"]
                .join
                .as_ref()
                .and_then(|j| j.parent.as_deref()),
            Some("customer_id")
        );
        assert!(
            cfg.sync["customers"]
                .join
                .as_ref()
                .is_some_and(|j| j.parent.is_none())
        );
        let parent_only = PAIR.split("[sync.orders]").next().expect("the parent half");
        parse(parent_only).expect("a parent whose children are configured later is not an error");
        parse(&PAIR.replace(
            "parent = \"customer_id\"",
            "parent = \"customer_id\"\n[[sync.orders.children]]\ntable = \"public.lines\"\nfield = \"lines\"\nforeign_key = \"order_id\"\n",
        ))
        .expect("embedding an array on a join child is unrelated to filing it under a parent");
    }

    #[test]
    fn a_pipeline_is_the_sections_choice_and_only_a_named_one_on_a_target_that_runs_them() {
        let refusal = refused(
            &MINIMAL.replace(
                "table = \"public.users\"",
                "table = \"public.users\"\npipeline = \"\"",
            ),
            "an empty pipeline name",
        );
        assert!(
            refusal.contains("[sync.users] pipeline must not be empty"),
            "{refusal}"
        );

        let refusal = refused(
            &MINIMAL
                .replace("[target]", "[target]\nflavor = \"meilisearch\"")
                .replace(
                    "table = \"public.users\"",
                    "table = \"public.users\"\npipeline = \"embed-users\"",
                ),
            "a target without ingest pipelines",
        );
        assert!(
            refusal.contains(
                "[sync.users] pipeline is an OpenSearch and Elasticsearch feature, which \
                 Meilisearch has no equivalent for; remove pipeline for this target"
            ),
            "{refusal}"
        );

        // per section, not per index: the pipeline rides on the operation, so
        // two sections writing one index may each name their own
        let cfg = parse(
            &PAIR
                .replace(
                    "id = \"customer-{id}\"",
                    "id = \"customer-{id}\"\npipeline = \"embed-customers\"",
                )
                .replace(
                    "id = \"order-{id}\"",
                    "id = \"order-{id}\"\npipeline = \"embed-orders\"",
                ),
        )
        .expect("two pipelines on one index");
        assert_eq!(
            cfg.sync["customers"].pipeline.as_deref(),
            Some("embed-customers")
        );
        assert_eq!(cfg.sync["orders"].pipeline.as_deref(), Some("embed-orders"));
    }

    #[test]
    fn a_routing_column_is_a_column_name_and_no_second_owner_of_the_shard() {
        let routed = MINIMAL.replace(
            "table = \"public.users\"",
            "table = \"public.users\"\nrouting = \"tenant\"",
        );
        let cfg = parse(&routed).expect("a routing column is a valid section");
        assert_eq!(cfg.sync["users"].routing.as_deref(), Some("tenant"));
        parse(&format!("{routed}append_only = true\n"))
            .expect("an append-only table only ever inserts, so it never needs the old routing");

        let refusal = refused(
            &MINIMAL.replace(
                "table = \"public.users\"",
                "table = \"public.users\"\nrouting = \"\"",
            ),
            "an empty routing column",
        );
        assert!(
            refusal.contains("[sync.users] routing must not be empty"),
            "{refusal}"
        );

        let refusal = refused(
            &PAIR.replace(
                "id = \"order-{id}\"",
                "id = \"order-{id}\"\nrouting = \"tenant\"",
            ),
            "a second owner of a join child's shard",
        );
        assert!(
            refusal.contains("[sync.orders] routing and join cannot be combined"),
            "{refusal}"
        );

        let refusal = refused(
            &routed.replace("[target]", "[target]\nflavor = \"meilisearch\""),
            "a target that ignores routing",
        );
        assert!(
            refusal.contains(
                "[sync.users] routing is an OpenSearch and Elasticsearch feature, which \
                 Meilisearch has no equivalent for; remove routing for this target"
            ),
            "{refusal}"
        );
    }

    #[test]
    fn a_join_pair_is_refused_when_it_is_not_one() {
        let refused = [
            (
                PAIR.replace("[sync.orders.join]\nfield = \"relation\"\nname = \"order\"\nparent = \"customer_id\"\n", ""),
                "[sync.orders] writes index \"shop\" without join, but [sync.customers] declares one",
                "a section on the shared index without join",
            ),
            (
                PAIR.replace("field = \"relation\"\nname = \"order\"", "field = \"rel\"\nname = \"order\""),
                "disagrees with [sync.customers.join] field \"relation\"",
                "two sections naming different join fields",
            ),
            (
                PAIR.replace("name = \"customer\"", "name = \"customer\"\nparent = \"account_id\""),
                "has no join parent",
                "every section naming a parent column",
            ),
            (
                PAIR.replace("\nparent = \"customer_id\"", ""),
                "are both the join parent",
                "two sections omitting parent",
            ),
            (
                PAIR.replace("name = \"order\"", "name = \"customer\""),
                "both use the join name \"customer\"",
                "two sections sharing a relation name",
            ),
            (
                PAIR.replace("id = \"customer-{id}\"", "id = \"customer-{tenant}-{id}\""),
                "names a column outside the primary key",
                "a parent id the child cannot render from one column",
            ),
            (
                PAIR.split("[sync.customers]").next().expect("the preamble").to_string()
                    + PAIR.split("[sync.orders]").nth(1).map(|s| format!("[sync.orders]{s}")).expect("the child half").as_str(),
                "no other [sync.*] section writes index \"shop\" as its parent",
                "a child alone in its index",
            ),
            (
                PAIR.replace("id = \"order-{id}\"", "id = \"order-{id}\"\nmapping_file = \"orders.json\""),
                "a join child must not set mapping_file",
                "the mapping belongs to the parent",
            ),
            (
                PAIR.replace("[sync.orders.join]", "[sync.orders.fan_out]\nfield = \"lines\"\nid = \"order-{id}-{line}\"\n[sync.orders.join]"),
                "fan_out and join cannot be combined",
                "a fanned row has no single parent",
            ),
            (
                PAIR.replace("[target]", "[target]\nflavor = \"meilisearch\""),
                "Meilisearch has no equivalent",
                "a target without a parent-child model",
            ),
            (
                PAIR.replace("field = \"relation\"\nname = \"customer\"", "field = \"\"\nname = \"customer\""),
                "[sync.customers.join] field must not be empty",
                "an empty field",
            ),
            (
                PAIR.replace("name = \"order\"", "name = \"\""),
                "[sync.orders.join] name must not be empty",
                "an empty name",
            ),
            (
                PAIR.replace("parent = \"customer_id\"", "parent = \"\""),
                "[sync.orders.join] parent must not be empty",
                "an empty parent",
            ),
            (
                PAIR.replace("id = \"order-{id}\"", "id = \"order-{id}\"\n[sync.orders.fields]\nrel = \"relation\""),
                "[sync.orders.fields] \"relation\" is the join field",
                "a rename onto the join field",
            ),
            (
                PAIR.replace("id = \"order-{id}\"", "id = \"order-{id}\"\n[sync.orders.fields]\nrelation = \"rel\""),
                "[sync.orders.fields] \"relation\" is the join field",
                "a rename of the join field",
            ),
            (
                PAIR.replace("id = \"order-{id}\"", "id = \"order-{id}\"\n[sync.orders.constants]\nrelation = \"x\""),
                "[sync.orders.constants] relation is the join field",
                "a constant named like the join field",
            ),
            (
                PAIR.replace("id = \"order-{id}\"", "id = \"order-{id}\"\ncolumns = [\"id\", \"relation\"]"),
                "columns lists relation, which is the join field",
                "a projection naming the join field",
            ),
            (
                PAIR.replace("name = \"customer\"", "name = \"customer\"\n[[sync.customers.children]]\ntable = \"public.notes\"\nfield = \"relation\"\nforeign_key = \"customer_id\""),
                "relation is the join field and also the field of child public.notes",
                "an embedded child under the join field's name",
            ),
        ];
        for (toml_text, expected, why) in refused {
            let err = parse(&toml_text).expect_err(why);
            let text = format!("{err:#}");
            assert!(text.contains(expected), "{why}: {text}");
        }
    }

    #[test]
    fn plaintext_secrets_warn_but_resolve() {
        let cfg = parse(MINIMAL).expect("valid");
        let secrets = cfg.resolve_secrets().expect("resolved");
        assert_eq!(secrets.source_url, "postgres://u:p@localhost/db");
        assert_eq!(secrets.warnings.len(), 1, "plain-text url must warn");
    }

    #[test]
    fn missing_env_var_is_a_clear_error() {
        let cfg = parse(&MINIMAL.replace(
            "url = \"postgres://u:p@localhost/db\"",
            "url_env = \"PG2OSYNC_TEST_MISSING_VAR\"",
        ))
        .expect("valid");
        let err = cfg.resolve_secrets().expect_err("must fail");
        assert!(err.to_string().contains("PG2OSYNC_TEST_MISSING_VAR"));
    }
}
