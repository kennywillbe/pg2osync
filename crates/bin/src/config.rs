//! Config loading and validation.
//!
//! Secrets are env-first: `*_env` keys are resolved at load time;
//! plain-text secrets in the file are accepted but warn deprecated.

use anyhow::{Context, Result};
use pg2osync_engine::mapping::TransformOp;
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
    /// Derived document id: literals plus `{column}` placeholders, e.g.
    /// `tenant-{tenant_id}-{id}`. Unset keeps the primary key as the id.
    #[serde(default)]
    pub id: Option<String>,
    /// One row to many documents: fan an array column out into one document
    /// per element.
    #[serde(default)]
    pub fan_out: Option<FanOut>,
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
        use pg2osync_engine::mapping::IdTemplate;
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

/// A child table joined into the parent document.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChildJoin {
    /// Schema-qualified child table, e.g. "public.orders".
    pub table: String,
    /// Field name on the parent document holding the nested array.
    pub field: String,
    /// FK column on the CHILD table referencing the parent PK.
    pub foreign_key: String,
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
}

impl ChildJoin {
    /// Every name this child writes on the parent document: the array, and
    /// the two fields a capped array reports itself with.
    fn claimed_fields(&self) -> [String; 3] {
        [
            self.field.clone(),
            format!("{}_truncated", self.field),
            format!("{}_total", self.field),
        ]
    }
}

impl TableSync {
    pub fn index_name(&self, key: &str) -> String {
        self.index.clone().unwrap_or_else(|| key.to_string())
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
        use std::collections::HashSet;

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
        if self.source.flavor == "mysql" && self.source.mode == "poll" {
            anyhow::bail!(
                "[source] mode = \"poll\" is PostgreSQL-only; MySQL always reads the binlog"
            );
        }
        let mut seen_indexes: HashSet<String> = HashSet::new();
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
            if !index.chars().next().is_some_and(|c| c.is_ascii_lowercase()) {
                anyhow::bail!("[sync.{key}] index {index:?} must start with a lowercase letter");
            }
            if !index
                .chars()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_' || c == '-')
            {
                anyhow::bail!("[sync.{key}] index {index:?} may only contain lowercase [a-z0-9_-]");
            }
            // OpenSearch rejects names starting with '_'; dot-prefix is reserved
            // for system indices.
            if index.starts_with('_') || index.starts_with('.') {
                anyhow::bail!("[sync.{key}] index {index:?} must not start with '_' or '.'");
            }
            if !seen_indexes.insert(index.clone()) {
                anyhow::bail!(
                    "[sync.{key}] two tables map to the same index {index:?}; document identity would be ambiguous"
                );
            }
            if tbl.columns.is_some() && !tbl.exclude_columns.is_empty() {
                anyhow::bail!("[sync.{key}] columns and exclude_columns are mutually exclusive");
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
        pg2osync_source::tls::TlsSettings::resolve(
            source_url,
            self.source.sslmode.as_deref(),
            self.source.sslrootcert.as_deref(),
        )
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
    fn index_names_and_duplicates_are_validated() {
        assert!(parse(&MINIMAL.replace("table = \"public.users\"", "table = \"users\"")).is_err());
        assert!(parse(&format!("{MINIMAL}index = \"_bad\"\n")).is_err());
        assert!(parse(&format!("{MINIMAL}index = \"Users\"\n")).is_err());
        let duplicate = format!(
            "{MINIMAL}index = \"same\"\n[sync.other]\ntable = \"public.other\"\nindex = \"same\"\n"
        );
        assert!(parse(&duplicate).is_err(), "two tables in one index");
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
