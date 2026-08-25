//! Config loading and validation.
//!
//! Secrets are env-first: `*_env` keys are resolved at load time;
//! plain-text secrets in the file are accepted but warn deprecated.

use anyhow::{Context, Result};
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
    /// Amazon OpenSearch Serverless profile: skips operations
    /// Serverless rejects (refresh, settings changes) and expects IAM-signed
    /// access via an authorization token env (proxy or SigV4 gateway).
    #[serde(default)]
    pub serverless: bool,
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
    /// Column transformations, e.g. email = "hash" | "redact"
    #[serde(default)]
    pub transform: std::collections::HashMap<String, String>,
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
            for (col, op) in &tbl.transform {
                if op != "hash" && op != "redact" {
                    anyhow::bail!(
                        "[sync.{key}.transform] {col} = {op:?} must be \"hash\" or \"redact\""
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
