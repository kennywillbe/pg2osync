//! Config loading and validation.
//!
//! Secrets are env-first (ADR #8): `*_env` keys are resolved at load time;
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
    // consumed by the engine wiring in M4
    #[allow(dead_code)]
    #[serde(default)]
    pub engine: pg2osync_engine::EngineConfig,
    #[serde(default)]
    pub metrics: MetricsConfig,
}

// slot_name/publication are consumed by M2 slot/publication management.
#[allow(dead_code)]
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
    /// poll mode: timestamp column name per table
    #[serde(default = "default_poll_column")]
    pub poll_column: String,
    #[serde(default = "default_poll_interval")]
    pub poll_interval_secs: u64,
    pub url: Option<String>,
    pub url_env: Option<String>,
    /// Nested-child queries use a dedicated connection; defaults to url.
    #[serde(default)]
    pub admin_url_env: Option<String>,
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

fn default_flavor() -> String {
    "postgres".into()
}

fn default_server_id() -> u32 {
    424242
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
    /// Amazon OpenSearch Serverless profile (VISION §2.2): skips operations
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

// fields consumed by the metrics server task
#[allow(dead_code)]
#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct MetricsConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "default_bind")]
    pub bind: String,
}

fn default_bind() -> String {
    "127.0.0.1:9100".into()
}

// derive(Default) would set enabled=false, silently disabling metrics
impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            bind: default_bind(),
        }
    }
}

// primary_key/replica_identity_full/columns feed doc building in M2/M4.
#[allow(dead_code)]
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
    #[serde(default)]
    pub replica_identity_full: bool,
    /// Column transformations, e.g. email = "hash" | "redact"
    #[serde(default)]
    pub transform: std::collections::HashMap<String, String>,
    /// One-to-many children embedded as JSON arrays (single level, 0.3).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub children: Vec<ChildJoin>,
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

// transform map is consumed by the engine via PipelineCtx

impl TableSync {
    pub fn index_name(&self, key: &str) -> String {
        self.index.clone().unwrap_or_else(|| key.to_string())
    }
}

pub struct ResolvedSecrets {
    pub source_url: String,
    pub target_password: Option<String>,
    pub warnings: Vec<String>,
}

impl AppConfig {
    pub fn load(path: &Path) -> Result<Self> {
        let raw = std::fs::read_to_string(path)
            .with_context(|| format!("cannot read config file {}", path.display()))?;
        let cfg: AppConfig =
            toml::from_str(&raw).with_context(|| format!("invalid TOML in {}", path.display()))?;
        cfg.validate()?;
        Ok(cfg)
    }

    /// Structural validation. Connection checks happen in `validate` command;
    /// this function never touches the network.
    pub fn validate(&self) -> Result<()> {
        use std::collections::HashSet;

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
            // for system indices (verified in M0 Spike C).
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
        }
        Ok(())
    }

    /// Resolve env-var indirections; returns deprecation warnings for any
    /// plain-text secret found in the file (ADR #8).
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

        Ok(ResolvedSecrets {
            source_url,
            target_password,
            warnings,
        })
    }
}

fn is_qualified_table(name: &str) -> bool {
    let parts: Vec<&str> = name.split('.').collect();
    parts.len() == 2 && parts.iter().all(|p| !p.is_empty())
}
