//! The `Pg2osync` custom resource.
//!
//! The spec is the Helm chart's `configs:` map, one level down: a chart user
//! moves a values file into a `Pg2osync` by copying the tree, because the
//! rendered TOML has to come out the same either way. Anything the chart
//! expresses that this does not is listed in `docs/decisions.md`.

use std::collections::BTreeMap;

use kube::CustomResource;
use schemars::{JsonSchema, Schema, SchemaGenerator, json_schema};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// One TOML table: keys to scalar or array values, exactly as the file takes
/// them.
pub type Table = BTreeMap<String, Value>;

/// A TOML table the API server stores without knowing its keys.
///
/// The config tree is pg2osync's schema, not Kubernetes'. Teaching the CRD
/// every option would make the operator the second place an option has to be
/// added, and the first place a new one is silently dropped — so the sections
/// are opaque objects and the pipeline validates them, the way it validates a
/// file written by hand.
fn opaque_table(_: &mut SchemaGenerator) -> Schema {
    json_schema!({
        "type": "object",
        "x-kubernetes-preserve-unknown-fields": true,
    })
}

/// One config file's tree, in the shape of one entry of the chart's `configs`.
#[derive(Serialize, Deserialize, Clone, Debug, Default, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ConfigTree {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(schema_with = "opaque_table")]
    pub source: Option<Table>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(schema_with = "opaque_table")]
    pub target: Option<Table>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(schema_with = "opaque_table")]
    pub engine: Option<Table>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(schema_with = "opaque_table")]
    pub metrics: Option<Table>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(schema_with = "opaque_table")]
    pub api: Option<Table>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(schema_with = "opaque_table")]
    pub log: Option<Table>,
    /// One entry per table, keyed the way `[sync.<key>]` is keyed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(schema_with = "opaque_table")]
    pub sync: Option<BTreeMap<String, Table>>,
    /// Raw TOML appended to this file, for constructs the tree cannot express
    /// — repeated `[[sync.x.children]]` blocks above all.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub extra_config: Option<String>,
}

/// What a change to the rendered config does to the running pod.
#[derive(Serialize, Deserialize, Clone, Copy, Debug, Default, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ReloadMode {
    /// The pod carries a checksum of the config, so an edit replaces it.
    #[default]
    Restart,
    /// The pod stays and a sidecar sends SIGHUP when the mounted file changes.
    Signal,
}

#[derive(CustomResource, Serialize, Deserialize, Clone, Debug, Default, JsonSchema)]
#[kube(
    group = "pg2osync.io",
    version = "v1alpha1",
    kind = "Pg2osync",
    plural = "pg2osyncs",
    singular = "pg2osync",
    shortname = "p2o",
    namespaced,
    status = "Pg2osyncStatus",
    printcolumn = r#"{"name":"Ready","type":"string","jsonPath":".status.ready"}"#,
    printcolumn = r#"{"name":"Sources","type":"integer","jsonPath":".status.sources"}"#,
    printcolumn = r#"{"name":"Age","type":"date","jsonPath":".metadata.creationTimestamp"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct Pg2osyncSpec {
    /// Overrides the image the operator was started with, for a tenant pinned
    /// to another version.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub image: Option<String>,
    /// One source database, rendered as `pg2osync.toml`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub config: Option<ConfigTree>,
    /// Several source databases in one process, each rendered as
    /// `<name>.toml`. Alternative to `config`, never both.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub configs: Option<BTreeMap<String, ConfigTree>>,
    /// Secrets in this namespace whose keys become environment variables. This
    /// is the only way credentials reach the pipeline: a password written into
    /// the spec is refused.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub secret_refs: Vec<String>,
    /// Plain environment variables — never credentials.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub env: BTreeMap<String, String>,
    /// Ask Prometheus to scrape this pipeline. Ignored, with a log line, in a
    /// cluster without prometheus-operator.
    #[serde(default)]
    pub service_monitor: bool,
    #[serde(default)]
    pub reload_on_change: ReloadMode,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Pg2osyncStatus {
    /// The `metadata.generation` this status describes.
    pub observed_generation: Option<i64>,
    /// The Deployment has its replica ready. This is what the operator can
    /// see; per-source health is `/healthz/<name>` on the pod.
    pub ready: bool,
    /// How many sources the rendered config carries.
    pub sources: i32,
    /// Why the pipeline is not ready, or why the spec was refused.
    pub message: Option<String>,
}

/// A spec the operator refuses to render, with the reason a human needs.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
#[error("{0}")]
pub struct Invalid(pub String);

/// Keys whose value is a credential. A config file may name the environment
/// variable that holds one; it may never hold one itself, because a spec is
/// readable by everyone with `get` on the resource and ends up in git.
const CREDENTIAL_KEYS: [&str; 5] = ["password", "token", "api_key", "master_key", "secret"];

fn valid_file_stem(name: &str) -> bool {
    !name.is_empty()
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
}

fn check_credentials(file: &str, section: &str, table: &Table) -> Result<(), Invalid> {
    for key in table.keys() {
        if CREDENTIAL_KEYS.contains(&key.as_str()) {
            return Err(Invalid(format!(
                "{file}: [{section}] {key} is a credential. Put it in a Secret listed under \
                 spec.secretRefs and name the variable with {key}_env"
            )));
        }
    }
    Ok(())
}

impl ConfigTree {
    /// The source's own name, which labels its metrics and answers
    /// `/healthz/<name>`, or the key it is filed under.
    fn source_name<'a>(&'a self, key: &'a str) -> &'a str {
        self.source
            .as_ref()
            .and_then(|s| s.get("name"))
            .and_then(Value::as_str)
            .unwrap_or(key)
    }

    fn validate(&self, file: &str) -> Result<(), Invalid> {
        let sections: [(&str, &Option<Table>); 6] = [
            ("source", &self.source),
            ("target", &self.target),
            ("engine", &self.engine),
            ("metrics", &self.metrics),
            ("api", &self.api),
            ("log", &self.log),
        ];
        for (name, table) in sections {
            if let Some(table) = table {
                check_credentials(file, name, table)?;
            }
        }
        if let Some(source) = &self.source
            && source.contains_key("url")
        {
            return Err(Invalid(format!(
                "{file}: [source] url carries the password. Put the connection string in a Secret \
                 listed under spec.secretRefs and name it with url_env"
            )));
        }
        // extraConfig is raw TOML the operator never parses, so this is a
        // textual check: it catches the obvious mistake without pretending the
        // operator understands the block.
        if let Some(extra) = &self.extra_config {
            for line in extra.lines() {
                let Some((key, _)) = line.split_once('=') else {
                    continue;
                };
                let key = key.trim();
                if CREDENTIAL_KEYS.contains(&key) {
                    return Err(Invalid(format!(
                        "{file}: extraConfig sets {key} inline. Name the environment variable \
                         with {key}_env instead"
                    )));
                }
            }
        }
        Ok(())
    }
}

impl Pg2osyncSpec {
    /// The files this spec renders to, in the order `--config-dir` will read
    /// them, or the reason it renders to none.
    pub fn files(&self) -> Result<Vec<(String, &ConfigTree)>, Invalid> {
        let files = match (&self.config, &self.configs) {
            (Some(_), Some(_)) => {
                return Err(Invalid(
                    "config and configs are both set: a process reads one file or one directory, \
                     not both. Move the tree under config into an entry of configs"
                        .into(),
                ));
            }
            (None, None) => {
                return Err(Invalid("neither config nor configs is set".into()));
            }
            (Some(tree), None) => vec![("pg2osync".to_string(), tree)],
            (None, Some(trees)) => {
                if trees.is_empty() {
                    return Err(Invalid("configs is empty".into()));
                }
                trees.iter().map(|(k, v)| (k.clone(), v)).collect()
            }
        };

        let mut seen: BTreeMap<&str, &str> = BTreeMap::new();
        for (key, tree) in &files {
            if !valid_file_stem(key) {
                return Err(Invalid(format!(
                    "{key} is not a usable name: it becomes {key}.toml in a ConfigMap, so it may \
                     only hold letters, digits, - and _"
                )));
            }
            tree.validate(key)?;
            let name = tree.source_name(key);
            if let Some(other) = seen.insert(name, key) {
                return Err(Invalid(format!(
                    "{other} and {key} are both the source named {name}: the name labels every \
                     metric and answers /healthz/<name>, so it has to be unique"
                )));
            }
        }
        Ok(files)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tree(toml_ish: serde_json::Value) -> ConfigTree {
        serde_json::from_value(toml_ish).expect("a valid tree")
    }

    fn spec(configs: serde_json::Value) -> Pg2osyncSpec {
        Pg2osyncSpec {
            configs: Some(serde_json::from_value(configs).expect("valid configs")),
            ..Default::default()
        }
    }

    #[test]
    fn an_inline_source_url_is_refused() {
        let spec = spec(serde_json::json!({
            "orders": { "source": { "url": "postgres://u:p@db/appdb" } }
        }));
        let err = spec.files().expect_err("a url is a credential");
        assert!(err.0.contains("url_env"), "{err}");
    }

    #[test]
    fn an_inline_target_password_is_refused() {
        let spec = spec(serde_json::json!({
            "orders": { "target": { "url": "http://os:9200", "password": "hunter2" } }
        }));
        let err = spec.files().expect_err("a password is a credential");
        assert!(err.0.contains("password_env"), "{err}");
    }

    #[test]
    fn a_credential_in_extra_config_is_refused() {
        let spec = spec(serde_json::json!({
            "orders": { "extraConfig": "[target]\napi_key = \"live\"\n" }
        }));
        let err = spec.files().expect_err("extraConfig is scanned too");
        assert!(err.0.contains("api_key_env"), "{err}");
    }

    #[test]
    fn two_files_naming_one_source_are_refused() {
        let spec = spec(serde_json::json!({
            "a": { "source": { "name": "orders", "url_env": "A" } },
            "b": { "source": { "name": "orders", "url_env": "B" } }
        }));
        let err = spec.files().expect_err("the name has to be unique");
        assert!(err.0.contains("/healthz/<name>"), "{err}");
    }

    #[test]
    fn both_config_styles_at_once_are_refused() {
        let spec = Pg2osyncSpec {
            config: Some(ConfigTree::default()),
            configs: Some(BTreeMap::from([("orders".into(), ConfigTree::default())])),
            ..Default::default()
        };
        let err = spec.files().expect_err("one file or one directory");
        assert!(err.0.contains("not both"), "{err}");
    }

    #[test]
    fn a_name_that_cannot_be_a_config_map_key_is_refused() {
        let spec = spec(serde_json::json!({ "orders/eu": { "source": { "url_env": "A" } } }));
        let err = spec.files().expect_err("the key becomes a file name");
        assert!(err.0.contains("orders/eu.toml"), "{err}");
    }

    #[test]
    fn a_single_config_renders_as_the_file_the_chart_renders() {
        let spec = Pg2osyncSpec {
            config: Some(tree(serde_json::json!({ "source": { "url_env": "A" } }))),
            ..Default::default()
        };
        let files = spec.files().expect("a valid spec");
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].0, "pg2osync");
    }
}
