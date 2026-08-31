//! Spec to config files.
//!
//! The sections come out in the order the chart's `pg2osync.configTree`
//! template emits them, and the values as JSON, which TOML reads back the same
//! for every scalar and array a config file holds. A tree that renders one way
//! through Helm and another way through the operator would make the migration
//! from a chart release a rewrite instead of a copy.

use std::collections::BTreeMap;

use serde_json::Value;

use crate::crd::{ConfigTree, Invalid, Pg2osyncSpec, Table};

/// The order sections appear in the rendered file.
const SECTIONS: [&str; 6] = ["source", "target", "engine", "metrics", "api", "log"];

fn scalar(value: &Value) -> String {
    // Every TOML scalar and array of scalars has the same JSON form, and a
    // config file holds nothing else — a nested table reaches the file through
    // its own section header, or through extraConfig.
    value.to_string()
}

fn push_table(out: &mut String, header: &str, table: &Table, skip: &str) {
    out.push_str(&format!("[{header}]\n"));
    for (key, value) in table {
        if key == skip {
            continue;
        }
        out.push_str(&format!("{key} = {}\n", scalar(value)));
    }
}

/// One config file's bytes.
pub fn render_tree(tree: &ConfigTree) -> String {
    let mut out = String::new();
    let sections: [&Option<Table>; 6] = [
        &tree.source,
        &tree.target,
        &tree.engine,
        &tree.metrics,
        &tree.api,
        &tree.log,
    ];
    for (name, table) in SECTIONS.iter().zip(sections) {
        if let Some(table) = table {
            push_table(&mut out, name, table, "");
            out.push('\n');
        }
    }
    for (key, table) in tree.sync.iter().flatten() {
        push_table(&mut out, &format!("sync.{key}"), table, "transform");
        if let Some(Value::Object(transform)) = table.get("transform") {
            out.push_str(&format!("\n[sync.{key}.transform]\n"));
            for (column, op) in transform {
                out.push_str(&format!("{column} = {}\n", scalar(op)));
            }
        }
        out.push('\n');
    }
    if let Some(extra) = &tree.extra_config {
        out.push_str(extra);
        out.push('\n');
    }
    format!("{}\n", out.trim())
}

/// Every file the spec renders to, keyed by the ConfigMap key that carries it
/// — which is also the file name `run --config-dir` reads.
pub fn render_files(spec: &Pg2osyncSpec) -> Result<BTreeMap<String, String>, Invalid> {
    Ok(spec
        .files()?
        .into_iter()
        .map(|(name, tree)| (format!("{name}.toml"), render_tree(tree)))
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec_from(json: serde_json::Value) -> Pg2osyncSpec {
        serde_json::from_value(json).expect("a valid spec")
    }

    fn two_sources() -> Pg2osyncSpec {
        spec_from(serde_json::json!({
            "configs": {
                "billing": {
                    "source": { "url_env": "PG2OSYNC_BILLING_URL" },
                    "target": { "url": "http://opensearch:9200" },
                    "sync": { "invoices": { "table": "public.invoices", "index": "invoices" } }
                },
                "orders": {
                    "source": {
                        "url_env": "PG2OSYNC_ORDERS_URL",
                        "slot_name": "pg2osync_orders",
                        "publication": "pg2osync_pub"
                    },
                    "target": { "url": "http://opensearch:9200" },
                    "metrics": { "bind": "0.0.0.0:9100" },
                    "sync": {
                        "orders": {
                            "table": "public.orders",
                            "index": "orders",
                            "exclude_columns": ["secret_note"],
                            "transform": { "email": "redact" }
                        }
                    },
                    "extraConfig": "[[sync.orders.children]]\ntable = \"public.order_lines\"\n"
                }
            }
        }))
    }

    #[test]
    fn a_two_source_spec_renders_the_checked_in_fixture() {
        let files = render_files(&two_sources()).expect("a valid spec");
        assert_eq!(
            files.keys().cloned().collect::<Vec<_>>(),
            vec!["billing.toml".to_string(), "orders.toml".to_string()]
        );
        assert_eq!(
            files["orders.toml"],
            include_str!("../tests/fixtures/orders.toml"),
        );
        assert_eq!(
            files["billing.toml"],
            include_str!("../tests/fixtures/billing.toml"),
        );
    }

    #[test]
    fn every_rendered_file_is_toml_the_parser_takes() {
        for (name, body) in render_files(&two_sources()).expect("a valid spec") {
            toml::from_str::<toml::Value>(&body).unwrap_or_else(|e| panic!("{name}: {e}"));
        }
    }

    #[test]
    fn an_unknown_option_reaches_the_file_untouched() {
        // The operator is not a second place an option has to be added: a key
        // it has never heard of renders the way the config file spells it.
        let spec = spec_from(serde_json::json!({
            "config": { "engine": { "a_future_option": 7 } }
        }));
        let files = render_files(&spec).expect("a valid spec");
        assert_eq!(files["pg2osync.toml"], "[engine]\na_future_option = 7\n");
    }
}
