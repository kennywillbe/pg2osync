//! Nested child-collection support.
//!
//! Children are resolved at the SOURCE side via SQL so the engine stays
//! source-agnostic: parent documents simply arrive with extra array fields.
//! Child values are typed natively by PG (`to_jsonb`), no client mapping.

use anyhow::{Context as _, Result};
use serde_json::Value;
use tokio_postgres::Client;

/// One configured `[sync.x.children]` entry, fully qualified.
#[derive(Debug, Clone)]
pub struct ChildSpec {
    pub schema: String,
    pub table: String,
    pub field: String,
    pub foreign_key: String,
    /// Parent column the FK references (parent PK).
    pub parent_column: String,
}

impl ChildSpec {
    pub fn new(
        qualified_child: &str,
        field: &str,
        foreign_key: &str,
        parent_column: &str,
    ) -> Result<Self> {
        let (schema, table) = qualified_child.split_once('.').context(format!(
            "child table {qualified_child:?} must be schema-qualified"
        ))?;
        Ok(Self {
            schema: schema.into(),
            table: table.into(),
            field: field.into(),
            foreign_key: foreign_key.into(),
            parent_column: parent_column.into(),
        })
    }

    pub fn qualified(&self) -> String {
        format!("{}.{}", self.schema, self.table)
    }

    /// Native-typed JSON array of children for one parent key value.
    /// `pk_value` is already a JSON scalar/object rendered by pk_to_id rules.
    pub async fn fetch(&self, client: &Client, parent_pk_json: &Value) -> Result<Value> {
        // to_jsonb on the whole row preserves native types; the fk filter uses
        // the JSON-rendered parent key which matches PK rendering for scalars
        let sql = format!(
            "SELECT COALESCE(jsonb_agg(to_jsonb(t)), '[]'::jsonb) \
             FROM (SELECT * FROM {}.{} WHERE {}::text = $1::text) t",
            self.schema,
            self.table,
            pg_quote_ident(&self.foreign_key),
        );
        // $1 is text form of the parent key; for bigint PKs text compares fine
        let val = match parent_pk_json {
            Value::String(s) => s.clone(),
            other => other.to_string(),
        };
        let row = client
            .query_one(&sql, &[&val])
            .await
            .with_context(|| format!("child fetch failed for {}", self.qualified()))?;
        let v: Value = row.get(0);
        Ok(v)
    }
}

fn pg_quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// Attach every configured child collection to a parent document.
pub async fn attach_children(
    doc: &mut Value,
    parent_pk: &Value,
    children: &[ChildSpec],
    client: &Client,
) -> Result<()> {
    for spec in children {
        let arr = spec.fetch(client, parent_pk).await?;
        if let Value::Object(map) = doc {
            map.insert(spec.field.clone(), arr);
        }
    }
    Ok(())
}

/// Refetch one parent row as a native-typed JSON document.
///
/// `pk_json` is the child's FK value (JSON-rendered). Returns None when the
/// parent no longer exists.
pub fn refetch_parent<'a>(
    client: &'a Client,
    schema: &'a str,
    table: &'a str,
    pk_json: &'a Value,
    pk_column: &'a str,
) -> impl std::future::Future<Output = Result<Option<Value>>> + 'a {
    let val = match pk_json {
        Value::String(s) => s.clone(),
        other => other.to_string(),
    };
    let sql = format!(
        "SELECT to_jsonb(t) FROM (SELECT * FROM {}.{} WHERE {}::text = $1::text) t",
        schema,
        table,
        pg_quote_ident(pk_column),
    );
    async move {
        let row = client
            .query_opt(&sql, &[&val])
            .await
            .with_context(|| format!("parent refetch failed for {schema}.{table}"))?;
        Ok(row.map(|r| {
            let v: Value = r.get(0);
            v
        }))
    }
}
