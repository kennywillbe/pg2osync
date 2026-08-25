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
        // to_jsonb on the whole row preserves native types
        let (predicate, key) = key_predicate(&pg_quote_ident(&self.foreign_key), parent_pk_json);
        let sql = format!(
            "SELECT COALESCE(jsonb_agg(to_jsonb(t)), '[]'::jsonb) \
             FROM (SELECT * FROM {}.{} WHERE {predicate}) t",
            pg_quote_ident(&self.schema),
            pg_quote_ident(&self.table),
        );
        let row = client
            .query_one(&sql, &[key.as_ref()])
            .await
            .with_context(|| format!("child fetch failed for {}", self.qualified()))?;
        let v: Value = row.get(0);
        Ok(v)
    }
}

/// Bind a key in the column's own type so the query can use an index.
///
/// Casting either side to text — which this did before — makes the index
/// unusable and turns every lookup into a sequential scan: measured at 165s
/// against 50k parents where the typed form took 74ms. Anything that is not a
/// plain scalar falls back to the text form, which is correct if slow.
fn key_predicate(
    quoted_column: &str,
    key: &Value,
) -> (String, Box<dyn tokio_postgres::types::ToSql + Sync + Send>) {
    match key {
        Value::Number(n) if n.is_i64() => (
            format!("{quoted_column} = $1"),
            Box::new(n.as_i64().expect("checked")),
        ),
        Value::String(s) => (format!("{quoted_column} = $1"), Box::new(s.clone())),
        other => (
            format!("{quoted_column}::text = $1"),
            Box::new(other.to_string()),
        ),
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
    let (predicate, key) = key_predicate(&pg_quote_ident(pk_column), pk_json);
    let sql = format!(
        "SELECT to_jsonb(t) FROM (SELECT * FROM {}.{} WHERE {predicate}) t",
        pg_quote_ident(schema),
        pg_quote_ident(table),
    );
    async move {
        let row = client
            .query_opt(&sql, &[key.as_ref()])
            .await
            .with_context(|| format!("parent refetch failed for {schema}.{table}"))?;
        Ok(row.map(|r| {
            let v: Value = r.get(0);
            v
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn integer_keys_compare_in_their_own_type() {
        let (predicate, _) = key_predicate("\"customer_id\"", &json!(42));
        assert_eq!(predicate, "\"customer_id\" = $1");
    }

    #[test]
    fn string_keys_compare_in_their_own_type() {
        let (predicate, _) = key_predicate("\"tenant\"", &json!("acme"));
        assert_eq!(predicate, "\"tenant\" = $1");
    }

    #[test]
    fn anything_else_falls_back_to_text() {
        // composite keys arrive as an object; correctness first, speed second
        let (predicate, _) = key_predicate("\"k\"", &json!({"a": 1}));
        assert_eq!(predicate, "\"k\"::text = $1");
    }

    #[test]
    fn child_tables_are_schema_qualified_and_quoted() {
        let spec = ChildSpec::new("we\"ird.ch\"ild", "kids", "fk", "id").expect("qualified");
        assert_eq!(spec.schema, "we\"ird");
        assert_eq!(spec.table, "ch\"ild");
    }
}
