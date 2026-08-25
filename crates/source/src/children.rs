//! Nested child-collection support.
//!
//! Children are resolved at the SOURCE side via SQL so the engine stays
//! source-agnostic: parent documents simply arrive with extra array fields.
//! Child values are typed natively by PG (`to_jsonb`), no client mapping.

use anyhow::{Context as _, Result};
use serde_json::Value;
use std::collections::HashMap;
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
    /// The child's own primary key, resolved from the catalogue at startup.
    ///
    /// Without an order the array's contents are a set in arbitrary order, so
    /// the initial load and a streamed re-fetch can embed the same children
    /// differently and a re-snapshot can change a document for no reason. With
    /// `max_rows` it matters more than cosmetically: two runs would otherwise
    /// keep *different* children.
    pub order_by: Vec<String>,
    /// How many children to embed, or all of them.
    pub max_rows: Option<u32>,
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
            order_by: Vec::new(),
            max_rows: None,
        })
    }

    /// The field naming how many children the source actually has, present only
    /// on a document whose array was cut short.
    pub fn total_field(&self) -> String {
        format!("{}_total", self.field)
    }

    /// The field saying the array is not the whole collection.
    pub fn truncated_field(&self) -> String {
        format!("{}_truncated", self.field)
    }

    /// Fill in what the catalogue knows: the child's key, to order by.
    pub async fn resolve_order(&mut self, client: &Client) -> Result<()> {
        let info = crate::catalog::table_info(client, &self.schema, &self.table).await?;
        if info.pk_columns.is_empty() && self.max_rows.is_some() {
            anyhow::bail!(
                "[[sync.*.children]] max_rows is set on {}, which has no primary key to \
                 order by — the same rows would not be kept twice running. Add a primary \
                 key or drop max_rows",
                self.qualified()
            );
        }
        if info.pk_columns.is_empty() {
            tracing::warn!(target: "pg2osync::source",
                "{} has no primary key, so the order of its embedded array is whatever the \
                 read returns and may differ between the initial load and a later re-fetch",
                self.qualified());
        }
        self.order_by = info.pk_columns;
        Ok(())
    }

    /// The subquery that turns one child table into one aggregated array per
    /// parent key, with the total before any cap.
    ///
    /// Shared by the initial load's `COPY` and the streaming re-fetch so the two
    /// cannot disagree about what a child array contains — a disagreement is
    /// invisible until someone re-snapshots.
    pub fn agg_subquery(&self, filter: Option<&str>) -> String {
        let fk = pg_quote_ident(&self.foreign_key);
        let ordered = if self.order_by.is_empty() {
            String::new()
        } else {
            let cols: Vec<String> = self.order_by.iter().map(|c| pg_quote_ident(c)).collect();
            format!(" ORDER BY {}", cols.join(", "))
        };
        // The total is counted before the cap applies, so a truncated document
        // can say how many there really are rather than only that it was cut.
        // The filter belongs to the innermost read: applied outside the
        // aggregate it would first aggregate the whole child table and only then
        // discard it, which is a sequential scan per batch.
        let where_clause = match filter {
            Some(predicate) => format!(" WHERE {predicate}"),
            None => String::new(),
        };
        let ranked = format!(
            "SELECT {fk} AS k, to_jsonb(t) AS doc, \
             row_number() OVER (PARTITION BY {fk}{ordered}) AS rn, \
             count(*) OVER (PARTITION BY {fk}) AS total \
             FROM {}.{} t{where_clause}",
            pg_quote_ident(&self.schema),
            pg_quote_ident(&self.table),
        );
        let capped = match self.max_rows {
            Some(n) => format!("SELECT * FROM ({ranked}) r WHERE r.rn <= {n}"),
            None => format!("SELECT * FROM ({ranked}) r"),
        };
        format!(
            "SELECT k, COALESCE(jsonb_agg(doc ORDER BY rn), '[]'::jsonb) AS agg, \
             max(total) AS total FROM ({capped}) c GROUP BY k"
        )
    }

    pub fn qualified(&self) -> String {
        format!("{}.{}", self.schema, self.table)
    }

    /// Every listed parent's child array, in one query.
    ///
    /// One query per collection per batch rather than per changed row: a
    /// transaction touching a thousand children of one parent asks once. Returns
    /// the array and the total the source holds, keyed by the parent key so the
    /// caller can match without another comparison.
    pub async fn fetch_many(
        &self,
        client: &Client,
        keys: &[Value],
    ) -> Result<HashMap<String, (Value, i64)>> {
        if keys.is_empty() {
            return Ok(HashMap::new());
        }
        let (predicate, param) = any_predicate(&pg_quote_ident(&self.foreign_key), keys);
        // to_jsonb of the key rather than its text: the caller matches on the
        // JSON form, so a number and its rendering cannot drift apart
        let sql = format!(
            "SELECT to_jsonb(c.k)::text, c.agg, c.total FROM ({}) c",
            self.agg_subquery(Some(&predicate)),
        );
        let rows = client
            .query(&sql, &[param.as_ref()])
            .await
            .with_context(|| format!("child fetch failed for {}", self.qualified()))?;
        let mut out = HashMap::new();
        for row in rows {
            let key: String = row.get(0);
            let agg: Value = row.get(1);
            let total: i64 = row.get(2);
            out.insert(key, (agg, total));
        }
        Ok(out)
    }
}

/// Build the lookup predicate and its parameter.
///
/// Casting the column to text makes an index unusable and turns every lookup
/// into a sequential scan: measured at 165s against 50k parents where the typed
/// comparison took 74ms. Integer keys therefore compare numerically; everything
/// else keeps the text form, which is correct for any column type at that cost.
/// The batch form of [`key_predicate`]: one predicate matching many keys.
///
/// Same reasoning about types — an integer array compares against any integer
/// width and keeps the index usable, and everything else falls back to text,
/// which is correct for any column at the cost of the index.
fn any_predicate(
    quoted_column: &str,
    keys: &[Value],
) -> (String, Box<dyn tokio_postgres::types::ToSql + Sync + Send>) {
    let all_ints: Option<Vec<i64>> = keys
        .iter()
        .map(|k| match k {
            Value::Number(n) => n.as_i64(),
            _ => None,
        })
        .collect();
    match all_ints {
        Some(ints) => (
            format!("{quoted_column} = ANY($1::bigint[])"),
            Box::new(ints),
        ),
        None => (
            format!("{quoted_column}::text = ANY($1::text[])"),
            Box::new(
                keys.iter()
                    .map(|k| match k {
                        Value::String(s) => s.clone(),
                        other => other.to_string(),
                    })
                    .collect::<Vec<String>>(),
            ),
        ),
    }
}

fn pg_quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// Attach every configured child collection to a group of parent documents.
///
/// One query per collection for the whole group, which is the difference between
/// this and asking per row: a transaction touching a thousand children of one
/// parent asks once.
///
/// A collection cut short by `max_rows` says so in the document — the array plus
/// `<field>_truncated` and `<field>_total` — because a consumer cannot otherwise
/// tell a short array from a complete one, and silently handing over part of a
/// collection as if it were all of it is the failure this avoids.
pub async fn attach_children_batch(
    docs: &mut [(Value, &mut Value)],
    children: &[ChildSpec],
    client: &Client,
) -> Result<()> {
    if docs.is_empty() {
        return Ok(());
    }
    let keys: Vec<Value> = docs.iter().map(|(key, _)| key.clone()).collect();
    for spec in children {
        let by_key = spec.fetch_many(client, &keys).await?;
        // Summarised per collection rather than logged per document: a batch
        // where every parent is truncated would otherwise be one line each.
        let mut cut = 0usize;
        let mut largest: Option<(Value, i64)> = None;
        for (key, doc) in docs.iter_mut() {
            let Value::Object(map) = doc else { continue };
            let (arr, total) = match by_key.get(&key_lookup(key)) {
                Some((arr, total)) => (arr.clone(), *total),
                None => (Value::Array(vec![]), 0),
            };
            let embedded = arr.as_array().map(Vec::len).unwrap_or(0) as i64;
            map.insert(spec.field.clone(), arr);
            if total > embedded {
                map.insert(spec.truncated_field(), Value::Bool(true));
                map.insert(spec.total_field(), Value::from(total));
                cut += 1;
            }
            if largest.as_ref().is_none_or(|(_, most)| total > *most) {
                largest = Some((key.clone(), total));
            }
        }
        match largest {
            Some((key, total)) if cut > 0 => tracing::warn!(target: "pg2osync::source",
                "{cut} document(s) embed only max_rows of {}, and say so in {} and {}; \
                 the largest is parent {key} with {total} rows",
                spec.qualified(), spec.truncated_field(), spec.total_field()),
            // The number where this stops being a throughput question: past it
            // OpenSearch refuses a document whose field is mapped `nested`,
            // because every element becomes a hidden Lucene sub-document.
            Some((key, total)) if total >= UNBOUNDED_ARRAY_WARNING => {
                tracing::warn!(target: "pg2osync::source",
                    "parent {key} embeds {total} rows from {}; a target that maps the field \
                     as `nested` refuses more than {UNBOUNDED_ARRAY_WARNING}. Set max_rows \
                     on the collection to bound it",
                    spec.qualified())
            }
            _ => {}
        }
    }
    Ok(())
}

/// How the caller looks a key up in what `fetch_many` returned.
///
/// Both sides render the key as JSON, so an integer and its text form cannot
/// disagree about whether they are the same key.
pub fn key_lookup(key: &Value) -> String {
    key.to_string()
}

/// Past this many embedded rows, say so. It is OpenSearch's own
/// `index.mapping.nested_objects.limit` default: the point where an unbounded
/// array stops being slow and starts being refused.
const UNBOUNDED_ARRAY_WARNING: i64 = 10_000;

/// Re-read a group of parent rows as native-typed JSON documents.
///
/// One query for the whole group rather than one per changed child row, keyed by
/// the parent key so the caller can match without comparing again. A key with no
/// row in the result no longer exists, which the caller reports as nothing rather
/// than as an empty document.
pub async fn refetch_parents(
    client: &Client,
    schema: &str,
    table: &str,
    pk_column: &str,
    keys: &[Value],
) -> Result<HashMap<String, Value>> {
    if keys.is_empty() {
        return Ok(HashMap::new());
    }
    let quoted_pk = pg_quote_ident(pk_column);
    let (predicate, param) = any_predicate(&quoted_pk, keys);
    let sql = format!(
        "SELECT to_jsonb(t.{quoted_pk})::text, to_jsonb(t) \
         FROM {}.{} t WHERE {predicate}",
        pg_quote_ident(schema),
        pg_quote_ident(table),
    );
    let rows = client
        .query(&sql, &[param.as_ref()])
        .await
        .with_context(|| format!("parent refetch failed for {schema}.{table}"))?;
    Ok(rows
        .into_iter()
        .map(|r| {
            let key: String = r.get(0);
            let doc: Value = r.get(1);
            (key, doc)
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn integer_keys_do_not_pin_a_width() {
        // an int4 column cannot take an i64 parameter, and the column's width
        // is not known here; ::bigint compares across all integer widths and
        // still uses the index
        let (predicate, _) = any_predicate("\"customer_id\"", &[json!(42), json!(43)]);
        assert_eq!(predicate, "\"customer_id\" = ANY($1::bigint[])");
    }

    #[test]
    fn text_keys_stay_on_the_portable_comparison() {
        // a text parameter has no operator against uuid, so correctness wins
        let (predicate, _) = any_predicate("\"tenant\"", &[json!("acme")]);
        assert_eq!(predicate, "\"tenant\"::text = ANY($1::text[])");
    }

    #[test]
    fn one_non_integer_key_takes_the_whole_group_to_text() {
        // the parameter is one array, so it has one type: a mixed group cannot
        // send some keys as integers and the rest as text
        let (predicate, _) = any_predicate("\"k\"", &[json!(1), json!("two")]);
        assert_eq!(predicate, "\"k\"::text = ANY($1::text[])");
    }

    #[test]
    fn the_aggregation_orders_and_caps_in_one_place() {
        let mut spec = ChildSpec::new("public.orders", "orders", "customer_id", "id").unwrap();
        let plain = spec.agg_subquery(None);
        assert!(
            !plain.contains("PARTITION BY \"customer_id\" ORDER BY") && !plain.contains("rn <="),
            "nothing to order by and no cap set: {plain}"
        );

        spec.order_by = vec!["id".into()];
        spec.max_rows = Some(10);
        let capped = spec.agg_subquery(None);
        assert!(
            capped.contains("PARTITION BY \"customer_id\" ORDER BY \"id\""),
            "the kept rows must be the same ones twice running: {capped}"
        );
        assert!(capped.contains("r.rn <= 10"), "{capped}");
        assert!(
            capped.contains("count(*) OVER (PARTITION BY \"customer_id\")"),
            "the total is counted before the cap, so a cut document can say how \
             many there really are: {capped}"
        );
        // the filter belongs inside, or the aggregate reads the whole table
        let filtered = spec.agg_subquery(Some("\"customer_id\" = ANY($1::bigint[])"));
        let inner = filtered.find("FROM \"public\".\"orders\"").unwrap();
        assert!(
            filtered[inner..].contains("WHERE \"customer_id\" = ANY"),
            "the key filter reaches the innermost read: {filtered}"
        );
    }

    #[test]
    fn child_tables_are_schema_qualified_and_quoted() {
        let spec = ChildSpec::new("we\"ird.ch\"ild", "kids", "fk", "id").expect("qualified");
        assert_eq!(spec.schema, "we\"ird");
        assert_eq!(spec.table, "ch\"ild");
    }
}
