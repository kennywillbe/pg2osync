//! Reading a child table's aggregate from MySQL and MariaDB.
//!
//! The same shape as `children`, one value wide: one grouped read per aggregate
//! per batch, keyed by the foreign key. Nothing of the type mapping applies
//! here — a count is an integer whatever the column it counted — so unlike an
//! embedded array this needs no `build_document`; only the *key* comes back as
//! a value, and it is read with the declared shape of the column that holds it
//! so a number and its text form cannot become two different parents.

pub use pg2osync_core::aggregate::{AggregateSpec, apply_count};

use crate::catalog;
use crate::children::key_list;
use crate::connection::MySqlConnection;
use crate::error::{Context as _, Result};
use pg2osync_core::children::key_lookup;
use serde_json::Value;
use std::collections::HashMap;

/// The grouped read one aggregate is, for the listed parents.
///
/// With no parents listed it is the read over the whole table, which is what a
/// startup probe asks the server to plan and nothing else uses: a read for
/// nobody would be a statement with an empty `IN ()`.
pub fn count_statement(spec: &AggregateSpec, keys: &[Value]) -> String {
    let fk = format!("t.{}", catalog::quote_ident(&spec.foreign_key));
    let mut conditions: Vec<String> = Vec::new();
    if !keys.is_empty() {
        conditions.push(format!("{fk} IN ({})", key_list(keys)));
    }
    // Which rows count, rendered from the parsed predicate rather than pasted
    // in: the operator's `where` never reaches the server as text of its own.
    if let Some(filter) = &spec.filter {
        conditions.push(format!("({})", filter.to_sql(&dialect())));
    }
    let where_clause = if conditions.is_empty() {
        String::new()
    } else {
        format!(" WHERE {}", conditions.join(" AND "))
    };
    format!(
        "SELECT {fk}, COUNT(*) FROM {}.{} t{where_clause} GROUP BY {fk}",
        catalog::quote_ident(&spec.schema),
        catalog::quote_ident(&spec.table),
    )
}

/// How the aggregate's `where` spells a column: the table it counts is aliased
/// `t`, and its own key rides beside the count.
fn dialect() -> pg2osync_core::filter::SqlDialect<'static> {
    fn ident(name: &str) -> String {
        format!("t.{}", catalog::quote_ident(name))
    }
    pg2osync_core::filter::SqlDialect {
        quote_ident: &ident,
        quote_str: &catalog::quote_str,
    }
}

/// Every listed parent's number, in one query.
///
/// A key with no row matched nothing, which the caller turns into zero.
pub async fn fetch_counts(
    spec: &AggregateSpec,
    conn: &mut MySqlConnection,
    keys: &[Value],
) -> Result<HashMap<String, i64>> {
    if keys.is_empty() {
        return Ok(HashMap::new());
    }
    // keyless_ok: counting rows addresses none of them, so the table this
    // aggregates needs no primary key of its own
    let resolved = catalog::table_schema(conn, &spec.schema, &spec.table, true).await?;
    let shape = resolved
        .columns
        .iter()
        .find(|c| c.name == spec.foreign_key)
        .map(|c| c.shape.clone())
        .unwrap_or(crate::typemap::ValueShape::Text);
    let sql = count_statement(spec, keys);
    let mut rows = conn
        .text_query(&sql)
        .await
        .catalog_ctx(|| format!("aggregate read failed for {}", spec.qualified()))?;
    let mut out = HashMap::new();
    while let Some(row) = rows.next().await? {
        let key = crate::typemap::convert(&shape, row.first().and_then(|v| v.as_deref()));
        let count: i64 = row
            .get(1)
            .and_then(|v| v.as_deref())
            .map(|b| String::from_utf8_lossy(b).parse().unwrap_or(0))
            .unwrap_or(0);
        out.insert(key_lookup(&key), count);
    }
    Ok(out)
}

/// Attach every configured aggregate to a group of parent documents.
pub async fn attach_aggregates(
    conn: &mut MySqlConnection,
    aggregates: &[AggregateSpec],
    rows: &mut [pg2osync_core::event::RowChange],
) -> Result<()> {
    if rows.is_empty() {
        return Ok(());
    }
    let keys: Vec<Value> = rows.iter().map(|r| r.pk().clone()).collect();
    for spec in aggregates {
        let by_key = fetch_counts(spec, conn, &keys).await?;
        for change in rows.iter_mut() {
            let key = change.pk().clone();
            let Some(doc) = change.doc_mut() else {
                continue;
            };
            apply_count(
                doc,
                spec,
                by_key.get(&key_lookup(&key)).copied().unwrap_or(0),
            );
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn spec() -> AggregateSpec {
        AggregateSpec::new("shop.deals", "open_deals", "contact_id", "id").expect("qualified")
    }

    #[test]
    fn the_read_groups_by_the_qualified_foreign_key() {
        let sql = count_statement(&spec(), &[json!(1), json!(2)]);
        assert_eq!(
            sql,
            "SELECT t.`contact_id`, COUNT(*) FROM `shop`.`deals` t \
             WHERE t.`contact_id` IN (1, 2) GROUP BY t.`contact_id`"
        );
    }

    #[test]
    fn the_where_subset_reaches_the_counted_rows() {
        let mut spec = spec();
        spec.filter =
            Some(pg2osync_core::filter::Filter::parse("status_type = 1").expect("parses"));
        let sql = count_statement(&spec, &[json!("acme")]);
        assert!(
            sql.contains("WHERE t.`contact_id` IN ('acme') AND (t.`status_type` = 1)"),
            "the predicate is qualified by the table it counts: {sql}"
        );
        assert!(
            sql.find("WHERE").expect("where") < sql.find("GROUP BY").expect("group"),
            "counted before grouping: {sql}"
        );
    }
}
