//! Reading a child table's aggregate from PostgreSQL.
//!
//! The same shape as `children`, one value wide: a grouped read of the child
//! table, keyed by the foreign key, joined into the load's `COPY` and read once
//! per transaction while streaming. What an aggregate is, and what a parent
//! nothing matched carries, lives in `pg2osync_core::aggregate` so this reader
//! and the MySQL one cannot answer it differently.

pub use pg2osync_core::aggregate::{AggregateSpec, apply_count};

use crate::children::{any_predicate, key_lookup, pg_quote_ident, pg_quote_literal};
use crate::error::{Context as _, Result};
use serde_json::Value;
use std::collections::HashMap;
use tokio_postgres::Client;

/// The grouped read one aggregate is: one row per parent key, and how many
/// child rows matched.
///
/// Shared by the initial load's `COPY` and the streaming re-read, for the
/// reason the child aggregation is: two builders would disagree the moment
/// either changed, and the disagreement is invisible until someone
/// re-snapshots.
pub fn count_subquery(spec: &AggregateSpec, key_predicate: Option<&str>) -> String {
    let fk = format!("t.{}", pg_quote_ident(&spec.foreign_key));
    let mut conditions: Vec<String> = Vec::new();
    // Which rows count, rendered from the parsed predicate rather than pasted
    // in: the operator's `where` never reaches the server as text of its own.
    if let Some(filter) = &spec.filter {
        conditions.push(format!("({})", filter.to_sql(&dialect())));
    }
    // The key filter belongs to the innermost read, as it does for a child
    // collection: applied outside the grouping it would count the whole table
    // and only then discard it.
    if let Some(predicate) = key_predicate {
        conditions.push(predicate.to_string());
    }
    let where_clause = if conditions.is_empty() {
        String::new()
    } else {
        format!(" WHERE {}", conditions.join(" AND "))
    };
    format!(
        "SELECT {fk} AS k, count(*) AS n FROM {}.{} t{where_clause} GROUP BY {fk}",
        pg_quote_ident(&spec.schema),
        pg_quote_ident(&spec.table),
    )
}

/// How the aggregate's `where` spells a column: the table it counts is aliased
/// `t`, and the grouped key sits beside it as `k`.
fn dialect() -> pg2osync_core::filter::SqlDialect<'static> {
    fn ident(name: &str) -> String {
        format!("t.{}", pg_quote_ident(name))
    }
    pg2osync_core::filter::SqlDialect {
        quote_ident: &ident,
        quote_str: &pg_quote_literal,
    }
}

/// Every listed parent's number, in one query.
///
/// One query per aggregate per batch rather than per changed row, exactly as a
/// child collection is read. A key with no row matched nothing, which the
/// caller turns into zero.
pub async fn fetch_counts(
    spec: &AggregateSpec,
    client: &Client,
    keys: &[Value],
) -> Result<HashMap<String, i64>> {
    if keys.is_empty() {
        return Ok(HashMap::new());
    }
    let (predicate, param) =
        any_predicate(&format!("t.{}", pg_quote_ident(&spec.foreign_key)), keys);
    // to_jsonb of the key rather than its text: the caller matches on the JSON
    // form, so a number and its rendering cannot drift apart
    let sql = format!(
        "SELECT to_jsonb(c.k)::text, c.n FROM ({}) c",
        count_subquery(spec, Some(&predicate)),
    );
    let rows = client
        .query(&sql, &[param.as_ref()])
        .await
        .catalog_ctx(|| format!("aggregate read failed for {}", spec.qualified()))?;
    Ok(rows
        .into_iter()
        .map(|row| {
            let key: String = row.get(0);
            let n: i64 = row.get(1);
            (key, n)
        })
        .collect())
}

/// Attach every configured aggregate to a group of parent documents.
pub async fn attach_aggregates_batch(
    docs: &mut [(Value, &mut Value)],
    aggregates: &[AggregateSpec],
    client: &Client,
) -> Result<()> {
    if docs.is_empty() {
        return Ok(());
    }
    let keys: Vec<Value> = docs.iter().map(|(key, _)| key.clone()).collect();
    for spec in aggregates {
        let by_key = fetch_counts(spec, client, &keys).await?;
        for (key, doc) in docs.iter_mut() {
            let count = by_key.get(&key_lookup(key)).copied().unwrap_or(0);
            apply_count(doc, spec, count);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn spec() -> AggregateSpec {
        AggregateSpec::new("public.deals", "open_deals", "contact_id", "id").expect("qualified")
    }

    #[test]
    fn the_read_groups_by_the_qualified_foreign_key() {
        let sql = count_subquery(&spec(), None);
        assert_eq!(
            sql,
            "SELECT t.\"contact_id\" AS k, count(*) AS n \
             FROM \"public\".\"deals\" t GROUP BY t.\"contact_id\""
        );
    }

    #[test]
    fn the_where_subset_reaches_the_counted_rows() {
        let mut spec = spec();
        spec.filter = Some(
            pg2osync_core::filter::Filter::parse("status_type = 1 AND note <> 'x'")
                .expect("parses"),
        );
        let sql = count_subquery(&spec, Some("t.\"contact_id\" = ANY($1::bigint[])"));
        assert!(
            sql.contains("t.\"status_type\" = 1 AND t.\"note\" <> 'x'"),
            "the predicate is qualified by the table it counts: {sql}"
        );
        assert!(
            sql.contains("AND t.\"contact_id\" = ANY($1::bigint[])"),
            "and the key filter joins it rather than replacing it: {sql}"
        );
        assert!(
            sql.find("WHERE").expect("where") < sql.find("GROUP BY").expect("group"),
            "counted before grouping, or the whole table is grouped first: {sql}"
        );
    }

    #[test]
    fn a_parent_no_row_matched_is_a_zero_rather_than_a_missing_field() {
        let spec = spec();
        let mut doc = json!({"id": 4});
        apply_count(&mut doc, &spec, 0);
        assert_eq!(doc["open_deals"], json!(0));
    }
}
