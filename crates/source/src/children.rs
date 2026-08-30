//! Reading nested child collections from PostgreSQL.
//!
//! Children are resolved on the source side so the engine stays
//! source-agnostic: a parent document simply arrives with extra array fields.
//! Child values are typed natively by PostgreSQL (`to_jsonb`), so no client-side
//! mapping is needed here — which is the one thing that does not carry over to
//! MySQL, whose JSON functions render binary, `bit`, `set` and `decimal`
//! differently from the rest of the pipeline.
//!
//! What a collection *is* — its field names, when a document may claim to hold
//! all of one, how a group of changed rows collapses to the parents it affects —
//! lives in `pg2osync_core::children`, so this reader and the MySQL one cannot
//! answer it differently.

pub use pg2osync_core::children::{
    ChildSpec, Duplicates, Pending, Through, UNBOUNDED_ARRAY_WARNING, apply_collection, key_lookup,
    keys_needing_refetch,
};

use crate::error::{Context as _, Result, SourceError};
use serde_json::Value;
use std::collections::HashMap;
use tokio_postgres::Client;

/// Fill in what the catalogue knows: the child's key, to order by.
pub async fn resolve_order(spec: &mut ChildSpec, client: &Client) -> Result<()> {
    let info = crate::catalog::table_info(client, &spec.schema, &spec.table).await?;
    if info.pk_columns.is_empty() && spec.single {
        return Err(SourceError::Config(format!(
            "[[sync.*.children]] single is set on {}, which has no primary key to order \
             by — with no order there is no first row, so two runs could embed different \
             ones. Add a primary key or drop single",
            spec.qualified()
        )));
    }
    if info.pk_columns.is_empty() && spec.max_rows.is_some() {
        return Err(SourceError::Config(format!(
            "[[sync.*.children]] max_rows is set on {}, which has no primary key to \
             order by — the same rows would not be kept twice running. Add a primary \
             key or drop max_rows",
            spec.qualified()
        )));
    }
    if info.pk_columns.is_empty() {
        tracing::warn!(target: "pg2osync::source",
            "{} has no primary key, so the order of its embedded array is whatever the \
             read returns and may differ between the initial load and a later re-fetch",
            spec.qualified());
    }
    let qualified = spec.qualified();
    if let Some(through) = &mut spec.through {
        let [child_key] = info.pk_columns.as_slice() else {
            return Err(SourceError::Config(format!(
                "[[sync.*.children]] {qualified} is reached through {}, so it needs a \
                 single-column primary key for the junction to point at; it has {}",
                through.qualified(),
                info.pk_columns.len()
            )));
        };
        through.child_key = child_key.clone();
    }
    spec.order_by = info.pk_columns;
    Ok(())
}

/// The subquery that turns one child table into one aggregated array per
/// parent key, with the total before any cap.
///
/// Shared by the initial load's `COPY` and the streaming re-fetch so the two
/// cannot disagree about what a child array contains — a disagreement is
/// invisible until someone re-snapshots.
pub fn agg_subquery(spec: &ChildSpec, filter: Option<&str>) -> String {
    let fk = fk_ref(spec);
    let ordered = if spec.order_by.is_empty() {
        String::new()
    } else {
        let cols: Vec<String> = spec
            .order_by
            .iter()
            .map(|c| format!("t.{}", pg_quote_ident(c)))
            .collect();
        format!(" ORDER BY {}", cols.join(", "))
    };
    // A many-to-many collection is this same aggregation with one more join:
    // the junction holds the parent's key, and nothing else about the read —
    // cap, count, order, projection — changes.
    let join = match &spec.through {
        Some(through) => format!(
            " JOIN {}.{} j ON j.{} = t.{}",
            pg_quote_ident(&through.schema),
            pg_quote_ident(&through.table),
            pg_quote_ident(&through.through_key),
            pg_quote_ident(&through.child_key),
        ),
        None => String::new(),
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
        "SELECT {fk} AS k, {element} AS doc, \
         row_number() OVER (PARTITION BY {fk}{ordered}) AS rn, \
         count(*) OVER (PARTITION BY {fk}) AS total \
         FROM {}.{} t{join}{where_clause}",
        pg_quote_ident(&spec.schema),
        pg_quote_ident(&spec.table),
        element = element_expr(spec),
    );
    let capped = match spec.max_rows {
        Some(n) => format!("SELECT * FROM ({ranked}) r WHERE r.rn <= {n}"),
        None => format!("SELECT * FROM ({ranked}) r"),
    };
    format!(
        "SELECT k, COALESCE(jsonb_agg(doc ORDER BY rn), '[]'::jsonb) AS agg, \
         max(total) AS total FROM ({capped}) c GROUP BY k"
    )
}

/// One child row as the JSON object that lands in the array.
///
/// The projection lives in the element expression rather than after the read, so
/// the `COPY` of the initial load and the per-transaction re-fetch share it by
/// construction; PostgreSQL also never reads a column the expression does not
/// name, which for a TOASTed one is the whole point.
fn element_expr(spec: &ChildSpec) -> String {
    if let Some(columns) = &spec.columns {
        let pairs: Vec<String> = columns
            .iter()
            .map(|c| format!("{}, t.{}", pg_quote_literal(c), pg_quote_ident(c)))
            .collect();
        return format!("jsonb_build_object({})", pairs.join(", "));
    }
    if spec.exclude_columns.is_empty() {
        return "to_jsonb(t)".to_string();
    }
    let names: Vec<String> = spec
        .exclude_columns
        .iter()
        .map(|c| pg_quote_literal(c))
        .collect();
    format!("to_jsonb(t) - ARRAY[{}]::text[]", names.join(", "))
}

/// Every listed parent's child array, in one query.
///
/// One query per collection per batch rather than per changed row: a
/// transaction touching a thousand children of one parent asks once. Returns
/// the array and the total the source holds, keyed by the parent key so the
/// caller can match without another comparison.
pub async fn fetch_many(
    spec: &ChildSpec,
    client: &Client,
    keys: &[Value],
) -> Result<HashMap<String, (Value, i64)>> {
    if keys.is_empty() {
        return Ok(HashMap::new());
    }
    let (predicate, param) = any_predicate(&fk_ref(spec), keys);
    // to_jsonb of the key rather than its text: the caller matches on the
    // JSON form, so a number and its rendering cannot drift apart
    let sql = format!(
        "SELECT to_jsonb(c.k)::text, c.agg, c.total FROM ({}) c",
        agg_subquery(spec, Some(&predicate)),
    );
    let rows = client
        .query(&sql, &[param.as_ref()])
        .await
        .catalog_ctx(|| format!("child fetch failed for {}", spec.qualified()))?;
    let mut out = HashMap::new();
    for row in rows {
        let key: String = row.get(0);
        let agg: Value = row.get(1);
        let total: i64 = row.get(2);
        out.insert(key, (agg, total));
    }
    Ok(out)
}

/// The parent key as the aggregation refers to it, qualified by the table that
/// holds it.
///
/// Qualified even without a junction: a child table with a column named like the
/// parent's key would otherwise make the predicate ambiguous the moment a join
/// stands beside it.
fn fk_ref(spec: &ChildSpec) -> String {
    let alias = if spec.through.is_some() { "j" } else { "t" };
    format!("{alias}.{}", pg_quote_ident(&spec.foreign_key))
}

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

fn pg_quote_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
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
        let by_key = fetch_many(spec, client, &keys).await?;
        // Summarised per collection rather than logged per document: a batch
        // where every parent is truncated would otherwise be one line each.
        let mut cut = 0usize;
        let mut largest: Option<(Value, i64)> = None;
        let mut duplicates = Duplicates::default();
        for (key, doc) in docs.iter_mut() {
            let (arr, total) = match by_key.get(&key_lookup(key)) {
                Some((arr, total)) => (arr.clone(), *total),
                None => (Value::Array(vec![]), 0),
            };
            // shaped by core rather than here, so the streamed document and the
            // loaded one cannot differ about what a collection looks like
            let applied = apply_collection(doc, spec, arr, total);
            if applied.truncated {
                cut += 1;
            }
            duplicates.record(spec, key, applied.matched);
            if largest.as_ref().is_none_or(|(_, most)| total > *most) {
                largest = Some((key.clone(), total));
            }
        }
        if let Some(message) = duplicates.message(spec) {
            tracing::warn!(target: "pg2osync::source", "{message}");
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
        .catalog_ctx(|| format!("parent refetch failed for {schema}.{table}"))?;
    Ok(rows
        .into_iter()
        .map(|r| {
            let key: String = r.get(0);
            let doc: Value = r.get(1);
            (key, doc)
        })
        .collect())
}

/// The parents a group of changed child rows belongs to, in one query.
///
/// A row of a through collection's child table names no parent — the junction
/// is what does — so the transaction's distinct child keys are resolved once, at
/// commit, and merged into the parents the group already names. One query per
/// collection, whatever the transaction touched.
pub async fn parents_through(
    client: &Client,
    spec: &ChildSpec,
    child_keys: &[Value],
) -> Result<Vec<Value>> {
    let Some(through) = &spec.through else {
        return Ok(Vec::new());
    };
    if child_keys.is_empty() {
        return Ok(Vec::new());
    }
    let (predicate, param) = any_predicate(
        &format!("j.{}", pg_quote_ident(&through.through_key)),
        child_keys,
    );
    let sql = format!(
        "SELECT DISTINCT to_jsonb(j.{}) FROM {}.{} j WHERE {predicate}",
        pg_quote_ident(&spec.foreign_key),
        pg_quote_ident(&through.schema),
        pg_quote_ident(&through.table),
    );
    let rows = client
        .query(&sql, &[param.as_ref()])
        .await
        .catalog_ctx(|| format!("junction lookup failed for {}", through.qualified()))?;
    Ok(rows.into_iter().map(|r| r.get(0)).collect())
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
    fn a_one_to_one_child_is_read_by_the_same_aggregation() {
        // The unwrapping belongs to core, so PostgreSQL keeps exactly one
        // builder: a second `LIMIT 1` query is what would let the initial load
        // and a streamed re-fetch drift apart.
        let mut spec = ChildSpec::new("public.profiles", "profile", "customer_id", "id").unwrap();
        spec.single = true;
        spec.order_by = vec!["customer_id".into()];
        let sql = agg_subquery(&spec, None);
        assert!(sql.contains("jsonb_agg(doc ORDER BY rn)"), "{sql}");
        assert!(!sql.contains("LIMIT"), "{sql}");

        // and what the document ends up with is the element, with neither of
        // the two fields a cut array writes
        let mut doc = json!({"id": 1});
        apply_collection(&mut doc, &spec, json!([{"bio": "hi"}]), 1);
        assert_eq!(doc["profile"], json!({"bio": "hi"}));
        assert!(doc.get("profile_truncated").is_none());
        assert!(doc.get("profile_total").is_none());
    }

    #[test]
    fn the_aggregation_orders_and_caps_in_one_place() {
        let mut spec = ChildSpec::new("public.orders", "orders", "customer_id", "id").unwrap();
        let plain = agg_subquery(&spec, None);
        assert!(
            !plain.contains("PARTITION BY t.\"customer_id\" ORDER BY") && !plain.contains("rn <="),
            "nothing to order by and no cap set: {plain}"
        );

        spec.order_by = vec!["id".into()];
        spec.max_rows = Some(10);
        let capped = agg_subquery(&spec, None);
        assert!(
            capped.contains("PARTITION BY t.\"customer_id\" ORDER BY t.\"id\""),
            "the kept rows must be the same ones twice running: {capped}"
        );
        assert!(capped.contains("r.rn <= 10"), "{capped}");
        assert!(
            capped.contains("count(*) OVER (PARTITION BY t.\"customer_id\")"),
            "the total is counted before the cap, so a cut document can say how \
             many there really are: {capped}"
        );
        // the filter belongs inside, or the aggregate reads the whole table
        let filtered = agg_subquery(&spec, Some("\"customer_id\" = ANY($1::bigint[])"));
        let inner = filtered.find("FROM \"public\".\"orders\"").unwrap();
        assert!(
            filtered[inner..].contains("WHERE \"customer_id\" = ANY"),
            "the key filter reaches the innermost read: {filtered}"
        );
    }

    #[test]
    fn the_projection_is_the_element_the_load_and_the_stream_share() {
        let mut spec = ChildSpec::new("public.orders", "orders", "customer_id", "id").unwrap();
        spec.order_by = vec!["id".into()];
        assert!(
            agg_subquery(&spec, None).contains("to_jsonb(t) AS doc"),
            "no projection leaves the whole row"
        );

        spec.columns = Some(vec!["id".into(), "o'dd".into()]);
        let listed = agg_subquery(&spec, None);
        assert!(
            listed.contains("jsonb_build_object('id', t.\"id\", 'o''dd', t.\"o'dd\") AS doc"),
            "names are literals, columns are identifiers: {listed}"
        );
        assert!(
            listed.contains("PARTITION BY t.\"customer_id\" ORDER BY t.\"id\""),
            "the key is read beside the element, so it need not be in it: {listed}"
        );

        spec.columns = None;
        spec.exclude_columns = vec!["internal_notes".into()];
        let excluded = agg_subquery(&spec, None);
        assert!(
            excluded.contains("to_jsonb(t) - ARRAY['internal_notes']::text[] AS doc"),
            "{excluded}"
        );
        assert!(excluded.contains("count(*) OVER (PARTITION BY t.\"customer_id\")"));
    }

    fn through_spec() -> ChildSpec {
        let mut spec = ChildSpec::new("public.authors", "authors", "book_id", "id").unwrap();
        let mut through = Through::new("public.book_author", "author_id").unwrap();
        through.child_key = "id".into();
        spec.through = Some(through);
        spec.order_by = vec!["id".into()];
        spec
    }

    #[test]
    fn a_many_to_many_collection_is_the_same_aggregation_with_one_join() {
        let sql = agg_subquery(&through_spec(), None);
        assert!(
            sql.contains(
                "FROM \"public\".\"authors\" t JOIN \"public\".\"book_author\" j \
                 ON j.\"author_id\" = t.\"id\""
            ),
            "the junction joins the child on the child's own key: {sql}"
        );
        assert!(
            sql.contains("SELECT j.\"book_id\" AS k")
                && sql.contains("PARTITION BY j.\"book_id\" ORDER BY t.\"id\"")
                && sql.contains("count(*) OVER (PARTITION BY j.\"book_id\")"),
            "the parent key comes off the junction, everywhere: {sql}"
        );
        assert!(
            sql.contains("to_jsonb(t) AS doc")
                && sql.contains("jsonb_agg(doc ORDER BY rn)")
                && sql.contains("GROUP BY k"),
            "the element and the aggregation are what they already were: {sql}"
        );
    }

    #[test]
    fn a_junction_keeps_the_cap_the_count_and_the_key_filter_inside() {
        let mut spec = through_spec();
        spec.max_rows = Some(3);
        let sql = agg_subquery(&spec, Some("j.\"book_id\" = ANY($1::bigint[])"));
        assert!(sql.contains("r.rn <= 3"), "{sql}");
        let inner = sql
            .find("FROM \"public\".\"authors\" t")
            .expect("inner read");
        assert!(
            sql[inner..].contains("WHERE j.\"book_id\" = ANY"),
            "the key filter reaches the innermost read, past the join: {sql}"
        );
        assert!(
            sql.find("JOIN").expect("join") < sql.find("WHERE").expect("where"),
            "the join comes before the predicate that uses it: {sql}"
        );
    }

    #[test]
    fn the_parent_key_is_qualified_by_the_table_that_holds_it() {
        // Unqualified, a child column named like the parent's key makes the
        // predicate ambiguous the moment a junction joins beside it — and the
        // error is a failed query on every batch.
        let mut spec = ChildSpec::new("public.orders", "orders", "customer_id", "id").unwrap();
        assert_eq!(fk_ref(&spec), "t.\"customer_id\"");
        let mut through = Through::new("public.book_author", "author_id").unwrap();
        through.child_key = "id".into();
        spec.through = Some(through);
        assert_eq!(fk_ref(&spec), "j.\"customer_id\"");
    }

    #[test]
    fn child_tables_are_schema_qualified_and_quoted() {
        let spec = ChildSpec::new("we\"ird.ch\"ild", "kids", "fk", "id").expect("qualified");
        assert_eq!(spec.schema, "we\"ird");
        assert_eq!(spec.table, "ch\"ild");
    }
}
