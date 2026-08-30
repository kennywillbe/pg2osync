//! Reading nested child collections from MySQL and MariaDB.
//!
//! The array is aggregated in Rust rather than by `JSON_ARRAYAGG(JSON_OBJECT(…))`,
//! which looks like the obvious tool and is the wrong one. Measured on both
//! servers, `JSON_OBJECT` renders a `varbinary` as `"base64:type15:AP8Q"` on MySQL
//! and as raw escaped bytes on MariaDB where the rest of the pipeline says
//! `"AP8Q"`; a `set` as `"a,b"` where the pipeline says `["a","b"]`; a `decimal`
//! as a JSON number, losing the precision the pipeline keeps as a string; and a
//! `bit` as base64 on MySQL and as *invalid JSON* on MariaDB — its own
//! `JSON_VALID` returns 0.
//!
//! Casting each column (`TO_BASE64`, `CAST(… AS CHAR)`, `CAST(… AS UNSIGNED)`)
//! gets closer and still fails: `TO_BASE64` wraps at 76 characters, so any value
//! over 57 bytes disagrees with the pipeline's base64, and a `set` cannot become
//! an array without `JSON_TABLE` per row. Even where it works it would mean
//! writing `typemap`'s decisions a second time, in SQL, for the two to agree.
//!
//! So child rows come back as ordinary rows and go through
//! `catalog::build_document` — the same function that builds a parent — and the
//! values inside an array are identical to the same row's values as a document
//! because it is the same code. The cost stays one query per collection per
//! batch: the window functions that order, cap and count are still done by the
//! server.

use crate::catalog::{self, TableSchema};
use crate::connection::MySqlConnection;
use crate::error::{Context as _, MySqlError, Result};
use pg2osync_core::children::{ChildSpec, key_lookup};
use serde_json::Value;
use std::collections::HashMap;

/// Fill in what the catalogue knows: the child's key, to order by.
pub async fn resolve_order(spec: &mut ChildSpec, conn: &mut MySqlConnection) -> Result<()> {
    let resolved = catalog::table_schema(conn, &spec.schema, &spec.table, false).await?;
    if resolved.pk_columns.is_empty() && spec.single {
        return Err(MySqlError::Config(format!(
            "[[sync.*.children]] single is set on {}, which has no primary key to order \
             by — with no order there is no first row, so two runs could embed different \
             ones. Add a primary key or drop single",
            spec.qualified()
        )));
    }
    if resolved.pk_columns.is_empty() && spec.max_rows.is_some() {
        return Err(MySqlError::Config(format!(
            "[[sync.*.children]] max_rows is set on {}, which has no primary key to order \
             by — the same rows would not be kept twice running. Add a primary key or drop \
             max_rows",
            spec.qualified()
        )));
    }
    if resolved.pk_columns.is_empty() {
        tracing::warn!(target: "pg2osync::source",
            "{} has no primary key, so the order of its embedded array is whatever the read \
             returns and may differ between the initial load and a later re-fetch",
            spec.qualified());
    }
    spec.order_by = resolved.pk_columns;
    Ok(())
}

/// Every listed parent's child array, in one query.
///
/// Returns each parent's array and the total the source holds before any cap,
/// keyed so the caller can match without comparing again.
pub async fn fetch_many(
    spec: &ChildSpec,
    conn: &mut MySqlConnection,
    keys: &[Value],
) -> Result<HashMap<String, (Value, i64)>> {
    if keys.is_empty() {
        return Ok(HashMap::new());
    }
    let resolved = catalog::table_schema(conn, &spec.schema, &spec.table, false).await?;
    let fk_shape = fk_shape(spec, &resolved);
    let projected = project(spec, &resolved);
    let sql = fetch_statement(spec, &projected, keys);
    let mut rows = conn
        .text_query(&sql)
        .await
        .catalog_ctx(|| format!("child fetch failed for {}", spec.qualified()))?;

    // The foreign key and the total ride after the child's own columns, so the
    // row prefix is exactly what build_document expects.
    let columns = projected.columns.len();
    let mut out: HashMap<String, (Value, i64)> = HashMap::new();
    while let Some(row) = rows.next().await? {
        let (doc, _) = catalog::build_document(&projected, &row);
        let key_raw = row.get(columns).and_then(|v| v.as_deref());
        let key = crate::typemap::convert(&fk_shape, key_raw);
        let total: i64 = row
            .get(columns + 1)
            .and_then(|v| v.as_deref())
            .map(|b| String::from_utf8_lossy(b).parse().unwrap_or(0))
            .unwrap_or(0);
        let entry = out
            .entry(key_lookup(&key))
            .or_insert_with(|| (Value::Array(Vec::new()), total));
        entry.1 = total;
        if let Value::Array(arr) = &mut entry.0 {
            arr.push(doc);
        }
    }
    // A parent with no children at all returns no rows, and the caller turns a
    // missing key into an empty array — the same answer either way.
    Ok(out)
}

/// Re-read a group of parent rows as documents.
pub async fn refetch_parents(
    conn: &mut MySqlConnection,
    schema: &str,
    table: &str,
    keys: &[Value],
) -> Result<HashMap<String, Value>> {
    if keys.is_empty() {
        return Ok(HashMap::new());
    }
    let resolved = catalog::table_schema(conn, schema, table, false).await?;
    let [pk_column] = resolved.pk_columns.as_slice() else {
        return Err(MySqlError::Config(format!(
            "{schema}.{table} is a parent of a nested collection, so it needs a single-column \
             primary key to be located by; it has {}",
            resolved.pk_columns.len()
        )));
    };
    let sql = format!(
        "SELECT {} FROM {}.{} WHERE {} IN ({})",
        resolved
            .columns
            .iter()
            .map(|c| catalog::quote_ident(&c.name))
            .collect::<Vec<_>>()
            .join(", "),
        catalog::quote_ident(schema),
        catalog::quote_ident(table),
        catalog::quote_ident(pk_column),
        key_list(keys),
    );
    let mut rows = conn
        .text_query(&sql)
        .await
        .catalog_ctx(|| format!("parent refetch failed for {schema}.{table}"))?;
    let mut out = HashMap::new();
    while let Some(row) = rows.next().await? {
        let (doc, pk) = catalog::build_document(&resolved, &row);
        out.insert(key_lookup(&pk), doc);
    }
    Ok(out)
}

/// How to read the parent key that rides beside each element.
///
/// It is taken from the whole table and never from the projection: the key is
/// its own column in the result, so projecting the foreign key out of the
/// element must not turn an integer key into text — the caller matches parents
/// on the JSON form, where `1` and `"1"` are different parents and every array
/// would silently come back empty.
fn fk_shape(spec: &ChildSpec, resolved: &TableSchema) -> crate::typemap::ValueShape {
    resolved
        .columns
        .iter()
        .find(|c| c.name == spec.foreign_key)
        .map(|c| c.shape.clone())
        .unwrap_or(crate::typemap::ValueShape::Text)
}

/// The child's columns as the element is to carry them.
///
/// MySQL aggregates in Rust, so the projection is the select list: the same
/// schema builds the statement, positions `pg2osync_fk` after the last column
/// and maps the row back to JSON, and the three cannot drift apart.
fn project(spec: &ChildSpec, resolved: &TableSchema) -> TableSchema {
    let keep = |name: &str| match &spec.columns {
        Some(columns) => columns.iter().any(|c| c == name),
        None => !spec.exclude_columns.iter().any(|c| c == name),
    };
    TableSchema {
        columns: resolved
            .columns
            .iter()
            .filter(|c| keep(&c.name))
            .cloned()
            .collect(),
        pk_columns: resolved.pk_columns.clone(),
    }
}

/// The statement one collection's rows come back on.
///
/// Ordered by the child's key so the same rows come back twice running, capped by
/// the server rather than after the fact, and carrying the count before the cap so
/// a cut array can say how much it is missing.
fn fetch_statement(spec: &ChildSpec, resolved: &TableSchema, keys: &[Value]) -> String {
    let columns: Vec<String> = resolved
        .columns
        .iter()
        .map(|c| format!("t.{}", catalog::quote_ident(&c.name)))
        .collect();
    let ordered = if spec.order_by.is_empty() {
        String::new()
    } else {
        let cols: Vec<String> = spec
            .order_by
            .iter()
            .map(|c| format!("t.{}", catalog::quote_ident(c)))
            .collect();
        format!(" ORDER BY {}", cols.join(", "))
    };
    // The key filter belongs to the innermost read: applied outside the window it
    // would rank the whole child table and only then discard it.
    let ranked = format!(
        "SELECT {}, t.{} AS pg2osync_fk, \
         ROW_NUMBER() OVER (PARTITION BY t.{}{ordered}) AS pg2osync_rn, \
         COUNT(*) OVER (PARTITION BY t.{}) AS pg2osync_total \
         FROM {}.{} t WHERE t.{} IN ({})",
        columns.join(", "),
        catalog::quote_ident(&spec.foreign_key),
        catalog::quote_ident(&spec.foreign_key),
        catalog::quote_ident(&spec.foreign_key),
        catalog::quote_ident(&spec.schema),
        catalog::quote_ident(&spec.table),
        catalog::quote_ident(&spec.foreign_key),
        key_list(keys),
    );
    let selected: Vec<String> = resolved
        .columns
        .iter()
        .map(|c| format!("r.{}", catalog::quote_ident(&c.name)))
        .collect();
    match spec.max_rows {
        Some(n) => format!(
            "SELECT {}, r.pg2osync_fk, r.pg2osync_total FROM ({ranked}) r \
             WHERE r.pg2osync_rn <= {n} ORDER BY r.pg2osync_fk, r.pg2osync_rn",
            selected.join(", ")
        ),
        None => format!(
            "SELECT {}, r.pg2osync_fk, r.pg2osync_total FROM ({ranked}) r \
             ORDER BY r.pg2osync_fk, r.pg2osync_rn",
            selected.join(", ")
        ),
    }
}

/// Parent keys as SQL literals.
///
/// The values come from the source itself, and are emitted through the same
/// literal rules the chunked load uses so a number stays a number and anything
/// else is quoted and escaped.
fn key_list(keys: &[Value]) -> String {
    keys.iter()
        .map(|k| match k {
            Value::Number(n) => n.to_string(),
            Value::String(s) => catalog::quote_str(s),
            other => catalog::quote_str(&other.to_string()),
        })
        .collect::<Vec<_>>()
        .join(", ")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::Column;
    use crate::typemap::ValueShape;
    use serde_json::json;

    fn schema() -> TableSchema {
        TableSchema {
            columns: vec![
                Column {
                    name: "id".into(),
                    shape: ValueShape::Int,
                },
                Column {
                    name: "parent_id".into(),
                    shape: ValueShape::Int,
                },
            ],
            pk_columns: vec!["id".into()],
        }
    }

    fn spec() -> ChildSpec {
        let mut spec = ChildSpec::new("shop.items", "items", "parent_id", "id").expect("qualified");
        spec.order_by = vec!["id".into()];
        spec
    }

    #[test]
    fn the_read_is_ordered_capped_and_filtered_inside_the_window() {
        let mut spec = spec();
        spec.max_rows = Some(5);
        let sql = fetch_statement(&spec, &schema(), &[json!(1), json!(2)]);
        assert!(
            sql.contains("PARTITION BY t.`parent_id` ORDER BY t.`id`"),
            "the same rows have to come back twice running: {sql}"
        );
        assert!(sql.contains("pg2osync_rn <= 5"), "{sql}");
        assert!(
            sql.contains("COUNT(*) OVER (PARTITION BY t.`parent_id`)"),
            "the total is counted before the cap: {sql}"
        );
        let inner = sql.find("FROM `shop`.`items` t").expect("inner read");
        assert!(
            sql[inner..].contains("WHERE t.`parent_id` IN (1, 2)"),
            "the filter reaches the innermost read, or the window ranks the whole \
             table: {sql}"
        );
    }

    #[test]
    fn without_a_cap_nothing_is_dropped() {
        let sql = fetch_statement(&spec(), &schema(), &[json!(1)]);
        assert!(!sql.contains("pg2osync_rn <="), "{sql}");
        assert!(
            sql.contains("ORDER BY r.pg2osync_fk, r.pg2osync_rn"),
            "{sql}"
        );
    }

    #[test]
    fn the_projection_is_the_select_list() {
        let mut spec = spec();
        spec.columns = Some(vec!["id".into()]);
        let projected = project(&spec, &schema());
        let sql = fetch_statement(&spec, &projected, &[json!(1)]);
        assert!(
            sql.contains("SELECT t.`id`, t.`parent_id` AS pg2osync_fk"),
            "{sql}"
        );
        assert!(
            !sql.contains("t.`parent_id`,"),
            "an unlisted column is never read: {sql}"
        );
        assert_eq!(
            projected.columns.len(),
            1,
            "the offset of pg2osync_fk is the projected column count"
        );

        spec.columns = None;
        spec.exclude_columns = vec!["parent_id".into()];
        assert_eq!(project(&spec, &schema()).columns.len(), 1);
    }

    #[test]
    fn an_excluded_foreign_key_still_reads_as_the_column_it_is() {
        // The trap: take the shape from the projection and an excluded integer
        // key falls through to Text, so every parent matches "1" against 1 and
        // silently gets an empty array.
        let mut spec = spec();
        spec.exclude_columns = vec!["parent_id".into()];
        let full = schema();
        assert!(
            !project(&spec, &full)
                .columns
                .iter()
                .any(|c| c.name == "parent_id"),
            "the key is not force-kept in the element"
        );
        assert!(matches!(fk_shape(&spec, &full), ValueShape::Int));
    }

    #[test]
    fn keys_are_literals_of_their_own_type() {
        assert_eq!(key_list(&[json!(7), json!(8)]), "7, 8");
        assert_eq!(key_list(&[json!("O'Hara")]), "'O''Hara'");
    }

    #[test]
    fn a_child_object_is_the_same_json_as_the_same_row_as_a_parent() {
        // The reason this module aggregates in Rust at all. If these ever differ,
        // a value means one thing as a document and another inside an array — and
        // MySQL's own JSON functions differ for exactly these types.
        let wide = TableSchema {
            columns: vec![
                Column {
                    name: "b".into(),
                    shape: ValueShape::Bytes,
                },
                Column {
                    name: "d".into(),
                    shape: ValueShape::Decimal,
                },
                Column {
                    name: "bt".into(),
                    shape: ValueShape::Bits,
                },
                Column {
                    name: "s".into(),
                    shape: ValueShape::Set(vec!["a".into(), "b".into()]),
                },
            ],
            pk_columns: vec!["b".into()],
        };
        let row = [
            Some(vec![0x00, 0xFF, 0x10]),
            Some(b"12.34".to_vec()),
            Some(vec![0x00, 0xFF]),
            Some(b"a,b".to_vec()),
        ];
        let (as_child, _) = catalog::build_document(&wide, &row);
        let (as_parent, _) = catalog::build_document(&wide, &row);
        assert_eq!(as_child, as_parent);
        // and the canonical forms MySQL's JSON_OBJECT would not have produced
        assert_eq!(as_child["b"], json!("AP8Q"));
        assert_eq!(as_child["d"], json!("12.34"));
        assert_eq!(as_child["bt"], json!(255));
        assert_eq!(as_child["s"], json!(["a", "b"]));
    }
}
