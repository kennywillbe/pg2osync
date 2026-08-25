//! Consistent initial load for the PostgreSQL source.
//!
//! Reads every configured table inside one repeatable-read snapshot taken
//! *after* the replication slot exists, so the streaming phase can only
//! re-deliver rows, never skip them. Duplicates are harmless under
//! at-least-once delivery with idempotent writes.

use anyhow::{Context as _, Result, bail};
use pg2osync_core::event::{ChangeEvent, RowKind, TransactionBoundary};
use pg2osync_core::lsn::Lsn;
use pg2osync_source::children::ChildSpec;
use std::collections::HashMap;
use tokio::sync::mpsc::Sender;

use crate::config::AppConfig;

/// Column metadata needed to rebuild documents outside the WAL stream.
#[derive(Clone)]
pub struct ColMeta {
    pub name: String,
    pub type_oid: u32,
    pub is_pk: bool,
}

/// Rows between synthetic commit boundaries. Large tables would otherwise
/// buffer entirely in the engine before the first flush.
const ROWS_PER_BOUNDARY: u64 = 5_000;

pub async fn columns_of(
    client: &tokio_postgres::Client,
    qualified_table: &str,
) -> Result<Vec<ColMeta>> {
    let rows = client
        .query(
            r#"
            SELECT a.attname AS name,
                   a.atttypid::int4 AS oid,
                   EXISTS(
                       SELECT 1 FROM pg_index i
                       WHERE i.indrelid = a.attrelid
                         AND i.indisprimary
                         AND a.attnum = ANY(i.indkey)
                   ) AS is_pk
            FROM pg_attribute a
            WHERE a.attrelid = ($1::text)::regclass
              AND a.attnum > 0
              AND NOT a.attisdropped
            ORDER BY a.attnum
            "#,
            &[&qualified_table],
        )
        .await?;
    Ok(rows
        .into_iter()
        .map(|r| ColMeta {
            name: r.get("name"),
            type_oid: r.get::<_, i32>(1) as u32,
            is_pk: r.get("is_pk"),
        })
        .collect())
}

/// Build the `COPY` statement for one table, embedding each child collection
/// as a pre-aggregated JSON column.
///
/// Aggregating child rows once and hash-joining beats fetching them per parent
/// by orders of magnitude, and unlike a correlated lateral it does not depend
/// on the child's foreign key being indexed — which a general-purpose tool
/// cannot assume.
fn copy_statement(qualified_table: &str, cols: &[ColMeta], children: &[ChildSpec]) -> String {
    let mut selected: Vec<String> = cols
        .iter()
        .map(|c| format!("p.{}::text", quote_ident(&c.name)))
        .collect();
    let mut joins = String::new();

    for (i, child) in children.iter().enumerate() {
        let alias = format!("c{i}");
        selected.push(format!("COALESCE({alias}.agg, '[]'::jsonb)::text"));
        // the key is compared in its own type: a ::text cast on either side
        // makes the index unusable and turns this into a sequential scan
        joins.push_str(&format!(
            " LEFT JOIN (SELECT {fk} AS k, jsonb_agg(to_jsonb(t)) AS agg \
             FROM {child_table} t GROUP BY {fk}) {alias} ON {alias}.k = p.{parent_key}",
            fk = quote_ident(&child.foreign_key),
            child_table = qualify(&child.qualified()),
            parent_key = quote_ident(&child.parent_column),
        ));
    }

    format!(
        "COPY (SELECT {} FROM {} p{}) TO STDOUT (FORMAT text)",
        selected.join(", "),
        qualify(qualified_table),
        joins
    )
}

/// A child collection reaches the document as one JSON column, so it can ride
/// along in the same `COPY` as an ordinary column of type `jsonb`.
fn child_column(child: &ChildSpec) -> ColMeta {
    const JSONB_OID: u32 = 3802;
    ColMeta {
        name: child.field.clone(),
        type_oid: JSONB_OID,
        is_pk: false,
    }
}

/// Stream every configured table into the engine channel.
pub async fn run(
    cfg: &AppConfig,
    source_url: &str,
    tls: &pg2osync_source::tls::TlsSettings,
    admin: &tokio_postgres::Client,
    children: &HashMap<(String, String), Vec<ChildSpec>>,
    tx: Sender<ChangeEvent>,
) -> Result<()> {
    // a dedicated connection holds the snapshot so the admin connection stays
    // free for catalog work
    let mut reader = pg2osync_source::tls::connect(tls, source_url)
        .await
        .context("backfill connection failed")?;
    let snapshot = reader.transaction().await?;
    snapshot
        .execute(
            "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY",
            &[],
        )
        .await?;

    for tbl in cfg.sync.values() {
        let (schema, table) = split_qualified(&tbl.table);
        let mut cols = columns_of(admin, &tbl.table).await?;
        let child_specs: &[ChildSpec] = children
            .get(&(schema.to_string(), table.to_string()))
            .map(Vec::as_slice)
            .unwrap_or_default();

        for child in child_specs {
            // shadowing a real column would produce a document that quietly
            // disagrees with the row it came from
            if cols.iter().any(|c| c.name == child.field) {
                bail!(
                    "[sync] child field {:?} collides with a column of {}; \
                     choose another field name",
                    child.field,
                    tbl.table
                );
            }
            cols.push(child_column(child));
        }
        let sql = copy_statement(
            &tbl.table,
            &cols[..cols.len() - child_specs.len()],
            child_specs,
        );

        let started = std::time::Instant::now();
        let mut count: u64 = 0;
        let copy_stream = snapshot.copy_out(&sql).await?;
        use futures::StreamExt;
        let mut pending = String::new();
        let mut stream = std::pin::pin!(copy_stream);
        while let Some(chunk) = stream.next().await {
            let chunk: bytes::Bytes = chunk.context("copy stream failed")?;
            pending.push_str(&String::from_utf8_lossy(&chunk));
            while let Some(pos) = pending.find('\n') {
                let line: String = pending.drain(..pos + 1).collect();
                let line = line.trim_end_matches('\n');
                if line.is_empty() {
                    continue;
                }
                count += 1;
                let fields: Vec<Option<Vec<u8>>> =
                    split_copy_line(line).iter().map(unescape_copy).collect();
                // child collections arrive as ordinary jsonb columns of this
                // very row, so nothing extra is fetched per parent
                let change = build_change(schema, table, &cols, &fields)?;
                if tx.send(ChangeEvent::Row(change)).await.is_err() {
                    bail!("engine closed during backfill");
                }
                if count.is_multiple_of(ROWS_PER_BOUNDARY) {
                    send_boundary(&tx).await?;
                }
            }
        }
        send_boundary(&tx).await?;
        let secs = started.elapsed().as_secs_f64();
        tracing::info!(target: "pg2osync::backfill",
            "backfilled {} rows from {} in {:.1}s (~{:.0} rows/s)",
            count, tbl.table, secs, count as f64 / secs.max(f64::EPSILON));
    }
    drop(snapshot);
    Ok(())
}

/// A boundary carrying `Lsn(0)`: it flushes the batch without advancing the
/// checkpoint, because backfill rows have no WAL position of their own.
async fn send_boundary(tx: &Sender<ChangeEvent>) -> Result<()> {
    tx.send(ChangeEvent::Transaction(TransactionBoundary::Commit {
        lsn: Lsn(0),
        commit_ts_micros: 0,
    }))
    .await
    .map_err(|_| anyhow::anyhow!("engine closed during backfill"))
}

fn build_change(
    schema: &str,
    table: &str,
    cols: &[ColMeta],
    fields: &[Option<Vec<u8>>],
) -> Result<pg2osync_core::event::RowChange> {
    let mut doc = serde_json::Map::new();
    let mut pk_map = serde_json::Map::new();
    for (i, meta) in cols.iter().enumerate() {
        let value = pg2osync_source::typemap::convert(
            meta.type_oid,
            fields.get(i).and_then(|f| f.as_deref()),
        )
        .map_err(|e| anyhow::anyhow!("column {}: {e}", meta.name))?;
        if meta.is_pk {
            pk_map.insert(meta.name.clone(), value.clone());
        }
        doc.insert(meta.name.clone(), value);
    }
    if pk_map.is_empty() {
        bail!(
            "table {schema}.{table} has no primary key; pg2osync needs one to \
             derive a stable document id"
        );
    }
    let pk = if pk_map.len() == 1 {
        pk_map.into_iter().next().expect("single entry").1
    } else {
        serde_json::Value::Object(pk_map)
    };
    Ok(pg2osync_core::event::RowChange {
        schema: schema.to_string(),
        table: table.to_string(),
        kind: RowKind::Insert {
            pk,
            doc: serde_json::Value::Object(doc),
        },
    })
}

/// COPY TEXT field unescaping; `\N` is the NULL marker.
fn unescape_copy(field: &String) -> Option<Vec<u8>> {
    if field == "\\N" {
        return None;
    }
    let mut out = Vec::with_capacity(field.len());
    let mut chars = field.chars();
    while let Some(c) = chars.next() {
        if c != '\\' {
            let mut buf = [0u8; 4];
            out.extend_from_slice(c.encode_utf8(&mut buf).as_bytes());
            continue;
        }
        match chars.next() {
            Some('t') => out.push(b'\t'),
            Some('n') => out.push(b'\n'),
            Some('r') => out.push(b'\r'),
            Some('\\') => out.push(b'\\'),
            Some(other) => {
                let mut buf = [0u8; 4];
                out.extend_from_slice(other.encode_utf8(&mut buf).as_bytes());
            }
            None => {}
        }
    }
    Some(out)
}

fn split_copy_line(line: &str) -> Vec<String> {
    let mut fields = vec![];
    let mut cur = String::new();
    let mut escaped = false;
    for c in line.chars() {
        if escaped {
            cur.push('\\');
            cur.push(c);
            escaped = false;
            continue;
        }
        match c {
            '\\' => escaped = true,
            '\t' => fields.push(std::mem::take(&mut cur)),
            other => cur.push(other),
        }
    }
    fields.push(cur);
    fields
}

pub fn split_qualified(qualified: &str) -> (&str, &str) {
    qualified.split_once('.').unwrap_or(("public", qualified))
}

fn qualify(qualified: &str) -> String {
    let (s, t) = split_qualified(qualified);
    format!("{}.{}", quote_ident(s), quote_ident(t))
}

fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn copy_line_splitting_respects_escapes() {
        let fields = split_copy_line("1\\tx\ta\\nb\t\\N");
        assert_eq!(fields, vec!["1\\tx", "a\\nb", "\\N"]);
        assert_eq!(unescape_copy(&fields[0]), Some(b"1\tx".to_vec()));
        assert_eq!(unescape_copy(&fields[2]), None);
    }

    fn child(field: &str, table: &str, fk: &str, parent_key: &str) -> ChildSpec {
        ChildSpec::new(table, field, fk, parent_key).expect("qualified")
    }

    fn col(name: &str) -> ColMeta {
        ColMeta {
            name: name.into(),
            type_oid: 25,
            is_pk: false,
        }
    }

    #[test]
    fn a_table_without_children_reads_straight_through() {
        let sql = copy_statement("public.users", &[col("id"), col("name")], &[]);
        assert_eq!(
            sql,
            concat!(
                "COPY (SELECT p.\"id\"::text, p.\"name\"::text ",
                "FROM \"public\".\"users\" p) TO STDOUT (FORMAT text)"
            )
        );
    }

    #[test]
    fn a_child_collection_becomes_one_aggregated_column() {
        let sql = copy_statement(
            "public.customers",
            &[col("id")],
            &[child("orders", "public.orders", "customer_id", "id")],
        );
        assert!(
            sql.contains("COALESCE(c0.agg, '[]'::jsonb)::text"),
            "a parent with no children must still get an empty array: {sql}"
        );
        assert!(sql.contains("GROUP BY \"customer_id\""), "{sql}");
        assert!(
            sql.contains("c0.k = p.\"id\""),
            "the key is compared in its own type, or the index goes unused: {sql}"
        );
        assert!(
            !sql.contains("::text = "),
            "no cast may appear on either side of the join: {sql}"
        );
    }

    #[test]
    fn several_collections_each_get_their_own_join() {
        let sql = copy_statement(
            "public.customers",
            &[col("id")],
            &[
                child("orders", "public.orders", "customer_id", "id"),
                child("tickets", "support.tickets", "cust", "id"),
            ],
        );
        assert!(sql.contains("c0.k = p.\"id\""), "{sql}");
        assert!(sql.contains("c1.k = p.\"id\""), "{sql}");
        assert!(sql.contains("\"support\".\"tickets\""), "{sql}");
        assert_eq!(sql.matches("LEFT JOIN").count(), 2);
    }

    #[test]
    fn identifiers_are_quoted_everywhere_they_appear() {
        let sql = copy_statement(
            "public.we\"ird",
            &[col("od\"d")],
            &[child("kids", "public.ch\"ild", "fk\"y", "pk\"y")],
        );
        assert!(sql.contains("\"we\"\"ird\""), "{sql}");
        assert!(sql.contains("\"od\"\"d\""), "{sql}");
        assert!(sql.contains("\"fk\"\"y\""), "{sql}");
    }

    #[test]
    fn multibyte_values_survive_unescaping() {
        assert_eq!(
            unescape_copy(&"çğü".to_string()),
            Some("çğü".as_bytes().to_vec())
        );
    }
}
