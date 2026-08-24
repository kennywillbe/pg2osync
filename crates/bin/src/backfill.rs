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

/// Stream every configured table into the engine channel.
pub async fn run(
    cfg: &AppConfig,
    source_url: &str,
    admin: &tokio_postgres::Client,
    children: &HashMap<(String, String), Vec<ChildSpec>>,
    tx: Sender<ChangeEvent>,
) -> Result<()> {
    // a dedicated connection holds the snapshot so the admin connection stays
    // free for catalog work
    let (mut reader, reader_bg) = tokio_postgres::connect(source_url, tokio_postgres::NoTls)
        .await
        .context("backfill connection failed")?;
    tokio::spawn(async move {
        let _ = reader_bg.await;
    });
    let snapshot = reader.transaction().await?;
    snapshot
        .execute(
            "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY",
            &[],
        )
        .await?;

    for tbl in cfg.sync.values() {
        let (schema, table) = split_qualified(&tbl.table);
        let cols = columns_of(admin, &tbl.table).await?;
        let child_specs = children.get(&(schema.to_string(), table.to_string()));
        let select_cols: Vec<String> = cols
            .iter()
            .map(|c| format!("{}::text", quote_ident(&c.name)))
            .collect();
        let sql = format!(
            "COPY (SELECT {} FROM {}) TO STDOUT (FORMAT text)",
            select_cols.join(", "),
            qualify(&tbl.table)
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
                let mut change = build_change(schema, table, &cols, &fields)?;
                // children are attached here too: without this the initial
                // load would ship parents with empty nested arrays until the
                // parent row happens to be updated
                if let Some(specs) = child_specs {
                    let pk = change.pk().clone();
                    if let Some(doc) = change.doc_mut() {
                        pg2osync_source::children::attach_children(doc, &pk, specs, admin).await?;
                    }
                }
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

    #[test]
    fn multibyte_values_survive_unescaping() {
        assert_eq!(
            unescape_copy(&"çğü".to_string()),
            Some("çğü".as_bytes().to_vec())
        );
    }
}
