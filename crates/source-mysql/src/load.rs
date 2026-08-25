//! Initial load for the MySQL source.
//!
//! A table is read in primary-key chunks, each chunk its own statement, rather
//! than every table inside one `START TRANSACTION WITH CONSISTENT SNAPSHOT`.
//! What makes that safe is not snapshot consistency: it is that the binlog
//! coordinate is read *before* the first chunk and the stream runs from it, so
//! anything a chunk missed or read stale is still in the binlog and is replayed
//! onto an idempotent, versioned write.
//!
//! Chunks are read *in key order*, which is the opposite of the PostgreSQL
//! loader and for the opposite reason: InnoDB's clustered index is the table, so
//! `WHERE key > cursor ORDER BY key LIMIT n` is an ordered walk of the rows
//! themselves. Nothing has to be sampled in advance, and each chunk's last key
//! is the next chunk's cursor — which makes the resume point exact rather than
//! dependent on a sample being reproducible.

use crate::catalog::{self, TableSchema};
use crate::connection::MySqlConnection;
use crate::typemap::ValueShape;
use anyhow::{Context as _, Result};
use pg2osync_core::checkpoint::StreamId;
use pg2osync_core::event::{ChangeEvent, RowChange, RowKind, TransactionBoundary};
use pg2osync_core::load::{LoadCursor, LoadProgress, load_progress_key};
use pg2osync_core::lsn::Lsn;
use pg2osync_core::sink::Sink;
use tokio::sync::mpsc::Sender;
use tokio::sync::watch;

/// Rows between synthetic commit boundaries. A large chunk would otherwise
/// buffer entirely in the engine before the first flush.
const ROWS_PER_BOUNDARY: u64 = 5_000;

/// Stream every configured table into the engine channel, chunk by chunk.
#[allow(clippy::too_many_arguments)]
pub async fn run(
    conn: &mut MySqlConnection,
    tables: &[(String, String)],
    chunk_rows: u64,
    tx: &Sender<ChangeEvent>,
    sink: &dyn Sink,
    stream: &StreamId,
    mut load_done: watch::Receiver<u64>,
) -> Result<()> {
    // No explicit transaction, and READ COMMITTED so that an implicit one
    // cannot outlive its statement either. A read view held for the length of a
    // load is what blocks InnoDB's purge, and the purge blocking is not a
    // slowdown — it stops.
    conn.query_text_rows("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")
        .await
        .context("cannot set the load session's isolation level")?;

    let chunk_rows = chunk_rows.max(1);
    // One counter for the whole load: a mark only has to be increasing, and a
    // single sequence keeps the wait condition a comparison.
    let mut mark: u64 = 0;
    let mut progress_keys: Vec<String> = Vec::new();

    for (schema, table) in tables {
        let qualified = format!("{schema}.{table}");
        let progress_key = load_progress_key(stream, &qualified);
        progress_keys.push(progress_key.clone());
        let resolved = catalog::table_schema(conn, schema, table).await?;

        let stored = sink
            .read_state(&progress_key)
            .await?
            .as_ref()
            .and_then(LoadProgress::from_doc);
        let mut cursor: Option<Vec<String>> = match stored {
            Some(p) if p.finished => {
                tracing::info!(target: "pg2osync::load",
                    "{qualified} was fully loaded by an earlier run; skipping");
                continue;
            }
            Some(LoadProgress {
                cursor: LoadCursor::After(key),
                ..
            }) if !key.is_empty() => {
                tracing::info!(target: "pg2osync::load",
                    "resuming the load of {qualified} after key {}", key.join(", "));
                Some(key)
            }
            _ => None,
        };

        // A key we cannot carry from one chunk to the next cannot be a cursor,
        // so such a table is read in one statement. One statement is still one
        // read view, which is the whole difference from a snapshot spanning
        // every table.
        let unusable_key: Vec<&str> = resolved
            .pk_columns
            .iter()
            .filter(|name| {
                !resolved
                    .columns
                    .iter()
                    .find(|c| &c.name == *name)
                    .is_some_and(|c| cursor_safe(&c.shape))
            })
            .map(String::as_str)
            .collect();
        let chunked = unusable_key.is_empty();
        if !chunked {
            tracing::warn!(target: "pg2osync::load",
                "{qualified} is read in one statement: its key column(s) {} cannot carry \
                 a cursor from one chunk to the next",
                unusable_key.join(", "));
            cursor = None;
        }

        let started = std::time::Instant::now();
        let mut count: u64 = 0;
        let mut chunks: u64 = 0;
        loop {
            // The position read before the chunk becomes the version of every
            // document the chunk produces. A change committed after this point
            // necessarily has a higher position, so it wins at the target
            // whichever order the two arrive in — which is what allows the load
            // to run beside the stream. It is a version only: these rows still
            // never advance the checkpoint.
            let (file, pos) = catalog::master_position(conn).await?;
            let version = Some(catalog::position_token(&file, pos));
            let sql = chunk_statement(
                schema,
                table,
                &resolved,
                cursor.as_deref(),
                chunked.then_some(chunk_rows),
            );

            let mut rows = conn
                .text_query(&sql)
                .await
                .with_context(|| format!("initial load of {qualified} failed reading a chunk"))?;
            let mut in_chunk: u64 = 0;
            let mut last: Option<Vec<String>> = None;
            while let Some(row) = rows.next().await? {
                in_chunk += 1;
                count += 1;
                if chunked {
                    last = key_literals(&resolved, &row);
                }
                let (doc, pk) = catalog::build_document(&resolved, &row);
                tx.send(ChangeEvent::Row(RowChange {
                    schema: schema.clone(),
                    table: table.clone(),
                    kind: RowKind::Insert { pk, doc },
                    version,
                }))
                .await
                .context("engine closed during the initial load")?;
                if count.is_multiple_of(ROWS_PER_BOUNDARY) {
                    send_boundary(tx).await?;
                }
            }
            chunks += 1;
            send_boundary(tx).await?;

            // Strict order: rows, then the mark, then the progress document. A
            // crash anywhere in it can only lose the chunk and redo it, which an
            // idempotent write makes free — the reverse order would claim a
            // chunk that was never written.
            mark += 1;
            tx.send(ChangeEvent::LoadMark(mark))
                .await
                .map_err(|_| anyhow::anyhow!("engine closed during the initial load"))?;
            load_done
                .wait_for(|written| *written >= mark)
                .await
                .map_err(|_| anyhow::anyhow!("engine stopped before the chunk was written"))?;

            // A short chunk is the last one: there is nothing past its final
            // key that this statement could have left behind. A full chunk that
            // yielded no cursor cannot be continued, and treating that as the
            // end would silently load part of a table.
            let finished = !chunked || in_chunk < chunk_rows;
            if !finished && last.is_none() {
                anyhow::bail!(
                    "{qualified}: a full chunk produced no key to continue from, \
                     so the rest of the table cannot be read"
                );
            }
            cursor = last;
            let progress = LoadProgress {
                cursor: LoadCursor::After(cursor.clone().unwrap_or_default()),
                finished,
            };
            sink.write_state(&progress_key, &progress.to_doc()).await?;
            if finished {
                break;
            }
        }
        let secs = started.elapsed().as_secs_f64();
        tracing::info!(target: "pg2osync::load",
            "loaded {count} rows from {qualified} in {secs:.1}s \
             (~{:.0} rows/s) over {chunks} chunk(s)",
            count as f64 / secs.max(f64::EPSILON));
    }

    // Nothing left to resume, so nothing should claim otherwise on the next
    // start. Left behind, a finished document is merely a wasted read.
    for key in progress_keys {
        if let Err(e) = sink.clear_state(&key).await {
            tracing::warn!(target: "pg2osync::load",
                "could not remove load progress {key}: {e}");
        }
    }
    Ok(())
}

/// A boundary carrying `Lsn(0)`: it flushes the batch without advancing the
/// checkpoint, because loaded rows have no binlog coordinate of their own.
async fn send_boundary(tx: &Sender<ChangeEvent>) -> Result<()> {
    tx.send(ChangeEvent::Transaction(TransactionBoundary::Commit {
        lsn: Lsn(0),
        commit_ts_micros: 0,
    }))
    .await
    .map_err(|_| anyhow::anyhow!("engine closed during the initial load"))
}

/// The statement for one chunk: everything after `cursor`, in key order.
fn chunk_statement(
    schema: &str,
    table: &str,
    resolved: &TableSchema,
    cursor: Option<&[String]>,
    limit: Option<u64>,
) -> String {
    let columns = resolved
        .columns
        .iter()
        .map(|c| catalog::quote_ident(&c.name))
        .collect::<Vec<_>>()
        .join(", ");
    let key: Vec<String> = resolved
        .pk_columns
        .iter()
        .map(|c| catalog::quote_ident(c))
        .collect();
    let mut sql = format!(
        "SELECT {columns} FROM {}.{}",
        catalog::quote_ident(schema),
        catalog::quote_ident(table)
    );
    if let Some(values) = cursor.filter(|v| v.len() == key.len()) {
        sql.push_str(" WHERE ");
        sql.push_str(&after_predicate(&key, values));
    }
    if limit.is_some() {
        sql.push_str(&format!(" ORDER BY {}", key.join(", ")));
    }
    if let Some(n) = limit {
        sql.push_str(&format!(" LIMIT {n}"));
    }
    sql
}

/// `key > values`, expanded.
///
/// The row constructor `(a, b) > (x, y)` says exactly this and plans as
/// `type: index` where the expansion plans as `type: range`
/// ([MySQL bug #111952](https://bugs.mysql.com/bug.php?id=111952), closed as
/// not-a-bug with a worklog opened instead), which turns every chunk into a
/// scan of everything before it. No `IS NOT NULL` guard accompanies the
/// comparison even though MySQL sorts NULLs first: a `PRIMARY KEY` column is
/// `NOT NULL` whether it was declared that way or not.
fn after_predicate(key: &[String], values: &[String]) -> String {
    let mut terms: Vec<String> = Vec::with_capacity(key.len());
    for i in 0..key.len() {
        let mut term: Vec<String> = (0..i)
            .map(|j| format!("{} = {}", key[j], values[j]))
            .collect();
        term.push(format!("{} > {}", key[i], values[i]));
        terms.push(format!("({})", term.join(" AND ")));
    }
    terms.join(" OR ")
}

/// The primary key of one row as SQL literals, in key order.
fn key_literals(resolved: &TableSchema, row: &[Option<Vec<u8>>]) -> Option<Vec<String>> {
    resolved
        .pk_columns
        .iter()
        .map(|name| {
            let idx = resolved.columns.iter().position(|c| &c.name == name)?;
            let raw = row.get(idx)?.as_deref()?;
            Some(literal(&resolved.columns[idx].shape, raw))
        })
        .collect()
}

/// A key value as a SQL literal that means exactly what the value means.
///
/// Bytes go in hexadecimal rather than quoted: a `varbinary` column compares
/// under the binary collation, which is what `x'…'` gets compared as, and its
/// bytes are not text to quote in the first place. A number is emitted bare so
/// the plan is not left depending on MySQL converting a string constant back —
/// and it is validated rather than trusted, because a value from the database is
/// otherwise interpolated straight into a statement.
fn literal(shape: &ValueShape, raw: &[u8]) -> String {
    if matches!(shape, ValueShape::Bytes) {
        let hex: String = raw.iter().map(|b| format!("{b:02x}")).collect();
        return format!("x'{hex}'");
    }
    let text = String::from_utf8_lossy(raw);
    let digits = text.strip_prefix('-').unwrap_or(&text);
    let (int, frac) = digits.split_once('.').unwrap_or((digits, "0"));
    let numeric = matches!(
        shape,
        ValueShape::Int | ValueShape::Decimal | ValueShape::Bits
    ) && !int.is_empty()
        && !frac.is_empty()
        && int.bytes().all(|b| b.is_ascii_digit())
        && frac.bytes().all(|b| b.is_ascii_digit());
    if numeric {
        text.into_owned()
    } else {
        catalog::quote_str(&text)
    }
}

/// Whether a key column can carry the cursor from one chunk to the next.
///
/// It has to survive the round trip through a SQL literal and order the same way
/// the chunk's `ORDER BY` does. A `float` cannot: its decimal text is a rounding
/// of the stored value. A `json` column cannot be indexed by MySQL at all, and a
/// `set` orders by its bitmask rather than by anything a caller would recognise.
fn cursor_safe(shape: &ValueShape) -> bool {
    matches!(
        shape,
        ValueShape::Int
            | ValueShape::Decimal
            | ValueShape::Bits
            | ValueShape::Text
            | ValueShape::Bytes
            | ValueShape::Enum(_)
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::Column;

    fn schema(pk: &[&str]) -> TableSchema {
        TableSchema {
            columns: ["id", "tenant", "name"]
                .iter()
                .map(|n| Column {
                    name: (*n).into(),
                    shape: if *n == "id" {
                        ValueShape::Int
                    } else {
                        ValueShape::Text
                    },
                })
                .collect(),
            pk_columns: pk.iter().map(|s| (*s).to_string()).collect(),
        }
    }

    #[test]
    fn a_composite_cursor_is_expanded_never_a_row_constructor() {
        let sql = chunk_statement(
            "shop",
            "orders",
            &schema(&["id", "tenant"]),
            Some(&["5".into(), "'acme'".into()]),
            Some(1000),
        );
        assert!(
            !sql.contains("(`id`, `tenant`) >"),
            "a row constructor plans as type: index"
        );
        assert!(sql.contains("(`id` > 5) OR (`id` = 5 AND `tenant` > 'acme')"));
        assert!(sql.ends_with("ORDER BY `id`, `tenant` LIMIT 1000"));
    }

    #[test]
    fn the_first_chunk_has_no_cursor() {
        let sql = chunk_statement("shop", "orders", &schema(&["id"]), None, Some(10));
        assert!(!sql.contains("WHERE"));
        assert!(sql.contains("FROM `shop`.`orders` ORDER BY `id` LIMIT 10"));
    }

    #[test]
    fn an_unchunkable_table_is_one_plain_statement() {
        let sql = chunk_statement("shop", "orders", &schema(&["id"]), None, None);
        assert!(!sql.contains("LIMIT"), "{sql}");
        assert!(!sql.contains("ORDER BY"), "ordering it would buy nothing");
    }

    #[test]
    fn cursor_literals_are_bare_only_when_they_are_numbers() {
        assert_eq!(literal(&ValueShape::Int, b"42"), "42");
        assert_eq!(literal(&ValueShape::Decimal, b"-1.75"), "-1.75");
        assert_eq!(literal(&ValueShape::Text, b"2024-01-01"), "'2024-01-01'");
        assert_eq!(literal(&ValueShape::Text, b"O'Hara"), "'O''Hara'");
        // never bare, whatever it looks like
        assert_eq!(
            literal(&ValueShape::Text, b"1; DROP TABLE t"),
            "'1; DROP TABLE t'"
        );
        assert_eq!(literal(&ValueShape::Int, b"inf"), "'inf'");
    }

    #[test]
    fn a_binary_key_travels_as_hex_not_as_quoted_text() {
        // its bytes are not text to quote, and x'..' compares under the binary
        // collation, which is the one the column orders by
        assert_eq!(
            literal(&ValueShape::Bytes, &[0x00, 0xFF, 0x10]),
            "x'00ff10'"
        );
        assert_eq!(literal(&ValueShape::Bytes, b""), "x''");
        assert!(cursor_safe(&ValueShape::Bytes), "so it can be chunked");
    }

    #[test]
    fn the_cursor_is_the_last_row_of_the_chunk() {
        let row = [
            Some(b"7".to_vec()),
            Some(b"acme".to_vec()),
            Some(b"x".to_vec()),
        ];
        assert_eq!(
            key_literals(&schema(&["id", "tenant"]), &row),
            Some(vec!["7".to_string(), "'acme'".to_string()])
        );
    }
}
