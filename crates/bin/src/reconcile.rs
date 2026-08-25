//! Walk an index against its source and remove documents whose row is gone.
//!
//! Streaming replication is at-least-once and idempotent, so the index should
//! never drift — but "should never" is not a guarantee anyone can check. Poll
//! mode genuinely cannot see a hard delete, an incident can leave the two out
//! of step, and until now there was no tool that could say whether they were.
//!
//! Documents are compared by key rather than by content: only keys move
//! between the two sides, which is what makes this far cheaper than a reindex.

use anyhow::{Context as _, Result};
use pg2osync_core::sink::{DocumentOp, LsnOp, Sink};
use serde_json::Value;
use std::sync::Arc;

/// How many documents to read from the index, and check against the source,
/// at a time. Bounds both the query size and the memory this holds.
const PAGE: usize = 1000;

pub struct Table {
    pub qualified: String,
    pub index: String,
    pub key_column: String,
    pub soft_delete: Option<String>,
}

pub struct Report {
    pub scanned: usize,
    pub orphaned: Vec<String>,
}

/// Compare one index against one table.
///
/// `delete` decides whether anything is removed. Reporting is the default
/// everywhere it is called from: a misconfigured key column would otherwise
/// empty an index, and finding that out afterwards is not a recoverable
/// mistake.
pub async fn table(
    client: &tokio_postgres::Client,
    sink: &Arc<dyn Sink>,
    table: &Table,
    delete: bool,
) -> Result<Report> {
    let mut report = Report {
        scanned: 0,
        orphaned: Vec::new(),
    };
    let mut after: Option<Value> = None;

    loop {
        let page = sink
            .scan_keys(&table.index, &table.key_column, after.as_ref(), PAGE)
            .await
            .with_context(|| format!("cannot page index {}", table.index))?;
        if page.is_empty() {
            break;
        }
        after = page.last().map(|(_, key)| key.clone());
        report.scanned += page.len();

        let keys: Vec<String> = page.iter().map(|(_, key)| render_key(key)).collect();
        let live = existing_keys(client, table, &keys).await?;
        for (id, key) in &page {
            if !live.contains(&render_key(key)) {
                report.orphaned.push(id.clone());
            }
        }
    }

    if delete && !report.orphaned.is_empty() {
        let ops: Vec<LsnOp> = report
            .orphaned
            .iter()
            .map(|id| LsnOp {
                // reconciliation has no source position of its own, and must
                // never move the checkpoint
                lsn: pg2osync_core::lsn::Lsn(0),
                op: DocumentOp::Delete {
                    index: table.index.clone(),
                    id: id.clone(),
                },
            })
            .collect();
        sink.write(ops).await.context("deleting orphans failed")?;
    }
    Ok(report)
}

/// Which of these keys the source still has.
///
/// Compared as text: the key column may be an integer, a uuid or anything
/// else, and the index reports whatever it stored. A cast on the column costs
/// the index on it, which for a pass that runs occasionally is the right side
/// of that trade.
async fn existing_keys(
    client: &tokio_postgres::Client,
    table: &Table,
    keys: &[String],
) -> Result<std::collections::HashSet<String>> {
    // a soft-deleted row still exists, and must still count as gone
    let filter = match &table.soft_delete {
        Some(predicate) => format!(" AND NOT ({predicate})"),
        None => String::new(),
    };
    let sql = format!(
        "SELECT {key}::text FROM {tbl} WHERE {key}::text = ANY($1){filter}",
        key = quote_ident(&table.key_column),
        tbl = qualify(&table.qualified),
    );
    let rows = client
        .query(&sql, &[&keys])
        .await
        .with_context(|| format!("cannot read keys of {}", table.qualified))?;
    Ok(rows.iter().map(|r| r.get::<_, String>(0)).collect())
}

/// The index's form of a key as text, matching what `::text` produces.
fn render_key(key: &Value) -> String {
    match key {
        Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

fn qualify(qualified: &str) -> String {
    match qualified.split_once('.') {
        Some((schema, table)) => format!("{}.{}", quote_ident(schema), quote_ident(table)),
        None => quote_ident(qualified),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn keys_render_the_way_a_text_cast_does() {
        assert_eq!(render_key(&json!(42)), "42");
        assert_eq!(render_key(&json!("abc")), "abc", "no JSON quotes around it");
        assert_eq!(render_key(&json!("42")), "42");
    }

    #[test]
    fn identifiers_are_quoted() {
        assert_eq!(qualify("we\"ird.tbl"), "\"we\"\"ird\".\"tbl\"");
        assert_eq!(qualify("plain"), "\"plain\"");
    }
}
