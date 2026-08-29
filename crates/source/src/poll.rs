//! Polling fallback source: for managed PostgreSQL instances where logical
//! replication cannot be enabled.
//!
//! Requires a monotonically increasing timestamp column per table (config:
//! `poll_column`). A hard `DELETE` leaves nothing to poll and cannot be seen
//! here at all; a soft delete can, which is what `soft_delete` is for.
//!
//! No source position exists to checkpoint, so the caller re-runs the backfill
//! on every start; at-least-once semantics make the replay harmless.

use anyhow::{Context as _, Result};
use pg2osync_core::event::{ChangeEvent, RowKind, TransactionBoundary};
use pg2osync_core::lsn::Lsn;
use serde_json::Value;
use std::collections::HashMap;
use std::time::Duration;

/// One polled table.
#[derive(Debug, Clone)]
pub struct PollTable {
    /// Schema-qualified name, e.g. `public.users`.
    pub qualified: String,
    /// Timestamp column driving the watermark.
    pub poll_column: String,
    /// Primary-key columns; a single column yields a scalar `_id`.
    pub pk_columns: Vec<String>,
    /// SQL predicate marking a row as deleted, e.g. `deleted_at IS NOT NULL`.
    /// Evaluated by the database, since only it knows the column types.
    pub soft_delete: Option<String>,
}

#[derive(Debug, Clone)]
pub struct PollSourceConfig {
    pub url: String,
    pub tables: Vec<PollTable>,
    pub interval_secs: u64,
    pub tls: crate::tls::TlsSettings,
    /// Rows fetched per table per cycle; bounds memory and query time.
    pub page_size: i64,
}

pub struct PollSource {
    cfg: PollSourceConfig,
}

impl PollSource {
    pub fn new(cfg: PollSourceConfig) -> Self {
        Self { cfg }
    }

    /// Poll until the shutdown signal fires.
    ///
    /// Watermarks start at `MAX(poll_column)` captured on startup, so the
    /// backfill's rows are not immediately re-sent. Comparison happens on the
    /// column's text form: it avoids guessing the column type and is correct
    /// for ISO-8601 timestamps, which is what this mode requires.
    pub async fn stream(
        &mut self,
        tx: tokio::sync::mpsc::Sender<ChangeEvent>,
        shutdown: tokio::sync::watch::Receiver<bool>,
    ) -> Result<()> {
        let client = crate::tls::connect(&self.cfg.tls, &self.cfg.url).await?;

        let mut watermarks: HashMap<String, String> = HashMap::new();
        for t in &self.cfg.tables {
            let sql = format!(
                "SELECT max({col})::text FROM {tbl}",
                col = quote_ident(&t.poll_column),
                tbl = qualify(&t.qualified)
            );
            let wm: Option<String> = client
                .query_one(&sql, &[])
                .await
                .with_context(|| {
                    format!(
                        "poll column {}.{} is not readable",
                        t.qualified, t.poll_column
                    )
                })?
                .get(0);
            // an empty table has no watermark: the first cycle then reads
            // every row it finds, which is what a new table should do
            if let Some(w) = wm {
                watermarks.insert(t.qualified.clone(), w);
            }
        }

        loop {
            if *shutdown.borrow() {
                return Ok(());
            }
            let mut sent_any = false;
            for t in &self.cfg.tables {
                let last = watermarks.get(&t.qualified).cloned();
                // The table's row filter is deliberately not here. A row that
                // has left it has to keep arriving so the engine can turn it
                // into the delete it now is; pushing the predicate in would
                // simply stop reporting the row, which is the hole
                // `soft_delete` exists to fill.
                // strict greater-than: rows sharing the boundary timestamp
                // replay as harmless duplicates under idempotent _id
                let filter = match &last {
                    Some(_) => format!("WHERE {}::text > $1", quote_ident(&t.poll_column)),
                    None => String::new(),
                };
                // the predicate is evaluated inside the subquery, where the
                // row's own columns are in scope and typed
                let deleted = t.soft_delete.as_deref().unwrap_or("false");
                let sql = format!(
                    "SELECT to_jsonb(t) - 'pg2osync_deleted', t.{col}::text, \
                            t.pg2osync_deleted FROM (\
                        SELECT *, ({deleted}) AS pg2osync_deleted \
                        FROM {tbl} {filter} ORDER BY {col} LIMIT {limit}\
                     ) t",
                    col = quote_ident(&t.poll_column),
                    tbl = qualify(&t.qualified),
                    filter = filter,
                    limit = self.cfg.page_size.max(1),
                );
                let rows = match &last {
                    Some(w) => client.query(&sql, &[w]).await,
                    None => client.query(&sql, &[]).await,
                }
                .with_context(|| {
                    format!(
                        "poll query failed for {} (needs column {})",
                        t.qualified, t.poll_column
                    )
                })?;

                let mut max_seen = last.clone();
                for row in rows {
                    let doc: Value = row.get(0);
                    let wm: Option<String> = row.get(1);
                    if let Some(w) = wm
                        && max_seen.as_deref().is_none_or(|m| w.as_str() > m)
                    {
                        max_seen = Some(w);
                    }
                    let (schema, table) = split_qualified(&t.qualified);
                    let pk = extract_pk(&doc, &t.pk_columns);
                    let deleted: Option<bool> = row.get(2);
                    let kind = if deleted.unwrap_or(false) {
                        // the polled row is the before-image: a derived id can
                        // be removed under the same name it was written under
                        RowKind::Delete {
                            pk,
                            before: Some(doc),
                        }
                    } else {
                        RowKind::Insert { pk, doc }
                    };
                    tx.send(ChangeEvent::Row(pg2osync_core::event::RowChange {
                        schema: schema.to_string(),
                        table: table.to_string(),
                        kind,
                        // polling reconstructs state, it does not observe a log:
                        // there is no position a row became visible at
                        version: None,
                    }))
                    .await
                    .context("change channel closed")?;
                    sent_any = true;
                }
                if let Some(m) = max_seen
                    && Some(&m) != last.as_ref()
                {
                    watermarks.insert(t.qualified.clone(), m);
                }
            }
            if sent_any {
                // Lsn(0) marks a positionless boundary: it flushes the batch
                // without advancing any checkpoint, which poll mode has none of
                tx.send(ChangeEvent::Transaction(TransactionBoundary::Commit {
                    lsn: Lsn(0),
                    commit_ts_micros: 0,
                }))
                .await
                .context("change channel closed")?;
            }
            tokio::time::sleep(Duration::from_secs(self.cfg.interval_secs.max(1))).await;
        }
    }
}

/// Primary key as a scalar for single-column keys, object for composite ones.
fn extract_pk(doc: &Value, pk_columns: &[String]) -> Value {
    match pk_columns {
        [] => Value::Null,
        [one] => doc.get(one).cloned().unwrap_or(Value::Null),
        many => Value::Object(
            many.iter()
                .map(|c| (c.clone(), doc.get(c).cloned().unwrap_or(Value::Null)))
                .collect(),
        ),
    }
}

fn split_qualified(qualified: &str) -> (&str, &str) {
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
    use serde_json::json;

    #[test]
    fn pk_extraction_handles_scalar_and_composite() {
        let doc = json!({"id": 7, "tenant": "acme", "name": "ada"});
        assert_eq!(extract_pk(&doc, &["id".into()]), json!(7));
        assert_eq!(
            extract_pk(&doc, &["id".into(), "tenant".into()]),
            json!({"id": 7, "tenant": "acme"})
        );
        assert_eq!(extract_pk(&doc, &[]), Value::Null);
        assert_eq!(extract_pk(&doc, &["missing".into()]), Value::Null);
    }

    #[test]
    fn identifiers_are_quoted_and_escaped() {
        assert_eq!(qualify("public.users"), "\"public\".\"users\"");
        assert_eq!(qualify("weird\"name"), "\"public\".\"weird\"\"name\"");
    }
}
