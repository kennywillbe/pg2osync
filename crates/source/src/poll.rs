//! Polling fallback source: for managed PostgreSQL instances where logical
//! replication cannot be enabled.
//!
//! Requires a monotonically increasing timestamp column per table (config:
//! `poll_column`). A hard `DELETE` leaves nothing to poll and cannot be seen
//! here at all; a soft delete can, which is what `soft_delete` is for.
//!
//! No source position exists to checkpoint, so the caller re-runs the backfill
//! on every start; at-least-once semantics make the replay harmless.

use crate::error::{Context as _, Result, SourceError};
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
            let sql = watermark_query(t);
            let wm: Option<String> = client
                .query_one(&sql, &[])
                .await
                .catalog_ctx(|| {
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
                let sql = page_query(t, last.is_some(), self.cfg.page_size);
                let rows = match &last {
                    Some(w) => client.query(&sql, &[w]).await,
                    None => client.query(&sql, &[]).await,
                }
                .catalog_ctx(|| {
                    format!(
                        "poll query failed for {} (needs column {})",
                        t.qualified, t.poll_column
                    )
                })?;

                let page = rows
                    .into_iter()
                    .map(|row| PolledRow {
                        doc: row.get(0),
                        watermark: row.get(1),
                        deleted: row.get(2),
                    })
                    .collect();
                let cycle = plan_cycle(t, last.as_deref(), page);
                for change in cycle.changes {
                    tx.send(ChangeEvent::Row(change))
                        .await
                        .map_err(|_| SourceError::ChannelClosed)?;
                    sent_any = true;
                }
                if let Some(m) = cycle.watermark {
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
                .map_err(|_| SourceError::ChannelClosed)?;
            }
            tokio::time::sleep(Duration::from_secs(self.cfg.interval_secs.max(1))).await;
        }
    }
}

/// One row as the page query returns it.
struct PolledRow {
    doc: Value,
    /// The row's own watermark value, in the text form comparison uses.
    watermark: Option<String>,
    deleted: Option<bool>,
}

/// What a fetched page turns into: the changes to emit, and the watermark to
/// carry into the next cycle when it actually moved.
struct Cycle {
    changes: Vec<pg2osync_core::event::RowChange>,
    watermark: Option<String>,
}

/// The startup query: where the watermark begins, so the backfill's rows are
/// not immediately re-sent.
fn watermark_query(t: &PollTable) -> String {
    format!(
        "SELECT max({col})::text FROM {tbl}",
        col = quote_ident(&t.poll_column),
        tbl = qualify(&t.qualified)
    )
}

/// One cycle's query for a table, bound to the previous watermark as `$1`.
///
/// The table's row filter is deliberately not here. A row that has left it has
/// to keep arriving so the engine can turn it into the delete it now is;
/// pushing the predicate in would simply stop reporting the row, which is the
/// hole `soft_delete` exists to fill.
fn page_query(t: &PollTable, has_watermark: bool, page_size: i64) -> String {
    // strict greater-than: rows sharing the boundary timestamp replay as
    // harmless duplicates under idempotent _id
    let filter = if has_watermark {
        format!("WHERE {}::text > $1", quote_ident(&t.poll_column))
    } else {
        String::new()
    };
    // the predicate is evaluated inside the subquery, where the row's own
    // columns are in scope and typed
    let deleted = t.soft_delete.as_deref().unwrap_or("false");
    format!(
        "SELECT to_jsonb(t) - 'pg2osync_deleted', t.{col}::text, \
                t.pg2osync_deleted FROM (\
            SELECT *, ({deleted}) AS pg2osync_deleted \
            FROM {tbl} {filter} ORDER BY {col} LIMIT {limit}\
         ) t",
        col = quote_ident(&t.poll_column),
        tbl = qualify(&t.qualified),
        limit = page_size.max(1),
    )
}

/// Turn a fetched page into the changes it means and the watermark it leaves.
fn plan_cycle(t: &PollTable, last: Option<&str>, page: Vec<PolledRow>) -> Cycle {
    let (schema, table) = split_qualified(&t.qualified);
    let mut max_seen = last.map(str::to_string);
    let mut changes = Vec::with_capacity(page.len());
    for row in page {
        if let Some(w) = row.watermark
            && max_seen.as_deref().is_none_or(|m| w.as_str() > m)
        {
            max_seen = Some(w);
        }
        let pk = extract_pk(&row.doc, &t.pk_columns);
        let kind = if row.deleted.unwrap_or(false) {
            // the polled row is the before-image: a derived id can be removed
            // under the same name it was written under
            RowKind::Delete {
                pk,
                before: Some(row.doc),
            }
        } else {
            RowKind::Insert { pk, doc: row.doc }
        };
        changes.push(pg2osync_core::event::RowChange {
            schema: schema.to_string(),
            table: table.to_string(),
            kind,
            // polling reconstructs state, it does not observe a log: there is
            // no position a row became visible at
            version: None,
        });
    }
    Cycle {
        changes,
        watermark: max_seen.filter(|m| Some(m.as_str()) != last),
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

    fn table() -> PollTable {
        PollTable {
            qualified: "public.users".into(),
            poll_column: "updated_at".into(),
            pk_columns: vec!["id".into()],
            soft_delete: None,
        }
    }

    fn row(id: i64, watermark: &str, deleted: Option<bool>) -> PolledRow {
        PolledRow {
            doc: json!({"id": id, "updated_at": watermark}),
            watermark: Some(watermark.into()),
            deleted,
        }
    }

    #[test]
    fn the_first_cycle_reads_the_whole_table_ordered_by_the_poll_column() {
        assert_eq!(
            watermark_query(&table()),
            "SELECT max(\"updated_at\")::text FROM \"public\".\"users\""
        );
        let sql = page_query(&table(), false, 500);
        assert!(!sql.contains("WHERE"), "no watermark, no boundary: {sql}");
        assert!(sql.contains("ORDER BY \"updated_at\" LIMIT 500"));
    }

    #[test]
    fn a_later_cycle_takes_rows_strictly_past_the_watermark() {
        let sql = page_query(&table(), true, 0);
        // strictly greater: a row *at* the watermark was already sent, and the
        // idempotent _id makes re-sending it harmless rather than necessary
        assert!(sql.contains("WHERE \"updated_at\"::text > $1"), "{sql}");
        assert!(!sql.contains(">="), "{sql}");
        // a page size of zero would fetch nothing forever
        assert!(sql.contains("LIMIT 1"), "{sql}");
    }

    #[test]
    fn the_soft_delete_predicate_rides_inside_the_page_query() {
        let mut t = table();
        t.soft_delete = Some("deleted_at IS NOT NULL".into());
        let sql = page_query(&t, false, 10);
        assert!(
            sql.contains("(deleted_at IS NOT NULL) AS pg2osync_deleted"),
            "{sql}"
        );
        assert!(!sql.contains("WHERE"), "the predicate marks, never filters");
    }

    #[test]
    fn a_row_the_predicate_marks_becomes_a_delete_carrying_its_before_image() {
        let cycle = plan_cycle(
            &table(),
            None,
            vec![
                row(1, "2026-01-01", Some(false)),
                row(2, "2026-01-02", Some(true)),
            ],
        );
        assert!(matches!(cycle.changes[0].kind, RowKind::Insert { .. }));
        match &cycle.changes[1].kind {
            RowKind::Delete { pk, before } => {
                assert_eq!(*pk, json!(2));
                assert!(before.is_some(), "the delete carries the polled row");
            }
            other => panic!("expected a delete, got {other:?}"),
        }
        assert_eq!(cycle.changes[0].schema, "public");
        assert_eq!(cycle.changes[0].table, "users");
        assert!(
            cycle.changes.iter().all(|c| c.version.is_none()),
            "polling has no position to version by"
        );
    }

    #[test]
    fn an_append_only_polled_row_carries_a_null_key() {
        let mut t = table();
        t.pk_columns.clear();
        let cycle = plan_cycle(&t, None, vec![row(1, "2026-01-01", None)]);
        match &cycle.changes[0].kind {
            RowKind::Insert { pk, .. } => assert_eq!(*pk, Value::Null),
            other => panic!("expected an insert, got {other:?}"),
        }
    }

    #[test]
    fn the_watermark_only_moves_when_a_row_is_past_it() {
        let advanced = plan_cycle(
            &table(),
            Some("2026-01-01"),
            vec![row(1, "2026-01-03", None), row(2, "2026-01-02", None)],
        );
        assert_eq!(advanced.watermark.as_deref(), Some("2026-01-03"));

        // an empty page leaves the watermark where it was, so the next cycle
        // asks the same question rather than rewinding
        let idle = plan_cycle(&table(), Some("2026-01-01"), vec![]);
        assert_eq!(idle.watermark, None);
        assert!(idle.changes.is_empty());
    }

    #[test]
    fn identifiers_are_quoted_and_escaped() {
        assert_eq!(qualify("public.users"), "\"public\".\"users\"");
        assert_eq!(qualify("weird\"name"), "\"public\".\"weird\"\"name\"");
    }
}
