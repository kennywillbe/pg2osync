//! Polling fallback source (v0.6): for managed PostgreSQL instances where
//! logical replication cannot be enabled.
//!
//! Requires an `updated_at`-style timestamp column per table (config:
//! `poll_column`). Upserts propagate; DELETES CANNOT BE DETECTED in this mode
//! — documented limitation, users needing deletes must use WAL mode.

use anyhow::{Context as _, Result};
use pg2osync_core::event::{ChangeEvent, RowKind};
use std::collections::HashMap;
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct PollSourceConfig {
    pub url: String,
    /// (schema, table) -> poll timestamp column
    pub tables: Vec<(String, String)>,
    pub interval_secs: u64,
}

pub struct PollSource {
    cfg: PollSourceConfig,
}

impl PollSource {
    pub fn new(cfg: PollSourceConfig) -> Self {
        Self { cfg }
    }

    /// Stream changes by polling. Watermarks start at MAX(updated_at) captured
    /// on startup, so startup itself does not re-index history.
    pub async fn stream(
        &mut self,
        tx: tokio::sync::mpsc::Sender<ChangeEvent>,
        shutdown: tokio::sync::watch::Receiver<bool>,
    ) -> Result<()> {
        let (client, conn) = tokio_postgres::connect(&self.cfg.url, tokio_postgres::NoTls).await?;
        tokio::spawn(async move {
            let _ = conn.await;
        });

        let mut watermarks: HashMap<String, String> = HashMap::new();
        for (qualified, col) in &self.cfg.tables {
            let wm: Option<String> = client
                .query_opt(&format!("SELECT max({col}::text) FROM {qualified}"), &[])
                .await?
                .map(|r| r.get(0));
            if let Some(w) = wm {
                watermarks.insert(qualified.clone(), w);
            }
        }

        loop {
            if *shutdown.borrow() {
                return Ok(());
            }
            for (qualified, col) in &self.cfg.tables {
                let last = watermarks.get(qualified).cloned().unwrap_or_default();
                // strict greater-than: overlapping timestamps replay harmless
                // duplicates under idempotent _id
                let sql = format!(
                    "SELECT to_jsonb(t) FROM (\
                        SELECT * FROM {qualified} WHERE {col} > $1::text \
                        ORDER BY {col} LIMIT 5000\
                     ) t"
                );
                let rows = client.query(&sql, &[&last]).await.with_context(|| {
                    format!("poll query failed for {qualified} (needs column {col})")
                })?;
                let mut max_seen = last.clone();
                for row in rows {
                    let doc: serde_json::Value = row.get(0);
                    // pk extraction relies on the primary key field being named
                    // "id" or present; composite keys unsupported in poll mode
                    let pk = doc.get("id").cloned().unwrap_or(serde_json::Value::Null);
                    max_seen = doc
                        .get(col.as_str())
                        .and_then(|v| v.as_str())
                        .map(String::from)
                        .filter(|s| s.as_str() > max_seen.as_str())
                        .unwrap_or_else(|| max_seen.clone());
                    tx.send(ChangeEvent::Row(pg2osync_core::event::RowChange {
                        schema: qualified.split('.').next().unwrap_or("public").into(),
                        table: qualified.rsplit('.').next().unwrap_or(qualified).into(),
                        kind: RowKind::Insert { pk, doc },
                    }))
                    .await
                    .context("change channel closed")?;
                }
                if max_seen != last {
                    watermarks.insert(qualified.clone(), max_seen);
                }
            }
            tokio::time::sleep(Duration::from_secs(self.cfg.interval_secs.max(1))).await;
        }
    }
}
