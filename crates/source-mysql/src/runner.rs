//! MySQL binlog source streaming into core::ChangeEvent channel.
//!
//! Failure policy: any source error terminates this task; the pipeline rebuilds
//! from the last checkpoint (binlog file+position). MySQL has no server-side
//! position tracking, so there is no feedback protocol — we reconnect from the
//! checkpointed position and replay duplicates harmlessly under idempotent
//! writes.

use crate::binlog::{self, RowsKind};
use crate::connection::{MySqlConfig, MySqlConnection};
use anyhow::{Context as _, Result};
use pg2osync_core::event::ChangeEvent;
use serde_json::{Map, Value};
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct MySqlSourceConfig {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub server_id: u32,
    /// (schema, table) pairs to replicate.
    pub tables: Vec<(String, String)>,
    /// Resume position; None = SHOW MASTER STATUS at startup.
    pub start_file: Option<String>,
    pub start_pos: u32,
}

/// Per-table info resolved at TABLE_MAP time.
#[derive(Debug, Clone)]
pub struct TableInfo {
    pub schema: String,
    pub table: String,
}

struct RegisteredTable {
    schema: String,
    table: String,
    meta: binlog::TableMeta,
}

pub struct MySqlSource {
    cfg: MySqlSourceConfig,
}

impl MySqlSource {
    pub fn new(cfg: MySqlSourceConfig) -> Self {
        Self { cfg }
    }

    /// Stream committed row changes until error or shutdown signal fires.
    ///
    /// Checkpoint semantics: MySQL has no server-side position tracking.
    /// The caller persists (file, pos) after durable flushes and passes them
    /// back via `start_file`/`start_pos` on restart.
    pub async fn stream(
        &mut self,
        tx: tokio::sync::mpsc::Sender<ChangeEvent>,
        shutdown: tokio::sync::watch::Receiver<bool>,
    ) -> Result<()> {
        let mysql_cfg = MySqlConfig {
            host: self.cfg.host.clone(),
            port: self.cfg.port,
            user: self.cfg.user.clone(),
            password: self.cfg.password.clone(),
            database: None,
            server_id: self.cfg.server_id,
        };
        let mut conn = MySqlConnection::connect(&mysql_cfg)
            .await
            .context("mysql connect failed")?;

        conn.negotiate_checksum().await?;

        let (start_file, start_pos) = match (&self.cfg.start_file, self.cfg.start_pos) {
            (Some(f), p) if p > 0 => (f.clone(), p),
            _ => {
                let vals = conn.query_text_row("SHOW MASTER STATUS").await?;
                let f = vals.first().cloned().flatten().unwrap_or_default();
                let p: u32 = vals
                    .get(1)
                    .and_then(|s| s.as_ref().and_then(|s| s.parse().ok()))
                    .unwrap_or(4);
                (f, p)
            }
        };

        conn.send_binlog_dump(&start_file, start_pos).await?;
        tracing::info!(target: "pg2osync::source",
            "mysql binlog dump from {start_file}@{start_pos}");

        let mut registered: HashMap<u64, RegisteredTable> = HashMap::new();
        let mut checksum_len: usize = 4;

        loop {
            if *shutdown.borrow() {
                tracing::info!(target: "pg2osync::source", "shutdown requested");
                return Ok(());
            }
            let pkt = tokio::select! {
                pkt = conn.read_packet() => pkt.context("binlog stream failed")?,
                _ = wait_shutdown(&shutdown) => return Ok(()),
            };
            if pkt.first() == Some(&0xFF) {
                anyhow::bail!("server error: {}", String::from_utf8_lossy(&pkt[9..]));
            }
            let ev = &pkt[1..];
            let Some(h) = binlog::parse_header(ev) else {
                continue;
            };
            let body = &ev[19..ev.len().saturating_sub(checksum_len)];

            match h.event_type {
                binlog::T_FORMAT_DESCRIPTION => {
                    let (_, clen) = binlog::parse_fde(body);
                    checksum_len = clen;
                }
                binlog::T_ROTATE => {
                    if let Some(rot) = binlog::parse_rotate(body) {
                        tracing::debug!(target: "pg2osync::source", "rotate {}", rot.next_file);
                    }
                }
                binlog::T_TABLE_MAP => {
                    let (tid, meta, _opt) = binlog::parse_table_map(body)?;
                    let key = format!("{}.{}", meta.schema, meta.name);
                    let configured = self.cfg.tables.iter().any(|(s, t)| {
                        format!("{s}.{t}") == key
                            || s == &meta.schema && t == &meta.name
                            || format!("{}.{}", s, t) == key
                    });
                    if configured {
                        registered.insert(
                            tid,
                            RegisteredTable {
                                schema: meta.schema.clone(),
                                table: meta.name.clone(),
                                meta,
                            },
                        );
                    } else {
                        tracing::debug!(target: "pg2osync::source",
                            "ignoring unconfigured table {key}");
                    }
                }
                t if binlog::rows_kind_for_type(t).is_some() => {
                    let mut tb = [0u8; 8];
                    tb[..6].copy_from_slice(&body[..6]);
                    let table_id = u64::from_le_bytes(tb);
                    let Some(rt) = registered.get_mut(&table_id) else {
                        continue;
                    };
                    let set = binlog::parse_rows(h.event_type, body, checksum_len, &rt.meta)?;
                    for row in &set.rows {
                        let change = build_change(&rt.schema, &rt.table, &set.kind, row)?;
                        tx.send(change).await.context("change channel closed")?;
                    }
                }
                _ => {}
            }
        }
    }
}

async fn wait_shutdown(shutdown: &tokio::sync::watch::Receiver<bool>) {
    let mut rx = shutdown.clone();
    while !*rx.borrow() {
        if rx.changed().await.is_err() {
            return;
        }
    }
}

fn build_change(
    schema: &str,
    table: &str,
    kind: &RowsKind,
    row: &crate::binlog::RowsRow,
) -> Result<ChangeEvent> {
    use pg2osync_core::event::RowChange;

    let vals = row
        .after
        .as_ref()
        .or(row.before.as_ref())
        .ok_or_else(|| anyhow::anyhow!("row has no images"))?;

    let mut doc = Map::new();
    for (i, v) in vals.iter().enumerate() {
        if let Some(v) = v {
            doc.insert(format!("col{i}"), v.clone());
        }
    }
    let pk = doc.values().next().cloned().unwrap_or(Value::Null);

    let kind = match kind {
        RowsKind::Write => pg2osync_core::event::RowKind::Insert {
            pk: pk.clone(),
            doc: Value::Object(doc),
        },
        RowsKind::Update => pg2osync_core::event::RowKind::Update {
            pk: pk.clone(),
            doc: Value::Object(doc),
            unchanged_toast_columns: vec![],
        },
        RowsKind::Delete => pg2osync_core::event::RowKind::Delete { pk },
    };

    Ok(ChangeEvent::Row(RowChange {
        schema: schema.to_string(),
        table: table.to_string(),
        kind,
    }))
}
