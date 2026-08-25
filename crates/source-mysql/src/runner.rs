//! MySQL binlog source streaming into a `core::ChangeEvent` channel.
//!
//! Failure policy: any source error terminates this task; the pipeline rebuilds
//! from the last checkpoint (binlog file + position). MySQL has no server-side
//! position tracking, so there is no feedback protocol — we reconnect from the
//! checkpointed position and replay duplicates harmlessly under idempotent
//! writes.

use crate::binlog::{self, RowsKind};
use crate::catalog::{self, SchemaCache, TableSchema};
use crate::connection::{MySqlConfig, MySqlConnection};
use anyhow::{Context as _, Result};
use pg2osync_core::event::{ChangeEvent, RowChange, RowKind, TransactionBoundary};
use pg2osync_core::lsn::Lsn;
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
    /// Resume coordinate; None starts from the server's current position.
    pub start_file: Option<String>,
    pub start_pos: u32,
    pub tls: pg2osync_tls::TlsSettings,
}

impl MySqlSourceConfig {
    fn connection(&self) -> MySqlConfig {
        MySqlConfig {
            host: self.host.clone(),
            port: self.port,
            user: self.user.clone(),
            password: self.password.clone(),
            database: None,
            server_id: self.server_id,
            tls: self.tls.clone(),
        }
    }
}

/// A table registered by a TABLE_MAP event, with its resolved schema.
struct RegisteredTable {
    schema: String,
    table: String,
    meta: binlog::TableMeta,
    columns: Vec<String>,
    pk_columns: Vec<String>,
}

pub struct MySqlSource {
    cfg: MySqlSourceConfig,
}

/// Result of the pre-stream snapshot: where to start streaming from.
pub struct SnapshotStart {
    pub file: String,
    pub pos: u32,
}

impl MySqlSource {
    pub fn new(cfg: MySqlSourceConfig) -> Self {
        Self { cfg }
    }

    /// Open an administrative connection for schema queries and snapshots.
    pub async fn admin_connection(&self) -> Result<MySqlConnection> {
        MySqlConnection::connect(&self.cfg.connection())
            .await
            .context("mysql admin connection failed")
    }

    /// Verify server prerequisites (`log_bin`, `binlog_format`,
    /// `binlog_row_image`) and that every configured table is usable.
    pub async fn bootstrap(&self, admin: &mut MySqlConnection) -> Result<()> {
        catalog::check_prerequisites(admin).await?;
        for (schema, table) in &self.cfg.tables {
            let resolved = catalog::table_schema(admin, schema, table).await?;
            tracing::debug!(target: "pg2osync::source",
                "{schema}.{table}: {} columns, pk {:?}",
                resolved.columns.len(), resolved.pk_columns);
        }
        Ok(())
    }

    /// Read every configured table inside one repeatable-read snapshot and
    /// report the binlog coordinate the snapshot corresponds to.
    ///
    /// The position is taken *inside* the transaction: InnoDB establishes the
    /// read view at `START TRANSACTION WITH CONSISTENT SNAPSHOT`, so streaming
    /// from that coordinate can only re-deliver rows, never skip them.
    pub async fn snapshot(
        &self,
        admin: &mut MySqlConnection,
        tx: &tokio::sync::mpsc::Sender<ChangeEvent>,
    ) -> Result<SnapshotStart> {
        admin
            .query_text_rows("SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            .await?;
        admin
            .query_text_rows("START TRANSACTION WITH CONSISTENT SNAPSHOT")
            .await?;
        let (file, pos) = catalog::master_position(admin).await?;

        for (schema, table) in &self.cfg.tables {
            let resolved = catalog::table_schema(admin, schema, table).await?;
            let sql = format!(
                "SELECT {} FROM {}.{}",
                resolved
                    .columns
                    .iter()
                    .map(|c| catalog::quote_ident(&c.name))
                    .collect::<Vec<_>>()
                    .join(", "),
                catalog::quote_ident(schema),
                catalog::quote_ident(table)
            );
            let rows = admin
                .query_text_rows(&sql)
                .await
                .with_context(|| format!("snapshot read failed for {schema}.{table}"))?;
            let count = rows.len();
            for row in rows {
                let (doc, pk) = catalog::build_document(&resolved, &row);
                tx.send(ChangeEvent::Row(RowChange {
                    schema: schema.clone(),
                    table: table.clone(),
                    kind: RowKind::Insert { pk, doc },
                }))
                .await
                .context("change channel closed during snapshot")?;
            }
            // positionless boundary: flushes the batch without advancing any
            // checkpoint, since snapshot rows have no binlog coordinate
            tx.send(ChangeEvent::Transaction(TransactionBoundary::Commit {
                lsn: Lsn(0),
                commit_ts_micros: 0,
            }))
            .await
            .context("change channel closed during snapshot")?;
            tracing::info!(target: "pg2osync::source",
                "snapshot of {schema}.{table}: {count} rows");
        }
        admin.query_text_rows("COMMIT").await?;
        Ok(SnapshotStart { file, pos })
    }

    /// Stream committed row changes until error or shutdown signal fires.
    pub async fn stream(
        &mut self,
        tx: tokio::sync::mpsc::Sender<ChangeEvent>,
        shutdown: tokio::sync::watch::Receiver<bool>,
    ) -> Result<()> {
        let mut admin = self.admin_connection().await?;
        let mut schemas = SchemaCache::default();

        let mut conn = MySqlConnection::connect(&self.cfg.connection())
            .await
            .context("mysql connect failed")?;
        conn.negotiate_checksum().await?;
        // frequent enough that a caller waiting on a position is not left
        // waiting for unrelated traffic, cheap enough to be invisible
        conn.set_heartbeat_period(std::time::Duration::from_millis(100))
            .await?;

        let (mut current_file, start_pos) = match (&self.cfg.start_file, self.cfg.start_pos) {
            (Some(f), p) if p > 0 => (f.clone(), p),
            _ => catalog::master_position(&mut admin).await?,
        };

        conn.send_binlog_dump(&current_file, start_pos).await?;
        tracing::info!(target: "pg2osync::source",
            "mysql binlog dump from {current_file}@{start_pos}");

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
                anyhow::bail!(
                    "server error: {}",
                    String::from_utf8_lossy(pkt.get(9..).unwrap_or(&[]))
                );
            }
            let ev = &pkt[1..];
            let Some(h) = binlog::parse_header(ev) else {
                continue;
            };
            let body = &ev[19..ev.len().saturating_sub(checksum_len)];

            match h.event_type {
                binlog::T_FORMAT_DESCRIPTION => {
                    // the FDE carries the checksum algorithm in its own last
                    // five bytes, so it has to be read before any stripping:
                    // handing it the stripped body reads a padding byte
                    // instead and settles on a length that then mangles the
                    // tail of every later event
                    let (_, clen) = binlog::parse_fde(&ev[19..]);
                    checksum_len = clen;
                }
                binlog::T_ROTATE => {
                    if let Some(rot) = binlog::parse_rotate(body) {
                        tracing::info!(target: "pg2osync::source",
                            "binlog rotated to {}", rot.next_file);
                        current_file = rot.next_file;
                        // table ids are only valid within one binlog file
                        registered.clear();
                    }
                }
                binlog::T_QUERY => {
                    // DDL changes column layout; drop cached schemas so the
                    // next TABLE_MAP resolves fresh names from the catalog
                    if let Some(q) = binlog::parse_query(body) {
                        let sql = q.sql.trim_start().to_uppercase();
                        if sql.starts_with("ALTER") || sql.starts_with("RENAME") {
                            tracing::info!(target: "pg2osync::source",
                                "DDL observed; invalidating schema cache");
                            schemas.invalidate_all();
                            registered.clear();
                        }
                    }
                }
                binlog::T_HEARTBEAT => {
                    // carries no data; its value is the position it reports
                    tx.send(ChangeEvent::Transaction(TransactionBoundary::Commit {
                        lsn: Lsn(catalog::position_token(&current_file, h.log_pos)),
                        commit_ts_micros: 0,
                    }))
                    .await
                    .context("change channel closed")?;
                }
                binlog::T_XID => {
                    // XID closes a transaction: this is the only point where a
                    // position may be acknowledged
                    tx.send(ChangeEvent::Transaction(TransactionBoundary::Commit {
                        lsn: Lsn(catalog::position_token(&current_file, h.log_pos)),
                        commit_ts_micros: i64::from(h.timestamp) * 1_000_000 - 946_684_800_000_000,
                    }))
                    .await
                    .context("change channel closed")?;
                }
                binlog::T_TABLE_MAP => {
                    let (tid, meta, opt) = binlog::parse_table_map(body)?;
                    if !self.is_configured(&meta.schema, &meta.name) {
                        tracing::trace!(target: "pg2osync::source",
                            "ignoring unconfigured table {}.{}", meta.schema, meta.name);
                        continue;
                    }
                    // TABLE_MAP optional metadata carries names only when the
                    // server runs binlog_row_metadata=FULL; information_schema
                    // is the portable source of truth
                    let resolved: TableSchema = schemas
                        .get(&mut admin, &meta.schema, &meta.name)
                        .await?
                        .clone();
                    let columns = if opt.column_names.len() == meta.columns.len() {
                        opt.column_names.clone()
                    } else {
                        resolved.column_names()
                    };
                    if columns.len() != meta.columns.len() {
                        anyhow::bail!(
                            "{}.{}: binlog reports {} columns but the catalog has {}; \
                             a concurrent DDL is in flight — restart to resynchronize",
                            meta.schema,
                            meta.name,
                            meta.columns.len(),
                            columns.len()
                        );
                    }
                    registered.insert(
                        tid,
                        RegisteredTable {
                            schema: meta.schema.clone(),
                            table: meta.name.clone(),
                            meta,
                            columns,
                            pk_columns: resolved.pk_columns.clone(),
                        },
                    );
                }
                t if binlog::rows_kind_for_type(t).is_some() => {
                    let mut tb = [0u8; 8];
                    tb[..6].copy_from_slice(&body[..6]);
                    let table_id = u64::from_le_bytes(tb);
                    let Some(rt) = registered.get(&table_id) else {
                        continue;
                    };
                    let set = binlog::parse_rows(h.event_type, body, &rt.meta)?;
                    for row in &set.rows {
                        let change = build_change(rt, &set.kind, row)?;
                        tx.send(change).await.context("change channel closed")?;
                    }
                }
                _ => {}
            }
        }
    }

    fn is_configured(&self, schema: &str, table: &str) -> bool {
        self.cfg
            .tables
            .iter()
            .any(|(s, t)| s == schema && t == table)
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

/// Turn one decoded binlog row into a change event with named columns.
fn build_change(
    rt: &RegisteredTable,
    kind: &RowsKind,
    row: &binlog::RowsRow,
) -> Result<ChangeEvent> {
    // Deletes carry only the before-image; inserts and updates carry an after-
    // image whose values are the new row state.
    let (values, key_values) = match kind {
        RowsKind::Delete => {
            let before = row
                .before
                .as_ref()
                .or(row.after.as_ref())
                .ok_or_else(|| anyhow::anyhow!("delete event carries no row image"))?;
            (before, before)
        }
        _ => {
            let after = row
                .after
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("row event carries no after image"))?;
            (after, after)
        }
    };

    let doc = document(&rt.columns, values);
    let pk = primary_key(&rt.columns, key_values, &rt.pk_columns)?;

    let kind = match kind {
        RowsKind::Write => RowKind::Insert {
            pk,
            doc: Value::Object(doc),
        },
        RowsKind::Update => RowKind::Update {
            pk,
            // binlog_row_image=FULL is a startup prerequisite, so the before
            // image is present and carries the key the row is moving away from
            previous_pk: row
                .before
                .as_ref()
                .and_then(|before| primary_key(&rt.columns, before, &rt.pk_columns).ok()),
            doc: Value::Object(doc),
            // binlog_row_image=FULL is a startup prerequisite, so every
            // column of the new row is present: nothing to complete
            unchanged_toast_columns: vec![],
        },
        RowsKind::Delete => RowKind::Delete { pk },
    };
    Ok(ChangeEvent::Row(RowChange {
        schema: rt.schema.clone(),
        table: rt.table.clone(),
        kind,
    }))
}

fn document(columns: &[String], values: &[Option<Value>]) -> Map<String, Value> {
    let mut doc = Map::new();
    for (name, value) in columns.iter().zip(values) {
        // a column absent from the row image is unknown, not null: leaving it
        // out keeps the previously indexed value intact on upsert
        if let Some(v) = value {
            doc.insert(name.clone(), v.clone());
        }
    }
    doc
}

fn primary_key(
    columns: &[String],
    values: &[Option<Value>],
    pk_columns: &[String],
) -> Result<Value> {
    let mut key = Map::new();
    for pk in pk_columns {
        let idx = columns
            .iter()
            .position(|c| c == pk)
            .ok_or_else(|| anyhow::anyhow!("primary key column {pk} not present in binlog row"))?;
        let value = values
            .get(idx)
            .and_then(|v| v.clone())
            .ok_or_else(|| anyhow::anyhow!("primary key column {pk} missing from row image"))?;
        key.insert(pk.clone(), value);
    }
    Ok(if key.len() == 1 {
        key.into_iter().next().expect("single entry").1
    } else {
        Value::Object(key)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn table() -> RegisteredTable {
        RegisteredTable {
            schema: "shop".into(),
            table: "orders".into(),
            meta: binlog::TableMeta {
                schema: "shop".into(),
                name: "orders".into(),
                columns: vec![],
            },
            columns: vec!["id".into(), "total".into()],
            pk_columns: vec!["id".into()],
        }
    }

    #[test]
    fn update_uses_after_image_and_named_columns() {
        let row = binlog::RowsRow {
            before: Some(vec![Some(json!(1)), Some(json!("5.00"))]),
            after: Some(vec![Some(json!(1)), Some(json!("9.00"))]),
        };
        let ChangeEvent::Row(change) = build_change(&table(), &RowsKind::Update, &row).unwrap()
        else {
            panic!("expected a row change");
        };
        assert_eq!(change.pk(), &json!(1));
        let RowKind::Update {
            doc, previous_pk, ..
        } = &change.kind
        else {
            panic!("expected an update");
        };
        assert_eq!(doc["total"], json!("9.00"));
        assert_eq!(previous_pk, &Some(json!(1)), "the key did not move");
    }

    #[test]
    fn a_changed_key_reports_where_the_row_moved_from() {
        let row = binlog::RowsRow {
            before: Some(vec![Some(json!(1)), Some(json!("5.00"))]),
            after: Some(vec![Some(json!(2)), Some(json!("5.00"))]),
        };
        let ChangeEvent::Row(change) = build_change(&table(), &RowsKind::Update, &row).unwrap()
        else {
            panic!("expected a row change");
        };
        let RowKind::Update {
            pk, previous_pk, ..
        } = &change.kind
        else {
            panic!("expected an update");
        };
        assert_eq!(pk, &json!(2), "the row lives at its new key");
        assert_eq!(previous_pk, &Some(json!(1)));
    }

    #[test]
    fn delete_takes_its_key_from_the_before_image() {
        let row = binlog::RowsRow {
            before: Some(vec![Some(json!(42)), Some(json!("1.00"))]),
            after: None,
        };
        let ChangeEvent::Row(change) = build_change(&table(), &RowsKind::Delete, &row).unwrap()
        else {
            panic!("expected a row change");
        };
        assert!(matches!(change.kind, RowKind::Delete { .. }));
        assert_eq!(change.pk(), &json!(42));
    }

    #[test]
    fn missing_key_column_is_an_error() {
        let row = binlog::RowsRow {
            before: None,
            after: Some(vec![None, Some(json!("1.00"))]),
        };
        assert!(build_change(&table(), &RowsKind::Write, &row).is_err());
    }
}
