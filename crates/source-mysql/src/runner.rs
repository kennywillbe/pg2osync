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
        // Where the stream is in the current file, tracked rather than read off
        // each event. MariaDB leaves `end_log_pos` at 0 on every event inside a
        // transaction group, filling it in only on the GTID event that opens the
        // group and the XID that closes it: a group's final position is not
        // known until the group is written, and not needing it is what lets the
        // checksums be computed in advance. `binlog_legacy_event_pos` restores
        // the old behaviour and is documented as costing binlog scalability, so
        // tracking the position here is the honest side of that trade. MySQL has
        // no such setting and fills every event in.
        let mut current_pos = start_pos;

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
            let end_pos = match h.event_type {
                // Reports where the stream is without being part of it, and is
                // not in the file at all — so its size must never be counted.
                binlog::T_HEARTBEAT => current_pos.max(h.log_pos),
                // Sent at the head of every dump, describing a file we may have
                // resumed into the middle of: its offset is near that file's
                // start and says nothing about where we are.
                binlog::T_FORMAT_DESCRIPTION | binlog::T_ROTATE => current_pos,
                // Inside a MariaDB transaction group, where nothing states a
                // position. A stated one is authoritative wherever it appears:
                // taking it back is what keeps a counted group from drifting
                // past its own end.
                _ if h.log_pos == 0 => current_pos.saturating_add(h.event_size),
                _ => h.log_pos,
            };
            current_pos = end_pos;

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
                    // The server opens every dump with a rotate naming the file
                    // being resumed, which has moved nothing: acting on it would
                    // reset the position to that event's own and throw away
                    // table registrations that are still valid.
                    if let Some(rot) = binlog::parse_rotate(body)
                        && rot.next_file != current_file
                    {
                        tracing::info!(target: "pg2osync::source",
                            "binlog rotated to {}", rot.next_file);
                        current_file = rot.next_file;
                        current_pos = rot.position as u32;
                        // table ids are only valid within one binlog file
                        registered.clear();
                    }
                }
                binlog::T_QUERY => {
                    if let Some(q) = binlog::parse_query(body) {
                        // TRUNCATE is logged as a statement rather than as row
                        // events, so without reading it here the target would
                        // keep every document of a table the source emptied
                        if let Some((schema, table)) = binlog::truncated_table(&q.sql, &q.database)
                        {
                            if self.is_configured(&schema, &table) {
                                tx.send(ChangeEvent::TableTruncated {
                                    schema,
                                    table,
                                    version: Some(catalog::position_token(&current_file, end_pos)),
                                })
                                .await
                                .context("change channel closed")?;
                                continue;
                            }
                            // a truncate we decline to act on is worth naming:
                            // silence here reads exactly like a decoding fault
                            tracing::debug!(target: "pg2osync::source",
                                "TRUNCATE of unconfigured table {schema}.{table} ignored");
                        }
                        if let Some((schema, table)) = binlog::dropped_table(&q.sql, &q.database)
                            && self.is_configured(&schema, &table)
                        {
                            // clearing the index would be presumptuous — the
                            // table may be about to be recreated — but leaving
                            // it stale without saying so would be worse
                            tracing::warn!(target: "pg2osync::source",
                                "{schema}.{table} was dropped; its index still holds \
                                 the documents it had and will not be updated again");
                            continue;
                        }
                        // a statement that reached here changed something we
                        // did not act on; naming it is what makes an
                        // unrecognised form diagnosable instead of silent
                        tracing::debug!(target: "pg2osync::source",
                            "statement not acted on, db={:?}: {:?}",
                            q.database,
                            q.sql.chars().take(60).collect::<String>());
                        // DDL changes column layout; drop cached schemas so the
                        // next TABLE_MAP resolves fresh names from the catalog
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
                        lsn: Lsn(catalog::position_token(&current_file, end_pos)),
                        commit_ts_micros: 0,
                    }))
                    .await
                    .context("change channel closed")?;
                }
                binlog::T_XID => {
                    // XID closes a transaction: this is the only point where a
                    // position may be acknowledged
                    tx.send(ChangeEvent::Transaction(TransactionBoundary::Commit {
                        lsn: Lsn(catalog::position_token(&current_file, end_pos)),
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
                    // The row image says nothing about whether a string column
                    // holds characters or bytes, nor what an enum ordinal means.
                    // Handing the decoder the declared shape is what keeps a
                    // streamed value equal to the one the load read.
                    let mut meta = meta;
                    for (col, resolved) in meta.columns.iter_mut().zip(&resolved.columns) {
                        col.shape = Some(resolved.shape.clone());
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
                    let version = catalog::position_token(&current_file, end_pos);
                    for row in &set.rows {
                        let change = build_change(rt, &set.kind, row, version)?;
                        tx.send(change).await.context("change channel closed")?;
                    }
                }
                binlog::T_PARTIAL_UPDATE_ROWS => {
                    // the setting that produces these is refused at startup, so
                    // reaching one means it was turned on underneath us
                    anyhow::bail!(
                        "the server sent a partial JSON update, which is not decoded here; \
                         set binlog_row_value_options to the empty string"
                    );
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
    version: u64,
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
        // The row event's own end position, which is finer than the commit's:
        // two updates of one row inside a transaction order correctly against
        // each other, and a later transaction is always higher.
        version: Some(version),
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
        let ChangeEvent::Row(change) =
            build_change(&table(), &RowsKind::Update, &row, 900).unwrap()
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
        let ChangeEvent::Row(change) =
            build_change(&table(), &RowsKind::Update, &row, 900).unwrap()
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
        let ChangeEvent::Row(change) =
            build_change(&table(), &RowsKind::Delete, &row, 900).unwrap()
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
        assert!(build_change(&table(), &RowsKind::Write, &row, 900).is_err());
    }
}
