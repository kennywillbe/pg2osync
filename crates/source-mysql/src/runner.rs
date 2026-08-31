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
use crate::error::{Context as _, MySqlError, Result};
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
    /// The tables this stream admits and what its decoder needs to know about
    /// each of them, shared rather than owned: a reload puts a table on a
    /// running stream through this handle.
    pub tables: pg2osync_core::tables::SharedTables,
    /// Resume coordinate; None starts from the server's current position.
    pub start_file: Option<String>,
    pub start_pos: u32,
    pub tls: pg2osync_tls::TlsSettings,
    /// Child collections keyed by PARENT (schema, table).
    pub children: HashMap<(String, String), Vec<pg2osync_core::children::ChildSpec>>,
    /// Aggregates over a child table, keyed by PARENT (schema, table). An
    /// aggregate's table is watched exactly as a child's is, and reaches this
    /// runner through the same reverse routing.
    pub aggregates: HashMap<(String, String), Vec<pg2osync_core::aggregate::AggregateSpec>>,
    /// Reverse routing: CHILD (schema, table) -> parent (schema, table).
    pub child_parents: HashMap<(String, String), (String, String)>,
    /// Which transactions have been consumed, so a checkpoint can resume on a
    /// server that never had this one's binlog files. Shared with whoever
    /// renders the checkpoint position: the stream fills it, the checkpoint
    /// reads it as of the position being written.
    pub gtid: Option<std::sync::Arc<std::sync::Mutex<crate::gtid::GtidTracker>>>,
    /// The GTID position to ask the server to resume from, when there is one.
    pub gtid_resume: Option<crate::gtid::GtidPosition>,
    /// Added to every coordinate to make the ordering token and the document
    /// version. A failover moves to a different, often lower, coordinate space,
    /// and versions may only go up — so the generation lives here rather than
    /// being applied by whoever consumes the events.
    pub version_base: u64,
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

    fn is_append_only(&self, schema: &str, table: &str) -> bool {
        self.tables
            .snapshot()
            .append_only
            .contains(&(schema.to_string(), table.to_string()))
    }
}

/// A table registered by a TABLE_MAP event, with its resolved schema.
struct RegisteredTable {
    schema: String,
    table: String,
    meta: binlog::TableMeta,
    columns: Vec<String>,
    pk_columns: Vec<String>,
    append_only: bool,
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
            .connect_ctx(|| "mysql admin connection failed".into())
    }

    /// Verify server prerequisites (`log_bin`, `binlog_format`,
    /// `binlog_row_image`) and that every configured table is usable.
    pub async fn bootstrap(&self, admin: &mut MySqlConnection) -> Result<()> {
        catalog::check_prerequisites(admin).await?;
        for (schema, table) in &self.cfg.tables.snapshot().tables {
            let resolved =
                catalog::table_schema(admin, schema, table, self.cfg.is_append_only(schema, table))
                    .await?;
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
            .connect_ctx(|| "mysql connect failed".into())?;
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
        // Every token this stream emits — a version, a commit boundary, a
        // checkpoint position — is the coordinate plus the generation. Built in
        // one place because a coordinate that escaped without it would be a
        // version from the previous server's numbering, which the target
        // silently refuses.
        let version_base = self.cfg.version_base;
        let token_at =
            |file: &str, pos: u32| version_base.saturating_add(catalog::position_token(file, pos));

        // Which command to ask with is the server's decision, not ours: MySQL
        // takes the set in a dump command of its own, MariaDB has no such
        // command and switches on a session variable instead.
        let support = catalog::gtid_support(&mut admin).await?;
        let tracking = self.cfg.gtid.is_some();
        // What actually decided where the stream starts, which is what the log
        // has to say — a coordinate printed for a stream the GTID position
        // governed would send the next reader looking in the wrong place.
        let mut resumed_by = format!("{current_file}@{start_pos}");
        if support.mariadb && tracking {
            let resume = match &self.cfg.gtid_resume {
                // rendered from the parsed position, never the stored text
                Some(crate::gtid::GtidPosition::MariaDb(pos)) => Some(pos.to_text()),
                _ => None,
            };
            conn.set_maria_gtid_state(resume.as_deref()).await?;
            if let Some(pos) = &resume {
                resumed_by = format!("gtid {pos}");
            }
        }
        let by_gtid = match (&self.cfg.gtid_resume, support.mariadb, tracking) {
            (Some(crate::gtid::GtidPosition::MySql(set)), false, true) => set.encode(),
            _ => None,
        };
        match by_gtid {
            Some(sids) => {
                if let Some(position) = &self.cfg.gtid_resume {
                    resumed_by = format!("gtid {}", position.to_text());
                }
                conn.send_binlog_dump_gtid(&sids).await?;
            }
            // The ordinary command, which MariaDB also uses in GTID mode: the
            // coordinate goes over but the server ignores it once
            // `@slave_connect_state` is set, exactly as its own replica does.
            None => conn.send_binlog_dump(&current_file, start_pos).await?,
        }
        tracing::info!(target: "pg2osync::source", "binlog dump from {resumed_by}");

        let mut registered: HashMap<u64, RegisteredTable> = HashMap::new();
        let mut pending = pg2osync_core::children::Pending::default();
        let mut checksum_len: usize = 4;

        loop {
            if *shutdown.borrow() {
                tracing::info!(target: "pg2osync::source", "shutdown requested");
                return Ok(());
            }
            let pkt = tokio::select! {
                pkt = conn.read_packet() => pkt.connect_ctx(|| "binlog stream failed".into())?,
                _ = wait_shutdown(&shutdown) => return Ok(()),
            };
            if pkt.first() == Some(&0xFF) {
                return Err(MySqlError::protocol(format!(
                    "server error: {}",
                    String::from_utf8_lossy(pkt.get(9..).unwrap_or(&[]))
                )));
            }
            let ev = &pkt[1..];
            let Some(h) = binlog::parse_header(ev) else {
                continue;
            };
            let body = event_body(ev, h.event_type, checksum_len)?;
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
                    let (_, clen) = binlog::parse_fde(&ev[binlog::HEADER_LEN..]);
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
                                    version: Some(token_at(&current_file, end_pos)),
                                })
                                .await
                                .map_err(|_| MySqlError::ChannelClosed)?;
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
                binlog::T_GTID | binlog::T_MARIA_GTID if self.cfg.gtid.is_some() => {
                    // Recorded against the position of the group it opens, so a
                    // checkpoint written for an earlier position never claims
                    // this transaction. The group's own end position is not
                    // known yet; its opening one is enough, because a
                    // checkpoint at or past it means the group was written.
                    let token = token_at(&current_file, end_pos);
                    let tracker = self.cfg.gtid.as_ref().expect("checked");
                    let mut tracker = tracker.lock().expect("not poisoned");
                    if h.event_type == binlog::T_GTID {
                        match binlog::parse_mysql_gtid(body) {
                            Some((uuid, gno)) => tracker.record_mysql(token, uuid, gno),
                            None => tracker.mark_incomplete("a GTID event could not be read"),
                        }
                    } else {
                        match binlog::parse_maria_gtid(body) {
                            Some((domain, seq_no)) => {
                                tracker.record_mariadb(token, domain, h.server_id, seq_no)
                            }
                            None => tracker.mark_incomplete("a GTID event could not be read"),
                        }
                    }
                }
                binlog::T_ANONYMOUS_GTID if self.cfg.gtid.is_some() => {
                    self.cfg
                        .gtid
                        .as_ref()
                        .expect("checked")
                        .lock()
                        .expect("not poisoned")
                        .mark_incomplete("a transaction was written with no GTID");
                }
                binlog::T_GTID_TAGGED if self.cfg.gtid.is_some() => {
                    self.cfg
                        .gtid
                        .as_ref()
                        .expect("checked")
                        .lock()
                        .expect("not poisoned")
                        .mark_incomplete("a tagged GTID arrived, which this reader cannot decode");
                }
                binlog::T_HEARTBEAT => {
                    // carries no data; its value is the position it reports
                    tx.send(ChangeEvent::Transaction(TransactionBoundary::Commit {
                        lsn: Lsn(token_at(&current_file, end_pos)),
                        commit_ts_micros: 0,
                    }))
                    .await
                    .map_err(|_| MySqlError::ChannelClosed)?;
                }
                binlog::T_XID => {
                    // Everything held for this transaction resolves here, before
                    // the boundary that flushes the batch — one query per
                    // collection for the whole group rather than two per row.
                    flush_pending(
                        &self.cfg,
                        &mut pending,
                        &mut admin,
                        &tx,
                        Some(token_at(&current_file, end_pos)),
                    )
                    .await?;
                    // XID closes a transaction: this is the only point where a
                    // position may be acknowledged
                    tx.send(ChangeEvent::Transaction(TransactionBoundary::Commit {
                        lsn: Lsn(token_at(&current_file, end_pos)),
                        commit_ts_micros: i64::from(h.timestamp) * 1_000_000 - 946_684_800_000_000,
                    }))
                    .await
                    .map_err(|_| MySqlError::ChannelClosed)?;
                }
                binlog::T_TABLE_MAP => {
                    let (tid, meta, opt) = binlog::parse_table_map(body)?;
                    if !self.is_configured(&meta.schema, &meta.name) {
                        // A registration outlives the TABLE_MAP that made it,
                        // because the rows that follow are addressed by id
                        // alone. A table that stops being configured under the
                        // running stream — a reload dropping its section —
                        // would otherwise keep being decoded through the entry
                        // it left behind, so the entry goes with it.
                        registered.remove(&tid);
                        tracing::trace!(target: "pg2osync::source",
                            "ignoring unconfigured table {}.{}", meta.schema, meta.name);
                        continue;
                    }
                    // TABLE_MAP optional metadata carries names only when the
                    // server runs binlog_row_metadata=FULL; information_schema
                    // is the portable source of truth
                    let append_only = self.cfg.is_append_only(&meta.schema, &meta.name);
                    let mut resolved: TableSchema = schemas
                        .get(&mut admin, &meta.schema, &meta.name, append_only)
                        .await?
                        .clone();
                    let mut columns = column_names(&opt, &meta, &resolved);
                    if let Some(drift) = schemas.take_drift(&meta.schema, &meta.name) {
                        report_drift(&tx, &meta.schema, &meta.name, drift).await?;
                    }
                    if columns.len() != meta.columns.len() {
                        // The cached answer predates the statement this
                        // TABLE_MAP already describes. MySQL DDL is not
                        // transactional, so by the time a row event under the
                        // new shape reaches us the statement has committed and
                        // information_schema answers with that shape — reading
                        // it again is what turns a restart into a hiccup.
                        schemas.invalidate(&meta.schema, &meta.name);
                        resolved = schemas
                            .get(&mut admin, &meta.schema, &meta.name, append_only)
                            .await?
                            .clone();
                        let before = columns.len();
                        columns = column_names(&opt, &meta, &resolved);
                        let drift =
                            schemas
                                .take_drift(&meta.schema, &meta.name)
                                .unwrap_or(format!(
                                    "binlog reports {} columns, the catalog had {before}",
                                    meta.columns.len()
                                ));
                        report_drift(&tx, &meta.schema, &meta.name, drift).await?;
                    }
                    if columns.len() != meta.columns.len() {
                        // A shape a fresh catalog read still disagrees with is
                        // history, not a race: the stream is replaying from a
                        // checkpoint written before a DDL that has since
                        // committed, and no read can bring back the layout
                        // those rows were written under. Failing the attempt
                        // here wedged the pipeline — every reconnect resumed
                        // from the same checkpoint and reached the same event,
                        // so nothing after it was ever replicated again, which
                        // costs far more than the rows this skips.
                        tracing::warn!(target: "pg2osync::source",
                            "{}.{}: the binlog describes {} columns and the catalog has {}; \
                             these rows predate a DDL that has already committed and cannot \
                             be decoded, so they are skipped and the stream carries on",
                            meta.schema,
                            meta.name,
                            meta.columns.len(),
                            columns.len());
                        registered.remove(&tid);
                        continue;
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
                            append_only,
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
                    let version = token_at(&current_file, end_pos);
                    let table = (rt.schema.clone(), rt.table.clone());
                    // A child row is not a document: it names a parent to re-read.
                    // Naming it rather than emitting it is what lets a thousand
                    // children of one parent cost one query and one document.
                    if let Some(parent) = self.cfg.child_parents.get(&table).cloned() {
                        match child_spec(&self.cfg, &parent, &table) {
                            Some(spec) => {
                                let spec = spec.clone();
                                // Which half of a many-to-many relation this row is: the
                                // junction carries the parent's key, the child only its
                                // own, and the junction is asked for the rest at commit.
                                let through = spec
                                    .through
                                    .as_ref()
                                    .filter(|_| spec.schema == table.0 && spec.table == table.1);
                                for row in &set.rows {
                                    let column = match through {
                                        Some(through) => &through.child_key,
                                        None => &spec.foreign_key,
                                    };
                                    let key = row_column(rt, &table, row, column)?;
                                    match through {
                                        Some(_) => {
                                            pending.name_through(parent.clone(), &spec.field, key)
                                        }
                                        None => pending.name_parent(parent.clone(), key),
                                    }
                                }
                            }
                            // A row of a table an aggregate counts names the
                            // parents to count again — both of them where the
                            // foreign key moved, since the parent the row left
                            // is as wrong as the one it joined.
                            None => {
                                let spec = aggregate_spec(&self.cfg, &parent, &table)?.clone();
                                for row in &set.rows {
                                    for image in [&row.after, &row.before].into_iter().flatten() {
                                        let key =
                                            image_column(rt, &table, image, &spec.foreign_key)?;
                                        pending.name_parent(parent.clone(), key);
                                    }
                                }
                            }
                        }
                    } else if self.cfg.children.contains_key(&table)
                        || self.cfg.aggregates.contains_key(&table)
                    {
                        // a parent row: its document is here, only the arrays are
                        // missing, so it waits for the group's read
                        for row in &set.rows {
                            let ChangeEvent::Row(change) =
                                build_change(rt, &set.kind, row, version)?
                            else {
                                continue;
                            };
                            pending.hold_parent(table.clone(), change);
                        }
                    } else {
                        for row in &set.rows {
                            let change = build_change(rt, &set.kind, row, version)?;
                            tx.send(change)
                                .await
                                .map_err(|_| MySqlError::ChannelClosed)?;
                        }
                    }
                    if pending.len() >= PENDING_FLUSH_ROWS {
                        flush_pending(&self.cfg, &mut pending, &mut admin, &tx, Some(version))
                            .await?;
                    }
                }
                binlog::T_PARTIAL_UPDATE_ROWS => {
                    // the setting that produces these is refused at startup, so
                    // reaching one means it was turned on underneath us
                    return Err(MySqlError::Config(
                        "the server sent a partial JSON update, which is not decoded here; \
                         set binlog_row_value_options to the empty string"
                            .into(),
                    ));
                }
                _ => {}
            }
        }
    }

    fn is_configured(&self, schema: &str, table: &str) -> bool {
        self.cfg.tables.contains(schema, table)
    }
}

/// How many held rows and parent keys resolve in one go.
///
/// Bounds what one transaction can hold before it is resolved in pieces. Rows of
/// tables with no children never reach this: they are their own document and go
/// straight out.
const PENDING_FLUSH_ROWS: usize = 5_000;

/// The column names a row image is to be read with: the binlog's own where the
/// server runs `binlog_row_metadata = FULL`, and `information_schema`'s
/// otherwise, which is the portable answer.
fn column_names(
    opt: &binlog::OptionalMeta,
    meta: &binlog::TableMeta,
    resolved: &TableSchema,
) -> Vec<String> {
    if opt.column_names.len() == meta.columns.len() {
        opt.column_names.clone()
    } else {
        resolved.column_names()
    }
}

/// Say a table changed shape, in the log and as an event the engine counts.
///
/// The log line names what changed for whoever is reading it; the event is
/// what makes the drift alertable, since the change is never applied and the
/// index keeps the shape it had.
async fn report_drift(
    tx: &tokio::sync::mpsc::Sender<ChangeEvent>,
    schema: &str,
    table: &str,
    detail: String,
) -> Result<()> {
    tracing::warn!(target: "pg2osync::source",
        "{schema}.{table} changed shape: {detail}. Documents already in the index \
         keep the old shape until it is rebuilt");
    tx.send(ChangeEvent::SchemaDrift {
        schema: schema.to_string(),
        table: table.to_string(),
        detail,
    })
    .await
    .map_err(|_| MySqlError::ChannelClosed)
}

/// The collection a streamed row belongs to, as either of the tables it reads.
fn child_spec<'a>(
    cfg: &'a MySqlSourceConfig,
    parent: &(String, String),
    table: &(String, String),
) -> Option<&'a pg2osync_core::children::ChildSpec> {
    cfg.children
        .get(parent)
        .and_then(|specs| specs.iter().find(|s| s.reads(&table.0, &table.1)))
}

/// The aggregate a streamed row counts towards.
fn aggregate_spec<'a>(
    cfg: &'a MySqlSourceConfig,
    parent: &(String, String),
    table: &(String, String),
) -> Result<&'a pg2osync_core::aggregate::AggregateSpec> {
    cfg.aggregates
        .get(parent)
        .and_then(|specs| specs.iter().find(|s| s.reads(&table.0, &table.1)))
        .ok_or_else(|| {
            MySqlError::protocol(format!(
                "child {}.{} has no matching children entry",
                table.0, table.1
            ))
        })
}

/// One column of a child row, as the value that locates what it belongs to.
///
/// `binlog_row_image = FULL` is a startup requirement, so a delete carries its
/// whole before-image and the column is always there — the case PostgreSQL
/// needs `REPLICA IDENTITY FULL` for, and warns about, cannot arise here.
fn row_column(
    rt: &RegisteredTable,
    table: &(String, String),
    row: &binlog::RowsRow,
    column: &str,
) -> Result<serde_json::Value> {
    let values = row
        .after
        .as_ref()
        .or(row.before.as_ref())
        .ok_or_else(|| MySqlError::protocol("child row carries no image"))?;
    image_column(rt, table, values, column)
}

/// The same column read out of one named image, so a row that moved between
/// parents can be read twice: once as it is, once as it was.
fn image_column(
    rt: &RegisteredTable,
    table: &(String, String),
    values: &[Option<serde_json::Value>],
    column: &str,
) -> Result<serde_json::Value> {
    let idx = rt.columns.iter().position(|c| c == column).ok_or_else(|| {
        MySqlError::protocol(format!(
            "column {column} missing on {}.{}",
            table.0, table.1
        ))
    })?;
    values.get(idx).and_then(|v| v.clone()).ok_or_else(|| {
        MySqlError::protocol(format!(
            "child row of {}.{} carries no {column}; cannot locate its parent",
            table.0, table.1
        ))
    })
}

/// Turn the group's changed many-to-many child rows into the parents they
/// belong to, before anything else is resolved.
///
/// One `SELECT DISTINCT` per through collection, whatever the transaction
/// touched, and the answer merges into the parents the group already names — so
/// a parent named by both a junction row and one of its child rows is still read
/// once and its collections aggregated once.
async fn resolve_through(
    cfg: &MySqlSourceConfig,
    pending: &mut pg2osync_core::children::Pending,
    conn: &mut MySqlConnection,
) -> Result<()> {
    for ((table, field), child_keys) in pending.take_through() {
        let Some(spec) = cfg
            .children
            .get(&table)
            .and_then(|specs| specs.iter().find(|s| s.field == field))
            .cloned()
        else {
            continue;
        };
        for key in crate::children::parents_through(&spec, conn, &child_keys).await? {
            pending.name_parent(table.clone(), key);
        }
    }
    Ok(())
}

/// Resolve everything held and emit it.
///
/// One query per child collection and one per parent table, for the whole group,
/// rather than two per changed row.
async fn flush_pending(
    cfg: &MySqlSourceConfig,
    pending: &mut pg2osync_core::children::Pending,
    conn: &mut MySqlConnection,
    tx: &tokio::sync::mpsc::Sender<ChangeEvent>,
    version: Option<u64>,
) -> Result<()> {
    if pending.is_empty() {
        return Ok(());
    }
    resolve_through(cfg, pending, conn).await?;
    for table in pending.tables() {
        let (mut emitted, named) = pending.take(&table);
        let specs = cfg.children.get(&table).cloned().unwrap_or_default();
        let missing = pg2osync_core::children::keys_needing_refetch(&emitted, named);
        if !missing.is_empty() {
            let found =
                crate::children::refetch_parents(conn, &table.0, &table.1, &missing).await?;
            for key in missing {
                // a parent that no longer exists emits nothing: the delete of the
                // parent itself is what removes the document
                if let Some(doc) = found.get(&pg2osync_core::children::key_lookup(&key)) {
                    emitted.push(RowChange {
                        schema: table.0.clone(),
                        table: table.1.clone(),
                        kind: RowKind::Insert {
                            pk: key,
                            doc: doc.clone(),
                        },
                        version: None,
                    });
                }
            }
        }
        for spec in &specs {
            let keys: Vec<serde_json::Value> = emitted.iter().map(|r| r.pk().clone()).collect();
            let by_key = crate::children::fetch_many(spec, conn, &keys).await?;
            let mut cut = 0usize;
            let mut duplicates = pg2osync_core::children::Duplicates::default();
            for change in emitted.iter_mut() {
                let key = change.pk().clone();
                let Some(doc) = change.doc_mut() else {
                    continue;
                };
                let (arr, total) = match by_key.get(&pg2osync_core::children::key_lookup(&key)) {
                    Some((arr, total)) => (arr.clone(), *total),
                    None => (serde_json::Value::Array(Vec::new()), 0),
                };
                let applied = pg2osync_core::children::apply_collection(doc, spec, arr, total);
                if applied.truncated {
                    cut += 1;
                }
                duplicates.record(spec, &key, applied.matched);
            }
            if let Some(message) = duplicates.message(spec) {
                tracing::warn!(target: "pg2osync::source", "{message}");
            }
            if cut > 0 {
                tracing::warn!(target: "pg2osync::source",
                    "{cut} document(s) embed only max_rows of {}, and say so in {} and {}",
                    spec.qualified(), spec.truncated_field(), spec.total_field());
            }
        }
        let aggregates = cfg.aggregates.get(&table).cloned().unwrap_or_default();
        crate::aggregate::attach_aggregates(conn, &aggregates, &mut emitted).await?;
        for mut change in emitted {
            change.version = version;
            tx.send(ChangeEvent::Row(change))
                .await
                .map_err(|_| MySqlError::ChannelClosed)?;
        }
    }
    pending.clear_seen();
    Ok(())
}

/// The event between its 19-byte header and the checksum the server appends.
///
/// The header alone says nothing about the checksum, so an event can parse a
/// header and still be too short to hold one; slicing it blindly would panic
/// where a corrupt or truncated stream should surface as an error.
fn event_body(ev: &[u8], event_type: u8, checksum_len: usize) -> Result<&[u8]> {
    ev.get(binlog::HEADER_LEN..ev.len().saturating_sub(checksum_len))
        .ok_or_else(|| {
            MySqlError::protocol(format!(
                "binlog event type {event_type} is {} bytes, too short for its header and \
                 {checksum_len}-byte checksum",
                ev.len()
            ))
        })
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
///
/// On an append-only table an insert carries no key — the engine files it
/// under a hash of its content — and an update or delete is an error, because
/// without a key nothing can say which document it is.
fn build_change(
    rt: &RegisteredTable,
    kind: &RowsKind,
    row: &binlog::RowsRow,
    version: u64,
) -> Result<ChangeEvent> {
    if rt.append_only && !matches!(kind, RowsKind::Write) {
        let what = match kind {
            RowsKind::Update => "an UPDATE",
            _ => "a DELETE",
        };
        return Err(MySqlError::Config(format!(
            "{}.{}: {what} arrived on an append-only table; nothing can say which \
             document it is",
            rt.schema, rt.table
        )));
    }
    // Deletes carry only the before-image; inserts and updates carry an after-
    // image whose values are the new row state.
    let (values, key_values) = match kind {
        RowsKind::Delete => {
            let before = row
                .before
                .as_ref()
                .or(row.after.as_ref())
                .ok_or_else(|| MySqlError::protocol("delete event carries no row image"))?;
            (before, before)
        }
        _ => {
            let after = row
                .after
                .as_ref()
                .ok_or_else(|| MySqlError::protocol("row event carries no after image"))?;
            (after, after)
        }
    };

    let doc = document(&rt.columns, values);
    let pk = if rt.append_only {
        Value::Null
    } else {
        primary_key(&rt.columns, key_values, &rt.pk_columns)?
    };

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
            // and the before-image is the whole row it replaced, which is what
            // lets a derived id find the document the row used to own
            before: row
                .before
                .as_ref()
                .map(|before| Value::Object(document(&rt.columns, before))),
        },
        RowsKind::Delete => RowKind::Delete {
            pk,
            // the delete's only image *is* the before-image
            before: Some(Value::Object(document(&rt.columns, values))),
        },
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
        let idx = columns.iter().position(|c| c == pk).ok_or_else(|| {
            MySqlError::protocol(format!("primary key column {pk} not present in binlog row"))
        })?;
        let value = values.get(idx).and_then(|v| v.clone()).ok_or_else(|| {
            MySqlError::protocol(format!("primary key column {pk} missing from row image"))
        })?;
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
            append_only: false,
        }
    }

    #[test]
    fn an_append_only_insert_carries_no_key_and_an_update_is_refused() {
        let rt = RegisteredTable {
            pk_columns: vec![],
            append_only: true,
            ..table()
        };
        let row = binlog::RowsRow {
            before: None,
            after: Some(vec![Some(json!(1)), Some(json!("5.00"))]),
        };
        let ChangeEvent::Row(change) = build_change(&rt, &RowsKind::Write, &row, 900).unwrap()
        else {
            panic!("expected a row change");
        };
        // the engine mints the id from the document, so a key here would only
        // be something for the load and the stream to disagree about
        assert_eq!(change.pk(), &Value::Null);
        let RowKind::Insert { doc, .. } = &change.kind else {
            panic!("expected an insert");
        };
        assert_eq!(doc["total"], json!("5.00"));

        let updated = binlog::RowsRow {
            before: Some(vec![Some(json!(1)), Some(json!("5.00"))]),
            after: Some(vec![Some(json!(1)), Some(json!("9.00"))]),
        };
        let err = build_change(&rt, &RowsKind::Update, &updated, 900)
            .expect_err("no key, so no document to replace");
        assert_eq!(
            err.to_string(),
            "shop.orders: an UPDATE arrived on an append-only table; nothing can say which \
             document it is"
        );
        let err = build_change(&rt, &RowsKind::Delete, &updated, 900)
            .expect_err("no key, so no document to remove");
        assert!(err.to_string().contains("a DELETE arrived"), "{err}");
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
            doc,
            previous_pk,
            before,
            ..
        } = &change.kind
        else {
            panic!("expected an update");
        };
        assert_eq!(doc["total"], json!("9.00"));
        assert_eq!(previous_pk, &Some(json!(1)), "the key did not move");
        assert_eq!(
            before,
            &Some(json!({"id": 1, "total": "5.00"})),
            "binlog_row_image=FULL means the whole old row is always there"
        );
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
        let RowKind::Delete { before, .. } = &change.kind else {
            unreachable!("checked above")
        };
        assert_eq!(
            before,
            &Some(json!({"id": 42, "total": "1.00"})),
            "a delete's only image is the row as it was"
        );
    }

    #[test]
    fn an_event_too_short_for_its_checksum_is_an_error_not_a_panic() {
        let header_only = [0u8; binlog::HEADER_LEN];
        let err = event_body(&header_only, binlog::T_QUERY, 4).expect_err("too short");
        assert!(err.to_string().contains("too short"), "{err}");

        let mut with_checksum = header_only.to_vec();
        with_checksum.extend_from_slice(&[1, 2, 3, 4]);
        assert!(
            event_body(&with_checksum, binlog::T_QUERY, 4)
                .expect("fits")
                .is_empty()
        );
        assert!(
            event_body(&header_only, binlog::T_QUERY, 0)
                .expect("no checksum")
                .is_empty()
        );
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
