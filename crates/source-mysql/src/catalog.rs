//! Schema resolution, prerequisite checks and consistent-snapshot backfill.
//!
//! Binlog row events identify columns only by ordinal, so column names and the
//! primary key come from `information_schema` over a second connection: the
//! dump connection cannot run queries once streaming has started.

use crate::connection::MySqlConnection;
use crate::error::{Context as _, MySqlError, Result};
use crate::typemap::{self, ValueShape};
use serde_json::{Map, Value};
use std::collections::HashMap;

/// Column names, primary key and declared types of one table.
#[derive(Debug, Clone, Default)]
pub struct TableSchema {
    pub columns: Vec<Column>,
    pub pk_columns: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct Column {
    pub name: String,
    /// What the column's bytes mean, decided once so both readers agree.
    pub shape: ValueShape,
}

impl TableSchema {
    pub fn column_names(&self) -> Vec<String> {
        self.columns.iter().map(|c| c.name.clone()).collect()
    }
}

/// What one table's shape changed into, as `added`/`removed`/`retyped`.
///
/// The binlog says a DDL ran but not what it did to a column layout, so the
/// only honest report is the one this comparison produces: what the catalog
/// answered before the statement against what it answers after.
pub fn column_drift(before: &TableSchema, after: &TableSchema) -> Option<String> {
    let (old, new) = (before.column_names(), after.column_names());
    let added: Vec<&str> = new
        .iter()
        .filter(|c| !old.contains(c))
        .map(String::as_str)
        .collect();
    let removed: Vec<&str> = old
        .iter()
        .filter(|c| !new.contains(c))
        .map(String::as_str)
        .collect();
    let retyped: Vec<String> = before
        .columns
        .iter()
        .filter_map(|b| {
            let a = after.columns.iter().find(|a| a.name == b.name)?;
            (a.shape != b.shape).then(|| format!("{} ({:?} -> {:?})", b.name, b.shape, a.shape))
        })
        .collect();

    let mut parts = Vec::new();
    if !added.is_empty() {
        parts.push(format!("added {}", added.join(", ")));
    }
    if !removed.is_empty() {
        parts.push(format!("removed {}", removed.join(", ")));
    }
    if !retyped.is_empty() {
        parts.push(format!("retyped {}", retyped.join(", ")));
    }
    (!parts.is_empty()).then(|| parts.join("; "))
}

/// Cache of resolved schemas; DDL invalidates an entry by dropping it.
#[derive(Default)]
pub struct SchemaCache {
    entries: HashMap<(String, String), TableSchema>,
    /// Shapes an invalidation dropped, kept until that table is resolved again
    /// so the fresh answer can be compared against the one the pipeline had
    /// been decoding rows with. Bounded by the configured tables.
    dropped: HashMap<(String, String), TableSchema>,
    /// Drift found while resolving, waiting for its caller to report it. The
    /// resolve happens behind a `&TableSchema` borrow, so the finding cannot be
    /// handed back with the schema itself.
    drift: HashMap<(String, String), String>,
}

impl SchemaCache {
    pub fn invalidate(&mut self, schema: &str, table: &str) {
        let key = (schema.to_string(), table.to_string());
        if let Some(previous) = self.entries.remove(&key) {
            self.dropped.insert(key, previous);
        }
    }

    pub fn invalidate_all(&mut self) {
        for (key, previous) in self.entries.drain() {
            self.dropped.insert(key, previous);
        }
    }

    /// Take the drift found the last time this table was resolved, if any.
    /// Reported once: a second TABLE_MAP for the same shape is not new drift.
    pub fn take_drift(&mut self, schema: &str, table: &str) -> Option<String> {
        self.drift.remove(&(schema.to_string(), table.to_string()))
    }

    pub async fn get(
        &mut self,
        conn: &mut MySqlConnection,
        schema: &str,
        table: &str,
        keyless_ok: bool,
    ) -> Result<&TableSchema> {
        let key = (schema.to_string(), table.to_string());
        if !self.entries.contains_key(&key) {
            let resolved = table_schema(conn, schema, table, keyless_ok).await?;
            if let Some(previous) = self.dropped.remove(&key)
                && let Some(drift) = column_drift(&previous, &resolved)
            {
                self.drift.insert(key.clone(), drift);
            }
            self.entries.insert(key.clone(), resolved);
        }
        Ok(self.entries.get(&key).expect("inserted above"))
    }
}

/// Resolve one table's columns and primary key.
///
/// A table without a primary key is an error unless `keyless_ok`, which a
/// section declared `append_only` grants: its rows are filed under a hash of
/// their content, so no key is needed and `pk_columns` stays empty.
pub async fn table_schema(
    conn: &mut MySqlConnection,
    schema: &str,
    table: &str,
    keyless_ok: bool,
) -> Result<TableSchema> {
    let rows = conn
        .query_text_rows(&format!(
            "SELECT column_name, data_type, column_type FROM information_schema.columns \
             WHERE table_schema = {} AND table_name = {} ORDER BY ordinal_position",
            quote_str(schema),
            quote_str(table)
        ))
        .await
        .catalog_ctx(|| format!("cannot read columns of {schema}.{table}"))?;
    if rows.is_empty() {
        return Err(MySqlError::Config(format!(
            "table {schema}.{table} does not exist or is not visible to this user"
        )));
    }
    let lower = |r: &Vec<Option<String>>, i: usize| -> String {
        r.get(i)
            .cloned()
            .flatten()
            .unwrap_or_default()
            .to_lowercase()
    };
    let columns: Vec<Column> = rows
        .iter()
        .map(|r| Column {
            name: r.first().cloned().flatten().unwrap_or_default(),
            // column_type carries what data_type leaves out: the declared enum
            // and set labels
            shape: typemap::shape_of(&lower(r, 1), &lower(r, 2)),
        })
        .collect();

    let pk_rows = conn
        .query_text_rows(&format!(
            "SELECT column_name FROM information_schema.statistics \
             WHERE table_schema = {} AND table_name = {} AND index_name = 'PRIMARY' \
             ORDER BY seq_in_index",
            quote_str(schema),
            quote_str(table)
        ))
        .await?;
    let pk_columns: Vec<String> = pk_rows
        .iter()
        .filter_map(|r| r.first().cloned().flatten())
        .collect();
    if pk_columns.is_empty() && !keyless_ok {
        return Err(MySqlError::Config(format!(
            "table {schema}.{table} has no PRIMARY KEY; pg2osync needs one to \
             derive a stable document id, or `append_only = true` on its [sync] \
             section to index its rows as they arrive"
        )));
    }
    Ok(TableSchema {
        columns,
        pk_columns,
    })
}

/// The whole script a DBA needs for a MySQL or MariaDB source.
///
/// The server settings are a file edit rather than SQL: `binlog_format` is
/// settable at runtime but does not survive a restart, and `log_bin` is not
/// settable at all, so anything but my.cnf is a trap.
pub fn setup_script(user: &str, databases: &[String]) -> String {
    let mut out = String::new();
    out.push_str("-- pg2osync source setup for MySQL/MariaDB. Review before running.\n\n");
    out.push_str("-- 1. Server settings. These belong in my.cnf and need a RESTART:\n");
    out.push_str("--    log_bin is not settable at runtime, and a SET GLOBAL of the\n");
    out.push_str("--    others is lost on the next one.\n");
    out.push_str("--\n");
    out.push_str("--    [mysqld]\n");
    out.push_str("--    log_bin                   = mysql-bin\n");
    out.push_str("--    binlog_format             = ROW\n");
    out.push_str("--    binlog_row_image          = FULL\n");
    out.push_str("--    binlog_row_value_options  =        # must be empty: PARTIAL_JSON\n");
    out.push_str("--                                       # writes JSON updates as diffs\n");
    out.push_str("--    server_id                 = 1\n\n");

    out.push_str("-- 2. A user for the pipeline.\n");
    out.push_str(&format!(
        "CREATE USER '{user}'@'%' IDENTIFIED BY 'change-me';\n\n"
    ));

    out.push_str("-- 3. Replication privileges. SLAVE opens the binlog stream,\n");
    out.push_str("--    CLIENT reads the position it should start from.\n");
    out.push_str(&format!(
        "GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO '{user}'@'%';\n\n"
    ));

    out.push_str("-- 4. Read access for the initial load and for column metadata.\n");
    for db in databases {
        out.push_str(&format!("GRANT SELECT ON `{db}`.* TO '{user}'@'%';\n"));
    }
    out.push_str("\nFLUSH PRIVILEGES;\n");
    out
}

/// Verify the server is configured for row-based CDC.
///
/// All three settings are required for correct updates and deletes; failing at
/// startup is far better than silently replicating partial rows.
pub async fn check_prerequisites(conn: &mut MySqlConnection) -> Result<()> {
    let log_bin = conn.global_var("log_bin").await?.unwrap_or_default();
    if log_bin != "1" && log_bin.to_uppercase() != "ON" {
        return Err(MySqlError::Config(format!(
            "binary logging is disabled (log_bin={log_bin:?}); enable it and restart MySQL"
        )));
    }
    let format = conn
        .global_var("binlog_format")
        .await?
        .unwrap_or_default()
        .to_uppercase();
    if format != "ROW" {
        return Err(MySqlError::Config(format!(
            "binlog_format is {format:?} but must be ROW; \
             set `binlog_format = ROW` in my.cnf and restart MySQL"
        )));
    }
    // MariaDB has no binlog_row_image before 10.1 and reports FULL by default
    match conn.global_var("binlog_row_image").await {
        Ok(Some(image)) if image.to_uppercase() != "FULL" => {
            return Err(MySqlError::Config(format!(
                "binlog_row_image is {image:?} but must be FULL; \
                 MINIMAL/NOBLOB images omit unchanged columns, which loses data"
            )));
        }
        _ => {}
    }
    // PARTIAL_JSON logs a JSON update as a diff in an event type of its own,
    // and changes the shape of every row image that follows it. Refusing is
    // honest; the alternative is dropping those updates without saying so.
    match conn.global_var("binlog_row_value_options").await {
        Ok(Some(options)) if !options.trim().is_empty() => {
            return Err(MySqlError::Config(format!(
                "binlog_row_value_options is {options:?} but must be empty; \
                 PARTIAL_JSON writes JSON updates as diffs, which are not decoded here"
            )));
        }
        _ => {}
    }
    Ok(())
}

/// Current binlog file and position (`SHOW MASTER STATUS`).
///
/// MySQL 8.4 renamed the statement; both spellings are tried.
pub async fn master_position(conn: &mut MySqlConnection) -> Result<(String, u32)> {
    for stmt in ["SHOW BINARY LOG STATUS", "SHOW MASTER STATUS"] {
        if let Ok(row) = conn.query_text_row(stmt).await
            && let Some(Some(file)) = row.first()
        {
            let pos = row
                .get(1)
                .and_then(|v| v.as_ref())
                .and_then(|s| s.parse().ok())
                .unwrap_or(4);
            return Ok((file.clone(), pos));
        }
    }
    Err(MySqlError::Config(
        "cannot read binlog position; the replication user needs REPLICATION CLIENT".into(),
    ))
}

/// Which server this is, and whether its GTIDs can be used to resume.
///
/// The two answers travel together because neither is useful alone: the
/// mechanism for asking differs by server, and MySQL can have GTIDs turned off
/// while MariaDB cannot have them at all.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GtidSupport {
    pub mariadb: bool,
    pub usable: bool,
}

/// Ask the server what it is and whether GTIDs are on.
///
/// MariaDB always has them, and has no `gtid_mode` to ask about. MySQL does,
/// and only `ON` is usable: under `ON_PERMISSIVE` a transaction may be written
/// with no GTID at all, so a set built from the stream would silently omit it.
pub async fn gtid_support(conn: &mut MySqlConnection) -> Result<GtidSupport> {
    let version = conn
        .query_text_row("SELECT VERSION()")
        .await
        .ok()
        .and_then(|row| row.first().cloned().flatten())
        .unwrap_or_default();
    let mariadb = version.to_lowercase().contains("mariadb");
    if mariadb {
        return Ok(GtidSupport {
            mariadb: true,
            usable: true,
        });
    }
    let mode = conn
        .query_text_row("SELECT @@GLOBAL.gtid_mode")
        .await
        .ok()
        .and_then(|row| row.first().cloned().flatten())
        .unwrap_or_default()
        .to_uppercase();
    if mode == "ON_PERMISSIVE" || mode == "OFF_PERMISSIVE" {
        tracing::warn!(target: "pg2osync::source",
            "gtid_mode is {mode}, which still allows transactions with no GTID, so \
             checkpoints keep using the binlog coordinate and cannot survive a failover. \
             Set gtid_mode = ON to change that");
    }
    Ok(GtidSupport {
        mariadb: false,
        usable: mode == "ON",
    })
}

/// Everything the server has already written, as a GTID position.
///
/// Read only when starting without a checkpoint, and read *before* the
/// coordinate the stream will start from. It is the baseline a later resume
/// needs: `COM_BINLOG_DUMP_GTID` sends every transaction *not* in the set it is
/// given, so a set holding only what this run streamed would have the server
/// replay the whole history before it. Taking it before the coordinate means it
/// can only under-claim, and an under-claim costs a replay onto an idempotent
/// write rather than a gap.
///
/// It is not a substitute for tracking: from here on, only what the stream
/// actually delivers is added, because this says what the *server* holds and
/// not what we have consumed.
pub async fn gtid_baseline(
    conn: &mut MySqlConnection,
    support: GtidSupport,
) -> Option<crate::gtid::GtidPosition> {
    if !support.usable {
        return None;
    }
    let variable = if support.mariadb {
        "@@GLOBAL.gtid_binlog_pos"
    } else {
        "@@GLOBAL.gtid_executed"
    };
    let text = conn
        .query_text_row(&format!("SELECT {variable}"))
        .await
        .ok()?
        .first()
        .cloned()
        .flatten()?;
    // A server with nothing in its binlog has an empty position, which is not a
    // failure: there is simply nothing to have consumed yet.
    crate::gtid::GtidPosition::parse(&text.replace('\n', ""))
}

/// Split `binlog.000004` into its prefix and numeric index.
pub fn split_binlog_file(file: &str) -> Option<(String, u32)> {
    let (prefix, idx) = file.rsplit_once('.')?;
    Some((prefix.to_string(), idx.parse().ok()?))
}

/// Pack a binlog coordinate into the engine's monotonic position token.
///
/// The file index occupies the high 32 bits so a rotation always compares
/// greater than any position in the previous file.
pub fn position_token(file: &str, pos: u32) -> u64 {
    let index = split_binlog_file(file).map(|(_, i)| i).unwrap_or(0);
    (u64::from(index) << 32) | u64::from(pos)
}

/// Render a token back into `prefix.NNNNNN:pos` form.
pub fn position_text(prefix: &str, token: u64) -> String {
    format!("{prefix}.{:06}:{}", token >> 32, token as u32)
}

/// Parse the textual form produced by [`position_text`].
pub fn parse_position(text: &str) -> Option<(String, u32)> {
    let stored = parse_stored_position(text)?;
    Some((stored.file, stored.pos))
}

/// Everything a MySQL checkpoint's position text says.
///
/// `core` treats that text as the source's own business, so the GTID position
/// and the version generation ride inside it rather than widening the
/// checkpoint document for one source.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredPosition {
    pub file: String,
    pub pos: u32,
    /// Where to resume regardless of which server this is now.
    pub gtid: Option<crate::gtid::GtidPosition>,
    /// Added to every coordinate to make the version. Zero for a checkpoint
    /// written before generations existed, which is exactly what it means: the
    /// first generation.
    pub base: u64,
}

impl StoredPosition {
    /// The version the coordinate in this position maps to.
    pub fn token(&self) -> u64 {
        self.base
            .saturating_add(position_token(&self.file, self.pos))
    }
}

/// Render a checkpoint position, carrying what it takes to resume elsewhere.
///
/// The bare `file:pos` form is kept when there is nothing to add, so a
/// checkpoint only grows the extra fields once they mean something.
pub fn position_text_full(prefix: &str, token: u64, base: u64, gtid: Option<&str>) -> String {
    let mut out = position_text(prefix, token.saturating_sub(base));
    if let Some(gtid) = gtid {
        out.push_str(";gtid=");
        out.push_str(gtid);
    }
    if base > 0 {
        out.push_str(&format!(";base={base}"));
    }
    out
}

/// Parse either the bare coordinate or the full form.
///
/// A field this version does not know is ignored rather than fatal: the
/// coordinate is the part every version has understood, and refusing the whole
/// checkpoint over an unrecognised suffix would force a reload for nothing.
pub fn parse_stored_position(text: &str) -> Option<StoredPosition> {
    let mut parts = text.split(';');
    let coordinate = parts.next()?;
    let (file, pos) = coordinate.rsplit_once(':')?;
    let mut stored = StoredPosition {
        file: file.to_string(),
        pos: pos.trim().parse().ok()?,
        gtid: None,
        base: 0,
    };
    for field in parts {
        match field.split_once('=') {
            Some(("gtid", value)) => stored.gtid = crate::gtid::GtidPosition::parse(value),
            Some(("base", value)) => stored.base = value.trim().parse().unwrap_or(0),
            _ => {}
        }
    }
    Some(stored)
}

/// Build one backfill document plus its primary key.
///
/// A keyless (append-only) table yields `Null` for the key: the engine mints
/// the id from the document, and an empty object here would be a key that
/// every row shares.
pub fn build_document(schema: &TableSchema, row: &[Option<Vec<u8>>]) -> (Value, Value) {
    let mut doc = Map::new();
    let mut pk = Map::new();
    for (i, col) in schema.columns.iter().enumerate() {
        let value = typemap::convert(&col.shape, row.get(i).and_then(|v| v.as_deref()));
        if schema.pk_columns.contains(&col.name) {
            pk.insert(col.name.clone(), value.clone());
        }
        doc.insert(col.name.clone(), value);
    }
    let pk = match pk.len() {
        0 => Value::Null,
        1 => pk.into_iter().next().expect("single entry").1,
        _ => Value::Object(pk),
    };
    (Value::Object(doc), pk)
}

pub fn quote_ident(name: &str) -> String {
    format!("`{}`", name.replace('`', "``"))
}

pub fn quote_str(value: &str) -> String {
    format!("'{}'", value.replace('\\', "\\\\").replace('\'', "''"))
}

/// How MySQL spells a filter: backticked identifiers, and strings that escape
/// the backslash MySQL would otherwise read as an escape.
pub fn dialect() -> pg2osync_core::filter::SqlDialect<'static> {
    pg2osync_core::filter::SqlDialect {
        quote_ident: &quote_ident,
        quote_str: &quote_str,
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn the_setup_script_names_the_settings_that_need_a_restart() {
        let script = setup_script("svc", &["shop".into()]);
        assert!(script.contains("RESTART"));
        assert!(script.contains("binlog_row_image          = FULL"));
        assert!(
            script.contains("binlog_row_value_options"),
            "PARTIAL_JSON is refused"
        );
        assert!(script.contains("CREATE USER 'svc'@'%'"));
        assert!(script.contains("GRANT REPLICATION SLAVE, REPLICATION CLIENT"));
        assert!(script.contains("GRANT SELECT ON `shop`.*"));
    }

    use super::*;

    #[test]
    fn position_tokens_order_across_rotations() {
        let a = position_token("binlog.000004", 900);
        let b = position_token("binlog.000005", 4);
        assert!(
            b > a,
            "a rotation must compare greater than any prior offset"
        );
        assert_eq!(position_text("binlog", a), "binlog.000004:900");
        assert_eq!(
            parse_position("binlog.000004:900"),
            Some(("binlog.000004".into(), 900))
        );
    }

    #[test]
    fn a_position_written_before_generations_still_reads() {
        let stored = parse_stored_position("binlog.000004:900").expect("parses");
        assert_eq!(stored.file, "binlog.000004");
        assert_eq!(stored.pos, 900);
        assert_eq!(stored.base, 0, "no generation means the first one");
        assert!(stored.gtid.is_none());
        assert_eq!(stored.token(), position_token("binlog.000004", 900));
    }

    #[test]
    fn a_full_position_survives_its_own_text() {
        let token = position_token("binlog.000004", 900) + 4096;
        let text = position_text_full("binlog", token, 4096, Some("0-3307-1431"));
        assert_eq!(text, "binlog.000004:900;gtid=0-3307-1431;base=4096");
        let stored = parse_stored_position(&text).expect("parses");
        assert_eq!(stored.pos, 900);
        assert_eq!(stored.base, 4096);
        assert_eq!(
            stored.gtid.as_ref().map(|g| g.to_text()).as_deref(),
            Some("0-3307-1431")
        );
        assert_eq!(
            stored.token(),
            token,
            "the token round trips through the text"
        );
    }

    #[test]
    fn nothing_extra_is_written_when_there_is_nothing_extra_to_say() {
        let token = position_token("binlog.000004", 900);
        assert_eq!(
            position_text_full("binlog", token, 0, None),
            "binlog.000004:900",
            "a checkpoint only grows the extra fields once they mean something"
        );
    }

    #[test]
    fn an_unknown_field_costs_the_extras_and_not_the_checkpoint() {
        let stored = parse_stored_position("binlog.000004:900;whatever=1;base=8").expect("parses");
        assert_eq!(stored.base, 8);
        assert_eq!(stored.pos, 900);
    }

    #[test]
    fn documents_carry_scalar_and_composite_keys() {
        let schema = TableSchema {
            columns: vec![
                Column {
                    name: "id".into(),
                    shape: ValueShape::Int,
                },
                Column {
                    name: "tenant".into(),
                    shape: ValueShape::Text,
                },
            ],
            pk_columns: vec!["id".into()],
        };
        let row = [Some(b"5".to_vec()), Some(b"acme".to_vec())];
        let (doc, pk) = build_document(&schema, &row);
        assert_eq!(pk, Value::from(5));
        assert_eq!(doc["tenant"], Value::from("acme"));

        let composite = TableSchema {
            pk_columns: vec!["id".into(), "tenant".into()],
            ..schema
        };
        let (_, pk) = build_document(&composite, &row);
        assert_eq!(pk["tenant"], Value::from("acme"));
    }

    #[test]
    fn a_keyless_document_carries_no_key() {
        // an append-only table: the engine files the row under a hash of its
        // content, and an empty object would be one key every row shared
        let schema = TableSchema {
            columns: vec![Column {
                name: "kind".into(),
                shape: ValueShape::Text,
            }],
            pk_columns: vec![],
        };
        let (doc, pk) = build_document(&schema, &[Some(b"click".to_vec())]);
        assert_eq!(pk, Value::Null);
        assert_eq!(doc["kind"], Value::from("click"));
    }

    #[test]
    fn every_shape_change_is_named() {
        let column = |name: &str, shape: ValueShape| Column {
            name: name.into(),
            shape,
        };
        let before = TableSchema {
            columns: vec![
                column("id", ValueShape::Int),
                column("name", ValueShape::Text),
                column("old", ValueShape::Text),
            ],
            pk_columns: vec!["id".into()],
        };
        let after = TableSchema {
            columns: vec![
                column("id", ValueShape::Text),
                column("name", ValueShape::Text),
                column("new", ValueShape::Text),
            ],
            pk_columns: vec!["id".into()],
        };
        assert_eq!(column_drift(&before, &before), None);
        let drift = column_drift(&before, &after).expect("changed");
        assert!(drift.contains("added new"), "{drift}");
        assert!(drift.contains("removed old"), "{drift}");
        assert!(drift.contains("retyped id"), "{drift}");
    }

    #[test]
    fn identifiers_and_literals_are_escaped() {
        assert_eq!(quote_ident("we`ird"), "`we``ird`");
        assert_eq!(quote_str("O'Hara"), "'O''Hara'");
    }
}
