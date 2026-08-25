//! Schema resolution, prerequisite checks and consistent-snapshot backfill.
//!
//! Binlog row events identify columns only by ordinal, so column names and the
//! primary key come from `information_schema` over a second connection: the
//! dump connection cannot run queries once streaming has started.

use crate::connection::MySqlConnection;
use anyhow::{Context as _, Result, bail};
use base64::Engine as _;
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
    /// `information_schema.columns.data_type`, lowercase (`int`, `json`, …).
    pub data_type: String,
}

impl TableSchema {
    pub fn column_names(&self) -> Vec<String> {
        self.columns.iter().map(|c| c.name.clone()).collect()
    }
}

/// Cache of resolved schemas; DDL invalidates an entry by dropping it.
#[derive(Default)]
pub struct SchemaCache {
    entries: HashMap<(String, String), TableSchema>,
}

impl SchemaCache {
    pub fn invalidate(&mut self, schema: &str, table: &str) {
        self.entries
            .remove(&(schema.to_string(), table.to_string()));
    }

    pub fn invalidate_all(&mut self) {
        self.entries.clear();
    }

    pub async fn get(
        &mut self,
        conn: &mut MySqlConnection,
        schema: &str,
        table: &str,
    ) -> Result<&TableSchema> {
        let key = (schema.to_string(), table.to_string());
        if !self.entries.contains_key(&key) {
            let resolved = table_schema(conn, schema, table).await?;
            self.entries.insert(key.clone(), resolved);
        }
        Ok(self.entries.get(&key).expect("inserted above"))
    }
}

/// Resolve one table's columns and primary key.
pub async fn table_schema(
    conn: &mut MySqlConnection,
    schema: &str,
    table: &str,
) -> Result<TableSchema> {
    let rows = conn
        .query_text_rows(&format!(
            "SELECT column_name, data_type FROM information_schema.columns \
             WHERE table_schema = {} AND table_name = {} ORDER BY ordinal_position",
            quote_str(schema),
            quote_str(table)
        ))
        .await
        .with_context(|| format!("cannot read columns of {schema}.{table}"))?;
    if rows.is_empty() {
        bail!("table {schema}.{table} does not exist or is not visible to this user");
    }
    let columns: Vec<Column> = rows
        .iter()
        .map(|r| Column {
            name: r.first().cloned().flatten().unwrap_or_default(),
            data_type: r
                .get(1)
                .cloned()
                .flatten()
                .unwrap_or_default()
                .to_lowercase(),
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
    if pk_columns.is_empty() {
        bail!(
            "table {schema}.{table} has no PRIMARY KEY; pg2osync needs one to \
             derive a stable document id"
        );
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
        bail!("binary logging is disabled (log_bin={log_bin:?}); enable it and restart MySQL");
    }
    let format = conn
        .global_var("binlog_format")
        .await?
        .unwrap_or_default()
        .to_uppercase();
    if format != "ROW" {
        bail!(
            "binlog_format is {format:?} but must be ROW; \
             set `binlog_format = ROW` in my.cnf and restart MySQL"
        );
    }
    // MariaDB has no binlog_row_image before 10.1 and reports FULL by default
    match conn.global_var("binlog_row_image").await {
        Ok(Some(image)) if image.to_uppercase() != "FULL" => bail!(
            "binlog_row_image is {image:?} but must be FULL; \
             MINIMAL/NOBLOB images omit unchanged columns, which loses data"
        ),
        _ => {}
    }
    // PARTIAL_JSON logs a JSON update as a diff in an event type of its own,
    // and changes the shape of every row image that follows it. Refusing is
    // honest; the alternative is dropping those updates without saying so.
    match conn.global_var("binlog_row_value_options").await {
        Ok(Some(options)) if !options.trim().is_empty() => bail!(
            "binlog_row_value_options is {options:?} but must be empty; \
             PARTIAL_JSON writes JSON updates as diffs, which are not decoded here"
        ),
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
    bail!("cannot read binlog position; the replication user needs REPLICATION CLIENT")
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
    let (file, pos) = text.rsplit_once(':')?;
    Some((file.to_string(), pos.parse().ok()?))
}

/// Convert one text-protocol value into JSON using the declared column type.
///
/// `decimal` stays a string for the same reason PostgreSQL `numeric` does:
/// a float round-trip loses precision.
pub fn convert(data_type: &str, raw: Option<&str>) -> Value {
    let Some(s) = raw else { return Value::Null };
    match data_type {
        "tinyint" | "smallint" | "mediumint" | "int" | "integer" | "bigint" | "year" => s
            .parse::<i64>()
            .map(|n| Value::Number(n.into()))
            .unwrap_or_else(|_| Value::String(s.to_string())),
        "float" | "double" => s
            .parse::<f64>()
            .ok()
            .and_then(serde_json::Number::from_f64)
            .map(Value::Number)
            .unwrap_or_else(|| Value::String(s.to_string())),
        "json" => serde_json::from_str(s).unwrap_or_else(|_| Value::String(s.to_string())),
        "bit" | "binary" | "varbinary" | "blob" | "tinyblob" | "mediumblob" | "longblob"
        | "geometry" => Value::String(base64::engine::general_purpose::STANDARD.encode(s)),
        _ => Value::String(s.to_string()),
    }
}

/// Build one backfill document plus its primary key.
pub fn build_document(schema: &TableSchema, row: &[Option<String>]) -> (Value, Value) {
    let mut doc = Map::new();
    let mut pk = Map::new();
    for (i, col) in schema.columns.iter().enumerate() {
        let value = convert(&col.data_type, row.get(i).and_then(|v| v.as_deref()));
        if schema.pk_columns.contains(&col.name) {
            pk.insert(col.name.clone(), value.clone());
        }
        doc.insert(col.name.clone(), value);
    }
    let pk = if pk.len() == 1 {
        pk.into_iter().next().expect("single entry").1
    } else {
        Value::Object(pk)
    };
    (Value::Object(doc), pk)
}

pub fn quote_ident(name: &str) -> String {
    format!("`{}`", name.replace('`', "``"))
}

fn quote_str(value: &str) -> String {
    format!("'{}'", value.replace('\\', "\\\\").replace('\'', "''"))
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
    fn conversion_preserves_decimal_precision() {
        assert_eq!(
            convert("decimal", Some("12345678901234567890.123")),
            Value::String("12345678901234567890.123".into())
        );
        assert_eq!(convert("int", Some("-7")), Value::Number((-7).into()));
        assert_eq!(convert("json", Some(r#"{"a":1}"#))["a"], Value::from(1));
        assert_eq!(convert("varchar", None), Value::Null);
    }

    #[test]
    fn documents_carry_scalar_and_composite_keys() {
        let schema = TableSchema {
            columns: vec![
                Column {
                    name: "id".into(),
                    data_type: "int".into(),
                },
                Column {
                    name: "tenant".into(),
                    data_type: "varchar".into(),
                },
            ],
            pk_columns: vec!["id".into()],
        };
        let (doc, pk) = build_document(&schema, &[Some("5".into()), Some("acme".into())]);
        assert_eq!(pk, Value::from(5));
        assert_eq!(doc["tenant"], Value::from("acme"));

        let composite = TableSchema {
            pk_columns: vec!["id".into(), "tenant".into()],
            ..schema
        };
        let (_, pk) = build_document(&composite, &[Some("5".into()), Some("acme".into())]);
        assert_eq!(pk["tenant"], Value::from("acme"));
    }

    #[test]
    fn identifiers_and_literals_are_escaped() {
        assert_eq!(quote_ident("we`ird"), "`we``ird`");
        assert_eq!(quote_str("O'Hara"), "'O''Hara'");
    }
}
