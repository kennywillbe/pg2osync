//! PostgreSQL sink: a pgvector-equipped database used as a search backend.
//!
//! One section writes one table. The operator supplies that table's DDL —
//! `mapping_file` pointing at a `.sql` file — and the sink applies it when the
//! table is absent, then checks the two structural things a document write
//! depends on. It never derives a table from the source: this target is a
//! document store that happens to speak SQL, not a replica of the source
//! database.
//!
//! Deviations from the OpenSearch contract, and why:
//! - no aliases, so `switch_alias` and everything built on it — `reindex`,
//!   `require_alias` — is refused rather than approximated
//! - no shards, so `routing` and the join data model are refused
//! - no dynamic mapping: a document field with no column of that name is a
//!   permanent rejection naming the column, which takes the configured
//!   `on_permanent_rejection` path like any other refusal
//! - `_version` is a column of the operator's table rather than metadata the
//!   engine keeps, so the DDL has to declare it

use async_trait::async_trait;
use pg2osync_core::checkpoint::Checkpoint;
use pg2osync_core::error::CoreError;
use pg2osync_core::sink::{
    DocumentOp, Health, IndexSpec, LsnOp, Rejection, Sink, SinkAck, StoredReject,
};
use serde_json::{Value, json};
use std::collections::HashMap;
use tokio_postgres::Client;
use tokio_postgres::error::SqlState;

/// Where one stream's checkpoint and the initial-load progress live.
pub const STATE_TABLE: &str = "pg2osync_state";
/// Where documents the target refused are kept, for the reason the rejects
/// index exists on the other targets: an operator can read or drop it without
/// going anywhere near a checkpoint.
pub const REJECTS_TABLE: &str = "pg2osync_rejects";
/// The column carrying the source position a row was written at.
///
/// Underscore-prefixed and stripped from every read-back, so it is the sink's
/// bookkeeping rather than a field of the document. A truncate deletes at a
/// position by comparing it, which is the whole reason it exists.
pub const VERSION_COLUMN: &str = "_version";

pub struct PostgresSink {
    url: String,
    tls: pg2osync_tls::TlsSettings,
    retry: crate::RetryPolicy,
    /// One connection, taken for the duration of each operation.
    ///
    /// `write` needs a transaction and `tokio_postgres::Client::transaction`
    /// takes `&mut self`, so the connection is owned by whoever is mid-batch.
    /// `None` after a failure that killed it: the next caller reconnects
    /// instead of returning the same dead-connection error forever.
    client: tokio::sync::Mutex<Option<Client>>,
    /// What `ensure_ready` found out about each table, so a write costs no
    /// catalog round trip.
    shapes: std::sync::Mutex<HashMap<String, TableShape>>,
}

#[derive(Debug, Clone)]
pub struct PostgresSinkConfig {
    pub url: String,
    pub retry: crate::RetryPolicy,
}

/// What a write needs to know about one target table.
#[derive(Debug, Clone, PartialEq, Eq)]
struct TableShape {
    /// In `attnum` order, which is the order `jsonb_populate_record` returns.
    columns: Vec<String>,
    /// The single-column primary key the document id is filed under.
    key: String,
}

impl TableShape {
    fn has(&self, column: &str) -> bool {
        self.columns.iter().any(|c| c == column)
    }

    /// The upsert, built once per table.
    ///
    /// The document arrives as one `jsonb` parameter and PostgreSQL maps it
    /// onto the row type by column name, using each column's own input
    /// function — which is what carries a JSON array of floats into a
    /// `vector(n)` column without this sink knowing that type exists.
    fn upsert_sql(&self, table: &str) -> String {
        let assignments: Vec<String> = self
            .columns
            .iter()
            .filter(|c| **c != self.key)
            .map(|c| format!("{} = EXCLUDED.{}", quote_ident(c), quote_ident(c)))
            .collect();
        let table = quote_qualified(table);
        let key = quote_ident(&self.key);
        let version = quote_ident(VERSION_COLUMN);
        // The guard is `<=` rather than `<` so a replay of a write that already
        // landed is applied again rather than refused: delivery is at-least-once
        // and an identical write has to be harmless. A row or a write with no
        // position — the initial load, poll mode — has no ordering to keep.
        format!(
            "INSERT INTO {table} SELECT * FROM jsonb_populate_record(NULL::{table}, $1::jsonb) \
             ON CONFLICT ({key}) DO UPDATE SET {} \
             WHERE {table}.{version} IS NULL OR EXCLUDED.{version} IS NULL \
             OR {table}.{version} <= EXCLUDED.{version}",
            assignments.join(", ")
        )
    }
}

/// One SQL identifier, quoted so a name needing it survives.
fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// A possibly schema-qualified table name, each part quoted separately.
///
/// `public.users` names a table in a schema; quoting it whole would name a
/// table called `public.users` in the search path, which is a different table
/// and would be created rather than found.
fn quote_qualified(name: &str) -> String {
    match name.split_once('.') {
        Some((schema, table)) => format!("{}.{}", quote_ident(schema), quote_ident(table)),
        None => quote_ident(name),
    }
}

/// Whether a database error is worth another attempt.
///
/// Classified by SQLSTATE class rather than by message: connection failures,
/// serialization failures and an administrator's intervention all end the same
/// statement, and all of them succeed on a retry. Everything else — a type the
/// value does not fit, a column that is not there, a constraint — will fail
/// identically forever, so it is the document that is wrong.
fn is_transient(state: Option<&SqlState>) -> bool {
    let Some(state) = state else {
        // no SQLSTATE at all is a connection that went away mid-statement
        return true;
    };
    matches!(
        state.code().get(..2),
        Some("08") | Some("40") | Some("53") | Some("57") | Some("58")
    )
}

/// A database error as the taxonomy the engine retries on.
fn db_err(context: &str, e: tokio_postgres::Error) -> CoreError {
    let reason = format!("{context}: {e}");
    if is_transient(e.code()) {
        CoreError::SinkTransient(reason)
    } else {
        CoreError::Sink(reason)
    }
}

/// The reason a document was refused, as an operator would need to read it.
///
/// The server's own sentence names the column and the type, and that is the
/// only thing that says which field to fix.
fn refusal(e: &tokio_postgres::Error) -> String {
    match e.as_db_error() {
        Some(db) => match db.column() {
            Some(column) => format!("column {column}: {}", db.message()),
            None => db.message().to_string(),
        },
        None => e.to_string(),
    }
}

impl PostgresSink {
    pub fn new(cfg: PostgresSinkConfig) -> Result<Self, CoreError> {
        // From the connection string alone: `sslmode` there is where a managed
        // provider's documentation puts it, and a target has no second place to
        // configure a connection the way `[source]` does.
        let tls =
            pg2osync_tls::TlsSettings::resolve(&cfg.url, pg2osync_tls::ConfiguredTls::default())
                .map_err(|e| CoreError::Sink(format!("target tls: {e}")))?;
        Ok(Self {
            url: cfg.url,
            tls,
            retry: cfg.retry,
            client: tokio::sync::Mutex::new(None),
            shapes: std::sync::Mutex::new(HashMap::new()),
        })
    }

    /// Run `f` with a live connection, reconnecting first if the last one died.
    ///
    /// The connection is dropped on a transient failure and kept on a permanent
    /// one: a syntax error leaves the session perfectly usable, and throwing it
    /// away would turn one bad document into a reconnect per batch.
    async fn with_client<T, F>(&self, f: F) -> Result<T, CoreError>
    where
        F: AsyncFnOnce(&mut Client) -> Result<T, CoreError>,
    {
        let mut held = self.client.lock().await;
        if held.as_ref().is_none_or(Client::is_closed) {
            *held = Some(
                connect_target(&self.tls, &self.url)
                    .await
                    .map_err(|e| CoreError::SinkTransient(format!("connect to target: {e}")))?,
            );
        }
        // proven Some by the branch above
        let client = held.as_mut().expect("a connection was just established");
        let out = f(client).await;
        if matches!(out, Err(CoreError::SinkTransient(_))) {
            *held = None;
        }
        out
    }

    /// The shape `ensure_ready` recorded, or a refusal naming what to run.
    fn shape_of(&self, table: &str) -> Result<TableShape, CoreError> {
        crate::lock(&self.shapes)
            .get(table)
            .cloned()
            .ok_or_else(|| {
                CoreError::Sink(format!(
                    "table {table} was never prepared; every section's table is created or \
                 checked before the first batch"
                ))
            })
    }

    /// Read a table's columns and its primary key, or `None` when it is absent.
    async fn read_shape(client: &Client, table: &str) -> Result<Option<TableShape>, CoreError> {
        let columns = client
            .query(
                "SELECT attname::text FROM pg_attribute \
                 WHERE attrelid = to_regclass($1) AND attnum > 0 AND NOT attisdropped \
                 ORDER BY attnum",
                &[&table],
            )
            .await
            .map_err(|e| db_err(&format!("read the columns of {table}"), e))?;
        if columns.is_empty() {
            return Ok(None);
        }
        let keys = client
            .query(
                "SELECT a.attname::text FROM pg_index i \
                 JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) \
                 WHERE i.indrelid = to_regclass($1) AND i.indisprimary",
                &[&table],
            )
            .await
            .map_err(|e| db_err(&format!("read the primary key of {table}"), e))?;
        let mut keys: Vec<String> = keys.iter().map(|r| r.get(0)).collect();
        if keys.len() != 1 {
            return Err(CoreError::Sink(format!(
                "table {table} has {} primary key columns; a document has one id, so its \
                 table needs a single-column primary key to file it under",
                keys.len()
            )));
        }
        Ok(Some(TableShape {
            columns: columns.iter().map(|r| r.get(0)).collect(),
            // proven single by the check above
            key: keys.pop().expect("exactly one primary key column"),
        }))
    }

    /// The tables this sink keeps its own bookkeeping in.
    ///
    /// Created here rather than asked of the operator because they are ours:
    /// the DDL an operator writes describes documents, and a checkpoint is not
    /// one.
    async fn ensure_own_tables(client: &Client) -> Result<(), CoreError> {
        client
            .batch_execute(&format!(
                "CREATE TABLE IF NOT EXISTS {state} (\
                   key text PRIMARY KEY, \
                   doc jsonb NOT NULL, \
                   updated_at timestamptz NOT NULL DEFAULT now());
                 CREATE TABLE IF NOT EXISTS {rejects} (\
                   id text PRIMARY KEY, \
                   doc jsonb NOT NULL, \
                   at_epoch bigint NOT NULL);",
                state = quote_ident(STATE_TABLE),
                rejects = quote_ident(REJECTS_TABLE),
            ))
            .await
            .map_err(|e| db_err("create the checkpoint and rejects tables", e))
    }

    /// Apply one operation inside the batch's transaction, on a savepoint.
    ///
    /// The savepoint is what makes a partial batch possible at all: a statement
    /// that fails aborts the transaction it is in, so without one a single
    /// refused document would take every other document of the batch with it —
    /// and the contract is that every rejection in a batch is visible, not just
    /// the first.
    async fn apply(
        tx: &mut tokio_postgres::Transaction<'_>,
        shape: &TableShape,
        op: &DocumentOp,
    ) -> Result<Option<String>, CoreError> {
        let point = tx
            .transaction()
            .await
            .map_err(|e| db_err("open a savepoint", e))?;
        let outcome = match op {
            DocumentOp::Upsert {
                index,
                id,
                doc,
                version,
                ..
            } => {
                let body = match document_body(shape, id, doc, *version) {
                    Ok(body) => body,
                    Err(why) => {
                        point
                            .rollback()
                            .await
                            .map_err(|e| db_err("roll back a savepoint", e))?;
                        return Ok(Some(why));
                    }
                };
                point
                    .execute(&shape.upsert_sql(index), &[&body])
                    .await
                    .map(|_| ())
            }
            DocumentOp::Delete {
                index, id, version, ..
            } => {
                // A delete carries the position it happened at for the same
                // reason a write does: a replayed delete must not remove a row
                // that a later write put back.
                let sql = format!(
                    "DELETE FROM {table} WHERE {key}::text = $1 \
                     AND ({version} IS NULL OR $2::bigint IS NULL OR {version} <= $2::bigint)",
                    table = quote_qualified(index),
                    key = quote_ident(&shape.key),
                    version = quote_ident(VERSION_COLUMN),
                );
                let at = version.and_then(|v| i64::try_from(v).ok());
                point.execute(&sql, &[&id, &at]).await.map(|_| ())
            }
            DocumentOp::DeleteChildren { .. } => {
                // an error and not a no-op: a cascade dropped here would be
                // indistinguishable from one that ran
                point
                    .rollback()
                    .await
                    .map_err(|e| db_err("roll back a savepoint", e))?;
                return Err(CoreError::Sink(
                    "a join field's cascade cannot be expressed against PostgreSQL, which has \
                     no parent-child document model; this operation should have been refused \
                     at startup"
                        .into(),
                ));
            }
        };
        match outcome {
            Ok(()) => {
                point
                    .commit()
                    .await
                    .map_err(|e| db_err("release a savepoint", e))?;
                Ok(None)
            }
            Err(e) if is_transient(e.code()) => Err(db_err("write a document", e)),
            Err(e) => {
                let why = refusal(&e);
                point
                    .rollback()
                    .await
                    .map_err(|e| db_err("roll back a savepoint", e))?;
                Ok(Some(why))
            }
        }
    }

    /// One attempt at a batch: all of it in one transaction, or none of it.
    async fn write_once(
        &self,
        batch: &[LsnOp],
        shapes: &HashMap<String, TableShape>,
    ) -> Result<Vec<(usize, String)>, CoreError> {
        self.with_client(async |client| {
            // Committed before anything is acknowledged: a partial transaction
            // must never be visible as a complete one, and the position the ack
            // carries is only durable once the commit returns.
            let mut tx = client
                .transaction()
                .await
                .map_err(|e| db_err("begin a batch", e))?;
            let mut refusals = Vec::new();
            for (nth, op) in batch.iter().enumerate() {
                let index = match &op.op {
                    DocumentOp::Upsert { index, .. }
                    | DocumentOp::Delete { index, .. }
                    | DocumentOp::DeleteChildren { index, .. } => index,
                };
                // present: every index of the batch was looked up before the
                // first attempt
                let shape = shapes.get(index).expect("a shape for every index");
                if let Some(why) = Self::apply(&mut tx, shape, &op.op).await? {
                    refusals.push((nth, why));
                }
            }
            tx.commit().await.map_err(|e| db_err("commit a batch", e))?;
            Ok(refusals)
        })
        .await
    }

    async fn state_doc(&self, key: &str) -> Result<Option<Value>, CoreError> {
        let key = key.to_string();
        self.with_client(async |client| {
            let rows = client
                .query(
                    &format!(
                        "SELECT doc FROM {} WHERE key = $1",
                        quote_ident(STATE_TABLE)
                    ),
                    &[&key],
                )
                .await;
            match rows {
                Ok(rows) => Ok(rows.first().map(|r| r.get::<_, Value>(0))),
                // nothing has ever been written to this target, which is not
                // the same as an error
                Err(e) if e.code() == Some(&SqlState::UNDEFINED_TABLE) => Ok(None),
                Err(e) => Err(db_err(&format!("read state {key}"), e)),
            }
        })
        .await
    }

    async fn put_state_doc(&self, key: &str, doc: &Value) -> Result<(), CoreError> {
        let key = key.to_string();
        let doc = doc.clone();
        self.with_client(async |client| {
            Self::ensure_own_tables(client).await?;
            client
                .execute(
                    &format!(
                        "INSERT INTO {} (key, doc, updated_at) VALUES ($1, $2, now()) \
                         ON CONFLICT (key) DO UPDATE SET doc = EXCLUDED.doc, \
                         updated_at = EXCLUDED.updated_at",
                        quote_ident(STATE_TABLE)
                    ),
                    &[&key, &doc],
                )
                .await
                .map(|_| ())
                .map_err(|e| db_err(&format!("write state {key}"), e))
        })
        .await
    }
}

/// The row `jsonb_populate_record` will build, or why the document cannot be
/// one.
///
/// The id lands in the primary key column whatever the document calls it, so a
/// derived id (`tenant-{tenant_id}-{id}`) is what the row is filed under rather
/// than whichever column it was built from. `_version` is set here for the same
/// reason: it is the sink's, not the document's.
fn document_body(
    shape: &TableShape,
    id: &str,
    doc: &Value,
    version: Option<u64>,
) -> Result<Value, String> {
    let Some(fields) = doc.as_object() else {
        return Err("a document that is not a JSON object has no columns to write".into());
    };
    let mut row = serde_json::Map::with_capacity(fields.len() + 2);
    for (field, value) in fields {
        if !shape.has(field) {
            // The DDL is the contract here: this target has no dynamic mapping
            // to invent a column from the first value that arrives, and running
            // the ALTER TABLE ourselves is the schema mirroring this sink is
            // deliberately not.
            return Err(format!(
                "column {field} does not exist; add it to the table's DDL or leave the field \
                 out with exclude_columns"
            ));
        }
        row.insert(field.clone(), value.clone());
    }
    row.insert(shape.key.clone(), json!(id));
    row.insert(
        VERSION_COLUMN.to_string(),
        match version.and_then(|v| i64::try_from(v).ok()) {
            Some(at) => json!(at),
            None => Value::Null,
        },
    );
    Ok(Value::Object(row))
}

/// Connect to the target, with TLS when it is asked for.
///
/// The same mode vocabulary the source uses, because it is the same kind of
/// connection to the same kind of server — and an operator who wrote
/// `sslmode=require` for one would be surprised to find it meant nothing here.
async fn connect_target(
    tls: &pg2osync_tls::TlsSettings,
    url: &str,
) -> Result<Client, Box<dyn std::error::Error + Send + Sync>> {
    if tls.mode == pg2osync_tls::SslMode::Disable {
        let (client, connection) = tokio_postgres::connect(url, tokio_postgres::NoTls).await?;
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                tracing::warn!(target: "pg2osync::sink", "target connection ended: {e}");
            }
        });
        return Ok(client);
    }
    let mut config: tokio_postgres::Config = url.parse()?;
    config.ssl_mode(if tls.mode.requires_tls() {
        tokio_postgres::config::SslMode::Require
    } else {
        tokio_postgres::config::SslMode::Prefer
    });
    let connector = tokio_postgres_rustls::MakeRustlsConnect::new(tls.client_config()?);
    let (client, connection) = config.connect(connector).await?;
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            tracing::warn!(target: "pg2osync::sink", "target connection ended: {e}");
        }
    });
    Ok(client)
}

#[async_trait]
impl Sink for PostgresSink {
    async fn ensure_ready(&self, tables: &[IndexSpec]) -> Result<(), CoreError> {
        for spec in tables {
            // refused by config before it gets here; kept so a new caller
            // cannot create a table literally named after a glob
            if spec.pattern {
                return Err(CoreError::Sink(format!(
                    "index {:?} is chosen per row, and this target has no way to know what \
                     DDL a name a row renders should be created with",
                    spec.name
                )));
            }
            let Some(ddl) = spec.mapping.as_ref().and_then(Value::as_str) else {
                return Err(CoreError::Sink(format!(
                    "table {} has no DDL; this target creates nothing on its own, so the \
                     section needs mapping_file pointing at the .sql file that creates it",
                    spec.name
                )));
            };
            let name = spec.name.clone();
            let ddl = ddl.to_string();
            let shape = self
                .with_client(async |client| {
                    Self::ensure_own_tables(client).await?;
                    if Self::read_shape(client, &name).await?.is_none() {
                        // Only when it is absent: re-running an operator's DDL
                        // over a table that already holds documents is a
                        // migration nobody asked for, which is the rule the
                        // other targets apply to a mapping.
                        client
                            .batch_execute(&ddl)
                            .await
                            .map_err(|e| db_err(&format!("apply the DDL for {name}"), e))?;
                    }
                    Self::read_shape(client, &name).await?.ok_or_else(|| {
                        CoreError::Sink(format!(
                            "the DDL configured for {name} ran without creating it; \
                             mapping_file has to create the table the section writes to"
                        ))
                    })
                })
                .await?;
            // Checked rather than compared field by field: this sink does not
            // parse SQL, so what it can honestly verify is that the table is
            // there, that a document has one column to be filed under, and that
            // a truncate has a position to compare. Every other disagreement
            // between the DDL and the documents shows up as a rejection naming
            // the column, which is where an operator can act on it.
            if !shape.has(VERSION_COLUMN) {
                return Err(CoreError::Sink(format!(
                    "table {} has no {VERSION_COLUMN} column; a TRUNCATE has to clear the \
                     documents written before it and keep the ones written after, which it \
                     compares positions to do. Add `{VERSION_COLUMN} bigint` to the DDL",
                    spec.name
                )));
            }
            crate::lock(&self.shapes).insert(spec.name.clone(), shape);
        }
        Ok(())
    }

    async fn get_documents(
        &self,
        index: &str,
        // routing is ignored: a table has no shards to find a row on
        ids: &[(String, Option<String>)],
    ) -> Result<Vec<Option<Value>>, CoreError> {
        if ids.is_empty() {
            return Ok(vec![]);
        }
        let shape = self.shape_of(index)?;
        let wanted: Vec<String> = ids.iter().map(|(id, _)| id.clone()).collect();
        let sql = format!(
            "SELECT t.{key}::text, to_jsonb(t) - '{version}' FROM {table} t \
             WHERE t.{key}::text = ANY($1)",
            key = quote_ident(&shape.key),
            version = VERSION_COLUMN,
            table = quote_qualified(index),
        );
        let found: HashMap<String, Value> = self
            .with_client(async |client| {
                client
                    .query(&sql, &[&wanted])
                    .await
                    .map_err(|e| db_err(&format!("read documents of {index}"), e))
            })
            .await?
            .into_iter()
            .map(|row| (row.get::<_, String>(0), row.get::<_, Value>(1)))
            .collect();
        // in request order, with a hole where the row is not there, which is
        // what the caller completing a TOAST marker relies on
        Ok(wanted.iter().map(|id| found.get(id).cloned()).collect())
    }

    fn set_retry_policy(
        &self,
        max_attempts: u32,
        base_backoff_ms: u64,
        max_elapsed_ms: Option<u64>,
    ) {
        self.retry
            .set(max_attempts, base_backoff_ms, max_elapsed_ms);
    }

    async fn write(&self, batch: Vec<LsnOp>) -> Result<SinkAck, CoreError> {
        if batch.is_empty() {
            return Err(CoreError::Sink(
                "engine must never send empty batches".into(),
            ));
        }
        let mut shapes = HashMap::new();
        for op in &batch {
            let index = match &op.op {
                DocumentOp::Upsert { index, .. }
                | DocumentOp::Delete { index, .. }
                | DocumentOp::DeleteChildren { index, .. } => index,
            };
            if !shapes.contains_key(index) {
                shapes.insert(index.clone(), self.shape_of(index)?);
            }
        }
        // The whole batch is retried rather than the failed statement: a
        // transaction that lost its connection never committed, so there is
        // nothing half-written to resume from and re-running it is the only
        // attempt there is.
        let refusals: Vec<(usize, String)> =
            crate::retry_transient(&self.retry, || self.write_once(&batch, &shapes)).await?;
        // the batch is non-empty, checked above
        let max_lsn = batch.last().expect("nonempty checked").lsn;
        let rejected = crate::rejections(&batch, refusals)?;
        Ok(SinkAck { max_lsn, rejected })
    }

    async fn truncate_index(
        &self,
        index: &str,
        version: Option<u64>,
        only: Option<(&str, &str)>,
    ) -> Result<(), CoreError> {
        // a scoped clear only exists for a join pair, which is refused for
        // this target at config load
        if only.is_some() {
            return Err(CoreError::Sink(
                "a scoped truncate belongs to a join pair, which this target has no data model \
                 for; this should have been refused at startup"
                    .into(),
            ));
        }
        let at = version.and_then(|v| i64::try_from(v).ok());
        // Where rows carry a position, the truncate carries one too: everything
        // written before it loses and everything written after it survives,
        // including a row re-inserted moments later.
        let sql = format!(
            "DELETE FROM {table} WHERE $1::bigint IS NULL OR {version} IS NULL \
             OR {version} <= $1::bigint",
            table = quote_qualified(index),
            version = quote_ident(VERSION_COLUMN),
        );
        self.with_client(async |client| {
            client
                .execute(&sql, &[&at])
                .await
                .map(|_| ())
                .map_err(|e| db_err(&format!("truncate {index}"), e))
        })
        .await
    }

    async fn refresh(&self, _indices: &[String]) -> Result<(), CoreError> {
        // a committed row is visible to the next reader; there is no segment
        // to make searchable
        Ok(())
    }

    async fn count_documents(&self, index: &str) -> Result<Option<u64>, CoreError> {
        let sql = format!("SELECT count(*) FROM {}", quote_qualified(index));
        self.with_client(async |client| {
            match client.query_one(&sql, &[]).await {
                Ok(row) => Ok(u64::try_from(row.get::<_, i64>(0)).ok()),
                // a table that was never created is a different answer from an
                // empty one, which is why this is Option and not zero
                Err(e) if e.code() == Some(&SqlState::UNDEFINED_TABLE) => Ok(None),
                Err(e) => Err(db_err(&format!("count {index}"), e)),
            }
        })
        .await
    }

    async fn index_exists(&self, name: &str) -> Result<bool, CoreError> {
        let name = name.to_string();
        self.with_client(async |client| {
            client
                .query_one("SELECT to_regclass($1) IS NOT NULL", &[&name])
                .await
                .map(|row| row.get(0))
                .map_err(|e| db_err(&format!("does table {name} exist"), e))
        })
        .await
    }

    async fn delete_index(&self, name: &str) -> Result<(), CoreError> {
        let sql = format!("DROP TABLE IF EXISTS {}", quote_qualified(name));
        self.with_client(async |client| {
            client
                .execute(&sql, &[])
                .await
                .map(|_| ())
                .map_err(|e| db_err(&format!("drop table {name}"), e))
        })
        .await
    }

    fn can_quarantine(&self) -> bool {
        true
    }

    async fn quarantine(&self, rejected: &[Rejection]) -> Result<(), CoreError> {
        if rejected.is_empty() {
            return Ok(());
        }
        let rows: Vec<(String, Value, i64)> = rejected
            .iter()
            .map(|r| {
                let doc = crate::reject_doc(r);
                let at = doc["at_epoch"].as_i64().unwrap_or_default();
                (crate::reject_doc_id(&r.index, &r.doc_id), doc, at)
            })
            .collect();
        self.with_client(async |client| {
            Self::ensure_own_tables(client).await?;
            let sql = format!(
                "INSERT INTO {} (id, doc, at_epoch) VALUES ($1, $2, $3) \
                 ON CONFLICT (id) DO UPDATE SET doc = EXCLUDED.doc, at_epoch = EXCLUDED.at_epoch",
                quote_ident(REJECTS_TABLE)
            );
            for (id, doc, at) in &rows {
                client
                    .execute(&sql, &[id, doc, at])
                    .await
                    .map_err(|e| db_err(&format!("quarantine {id}"), e))?;
            }
            Ok(())
        })
        .await
    }

    async fn list_rejects(&self, limit: usize) -> Result<(Vec<StoredReject>, u64), CoreError> {
        let limit = i64::try_from(limit).unwrap_or(i64::MAX);
        self.with_client(async |client| {
            let sql = format!(
                "SELECT id, doc, count(*) OVER () FROM {} ORDER BY at_epoch DESC LIMIT $1",
                quote_ident(REJECTS_TABLE)
            );
            let rows = match client.query(&sql, &[&limit]).await {
                Ok(rows) => rows,
                // nothing was ever quarantined, which is not an error
                Err(e) if e.code() == Some(&SqlState::UNDEFINED_TABLE) => return Ok((vec![], 0)),
                Err(e) => return Err(db_err("list rejects", e)),
            };
            let stored: Vec<StoredReject> = rows
                .iter()
                .filter_map(|r| {
                    crate::reject_from_doc(&r.get::<_, String>(0), &r.get::<_, Value>(1))
                })
                .collect();
            // The window counts what the query would have returned without the
            // LIMIT, so a page carries the whole store's size with it. An empty
            // page carries nothing — which `validate` asks for, with `limit = 0`
            // — so that one case counts on its own rather than reporting zero.
            let total = match rows.first() {
                Some(row) => u64::try_from(row.get::<_, i64>(2)).unwrap_or_default(),
                None => {
                    let sql = format!("SELECT count(*) FROM {}", quote_ident(REJECTS_TABLE));
                    let row = client
                        .query_one(&sql, &[])
                        .await
                        .map_err(|e| db_err("count rejects", e))?;
                    u64::try_from(row.get::<_, i64>(0)).unwrap_or_default()
                }
            };
            Ok((stored, total))
        })
        .await
    }

    async fn clear_reject(&self, id: &str) -> Result<(), CoreError> {
        let id = id.to_string();
        self.with_client(async |client| {
            let sql = format!("DELETE FROM {} WHERE id = $1", quote_ident(REJECTS_TABLE));
            match client.execute(&sql, &[&id]).await {
                Ok(_) => Ok(()),
                // already gone is the state the caller asked for
                Err(e) if e.code() == Some(&SqlState::UNDEFINED_TABLE) => Ok(()),
                Err(e) => Err(db_err(&format!("clear reject {id}"), e)),
            }
        })
        .await
    }

    async fn read_state(&self, key: &str) -> Result<Option<Value>, CoreError> {
        self.state_doc(&format!("state-{key}")).await
    }

    async fn write_state(&self, key: &str, doc: &Value) -> Result<(), CoreError> {
        self.put_state_doc(&format!("state-{key}"), doc).await
    }

    async fn clear_state(&self, key: &str) -> Result<(), CoreError> {
        let key = format!("state-{key}");
        self.with_client(async |client| {
            let sql = format!("DELETE FROM {} WHERE key = $1", quote_ident(STATE_TABLE));
            match client.execute(&sql, &[&key]).await {
                Ok(_) => Ok(()),
                Err(e) if e.code() == Some(&SqlState::UNDEFINED_TABLE) => Ok(()),
                Err(e) => Err(db_err(&format!("clear state {key}"), e)),
            }
        })
        .await
    }

    async fn write_checkpoint(&self, checkpoint: &Checkpoint) -> Result<(), CoreError> {
        self.put_state_doc(
            &checkpoint_key(&checkpoint.stream),
            &crate::checkpoint_doc(checkpoint),
        )
        .await
    }

    async fn read_checkpoint(
        &self,
        stream: &pg2osync_core::checkpoint::StreamId,
    ) -> Result<Option<Checkpoint>, CoreError> {
        Ok(self
            .state_doc(&checkpoint_key(stream))
            .await?
            .as_ref()
            .and_then(crate::checkpoint_from_doc))
    }

    async fn health(&self) -> Result<Health, CoreError> {
        match self
            .with_client(async |client| {
                client
                    .query_one("SELECT 1", &[])
                    .await
                    .map(|_| ())
                    .map_err(|e| db_err("probe the target", e))
            })
            .await
        {
            Ok(()) => Ok(Health::Up),
            Err(e) => Ok(Health::Down(e.to_string())),
        }
    }
}

/// Named after the stream, so two pipelines against one target database do not
/// overwrite each other's position.
fn checkpoint_key(stream: &pg2osync_core::checkpoint::StreamId) -> String {
    format!("checkpoint-{}", crate::checkpoint_doc_id(stream))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn users() -> TableShape {
        TableShape {
            columns: vec![
                "id".into(),
                "name".into(),
                "embedding".into(),
                VERSION_COLUMN.into(),
            ],
            key: "id".into(),
        }
    }

    #[test]
    fn a_schema_qualified_table_keeps_its_two_parts_apart() {
        assert_eq!(quote_qualified("public.users"), "\"public\".\"users\"");
        assert_eq!(quote_qualified("users"), "\"users\"");
    }

    #[test]
    fn a_quoted_identifier_survives_a_quote_of_its_own() {
        assert_eq!(quote_ident("we\"ird"), "\"we\"\"ird\"");
    }

    #[test]
    fn the_upsert_updates_every_column_but_the_key_and_keeps_the_later_write() {
        let sql = users().upsert_sql("public.users");
        assert!(sql.contains("ON CONFLICT (\"id\")"), "{sql}");
        assert!(sql.contains("\"name\" = EXCLUDED.\"name\""), "{sql}");
        assert!(
            sql.contains("\"embedding\" = EXCLUDED.\"embedding\""),
            "{sql}"
        );
        assert!(!sql.contains("\"id\" = EXCLUDED.\"id\""), "{sql}");
        assert!(
            sql.contains("\"public\".\"users\".\"_version\" <= EXCLUDED.\"_version\""),
            "{sql}"
        );
    }

    #[test]
    fn a_replay_of_the_write_that_already_landed_is_applied_again() {
        // `<` would refuse it, and at-least-once delivery means the same write
        // arrives twice after every restart
        assert!(users().upsert_sql("t").contains("<= EXCLUDED"));
    }

    #[test]
    fn the_id_lands_in_the_primary_key_column_whatever_the_document_says() {
        let body = document_body(
            &users(),
            "tenant-7-1",
            &json!({"id": 1, "name": "alice"}),
            Some(42),
        )
        .expect("a document of known columns");
        assert_eq!(body["id"], json!("tenant-7-1"));
        assert_eq!(body["name"], json!("alice"));
        assert_eq!(body[VERSION_COLUMN], json!(42));
    }

    #[test]
    fn a_vector_is_carried_as_the_array_the_source_produced() {
        // bring-your-own-embedding: the value goes to the column untouched and
        // PostgreSQL's own input function makes it a vector
        let body = document_body(&users(), "1", &json!({"embedding": [1.0, 2.0]}), None)
            .expect("a document of known columns");
        assert_eq!(body["embedding"], json!([1.0, 2.0]));
        assert_eq!(body[VERSION_COLUMN], Value::Null);
    }

    #[test]
    fn a_field_with_no_column_is_refused_by_name() {
        let why = document_body(&users(), "1", &json!({"nickname": "al"}), None)
            .expect_err("a field the table has no column for");
        assert!(why.contains("nickname"), "{why}");
        assert!(why.contains("exclude_columns"), "{why}");
    }

    #[test]
    fn a_position_too_large_to_express_is_left_unversioned() {
        let body = document_body(&users(), "1", &json!({}), Some(u64::MAX))
            .expect("a document of known columns");
        assert_eq!(body[VERSION_COLUMN], Value::Null);
    }

    #[test]
    fn a_connection_failure_is_retried_and_a_bad_column_is_not() {
        assert!(is_transient(Some(&SqlState::CONNECTION_FAILURE)));
        assert!(is_transient(Some(&SqlState::T_R_SERIALIZATION_FAILURE)));
        assert!(is_transient(None));
        assert!(!is_transient(Some(&SqlState::UNDEFINED_COLUMN)));
        assert!(!is_transient(Some(&SqlState::INVALID_TEXT_REPRESENTATION)));
    }
}
