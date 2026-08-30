//! Live WAL streaming into a `ChangeEvent` channel.
//!
//! Failure policy: any source error terminates this task; the engine
//! tears down and rebuilds the whole pipeline from the last checkpoint. This
//! task never reconnects on its own.

use crate::catalog;
use crate::transport::to_core_lsn;
use anyhow::{Context as _, Result};
use pg2osync_core::Lsn;
use pg2osync_core::event::ChangeEvent;
use std::collections::HashMap;

/// Everything the source needs to know; mirrors the `[source]` + `[sync]`
/// config sections already validated by the binary.
#[derive(Debug, Clone)]
pub struct WalSourceConfig {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub database: String,
    pub slot_name: String,
    pub publication: String,
    /// Schema-qualified tables, aligned with the publication.
    pub tables: Vec<String>,
    /// Explicit WAL start position; None defers to the slot's
    /// confirmed_flush_lsn. Callers should resolve this from the catalog
    /// instead of passing zero — the zero path stalls on some servers.
    pub start_lsn: Option<Lsn>,
    /// Kept for the error message when children are configured without a SQL
    /// connection; the connection itself is the caller's.
    pub admin_url: Option<String>,
    /// Applies to the replication transport and to the child-query connection
    /// alike: one source must not be half encrypted.
    pub tls: crate::tls::TlsSettings,
    /// Child collections keyed by PARENT (schema, table).
    pub children: HashMap<(String, String), Vec<crate::children::ChildSpec>>,
    /// Reverse routing: CHILD (schema, table) -> parent (schema, table).
    pub child_parents: HashMap<(String, String), (String, String)>,
    /// Parent PK columns for refetch queries, keyed by parent (schema, table).
    pub parent_pk_columns: HashMap<(String, String), String>,
    /// Each synced table's primary key, from the catalogue. Under REPLICA
    /// IDENTITY FULL pgoutput flags every column as identity, and only this
    /// says which of them a document is actually filed under.
    pub key_columns: HashMap<(String, String), Vec<String>>,
    /// Tables declared `append_only`: their inserts carry no key, and an
    /// update or delete on one is an error rather than a document nothing
    /// can find.
    pub append_only: std::collections::HashSet<(String, String)>,
    /// Highest durably-flushed position as reported by the engine's
    /// checkpoint task. Feedback to PostgreSQL is clamped to this: acking
    /// beyond it lets PG recycle WAL for events we have not indexed yet,
    /// which loses data on crash-restart.
    pub durable: Option<std::sync::Arc<std::sync::atomic::AtomicU64>>,
}

pub struct WalSource {
    cfg: WalSourceConfig,
    /// Tables seen in the stream that nothing maps, so the warning is said once
    /// rather than per row.
    unmapped: std::collections::HashSet<(String, String)>,
}

/// How many held rows and parent keys resolve in one go.
///
/// Bounds what one transaction can hold before it is resolved in pieces. Rows of
/// tables with no children never reach this: they are their own document and go
/// straight out.
const PENDING_FLUSH_ROWS: usize = 5_000;

impl WalSource {
    pub fn new(cfg: WalSourceConfig) -> Self {
        Self {
            cfg,
            unmapped: std::collections::HashSet::new(),
        }
    }

    /// Whether this table is one the config asked for.
    ///
    /// Children count: their rows are not documents, but they are ours to act
    /// on. Everything else in the publication is not, and cannot be — the
    /// engine has no index to write it to.
    fn is_configured(&self, schema: &str, table: &str) -> bool {
        let key = (schema.to_string(), table.to_string());
        self.cfg.children.contains_key(&key)
            || self.cfg.child_parents.contains_key(&key)
            || self
                .cfg
                .tables
                .iter()
                .any(|t| t == &format!("{schema}.{table}"))
    }

    /// Idempotent bootstrap: wal_level check, publication aligned to config
    /// (drift is an error, never auto-applied) and slot created if missing.
    pub async fn bootstrap(&self, admin: &tokio_postgres::Client) -> Result<()> {
        catalog::check_wal_level(admin).await?;
        catalog::ensure_publication(admin, &self.cfg.publication, &self.cfg.tables).await?;
        catalog::ensure_slot(admin, &self.cfg.slot_name).await?;
        catalog::warn_about_idle_slots(admin, &self.cfg.slot_name).await;
        Ok(())
    }

    /// Stream committed row changes until error or shutdown signal fires.
    ///
    /// Acking: the transport's applied-LSN is clamped to the durable
    /// checkpoint position (see `durable`), so PG can never recycle WAL for
    /// rows that were not yet indexed.
    ///
    /// Shutdown races `client.recv()` rather than waiting for it: the socket
    /// belongs to the transport's worker task and `recv` only takes frames
    /// from its channel, so cancelling the wait loses nothing — and on an idle
    /// stream the next frame is a keepalive seconds away, which is longer than
    /// `docker stop` gives a container.
    /// `admin` is an ordinary SQL connection for the child re-fetch queries.
    /// The replication connection cannot run them, which is unavoidable; a
    /// third connection of our own was not, since the caller already holds one.
    pub async fn stream(
        &mut self,
        tx: tokio::sync::mpsc::Sender<ChangeEvent>,
        shutdown: tokio::sync::watch::Receiver<bool>,
        admin: Option<&tokio_postgres::Client>,
    ) -> Result<()> {
        use pgwire_replication::client::ReplicationEvent;
        use pgwire_replication::{ReplicationClient, ReplicationConfig};

        let mut client = ReplicationClient::connect(
            ReplicationConfig::new(
                &self.cfg.host,
                &self.cfg.user,
                &self.cfg.password,
                &self.cfg.database,
                &self.cfg.slot_name,
                self.cfg.publication.clone(),
            )
            .with_port(self.cfg.port)
            .with_start_lsn(pgwire_replication::Lsn(
                self.cfg.start_lsn.unwrap_or(Lsn(0)).0,
            ))
            .with_tls(crate::tls::replication_config(&self.cfg.tls)),
        )
        .await
        .context("replication connect failed")?;

        // RELATION messages arrive after every relcache invalidation, so the
        // registry is upserted rather than built once at startup.
        let mut relations: HashMap<u32, crate::pgoutput::Relation> = HashMap::new();
        // Keepalives may only advance the position between transactions: doing
        // it mid-transaction would acknowledge a position the buffered rows
        // have not reached yet.
        let mut in_transaction = false;
        let mut pending = crate::children::Pending::default();
        // The commit position of the open transaction, stamped onto each row it
        // produces so the engine never has to infer it from event order.
        let mut txn_version: Option<u64> = None;
        // Only child re-fetch needs SQL, so without children the connection the
        // caller passed is left alone.
        let needs_admin = !self.cfg.children.is_empty() || !self.cfg.child_parents.is_empty();
        let admin_client = needs_admin.then_some(admin).flatten();
        if needs_admin && admin_client.is_none() {
            anyhow::bail!("nested children are configured but no SQL connection was provided");
        }
        tracing::info!(target: "pg2osync::source", "stream loop starting");

        loop {
            let ev = tokio::select! {
                biased;
                _ = wait_shutdown(&shutdown) => {
                    tracing::info!(target: "pg2osync::source", "shutdown requested");
                    client.shutdown().await.ok();
                    return Ok(());
                }
                received = client.recv() => match received {
                    Ok(Some(ev)) => ev,
                    Ok(None) => {
                        tracing::warn!(target: "pg2osync::source", "stream ended normally");
                        return Ok(());
                    }
                    Err(e) => return Err(e).context("replication stream failed"),
                },
            };
            tracing::trace!(target: "pg2osync::source", "raw event");
            match ev {
                ReplicationEvent::Begin { final_lsn, .. } => {
                    in_transaction = true;
                    txn_version = Some(to_core_lsn(final_lsn).0);
                    // pgoutput reports the commit position before the rows, so
                    // every document this transaction produces can be versioned
                    // by the position it becomes visible at
                    tx.send(ChangeEvent::Transaction(
                        pg2osync_core::event::TransactionBoundary::Begin {
                            lsn: to_core_lsn(final_lsn),
                        },
                    ))
                    .await
                    .context("change channel closed")?;
                }
                ReplicationEvent::Commit {
                    end_lsn,
                    commit_time_micros,
                    ..
                } => {
                    // Everything held for this transaction resolves here, before
                    // the boundary that flushes the batch — one query per
                    // collection for the whole group rather than two per row.
                    if let Some(admin) = admin_client {
                        flush_pending(&self.cfg, &mut pending, admin, &tx, txn_version).await?;
                    }
                    in_transaction = false;
                    txn_version = None;
                    // end_lsn, not the commit record's own position: this is
                    // what pg_current_wal_lsn() reports after the commit, so a
                    // caller waiting for its own write compares like with like.
                    tx.send(ChangeEvent::Transaction(
                        pg2osync_core::event::TransactionBoundary::Commit {
                            lsn: to_core_lsn(end_lsn),
                            commit_ts_micros: commit_time_micros,
                        },
                    ))
                    .await
                    .context("change channel closed")?;
                }
                ReplicationEvent::XLogData { data, .. } => {
                    let msg = crate::pgoutput::parse(&data)
                        .map_err(|e| anyhow::anyhow!("pgoutput decode failed: {e}"))?;
                    match msg {
                        crate::pgoutput::Message::Relation(rel) => {
                            if let Some(previous) = relations.get(&rel.rel_id)
                                && let Some(drift) = crate::pgoutput::column_drift(previous, &rel)
                            {
                                // documents written before this point keep the
                                // old shape until the index is rebuilt, which
                                // is why the index name is configuration
                                tracing::warn!(target: "pg2osync::source",
                                    "{}.{} changed shape: {drift}. Documents already in \
                                     the index keep the old shape until it is rebuilt",
                                    rel.schema, rel.name);
                                // A log line is not alertable; the engine
                                // counts this one.
                                tx.send(ChangeEvent::SchemaDrift {
                                    schema: rel.schema.clone(),
                                    table: rel.name.clone(),
                                    detail: drift,
                                })
                                .await
                                .context("change channel closed")?;
                            }
                            relations.insert(rel.rel_id, rel.clone());
                        }
                        crate::pgoutput::Message::Truncate(tr) => {
                            // one event per relation: the engine clears each
                            // mapped index independently
                            for rel_id in &tr.rel_ids {
                                let Some(rel) = relations.get(rel_id) else {
                                    return Err(anyhow::anyhow!(
                                        "TRUNCATE for unknown relation oid {rel_id}"
                                    ));
                                };
                                tx.send(ChangeEvent::TableTruncated {
                                    schema: rel.schema.clone(),
                                    table: rel.name.clone(),
                                    version: txn_version,
                                })
                                .await
                                .context("change channel closed")?;
                            }
                        }
                        msg @ (crate::pgoutput::Message::Insert(_)
                        | crate::pgoutput::Message::Update(_)
                        | crate::pgoutput::Message::Delete(_)) => {
                            match self.classify(&msg, &relations)? {
                                Classified::Row(mut change) => {
                                    change.version = txn_version;
                                    send_change(&tx, change).await?;
                                }
                                Classified::ParentRow(table, change) => {
                                    pending.hold_parent(table, change)
                                }
                                Classified::NamesParent { parent, key } => {
                                    pending.name_parent(parent, key)
                                }
                                Classified::Skip => {}
                            }
                            // A transaction large enough to matter is resolved in
                            // pieces rather than held whole: the engine already
                            // splits an oversized transaction, and holding one
                            // in memory here would be the same mistake twice.
                            if pending.len() >= PENDING_FLUSH_ROWS
                                && let Some(admin) = admin_client
                            {
                                flush_pending(&self.cfg, &mut pending, admin, &tx, txn_version)
                                    .await?;
                            }
                        }
                        _ => {}
                    }
                }
                // A logical decoding message carries no row data; its value is
                // its position. PostgreSQL skips transactions that touch no
                // published table, so on a quiet database our position can sit
                // arbitrarily far behind the server's — emitting a message is
                // how anything (the /synced endpoint, an idle pipeline) pushes
                // the stream forward without writing to a user table.
                ReplicationEvent::Message { lsn, .. } if !in_transaction => {
                    tx.send(ChangeEvent::Transaction(
                        pg2osync_core::event::TransactionBoundary::Commit {
                            lsn: to_core_lsn(lsn),
                            commit_ts_micros: 0,
                        },
                    ))
                    .await
                    .context("change channel closed")?;
                }
                ReplicationEvent::KeepAlive { wal_end, .. } => {
                    // A keepalive means the server has sent everything up to
                    // wal_end. Publishing it advances the position on an idle
                    // database, which is what lets a caller wait for its own
                    // commit when no later traffic follows it — and stops WAL
                    // accumulating for a pipeline that is simply caught up.
                    if !in_transaction {
                        tx.send(ChangeEvent::Transaction(
                            pg2osync_core::event::TransactionBoundary::Commit {
                                lsn: to_core_lsn(wal_end),
                                commit_ts_micros: 0,
                            },
                        ))
                        .await
                        .context("change channel closed")?;
                    }
                    // clamp feedback to the durable position: acknowledging
                    // ahead of it would allow PG to discard WAL we cannot
                    // recover from after a crash
                    let safe = self
                        .cfg
                        .durable
                        .as_ref()
                        .map(|a| a.load(std::sync::atomic::Ordering::SeqCst))
                        .unwrap_or(u64::MAX);
                    client.update_applied_lsn(pgwire_replication::Lsn(
                        to_core_lsn(wal_end).0.min(safe),
                    ));
                }
                other => {
                    tracing::debug!(target: "pg2osync::source", "ignoring event {other:?}");
                }
            }
        }
    }
}

impl WalSource {
    /// What one Insert/Update/Delete message means for the documents.
    ///
    /// Rows of tables with no children configured become their document
    /// directly. The rest are held: a parent row is missing only its arrays, and
    /// a child row is not a document at all — it names a parent that has to be
    /// re-read. Holding them is what allows one query per collection per
    /// transaction instead of one per row.
    fn classify(
        &mut self,
        msg: &crate::pgoutput::Message,
        relations: &HashMap<u32, crate::pgoutput::Relation>,
    ) -> Result<Classified> {
        use crate::docbuild::Incoming;
        use crate::pgoutput::Message;

        let (rel_id, incoming) = match msg {
            Message::Insert(i) => (i.rel_id, Incoming::Insert(i.new_tuple.clone())),
            Message::Update(u) => {
                let old = u.old_tuple.as_ref().map(|o| match o {
                    crate::pgoutput::OldTuple::Key(t) | crate::pgoutput::OldTuple::Full(t) => {
                        t.clone()
                    }
                });
                (u.rel_id, Incoming::Update(old, u.new_tuple.clone()))
            }
            Message::Delete(d) => {
                let key = match &d.key_tuple {
                    crate::pgoutput::OldTuple::Key(t) | crate::pgoutput::OldTuple::Full(t) => {
                        t.clone()
                    }
                };
                (d.rel_id, Incoming::Delete(key))
            }
            _ => return Ok(Classified::Skip),
        };
        let Some(rel) = relations.get(&rel_id) else {
            // PG always sends RELATION before the first row of a relation on a
            // fresh stream; missing entries mean our own bookkeeping is broken
            return Err(anyhow::anyhow!("row for unknown relation oid {rel_id}"));
        };
        let table = (rel.schema.clone(), rel.name.clone());

        // CHILD table: it names a parent to re-read, and carries nothing else
        if let Some(parent) = self.cfg.child_parents.get(&table) {
            let specs = self.cfg.children.get(parent).cloned().unwrap_or_default();
            let cspec = specs
                .iter()
                .find(|s| s.schema == rel.schema && s.table == rel.name)
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "child {}.{} has no matching children entry",
                        rel.schema,
                        rel.name
                    )
                })?;
            // the parent is located by the FOREIGN KEY value carried in the
            // row; deletes only carry it under REPLICA IDENTITY FULL
            let fk_idx =
                super::docbuild::column_index(rel, &cspec.foreign_key).ok_or_else(|| {
                    anyhow::anyhow!(
                        "fk column {} missing on {}.{}",
                        cspec.foreign_key,
                        rel.schema,
                        rel.name
                    )
                })?;
            let fk_json = super::docbuild::convert_column_at(rel, fk_idx, incoming.tuple())
                .map_err(|e| anyhow::anyhow!("fk decode: {e}"))?;
            if fk_json.is_null() {
                return Err(anyhow::anyhow!(
                    "child row carries NULL {}; cannot locate parent. \
                     Consider ALTER TABLE {}.{} REPLICA IDENTITY FULL",
                    cspec.foreign_key,
                    rel.schema,
                    rel.name
                ));
            }
            return Ok(Classified::NamesParent {
                parent: parent.clone(),
                key: fk_json,
            });
        }

        // A table the publication carries but the config does not name. The
        // publication normally holds exactly the configured tables, and then a
        // rename moves one out from under its entry without leaving it: the
        // rows keep arriving under a name nothing maps. Dropping them here is
        // what keeps the engine's mapping an internal invariant rather than
        // something an `ALTER TABLE` can violate from outside.
        if !self.is_configured(&rel.schema, &rel.name) {
            if self.unmapped.insert(table.clone()) {
                tracing::warn!(target: "pg2osync::source",
                    "{}.{} is in publication {} but not in [sync]; its rows are being \
                     dropped. A renamed table does this — the index still holds what it \
                     had under the old name, and nothing will update it until the config \
                     names the new one",
                    rel.schema, rel.name, self.cfg.publication);
            }
            return Ok(Classified::Skip);
        }

        let change = super::docbuild::build_row_change(
            rel,
            incoming,
            self.cfg.key_columns.get(&table).map(Vec::as_slice),
            self.cfg.append_only.contains(&table),
        )?;
        if self.cfg.children.contains_key(&table) {
            Ok(Classified::ParentRow(table, change))
        } else {
            Ok(Classified::Row(change))
        }
    }
}

/// What a decoded row turned out to be.
enum Classified {
    /// A document, complete as it stands.
    Row(pg2osync_core::event::RowChange),
    /// A document missing only its child arrays.
    ParentRow((String, String), pg2osync_core::event::RowChange),
    /// Not a document: the parent it names has to be re-read.
    NamesParent {
        parent: (String, String),
        key: serde_json::Value,
    },
    Skip,
}

async fn wait_shutdown(shutdown: &tokio::sync::watch::Receiver<bool>) {
    let mut rx = shutdown.clone();
    while !*rx.borrow() {
        if rx.changed().await.is_err() {
            return;
        }
    }
}

/// Resolve everything held and emit it.
///
/// One query per child collection and one per parent table, for the whole group,
/// rather than two per changed row.
async fn flush_pending(
    cfg: &WalSourceConfig,
    pending: &mut crate::children::Pending,
    admin: &tokio_postgres::Client,
    tx: &tokio::sync::mpsc::Sender<ChangeEvent>,
    version: Option<u64>,
) -> Result<()> {
    if pending.is_empty() {
        return Ok(());
    }
    for table in pending.tables() {
        let (mut emitted, named) = pending.take(&table);
        let specs = cfg.children.get(&table).cloned().unwrap_or_default();

        let missing = crate::children::keys_needing_refetch(&emitted, named);
        if !missing.is_empty() {
            let pk_column = cfg.parent_pk_column(&table.0, &table.1);
            let found =
                crate::children::refetch_parents(admin, &table.0, &table.1, &pk_column, &missing)
                    .await?;
            for key in missing {
                // a parent that no longer exists emits nothing, as before: the
                // delete of the parent itself is what removes the document
                if let Some(doc) = found.get(&crate::children::key_lookup(&key)) {
                    emitted.push(pg2osync_core::event::RowChange {
                        schema: table.0.clone(),
                        table: table.1.clone(),
                        kind: pg2osync_core::event::RowKind::Insert {
                            pk: key,
                            doc: doc.clone(),
                        },
                        version: None,
                    });
                }
            }
        }

        let mut docs: Vec<(serde_json::Value, &mut serde_json::Value)> = emitted
            .iter_mut()
            .filter_map(|r| {
                let pk = r.pk().clone();
                r.doc_mut().map(|doc| (pk, doc))
            })
            .collect();
        crate::children::attach_children_batch(&mut docs, &specs, admin).await?;
        drop(docs);

        for mut change in emitted {
            change.version = version;
            send_change(tx, change).await?;
        }
    }
    pending.clear_seen();
    Ok(())
}

async fn send_change(
    tx: &tokio::sync::mpsc::Sender<ChangeEvent>,
    change: pg2osync_core::event::RowChange,
) -> Result<()> {
    tx.send(ChangeEvent::Row(change))
        .await
        .context("change channel closed")
}

impl WalSourceConfig {
    /// PK column of a parent table; resolved from catalog at bootstrap in the
    /// binary wiring and injected here. Defaults to "id" for standalone use.
    pub fn parent_pk_column(&self, schema: &str, table: &str) -> String {
        self.parent_pk_columns
            .get(&(schema.to_string(), table.to_string()))
            .cloned()
            .unwrap_or_else(|| "id".to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn source(tables: &[&str]) -> WalSource {
        WalSource::new(WalSourceConfig {
            host: "h".into(),
            port: 5432,
            user: "u".into(),
            password: String::new(),
            database: "d".into(),
            slot_name: "s".into(),
            publication: "p".into(),
            tables: tables.iter().map(|t| t.to_string()).collect(),
            start_lsn: None,
            admin_url: None,
            tls: Default::default(),
            children: HashMap::new(),
            child_parents: HashMap::new(),
            parent_pk_columns: HashMap::new(),
            key_columns: HashMap::new(),
            append_only: Default::default(),
            durable: None,
        })
    }

    #[test]
    fn a_table_the_publication_carries_but_the_config_does_not_is_not_ours() {
        let src = source(&["public.users"]);
        assert!(src.is_configured("public", "users"));
        // What a rename leaves behind: the publication follows the table, so its
        // rows keep arriving under a name nothing maps. Acting on them means
        // asking the engine for an index that does not exist.
        assert!(!src.is_configured("public", "users_old"));
        assert!(!src.is_configured("other", "users"));
    }

    #[test]
    fn a_child_table_is_ours_even_though_it_is_not_a_document() {
        let mut cfg = source(&["public.customers"]).cfg;
        cfg.child_parents.insert(
            ("public".into(), "orders".into()),
            ("public".into(), "customers".into()),
        );
        let src = WalSource::new(cfg);
        assert!(
            src.is_configured("public", "orders"),
            "a child row is not a document, but dropping it would stop its parent \
             from being re-read"
        );
    }
}
