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
    /// Highest durably-flushed position as reported by the engine's
    /// checkpoint task. Feedback to PostgreSQL is clamped to this: acking
    /// beyond it lets PG recycle WAL for events we have not indexed yet,
    /// which loses data on crash-restart.
    pub durable: Option<std::sync::Arc<std::sync::atomic::AtomicU64>>,
}

pub struct WalSource {
    cfg: WalSourceConfig,
}

impl WalSource {
    pub fn new(cfg: WalSourceConfig) -> Self {
        Self { cfg }
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
    /// NOTE: no tokio::select! around `client.recv()` — cancelling an in-flight
    /// protocol read risks losing frames. Shutdown is cooperative: the loop
    /// checks the watch flag between messages and the transport wakes the idle
    /// read periodically.
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
        // Only child re-fetch needs SQL, so without children the connection the
        // caller passed is left alone.
        let needs_admin = !self.cfg.children.is_empty() || !self.cfg.child_parents.is_empty();
        let admin_client = needs_admin.then_some(admin).flatten();
        if needs_admin && admin_client.is_none() {
            anyhow::bail!("nested children are configured but no SQL connection was provided");
        }
        tracing::info!(target: "pg2osync::source", "stream loop starting");

        loop {
            if *shutdown.borrow() {
                tracing::info!(target: "pg2osync::source", "shutdown requested");
                client.shutdown().await.ok();
                return Ok(());
            }
            let ev = match client.recv().await {
                Ok(Some(ev)) => ev,
                Ok(None) => {
                    tracing::warn!(target: "pg2osync::source", "stream ended normally");
                    return Ok(());
                }
                Err(e) => return Err(e).context("replication stream failed"),
            };
            tracing::trace!(target: "pg2osync::source", "raw event");
            match ev {
                ReplicationEvent::Begin { .. } => {
                    in_transaction = true;
                }
                ReplicationEvent::Commit {
                    end_lsn,
                    commit_time_micros,
                    ..
                } => {
                    in_transaction = false;
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
                                })
                                .await
                                .context("change channel closed")?;
                            }
                        }
                        msg @ (crate::pgoutput::Message::Insert(_)
                        | crate::pgoutput::Message::Update(_)
                        | crate::pgoutput::Message::Delete(_)) => {
                            if let Some(ChangeEvent::Row(row_change)) = self
                                .build_change_from_message(&msg, &relations, admin_client)
                                .await?
                            {
                                send_change(&tx, row_change).await?;
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
    /// Build a ChangeEvent for Insert/Update/Delete messages.
    ///
    /// Parent tables: children attached per config (nested documents).
    /// Child tables: the PARENT document is refetched so downstream stays fresh.
    async fn build_change_from_message(
        &self,
        msg: &crate::pgoutput::Message,
        relations: &HashMap<u32, crate::pgoutput::Relation>,
        admin: Option<&tokio_postgres::Client>,
    ) -> Result<Option<ChangeEvent>> {
        use crate::children::{attach_children, refetch_parent};
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
            _ => return Ok(None),
        };
        let Some(rel) = relations.get(&rel_id) else {
            // PG always sends RELATION before the first row of a relation on a
            // fresh stream; missing entries mean our own bookkeeping is broken
            return Err(anyhow::anyhow!("row for unknown relation oid {rel_id}"));
        };
        let parent_key = (rel.schema.clone(), rel.name.clone());

        // CHILD table: refresh the parent document instead of emitting the row
        if let Some(parent_key) = self.cfg.child_parents.get(&parent_key) {
            let Some(admin) = admin else {
                return Err(anyhow::anyhow!(
                    "child table configured but no SQL connection for parent refetch"
                ));
            };
            let child_specs = self
                .cfg
                .children
                .get(parent_key)
                .cloned()
                .unwrap_or_default();
            let cspec = child_specs
                .iter()
                .find(|s| s.schema == rel.schema && s.table == rel.name)
                .cloned()
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
            return match refetch_parent(
                admin,
                &parent_key.0,
                &parent_key.1,
                &fk_json,
                &self.cfg.parent_pk_column(&parent_key.0, &parent_key.1),
            )
            .await?
            {
                Some(mut doc) => {
                    attach_children(&mut doc, &fk_json, &child_specs, admin).await?;
                    Ok(Some(ChangeEvent::Row(pg2osync_core::event::RowChange {
                        schema: parent_key.0.clone(),
                        table: parent_key.1.clone(),
                        kind: pg2osync_core::event::RowKind::Insert { pk: fk_json, doc },
                    })))
                }
                None => Ok(None),
            };
        }

        let mut change = super::docbuild::build_row_change(rel, incoming)?;
        if let (Some(admin), Some(specs)) = (admin, self.cfg.children.get(&parent_key)) {
            let pk = change.pk().clone();
            if let Some(doc_mut) = change.doc_mut() {
                attach_children(doc_mut, &pk, specs, admin).await?;
            }
        }
        Ok(Some(ChangeEvent::Row(*Box::new(change))))
    }
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
