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

/// How many held rows and parent keys resolve in one go.
///
/// Bounds what one transaction can hold before it is resolved in pieces. Rows of
/// tables with no children never reach this: they are their own document and go
/// straight out.
const PENDING_FLUSH_ROWS: usize = 5_000;

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
        let mut pending = Pending::default();
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
        &self,
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

        let change = super::docbuild::build_row_change(rel, incoming)?;
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

/// Rows held back until their children can be resolved for the whole group.
///
/// A parent row keeps its decoded change, because its document comes from the
/// WAL tuple and only the arrays are missing. A child row keeps nothing but the
/// parent key it names, deduplicated — which is why a transaction touching a
/// thousand children of one parent holds one key rather than a thousand rows.
#[derive(Default)]
struct Pending {
    parents: HashMap<(String, String), Vec<pg2osync_core::event::RowChange>>,
    named: HashMap<(String, String), Vec<serde_json::Value>>,
    seen: std::collections::HashSet<(String, String, String)>,
}

impl Pending {
    fn hold_parent(&mut self, table: (String, String), change: pg2osync_core::event::RowChange) {
        self.parents.entry(table).or_default().push(change);
    }

    fn name_parent(&mut self, table: (String, String), key: serde_json::Value) {
        let id = (
            table.0.clone(),
            table.1.clone(),
            crate::children::key_lookup(&key),
        );
        if self.seen.insert(id) {
            self.named.entry(table).or_default().push(key);
        }
    }

    fn is_empty(&self) -> bool {
        self.parents.is_empty() && self.named.is_empty()
    }

    /// How much is held, for the cap that keeps one enormous transaction from
    /// living entirely in memory.
    fn len(&self) -> usize {
        self.parents.values().map(Vec::len).sum::<usize>()
            + self.named.values().map(Vec::len).sum::<usize>()
    }
}

/// Which named parent keys still need reading.
///
/// A key a parent row in this group already carries needs no re-read: that row's
/// document came from the WAL, which is the fresher of the two and saves a query.
fn keys_needing_refetch(
    rows: &[pg2osync_core::event::RowChange],
    named: Vec<serde_json::Value>,
) -> Vec<serde_json::Value> {
    let covered: std::collections::HashSet<String> = rows
        .iter()
        .map(|r| crate::children::key_lookup(r.pk()))
        .collect();
    named
        .into_iter()
        .filter(|k| !covered.contains(&crate::children::key_lookup(k)))
        .collect()
}

/// Resolve everything held and emit it.
///
/// One query per child collection and one per parent table, for the whole group,
/// rather than two per changed row.
async fn flush_pending(
    cfg: &WalSourceConfig,
    pending: &mut Pending,
    admin: &tokio_postgres::Client,
    tx: &tokio::sync::mpsc::Sender<ChangeEvent>,
    version: Option<u64>,
) -> Result<()> {
    if pending.is_empty() {
        return Ok(());
    }
    let tables: Vec<(String, String)> = pending
        .parents
        .keys()
        .chain(pending.named.keys())
        .cloned()
        .collect::<std::collections::HashSet<_>>()
        .into_iter()
        .collect();
    for table in tables {
        let mut emitted = pending.parents.remove(&table).unwrap_or_default();
        let named = pending.named.remove(&table).unwrap_or_default();
        let specs = cfg.children.get(&table).cloned().unwrap_or_default();

        let missing = keys_needing_refetch(&emitted, named);
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
    pending.seen.clear();
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
    use pg2osync_core::event::{RowChange, RowKind};
    use serde_json::json;

    fn parent_row(id: i64) -> RowChange {
        RowChange {
            schema: "public".into(),
            table: "customers".into(),
            kind: RowKind::Insert {
                pk: json!(id),
                doc: json!({"id": id}),
            },
            version: None,
        }
    }

    fn table() -> (String, String) {
        ("public".to_string(), "customers".to_string())
    }

    #[test]
    fn many_children_of_one_parent_hold_one_key() {
        // The whole point: 500 child rows on one parent must not become 500
        // held rows, 500 queries and 500 identical documents.
        let mut pending = Pending::default();
        for _ in 0..500 {
            pending.name_parent(table(), json!(7));
        }
        assert_eq!(pending.len(), 1);
    }

    #[test]
    fn the_same_key_on_two_parent_tables_is_two_keys() {
        let mut pending = Pending::default();
        pending.name_parent(("public".into(), "customers".into()), json!(1));
        pending.name_parent(("public".into(), "invoices".into()), json!(1));
        assert_eq!(pending.len(), 2, "different documents, same key value");
    }

    #[test]
    fn a_parent_row_in_the_group_saves_the_re_read() {
        // Its document came from the WAL, so re-reading the same key would be a
        // query for something already in hand.
        let needed = keys_needing_refetch(&[parent_row(1)], vec![json!(1), json!(2)]);
        assert_eq!(needed, vec![json!(2)]);
    }

    #[test]
    fn a_deleted_parent_still_suppresses_the_re_read() {
        // The delete is what removes the document; re-reading the key would find
        // nothing and emit nothing, so the query is pure waste.
        let deleted = RowChange {
            schema: "public".into(),
            table: "customers".into(),
            kind: RowKind::Delete { pk: json!(9) },
            version: None,
        };
        assert!(keys_needing_refetch(&[deleted], vec![json!(9)]).is_empty());
    }

    #[test]
    fn nothing_held_is_nothing_to_resolve() {
        let mut pending = Pending::default();
        assert!(pending.is_empty());
        pending.hold_parent(table(), parent_row(1));
        assert!(!pending.is_empty());
    }
}
