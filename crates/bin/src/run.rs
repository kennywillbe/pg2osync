//! Pipeline orchestration: bootstrap, initial load, live streaming.
//!
//! Everything source-specific stays behind `SourceKind`; the engine and sink
//! wiring below is identical for every source.

use anyhow::{Context as _, Result, bail};
use pg2osync_core::checkpoint::{Checkpoint, SOURCE_MYSQL, SOURCE_POSTGRES, StreamId};
use pg2osync_core::event::ChangeEvent;
use pg2osync_core::lsn::Lsn;
use pg2osync_core::sink::{IndexSpec, Sink};
use pg2osync_engine::mapping::{
    Constants, DurableLsn, JoinParent, JoinRule, Joins, ParentId, Projection, Projections, Rename,
    Renames, TableMapping, Transforms,
};
use pg2osync_engine::metrics::SharedMetrics;
use pg2osync_engine::{PipelineCtx, PositionRenderer};
use pg2osync_source::reconnect::ReconnectPolicy;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use tokio::sync::{mpsc, watch};

use crate::backfill::split_qualified;
use crate::config::AppConfig;

/// How far the CLI takes the pipeline.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    /// Create source-side objects and target indices, then exit.
    Bootstrap,
    /// Initial load plus continuous streaming.
    Run,
}

/// Channel depth between source and engine. Bounded channels are the whole
/// backpressure mechanism: a slow sink must stall the source, not buffer.
const EVENT_CHANNEL_DEPTH: usize = 10_000;

/// Depth of the separate channel the initial load feeds.
///
/// Two pulls in opposite directions: shallow so the copy paces itself to what
/// the target can absorb rather than piling up ahead of it, deep enough that a
/// bulk batch still fills from one pull instead of one row at a time. Change
/// events jump this queue regardless, so its depth costs the stream nothing.
pub const COPY_CHANNEL_DEPTH: usize = 2_000;

#[allow(clippy::too_many_arguments)]
pub async fn run_pipeline(
    cfg: AppConfig,
    source_url: String,
    admin_url: String,
    target_password: Option<String>,
    shutdown_rx: watch::Receiver<bool>,
    durable: DurableLsn,
    mode: Mode,
) -> Result<()> {
    let sink = build_sink(&cfg, target_password)?;
    check_rejection_policy(&cfg, sink.as_ref())?;
    for note in embedded_children_with_own_section(&cfg) {
        tracing::warn!(target: "pg2osync::run", "{note}");
    }
    let index_specs = index_specs(&cfg);

    match cfg.source.flavor.as_str() {
        "mysql" => {
            run_mysql(
                cfg,
                source_url,
                sink,
                index_specs,
                shutdown_rx,
                durable,
                mode,
            )
            .await
        }
        "postgres" | "postgresql" => {
            run_postgres(
                cfg,
                source_url,
                admin_url,
                sink,
                index_specs,
                shutdown_rx,
                durable,
                mode,
            )
            .await
        }
        other => bail!("unsupported source.flavor {other:?}; expected \"postgres\" or \"mysql\""),
    }
}

// ---------------------------------------------------------------- shared wiring

/// Refuse a quarantine policy the target cannot honour, before the pipeline
/// runs rather than at the first bad document.
///
/// Silently falling back to halting would be defensible; silently dropping the
/// document would not, and the difference between the two is exactly what a
/// permissive default here would blur.
/// Tables that are both an embedded child of another section and a section
/// of their own. The replication runner classifies such a table as somebody's
/// child and reads its rows only as a re-fetch of the owner, so the table's
/// own index gets the initial load and then never sees a change. Not refused:
/// a load-once index is a legitimate thing to want, and the dev suites use it
/// to compare an embedded object with the row itself — but it has to be said.
pub fn embedded_children_with_own_section(cfg: &AppConfig) -> Vec<String> {
    let mut notes = Vec::new();
    for (owner, tbl) in &cfg.sync {
        for child in &tbl.children {
            if let Some((key, own)) = cfg.sync.iter().find(|(_, t)| t.table == child.table) {
                notes.push(format!(
                    "[sync.{key}] {} is also an embedded child of [sync.{owner}]: the \
                     replication runner reads its rows only as a re-fetch of {}, so \
                     index {:?} receives the initial load and no streamed change",
                    child.table,
                    tbl.table,
                    own.index_name(key)
                ));
            }
        }
    }
    notes
}

pub fn check_rejection_policy(cfg: &AppConfig, sink: &dyn Sink) -> Result<()> {
    if cfg.engine.on_permanent_rejection == pg2osync_engine::RejectionPolicy::Quarantine
        && !sink.can_quarantine()
    {
        bail!(
            "[engine] on_permanent_rejection = \"quarantine\" needs a target that can record a \
             refused document, and {} cannot. Use \"halt\"",
            cfg.target.flavor
        );
    }
    if cfg.engine.write_concurrency > 1 && !sink.orders_by_version() {
        bail!(
            "[engine] write_concurrency = {} needs a target that decides between two writes of \
             one document by their version, and {} keeps whichever landed last. Leave it at 1",
            cfg.engine.write_concurrency,
            cfg.target.flavor
        );
    }
    Ok(())
}

pub fn build_sink(cfg: &AppConfig, target_password: Option<String>) -> Result<Arc<dyn Sink>> {
    let api_key = cfg
        .target
        .api_key_env
        .as_ref()
        .and_then(|k| std::env::var(k).ok());
    let retry = pg2osync_sink::RetryPolicy {
        max_attempts: cfg.engine.retry_max.max(1),
        base_backoff_ms: cfg.engine.retry_backoff_ms.max(1),
    };
    let sink: Arc<dyn Sink> = match cfg.target.flavor.as_str() {
        "elasticsearch" => Arc::new(pg2osync_sink::elasticsearch::ElasticsearchSink::new(
            pg2osync_sink::elasticsearch::ElasticsearchSinkConfig {
                url: cfg.target.url.clone(),
                username: cfg.target.username.clone(),
                password: target_password,
                api_key,
                tls_verify: cfg.target.tls_verify,
                retry,
            },
        )?),
        "meilisearch" => Arc::new(pg2osync_sink::meilisearch::MeilisearchSink::new(
            pg2osync_sink::meilisearch::MeilisearchSinkConfig {
                url: cfg.target.url.clone(),
                api_key: api_key.or(target_password),
                state_dir: cfg.target.state_dir.clone(),
            },
        )?),
        "opensearch" => Arc::new(pg2osync_sink::OpenSearchSink::new(
            pg2osync_sink::OpenSearchSinkConfig {
                url: cfg.target.url.clone(),
                username: cfg.target.username.clone(),
                password: target_password,
                tls_verify: cfg.target.tls_verify,
                retry,
            },
        )?),
        other => bail!(
            "unsupported target.flavor {other:?}; expected \"opensearch\", \
             \"elasticsearch\" or \"meilisearch\""
        ),
    };
    Ok(sink)
}

/// One spec per target index, in section order.
///
/// Several sections may feed one index — a join pair, or tables that each
/// declare their id — and `ensure_ready` creates it once, so the mapping is
/// kept from whichever section set it: config allows at most one to.
pub fn index_specs(cfg: &AppConfig) -> Vec<IndexSpec> {
    let mut specs: Vec<IndexSpec> = Vec::with_capacity(cfg.sync.len());
    for (key, tbl) in &cfg.sync {
        let name = tbl.index_name(key);
        match specs.iter_mut().find(|spec| spec.name == name) {
            Some(spec) => {
                if spec.mapping.is_none() {
                    spec.mapping = tbl.mapping.clone();
                }
            }
            None => specs.push(IndexSpec {
                name,
                mapping: tbl.mapping.clone(),
            }),
        }
    }
    specs
}

/// Every target index once, in section order. The per-index settings the
/// initial load suspends would otherwise be saved and restored twice for a
/// shared index, the second restore reading the already-suspended values.
pub fn index_names(cfg: &AppConfig) -> Vec<String> {
    index_specs(cfg).into_iter().map(|spec| spec.name).collect()
}

fn table_mapping(cfg: &AppConfig) -> TableMapping {
    TableMapping::from_pairs(cfg.sync.iter().map(|(key, tbl)| {
        let (schema, table) = split_qualified(&tbl.table);
        ((schema.to_string(), table.to_string()), tbl.index_name(key))
    }))
}

fn projections(cfg: &AppConfig) -> Projections {
    Projections::from_pairs(cfg.sync.values().filter_map(|tbl| {
        let (schema, table) = split_qualified(&tbl.table);
        let rule = match (&tbl.columns, tbl.exclude_columns.is_empty()) {
            (Some(cols), _) => Projection::Include(cols.clone()),
            (None, false) => Projection::Exclude(tbl.exclude_columns.clone()),
            (None, true) => return None,
        };
        Some(((schema.to_string(), table.to_string()), rule))
    }))
}

fn transforms(cfg: &AppConfig) -> Result<Transforms> {
    let mut pairs = Vec::new();
    for (key, tbl) in &cfg.sync {
        if tbl.transform.is_empty() {
            continue;
        }
        let (schema, table) = split_qualified(&tbl.table);
        let mut rules = HashMap::new();
        for (col, spec) in &tbl.transform {
            let parsed = spec
                .parse()
                .map_err(|e| anyhow::anyhow!("[sync.{key}.transform] {col}: {e}"))?;
            rules.insert(col.clone(), parsed);
        }
        pairs.push(((schema.to_string(), table.to_string()), rules));
    }
    Ok(Transforms::from_pairs(pairs))
}

fn renames(cfg: &AppConfig) -> Renames {
    Renames::from_pairs(cfg.sync.values().filter_map(|tbl| {
        let nested: HashMap<String, HashMap<String, String>> = tbl
            .children
            .iter()
            .filter(|child| !child.fields.is_empty())
            .map(|child| (child.field.clone(), child.fields.clone()))
            .collect();
        if tbl.fields.is_empty() && nested.is_empty() {
            return None;
        }
        let (schema, table) = split_qualified(&tbl.table);
        Some((
            (schema.to_string(), table.to_string()),
            Rename {
                columns: tbl.fields.clone(),
                nested,
            },
        ))
    }))
}

/// Rendered once here, so the engine inserts literals and never sees a
/// template. `validate` already refused a bad one, but `run` does not go
/// through `validate`, so the error is mapped rather than assumed away.
fn constants(cfg: &AppConfig) -> Result<Constants> {
    let mut pairs = Vec::new();
    for (key, tbl) in &cfg.sync {
        if tbl.constants.is_empty() {
            continue;
        }
        let (schema, table) = split_qualified(&tbl.table);
        let mut fields = HashMap::new();
        for (name, value) in &tbl.constants {
            let rendered = value
                .render(schema, table)
                .map_err(|e| anyhow::anyhow!("[sync.{key}.constants] {name}: {e}"))?;
            fields.insert(name.clone(), rendered);
        }
        pairs.push(((schema.to_string(), table.to_string()), fields));
    }
    Ok(Constants::from_pairs(pairs))
}

/// The key columns a table's id may be rendered from without a before-image,
/// as far as the engine can tell: it has no catalog, and the WAL path takes
/// the key from the replica identity rather than from configuration. The
/// startup check beside it is what makes this declaration true — where the
/// declared key and the database's own key disagree about a template, the
/// table is required to be REPLICA IDENTITY FULL, so the before-image (which
/// carries both) is what an id ever renders from.
fn pk_columns_for(tbl: &crate::config::TableSync) -> Vec<String> {
    vec![tbl.primary_key.clone().unwrap_or_else(|| "id".to_string())]
}

fn id_templates(cfg: &AppConfig) -> Result<pg2osync_engine::mapping::IdTemplates> {
    let mut pairs = Vec::new();
    for (key, tbl) in &cfg.sync {
        let Some(spec) = &tbl.id else { continue };
        let (schema, table) = split_qualified(&tbl.table);
        let template = pg2osync_engine::mapping::IdTemplate::parse(spec, &pk_columns_for(tbl))
            .map_err(|e| anyhow::anyhow!("[sync.{key}] id {spec:?} is not a usable id: {e}"))?;
        pairs.push(((schema.to_string(), table.to_string()), template));
    }
    Ok(pg2osync_engine::mapping::IdTemplates::from_pairs(pairs))
}

fn fan_outs(cfg: &AppConfig) -> Result<pg2osync_engine::mapping::FanOuts> {
    let mut pairs = Vec::new();
    for (key, tbl) in &cfg.sync {
        let Some(fan) = &tbl.fan_out else { continue };
        let (schema, table) = split_qualified(&tbl.table);
        let id = pg2osync_engine::mapping::IdTemplate::parse(&fan.id, &[]).map_err(|e| {
            anyhow::anyhow!(
                "[sync.{key}.fan_out] id {:?} is not a usable id: {e}",
                fan.id
            )
        })?;
        pairs.push((
            (schema.to_string(), table.to_string()),
            pg2osync_engine::mapping::FanOut {
                field: fan.field.clone(),
                id,
            },
        ));
    }
    Ok(pg2osync_engine::mapping::FanOuts::from_pairs(pairs))
}

/// The join rules, resolved per index: a child's rule carries its parent
/// section's relation name and id rule, because the parent's id is what the
/// child's routing has to reproduce from its own column.
fn joins(cfg: &AppConfig) -> Result<Joins> {
    let mut by_index: HashMap<String, Vec<(&String, &crate::config::TableSync)>> = HashMap::new();
    for (key, tbl) in &cfg.sync {
        by_index
            .entry(tbl.index_name(key))
            .or_default()
            .push((key, tbl));
    }
    let mut pairs = Vec::new();
    for (index, members) in &by_index {
        let parent = members.iter().find_map(|(key, tbl)| {
            let join = tbl.join.as_ref()?;
            join.parent.is_none().then_some((*key, *tbl, join))
        });
        for (key, tbl) in members {
            let Some(join) = &tbl.join else { continue };
            let parent = match &join.parent {
                None => None,
                Some(column) => {
                    let Some((parent_key, parent_tbl, parent_join)) = parent else {
                        bail!(
                            "[sync.{key}] join names a parent through {column}, but no other \
                             [sync.*] section writes index {index:?} as its parent"
                        );
                    };
                    let id = match &parent_tbl.id {
                        None => ParentId::Key,
                        Some(spec) => ParentId::Template(
                            pg2osync_engine::mapping::IdTemplate::parse(
                                spec,
                                &pk_columns_for(parent_tbl),
                            )
                            .map_err(|e| {
                                anyhow::anyhow!(
                                    "[sync.{parent_key}] id {spec:?} is not a usable id: {e}"
                                )
                            })?,
                        ),
                    };
                    Some(JoinParent {
                        column: column.clone(),
                        name: parent_join.name.clone(),
                        id,
                        key_column: pk_columns_for(tbl).contains(column),
                    })
                }
            };
            let (schema, table) = split_qualified(&tbl.table);
            pairs.push((
                (schema.to_string(), table.to_string()),
                JoinRule {
                    field: join.field.clone(),
                    name: join.name.clone(),
                    parent,
                },
            ));
        }
    }
    Ok(Joins::from_pairs(pairs))
}

/// The configured row filters, keyed by qualified table for the loaders.
pub fn table_filters(cfg: &AppConfig) -> Result<HashMap<String, pg2osync_core::filter::Filter>> {
    let mut out = HashMap::new();
    for (key, tbl) in &cfg.sync {
        let Some(spec) = &tbl.filter else { continue };
        let filter = pg2osync_core::filter::Filter::parse(spec)
            .map_err(|e| anyhow::anyhow!("[sync.{key}] where {spec:?}: {e}"))?;
        out.insert(tbl.table.clone(), filter);
    }
    Ok(out)
}

/// The same predicates keyed by (schema, table) for the engine.
fn filters(cfg: &AppConfig) -> Result<pg2osync_engine::mapping::Filters> {
    Ok(pg2osync_engine::mapping::Filters::from_pairs(
        table_filters(cfg)?.into_iter().map(|(qualified, filter)| {
            let (schema, table) = split_qualified(&qualified);
            ((schema.to_string(), table.to_string()), filter)
        }),
    ))
}

/// Metrics outlive any one attempt: they are created once and the endpoint is
/// served once, so a reconnect does not reset every counter or re-bind the port.
fn start_metrics(cfg: &AppConfig) -> Result<SharedMetrics> {
    let metrics = Arc::new(pg2osync_engine::metrics::Metrics::default());
    if cfg.metrics.enabled {
        let bind = cfg.metrics.bind.clone();
        let token = read_token(cfg.metrics.token_env.as_deref(), "metrics")?;
        let m = metrics.clone();
        tokio::spawn(async move { pg2osync_engine::metrics::serve(&bind, m, token).await });
    }
    Ok(metrics)
}

/// Report what the source's replication slots are holding, for as long as this
/// process runs.
///
/// Started outside the retry loop on purpose: a pipeline that cannot connect is
/// exactly when retention grows, so the number has to keep being reported while
/// the stream is being retried rather than only while it is healthy.
///
/// Its own connection, opened per poll. Holding one for the process's lifetime
/// would pin a backend against a database this is supposed to be cheap for, and
/// reconnecting is what lets it survive a source restart.
fn start_slot_watch(cfg: &AppConfig, admin_url: String, metrics: SharedMetrics) -> Result<()> {
    if !cfg.metrics.enabled || cfg.source.flavor == "mysql" {
        return Ok(());
    }
    let tls = cfg.tls_settings(&admin_url)?;
    tokio::spawn(async move {
        // `pg_replication_slots` is a shared-memory read, so this is cheap; the
        // interval is about not being noisy in a log when the source is down,
        // not about the query's cost.
        const EVERY: std::time::Duration = std::time::Duration::from_secs(30);
        let mut complained = false;
        loop {
            match slot_snapshot(&tls, &admin_url).await {
                Ok(slots) => {
                    complained = false;
                    metrics.set_slots(slots);
                }
                Err(e) => {
                    if !complained {
                        complained = true;
                        tracing::warn!(target: "pg2osync::metrics",
                            "cannot read what the replication slots are holding: {e}");
                    }
                }
            }
            tokio::time::sleep(EVERY).await;
        }
    });
    Ok(())
}

async fn slot_snapshot(
    tls: &pg2osync_source::tls::TlsSettings,
    admin_url: &str,
) -> Result<Vec<pg2osync_engine::metrics::SlotState>> {
    let client = pg2osync_source::tls::connect(tls, admin_url).await?;
    let slots = pg2osync_source::catalog::all_slot_pressure(&client).await?;
    Ok(slots
        .into_iter()
        .map(
            |(name, active, pressure)| pg2osync_engine::metrics::SlotState {
                name,
                active,
                retained_bytes: pressure.retained_bytes.map(|b| b.max(0) as u64),
                safe_wal_size: pressure.safe_wal_size.map(|b| b.max(0) as u64),
                wal_status: pressure.wal_status,
            },
        )
        .collect())
}

/// Resolve a bearer token from the environment.
///
/// The token is named by variable rather than written in the config so it never
/// has to live in a file that gets committed or mounted as a ConfigMap.
fn read_token(var: Option<&str>, endpoint: &str) -> Result<Option<String>> {
    match var {
        Some(key) => std::env::var(key).map(Some).map_err(|_| {
            anyhow::anyhow!("{endpoint}.token_env={key:?} is set but the variable is missing")
        }),
        None => Ok(None),
    }
}

/// Build the engine context for one attempt.
/// Which stream this configuration is, as the checkpoint and the load's progress
/// documents key themselves.
pub fn stream_id_for(cfg: &AppConfig) -> StreamId {
    if cfg.source.flavor == "mysql" {
        StreamId {
            source: SOURCE_MYSQL.into(),
            stream: cfg.source.server_id.to_string(),
            publication: String::new(),
        }
    } else {
        StreamId {
            source: SOURCE_POSTGRES.into(),
            stream: cfg.source.slot_name.clone(),
            publication: cfg.source.publication.clone(),
        }
    }
}

pub fn pipeline_ctx(
    cfg: &AppConfig,
    sink: Arc<dyn Sink>,
    metrics: SharedMetrics,
    ack_tx: watch::Sender<Option<Lsn>>,
    load_done_tx: watch::Sender<u64>,
) -> Result<Arc<PipelineCtx>> {
    Ok(Arc::new(PipelineCtx {
        sink,
        mapping: table_mapping(cfg),
        projections: projections(cfg),
        transforms: transforms(cfg)?,
        renames: renames(cfg),
        constants: constants(cfg)?,
        id_templates: id_templates(cfg)?,
        fan_outs: fan_outs(cfg)?,
        joins: joins(cfg)?,
        filters: filters(cfg)?,
        cfg: cfg.engine.clone(),
        ack_tx,
        load_done_tx,
        metrics,
    }))
}

/// Refuse to start a pipeline whose derived identity the source cannot supply.
///
/// An `id` that references columns outside the key, and a `fan_out` whose
/// deletes and update-diffs need the row's old values, both depend on the
/// whole old row arriving in the WAL — which only REPLICA IDENTITY FULL
/// guarantees. Finding that out on the first delete, at 3am, is worse than
/// naming the ALTER statement now, the same way child tables are checked.
/// A row filter adds no requirement of its own: a key-only id renders the
/// delete of a row that left the filter from the key, and the other cases are
/// the two above. A join child is the third: its delete has to name its
/// parent's shard, and its routing comes from the same place its id does.
async fn check_derived_identity_requirements(
    cfg: &AppConfig,
    admin: &tokio_postgres::Client,
) -> Result<()> {
    for (key, tbl) in &cfg.sync {
        if tbl.fan_out.is_none() && tbl.id.is_none() && tbl.join.is_none() {
            continue;
        }
        let (schema, table) = split_qualified(&tbl.table);
        let info = pg2osync_source::catalog::table_info(admin, schema, table)
            .await
            .with_context(|| format!("cannot inspect table {}", tbl.table))?;
        // "outside the key" is decided twice, because there are two keys to
        // ask: the one the engine sees at write time (`primary_key`, one
        // column defaulting to `id`) and the one the database enforces. With
        // a composite real key and a single declared one, a delete's scalar
        // key could otherwise be bound to a placeholder naming a different
        // column; requiring FULL where the two views disagree is what makes
        // the before-image — already unavoidable for anything outside a key
        // — the only thing an id is ever rendered from.
        let in_both_keys = |column: &str| {
            info.pk_columns.iter().any(|k| k == column)
                && pk_columns_for(tbl).iter().any(|k| k == column)
        };
        let needs_full = if tbl.fan_out.is_some() {
            true
        } else {
            match &tbl.id {
                Some(spec) => {
                    let from_catalog =
                        pg2osync_engine::mapping::IdTemplate::parse(spec, &info.pk_columns)
                            .map_err(|e| anyhow::anyhow!("[sync.{key}] id {spec:?}: {e}"))?;
                    let from_config =
                        pg2osync_engine::mapping::IdTemplate::parse(spec, &pk_columns_for(tbl))
                            .map_err(|e| anyhow::anyhow!("[sync.{key}] id {spec:?}: {e}"))?;
                    !(from_catalog.is_pk_only() && from_config.is_pk_only())
                }
                None => false,
            }
        };
        if let Some(join) = &tbl.join {
            match join.parent.as_deref() {
                // the child carries one column and renders the parent's id
                // from it alone, which a composite key's `col=val` list is not
                None if info.pk_columns.len() > 1 => bail!(
                    "[sync.{key}] {} is a join parent with a composite primary key ({}); a \
                     child carries one column and cannot render `{}`. A join parent needs a \
                     single-column key",
                    tbl.table,
                    info.pk_columns.join(", "),
                    info.pk_columns
                        .iter()
                        .map(|c| format!("{c}=…"))
                        .collect::<Vec<_>>()
                        .join(",")
                ),
                None => {}
                // the same two-key rule as an id: a bare key value is bound to
                // the parent column only when both views agree it is the key
                Some(column) if !in_both_keys(column) && info.relreplident != 'f' => bail!(
                    "[sync.{key}] {} has REPLICA IDENTITY '{}', but it is a join child whose \
                     {column} is not part of its key: a delete would carry no parent, so the \
                     document could not be routed to the shard that holds it. \
                     Run: ALTER TABLE {} REPLICA IDENTITY FULL",
                    tbl.table,
                    info.relreplident,
                    tbl.table
                ),
                Some(_) => {}
            }
        }
        if !needs_full {
            continue;
        }
        if info.relreplident != 'f' {
            bail!(
                "[sync.{key}] {} has REPLICA IDENTITY '{}', but its derived identity needs \
                 the whole old row: deletes and updates could not find the documents they \
                 replace. Run: ALTER TABLE {} REPLICA IDENTITY FULL",
                tbl.table,
                info.relreplident,
                tbl.table
            );
        }
        if tbl.fan_out.is_some() {
            check_fan_out_column(admin, key, tbl).await?;
        }
    }
    Ok(())
}

/// A `fan_out` field must hold an array: a PostgreSQL array column, or a
/// jsonb/json whose value is one. Anything else fails per row at write time
/// with a permanent rejection, which is a slow way of saying what the
/// catalog can say at startup.
async fn check_fan_out_column(
    admin: &tokio_postgres::Client,
    key: &str,
    tbl: &crate::config::TableSync,
) -> Result<()> {
    let fan = tbl
        .fan_out
        .as_ref()
        .expect("checked by the caller, which only calls this for fanned tables");
    let dtype: Option<String> = admin
        .query_opt(
            "SELECT format_type(a.atttypid, a.atttypmod) FROM pg_attribute a \
             WHERE a.attrelid = to_regclass($1) AND a.attname = $2 \
               AND a.attnum > 0 AND NOT a.attisdropped",
            &[&tbl.table, &fan.field],
        )
        .await?
        .map(|r| r.get(0));
    match dtype.as_deref() {
        None => bail!(
            "[sync.{key}.fan_out] table {} has no column {:?}",
            tbl.table,
            fan.field
        ),
        // json/jsonb is accepted on the promise that the *value* is an array,
        // which is enforced per row when the documents are expanded
        Some(t) if t.ends_with("[]") || t == "json" || t == "jsonb" => Ok(()),
        Some(t) => bail!(
            "[sync.{key}.fan_out] column {} is of type {t:?}; fan-out needs a PostgreSQL \
             array column or a jsonb column holding a JSON array",
            fan.field
        ),
    }
}

/// Start the read-your-writes endpoint, if the operator asked for one.
///
/// Like the metrics endpoint it is started once, outside the retry loop: the
/// acknowledged-position channel it watches has to survive a reconnect.
fn start_api(
    cfg: &AppConfig,
    acked: watch::Receiver<Option<Lsn>>,
    parse_position: pg2osync_engine::PositionParser,
    render_position: PositionRenderer,
    sink: Arc<dyn Sink>,
    nudge: Option<pg2osync_engine::api::StreamNudge>,
    current_position: Option<pg2osync_engine::api::CurrentPosition>,
) -> Result<()> {
    if !cfg.api.enabled {
        return Ok(());
    }
    let token = read_token(cfg.api.token_env.as_deref(), "api")?;
    if token.is_none() && !pg2osync_engine::http::is_loopback(&cfg.api.bind) {
        tracing::warn!(target: "pg2osync::api",
            "the endpoint is bound to {} without a token; anything that can \
             reach it can query the pipeline position", cfg.api.bind);
    }
    let api_cfg = pg2osync_engine::api::ApiConfig {
        bind: cfg.api.bind.clone(),
        token,
        indices: index_names(cfg),
    };
    tokio::spawn(async move {
        pg2osync_engine::api::serve(
            api_cfg,
            acked,
            parse_position,
            render_position,
            sink,
            nudge,
            current_position,
        )
        .await
    });
    Ok(())
}

/// How one streaming attempt ended.
enum AttemptEnd {
    /// The shutdown signal fired; nothing to retry.
    Shutdown,
    /// The stream stopped on its own. Streaming for `streamed_for` first is
    /// what separates a healthy connection that dropped from a crash loop.
    Ended { streamed_for: std::time::Duration },
}

/// Run attempts until one shuts down cleanly or the policy gives up.
///
/// Every attempt rebuilds the channels and the engine task from scratch. That
/// is deliberate rather than wasteful: a partially buffered transaction is
/// invalid once the stream repositions, and tearing the pipeline down is what
/// discards it.
async fn stream_with_reconnect<A, F>(
    policy: ReconnectPolicy,
    metrics: SharedMetrics,
    shutdown: &watch::Receiver<bool>,
    mut attempt: A,
) -> Result<()>
where
    A: FnMut() -> F,
    F: std::future::Future<Output = Result<AttemptEnd>>,
{
    let mut failures = 0u32;
    loop {
        metrics.set_source_connected(true);
        let outcome = attempt().await;
        metrics.set_source_connected(false);

        if *shutdown.borrow() {
            return Ok(());
        }

        let (error, streamed_for) = match outcome {
            Ok(AttemptEnd::Shutdown) => return Ok(()),
            // the source returning without an error, while nobody asked it to
            // stop, still means the stream is gone
            Ok(AttemptEnd::Ended { streamed_for }) => (
                anyhow::anyhow!("the source closed the stream"),
                streamed_for,
            ),
            Err(e) => (e, std::time::Duration::ZERO),
        };

        if policy.attempt_recovered(streamed_for) {
            failures = 0;
        }
        if !policy.should_retry(failures) {
            return Err(error.context(format!(
                "giving up after {} consecutive source failures",
                failures + 1
            )));
        }

        let delay = policy.delay_for(failures);
        failures += 1;
        metrics.reconnects_total.fetch_add(1, Ordering::Relaxed);
        tracing::warn!(target: "pg2osync::run",
            "source stream failed ({error:#}); reconnecting in {:.1}s (attempt {failures})",
            delay.as_secs_f64());
        tokio::time::sleep(delay).await;
    }
}

/// A stored checkpoint is only usable when it belongs to this exact stream.
fn usable_checkpoint(stored: Option<Checkpoint>, expected: &StreamId) -> Option<Checkpoint> {
    let stored = stored?;
    if stored.stream.source != expected.source || stored.stream.stream != expected.stream {
        tracing::warn!(target: "pg2osync::run",
            "checkpoint belongs to {}/{} but this run is {}/{}; ignoring it and \
             running a full initial load",
            stored.stream.source, stored.stream.stream, expected.source, expected.stream);
        return None;
    }
    Some(stored)
}

// ------------------------------------------------------------------- PostgreSQL

#[allow(clippy::too_many_arguments)]
async fn run_postgres(
    cfg: AppConfig,
    source_url: String,
    admin_url: String,
    sink: Arc<dyn Sink>,
    index_specs: Vec<IndexSpec>,
    shutdown_rx: watch::Receiver<bool>,
    durable: DurableLsn,
    mode: Mode,
) -> Result<()> {
    use pg2osync_source::runner::{WalSource, WalSourceConfig};

    let polling = cfg.source.mode == "poll";
    let tls = cfg.tls_settings(&source_url)?;
    tracing::info!(target: "pg2osync::run", "source sslmode={}", tls.mode.as_str());
    let admin = pg2osync_source::tls::connect(&tls, &admin_url)
        .await
        .context("cannot connect to source PostgreSQL")?;

    let mut children = child_specs_for(&cfg)?;
    resolve_child_order(&mut children, &admin).await?;
    let mut tables: Vec<String> = cfg.sync.values().map(|t| t.table.clone()).collect();
    // child tables must join the publication or their changes never reach us
    for tbl in cfg.sync.values() {
        for child in &tbl.children {
            if !tables.contains(&child.table) {
                tables.push(child.table.clone());
            }
        }
    }

    let src_cfg: WalSourceConfig = wal_config(
        &cfg,
        &source_url,
        &admin_url,
        &tables,
        &children,
        &durable,
        &tls,
    )?;
    let source = WalSource::new(src_cfg.clone());

    if !polling {
        source.bootstrap(&admin).await?;
        warn_on_child_replica_identity(&cfg, &admin).await?;
        check_derived_identity_requirements(&cfg, &admin).await?;
    }
    sink.ensure_ready(&index_specs).await?;

    if mode == Mode::Bootstrap {
        println!("✓ source objects and target indices are ready");
        if !polling {
            println!(
                "  publication: {}\n  slot: {}",
                cfg.source.publication, cfg.source.slot_name
            );
        }
        return Ok(());
    }

    let stream_id = stream_id_for(&cfg);
    let render: PositionRenderer = Arc::new(|token| Lsn(token).to_string());
    let parse: pg2osync_engine::PositionParser =
        Arc::new(|text| text.trim().parse::<Lsn>().ok().map(|lsn| lsn.0));
    let metrics = start_metrics(&cfg)?;
    start_slot_watch(&cfg, admin_url.clone(), metrics.clone())?;
    let (ack_tx, ack_rx) = watch::channel(None);
    let nudge: Option<pg2osync_engine::api::StreamNudge> = if cfg.api.enabled {
        let url = admin_url.clone();
        let tls = tls.clone();
        Some(Arc::new(move || {
            let url = url.clone();
            let tls = tls.clone();
            Box::pin(async move {
                if let Err(e) = emit_stream_marker(&tls, &url).await {
                    tracing::warn!(target: "pg2osync::api", "cannot advance the stream: {e:#}");
                }
            })
        }))
    } else {
        None
    };
    let current_position: Option<pg2osync_engine::api::CurrentPosition> = if cfg.api.enabled {
        let url = admin_url.clone();
        let tls = tls.clone();
        Some(Arc::new(move || {
            let url = url.clone();
            let tls = tls.clone();
            Box::pin(async move { read_current_lsn(&tls, &url).await })
        }))
    } else {
        None
    };
    start_api(
        &cfg,
        ack_rx,
        parse,
        render.clone(),
        sink.clone(),
        nudge,
        current_position,
    )?;

    // setup is done; each attempt opens the SQL connection it needs, so holding
    // this one open would be a third connection doing nothing
    drop(admin);

    stream_with_reconnect(
        cfg.source.reconnect_policy(),
        metrics.clone(),
        &shutdown_rx,
        || {
            attempt_postgres(
                &cfg,
                &source_url,
                &admin_url,
                &tls,
                &children,
                &src_cfg,
                sink.clone(),
                metrics.clone(),
                ack_tx.clone(),
                stream_id.clone(),
                render.clone(),
                durable.clone(),
                shutdown_rx.clone(),
                polling,
            )
        },
    )
    .await
}

/// Run an initial load with the target's per-load settings relaxed.
///
/// Restoring is not optional and not conditional on success: an index left with
/// refresh suspended looks empty to every search against it, and nothing would
/// ever put it back.
async fn with_bulk_load_settings<F, T>(sink: &Arc<dyn Sink>, cfg: &AppConfig, load: F) -> Result<T>
where
    F: std::future::Future<Output = Result<T>>,
{
    let saved = sink.begin_bulk_load(&index_names(cfg)).await?;
    let result = load.await;
    // the tail still in the event channel is written after this, which is a
    // batch or two out of millions
    if let Err(e) = sink.end_bulk_load(&saved).await {
        tracing::error!(target: "pg2osync::run",
            "could not restore index settings after the initial load: {e}. \
             Set refresh_interval and number_of_replicas back by hand");
    }
    result
}

/// One PostgreSQL streaming attempt: decide where to resume, build the
/// pipeline, load if needed, stream until it stops.
///
/// The checkpoint is read here rather than once at startup, so a reconnect
/// resumes from wherever the last attempt actually got to.
#[allow(clippy::too_many_arguments)]
async fn attempt_postgres(
    cfg: &AppConfig,
    source_url: &str,
    admin_url: &str,
    tls: &pg2osync_source::tls::TlsSettings,
    children: &HashMap<(String, String), Vec<pg2osync_source::children::ChildSpec>>,
    src_cfg: &pg2osync_source::runner::WalSourceConfig,
    sink: Arc<dyn Sink>,
    metrics: SharedMetrics,
    ack_tx: watch::Sender<Option<Lsn>>,
    stream_id: StreamId,
    render: PositionRenderer,
    durable: DurableLsn,
    shutdown_rx: watch::Receiver<bool>,
    polling: bool,
) -> Result<AttemptEnd> {
    use pg2osync_source::runner::WalSource;

    // One SQL connection per attempt, shared by the checkpoint check, the
    // initial load and child re-fetch. Opening it here rather than once at
    // startup means a connection lost during an outage is replaced by the
    // reconnect instead of staying dead for the life of the process.
    let admin = pg2osync_source::tls::connect(tls, admin_url)
        .await
        .context("cannot connect to source PostgreSQL")?;
    let admin = &admin;

    // Poll mode has no source position to resume from, so a leftover WAL
    // checkpoint would skip rows changed while the process was down.
    let stored = if polling {
        None
    } else {
        usable_checkpoint(sink.read_checkpoint(&stream_id).await?, &stream_id)
    };
    let mut resume_from = stored.map(|c| Lsn(c.token));
    if !polling {
        let slot_lsn =
            pg2osync_source::catalog::confirmed_flush_lsn(admin, &cfg.source.slot_name).await?;
        // A checkpoint behind the slot's replay position is unusable: streaming
        // resumes at the slot position, so the gap between them would be lost.
        if let (Some(cp), Some(slot)) = (resume_from, slot_lsn)
            && cp < slot
        {
            tracing::warn!(target: "pg2osync::run",
                "checkpoint {cp} predates slot position {slot}; running a full \
                 initial load to avoid a gap");
            resume_from = None;
        }
    }
    // A checkpoint is not proof that the load finished: it says where streaming
    // got to, and with a load recording its own progress the two are separate
    // facts. Trusting the checkpoint alone is what silently skips a load.
    let load_pending = pg2osync_core::load::unfinished(
        sink.as_ref(),
        &stream_id,
        cfg.sync.values().map(|t| t.table.as_str()),
    )
    .await?;
    match (&resume_from, load_pending) {
        (Some(lsn), false) => {
            tracing::info!(target: "pg2osync::run", "resuming from checkpoint {lsn}")
        }
        (Some(lsn), true) => tracing::info!(target: "pg2osync::run",
            "resuming from checkpoint {lsn}, with an initial load still to finish"),
        (None, _) => tracing::info!(target: "pg2osync::run", "no usable checkpoint; initial load"),
    }

    let (events_tx, events_rx) = mpsc::channel::<ChangeEvent>(EVENT_CHANNEL_DEPTH);
    let (copy_tx, copy_rx) = mpsc::channel::<ChangeEvent>(COPY_CHANNEL_DEPTH);
    let (load_done_tx, load_done_rx) = watch::channel(0u64);
    let load_sink = sink.clone();
    let load_stream_id = stream_id.clone();
    let ctx = pipeline_ctx(cfg, sink, metrics, ack_tx, load_done_tx)?;
    let engine = spawn_engine(
        events_rx,
        copy_rx,
        ctx,
        stream_id,
        render,
        durable,
        shutdown_rx.clone(),
    );

    // The load runs *beside* the stream, not before it. Loading first means
    // nothing acknowledges a position for the load's whole duration, so the
    // slot's retained WAL grows monotonically until PostgreSQL invalidates it —
    // and an invalidated slot forces the full reload the load was trying to
    // finish. What makes the overlap safe is that every document carries the
    // position it became visible at, so a copied row that was already stale
    // loses to the streamed change regardless of which arrives first.
    let load = async {
        // moved in, so the copy channel closes when the load is done and the
        // engine can tell the difference between "paused" and "finished"
        let copy_tx = copy_tx;
        if resume_from.is_none() || load_pending {
            with_bulk_load_settings(&load_sink, cfg, async {
                crate::backfill::run(
                    cfg,
                    admin_url,
                    tls,
                    admin,
                    children,
                    copy_tx,
                    load_sink.as_ref(),
                    &load_stream_id,
                    load_done_rx,
                    &pg2osync_core::load::LoadScope::initial_load()
                        .with_table_filters(table_filters(cfg)?),
                )
                .await
            })
            .await
        } else {
            Ok(())
        }
    };

    let started = std::time::Instant::now();
    let stream = async {
        if polling {
            let mut poll =
                pg2osync_source::poll::PollSource::new(poll_config(cfg, source_url, tls.clone())?);
            poll.stream(events_tx, shutdown_rx.clone()).await
        } else {
            let mut source = WalSource::new(src_cfg.clone());
            source
                .stream(events_tx, shutdown_rx.clone(), Some(admin))
                .await
        }
    };
    // Either failing abandons the other: a stream error is a reconnect, and the
    // load picks up from its recorded progress on the next attempt.
    let result = futures::future::try_join(load, stream).await.map(|_| ());
    // dropping both senders above is what lets the engine drain and exit
    let _ = engine.await;
    result?;

    Ok(if *shutdown_rx.borrow() {
        AttemptEnd::Shutdown
    } else {
        AttemptEnd::Ended {
            streamed_for: started.elapsed(),
        }
    })
}

/// Write a logical decoding message so the stream carries a position past the
/// caller's.
///
/// PostgreSQL omits transactions that touch no published table, so on a quiet
/// database the position a caller reads from `pg_current_wal_lsn()` is one the
/// pipeline would never see. A message is the table-less way to produce one —
/// no schema of ours in the user's database, no DDL privileges required.
async fn emit_stream_marker(
    tls: &pg2osync_source::tls::TlsSettings,
    admin_url: &str,
) -> Result<()> {
    let client = pg2osync_source::tls::connect(tls, admin_url).await?;
    client
        .execute(
            "SELECT pg_logical_emit_message(false, 'pg2osync', 'sync')",
            &[],
        )
        .await
        .context("emitting a logical decoding message failed")?;
    Ok(())
}

/// The source's current WAL position, read on pg2osync's own connection.
async fn read_current_lsn(tls: &pg2osync_source::tls::TlsSettings, admin_url: &str) -> Option<u64> {
    let client = pg2osync_source::tls::connect(tls, admin_url).await.ok()?;
    let row = client
        .query_one("SELECT pg_current_wal_lsn()::text", &[])
        .await
        .ok()?;
    row.get::<_, String>(0).parse::<Lsn>().ok().map(|lsn| lsn.0)
}

fn wal_config(
    cfg: &AppConfig,
    source_url: &str,
    admin_url: &str,
    tables: &[String],
    children: &HashMap<(String, String), Vec<pg2osync_source::children::ChildSpec>>,
    durable: &DurableLsn,
    tls: &pg2osync_source::tls::TlsSettings,
) -> Result<pg2osync_source::runner::WalSourceConfig> {
    let url = url::Url::parse(source_url).context("source url is not a valid URL")?;
    let mut child_parents = HashMap::new();
    let mut parent_pk_columns = HashMap::new();
    for tbl in cfg.sync.values() {
        let (ps, pt) = split_qualified(&tbl.table);
        parent_pk_columns.insert(
            (ps.to_string(), pt.to_string()),
            tbl.primary_key.clone().unwrap_or_else(|| "id".into()),
        );
        for child in &tbl.children {
            let (cs, ct) = split_qualified(&child.table);
            child_parents.insert(
                (cs.to_string(), ct.to_string()),
                (ps.to_string(), pt.to_string()),
            );
        }
    }
    Ok(pg2osync_source::runner::WalSourceConfig {
        host: url.host_str().unwrap_or("localhost").into(),
        port: url.port().unwrap_or(5432),
        user: url.username().into(),
        password: url.password().unwrap_or_default().into(),
        database: url.path().trim_start_matches('/').to_string(),
        slot_name: cfg.source.slot_name.clone(),
        publication: cfg.source.publication.clone(),
        tables: tables.to_vec(),
        start_lsn: None,
        // Feedback to PostgreSQL is clamped to this: acknowledging beyond the
        // durable checkpoint lets PG recycle WAL we have not indexed yet, which
        // loses data on crash-restart.
        durable: Some(durable.0.clone()),
        admin_url: Some(admin_url.to_string()),
        tls: tls.clone(),
        children: children.clone(),
        child_parents,
        parent_pk_columns,
    })
}

fn poll_config(
    cfg: &AppConfig,
    source_url: &str,
    tls: pg2osync_source::tls::TlsSettings,
) -> Result<pg2osync_source::poll::PollSourceConfig> {
    Ok(pg2osync_source::poll::PollSourceConfig {
        url: source_url.to_string(),
        tables: cfg
            .sync
            .values()
            .map(|t| pg2osync_source::poll::PollTable {
                qualified: t.table.clone(),
                poll_column: t
                    .poll_column
                    .clone()
                    .unwrap_or_else(|| cfg.source.poll_column.clone()),
                pk_columns: vec![t.primary_key.clone().unwrap_or_else(|| "id".into())],
                soft_delete: t.soft_delete.clone(),
            })
            .collect(),
        interval_secs: cfg.source.poll_interval_secs,
        page_size: cfg.source.poll_page_size,
        tls,
    })
}

/// Ask the catalogue what to order each child collection by.
///
/// Without an order the embedded array is a set in arbitrary order, so the
/// initial load and a streamed re-fetch can hold the same children differently
/// and a re-snapshot changes a document for no reason. With `max_rows` it decides
/// *which* children are kept, so two runs would otherwise keep different ones.
pub async fn resolve_child_order(
    children: &mut HashMap<(String, String), Vec<pg2osync_source::children::ChildSpec>>,
    admin: &tokio_postgres::Client,
) -> Result<()> {
    for specs in children.values_mut() {
        for spec in specs.iter_mut() {
            pg2osync_source::children::resolve_order(spec, admin).await?;
        }
    }
    Ok(())
}

/// Ask MySQL's catalogue what to order each child collection by.
///
/// The same reasoning as the PostgreSQL counterpart: without an order the array
/// is a set, so the load and a re-fetch can embed it differently, and with
/// `max_rows` they would keep different rows.
pub async fn resolve_mysql_child_order(
    children: &mut HashMap<(String, String), Vec<pg2osync_core::children::ChildSpec>>,
    conn: &mut pg2osync_source_mysql::connection::MySqlConnection,
) -> Result<()> {
    for specs in children.values_mut() {
        for spec in specs.iter_mut() {
            pg2osync_source_mysql::children::resolve_order(spec, conn).await?;
        }
    }
    Ok(())
}

pub fn child_specs_for(
    cfg: &AppConfig,
) -> Result<HashMap<(String, String), Vec<pg2osync_source::children::ChildSpec>>> {
    let mut map: HashMap<_, Vec<_>> = HashMap::new();
    for tbl in cfg.sync.values() {
        let (schema, table) = split_qualified(&tbl.table);
        for child in &tbl.children {
            let mut spec = pg2osync_source::children::ChildSpec::new(
                &child.table,
                &child.field,
                &child.foreign_key,
                &tbl.primary_key.clone().unwrap_or_else(|| "id".into()),
            )?;
            spec.max_rows = child.max_rows;
            map.entry((schema.to_string(), table.to_string()))
                .or_default()
                .push(spec);
        }
    }
    Ok(map)
}

/// Deletes on a child table carry no foreign key under the default replica
/// identity, so the parent cannot be located. Warn before it happens.
async fn warn_on_child_replica_identity(
    cfg: &AppConfig,
    admin: &tokio_postgres::Client,
) -> Result<()> {
    for tbl in cfg.sync.values() {
        for child in &tbl.children {
            let (schema, table) = split_qualified(&child.table);
            let info = pg2osync_source::catalog::table_info(admin, schema, table)
                .await
                .with_context(|| format!("cannot inspect child table {}", child.table))?;
            if info.relreplident != 'f' {
                tracing::warn!(target: "pg2osync::run",
                    "child table {} has REPLICA IDENTITY '{}': DELETEs on it cannot \
                     refresh the parent document. Run: ALTER TABLE {} REPLICA IDENTITY FULL",
                    child.table, info.relreplident, child.table);
            }
        }
    }
    Ok(())
}

// ------------------------------------------------------------------------ MySQL

#[allow(clippy::too_many_arguments)]
async fn run_mysql(
    cfg: AppConfig,
    source_url: String,
    sink: Arc<dyn Sink>,
    index_specs: Vec<IndexSpec>,
    shutdown_rx: watch::Receiver<bool>,
    durable: DurableLsn,
    mode: Mode,
) -> Result<()> {
    use pg2osync_source_mysql::runner::MySqlSource;

    let src_cfg = mysql_config_for(&cfg, &source_url)?;
    let source = MySqlSource::new(src_cfg);
    let mut admin = source.admin_connection().await?;
    source.bootstrap(&mut admin).await?;
    sink.ensure_ready(&index_specs).await?;

    if mode == Mode::Bootstrap {
        println!("✓ MySQL prerequisites met and target indices are ready");
        return Ok(());
    }

    let stream_id = stream_id_for(&cfg);
    let metrics = start_metrics(&cfg)?;
    let (ack_tx, ack_rx) = watch::channel(None);
    // The generation the pipeline is versioning in, shared with the endpoints
    // outside the retry loop. They speak binlog coordinates, the pipeline
    // speaks versions, and after a failover those differ by exactly this.
    let version_base = Arc::new(std::sync::atomic::AtomicU64::new(0));
    // the binlog prefix is only known once a position has been read, so the
    // renderer and parser are built from the source's own vocabulary
    let api_prefix = mysql_binlog_prefix(&cfg, &source_url).await?;
    {
        let prefix = api_prefix.clone();
        let render: PositionRenderer = {
            let base = version_base.clone();
            Arc::new(move |token| {
                let base = base.load(std::sync::atomic::Ordering::Relaxed);
                pg2osync_source_mysql::catalog::position_text(&prefix, token.saturating_sub(base))
            })
        };
        let parse: pg2osync_engine::PositionParser = {
            let base = version_base.clone();
            Arc::new(move |text| {
                let (file, pos) = pg2osync_source_mysql::catalog::parse_position(text)?;
                Some(
                    base.load(std::sync::atomic::Ordering::Relaxed)
                        .saturating_add(pg2osync_source_mysql::catalog::position_token(&file, pos)),
                )
            })
        };
        let current_position: Option<pg2osync_engine::api::CurrentPosition> = if cfg.api.enabled {
            let cfg = cfg.clone();
            let url = source_url.clone();
            Some(Arc::new(move || {
                let cfg = cfg.clone();
                let url = url.clone();
                Box::pin(async move { read_current_binlog_position(&cfg, &url).await })
            }))
        } else {
            None
        };
        start_api(
            &cfg,
            ack_rx,
            parse,
            render,
            sink.clone(),
            None,
            current_position,
        )?;
    }

    stream_with_reconnect(
        cfg.source.reconnect_policy(),
        metrics.clone(),
        &shutdown_rx,
        || {
            attempt_mysql(
                &cfg,
                &source_url,
                AttemptWiring {
                    sink: sink.clone(),
                    metrics: metrics.clone(),
                    ack_tx: ack_tx.clone(),
                    stream_id: stream_id.clone(),
                    durable: durable.clone(),
                    shutdown_rx: shutdown_rx.clone(),
                    version_base: version_base.clone(),
                },
            )
        },
    )
    .await
}

/// One MySQL streaming attempt.
///
/// Each attempt opens its own administrative connection: whatever broke the
/// binlog stream — a restart, a failover, a killed thread — usually took that
/// connection with it.
/// What one attempt needs beyond the configuration.
struct AttemptWiring {
    sink: Arc<dyn Sink>,
    metrics: SharedMetrics,
    ack_tx: watch::Sender<Option<Lsn>>,
    stream_id: StreamId,
    durable: DurableLsn,
    shutdown_rx: watch::Receiver<bool>,
    /// Where the endpoints learn which generation the pipeline is in.
    version_base: Arc<std::sync::atomic::AtomicU64>,
}

async fn attempt_mysql(
    cfg: &AppConfig,
    source_url: &str,
    wiring: AttemptWiring,
) -> Result<AttemptEnd> {
    use pg2osync_source_mysql::catalog as mysql_catalog;
    use pg2osync_source_mysql::runner::MySqlSource;

    let AttemptWiring {
        sink,
        metrics,
        ack_tx,
        stream_id,
        durable,
        shutdown_rx,
        version_base,
    } = wiring;
    let source = MySqlSource::new(mysql_config_for(cfg, source_url)?);
    let mut admin = source.admin_connection().await?;

    let stored = usable_checkpoint(sink.read_checkpoint(&stream_id).await?, &stream_id);
    let stored_position = stored
        .as_ref()
        .and_then(|c| mysql_catalog::parse_stored_position(&c.position));
    let resume = stored_position.as_ref().map(|p| (p.file.clone(), p.pos));
    // A checkpoint says where streaming got to; a load records its own progress.
    // Trusting the checkpoint alone is what silently skips an unfinished load.
    let load_pending = pg2osync_core::load::unfinished(
        sink.as_ref(),
        &stream_id,
        cfg.sync.values().map(|t| t.table.as_str()),
    )
    .await?;
    // The position token is now a document version as well as a checkpoint, and
    // a version at the target only ever goes up. So a history that restarted —
    // RESET BINARY LOGS AND GTIDS, or a different server behind the same
    // address — would have every write from the new, lower numbering silently
    // rejected as a version conflict, leaving the index quietly stale. Reloading
    // would not fix it either: the old versions are in the target, not here.
    // How many generations of coordinate space this pipeline has lived through.
    // The version is `base + coordinate`, so a failover onto a server whose
    // numbering is lower does not push versions backwards — which would have
    // the target refuse every write and leave the index quietly stale.
    let mut base = stored_position.as_ref().map(|p| p.base).unwrap_or(0);
    let gtid_support = mysql_catalog::gtid_support(&mut admin).await?;
    let mut gtid_resume = stored_position.as_ref().and_then(|p| p.gtid.clone());
    // What the server had before this stream began, for a first run that has no
    // checkpoint to resume from but still has to leave one that can be resumed.
    let mut gtid_baseline: Option<pg2osync_source_mysql::gtid::GtidPosition> = None;
    if let Some((file, pos)) = &resume {
        let (current_file, current_pos) = mysql_catalog::master_position(&mut admin).await?;
        let stored_token = stored_position
            .as_ref()
            .map(mysql_catalog::StoredPosition::token)
            .unwrap_or(0);
        if base.saturating_add(mysql_catalog::position_token(&current_file, current_pos))
            < stored_token
        {
            // The margin has to clear the highest version already written but
            // not yet acknowledged, which one unacknowledged transaction bounds
            // to a few file rotations. A thousand rotations of headroom is far
            // past that, and still leaves millions of generations in a u64.
            const GENERATION_MARGIN: u64 = 1 << 40;
            let Some(position) = gtid_resume.clone() else {
                bail!(
                    "the source is at {current_file}@{current_pos}, behind the checkpointed \
                     {file}@{pos}, and the checkpoint carries no GTID position to resume from. \
                     Either this server's binlog history restarted or it is not the server the \
                     checkpoint came from; the target's document versions come from the old \
                     numbering and would reject everything written under the new one, so \
                     pg2osync will not continue. Turn on GTIDs before the next failover, or \
                     point [sync] at a fresh index name to load again from here"
                );
            };
            if !gtid_support.usable {
                bail!(
                    "the source is at {current_file}@{current_pos}, behind the checkpointed \
                     {file}@{pos}, and this server cannot resume from the checkpoint's GTID \
                     position because GTIDs are off on it. Enable them, or point [sync] at a \
                     fresh index name to load again from here"
                );
            }
            base = stored_token.saturating_add(GENERATION_MARGIN);
            tracing::warn!(target: "pg2osync::run",
                "the source is behind the checkpoint, so this is a different binlog history: \
                 resuming from gtid {} and versioning documents from a new generation at {base}",
                position.to_text());
        }
    }
    version_base.store(base, std::sync::atomic::Ordering::Relaxed);
    if gtid_resume.is_some() && !gtid_support.usable {
        tracing::warn!(target: "pg2osync::run",
            "the checkpoint holds a GTID position but this server has GTIDs off, so the \
             binlog coordinate is what resumes and only this server can honour it");
        gtid_resume = None;
    }
    // The coordinate is read *before* the first chunk, so streaming from it
    // replays anything a chunk missed or read stale onto an idempotent write.
    // That, not a snapshot, is what makes chunked reads correct.
    let (start_file, start_pos) = match &resume {
        Some((file, pos)) => {
            tracing::info!(target: "pg2osync::run",
            "resuming binlog from {file}@{pos}{}", if load_pending {
                ", with an initial load still to finish"
            } else {
                ""
            });
            (file.clone(), *pos)
        }
        None => {
            tracing::info!(target: "pg2osync::run", "no usable checkpoint; initial load");
            // Progress from an earlier load is unusable without a checkpoint to
            // go with it. The load would carry on from its cursor while the
            // stream could only start from here, and every change made in
            // between to a key the cursor has already passed would be lost.
            if load_pending {
                tracing::warn!(target: "pg2osync::run",
                    "an unfinished load has no checkpoint to resume its stream from, \
                     so it starts over rather than leaving a gap behind its cursor");
                for tbl in cfg.sync.values() {
                    let key = pg2osync_core::load::load_progress_key(&stream_id, &tbl.table);
                    if let Err(e) = sink.clear_state(&key).await {
                        tracing::warn!(target: "pg2osync::run",
                            "could not discard load progress {key}: {e}");
                    }
                }
            }
            // Read before the coordinate on purpose: it is the GTID form of
            // "everything that predates this stream", and taking it first can
            // only leave a transaction out, which costs a replay rather than a
            // gap. Without it a later GTID resume would ask for everything the
            // server ever wrote.
            gtid_baseline = mysql_catalog::gtid_baseline(&mut admin, gtid_support).await;
            let (file, pos) = mysql_catalog::master_position(&mut admin).await?;
            // PostgreSQL keeps this coordinate in the slot, which survives
            // whether or not a checkpoint was ever written. MySQL has nowhere
            // else to put it, so it is persisted here, before the first chunk:
            // the load's replay argument rests on the stream resuming from a
            // position that predates every chunk, and this is what remembers it.
            sink.write_checkpoint(&Checkpoint {
                stream: stream_id.clone(),
                token: mysql_catalog::position_token(&file, pos),
                position: mysql_catalog::position_text_full(
                    &mysql_catalog::split_binlog_file(&file)
                        .map(|(prefix, _)| prefix)
                        .unwrap_or_else(|| "binlog".to_string()),
                    mysql_catalog::position_token(&file, pos),
                    0,
                    gtid_baseline.as_ref().map(|p| p.to_text()).as_deref(),
                ),
            })
            .await?;
            (file, pos)
        }
    };
    // What has been consumed, so a checkpoint can say where to resume on a
    // server that never held this one's binlog files. Only kept when the server
    // can actually answer for it: a position built from an incomplete stream
    // would resume in the wrong place, which is worse than not having one.
    let tracker = gtid_support.usable.then(|| {
        Arc::new(std::sync::Mutex::new(
            pg2osync_source_mysql::gtid::GtidTracker::new(
                gtid_support.mariadb,
                gtid_resume.clone().or_else(|| gtid_baseline.clone()),
            ),
        ))
    });
    let render: PositionRenderer = {
        let (prefix, _) = mysql_catalog::split_binlog_file(&start_file)
            .unwrap_or_else(|| ("binlog".to_string(), 0));
        let tracker = tracker.clone();
        Arc::new(move |token| {
            // Asked for the position *as of this token*, never the newest one:
            // the stream runs ahead of what the target has taken, and claiming
            // its position would resume past data that was never written.
            let gtid = tracker
                .as_ref()
                .and_then(|t| t.lock().expect("not poisoned").position_at(token));
            mysql_catalog::position_text_full(&prefix, token, base, gtid.as_deref())
        })
    };

    let mut src_cfg = mysql_config_for(cfg, source_url)?;
    resolve_mysql_child_order(&mut src_cfg.children, &mut admin).await?;
    let load_children = src_cfg.children.clone();
    src_cfg.start_file = Some(start_file);
    src_cfg.start_pos = start_pos;
    src_cfg.gtid = tracker.clone();
    src_cfg.gtid_resume = gtid_resume.clone();
    src_cfg.version_base = base;

    let (events_tx, events_rx) = mpsc::channel::<ChangeEvent>(EVENT_CHANNEL_DEPTH);
    let (copy_tx, copy_rx) = mpsc::channel::<ChangeEvent>(COPY_CHANNEL_DEPTH);
    let (load_done_tx, load_done_rx) = watch::channel(0u64);
    let load_sink = sink.clone();
    let load_stream_id = stream_id.clone();
    let ctx = pipeline_ctx(cfg, sink, metrics, ack_tx, load_done_tx)?;
    let engine = spawn_engine(
        events_rx,
        copy_rx,
        ctx,
        stream_id,
        render,
        durable,
        shutdown_rx.clone(),
    );

    // The load runs *beside* the stream, not before it. What makes the overlap
    // safe is that every document carries the position it became visible at, so
    // a copied row that was already stale loses to the streamed change at the
    // target regardless of which arrives first.
    //
    // Unlike PostgreSQL there is nothing to pause for. A replication slot
    // retains WAL until it is consumed, so a slow consumer is what invalidates
    // it; MySQL purges binlogs on its own time and space policy and ignores
    // consumers entirely. Nothing accumulates because of us, and the hazard runs
    // the other way — the file we still need being purged — which pausing the
    // load would only make likelier.
    let load = async {
        // moved in, so the copy channel closes when the load is done and the
        // engine can tell the difference between "paused" and "finished"
        let copy_tx = copy_tx;
        if resume.is_none() || load_pending {
            let tables = src_cfg.tables.clone();
            with_bulk_load_settings(&load_sink, cfg, async {
                pg2osync_source_mysql::load::run(
                    &mut admin,
                    &tables,
                    cfg.source.load_chunk_rows.max(1) as u64,
                    &copy_tx,
                    load_sink.as_ref(),
                    &load_stream_id,
                    load_done_rx,
                    &pg2osync_core::load::LoadScope::initial_load()
                        .with_table_filters(table_filters(cfg)?),
                    &load_children,
                    base,
                )
                .await
            })
            .await
        } else {
            Ok(())
        }
    };

    let started = std::time::Instant::now();
    let stream = async {
        let mut streaming = MySqlSource::new(src_cfg.clone());
        streaming.stream(events_tx, shutdown_rx.clone()).await
    };
    // Either failing abandons the other: a stream error is a reconnect, and the
    // load picks up from its recorded progress on the next attempt.
    let result = futures::future::try_join(load, stream).await.map(|_| ());
    // dropping both senders above is what lets the engine drain and exit
    let _ = engine.await;
    result?;

    Ok(if *shutdown_rx.borrow() {
        AttemptEnd::Shutdown
    } else {
        AttemptEnd::Ended {
            streamed_for: started.elapsed(),
        }
    })
}

/// The source's current binlog coordinate, read on pg2osync's own connection.
///
/// Reading it needs `REPLICATION CLIENT`, which an application account should
/// not hold — so pg2osync reads it instead of asking the caller to.
async fn read_current_binlog_position(cfg: &AppConfig, source_url: &str) -> Option<u64> {
    use pg2osync_source_mysql::catalog as mysql_catalog;
    let source =
        pg2osync_source_mysql::runner::MySqlSource::new(mysql_config_for(cfg, source_url).ok()?);
    let mut admin = source.admin_connection().await.ok()?;
    let (file, pos) = mysql_catalog::master_position(&mut admin).await.ok()?;
    Some(mysql_catalog::position_token(&file, pos))
}

/// The binlog file prefix in use, so positions can be rendered and parsed.
async fn mysql_binlog_prefix(cfg: &AppConfig, source_url: &str) -> Result<String> {
    use pg2osync_source_mysql::catalog as mysql_catalog;
    let source =
        pg2osync_source_mysql::runner::MySqlSource::new(mysql_config_for(cfg, source_url)?);
    let mut admin = source.admin_connection().await?;
    let (file, _) = mysql_catalog::master_position(&mut admin).await?;
    Ok(mysql_catalog::split_binlog_file(&file)
        .map(|(prefix, _)| prefix)
        .unwrap_or_else(|| "binlog".to_string()))
}

pub fn mysql_config_for(
    cfg: &AppConfig,
    source_url: &str,
) -> Result<pg2osync_source_mysql::runner::MySqlSourceConfig> {
    let url = url::Url::parse(source_url).context("source url is not a valid URL")?;
    let children = child_specs_for(cfg)?;
    let mut child_parents = HashMap::new();
    let mut tables: Vec<(String, String)> = cfg
        .sync
        .values()
        .map(|t| {
            let (schema, table) = split_qualified(&t.table);
            (schema.to_string(), table.to_string())
        })
        .collect();
    // A child table has to be streamed or its changes never reach us, which is
    // the counterpart of adding it to the publication on PostgreSQL. Its rows
    // resolve to a parent instead of becoming documents.
    for (parent, specs) in &children {
        for spec in specs {
            let child = (spec.schema.clone(), spec.table.clone());
            if !tables.contains(&child) {
                tables.push(child.clone());
            }
            child_parents.insert(child, parent.clone());
        }
    }
    Ok(pg2osync_source_mysql::runner::MySqlSourceConfig {
        host: url.host_str().unwrap_or("localhost").into(),
        port: url.port().unwrap_or(3306),
        user: percent_decode(url.username()),
        password: percent_decode(url.password().unwrap_or_default()),
        server_id: cfg.source.server_id,
        tables,
        start_file: None,
        start_pos: 0,
        tls: cfg.tls_settings(source_url)?,
        children,
        child_parents,
        gtid: None,
        gtid_resume: None,
        version_base: 0,
    })
}

/// URL-encoded credentials are common in connection strings; MySQL auth needs
/// the decoded bytes.
fn percent_decode(value: &str) -> String {
    let bytes = value.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            let hex = std::str::from_utf8(&bytes[i + 1..i + 3]).unwrap_or("");
            if let Ok(b) = u8::from_str_radix(hex, 16) {
                out.push(b);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

pub fn spawn_engine(
    events_rx: mpsc::Receiver<ChangeEvent>,
    copy_rx: mpsc::Receiver<ChangeEvent>,
    ctx: Arc<PipelineCtx>,
    stream_id: StreamId,
    render: PositionRenderer,
    durable: DurableLsn,
    shutdown: watch::Receiver<bool>,
) -> tokio::task::JoinHandle<Result<(), pg2osync_core::CoreError>> {
    tokio::spawn(async move {
        pg2osync_engine::run(
            events_rx, copy_rx, ctx, stream_id, render, durable, shutdown,
        )
        .await
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn credentials_are_percent_decoded() {
        assert_eq!(percent_decode("p%40ss%3Aword"), "p@ss:word");
        assert_eq!(percent_decode("plain"), "plain");
        assert_eq!(percent_decode("trailing%"), "trailing%");
    }

    #[test]
    fn an_index_two_tables_feed_gets_one_spec_carrying_the_mapping() {
        let mut cfg: AppConfig = toml::from_str(
            r#"
[source]
url = "postgres://u:p@localhost/db"
[target]
url = "http://localhost:9200"
[sync.orders]
table = "public.orders"
index = "search"
id = "order-{id}"
[sync.users]
table = "public.users"
index = "search"
id = "user-{id}"
"#,
        )
        .expect("parses");
        // the mapping is read from a file at load; the test stands in for it,
        // on the later section so an earlier one without cannot shadow it
        cfg.sync.get_mut("users").expect("section").mapping =
            Some(serde_json::json!({"mappings": {}}));

        let specs = index_specs(&cfg);
        assert_eq!(specs.len(), 1, "two sections, one index");
        assert_eq!(specs[0].name, "search");
        assert!(specs[0].mapping.is_some());
        assert_eq!(index_names(&cfg), ["search"]);
    }

    #[test]
    fn a_shared_index_is_listed_once_with_the_mapping_its_parent_carries() {
        let mut cfg: AppConfig = toml::from_str(
            r#"
[source]
url = "postgres://u:p@localhost/db"
[target]
url = "http://localhost:9200"
[sync.customers]
table = "public.customers"
index = "shop"
[sync.customers.join]
field = "relation"
name = "customer"
[sync.orders]
table = "public.orders"
index = "shop"
[sync.orders.join]
field = "relation"
name = "order"
parent = "customer_id"
[sync.users]
table = "public.users"
"#,
        )
        .expect("parses");
        // the mapping is read from a file at load; the test stands in for it
        cfg.sync.get_mut("customers").expect("section").mapping =
            Some(serde_json::json!({"mappings": {}}));

        let specs = index_specs(&cfg);
        assert_eq!(
            specs.iter().map(|s| s.name.as_str()).collect::<Vec<_>>(),
            ["shop", "users"],
            "one entry per index, in section order"
        );
        assert!(
            specs[0].mapping.is_some(),
            "the parent's mapping survives the child's section, which has none"
        );
        assert_eq!(index_names(&cfg), ["shop", "users"]);

        let joins = joins(&cfg).expect("resolves");
        let child = joins
            .for_table("public", "orders")
            .expect("the child has a rule");
        let parent = child.parent.as_ref().expect("it names its parent");
        assert_eq!(parent.name, "customer");
        assert!(
            matches!(parent.id, ParentId::Key),
            "a parent without an id files its documents under the key"
        );
        assert!(
            !parent.key_column,
            "customer_id is not the child's declared key"
        );
        assert!(
            joins
                .for_table("public", "customers")
                .expect("the parent has a rule")
                .parent
                .is_none()
        );
    }

    #[test]
    fn checkpoints_from_another_stream_are_rejected() {
        let expected = StreamId {
            source: SOURCE_POSTGRES.into(),
            stream: "slot_a".into(),
            publication: "pub".into(),
        };
        let stored = Checkpoint {
            stream: StreamId {
                source: SOURCE_POSTGRES.into(),
                stream: "slot_b".into(),
                publication: "pub".into(),
            },
            token: 42,
            position: "0/2A".into(),
        };
        assert!(usable_checkpoint(Some(stored.clone()), &expected).is_none());
        let mut same = stored;
        same.stream.stream = "slot_a".into();
        assert!(usable_checkpoint(Some(same), &expected).is_some());
    }
}
