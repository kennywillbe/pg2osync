//! Pipeline orchestration: bootstrap, initial load, live streaming.
//!
//! Everything source-specific stays behind `SourceKind`; the engine and sink
//! wiring below is identical for every source.

use anyhow::{Context as _, Result, bail};
use pg2osync_core::checkpoint::{Checkpoint, StreamId};
use pg2osync_core::event::ChangeEvent;
use pg2osync_core::lsn::Lsn;
use pg2osync_core::sink::{IndexSpec, Sink};
use pg2osync_engine::mapping::{
    Constants, DurableLsn, IndexTarget, JoinParent, JoinRule, Joins, ParentId, Projection,
    Projections, Rename, Renames, TableMapping, Transforms,
};
use pg2osync_engine::metrics::{SharedMetrics, SourceState};
use pg2osync_engine::{PipelineCtx, PositionRenderer};
use pg2osync_source::reconnect::ReconnectPolicy;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use tokio::sync::{mpsc, watch};

use crate::backfill::split_qualified;
use crate::config::{AppConfig, ResolvedSecrets};
use crate::supervisor::SourceRuntime;

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

/// One source's pipeline, from its own configuration.
///
/// Everything process-wide — the listeners, the exit code, the decision that a
/// failure here is one source's — belongs to [`crate::supervisor`]. What
/// arrives in `rt` is this source's share of it.
pub async fn run_pipeline(
    cfg: AppConfig,
    secrets: ResolvedSecrets,
    rt: SourceRuntime,
) -> Result<()> {
    let sink = build_sink(&cfg, secrets.target_password)?;
    check_rejection_policy(&cfg, sink.as_ref())?;
    for note in embedded_children_with_own_section(&cfg) {
        tracing::warn!(target: "pg2osync::run", "{note}");
    }
    let index_specs = index_specs(&cfg)?;

    match cfg.source.flavor.as_str() {
        "mysql" => run_mysql(cfg, secrets.source_url, sink, index_specs, rt).await,
        "postgres" | "postgresql" => {
            run_postgres(
                cfg,
                secrets.source_url,
                secrets.admin_url,
                sink,
                index_specs,
                rt,
            )
            .await
        }
        other => bail!("unsupported source.flavor {other:?}; expected \"postgres\" or \"mysql\""),
    }
}

// ---------------------------------------------------------------- shared wiring

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
            // the junction is read as somebody's child in exactly the same way
            for watched in [Some(&child.table), child.through.as_ref()]
                .into_iter()
                .flatten()
            {
                if let Some((key, own)) = cfg.sync.iter().find(|(_, t)| &t.table == watched) {
                    notes.push(format!(
                        "[sync.{key}] {watched} is also an embedded child of [sync.{owner}]: \
                         the replication runner reads its rows only as a re-fetch of {}, so \
                         index {:?} receives the initial load and no streamed change",
                        tbl.table,
                        own.index_name(key)
                    ));
                }
            }
        }
    }
    notes
}

/// Refuse a configuration the target cannot honour, before the pipeline runs
/// rather than at the first document that finds out.
///
/// A quarantine policy on a target that cannot record a refused document:
/// silently falling back to halting would be defensible; silently dropping
/// the document would not, and the difference between the two is exactly what
/// a permissive default here would blur. Concurrent writes on a target that
/// keeps whichever landed last. And a per-row index on a target without
/// mappings, where nothing could create the index a row chose with the shape
/// it should have.
pub fn check_rejection_policy(cfg: &AppConfig, sink: &dyn Sink) -> Result<()> {
    if cfg.target.flavor == "meilisearch"
        && let Some((key, tbl)) = cfg.sync.iter().find(|(_, t)| t.is_templated())
    {
        bail!(
            "[sync.{key}] index {:?} chooses an index per row, which needs a target that can \
             create one on demand with the mapping it should have, and {} has no mappings to \
             create it with. Give this table a fixed index",
            tbl.index_name(key),
            cfg.target.flavor
        );
    }
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
    let retry = pg2osync_sink::RetryPolicy::new(
        cfg.engine.retry_max.max(1),
        cfg.engine.retry_backoff_ms.max(1),
        cfg.engine.retry_max_elapsed_ms,
    );
    let sink: Arc<dyn Sink> = match cfg.target.flavor.as_str() {
        "elasticsearch" => Arc::new(pg2osync_sink::elasticsearch::ElasticsearchSink::new(
            pg2osync_sink::elasticsearch::ElasticsearchSinkConfig {
                url: cfg.target.url.clone(),
                username: cfg.target.username.clone(),
                password: target_password,
                api_key,
                tls_verify: cfg.target.tls_verify,
                retry,
                require_alias: cfg.target.require_alias,
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
                require_alias: cfg.target.require_alias,
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
/// kept from whichever section set it: config allows at most one to. A
/// templated section contributes the glob its rows render into, flagged as a
/// pattern: nothing is created for it up front, and the mapping travels with
/// the glob so the index a row chooses can be created with it.
pub fn index_specs(cfg: &AppConfig) -> Result<Vec<IndexSpec>> {
    let mut specs: Vec<IndexSpec> = Vec::with_capacity(cfg.sync.len());
    for (key, tbl) in &cfg.sync {
        let target = index_target(key, tbl)?;
        let (name, pattern) = match &target {
            IndexTarget::Static(name) => (name.clone(), false),
            IndexTarget::Template { .. } => (target.pattern(), true),
        };
        match specs.iter_mut().find(|spec| spec.name == name) {
            Some(spec) => {
                if spec.mapping.is_none() {
                    spec.mapping = tbl.mapping.clone();
                }
            }
            None => specs.push(IndexSpec {
                name,
                mapping: tbl.mapping.clone(),
                pattern,
            }),
        }
    }
    Ok(specs)
}

/// Every target index once, in section order, a templated section standing
/// for its glob. The per-index settings the initial load suspends would
/// otherwise be saved and restored twice for a shared index, the second
/// restore reading the already-suspended values.
pub fn index_names(cfg: &AppConfig) -> Result<Vec<String>> {
    Ok(index_specs(cfg)?
        .into_iter()
        .map(|spec| spec.name)
        .collect())
}

/// The indices whose settings an initial load may relax: the fixed ones.
///
/// A templated section is left out because its glob is not an index:
/// `get_settings` on a glob answers per index, which the lookup by name would
/// miss, and `put_settings` on a glob that matches nothing yet is an error.
/// An index created mid-load takes the target's defaults; relaxing refresh
/// is an optimisation, not correctness.
pub fn fixed_index_names(cfg: &AppConfig) -> Result<Vec<String>> {
    Ok(index_specs(cfg)?
        .into_iter()
        .filter(|spec| !spec.pattern)
        .map(|spec| spec.name)
        .collect())
}

/// The section's target with the section named in the failure, since the
/// grammar is checked here as well as in `validate`.
fn index_target(key: &str, tbl: &crate::config::TableSync) -> Result<IndexTarget> {
    tbl.index_target(key, &pk_columns_for(tbl))
        .map_err(|e| anyhow::anyhow!("[sync.{key}] {e}"))
}

fn table_mapping(cfg: &AppConfig) -> Result<TableMapping> {
    let mut pairs = Vec::with_capacity(cfg.sync.len());
    for (key, tbl) in &cfg.sync {
        let (schema, table) = split_qualified(&tbl.table);
        pairs.push((
            (schema.to_string(), table.to_string()),
            index_target(key, tbl)?,
        ));
    }
    Ok(TableMapping::from_pairs(pairs))
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
    Transforms::from_pairs(pairs)
        .with_keys(cfg.pseudonym_keys()?)
        .map_err(|e| anyhow::anyhow!("{e}"))
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

/// The key columns a table's id or index may be rendered from without a
/// before-image, as far as the engine can tell: it has no catalog, and the
/// WAL path takes the key from the replica identity rather than from
/// configuration. The startup check beside it is what makes this declaration
/// true — where the declared key and the database's own key disagree about a
/// template, the table is required to be REPLICA IDENTITY FULL, so the
/// before-image (which carries both) is what a template ever renders from.
///
/// An append-only table has no key at all, so nothing renders from one: a
/// template on it always reads the row.
pub fn pk_columns_for(tbl: &crate::config::TableSync) -> Vec<String> {
    if tbl.append_only {
        return Vec::new();
    }
    vec![tbl.primary_key.clone().unwrap_or_else(|| "id".to_string())]
}

/// The (schema, table) pairs declared `append_only`, for whoever has to know
/// a row of theirs carries no key.
pub fn append_only_tables(cfg: &AppConfig) -> std::collections::HashSet<(String, String)> {
    cfg.sync
        .values()
        .filter(|tbl| tbl.append_only)
        .map(|tbl| {
            let (schema, table) = split_qualified(&tbl.table);
            (schema.to_string(), table.to_string())
        })
        .collect()
}

fn append_only(cfg: &AppConfig) -> pg2osync_engine::mapping::AppendOnly {
    pg2osync_engine::mapping::AppendOnly::from_iter(append_only_tables(cfg))
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

/// Each section's ingest pipeline keyed by (schema, table) for the engine.
fn pipelines(cfg: &AppConfig) -> pg2osync_engine::mapping::Pipelines {
    pg2osync_engine::mapping::Pipelines::from_pairs(cfg.sync.values().filter_map(|tbl| {
        let pipeline = tbl.pipeline.clone()?;
        let (schema, table) = split_qualified(&tbl.table);
        Some(((schema.to_string(), table.to_string()), pipeline))
    }))
}

/// Each section's routing column keyed by (schema, table) for the engine.
fn routings(cfg: &AppConfig) -> pg2osync_engine::mapping::Routings {
    pg2osync_engine::mapping::Routings::from_pairs(cfg.sync.values().filter_map(|tbl| {
        let column = tbl.routing.clone()?;
        let (schema, table) = split_qualified(&tbl.table);
        let key_column = pk_columns_for(tbl).contains(&column);
        Some((
            (schema.to_string(), table.to_string()),
            pg2osync_engine::mapping::RoutingColumn { column, key_column },
        ))
    }))
}

/// Every synced table's primary key from the catalogue, for the decoder: under
/// REPLICA IDENTITY FULL the WAL flags every column as identity, and the key
/// the load filed a row under is the only thing a streamed change may address
/// it by.
async fn key_columns_for(
    cfg: &AppConfig,
    admin: &tokio_postgres::Client,
) -> Result<HashMap<(String, String), Vec<String>>> {
    let mut out = HashMap::new();
    for tbl in cfg.sync.values() {
        let (schema, table) = split_qualified(&tbl.table);
        // declared keyless, so whatever the catalogue says is not what a
        // document is filed under
        let key = if tbl.append_only {
            Vec::new()
        } else {
            pg2osync_source::catalog::table_info(admin, schema, table)
                .await
                .with_context(|| format!("cannot read the key of {}", tbl.table))?
                .pk_columns
        };
        out.insert((schema.to_string(), table.to_string()), key);
    }
    Ok(out)
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
pub fn read_token(var: Option<&str>, endpoint: &str) -> Result<Option<String>> {
    match var {
        Some(key) => std::env::var(key).map(Some).map_err(|_| {
            anyhow::anyhow!("{endpoint}.token_env={key:?} is set but the variable is missing")
        }),
        None => Ok(None),
    }
}

/// Build the engine context for one attempt.
pub fn pipeline_ctx(
    cfg: &AppConfig,
    sink: Arc<dyn Sink>,
    metrics: SharedMetrics,
    ack_tx: watch::Sender<Option<Lsn>>,
    load_done_tx: watch::Sender<u64>,
    settings: watch::Receiver<pg2osync_engine::EngineSettings>,
) -> Result<Arc<PipelineCtx>> {
    Ok(Arc::new(PipelineCtx {
        sink,
        mapping: table_mapping(cfg)?,
        projections: projections(cfg),
        transforms: transforms(cfg)?,
        renames: renames(cfg),
        constants: constants(cfg)?,
        id_templates: id_templates(cfg)?,
        fan_outs: fan_outs(cfg)?,
        joins: joins(cfg)?,
        filters: filters(cfg)?,
        pipelines: pipelines(cfg),
        routings: routings(cfg),
        append_only: append_only(cfg),
        cfg: cfg.engine.clone(),
        settings,
        ack_tx,
        load_done_tx,
        metrics,
    }))
}

/// Refuse to start a pipeline whose derived identity the source cannot supply.
///
/// An `id` or an `index` that references columns outside the key, and a
/// `fan_out` whose deletes and update-diffs need the row's old values, all
/// depend on the whole old row arriving in the WAL — which only REPLICA
/// IDENTITY FULL guarantees. Finding that out on the first delete, at 3am, is
/// worse than naming the ALTER statement now, the same way child tables are
/// checked. A row filter adds no requirement of its own: a key-only template
/// renders the delete of a row that left the filter from the key, and the
/// other cases are the ones above. A join child is one more: its delete has
/// to name its parent's shard, and its routing comes from the same place its
/// id does, and so is a section routed by a column: an update that changes
/// that column has to delete the document under the routing it had before.
async fn check_derived_identity_requirements(
    cfg: &AppConfig,
    admin: &tokio_postgres::Client,
) -> Result<()> {
    for (key, tbl) in &cfg.sync {
        // no update or delete ever reaches an append-only table, so nothing
        // on it is ever rendered from a before-image
        if tbl.append_only {
            continue;
        }
        // (option, what it is for, the template): the two templates are one
        // rule, told apart only so the refusal can say which option needs it
        let mut declared: Vec<(&str, &str, &String)> = Vec::new();
        if let Some(spec) = &tbl.id {
            declared.push(("id", "derived id", spec));
        }
        if let Some(spec) = &tbl.index
            && tbl.is_templated()
        {
            declared.push(("index", "per-row index", spec));
        }
        if declared.is_empty()
            && tbl.fan_out.is_none()
            && tbl.join.is_none()
            && tbl.routing.is_none()
        {
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
        // — the only thing a template is ever rendered from.
        let in_both_keys = |column: &str| {
            info.pk_columns.iter().any(|k| k == column)
                && pk_columns_for(tbl).iter().any(|k| k == column)
        };
        let mut needs: Vec<&str> = Vec::new();
        for (option, what, spec) in &declared {
            let from_catalog = pg2osync_engine::mapping::IdTemplate::parse(spec, &info.pk_columns)
                .map_err(|e| anyhow::anyhow!("[sync.{key}] {option} {spec:?}: {e}"))?;
            let from_config =
                pg2osync_engine::mapping::IdTemplate::parse(spec, &pk_columns_for(tbl))
                    .map_err(|e| anyhow::anyhow!("[sync.{key}] {option} {spec:?}: {e}"))?;
            if !(from_catalog.is_pk_only() && from_config.is_pk_only()) {
                needs.push(what);
            }
        }
        if tbl.fan_out.is_some() {
            needs.push("fan-out");
        }
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
        if let Some(column) = &tbl.routing
            && !in_both_keys(column)
            && info.relreplident != 'f'
        {
            bail!(
                "[sync.{key}] {} has REPLICA IDENTITY '{}', but its routing column {column} is \
                 not part of its key: a delete would carry no routing, and an update that \
                 changes it could not remove the document from the shard it is on. \
                 Run: ALTER TABLE {} REPLICA IDENTITY FULL",
                tbl.table,
                info.relreplident,
                tbl.table
            );
        }
        if needs.is_empty() {
            continue;
        }
        if info.relreplident != 'f' {
            bail!(
                "[sync.{key}] {} has REPLICA IDENTITY '{}', but its {} needs the whole old \
                 row: deletes and updates could not find the documents they replace. \
                 Run: ALTER TABLE {} REPLICA IDENTITY FULL",
                tbl.table,
                info.relreplident,
                needs.join(" and "),
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

/// Tell the read-your-writes endpoint what this source answers with.
///
/// The listener belongs to the process and is already accepting requests; a
/// source joins it as soon as it can render a position. Sent once, outside the
/// retry loop: the acknowledged-position channel has to survive a reconnect.
fn publish_endpoints(
    cfg: &AppConfig,
    rt: &SourceRuntime,
    endpoints: pg2osync_engine::api::SourceEndpoints,
) {
    if !cfg.api.enabled {
        return;
    }
    let sender = rt.endpoints.clone();
    let name = rt.name.clone();
    tokio::spawn(async move {
        if sender.send((name.clone(), endpoints)).await.is_err() {
            tracing::warn!(target: "pg2osync::api",
                "the endpoint is not listening; /synced will not answer for {name}");
        }
    });
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
async fn stream_with_reconnect<A>(
    policy: ReconnectPolicy,
    metrics: SharedMetrics,
    shutdown: &watch::Receiver<bool>,
    mut attempt: A,
) -> Result<()>
where
    // A boxed future rather than an opaque one: the supervisor spawns this
    // whole pipeline, and an attempt future that borrows the configuration is
    // only provably Send where its lifetime is written down.
    A: FnMut() -> futures::future::BoxFuture<'static, Result<AttemptEnd>>,
{
    let mut failures = 0u32;
    loop {
        metrics.set_source_connected(true);
        metrics.set_state(SourceState::Streaming);
        let outcome = attempt().await;
        metrics.set_source_connected(false);

        if *shutdown.borrow() {
            metrics.set_state(SourceState::Stopped);
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
        // The source's own verdict comes first: counting attempts cannot tell
        // a dropped connection from a configuration nothing can satisfy, and
        // the second one fails the same way on every attempt.
        if !worth_retrying(&error) {
            return Err(error);
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
        metrics.set_state(SourceState::Reconnecting);
        tracing::warn!(target: "pg2osync::run",
            "source stream failed ({error:#}); reconnecting in {:.1}s (attempt {failures})",
            delay.as_secs_f64());
        tokio::time::sleep(delay).await;
    }
}

/// Whether the failure leaves anything for another attempt to do.
///
/// Only a typed source error can answer. Anything raised by the wiring here is
/// retried, because nothing has claimed it cannot succeed — and a pipeline
/// that stops on an unclassified failure is worse than one that tries again.
fn worth_retrying(error: &anyhow::Error) -> bool {
    if let Some(e) = error.downcast_ref::<pg2osync_source::SourceError>() {
        return e.is_retryable();
    }
    if let Some(e) = error.downcast_ref::<pg2osync_source_mysql::MySqlError>() {
        return e.is_retryable();
    }
    true
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
    rt: SourceRuntime,
) -> Result<()> {
    use pg2osync_source::runner::{WalSource, WalSourceConfig};

    let SourceRuntime {
        metrics,
        shutdown: shutdown_rx,
        durable,
        mode,
        reload,
        ..
    } = rt.clone();

    let polling = cfg.source.mode == "poll";
    let tls = cfg.tls_settings(&source_url)?;
    tracing::info!(target: "pg2osync::run", "source sslmode={} client_cert={}",
        tls.mode.as_str(),
        if tls.presents_client_certificate() { "yes" } else { "no" });
    let admin = pg2osync_source::tls::connect(&tls, &admin_url)
        .await
        .context("cannot connect to source PostgreSQL")?;

    let mut children = child_specs_for(&cfg)?;
    resolve_child_order(&mut children, &admin).await?;
    let mut tables: Vec<String> = cfg.sync.values().map(|t| t.table.clone()).collect();
    // Child tables must join the publication or their changes never reach us,
    // and so must a junction: each of the two carries half of what a
    // many-to-many collection is made of.
    for tbl in cfg.sync.values() {
        for child in &tbl.children {
            for table in [Some(&child.table), child.through.as_ref()]
                .into_iter()
                .flatten()
            {
                if !tables.contains(table) {
                    tables.push(table.clone());
                }
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
    let mut src_cfg = src_cfg;
    if !polling {
        src_cfg.key_columns = key_columns_for(&cfg, &admin).await?;
    }
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

    let stream_id = cfg.stream_id();
    let render: PositionRenderer = Arc::new(|token| Lsn(token).to_string());
    let parse: pg2osync_engine::PositionParser =
        Arc::new(|text| text.trim().parse::<Lsn>().ok().map(|lsn| lsn.0));
    start_slot_watch(&cfg, admin_url.clone(), metrics.clone())?;
    // Reloads are watched from here rather than from `main`, because this is
    // where the settings channel and the sink they reach into exist, and only
    // a pipeline that is actually running has anything to reload.
    let (settings_tx, settings_rx) = cfg.engine.settings_channel();
    crate::reload::spawn(
        reload,
        cfg.clone(),
        crate::reload::Handles {
            settings: settings_tx,
            sink: sink.clone(),
            metrics: metrics.clone(),
        },
    );
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
    publish_endpoints(
        &cfg,
        &rt,
        pg2osync_engine::api::SourceEndpoints {
            acked: ack_rx,
            parse_position: parse,
            render_position: render.clone(),
            sink: sink.clone(),
            nudge,
            current_position,
            // a templated section's glob is refreshed as one: OpenSearch and
            // Elasticsearch take a wildcard, and Meilisearch never sees a
            // template
            indices: index_names(&cfg)?,
        },
    );

    // setup is done; each attempt opens the SQL connection it needs, so holding
    // this one open would be a third connection doing nothing
    drop(admin);

    // shared with every attempt rather than borrowed by it
    let cfg = Arc::new(cfg);
    let source_url: Arc<str> = Arc::from(source_url);
    let admin_url: Arc<str> = Arc::from(admin_url);
    let children = Arc::new(children);

    stream_with_reconnect(
        cfg.source.reconnect_policy(),
        metrics.clone(),
        &shutdown_rx,
        || {
            Box::pin(attempt_postgres(
                cfg.clone(),
                source_url.clone(),
                admin_url.clone(),
                tls.clone(),
                children.clone(),
                src_cfg.clone(),
                sink.clone(),
                metrics.clone(),
                ack_tx.clone(),
                stream_id.clone(),
                render.clone(),
                durable.clone(),
                shutdown_rx.clone(),
                polling,
                settings_rx.clone(),
            ))
        },
    )
    .await
}

/// Which position a reconnect may resume streaming from.
///
/// Poll mode has no source position to resume from, so a leftover WAL
/// checkpoint would skip rows changed while the process was down: it always
/// reloads. A checkpoint behind the slot's replay position is unusable for the
/// same reason — streaming resumes at the slot position, so the gap between
/// them would be lost.
fn resume_position(polling: bool, checkpoint: Option<Lsn>, slot: Option<Lsn>) -> Option<Lsn> {
    if polling {
        return None;
    }
    let checkpoint = checkpoint?;
    if let Some(slot) = slot
        && checkpoint < slot
    {
        tracing::warn!(target: "pg2osync::run",
            "checkpoint {checkpoint} predates slot position {slot}; running a full \
             initial load to avoid a gap");
        return None;
    }
    Some(checkpoint)
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
    let saved = sink.begin_bulk_load(&fixed_index_names(cfg)?).await?;
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
    cfg: Arc<AppConfig>,
    source_url: Arc<str>,
    admin_url: Arc<str>,
    tls: pg2osync_source::tls::TlsSettings,
    children: Arc<HashMap<(String, String), Vec<pg2osync_source::children::ChildSpec>>>,
    src_cfg: pg2osync_source::runner::WalSourceConfig,
    sink: Arc<dyn Sink>,
    metrics: SharedMetrics,
    ack_tx: watch::Sender<Option<Lsn>>,
    stream_id: StreamId,
    render: PositionRenderer,
    durable: DurableLsn,
    shutdown_rx: watch::Receiver<bool>,
    polling: bool,
    settings: watch::Receiver<pg2osync_engine::EngineSettings>,
) -> Result<AttemptEnd> {
    use pg2osync_source::runner::WalSource;

    // Everything an attempt needs is owned by the attempt. A future that
    // borrows the caller's locals cannot be proven `Send`, and this one is
    // spawned: the process runs a pipeline per source, on whatever thread the
    // runtime has free.
    let cfg = &*cfg;
    let source_url = &*source_url;
    let admin_url = &*admin_url;
    let tls = &tls;
    let children = &*children;
    let src_cfg = &src_cfg;

    // One SQL connection per attempt, shared by the checkpoint check, the
    // initial load and child re-fetch. Opening it here rather than once at
    // startup means a connection lost during an outage is replaced by the
    // reconnect instead of staying dead for the life of the process.
    let admin = pg2osync_source::tls::connect(tls, admin_url)
        .await
        .context("cannot connect to source PostgreSQL")?;
    let admin = &admin;

    let stored = if polling {
        None
    } else {
        usable_checkpoint(sink.read_checkpoint(&stream_id).await?, &stream_id)
    };
    let slot_lsn = if polling {
        None
    } else {
        pg2osync_source::catalog::confirmed_flush_lsn(admin, &cfg.source.slot_name).await?
    };
    let resume_from = resume_position(polling, stored.map(|c| Lsn(c.token)), slot_lsn);
    // A checkpoint is not proof that the load finished: it says where streaming
    // got to, and with a load recording its own progress the two are separate
    // facts. Trusting the checkpoint alone is what silently skips a load.
    // Collected rather than mapped inline: a closure borrowing the config
    // across this await leaves the whole attempt future un-provably Send, and
    // the attempt is what the supervisor spawns.
    let synced_tables: Vec<String> = cfg.sync.values().map(|t| t.table.clone()).collect();
    let load_pending =
        pg2osync_core::load::unfinished(sink.as_ref(), &stream_id, &synced_tables).await?;
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
    let load_metrics = metrics.clone();
    let ctx = pipeline_ctx(cfg, sink, metrics, ack_tx, load_done_tx, settings)?;
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
            // The stream is already running beside this; what the state says
            // is that a whole table has still to arrive, which is the
            // difference between "behind" and "caught up".
            load_metrics.set_state(SourceState::Loading);
            let copied = with_bulk_load_settings(&load_sink, cfg, async {
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
            .await;
            load_metrics.set_state(SourceState::Streaming);
            copied
        } else {
            Ok(())
        }
    };

    let started = std::time::Instant::now();
    let stream = async {
        if polling {
            let mut poll =
                pg2osync_source::poll::PollSource::new(poll_config(cfg, source_url, tls.clone())?);
            poll.stream(events_tx, shutdown_rx.clone()).await?;
        } else {
            let mut source = WalSource::new(src_cfg.clone());
            source
                .stream(events_tx, shutdown_rx.clone(), Some(admin))
                .await?;
        }
        Ok(())
    };
    // Either failing abandons the other: a stream error is a reconnect, and the
    // load picks up from its recorded progress on the next attempt.
    let result = futures::future::try_join(load, stream).await.map(|_| ());
    // dropping both senders above is what lets the engine drain and exit.
    // An engine halt closes the change channel, which the source then
    // reports as its own failure; the engine's reason is the one that
    // matters, and the one an operator has to be able to read.
    if let Ok(Err(e)) = engine.await {
        tracing::error!(target: "pg2osync::engine", "engine stopped: {e}");
        return Err(anyhow::Error::from(e).context("engine stopped"));
    }
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
            // the junction as well as the child: a row of either one is not a
            // document, and both resolve to this parent
            for table in [Some(&child.table), child.through.as_ref()]
                .into_iter()
                .flatten()
            {
                let (cs, ct) = split_qualified(table);
                child_parents.insert(
                    (cs.to_string(), ct.to_string()),
                    (ps.to_string(), pt.to_string()),
                );
            }
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
        key_columns: HashMap::new(),
        append_only: append_only_tables(cfg),
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
                pk_columns: pk_columns_for(t),
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
            spec.columns = child.columns.clone();
            spec.exclude_columns = child.exclude_columns.clone();
            spec.single = child.single;
            if let (Some(through), Some(through_key)) = (&child.through, &child.through_key) {
                spec.through = Some(pg2osync_source::children::Through::new(
                    through,
                    through_key,
                )?);
            }
            map.entry((schema.to_string(), table.to_string()))
                .or_default()
                .push(spec);
        }
    }
    Ok(map)
}

/// Deletes carry only the replica identity, so a table whose delete has to name
/// something else cannot locate the parent. Warn before it happens.
///
/// Which column that is depends on the role. A direct child is located by its
/// foreign key, which is not part of its key, so it needs FULL. A junction is
/// located by the same foreign key — but that column is normally half of its
/// `(parent, child)` primary key, and the default identity carries it, so only a
/// junction keyed some other way needs the warning. A through child is located
/// by its own primary key, which every replica identity carries, so it never
/// does.
async fn warn_on_child_replica_identity(
    cfg: &AppConfig,
    admin: &tokio_postgres::Client,
) -> Result<()> {
    for tbl in cfg.sync.values() {
        for child in &tbl.children {
            let located_by = match &child.through {
                Some(through) => (through.clone(), child.foreign_key.clone()),
                None => (child.table.clone(), child.foreign_key.clone()),
            };
            let (qualified, column) = located_by;
            let (schema, table) = split_qualified(&qualified);
            let info = pg2osync_source::catalog::table_info(admin, schema, table)
                .await
                .with_context(|| format!("cannot inspect child table {qualified}"))?;
            if info.relreplident != 'f' && !info.pk_columns.contains(&column) {
                tracing::warn!(target: "pg2osync::run",
                    "{qualified} has REPLICA IDENTITY '{}' and {column} is not in its \
                     primary key: DELETEs on it cannot refresh the parent document. \
                     Run: ALTER TABLE {qualified} REPLICA IDENTITY FULL",
                    info.relreplident);
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
    rt: SourceRuntime,
) -> Result<()> {
    use pg2osync_source_mysql::runner::MySqlSource;

    let SourceRuntime {
        metrics,
        shutdown: shutdown_rx,
        durable,
        mode,
        reload,
        ..
    } = rt.clone();

    let src_cfg = mysql_config_for(&cfg, &source_url)?;
    let source = MySqlSource::new(src_cfg);
    let mut admin = source.admin_connection().await?;
    source.bootstrap(&mut admin).await?;
    sink.ensure_ready(&index_specs).await?;

    if mode == Mode::Bootstrap {
        println!("✓ MySQL prerequisites met and target indices are ready");
        return Ok(());
    }

    let stream_id = cfg.stream_id();
    // Reloads are watched from here rather than from `main`, because this is
    // where the settings channel and the sink they reach into exist, and only
    // a pipeline that is actually running has anything to reload.
    let (settings_tx, settings_rx) = cfg.engine.settings_channel();
    crate::reload::spawn(
        reload,
        cfg.clone(),
        crate::reload::Handles {
            settings: settings_tx,
            sink: sink.clone(),
            metrics: metrics.clone(),
        },
    );
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
        publish_endpoints(
            &cfg,
            &rt,
            pg2osync_engine::api::SourceEndpoints {
                acked: ack_rx,
                parse_position: parse,
                render_position: render,
                sink: sink.clone(),
                // MySQL has nothing to write that the binlog would carry
                // without touching a synced table
                nudge: None,
                current_position,
                indices: index_names(&cfg)?,
            },
        );
    }

    // shared with every attempt rather than borrowed by it
    let cfg = Arc::new(cfg);
    let source_url: Arc<str> = Arc::from(source_url);

    stream_with_reconnect(
        cfg.source.reconnect_policy(),
        metrics.clone(),
        &shutdown_rx,
        || {
            Box::pin(attempt_mysql(
                cfg.clone(),
                source_url.clone(),
                AttemptWiring {
                    sink: sink.clone(),
                    metrics: metrics.clone(),
                    ack_tx: ack_tx.clone(),
                    stream_id: stream_id.clone(),
                    durable: durable.clone(),
                    shutdown_rx: shutdown_rx.clone(),
                    version_base: version_base.clone(),
                    settings: settings_rx.clone(),
                },
            ))
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
    /// The `[engine]` settings each batch re-reads, so a reload reaches one
    /// without the attempt being rebuilt.
    settings: watch::Receiver<pg2osync_engine::EngineSettings>,
}

/// Where MySQL document versions are numbered from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum VersionBase {
    /// The coordinate space the checkpoint was written in still holds.
    Kept(u64),
    /// The source is behind the checkpoint, so this is a different binlog
    /// history and versions restart above everything already written.
    NewGeneration(u64),
}

/// Decide the version base from the checkpoint and the source's coordinate.
///
/// The version is `base + coordinate`, and a version at the target only ever
/// goes up. A failover onto a server whose numbering is lower would therefore
/// have every write refused, leaving the index quietly stale — so when the
/// coordinate the source is at now versions below the checkpoint's own token, a
/// new generation opens above that token instead.
fn version_base_for(base: u64, stored_token: u64, current_coordinate: u64) -> VersionBase {
    if base.saturating_add(current_coordinate) >= stored_token {
        return VersionBase::Kept(base);
    }
    // The margin has to clear the highest version already written but not yet
    // acknowledged, which one unacknowledged transaction bounds to a few file
    // rotations. A thousand rotations of headroom is far past that, and still
    // leaves millions of generations in a u64.
    const GENERATION_MARGIN: u64 = 1 << 40;
    VersionBase::NewGeneration(stored_token.saturating_add(GENERATION_MARGIN))
}

async fn attempt_mysql(
    cfg: Arc<AppConfig>,
    source_url: Arc<str>,
    wiring: AttemptWiring,
) -> Result<AttemptEnd> {
    // owned for the same reason as the PostgreSQL attempt: the pipeline is
    // spawned, so nothing it holds may borrow the caller
    let cfg = &*cfg;
    let source_url = &*source_url;
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
        settings,
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
    // Collected rather than mapped inline: a closure borrowing the config
    // across this await leaves the whole attempt future un-provably Send, and
    // the attempt is what the supervisor spawns.
    let synced_tables: Vec<String> = cfg.sync.values().map(|t| t.table.clone()).collect();
    let load_pending =
        pg2osync_core::load::unfinished(sink.as_ref(), &stream_id, &synced_tables).await?;
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
        if let VersionBase::NewGeneration(opened) = version_base_for(
            base,
            stored_token,
            mysql_catalog::position_token(&current_file, current_pos),
        ) {
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
            base = opened;
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
    let load_metrics = metrics.clone();
    let ctx = pipeline_ctx(cfg, sink, metrics, ack_tx, load_done_tx, settings)?;
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
            let scope = pg2osync_core::load::LoadScope::initial_load()
                .with_table_filters(table_filters(cfg)?);
            // as on PostgreSQL: the stream is already running, and the state
            // says a whole table has still to arrive
            load_metrics.set_state(SourceState::Loading);
            let copied = with_bulk_load_settings(&load_sink, cfg, async {
                pg2osync_source_mysql::load::run(
                    &mut admin,
                    &tables,
                    cfg.source.load_chunk_rows.max(1) as u64,
                    &copy_tx,
                    load_sink.as_ref(),
                    &load_stream_id,
                    load_done_rx,
                    &scope,
                    &load_children,
                    base,
                    &src_cfg.append_only,
                    cfg.engine.load_max_rows_per_sec,
                )
                .await?;
                Ok(())
            })
            .await;
            load_metrics.set_state(SourceState::Streaming);
            copied
        } else {
            Ok(())
        }
    };

    let started = std::time::Instant::now();
    let stream = async {
        let mut streaming = MySqlSource::new(src_cfg.clone());
        streaming.stream(events_tx, shutdown_rx.clone()).await?;
        Ok(())
    };
    // Either failing abandons the other: a stream error is a reconnect, and the
    // load picks up from its recorded progress on the next attempt.
    let result = futures::future::try_join(load, stream).await.map(|_| ());
    // dropping both senders above is what lets the engine drain and exit.
    // An engine halt closes the change channel, which the source then
    // reports as its own failure; the engine's reason is the one that
    // matters, and the one an operator has to be able to read.
    if let Ok(Err(e)) = engine.await {
        tracing::error!(target: "pg2osync::engine", "engine stopped: {e}");
        return Err(anyhow::Error::from(e).context("engine stopped"));
    }
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
            let junction = spec
                .through
                .as_ref()
                .map(|t| (t.schema.clone(), t.table.clone()));
            for watched in [Some((spec.schema.clone(), spec.table.clone())), junction]
                .into_iter()
                .flatten()
            {
                if !tables.contains(&watched) {
                    tables.push(watched.clone());
                }
                child_parents.insert(watched, parent.clone());
            }
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
        append_only: append_only_tables(cfg),
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
    use pg2osync_core::checkpoint::SOURCE_POSTGRES;

    /// `(file index << 32) | offset`, the coordinate the version is built on.
    fn coordinate(file_index: u64, offset: u64) -> u64 {
        (file_index << 32) | offset
    }

    const MARGIN: u64 = 1 << 40;

    #[test]
    fn a_first_run_versions_from_zero_so_the_version_is_the_bare_coordinate() {
        assert_eq!(
            version_base_for(0, 0, coordinate(1, 4)),
            VersionBase::Kept(0)
        );
    }

    #[test]
    fn a_source_ahead_of_its_checkpoint_keeps_the_generation_it_is_in() {
        let stored = coordinate(7, 120);
        assert_eq!(
            version_base_for(0, stored, coordinate(9, 4)),
            VersionBase::Kept(0),
            "the same server, further along: nothing to renumber"
        );
        // exactly at the checkpoint is a restart, not a different history
        assert_eq!(version_base_for(0, stored, stored), VersionBase::Kept(0));
        // a pipeline already in a later generation compares within it
        assert_eq!(
            version_base_for(MARGIN, MARGIN + stored, coordinate(8, 0)),
            VersionBase::Kept(MARGIN)
        );
    }

    #[test]
    fn a_failover_onto_a_lower_coordinate_opens_a_generation_above_every_stored_version() {
        // the promoted replica rotated fewer times than the old primary, so its
        // own coordinate versions below what the target already holds
        let stored = coordinate(9, 800);
        let promoted = coordinate(2, 40);
        let VersionBase::NewGeneration(base) = version_base_for(0, stored, promoted) else {
            panic!("a lower coordinate has to open a generation");
        };
        assert_eq!(base, stored + MARGIN);
        assert!(
            base + promoted > stored,
            "the first version written after the failover outranks the last one before it"
        );
    }

    #[test]
    fn each_failover_stacks_another_margin_so_versions_never_go_backwards() {
        let first = coordinate(9, 800);
        let VersionBase::NewGeneration(second_base) = version_base_for(0, first, coordinate(2, 40))
        else {
            panic!("the first failover opens a generation");
        };
        // the second failover compares against the version the first generation
        // reached, not against a bare coordinate
        let reached = second_base + coordinate(5, 10);
        let VersionBase::NewGeneration(third_base) =
            version_base_for(second_base, reached, coordinate(1, 0))
        else {
            panic!("the second failover opens another generation");
        };
        assert!(third_base > second_base);
        assert_eq!(third_base, reached + MARGIN);
    }

    #[test]
    fn the_margin_leaves_room_for_the_coordinate_space_and_for_millions_of_generations() {
        // 2^40 is 2^8 whole file indexes of coordinate space, and a file caps
        // at MySQL's 1 GiB, so a thousand rotations of headroom
        assert_eq!(MARGIN / coordinate(1, 0), 1 << 8);
        assert_eq!(MARGIN / (1 << 30), 1024, "1 GiB rotations");
        assert_eq!(u64::MAX / MARGIN, (1 << 24) - 1, "millions of generations");
    }

    #[test]
    fn poll_mode_always_reloads_and_a_checkpoint_behind_the_slot_is_unusable() {
        // no source position to resume from, so a leftover WAL checkpoint would
        // skip whatever changed while the process was down
        assert_eq!(resume_position(true, Some(Lsn(90)), None), None);
        assert_eq!(resume_position(false, None, Some(Lsn(10))), None);
        assert_eq!(
            resume_position(false, Some(Lsn(90)), Some(Lsn(10))),
            Some(Lsn(90))
        );
        assert_eq!(
            resume_position(false, Some(Lsn(90)), Some(Lsn(90))),
            Some(Lsn(90))
        );
        // the gap between the checkpoint and the slot's replay position would
        // be lost, so the load runs again instead
        assert_eq!(resume_position(false, Some(Lsn(10)), Some(Lsn(90))), None);
    }

    #[test]
    fn only_a_configuration_the_source_cannot_satisfy_stops_the_reconnect_loop() {
        use pg2osync_source::SourceError;

        let transient = SourceError::connect(
            "replication connect failed",
            std::io::Error::from(std::io::ErrorKind::ConnectionRefused),
        );
        assert!(worth_retrying(&anyhow::Error::from(transient)));

        let permanent = SourceError::Config("wal_level is 'replica' but must be 'logical'".into());
        assert!(!worth_retrying(&anyhow::Error::from(permanent)));

        // the context the binary attaches must not hide the verdict
        let wrapped = anyhow::Error::from(pg2osync_source_mysql::MySqlError::Config(
            "binlog_format is \"STATEMENT\" but must be ROW".into(),
        ))
        .context("mysql source");
        assert!(!worth_retrying(&wrapped));

        // an error this module raised itself says nothing, so it is retried
        assert!(worth_retrying(&anyhow::anyhow!(
            "the source closed the stream"
        )));
    }

    #[test]
    fn credentials_are_percent_decoded() {
        assert_eq!(percent_decode("p%40ss%3Aword"), "p@ss:word");
        assert_eq!(percent_decode("plain"), "plain");
        assert_eq!(percent_decode("trailing%"), "trailing%");
    }

    #[test]
    fn an_append_only_table_has_no_key_columns() {
        let cfg: AppConfig = toml::from_str(
            r#"
[source]
url = "postgres://u:p@localhost/db"
[target]
url = "http://localhost:9200"
[sync.events]
table = "public.events"
append_only = true
[sync.users]
table = "public.users"
"#,
        )
        .expect("parses");
        // nothing renders from a key the table does not have: an id or index
        // template on it always reads the row
        assert!(pk_columns_for(&cfg.sync["events"]).is_empty());
        assert_eq!(pk_columns_for(&cfg.sync["users"]), ["id"]);
        assert!(append_only(&cfg).contains("public", "events"));
        assert!(!append_only(&cfg).contains("public", "users"));
        let poll = poll_config(&cfg, "postgres://u:p@localhost/db", Default::default())
            .expect("a poll config");
        let events = poll
            .tables
            .iter()
            .find(|t| t.qualified == "public.events")
            .expect("configured");
        assert!(events.pk_columns.is_empty(), "the poll row carries no key");
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

        let specs = index_specs(&cfg).expect("fixed names");
        assert_eq!(specs.len(), 1, "two sections, one index");
        assert_eq!(specs[0].name, "search");
        assert!(specs[0].mapping.is_some());
        assert!(!specs[0].pattern);
        assert_eq!(index_names(&cfg).expect("fixed names"), ["search"]);
    }

    #[test]
    fn a_templated_section_is_listed_as_the_glob_its_rows_render_into() {
        let mut cfg: AppConfig = toml::from_str(
            r#"
[source]
url = "postgres://u:p@localhost/db"
[target]
url = "http://localhost:9200"
[sync.events]
table = "public.events"
index = "events-{tenant}"
[sync.users]
table = "public.users"
"#,
        )
        .expect("parses");
        // the mapping is read from a file at load; the test stands in for it
        cfg.sync.get_mut("events").expect("section").mapping =
            Some(serde_json::json!({"mappings": {}}));

        let specs = index_specs(&cfg).expect("a template with a literal prefix");
        assert_eq!(specs.len(), 2);
        assert_eq!(specs[0].name, "events-*");
        assert!(specs[0].pattern, "a glob, not an index to create");
        assert!(
            specs[0].mapping.is_some(),
            "the mapping travels with the glob, for the index a row chooses"
        );
        assert_eq!(specs[1].name, "users");
        assert!(!specs[1].pattern);
        assert_eq!(
            index_names(&cfg).expect("names"),
            ["events-*", "users"],
            "the glob stands for every index the template renders"
        );
        assert_eq!(
            fixed_index_names(&cfg).expect("names"),
            ["users"],
            "bulk-load settings are only relaxed on an index that exists to relax"
        );
        let mapping = table_mapping(&cfg).expect("targets");
        assert!(matches!(
            mapping.target_for("public", "events"),
            Some(IndexTarget::Template { spec, .. }) if spec == "events-{tenant}"
        ));
        assert_eq!(
            mapping.target_for("public", "users"),
            Some(&IndexTarget::Static("users".into()))
        );
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

        let specs = index_specs(&cfg).expect("fixed names");
        assert_eq!(
            specs.iter().map(|s| s.name.as_str()).collect::<Vec<_>>(),
            ["shop", "users"],
            "one entry per index, in section order"
        );
        assert!(
            specs[0].mapping.is_some(),
            "the parent's mapping survives the child's section, which has none"
        );
        assert_eq!(index_names(&cfg).expect("fixed names"), ["shop", "users"]);

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
