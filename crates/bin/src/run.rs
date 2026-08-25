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
    DurableLsn, Projection, Projections, TableMapping, TransformOp, Transforms,
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
const COPY_CHANNEL_DEPTH: usize = 2_000;

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
    let index_specs: Vec<IndexSpec> = cfg
        .sync
        .iter()
        .map(|(k, t)| IndexSpec {
            name: t.index_name(k),
            mapping: t.mapping.clone(),
        })
        .collect();

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
                serverless: cfg.target.serverless,
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
        for (col, op) in &tbl.transform {
            let parsed = TransformOp::parse(op).with_context(|| {
                format!("[sync.{key}.transform] {col} = {op:?} is not \"hash\" or \"redact\"")
            })?;
            rules.insert(col.clone(), parsed);
        }
        pairs.push(((schema.to_string(), table.to_string()), rules));
    }
    Ok(Transforms::from_pairs(pairs))
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
fn pipeline_ctx(
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
        cfg: cfg.engine.clone(),
        ack_tx,
        load_done_tx,
        metrics,
    }))
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
        indices: cfg
            .sync
            .iter()
            .map(|(key, tbl)| tbl.index_name(key))
            .collect(),
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

    let children = child_specs(&cfg)?;
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

    let stream_id = StreamId {
        source: SOURCE_POSTGRES.into(),
        stream: cfg.source.slot_name.clone(),
        publication: cfg.source.publication.clone(),
    };
    let render: PositionRenderer = Arc::new(|token| Lsn(token).to_string());
    let parse: pg2osync_engine::PositionParser =
        Arc::new(|text| text.trim().parse::<Lsn>().ok().map(|lsn| lsn.0));
    let metrics = start_metrics(&cfg)?;
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
    let indices: Vec<String> = cfg.sync.iter().map(|(k, t)| t.index_name(k)).collect();
    let saved = sink.begin_bulk_load(&indices).await?;
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

fn child_specs(
    cfg: &AppConfig,
) -> Result<HashMap<(String, String), Vec<pg2osync_source::children::ChildSpec>>> {
    let mut map: HashMap<_, Vec<_>> = HashMap::new();
    for tbl in cfg.sync.values() {
        let (schema, table) = split_qualified(&tbl.table);
        for child in &tbl.children {
            let spec = pg2osync_source::children::ChildSpec::new(
                &child.table,
                &child.field,
                &child.foreign_key,
                &tbl.primary_key.clone().unwrap_or_else(|| "id".into()),
            )?;
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

    if cfg.sync.values().any(|t| !t.children.is_empty()) {
        bail!("nested children are not supported for the MySQL source yet");
    }
    let src_cfg = mysql_config(&cfg, &source_url)?;
    let source = MySqlSource::new(src_cfg);
    let mut admin = source.admin_connection().await?;
    source.bootstrap(&mut admin).await?;
    sink.ensure_ready(&index_specs).await?;

    if mode == Mode::Bootstrap {
        println!("✓ MySQL prerequisites met and target indices are ready");
        return Ok(());
    }

    let stream_id = StreamId {
        source: SOURCE_MYSQL.into(),
        stream: cfg.source.server_id.to_string(),
        publication: String::new(),
    };
    let metrics = start_metrics(&cfg)?;
    let (ack_tx, ack_rx) = watch::channel(None);
    // the binlog prefix is only known once a position has been read, so the
    // renderer and parser are built from the source's own vocabulary
    let api_prefix = mysql_binlog_prefix(&cfg, &source_url).await?;
    {
        let prefix = api_prefix.clone();
        let render: PositionRenderer =
            Arc::new(move |token| pg2osync_source_mysql::catalog::position_text(&prefix, token));
        let parse: pg2osync_engine::PositionParser = Arc::new(|text| {
            let (file, pos) = pg2osync_source_mysql::catalog::parse_position(text)?;
            Some(pg2osync_source_mysql::catalog::position_token(&file, pos))
        });
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
    } = wiring;
    let source = MySqlSource::new(mysql_config(cfg, source_url)?);
    let mut admin = source.admin_connection().await?;

    let stored = usable_checkpoint(sink.read_checkpoint(&stream_id).await?, &stream_id);
    let resume = stored.and_then(|c| mysql_catalog::parse_position(&c.position));
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
    if let Some((file, pos)) = &resume {
        let (current_file, current_pos) = mysql_catalog::master_position(&mut admin).await?;
        let stored = mysql_catalog::position_token(file, *pos);
        if mysql_catalog::position_token(&current_file, current_pos) < stored {
            bail!(
                "the source is at {current_file}@{current_pos}, behind the checkpointed \
                 {file}@{pos}: this server's binlog history restarted, or it is not the \
                 server the checkpoint came from. The target's document versions come from \
                 the old numbering and would reject everything written under the new one, \
                 so pg2osync will not continue. Point [sync] at a fresh index name, or \
                 delete the target index, to load again from here"
            );
        }
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
            mysql_catalog::master_position(&mut admin).await?
        }
    };
    let render: PositionRenderer = {
        let (prefix, _) = mysql_catalog::split_binlog_file(&start_file)
            .unwrap_or_else(|| ("binlog".to_string(), 0));
        Arc::new(move |token| mysql_catalog::position_text(&prefix, token))
    };

    let mut src_cfg = mysql_config(cfg, source_url)?;
    src_cfg.start_file = Some(start_file);
    src_cfg.start_pos = start_pos;

    let (events_tx, events_rx) = mpsc::channel::<ChangeEvent>(EVENT_CHANNEL_DEPTH);
    // MySQL's load does not run beside the stream yet, so the copy channel is
    // closed immediately.
    let (_, copy_rx) = mpsc::channel::<ChangeEvent>(1);
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

    if resume.is_none() || load_pending {
        let tables = src_cfg.tables.clone();
        with_bulk_load_settings(&load_sink, cfg, async {
            pg2osync_source_mysql::load::run(
                &mut admin,
                &tables,
                cfg.source.load_chunk_rows.max(1) as u64,
                &events_tx,
                load_sink.as_ref(),
                &load_stream_id,
                load_done_rx,
            )
            .await
        })
        .await?;
    }

    let started = std::time::Instant::now();
    let mut streaming = MySqlSource::new(src_cfg);
    let result = streaming.stream(events_tx, shutdown_rx.clone()).await;
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
        pg2osync_source_mysql::runner::MySqlSource::new(mysql_config(cfg, source_url).ok()?);
    let mut admin = source.admin_connection().await.ok()?;
    let (file, pos) = mysql_catalog::master_position(&mut admin).await.ok()?;
    Some(mysql_catalog::position_token(&file, pos))
}

/// The binlog file prefix in use, so positions can be rendered and parsed.
async fn mysql_binlog_prefix(cfg: &AppConfig, source_url: &str) -> Result<String> {
    use pg2osync_source_mysql::catalog as mysql_catalog;
    let source = pg2osync_source_mysql::runner::MySqlSource::new(mysql_config(cfg, source_url)?);
    let mut admin = source.admin_connection().await?;
    let (file, _) = mysql_catalog::master_position(&mut admin).await?;
    Ok(mysql_catalog::split_binlog_file(&file)
        .map(|(prefix, _)| prefix)
        .unwrap_or_else(|| "binlog".to_string()))
}

fn mysql_config(
    cfg: &AppConfig,
    source_url: &str,
) -> Result<pg2osync_source_mysql::runner::MySqlSourceConfig> {
    let url = url::Url::parse(source_url).context("source url is not a valid URL")?;
    let tables = cfg
        .sync
        .values()
        .map(|t| {
            let (schema, table) = split_qualified(&t.table);
            (schema.to_string(), table.to_string())
        })
        .collect();
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

fn spawn_engine(
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
