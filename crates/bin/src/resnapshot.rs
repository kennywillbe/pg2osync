//! Read one table again, into the index it is configured to write to.
//!
//! There used to be exactly one way to rebuild an index: delete the checkpoint
//! and let the process reload every configured table. After a mapping change, or
//! after fixing a decoding bug that touched one field, that is hours of work
//! nobody asked for.
//!
//! This is the chunked reader the initial load already uses, pointed at a single
//! table, and it goes through the whole ordinary write path — mapping,
//! projections, transforms, child collections, id derivation. That is the point:
//! a document written here has to be indistinguishable from one the load wrote,
//! and a second write path would drift from the first immediately.
//!
//! Two things it deliberately does not do. It does not delete: a row that is gone
//! from the source stays in the index, because deciding that is `reconcile`'s job
//! and keeping the two separate keeps each one explainable. And it does not touch
//! the checkpoint — its rows carry position `0`, so nothing acknowledges a
//! position and the checkpoint task has nothing to persist. That is what makes it
//! safe to run against a live pipeline.

use anyhow::{Context as _, Result, bail};
use pg2osync_core::event::ChangeEvent;
use pg2osync_core::load::LoadScope;
use pg2osync_core::sink::IndexSpec;
use std::sync::Arc;
use tokio::sync::{mpsc, watch};

use crate::config::AppConfig;
use crate::run;

/// Re-read `qualified_table`, optionally narrowed by `filter`.
pub async fn run_for(
    cfg: &AppConfig,
    source_url: &str,
    admin_url: &str,
    target_password: Option<String>,
    qualified_table: &str,
    filter: Option<String>,
) -> Result<()> {
    let Some((key, tbl)) = cfg.sync.iter().find(|(_, t)| t.table == qualified_table) else {
        bail!(
            "{qualified_table} is not in this config; a re-snapshot writes into the index the \
             table is mapped to, so there is nowhere to put it. Configured: {}",
            cfg.sync
                .values()
                .map(|t| t.table.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        );
    };
    let index = tbl.index_name(key);

    let sink = run::build_sink(cfg, target_password)?;
    // So a config pointed at a *new* index name gets its mapping first, which is
    // what the re-index recipe does before switching an alias onto it.
    sink.ensure_ready(&[IndexSpec {
        name: index.clone(),
        mapping: tbl.mapping.clone(),
    }])
    .await?;

    let stream_id = run::stream_id_for(cfg);
    let (events_tx, events_rx) = mpsc::channel::<ChangeEvent>(1);
    // no source stream in this process: closing it leaves the engine reading only
    // the copy channel
    drop(events_tx);
    let (copy_tx, copy_rx) = mpsc::channel::<ChangeEvent>(run::COPY_CHANNEL_DEPTH);
    let (load_done_tx, load_done_rx) = watch::channel(0u64);
    let (ack_tx, _ack_rx) = watch::channel(None);
    let (_shutdown_tx, shutdown_rx) = watch::channel(false);
    // A bare counter rather than `start_metrics`: binding the metrics port would
    // collide with the pipeline this is most likely running beside.
    let metrics = Arc::new(pg2osync_engine::metrics::Metrics::default());
    let ctx = run::pipeline_ctx(cfg, sink.clone(), metrics, ack_tx, load_done_tx)?;
    let engine = run::spawn_engine(
        events_rx,
        copy_rx,
        ctx,
        stream_id.clone(),
        Arc::new(|token| token.to_string()),
        pg2osync_engine::mapping::DurableLsn::default(),
        shutdown_rx,
    );

    let scope = LoadScope::resnapshot(qualified_table, filter);
    println!(
        "re-reading {qualified_table} into {index}{}",
        scope
            .filter
            .as_deref()
            .map(|f| format!(" where {f}"))
            .unwrap_or_default()
    );
    let started = std::time::Instant::now();
    let read = async {
        // moved in, so the channel closes when the read is done and the engine
        // can finish
        let copy_tx = copy_tx;
        match cfg.source.flavor.as_str() {
            "mysql" => {
                mysql(
                    cfg,
                    source_url,
                    &copy_tx,
                    sink.as_ref(),
                    &stream_id,
                    load_done_rx,
                    &scope,
                )
                .await
            }
            _ => {
                postgres(
                    cfg,
                    source_url,
                    admin_url,
                    &copy_tx,
                    sink.as_ref(),
                    &stream_id,
                    load_done_rx,
                    &scope,
                )
                .await
            }
        }
    };
    read.await?;
    engine
        .await
        .context("engine task failed")?
        .context("engine stopped before the re-snapshot finished")?;

    // `refresh_interval` is left alone throughout, unlike an initial load's:
    // this repairs an index that is in use, and hiding its ordinary writes for
    // the duration would be the wrong trade. One refresh at the end so whoever
    // ran the command can see the result immediately.
    if let Err(e) = sink.refresh(&[index]).await {
        tracing::warn!(target: "pg2osync::resnapshot",
            "the re-snapshot is written, but the index could not be refreshed, so it \
             may not be searchable yet: {e}");
    }
    println!(
        "re-snapshot of {qualified_table} finished in {:.1}s",
        started.elapsed().as_secs_f64()
    );
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn postgres(
    cfg: &AppConfig,
    source_url: &str,
    admin_url: &str,
    copy_tx: &mpsc::Sender<ChangeEvent>,
    sink: &dyn pg2osync_core::sink::Sink,
    stream_id: &pg2osync_core::checkpoint::StreamId,
    load_done: watch::Receiver<u64>,
    scope: &LoadScope,
) -> Result<()> {
    let tls = cfg.tls_settings(source_url)?;
    let admin = pg2osync_source::tls::connect(&tls, admin_url)
        .await
        .context("cannot connect to source PostgreSQL")?;
    let mut children = run::child_specs_for(cfg)?;
    run::resolve_child_order(&mut children, &admin).await?;
    crate::backfill::run(
        cfg,
        source_url,
        &tls,
        &admin,
        &children,
        copy_tx.clone(),
        sink,
        stream_id,
        load_done,
        scope,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn mysql(
    cfg: &AppConfig,
    source_url: &str,
    copy_tx: &mpsc::Sender<ChangeEvent>,
    sink: &dyn pg2osync_core::sink::Sink,
    stream_id: &pg2osync_core::checkpoint::StreamId,
    load_done: watch::Receiver<u64>,
    scope: &LoadScope,
) -> Result<()> {
    let src_cfg = run::mysql_config_for(cfg, source_url)?;
    let tables = src_cfg.tables.clone();
    let mut children = src_cfg.children.clone();
    let source = pg2osync_source_mysql::runner::MySqlSource::new(src_cfg);
    let mut conn = source.admin_connection().await?;
    run::resolve_mysql_child_order(&mut children, &mut conn).await?;
    // The generation the pipeline is versioning in. A re-snapshot writing under
    // an older one would have every document refused by the target and repair
    // nothing, which is the one outcome worse than doing nothing at all.
    let version_base = sink
        .read_checkpoint(stream_id)
        .await?
        .and_then(|c| pg2osync_source_mysql::catalog::parse_stored_position(&c.position))
        .map(|p| p.base)
        .unwrap_or(0);
    pg2osync_source_mysql::load::run(
        &mut conn,
        &tables,
        cfg.source.load_chunk_rows.max(1) as u64,
        copy_tx,
        sink,
        stream_id,
        load_done,
        scope,
        &children,
        version_base,
    )
    .await
}
