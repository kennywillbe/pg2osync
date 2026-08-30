//! Every configured source, running in one process.
//!
//! What is shared is the process and its two listeners; what is not shared is
//! everything a source is made of. Each one has its own sink, its own engine,
//! its own checkpoint and its own retry policy, so a source that cannot be
//! satisfied stops being a source that is running and nothing more: the others
//! never see it.

use crate::run::{self, Mode};
use crate::workspace::{Source, Workspace};
use anyhow::Result;
use pg2osync_engine::api::SourceEndpoints;
use pg2osync_engine::mapping::DurableLsn;
use pg2osync_engine::metrics::{Registry, SharedMetrics, SourceState};
use std::sync::Arc;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinSet;

/// What one source is given by the process it shares.
#[derive(Clone)]
pub struct SourceRuntime {
    pub name: String,
    pub metrics: SharedMetrics,
    /// Where a source announces what `/synced` should answer for it. It sends
    /// once it can render a position, which for MySQL is only after a round
    /// trip to the server — so the listener cannot wait for it.
    pub endpoints: mpsc::Sender<(String, SourceEndpoints)>,
    pub shutdown: watch::Receiver<bool>,
    /// The file this source was loaded from and the SIGHUPs asking for it to
    /// be read again. The signal is the process's; which file it re-reads is
    /// the source's.
    pub reload: crate::reload::ReloadSource,
    pub durable: DurableLsn,
    pub mode: Mode,
}

/// Run every source in the workspace until they all drain or fail.
///
/// There is deliberately no cap on how much the sources may have in flight
/// between them. A shared write budget would be a semaphore every source waits
/// on, which is precisely the coupling running them in one process is supposed
/// to remove: one slow target would then pace the pipelines reading a different
/// database entirely. What the sum of `[engine] write_concurrency` and
/// `batch_size` costs is the operator's to size, and documented.
pub async fn run_all(ws: Workspace, shutdown: watch::Receiver<bool>, mode: Mode) -> Result<()> {
    // The subscriber is the process's, so the filter is the workspace's: the
    // files that declare [log] have already been made to agree on it.
    crate::reload::apply_log_filter(ws.log.filter.as_deref())?;

    let registry = Arc::new(Registry::default());
    let names: Vec<String> = ws.sources.iter().map(|s| s.name.clone()).collect();
    // one slot per source: a source registers once, and the listener drains
    // the channel as it goes
    let (endpoints_tx, endpoints_rx) = mpsc::channel(names.len().max(1));

    // Bootstrap creates objects and exits, so it opens no ports: a second
    // process bootstrapping beside a running one must not fail on a bind.
    if mode == Mode::Run {
        if ws.metrics.enabled {
            let bind = ws.metrics.bind.clone();
            let token = run::read_token(ws.metrics.token_env.as_deref(), "metrics")?;
            let registry = registry.clone();
            tokio::spawn(
                async move { pg2osync_engine::metrics::serve(&bind, registry, token).await },
            );
        }
        if ws.api.enabled {
            let token = run::read_token(ws.api.token_env.as_deref(), "api")?;
            if token.is_none() && !pg2osync_engine::http::is_loopback(&ws.api.bind) {
                tracing::warn!(target: "pg2osync::api",
                    "the endpoint is bound to {} without a token; anything that can \
                     reach it can query the pipeline position", ws.api.bind);
            }
            let cfg = pg2osync_engine::api::ApiConfig {
                bind: ws.api.bind.clone(),
                token,
            };
            let deps = pg2osync_engine::api::ApiDeps {
                names: names.clone(),
                registrations: endpoints_rx,
                trace_link: crate::trace_link(),
            };
            tokio::spawn(async move { pg2osync_engine::api::serve(cfg, deps).await });
        }
    }

    // The sections that describe the process rather than a source are the
    // workspace's, not each file's: one process opens one metrics port
    // whatever a file that left the section out would have defaulted to.
    let (metrics_cfg, api_cfg) = (ws.metrics.clone(), ws.api.clone());

    // Installed before any pipeline starts, so a SIGHUP that arrives during an
    // initial load is a reload rather than the default disposition, which is to
    // kill the process — every source in it, not just the one being edited.
    let generations = crate::reload::on_sighup();

    let mut set: JoinSet<(String, Result<()>)> = JoinSet::new();
    for mut source in ws.sources {
        source.cfg.metrics = metrics_cfg.clone();
        source.cfg.api = api_cfg.clone();
        let rt = SourceRuntime {
            name: source.name.clone(),
            metrics: registry.register(&source.name),
            endpoints: endpoints_tx.clone(),
            shutdown: shutdown.clone(),
            reload: crate::reload::ReloadSource {
                path: source.path.clone(),
                generations: generations.clone(),
            },
            durable: DurableLsn::default(),
            mode,
        };
        set.spawn(async move {
            let name = rt.name.clone();
            let metrics = rt.metrics.clone();
            let result = one_source(source, rt).await;
            match &result {
                Ok(()) => metrics.set_state(SourceState::Stopped),
                Err(e) => {
                    // Not the process's failure: the source is now a source
                    // that is halted, which is a state an operator reads off
                    // /healthz/<name> and the state set, not an exit.
                    tracing::error!(target: "pg2osync::run", "source {name} halted: {e:#}");
                    metrics.set_state(SourceState::Halted);
                }
            }
            (name, result)
        });
    }
    // the listener's map is fed by the sources; nothing else holds the sender
    drop(endpoints_tx);

    wait_for_all(set).await
}

/// Resolve one source's secrets and run its pipeline.
///
/// The secrets are each file's own — one `url_env` per source — and a warning
/// about them names the source, because in a directory of thirty configs
/// "credentials in plain text" says nothing about which file to open.
async fn one_source(source: Source, rt: SourceRuntime) -> Result<()> {
    let secrets = source.cfg.resolve_secrets()?;
    for warning in &secrets.warnings {
        tracing::warn!(target: "pg2osync::config", "{}: {warning}", rt.name);
    }
    run::run_pipeline(source.cfg, secrets, rt).await
}

/// Wait for every source and decide what the process exits with.
///
/// One source halting is not the process failing — the others are still
/// streaming, and killing them would turn one broken configuration into an
/// outage for every database in the file. Exiting non-zero is reserved for a
/// process that has nothing left to do, which for a single source is exactly
/// today's behaviour.
async fn wait_for_all(mut set: JoinSet<(String, Result<()>)>) -> Result<()> {
    let total = set.len();
    let mut halted: Vec<String> = Vec::new();
    let mut first: Option<anyhow::Error> = None;
    while let Some(joined) = set.join_next().await {
        match joined {
            Ok((name, Ok(()))) => {
                tracing::info!(target: "pg2osync::run", "source {name} stopped cleanly")
            }
            Ok((name, Err(e))) => {
                halted.push(name);
                first.get_or_insert(e);
            }
            Err(join) => {
                // a panic is somebody's bug, and losing it in a clean exit is
                // how it stays one
                halted.push("<panicked>".into());
                first.get_or_insert_with(|| anyhow::anyhow!("a source task panicked: {join}"));
            }
        }
    }
    match first {
        Some(e) if halted.len() == total => Err(e),
        Some(_) => {
            tracing::warn!(target: "pg2osync::run",
                "{} of {total} source(s) halted ({}); the rest drained",
                halted.len(), halted.join(", "));
            Ok(())
        }
        None => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::future::Future;
    use std::sync::atomic::{AtomicBool, Ordering};

    /// A source's whole pipeline, stubbed: what this module decides is what a
    /// finished pipeline means for the process, not how one runs.
    type StubSource = std::pin::Pin<Box<dyn Future<Output = Result<()>> + Send>>;

    fn set_of(tasks: Vec<(&'static str, StubSource)>) -> JoinSet<(String, Result<()>)> {
        let mut set = JoinSet::new();
        for (name, task) in tasks {
            set.spawn(async move { (name.to_string(), task.await) });
        }
        set
    }

    fn ok() -> StubSource {
        Box::pin(async { Ok(()) })
    }

    fn fails(why: &'static str) -> StubSource {
        Box::pin(async move { Err(anyhow::anyhow!(why)) })
    }

    #[tokio::test]
    async fn one_halted_source_leaves_the_others_running() {
        static FINISHED: AtomicBool = AtomicBool::new(false);
        let slow: StubSource = Box::pin(async {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            FINISHED.store(true, Ordering::SeqCst);
            Ok(())
        });
        let result = wait_for_all(set_of(vec![
            ("bad", fails("no such table")),
            ("good", slow),
        ]))
        .await;
        assert!(result.is_ok(), "{:?}", result.err());
        assert!(
            FINISHED.load(Ordering::SeqCst),
            "the surviving source was cut short by the halted one"
        );
    }

    #[tokio::test]
    async fn a_process_with_nothing_left_to_do_exits_non_zero() {
        let result =
            wait_for_all(set_of(vec![("a", fails("first")), ("b", fails("second"))])).await;
        let error = format!("{:#}", result.expect_err("every source halted"));
        assert!(
            error.contains("first") || error.contains("second"),
            "{error}"
        );
    }

    #[tokio::test]
    async fn one_source_that_fails_is_the_exit_code() {
        // the single-config case has to behave exactly as it did before
        let result = wait_for_all(set_of(vec![("only", fails("cannot connect"))])).await;
        assert!(format!("{:#}", result.expect_err("halted")).contains("cannot connect"));
    }

    #[tokio::test]
    async fn a_clean_drain_is_success() {
        assert!(
            wait_for_all(set_of(vec![("a", ok()), ("b", ok())]))
                .await
                .is_ok()
        );
    }
}
