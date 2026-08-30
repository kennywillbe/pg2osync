//! Re-reading the configuration file while the pipeline runs.
//!
//! SIGHUP and nothing else. There is no `pg2osync reload` subcommand because a
//! second process would have to find the first one, and the only portable way
//! to do that is a pidfile — a piece of state outside the target, which is the
//! one place this project keeps state. Under a container runtime the step is
//! the same either way (`kubectl exec -- kill -HUP 1`), and under systemd it is
//! `ExecReload`.
//!
//! The whole file is validated before anything is applied, so a file with a
//! mistake anywhere in it changes nothing at all. What survives that is split
//! in two: the handful of settings a batch re-reads each time round, which are
//! swapped, and everything else, which is refused in place with the reason —
//! naming the field and what it would take. Refusing loudly is the point. A
//! reload that silently ignored half the file would be worse than no reload,
//! because the file would stop describing the process.

use anyhow::Result;
use pg2osync_core::sink::Sink;
use pg2osync_engine::EngineSettings;
use pg2osync_engine::metrics::SharedMetrics;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::watch;

use crate::config::AppConfig;

/// The configuration file, and the signals asking for it to be read again.
pub struct ReloadSource {
    pub path: PathBuf,
    /// A generation counter rather than a bare notification: two SIGHUPs a
    /// moment apart are two increments, and one that arrives while a reload is
    /// being applied coalesces into the next one rather than being lost.
    pub generations: watch::Receiver<u64>,
}

/// What the reload task may reach into.
pub struct Handles {
    pub settings: watch::Sender<EngineSettings>,
    pub sink: Arc<dyn Sink>,
    pub metrics: SharedMetrics,
}

/// The values a reload is allowed to change on a running pipeline.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Hot {
    pub settings: EngineSettings,
    pub retry_max: u32,
    pub retry_backoff_ms: u64,
    pub retry_max_elapsed_ms: Option<u64>,
    pub log_filter: Option<String>,
}

impl Hot {
    pub fn of(cfg: &AppConfig) -> Self {
        Self {
            settings: cfg.engine.settings(),
            retry_max: cfg.engine.retry_max.max(1),
            retry_backoff_ms: cfg.engine.retry_backoff_ms.max(1),
            retry_max_elapsed_ms: cfg.engine.retry_max_elapsed_ms,
            log_filter: cfg.log.filter.clone(),
        }
    }
}

/// One reload's verdict: what to apply, and what the file asked for that a
/// running pipeline cannot do.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Plan {
    pub hot: Hot,
    /// One line per refusal, each naming the field and what it would take.
    pub refusals: Vec<String>,
}

impl Plan {
    /// How this reload is counted, and how its summary line reads.
    pub fn result(&self) -> &'static str {
        if self.refusals.is_empty() {
            "applied"
        } else {
            "refused"
        }
    }
}

/// SIGHUP as a generation counter.
///
/// Installing a handler changes what the signal does: its default disposition
/// is to terminate, so a deployment that was using `kill -HUP` as a blunt
/// restart now gets a reload instead.
#[cfg(unix)]
pub fn on_sighup() -> watch::Receiver<u64> {
    let (tx, rx) = watch::channel(0u64);
    tokio::spawn(async move {
        use tokio::signal::unix::{SignalKind, signal};
        let mut hangup = match signal(SignalKind::hangup()) {
            Ok(stream) => stream,
            Err(e) => {
                tracing::warn!(target: "pg2osync::reload",
                    "cannot listen for SIGHUP ({e}); the configuration can only change \
                     with a restart");
                return;
            }
        };
        let mut generation = 0u64;
        while hangup.recv().await.is_some() {
            generation += 1;
            let _ = tx.send(generation);
        }
    });
    rx
}

/// No SIGHUP off unix, so the counter never moves and the task idles.
#[cfg(not(unix))]
pub fn on_sighup() -> watch::Receiver<u64> {
    watch::channel(0u64).1
}

/// Watch for reload requests until the process ends.
pub fn spawn(source: ReloadSource, cfg: AppConfig, handles: Handles) {
    tokio::spawn(async move {
        let mut source = source;
        let mut current = cfg;
        while source.generations.changed().await.is_ok() {
            let generation = *source.generations.borrow();
            apply(&source.path, generation, &mut current, &handles).await;
        }
    });
}

/// Read the file, decide what it may change, change it.
///
/// `current` is what the pipeline is running, and it is advanced only by what
/// was actually applied: a refused section stays as it is, so the next reload
/// diffs against the running definition and repeats the refusal rather than
/// quietly accepting a change that never took.
async fn apply(path: &Path, generation: u64, current: &mut AppConfig, handles: &Handles) {
    tracing::info!(target: "pg2osync::reload",
        "SIGHUP: re-reading {} (generation {generation})", path.display());
    let owned = path.to_path_buf();
    // The read and the TOML parse are blocking, and a mapping_file makes it a
    // second read; a signal must not stall the runtime on the operator's disk.
    let loaded = tokio::task::spawn_blocking(move || AppConfig::load(&owned)).await;
    let new = match loaded {
        Ok(Ok(cfg)) => cfg,
        Ok(Err(e)) => {
            tracing::error!(target: "pg2osync::reload",
                "the configuration was not reloaded and nothing changed: {e:#}");
            handles.metrics.incr_config_reload("invalid");
            return;
        }
        Err(e) => {
            tracing::error!(target: "pg2osync::reload",
                "the configuration could not be read: {e}");
            handles.metrics.incr_config_reload("failed");
            return;
        }
    };

    let plan = classify(current, &new);
    for refusal in &plan.refusals {
        tracing::error!(target: "pg2osync::reload", "{refusal}");
    }
    let before = Hot::of(current);
    if plan.hot != before {
        handles.settings.send_replace(plan.hot.settings);
        handles.sink.set_retry_policy(
            plan.hot.retry_max,
            plan.hot.retry_backoff_ms,
            plan.hot.retry_max_elapsed_ms,
        );
        if plan.hot.log_filter != before.log_filter
            && let Err(e) = apply_log_filter(plan.hot.log_filter.as_deref())
        {
            tracing::error!(target: "pg2osync::reload", "the log filter was not changed: {e:#}");
            handles.metrics.incr_config_reload("failed");
            return;
        }
        // The engine's own values are what a batch reads; these are only the
        // fields worth naming in a log an operator reads after the fact.
        tracing::info!(target: "pg2osync::reload",
            "applied: batch_size={} batch_max_bytes={} txn_buffer_cap_mb={} \
             checkpoint_interval_ms={} retry_max={} retry_backoff_ms={} \
             retry_max_elapsed_ms={:?}",
            plan.hot.settings.batch_size, plan.hot.settings.batch_max_bytes,
            plan.hot.settings.txn_buffer_cap_mb, plan.hot.settings.checkpoint_interval_ms,
            plan.hot.retry_max, plan.hot.retry_backoff_ms, plan.hot.retry_max_elapsed_ms);
    } else if plan.refusals.is_empty() {
        tracing::info!(target: "pg2osync::reload", "the configuration is unchanged");
    }
    // Only the applied half moves forward; everything else keeps running as it
    // was, which is what the refusals above said would happen.
    current.engine.batch_size = plan.hot.settings.batch_size;
    current.engine.batch_max_bytes = plan.hot.settings.batch_max_bytes;
    current.engine.txn_buffer_cap_mb = plan.hot.settings.txn_buffer_cap_mb;
    current.engine.checkpoint_interval_ms = plan.hot.settings.checkpoint_interval_ms;
    current.engine.load_max_rows_per_sec = plan.hot.settings.load_max_rows_per_sec;
    current.engine.retry_max = plan.hot.retry_max;
    current.engine.retry_backoff_ms = plan.hot.retry_backoff_ms;
    current.engine.retry_max_elapsed_ms = plan.hot.retry_max_elapsed_ms;
    current.log.filter = plan.hot.log_filter.clone();

    handles.metrics.incr_config_reload(plan.result());
}

/// Name the fields of `$section` that differ, each with the reason a running
/// pipeline cannot take the change.
macro_rules! refuse_changed {
    ($out:expr, $old:expr, $new:expr, $section:expr, $why:expr, [$($field:ident),+ $(,)?]) => {
        $(if $old.$field != $new.$field {
            $out.push(format!(
                "[{}] {} changed from {} to {}; {}",
                $section,
                stringify!($field),
                brief(&$old.$field),
                brief(&$new.$field),
                $why,
            ));
        })+
    };
}

/// Which fields of `$section` differ, by name only.
macro_rules! names_changed {
    ($out:expr, $old:expr, $new:expr, [$(($field:ident, $name:expr)),+ $(,)?]) => {
        $(if $old.$field != $new.$field {
            $out.push($name);
        })+
    };
}

/// A value as a refusal names it, short enough to keep the line readable.
fn brief<T: std::fmt::Debug>(value: &T) -> String {
    let rendered = format!("{value:?}");
    if rendered.chars().count() <= 80 {
        return rendered;
    }
    let head: String = rendered.chars().take(77).collect();
    format!("{head}...")
}

const WHY_SOURCE: &str = "the stream, the slot and the connection to the source are opened when \
                          the pipeline starts and the checkpoint is named by them, so the running \
                          value is kept — restart to change it";
const WHY_TARGET: &str = "the target's client and the checkpoint it holds are built when the \
                          pipeline starts, so the running value is kept — restart to change it";
const WHY_LISTENER: &str = "the listener is bound once and an environment variable is fixed when \
                            the process is executed, so the running value is kept — restart to \
                            change it";
const WHY_ENGINE: &str = "the sink task is built with it when a streaming attempt starts, so the \
                          running value is kept — restart to change it";
const WHY_POLL: &str = "the poll query is built when a streaming attempt starts and this decides \
                        which rows it picks up, so the running value is kept — restart to change \
                        it";

/// What a reload of `new` over the running `old` may and may not do.
pub fn classify(old: &AppConfig, new: &AppConfig) -> Plan {
    let mut refusals = Vec::new();

    // `[source] name` is deliberately absent: it names a source among the
    // files of a config directory, which only `validate` and `status` read.
    // A running pipeline never consults it, so refusing a change to it would
    // claim an effect it does not have.
    refuse_changed!(
        refusals,
        old.source,
        new.source,
        "source",
        WHY_SOURCE,
        [
            mode,
            flavor,
            server_id,
            poll_column,
            poll_interval_secs,
            poll_page_size,
            url,
            url_env,
            sslmode,
            sslrootcert,
            sslcert,
            sslkey,
            admin_url_env,
            reconnect_max,
            reconnect_backoff_ms,
            load_chunk_rows,
            load_workers,
            slot_name,
            publication,
        ]
    );
    refuse_changed!(
        refusals,
        old.target,
        new.target,
        "target",
        WHY_TARGET,
        [
            url,
            flavor,
            username,
            password,
            password_env,
            api_key_env,
            tls_verify,
            state_dir,
            require_alias,
        ]
    );
    refuse_changed!(
        refusals,
        old.metrics,
        new.metrics,
        "metrics",
        WHY_LISTENER,
        [enabled, bind, token_env,]
    );
    refuse_changed!(
        refusals,
        old.api,
        new.api,
        "api",
        WHY_LISTENER,
        [enabled, bind, token_env,]
    );
    refuse_changed!(
        refusals,
        old.engine,
        new.engine,
        "engine",
        WHY_ENGINE,
        [write_concurrency, on_permanent_rejection, max_rejects,]
    );

    for (key, tbl) in &new.sync {
        match old.sync.get(key) {
            None => refusals.push(format!(
                "[sync.{key}] is new: a table joins a running pipeline only once its rows have \
                 been loaded beside the stream and — on PostgreSQL — its name is in the \
                 publication, so it is not being synced. Restart to pick it up"
            )),
            Some(running) => refusals.extend(section_refusals(key, running, tbl)),
        }
    }
    for key in old.sync.keys() {
        if !new.sync.contains_key(key) {
            refusals.push(format!(
                "[sync.{key}] was removed but its rows are still being routed: dropping a table \
                 from a running stream is a restart. The index it writes to is left as it is \
                 either way — nothing here deletes documents"
            ));
        }
    }

    Plan {
        hot: Hot::of(new),
        refusals,
    }
}

/// Why one section cannot be changed under the running pipeline.
///
/// Two classes, and the difference is what it costs to put right. Identity —
/// which document a row *is* — cannot be changed at all without rewriting
/// every document the section already wrote, so the answer is a re-index
/// behind an alias. A reshape only changes the document's shape, so the index
/// holds a mixture until the table is read again; the answer there is a
/// re-snapshot. Both are refused in place: applying either hot would leave one
/// index holding documents built two ways, with nothing recording which.
/// `poll_column` is neither, and is handled on its own below.
fn section_refusals(
    key: &str,
    old: &crate::config::TableSync,
    new: &crate::config::TableSync,
) -> Vec<String> {
    let mut out = Vec::new();
    let identity = format!(
        "changing what a row is filed as means every document this section already wrote is \
         filed the old way, so the running definition is kept. Re-index into a new name and \
         move the alias onto it: pg2osync reindex --table {} --alias <alias>",
        old.table
    );
    refuse_changed!(
        out,
        old,
        new,
        format!("sync.{key}"),
        identity,
        [table, index, primary_key, append_only, id, routing,]
    );
    // Structural, and their Debug form is a whole nested table, so these are
    // named rather than printed.
    for what in ["fan_out", "join", "children"] {
        let differs = match what {
            "fan_out" => old.fan_out != new.fan_out,
            "join" => old.join != new.join,
            _ => old.children != new.children,
        };
        if differs {
            out.push(format!("[sync.{key}] {what} changed; {identity}"));
        }
    }
    // A third class of one. `poll_column` decides which rows the poll query
    // picks up, so it neither refiles a document nor reshapes one: a rebuild
    // and a re-snapshot are both the wrong advice, and it is reported whatever
    // else the section changed because neither of those would cover it.
    let mut restart = Vec::new();
    refuse_changed!(
        restart,
        old,
        new,
        format!("sync.{key}"),
        WHY_POLL,
        [poll_column]
    );
    if !out.is_empty() {
        out.extend(restart);
        return out;
    }

    let mut reshaped: Vec<&str> = Vec::new();
    names_changed!(
        reshaped,
        old,
        new,
        [
            (columns, "columns"),
            (exclude_columns, "exclude_columns"),
            (transform, "transform"),
            (fields, "fields"),
            (constants, "constants"),
            (filter, "where"),
            (pipeline, "pipeline"),
            (soft_delete, "soft_delete"),
            (mapping_file, "mapping_file"),
            (mapping, "the contents of mapping_file"),
        ]
    );
    if !reshaped.is_empty() {
        out.push(format!(
            "[sync.{key}] {} changed, so the documents this section writes would change shape: \
             the index already holds documents built the old way and nothing records which, so \
             the running definition is kept. Change it and restart, then read the table again: \
             pg2osync resnapshot --table {}",
            reshaped.join(", "),
            old.table
        ));
    }
    out.extend(restart);
    out
}

/// The process's log filter, swapped behind the subscriber that was installed
/// at startup.
type FilterReload =
    Arc<dyn Fn(tracing_subscriber::EnvFilter) -> Result<(), String> + Send + Sync + 'static>;

static LOG_FILTER: std::sync::OnceLock<FilterReload> = std::sync::OnceLock::new();

/// Hand the reload path the subscriber's filter handle. Called once, by
/// `init_logging`; a process-global because the subscriber is one.
pub fn set_filter_reload(reload: FilterReload) {
    let _ = LOG_FILTER.set(reload);
}

/// What this process logs when nothing in the file says otherwise.
pub fn default_filter() -> tracing_subscriber::EnvFilter {
    tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "pg2osync=info".into())
}

/// Whether a filter from the file is currently installed, so removing the key
/// puts the default back instead of leaving the last one in place forever.
static FROM_FILE: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

/// Put `[log] filter` into effect, or say why it is not.
///
/// `RUST_LOG` wins wherever it is set: the environment is what a container
/// runtime, a systemd unit and a developer's shell all reach for, and a config
/// file that quietly overrode it would make the variable a lie. The file's
/// filter exists for the case the variable cannot serve — the environment is
/// fixed when the process is executed, so it is the only way to turn a level
/// up on a pipeline that is already running.
pub fn apply_log_filter(filter: Option<&str>) -> Result<()> {
    use std::sync::atomic::Ordering;

    if std::env::var_os("RUST_LOG").is_some() {
        if filter.is_some() {
            tracing::info!(target: "pg2osync::reload",
                "[log] filter is set and so is RUST_LOG, which wins; unset RUST_LOG to let \
                 the configuration file decide what is logged");
        }
        return Ok(());
    }
    // Nothing to put back where nothing was ever taken away.
    if filter.is_none() && !FROM_FILE.load(Ordering::Relaxed) {
        return Ok(());
    }
    let Some(reload) = LOG_FILTER.get() else {
        anyhow::bail!("the log subscriber has no reload handle");
    };
    let parsed = match filter {
        Some(filter) => tracing_subscriber::EnvFilter::try_new(filter)
            .map_err(|e| anyhow::anyhow!("[log] filter {filter:?} is not a filter: {e}"))?,
        None => default_filter(),
    };
    let rendered = parsed.to_string();
    reload(parsed).map_err(|e| anyhow::anyhow!("[log] filter was not applied: {e}"))?;
    FROM_FILE.store(filter.is_some(), Ordering::Relaxed);
    tracing::info!(target: "pg2osync::reload", "logging at {rendered:?}");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(body: &str) -> AppConfig {
        let cfg: AppConfig = toml::from_str(body).expect("parses");
        cfg
    }

    const BASE: &str = r#"
[source]
url = "postgres://u:p@localhost/db"
[target]
url = "http://localhost:9200"
[sync.users]
table = "public.users"
"#;

    #[test]
    fn the_settings_a_batch_reads_are_taken_and_nothing_is_refused() {
        let old = config(BASE);
        let new = config(&format!(
            "{BASE}\n[engine]\nbatch_size = 5\ncheckpoint_interval_ms = 50\n\
             retry_max = 3\nretry_max_elapsed_ms = 30000\nload_max_rows_per_sec = 100\n"
        ));
        let plan = classify(&old, &new);
        assert!(plan.refusals.is_empty(), "{:?}", plan.refusals);
        assert_eq!(plan.hot.settings.batch_size, 5);
        assert_eq!(plan.hot.settings.checkpoint_interval_ms, 50);
        assert_eq!(plan.hot.settings.load_max_rows_per_sec, Some(100));
        assert_eq!(plan.hot.retry_max, 3);
        assert_eq!(plan.hot.retry_max_elapsed_ms, Some(30_000));
        assert_eq!(plan.result(), "applied");
    }

    #[test]
    fn an_engine_setting_the_sink_task_was_built_with_is_refused_by_name() {
        let old = config(BASE);
        let new = config(&format!(
            "{BASE}\n[engine]\nwrite_concurrency = 4\nmax_rejects = 7\n"
        ));
        let plan = classify(&old, &new);
        assert_eq!(plan.result(), "refused");
        assert!(
            plan.refusals
                .iter()
                .any(|r| r.contains("[engine] write_concurrency changed from 1 to 4")),
            "{:?}",
            plan.refusals
        );
        assert!(plan.refusals.iter().any(|r| r.contains("max_rejects")));
        // the hot half is still what the file says: a refusal elsewhere does
        // not hold back the settings the operator may change
        assert_eq!(plan.hot.settings.batch_size, 500);
    }

    #[test]
    fn the_stream_and_the_target_the_checkpoint_is_bound_to_are_refused() {
        let old = config(BASE);
        for (field, body) in [
            (
                "slot_name",
                BASE.replace("[source]", "[source]\nslot_name = \"other\""),
            ),
            (
                "url",
                BASE.replace("http://localhost:9200", "http://elsewhere:9200"),
            ),
        ] {
            let plan = classify(&old, &config(&body));
            assert!(
                plan.refusals.iter().any(|r| r.contains(field)),
                "{field} should be refused, got {:?}",
                plan.refusals
            );
        }
    }

    #[test]
    fn a_listener_bound_once_and_a_token_read_from_the_environment_are_refused() {
        let old = config(BASE);
        let new = config(&format!(
            "{BASE}\n[metrics]\nbind = \"0.0.0.0:9100\"\ntoken_env = \"TOKEN\"\n\
             [api]\nenabled = true\n"
        ));
        let plan = classify(&old, &new);
        assert_eq!(plan.refusals.len(), 3, "{:?}", plan.refusals);
        assert!(
            plan.refusals
                .iter()
                .all(|r| r.contains("restart to change it"))
        );
    }

    #[test]
    fn an_identity_change_names_both_values_and_points_at_a_rebuild() {
        let old = config(BASE);
        let new = config(&BASE.replace(
            "table = \"public.users\"",
            "table = \"public.users\"\nid = \"user-{id}\"",
        ));
        let plan = classify(&old, &new);
        assert_eq!(plan.refusals.len(), 1, "{:?}", plan.refusals);
        let refusal = &plan.refusals[0];
        assert!(refusal.starts_with("[sync.users] id changed from None to Some(\"user-{id}\")"));
        assert!(refusal.contains("pg2osync reindex --table public.users"));
    }

    #[test]
    fn a_reshaped_document_is_refused_as_one_line_naming_every_field() {
        let old = config(BASE);
        let new = config(&BASE.replace(
            "table = \"public.users\"",
            "table = \"public.users\"\ncolumns = [\"id\", \"email\"]\nwhere = \"id > 0\"",
        ));
        let plan = classify(&old, &new);
        assert_eq!(plan.refusals.len(), 1, "{:?}", plan.refusals);
        let refusal = &plan.refusals[0];
        assert!(refusal.contains("columns, where changed"), "{refusal}");
        assert!(refusal.contains("pg2osync resnapshot --table public.users"));
    }

    #[test]
    fn an_identity_change_hides_the_reshape_it_came_with_because_the_rebuild_covers_both() {
        let old = config(BASE);
        let new = config(&BASE.replace(
            "table = \"public.users\"",
            "table = \"public.users\"\nindex = \"people\"\ncolumns = [\"id\"]",
        ));
        let plan = classify(&old, &new);
        assert_eq!(plan.refusals.len(), 1, "{:?}", plan.refusals);
        assert!(plan.refusals[0].contains("index changed"));
    }

    #[test]
    fn a_poll_column_asks_for_a_restart_and_survives_a_refusal_that_would_hide_it() {
        let old = config(BASE);
        let new = config(&BASE.replace(
            "table = \"public.users\"",
            "table = \"public.users\"\npoll_column = \"changed_at\"",
        ));
        let plan = classify(&old, &new);
        assert_eq!(plan.refusals.len(), 1, "{:?}", plan.refusals);
        let refusal = &plan.refusals[0];
        assert!(
            refusal.contains("[sync.users] poll_column changed"),
            "{refusal}"
        );
        assert!(refusal.contains("restart to change it"), "{refusal}");
        assert!(
            !refusal.contains("reindex") && !refusal.contains("resnapshot"),
            "which rows the poll reads is neither a refile nor a reshape: {refusal}"
        );

        // an identity change hides a reshape, because the rebuild covers both;
        // it does not cover this, so this is still reported beside it
        let both = config(&BASE.replace(
            "table = \"public.users\"",
            "table = \"public.users\"\nid = \"user-{id}\"\npoll_column = \"changed_at\"",
        ));
        let plan = classify(&old, &both);
        assert_eq!(plan.refusals.len(), 2, "{:?}", plan.refusals);
        assert!(plan.refusals.iter().any(|r| r.contains("id changed")));
        assert!(
            plan.refusals
                .iter()
                .any(|r| r.contains("poll_column changed"))
        );
    }

    #[test]
    fn a_section_that_appears_or_disappears_says_what_is_happening_to_its_rows() {
        let old = config(BASE);
        let added = config(&format!(
            "{BASE}\n[sync.orders]\ntable = \"public.orders\"\n"
        ));
        let plan = classify(&old, &added);
        assert_eq!(plan.refusals.len(), 1);
        assert!(plan.refusals[0].contains("[sync.orders] is new"));

        let plan = classify(&added, &old);
        assert_eq!(plan.refusals.len(), 1);
        assert!(plan.refusals[0].contains("[sync.orders] was removed"));
        assert!(plan.refusals[0].contains("index"));
    }

    #[test]
    fn an_unchanged_file_is_applied_and_changes_nothing() {
        let cfg = config(BASE);
        let plan = classify(&cfg, &cfg);
        assert!(plan.refusals.is_empty());
        assert_eq!(plan.hot, Hot::of(&cfg));
    }

    #[test]
    fn an_unparsable_log_filter_fails_validation_so_a_reload_never_reaches_it() {
        let body = format!("{BASE}\n[log]\nfilter = \"not a filter=@@\"\n");
        let cfg: AppConfig = toml::from_str(&body).expect("parses as TOML");
        assert!(cfg.validate().is_err(), "the filter has to be refused");
    }

    #[test]
    fn a_long_value_is_shortened_so_a_refusal_stays_one_readable_line() {
        let long = "x".repeat(200);
        let rendered = brief(&long);
        assert!(rendered.ends_with("..."));
        assert_eq!(rendered.chars().count(), 80);
        assert_eq!(brief(&"short"), "\"short\"");
    }
}
