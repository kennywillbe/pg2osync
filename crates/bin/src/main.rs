//! CLI entry point. This is the only crate that turns typed errors into exit
//! codes and human-readable diagnostics.

mod backfill;
mod config;
mod reconcile;
mod reindex;
mod reload;
mod resnapshot;
mod run;
mod supervisor;
mod workspace;

use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand};
use pg2osync_core::lsn::Lsn;
use pg2osync_core::sink::Health;
use std::path::{Path, PathBuf};

#[derive(Parser)]
#[command(
    name = "pg2osync",
    version,
    about = "Real-time PostgreSQL/MySQL to OpenSearch, Elasticsearch and Meilisearch sync"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

/// Where the configuration comes from: one file, or a directory of them.
///
/// Alternatives everywhere they appear — a command reads one file or one
/// directory, and clap refuses both.
#[derive(clap::Args)]
struct ConfigArgs {
    #[arg(
        short,
        long,
        value_name = "FILE",
        default_value = "pg2osync.toml",
        group = "configs"
    )]
    config: PathBuf,
    /// Every *.toml directly in this directory, loaded and validated as one
    /// set.
    #[arg(long, value_name = "DIR", group = "configs")]
    config_dir: Option<PathBuf>,
}

#[derive(Subcommand)]
enum Command {
    /// Initial load plus continuous streaming (main mode).
    ///
    /// A directory runs as one process: one metrics port, one /synced
    /// endpoint, and a pipeline of its own per file.
    Run {
        #[command(flatten)]
        config: ConfigArgs,
        /// Act on this source only. Without it, every source the config
        /// names.
        #[arg(long, value_name = "NAME")]
        source: Option<String>,
    },
    /// Validate the config and check both connections.
    ///
    /// A directory is checked as one set — two sources sharing a slot, an
    /// index or a name — and every failure in it is reported, not the first.
    Validate {
        #[command(flatten)]
        config: ConfigArgs,
        /// Act on this source only. Without it, every source the config
        /// names.
        #[arg(long, value_name = "NAME")]
        source: Option<String>,
    },
    /// Create source-side objects and target indices, then exit.
    Bootstrap {
        #[command(flatten)]
        config: ConfigArgs,
        /// Act on this source only. Without it, every source the config
        /// names.
        #[arg(long, value_name = "NAME")]
        source: Option<String>,
    },
    /// Show the checkpoint and the source's current position.
    Status {
        #[command(flatten)]
        config: ConfigArgs,
        /// Act on this source only. Without it, every source the config
        /// names.
        #[arg(long, value_name = "NAME")]
        source: Option<String>,
        /// Exit 0 only once the checkpoint has reached the source's current
        /// position, so a script can wait instead of comparing by eye.
        #[arg(long)]
        caught_up: bool,
        /// Seconds to keep checking with --caught-up.
        #[arg(long, default_value_t = 0)]
        timeout: u64,
        /// Exit non-zero when any slot is holding more than this many MB of
        /// WAL, so a scheduled check catches a pipeline that has been down
        /// long enough to threaten the source's disk.
        #[arg(long, value_name = "MB")]
        max_retained_mb: Option<u64>,
    },
    /// Point an alias at this config's index, atomically.
    SwitchAlias {
        #[command(flatten)]
        config: ConfigArgs,
        /// Which source to act on. Required when --config-dir names more
        /// than one.
        #[arg(long, value_name = "NAME")]
        source: Option<String>,
        /// The alias to move. Applies to the single configured table.
        #[arg(long)]
        alias: String,
    },
    /// Compare each index against its source table and report documents whose
    /// row is gone. Reports only unless --delete is given.
    Reconcile {
        #[command(flatten)]
        config: ConfigArgs,
        /// Which source to act on. Required when --config-dir names more
        /// than one.
        #[arg(long, value_name = "NAME")]
        source: Option<String>,
        /// Remove the documents instead of only naming them.
        #[arg(long)]
        delete: bool,
    },
    /// Write a starter config, checking the tables it names against the source.
    ///
    /// The first thing anyone needs and the one thing the other subcommands
    /// cannot do: every one of them takes a `-c FILE` that has to exist first.
    Init {
        /// Where to write it. Refuses to overwrite without --force.
        #[arg(short, long, value_name = "FILE", default_value = "pg2osync.toml")]
        config: PathBuf,
        /// Tables to sync. An unqualified name is qualified from the source, so
        /// `--table users` becomes `public.users` rather than an error later.
        #[arg(long = "table", value_name = "TABLE")]
        tables: Vec<String>,
        /// Source URL. Defaults to $PG2OSYNC_SOURCE_URL, which is also what the
        /// generated config reads at run time.
        #[arg(long, value_name = "URL")]
        source: Option<String>,
        /// Target URL.
        #[arg(long, value_name = "URL", default_value = "http://localhost:9200")]
        target: String,
        /// "postgres" (default) or "mysql".
        #[arg(long, default_value = "postgres")]
        flavor: String,
        #[arg(long)]
        force: bool,
    },
    /// Print the SQL a DBA needs to run, derived from the config.
    SetupSql {
        #[command(flatten)]
        config: ConfigArgs,
        /// Which source to act on. Required when --config-dir names more
        /// than one.
        #[arg(long, value_name = "NAME")]
        source: Option<String>,
    },
    /// Read one table again into its index, without reloading everything else.
    ///
    /// Adds and updates; never deletes — `reconcile` is the other half. Safe to
    /// run while the pipeline is streaming: its rows carry the position they were
    /// read at, so a concurrent change wins.
    Resnapshot {
        #[command(flatten)]
        config: ConfigArgs,
        /// Which source to act on. Required when --config-dir names more
        /// than one.
        #[arg(long, value_name = "NAME")]
        source: Option<String>,
        /// Qualified table name, as it appears in the config.
        #[arg(long, value_name = "SCHEMA.TABLE")]
        table: String,
        /// SQL predicate narrowing what is re-read, e.g. "tenant_id = 42".
        #[arg(long = "where", value_name = "PREDICATE")]
        filter: Option<String>,
    },
    /// Rebuild one table's index under a fresh name and point an alias at it.
    ///
    /// Stop the pipeline first: the fresh index is one the stream is not
    /// writing to, so a row that changes during the rebuild has nothing there
    /// to lose to. The checkpoint does not move, so the restart replays
    /// everything committed since.
    Reindex {
        #[command(flatten)]
        config: ConfigArgs,
        /// Which source to act on. Required when --config-dir names more
        /// than one.
        #[arg(long, value_name = "NAME")]
        source: Option<String>,
        /// Qualified table name, as it appears in the config.
        #[arg(long, value_name = "SCHEMA.TABLE")]
        table: String,
        /// The alias to point at the rebuilt index. On Meilisearch, which has
        /// no aliases, this is the configured index itself: the rebuilt one is
        /// swapped into that name.
        #[arg(long)]
        alias: String,
        /// Delete whichever index is left holding the documents from before the
        /// rebuild. Kept by default: it is the rollback, one switch away.
        #[arg(long)]
        drop_old: bool,
    },
    /// List the documents the target refused and, with --replay, submit them
    /// again once the mapping that refused them is fixed.
    ///
    /// The quarantine store belongs to the target, not to a source: every
    /// source writing to that target shares it. So --source picks the
    /// configuration this reaches the target with, not which documents are
    /// listed.
    Rejects {
        #[command(flatten)]
        config: ConfigArgs,
        /// Which source to act on. Required when --config-dir names more
        /// than one.
        #[arg(long, value_name = "NAME")]
        source: Option<String>,
        /// Submit each one again, clearing the ones the target now accepts.
        #[arg(long)]
        replay: bool,
        /// How many to show.
        #[arg(long, default_value_t = 20)]
        limit: usize,
    },
    /// Drop the replication slot (PostgreSQL only).
    DropSlot {
        #[command(flatten)]
        config: ConfigArgs,
        /// Which source to act on. Required when --config-dir names more
        /// than one.
        #[arg(long, value_name = "NAME")]
        source: Option<String>,
        /// Also drop the publication. Off by default: a second pipeline may be
        /// reading the same one.
        #[arg(long)]
        publication: bool,
    },
}

/// How log lines are written. An environment variable rather than a config key
/// because the format belongs to whoever collects the logs of a deployment, not
/// to the pipeline the config describes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum LogFormat {
    Text,
    Json,
}

impl LogFormat {
    fn parse(value: &str) -> Result<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "text" => Ok(Self::Text),
            "json" => Ok(Self::Json),
            other => bail!("PG2OSYNC_LOG_FORMAT is \"{other}\": expected \"text\" or \"json\""),
        }
    }

    fn from_env() -> Result<Self> {
        match std::env::var("PG2OSYNC_LOG_FORMAT") {
            Ok(value) => Self::parse(&value),
            Err(_) => Ok(Self::Text),
        }
    }
}

/// Install the subscriber: the filter, then the traces, then the format, on one
/// registry.
///
/// The filter goes in behind a reload layer, which is what lets `[log] filter`
/// change on a running process. It is the outermost layer, so turning a level
/// up or down decides what is exported as well as what is printed.
fn init_logging(
    format: LogFormat,
    traces: Option<impl tracing_subscriber::Layer<LogRegistry> + Send + Sync + 'static>,
) {
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;

    let (filter, handle) = tracing_subscriber::reload::Layer::new(reload::default_filter());
    reload::set_filter_reload(std::sync::Arc::new(move |filter| {
        handle.reload(filter).map_err(|e| e.to_string())
    }));
    let base = tracing_subscriber::registry().with(filter).with(traces);
    let fmt = tracing_subscriber::fmt::layer();
    match format {
        LogFormat::Text => base.with(fmt).init(),
        // Flattened, so a collector reads the event's own fields as top-level
        // keys beside level and target rather than under a nested object.
        LogFormat::Json => base.with(fmt.json().flatten_event(true)).init(),
    }
}

/// What the trace layer is layered onto: the registry plus the reloadable
/// filter, which is what is built before any format layer is added.
type LogRegistry = tracing_subscriber::layer::Layered<
    tracing_subscriber::reload::Layer<tracing_subscriber::EnvFilter, tracing_subscriber::Registry>,
    tracing_subscriber::Registry,
>;

/// Where traces go, and how many of them.
///
/// Environment variables rather than config keys, for the reason the log format
/// is one: the tracing backend belongs to whoever runs the deployment, not to
/// the pipeline the config file describes. No endpoint means nothing is built —
/// no exporter, no batch thread, no connection attempt.
#[derive(Debug, PartialEq)]
struct TraceExport {
    endpoint: String,
    /// Fraction of traces to keep, honoured only where nothing upstream has
    /// already decided: a caller's sampled `traceparent` outranks it.
    ratio: f64,
    service_name: String,
}

impl TraceExport {
    fn from_env() -> Result<Option<Self>> {
        Self::parse(
            std::env::var("PG2OSYNC_OTLP_ENDPOINT").ok().as_deref(),
            std::env::var("PG2OSYNC_OTLP_SAMPLE_RATIO").ok().as_deref(),
            std::env::var("PG2OSYNC_OTLP_SERVICE_NAME").ok().as_deref(),
        )
    }

    /// Kept separate from `from_env` so the rules can be tested without a
    /// process environment every other test in the binary shares.
    fn parse(
        endpoint: Option<&str>,
        ratio: Option<&str>,
        service_name: Option<&str>,
    ) -> Result<Option<Self>> {
        let Some(endpoint) = endpoint.map(str::trim).filter(|e| !e.is_empty()) else {
            return Ok(None);
        };
        let ratio = match ratio.map(str::trim).filter(|r| !r.is_empty()) {
            Some(raw) => raw
                .parse::<f64>()
                .ok()
                .filter(|r| (0.0..=1.0).contains(r))
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "PG2OSYNC_OTLP_SAMPLE_RATIO is {raw:?}: expected a number \
                         from 0.0 to 1.0"
                    )
                })?,
            None => 1.0,
        };
        Ok(Some(Self {
            endpoint: endpoint.to_string(),
            ratio,
            service_name: service_name
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .unwrap_or("pg2osync")
                .to_string(),
        }))
    }
}

/// An exporter that says the collector is unreachable once, then stops saying
/// it.
///
/// A tracing backend that is down must cost the pipeline nothing: the batch
/// processor drops the spans it could not send and replication carries on, and
/// the operator gets one line rather than one per batch for as long as the
/// outage lasts.
#[derive(Debug)]
struct QuietExporter<E> {
    inner: E,
    complained: std::sync::atomic::AtomicBool,
}

impl<E: opentelemetry_sdk::trace::SpanExporter> opentelemetry_sdk::trace::SpanExporter
    for QuietExporter<E>
{
    async fn export(
        &self,
        batch: Vec<opentelemetry_sdk::trace::SpanData>,
    ) -> opentelemetry_sdk::error::OTelSdkResult {
        let result = self.inner.export(batch).await;
        if let Err(e) = &result
            && !self
                .complained
                .swap(true, std::sync::atomic::Ordering::Relaxed)
        {
            tracing::warn!(target: "pg2osync::traces",
                "spans are not reaching the OTLP endpoint and are being dropped; \
                 replication is unaffected: {e}");
        }
        result
    }

    fn shutdown_with_timeout(
        &self,
        timeout: std::time::Duration,
    ) -> opentelemetry_sdk::error::OTelSdkResult {
        self.inner.shutdown_with_timeout(timeout)
    }

    fn force_flush(&self) -> opentelemetry_sdk::error::OTelSdkResult {
        self.inner.force_flush()
    }

    fn set_resource(&mut self, resource: &opentelemetry_sdk::Resource) {
        self.inner.set_resource(resource);
    }
}

/// Build the OTLP exporter and the provider that owns it.
///
/// Called from inside the tokio runtime, which the tonic client needs.
fn build_tracer_provider(cfg: &TraceExport) -> Result<opentelemetry_sdk::trace::SdkTracerProvider> {
    use opentelemetry_otlp::WithExportConfig;

    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_tonic()
        .with_endpoint(&cfg.endpoint)
        .build()
        .with_context(|| format!("cannot build an OTLP exporter for {}", cfg.endpoint))?;
    Ok(opentelemetry_sdk::trace::SdkTracerProvider::builder()
        .with_batch_exporter(QuietExporter {
            inner: exporter,
            complained: std::sync::atomic::AtomicBool::new(false),
        })
        // Parent-based, so a caller who decided to sample their own request has
        // decided for the writes that request waits on as well.
        .with_sampler(opentelemetry_sdk::trace::Sampler::ParentBased(Box::new(
            opentelemetry_sdk::trace::Sampler::TraceIdRatioBased(cfg.ratio),
        )))
        .with_resource(
            opentelemetry_sdk::Resource::builder()
                .with_service_name(cfg.service_name.clone())
                .build(),
        )
        .build())
}

/// Continues a caller's trace into the wait `/synced` performs.
///
/// It lives here because this is the only crate that may name an OpenTelemetry
/// type: the engine hands over a `tracing` span and the header it was given,
/// and knows nothing about what happens to either. Process-wide, like the
/// subscriber it belongs to, and set only when traces are on — so the endpoint
/// links nothing while nothing is exporting.
static TRACE_LINK: std::sync::OnceLock<pg2osync_engine::api::TraceLink> =
    std::sync::OnceLock::new();

pub(crate) fn trace_link() -> Option<pg2osync_engine::api::TraceLink> {
    TRACE_LINK.get().cloned()
}

fn install_trace_link() {
    let _ = TRACE_LINK.set(std::sync::Arc::new(|span, traceparent| {
        use opentelemetry::propagation::TextMapPropagator;
        use tracing_opentelemetry::OpenTelemetrySpanExt;

        let carrier =
            std::collections::HashMap::from([("traceparent".to_string(), traceparent.to_string())]);
        let parent =
            opentelemetry_sdk::propagation::TraceContextPropagator::new().extract(&carrier);
        // A header that is not a W3C context extracts to an empty context,
        // which `set_parent` treats as no parent at all: a malformed header
        // costs the caller the link, never the request.
        let _ = span.set_parent(parent);
    }));
}

#[tokio::main]
async fn main() -> Result<()> {
    // Arguments first: `--help` and a mistyped subcommand exit here, and
    // neither should have built an exporter on the way.
    let command = Cli::parse().command;
    let format = LogFormat::from_env()?;
    // Read before anything is built, so a typo in either variable is a refusal
    // at startup rather than a pipeline that runs half-configured.
    let (provider, layer) = match TraceExport::from_env()? {
        Some(cfg) => {
            let provider = build_tracer_provider(&cfg)?;
            let layer = tracing_opentelemetry::layer().with_tracer(
                opentelemetry::trace::TracerProvider::tracer(&provider, "pg2osync"),
            );
            (Some(provider), Some(layer))
        }
        None => (None, None),
    };
    init_logging(format, layer);
    if provider.is_some() {
        install_trace_link();
    }

    let result = run_command(command).await;

    // The spans of the last batch are still in the batch processor's queue when
    // the pipeline returns from its drain; without this they are lost exactly
    // when a trace is most wanted, on the shutdown that ended the run.
    if let Some(provider) = provider
        && let Err(e) = provider.shutdown()
    {
        tracing::warn!(target: "pg2osync::traces", "the last spans were not flushed: {e}");
    }
    result
}

async fn run_command(command: Command) -> Result<()> {
    match command {
        Command::Run { config, source } => {
            pipeline(chosen_sources(&config, source.as_deref())?, run::Mode::Run).await
        }
        Command::Bootstrap { config, source } => {
            pipeline(
                chosen_sources(&config, source.as_deref())?,
                run::Mode::Bootstrap,
            )
            .await
        }
        Command::Validate { config, source } => {
            validate(chosen_sources(&config, source.as_deref())?).await
        }
        Command::Status {
            config,
            source,
            caught_up,
            timeout,
            max_retained_mb,
        } => {
            status(
                chosen_sources(&config, source.as_deref())?,
                caught_up,
                timeout,
                max_retained_mb,
            )
            .await
        }
        Command::Reconcile {
            config,
            source,
            delete,
        } => reconcile_cmd(one_source(&config, source.as_deref(), "reconcile")?, delete).await,
        Command::SwitchAlias {
            config,
            source,
            alias,
        } => {
            switch_alias(
                one_source(&config, source.as_deref(), "switch-alias")?,
                &alias,
            )
            .await
        }
        Command::Init {
            config,
            tables,
            source,
            target,
            flavor,
            force,
        } => init(&config, &tables, source, &target, &flavor, force).await,
        Command::SetupSql { config, source } => {
            setup_sql(one_source(&config, source.as_deref(), "setup-sql")?)
        }
        Command::Resnapshot {
            config,
            source,
            table,
            filter,
        } => {
            resnapshot_cmd(
                one_source(&config, source.as_deref(), "resnapshot")?,
                &table,
                filter,
            )
            .await
        }
        Command::Reindex {
            config,
            source,
            table,
            alias,
            drop_old,
        } => {
            reindex_cmd(
                one_source(&config, source.as_deref(), "reindex")?,
                &table,
                &alias,
                drop_old,
            )
            .await
        }
        Command::Rejects {
            config,
            source,
            replay,
            limit,
        } => {
            rejects_cmd(
                one_source(&config, source.as_deref(), "rejects")?,
                replay,
                limit,
            )
            .await
        }
        Command::DropSlot {
            config,
            source,
            publication,
        } => {
            drop_slot(
                one_source(&config, source.as_deref(), "drop-slot")?,
                publication,
            )
            .await
        }
    }
}

/// Every source the command was pointed at, narrowed to one when `--source`
/// names one.
fn chosen_sources(config: &ConfigArgs, source: Option<&str>) -> Result<workspace::Workspace> {
    let ws = workspace::Workspace::load(&config.config, config.config_dir.as_deref())?;
    match source {
        Some(name) => ws.only(name),
        None => Ok(ws),
    }
}

/// The single source a maintenance command acts on.
///
/// These commands rebuild an index, drop a slot, re-read a table: each one
/// changes what one source owns, and a directory does not say which. Rather
/// than picking the first file or running over all of them, the name is asked
/// for — the destructive ones are the last place to guess.
fn one_source(
    config: &ConfigArgs,
    source: Option<&str>,
    command: &str,
) -> Result<config::AppConfig> {
    let ws = chosen_sources(config, source)?;
    if ws.sources.len() > 1 {
        let names: Vec<&str> = ws.sources.iter().map(|s| s.name.as_str()).collect();
        bail!(
            "{command} acts on one source, and this directory has {}: {}. Name one \
             with --source",
            names.len(),
            names.join(", ")
        );
    }
    // A workspace that loaded holds at least one source: an empty directory is
    // refused by the loader, and `only` refuses a name that matches nothing.
    Ok(ws
        .sources
        .into_iter()
        .next()
        .expect("a loaded workspace has a source")
        .cfg)
}

/// Hand the workspace to the supervisor. One source or thirty is the same
/// call: the shutdown signal is the process's, and so is the exit code.
async fn pipeline(ws: workspace::Workspace, mode: run::Mode) -> Result<()> {
    supervisor::run_all(ws, shutdown_signal(), mode).await
}

/// Write a starter config, and check what it names against the real source.
///
/// The friction this removes was measured rather than guessed: every other
/// subcommand needs a `-c FILE`, nothing created one, so the first step was
/// reading a 500-line reference — and the first hand-written attempt failed on
/// an unqualified table name. So this qualifies names from the catalogue, says
/// which tables are eligible when none are given, and prints the two commands
/// that come next.
async fn init(
    path: &Path,
    tables: &[String],
    source: Option<String>,
    target: &str,
    flavor: &str,
    force: bool,
) -> Result<()> {
    if path.exists() && !force {
        bail!(
            "{} already exists; pass --force to overwrite it",
            path.display()
        );
    }
    let mysql = flavor == "mysql";
    if !mysql && flavor != "postgres" && flavor != "postgresql" {
        bail!("--flavor must be \"postgres\" or \"mysql\", not {flavor:?}");
    }
    let source_url = source.or_else(|| std::env::var("PG2OSYNC_SOURCE_URL").ok());

    // Everything below is advice, not a requirement: a config can be written
    // without ever reaching the database, which is what makes this work before
    // the credentials exist.
    let mut resolved: Vec<SourceTable> = Vec::new();
    let mut notes: Vec<String> = Vec::new();
    match &source_url {
        Some(url) => match eligible_tables(url, mysql).await {
            Ok(found) => {
                let names: Vec<String> = found.iter().map(|t| t.qualified.clone()).collect();
                if found.is_empty() {
                    notes.push("no tables were found in the source".into());
                }
                for wanted in tables {
                    match qualify(wanted, &names) {
                        Some(full) => resolved.push(SourceTable {
                            keyed: found.iter().any(|t| t.qualified == full && t.keyed),
                            qualified: full,
                        }),
                        None => bail!(
                            "table {wanted:?} does not exist in the source. Tables it can \
                             sync: {}",
                            preview(&names)
                        ),
                    }
                }
                if tables.is_empty() && !found.is_empty() {
                    notes.push(format!(
                        "tables available to sync: {}. Add one with --table, or edit the \
                         [sync.*] section below",
                        preview(&names)
                    ));
                }
            }
            Err(e) => notes.push(format!(
                "could not read the source catalogue ({e}); the table names below are \
                 unchecked"
            )),
        },
        None => notes.push(
            "no source URL given and PG2OSYNC_SOURCE_URL is unset, so nothing was checked \
             against the database"
                .into(),
        ),
    }
    if resolved.is_empty() {
        // A placeholder rather than an empty file: a config with no table is
        // rejected at load time, and an example is what makes the shape obvious.
        resolved.push(SourceTable {
            qualified: if mysql {
                "appdb.users".into()
            } else {
                "public.users".into()
            },
            keyed: true,
        });
    }

    std::fs::write(path, starter_config(&resolved, target, mysql))
        .with_context(|| format!("cannot write {}", path.display()))?;

    println!("✓ wrote {}", path.display());
    for note in &notes {
        println!("  note: {note}");
    }
    println!();
    println!("Next:");
    if source_url.is_none() {
        println!("  export PG2OSYNC_SOURCE_URL=\"postgres://user:pass@host:5432/db\"");
    }
    // `-c` only when it is not the default, so the printed commands are the
    // shortest ones that actually work.
    let flag = if path == Path::new("pg2osync.toml") {
        String::new()
    } else {
        format!(" -c {}", path.display())
    };
    println!("  pg2osync validate{flag}     # checks both ends and the server's settings");
    println!("  pg2osync run{flag}          # initial load, then streaming");
    Ok(())
}

/// A table `init` found in the source.
struct SourceTable {
    qualified: String,
    /// Whether it has a primary key. Without one the starter config declares
    /// it `append_only`, which is the only way such a table syncs.
    keyed: bool,
}

/// Every base table in the source, and whether it has a primary key.
async fn eligible_tables(source_url: &str, mysql: bool) -> Result<Vec<SourceTable>> {
    if mysql {
        let url = url::Url::parse(source_url).context("source url is not a valid URL")?;
        let tls = pg2osync_source::tls::TlsSettings::resolve(
            source_url,
            pg2osync_source::tls::ConfiguredTls::default(),
        )?;
        let mut conn = pg2osync_source_mysql::connection::MySqlConnection::connect(
            &pg2osync_source_mysql::connection::MySqlConfig {
                host: url.host_str().unwrap_or("localhost").into(),
                port: url.port().unwrap_or(3306),
                user: url.username().into(),
                password: url.password().unwrap_or_default().into(),
                database: None,
                // Only used by a replication stream, and this opens none.
                server_id: 0,
                tls,
            },
        )
        .await
        .context("cannot connect to the source")?;
        let rows = conn
            .query_text_rows(
                "SELECT t.table_schema, t.table_name, c.constraint_name IS NOT NULL \
                 FROM information_schema.tables t \
                 LEFT JOIN information_schema.table_constraints c \
                   ON c.table_schema = t.table_schema AND c.table_name = t.table_name \
                  AND c.constraint_type = 'PRIMARY KEY' \
                 WHERE t.table_type = 'BASE TABLE' \
                   AND t.table_schema NOT IN ('mysql','information_schema','performance_schema','sys') \
                 ORDER BY 1, 2",
            )
            .await?;
        return Ok(rows
            .iter()
            .filter_map(
                |r| match (r.first().cloned().flatten(), r.get(1).cloned().flatten()) {
                    (Some(schema), Some(table)) => Some(SourceTable {
                        qualified: format!("{schema}.{table}"),
                        keyed: r.get(2).cloned().flatten().as_deref() == Some("1"),
                    }),
                    _ => None,
                },
            )
            .collect());
    }
    // Resolved from the URL alone: there is no config yet, which is the point.
    let tls = pg2osync_source::tls::TlsSettings::resolve(
        source_url,
        pg2osync_source::tls::ConfiguredTls::default(),
    )?;
    let client = pg2osync_source::tls::connect(&tls, source_url)
        .await
        .context("cannot connect to the source")?;
    let rows = client
        .query(
            "SELECT n.nspname, c.relname, \
                    EXISTS (SELECT 1 FROM pg_index i WHERE i.indrelid = c.oid AND i.indisprimary) \
             FROM pg_class c \
             JOIN pg_namespace n ON n.oid = c.relnamespace \
             WHERE c.relkind = 'r' AND n.nspname NOT IN ('pg_catalog', 'information_schema') \
             ORDER BY 1, 2",
            &[],
        )
        .await
        .context("cannot list tables")?;
    Ok(rows
        .iter()
        .map(|r| SourceTable {
            qualified: format!("{}.{}", r.get::<_, String>(0), r.get::<_, String>(1)),
            keyed: r.get(2),
        })
        .collect())
}

/// Turn `users` into `public.users`, using what the source actually has.
///
/// An unqualified name is the mistake a first config makes, and it surfaces two
/// commands later as a validation error. Resolving it here is the whole point.
fn qualify(wanted: &str, found: &[String]) -> Option<String> {
    if found.iter().any(|f| f == wanted) {
        return Some(wanted.to_string());
    }
    if wanted.contains('.') {
        return None;
    }
    let suffix = format!(".{wanted}");
    let mut matches = found.iter().filter(|f| f.ends_with(&suffix));
    let first = matches.next()?;
    // An ambiguous bare name is not resolved silently: two schemas holding the
    // same table name is exactly when guessing writes the wrong config.
    matches.next().is_none().then(|| first.clone())
}

fn preview(found: &[String]) -> String {
    let shown: Vec<&str> = found.iter().take(12).map(String::as_str).collect();
    if found.len() > shown.len() {
        format!(
            "{} … and {} more",
            shown.join(", "),
            found.len() - shown.len()
        )
    } else {
        shown.join(", ")
    }
}

/// The smallest config that runs, with the two decisions an operator has to
/// make left visible rather than buried.
fn starter_config(tables: &[SourceTable], target: &str, mysql: bool) -> String {
    let mut out = String::new();
    out.push_str("# Written by `pg2osync init`. Every option is documented in\n");
    out.push_str("# docs/configuration.md; what is here is what a run needs.\n\n");
    out.push_str("[source]\n");
    if mysql {
        out.push_str("flavor = \"mysql\"\n");
    }
    out.push_str("# The URL is read from the environment so it never lands in version control.\n");
    out.push_str("url_env = \"PG2OSYNC_SOURCE_URL\"\n");
    if mysql {
        out.push_str("# Must be unique among the server's replicas.\n");
        out.push_str("server_id = 424242\n");
    }
    out.push_str("\n[target]\n");
    out.push_str(&format!("url = \"{target}\"\n"));
    out.push_str("\n[metrics]\n");
    out.push_str("bind = \"127.0.0.1:9100\"\n");
    for table in tables {
        let index = table
            .qualified
            .split_once('.')
            .map(|(_, t)| t.to_string())
            .unwrap_or_else(|| table.qualified.clone());
        out.push_str(&format!("\n[sync.{index}]\n"));
        out.push_str(&format!("table = \"{}\"\n", table.qualified));
        out.push_str(&format!("index = \"{index}\"\n"));
        if !table.keyed {
            out.push_str(
                "# No primary key: rows are indexed as they arrive, under a hash of their \
                 content; an UPDATE or DELETE halts the pipeline.\n",
            );
            out.push_str("append_only = true\n");
        }
    }
    out
}

/// Print the source-side setup script for this config.
///
/// Offline on purpose: the point is to hand something to whoever holds the
/// privileges, which is usually not whoever is running this.
fn setup_sql(cfg: config::AppConfig) -> Result<()> {
    let secrets = cfg.resolve_secrets()?;
    let url = url::Url::parse(&secrets.source_url).context("source url is not a valid URL")?;
    let user = match url.username() {
        "" => "pg2osync",
        name => name,
    };

    if cfg.source.flavor == "mysql" {
        let mut databases: Vec<String> = cfg
            .sync
            .values()
            .filter_map(|t| t.table.split_once('.').map(|(db, _)| db.to_string()))
            .collect();
        databases.sort();
        databases.dedup();
        print!(
            "{}",
            pg2osync_source_mysql::catalog::setup_script(user, &databases)
        );
        return Ok(());
    }

    let mut tables: Vec<String> = cfg.sync.values().map(|t| t.table.clone()).collect();
    // child tables are read by the initial load too, and a junction is read by
    // every aggregation over the collection it joins, so both need the grant
    for table in cfg.sync.values() {
        for name in table
            .children
            .iter()
            .flat_map(|child| [Some(&child.table), child.through.as_ref()])
            .flatten()
            // an aggregated table is read by the load and by every re-count
            .chain(table.aggregates.iter().map(|agg| &agg.table))
        {
            if !tables.contains(name) {
                tables.push(name.clone());
            }
        }
    }
    print!(
        "{}",
        pg2osync_source::catalog::setup_script(
            user,
            &tables,
            &cfg.source.publication,
            &cfg.source.slot_name,
        )
    );
    Ok(())
}

/// Compare every configured index against its table.
///
/// Run it when the pipeline is caught up: a document whose row was inserted
/// seconds ago and has not been loaded yet looks exactly like an orphan.
async fn reconcile_cmd(cfg: config::AppConfig, delete: bool) -> Result<()> {
    if cfg.source.flavor == "mysql" {
        bail!("reconcile is PostgreSQL-only for now");
    }
    let secrets = cfg.resolve_secrets()?;
    let sink = run::build_sink(&cfg, secrets.target_password)?;
    let client = connect_pg(&cfg, &secrets.source_url).await?;

    let shared = cfg.shared_indexes();
    let mut total_orphans = 0usize;
    for (key, table) in &cfg.sync {
        // reconcile pages an index by one key column, and a fanned table has
        // many documents per row: the pagination would visit the same key
        // over and over. Refuse until it can page by _id instead (#62).
        if table.fan_out.is_some() {
            bail!(
                "[sync.{key}] fan_out is not supported by reconcile: it pages {} by \
                 its key column, and one row now holds many documents under many ids",
                table.index_name(key)
            );
        }
        // reconcile pages one index by one table's key column: every document
        // that belongs to a *different* table in the same index has no row
        // here and looks exactly like an orphan, which `--delete` would then
        // remove. A join pair is scoped by its relation, so it can be paged.
        let index = table.index_name(key);
        if shared.contains(&index) && !cfg.is_join_index(&index) {
            bail!(
                "[sync.{key}] index {index} is fed by more than one table: reconcile pages it \
                 by {}'s key column and cannot tell one table's documents from another's, so \
                 the other tables' documents would all be reported as orphans",
                table.table
            );
        }
        // and there is no key column to page by on a table declared keyless
        if table.append_only {
            bail!(
                "[sync.{key}] {} is append-only: reconcile pages the index by a key column \
                 this table does not have",
                table.table
            );
        }
        // and there is no single index to page when each row chose its own
        if table.is_templated() {
            bail!(
                "[sync.{key}] index {index:?} is chosen per row: reconcile pages one index by \
                 its key column, and this table's documents are spread across every index the \
                 template renders"
            );
        }
        let spec = reconcile::Table {
            qualified: table.table.clone(),
            index: table.index_name(key),
            key_column: table.primary_key.clone().unwrap_or_else(|| "id".into()),
            scope: table
                .join
                .as_ref()
                .map(|join| (join.field.clone(), join.name.clone())),
            soft_delete: table.soft_delete.clone(),
            filter: table
                .filter
                .as_deref()
                .map(pg2osync_core::filter::Filter::parse)
                .transpose()
                .map_err(|e| anyhow::anyhow!("where predicate of {}: {e}", table.table))?,
        };
        let report = reconcile::table(&client, &sink, &spec, delete).await?;
        total_orphans += report.orphaned.len();
        let verb = if delete { "removed" } else { "found" };
        println!(
            "{}: {} document(s) scanned, {} {verb} with no row in {}",
            spec.index,
            report.scanned,
            report.orphaned.len(),
            spec.qualified
        );
        // enough to investigate with, without pasting a whole index into a
        // terminal when something has gone badly wrong
        for (id, _) in report.orphaned.iter().take(10) {
            println!("  {id}");
        }
        if report.orphaned.len() > 10 {
            println!("  … and {} more", report.orphaned.len() - 10);
        }
    }
    if total_orphans > 0 && !delete {
        println!("\nRe-run with --delete to remove them.");
    }
    Ok(())
}

/// Move an alias onto the index this config writes.
///
/// The last step of a reindex, and the one that has to be atomic: a reader
/// resolving the alias between a remove and an add gets an error, which is
/// exactly what the exercise was avoiding.
async fn switch_alias(cfg: config::AppConfig, alias: &str) -> Result<()> {
    let indices = run::index_names(&cfg)?;
    // an alias points at one index, and a template's glob is not one
    if let Some((key, _)) = cfg.sync.iter().find(|(_, tbl)| tbl.is_templated()) {
        bail!(
            "switch-alias needs a config that writes to one fixed index; [sync.{key}] chooses \
             its index per row"
        );
    }
    let [index] = indices.as_slice() else {
        bail!(
            "switch-alias needs a config that writes to exactly one index; this one writes to {}",
            indices.len()
        );
    };
    let secrets = cfg.resolve_secrets()?;
    let sink = run::build_sink(&cfg, secrets.target_password)?;
    sink.switch_alias(alias, index).await?;
    println!("alias {alias} now points at {index}");
    Ok(())
}

/// A binlog checkpoint token as the pair it orders by.
///
/// The file name carries a zero-padded sequence number, so files compare as
/// text; the offset within one does not, and must be a number.
fn binlog_coordinate(token: &str) -> Option<(String, u32)> {
    let (file, pos) = token.rsplit_once(':')?;
    Some((file.to_string(), pos.parse().ok()?))
}

/// Exit 0 once the checkpoint has reached the source's current position.
///
/// The reindex recipe says "wait for lag to reach zero", which until now meant
/// watching a metric by eye. This is the same question with an exit code.
async fn wait_until_caught_up(cfg: config::AppConfig, timeout_secs: u64) -> Result<()> {
    let secrets = cfg.resolve_secrets()?;
    let sink = run::build_sink(&cfg, secrets.target_password)?;
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(timeout_secs);

    loop {
        let checkpoint = sink.read_checkpoint(&cfg.stream_id()).await?;
        let position = checkpoint.map(|c| c.position);
        let (current, reached) = if cfg.source.flavor == "mysql" {
            let source = mysql_source(&cfg, &secrets.source_url)?;
            let mut admin = source.admin_connection().await?;
            let (file, pos) = pg2osync_source_mysql::catalog::master_position(&mut admin).await?;
            let current = format!("{file}:{pos}");
            let reached = position
                .as_deref()
                .and_then(binlog_coordinate)
                .is_some_and(|have| have >= (file, pos));
            (current, reached)
        } else {
            let client = connect_pg(&cfg, &secrets.source_url).await?;
            let current: String = client
                .query_one("SELECT pg_current_wal_lsn()::text", &[])
                .await?
                .get(0);
            // parsed, not compared as text: LSN halves are unpadded hex, so
            // "0/9ABC" sorts above "0/10000" as a string and below it in fact
            let reached = match (
                position.as_deref().and_then(|p| p.parse::<Lsn>().ok()),
                current.parse::<Lsn>(),
            ) {
                (Some(have), Ok(want)) => have >= want,
                _ => false,
            };
            (current, reached)
        };
        let shown = position.clone().unwrap_or_else(|| "none".into());
        if reached {
            println!("caught up: checkpoint {shown} has reached {current}");
            return Ok(());
        }
        if std::time::Instant::now() >= deadline {
            bail!("not caught up: checkpoint {shown}, source at {current}");
        }
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    }
}

async fn resnapshot_cmd(cfg: config::AppConfig, table: &str, filter: Option<String>) -> Result<()> {
    let secrets = cfg.resolve_secrets()?;
    for warning in &secrets.warnings {
        tracing::warn!(target: "pg2osync::config", "{warning}");
    }
    resnapshot::run_for(
        &cfg,
        &secrets.source_url,
        &secrets.admin_url,
        secrets.target_password,
        table,
        filter,
    )
    .await
}

/// Rebuild one table's index and hand the alias to the copy.
async fn reindex_cmd(
    cfg: config::AppConfig,
    table: &str,
    alias: &str,
    drop_old: bool,
) -> Result<()> {
    let secrets = cfg.resolve_secrets()?;
    for warning in &secrets.warnings {
        tracing::warn!(target: "pg2osync::config", "{warning}");
    }
    reindex::run_for(
        &cfg,
        &secrets.source_url,
        &secrets.admin_url,
        secrets.target_password,
        table,
        alias,
        drop_old,
    )
    .await
}

/// Show what the target refused, and optionally offer it again.
///
/// A replay reuses the ordinary write path with the document's original
/// position as its version, so anything the source has since superseded is
/// refused by the version rule and the newer value stands — which is what
/// should happen, and costs no special handling here.
async fn rejects_cmd(cfg: config::AppConfig, replay: bool, limit: usize) -> Result<()> {
    let secrets = cfg.resolve_secrets()?;
    let sink = run::build_sink(&cfg, secrets.target_password)?;
    let (stored, total) = sink.list_rejects(limit).await?;
    if total == 0 {
        println!("no quarantined documents");
        return Ok(());
    }
    println!(
        "{total} quarantined document(s){}",
        if stored.len() as u64 == total {
            String::new()
        } else {
            format!(", showing {}", stored.len())
        }
    );
    let mut replayed = 0usize;
    let mut still_refused = 0usize;
    for reject in &stored {
        let r = &reject.rejection;
        println!("  {}/{} at {}: {}", r.index, r.doc_id, r.lsn, r.reason);
        if !replay {
            continue;
        }
        let ack = sink
            .write(vec![pg2osync_core::sink::LsnOp {
                lsn: r.lsn,
                op: r.op.clone(),
            }])
            .await?;
        if ack.rejected.is_empty() {
            // cleared only once the target has taken it: the record is the only
            // copy of this document, so removing it first could lose it
            sink.clear_reject(&reject.id).await?;
            replayed += 1;
            // "no longer refused" rather than "written": a document the source
            // has changed since is declined by the version rule instead, and
            // either way the record has done its job
            println!("    ✓ no longer refused, cleared from the store");
        } else {
            still_refused += 1;
            println!("    ✗ still refused: {}", ack.rejected[0].reason);
        }
    }
    if replay {
        println!("\nreplayed {replayed}, still refused {still_refused}");
        if still_refused > 0 {
            bail!("{still_refused} document(s) are still refused");
        }
    }
    Ok(())
}

/// Validate every source, and report all of them: an operator fixing a
/// directory of configs wants the whole list, not the first line of it.
async fn validate(ws: workspace::Workspace) -> Result<()> {
    let total = ws.sources.len();
    let mut failed: Vec<String> = Vec::new();
    for source in &ws.sources {
        if total > 1 {
            println!("\n── {} ({})", source.name, source.path.display());
        }
        if let Err(e) = validate_source(source.cfg.clone()).await {
            // one config is what `--config` has always given, and its refusal
            // is that refusal: nothing to gather across, nothing to head
            if total == 1 {
                return Err(e);
            }
            println!("✗ {e:#}");
            failed.push(source.name.clone());
        }
    }
    every_source(&failed, total, "failed")?;
    println!("\nall checks passed");
    Ok(())
}

/// The verdict on a set of sources. A refusal anywhere is the command's
/// refusal: the report is read whole, and the exit status has to say that
/// there is a refusal in it.
fn every_source(failed: &[String], total: usize, verb: &str) -> Result<()> {
    if failed.is_empty() {
        return Ok(());
    }
    bail!(
        "{} of {total} source(s) {verb}: {}",
        failed.len(),
        failed.join(", ")
    )
}

async fn validate_source(cfg: config::AppConfig) -> Result<()> {
    let secrets = cfg.resolve_secrets()?;
    for warning in &secrets.warnings {
        tracing::warn!(target: "pg2osync::config", "{warning}");
    }
    println!(
        "✓ config structure valid ({} table mappings)",
        cfg.sync.len()
    );
    // The name only, never the key: this output is what people paste into an
    // issue when a pipeline will not start.
    for name in &secrets.pseudonym_key_vars {
        println!("✓ pseudonym key present (64 bytes) from {name}");
    }

    if cfg.source.flavor == "mysql" {
        validate_mysql(&cfg, &secrets.source_url).await?;
    } else {
        validate_postgres(&cfg, &secrets.source_url).await?;
    }

    let sink = run::build_sink(&cfg, secrets.target_password)?;
    match sink.health().await? {
        Health::Up => println!("✓ {} reachable at {}", cfg.target.flavor, cfg.target.url),
        Health::Down(reason) => bail!("{} is reachable but unhealthy: {reason}", cfg.target.flavor),
    }
    // Checked here and not at startup: a pipeline that vanishes while the
    // pipeline runs is the target refusing the batch, which quarantine or halt
    // already report; what this catches is the typo before the first batch.
    for (key, name) in cfg
        .sync
        .iter()
        .filter_map(|(key, tbl)| tbl.pipeline.as_deref().map(|name| (key, name)))
    {
        if !sink.has_pipeline(name).await? {
            bail!(
                "ingest pipeline {name:?} of [sync.{key}] does not exist on the target; \
                 create it first (PUT _ingest/pipeline/{name})"
            );
        }
        println!("✓ ingest pipeline {name} exists for [sync.{key}]");
    }
    // Before the first write rather than at it: the target reports a name that
    // is an index and not an alias as a refused document, one batch at a time,
    // and the pipeline halts having already been configured wrong for as long
    // as it took to notice.
    if cfg.target.require_alias {
        for spec in run::index_specs(&cfg)?.iter().filter(|s| !s.pattern) {
            if !sink.is_alias(&spec.name).await? {
                bail!(
                    "[target] require_alias is set, but {} is an index, not an alias; \
                     point the section at the alias that resolves to it, or unset \
                     require_alias",
                    spec.name
                );
            }
        }
        println!("✓ require_alias: every configured index is an alias");
    }
    run::check_rejection_policy(&cfg, sink.as_ref())?;
    for note in run::embedded_children_with_own_section(&cfg) {
        println!("! {note}");
    }
    if cfg.engine.on_permanent_rejection == pg2osync_engine::RejectionPolicy::Quarantine {
        let (_, held) = sink.list_rejects(0).await.unwrap_or_default();
        println!(
            "✓ quarantine on permanent rejection, {held} of {} used",
            cfg.engine.max_rejects
        );
    }

    Ok(())
}

/// Check a table's configuration against the columns it actually has.
///
/// A `columns` list naming something that no longer exists is silently ignored
/// today, so the field simply stops appearing in new documents and the index
/// disagrees with the database with nothing to show for it. Naming it at
/// startup is far cheaper than finding it later.
fn check_configured_columns(
    table: &config::TableSync,
    live: &[String],
    pk_columns: &[String],
    nullable: &[String],
) -> Result<()> {
    let missing = |names: &[String]| -> Vec<String> {
        names
            .iter()
            .filter(|n| !live.iter().any(|c| c.eq_ignore_ascii_case(n)))
            .cloned()
            .collect()
    };

    if let Some(columns) = &table.columns {
        let gone = missing(columns);
        if !gone.is_empty() {
            bail!(
                "table {} has no column(s) {}; the `columns` list would silently drop them",
                table.table,
                gone.join(", ")
            );
        }
    }
    if let Some(pk) = &table.primary_key
        && !missing(std::slice::from_ref(pk)).is_empty()
    {
        bail!(
            "table {} has no column {pk} to use as primary_key",
            table.table
        );
    }
    // an id placeholder naming a column that is gone could never render, and
    // one naming a nullable column is a halt waiting to happen
    if let Some(spec) = &table.id {
        let template = pg2osync_engine::mapping::IdTemplate::parse(spec, pk_columns)
            .map_err(|e| anyhow::anyhow!("id {spec:?} of {}: {e}", table.table))?;
        for col in template.columns() {
            if !live.iter().any(|c| c.eq_ignore_ascii_case(col)) {
                bail!(
                    "table {} has no column {col} to derive its id from",
                    table.table
                );
            }
            if nullable.iter().any(|c| c.eq_ignore_ascii_case(col)) {
                println!(
                    "! id placeholder {{{col}}} on {} is nullable; a NULL in it \
                     halts the pipeline",
                    table.table
                );
            }
        }
    }
    // the same rule for an index placeholder: a NULL index column halts
    // exactly like a NULL id
    if let Some(spec) = &table.index
        && table.is_templated()
    {
        let template = pg2osync_engine::mapping::IdTemplate::parse(spec, pk_columns)
            .map_err(|e| anyhow::anyhow!("index {spec:?} of {}: {e}", table.table))?;
        for col in template.columns() {
            if !live.iter().any(|c| c.eq_ignore_ascii_case(col)) {
                bail!(
                    "table {} has no column {col} to choose its index by",
                    table.table
                );
            }
            if nullable.iter().any(|c| c.eq_ignore_ascii_case(col)) {
                println!(
                    "! index placeholder {{{col}}} on {} is nullable; a NULL in it \
                     halts the pipeline",
                    table.table
                );
            }
        }
    }
    // A predicate naming a column that is gone evaluates to unknown for every
    // row, which empties the index — a data-loss failure, not the silent
    // drift a stale `transform` entry causes. No nullable warning goes with
    // it: NULL is an ordinary value in a predicate, `deleted_at IS NULL`
    // depends on it.
    if let Some(spec) = &table.filter {
        let filter = pg2osync_core::filter::Filter::parse(spec)
            .map_err(|e| anyhow::anyhow!("where {spec:?} of {}: {e}", table.table))?;
        for col in filter.columns() {
            if !live.iter().any(|c| c.eq_ignore_ascii_case(col)) {
                bail!(
                    "table {} has no column {col} for its `where` predicate",
                    table.table
                );
            }
        }
    }
    if let Some(fan) = &table.fan_out
        && !live.iter().any(|c| c.eq_ignore_ascii_case(&fan.field))
    {
        bail!(
            "table {} has no column {} to fan out",
            table.table,
            fan.field
        );
    }
    // the parent column is where a child's routing comes from, so like an id
    // placeholder a missing one could never render and a nullable one is a
    // halt waiting to happen
    if let Some(column) = table
        .join
        .as_ref()
        .and_then(|join| join.parent_column(table.fan_out.as_ref()))
    {
        if !live.iter().any(|c| c.eq_ignore_ascii_case(column)) {
            bail!(
                "table {} has no column {column} to find its join parent by",
                table.table
            );
        }
        if nullable.iter().any(|c| c.eq_ignore_ascii_case(column)) {
            println!(
                "! join parent column {column} on {} is nullable; a NULL in it halts the pipeline",
                table.table
            );
        }
    }
    // routing decides which shard a document lives on, so a missing column
    // could never render one and a nullable one is a halt waiting to happen
    if let Some(column) = &table.routing {
        if !live.iter().any(|c| c.eq_ignore_ascii_case(column)) {
            bail!(
                "table {} has no column {column} to route its documents by",
                table.table
            );
        }
        if nullable.iter().any(|c| c.eq_ignore_ascii_case(column)) {
            println!(
                "! routing column {column} on {} is nullable; a NULL in it halts the pipeline",
                table.table
            );
        }
    }
    // a rename onto a column that still reaches the target would bury that
    // column under the renamed value; the config check could only see this
    // with an explicit `columns` list, the catalogue always can
    let survives = |c: &String| {
        !table.fields.contains_key(c)
            && !table.exclude_columns.contains(c)
            && table.columns.as_ref().is_none_or(|cols| cols.contains(c))
    };
    for (col, target) in &table.fields {
        if let Some(c) = live
            .iter()
            .find(|c| c.eq_ignore_ascii_case(target) && survives(c))
        {
            bail!(
                "table {} has a column {c}; renaming {col} to it would overwrite that column",
                table.table
            );
        }
    }
    // the same for a constant, which is written last; nothing here goes stale,
    // a constant names nothing in the source
    for name in table.constants.keys() {
        if let Some(c) = live
            .iter()
            .find(|c| c.eq_ignore_ascii_case(name) && survives(c))
        {
            bail!(
                "table {} has a column {c}; the constant {name} would overwrite it",
                table.table
            );
        }
    }
    // a flattened child lands where the parent's own columns land, and the
    // config check can only see this where the section lists its `columns`
    for child in &table.children {
        for name in child.lifted_fields() {
            if let Some(c) = live
                .iter()
                .find(|c| c.eq_ignore_ascii_case(&name) && survives(c))
            {
                bail!(
                    "table {} has a column {c}; child {} lifts {name} onto the document and \
                     would overwrite it",
                    table.table,
                    child.table
                );
            }
        }
    }
    // and for the join field, written after the constants
    if let Some(join) = &table.join
        && let Some(c) = live
            .iter()
            .find(|c| c.eq_ignore_ascii_case(&join.field) && survives(c))
    {
        bail!(
            "table {} has a column {c}; the join field {} would overwrite it",
            table.table,
            join.field
        );
    }
    // an exclusion, a transform or a rename for a column that is gone changes
    // nothing, so it is stale configuration rather than a fault
    for (label, names) in [
        ("exclude_columns", table.exclude_columns.clone()),
        (
            "transform",
            table.transform.keys().cloned().collect::<Vec<_>>(),
        ),
        ("fields", table.fields.keys().cloned().collect::<Vec<_>>()),
    ] {
        let gone = missing(&names);
        if !gone.is_empty() {
            println!(
                "! {label} on {} names column(s) that do not exist: {}",
                table.table,
                gone.join(", ")
            );
        }
    }
    Ok(())
}

/// Say what the client sent and what the server made of it.
///
/// `pg_stat_ssl` is the only place the answer is authoritative: the server
/// decides whether to ask for a certificate at all, so a successful connection
/// proves nothing on its own. A failure to read it is not a validation failure.
async fn report_client_certificate(
    client: &tokio_postgres::Client,
    tls: &pg2osync_source::tls::TlsSettings,
) {
    let Some(path) = &tls.client_cert else {
        return;
    };
    println!("✓ client certificate presented ({})", path.display());
    let Ok(row) = client
        .query_opt(
            "SELECT client_dn FROM pg_stat_ssl WHERE pid = pg_backend_pid()",
            &[],
        )
        .await
    else {
        return;
    };
    match row.as_ref().and_then(|r| r.get::<_, Option<String>>(0)) {
        Some(dn) => println!("✓ server accepted the client certificate (DN={dn})"),
        None => println!("! the server did not ask for a client certificate"),
    }
}

/// One column as a join has to compare it: its name, its type, and the family
/// that type belongs to.
#[derive(Clone)]
struct JoinColumn {
    name: String,
    type_name: String,
    category: char,
}

/// What a junction adds to the child check.
///
/// Gathered by whichever source `validate` is talking to, so the check itself
/// stays one function that both of them run.
#[derive(Default)]
struct Junction {
    qualified: String,
    columns: Vec<JoinColumn>,
    /// The parent column the junction's `foreign_key` has to compare against.
    parent_key: Option<JoinColumn>,
    /// The child's primary key, which is what `through_key` points at.
    child_key: Vec<JoinColumn>,
    /// Whether an index leads with `through_key`, which is what a changed child
    /// row is looked back up by.
    leading_index_on_through_key: bool,
}

/// Whether two columns can be joined on at all.
///
/// Compared by family rather than by type, because `int4` against `int8` is
/// fine and `text` against `int` is not: the mismatch does not fail the query
/// on every server, it silently matches nothing, and the only symptom is an
/// array that is always empty.
fn joinable(left: &JoinColumn, right: &JoinColumn) -> bool {
    left.category == right.category
}

/// The junction half of the child check: that the join can be made, and that
/// the lookups it costs are indexed.
fn check_through(child: &config::ChildJoin, junction: &Junction) -> Result<()> {
    let Some(through_key) = &child.through_key else {
        return Ok(());
    };
    let named = |name: &str| junction.columns.iter().find(|c| c.name == name);
    let (Some(foreign_key), Some(through)) = (named(&child.foreign_key), named(through_key)) else {
        bail!(
            "junction {} has no column(s) {}; with `through` set, foreign_key and \
             through_key are columns of the junction, not of {}",
            junction.qualified,
            [child.foreign_key.as_str(), through_key.as_str()]
                .into_iter()
                .filter(|n| named(n).is_none())
                .collect::<Vec<_>>()
                .join(", "),
            child.table
        );
    };
    let [child_key] = junction.child_key.as_slice() else {
        bail!(
            "child {} is reached through {}, so it needs a single-column primary key \
             for {through_key} to point at; it has {}",
            child.table,
            junction.qualified,
            junction.child_key.len()
        );
    };
    if let Some(parent_key) = &junction.parent_key
        && !joinable(foreign_key, parent_key)
    {
        bail!(
            "junction column {}.{} is {} and the parent key it references is {}; \
             the two do not compare, so every embedded array would be empty",
            junction.qualified,
            foreign_key.name,
            foreign_key.type_name,
            parent_key.type_name
        );
    }
    if !joinable(through, child_key) {
        bail!(
            "junction column {}.{} is {} and {}.{} is {}; the two do not compare, \
             so every embedded array would be empty",
            junction.qualified,
            through.name,
            through.type_name,
            child.table,
            child_key.name,
            child_key.type_name
        );
    }
    if !junction.leading_index_on_through_key {
        println!(
            "! no index on {}({}): a changed row of {} is looked back up by it, \
             which without one scans the junction",
            junction.qualified, through.name, child.table
        );
    }
    println!(
        "✓ child {} through {} ({} → parent, {} → {}.{})",
        child.table,
        junction.qualified,
        foreign_key.name,
        through.name,
        child.table,
        child_key.name
    );
    Ok(())
}

/// The same check for one child collection, against the child table's columns.
///
/// A child's projection and renames name columns of the *child* table, which the
/// parent's check never sees; without this a `columns` list that outlived a
/// dropped column would quietly shrink every embedded element.
fn check_child_columns(
    child: &config::ChildJoin,
    live: &[String],
    junction: Option<&Junction>,
) -> Result<()> {
    if let Some(junction) = junction {
        check_through(child, junction)?;
    }
    let missing = |names: &[String]| -> Vec<String> {
        names
            .iter()
            .filter(|n| !live.iter().any(|c| c.eq_ignore_ascii_case(n)))
            .cloned()
            .collect()
    };
    if let Some(columns) = &child.columns {
        let gone = missing(columns);
        if !gone.is_empty() {
            bail!(
                "child {} has no column(s) {}; the `columns` list would silently drop them",
                child.table,
                gone.join(", ")
            );
        }
    }
    // a rename onto a column the element still carries would bury that column
    let survives = |c: &String| {
        !child.fields.contains_key(c)
            && !child.exclude_columns.contains(c)
            && child.columns.as_ref().is_none_or(|cols| cols.contains(c))
    };
    for (col, target) in &child.fields {
        if let Some(c) = live
            .iter()
            .find(|c| c.eq_ignore_ascii_case(target) && survives(c))
        {
            bail!(
                "child {} has a column {c}; renaming {col} to it would overwrite that column",
                child.table
            );
        }
    }
    // as on a parent, naming a column that is gone changes nothing: stale
    // configuration rather than a fault
    for (label, names) in [
        ("exclude_columns", child.exclude_columns.clone()),
        ("fields", child.fields.keys().cloned().collect::<Vec<_>>()),
    ] {
        let gone = missing(&names);
        if !gone.is_empty() {
            println!(
                "! {label} on child {} names column(s) that do not exist: {}",
                child.table,
                gone.join(", ")
            );
        }
    }
    Ok(())
}

/// One table's columns as `check_through` compares them.
async fn pg_columns(client: &tokio_postgres::Client, qualified: &str) -> Result<Vec<JoinColumn>> {
    Ok(client
        .query(
            "SELECT a.attname::text, format_type(a.atttypid, a.atttypmod), \
                    t.typcategory::text \
             FROM pg_attribute a JOIN pg_type t ON t.oid = a.atttypid \
             WHERE a.attrelid = to_regclass($1) AND a.attnum > 0 AND NOT a.attisdropped \
             ORDER BY a.attnum",
            &[&qualified],
        )
        .await?
        .iter()
        .map(|r| {
            let category: String = r.get(2);
            JoinColumn {
                name: r.get(0),
                type_name: r.get(1),
                category: category.chars().next().unwrap_or('?'),
            }
        })
        .collect())
}

/// What PostgreSQL knows about a many-to-many child's junction.
async fn pg_junction(
    client: &tokio_postgres::Client,
    table: &config::TableSync,
    child: &config::ChildJoin,
) -> Result<Option<Junction>> {
    let (Some(qualified), Some(through_key)) = (&child.through, &child.through_key) else {
        return Ok(None);
    };
    let columns = pg_columns(client, qualified).await?;
    if columns.is_empty() {
        bail!("through table {qualified} does not exist");
    }
    let parent_key_name = table.primary_key.clone().unwrap_or_else(|| "id".into());
    let parent_key = pg_columns(client, &table.table)
        .await?
        .into_iter()
        .find(|c| c.name == parent_key_name);
    let (schema, name) = child
        .table
        .split_once('.')
        .context("child table must be schema-qualified")?;
    let pk_columns = pg2osync_source::catalog::table_info(client, schema, name)
        .await?
        .pk_columns;
    let child_columns = pg_columns(client, &child.table).await?;
    let child_key = child_columns
        .into_iter()
        .filter(|c| pk_columns.contains(&c.name))
        .collect();
    // indkey[0] is the leading column: an index the lookup can descend, rather
    // than any index the column happens to appear in
    let leading_index_on_through_key = client
        .query_opt(
            "SELECT 1 FROM pg_index i \
             WHERE i.indrelid = to_regclass($1) \
               AND i.indkey[0] = (SELECT a.attnum FROM pg_attribute a \
                                  WHERE a.attrelid = to_regclass($1) AND a.attname = $2)",
            &[qualified, through_key],
        )
        .await?
        .is_some();
    Ok(Some(Junction {
        qualified: qualified.clone(),
        columns,
        parent_key,
        child_key,
        leading_index_on_through_key,
    }))
}

async fn validate_postgres(cfg: &config::AppConfig, source_url: &str) -> Result<()> {
    let client = connect_pg(cfg, source_url).await?;
    let tls = cfg.tls_settings(source_url)?;
    println!("✓ connected to PostgreSQL (sslmode={})", tls.mode.as_str());
    report_client_certificate(&client, &tls).await;

    if cfg.source.mode == "wal" {
        pg2osync_source::catalog::check_wal_level(&client).await?;
        println!("✓ wal_level = logical");
    }
    let mut tables: Vec<String> = Vec::new();
    for table in cfg.sync.values() {
        let exists = client
            .query_opt(
                "SELECT 1 FROM to_regclass($1) WHERE to_regclass IS NOT NULL",
                &[&table.table],
            )
            .await?;
        if exists.is_none() {
            bail!("table {} does not exist", table.table);
        }
        let live: Vec<String> = client
            .query(
                "SELECT attname::text FROM pg_attribute \
                 WHERE attrelid = to_regclass($1) AND attnum > 0 AND NOT attisdropped \
                 ORDER BY attnum",
                &[&table.table],
            )
            .await?
            .iter()
            .map(|r| r.get(0))
            .collect();
        let (schema, name) = table
            .table
            .split_once('.')
            .context("table must be schema-qualified")?;
        let info = pg2osync_source::catalog::table_info(&client, schema, name).await?;
        // the load would say the same thing later; here it also says what
        // to do about it
        if info.pk_columns.is_empty() && !table.append_only {
            bail!(
                "table {} has no primary key; pg2osync needs one to derive a stable \
                 document id, or `append_only = true` on its [sync] section to index its \
                 rows as they arrive",
                table.table
            );
        }
        let nullable: Vec<String> = client
            .query(
                "SELECT attname::text FROM pg_attribute \
                 WHERE attrelid = to_regclass($1) AND attnum > 0 AND NOT attisdropped \
                 AND NOT attnotnull",
                &[&table.table],
            )
            .await?
            .iter()
            .map(|r| r.get(0))
            .collect();
        check_configured_columns(table, &live, &info.pk_columns, &nullable)?;
        if table.append_only {
            println!(
                "✓ table {} exists, append-only ({} columns)",
                table.table,
                live.len()
            );
        } else {
            println!("✓ table {} exists ({} columns)", table.table, live.len());
        }
        // LIMIT 0 still parses, plans and type-checks the predicate — which
        // the grammar cannot do — and reads nothing
        if let Some(spec) = &table.filter {
            let sql = pg2osync_core::filter::Filter::parse(spec)
                .map_err(|e| anyhow::anyhow!("where {spec:?} of {}: {e}", table.table))?
                .to_sql(&backfill::pg_dialect_bare());
            let probe = format!("SELECT 1 FROM {} WHERE ({sql}) LIMIT 0", table.table);
            client.query(&probe, &[]).await.with_context(|| {
                format!(
                    "the `where` predicate of {} is not valid SQL against that table",
                    table.table
                )
            })?;
            println!(
                "✓ where predicate of {} runs against the table",
                table.table
            );
        }
        tables.push(table.table.clone());
        for child in &table.children {
            let child_live: Vec<String> = client
                .query(
                    "SELECT attname::text FROM pg_attribute \
                     WHERE attrelid = to_regclass($1) AND attnum > 0 AND NOT attisdropped \
                     ORDER BY attnum",
                    &[&child.table],
                )
                .await?
                .iter()
                .map(|r| r.get(0))
                .collect();
            if child_live.is_empty() {
                bail!("child table {} does not exist", child.table);
            }
            let junction = pg_junction(&client, table, child).await?;
            check_child_columns(child, &child_live, junction.as_ref())?;
            tables.push(child.table.clone());
            if let Some(through) = &child.through {
                tables.push(through.clone());
            }
        }
        for agg in &table.aggregates {
            // The grouped read itself, with LIMIT 0: it parses, plans and
            // type-checks the foreign key and the `where` against the real
            // table, which the grammar cannot do — and reads nothing.
            let spec = pg2osync_core::aggregate::AggregateSpec::new(
                &agg.table,
                &agg.field,
                &agg.foreign_key,
                &table.primary_key.clone().unwrap_or_else(|| "id".into()),
            )?;
            let mut spec = spec;
            if let Some(predicate) = &agg.filter {
                spec.filter = Some(
                    pg2osync_core::filter::Filter::parse(predicate).map_err(|e| {
                        anyhow::anyhow!("where {predicate:?} of {}: {e}", agg.table)
                    })?,
                );
            }
            let probe = format!(
                "SELECT 1 FROM ({}) c LIMIT 0",
                pg2osync_source::aggregate::count_subquery(&spec, None)
            );
            client.query(&probe, &[]).await.with_context(|| {
                format!(
                    "the aggregate {} of {} does not run against {}: check foreign_key and \
                     where",
                    agg.field, table.table, agg.table
                )
            })?;
            println!(
                "✓ aggregate {} counts {} by {}",
                agg.field, agg.table, agg.foreign_key
            );
            tables.push(agg.table.clone());
        }
    }

    if cfg.source.mode == "wal" {
        report_privileges(&client, cfg, &tables).await?;
    }
    Ok(())
}

/// Report whether this role can actually run the pipeline.
///
/// Reading tables is not enough: PostgreSQL requires table *ownership* to
/// publish a table and the `REPLICATION` attribute to create a slot. Finding
/// that out during `run` wastes a maintenance window, so it is checked here.
async fn report_privileges(
    client: &tokio_postgres::Client,
    cfg: &config::AppConfig,
    tables: &[String],
) -> Result<()> {
    let pre = pg2osync_source::catalog::preflight(
        client,
        &cfg.source.publication,
        &cfg.source.slot_name,
        tables,
    )
    .await?;

    if !pre.tables_not_readable.is_empty() {
        bail!(
            "no SELECT privilege on {:?}; the initial load cannot read them",
            pre.tables_not_readable
        );
    }
    if pre.publication_exists {
        println!("✓ publication {} exists", cfg.source.publication);
    }
    if pre.slot_exists {
        println!("✓ replication slot {} exists", cfg.source.slot_name);
    }
    if pre.can_bootstrap() {
        if !pre.publication_exists || !pre.slot_exists {
            println!("✓ privileges sufficient to create the missing objects");
        }
        return Ok(());
    }

    println!("\n✗ this role cannot create what is missing:");
    if !pre.publication_exists {
        if !pre.can_create_in_database {
            println!("  - CREATE on the database is required to create a publication");
        }
        if !pre.tables_not_owned.is_empty() {
            println!(
                "  - publishing a table requires owning it; not owned: {:?}",
                pre.tables_not_owned
            );
        }
    }
    if !pre.slot_exists && !pre.can_replicate {
        println!("  - the REPLICATION attribute is required to create a slot");
        if let Some(service) = pre.service {
            println!("    {}", service.replication_role_remedy());
        }
    }
    println!("\nAsk a privileged role to run this once, then re-run validate:\n");
    for statement in pre.setup_sql(&cfg.source.publication, &cfg.source.slot_name, tables) {
        println!("  {statement}");
    }
    println!();
    bail!("insufficient privileges to bootstrap; see the statements above")
}

/// A MySQL column as `check_through` compares it.
///
/// The family comes from the shape the rest of the pipeline already decided,
/// rather than from a second reading of `information_schema`: a column joins
/// against another one exactly when the two mean the same kind of value.
fn mysql_join_column(column: &pg2osync_source_mysql::catalog::Column) -> JoinColumn {
    use pg2osync_source_mysql::typemap::ValueShape;
    let (type_name, category) = match &column.shape {
        ValueShape::Int => ("integer", 'N'),
        ValueShape::Float => ("floating point", 'N'),
        ValueShape::Decimal => ("decimal", 'N'),
        ValueShape::Bits => ("bit", 'N'),
        ValueShape::Bytes => ("binary", 'B'),
        ValueShape::Text => ("text", 'S'),
        ValueShape::Json => ("json", 'S'),
        ValueShape::Enum(_) => ("enum", 'S'),
        ValueShape::Set(_) => ("set", 'S'),
    };
    JoinColumn {
        name: column.name.clone(),
        type_name: type_name.into(),
        category,
    }
}

/// What MySQL knows about a many-to-many child's junction.
async fn mysql_junction(
    admin: &mut pg2osync_source_mysql::connection::MySqlConnection,
    table: &config::TableSync,
    child: &config::ChildJoin,
    child_live: &pg2osync_source_mysql::catalog::TableSchema,
) -> Result<Option<Junction>> {
    let (Some(qualified), Some(through_key)) = (&child.through, &child.through_key) else {
        return Ok(None);
    };
    let (schema, name) = qualified
        .split_once('.')
        .context("through table must be written as database.table for MySQL")?;
    // keyless_ok: a junction is keyed by the pair, and nothing here addresses
    // one of its rows
    let junction = pg2osync_source_mysql::catalog::table_schema(admin, schema, name, true).await?;
    let (pschema, pname) = table
        .table
        .split_once('.')
        .context("table must be written as database.table for MySQL")?;
    let parent = pg2osync_source_mysql::catalog::table_schema(admin, pschema, pname, true).await?;
    let parent_key_name = table.primary_key.clone().unwrap_or_else(|| "id".into());
    let leading = admin
        .query_text_rows(&format!(
            "SELECT 1 FROM information_schema.statistics WHERE table_schema = {} \
             AND table_name = {} AND seq_in_index = 1 AND column_name = {}",
            pg2osync_source_mysql::catalog::quote_str(schema),
            pg2osync_source_mysql::catalog::quote_str(name),
            pg2osync_source_mysql::catalog::quote_str(through_key),
        ))
        .await?;
    Ok(Some(Junction {
        qualified: qualified.clone(),
        columns: junction.columns.iter().map(mysql_join_column).collect(),
        parent_key: parent
            .columns
            .iter()
            .find(|c| c.name == parent_key_name)
            .map(mysql_join_column),
        child_key: child_live
            .columns
            .iter()
            .filter(|c| child_live.pk_columns.contains(&c.name))
            .map(mysql_join_column)
            .collect(),
        leading_index_on_through_key: !leading.is_empty(),
    }))
}

async fn validate_mysql(cfg: &config::AppConfig, source_url: &str) -> Result<()> {
    let source = mysql_source(cfg, source_url)?;
    let mut admin = source.admin_connection().await?;
    println!("✓ connected to MySQL");
    // a REQUIRE X509 account refuses the handshake outright, so reaching this
    // line with a certificate configured is itself the server's acceptance
    if let Some(path) = &cfg.tls_settings(source_url)?.client_cert {
        println!("✓ client certificate presented ({})", path.display());
    }
    pg2osync_source_mysql::catalog::check_prerequisites(&mut admin).await?;
    println!("✓ log_bin, binlog_format = ROW, binlog_row_image = FULL");
    source.bootstrap(&mut admin).await?;
    for table in cfg.sync.values() {
        let (schema, name) = table
            .table
            .split_once('.')
            .context("table must be written as database.table for MySQL")?;
        let live = pg2osync_source_mysql::catalog::table_schema(
            &mut admin,
            schema,
            name,
            table.append_only,
        )
        .await?;
        let names: Vec<String> = live.columns.iter().map(|c| c.name.clone()).collect();
        check_configured_columns(table, &names, &live.pk_columns, &[])?;
        for child in &table.children {
            let (cschema, cname) = child
                .table
                .split_once('.')
                .context("child table must be written as database.table for MySQL")?;
            let child_live =
                pg2osync_source_mysql::catalog::table_schema(&mut admin, cschema, cname, false)
                    .await?;
            let junction = mysql_junction(&mut admin, table, child, &child_live).await?;
            check_child_columns(child, &child_live.column_names(), junction.as_ref())?;
        }
        for agg in &table.aggregates {
            // the grouped read itself, bounded to nothing: it checks the
            // foreign key and the `where` against the real table
            let mut spec = pg2osync_core::aggregate::AggregateSpec::new(
                &agg.table,
                &agg.field,
                &agg.foreign_key,
                &table.primary_key.clone().unwrap_or_else(|| "id".into()),
            )?;
            if let Some(predicate) = &agg.filter {
                spec.filter = Some(
                    pg2osync_core::filter::Filter::parse(predicate).map_err(|e| {
                        anyhow::anyhow!("where {predicate:?} of {}: {e}", agg.table)
                    })?,
                );
            }
            let probe = format!(
                "SELECT 1 FROM ({}) c LIMIT 0",
                pg2osync_source_mysql::aggregate::count_statement(&spec, &[])
            );
            admin.query_text_rows(&probe).await.with_context(|| {
                format!(
                    "the aggregate {} of {} does not run against {}: check foreign_key and \
                     where",
                    agg.field, table.table, agg.table
                )
            })?;
            println!(
                "✓ aggregate {} counts {} by {}",
                agg.field, agg.table, agg.foreign_key
            );
        }
        if table.append_only {
            println!(
                "✓ table {} exists, append-only ({} columns)",
                table.table,
                names.len()
            );
        } else {
            println!(
                "✓ table {} exists with a primary key ({} columns)",
                table.table,
                names.len()
            );
        }
        // MySQL coerces where PostgreSQL errors, so this mostly catches a
        // column the catalogue spelled differently — still cheap, still worth
        // having before a load
        if let Some(spec) = &table.filter {
            let sql = pg2osync_core::filter::Filter::parse(spec)
                .map_err(|e| anyhow::anyhow!("where {spec:?} of {}: {e}", table.table))?
                .to_sql(&pg2osync_source_mysql::catalog::dialect());
            let probe = format!(
                "SELECT 1 FROM {}.{} WHERE ({sql}) LIMIT 0",
                pg2osync_source_mysql::catalog::quote_ident(schema),
                pg2osync_source_mysql::catalog::quote_ident(name)
            );
            admin.query_text_rows(&probe).await.with_context(|| {
                format!(
                    "the `where` predicate of {} is not valid SQL against that table",
                    table.table
                )
            })?;
            println!(
                "✓ where predicate of {} runs against the table",
                table.table
            );
        }
    }
    Ok(())
}

fn shutdown_signal() -> tokio::sync::watch::Receiver<bool> {
    let (tx, rx) = tokio::sync::watch::channel(false);
    tokio::spawn(async move {
        let signal = wait_for_shutdown_signal().await;
        tracing::info!(target: "pg2osync", "shutdown signal received ({signal}); draining");
        let _ = tx.send(true);
    });
    rx
}

/// Wait for a stop request and name the signal that carried it.
///
/// `docker stop`, Kubernetes and systemd all send SIGTERM; only a terminal
/// sends SIGINT. Leaving SIGTERM to the default handler made every routine
/// deploy an abrupt exit that replayed a checkpoint interval's worth of work.
#[cfg(unix)]
async fn wait_for_shutdown_signal() -> &'static str {
    use tokio::signal::unix::{SignalKind, signal};
    let mut terminate = match signal(SignalKind::terminate()) {
        Ok(stream) => stream,
        Err(e) => {
            tracing::warn!(target: "pg2osync", "cannot listen for SIGTERM ({e}); only SIGINT drains");
            let _ = tokio::signal::ctrl_c().await;
            return "SIGINT";
        }
    };
    tokio::select! {
        _ = tokio::signal::ctrl_c() => "SIGINT",
        _ = terminate.recv() => "SIGTERM",
    }
}

#[cfg(not(unix))]
async fn wait_for_shutdown_signal() -> &'static str {
    let _ = tokio::signal::ctrl_c().await;
    "Ctrl-C"
}

/// Report each source in turn. One failure is not the others': a directory
/// of configs is a directory of independent pipelines.
async fn status(
    ws: workspace::Workspace,
    caught_up: bool,
    timeout: u64,
    max_retained_mb: Option<u64>,
) -> Result<()> {
    let total = ws.sources.len();
    let mut failed: Vec<String> = Vec::new();
    for source in &ws.sources {
        if total > 1 {
            println!("\n── {} ({})", source.name, source.path.display());
        }
        let result = if caught_up {
            wait_until_caught_up(source.cfg.clone(), timeout).await
        } else {
            status_of(source.cfg.clone(), max_retained_mb).await
        };
        if let Err(e) = result {
            if total == 1 {
                return Err(e);
            }
            println!("✗ {e:#}");
            failed.push(source.name.clone());
        }
    }
    every_source(&failed, total, "reported a problem")
}

async fn status_of(cfg: config::AppConfig, max_retained_mb: Option<u64>) -> Result<()> {
    let secrets = cfg.resolve_secrets()?;
    let sink = run::build_sink(&cfg, secrets.target_password)?;
    match sink.read_checkpoint(&cfg.stream_id()).await? {
        Some(ckpt) => println!(
            "checkpoint: source={} stream={} position={}",
            ckpt.stream.source, ckpt.stream.stream, ckpt.position
        ),
        None => println!("checkpoint: none (a run will start with a full initial load)"),
    }

    if cfg.source.flavor == "mysql" {
        let source = mysql_source(&cfg, &secrets.source_url)?;
        let mut admin = source.admin_connection().await?;
        let (file, pos) = pg2osync_source_mysql::catalog::master_position(&mut admin).await?;
        println!("source: binlog at {file}:{pos}");
        return Ok(());
    }

    let client = connect_pg(&cfg, &secrets.source_url).await?;
    // every slot, not just ours: an orphan retains WAL until someone drops it,
    // and it is invisible to anyone who only asks about the configured name
    let slots = pg2osync_source::catalog::all_slots(&client).await?;
    if !slots.iter().any(|s| s.name == cfg.source.slot_name) {
        println!("slot {} does not exist", cfg.source.slot_name);
    }
    for slot in &slots {
        let mine = if slot.name == cfg.source.slot_name {
            " (configured)"
        } else {
            ""
        };
        // A slot past `reserved` is on its way to being unusable, and `lost`
        // already is: printing a big number without the server's own verdict
        // leaves the reader to guess which.
        let verdict = match slot.wal_status.as_str() {
            "" | "reserved" => String::new(),
            other => format!(" wal_status={other}"),
        };
        println!(
            "slot {}{mine}: active={} retained_wal={}{verdict}",
            slot.name,
            slot.active,
            slot.retained_pretty(),
        );
        if slot.wal_status == "lost" {
            println!(
                "  this slot can no longer be resumed from: the WAL it needed is gone, \
                 so a pipeline using it starts with a full initial load"
            );
        }
    }
    let idle: Vec<&str> = slots
        .iter()
        .filter(|s| !s.active && s.name != cfg.source.slot_name)
        .map(|s| s.name.as_str())
        .collect();
    if !idle.is_empty() {
        println!(
            "\n{} inactive slot(s) not named in this config: {}",
            idle.len(),
            idle.join(", ")
        );
        println!(
            "each holds WAL until it is dropped. If one is a former slot_name of \
             this pipeline: SELECT pg_drop_replication_slot('{}');",
            idle[0]
        );
    }
    // Checked over every slot, not only the configured one: an orphan fills the
    // same disk, and the whole point of a scheduled check is that it runs when
    // no pipeline is running to notice.
    if let Some(limit_mb) = max_retained_mb {
        let limit = limit_mb.saturating_mul(1024 * 1024) as i64;
        let over: Vec<&pg2osync_source::catalog::SlotInfo> = slots
            .iter()
            .filter(|s| s.retained_bytes.unwrap_or(0) > limit)
            .collect();
        if !over.is_empty() {
            for slot in &over {
                println!(
                    "\nslot {} is holding {}, over the {limit_mb} MB limit",
                    slot.name,
                    slot.retained_pretty()
                );
            }
            bail!(
                "{} slot(s) over the retention limit; the source keeps this WAL until \
                 the pipeline catches up or the slot is dropped",
                over.len()
            );
        }
    }
    Ok(())
}

async fn drop_slot(cfg: config::AppConfig, publication: bool) -> Result<()> {
    if cfg.source.flavor == "mysql" {
        bail!("drop-slot is PostgreSQL-only; MySQL keeps no server-side state for us");
    }
    let secrets = cfg.resolve_secrets()?;
    let client = connect_pg(&cfg, &secrets.source_url).await?;
    pg2osync_source::catalog::drop_slot(&client, &cfg.source.slot_name).await?;
    if publication {
        return Ok(
            pg2osync_source::catalog::drop_publication(&client, &cfg.source.publication).await?,
        );
    }
    // A reindex runs two pipelines on one publication, so dropping the old
    // one's slot used to take the publication out from under the new one —
    // which then could not stream at all.
    println!(
        "publication {} left in place; another pipeline may be reading it.\n\
         Drop it with --publication, or: DROP PUBLICATION {};",
        cfg.source.publication, cfg.source.publication
    );
    Ok(())
}

async fn connect_pg(cfg: &config::AppConfig, source_url: &str) -> Result<tokio_postgres::Client> {
    pg2osync_source::tls::connect(&cfg.tls_settings(source_url)?, source_url)
        .await
        .context("cannot connect to source PostgreSQL")
}

fn mysql_source(
    cfg: &config::AppConfig,
    source_url: &str,
) -> Result<pg2osync_source_mysql::runner::MySqlSource> {
    let url = url::Url::parse(source_url).context("source url is not a valid URL")?;
    Ok(pg2osync_source_mysql::runner::MySqlSource::new(
        pg2osync_source_mysql::runner::MySqlSourceConfig {
            host: url.host_str().unwrap_or("localhost").into(),
            port: url.port().unwrap_or(3306),
            user: url.username().into(),
            password: url.password().unwrap_or_default().into(),
            server_id: cfg.source.server_id,
            tables: cfg
                .sync
                .values()
                .map(|t| {
                    let (schema, table) = backfill::split_qualified(&t.table);
                    (schema.to_string(), table.to_string())
                })
                .collect(),
            start_file: None,
            start_pos: 0,
            children: Default::default(),
            aggregates: Default::default(),
            child_parents: Default::default(),
            // this source is built for one-off catalog work, not for streaming,
            // so it records nothing and resumes from nowhere
            gtid: None,
            gtid_resume: None,
            append_only: run::append_only_tables(cfg),
            version_base: 0,
            tls: cfg.tls_settings(source_url)?,
        },
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::workspace::fixtures::{TempDir, config as config_toml};
    use clap::Parser;

    /// What the command line accepts, without running anything.
    fn parse(args: &[&str]) -> Result<Command, clap::Error> {
        Cli::try_parse_from(std::iter::once("pg2osync").chain(args.iter().copied()))
            .map(|cli| cli.command)
    }

    /// Two sources in a directory, and the arguments a command reads them
    /// with.
    fn two_sources(tag: &str) -> (TempDir, ConfigArgs) {
        let dir = TempDir::new(tag);
        dir.write("orders.toml", &config_toml("", "orders"));
        dir.write(
            "billing.toml",
            &config_toml("slot_name = \"billing\"", "invoices"),
        );
        let args = ConfigArgs {
            config: PathBuf::from("pg2osync.toml"),
            config_dir: Some(dir.path().to_path_buf()),
        };
        (dir, args)
    }

    #[test]
    fn a_file_and_a_directory_are_alternatives_on_every_subcommand_that_takes_both() {
        for command in [
            "run",
            "validate",
            "bootstrap",
            "status",
            "setup-sql",
            "drop-slot",
            "rejects",
        ] {
            parse(&[command, "--config-dir", "/etc/pg2osync"])
                .unwrap_or_else(|e| panic!("{command} takes a directory: {e}"));
            assert!(
                parse(&[command, "-c", "one.toml", "--config-dir", "/etc/pg2osync"]).is_err(),
                "{command} must refuse a file and a directory at once"
            );
        }
    }

    #[test]
    fn init_is_the_one_command_a_directory_means_nothing_to() {
        // It writes the first config; there is no set of them to write into.
        assert!(parse(&["init", "--config-dir", "/etc/pg2osync"]).is_err());
        parse(&["init", "-c", "pg2osync.toml"]).expect("a file is what init writes");
    }

    #[test]
    fn a_maintenance_command_over_a_directory_asks_which_source() {
        let (_dir, args) = two_sources("maintenance");
        let why = format!(
            "{:#}",
            one_source(&args, None, "drop-slot").expect_err("two sources, no name")
        );
        assert!(
            why.contains("drop-slot acts on one source") && why.contains("billing, orders"),
            "the refusal has to name the command and the choices: {why}"
        );

        let cfg = one_source(&args, Some("billing"), "drop-slot").expect("a named source");
        assert_eq!(cfg.source.slot_name, "billing");
        assert!(
            one_source(&args, Some("typo"), "drop-slot").is_err(),
            "a name no file carries is a refusal, not the first file"
        );
    }

    #[test]
    fn one_file_needs_no_name_and_a_directory_of_one_does_not_either() {
        let dir = TempDir::new("single");
        let path = dir.write("orders.toml", &config_toml("", "orders"));
        let file = ConfigArgs {
            config: path,
            config_dir: None,
        };
        one_source(&file, None, "rejects").expect("one file is the source");

        let whole_dir = ConfigArgs {
            config: PathBuf::from("pg2osync.toml"),
            config_dir: Some(dir.path().to_path_buf()),
        };
        one_source(&whole_dir, None, "rejects")
            .expect("a directory holding one source names it unambiguously");
    }

    #[test]
    fn a_command_over_every_source_narrows_to_one_when_asked() {
        let (_dir, args) = two_sources("narrowing");
        let all = chosen_sources(&args, None).expect("both sources");
        assert_eq!(all.sources.len(), 2);
        let one = chosen_sources(&args, Some("orders")).expect("one source");
        assert_eq!(
            one.sources
                .iter()
                .map(|s| s.name.as_str())
                .collect::<Vec<_>>(),
            ["orders"]
        );
        assert!(chosen_sources(&args, Some("typo")).is_err());
    }

    #[test]
    fn a_log_format_is_named_in_any_case_and_nothing_else_is_accepted() {
        assert_eq!(LogFormat::parse("json").expect("json"), LogFormat::Json);
        assert_eq!(LogFormat::parse("JSON").expect("JSON"), LogFormat::Json);
        assert_eq!(LogFormat::parse(" Text ").expect("Text"), LogFormat::Text);
        assert_eq!(LogFormat::parse("text").expect("text"), LogFormat::Text);
        assert!(LogFormat::parse("jsonl").is_err());
        assert!(LogFormat::parse("").is_err());
    }

    #[test]
    fn a_refusal_anywhere_in_the_set_is_the_command_s_refusal() {
        every_source(&[], 3, "failed").expect("nothing failed");
        let why = format!(
            "{:#}",
            every_source(&["billing".to_string()], 3, "failed").expect_err("one failed")
        );
        assert_eq!(why, "1 of 3 source(s) failed: billing");
    }

    #[test]
    fn without_an_endpoint_no_trace_export_is_configured_at_all() {
        assert_eq!(
            TraceExport::parse(None, Some("0.1"), Some("shop"))
                .expect("no endpoint is not an error"),
            None,
            "a ratio and a name alone must not build an exporter"
        );
        assert_eq!(
            TraceExport::parse(Some("  "), None, None).expect("blank is unset"),
            None
        );
    }

    #[test]
    fn a_sample_ratio_is_a_fraction_and_nothing_else_is_accepted() {
        let with = |ratio| TraceExport::parse(Some("http://otel:4317"), Some(ratio), None);
        assert_eq!(
            with("0.05").expect("a fraction").expect("built").ratio,
            0.05
        );
        assert_eq!(with(" 1 ").expect("padded").expect("built").ratio, 1.0);
        for refused in ["1.5", "-0.1", "half", "100%", "0.1.2"] {
            assert!(
                with(refused).is_err(),
                "{refused:?} is not a sampling ratio and must be refused at startup"
            );
        }
    }

    #[test]
    fn the_service_name_defaults_to_the_binarys_own() {
        let built = TraceExport::parse(Some("http://otel:4317"), None, None)
            .expect("valid")
            .expect("built");
        assert_eq!(built.service_name, "pg2osync");
        assert_eq!(built.ratio, 1.0, "every trace, until an operator says less");
        let named = TraceExport::parse(Some("http://otel:4317"), None, Some("shop-sync"))
            .expect("valid")
            .expect("built");
        assert_eq!(named.service_name, "shop-sync");
    }

    fn found() -> Vec<String> {
        vec![
            "public.products".to_string(),
            "public.users".to_string(),
            "shop.users".to_string(),
        ]
    }

    #[test]
    fn a_rename_onto_a_live_column_is_refused_and_a_stale_key_is_not() {
        let table = |extra: &str| -> config::TableSync {
            toml::from_str(&format!("table = \"public.users\"\n{extra}")).expect("parses")
        };
        let live: Vec<String> = ["id", "name", "email"]
            .iter()
            .map(|c| c.to_string())
            .collect();
        let pk = vec!["id".to_string()];

        assert!(
            check_configured_columns(&table("[fields]\nname = \"email\"\n"), &live, &pk, &[])
                .is_err(),
            "email still reaches the target, so name would bury it"
        );
        check_configured_columns(&table("[fields]\ngone = \"x\"\n"), &live, &pk, &[])
            .expect("a rename of a column that is not there is stale config, not a fault");
        check_configured_columns(
            &table("exclude_columns = [\"email\"]\n[fields]\nname = \"email\"\n"),
            &live,
            &pk,
            &[],
        )
        .expect("an excluded column leaves its name free");
    }

    #[test]
    fn a_constant_over_a_live_column_is_refused() {
        let table = |extra: &str| -> config::TableSync {
            toml::from_str(&format!("table = \"public.users\"\n{extra}")).expect("parses")
        };
        let live: Vec<String> = ["id", "name", "email"]
            .iter()
            .map(|c| c.to_string())
            .collect();
        let pk = vec!["id".to_string()];

        assert!(
            check_configured_columns(&table("[constants]\nname = \"x\"\n"), &live, &pk, &[])
                .is_err(),
            "name still reaches the target, so the constant would bury it"
        );
        check_configured_columns(
            &table("exclude_columns = [\"name\"]\n[constants]\nname = \"x\"\n"),
            &live,
            &pk,
            &[],
        )
        .expect("an excluded column leaves its name free");
        check_configured_columns(
            &table("[fields]\nname = \"n\"\n[constants]\nname = \"x\"\n"),
            &live,
            &pk,
            &[],
        )
        .expect("a renamed column leaves its name free");
    }

    #[test]
    fn a_join_child_needs_its_parent_column_and_the_join_field_needs_a_free_name() {
        let table = |extra: &str| -> config::TableSync {
            toml::from_str(&format!("table = \"public.orders\"\n{extra}")).expect("parses")
        };
        let live: Vec<String> = ["id", "customer_id", "relation"]
            .iter()
            .map(|c| c.to_string())
            .collect();
        let pk = vec!["id".to_string()];
        const CHILD: &str = "[join]\nfield = \"rel\"\nname = \"order\"\nparent = \"customer_id\"\n";

        check_configured_columns(&table(CHILD), &live, &pk, &[]).expect("the parent column exists");
        let err = check_configured_columns(
            &table(&CHILD.replace("customer_id", "account_id")),
            &live,
            &pk,
            &[],
        )
        .expect_err("a parent column the table lacks could never route a document");
        assert!(err.to_string().contains("account_id"), "{err}");
        let err = check_configured_columns(
            &table(&CHILD.replace("field = \"rel\"", "field = \"relation\"")),
            &live,
            &pk,
            &[],
        )
        .expect_err("a live column under the join field's name would be buried");
        assert!(err.to_string().contains("join field relation"), "{err}");
        check_configured_columns(
            &table(&format!(
                "exclude_columns = [\"relation\"]\n{}",
                CHILD.replace("field = \"rel\"", "field = \"relation\"")
            )),
            &live,
            &pk,
            &[],
        )
        .expect("an excluded column leaves its name free");
    }

    #[test]
    fn a_flattened_child_may_not_lift_a_name_the_parent_already_carries() {
        let table = |extra: &str| -> config::TableSync {
            toml::from_str(&format!("table = \"public.contacts\"\n{extra}")).expect("parses")
        };
        let live: Vec<String> = ["id", "company_name"]
            .iter()
            .map(|c| c.to_string())
            .collect();
        let pk = vec!["id".to_string()];
        const CHILD: &str = "[[children]]\ntable = \"public.company\"\nfield = \"company\"\n\
                             foreign_key = \"company_id\"\nsingle = true\nflatten = true\n\
                             columns = [\"customer_name\"]\n";

        check_configured_columns(&table(CHILD), &live, &pk, &[])
            .expect("customer_name is not a column of the parent");
        let err = check_configured_columns(
            &table(&format!(
                "{CHILD}[children.fields]\ncustomer_name = \"company_name\"\n"
            )),
            &live,
            &pk,
            &[],
        )
        .expect_err("the lifted name is a column of the parent");
        assert!(err.to_string().contains("lifts company_name"), "{err}");
        // the parent's own column is renamed out of the way, so the name is free
        check_configured_columns(
            &table(&format!(
                "[fields]\ncompany_name = \"legacy_name\"\n{CHILD}\
                 [children.fields]\ncustomer_name = \"company_name\"\n"
            )),
            &live,
            &pk,
            &[],
        )
        .expect("a renamed column leaves its name free");
    }

    #[test]
    fn an_index_placeholder_must_name_a_live_column() {
        let table = |extra: &str| -> config::TableSync {
            toml::from_str(&format!("table = \"public.events\"\n{extra}")).expect("parses")
        };
        let live: Vec<String> = ["id", "tenant"].iter().map(|c| c.to_string()).collect();
        let pk = vec!["id".to_string()];

        check_configured_columns(&table("index = \"events-{tenant}\"\n"), &live, &pk, &[])
            .expect("the placeholder names a column the table has");
        let err =
            check_configured_columns(&table("index = \"events-{region}\"\n"), &live, &pk, &[])
                .expect_err("a column the table lacks could never choose an index");
        assert!(
            err.to_string()
                .contains("table public.events has no column region to choose its index by"),
            "{err}"
        );
        check_configured_columns(&table("index = \"events\"\n"), &live, &pk, &[])
            .expect("a fixed index names no column");
    }

    #[test]
    fn a_bare_name_is_qualified_from_the_catalogue() {
        // The mistake a first config makes, which used to surface two commands
        // later as a validation error.
        assert_eq!(
            qualify("products", &found()),
            Some("public.products".to_string())
        );
    }

    #[test]
    fn a_bare_name_two_schemas_share_is_not_guessed() {
        assert_eq!(
            qualify("users", &found()),
            None,
            "guessing here writes a config that syncs the wrong table"
        );
    }

    #[test]
    fn a_qualified_name_is_taken_only_if_the_source_has_it() {
        assert_eq!(
            qualify("shop.users", &found()),
            Some("shop.users".to_string())
        );
        assert_eq!(qualify("public.missing", &found()), None);
    }

    fn keyed(qualified: &str) -> SourceTable {
        SourceTable {
            qualified: qualified.to_string(),
            keyed: true,
        }
    }

    #[test]
    fn the_starter_config_loads_and_names_the_table_it_was_given() {
        let toml_text = starter_config(&[keyed("public.orders")], "http://os:9200", false);
        let parsed: config::AppConfig =
            toml::from_str(&toml_text).expect("what init writes has to load");
        assert_eq!(parsed.sync.len(), 1);
        let table = parsed.sync.get("orders").expect("named after the table");
        assert_eq!(table.table, "public.orders");
        assert_eq!(parsed.target.url, "http://os:9200");
    }

    #[test]
    fn a_keyless_table_is_written_as_append_only() {
        // the only way such a table syncs, so the smallest config still runs
        let toml_text = starter_config(
            &[
                keyed("public.orders"),
                SourceTable {
                    qualified: "public.events_log".to_string(),
                    keyed: false,
                },
            ],
            "http://os:9200",
            false,
        );
        let parsed: config::AppConfig =
            toml::from_str(&toml_text).expect("what init writes has to load");
        parsed.validate().expect("and pass validation");
        assert!(parsed.sync["events_log"].append_only);
        assert!(!parsed.sync["orders"].append_only);
        assert!(
            toml_text.contains("append_only = true"),
            "the flag is spelled out where the operator will read it"
        );
    }

    #[test]
    fn the_mysql_starter_says_so_and_carries_a_server_id() {
        let toml_text = starter_config(&[keyed("appdb.users")], "http://os:9200", true);
        let parsed: config::AppConfig = toml::from_str(&toml_text).expect("loads");
        assert_eq!(parsed.source.flavor, "mysql");
        assert_ne!(
            parsed.source.server_id, 0,
            "a replica id of zero is not usable against a real server"
        );
    }
}
