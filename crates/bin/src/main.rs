//! CLI entry point. This is the only crate that turns typed errors into exit
//! codes and human-readable diagnostics.

mod backfill;
mod config;
mod reconcile;
mod run;

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

#[derive(Subcommand)]
enum Command {
    /// Initial load plus continuous streaming (main mode).
    Run {
        #[arg(short, long, value_name = "FILE")]
        config: PathBuf,
    },
    /// Validate the config and check both connections.
    Validate {
        #[arg(short, long, value_name = "FILE")]
        config: PathBuf,
    },
    /// Create source-side objects and target indices, then exit.
    Bootstrap {
        #[arg(short, long, value_name = "FILE")]
        config: PathBuf,
    },
    /// Show the checkpoint and the source's current position.
    Status {
        #[arg(short, long, value_name = "FILE")]
        config: PathBuf,
        /// Exit 0 only once the checkpoint has reached the source's current
        /// position, so a script can wait instead of comparing by eye.
        #[arg(long)]
        caught_up: bool,
        /// Seconds to keep checking with --caught-up.
        #[arg(long, default_value_t = 0)]
        timeout: u64,
    },
    /// Point an alias at this config's index, atomically.
    SwitchAlias {
        #[arg(short, long, value_name = "FILE")]
        config: PathBuf,
        /// The alias to move. Applies to the single configured table.
        #[arg(long)]
        alias: String,
    },
    /// Compare each index against its source table and report documents whose
    /// row is gone. Reports only unless --delete is given.
    Reconcile {
        #[arg(short, long, value_name = "FILE")]
        config: PathBuf,
        /// Remove the documents instead of only naming them.
        #[arg(long)]
        delete: bool,
    },
    /// Print the SQL a DBA needs to run, derived from the config.
    SetupSql {
        #[arg(short, long, value_name = "FILE")]
        config: PathBuf,
    },
    /// List the documents the target refused and, with --replay, submit them
    /// again once the mapping that refused them is fixed.
    Rejects {
        #[arg(short, long, value_name = "FILE")]
        config: PathBuf,
        /// Submit each one again, clearing the ones the target now accepts.
        #[arg(long)]
        replay: bool,
        /// How many to show.
        #[arg(long, default_value_t = 20)]
        limit: usize,
    },
    /// Drop the replication slot (PostgreSQL only).
    DropSlot {
        #[arg(short, long, value_name = "FILE")]
        config: PathBuf,
        /// Also drop the publication. Off by default: a second pipeline may be
        /// reading the same one.
        #[arg(long)]
        publication: bool,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "pg2osync=info".into()),
        )
        .init();

    match Cli::parse().command {
        Command::Run { config } => pipeline(&config, run::Mode::Run).await,
        Command::Bootstrap { config } => pipeline(&config, run::Mode::Bootstrap).await,
        Command::Validate { config } => validate(&config).await,
        Command::Status {
            config,
            caught_up,
            timeout,
        } => {
            if caught_up {
                wait_until_caught_up(&config, timeout).await
            } else {
                status(&config).await
            }
        }
        Command::Reconcile { config, delete } => reconcile_cmd(&config, delete).await,
        Command::SwitchAlias { config, alias } => switch_alias(&config, &alias).await,
        Command::SetupSql { config } => setup_sql(&config),
        Command::Rejects {
            config,
            replay,
            limit,
        } => rejects_cmd(&config, replay, limit).await,
        Command::DropSlot {
            config,
            publication,
        } => drop_slot(&config, publication).await,
    }
}

async fn pipeline(path: &Path, mode: run::Mode) -> Result<()> {
    let cfg = config::AppConfig::load(path)?;
    let secrets = cfg.resolve_secrets()?;
    for warning in &secrets.warnings {
        tracing::warn!(target: "pg2osync::config", "{warning}");
    }
    run::run_pipeline(
        cfg,
        secrets.source_url,
        secrets.admin_url,
        secrets.target_password,
        shutdown_signal(),
        pg2osync_engine::mapping::DurableLsn::default(),
        mode,
    )
    .await
}

/// Print the source-side setup script for this config.
///
/// Offline on purpose: the point is to hand something to whoever holds the
/// privileges, which is usually not whoever is running this.
fn setup_sql(path: &Path) -> Result<()> {
    let cfg = config::AppConfig::load(path)?;
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
    // child tables are read by the initial load too, so they need the grant
    for table in cfg.sync.values() {
        for child in &table.children {
            if !tables.contains(&child.table) {
                tables.push(child.table.clone());
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
async fn reconcile_cmd(path: &Path, delete: bool) -> Result<()> {
    let cfg = config::AppConfig::load(path)?;
    if cfg.source.flavor == "mysql" {
        bail!("reconcile is PostgreSQL-only for now");
    }
    let secrets = cfg.resolve_secrets()?;
    let sink = run::build_sink(&cfg, secrets.target_password)?;
    let client = connect_pg(&cfg, &secrets.source_url).await?;

    let mut total_orphans = 0usize;
    for (key, table) in &cfg.sync {
        let spec = reconcile::Table {
            qualified: table.table.clone(),
            index: table.index_name(key),
            key_column: table.primary_key.clone().unwrap_or_else(|| "id".into()),
            soft_delete: table.soft_delete.clone(),
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
        for id in report.orphaned.iter().take(10) {
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
async fn switch_alias(path: &Path, alias: &str) -> Result<()> {
    let cfg = config::AppConfig::load(path)?;
    let mut indices: Vec<String> = cfg.sync.iter().map(|(k, t)| t.index_name(k)).collect();
    indices.sort();
    indices.dedup();
    let [index] = indices.as_slice() else {
        bail!(
            "switch-alias needs a config with exactly one table; this one has {}",
            indices.len()
        );
    };
    let secrets = cfg.resolve_secrets()?;
    let sink = run::build_sink(&cfg, secrets.target_password)?;
    sink.switch_alias(alias, index).await?;
    println!("alias {alias} now points at {index}");
    Ok(())
}

/// The stream this config identifies, which is also where its checkpoint lives.
fn stream_id(cfg: &config::AppConfig) -> pg2osync_core::checkpoint::StreamId {
    if cfg.source.flavor == "mysql" {
        pg2osync_core::checkpoint::StreamId {
            source: "mysql".into(),
            stream: cfg.source.server_id.to_string(),
            publication: String::new(),
        }
    } else {
        pg2osync_core::checkpoint::StreamId {
            source: "postgres".into(),
            stream: cfg.source.slot_name.clone(),
            publication: cfg.source.publication.clone(),
        }
    }
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
async fn wait_until_caught_up(path: &Path, timeout_secs: u64) -> Result<()> {
    let cfg = config::AppConfig::load(path)?;
    let secrets = cfg.resolve_secrets()?;
    let sink = run::build_sink(&cfg, secrets.target_password)?;
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(timeout_secs);

    loop {
        let checkpoint = sink.read_checkpoint(&stream_id(&cfg)).await?;
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

/// Show what the target refused, and optionally offer it again.
///
/// A replay reuses the ordinary write path with the document's original
/// position as its version, so anything the source has since superseded is
/// refused by the version rule and the newer value stands — which is what
/// should happen, and costs no special handling here.
async fn rejects_cmd(path: &Path, replay: bool, limit: usize) -> Result<()> {
    let cfg = config::AppConfig::load(path)?;
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

async fn validate(path: &Path) -> Result<()> {
    let cfg = config::AppConfig::load(path)?;
    let secrets = cfg.resolve_secrets()?;
    for warning in &secrets.warnings {
        tracing::warn!(target: "pg2osync::config", "{warning}");
    }
    println!(
        "✓ config structure valid ({} table mappings)",
        cfg.sync.len()
    );

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
    run::check_rejection_policy(&cfg, sink.as_ref())?;
    if cfg.engine.on_permanent_rejection == pg2osync_engine::RejectionPolicy::Quarantine {
        let (_, held) = sink.list_rejects(0).await.unwrap_or_default();
        println!(
            "✓ quarantine on permanent rejection, {held} of {} used",
            cfg.engine.max_rejects
        );
    }

    println!("\nall checks passed");
    Ok(())
}

/// Check a table's configuration against the columns it actually has.
///
/// A `columns` list naming something that no longer exists is silently ignored
/// today, so the field simply stops appearing in new documents and the index
/// disagrees with the database with nothing to show for it. Naming it at
/// startup is far cheaper than finding it later.
fn check_configured_columns(table: &config::TableSync, live: &[String]) -> Result<()> {
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
    // an exclusion or a transform for a column that is gone changes nothing,
    // so it is stale configuration rather than a fault
    for (label, names) in [
        ("exclude_columns", table.exclude_columns.clone()),
        (
            "transform",
            table.transform.keys().cloned().collect::<Vec<_>>(),
        ),
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

async fn validate_postgres(cfg: &config::AppConfig, source_url: &str) -> Result<()> {
    let client = connect_pg(cfg, source_url).await?;
    println!(
        "✓ connected to PostgreSQL (sslmode={})",
        cfg.tls_settings(source_url)?.mode.as_str()
    );

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
        check_configured_columns(table, &live)?;
        println!("✓ table {} exists ({} columns)", table.table, live.len());
        tables.push(table.table.clone());
        for child in &table.children {
            tables.push(child.table.clone());
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
    }
    println!("\nAsk a privileged role to run this once, then re-run validate:\n");
    for statement in pre.setup_sql(&cfg.source.publication, &cfg.source.slot_name, tables) {
        println!("  {statement}");
    }
    println!();
    bail!("insufficient privileges to bootstrap; see the statements above")
}

async fn validate_mysql(cfg: &config::AppConfig, source_url: &str) -> Result<()> {
    let source = mysql_source(cfg, source_url)?;
    let mut admin = source.admin_connection().await?;
    println!("✓ connected to MySQL");
    pg2osync_source_mysql::catalog::check_prerequisites(&mut admin).await?;
    println!("✓ log_bin, binlog_format = ROW, binlog_row_image = FULL");
    source.bootstrap(&mut admin).await?;
    for table in cfg.sync.values() {
        let (schema, name) = table
            .table
            .split_once('.')
            .context("table must be written as database.table for MySQL")?;
        let live = pg2osync_source_mysql::catalog::table_schema(&mut admin, schema, name).await?;
        let names: Vec<String> = live.columns.iter().map(|c| c.name.clone()).collect();
        check_configured_columns(table, &names)?;
        println!(
            "✓ table {} exists with a primary key ({} columns)",
            table.table,
            names.len()
        );
    }
    Ok(())
}

fn shutdown_signal() -> tokio::sync::watch::Receiver<bool> {
    let (tx, rx) = tokio::sync::watch::channel(false);
    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        tracing::info!(target: "pg2osync", "shutdown signal received; draining");
        let _ = tx.send(true);
    });
    rx
}

async fn status(path: &Path) -> Result<()> {
    let cfg = config::AppConfig::load(path)?;
    let secrets = cfg.resolve_secrets()?;
    let sink = run::build_sink(&cfg, secrets.target_password)?;
    match sink.read_checkpoint(&stream_id(&cfg)).await? {
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
        println!(
            "slot {}{mine}: active={} retained_wal={}",
            slot.name,
            slot.active,
            slot.retained_pretty(),
        );
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
    Ok(())
}

async fn drop_slot(path: &Path, publication: bool) -> Result<()> {
    let cfg = config::AppConfig::load(path)?;
    if cfg.source.flavor == "mysql" {
        bail!("drop-slot is PostgreSQL-only; MySQL keeps no server-side state for us");
    }
    let secrets = cfg.resolve_secrets()?;
    let client = connect_pg(&cfg, &secrets.source_url).await?;
    pg2osync_source::catalog::drop_slot(&client, &cfg.source.slot_name).await?;
    if publication {
        return pg2osync_source::catalog::drop_publication(&client, &cfg.source.publication).await;
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
            tls: cfg.tls_settings(source_url)?,
        },
    ))
}
