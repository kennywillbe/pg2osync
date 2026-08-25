//! CLI entry point. This is the only crate that turns typed errors into exit
//! codes and human-readable diagnostics.

mod backfill;
mod config;
mod run;

use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand};
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
    },
    /// Print the SQL a DBA needs to run, derived from the config.
    SetupSql {
        #[arg(short, long, value_name = "FILE")]
        config: PathBuf,
    },
    /// Drop the replication slot and publication (PostgreSQL only).
    DropSlot {
        #[arg(short, long, value_name = "FILE")]
        config: PathBuf,
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
        Command::Status { config } => status(&config).await,
        Command::SetupSql { config } => setup_sql(&config),
        Command::DropSlot { config } => drop_slot(&config).await,
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
        bail!("table {} has no column {pk} to use as primary_key", table.table);
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
    match sink.read_checkpoint().await? {
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

async fn drop_slot(path: &Path) -> Result<()> {
    let cfg = config::AppConfig::load(path)?;
    if cfg.source.flavor == "mysql" {
        bail!("drop-slot is PostgreSQL-only; MySQL keeps no server-side state for us");
    }
    let secrets = cfg.resolve_secrets()?;
    let client = connect_pg(&cfg, &secrets.source_url).await?;
    pg2osync_source::catalog::drop_slot(&client, &cfg.source.slot_name).await?;
    pg2osync_source::catalog::drop_publication(&client, &cfg.source.publication).await
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
