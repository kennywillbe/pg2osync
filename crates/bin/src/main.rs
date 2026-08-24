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
        println!("✓ table {} exists", table.table);
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
        println!("✓ table {} exists with a primary key", table.table);
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
    match client
        .query_opt(
            "SELECT active, confirmed_flush_lsn::text, \
                    pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)) \
             FROM pg_replication_slots WHERE slot_name = $1",
            &[&cfg.source.slot_name],
        )
        .await?
    {
        Some(row) => println!(
            "slot {}: active={} confirmed_flush={} retained_wal={}",
            cfg.source.slot_name,
            row.get::<_, bool>(0),
            row.get::<_, String>(1),
            row.get::<_, String>(2),
        ),
        None => println!("slot {} does not exist", cfg.source.slot_name),
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
    cfg.tls_settings(source_url)?
        .connect(source_url)
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
        },
    ))
}
