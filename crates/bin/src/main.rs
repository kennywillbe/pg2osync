mod config;
mod run;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use pg2osync_core::sink::Sink;
use std::path::{Path, PathBuf};

#[derive(Parser)]
#[command(
    name = "pg2osync",
    version,
    about = "PostgreSQL to OpenSearch real-time sync"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Backfill + streaming (main mode).
    Run {
        #[arg(short, long)]
        config: PathBuf,
    },
    /// Show checkpoint and replication slot state.
    Status {
        #[arg(short, long)]
        config: PathBuf,
    },
    /// Drop the replication slot (prevents WAL buildup after decommissioning).
    DropSlot {
        #[arg(short, long)]
        config: PathBuf,
    },
    /// Validate config and check both connections.
    Validate {
        #[arg(short, long)]
        config: PathBuf,
    },
    /// Create publication/slot and run the backfill, then exit.
    Bootstrap {
        #[arg(short, long)]
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
        Command::Validate { config } => validate(&config).await,
        Command::Run { config } | Command::Bootstrap { config } => {
            let cfg = config::AppConfig::load(&config)?;
            let secrets = cfg.resolve_secrets()?;
            for w in &secrets.warnings {
                tracing::warn!(target: "pg2osync::config", "{w}");
            }
            let durable = pg2osync_engine::mapping::DurableLsn::default();
            run::run_pipeline(
                cfg,
                secrets.source_url,
                secrets.target_password,
                shutdown_signal(),
                durable,
            )
            .await
        }
        Command::Status { config } => status(&config).await,
        Command::DropSlot { config } => drop_slot(&config).await,
    }
}

async fn validate(path: &Path) -> Result<()> {
    let cfg = config::AppConfig::load(path)?;
    let secrets = cfg.resolve_secrets()?;

    for w in &secrets.warnings {
        tracing::warn!(target: "pg2osync::config", "{w}");
    }
    println!(
        "✓ config structure valid ({} table mappings)",
        cfg.sync.len()
    );

    let (client, conn) = tokio_postgres::connect(&secrets.source_url, tokio_postgres::NoTls)
        .await
        .context("cannot connect to source PostgreSQL")?;
    tokio::spawn(async move {
        if let Err(e) = conn.await {
            tracing::debug!("source connection closed: {e}");
        }
    });
    println!("✓ connected to PostgreSQL");

    let wal_level: String = client
        .query_one(
            "SELECT setting FROM pg_settings WHERE name = 'wal_level'",
            &[],
        )
        .await?
        .get(0);
    if wal_level != "logical" {
        anyhow::bail!(
            "wal_level is '{wal_level}' but must be 'logical'; \
             set `wal_level = logical` in postgresql.conf and restart PostgreSQL"
        );
    }
    println!("✓ wal_level = logical");

    for tbl in cfg.sync.values() {
        let exists = client
            .query_opt("SELECT 1 FROM to_regclass($1)", &[&tbl.table])
            .await?;
        if exists.is_none() {
            anyhow::bail!("table {} does not exist", tbl.table);
        }
        println!("✓ table {} exists", tbl.table);
    }

    let sink_cfg = pg2osync_sink::OpenSearchSinkConfig {
        url: cfg.target.url.clone(),
        username: cfg.target.username.clone(),
        password: secrets.target_password,
        tls_verify: cfg.target.tls_verify,
        serverless: cfg.target.serverless,
    };
    let sink = pg2osync_sink::OpenSearchSink::new(sink_cfg)?;
    match sink.health().await? {
        pg2osync_core::sink::Health::Up => println!("✓ OpenSearch reachable at {}", cfg.target.url),
        pg2osync_core::sink::Health::Down(reason) => {
            anyhow::bail!("OpenSearch responded but is not healthy: {reason}")
        }
    }

    println!("\nall checks passed");
    Ok(())
}

fn shutdown_signal() -> tokio::sync::watch::Receiver<bool> {
    let (tx, rx) = tokio::sync::watch::channel(false);
    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        tracing::info!(target: "pg2osync", "shutdown signal received");
        let _ = tx.send(true);
    });
    rx
}

async fn status(path: &Path) -> Result<()> {
    let cfg = config::AppConfig::load(path)?;
    let secrets = cfg.resolve_secrets()?;
    let sink_cfg = pg2osync_sink::OpenSearchSinkConfig {
        url: cfg.target.url.clone(),
        username: cfg.target.username.clone(),
        password: secrets.target_password,
        tls_verify: cfg.target.tls_verify,
        serverless: cfg.target.serverless,
    };
    let sink = pg2osync_sink::OpenSearchSink::new(sink_cfg)?;
    match sink.read_checkpoint().await? {
        Some(lsn) => println!("checkpoint confirmed_lsn = {lsn}"),
        None => println!("no checkpoint present"),
    }
    let (client, conn) = tokio_postgres::connect(&secrets.source_url, tokio_postgres::NoTls)
        .await
        .context("cannot connect to source")?;
    tokio::spawn(async move {
        let _ = conn.await;
    });
    if let Some(r) = client
        .query_opt(
            "SELECT active, confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = $1",
            &[&cfg.source.slot_name],
        )
        .await?
    {
        println!(
            "slot {} active={} confirmed_flush={}",
            cfg.source.slot_name,
            r.get::<_, bool>(0),
            r.get::<_, String>(1),
        );
    } else {
        println!("slot {} does not exist", cfg.source.slot_name);
    }
    Ok(())
}

async fn drop_slot(path: &Path) -> Result<()> {
    let cfg = config::AppConfig::load(path)?;
    let secrets = cfg.resolve_secrets()?;
    let (client, conn) = tokio_postgres::connect(&secrets.source_url, tokio_postgres::NoTls)
        .await
        .context("cannot connect to source")?;
    tokio::spawn(async move {
        let _ = conn.await;
    });
    pg2osync_source::catalog::drop_slot(&client, &cfg.source.slot_name).await
}
