//! Prints parsed ChangeEvents (JSON documents) live from PostgreSQL WAL.
//!
//! Usage:
//!   cargo run -p pg2osync-source --example dump-wal -- \
//!       "postgres://postgres:postgres@localhost:15432/sourcedb" slot_name pub_name public.users
//!
//! Make changes to the table in another session; each committed row change
//! is printed as a JSON document. Ctrl-C exits cleanly.

use anyhow::Result;
use pg2osync_core::event::{ChangeEvent, RowKind};
use pg2osync_source::runner::{WalSource, WalSourceConfig};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "pg2osync=debug".into()),
        )
        .init();

    let mut args = std::env::args().skip(1);
    let url = args.next().expect("admin url required");
    let slot = args.next().expect("slot name required");
    let publication = args.next().expect("publication name required");
    let table = args.next().expect("qualified table name required");

    let cfg_url = url::Url::parse(&url)?;
    let tls = pg2osync_source::tls::TlsSettings::resolve(
        &url,
        pg2osync_source::tls::ConfiguredTls::default(),
    )?;
    let cfg = WalSourceConfig {
        tls: tls.clone(),
        host: cfg_url.host_str().unwrap_or("localhost").to_string(),
        port: cfg_url.port().unwrap_or(5432),
        user: cfg_url.username().to_string(),
        password: cfg_url.password().unwrap_or_default().to_string(),
        database: cfg_url
            .path()
            .trim_start_matches('/')
            .split('?')
            .next()
            .unwrap_or_default()
            .to_string(),
        slot_name: slot.clone(),
        publication: publication.clone(),
        tables: vec![table.clone()],
        start_lsn: None,
        durable: None,
        admin_url: Some(url.clone()),
        children: Default::default(),
        child_parents: Default::default(),
        parent_pk_columns: Default::default(),
        key_columns: Default::default(),
        append_only: Default::default(),
    };

    let admin = pg2osync_source::tls::connect(&tls, &url).await?;

    WalSource::new(cfg.clone()).bootstrap(&admin).await?;
    let start_lsn = pg2osync_source::catalog::confirmed_flush_lsn(&admin, &cfg.slot_name).await?;
    let tls = pg2osync_source::tls::TlsSettings::resolve(
        &url,
        pg2osync_source::tls::ConfiguredTls::default(),
    )?;
    let cfg = WalSourceConfig {
        tls: tls.clone(),
        start_lsn,
        durable: None,
        ..cfg
    };
    println!(
        "bootstrapped: slot={slot} publication={publication} table={table}\nstreaming... (ctrl-c to stop)\n"
    );

    let (tx, mut rx) = tokio::sync::mpsc::channel::<ChangeEvent>(1024);
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

    // printer owns the receiver; ends when the source closes the channel
    let printer = tokio::spawn(async move {
        while let Some(ev) = rx.recv().await {
            if let ChangeEvent::Row(r) = ev {
                match &r.kind {
                    RowKind::Insert { pk, doc } => println!(
                        "[{}.{}] INSERT pk={pk}\n  {}",
                        r.schema,
                        r.table,
                        serde_json::to_string_pretty(doc)?
                    ),
                    RowKind::Update {
                        pk,
                        previous_pk,
                        doc,
                        unchanged_toast_columns,
                        ..
                    } => println!(
                        "[{}.{}] UPDATE pk={pk}{} toast_incomplete={unchanged_toast_columns:?}\n  {}",
                        r.schema,
                        r.table,
                        match previous_pk {
                            Some(previous) if previous != pk => format!(" (moved from {previous})"),
                            _ => String::new(),
                        },
                        serde_json::to_string_pretty(doc)?
                    ),
                    RowKind::Delete { pk, .. } => {
                        println!("[{}.{}] DELETE pk={pk}", r.schema, r.table)
                    }
                }
                println!();
                use std::io::Write;
                std::io::stdout().flush()?;
            }
        }
        Ok::<(), anyhow::Error>(())
    });

    // ctrl-c requests graceful source shutdown
    tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            let _ = shutdown_tx.send(true);
        }
    });

    let mut source = WalSource::new(cfg);
    let result = source.stream(tx, shutdown_rx, None).await;
    // surface source termination loudly: silent exits here cost hours of debugging
    if let Err(e) = &result {
        eprintln!("source terminated with error: {e:#}");
    } else {
        eprintln!("source stream ended normally");
    }
    let _ = printer.await;
    Ok(result?)
}
