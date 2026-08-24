//! PostgreSQL-specific TLS glue.
//!
//! The mode vocabulary and the rustls configuration are shared with the MySQL
//! source; what lives here is how a PostgreSQL connection and the replication
//! transport consume them.

use anyhow::{Context as _, Result};
use tokio_postgres::Client;

pub use pg2osync_tls::{SslMode, TlsSettings};

/// Connect and spawn the connection task, with or without TLS.
pub async fn connect(tls: &TlsSettings, url: &str) -> Result<Client> {
    if tls.mode == SslMode::Disable {
        let (client, connection) = tokio_postgres::connect(url, tokio_postgres::NoTls)
            .await
            .context("connection failed")?;
        spawn_connection_task(connection);
        return Ok(client);
    }

    let mut config: tokio_postgres::Config = url.parse().context("invalid connection url")?;
    // tokio-postgres only distinguishes "must be encrypted" from "try";
    // certificate and hostname checking is the connector's job.
    config.ssl_mode(if tls.mode.requires_tls() {
        tokio_postgres::config::SslMode::Require
    } else {
        tokio_postgres::config::SslMode::Prefer
    });

    let connector = tokio_postgres_rustls::MakeRustlsConnect::new(tls.client_config()?);
    let (client, connection) = config
        .connect(connector)
        .await
        .with_context(|| format!("connection failed (sslmode={})", tls.mode.as_str()))?;
    spawn_connection_task(connection);
    Ok(client)
}

/// The equivalent configuration for the replication transport.
pub fn replication_config(tls: &TlsSettings) -> pgwire_replication::TlsConfig {
    use pgwire_replication::SslMode as PgWire;
    pgwire_replication::TlsConfig {
        mode: match tls.mode {
            SslMode::Disable => PgWire::Disable,
            SslMode::Prefer => PgWire::Prefer,
            SslMode::Require => PgWire::Require,
            SslMode::VerifyCa => PgWire::VerifyCa,
            SslMode::VerifyFull => PgWire::VerifyFull,
        },
        ca_pem_path: tls.root_cert.clone(),
        sni_hostname: None,
        client_cert_pem_path: None,
        client_key_pem_path: None,
    }
}

fn spawn_connection_task<S, T>(connection: tokio_postgres::Connection<S, T>)
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
    T: tokio_postgres::tls::TlsStream + Unpin + Send + 'static,
{
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            tracing::debug!(target: "pg2osync::tls", "connection closed: {e}");
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn replication_config_mirrors_the_mode() {
        let tls = TlsSettings::resolve("postgres://u:p@h/db", Some("require"), None).unwrap();
        assert_eq!(
            replication_config(&tls).mode,
            pgwire_replication::SslMode::Require
        );
    }
}
