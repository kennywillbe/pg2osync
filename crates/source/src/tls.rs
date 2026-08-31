//! PostgreSQL-specific TLS glue.
//!
//! The mode vocabulary and the rustls configuration are shared with the MySQL
//! source; what lives here is how a PostgreSQL connection and the replication
//! transport consume them.

use crate::error::{Result, SourceError};
use tokio_postgres::Client;

pub use pg2osync_tls::{ConfiguredTls, SslMode, TlsSettings};

/// Connect and spawn the connection task, with or without TLS.
pub async fn connect(tls: &TlsSettings, url: &str) -> Result<Client> {
    if tls.mode == SslMode::Disable {
        let (client, connection) = tokio_postgres::connect(url, tokio_postgres::NoTls)
            .await
            .map_err(|e| connect_failure("connection failed".into(), e))?;
        spawn_connection_task(connection);
        return Ok(client);
    }

    let mut config: tokio_postgres::Config = url
        .parse()
        .map_err(|e| SourceError::Config(format!("invalid connection url: {e}")))?;
    // tokio-postgres only distinguishes "must be encrypted" from "try";
    // certificate and hostname checking is the connector's job.
    config.ssl_mode(if tls.mode.requires_tls() {
        tokio_postgres::config::SslMode::Require
    } else {
        tokio_postgres::config::SslMode::Prefer
    });

    let connector = tokio_postgres_rustls::MakeRustlsConnect::new(tls.client_config()?);
    let (client, connection) = match config.connect(connector).await {
        Ok(pair) => pair,
        Err(e) => {
            let context = match client_certificate_hint(tls, &whole_chain(&e)) {
                Some(hint) => format!("connection failed (sslmode={}): {hint}", tls.mode.as_str()),
                None => format!("connection failed (sslmode={})", tls.mode.as_str()),
            };
            return Err(connect_failure(context, e));
        }
    };
    spawn_connection_task(connection);
    Ok(client)
}

/// Whether the server answered at all.
///
/// A `db error` carries a SQLSTATE the server itself sent, so the connection
/// got there and was turned away — a password it will not accept, a database
/// that is not there, a role without the replication attribute. Anything else
/// (refused, timed out, no such host, a handshake that never completed) never
/// reached a server that could have an opinion, which is the only case where
/// trying again later is more than a delay.
fn connect_failure(context: String, e: tokio_postgres::Error) -> SourceError {
    match e.as_db_error() {
        Some(_) => SourceError::refused(context, e),
        None => SourceError::connect(context, e),
    }
}

/// One line holding every layer of a failure.
///
/// tokio-postgres summarises a rejected login as "db error" and leaves the
/// server's own sentence in the cause, so anything that reads the reason has
/// to walk the chain rather than trust the outermost message.
fn whole_chain(error: &(dyn std::error::Error + 'static)) -> String {
    let mut out = error.to_string();
    let mut cause = error.source();
    while let Some(next) = cause {
        out.push_str(&format!(": {next}"));
        cause = next.source();
    }
    out
}

/// What to try next when the server rejected us over a client certificate.
///
/// PostgreSQL says `connection requires a valid client certificate` both when
/// none was sent and when the one sent did not satisfy `clientcert=verify-full`,
/// so the advice has to turn on what we actually presented.
fn client_certificate_hint(tls: &TlsSettings, message: &str) -> Option<String> {
    let lower = message.to_ascii_lowercase();
    if !lower.contains("client certificate") && !lower.contains("certificate authentication") {
        return None;
    }
    Some(if tls.presents_client_certificate() {
        "the client certificate was presented and rejected: check that it chains to the CA \
         in the server's ssl_ca_file and that its CN matches the database user"
            .into()
    } else {
        "the server requires a client certificate; set [source] sslcert and sslkey".into()
    })
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
        client_cert_pem_path: tls.client_cert.clone(),
        client_key_pem_path: tls.client_key.clone(),
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

    fn fixture(name: &str) -> String {
        format!(
            "{}/../tls/tests/fixtures/{name}",
            env!("CARGO_MANIFEST_DIR")
        )
    }

    #[test]
    fn replication_config_mirrors_the_mode() {
        let tls = TlsSettings::resolve(
            "postgres://u:p@h/db",
            ConfiguredTls {
                sslmode: Some("require"),
                ..ConfiguredTls::default()
            },
        )
        .unwrap();
        assert_eq!(
            replication_config(&tls).mode,
            pgwire_replication::SslMode::Require
        );
        assert!(!replication_config(&tls).is_mtls());
    }

    #[test]
    fn the_replication_stream_carries_the_client_identity() {
        let cert = fixture("client.crt");
        let key = fixture("pkcs8.key");
        let tls = TlsSettings::resolve(
            "postgres://u:p@h/db",
            ConfiguredTls {
                sslmode: Some("require"),
                sslcert: Some(&cert),
                sslkey: Some(&key),
                ..ConfiguredTls::default()
            },
        )
        .unwrap();
        assert!(replication_config(&tls).is_mtls());
    }

    #[test]
    fn the_hint_names_the_missing_options_only_when_none_was_sent() {
        let none = TlsSettings::default();
        let hint = client_certificate_hint(
            &none,
            "FATAL: connection requires a valid client certificate",
        )
        .expect("must hint");
        assert!(hint.contains("sslcert"), "{hint}");
        assert!(
            client_certificate_hint(&none, "password authentication failed").is_none(),
            "an unrelated failure must not mention certificates"
        );
    }
}
