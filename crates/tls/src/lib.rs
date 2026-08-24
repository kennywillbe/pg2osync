//! TLS settings shared by every source.
//!
//! Modes mirror libpq's `sslmode` so a connection string copied from a managed
//! provider behaves the way its documentation says it will. MySQL's own
//! vocabulary (`DISABLED`, `PREFERRED`, `REQUIRED`, `VERIFY_CA`,
//! `VERIFY_IDENTITY`) maps onto the same five levels, so one setting covers
//! both sources, and a source can never be half encrypted.

use anyhow::{Context as _, Result, bail};
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// How much protection the connection must have, in libpq's vocabulary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SslMode {
    /// Never negotiate TLS.
    Disable,
    /// Use TLS when the server offers it, fall back to plaintext otherwise.
    ///
    /// libpq's default, and ours: it improves an unconfigured deployment
    /// without breaking a server that has no certificate. It is not a security
    /// guarantee — a downgrade is exactly what it permits.
    #[default]
    Prefer,
    /// Require TLS, accept any certificate.
    ///
    /// Stops passive eavesdropping, not an active man in the middle.
    Require,
    /// Require TLS and verify the certificate chain.
    VerifyCa,
    /// Require TLS, verify the chain and the hostname.
    VerifyFull,
}

impl SslMode {
    pub fn parse(value: &str) -> Result<Self> {
        match value.trim().to_ascii_lowercase().replace('_', "-").as_str() {
            "disable" => Ok(Self::Disable),
            "prefer" => Ok(Self::Prefer),
            "require" => Ok(Self::Require),
            "verify-ca" => Ok(Self::VerifyCa),
            "verify-full" => Ok(Self::VerifyFull),
            other => bail!(
                "unknown sslmode {other:?}; expected disable, prefer, require, \
                 verify-ca or verify-full"
            ),
        }
    }

    pub fn requires_tls(self) -> bool {
        !matches!(self, Self::Disable | Self::Prefer)
    }

    pub fn verifies_certificate(self) -> bool {
        matches!(self, Self::VerifyCa | Self::VerifyFull)
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Disable => "disable",
            Self::Prefer => "prefer",
            Self::Require => "require",
            Self::VerifyCa => "verify-ca",
            Self::VerifyFull => "verify-full",
        }
    }
}

/// Resolved TLS settings for one source.
#[derive(Debug, Clone, Default)]
pub struct TlsSettings {
    pub mode: SslMode,
    /// PEM bundle of trusted roots; falls back to the bundled Mozilla roots.
    pub root_cert: Option<PathBuf>,
}

impl TlsSettings {
    /// Resolve the effective settings.
    ///
    /// Explicit configuration wins over the connection string, because a
    /// deployment that pins a mode should not be silently downgraded by a URL
    /// pasted from elsewhere.
    pub fn resolve(
        url: &str,
        configured_mode: Option<&str>,
        root_cert: Option<&str>,
    ) -> Result<Self> {
        let mode = match configured_mode {
            Some(value) => SslMode::parse(value)?,
            None => match sslmode_from_url(url) {
                Some(value) => SslMode::parse(&value)?,
                None => SslMode::default(),
            },
        };
        let root_cert = root_cert
            .map(PathBuf::from)
            .or_else(|| sslrootcert_from_url(url).map(PathBuf::from));

        if let Some(path) = &root_cert
            && !path.exists()
        {
            bail!("sslrootcert {} does not exist", path.display());
        }
        if root_cert.is_some() && !mode.verifies_certificate() {
            tracing::warn!(target: "pg2osync::tls",
                "a root certificate is configured but sslmode={} does not verify it",
                mode.as_str());
        }
        Ok(Self { mode, root_cert })
    }

    /// The rustls configuration this mode implies.
    pub fn client_config(&self) -> Result<rustls::ClientConfig> {
        let builder = rustls::ClientConfig::builder_with_provider(crypto_provider())
            .with_safe_default_protocol_versions()
            .context("cannot initialise TLS")?;
        if !self.mode.verifies_certificate() {
            // `require` promises encryption, not authentication: libpq accepts
            // any certificate here, and a self-signed managed instance is the
            // common reason to choose it.
            return Ok(builder
                .dangerous()
                .with_custom_certificate_verifier(Arc::new(danger::AcceptAnyCertificate::new()))
                .with_no_client_auth());
        }

        let roots = match &self.root_cert {
            Some(path) => load_roots(path)?,
            None => rustls::RootCertStore {
                roots: webpki_roots::TLS_SERVER_ROOTS.to_vec(),
            },
        };
        let mut config = builder.with_root_certificates(roots).with_no_client_auth();
        if self.mode == SslMode::VerifyCa {
            // verify-ca checks the chain but not the name, which rustls has no
            // switch for; the chain verification is delegated back to the
            // standard verifier and only the name check is dropped.
            let inner = rustls::client::WebPkiServerVerifier::builder_with_provider(
                Arc::new(match &self.root_cert {
                    Some(path) => load_roots(path)?,
                    None => rustls::RootCertStore {
                        roots: webpki_roots::TLS_SERVER_ROOTS.to_vec(),
                    },
                }),
                crypto_provider(),
            )
            .build()
            .context("cannot build certificate verifier")?;
            config = rustls::ClientConfig::builder_with_provider(crypto_provider())
                .with_safe_default_protocol_versions()
                .context("cannot initialise TLS")?
                .dangerous()
                .with_custom_certificate_verifier(Arc::new(danger::SkipHostnameCheck::new(inner)))
                .with_no_client_auth();
        }
        Ok(config)
    }
}

/// The crypto backend, chosen explicitly.
///
/// Both `ring` and `aws-lc-rs` end up in this binary through different
/// dependencies, so rustls refuses to guess a process-wide default. Passing the
/// provider into every builder keeps the choice local and independent of what
/// any other crate installs.
fn crypto_provider() -> Arc<rustls::crypto::CryptoProvider> {
    static PROVIDER: std::sync::OnceLock<Arc<rustls::crypto::CryptoProvider>> =
        std::sync::OnceLock::new();
    PROVIDER
        .get_or_init(|| Arc::new(rustls::crypto::ring::default_provider()))
        .clone()
}

fn load_roots(path: &Path) -> Result<rustls::RootCertStore> {
    let pem = std::fs::read(path)
        .with_context(|| format!("cannot read root certificate {}", path.display()))?;
    let mut store = rustls::RootCertStore::empty();
    let mut cursor = std::io::Cursor::new(pem);
    let mut added = 0usize;
    for cert in rustls_pemfile::certs(&mut cursor) {
        let cert = cert.with_context(|| format!("malformed PEM in {}", path.display()))?;
        store
            .add(cert)
            .with_context(|| format!("cannot trust a certificate from {}", path.display()))?;
        added += 1;
    }
    if added == 0 {
        bail!("no certificates found in {}", path.display());
    }
    Ok(store)
}

fn query_param(url: &str, key: &str) -> Option<String> {
    let parsed = url::Url::parse(url).ok()?;
    parsed
        .query_pairs()
        .find(|(k, _)| k == key)
        .map(|(_, v)| v.into_owned())
}

fn sslmode_from_url(url: &str) -> Option<String> {
    query_param(url, "sslmode")
}

fn sslrootcert_from_url(url: &str) -> Option<String> {
    query_param(url, "sslrootcert")
}

/// Verifiers that deliberately relax a check, matching a libpq mode.
mod danger {
    use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
    use rustls::crypto::{verify_tls12_signature, verify_tls13_signature};
    use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
    use rustls::{DigitallySignedStruct, Error, SignatureScheme};
    use std::sync::Arc;

    /// `sslmode=require`: encrypt, but accept whatever certificate arrives.
    #[derive(Debug)]
    pub struct AcceptAnyCertificate {
        supported: rustls::crypto::WebPkiSupportedAlgorithms,
    }

    impl AcceptAnyCertificate {
        pub fn new() -> Self {
            Self {
                supported: super::crypto_provider().signature_verification_algorithms,
            }
        }
    }

    impl ServerCertVerifier for AcceptAnyCertificate {
        fn verify_server_cert(
            &self,
            _end_entity: &CertificateDer<'_>,
            _intermediates: &[CertificateDer<'_>],
            _server_name: &ServerName<'_>,
            _ocsp: &[u8],
            _now: UnixTime,
        ) -> Result<ServerCertVerified, Error> {
            Ok(ServerCertVerified::assertion())
        }

        fn verify_tls12_signature(
            &self,
            message: &[u8],
            cert: &CertificateDer<'_>,
            dss: &DigitallySignedStruct,
        ) -> Result<HandshakeSignatureValid, Error> {
            verify_tls12_signature(message, cert, dss, &self.supported)
        }

        fn verify_tls13_signature(
            &self,
            message: &[u8],
            cert: &CertificateDer<'_>,
            dss: &DigitallySignedStruct,
        ) -> Result<HandshakeSignatureValid, Error> {
            verify_tls13_signature(message, cert, dss, &self.supported)
        }

        fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
            self.supported.supported_schemes()
        }
    }

    /// `sslmode=verify-ca`: the chain is checked, the hostname is not.
    #[derive(Debug)]
    pub struct SkipHostnameCheck {
        inner: Arc<rustls::client::WebPkiServerVerifier>,
    }

    impl SkipHostnameCheck {
        pub fn new(inner: Arc<rustls::client::WebPkiServerVerifier>) -> Self {
            Self { inner }
        }
    }

    impl ServerCertVerifier for SkipHostnameCheck {
        fn verify_server_cert(
            &self,
            end_entity: &CertificateDer<'_>,
            intermediates: &[CertificateDer<'_>],
            server_name: &ServerName<'_>,
            ocsp: &[u8],
            now: UnixTime,
        ) -> Result<ServerCertVerified, Error> {
            match self
                .inner
                .verify_server_cert(end_entity, intermediates, server_name, ocsp, now)
            {
                Ok(verified) => Ok(verified),
                // the chain verified; only the name did not match, which is
                // precisely what verify-ca tolerates
                Err(Error::InvalidCertificate(rustls::CertificateError::NotValidForName))
                | Err(Error::InvalidCertificate(
                    rustls::CertificateError::NotValidForNameContext { .. },
                )) => Ok(ServerCertVerified::assertion()),
                Err(other) => Err(other),
            }
        }

        fn verify_tls12_signature(
            &self,
            message: &[u8],
            cert: &CertificateDer<'_>,
            dss: &DigitallySignedStruct,
        ) -> Result<HandshakeSignatureValid, Error> {
            self.inner.verify_tls12_signature(message, cert, dss)
        }

        fn verify_tls13_signature(
            &self,
            message: &[u8],
            cert: &CertificateDer<'_>,
            dss: &DigitallySignedStruct,
        ) -> Result<HandshakeSignatureValid, Error> {
            self.inner.verify_tls13_signature(message, cert, dss)
        }

        fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
            self.inner.supported_verify_schemes()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn modes_parse_in_libpq_spelling() {
        assert_eq!(SslMode::parse("disable").unwrap(), SslMode::Disable);
        assert_eq!(SslMode::parse("verify-full").unwrap(), SslMode::VerifyFull);
        assert_eq!(SslMode::parse("VERIFY_CA").unwrap(), SslMode::VerifyCa);
        assert!(SslMode::parse("sometimes").is_err());
    }

    #[test]
    fn default_is_prefer_like_libpq() {
        let settings = TlsSettings::resolve("postgres://u:p@h/db", None, None).unwrap();
        assert_eq!(settings.mode, SslMode::Prefer);
        assert!(!settings.mode.requires_tls(), "prefer must allow fallback");
    }

    #[test]
    fn the_url_supplies_a_mode_when_config_does_not() {
        let settings =
            TlsSettings::resolve("postgres://u:p@h/db?sslmode=require", None, None).unwrap();
        assert_eq!(settings.mode, SslMode::Require);
    }

    #[test]
    fn explicit_config_wins_over_the_url() {
        // a URL pasted from a provider must not weaken a pinned deployment
        let settings = TlsSettings::resolve(
            "postgres://u:p@h/db?sslmode=disable",
            Some("verify-full"),
            None,
        )
        .unwrap();
        assert_eq!(settings.mode, SslMode::VerifyFull);
    }

    #[test]
    fn a_missing_root_certificate_is_reported_at_load_time() {
        let err = TlsSettings::resolve(
            "postgres://u:p@h/db",
            Some("verify-full"),
            Some("/nonexistent/ca.pem"),
        )
        .expect_err("must fail");
        assert!(err.to_string().contains("does not exist"));
    }

    #[test]
    fn verification_is_only_claimed_by_the_verifying_modes() {
        assert!(!SslMode::Require.verifies_certificate());
        assert!(SslMode::VerifyCa.verifies_certificate());
        assert!(SslMode::VerifyFull.verifies_certificate());
    }
}
