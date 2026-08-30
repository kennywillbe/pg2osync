//! TLS settings shared by every source.
//!
//! Modes mirror libpq's `sslmode` so a connection string copied from a managed
//! provider behaves the way its documentation says it will. MySQL's own
//! vocabulary (`DISABLED`, `PREFERRED`, `REQUIRED`, `VERIFY_CA`,
//! `VERIFY_IDENTITY`) maps onto the same five levels, so one setting covers
//! both sources, and a source can never be half encrypted. The option names
//! are libpq's for MySQL too: one spelling per concept beats two aliases for
//! the same file.

mod error;
use error::Result;
pub use error::TlsError;
// through rustls rather than as a dependency of its own: rustls-pemfile, which
// used to do this, was folded into pki-types and is now unmaintained
use rustls::pki_types::pem::PemObject as _;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
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
            other => Err(TlsError::UnknownMode {
                value: other.to_string(),
            }),
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

/// TLS options as a configuration file spells them, before resolution.
///
/// A struct rather than positional arguments: the options are all
/// `Option<&str>`, and a caller that swapped two of them would weaken the
/// connection without the compiler noticing.
#[derive(Debug, Clone, Copy, Default)]
pub struct ConfiguredTls<'a> {
    pub sslmode: Option<&'a str>,
    pub sslrootcert: Option<&'a str>,
    pub sslcert: Option<&'a str>,
    pub sslkey: Option<&'a str>,
}

/// Resolved TLS settings for one source.
#[derive(Debug, Clone, Default)]
pub struct TlsSettings {
    pub mode: SslMode,
    /// PEM bundle of trusted roots; falls back to the bundled Mozilla roots.
    pub root_cert: Option<PathBuf>,
    /// PEM certificate chain presented to the server (mTLS).
    pub client_cert: Option<PathBuf>,
    /// PEM private key for `client_cert`; PKCS#8, PKCS#1 (RSA) or SEC1 (EC).
    pub client_key: Option<PathBuf>,
}

impl TlsSettings {
    /// Resolve the effective settings.
    ///
    /// Explicit configuration wins over the connection string, because a
    /// deployment that pins a mode should not be silently downgraded by a URL
    /// pasted from elsewhere.
    pub fn resolve(url: &str, configured: ConfiguredTls<'_>) -> Result<Self> {
        let mode = match configured.sslmode {
            Some(value) => SslMode::parse(value)?,
            None => match query_param(url, "sslmode") {
                Some(value) => SslMode::parse(&value)?,
                None => SslMode::default(),
            },
        };
        // per option, so a URL may carry one path and the config another
        let path_of = |value: Option<&str>, key: &str| {
            value
                .map(PathBuf::from)
                .or_else(|| query_param(url, key).map(PathBuf::from))
        };
        let root_cert = path_of(configured.sslrootcert, "sslrootcert");
        let client_cert = path_of(configured.sslcert, "sslcert");
        let client_key = path_of(configured.sslkey, "sslkey");

        if let Some(path) = &root_cert
            && !path.exists()
        {
            return Err(TlsError::missing_file("sslrootcert", path));
        }
        if root_cert.is_some() && !mode.verifies_certificate() {
            tracing::warn!(target: "pg2osync::tls",
                "a root certificate is configured but sslmode={} does not verify it",
                mode.as_str());
        }

        // both halves of a client identity, or neither: pgwire checks this too,
        // but only on the replication path and only once the socket is open
        match (&client_cert, &client_key) {
            (Some(_), None) => return Err(TlsError::HalfIdentity { half: "sslcert" }),
            (None, Some(_)) => return Err(TlsError::HalfIdentity { half: "sslkey" }),
            _ => {}
        }
        for (option, path) in [("sslcert", &client_cert), ("sslkey", &client_key)] {
            if let Some(path) = path
                && !path.exists()
            {
                return Err(TlsError::missing_file(option, path));
            }
        }
        if client_cert.is_some() && mode == SslMode::Disable {
            tracing::warn!(target: "pg2osync::tls",
                "a client certificate is configured but sslmode=disable never negotiates \
                 TLS, so no certificate can be presented");
        }

        Ok(Self {
            mode,
            root_cert,
            client_cert,
            client_key,
        })
    }

    /// Whether this connection proves its own identity with a certificate.
    pub fn presents_client_certificate(&self) -> bool {
        self.client_cert.is_some()
    }

    /// The rustls configuration this mode implies.
    pub fn client_config(&self) -> Result<rustls::ClientConfig> {
        let builder = rustls::ClientConfig::builder_with_provider(crypto_provider())
            .with_safe_default_protocol_versions()
            .map_err(|e| TlsError::rustls("cannot initialise TLS", e))?;
        if !self.mode.verifies_certificate() {
            // `require` promises encryption, not authentication: libpq accepts
            // any certificate here, and a self-signed managed instance is the
            // common reason to choose it. Presenting our own certificate is a
            // separate question, so a client certificate still applies.
            return self.client_auth(
                builder
                    .dangerous()
                    .with_custom_certificate_verifier(
                        Arc::new(danger::AcceptAnyCertificate::new()),
                    ),
            );
        }

        let roots = match &self.root_cert {
            Some(path) => load_roots(path)?,
            None => rustls::RootCertStore {
                roots: webpki_roots::TLS_SERVER_ROOTS.to_vec(),
            },
        };
        if self.mode == SslMode::VerifyCa {
            // verify-ca checks the chain but not the name, which rustls has no
            // switch for; the chain verification is delegated back to the
            // standard verifier and only the name check is dropped.
            let inner = rustls::client::WebPkiServerVerifier::builder_with_provider(
                Arc::new(roots),
                crypto_provider(),
            )
            .build()
            .map_err(|e| TlsError::certificate("cannot build certificate verifier", e))?;
            return self.client_auth(builder.dangerous().with_custom_certificate_verifier(
                Arc::new(danger::SkipHostnameCheck::new(inner)),
            ));
        }
        self.client_auth(builder.with_root_certificates(roots))
    }

    /// Finish a builder with the configured client identity, if there is one.
    fn client_auth(
        &self,
        builder: rustls::ConfigBuilder<rustls::ClientConfig, rustls::client::WantsClientCert>,
    ) -> Result<rustls::ClientConfig> {
        let (Some(cert), Some(key)) = (&self.client_cert, &self.client_key) else {
            return Ok(builder.with_no_client_auth());
        };
        builder
            .with_client_auth_cert(load_client_chain(cert)?, load_client_key(key)?)
            .map_err(|e| {
                TlsError::rustls(
                    format!("cannot use the client certificate {}", cert.display()),
                    e,
                )
            })
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
    let pem = std::fs::read(path).map_err(|e| {
        TlsError::certificate(
            format!("cannot read root certificate {}", path.display()),
            e,
        )
    })?;
    let mut store = rustls::RootCertStore::empty();
    let mut added = 0usize;
    for cert in CertificateDer::pem_slice_iter(&pem) {
        let cert = cert.map_err(|e| {
            TlsError::certificate(format!("malformed PEM in {}", path.display()), e)
        })?;
        store.add(cert).map_err(|e| {
            TlsError::certificate(
                format!("cannot trust a certificate from {}", path.display()),
                e,
            )
        })?;
        added += 1;
    }
    if added == 0 {
        return Err(TlsError::NoMaterial(format!(
            "no certificates found in {}",
            path.display()
        )));
    }
    Ok(store)
}

fn load_client_chain(path: &Path) -> Result<Vec<CertificateDer<'static>>> {
    let chain = CertificateDer::pem_file_iter(path)
        .map_err(|e| {
            TlsError::certificate(
                format!("cannot read client certificate {}", path.display()),
                e,
            )
        })?
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| TlsError::certificate(format!("malformed PEM in {}", path.display()), e))?;
    if chain.is_empty() {
        return Err(TlsError::NoMaterial(format!(
            "no certificates found in sslcert {}",
            path.display()
        )));
    }
    Ok(chain)
}

fn load_client_key(path: &Path) -> Result<PrivateKeyDer<'static>> {
    // PKCS#8, PKCS#1 and SEC1 all resolve here; an encrypted key is an
    // `ENCRYPTED PRIVATE KEY` section this iterator does not recognise, so it
    // simply yields nothing and needs saying out loud.
    PrivateKeyDer::pem_file_iter(path)
        .map_err(|e| TlsError::certificate(format!("cannot read sslkey {}", path.display()), e))?
        .next()
        .transpose()
        .map_err(|e| {
            TlsError::certificate(format!("malformed PEM in sslkey {}", path.display()), e)
        })?
        .ok_or_else(|| {
            TlsError::NoMaterial(format!(
                "no private key found in sslkey {}; it must be an unencrypted \
                 PKCS#8, RSA or EC key",
                path.display()
            ))
        })
}

fn query_param(url: &str, key: &str) -> Option<String> {
    let parsed = url::Url::parse(url).ok()?;
    parsed
        .query_pairs()
        .find(|(k, _)| k == key)
        .map(|(_, v)| v.into_owned())
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

    fn fixture(name: &str) -> String {
        format!("{}/tests/fixtures/{name}", env!("CARGO_MANIFEST_DIR"))
    }

    fn with_client_identity(mode: &str, cert: &str, key: &str) -> TlsSettings {
        let cert = fixture(cert);
        let key = fixture(key);
        TlsSettings::resolve(
            "postgres://u:p@h/db",
            ConfiguredTls {
                sslmode: Some(mode),
                sslcert: Some(&cert),
                sslkey: Some(&key),
                ..ConfiguredTls::default()
            },
        )
        .unwrap()
    }

    #[test]
    fn modes_parse_in_libpq_spelling() {
        assert_eq!(SslMode::parse("disable").unwrap(), SslMode::Disable);
        assert_eq!(SslMode::parse("verify-full").unwrap(), SslMode::VerifyFull);
        assert_eq!(SslMode::parse("VERIFY_CA").unwrap(), SslMode::VerifyCa);
        assert!(SslMode::parse("sometimes").is_err());
    }

    #[test]
    fn default_is_prefer_like_libpq() {
        let settings =
            TlsSettings::resolve("postgres://u:p@h/db", ConfiguredTls::default()).unwrap();
        assert_eq!(settings.mode, SslMode::Prefer);
        assert!(!settings.mode.requires_tls(), "prefer must allow fallback");
        assert!(!settings.presents_client_certificate());
    }

    #[test]
    fn the_url_supplies_a_mode_when_config_does_not() {
        let settings = TlsSettings::resolve(
            "postgres://u:p@h/db?sslmode=require",
            ConfiguredTls::default(),
        )
        .unwrap();
        assert_eq!(settings.mode, SslMode::Require);
    }

    #[test]
    fn explicit_config_wins_over_the_url() {
        // a URL pasted from a provider must not weaken a pinned deployment
        let settings = TlsSettings::resolve(
            "postgres://u:p@h/db?sslmode=disable",
            ConfiguredTls {
                sslmode: Some("verify-full"),
                ..ConfiguredTls::default()
            },
        )
        .unwrap();
        assert_eq!(settings.mode, SslMode::VerifyFull);
    }

    #[test]
    fn a_missing_root_certificate_is_reported_at_load_time() {
        let err = TlsSettings::resolve(
            "postgres://u:p@h/db",
            ConfiguredTls {
                sslmode: Some("verify-full"),
                sslrootcert: Some("/nonexistent/ca.pem"),
                ..ConfiguredTls::default()
            },
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

    #[test]
    fn the_url_supplies_a_client_identity_when_the_config_does_not() {
        let url = format!(
            "postgres://u:p@h/db?sslcert={}&sslkey={}",
            fixture("client.crt"),
            fixture("pkcs8.key")
        );
        let settings = TlsSettings::resolve(&url, ConfiguredTls::default()).unwrap();
        assert_eq!(settings.client_cert, Some(fixture("client.crt").into()));
        assert_eq!(settings.client_key, Some(fixture("pkcs8.key").into()));
        assert!(settings.presents_client_certificate());
    }

    #[test]
    fn the_config_wins_over_the_url_for_each_path_on_its_own() {
        let url = format!(
            "postgres://u:p@h/db?sslcert={}&sslkey={}",
            fixture("ec-client.crt"),
            fixture("pkcs8.key")
        );
        let cert = fixture("client.crt");
        let settings = TlsSettings::resolve(
            &url,
            ConfiguredTls {
                sslcert: Some(&cert),
                ..ConfiguredTls::default()
            },
        )
        .unwrap();
        assert_eq!(settings.client_cert, Some(cert.into()));
        assert_eq!(settings.client_key, Some(fixture("pkcs8.key").into()));
    }

    #[test]
    fn half_a_client_identity_is_refused() {
        let cert = fixture("client.crt");
        let err = TlsSettings::resolve(
            "postgres://u:p@h/db",
            ConfiguredTls {
                sslcert: Some(&cert),
                ..ConfiguredTls::default()
            },
        )
        .expect_err("must fail");
        assert!(err.to_string().contains("got sslcert only"), "{err}");

        let key = fixture("pkcs8.key");
        let err = TlsSettings::resolve(
            "postgres://u:p@h/db",
            ConfiguredTls {
                sslkey: Some(&key),
                ..ConfiguredTls::default()
            },
        )
        .expect_err("must fail");
        assert!(err.to_string().contains("got sslkey only"), "{err}");
    }

    #[test]
    fn a_missing_client_certificate_is_reported_at_resolve_time() {
        let key = fixture("pkcs8.key");
        let err = TlsSettings::resolve(
            "postgres://u:p@h/db",
            ConfiguredTls {
                sslcert: Some("/nonexistent/client.crt"),
                sslkey: Some(&key),
                ..ConfiguredTls::default()
            },
        )
        .expect_err("must fail");
        assert!(err.to_string().contains("sslcert"), "{err}");
        assert!(err.to_string().contains("does not exist"), "{err}");

        let cert = fixture("client.crt");
        let err = TlsSettings::resolve(
            "postgres://u:p@h/db",
            ConfiguredTls {
                sslcert: Some(&cert),
                sslkey: Some("/nonexistent/client.key"),
                ..ConfiguredTls::default()
            },
        )
        .expect_err("must fail");
        assert!(err.to_string().contains("sslkey"), "{err}");
    }

    #[test]
    fn a_client_certificate_applies_to_every_mode() {
        // `require` does not verify the server, which is no reason to stop
        // proving who we are; this is the branch the feature used to miss
        for mode in ["require", "verify-ca", "verify-full"] {
            with_client_identity(mode, "client.crt", "pkcs8.key")
                .client_config()
                .unwrap_or_else(|e| panic!("sslmode={mode}: {e}"));
        }
    }

    #[test]
    fn every_key_encoding_libpq_accepts_loads() {
        for (cert, key) in [
            ("client.crt", "pkcs8.key"),
            ("client.crt", "rsa-pkcs1.key"),
            ("ec-client.crt", "ec-sec1.key"),
            // one file holding both halves, as openssl and many tools emit it
            ("combined.pem", "combined.pem"),
        ] {
            with_client_identity("require", cert, key)
                .client_config()
                .unwrap_or_else(|e| panic!("{cert}/{key}: {e}"));
        }
    }

    #[test]
    fn an_encrypted_key_says_so() {
        let err = with_client_identity("require", "client.crt", "encrypted.key")
            .client_config()
            .expect_err("must fail");
        assert!(format!("{err:#}").contains("unencrypted"), "{err:#}");
    }
}
