//! What can go wrong while resolving TLS settings or building a client config.
//!
//! Every variant names the option an operator has to change, because that is
//! the only thing they can act on: a rustls parse failure on its own says
//! nothing about which line of the config produced it. The cause travels in
//! `source` rather than in the message, so the binary prints the whole chain
//! and each layer says one thing.

use std::path::Path;

type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

#[derive(Debug, thiserror::Error)]
pub enum TlsError {
    #[error(
        "unknown sslmode {value:?}; expected disable, prefer, require, \
         verify-ca or verify-full"
    )]
    UnknownMode { value: String },

    /// A file the configuration names is not there. Named after the option
    /// rather than the path alone, so the fix is one line of the config.
    #[error("{option} {path} does not exist")]
    MissingFile { option: &'static str, path: String },

    /// Half a client identity is never a working one, and the failure it
    /// causes otherwise surfaces mid-handshake.
    #[error("sslcert and sslkey must be set together; got {half} only")]
    HalfIdentity { half: &'static str },

    /// A configured file could not be read, parsed or trusted.
    #[error("{context}")]
    Certificate {
        context: String,
        #[source]
        source: BoxError,
    },

    /// The material loaded but held no usable certificate or key, which no
    /// underlying error reports — the parser simply yields nothing.
    #[error("{0}")]
    NoMaterial(String),

    /// rustls refused to build a configuration from settings that parsed.
    #[error("{context}")]
    Rustls {
        context: String,
        #[source]
        source: rustls::Error,
    },
}

impl TlsError {
    pub(crate) fn missing_file(option: &'static str, path: &Path) -> Self {
        Self::MissingFile {
            option,
            path: path.display().to_string(),
        }
    }

    pub(crate) fn certificate(context: impl Into<String>, source: impl Into<BoxError>) -> Self {
        Self::Certificate {
            context: context.into(),
            source: source.into(),
        }
    }

    pub(crate) fn rustls(context: impl Into<String>, source: rustls::Error) -> Self {
        Self::Rustls {
            context: context.into(),
            source,
        }
    }
}

pub type Result<T, E = TlsError> = std::result::Result<T, E>;
