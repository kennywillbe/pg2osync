//! Shared error taxonomy.
//!
//! Library crates return these typed errors; only the binary converts them
//! into exit codes and human-readable diagnostics via anyhow.

#[derive(Debug, thiserror::Error)]
pub enum CoreError {
    #[error("source error: {0}")]
    Source(String),

    #[error("sink error: {0}")]
    Sink(String),

    /// A sink failure that is expected to succeed on a later attempt
    /// (HTTP 429/5xx, connection resets). Classifying at construction time
    /// keeps retry decisions out of string matching.
    #[error("transient sink error: {0}")]
    SinkTransient(String),

    #[error(transparent)]
    InvalidLsn(#[from] crate::lsn::InvalidLsn),

    #[error("{0}")]
    Other(String),
}
