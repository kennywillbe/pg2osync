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

    #[error("sink rejected document {doc_id} on index {index}: {reason}")]
    DocumentRejected {
        index: String,
        doc_id: String,
        reason: String,
    },

    #[error(transparent)]
    InvalidLsn(#[from] crate::lsn::InvalidLsn),

    #[error("{0}")]
    Other(String),
}
