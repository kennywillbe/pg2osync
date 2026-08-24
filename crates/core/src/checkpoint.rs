//! The pipeline checkpoint: where a sink records how far the source was read.
//!
//! The engine orders positions by an opaque `u64` token and never interprets
//! it, so one checkpoint document serves every source kind. `position` is the
//! source's own textual form of that token — the only value a source needs to
//! resume — and `source` tells the resuming process how to parse it.

/// Source-kind discriminator stored in the checkpoint document.
pub const SOURCE_POSTGRES: &str = "postgres";
pub const SOURCE_MYSQL: &str = "mysql";

/// Immutable identity of the stream a checkpoint belongs to.
///
/// A checkpoint written for a different stream must never be used to resume:
/// the position space of a PostgreSQL slot and a MySQL binlog are unrelated.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamId {
    /// `postgres` or `mysql`.
    pub source: String,
    /// PostgreSQL: replication slot name. MySQL: `server_id`.
    pub stream: String,
    /// PostgreSQL: publication name. MySQL: empty.
    pub publication: String,
}

/// One persisted checkpoint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Checkpoint {
    pub stream: StreamId,
    /// Ordering token: WAL LSN as u64, or MySQL binlog file index and offset
    /// packed as `(file_index << 32) | position`.
    pub token: u64,
    /// Source-specific textual position (`"0/1B4F2A8"`, `"binlog.000004:1234"`).
    pub position: String,
}
