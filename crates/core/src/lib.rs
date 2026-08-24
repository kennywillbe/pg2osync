//! Shared types and trait boundaries for pg2osync.
//!
//! Every other crate depends on this one; this crate depends on none of them.
//! The `Sink` trait lives here so the engine never imports the sink crate.

pub mod error;
pub mod event;
pub mod lsn;
pub mod sink;

pub use error::CoreError;
pub use lsn::Lsn;

pub use event::{ChangeEvent, RowKind, TransactionBoundary};
