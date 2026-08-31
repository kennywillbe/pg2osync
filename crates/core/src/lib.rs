//! Shared types and trait boundaries for pg2osync.
//!
//! Every other crate depends on this one; this crate depends on none of them.
//! The `Sink` trait lives here so the engine never imports the sink crate.

pub mod aggregate;
pub mod checkpoint;
pub mod children;
pub mod error;
pub mod event;
pub mod filter;
pub mod load;
pub mod lsn;
pub mod sink;
/// The conformance suite a sink implementation runs against a live target.
///
/// Behind a feature so it is a test dependency rather than something a release
/// binary carries: nothing in the pipeline calls it.
#[cfg(feature = "testkit")]
pub mod testkit;

pub use checkpoint::{Checkpoint, StreamId};
pub use error::CoreError;
pub use lsn::Lsn;

pub use event::{ChangeEvent, RowKind, TransactionBoundary};
