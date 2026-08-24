//! MySQL binlog CDC source (stage 0.7).
//!
//! Own wire-protocol transport behind this crate's boundary; event decoding in
//! `binlog` mirrors the pgoutput decoder's role for PostgreSQL.

pub mod binlog;
pub mod connection;
pub mod packet;
pub mod runner;
