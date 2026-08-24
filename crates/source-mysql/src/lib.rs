//! MySQL binlog CDC source.
//!
//! Own wire-protocol transport behind this crate's boundary; event decoding in
//! `binlog` mirrors the pgoutput decoder's role for PostgreSQL.

pub mod binlog;
pub mod catalog;
pub mod connection;
pub mod packet;
pub mod runner;
