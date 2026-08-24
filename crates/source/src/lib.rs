//! PostgreSQL source: transport adapter, pgoutput decoding, slot/publication
//! management, type mapping and document construction.
//! Everything PostgreSQL-specific lives here and nowhere else.

pub mod catalog;
pub mod children;
pub mod docbuild;
pub mod pgoutput;
pub mod poll;
pub mod reconnect;
pub mod runner;
pub mod tls;
pub mod transport;
pub mod typemap;
