//! Prints decoded binlog events from a live MySQL/MariaDB server.
//!
//! A debugging aid for protocol work: it shows what the decoder sees without
//! the rest of the pipeline in the way.
//!
//! Usage:
//!   cargo run -p pg2osync-source-mysql --example dump-live -- \
//!       "mysql://repl:pass@localhost:3306/appdb" [binlog-file] [position]
//!
//! The replication user must use mysql_native_password and hold
//! REPLICATION SLAVE and REPLICATION CLIENT.

use anyhow::{Context, Result};
use pg2osync_source_mysql::binlog::{
    self, parse_fde, parse_header, parse_query, parse_rotate, parse_rows, parse_table_map,
    parse_xid, rows_kind_for_type,
};
use pg2osync_source_mysql::catalog;
use pg2osync_source_mysql::connection::{MySqlConfig, MySqlConnection};
use std::collections::HashMap;

#[tokio::main]
async fn main() -> Result<()> {
    let mut args = std::env::args().skip(1);
    let url = args
        .next()
        .context("usage: dump-live <mysql-url> [binlog-file] [position]")?;
    let url = url::Url::parse(&url)?;

    let cfg = MySqlConfig {
        host: url.host_str().unwrap_or("localhost").into(),
        port: url.port().unwrap_or(3306),
        user: url.username().into(),
        password: url.password().unwrap_or_default().into(),
        database: None,
        // high value so it cannot collide with a real replica
        server_id: 4_242_424,
        tls: pg2osync_tls::TlsSettings::resolve(url.as_str(), None, None)?,
    };

    let mut conn = MySqlConnection::connect(&cfg).await?;
    conn.negotiate_checksum().await?;

    let (file, pos) = match (args.next(), args.next()) {
        (Some(f), Some(p)) => (f, p.parse().unwrap_or(4)),
        _ => {
            let mut admin = MySqlConnection::connect(&cfg).await?;
            catalog::master_position(&mut admin).await?
        }
    };
    conn.send_binlog_dump(&file, pos).await?;
    println!("dumping {file}@{pos}");

    let mut current_file = file;
    let mut checksum_len = 4usize;
    let mut tables: HashMap<u64, binlog::TableMeta> = HashMap::new();

    loop {
        let pkt = conn.read_packet().await?;
        if pkt.first() == Some(&0xFF) {
            anyhow::bail!(
                "server error: {}",
                String::from_utf8_lossy(pkt.get(9..).unwrap_or(&[]))
            );
        }
        let ev = &pkt[1..];
        let Some(h) = parse_header(ev) else { continue };
        let body = &ev[19..ev.len().saturating_sub(checksum_len)];

        match h.event_type {
            binlog::T_FORMAT_DESCRIPTION => {
                let (header_len, clen) = parse_fde(body);
                checksum_len = clen;
                println!("[FDE] header_len={header_len} checksum_len={clen}");
            }
            binlog::T_ROTATE => {
                if let Some(rot) = parse_rotate(body) {
                    println!("[ROTATE] -> {}@{}", rot.next_file, rot.position);
                    current_file = rot.next_file;
                    tables.clear();
                }
            }
            binlog::T_QUERY => {
                if let Some(q) = parse_query(body) {
                    println!("[QUERY] {}: {}", q.database, q.sql);
                }
            }
            binlog::T_XID => {
                println!(
                    "[XID] {:?} next position {}:{}",
                    parse_xid(body),
                    current_file,
                    h.log_pos
                );
            }
            binlog::T_TABLE_MAP => {
                let (table_id, meta, opt) = parse_table_map(body)?;
                println!(
                    "[TABLE_MAP] id={table_id} {}.{} types={:?} names={:?}",
                    meta.schema,
                    meta.name,
                    meta.columns.iter().map(|c| c.type_code).collect::<Vec<_>>(),
                    opt.column_names
                );
                tables.insert(table_id, meta);
            }
            ty if rows_kind_for_type(ty).is_some() => {
                let mut id_bytes = [0u8; 8];
                id_bytes[..6].copy_from_slice(&body[..6]);
                let table_id = u64::from_le_bytes(id_bytes);
                let Some(meta) = tables.get(&table_id) else {
                    println!("[ROWS] unknown table id {table_id}");
                    continue;
                };
                let set = parse_rows(h.event_type, body, checksum_len, meta)?;
                for row in &set.rows {
                    if let Some(before) = &row.before {
                        println!(
                            "[{:?} {}.{}] before {before:?}",
                            set.kind, meta.schema, meta.name
                        );
                    }
                    if let Some(after) = &row.after {
                        println!(
                            "[{:?} {}.{}] after  {after:?}",
                            set.kind, meta.schema, meta.name
                        );
                    }
                }
            }
            other => println!("[event {other}] {} bytes", body.len()),
        }
    }
}
