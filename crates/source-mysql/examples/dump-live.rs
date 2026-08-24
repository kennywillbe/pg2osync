//! Live MySQL binlog dump with full decoding: TableMap registry +
//! row values as JSON. Requires a mysql_native_password replication user.

use anyhow::Result;
use pg2osync_source_mysql::binlog::{
    BinlogState, parse_fde, parse_header, parse_query, parse_rotate, parse_rows, parse_table_map,
    parse_xid, rows_kind_for_type,
};
use pg2osync_source_mysql::connection::{MySqlConfig, MySqlConnection};

#[tokio::main]
async fn main() -> Result<()> {
    let cfg = MySqlConfig {
        host: "localhost".into(),
        port: 13306,
        user: "repl".into(),
        password: "replpw".into(),
        database: None,
        server_id: 424243,
    };
    let mut conn = MySqlConnection::connect(&cfg).await?;
    println!("authenticated build=B");
    eprintln!("BUILD_MARKER_B active");

    conn.negotiate_checksum().await?;
    println!("checksum negotiated");

    let mut args = std::env::args().skip(1);
    let file = args.next().unwrap_or_else(|| "mysql-bin.000001".into());
    let pos: u32 = args.next().and_then(|s| s.parse().ok()).unwrap_or(4);

    conn.send_binlog_dump(&file, pos).await?;
    println!("dumping from {file}@{pos}");

    let mut state = BinlogState {
        current_file: file.clone(),
        ..Default::default()
    };
    loop {
        let pkt =
            tokio::time::timeout(std::time::Duration::from_secs(30), conn.read_packet()).await??;
        if pkt.first() == Some(&0xFF) {
            anyhow::bail!("server error: {}", String::from_utf8_lossy(&pkt[9..]));
        }
        eprintln!(
            "PKT {}B first={:#04x}",
            pkt.len(),
            pkt.first().copied().unwrap_or(0)
        );
        let ev = &pkt[1..]; // strip OK byte
        let Some(h) = parse_header(ev) else { continue };
        eprintln!(
            "EV-IN type={} bodylen={} csum={}",
            h.event_type,
            ev.len() - 19,
            state.checksum_len
        );
        let body = &ev[19..ev.len().saturating_sub(state.checksum_len)];

        match h.event_type {
            T_FORMAT_DESCRIPTION_CONST => {
                let tail: Vec<String> = body[body.len().saturating_sub(8)..]
                    .iter()
                    .map(|b| format!("{b:02x}"))
                    .collect();
                let (hlen, clen) = parse_fde(body);
                state.checksum_len = clen;
                println!(
                    "[FDE] header_len={hlen} checksum={clen} last8={:?} bodylen={}",
                    tail,
                    body.len()
                );
            }
            T_ROTATE_CONST => {
                if let Some(rot) = parse_rotate(body) {
                    println!("[ROTATE] -> {} @ {}", rot.next_file, rot.position);
                    state.current_file = rot.next_file;
                }
            }
            T_TABLE_MAP_CONST => {
                let hx: String = body.iter().map(|b| format!("{b:02x}")).collect();
                println!("TMRAW {}B {}", body.len(), hx);
                {
                    let hx: String = body.iter().map(|b| format!("{b:02x}")).collect();
                    println!("TM-HEX {}B: {hx}", body.len());
                }
                let (tid, meta, opt) = parse_table_map(body)?;
                println!(
                    "[TABLE_MAP] id={tid} {}.{} cols={} types={:?}",
                    tid,
                    meta.schema,
                    meta.name,
                    meta.columns.iter().map(|c| c.type_code).collect::<Vec<_>>()
                );
                println!(
                    "[OPT] names={:?} pk={:?}",
                    opt.column_names, opt.pk_column_indexes
                );
                state.tables.insert(tid, meta);
            }
            ty if rows_kind_for_type(ty).is_some() => {
                let full_hex: String = ev.iter().map(|b| format!("{b:02x}")).collect();
                println!("FULL-EV {}B: {full_hex}", ev.len());
                let mut tb = [0u8; 8];
                tb[..6].copy_from_slice(&body[..6]);
                let table_id = u64::from_le_bytes(tb);
                let Some(meta) = state.tables.get(&table_id) else {
                    println!("[ROWS] unknown table id {table_id}");
                    continue;
                };
                // pass the UNSTRIPPED event body: parse_rows strips the CRC
                // itself using state.checksum_len (double-strip bug fixed)
                let raw_body = &ev[19..];
                let set = match parse_rows(h.event_type, raw_body, state.checksum_len, meta) {
                    Ok(s) => s,
                    Err(e) => {
                        eprintln!("ROWS-ERR {e}");
                        return Err(e.into());
                    }
                };
                eprintln!("ROWS-OK table={} rows={}", set.table_id, set.rows.len());
                for row in &set.rows {
                    let op = match set.kind {
                        pg2osync_source_mysql::binlog::RowsKind::Write => "INSERT",
                        pg2osync_source_mysql::binlog::RowsKind::Update => "UPDATE",
                        pg2osync_source_mysql::binlog::RowsKind::Delete => "DELETE",
                    };
                    if let Some(before) = &row.before {
                        println!("[{op} {}.{}] BEFORE {:?}", meta.schema, meta.name, before);
                    }
                    println!(
                        "[{op} {}.{}] AFTER {:?}",
                        meta.schema,
                        meta.name,
                        row.after.clone()
                    );
                }
            }
            T_XID_CONST => {
                let xid = parse_xid(body).unwrap_or(0);
                println!("[XID] commit xid={xid} pos={}", h.log_pos);
            }
            T_QUERY_CONST => {
                if let Some(q) = parse_query(body) {
                    let brief: String = q.sql.chars().take(70).collect();
                    println!("[QUERY] db={} sql={brief}", q.database);
                }
            }
            _ => {}
        }
    }
}

// local aliases so the example stays readable without importing every const
use pg2osync_source_mysql::binlog::{
    T_FORMAT_DESCRIPTION as T_FORMAT_DESCRIPTION_CONST, T_QUERY as T_QUERY_CONST,
    T_ROTATE as T_ROTATE_CONST, T_TABLE_MAP as T_TABLE_MAP_CONST, T_XID as T_XID_CONST,
};
