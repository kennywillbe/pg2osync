//! MySQL binlog event parsing.
//!
//! Scope: ROW-format streams from MySQL 8.0 defaults — FORMAT_DESCRIPTION,
//! ROTATE, TABLE_MAP, WRITE/UPDATE/DELETE_ROWS (v1+v2), XID, QUERY.
//!
//! A row image does not describe itself: a string column carries no charset, so
//! `char` and `binary` share a type code and so do `text` and `blob`, and an
//! enum arrives as an ordinal with its labels nowhere. Those columns are decoded
//! against the shape the catalog resolved from the declared type, which is what
//! keeps a streamed value equal to the one the initial load read.
//!
//! Known limitations:
//! - GEOMETRY decodes as base64 blob
//! - MariaDB-specific event types (Annotate_rows etc.) are tolerated, ignored

use crate::typemap::ValueShape;
use base64::Engine as _;
use serde_json::Value;

#[derive(Debug, thiserror::Error)]
pub enum DecodeError {
    #[error("unexpected end of event payload")]
    Eof,
    #[error("rows event for unknown table_id {0}")]
    UnknownTable(u64),
    #[error("unsupported mysql column type")]
    UnsupportedType { ty: u8, col: String },
    #[error("unknown event type {0}")]
    UnknownType(u8),
}

// ---- low-level reader -------------------------------------------------------

struct R<'a> {
    pub b: &'a [u8],
    pub pos: usize,
}

impl<'a> R<'a> {
    fn new(b: &'a [u8]) -> Self {
        Self { b, pos: 0 }
    }
    fn take(&mut self, n: usize) -> Result<&'a [u8], DecodeError> {
        let end = self.pos + n;
        self.b
            .get(self.pos..end)
            .inspect(|_s| {
                self.pos = end;
            })
            .ok_or(DecodeError::Eof)
    }
    fn u8v(&mut self) -> Result<u8, DecodeError> {
        Ok(self.take(1)?[0])
    }
    fn u16v(&mut self) -> Result<u16, DecodeError> {
        let s = self.take(2)?;
        Ok(u16::from_le_bytes([s[0], s[1]]))
    }
    fn u24(&mut self) -> Result<u32, DecodeError> {
        let s = self.take(3)?;
        Ok(s[0] as u32 | (s[1] as u32) << 8 | (s[2] as u32) << 16)
    }
    fn u32v(&mut self) -> Result<u32, DecodeError> {
        let s = self.take(4)?;
        Ok(u32::from_le_bytes(s.try_into().unwrap()))
    }
    fn u48(&mut self) -> Result<u64, DecodeError> {
        let s = self.take(6)?;
        let mut b = [0u8; 8];
        b[..6].copy_from_slice(s);
        Ok(u64::from_le_bytes(b))
    }
    fn u64v(&mut self) -> Result<u64, DecodeError> {
        let s = self.take(8)?;
        Ok(u64::from_le_bytes(s.try_into().unwrap()))
    }
    fn lenenc(&mut self) -> Result<usize, DecodeError> {
        match self.u8v()? {
            0xFB => Ok(0),
            0xFC => {
                let s = self.take(2)?;
                Ok(u16::from_le_bytes([s[0], s[1]]) as usize)
            }
            0xFD => Ok(self.u24()? as usize),
            0xFE => Ok(self.u64v()? as usize),
            v => Ok(v as usize),
        }
    }
    fn rest_len(&self) -> usize {
        self.b.len() - self.pos.min(self.b.len())
    }
    fn rest(&self) -> &'a [u8] {
        &self.b[self.pos.min(self.b.len())..]
    }
}

// ---- event header -----------------------------------------------------------

pub struct EventHeader {
    pub timestamp: u32,
    pub event_type: u8,
    pub event_size: u32,
    /// Offset of the *next* event in the current binlog file: this is what a
    /// resume position must be, not the offset of this event.
    pub log_pos: u32,
    /// The server that originated the event. MariaDB puts it only here, and its
    /// GTID needs it, so a group written by a former primary keeps that
    /// primary's id rather than being relabelled by whoever streams it.
    pub server_id: u32,
}

pub const T_ROTATE: u8 = 4;
pub const T_QUERY: u8 = 2;
pub const T_FORMAT_DESCRIPTION: u8 = 15;
pub const T_XID: u8 = 16;
/// Sent when the server has nothing else to send. Its header position is how a
/// consumer learns the stream has moved on during quiet periods.
pub const T_HEARTBEAT: u8 = 27;
/// A JSON update logged as a diff rather than as a whole row image. Only ever
/// produced when the server sets `binlog_row_value_options = PARTIAL_JSON`.
pub const T_PARTIAL_UPDATE_ROWS: u8 = 39;
pub const T_TABLE_MAP: u8 = 19;
pub const T_WRITE_ROWS_V1: u8 = 20;
pub const T_UPDATE_ROWS_V1: u8 = 21;
pub const T_DELETE_ROWS_V1: u8 = 22;
pub const T_WRITE_ROWS_V2: u8 = 30;
pub const T_UPDATE_ROWS_V2: u8 = 31;
pub const T_DELETE_ROWS_V2: u8 = 32;
/// MySQL's GTID for the transaction that follows.
pub const T_GTID: u8 = 33;
/// A transaction with no GTID, which `gtid_mode = ON_PERMISSIVE` still allows.
/// Nothing can record it, so seeing one means the set is not the whole story.
pub const T_ANONYMOUS_GTID: u8 = 34;
/// MySQL 8.4's tagged GTID, an event type of its own rather than a variant of
/// [`T_GTID`]. Its set cannot be built by the untagged reader below, so it is
/// recognised only to refuse rather than to be quietly skipped.
pub const T_GTID_TAGGED: u8 = 42;
/// MariaDB's GTID, which opens a transaction group.
pub const T_MARIA_GTID: u8 = 162;

// MariaDB renumbered the v2 row events (live-verified against MariaDB 11.8):
pub const T_MARIA_WRITE_ROWS_V2: u8 = 23;
pub const T_MARIA_UPDATE_ROWS_V2: u8 = 24;
pub const T_MARIA_DELETE_ROWS_V2: u8 = 25;

/// Size of the common event header (v4 binlog format), which every event
/// starts with regardless of type.
pub const HEADER_LEN: usize = 19;

pub fn parse_header(ev: &[u8]) -> Option<EventHeader> {
    if ev.len() < HEADER_LEN {
        return None;
    }
    Some(EventHeader {
        timestamp: u32::from_le_bytes(ev[0..4].try_into().unwrap()),
        event_type: ev[4],
        // header layout: timestamp(4) type(1) server_id(4) event_size(4)
        //                log_pos(4) flags(2)
        event_size: u32::from_le_bytes(ev[9..13].try_into().unwrap()),
        log_pos: u32::from_le_bytes(ev[13..17].try_into().unwrap()),
        server_id: u32::from_le_bytes(ev[5..9].try_into().unwrap()),
    })
}

// ---- GTID -------------------------------------------------------------------

/// MySQL's GTID: the uuid and number of the transaction that follows.
///
/// Layout from the server's own decoder: flags(1), the uuid's 16 raw bytes,
/// then the number as 8 bytes. Everything after that is commit-order and
/// timestamp bookkeeping this does not need.
pub fn parse_mysql_gtid(body: &[u8]) -> Option<(String, u64)> {
    if body.len() < 25 {
        return None;
    }
    let gno = i64::from_le_bytes(body[17..25].try_into().unwrap());
    // A number at or below zero is not a transaction; treating one as a GTID
    // would put a nonsense interval in the set the server has to accept back.
    let gno = u64::try_from(gno).ok().filter(|n| *n > 0)?;
    Some((format_uuid(&body[1..17]), gno))
}

/// MariaDB's GTID: the sequence number and domain of the group that follows.
///
/// Layout from the server's own decoder: seq_no(8), domain_id(4), flags(1).
/// The server id is not in here at all — it is in the event header, which is
/// why the header carries it.
pub fn parse_maria_gtid(body: &[u8]) -> Option<(u32, u64)> {
    if body.len() < 13 {
        return None;
    }
    let seq_no = u64::from_le_bytes(body[0..8].try_into().unwrap());
    let domain = u32::from_le_bytes(body[8..12].try_into().unwrap());
    (seq_no > 0).then_some((domain, seq_no))
}

/// 16 bytes as `8-4-4-4-12`, lowercase to match what the server prints.
fn format_uuid(raw: &[u8]) -> String {
    let hex: String = raw.iter().map(|b| format!("{b:02x}")).collect();
    format!(
        "{}-{}-{}-{}-{}",
        &hex[0..8],
        &hex[8..12],
        &hex[12..16],
        &hex[16..20],
        &hex[20..32]
    )
}

// ---- FORMAT_DESCRIPTION -----------------------------------------------------

/// Returns (binlog header length, checksum length for subsequent events).
pub fn parse_fde(body_after_header: &[u8]) -> (usize, usize) {
    // FDE body layout: binlog_ver(2) server_ver(50) ts(4)
    //   -> common_header_len byte at [56]
    //   -> post-header lengths[] -> checksum_alg byte -> crc32(4) when enabled
    if body_after_header.len() < 62 {
        return (19, 4); // negotiated CRC32 default when undecodable
    }
    let header_len = body_after_header[56] as usize;
    let alg = *body_after_header
        .get(body_after_header.len() - 5)
        .unwrap_or(&1);
    let checksum_len = if alg == 1 { 4 } else { 0 };
    (header_len.max(19), checksum_len)
}

// ---- ROTATE -----------------------------------------------------------------

pub struct RotateInfo {
    pub next_file: String,
    pub position: u64,
}

pub fn parse_rotate(payload: &[u8]) -> Option<RotateInfo> {
    let mut r = R::new(payload);
    let position = r.u64v().ok()?;
    let end = payload[r.pos..]
        .iter()
        .position(|&b| b == 0)
        .unwrap_or(payload.len());
    let next_file = String::from_utf8_lossy(&payload[r.pos..end]).into_owned();
    Some(RotateInfo {
        next_file,
        position,
    })
}

// ---- XID --------------------------------------------------------------------

pub fn parse_xid(payload: &[u8]) -> Option<u64> {
    let mut r = R::new(payload);
    r.u64v().ok()
}

// ---- QUERY ------------------------------------------------------------------

pub struct QueryInfo {
    pub database: String,
    pub sql: String,
}

pub fn parse_query(payload: &[u8]) -> Option<QueryInfo> {
    let mut r = R::new(payload);
    let _thread_id = r.u32v().ok()?;
    let _exec_time = r.u32v().ok()?;
    let db_len = r.u8v().ok()? as usize;
    let _err_code = r.u16v().ok()?;
    let status_len = r.u16v().ok()? as usize;
    r.take(status_len).ok()?;
    let db = String::from_utf8_lossy(r.take(db_len).ok()?).into_owned();
    // the database name is NUL-terminated; without consuming that byte every
    // statement would begin with it and no keyword would ever match
    r.u8v().ok()?;
    let sql = String::from_utf8_lossy(r.rest()).into_owned();
    Some(QueryInfo { database: db, sql })
}

/// The table a `TRUNCATE` statement clears, if that is what this statement is.
///
/// InnoDB logs `TRUNCATE` as a statement rather than as row events, so reading
/// it out of the SQL is the only way to see it at all. Only the table-name
/// token is read; whatever follows it — a semicolon, a comment, padding — says
/// nothing about which table was cleared.
pub fn truncated_table(sql: &str, default_db: &str) -> Option<(String, String)> {
    let mut words = sql.split_whitespace();
    if !words.next()?.eq_ignore_ascii_case("TRUNCATE") {
        return None;
    }
    let mut name = words.next()?;
    // "TABLE" is optional: both TRUNCATE TABLE t and TRUNCATE t are logged
    if name.eq_ignore_ascii_case("TABLE") {
        name = words.next()?;
    }
    split_qualified_name(name, default_db)
}

/// The table a `DROP TABLE` names, for warning that its index is now stale.
pub fn dropped_table(sql: &str, default_db: &str) -> Option<(String, String)> {
    let mut words = sql.split_whitespace();
    if !words.next()?.eq_ignore_ascii_case("DROP") {
        return None;
    }
    if !words.next()?.eq_ignore_ascii_case("TABLE") {
        return None;
    }
    let mut name = words.next()?;
    for optional in ["IF", "EXISTS"] {
        if name.eq_ignore_ascii_case(optional) {
            name = words.next()?;
        }
    }
    split_qualified_name(name, default_db)
}

/// Split `db.table`, `` `db`.`table` `` or a bare name into its two parts.
fn split_qualified_name(raw: &str, default_db: &str) -> Option<(String, String)> {
    let (first, rest) = read_identifier(raw)?;
    match rest.strip_prefix('.') {
        Some(after_dot) => {
            let (table, _) = read_identifier(after_dot)?;
            (!first.is_empty() && !table.is_empty()).then_some((first, table))
        }
        None => {
            let schema = default_db.trim().to_string();
            (!schema.is_empty() && !first.is_empty()).then_some((schema, first))
        }
    }
}

/// Read one identifier and report what follows it.
///
/// Stopping at the end of the identifier is not fastidiousness: statements
/// arrive with whatever trails them, and MariaDB pads a replaced event out to
/// the length of the one it stands in for. Treating any of that as part of the
/// name silently breaks the match, and a name that fails to match looks exactly
/// like a table nobody configured.
fn read_identifier(raw: &str) -> Option<(String, &str)> {
    if let Some(quoted) = raw.strip_prefix('`') {
        let end = quoted.find('`')?;
        return Some((quoted[..end].to_string(), &quoted[end + 1..]));
    }
    let end = raw
        .find(|c: char| !(c.is_alphanumeric() || c == '_' || c == '$'))
        .unwrap_or(raw.len());
    (end > 0).then(|| (raw[..end].to_string(), &raw[end..]))
}

// ---- TABLE_MAP --------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct TableMeta {
    pub schema: String,
    pub name: String,
    pub columns: Vec<ColMeta>,
}

/// Optional metadata extracted from TABLE_MAP TLVs (MySQL 8.0.2+, requires
/// binlog_row_metadata=FULL on the server for full information).
#[derive(Debug, Clone, Default)]
pub struct OptionalMeta {
    pub column_names: Vec<String>,
    pub pk_column_indexes: Vec<usize>,
}

#[derive(Debug, Clone)]
pub struct ColMeta {
    /// Resolved from information_schema at bootstrap when available.
    pub name: String,
    pub type_code: u8,
    pub meta: Vec<u8>,
    /// Optional-metadata signedness bit (numeric columns).
    pub unsigned: bool,
    pub primary_key: bool,
    /// What the declared type says the value is, filled in from the catalog.
    ///
    /// The row image cannot say: a string column carries no charset here, so
    /// `char` and `binary` share a type code and so do `text` and `blob`, and an
    /// enum arrives as an ordinal with its labels nowhere. Without this the
    /// stream and the initial load disagree about the same row.
    pub shape: Option<crate::typemap::ValueShape>,
}

pub fn parse_table_map(payload: &[u8]) -> Result<(u64, TableMeta, OptionalMeta), DecodeError> {
    // Layout (MySQL 8.0):
    //   table_id(6) flags(2) db_len(1) db NUL tbl_len(1) table NUL
    //   ncols(lenenc) types[](ncols)
    //   meta_block_len(lenenc) metadata[](meta_block_len bytes)
    //   null_bitmap((ncols+7)/8)
    //   optional TLVs -> end of payload
    //
    // Per-column metas are walked SEQUENTIALLY inside metadata[]; their sizes
    // are implied by each column type.
    let mut r = R::new(payload);
    let table_id = r.u48()?;
    let _flags = r.u16v()?;
    let slen = r.u8v()? as usize;
    let schema = String::from_utf8_lossy(r.take(slen)?).into_owned();
    r.u8v()?;
    let tlen = r.u8v()? as usize;
    let name = String::from_utf8_lossy(r.take(tlen)?).into_owned();
    r.u8v()?;
    let ncols = r.lenenc()?;
    let types: Vec<u8> = r.take(ncols)?.to_vec();

    fn meta_size(ty: u8) -> usize {
        match ty {
            15 | 253 => 2,  // VARCHAR / VAR_STRING max-len u16
            254 => 2,       // STRING
            246 => 2,       // NEWDECIMAL precision+scale
            16 => 2,        // BIT
            17..=19 => 1,   // TIMESTAMP2/DATETIME2/TIME2 fsp
            245 => 1,       // JSON: one byte holding the length field's width
            250..=252 => 1, // blobs
            _ => 0,
        }
    }

    let meta_block_len = r.lenenc()?;
    let meta_area = r.take(meta_block_len)?.to_vec();

    let nb_bytes = ncols.div_ceil(8);
    r.take(nb_bytes)?;

    // walk per-column metas inside meta_area
    let mut mpos = 0usize;
    let mut columns: Vec<ColMeta> = Vec::with_capacity(ncols);
    for &ty in &types {
        let sz = meta_size(ty);
        let meta = meta_area.get(mpos..mpos + sz).unwrap_or(&[]).to_vec();
        mpos += sz;
        columns.push(ColMeta {
            shape: None,
            name: String::new(),
            type_code: ty,
            meta,
            unsigned: false,
            primary_key: false,
        });
    }

    // optional metadata TLVs (COLUMN_NAME=3, SIGNEDNESS=1, SIMPLE_PK=5 ...)
    let mut opt = OptionalMeta::default();
    if !r.rest().is_empty() {
        let mut tl = R::new(r.rest());
        while tl.rest_len() >= 4 {
            let ty = match tl.u16v() {
                Ok(v) => v,
                Err(_) => break,
            };
            let ln = match tl.u16v() {
                Ok(v) => v as usize,
                Err(_) => break,
            };
            let val = match tl.take(ln) {
                Ok(v) => v,
                Err(_) => break,
            };
            match ty {
                3 => {
                    opt.column_names = String::from_utf8_lossy(val)
                        .split('\x00')
                        .filter(|s| !s.is_empty())
                        .map(String::from)
                        .collect();
                }
                5 => {
                    let mut rr = R::new(val);
                    while let Ok(i) = rr.lenenc() {
                        opt.pk_column_indexes.push(i);
                    }
                }
                _ => {}
            }
        }
    }

    for (i, cm) in columns.iter_mut().enumerate() {
        if let Some(n) = opt.column_names.get(i) {
            cm.name = n.clone();
        }
        cm.primary_key = opt.pk_column_indexes.contains(&i);
    }

    Ok((
        table_id,
        TableMeta {
            schema,
            name,
            columns,
        },
        opt,
    ))
}

// ---- ROWS -------------------------------------------------------------------

pub fn rows_kind_for_type(ty: u8) -> Option<RowsKind> {
    match ty {
        T_WRITE_ROWS_V1 | T_WRITE_ROWS_V2 | T_MARIA_WRITE_ROWS_V2 => Some(RowsKind::Write),
        T_UPDATE_ROWS_V1 | T_UPDATE_ROWS_V2 | T_MARIA_UPDATE_ROWS_V2 => Some(RowsKind::Update),
        T_DELETE_ROWS_V1 | T_DELETE_ROWS_V2 | T_MARIA_DELETE_ROWS_V2 => Some(RowsKind::Delete),
        _ => None,
    }
}

fn bmp_bit(bmp: &[u8], i: usize) -> bool {
    bmp.get(i / 8).copied().unwrap_or(0) >> (i % 8) & 1 == 1
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum RowsKind {
    Write,
    Update,
    Delete,
}

pub struct RowsRow {
    pub before: Option<Vec<Option<Value>>>,
    pub after: Option<Vec<Option<Value>>>,
}

pub struct RowsRowSet {
    pub table_id: u64,
    pub kind: RowsKind,
    pub rows: Vec<RowsRow>,
}

/// `payload` is the event body with any checksum already removed: stripping
/// belongs to the reader that knows the stream's checksum length, and doing it
/// in two places once cost four bytes off the end of every row event.
pub fn parse_rows(
    ev_type: u8,
    payload: &[u8],
    meta: &TableMeta,
) -> Result<RowsRowSet, DecodeError> {
    let (kind, _is_v2) = match ev_type {
        T_WRITE_ROWS_V1 => (RowsKind::Write, false),
        T_UPDATE_ROWS_V1 => (RowsKind::Update, false),
        T_DELETE_ROWS_V1 => (RowsKind::Delete, false),
        T_WRITE_ROWS_V2 | T_MARIA_WRITE_ROWS_V2 => (RowsKind::Write, true),
        T_UPDATE_ROWS_V2 | T_MARIA_UPDATE_ROWS_V2 => (RowsKind::Update, true),
        T_DELETE_ROWS_V2 | T_MARIA_DELETE_ROWS_V2 => (RowsKind::Delete, true),
        other => return Err(DecodeError::UnknownType(other)),
    };

    let mut r = R::new(payload);
    let table_id = r.u48()?;
    let _flags = r.u16v()?;
    // MySQL v2 row events carry a 2-byte extra-data length + payload;
    // MariaDB's renumbered v2 events omit this field entirely.
    if matches!(
        ev_type,
        T_WRITE_ROWS_V2 | T_UPDATE_ROWS_V2 | T_DELETE_ROWS_V2
    ) {
        let extra = r.u16v()? as usize;
        r.take(extra.saturating_sub(2))?;
    }
    let ncols = r.lenenc()?;
    let bmp_len = ncols.div_ceil(8);
    let after_bmp = r.take(bmp_len)?.to_vec();
    let before_bmp = match kind {
        RowsKind::Update => r.take(bmp_len)?.to_vec(),
        _ => Vec::new(),
    };

    let mut rows = Vec::new();
    loop {
        // the rows section ends exactly at the event boundary; anything else
        // would decode padding into a phantom all-NULL row
        if r.rest_len() == 0 {
            break;
        }
        // trailing padding/CRC bytes can look like an extra row start; per the
        // convention used by every other connector, Eof mid-row ends cleanly
        let before = if kind == RowsKind::Update {
            match decode_image(&mut r, meta, ncols, &before_bmp) {
                Ok(img) => Some(img),
                Err(DecodeError::Eof) => break,
                Err(e) => return Err(e),
            }
        } else {
            None
        };
        let after = match decode_image(&mut r, meta, ncols, &after_bmp) {
            Ok(img) => img,
            Err(DecodeError::Eof) => break,
            Err(e) => return Err(e),
        };
        rows.push(RowsRow {
            before,
            after: Some(after),
        });
    }

    Ok(RowsRowSet {
        table_id,
        kind,
        rows,
    })
}

fn decode_image(
    r: &mut R,
    meta: &TableMeta,
    ncols: usize,
    present_bmp: &[u8],
) -> Result<Vec<Option<Value>>, DecodeError> {
    // The null bitmap covers only the columns present in this image, indexed
    // by their position among the present ones — not by column ordinal. With
    // binlog_row_image=FULL the two coincide; with a partial image they do not,
    // and using the ordinal would shift every value after the first gap.
    let present_count = (0..ncols).filter(|&i| bmp_bit(present_bmp, i)).count();
    let null_bmp = r.take(present_count.div_ceil(8))?.to_vec();
    let is_null = |nth: usize| null_bmp.get(nth / 8).copied().unwrap_or(0) >> (nth % 8) & 1 == 1;

    let mut out = Vec::with_capacity(ncols);
    let mut nth = 0usize;
    for (i, cm) in meta.columns.iter().enumerate() {
        if !bmp_bit(present_bmp, i) {
            out.push(None);
            continue;
        }
        if is_null(nth) {
            out.push(Some(Value::Null));
        } else {
            out.push(Some(decode_value_inner(
                r,
                cm.type_code,
                &cm.meta,
                cm.unsigned,
                cm.shape.as_ref(),
            )?));
        }
        nth += 1;
    }
    Ok(out)
}

fn decode_value_inner(
    r: &mut R,
    ty: u8,
    meta: &[u8],
    unsigned: bool,
    shape: Option<&ValueShape>,
) -> Result<Value, DecodeError> {
    // A string column's bytes are characters or they are not, and only the
    // declared type knows which. Without it the old default stood: strings for
    // the VARCHAR and STRING codes, base64 for the BLOB codes — which made
    // `varbinary` text and `text` base64.
    let string_like = |bytes: &[u8]| -> Value {
        match shape {
            Some(ValueShape::Bytes) => {
                str_val(base64::engine::general_purpose::STANDARD.encode(bytes))
            }
            _ => str_from_bytes(bytes),
        }
    };
    let signed_int = |r: &mut R, nbytes: usize| -> Result<Value, DecodeError> {
        let raw = r.take(nbytes)?;
        let mut buf = [0u8; 8];
        buf[..nbytes].copy_from_slice(raw);
        // sign-extend from the top byte
        if raw[nbytes - 1] & 0x80 != 0 {
            for b in buf[nbytes..].iter_mut() {
                *b = 0xFF;
            }
        }
        Ok(Value::Number(i64::from_le_bytes(buf).into()))
    };
    match ty {
        1 => {
            if unsigned {
                Ok(num(r.u8v()? as u64))
            } else {
                signed_int(r, 1)
            }
        }
        2 => {
            if unsigned {
                Ok(num(r.u16v()? as u64))
            } else {
                signed_int(r, 2)
            }
        }
        9 => {
            let v = r.u24()?;
            Ok(if unsigned {
                num(v as u64)
            } else {
                let se = (v as i32).wrapping_shl(8).wrapping_shr(8);
                num((se as i64) as u64)
            })
        }
        3 => {
            if unsigned {
                Ok(num(r.u32v()? as u64))
            } else {
                signed_int(r, 4)
            }
        }
        8 => {
            if unsigned {
                Ok(num(r.u64v()?))
            } else {
                signed_int(r, 8)
            }
        }
        13 => Ok(num(r.u8v()? as u64 + 1900)), // YEAR
        10 | 14 => {
            // DATE / NEWDATE: 3-byte packed YYYMMMDDD
            let v = r.u24()?;
            let day = v & 31;
            let month = (v >> 5) & 15;
            let year = v >> 9;
            Ok(str_val(format!("{year:04}-{month:02}-{day:02}")))
        }
        17 => {
            // TIMESTAMP2: BE unix seconds + LE fraction bytes
            let secs_raw = r.take(4)?;
            let secs = u32::from_be_bytes(secs_raw.try_into().unwrap());
            let fsp = *meta.first().unwrap_or(&0) as usize;
            let frac_bytes = fsp.div_ceil(2);
            let frac_raw = r.take(frac_bytes)?;
            let mut frac = 0u32;
            for (i, b) in frac_raw.iter().enumerate() {
                frac |= (*b as u32) << (8 * i); // little-endian
            }
            let micros = frac * 10u32.pow((6 - fsp) as u32);
            Ok(str_val(format_unix_ts(secs, micros)))
        }
        18 => {
            // DATETIME2: 5-byte big-endian packed integer + fraction bytes.
            // Packed layout (MySQL my_datetime_packed_to_binary):
            //   bits 38-22: year_month = year*13+month (17 bits)
            //   bits 21-17: day (5 bits)
            //   bits 16-12: hour (5 bits)
            //   bits 11-6:  minute (6 bits)
            //   bits 5-0:   second (6 bits)
            let fsp = *meta.first().unwrap_or(&0) as usize;
            let raw = r.take(5)?;
            let int_part = ((raw[0] as u64) << 32)
                | ((raw[1] as u64) << 24)
                | ((raw[2] as u64) << 16)
                | ((raw[3] as u64) << 8)
                | (raw[4] as u64);

            let ym = ((int_part >> 22) & 0x1_FFFF) as u32;
            let day = ((int_part >> 17) & 0x1F) as u32;
            let hour = ((int_part >> 12) & 0x1F) as u32;
            let minute = ((int_part >> 6) & 0x3F) as u32;
            let second = (int_part & 0x3F) as u32;
            let year = ym / 13;
            let month = ym % 13;

            let frac_bytes = fsp.div_ceil(2);
            let frac_raw = r.take(frac_bytes)?.to_vec();
            let mut micros = 0u64;
            for b in &frac_raw {
                micros = (micros << 8) | *b as u64;
            }
            if fsp > 0 {
                micros *= 10_u64.pow((6 - fsp) as u32);
            }
            let frac_str = if fsp > 0 {
                format!(
                    ".{:0width$}",
                    micros / 10u64.pow((6 - fsp) as u32),
                    width = fsp
                )
            } else {
                String::new()
            };
            Ok(str_val(format!(
                "{year:04}-{month:02}-{day:02} {hour:02}:{minute:02}:{second:02}{frac_str}"
            )))
        }
        19 => {
            // TIME2: BE packed with inverted-sign convention
            let fsp = *meta.first().unwrap_or(&0) as usize;
            let nbytes = 3 + fsp.div_ceil(2);
            let raw = r.take(nbytes)?.to_vec();
            let negative = raw[0] & 0x80 == 0;
            let mut v: u64 = 0;
            for b in raw.iter() {
                v = (v << 8)
                    | if negative {
                        (!b as u64) & 0xFF
                    } else {
                        *b as u64
                    };
            }
            v &= (1 << ((nbytes * 8) - 1)) - 1;
            let hours = v >> 24;
            let minutes = (v >> 16) & 0xFF;
            let seconds = (v >> 8) & 0xFF;
            let sign = if negative { "-" } else { "" };
            Ok(str_val(format!(
                "{sign}{hours:02}:{minutes:02}:{seconds:02}"
            )))
        }
        246 => {
            let precision = *meta.first().unwrap_or(&0) as usize;
            let scale = meta.get(1).copied().unwrap_or(0) as usize;
            decode_decimal(r, precision, scale)
        }
        15 | 253 => {
            let max_len = u16::from_le_bytes([
                meta.first().copied().unwrap_or(0),
                meta.get(1).copied().unwrap_or(0),
            ]);
            let len = if max_len < 256 {
                r.u8v()? as usize
            } else {
                r.u16v()? as usize
            };
            Ok(string_like(r.take(len)?))
        }
        254 => {
            // STRING: high meta byte may hide ENUM/SET/legacy VARCHAR
            let real = meta.first().copied().unwrap_or(254);
            match real {
                // ENUM carries an ordinal into the declared labels, and SET a
                // bitmask over them, lowest bit first. Both are meaningless
                // without the labels, which is why the load used to report a
                // name where the stream reported a number.
                247 => {
                    let ordinal = r.u8v()? as u64;
                    Ok(match shape {
                        Some(ValueShape::Enum(labels)) => {
                            crate::typemap::enum_label(labels, ordinal)
                        }
                        _ => num(ordinal),
                    })
                }
                248 => {
                    let nbytes = meta.get(1).copied().unwrap_or(1) as usize;
                    let mask = read_uint_n(r, nbytes)?;
                    Ok(match shape {
                        Some(ValueShape::Set(labels)) => crate::typemap::set_labels(labels, mask),
                        _ => num(mask),
                    })
                }
                _ => {
                    let len_byte = meta.get(1).copied().unwrap_or(0);
                    let len = if len_byte >= 128 {
                        r.u16v()? as usize
                    } else {
                        r.u8v()? as usize
                    };
                    Ok(string_like(r.take(len)?))
                }
            }
        }
        245 => {
            // the metadata byte says how wide the length field is; it is 4 in
            // every build seen so far, but reading it is what keeps the column
            // aligned with the next one
            let width = meta.first().copied().unwrap_or(4) as usize;
            let len = read_uint_n(r, width)? as usize;
            let bytes = r.take(len)?;
            match crate::json::decode(bytes) {
                Some(value) => Ok(value),
                None => {
                    // keeping the bytes beats inventing a value: the document
                    // stays recoverable and the log says which row to look at
                    let hex: String = bytes.iter().map(|b| format!("{b:02x}")).collect();
                    tracing::warn!(target: "pg2osync::source",
                        "could not decode a JSON value; storing it as hex ({} bytes)", len);
                    Ok(str_val(format!("__mysql_json_hex:{hex}")))
                }
            }
        }
        249..=252 => {
            let size = *meta.first().unwrap_or(&1) as usize;
            let len = read_uint_n(r, size)? as usize;
            // TEXT and BLOB share these codes; only the declared type separates
            // them, and getting it wrong made every TEXT column base64.
            Ok(string_like(r.take(len)?))
        }
        16 => {
            // BIT: the server writes meta[0] = bits % 8 and meta[1] = bits / 8,
            // so the width is meta[1] * 8 + meta[0] — not meta[0] * 256 + meta[1],
            // which read one byte for every BIT wider than eight and left the
            // reader mid-column for everything after it.
            //
            // The bits themselves are stored most significant byte first, unlike
            // every other integer in this format.
            let bits = (meta.get(1).copied().unwrap_or(0) as usize) * 8
                + (*meta.first().unwrap_or(&0) as usize);
            let nbytes = bits.div_ceil(8).max(1);
            Ok(num(crate::typemap::be_uint(r.take(nbytes)?)))
        }
        255 => {
            // GEOMETRY: blob-like
            let size = *meta.first().unwrap_or(&1) as usize;
            let len = read_uint_n(r, size)? as usize;
            Ok(str_val(
                base64::engine::general_purpose::STANDARD.encode(r.take(len)?),
            ))
        }
        0 => {
            // legacy DECIMAL: unsupported, skip as string of raw bytes
            Err(DecodeError::UnsupportedType {
                ty: 0,
                col: "decimal".into(),
            })
        }
        other => Err(DecodeError::UnsupportedType {
            ty: other,
            col: "?".into(),
        }),
    }
}

fn num(v: u64) -> Value {
    Value::Number(v.into())
}

fn str_val(s: String) -> Value {
    Value::String(s)
}

fn str_from_bytes(b: &[u8]) -> Value {
    Value::String(String::from_utf8_lossy(b).into_owned())
}

fn read_uint_n(r: &mut R, n: usize) -> Result<u64, DecodeError> {
    let n = n.min(8);
    let s = r.take(n)?;
    let mut buf = [0u8; 8];
    buf[..n].copy_from_slice(s);
    Ok(u64::from_le_bytes(buf))
}

/// Decode a packed decimal that is not part of a row event.
///
/// A DECIMAL inside a JSON document is packed identically; sharing the reader
/// is what keeps the same number from rendering two ways depending on where it
/// was found.
pub(crate) fn decode_packed_decimal(
    bytes: &[u8],
    precision: usize,
    scale: usize,
) -> Option<String> {
    let mut r = R::new(bytes);
    match decode_decimal(&mut r, precision, scale) {
        Ok(Value::String(s)) => Some(s),
        _ => None,
    }
}

fn decode_decimal(r: &mut R, precision: usize, scale: usize) -> Result<Value, DecodeError> {
    let dig2bytes = [0usize, 1, 1, 2, 2, 3, 3, 4, 4, 4];
    let intg = precision.saturating_sub(scale);
    let intg0 = intg / 9;
    let intg_first = intg - intg0 * 9;
    let frac0 = scale / 9;
    let frac_last = scale - frac0 * 9;

    let total = intg0 * 4 + dig2bytes[intg_first] + frac0 * 4 + dig2bytes[frac_last];
    let mut buf = r.take(total)?.to_vec();

    let positive = buf[0] & 0x80 != 0;
    if !positive {
        // negative numbers store the one's complement of the positive form;
        // the sign marker lives in the top bit of the first byte either way
        for b in buf.iter_mut() {
            *b = !*b;
        }
    }
    buf[0] &= 0x7F;

    let mut pos = 0usize;
    let group = |buf: &[u8], pos: &mut usize, digits: usize| -> u32 {
        let nbytes = dig2bytes[digits];
        let mut v = 0u32;
        for b in &buf[*pos..*pos + nbytes] {
            v = (v << 8) | *b as u32;
        }
        *pos += nbytes;
        v
    };

    let mut int_digits = String::new();
    if intg_first > 0 {
        int_digits.push_str(&group(&buf, &mut pos, intg_first).to_string());
    }
    for _ in 0..intg0 {
        int_digits.push_str(&format!("{:09}", group(&buf, &mut pos, 9)));
    }
    let int_digits = int_digits.trim_start_matches('0').to_string();

    let mut frac_digits = String::new();
    for _ in 0..frac0 {
        frac_digits.push_str(&format!("{:09}", group(&buf, &mut pos, 9)));
    }
    if frac_last > 0 {
        frac_digits.push_str(&format!(
            "{:0width$}",
            group(&buf, &mut pos, frac_last),
            width = frac_last
        ));
    }
    // keep the column's declared scale: trimming would make the streamed
    // value differ from the same row read during the initial load
    frac_digits.truncate(scale);

    let sign = if positive || (int_digits.is_empty() && frac_digits.is_empty()) {
        ""
    } else {
        "-"
    };
    let int_digits = if int_digits.is_empty() {
        "0"
    } else {
        &int_digits
    };

    if scale == 0 {
        Ok(Value::String(format!("{sign}{int_digits}")))
    } else {
        Ok(Value::String(format!(
            "{sign}{int_digits}.{}",
            if frac_digits.is_empty() {
                "0".repeat(scale)
            } else {
                frac_digits
            }
        )))
    }
}

fn format_unix_ts(secs: u32, micros: u32) -> String {
    let days = secs as i64 / 86_400;
    let rem = secs as i64 % 86_400;
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    let frac = if micros > 0 {
        format!(".{micros:06}")
    } else {
        String::new()
    };
    format!(
        "{y:04}-{m:02}-{d:02} {:02}:{:02}:{:02}{frac}",
        rem / 3600,
        (rem % 3600) / 60,
        rem % 60
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A realistic FDE body: 2 + 50 + 4 header fields, the common-header
    /// length at [56], then the post-header array, the checksum algorithm
    /// byte, and the CRC32 itself.
    fn fde_body(alg: u8, len: usize) -> Vec<u8> {
        let mut b = vec![0u8; len];
        b[56] = 19;
        b[len - 5] = alg;
        b
    }

    #[test]
    fn the_checksum_algorithm_is_read_from_the_unstripped_body() {
        assert_eq!(parse_fde(&fde_body(1, 100)), (19, 4));
        assert_eq!(parse_fde(&fde_body(0, 100)), (19, 0), "checksums disabled");
    }

    #[test]
    fn a_body_that_already_lost_its_checksum_reads_the_wrong_byte() {
        // the guarantee this pins down is the caller's: parse_fde must see the
        // event whole. Stripping first moves the algorithm byte out of reach
        // and the answer stops depending on what the server actually sends.
        let whole = fde_body(1, 100);
        let stripped = &whole[..whole.len() - 4];
        assert_ne!(
            parse_fde(stripped).1,
            4,
            "a pre-stripped body cannot report CRC32; runner must pass the whole event"
        );
    }

    #[test]
    fn a_query_event_yields_the_statement_without_its_separator() {
        // db_len(1) err(2) status_len(2) status db NUL sql, after the
        // thread id and exec time
        let mut body = Vec::new();
        body.extend_from_slice(&7u32.to_le_bytes()); // thread id
        body.extend_from_slice(&0u32.to_le_bytes()); // exec time
        body.push(8); // length of "sourcedb"
        body.extend_from_slice(&0u16.to_le_bytes()); // error code
        body.extend_from_slice(&0u16.to_le_bytes()); // status block length
        body.extend_from_slice(b"sourcedb");
        body.push(0);
        body.extend_from_slice(b"TRUNCATE TABLE t");

        let q = parse_query(&body).expect("parsed");
        assert_eq!(q.database, "sourcedb");
        assert_eq!(q.sql, "TRUNCATE TABLE t", "the NUL must not survive");
        assert_eq!(
            truncated_table(&q.sql, &q.database),
            Some(("sourcedb".into(), "t".into()))
        );
    }

    #[test]
    fn truncate_is_recognised_in_the_forms_the_server_logs() {
        // both observed against MySQL 8.0.46; TABLE is optional
        assert_eq!(
            truncated_table("TRUNCATE TABLE trunc_probe", "sourcedb"),
            Some(("sourcedb".into(), "trunc_probe".into()))
        );
        assert_eq!(
            truncated_table("TRUNCATE trunc_probe", "sourcedb"),
            Some(("sourcedb".into(), "trunc_probe".into()))
        );
        assert_eq!(
            truncated_table("truncate TaBLe `shop`.`users`", ""),
            Some(("shop".into(), "users".into())),
            "a qualified name needs no default database"
        );
        assert_eq!(
            truncated_table("TRUNCATE TABLE shop.users", "other"),
            Some(("shop".into(), "users".into())),
            "the statement's own qualification wins"
        );
    }

    #[test]
    fn the_name_ends_where_the_identifier_ends() {
        // the statement text is not guaranteed to end cleanly: a checksum can
        // follow it, and reading to the end would corrupt the table name
        assert_eq!(
            truncated_table("TRUNCATE trunc_probe;", "db"),
            Some(("db".into(), "trunc_probe".into()))
        );
        assert_eq!(
            truncated_table("TRUNCATE TABLE `t` /* generated by server */", "db"),
            Some(("db".into(), "t".into()))
        );
        // exactly what MySQL 8.0.46 sends: the CRC32 follows the name with no
        // separator at all
        assert_eq!(
            truncated_table("TRUNCATE TABLE shop_users\u{feff}\u{7f}p{", "sourcedb"),
            Some(("sourcedb".into(), "shop_users".into()))
        );
        assert_eq!(
            truncated_table("TRUNCATE TABLE `shop`.`users`\u{7f}\u{1}", "db"),
            Some(("shop".into(), "users".into()))
        );
    }

    #[test]
    fn other_statements_are_not_truncates() {
        assert_eq!(truncated_table("DELETE FROM t", "db"), None);
        assert_eq!(truncated_table("BEGIN", "db"), None);
        assert_eq!(truncated_table("TRUNCATE", "db"), None, "no table named");
        assert_eq!(
            truncated_table("TRUNCATE t", ""),
            None,
            "unqualified with no default database cannot be resolved"
        );
    }

    #[test]
    fn drops_are_recognised_including_the_optional_clause() {
        assert_eq!(
            dropped_table(
                "DROP TABLE `trunc_probe` /* generated by server */",
                "sourcedb"
            ),
            Some(("sourcedb".into(), "trunc_probe".into()))
        );
        assert_eq!(
            dropped_table("DROP TABLE IF EXISTS shop.users", "db"),
            Some(("shop".into(), "users".into()))
        );
        assert_eq!(dropped_table("DROP DATABASE shop", "db"), None);
        assert_eq!(dropped_table("TRUNCATE t", "db"), None);
    }

    #[test]
    fn header_parses() {
        let mut ev = vec![0u8; 19];
        ev[0..4].copy_from_slice(&1_700_000_000u32.to_le_bytes());
        ev[4] = T_QUERY;
        ev[5..9].copy_from_slice(&7u32.to_le_bytes()); // server id
        ev[9..13].copy_from_slice(&31u32.to_le_bytes()); // event size
        ev[13..17].copy_from_slice(&4305u32.to_le_bytes()); // next event position
        let h = parse_header(&ev).unwrap();
        assert_eq!(h.event_type, T_QUERY);
        assert_eq!(h.event_size, 31);
        // reading the size as the position would checkpoint mid-event and the
        // server would reject the resume with "bogus data in log event"
        assert_eq!(h.log_pos, 4305);
    }

    #[test]
    fn decimals_keep_their_declared_scale() {
        // decimal(12,2) value 8.50 must not collapse to "8.5": the initial
        // load reads the same row as "8.50" via the text protocol
        let v = decode_decimal(&mut R::new(&[0x80, 0x00, 0x00, 0x00, 0x08, 0x32]), 12, 2).unwrap();
        assert_eq!(v, Value::String("8.50".into()));
    }

    #[test]
    fn fde_reports_crc32() {
        // body up to and including the checksum-alg byte
        let mut body = vec![0u8; 58];
        body[53] = 19; // header length
        body[57] = 1; // checksum alg CRC32 (last byte)
        let (hlen, clen) = parse_fde(&body);
        assert_eq!(hlen, 19);
        assert_eq!(clen, 4);
    }

    #[test]
    fn table_map_roundtrip() {
        // synthetic TABLE_MAP for db.users(id BIGINT, email VARCHAR(100))
        let mut p = vec![];
        p.extend_from_slice(&123u64.to_le_bytes()[..6]); // table id
        p.extend_from_slice(&0u16.to_le_bytes()); // flags
        p.push(2);
        p.extend_from_slice(b"db");
        p.push(0);
        p.push(5);
        p.extend_from_slice(b"users");
        p.push(0);
        p.push(2); // 2 columns
        p.extend_from_slice(&[8, 15]); // LONGLONG, VARCHAR
        // metadata: varchar needs u16 maxlen (100), bigint none
        p.push(2u8);
        p.extend_from_slice(&100u16.to_le_bytes());
        p.push(1); // null bitmap

        let (tid, meta, _opt) = parse_table_map(&p).unwrap();
        assert_eq!(tid, 123);
        assert_eq!(meta.schema, "db");
        assert_eq!(meta.name, "users");
        assert_eq!(meta.columns.len(), 2);
        assert_eq!(meta.columns[0].type_code, 8);
        assert_eq!(meta.columns[1].type_code, 15);
    }

    #[test]
    fn write_rows_v2_decodes_insert() {
        // build a WRITE_ROWS_V2 payload matching the fixture table above:
        // columns [id=7, email='x@y.z']
        let ncols = 2usize;
        let mut p = vec![];
        p.extend_from_slice(&123u64.to_le_bytes()[..6]);
        p.extend_from_slice(&0u16.to_le_bytes()); // flags
        p.extend_from_slice(&2u16.to_le_bytes()); // extra data len (self)
        p.push(ncols as u8);
        p.push(0b11); // both columns present
        // null bitmap: no nulls
        p.push(0);
        // id: int8 LE
        p.extend_from_slice(&42i64.to_le_bytes());
        // varchar(100), maxlen<256 -> 1-byte length prefix
        p.push(5);
        p.extend_from_slice(b"x@y.z");

        let (_, meta, _opt) = parse_table_map(&{
            let mut tp = vec![];
            tp.extend_from_slice(&123u64.to_le_bytes()[..6]);
            tp.extend_from_slice(&0u16.to_le_bytes());
            tp.push(2);
            tp.extend_from_slice(b"db");
            tp.push(0);
            tp.push(5);
            tp.extend_from_slice(b"users");
            tp.push(0);
            tp.push(2);
            tp.extend_from_slice(&[8, 15]);
            tp.push(2u8); // meta block len
            tp.extend_from_slice(&100u16.to_le_bytes()); // varchar maxlen
            tp.push(1); // null bitmap
            tp
        })
        .unwrap();

        let set = parse_rows(T_WRITE_ROWS_V2, &p, &meta).unwrap();
        assert_eq!(set.kind, RowsKind::Write);
        assert_eq!(set.rows.len(), 1);
        let img = set.rows[0].after.as_ref().unwrap();
        match &img[0] {
            Some(Value::Number(n)) => assert_eq!(n.as_i64(), Some(42)),
            other => panic!("bad id {other:?}"),
        }
        match &img[1] {
            Some(Value::String(s)) => assert_eq!(s, "x@y.z"),
            other => panic!("bad email {other:?}"),
        }
    }

    /// One value out of a row image, as the stream reads it.
    fn streamed(ty: u8, meta: &[u8], shape: &ValueShape, image: &[u8]) -> Value {
        let mut r = R::new(image);
        decode_value_inner(&mut r, ty, meta, false, Some(shape)).expect("decodes")
    }

    /// The same logical value, as the initial load reads it off the text
    /// protocol.
    fn loaded(shape: &ValueShape, text: &[u8]) -> Value {
        crate::typemap::convert(shape, Some(text))
    }

    #[test]
    fn both_readers_agree_on_every_type_the_row_image_cannot_describe() {
        // The disagreement this pins is invisible to a test of either path
        // alone: the row image gives a string column no charset and an enum no
        // labels, so `text` used to arrive as base64 while the load called it a
        // string, and an enum arrived as `2` while the load called it "medium".
        let labels = |v: &[&str]| v.iter().map(|s| s.to_string()).collect::<Vec<_>>();

        // TEXT: a BLOB code with a character charset
        let shape = ValueShape::Text;
        let mut image = vec![5, 0];
        image.extend_from_slice(b"hello");
        assert_eq!(
            streamed(252, &[2], &shape, &image),
            loaded(&shape, b"hello"),
            "text is characters on both sides"
        );

        // VARBINARY: the same code family as VARCHAR, and not characters
        let shape = ValueShape::Bytes;
        let bytes = [0x00u8, 0xFF, 0x10];
        let mut image = vec![3];
        image.extend_from_slice(&bytes);
        assert_eq!(
            streamed(253, &16u16.to_le_bytes(), &shape, &image),
            loaded(&shape, &bytes),
            "bytes are base64 of the bytes, not of their text"
        );

        // BINARY: the STRING code, whose metadata hides the real type
        let mut image = vec![3];
        image.extend_from_slice(&bytes);
        assert_eq!(
            streamed(254, &[254, 16], &shape, &image),
            loaded(&shape, &bytes)
        );

        // BLOB
        let mut image = vec![3, 0];
        image.extend_from_slice(&bytes);
        assert_eq!(streamed(252, &[2], &shape, &image), loaded(&shape, &bytes));

        // ENUM: an ordinal in the image, a label in the text protocol
        let shape = ValueShape::Enum(labels(&["low", "medium", "high"]));
        assert_eq!(
            streamed(254, &[247, 0], &shape, &[2]),
            loaded(&shape, b"medium")
        );

        // SET: a bitmask in the image, the labels in the text protocol
        let shape = ValueShape::Set(labels(&["a", "b", "c"]));
        assert_eq!(
            streamed(254, &[248, 1], &shape, &[0b101]),
            loaded(&shape, b"a,c")
        );
        assert_eq!(streamed(254, &[248, 1], &shape, &[0]), loaded(&shape, b""));

        // BIT: big-endian in the image, the same bytes in the text protocol
        let shape = ValueShape::Bits;
        assert_eq!(
            streamed(16, &[0, 2], &shape, &[0x00, 0xFF]),
            loaded(&shape, &[0x00, 0xFF]),
            "a two-byte BIT is 255, not 65280"
        );
    }

    #[test]
    fn a_bit_wider_than_a_byte_consumes_both_of_them() {
        // The width is meta[1] * 8 + meta[0]. Getting it backwards read one byte
        // and left the reader inside the column, corrupting every value after
        // it — so the guard is that the reader ends where the value ends.
        let mut r = R::new(&[0x01, 0x00, 0x7B]);
        let bits = decode_value_inner(&mut r, 16, &[0, 2], false, Some(&ValueShape::Bits))
            .expect("decodes");
        assert_eq!(bits, Value::from(256));
        let next = decode_value_inner(&mut r, 1, &[], true, None).expect("decodes");
        assert_eq!(
            next,
            Value::from(123),
            "the next column starts where it should"
        );
    }

    #[test]
    fn decimal_decode_known_vector() {
        // decimal(3,2) value 0.01 -> bytes [0x80, 0x01]
        let v = decode_decimal(&mut R::new(&[0x80, 0x01]), 3, 2).unwrap();
        assert_eq!(v, Value::String("0.01".into()));
        // -0.01 -> complement bytes
        let v = decode_decimal(&mut R::new(&[0x7F, 0xFE]), 3, 2).unwrap();
        assert_eq!(v, Value::String("-0.01".into()));
    }
}
