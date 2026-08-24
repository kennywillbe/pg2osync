//! MySQL binlog event parsing.
//!
//! Scope: ROW-format streams from MySQL 8.0 defaults — FORMAT_DESCRIPTION,
//! ROTATE, TABLE_MAP, WRITE/UPDATE/DELETE_ROWS (v1+v2), XID, QUERY.
//!
//! Known limitations:
//! - JSON columns decode as `__mysql_json_hex:<hex>` placeholders
//! - GEOMETRY decodes as base64 blob
//! - MariaDB-specific event types (Annotate_rows etc.) are tolerated, ignored

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
}

pub const T_ROTATE: u8 = 4;
pub const T_QUERY: u8 = 2;
pub const T_FORMAT_DESCRIPTION: u8 = 15;
pub const T_XID: u8 = 16;
pub const T_TABLE_MAP: u8 = 19;
pub const T_WRITE_ROWS_V1: u8 = 20;
pub const T_UPDATE_ROWS_V1: u8 = 21;
pub const T_DELETE_ROWS_V1: u8 = 22;
pub const T_WRITE_ROWS_V2: u8 = 30;
pub const T_UPDATE_ROWS_V2: u8 = 31;
pub const T_DELETE_ROWS_V2: u8 = 32;
// MariaDB renumbered the v2 row events (live-verified against MariaDB 11.8):
pub const T_MARIA_WRITE_ROWS_V2: u8 = 23;
pub const T_MARIA_UPDATE_ROWS_V2: u8 = 24;
pub const T_MARIA_DELETE_ROWS_V2: u8 = 25;

pub fn parse_header(ev: &[u8]) -> Option<EventHeader> {
    if ev.len() < 19 {
        return None;
    }
    Some(EventHeader {
        timestamp: u32::from_le_bytes(ev[0..4].try_into().unwrap()),
        event_type: ev[4],
        // header layout: timestamp(4) type(1) server_id(4) event_size(4)
        //                log_pos(4) flags(2)
        event_size: u32::from_le_bytes(ev[9..13].try_into().unwrap()),
        log_pos: u32::from_le_bytes(ev[13..17].try_into().unwrap()),
    })
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

/// Strip trailing checksum bytes from a non-FDE event.
fn strip_checksum(ev_body: &[u8], checksum_len: usize) -> &[u8] {
    &ev_body[..ev_body.len().saturating_sub(checksum_len)]
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
    let sql = String::from_utf8_lossy(r.rest()).into_owned();
    Some(QueryInfo { database: db, sql })
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
            245 => 4,       // JSON
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

pub fn parse_rows(
    ev_type: u8,
    payload: &[u8],
    checksum_len: usize,
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

    let body = strip_checksum(payload, checksum_len);
    let mut r = R::new(body);
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
) -> Result<Value, DecodeError> {
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
            Ok(str_from_bytes(r.take(len)?))
        }
        254 => {
            // STRING: high meta byte may hide ENUM/SET/legacy VARCHAR
            let real = meta.first().copied().unwrap_or(254);
            match real {
                247 => Ok(num(r.u8v()? as u64)), // ENUM index
                248 => {
                    let nbytes = meta.get(1).copied().unwrap_or(1) as usize;
                    let v = read_uint_n(r, nbytes)?;
                    Ok(num(v))
                }
                _ => {
                    let len_byte = meta.get(1).copied().unwrap_or(0);
                    let len = if len_byte >= 128 {
                        r.u16v()? as usize
                    } else {
                        r.u8v()? as usize
                    };
                    Ok(str_from_bytes(r.take(len)?))
                }
            }
        }
        245 => {
            // JSON binary format decoding deferred (documented limitation)
            let len = r.u32v()? as usize;
            let bytes = r.take(len)?;
            let hex: String = bytes.iter().map(|b| format!("{b:02x}")).collect();
            Ok(str_val(format!("__mysql_json_hex:{hex}")))
        }
        249..=252 => {
            let size = *meta.first().unwrap_or(&1) as usize;
            let len = read_uint_n(r, size)? as usize;
            use base64::Engine as _;
            Ok(str_val(
                base64::engine::general_purpose::STANDARD.encode(r.take(len)?),
            ))
        }
        16 => {
            // BIT: meta = [bits % 8, bits / 8]
            let bits = (*meta.first().unwrap_or(&0) as usize) * 256
                + (meta.get(1).copied().unwrap_or(0) as usize);
            let nbytes = bits.div_ceil(8);
            Ok(num(read_uint_n(r, nbytes)?))
        }
        255 => {
            // GEOMETRY: blob-like
            let size = *meta.first().unwrap_or(&1) as usize;
            let len = read_uint_n(r, size)? as usize;
            use base64::Engine as _;
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

        let set = parse_rows(T_WRITE_ROWS_V2, &p, 0, &meta).unwrap();
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
