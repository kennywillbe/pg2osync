//! MySQL's binary JSON format.
//!
//! A `JSON` column read during the initial load arrives as text and parses
//! straight away; the same column arriving through the binlog is stored in
//! MySQL's own binary form. Without this module the second path stored a hex
//! placeholder, so a document changed shape the first time its row was updated
//! — a silent degradation that any consumer of the field would hit.
//!
//! MariaDB does not use this format: it stores `JSON` as `LONGTEXT`.

use serde_json::{Map, Value};

// Type codes as they appear in a value entry or as a document's first byte.
const SMALL_OBJECT: u8 = 0x00;
const LARGE_OBJECT: u8 = 0x01;
const SMALL_ARRAY: u8 = 0x02;
const LARGE_ARRAY: u8 = 0x03;
const LITERAL: u8 = 0x04;
const INT16: u8 = 0x05;
const UINT16: u8 = 0x06;
const INT32: u8 = 0x07;
const UINT32: u8 = 0x08;
const INT64: u8 = 0x09;
const UINT64: u8 = 0x0a;
const DOUBLE: u8 = 0x0b;
const STRING: u8 = 0x0c;
const OPAQUE: u8 = 0x0f;

const LITERAL_NULL: u8 = 0x00;
const LITERAL_TRUE: u8 = 0x01;
const LITERAL_FALSE: u8 = 0x02;

/// Decode one binary JSON document.
///
/// `None` for anything malformed: a column that cannot be decoded is worth
/// reporting as such, never worth guessing at.
pub fn decode(doc: &[u8]) -> Option<Value> {
    // MySQL stores SQL NULL as an empty value rather than as a document
    if doc.is_empty() {
        return Some(Value::Null);
    }
    decode_value(doc[0], &doc[1..])
}

/// Decode a value of `type_code` whose bytes begin at `data`.
fn decode_value(type_code: u8, data: &[u8]) -> Option<Value> {
    match type_code {
        SMALL_OBJECT => decode_object(data, false),
        LARGE_OBJECT => decode_object(data, true),
        SMALL_ARRAY => decode_array(data, false),
        LARGE_ARRAY => decode_array(data, true),
        LITERAL => literal(*data.first()?),
        INT16 => Some(Value::from(i16::from_le_bytes(head(data)?))),
        UINT16 => Some(Value::from(u16::from_le_bytes(head(data)?))),
        INT32 => Some(Value::from(i32::from_le_bytes(head(data)?))),
        UINT32 => Some(Value::from(u32::from_le_bytes(head(data)?))),
        INT64 => Some(Value::from(i64::from_le_bytes(head(data)?))),
        UINT64 => Some(Value::from(u64::from_le_bytes(head(data)?))),
        DOUBLE => {
            let f = f64::from_le_bytes(head(data)?);
            // JSON has no way to spell a NaN or an infinity
            serde_json::Number::from_f64(f).map(Value::Number)
        }
        STRING => {
            let (len, rest) = var_len(data)?;
            Some(Value::String(
                String::from_utf8_lossy(rest.get(..len)?).into_owned(),
            ))
        }
        OPAQUE => decode_opaque(data),
        _ => None,
    }
}

fn head<const N: usize>(data: &[u8]) -> Option<[u8; N]> {
    data.get(..N)?.try_into().ok()
}

fn literal(code: u8) -> Option<Value> {
    match code {
        LITERAL_NULL => Some(Value::Null),
        LITERAL_TRUE => Some(Value::Bool(true)),
        LITERAL_FALSE => Some(Value::Bool(false)),
        _ => None,
    }
}

/// Read the length prefix of a string.
///
/// Seven bits per byte, low group first, with the high bit meaning "another
/// byte follows" — the same shape as a protobuf varint.
fn var_len(data: &[u8]) -> Option<(usize, &[u8])> {
    let mut len = 0usize;
    for (i, byte) in data.iter().enumerate().take(5) {
        len |= ((byte & 0x7f) as usize) << (7 * i);
        if byte & 0x80 == 0 {
            return Some((len, data.get(i + 1..)?));
        }
    }
    None
}

/// Read an offset or count field: two bytes in a small container, four in a
/// large one. Every offset inside a container is measured from the start of
/// that container's body — not from the start of the document, and not from
/// the type code that introduced the container.
fn field(data: &[u8], at: usize, large: bool) -> Option<usize> {
    if large {
        Some(u32::from_le_bytes(head(data.get(at..)?)?) as usize)
    } else {
        Some(u16::from_le_bytes(head(data.get(at..)?)?) as usize)
    }
}

fn decode_object(body: &[u8], large: bool) -> Option<Value> {
    let width = if large { 4 } else { 2 };
    // a key entry is an offset plus a length, and the length is two bytes wide
    // whatever the container is: a key cannot exceed what uint16 can say
    let key_entry = width + 2;
    let value_entry = width + 1;
    let count = field(body, 0, large)?;
    let mut map = Map::with_capacity(count);
    for i in 0..count {
        let key_at = width * 2 + i * key_entry;
        let key_offset = field(body, key_at, large)?;
        let key_len = u16::from_le_bytes(head(body.get(key_at + width..)?)?) as usize;
        let key =
            String::from_utf8_lossy(body.get(key_offset..key_offset + key_len)?).into_owned();

        let value_at = width * 2 + count * key_entry + i * value_entry;
        map.insert(key, entry_value(body, value_at, large)?);
    }
    Some(Value::Object(map))
}

fn decode_array(body: &[u8], large: bool) -> Option<Value> {
    let width = if large { 4 } else { 2 };
    let count = field(body, 0, large)?;
    let mut out = Vec::with_capacity(count);
    for i in 0..count {
        out.push(entry_value(body, width * 2 + i * (width + 1), large)?);
    }
    Some(Value::Array(out))
}

/// Resolve one value entry: small values live in the entry itself, larger ones
/// are an offset into the container.
fn entry_value(body: &[u8], at: usize, large: bool) -> Option<Value> {
    let type_code = *body.get(at)?;
    let inline = body.get(at + 1..)?;
    match type_code {
        LITERAL => literal(*inline.first()?),
        INT16 => Some(Value::from(i16::from_le_bytes(head(inline)?))),
        UINT16 => Some(Value::from(u16::from_le_bytes(head(inline)?))),
        // an entry in a large container is four bytes wide, so these fit; in a
        // small container they do not, and the entry holds an offset instead
        INT32 if large => Some(Value::from(i32::from_le_bytes(head(inline)?))),
        UINT32 if large => Some(Value::from(u32::from_le_bytes(head(inline)?))),
        _ => {
            let offset = field(body, at + 1, large)?;
            decode_value(type_code, body.get(offset..)?)
        }
    }
}

/// Values MySQL cannot express in JSON's own type system, wrapped with the
/// field type they came from.
///
/// They are rendered the way the row decoder renders the same types, so a
/// `DATETIME` inside a JSON document and one in its own column read alike.
fn decode_opaque(data: &[u8]) -> Option<Value> {
    let field_type = *data.first()?;
    let (len, rest) = var_len(data.get(1..)?)?;
    let body = rest.get(..len)?;
    match field_type {
        // NEWDECIMAL: precision and scale precede the packed digits
        246 => {
            let precision = *body.first()? as usize;
            let scale = *body.get(1)? as usize;
            let text = crate::binlog::decode_packed_decimal(body.get(2..)?, precision, scale)?;
            // a DECIMAL column is kept as a string so money survives a float
            // round-trip, but one *inside* a JSON document must match what the
            // initial load produces, and that path parses MySQL's JSON text —
            // where the same value is a bare number
            Some(
                text.parse::<f64>()
                    .ok()
                    .and_then(serde_json::Number::from_f64)
                    .map_or(Value::String(text), Value::Number),
            )
        }
        10 => date_from(body),
        11 => time_from(body),
        12 | 7 => datetime_from(body),
        _ => {
            use base64::Engine as _;
            Some(Value::String(
                base64::engine::general_purpose::STANDARD.encode(body),
            ))
        }
    }
}

/// MySQL packs a JSON date, time or datetime into one signed 64-bit word.
///
/// This is not the layout the same types use in a row event: there the value is
/// big-endian and field-specific, here it is `my_datetime_packed` — which is
/// why the two decoders cannot share one reader.
fn packed(body: &[u8]) -> Option<i64> {
    let mut b = [0u8; 8];
    let n = body.len().min(8);
    b[..n].copy_from_slice(body.get(..n)?);
    Some(i64::from_le_bytes(b))
}

fn parts(packed: i64) -> (i64, i64, i64, i64, i64, i64, i64) {
    let micros = packed.rem_euclid(1 << 24);
    let hms = (packed >> 24) & 0x1_ffff;
    let ymd = packed >> 41;
    let day = ymd & 0x1f;
    let ym = ymd >> 5;
    (
        ym / 13,
        ym % 13,
        day,
        hms >> 12,
        (hms >> 6) % 64,
        hms % 64,
        micros,
    )
}

fn date_from(body: &[u8]) -> Option<Value> {
    let (year, month, day, ..) = parts(packed(body)?);
    Some(Value::String(format!("{year:04}-{month:02}-{day:02}")))
}

fn time_from(body: &[u8]) -> Option<Value> {
    let (.., hour, minute, second, micros) = parts(packed(body)?);
    Some(Value::String(clock(hour, minute, second, micros)))
}

fn datetime_from(body: &[u8]) -> Option<Value> {
    let (year, month, day, hour, minute, second, micros) = parts(packed(body)?);
    Some(Value::String(format!(
        "{year:04}-{month:02}-{day:02} {}",
        clock(hour, minute, second, micros)
    )))
}

fn clock(hour: i64, minute: i64, second: i64, micros: i64) -> String {
    if micros > 0 {
        format!("{hour:02}:{minute:02}:{second:02}.{micros:06}")
    } else {
        format!("{hour:02}:{minute:02}:{second:02}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn from_hex(hex: &str) -> Vec<u8> {
        (0..hex.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).expect("hex"))
            .collect()
    }

    #[test]
    fn a_document_captured_from_a_real_binlog() {
        // {"a": 1, "b": [true, null, "x"]} as MySQL 8.0 wrote it
        let doc = from_hex(
            "00020023001200010013000100050100021400616203000f0004010004\
             00000c0d000178",
        );
        assert_eq!(decode(&doc), Some(json!({"a": 1, "b": [true, null, "x"]})));
    }

    /// Build a small object from key/value entry parts, so the layout under
    /// test is written out rather than trusted.
    fn small_object(keys: &[&str], entries: &[(u8, [u8; 2])], tail: &[u8]) -> Vec<u8> {
        let count = keys.len();
        let header = 2 + 2 + count * 4 + count * 3;
        let mut key_area = Vec::new();
        let mut key_entries = Vec::new();
        for key in keys {
            // measured from the start of the body, which is where the count is
            let offset = (header + key_area.len()) as u16;
            key_entries.extend_from_slice(&offset.to_le_bytes());
            key_entries.extend_from_slice(&(key.len() as u16).to_le_bytes());
            key_area.extend_from_slice(key.as_bytes());
        }
        let mut value_entries = Vec::new();
        for (type_code, inline) in entries {
            value_entries.push(*type_code);
            value_entries.extend_from_slice(inline);
        }
        let size = header + key_area.len() + tail.len();
        let mut out = vec![SMALL_OBJECT];
        out.extend_from_slice(&(count as u16).to_le_bytes());
        out.extend_from_slice(&(size as u16).to_le_bytes());
        out.extend_from_slice(&key_entries);
        out.extend_from_slice(&value_entries);
        out.extend_from_slice(&key_area);
        out.extend_from_slice(tail);
        out
    }

    #[test]
    fn the_three_literals_are_inlined_in_the_entry() {
        let doc = small_object(
            &["t", "f", "n"],
            &[
                (LITERAL, [LITERAL_TRUE, 0]),
                (LITERAL, [LITERAL_FALSE, 0]),
                (LITERAL, [LITERAL_NULL, 0]),
            ],
            &[],
        );
        assert_eq!(decode(&doc), Some(json!({"t": true, "f": false, "n": null})));
    }

    #[test]
    fn an_empty_document_of_each_shape() {
        assert_eq!(decode(&[SMALL_OBJECT, 0, 0, 4, 0]), Some(json!({})));
        assert_eq!(decode(&[SMALL_ARRAY, 0, 0, 4, 0]), Some(json!([])));
        // a NULL column is stored as no document at all
        assert_eq!(decode(&[]), Some(Value::Null));
    }

    #[test]
    fn a_string_length_spans_more_than_one_byte() {
        let text = "y".repeat(200);
        let mut tail = vec![0xc8, 0x01]; // 200, in seven-bit groups
        tail.extend_from_slice(text.as_bytes());
        let doc = small_object(&["k"], &[(STRING, [(2 + 2 + 4 + 3 + 1) as u8, 0])], &tail);
        assert_eq!(decode(&doc), Some(json!({ "k": text })));
    }

    #[test]
    fn seven_bit_groups_decode_low_group_first() {
        assert_eq!(var_len(&[0x05, b'x']), Some((5, &b"x"[..])));
        assert_eq!(var_len(&[0xc8, 0x01]), Some((200, &[][..])));
        assert_eq!(var_len(&[0x80, 0x80, 0x01]), Some((1 << 14, &[][..])));
    }

    #[test]
    fn a_wide_integer_lives_at_an_offset_in_a_small_container() {
        // int32 does not fit a two-byte entry, so the entry holds an offset
        let value = 70_000i32;
        let offset = (2 + 2 + 4 + 3 + 1) as u8;
        let doc = small_object(&["n"], &[(INT32, [offset, 0])], &value.to_le_bytes());
        assert_eq!(decode(&doc), Some(json!({ "n": 70_000 })));
    }

    #[test]
    fn a_truncated_document_is_refused_rather_than_guessed_at() {
        let doc = from_hex("00020023001200010013000100050100021400");
        assert_eq!(decode(&doc), None);
        assert_eq!(decode(&[SMALL_OBJECT, 9, 0]), None, "count without entries");
        assert_eq!(decode(&[0x7f]), None, "unknown type code");
    }

    /// Wrap a value in the opaque envelope: inner field type, then a length,
    /// then the value itself.
    fn opaque(field_type: u8, body: &[u8]) -> Vec<u8> {
        let mut out = vec![OPAQUE, field_type, body.len() as u8];
        out.extend_from_slice(body);
        out
    }

    #[test]
    fn dates_and_times_use_the_packed_layout_not_the_row_one() {
        // 2024-03-05 06:07:08.123456, packed the way my_datetime_packed does
        let ymd: i64 = ((2024 * 13 + 3) << 5) | 5;
        let hms: i64 = (6 << 12) | (7 << 6) | 8;
        let packed = ((ymd << 17 | hms) << 24) | 123_456;
        assert_eq!(
            decode(&opaque(12, &packed.to_le_bytes())),
            Some(json!("2024-03-05 06:07:08.123456"))
        );
        assert_eq!(
            decode(&opaque(10, &((ymd << 17) << 24).to_le_bytes())),
            Some(json!("2024-03-05"))
        );
        let time: i64 = (10i64 << 12 | 20i64 << 6 | 30) << 24;
        assert_eq!(decode(&opaque(11, &time.to_le_bytes())), Some(json!("10:20:30")));
    }

    #[test]
    fn a_decimal_inside_a_document_reads_as_a_number() {
        // the initial load parses MySQL's JSON text, where it is a bare number;
        // a DECIMAL column of its own still reads as a string
        let mut body = vec![10, 3];
        body.extend_from_slice(&[0x80, 0x00, 0x30, 0x39, 0x0a, 0x9a, 0x60]);
        let decoded = decode(&opaque(246, &body)).expect("decodes");
        assert!(decoded.is_number(), "got {decoded}");
    }

    #[test]
    fn a_double_that_json_cannot_spell_is_refused() {
        let mut doc = vec![DOUBLE];
        doc.extend_from_slice(&f64::INFINITY.to_le_bytes());
        assert_eq!(decode(&doc), None);
    }
}
