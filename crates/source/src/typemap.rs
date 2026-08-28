//! PG text-format values to JSON.
//!
//! pgoutput (protocol v1) transmits every column value as the type's text
//! representation. Conversion rules that are binding:
//! - `numeric` → JSON string (never float: precision must not be lost)
//! - unknown/custom types → JSON string via fallback
//! - `bytea` → base64 of the decoded bytes
//! - `json`/`jsonb` → parsed JSON value
//! - booleans/integer types → real JSON numbers
//! - everything temporal stays a string (PG's own textual form)

use serde_json::Value;

/// OIDs for the types we translate structurally. Everything else falls back
/// to string — including enums, domains, ranges and composites.
#[allow(dead_code)]
mod oid {
    pub const BOOL: u32 = 16;
    pub const BYTEA: u32 = 17;
    pub const INT8: u32 = 20;
    pub const INT2: u32 = 21;
    pub const INT4: u32 = 23;
    pub const OID: u32 = 26;
    pub const JSON: u32 = 114;
    pub const FLOAT4: u32 = 700;
    pub const FLOAT8: u32 = 701;
    pub const JSONB: u32 = 3802;
    pub const BOOL_ARRAY: u32 = 1000;
    pub const INT2_ARRAY: u32 = 1005;
    pub const INT4_ARRAY: u32 = 1007;
    pub const INT8_ARRAY: u32 = 1016;
    pub const JSON_ARRAY: u32 = 199;
    pub const BPCHAR_ARRAY: u32 = 1014;
    pub const TEXT_ARRAY: u32 = 1009;
    pub const VARCHAR_ARRAY: u32 = 1015;
    pub const FLOAT4_ARRAY: u32 = 1021;
    pub const FLOAT8_ARRAY: u32 = 1022;
    pub const JSONB_ARRAY: u32 = 3807;
}

/// The element type of an array type we decode structurally. Composite and
/// domain arrays keep their textual form: their elements would have to be
/// parsed against a type this map does not know.
fn element_type(array_oid: u32) -> Option<u32> {
    match array_oid {
        oid::BOOL_ARRAY => Some(oid::BOOL),
        oid::INT2_ARRAY => Some(oid::INT2),
        oid::INT4_ARRAY => Some(oid::INT4),
        oid::INT8_ARRAY => Some(oid::INT8),
        oid::FLOAT4_ARRAY => Some(oid::FLOAT4),
        oid::FLOAT8_ARRAY => Some(oid::FLOAT8),
        oid::JSON_ARRAY => Some(oid::JSON),
        oid::JSONB_ARRAY => Some(oid::JSONB),
        oid::TEXT_ARRAY | oid::VARCHAR_ARRAY | oid::BPCHAR_ARRAY => Some(0), // plain text
        _ => None,
    }
}

#[derive(Debug, thiserror::Error)]
pub enum TypeError {
    #[error("cannot parse {oid} value {value:?} as number")]
    BadNumber { oid: u32, value: String },
    #[error("malformed array literal: {0}")]
    BadArray(String),
    #[error("bytea value does not start with \\x prefix")]
    BadBytea,
}

/// Convert one column value to JSON. `raw == None` means SQL NULL.
pub fn convert(type_oid: u32, raw: Option<&[u8]>) -> Result<Value, TypeError> {
    let Some(bytes) = raw else {
        return Ok(Value::Null);
    };
    let s = std::str::from_utf8(bytes).unwrap_or_default();
    match type_oid {
        // WAL emits 't'/'f'; bool::text casts produce true/false — accept both
        // because backfill reads ::text casts while streaming reads raw output
        oid::BOOL => match s {
            "t" | "true" => Ok(Value::Bool(true)),
            "f" | "false" => Ok(Value::Bool(false)),
            other => Err(TypeError::BadNumber {
                oid: oid::BOOL,
                value: other.into(),
            }),
        },
        oid::INT2 | oid::INT4 | oid::INT8 | oid::OID => s
            .parse::<i64>()
            .map(|n| Value::Number(n.into()))
            .map_err(|_| TypeError::BadNumber {
                oid: type_oid,
                value: s.into(),
            }),
        oid::FLOAT4 | oid::FLOAT8 => s
            .parse::<f64>()
            .ok()
            .and_then(serde_json::Number::from_f64)
            .map(Value::Number)
            .ok_or(TypeError::BadNumber {
                oid: type_oid,
                value: s.into(),
            }),
        oid::JSON | oid::JSONB => {
            serde_json::from_str(s).map_err(|_| TypeError::BadArray(s.to_string()))
        }
        oid::BYTEA => decode_bytea(s),
        // pgoutput transmits arrays in their literal form (`{a,b}`); anything
        // that fans out into documents needs them as real JSON arrays, and
        // element types this map knows can be converted like scalars
        array if element_type(array).is_some() => {
            let elem = element_type(array).expect("checked by the guard");
            parse_array_literal(s)?
                .into_iter()
                .map(|e| match e {
                    None => Ok(Value::Null),
                    Some(text) if elem == 0 => Ok(Value::String(text)),
                    Some(text) => convert(elem, Some(text.as_bytes())),
                })
                .collect()
        }
        _ => Ok(Value::String(s.to_string())),
    }
}

fn decode_bytea(s: &str) -> Result<Value, TypeError> {
    let hex = s.strip_prefix("\\x").ok_or(TypeError::BadBytea)?;
    let mut out = Vec::with_capacity(hex.len() / 2);
    let bytes = hex.as_bytes();
    if bytes.len() % 2 != 0 {
        return Err(TypeError::BadBytea);
    }
    for pair in bytes.chunks(2) {
        let hi = (pair[0] as char).to_digit(16).ok_or(TypeError::BadBytea)?;
        let lo = (pair[1] as char).to_digit(16).ok_or(TypeError::BadBytea)?;
        out.push((hi * 16 + lo) as u8);
    }
    use base64::Engine as _;
    Ok(Value::String(
        base64::engine::general_purpose::STANDARD.encode(out),
    ))
}

/// Parse a PG array literal (`{1,2,3}` / `{"a b","c\"d"}`) into a JSON array.
///
/// Elements keep their textual form; callers re-run `convert` per element when
/// element type is known. NULL elements become JSON null.
pub fn parse_array_literal(lit: &str) -> Result<Vec<Option<String>>, TypeError> {
    let mut chars = lit.chars().peekable();
    if chars.next() != Some('{') {
        return Err(TypeError::BadArray(lit.into()));
    }
    if chars.peek() == Some(&'}') {
        return Ok(Vec::new());
    }
    let mut out = Vec::new();
    loop {
        let mut cur = String::new();
        let mut in_quotes = false;
        let quoted = false;
        loop {
            match chars.next() {
                None => return Err(TypeError::BadArray(lit.into())),
                Some('"') if !in_quotes => in_quotes = true,
                Some('"') if in_quotes => {
                    if chars.peek() == Some(&'"') {
                        cur.push('"');
                        chars.next();
                    } else {
                        in_quotes = false;
                    }
                }
                Some('\\') if in_quotes => {
                    if let Some(&next) = chars.peek() {
                        cur.push(next);
                        chars.next();
                    }
                }
                Some(',') if !in_quotes => break,
                Some('}') if !in_quotes => {
                    out.push(make_element(cur, quoted));
                    return Ok(out);
                }
                Some(c) => cur.push(c),
            }
        }
        out.push(make_element(cur, quoted));
    }
}

/// Unquoted NULL is a null element; everything else keeps its textual form
/// (an explicitly quoted "NULL" is the string, not SQL null).
fn make_element(cur: String, quoted: bool) -> Option<String> {
    if !quoted && cur == "NULL" {
        None
    } else {
        Some(cur)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn conv_str(oid: u32, v: &str) -> Value {
        convert(oid, Some(v.as_bytes())).unwrap()
    }

    #[test]
    fn numbers_and_bools() {
        assert_eq!(
            conv_str(oid::INT8, "9007199254740993"),
            json!("9007199254740993".parse::<i64>().unwrap())
        );
        assert_eq!(conv_str(oid::INT4, "-42"), json!(-42));
        assert_eq!(conv_str(oid::FLOAT8, "2.5"), json!(2.5));
        assert_eq!(conv_str(oid::BOOL, "t"), json!(true));
        assert_eq!(conv_str(oid::BOOL, "f"), json!(false));
    }

    #[test]
    fn numeric_stays_string_even_when_numberlike() {
        // numeric has its own OID (1700) which is NOT in our structural map,
        // so it falls through to string: precision is never lost.
        assert_eq!(
            conv_str(1700, "12345678901234567890.123456789"),
            json!("12345678901234567890.123456789")
        );
    }

    #[test]
    fn jsonb_parses_into_real_json() {
        assert_eq!(
            conv_str(oid::JSONB, r#"{"k": [1, true]}"#),
            json!({"k": [1, true]})
        );
    }

    #[test]
    fn bytea_becomes_base64() {
        // \x48656c6c6f = "Hello"
        assert_eq!(conv_str(oid::BYTEA, "\\x48656c6c6f"), json!("SGVsbG8="));
        assert!(convert(oid::BYTEA, Some(b"no-prefix")).is_err());
    }

    #[test]
    fn null_is_json_null() {
        assert_eq!(convert(25, None).unwrap(), Value::Null);
    }

    #[test]
    fn unknown_type_falls_back_to_string() {
        assert_eq!(conv_str(999999, "anything goes"), json!("anything goes"));
    }

    #[test]
    fn known_array_types_decode_to_json_arrays() {
        assert_eq!(
            conv_str(oid::TEXT_ARRAY, r#"{red,"a b",NULL}"#),
            json!(["red", "a b", null])
        );
        assert_eq!(conv_str(oid::INT8_ARRAY, "{1,2}"), json!([1, 2]));
        assert_eq!(conv_str(oid::BOOL_ARRAY, "{t,f}"), json!([true, false]));
        // jsonb[] arrives as quoted, escaped text exactly as PG writes it
        assert_eq!(
            conv_str(oid::JSONB_ARRAY, r#"{"{\"k\":1}","[1,2]"}"#),
            json!([{"k": 1}, [1, 2]])
        );
        assert_eq!(conv_str(oid::INT4_ARRAY, "{}"), json!([]));
    }

    #[test]
    fn arrays_parse_with_quoting_rules() {
        let parsed = parse_array_literal(r#"{"a b","c""d",NULL,42}"#).unwrap();
        assert_eq!(
            parsed,
            vec![
                Some("a b".into()),
                Some("c\"d".into()),
                None,
                Some("42".into())
            ]
        );
        assert_eq!(
            parse_array_literal("{}").unwrap(),
            Vec::<Option<String>>::new()
        );
        assert!(parse_array_literal("{unterminated").is_err());
    }
}
