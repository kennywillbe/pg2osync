//! What a MySQL column's value *is*, decided once from the declared type and
//! consulted by both readers.
//!
//! The two paths read different wire formats — the text protocol for the initial
//! load, row images for the stream — and neither format is self-describing where
//! it matters. The binlog gives a string column no charset, so `char` and
//! `binary` arrive as the same type code, as do `text` and `blob`; it gives an
//! enum an ordinal and a set a bitmask, with the labels nowhere. The text
//! protocol has the opposite gap: every value is bytes, and only the declared
//! type says whether those bytes are characters.
//!
//! So the shape is resolved from `information_schema` once and both decoders ask
//! for it. That is the only thing that keeps a resnapshot from changing a value
//! a streamed change had already written, and vice versa.

use base64::Engine as _;
use serde_json::Value;

/// How one column's bytes become JSON.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ValueShape {
    Int,
    Float,
    /// Stays a string: a float round-trip loses precision, and these columns are
    /// usually money.
    Decimal,
    /// Characters. Safe to decode as UTF-8 whichever path it came from: the
    /// connection negotiates utf8mb4 so the server transcodes for the text
    /// protocol, and a `text` column's row image is already in its own charset.
    Text,
    /// Base64. Binary cannot go into JSON any other way.
    Bytes,
    /// A number. MySQL caps `bit` at 64 bits, so this is lossless, and it is
    /// searchable where base64 would not be.
    Bits,
    Json,
    /// The declared labels. The binlog carries a 1-based ordinal into this list.
    Enum(Vec<String>),
    /// The declared labels. The binlog carries a bitmask over this list, lowest
    /// bit first.
    Set(Vec<String>),
}

/// Classify a column from `information_schema.columns`.
///
/// `data_type` is the bare type (`varchar`, `enum`); `column_type` is the full
/// declaration (`enum('low','high')`), which is where the labels live.
pub fn shape_of(data_type: &str, column_type: &str) -> ValueShape {
    match data_type {
        "tinyint" | "smallint" | "mediumint" | "int" | "integer" | "bigint" | "year" => {
            ValueShape::Int
        }
        "float" | "double" => ValueShape::Float,
        "decimal" | "numeric" => ValueShape::Decimal,
        "json" => ValueShape::Json,
        "bit" => ValueShape::Bits,
        "enum" => ValueShape::Enum(labels(column_type)),
        "set" => ValueShape::Set(labels(column_type)),
        // Every spatial type is a blob on the wire, and `data_type` names the
        // subtype rather than `geometry` for a column declared as one.
        "binary" | "varbinary" | "tinyblob" | "blob" | "mediumblob" | "longblob" | "geometry"
        | "point" | "linestring" | "polygon" | "multipoint" | "multilinestring"
        | "multipolygon" | "geomcollection" | "geometrycollection" => ValueShape::Bytes,
        // char, varchar, the text family, dates and times, and anything we do
        // not recognise: passed through as text rather than guessed at.
        _ => ValueShape::Text,
    }
}

/// The labels out of `enum('a','b')` or `set('a','b')`.
///
/// MySQL renders an embedded quote by doubling it, so `'it''s'` is one label.
fn labels(column_type: &str) -> Vec<String> {
    let Some(open) = column_type.find('(') else {
        return Vec::new();
    };
    let inner = column_type[open + 1..].trim_end_matches(')');
    let mut out = Vec::new();
    let mut chars = inner.chars().peekable();
    while let Some(c) = chars.next() {
        if c != '\'' {
            continue;
        }
        let mut label = String::new();
        while let Some(c) = chars.next() {
            if c != '\'' {
                label.push(c);
            } else if chars.peek() == Some(&'\'') {
                chars.next();
                label.push('\'');
            } else {
                break;
            }
        }
        out.push(label);
    }
    out
}

/// One text-protocol value as JSON.
pub fn convert(shape: &ValueShape, raw: Option<&[u8]>) -> Value {
    let Some(raw) = raw else { return Value::Null };
    match shape {
        ValueShape::Int => text(raw)
            .parse::<i64>()
            .map(|n| Value::Number(n.into()))
            .unwrap_or_else(|_| Value::String(text(raw))),
        ValueShape::Float => text(raw)
            .parse::<f64>()
            .ok()
            .and_then(serde_json::Number::from_f64)
            .map(Value::Number)
            .unwrap_or_else(|| Value::String(text(raw))),
        ValueShape::Json => {
            serde_json::from_slice(raw).unwrap_or_else(|_| Value::String(text(raw)))
        }
        ValueShape::Bytes => Value::String(base64::engine::general_purpose::STANDARD.encode(raw)),
        // The text protocol sends the bits as bytes, most significant first.
        ValueShape::Bits => Value::Number(be_uint(raw).into()),
        // Already the label, and already the labels joined by commas.
        ValueShape::Enum(_) => Value::String(text(raw)),
        ValueShape::Set(_) => Value::Array(
            text(raw)
                .split(',')
                .filter(|s| !s.is_empty())
                .map(|s| Value::String(s.to_string()))
                .collect(),
        ),
        ValueShape::Decimal | ValueShape::Text => Value::String(text(raw)),
    }
}

/// The label an enum ordinal names. `0` is MySQL's empty-string error value.
pub fn enum_label(labels: &[String], ordinal: u64) -> Value {
    match ordinal.checked_sub(1).and_then(|i| labels.get(i as usize)) {
        Some(label) => Value::String(label.clone()),
        None => Value::String(String::new()),
    }
}

/// The labels a set bitmask names, lowest bit first.
pub fn set_labels(labels: &[String], mask: u64) -> Value {
    Value::Array(
        labels
            .iter()
            .enumerate()
            .filter(|(i, _)| *i < 64 && mask & (1u64 << i) != 0)
            .map(|(_, label)| Value::String(label.clone()))
            .collect(),
    )
}

/// Bytes as an unsigned integer, most significant byte first.
pub fn be_uint(bytes: &[u8]) -> u64 {
    bytes
        .iter()
        .take(8)
        .fold(0u64, |acc, b| (acc << 8) | u64::from(*b))
}

fn text(raw: &[u8]) -> String {
    String::from_utf8_lossy(raw).into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_binary_column_is_bytes_and_a_text_column_is_not() {
        assert_eq!(shape_of("varbinary", "varbinary(16)"), ValueShape::Bytes);
        assert_eq!(shape_of("blob", "blob"), ValueShape::Bytes);
        assert_eq!(shape_of("point", "point"), ValueShape::Bytes);
        assert_eq!(shape_of("text", "text"), ValueShape::Text);
        assert_eq!(shape_of("varchar", "varchar(10)"), ValueShape::Text);
        assert_eq!(shape_of("datetime", "datetime"), ValueShape::Text);
    }

    #[test]
    fn labels_survive_a_quote_and_a_comma() {
        assert_eq!(
            labels("enum('low','it''s high','a,b')"),
            vec![
                "low".to_string(),
                "it's high".to_string(),
                "a,b".to_string()
            ]
        );
        assert!(labels("text").is_empty());
    }

    #[test]
    fn bytes_are_base64_of_what_arrived_not_of_its_text() {
        // 0xFF is not valid UTF-8; going through a String first replaces it
        let value = convert(&ValueShape::Bytes, Some(&[0x00, 0xFF, 0x10]));
        assert_eq!(value, Value::String("AP8Q".into()));
    }

    #[test]
    fn bits_read_most_significant_byte_first() {
        // the wire order is big-endian, so 0x00FF is 255 and not 65280
        assert_eq!(
            convert(&ValueShape::Bits, Some(&[0x00, 0xFF])),
            Value::from(255)
        );
        assert_eq!(be_uint(&[0x01, 0x00]), 256);
    }

    #[test]
    fn a_set_is_its_labels_whichever_side_it_came_from() {
        let labels = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let shape = ValueShape::Set(labels.clone());
        // text protocol: the labels, comma separated
        assert_eq!(convert(&shape, Some(b"a,c")), set_labels(&labels, 0b101));
        assert_eq!(convert(&shape, Some(b"")), set_labels(&labels, 0));
        assert_eq!(set_labels(&labels, 0), Value::Array(vec![]));
    }

    #[test]
    fn an_enum_is_its_label_whichever_side_it_came_from() {
        let labels = vec!["low".to_string(), "medium".to_string()];
        let shape = ValueShape::Enum(labels.clone());
        assert_eq!(convert(&shape, Some(b"medium")), enum_label(&labels, 2));
        // MySQL's out-of-range marker
        assert_eq!(enum_label(&labels, 0), Value::String(String::new()));
    }

    #[test]
    fn decimals_keep_their_precision() {
        assert_eq!(
            convert(&ValueShape::Decimal, Some(b"12345678901234567890.123")),
            Value::String("12345678901234567890.123".into())
        );
        assert_eq!(
            convert(&ValueShape::Int, Some(b"-7")),
            Value::Number((-7).into())
        );
        assert_eq!(
            convert(&ValueShape::Json, Some(br#"{"a":1}"#))["a"],
            Value::from(1)
        );
        assert_eq!(convert(&ValueShape::Text, None), Value::Null);
    }
}
