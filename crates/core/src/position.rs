//! The opaque source position the engine orders, acknowledges and checkpoints.

use std::fmt;

/// A source position as bytes whose lexicographic order is the stream order.
///
/// The engine compares positions and never interprets them; turning a WAL LSN
/// or a binlog coordinate into bytes with that property is the source's job,
/// which is why nothing here knows how wide a position is or what it means.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Position(Vec<u8>);

#[derive(Debug, Clone, thiserror::Error)]
#[error("invalid position hex string: {0:?}")]
pub struct InvalidPosition(String);

impl Position {
    pub fn from_bytes(bytes: impl Into<Vec<u8>>) -> Self {
        Self(bytes.into())
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    /// Lowercase, two digits a byte, so the text sorts the way the bytes do.
    pub fn to_hex(&self) -> String {
        const DIGITS: &[u8; 16] = b"0123456789abcdef";
        let mut out = String::with_capacity(self.0.len() * 2);
        for byte in &self.0 {
            out.push(char::from(DIGITS[usize::from(byte >> 4)]));
            out.push(char::from(DIGITS[usize::from(byte & 0x0F)]));
        }
        out
    }

    pub fn from_hex(text: &str) -> Result<Self, InvalidPosition> {
        let invalid = || InvalidPosition(text.to_string());
        let digits = text.as_bytes();
        if !digits.len().is_multiple_of(2) {
            return Err(invalid());
        }
        let nibble = |c: u8| char::from(c).to_digit(16).ok_or_else(invalid);
        let mut bytes = Vec::with_capacity(digits.len() / 2);
        for [high, low] in digits.as_chunks::<2>().0 {
            // to_digit(16) is below 16, so the pair always fits a byte
            bytes.push(((nibble(*high)? << 4) | nibble(*low)?) as u8);
        }
        Ok(Self(bytes))
    }
}

impl fmt::Debug for Position {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Position({})", self.to_hex())
    }
}

impl fmt::Display for Position {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.to_hex())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn be(n: u64) -> Position {
        Position::from_bytes(n.to_be_bytes())
    }

    #[test]
    fn big_endian_bytes_order_like_the_integers_they_encode() {
        let ordered = [0u64, 1, 0xFF, 0x100, 0xFFFF_FFFF, 0x1_0000_0000, u64::MAX];
        for pair in ordered.windows(2) {
            assert!(be(pair[0]) < be(pair[1]), "{:#x} < {:#x}", pair[0], pair[1]);
        }
    }

    #[test]
    fn hex_round_trips_and_is_lowercase_and_fixed_width() {
        let p = be(0x1B4_F2A8);
        assert_eq!(p.to_hex(), "0000000001b4f2a8");
        assert_eq!(Position::from_hex("0000000001b4f2a8").unwrap(), p);
        assert_eq!(Position::from_hex("0000000001B4F2A8").unwrap(), p);
        assert_eq!(Position::from_hex("").unwrap().as_bytes(), b"");
    }

    #[test]
    fn hex_text_orders_like_the_bytes() {
        // the stored form has to sort the way the position does, or anything
        // reading the target directly would see a different order
        let (a, b) = (be(0xFFFF_FFFF), be(0x1_0000_0000));
        assert!(a.to_hex() < b.to_hex());
    }

    #[test]
    fn malformed_hex_is_refused() {
        assert!(Position::from_hex("abc").is_err(), "odd length");
        assert!(Position::from_hex("zz").is_err());
        assert!(Position::from_hex("0/1B4F2A8").is_err());
        assert!(Position::from_hex("+1").is_err());
    }
}
