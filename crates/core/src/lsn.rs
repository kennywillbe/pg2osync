use std::fmt;
use std::str::FromStr;

/// A PostgreSQL Log Sequence Number.
///
/// Wire format is `X/Y` where X and Y are uppercase hex halves of a u64.
/// Ordering matches WAL position ordering, so LSNs compare correctly as u64.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Lsn(pub u64);

#[derive(Debug, Clone, thiserror::Error)]
#[error("invalid LSN string: {0:?}")]
pub struct InvalidLsn(String);

impl FromStr for Lsn {
    type Err = InvalidLsn;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (hi, lo) = s.split_once('/').ok_or_else(|| InvalidLsn(s.to_string()))?;
        let hi = u64::from_str_radix(hi, 16).map_err(|_| InvalidLsn(s.to_string()))?;
        let lo = u64::from_str_radix(lo, 16).map_err(|_| InvalidLsn(s.to_string()))?;
        Ok(Lsn((hi << 32) | lo))
    }
}

impl fmt::Display for Lsn {
    // PG's own textual form pads neither half ("0/1B4F2A8"), so round-trips
    // through FromStr produce identical strings.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:X}/{:X}", self.0 >> 32, self.0 & u64::from(u32::MAX))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrips() {
        let l: Lsn = "0/1B4F2A8".parse().unwrap();
        assert_eq!(l.0, 0x1B4_F2A8);
        assert_eq!(l.to_string(), "0/1B4F2A8");
    }

    #[test]
    fn orders_correctly() {
        assert!("0/1B4F2A8".parse::<Lsn>().unwrap() < "1/0".parse::<Lsn>().unwrap());
        assert!("0/FFFFFFFF".parse::<Lsn>().unwrap() < "0/100000000".parse::<Lsn>().unwrap());
    }

    #[test]
    fn rejects_garbage() {
        assert!("".parse::<Lsn>().is_err());
        assert!("nope".parse::<Lsn>().is_err());
        assert!("0/ZZZZ".parse::<Lsn>().is_err());
    }
}
