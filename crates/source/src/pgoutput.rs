//! In-house pgoutput decoder.
//!
//! Parses PostgreSQL logical replication messages (pgoutput, protocol v1) from
//! raw `XLogData` payload bytes. This is the project's core value — it must not
//! depend on anything outside `std`.

use std::fmt;

#[derive(Debug)]
pub enum ParseError {
    UnexpectedEnd,
    UnknownMessage(u8),
    TrailingBytes(usize),
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ParseError::UnexpectedEnd => write!(f, "message ended unexpectedly"),
            ParseError::UnknownMessage(t) => write!(f, "unknown pgoutput message tag {}", t),
            ParseError::TrailingBytes(n) => write!(f, "{n} unparsed trailing bytes"),
        }
    }
}

impl std::error::Error for ParseError {}

/// Microseconds since 2000-01-01, as PG transmits timestamps on the wire.
pub type PgTimestamp = i64;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Begin {
    pub final_lsn: u64,
    pub commit_ts: PgTimestamp,
    pub xid: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Commit {
    pub flags: u8,
    pub commit_lsn: u64,
    pub end_lsn: u64,
    pub commit_ts: PgTimestamp,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicaIdentity {
    Default,
    None,
    Full,
    Index,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationColumn {
    pub name: String,
    pub type_oid: u32,
    pub typmod: i32,
    pub in_replica_identity: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Relation {
    pub rel_id: u32,
    pub schema: String,
    pub name: String,
    pub replica_identity: ReplicaIdentity,
    pub columns: Vec<RelationColumn>,
}

/// One column of a row tuple.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TupleValue {
    Null,
    /// The real value was TOASTed and unchanged since the last write; the wire
    /// carries no data. Completing it requires reading the previously indexed
    /// document or the old tuple of a REPLICA IDENTITY FULL row.
    UnchangedToast,
    Text(Vec<u8>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Tuple(pub Vec<TupleValue>);

impl Tuple {
    pub fn get(&self, idx: usize) -> Option<&TupleValue> {
        self.0.get(idx)
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Insert {
    pub rel_id: u32,
    pub new_tuple: Tuple,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OldTuple {
    Key(Tuple),
    Full(Tuple),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Update {
    pub rel_id: u32,
    pub old_tuple: Option<OldTuple>,
    pub new_tuple: Tuple,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Delete {
    pub rel_id: u32,
    pub key_tuple: OldTuple,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Truncate {
    /// Bit 0: CASCADE, bit 1: RESTART IDENTITY (per PG docs §54.5).
    pub flags: u8,
    pub rel_ids: Vec<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TypeDef {
    pub oid: u32,
    pub schema: String,
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Message {
    Begin(Begin),
    Relation(Relation),
    Type(TypeDef),
    Insert(Insert),
    Update(Update),
    Delete(Delete),
    Truncate(Truncate),
    Commit(Commit),
}

struct Reader<'a> {
    buf: &'a [u8],
}

impl<'a> Reader<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Reader { buf }
    }

    fn take(&mut self, n: usize) -> Result<&'a [u8], ParseError> {
        if self.buf.len() < n {
            return Err(ParseError::UnexpectedEnd);
        }
        let (head, tail) = self.buf.split_at(n);
        self.buf = tail;
        Ok(head)
    }

    fn u8(&mut self) -> Result<u8, ParseError> {
        Ok(self.take(1)?[0])
    }

    fn u16(&mut self) -> Result<u16, ParseError> {
        let b = self.take(2)?;
        Ok(u16::from_be_bytes([b[0], b[1]]))
    }

    fn u32(&mut self) -> Result<u32, ParseError> {
        let b = self.take(4)?;
        Ok(u32::from_be_bytes([b[0], b[1], b[2], b[3]]))
    }

    fn i32(&mut self) -> Result<i32, ParseError> {
        Ok(self.u32()? as i32)
    }

    fn u64(&mut self) -> Result<u64, ParseError> {
        let b = self.take(8)?;
        Ok(u64::from_be_bytes([
            b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7],
        ]))
    }

    fn string(&mut self) -> Result<String, ParseError> {
        let pos = self
            .buf
            .iter()
            .position(|&b| b == 0)
            .ok_or(ParseError::UnexpectedEnd)?;
        let s = String::from_utf8_lossy(&self.buf[..pos]).into_owned();
        self.buf = &self.buf[pos + 1..];
        Ok(s)
    }

    fn tuple(&mut self) -> Result<Tuple, ParseError> {
        let ncols = self.u16()?;
        let mut cols = Vec::with_capacity(ncols as usize);
        for _ in 0..ncols {
            match self.u8()? {
                b'n' => cols.push(TupleValue::Null),
                b'u' => cols.push(TupleValue::UnchangedToast),
                b't' | b'b' => {
                    let len = self.i32()?;
                    if len < 0 {
                        return Err(ParseError::UnexpectedEnd);
                    }
                    cols.push(TupleValue::Text(self.take(len as usize)?.to_vec()));
                }
                other => return Err(ParseError::UnknownMessage(other)),
            }
        }
        Ok(Tuple(cols))
    }

    fn relation(&mut self) -> Result<Relation, ParseError> {
        let rel_id = self.u32()?;
        let schema = self.string()?;
        let name = self.string()?;
        let replica_identity = match self.u8()? {
            b'd' => ReplicaIdentity::Default,
            b'n' => ReplicaIdentity::None,
            b'f' => ReplicaIdentity::Full,
            b'i' => ReplicaIdentity::Index,
            _ => return Err(ParseError::UnknownMessage(b'R')),
        };
        let ncols = self.u16()?;
        let mut columns = Vec::with_capacity(ncols as usize);
        for _ in 0..ncols {
            let flag = self.u8()?;
            let cname = self.string()?;
            let type_oid = self.u32()?;
            let typmod = self.i32()?;
            columns.push(RelationColumn {
                name: cname,
                type_oid,
                typmod,
                // PG sends the raw value 1 (not ASCII '1') when the column
                // belongs to the replica identity (proto.c LOGICALREP_IS_REPLICA_IDENTITY)
                in_replica_identity: matches!(flag, 1 | b'1'),
            });
        }
        Ok(Relation {
            rel_id,
            schema,
            name,
            replica_identity,
            columns,
        })
    }

    fn remaining(&self) -> usize {
        self.buf.len()
    }
}

fn expect_tag(tag: u8, msg: &[u8]) -> Result<Reader<'_>, ParseError> {
    if msg.first() != Some(&tag) {
        return Err(ParseError::UnknownMessage(*msg.first().unwrap_or(&tag)));
    }
    Ok(Reader::new(&msg[1..]))
}

/// Parse one pgoutput message from an `XLogData` payload (the payload includes
/// the single-byte message tag followed by message-specific fields).
pub fn parse(msg: &[u8]) -> Result<Message, ParseError> {
    let tag = *msg.first().ok_or(ParseError::UnexpectedEnd)?;
    let mut r = expect_tag(tag, msg)?;
    let out = match tag {
        b'B' => Message::Begin(Begin {
            final_lsn: r.u64()?,
            commit_ts: r.u64()? as PgTimestamp,
            xid: r.u32()?,
        }),
        b'C' => Message::Commit(Commit {
            flags: r.u8()?,
            commit_lsn: r.u64()?,
            end_lsn: r.u64()?,
            commit_ts: r.u64()? as PgTimestamp,
        }),
        b'R' => Message::Relation(r.relation()?),
        b'Y' => Message::Type(TypeDef {
            oid: r.u32()?,
            schema: r.string()?,
            name: r.string()?,
        }),
        b'I' => {
            let rel_id = r.u32()?;
            let marker = r.u8()?;
            if marker != b'N' {
                return Err(ParseError::UnknownMessage(marker));
            }
            Message::Insert(Insert {
                rel_id,
                new_tuple: r.tuple()?,
            })
        }
        b'U' => {
            let rel_id = r.u32()?;
            let old_tuple = match r.buf.first() {
                Some(&k @ (b'K' | b'O')) => {
                    r.u8()?;
                    let t = r.tuple()?;
                    Some(if k == b'K' {
                        OldTuple::Key(t)
                    } else {
                        OldTuple::Full(t)
                    })
                }
                _ => None,
            };
            let marker = r.u8()?;
            if marker != b'N' {
                return Err(ParseError::UnknownMessage(marker));
            }
            Message::Update(Update {
                rel_id,
                old_tuple,
                new_tuple: r.tuple()?,
            })
        }
        b'D' => {
            let rel_id = r.u32()?;
            let k = r.u8()?;
            if k != b'K' && k != b'O' {
                return Err(ParseError::UnknownMessage(k));
            }
            let t = r.tuple()?;
            Message::Delete(Delete {
                rel_id,
                key_tuple: if k == b'K' {
                    OldTuple::Key(t)
                } else {
                    OldTuple::Full(t)
                },
            })
        }
        b'T' => {
            let nrels = r.u32()?;
            let flags = r.u8()?;
            let mut rel_ids = Vec::with_capacity(nrels as usize);
            for _ in 0..nrels {
                rel_ids.push(r.u32()?);
            }
            Message::Truncate(Truncate { flags, rel_ids })
        }
        other => return Err(ParseError::UnknownMessage(other)),
    };
    let leftover = r.remaining();
    if leftover > 0 {
        return Err(ParseError::TrailingBytes(leftover));
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn put_str(v: &mut Vec<u8>, s: &str) {
        v.extend_from_slice(s.as_bytes());
        v.push(0);
    }

    fn text_tuple(vals: &[Option<&str>]) -> Vec<u8> {
        let mut v = Vec::new();
        v.extend_from_slice(&(vals.len() as u16).to_be_bytes());
        for val in vals {
            match val {
                None => v.push(b'n'),
                Some(s) => {
                    v.push(b't');
                    v.extend_from_slice(&(s.len() as i32).to_be_bytes());
                    v.extend_from_slice(s.as_bytes());
                }
            }
        }
        v
    }

    #[test]
    fn parses_begin_and_commit() {
        let begin = {
            let mut m = vec![b'B'];
            m.extend_from_slice(&0x1B4_F2A8u64.to_be_bytes());
            m.extend_from_slice(&7_000_000i64.to_be_bytes());
            m.extend_from_slice(&754u32.to_be_bytes());
            m
        };
        match parse(&begin).unwrap() {
            Message::Begin(b) => {
                assert_eq!(b.final_lsn, 0x1B4_F2A8);
                assert_eq!(b.xid, 754);
                assert_eq!(b.commit_ts, 7_000_000);
            }
            other => panic!("wrong variant: {other:?}"),
        }

        let commit = {
            let mut m = vec![b'C', 0];
            m.extend_from_slice(&100u64.to_be_bytes());
            m.extend_from_slice(&200u64.to_be_bytes());
            m.extend_from_slice(&9_000_000i64.to_be_bytes());
            m
        };
        match parse(&commit).unwrap() {
            Message::Commit(c) => {
                assert_eq!(c.commit_lsn, 100);
                assert_eq!(c.end_lsn, 200);
            }
            other => panic!("wrong variant: {other:?}"),
        }
    }

    #[test]
    fn parses_relation_insert_update_delete_truncate() {
        let relation = {
            let mut m = vec![b'R'];
            m.extend_from_slice(&16384u32.to_be_bytes());
            put_str(&mut m, "public");
            put_str(&mut m, "users");
            m.push(b'd');
            m.extend_from_slice(&3u16.to_be_bytes());

            let mut col = |name: &str, oid: u32, key: bool| {
                m.push(if key { b'1' } else { b'0' });
                put_str(&mut m, name);
                m.extend_from_slice(&oid.to_be_bytes());
                m.extend_from_slice(&(-1i32).to_be_bytes());
            };
            col("id", 20, true);
            col("name", 25, false);
            col("email", 25, false);
            m
        };
        let rel = match parse(&relation).unwrap() {
            Message::Relation(r) => r,
            other => panic!("wrong variant: {other:?}"),
        };
        assert_eq!(rel.rel_id, 16384);
        assert_eq!(rel.schema, "public");
        assert_eq!(rel.replica_identity, ReplicaIdentity::Default);
        assert_eq!(rel.columns.len(), 3);
        assert!(rel.columns[0].in_replica_identity);
        assert!(!rel.columns[1].in_replica_identity);

        let insert = {
            let mut m = vec![b'I'];
            m.extend_from_slice(&16384u32.to_be_bytes());
            m.push(b'N');
            m.extend(text_tuple(&[Some("42"), Some("ada"), None]));
            m
        };
        match parse(&insert).unwrap() {
            Message::Insert(i) => {
                assert_eq!(i.rel_id, 16384);
                assert_eq!(i.new_tuple.len(), 3);
                assert_eq!(i.new_tuple.get(0), Some(&TupleValue::Text(b"42".to_vec())));
                assert_eq!(i.new_tuple.get(2), Some(&TupleValue::Null));
            }
            other => panic!("wrong variant: {other:?}"),
        }

        let update = {
            // new tuple: col0 unchanged-toast, col1 "grace" — exercises the
            // TOAST marker path that drives the engine's completion logic.
            let mut m = vec![b'U'];
            m.extend_from_slice(&16384u32.to_be_bytes());
            m.push(b'K');
            m.extend(text_tuple(&[Some("42")]));
            m.push(b'N');
            let mut nt = Vec::new();
            nt.extend_from_slice(&2u16.to_be_bytes());
            nt.push(b'u');
            nt.push(b't');
            nt.extend_from_slice(&5i32.to_be_bytes());
            nt.extend_from_slice(b"grace");
            m.extend(nt);
            m
        };
        match parse(&update).unwrap() {
            Message::Update(u) => {
                assert_eq!(
                    u.old_tuple,
                    Some(OldTuple::Key(Tuple(vec![TupleValue::Text(b"42".to_vec())])))
                );
                assert_eq!(u.new_tuple.get(0), Some(&TupleValue::UnchangedToast));
                assert_eq!(
                    u.new_tuple.get(1),
                    Some(&TupleValue::Text(b"grace".to_vec()))
                );
            }
            other => panic!("wrong variant: {other:?}"),
        }

        let delete = {
            let mut m = vec![b'D'];
            m.extend_from_slice(&16384u32.to_be_bytes());
            m.push(b'O');
            m.extend(text_tuple(&[Some("42"), Some("grace"), Some("g@x.io")]));
            m
        };
        match parse(&delete).unwrap() {
            Message::Delete(d) => match d.key_tuple {
                OldTuple::Full(t) => {
                    assert_eq!(t.get(2), Some(&TupleValue::Text(b"g@x.io".to_vec())))
                }
                other => panic!("expected full tuple: {other:?}"),
            },
            other => panic!("wrong variant: {other:?}"),
        }

        let truncate = {
            let mut m = vec![b'T'];
            m.extend_from_slice(&2u32.to_be_bytes());
            m.push(1);
            m.extend_from_slice(&16384u32.to_be_bytes());
            m.extend_from_slice(&16385u32.to_be_bytes());
            m
        };
        match parse(&truncate).unwrap() {
            Message::Truncate(t) => {
                assert_eq!(t.flags, 1);
                assert_eq!(t.rel_ids, vec![16384, 16385]);
            }
            other => panic!("wrong variant: {other:?}"),
        }
    }

    #[test]
    fn rejects_garbage() {
        assert!(matches!(parse(&[]), Err(ParseError::UnexpectedEnd)));
        assert!(matches!(parse(b"Z"), Err(ParseError::UnknownMessage(b'Z'))));

        let truncated = vec![b'B', 0, 0];
        assert!(matches!(parse(&truncated), Err(ParseError::UnexpectedEnd)));

        let trailing = {
            let mut m = vec![b'I'];
            m.extend_from_slice(&16384u32.to_be_bytes());
            m.push(b'N');
            m.extend(text_tuple(&[Some("1")]));
            m.push(0xFF);
            m
        };
        assert!(matches!(
            parse(&trailing),
            Err(ParseError::TrailingBytes(1))
        ));
    }
}
