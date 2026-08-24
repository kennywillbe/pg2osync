//! MySQL client/server packet framing.
//!
//! Every logical message travels as one or more 3-byte-length-prefixed
//! packets with a sequence id; sequences >= 255 wrap to 0 and continue.
//! Reassembly across packets is mandatory for large binlog events.

#[derive(Debug, thiserror::Error)]
pub enum FrameError {
    #[error("connection closed mid-packet")]
    Eof,
    #[error("io: {0}")]
    Io(String),
}

pub struct Packet {
    pub seq: u8,
    pub payload: Vec<u8>,
}

/// Build one framed packet (payload must be <= 16MB - 4).
pub fn frame(seq: &mut u8, payload: &[u8]) -> Vec<u8> {
    let len = payload.len() as u32;
    let mut out = Vec::with_capacity(payload.len() + 4);
    let lb = len.to_le_bytes();
    out.extend_from_slice(&lb[..3]);
    out.push(*seq);
    *seq = seq.wrapping_add(1);
    out.extend_from_slice(payload);
    out
}

/// Split a >16MB logical payload into framed packets.
pub fn frame_all(start_seq: u8, payload: &[u8]) -> Vec<Vec<u8>> {
    const MAX: usize = 0x00FF_FFFF;
    let mut out = Vec::new();
    let mut seq = start_seq;
    for chunk in payload.chunks(MAX.max(1)) {
        out.push(frame(&mut seq, chunk));
    }
    if payload.is_empty() || payload.len().is_multiple_of(MAX) {
        // protocol requires an empty terminating packet at exact multiples
        out.push(frame(&mut seq, &[]));
    }
    out
}

/// Read one framed packet. `first_seq` is the expected sequence of this read.
pub async fn read_packet<S>(stream: &mut S) -> Result<Packet, FrameError>
where
    S: tokio::io::AsyncRead + Unpin,
{
    use tokio::io::AsyncReadExt;
    let mut head = [0u8; 4];
    stream.read_exact(&mut head).await.map_err(io_err)?;
    let len = u32::from_le_bytes([head[0], head[1], head[2], 0]) as usize;
    let seq = head[3];
    let mut payload = vec![0u8; len];
    stream
        .read_exact(&mut payload)
        .await
        .map_err(|e| match e.kind() {
            tokio::io::ErrorKind::UnexpectedEof => FrameError::Eof,
            _ => io_err(e),
        })?;
    Ok(Packet { seq, payload })
}

fn io_err(e: std::io::Error) -> FrameError {
    FrameError::Io(e.to_string())
}

/// mysql_native_password scramble:
/// SHA1(password) XOR SHA1(scramble20 || SHA1(SHA1(password)))
pub fn native_password_scramble(password: &[u8], scramble20: &[u8]) -> Vec<u8> {
    use sha1::{Digest, Sha1};
    if password.is_empty() {
        return vec![];
    }
    let stage1 = Sha1::digest(password);
    let stage2 = Sha1::digest(stage1);
    let mut hasher = Sha1::new();
    hasher.update(scramble20);
    hasher.update(stage2);
    let stage3 = hasher.finalize();
    stage1
        .iter()
        .zip(stage3.iter())
        .map(|(a, b)| a ^ b)
        .collect()
}

/// Parse a Handshake V10 greeting into capabilities, scramble and plugin name.
pub struct ServerGreeting {
    pub capabilities: u32,
    /// Full scramble: auth-plugin-data-part-1 + part-2 (nulls stripped).
    pub auth_plugin_data: Vec<u8>,
    pub auth_plugin_name: String,
}

pub fn parse_greeting(payload: &[u8]) -> Option<ServerGreeting> {
    struct C<'a> {
        b: &'a [u8],
        pos: usize,
    }
    impl<'a> C<'a> {
        fn byte(&mut self) -> Option<u8> {
            let v = *self.b.get(self.pos)?;
            self.pos += 1;
            Some(v)
        }
        fn take(&mut self, n: usize) -> Option<&'a [u8]> {
            let s = self.b.get(self.pos..self.pos + n)?;
            self.pos += n;
            Some(s)
        }
        fn u16(&mut self) -> Option<u16> {
            let b = self.take(2)?;
            Some(u16::from_le_bytes([b[0], b[1]]))
        }
        fn cstring(&mut self) -> Option<String> {
            let end = self.b[self.pos..].iter().position(|&b| b == 0)? + self.pos;
            let s = String::from_utf8_lossy(&self.b[self.pos..end]).into_owned();
            self.pos = end + 1;
            Some(s)
        }
    }

    let mut c = C { b: payload, pos: 0 };
    if c.byte()? != 10 {
        return None;
    }
    c.cstring()?; // server version
    c.take(4)?; // thread id
    let part1 = c.take(8)?.to_vec();
    if c.byte()? != 0 {
        return None; // filler
    }
    let caps_lower = c.u16()?;
    let _charset = c.byte()?;
    let _status = c.u16()?;
    let caps_upper = c.u16()?;
    let plugin_data_len = c.byte()? as usize;
    c.take(10)?; // reserved
    // part-2 length is plugin_data_len - 8 when CLIENT_PLUGIN_AUTH, else capped
    let part2_len = plugin_data_len.saturating_sub(8).min(13);
    let part2_raw = c.take(part2_len)?.to_vec();
    let part2: Vec<u8> = part2_raw.into_iter().filter(|&b| b != 0).collect();
    let name_end = payload[c.pos..]
        .iter()
        .position(|&b| b == 0)
        .map(|e| e + c.pos);
    let name = match name_end {
        Some(e) => String::from_utf8_lossy(&payload[c.pos..e]).into_owned(),
        None => "mysql_native_password".into(),
    };
    Some(ServerGreeting {
        capabilities: (caps_lower as u32) | ((caps_upper as u32) << 16),
        auth_plugin_data: [part1, part2].concat(),
        auth_plugin_name: name,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn native_scramble_matches_reference() {
        let pw = b"pw";
        let scramble = [0u8; 20];
        let token = native_password_scramble(pw, &scramble);
        use sha1::{Digest, Sha1};
        let s1 = Sha1::digest(pw);
        let s2 = Sha1::digest(s1);
        let s3 = Sha1::new()
            .chain_update(scramble)
            .chain_update(s2)
            .finalize();
        let expect: Vec<u8> = s1.iter().zip(s3.iter()).map(|(a, b)| a ^ b).collect();
        assert_eq!(token, expect);
        assert!(native_password_scramble(b"", &scramble).is_empty());
    }

    #[test]
    fn framing_roundtrip() {
        let mut seq = 5u8;
        let f = frame(&mut seq, b"hello");
        assert_eq!(&f[..3], &[5, 0, 0]);
        assert_eq!(f[3], 5);
        assert_eq!(&f[4..], b"hello");
    }

    #[test]
    fn greeting_parse_minimal() {
        // synthetic Handshake V10
        let mut p = vec![10u8];
        p.extend_from_slice(b"8.0.46\0");
        p.extend_from_slice(&1u32.to_le_bytes());
        p.extend_from_slice(&[1, 2, 3, 4, 5, 6, 7, 8]); // scramble part 1
        p.push(0);
        p.extend_from_slice(&0x0DAAu16.to_le_bytes()); // caps lower (PLUGIN_AUTH set)
        p.push(45);
        p.extend_from_slice(&2u16.to_le_bytes());
        p.extend_from_slice(&0x0DAAu16.to_le_bytes()); // caps upper
        p.push(21); // plugin data len
        p.extend_from_slice(&[0u8; 10]); // reserved
        p.extend_from_slice(&[9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 0]); // part 2 (12 + NUL)
        p.extend_from_slice(b"mysql_native_password\0");

        let g = parse_greeting(&p).expect("parse");
        assert_eq!(g.auth_plugin_name, "mysql_native_password");
        assert_eq!(g.auth_plugin_data.len(), 20);
        assert_ne!(g.capabilities & 0x80000, 0);
    }
}
