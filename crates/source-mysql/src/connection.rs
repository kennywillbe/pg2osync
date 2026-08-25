//! Live connection: TLS negotiation, authentication and COM_BINLOG_DUMP.
//!
//! Supports `mysql_native_password` and `caching_sha2_password`, which is the
//! default plugin from MySQL 8.0 onward. Full authentication — needed whenever
//! the server has not cached the account — sends the password over TLS, or
//! encrypts it with the server's public key when the socket is plaintext, so
//! the password never crosses the wire in the clear either way.

use crate::auth;
use crate::packet::{frame_all, parse_greeting};
use anyhow::{Context as _, Result, bail};
use pg2osync_tls::{SslMode, TlsSettings};
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;

const CLIENT_LONG_PASSWORD: u32 = 0x1;
const CLIENT_LONG_FLAG: u32 = 0x4;
const CLIENT_SSL: u32 = 0x800;
const CLIENT_PROTOCOL_41: u32 = 0x200;
const CLIENT_TRANSACTIONS: u32 = 0x2000;
const CLIENT_SECURE_CONNECTION: u32 = 0x8000;
const CLIENT_PLUGIN_AUTH: u32 = 0x80_000;

const NATIVE_PASSWORD: &str = "mysql_native_password";
const CACHING_SHA2: &str = "caching_sha2_password";

/// caching_sha2_password full-authentication signals.
const FAST_AUTH_SUCCESS: u8 = 0x03;
const FULL_AUTH_REQUIRED: u8 = 0x04;
const REQUEST_PUBLIC_KEY: u8 = 0x02;

/// utf8mb4, so text-protocol results decode as UTF-8.
const CHARSET_UTF8MB4: u8 = 45;

#[derive(Debug, Clone)]
pub struct MySqlConfig {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub database: Option<String>,
    pub server_id: u32,
    pub tls: TlsSettings,
}

/// Either a plain socket or a TLS session over one.
///
/// The protocol above this point is identical, so the rest of the client is
/// written against the trait rather than against two nearly identical paths.
trait Stream: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> Stream for T {}

pub struct MySqlConnection {
    stream: Box<dyn Stream>,
    server_id: u32,
    encrypted: bool,
}

impl MySqlConnection {
    /// Whether this connection negotiated TLS.
    pub fn is_encrypted(&self) -> bool {
        self.encrypted
    }

    pub async fn connect(cfg: &MySqlConfig) -> Result<Self> {
        let tcp = TcpStream::connect((cfg.host.as_str(), cfg.port))
            .await
            .context("mysql tcp connect failed")?;
        tcp.set_nodelay(true).ok();
        let mut stream: Box<dyn Stream> = Box::new(tcp);

        let greeting_payload = read_one(&mut stream).await?;
        let greeting = parse_greeting(&greeting_payload).context("cannot parse mysql greeting")?;
        if greeting.capabilities & CLIENT_PLUGIN_AUTH == 0 {
            bail!("server lacks CLIENT_PLUGIN_AUTH");
        }
        let server_supports_tls = greeting.capabilities & CLIENT_SSL != 0;

        let use_tls = match cfg.tls.mode {
            SslMode::Disable => false,
            SslMode::Prefer => server_supports_tls,
            _ if !server_supports_tls => bail!(
                "sslmode={} was requested but the server does not offer TLS",
                cfg.tls.mode.as_str()
            ),
            _ => true,
        };

        let mut caps = CLIENT_LONG_PASSWORD
            | CLIENT_LONG_FLAG
            | CLIENT_PROTOCOL_41
            | CLIENT_TRANSACTIONS
            | CLIENT_SECURE_CONNECTION
            | CLIENT_PLUGIN_AUTH;
        if use_tls {
            caps |= CLIENT_SSL;
        }

        // The greeting is packet 0, so ours is 1. When TLS is used the
        // SSLRequest takes that slot and the real handshake response becomes 2.
        let mut seq = 1u8;
        if use_tls {
            write_framed_at(&mut stream, seq, &ssl_request(caps)).await?;
            seq += 1;
            stream = upgrade_to_tls(stream, cfg).await?;
        }

        // Answer with the plugin the server named; it switches us if the
        // account uses a different one.
        let nonce = greeting.auth_plugin_data.clone();
        let plugin = if greeting.auth_plugin_name.is_empty() {
            NATIVE_PASSWORD.to_string()
        } else {
            greeting.auth_plugin_name.clone()
        };
        let token = auth_token(&plugin, cfg.password.as_bytes(), &nonce)?;
        let response = handshake_response(caps, &cfg.user, &token, &plugin);
        write_framed_at(&mut stream, seq, &response).await?;

        let mut connection = Self {
            stream,
            server_id: cfg.server_id,
            encrypted: use_tls,
        };
        connection.finish_authentication(cfg, &nonce).await?;
        Ok(connection)
    }

    /// Drive the packets that follow the handshake response until the server
    /// accepts or rejects the credentials.
    async fn finish_authentication(
        &mut self,
        cfg: &MySqlConfig,
        initial_nonce: &[u8],
    ) -> Result<()> {
        let mut nonce = initial_nonce.to_vec();
        let mut seq;

        // A handful of round-trips covers switch, fast-auth and full-auth; more
        // than that means the server and this client disagree about the flow.
        for _ in 0..8 {
            let packet = self.read_seq().await?;
            seq = packet.seq;
            match packet.payload.first() {
                Some(0x00) | None => return Ok(()),
                Some(0xFF) => return Err(auth_error(&packet.payload)),
                Some(0xFE) => {
                    let (plugin, new_nonce) = parse_auth_switch(&packet.payload);
                    nonce = new_nonce;
                    let token = auth_token(&plugin, cfg.password.as_bytes(), &nonce)?;
                    self.write_at(seq + 1, &token).await?;
                }
                Some(0x01) => match packet.payload.get(1).copied() {
                    Some(FAST_AUTH_SUCCESS) => continue,
                    Some(FULL_AUTH_REQUIRED) => {
                        seq = self.full_authentication(cfg, &nonce, seq).await?;
                        let _ = seq;
                    }
                    // any other AuthMoreData at this point is the public key,
                    // which only arrives after we asked for it
                    _ => bail!("unexpected auth data from the server"),
                },
                other => bail!("unexpected auth reply {other:?}"),
            }
        }
        bail!("authentication did not complete after 8 exchanges")
    }

    /// caching_sha2_password full authentication.
    ///
    /// Over TLS the password goes as cleartext, which is what the server
    /// expects. On a plaintext socket it is XORed with the nonce and encrypted
    /// with the server's public key instead.
    async fn full_authentication(
        &mut self,
        cfg: &MySqlConfig,
        nonce: &[u8],
        seq: u8,
    ) -> Result<u8> {
        if self.encrypted {
            self.write_at(seq + 1, &auth::cleartext_password(cfg.password.as_bytes()))
                .await?;
            return Ok(seq + 1);
        }

        self.write_at(seq + 1, &[REQUEST_PUBLIC_KEY]).await?;
        let key_packet = self.read_seq().await?;
        if key_packet.payload.first() != Some(&0x01) {
            bail!("server did not return a public key for full authentication");
        }
        let encrypted =
            auth::rsa_encrypted_password(cfg.password.as_bytes(), nonce, &key_packet.payload[1..])?;
        self.write_at(key_packet.seq + 1, &encrypted).await?;
        Ok(key_packet.seq + 1)
    }

    async fn read_seq(&mut self) -> Result<SeqPacket> {
        read_one_with_seq(&mut self.stream).await
    }

    async fn write_at(&mut self, seq: u8, payload: &[u8]) -> Result<()> {
        write_framed_at(&mut self.stream, seq, payload).await
    }

    pub async fn send_query(&mut self, sql: &str) -> Result<()> {
        let mut p = vec![0x03];
        p.extend_from_slice(sql.as_bytes());
        self.write_command(&p).await
    }

    /// Start streaming binlog events from file@position.
    pub async fn send_binlog_dump(&mut self, filename: &str, pos: u32) -> Result<()> {
        let mut p = vec![0x12];
        p.extend_from_slice(&pos.to_le_bytes());
        p.extend_from_slice(&0u16.to_le_bytes()); // flags: BINLOG_DUMP_NON_BLOCK=0
        p.extend_from_slice(&self.server_id.to_le_bytes());
        p.extend_from_slice(filename.as_bytes());
        p.push(0);
        self.write_command(&p).await
    }

    async fn write_command(&mut self, payload: &[u8]) -> Result<()> {
        // every command starts a new sequence
        write_framed_at(&mut self.stream, 0, payload).await
    }

    /// Read one framed packet of the active phase.
    pub async fn read_packet(&mut self) -> Result<Vec<u8>> {
        read_one(&mut self.stream).await
    }

    /// Ask the server for heartbeats while the binlog is quiet.
    ///
    /// Without them the position only moves when something is written, so a
    /// caller waiting for its own commit on an idle server would wait for the
    /// next unrelated write. Must run before COM_BINLOG_DUMP.
    pub async fn set_heartbeat_period(&mut self, period: std::time::Duration) -> Result<()> {
        let nanos = period.as_nanos().min(u64::MAX as u128) as u64;
        self.send_query(&format!("SET @master_heartbeat_period = {nanos}"))
            .await?;
        let _ = read_one(&mut self.stream).await?;
        Ok(())
    }

    /// Required before COM_BINLOG_DUMP when the source logs CRC32 checksums:
    /// tells the server we can consume them. Must run on the same connection.
    pub async fn negotiate_checksum(&mut self) -> Result<()> {
        self.send_query("SET @master_binlog_checksum= @@global.binlog_checksum")
            .await?;
        let _ = read_one(&mut self.stream).await?; // OK reply
        Ok(())
    }
}

/// One packet with the sequence number the server stamped on it.
struct SeqPacket {
    seq: u8,
    payload: Vec<u8>,
}

async fn read_one<S>(stream: &mut S) -> Result<Vec<u8>>
where
    S: AsyncRead + Unpin,
{
    Ok(read_one_with_seq(stream).await?.payload)
}

async fn read_one_with_seq<S>(stream: &mut S) -> Result<SeqPacket>
where
    S: AsyncRead + Unpin,
{
    let mut head = [0u8; 4];
    stream
        .read_exact(&mut head)
        .await
        .context("connection closed mid-packet")?;
    let len = u32::from_le_bytes([head[0], head[1], head[2], 0]) as usize;
    let mut payload = vec![0u8; len];
    stream
        .read_exact(&mut payload)
        .await
        .context("connection closed mid-payload")?;
    Ok(SeqPacket {
        seq: head[3],
        payload,
    })
}

async fn write_framed_at<S>(stream: &mut S, start_seq: u8, payload: &[u8]) -> Result<()>
where
    S: AsyncWrite + Unpin,
{
    for f in frame_all(start_seq, payload) {
        stream.write_all(&f).await.context("mysql write failed")?;
    }
    stream.flush().await.context("mysql flush failed")
}

/// The 32-byte prelude that asks the server to start TLS.
///
/// It is the head of a normal handshake response and nothing more: everything
/// that would identify the user is withheld until the session is encrypted.
fn ssl_request(caps: u32) -> Vec<u8> {
    let mut p = Vec::with_capacity(32);
    p.extend_from_slice(&caps.to_le_bytes());
    p.extend_from_slice(&0x00FF_FFFFu32.to_le_bytes());
    p.push(CHARSET_UTF8MB4);
    p.extend_from_slice(&[0u8; 23]);
    p
}

fn handshake_response(caps: u32, user: &str, token: &[u8], plugin: &str) -> Vec<u8> {
    let mut p = Vec::with_capacity(128);
    p.extend_from_slice(&caps.to_le_bytes());
    p.extend_from_slice(&0x00FF_FFFFu32.to_le_bytes());
    p.push(CHARSET_UTF8MB4);
    p.extend_from_slice(&[0u8; 23]);
    p.extend_from_slice(user.as_bytes());
    p.push(0);
    p.push(token.len() as u8);
    p.extend_from_slice(token);
    p.extend_from_slice(plugin.as_bytes());
    p.push(0);
    p
}

fn auth_token(plugin: &str, password: &[u8], nonce: &[u8]) -> Result<Vec<u8>> {
    let nonce = &nonce[..20.min(nonce.len())];
    match plugin {
        NATIVE_PASSWORD => Ok(auth::native_password(password, nonce)),
        CACHING_SHA2 => Ok(auth::caching_sha2_fast(password, nonce)),
        other => bail!(
            "unsupported authentication plugin {other:?}; this client speaks \
             mysql_native_password and caching_sha2_password"
        ),
    }
}

fn parse_auth_switch(payload: &[u8]) -> (String, Vec<u8>) {
    let rest = &payload[1..];
    let name_end = rest.iter().position(|&b| b == 0).unwrap_or(rest.len());
    let plugin = String::from_utf8_lossy(&rest[..name_end]).into_owned();
    let nonce_raw = rest.get(name_end + 1..).unwrap_or(&[]);
    // the trailing NUL is not part of the nonce
    let nonce = nonce_raw
        .iter()
        .copied()
        .take_while(|&b| b != 0)
        .collect::<Vec<u8>>();
    (plugin, nonce)
}

/// Wrap the socket in a TLS session using the shared source TLS settings.
async fn upgrade_to_tls(stream: Box<dyn Stream>, cfg: &MySqlConfig) -> Result<Box<dyn Stream>> {
    let connector = tokio_rustls::TlsConnector::from(Arc::new(cfg.tls.client_config()?));
    let server_name = rustls::pki_types::ServerName::try_from(cfg.host.clone())
        .with_context(|| format!("{} is not a valid TLS server name", cfg.host))?;
    let tls = connector
        .connect(server_name, stream)
        .await
        .with_context(|| {
            format!(
                "mysql TLS handshake failed (sslmode={})",
                cfg.tls.mode.as_str()
            )
        })?;
    Ok(Box::new(tls))
}

fn auth_error(payload: &[u8]) -> anyhow::Error {
    let msg = String::from_utf8_lossy(payload.get(9..).unwrap_or(&[])).into_owned();
    let code = u16::from_le_bytes([*payload.get(1).unwrap_or(&0), *payload.get(2).unwrap_or(&0)]);
    anyhow::anyhow!("auth failed ({code}): {msg}")
}

impl MySqlConnection {
    /// Run a query and collect every row of the text-protocol resultset.
    ///
    /// For anything whose size is bounded by the schema — column lists, one
    /// binlog coordinate. A table is read through [`Self::text_query`] instead.
    pub async fn query_text_rows(&mut self, sql: &str) -> Result<Vec<Vec<Option<String>>>> {
        let mut rows = self.text_query(sql).await?;
        let mut out = Vec::new();
        while let Some(row) = rows.next().await? {
            out.push(row);
        }
        Ok(out)
    }

    /// Open a resultset and read it row by row.
    ///
    /// The borrow is the guarantee: nothing else can use the connection until
    /// the cursor is dropped, and a cursor dropped with rows still unread would
    /// leave them in the socket for the next query to misparse.
    pub async fn text_query(&mut self, sql: &str) -> Result<TextRows<'_>> {
        self.send_query(sql).await?;
        let head = read_one(&mut self.stream).await?;
        let ncols = match head.first() {
            Some(0xFF) => bail!("query failed: {}", error_message(&head)),
            // OK packet: a statement with no resultset at all
            Some(0x00) | None => 0,
            _ => lenenc_at(&head, &mut 0).context("resultset header is not a column count")?,
        };
        // column definitions, then the EOF that closes them
        if ncols > 0 {
            for _ in 0..=ncols {
                let _ = read_one(&mut self.stream).await?;
            }
        }
        Ok(TextRows {
            conn: self,
            ncols,
            done: ncols == 0,
        })
    }

    /// Convenience wrapper for single-row queries such as `SHOW MASTER STATUS`.
    pub async fn query_text_row(&mut self, sql: &str) -> Result<Vec<Option<String>>> {
        Ok(self
            .query_text_rows(sql)
            .await?
            .into_iter()
            .next()
            .unwrap_or_default())
    }

    /// Read one global variable, e.g. `binlog_format`.
    pub async fn global_var(&mut self, name: &str) -> Result<Option<String>> {
        let row = self
            .query_text_row(&format!("SELECT @@GLOBAL.{name}"))
            .await?;
        Ok(row.into_iter().next().flatten())
    }
}

/// An open text-protocol resultset, read one row at a time.
pub struct TextRows<'a> {
    conn: &'a mut MySqlConnection,
    ncols: usize,
    done: bool,
}

impl TextRows<'_> {
    /// The next row, or `None` once the resultset is exhausted.
    pub async fn next(&mut self) -> Result<Option<Vec<Option<String>>>> {
        if self.done {
            return Ok(None);
        }
        let pkt = read_one(&mut self.conn.stream).await?;
        match pkt.first() {
            Some(0xFF) => bail!("query failed mid-resultset: {}", error_message(&pkt)),
            // EOF packet: 0xFE with a short payload, and CLIENT_DEPRECATE_EOF
            // is not negotiated so it stays five bytes. A row whose first value
            // is 8-byte length-encoded also starts with 0xFE, but its marker
            // and length alone are nine bytes, so the two cannot be confused.
            Some(0xFE) if pkt.len() < 9 => {
                self.done = true;
                Ok(None)
            }
            None => {
                self.done = true;
                Ok(None)
            }
            _ => Ok(Some(parse_text_row(&pkt, self.ncols)?)),
        }
    }
}

/// A length-encoded integer at `pos`, advancing it past what it read.
///
/// Reading only the first byte works until a value — or a column count —
/// reaches 251 and then misreads the bytes that follow it.
fn lenenc_at(pkt: &[u8], pos: &mut usize) -> Option<usize> {
    let first = *pkt.get(*pos)?;
    *pos += 1;
    let (len, width) = match first {
        0xFC => (
            u16::from_le_bytes(pkt.get(*pos..*pos + 2)?.try_into().ok()?) as usize,
            2,
        ),
        0xFD => {
            let b = pkt.get(*pos..*pos + 3)?;
            (
                b[0] as usize | (b[1] as usize) << 8 | (b[2] as usize) << 16,
                3,
            )
        }
        0xFE => (
            u64::from_le_bytes(pkt.get(*pos..*pos + 8)?.try_into().ok()?) as usize,
            8,
        ),
        n => (n as usize, 0),
    };
    *pos += width;
    Some(len)
}

/// Decode one text-protocol row: length-encoded strings, 0xFB marking NULL.
fn parse_text_row(pkt: &[u8], ncols: usize) -> Result<Vec<Option<String>>> {
    let mut vals = Vec::with_capacity(ncols);
    let mut pos = 0usize;
    for _ in 0..ncols {
        let Some(&first) = pkt.get(pos) else { break };
        if first == 0xFB {
            pos += 1;
            vals.push(None);
            continue;
        }
        let len = lenenc_at(pkt, &mut pos).context("row truncated")?;
        let raw = pkt.get(pos..pos + len).context("row value truncated")?;
        pos += len;
        vals.push(Some(String::from_utf8_lossy(raw).into_owned()));
    }
    Ok(vals)
}

fn error_message(payload: &[u8]) -> String {
    String::from_utf8_lossy(payload.get(9..).unwrap_or(&[])).into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn text_row_handles_null_and_long_values() {
        let mut pkt = vec![3];
        pkt.extend_from_slice(b"row");
        pkt.push(0xFB);
        pkt.push(0xFC);
        pkt.extend_from_slice(&300u16.to_le_bytes());
        pkt.extend(std::iter::repeat_n(b'x', 300));
        let row = parse_text_row(&pkt, 3).expect("parse");
        assert_eq!(row[0].as_deref(), Some("row"));
        assert_eq!(row[1], None);
        assert_eq!(row[2].as_deref().map(str::len), Some(300));
    }

    #[test]
    fn truncated_row_is_an_error_not_a_panic() {
        let pkt = vec![10, b'a'];
        assert!(parse_text_row(&pkt, 1).is_err());
    }

    #[test]
    fn a_wide_resultset_reports_its_real_column_count() {
        // one byte holds 250 at most, so a table with more columns than that
        // announces itself length-encoded
        let mut head = vec![0xFC];
        head.extend_from_slice(&300u16.to_le_bytes());
        assert_eq!(lenenc_at(&head, &mut 0), Some(300));
        assert_eq!(lenenc_at(&[7], &mut 0), Some(7));
    }
}
