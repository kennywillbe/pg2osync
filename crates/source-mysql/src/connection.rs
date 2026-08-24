//! Live connection: handshake, native-password auth, COM_BINLOG_DUMP.
//!
//! Stage limitation (documented): only `mysql_native_password` is supported.
//! caching_sha2_password full-auth requires TLS or RSA exchange; create the
//! replication user with mysql_native_password. The greeting may advertise the
//! server default plugin — we request native explicitly and follow at most a
//! handful of AuthSwitchRequest packets.

use crate::packet::{frame_all, native_password_scramble, parse_greeting};
use anyhow::{Context as _, Result, bail};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

const CLIENT_LONG_PASSWORD: u32 = 0x1;
const CLIENT_LONG_FLAG: u32 = 0x4;
const CLIENT_PROTOCOL_41: u32 = 0x200;
const CLIENT_TRANSACTIONS: u32 = 0x2000;
const CLIENT_SECURE_CONNECTION: u32 = 0x8000;
const CLIENT_PLUGIN_AUTH: u32 = 0x80_000;

#[derive(Debug, Clone)]
pub struct MySqlConfig {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub database: Option<String>,
    pub server_id: u32,
}

pub struct MySqlConnection {
    stream: TcpStream,
}

impl MySqlConnection {
    pub async fn connect(cfg: &MySqlConfig) -> Result<Self> {
        let mut stream = TcpStream::connect((cfg.host.as_str(), cfg.port))
            .await
            .context("mysql tcp connect failed")?;
        stream.set_nodelay(true).ok();

        let greeting_payload = read_one(&mut stream).await?;
        let greeting = parse_greeting(&greeting_payload).context("cannot parse mysql greeting")?;
        if greeting.capabilities & CLIENT_PLUGIN_AUTH == 0 {
            bail!("server lacks CLIENT_PLUGIN_AUTH");
        }
        let scramble = &greeting.auth_plugin_data[..20.min(greeting.auth_plugin_data.len())];
        let token = native_password_scramble(cfg.password.as_bytes(), scramble);

        // request mysql_native_password explicitly; the server switches us when
        // the target user's plugin differs
        let caps = CLIENT_LONG_PASSWORD
            | CLIENT_LONG_FLAG
            | CLIENT_PROTOCOL_41
            | CLIENT_TRANSACTIONS
            | CLIENT_SECURE_CONNECTION
            | CLIENT_PLUGIN_AUTH;
        let mut resp = Vec::with_capacity(128);
        resp.extend_from_slice(&caps.to_le_bytes());
        resp.extend_from_slice(&0x00FF_FFFFu32.to_le_bytes());
        resp.push(45);
        resp.extend_from_slice(&[0u8; 23]);
        resp.extend_from_slice(cfg.user.as_bytes());
        resp.push(0);
        resp.push(token.len() as u8);
        resp.extend_from_slice(&token);
        resp.extend_from_slice(b"mysql_native_password");
        resp.push(0);

        // packet sequences continue across the whole exchange: greeting was 0,
        // our response is 1, and every later packet keeps counting up
        let mut seq = 2u8;
        write_framed_at(&mut stream, 1, &resp).await?;

        for _ in 0..4 {
            let payload = read_one(&mut stream).await?;
            match payload.first() {
                Some(0x00) | None => return Ok(Self { stream }),
                Some(0xFF) => return Err(auth_error(&payload)),
                Some(0xFE) => {
                    // our switch response must use the NEXT sequence after the
                    // server's request
                    seq = seq.wrapping_add(1);
                    let rest = &payload[1..];
                    let name_end = rest.iter().position(|&b| b == 0).unwrap_or(rest.len());
                    let new_plugin = String::from_utf8_lossy(&rest[..name_end]).into_owned();
                    let new_scramble_raw = &rest[name_end + 1..];
                    let new_scramble = &new_scramble_raw[..20.min(new_scramble_raw.len())];
                    if !new_plugin.starts_with("mysql_native_password") {
                        bail!(
                            "server switched to '{new_plugin}'; unsupported this stage — \
                             create the replication user with mysql_native_password"
                        );
                    }
                    let tok = native_password_scramble(cfg.password.as_bytes(), new_scramble);
                    write_framed(&mut stream, &mut seq, &tok).await?;
                }
                other => bail!("unexpected auth reply {other:?}"),
            }
        }
        bail!("too many auth switch requests")
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
        p.extend_from_slice(&self.server_id().to_le_bytes());
        p.extend_from_slice(filename.as_bytes());
        p.push(0);
        self.write_command(&p).await
    }

    fn server_id(&self) -> u32 {
        std::env::var("PG2OSYNC_MYSQL_SERVER_ID")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(424243)
    }

    async fn write_command(&mut self, payload: &[u8]) -> Result<()> {
        write_framed(&mut self.stream, &mut 0u8, payload).await
    }

    /// Read one framed packet of the active phase.
    pub async fn read_packet(&mut self) -> Result<Vec<u8>> {
        read_one(&mut self.stream).await
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

async fn read_one<S>(stream: &mut S) -> Result<Vec<u8>>
where
    S: tokio::io::AsyncRead + Unpin,
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
    Ok(payload)
}

async fn write_framed<S>(stream: &mut S, seq: &mut u8, payload: &[u8]) -> Result<()>
where
    S: tokio::io::AsyncWrite + Unpin,
{
    for f in frame_all(*seq, payload) {
        stream.write_all(&f).await.context("mysql write failed")?;
        *seq = seq.wrapping_add(1);
    }
    stream.flush().await.context("mysql flush failed")
}

async fn write_framed_at<S>(stream: &mut S, start_seq: u8, payload: &[u8]) -> Result<()>
where
    S: tokio::io::AsyncWrite + Unpin,
{
    for f in frame_all(start_seq, payload) {
        stream.write_all(&f).await.context("mysql write failed")?;
    }
    stream.flush().await.context("mysql flush failed")
}

fn auth_error(payload: &[u8]) -> anyhow::Error {
    let msg = String::from_utf8_lossy(payload.get(9..).unwrap_or(&[])).into_owned();
    let code = u16::from_le_bytes([*payload.get(1).unwrap_or(&0), *payload.get(2).unwrap_or(&0)]);
    anyhow::anyhow!("auth failed ({code}): {msg}")
}

impl MySqlConnection {
    /// Read a text-protocol resultset first-row's values as strings.
    /// Used for SHOW MASTER STATUS and similar single-row queries.
    pub async fn query_text_row(&mut self, sql: &str) -> Result<Vec<Option<String>>> {
        self.send_query(sql).await?;
        // column count packet
        let cc = read_one(&mut self.stream).await?;
        if cc.first() == Some(&0xFF) {
            anyhow::bail!("query error: {}", String::from_utf8_lossy(&cc[9..]));
        }
        let ncols = cc.first().copied().unwrap_or(0) as usize;
        // skip column definition packets
        for _ in 0..=ncols {
            let _ = read_one(&mut self.stream).await?;
        }
        // row packet: lenenc-prefixed string values; NULL = 0xFB byte
        let row = read_one(&mut self.stream).await?;
        let mut vals = Vec::with_capacity(ncols);
        let mut pos = 0usize;
        for _ in 0..ncols {
            if pos >= row.len() || row[pos] == 0xFB {
                vals.push(None);
                pos += 1;
                continue;
            }
            let len = row[pos] as usize;
            pos += 1;
            if pos + len > row.len() {
                anyhow::bail!("row data truncated");
            }
            vals.push(Some(
                String::from_utf8_lossy(&row[pos..pos + len]).into_owned(),
            ));
            pos += len;
        }
        Ok(vals)
    }
}
