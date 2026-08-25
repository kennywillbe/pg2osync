//! The read-your-writes endpoint.
//!
//! `GET /synced?position=…` blocks until the pipeline has written past that
//! source position, so a caller that just committed can wait for its own change
//! before querying the target. The wait lands on the request that needs it,
//! never on the write path — which is the whole reason this is an endpoint
//! rather than a synchronous write.

use crate::http::{authorized, request_target, split_target};
use crate::PositionParser;
use pg2osync_core::lsn::Lsn;
use pg2osync_core::sink::Sink;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::watch;

/// A caller cannot hold a connection open indefinitely.
const MAX_TIMEOUT: Duration = Duration::from_secs(30);
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(5);
/// How long ordinary traffic is given to close the gap before a marker is
/// written. Under load this is the whole cost and nothing is written at all.
const GRACE_BEFORE_NUDGE: Duration = Duration::from_millis(50);

/// Pushes the source stream forward so a position that only trails filtered-out
/// activity can still be reached.
///
/// PostgreSQL never sends a transaction that touches no published table, so on
/// a quiet database the caller's position may be one the pipeline would never
/// otherwise see. The implementation writes a marker the stream does carry.
pub type StreamNudge = Arc<dyn Fn() -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>;

/// Reads the source's current position.
///
/// Lets a caller omit `position` entirely. That matters for more than
/// convenience: reading it requires `REPLICATION CLIENT` on MySQL, a privilege
/// an application account should not hold, and pg2osync already has a
/// connection that may.
pub type CurrentPosition =
    Arc<dyn Fn() -> Pin<Box<dyn Future<Output = Option<u64>> + Send>> + Send + Sync>;

pub struct ApiConfig {
    pub bind: String,
    /// Optional bearer token. The endpoint is application-facing, so it may sit
    /// somewhere less private than the metrics port.
    pub token: Option<String>,
    /// Indices to refresh when a caller asks for search visibility.
    pub indices: Vec<String>,
}

/// Everything the handler needs, shared across connections.
struct ApiState {
    cfg: ApiConfig,
    acked: watch::Receiver<Option<Lsn>>,
    parse_position: PositionParser,
    render_position: crate::PositionRenderer,
    sink: Arc<dyn Sink>,
    nudge: Option<StreamNudge>,
    current_position: Option<CurrentPosition>,
}

/// Serve until the process exits. Failures are logged, never fatal: losing this
/// endpoint must not take replication down with it.
pub async fn serve(
    cfg: ApiConfig,
    acked: watch::Receiver<Option<Lsn>>,
    parse_position: PositionParser,
    render_position: crate::PositionRenderer,
    sink: Arc<dyn Sink>,
    nudge: Option<StreamNudge>,
    current_position: Option<CurrentPosition>,
) {
    let listener = match tokio::net::TcpListener::bind(&cfg.bind).await {
        Ok(l) => l,
        Err(e) => {
            tracing::warn!(target: "pg2osync::api", "cannot bind {}: {e}", cfg.bind);
            return;
        }
    };
    tracing::info!(target: "pg2osync::api",
        "read-your-writes endpoint on http://{}/synced", cfg.bind);

    let state = Arc::new(ApiState {
        cfg,
        acked,
        parse_position,
        render_position,
        sink,
        nudge,
        current_position,
    });

    loop {
        let Ok((mut sock, _)) = listener.accept().await else {
            continue;
        };
        let state = state.clone();
        tokio::spawn(async move {
            let mut buf = vec![0u8; 2048];
            let Ok(read) = sock.read(&mut buf).await else {
                return;
            };
            let request = String::from_utf8_lossy(&buf[..read]).into_owned();
            let (status, body) = handle(&state, &request).await;
            let response = format!(
                "HTTP/1.1 {status}\r\nContent-Type: application/json\r\n\
                 Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            let _ = sock.write_all(response.as_bytes()).await;
        });
    }
}

async fn handle(state: &ApiState, request: &str) -> (&'static str, String) {
    let Some(target) = request_target(request) else {
        return ("400 Bad Request", error_body("malformed request"));
    };
    if let Some(expected) = &state.cfg.token
        && !authorized(request, expected)
    {
        return ("401 Unauthorized", error_body("missing or invalid token"));
    }
    let (path, query) = split_target(target);
    if path != "/synced" {
        return ("404 Not Found", error_body("only /synced is served here"));
    }

    // Omitting the position means "everything committed before this request":
    // pg2osync reads the source's position itself, so the caller needs neither
    // the privilege to read it nor any knowledge of LSN or binlog syntax.
    let (requested, raw_position) = match query_param(query, "position") {
        Some(raw) => match (state.parse_position)(&raw) {
            Some(token) => (token, raw),
            None => {
                return (
                    "400 Bad Request",
                    error_body(&format!("cannot parse position {raw:?}")),
                );
            }
        },
        None => match &state.current_position {
            Some(read) => match read().await {
                Some(token) => (token, (state.render_position)(token)),
                None => {
                    return (
                        "503 Service Unavailable",
                        error_body("cannot read the source position"),
                    );
                }
            },
            None => {
                return (
                    "400 Bad Request",
                    error_body("position is required: this pipeline cannot read one itself"),
                );
            }
        },
    };
    let timeout = requested_timeout(query);
    let refresh = query_param(query, "refresh").as_deref() == Some("true");

    let started = Instant::now();
    let reached = wait_for_position(state, requested, timeout).await;
    if reached && refresh {
        // an accepted write is not searchable until the target refreshes, so
        // without this the guarantee would stop one step short of useful
        if let Err(e) = state.sink.refresh(&state.cfg.indices).await {
            tracing::warn!(target: "pg2osync::api", "refresh failed: {e}");
            return (
                "503 Service Unavailable",
                error_body(&format!("refresh failed: {e}")),
            );
        }
    }

    let confirmed = (*state.acked.borrow()).map(|lsn| (state.render_position)(lsn.0));
    let body = format!(
        r#"{{"synced":{reached},"requested":{},"confirmed":{},"waited_ms":{}}}"#,
        json_string(&raw_position),
        confirmed
            .as_deref()
            .map(json_string)
            .unwrap_or_else(|| "null".into()),
        started.elapsed().as_millis(),
    );
    if reached {
        ("200 OK", body)
    } else {
        ("408 Request Timeout", body)
    }
}

/// Wait for the position, pushing the stream along if it will not arrive on
/// its own.
///
/// Under traffic the position moves by itself and the grace period is all that
/// is spent. On a quiet database nothing would ever close the gap, so a marker
/// is written once and the wait resumes.
async fn wait_for_position(state: &ApiState, requested: u64, timeout: Duration) -> bool {
    let grace = GRACE_BEFORE_NUDGE.min(timeout);
    if wait_for(state.acked.clone(), requested, grace).await {
        return true;
    }
    if let Some(nudge) = &state.nudge {
        nudge().await;
    }
    wait_for(
        state.acked.clone(),
        requested,
        timeout.saturating_sub(grace),
    )
    .await
}

/// Wait until the acknowledged position passes `requested`.
///
/// Driven by the watch channel the sink task updates, so a caller is woken by
/// the write itself rather than by a polling interval.
async fn wait_for(
    mut acked: watch::Receiver<Option<Lsn>>,
    requested: u64,
    timeout: Duration,
) -> bool {
    let deadline = Instant::now() + timeout;
    loop {
        if acked
            .borrow_and_update()
            .is_some_and(|lsn| lsn.0 >= requested)
        {
            return true;
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return false;
        }
        if tokio::time::timeout(remaining, acked.changed())
            .await
            .is_err()
        {
            return false;
        }
    }
}

fn query_param(query: &str, key: &str) -> Option<String> {
    query.split('&').find_map(|pair| {
        let (name, value) = pair.split_once('=')?;
        (name == key).then(|| percent_decode(value))
    })
}

fn requested_timeout(query: &str) -> Duration {
    query_param(query, "timeout")
        .and_then(|raw| raw.parse::<u64>().ok())
        .map(Duration::from_millis)
        .unwrap_or(DEFAULT_TIMEOUT)
        .min(MAX_TIMEOUT)
}

/// Query values arrive percent-encoded; a PostgreSQL LSN contains `/`.
fn percent_decode(value: &str) -> String {
    let bytes = value.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'%' if i + 2 < bytes.len() => {
                match u8::from_str_radix(
                    std::str::from_utf8(&bytes[i + 1..i + 3]).unwrap_or(""),
                    16,
                ) {
                    Ok(decoded) => {
                        out.push(decoded);
                        i += 3;
                    }
                    Err(_) => {
                        out.push(bytes[i]);
                        i += 1;
                    }
                }
            }
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            other => {
                out.push(other);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn json_string(value: &str) -> String {
    let escaped: String = value
        .chars()
        .flat_map(|c| match c {
            '"' => vec!['\\', '"'],
            '\\' => vec!['\\', '\\'],
            c => vec![c],
        })
        .collect();
    format!("\"{escaped}\"")
}

fn error_body(message: &str) -> String {
    format!(r#"{{"error":{}}}"#, json_string(message))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_request_line_yields_path_and_query() {
        let request = "GET /synced?position=0%2F1B4F2A8 HTTP/1.1\r\nHost: x\r\n\r\n";
        let target = request_target(request).expect("a target");
        let (path, query) = split_target(target);
        assert_eq!(path, "/synced");
        assert_eq!(
            query_param(query, "position").as_deref(),
            Some("0/1B4F2A8"),
            "an LSN's slash arrives percent-encoded"
        );
    }

    #[test]
    fn only_get_is_served() {
        assert!(request_target("POST /synced HTTP/1.1\r\n\r\n").is_none());
    }

    #[test]
    fn the_timeout_is_bounded_on_both_ends() {
        assert_eq!(requested_timeout(""), DEFAULT_TIMEOUT);
        assert_eq!(requested_timeout("timeout=250"), Duration::from_millis(250));
        assert_eq!(
            requested_timeout("timeout=600000"),
            MAX_TIMEOUT,
            "a caller must not be able to hold a connection open forever"
        );
        assert_eq!(requested_timeout("timeout=abc"), DEFAULT_TIMEOUT);
    }

    #[test]
    fn a_request_without_the_token_is_refused_before_anything_else() {
        let with = |header: &str| format!("GET /synced HTTP/1.1\r\n{header}\r\n\r\n");
        assert!(authorized(&with("Authorization: Bearer secret"), "secret"));
        assert!(!authorized(&with("Host: x"), "secret"));
    }

    #[tokio::test]
    async fn waiting_returns_as_soon_as_the_position_passes() {
        let (tx, rx) = watch::channel(None);
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(20)).await;
            tx.send_replace(Some(Lsn(500)));
        });
        assert!(wait_for(rx, 400, Duration::from_secs(2)).await);
    }

    #[tokio::test]
    async fn an_already_passed_position_returns_immediately() {
        let (_tx, rx) = watch::channel(Some(Lsn(900)));
        assert!(wait_for(rx, 900, Duration::from_millis(1)).await);
    }

    #[tokio::test]
    async fn a_position_that_never_arrives_times_out() {
        let (_tx, rx) = watch::channel(Some(Lsn(10)));
        assert!(!wait_for(rx, u64::MAX, Duration::from_millis(30)).await);
    }

    #[test]
    fn json_values_are_escaped() {
        assert_eq!(json_string(r#"a"b\c"#), r#""a\"b\\c""#);
    }
}
