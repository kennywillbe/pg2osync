//! The read-your-writes endpoint.
//!
//! `GET /synced?position=…` blocks until the pipeline has written past that
//! source position, so a caller that just committed can wait for its own change
//! before querying the target. The wait lands on the request that needs it,
//! never on the write path — which is the whole reason this is an endpoint
//! rather than a synchronous write.

use crate::PositionParser;
use crate::http::{authorized, request_target, split_target};
use pg2osync_core::lsn::Lsn;
use pg2osync_core::sink::Sink;
use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{mpsc, watch};
use tracing::Instrument;

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

/// Continues a caller's trace inside the wait, given the span and the W3C
/// `traceparent` header the request carried.
///
/// A closure the binary injects, like the two above it, because linking a
/// header to a span means naming a tracing backend's types and the engine may
/// not: it emits `tracing` spans and nothing else. Absent — which is the case
/// whenever nothing is exporting traces — a `traceparent` is simply ignored.
pub type TraceLink = Arc<dyn Fn(&tracing::Span, &str) + Send + Sync>;

pub struct ApiConfig {
    pub bind: String,
    /// Optional bearer token. The endpoint is application-facing, so it may sit
    /// somewhere less private than the metrics port.
    pub token: Option<String>,
}

/// What one source answers `/synced` with.
///
/// Cloned out of the map before a request waits on it: the wait lasts seconds,
/// and holding a lock for it would keep the source that is still connecting
/// from registering.
#[derive(Clone)]
pub struct SourceEndpoints {
    pub acked: watch::Receiver<Option<Lsn>>,
    pub parse_position: PositionParser,
    pub render_position: crate::PositionRenderer,
    pub sink: Arc<dyn Sink>,
    pub nudge: Option<StreamNudge>,
    pub current_position: Option<CurrentPosition>,
    /// Indices to refresh when a caller asks for search visibility. Watched
    /// rather than owned: a table added to the pipeline brings an index the
    /// endpoint has to refresh before it may promise a write is searchable.
    pub indices: watch::Receiver<Arc<Vec<String>>>,
}

/// What the endpoint borrows from the process rather than from one source.
///
/// Named fields rather than a row of positional arguments: a call site that
/// swapped two of them would still compile.
pub struct ApiDeps {
    /// Every source the process was configured with, known before any of them
    /// has connected: it is what tells an unknown name from one that is still
    /// starting up.
    pub names: Vec<String>,
    /// Each source announces itself here once it can answer, which on MySQL
    /// takes a server round trip.
    pub registrations: mpsc::Receiver<(String, SourceEndpoints)>,
    pub trace_link: Option<TraceLink>,
}

/// Everything the handler needs, shared across connections.
struct ApiState {
    cfg: ApiConfig,
    names: Vec<String>,
    trace_link: Option<TraceLink>,
    sources: RwLock<BTreeMap<String, SourceEndpoints>>,
}

impl ApiState {
    fn endpoints(&self, name: &str) -> Option<SourceEndpoints> {
        self.sources.read().unwrap().get(name).cloned()
    }

    /// Which source a request is about.
    ///
    /// One source and no `source=` is the single-config case, unchanged.
    /// Several and no `source=` cannot be guessed: answering for whichever one
    /// happened to be first would tell a caller its write is visible when the
    /// pipeline that carries it has not written anything.
    fn choose(&self, query: &str) -> Result<String, (&'static str, String)> {
        match query_param(query, "source") {
            Some(name) if self.names.contains(&name) => Ok(name),
            Some(name) => Err((
                "404 Not Found",
                error_body(&format!(
                    "no source is called {name:?}; this process runs: {}",
                    self.names.join(", ")
                )),
            )),
            None if self.names.len() == 1 => Ok(self.names[0].clone()),
            None => Err((
                "400 Bad Request",
                error_body(&format!(
                    "this process runs {} sources ({}); name one with ?source=",
                    self.names.len(),
                    self.names.join(", ")
                )),
            )),
        }
    }
}

/// Serve until the process exits. Failures are logged, never fatal: losing this
/// endpoint must not take replication down with it.
///
/// The listener opens before any source has connected, and each one registers
/// as it becomes able to answer — MySQL cannot render a position until it has
/// read the binlog prefix off the server. A source that has not registered yet
/// is reported as such rather than kept waiting on a connection that may never
/// open.
pub async fn serve(cfg: ApiConfig, deps: ApiDeps) {
    let listener = match tokio::net::TcpListener::bind(&cfg.bind).await {
        Ok(l) => l,
        Err(e) => {
            tracing::warn!(target: "pg2osync::api", "cannot bind {}: {e}", cfg.bind);
            return;
        }
    };
    tracing::info!(target: "pg2osync::api",
        "read-your-writes endpoint on http://{}/synced", cfg.bind);

    let ApiDeps {
        names,
        mut registrations,
        trace_link,
    } = deps;
    let state = Arc::new(ApiState {
        cfg,
        names,
        trace_link,
        sources: RwLock::new(BTreeMap::new()),
    });

    {
        let state = state.clone();
        tokio::spawn(async move {
            while let Some((name, endpoints)) = registrations.recv().await {
                state.sources.write().unwrap().insert(name, endpoints);
            }
        });
    }

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
    let name = match state.choose(query) {
        Ok(name) => name,
        Err(response) => return response,
    };
    let Some(source) = state.endpoints(&name) else {
        return (
            "503 Service Unavailable",
            error_body(&format!("source {name:?} has not connected yet")),
        );
    };

    // Omitting the position means "everything committed before this request":
    // pg2osync reads the source's position itself, so the caller needs neither
    // the privilege to read it nor any knowledge of LSN or binlog syntax.
    let (requested, raw_position) = match query_param(query, "position") {
        Some(raw) => match (source.parse_position)(&raw) {
            Some(token) => (token, raw),
            None => {
                return (
                    "400 Bad Request",
                    error_body(&format!("cannot parse position {raw:?}")),
                );
            }
        },
        None => match &source.current_position {
            Some(read) => match read().await {
                Some(token) => (token, (source.render_position)(token)),
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

    // The caller's own trace continues into the write they are waiting for,
    // which is the point of the endpoint: their request and the batch that
    // satisfies it belong to one timeline rather than two unrelated ones.
    let wait = tracing::info_span!(
        target: "pg2osync::api",
        "synced",
        position = %raw_position,
        synced = tracing::field::Empty,
    );
    if let Some(link) = &state.trace_link
        && let Some(traceparent) = crate::http::header(request, "traceparent")
    {
        link(&wait, traceparent);
    }

    let started = Instant::now();
    let reached = wait_for_position(&source, requested, timeout)
        .instrument(wait.clone())
        .await;
    wait.record("synced", reached);
    if reached && refresh {
        // an accepted write is not searchable until the target refreshes, so
        // without this the guarantee would stop one step short of useful
        let indices = source.indices.borrow().clone();
        if let Err(e) = source.sink.refresh(&indices).await {
            tracing::warn!(target: "pg2osync::api", "refresh failed: {e}");
            return (
                "503 Service Unavailable",
                error_body(&format!("refresh failed: {e}")),
            );
        }
    }

    let confirmed = (*source.acked.borrow()).map(|lsn| (source.render_position)(lsn.0));
    let body = format!(
        r#"{{"source":{},"synced":{reached},"requested":{},"confirmed":{},"waited_ms":{}}}"#,
        json_string(&name),
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
async fn wait_for_position(source: &SourceEndpoints, requested: u64, timeout: Duration) -> bool {
    let grace = GRACE_BEFORE_NUDGE.min(timeout);
    if wait_for(source.acked.clone(), requested, grace).await {
        return true;
    }
    if let Some(nudge) = &source.nudge {
        nudge().await;
    }
    wait_for(
        source.acked.clone(),
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

    /// A state with `names` configured and `connected` registered.
    fn state(names: &[&str], connected: &[&str]) -> ApiState {
        let sources = connected
            .iter()
            .map(|name| ((*name).to_string(), endpoints()))
            .collect();
        ApiState {
            cfg: ApiConfig {
                bind: "127.0.0.1:0".into(),
                token: None,
            },
            names: names.iter().map(|n| (*n).to_string()).collect(),
            trace_link: None,
            sources: RwLock::new(sources),
        }
    }

    /// The routing tests never reach the target: what is under test is which
    /// source a request is answered by, and every one of them is already past
    /// its position.
    struct UnusedSink;

    #[async_trait::async_trait]
    impl Sink for UnusedSink {
        async fn ensure_ready(
            &self,
            _tables: &[pg2osync_core::sink::IndexSpec],
        ) -> Result<(), pg2osync_core::error::CoreError> {
            Ok(())
        }
        async fn get_documents(
            &self,
            _index: &str,
            _ids: &[(String, Option<String>)],
        ) -> Result<Vec<Option<serde_json::Value>>, pg2osync_core::error::CoreError> {
            Ok(Vec::new())
        }
        async fn write(
            &self,
            _batch: Vec<pg2osync_core::sink::LsnOp>,
        ) -> Result<pg2osync_core::sink::SinkAck, pg2osync_core::error::CoreError> {
            Ok(pg2osync_core::sink::SinkAck::written(Lsn(0)))
        }
        async fn truncate_index(
            &self,
            _index: &str,
            _version: Option<u64>,
            _only: Option<(&str, &str)>,
        ) -> Result<(), pg2osync_core::error::CoreError> {
            Ok(())
        }
        async fn refresh(
            &self,
            _indices: &[String],
        ) -> Result<(), pg2osync_core::error::CoreError> {
            Ok(())
        }
        async fn write_checkpoint(
            &self,
            _checkpoint: &pg2osync_core::checkpoint::Checkpoint,
        ) -> Result<(), pg2osync_core::error::CoreError> {
            Ok(())
        }
        async fn read_checkpoint(
            &self,
            _stream: &pg2osync_core::checkpoint::StreamId,
        ) -> Result<Option<pg2osync_core::checkpoint::Checkpoint>, pg2osync_core::error::CoreError>
        {
            Ok(None)
        }
        async fn health(
            &self,
        ) -> Result<pg2osync_core::sink::Health, pg2osync_core::error::CoreError> {
            Ok(pg2osync_core::sink::Health::Up)
        }
    }

    fn endpoints() -> SourceEndpoints {
        let (_tx, acked) = watch::channel(Some(Lsn(10)));
        SourceEndpoints {
            acked,
            parse_position: Arc::new(|text| text.parse::<u64>().ok()),
            render_position: Arc::new(|token| token.to_string()),
            sink: Arc::new(UnusedSink),
            nudge: None,
            current_position: None,
            indices: watch::channel(Arc::new(Vec::new())).1,
        }
    }

    async fn synced(state: &ApiState, query: &str) -> (&'static str, String) {
        handle(
            state,
            &format!("GET /synced{query} HTTP/1.1\r\nHost: h\r\n\r\n"),
        )
        .await
    }

    #[tokio::test]
    async fn one_source_needs_no_name() {
        // what every single-config deployment asks for, unchanged
        let (status, body) = synced(&state(&["orders"], &["orders"]), "?position=1").await;
        assert_eq!(status, "200 OK");
        assert!(body.contains(r#""source":"orders""#), "{body}");
    }

    #[tokio::test]
    async fn several_sources_and_no_name_is_refused() {
        let (status, body) = synced(&state(&["orders", "users"], &["orders", "users"]), "").await;
        assert_eq!(status, "400 Bad Request");
        assert!(body.contains("orders") && body.contains("users"), "{body}");
    }

    #[tokio::test]
    async fn a_named_source_is_answered_and_an_unknown_one_is_not() {
        let state = state(&["orders", "users"], &["orders", "users"]);
        assert_eq!(synced(&state, "?source=users&position=1").await.0, "200 OK");
        let (status, body) = synced(&state, "?source=typo&position=1").await;
        assert_eq!(status, "404 Not Found");
        assert!(body.contains("orders"), "{body}");
    }

    #[tokio::test]
    async fn a_source_that_has_not_connected_yet_says_so() {
        // the listener opens before the sources do, and a caller has to be
        // able to tell "not yet" from "no such source"
        let (status, body) = synced(&state(&["orders"], &[]), "?position=1").await;
        assert_eq!(status, "503 Service Unavailable");
        assert!(body.contains("has not connected yet"), "{body}");
    }

    #[test]
    fn json_values_are_escaped() {
        assert_eq!(json_string(r#"a"b\c"#), r#""a\"b\\c""#);
    }
}
