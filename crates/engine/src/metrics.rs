//! Minimal Prometheus text exposition served over a tiny TCP listener.
//!
//! Hand-rolled instead of pulling prometheus/axum: six counters and one
//! histogram summary do not justify the dependency weight (YAGNI).

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

#[derive(Default)]
pub struct Metrics {
    pub events_total: Mutex<HashMap<String, AtomicU64>>,
    pub batches_flushed: AtomicU64,
    /// Reads of the target to complete an update whose unchanged TOASTed
    /// columns arrived as markers. Each one is a round-trip in the middle of
    /// the pipeline, so it is worth being able to see how many there are.
    pub toast_readbacks_total: AtomicU64,
    pub sink_errors_total: AtomicU64,
    /// Documents the target refused and that were recorded rather than written.
    /// Non-zero means data is in the quarantine store and not in the index.
    pub rejected_total: AtomicU64,
    pub reconnects_total: AtomicU64,
    /// 1 while the source is streaming, 0 while it is being retried. The
    /// counter says how often it broke; this says whether it is broken now.
    pub source_connected: AtomicU64,
    /// end-to-end latency samples in ms (commit -> indexed), capped ring
    pub latencies_ms: Mutex<Vec<u64>>,
    /// Highest position token seen from the source (WAL LSN / binlog offset).
    pub position_current: AtomicU64,
    /// Highest position token whose checkpoint is durably persisted.
    pub position_confirmed: AtomicU64,
}

pub type SharedMetrics = Arc<Metrics>;

impl Metrics {
    pub fn incr_event(&self, kind: &str) {
        self.incr_event_by(kind, 1);
    }

    pub fn incr_event_by(&self, kind: &str, n: u64) {
        self.events_total
            .lock()
            .unwrap()
            .entry(kind.to_string())
            .or_insert_with(|| AtomicU64::new(0))
            .fetch_add(n, Ordering::Relaxed);
    }

    pub fn set_source_connected(&self, connected: bool) {
        self.source_connected
            .store(u64::from(connected), Ordering::Relaxed);
    }

    pub fn set_current_position(&self, token: u64) {
        self.position_current.store(token, Ordering::Relaxed);
    }

    pub fn set_confirmed_position(&self, token: u64) {
        self.position_confirmed.store(token, Ordering::Relaxed);
    }

    pub fn record_latency(&self, ms: u64) {
        let mut l = self.latencies_ms.lock().unwrap();
        if l.len() < 10_000 {
            l.push(ms);
        }
    }

    pub fn render(&self) -> String {
        let mut out = String::new();
        let push = |out: &mut String, name: &str, help: &str, typ: &str, val: String| {
            out.push_str(&format!(
                "# HELP {name} {help}\n# TYPE {name} {typ}\n{name} {val}\n"
            ));
        };
        let events: Vec<String> = self
            .events_total
            .lock()
            .unwrap()
            .iter()
            .map(|(k, v)| {
                format!(
                    "pg2osync_events_total{{type=\"{k}\"}} {}",
                    v.load(Ordering::Relaxed)
                )
            })
            .collect();
        if !events.is_empty() {
            out.push_str("# HELP pg2osync_events_total Change events received from the source\n");
            out.push_str("# TYPE pg2osync_events_total counter\n");
            out.push_str(&events.join("\n"));
            out.push('\n');
        }
        push(
            &mut out,
            "pg2osync_batches_flushed",
            "Batches written to the sink",
            "counter",
            self.batches_flushed.load(Ordering::Relaxed).to_string(),
        );
        push(
            &mut out,
            "pg2osync_toast_readbacks_total",
            "Reads of the target to complete unchanged TOASTed columns",
            "counter",
            self.toast_readbacks_total
                .load(Ordering::Relaxed)
                .to_string(),
        );
        push(
            &mut out,
            "pg2osync_sink_errors_total",
            "Sink write failures",
            "counter",
            self.sink_errors_total.load(Ordering::Relaxed).to_string(),
        );
        push(
            &mut out,
            "pg2osync_rejected_total",
            "Documents the target refused that were quarantined instead of written",
            "counter",
            self.rejected_total.load(Ordering::Relaxed).to_string(),
        );
        push(
            &mut out,
            "pg2osync_reconnects_total",
            "Source reconnect attempts",
            "counter",
            self.reconnects_total.load(Ordering::Relaxed).to_string(),
        );
        push(
            &mut out,
            "pg2osync_source_connected",
            "1 while the source is streaming, 0 while reconnecting",
            "gauge",
            self.source_connected.load(Ordering::Relaxed).to_string(),
        );
        let lat = self.latencies_ms.lock().unwrap();
        if !lat.is_empty() {
            let mut sorted = lat.clone();
            sorted.sort_unstable();
            let q = |p: usize| sorted[(p * (sorted.len() - 1)) / 100];
            // one line per quantile: a summary with several quantiles on a
            // single line is not valid exposition format
            out.push_str("# HELP pg2osync_latency_ms End-to-end sync latency (commit->indexed)\n");
            out.push_str("# TYPE pg2osync_latency_ms summary\n");
            // literal labels: "0.{p}" would render p50 as the unconventional
            // quantile label 0.50, which scrapers and dashboards do not expect
            for (label, p) in [("0.5", 50usize), ("0.9", 90), ("0.99", 99)] {
                out.push_str(&format!(
                    "pg2osync_latency_ms{{quantile=\"{label}\"}} {}\n",
                    q(p)
                ));
            }
            out.push_str(&format!("pg2osync_latency_ms_count {}\n", sorted.len()));
        }
        drop(lat);
        let current = self.position_current.load(Ordering::Relaxed);
        let confirmed = self.position_confirmed.load(Ordering::Relaxed);
        push(
            &mut out,
            "pg2osync_position_current",
            "Highest source position token received (WAL LSN or binlog offset)",
            "gauge",
            current.to_string(),
        );
        push(
            &mut out,
            "pg2osync_position_confirmed",
            "Highest source position token durably checkpointed",
            "gauge",
            confirmed.to_string(),
        );
        push(
            &mut out,
            "pg2osync_position_lag",
            "Position tokens received but not yet checkpointed",
            "gauge",
            current.saturating_sub(confirmed).to_string(),
        );
        out
    }
}

/// What the metrics port answers, and with what.
///
/// Pure so the routing can be tested without a socket: the endpoint is the one
/// piece of the process an operator points the outside world at.
fn respond(request: &str, token: Option<&str>, render: impl FnOnce() -> String) -> Response {
    let Some(target) = crate::http::request_target(request) else {
        return Response::text("405 Method Not Allowed", "only GET is served here");
    };
    let (path, _) = crate::http::split_target(target);
    // probes have to reach the process before it can prove who is asking, and a
    // liveness check that fails closed on a missing token would restart a
    // perfectly healthy pipeline
    if path == "/healthz" {
        return Response::text("200 OK", "ok");
    }
    if path != "/metrics" {
        return Response::text("404 Not Found", "only /metrics is served here");
    }
    if let Some(expected) = token
        && !crate::http::authorized(request, expected)
    {
        return Response::text("401 Unauthorized", "missing or invalid token");
    }
    Response {
        status: "200 OK",
        content_type: "text/plain; version=0.0.4",
        body: render(),
    }
}

struct Response {
    status: &'static str,
    content_type: &'static str,
    body: String,
}

impl Response {
    fn text(status: &'static str, body: &str) -> Self {
        Self {
            status,
            content_type: "text/plain; charset=utf-8",
            body: format!("{body}\n"),
        }
    }

    fn render(&self) -> String {
        format!(
            "HTTP/1.1 {}\r\nContent-Type: {}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            self.status,
            self.content_type,
            self.body.len(),
            self.body
        )
    }
}

/// Serve /metrics until the process exits. Errors are logged, never fatal:
/// losing a metrics scrape must not take down replication.
///
/// `token`, when set, is required on /metrics. /healthz is never authenticated.
pub async fn serve(bind: &str, metrics: SharedMetrics, token: Option<String>) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = match tokio::net::TcpListener::bind(bind).await {
        Ok(l) => l,
        Err(e) => {
            tracing::warn!(target: "pg2osync::metrics", "cannot bind {bind}: {e}");
            return;
        }
    };
    if token.is_none() && !crate::http::is_loopback(bind) {
        // the exposition names every table being synced and how far behind the
        // pipeline is; that is operational detail, and reaching this port
        // should be a decision rather than a consequence of the bind address
        tracing::warn!(target: "pg2osync::metrics",
            "metrics are served on {bind} without a token: anything that can \
             route to this port can read them. Set [metrics] token_env, or \
             keep the port on an internal network.");
    }
    tracing::info!(target: "pg2osync::metrics", "metrics listening on http://{bind}/metrics");
    let token = token.map(std::sync::Arc::new);
    loop {
        let Ok((mut sock, _)) = listener.accept().await else {
            continue;
        };
        let m = metrics.clone();
        let token = token.clone();
        tokio::spawn(async move {
            let mut buf = [0u8; 2048];
            let Ok(read) = sock.read(&mut buf).await else {
                return;
            };
            let request = String::from_utf8_lossy(&buf[..read]);
            let response = respond(&request, token.as_deref().map(String::as_str), || {
                m.render()
            });
            let _ = sock.write_all(response.render().as_bytes()).await;
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn get(path: &str, headers: &str) -> String {
        format!("GET {path} HTTP/1.1\r\nHost: h\r\n{headers}\r\n\r\n")
    }

    fn body(request: &str, token: Option<&str>) -> (&'static str, String) {
        let r = respond(request, token, || "metric 1\n".into());
        (r.status, r.body)
    }

    #[test]
    fn metrics_are_served_when_no_token_is_configured() {
        let (status, body) = body(&get("/metrics", ""), None);
        assert_eq!(status, "200 OK");
        assert_eq!(body, "metric 1\n");
    }

    #[test]
    fn a_token_is_required_once_one_is_configured() {
        assert_eq!(body(&get("/metrics", ""), Some("s")).0, "401 Unauthorized");
        assert_eq!(
            body(&get("/metrics", "Authorization: Bearer s"), Some("s")).0,
            "200 OK"
        );
    }

    #[test]
    fn the_exposition_is_not_returned_for_every_path() {
        // the endpoint used to answer any path with the full exposition
        let (status, body) = body(&get("/", ""), None);
        assert_eq!(status, "404 Not Found");
        assert!(!body.contains("metric 1"));
    }

    #[test]
    fn probes_reach_health_without_a_token() {
        // a liveness probe cannot carry one, and failing it would restart a
        // pipeline that is working
        let (status, body) = body(&get("/healthz", ""), Some("s"));
        assert_eq!(status, "200 OK");
        assert_eq!(body, "ok\n");
    }

    #[test]
    fn only_get_is_answered() {
        let post = "POST /metrics HTTP/1.1\r\nHost: h\r\n\r\n";
        assert_eq!(body(post, None).0, "405 Method Not Allowed");
    }
}
