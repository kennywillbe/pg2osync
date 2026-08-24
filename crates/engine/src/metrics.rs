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
    pub sink_errors_total: AtomicU64,
    pub reconnects_total: AtomicU64,
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
        self.events_total
            .lock()
            .unwrap()
            .entry(kind.to_string())
            .or_insert_with(|| AtomicU64::new(0))
            .fetch_add(1, Ordering::Relaxed);
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
            "pg2osync_sink_errors_total",
            "Sink write failures",
            "counter",
            self.sink_errors_total.load(Ordering::Relaxed).to_string(),
        );
        push(
            &mut out,
            "pg2osync_reconnects_total",
            "Source reconnect attempts",
            "counter",
            self.reconnects_total.load(Ordering::Relaxed).to_string(),
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

/// Serve /metrics until the process exits. Errors are logged, never fatal:
/// losing a metrics scrape must not take down replication.
pub async fn serve(bind: &str, metrics: SharedMetrics) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = match tokio::net::TcpListener::bind(bind).await {
        Ok(l) => l,
        Err(e) => {
            tracing::warn!(target: "pg2osync::metrics", "cannot bind {bind}: {e}");
            return;
        }
    };
    tracing::info!(target: "pg2osync::metrics", "metrics listening on http://{bind}/metrics");
    loop {
        let Ok((mut sock, _)) = listener.accept().await else {
            continue;
        };
        let m = metrics.clone();
        tokio::spawn(async move {
            let mut buf = [0u8; 1024];
            let _ = sock.read(&mut buf).await;
            let body = m.render();
            let resp = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: text/plain; version=0.0.4\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            let _ = sock.write_all(resp.as_bytes()).await;
        });
    }
}
