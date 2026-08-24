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
    pub lsn_current: Mutex<Option<u64>>,
    pub lsn_confirmed: Mutex<Option<u64>>,
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
        out.push_str(&events.join("\n"));
        out.push('\n');
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
            push(
                &mut out,
                "pg2osync_latency_ms",
                "End-to-end sync latency (commit->indexed)",
                "summary",
                format!(
                    "{{quantile=\"0.5\"}} {} {{quantile=\"0.9\"}} {} {{quantile=\"0.99\"}} {}",
                    q(50),
                    q(90),
                    q(99)
                ),
            );
        }
        drop(lat);
        if let Some(l) = self.lsn_confirmed.lock().unwrap().as_ref() {
            push(
                &mut out,
                "pg2osync_lsn_confirmed",
                "Highest durably indexed LSN",
                "gauge",
                l.to_string(),
            );
        }
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
