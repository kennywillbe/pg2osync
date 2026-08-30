//! Minimal Prometheus text exposition served over a tiny TCP listener.
//!
//! Hand-rolled instead of pulling prometheus/axum: six counters and one
//! histogram summary do not justify the dependency weight (YAGNI).
//!
//! One process may run several sources, so what is served is a [`Registry`]
//! of them rather than one source's counters: the exposition is assembled
//! once, with `source` on every series, and health is answered per source.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};
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
    /// Values a configured transform could not convert, indexed as they
    /// arrived. Non-zero means the index holds a shape the configuration did
    /// not ask for — usually a wrong `date` format, or a column that is not
    /// what it looks like.
    pub transform_unconverted_total: AtomicU64,
    /// Tables whose shape changed under the running pipeline, by qualified
    /// name. Applying the change is refused, so this is how an operator learns
    /// that the index and the table now disagree; before it, the only report
    /// was a log line nothing can alert on.
    pub schema_drift_total: Mutex<HashMap<String, AtomicU64>>,
    /// Configuration reloads by outcome: `applied`, `invalid` (the file did
    /// not load, so nothing changed), `refused` (something in it needs a
    /// restart or a rebuild, and the running definition was kept) or `failed`.
    /// A pipeline whose file is edited by a deployment tool is one an alert
    /// should be able to ask this of.
    pub config_reloads_total: Mutex<HashMap<String, AtomicU64>>,
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
    /// What each replication slot on the source is holding, refreshed by a
    /// poller rather than counted here.
    ///
    /// This is the number that takes a database down, and nothing else here
    /// reports it: `position_lag` is the gap between received and checkpointed,
    /// which stays kilobytes even while a slot pins gigabytes.
    pub slots: Mutex<Vec<SlotState>>,
    /// Where this source is in its life, as a number so it can be read
    /// without a lock from a scrape or a health probe.
    state: AtomicU8,
}

/// One replication slot as the source describes it.
#[derive(Debug, Clone)]
pub struct SlotState {
    pub name: String,
    pub active: bool,
    /// WAL the slot forbids recycling, in bytes. `None` when the slot has never
    /// reserved a position.
    pub retained_bytes: Option<u64>,
    /// How much more WAL can be written before the slot is lost. The server
    /// leaves this null when `max_slot_wal_keep_size` is unlimited — which is
    /// precisely the case that fills a disk, so its absence is the warning.
    pub safe_wal_size: Option<u64>,
    /// `reserved`, `extended`, `unreserved` or `lost`.
    pub wal_status: String,
}

/// Where one source is in its life.
///
/// The process runs several of them and they fail apart from each other, so
/// "is the pipeline up" is no longer a question with one answer: this is what
/// a scrape and a per-source health probe both read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceState {
    /// Registered, doing its setup: connecting, bootstrapping, ensuring
    /// indices exist.
    Starting,
    /// Streaming with an initial load still copying rows beside it.
    Loading,
    Streaming,
    /// The stream broke and the retry policy has not given up.
    Reconnecting,
    /// It stopped and will not come back without a restart.
    Halted,
    /// It drained on a shutdown signal.
    Stopped,
}

impl SourceState {
    /// Every state, so the state set can report a 0 for the ones this source
    /// is not in. A series that simply disappears leaves an alert evaluating
    /// its last value for as long as the scraper remembers it.
    pub const ALL: [SourceState; 6] = [
        Self::Starting,
        Self::Loading,
        Self::Streaming,
        Self::Reconnecting,
        Self::Halted,
        Self::Stopped,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Starting => "starting",
            Self::Loading => "loading",
            Self::Streaming => "streaming",
            Self::Reconnecting => "reconnecting",
            Self::Halted => "halted",
            Self::Stopped => "stopped",
        }
    }

    /// Whether the source is still expected to produce data. A health probe
    /// asks exactly this.
    pub fn is_live(self) -> bool {
        !matches!(self, Self::Halted | Self::Stopped)
    }

    fn code(self) -> u8 {
        match self {
            Self::Starting => 0,
            Self::Loading => 1,
            Self::Streaming => 2,
            Self::Reconnecting => 3,
            Self::Halted => 4,
            Self::Stopped => 5,
        }
    }

    fn from_code(code: u8) -> Self {
        Self::ALL
            .into_iter()
            .find(|state| state.code() == code)
            .unwrap_or(Self::Starting)
    }
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

    /// `table` is the qualified `schema.table` the drift was observed on.
    pub fn incr_schema_drift(&self, table: &str) {
        self.schema_drift_total
            .lock()
            .unwrap()
            .entry(table.to_string())
            .or_insert_with(|| AtomicU64::new(0))
            .fetch_add(1, Ordering::Relaxed);
    }

    /// `result` is one reload's outcome, one of the four documented above.
    pub fn incr_config_reload(&self, result: &str) {
        self.config_reloads_total
            .lock()
            .unwrap()
            .entry(result.to_string())
            .or_insert_with(|| AtomicU64::new(0))
            .fetch_add(1, Ordering::Relaxed);
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

    /// Replace what is known about the source's slots.
    ///
    /// Wholesale rather than per slot: a slot that has been dropped must stop
    /// being reported, and a stale gauge for it would keep an alert firing
    /// against something that no longer exists.
    pub fn set_slots(&self, slots: Vec<SlotState>) {
        *self.slots.lock().unwrap() = slots;
    }

    pub fn set_state(&self, state: SourceState) {
        self.state.store(state.code(), Ordering::Relaxed);
    }

    pub fn state(&self) -> SourceState {
        SourceState::from_code(self.state.load(Ordering::Relaxed))
    }

    pub fn record_latency(&self, ms: u64) {
        let mut l = self.latencies_ms.lock().unwrap();
        if l.len() < 10_000 {
            l.push(ms);
        }
    }

    /// Add this source's series to a shared exposition.
    ///
    /// Rendering to a string per source and concatenating would repeat every
    /// family's `# HELP` and `# TYPE` and scatter its series, which no scraper
    /// accepts. The families are shared instead, and the source a series came
    /// from is a label on it.
    pub fn write_series(&self, out: &mut Exposition, source: &str) {
        // a source name is `^[A-Za-z0-9_-]+$`, so nothing in it needs escaping
        let src = format!("source=\"{source}\"");
        for (kind, count) in self.events_total.lock().unwrap().iter() {
            out.push(
                "pg2osync_events_total",
                "Change events received from the source",
                "counter",
                &format!("{src},type=\"{kind}\""),
                count.load(Ordering::Relaxed),
            );
        }
        out.push(
            "pg2osync_batches_flushed",
            "Batches written to the sink",
            "counter",
            &src,
            self.batches_flushed.load(Ordering::Relaxed),
        );
        out.push(
            "pg2osync_toast_readbacks_total",
            "Reads of the target to complete unchanged TOASTed columns",
            "counter",
            &src,
            self.toast_readbacks_total.load(Ordering::Relaxed),
        );
        out.push(
            "pg2osync_sink_errors_total",
            "Sink write failures",
            "counter",
            &src,
            self.sink_errors_total.load(Ordering::Relaxed),
        );
        out.push(
            "pg2osync_rejected_total",
            "Documents the target refused that were quarantined instead of written",
            "counter",
            &src,
            self.rejected_total.load(Ordering::Relaxed),
        );
        out.push(
            "pg2osync_transform_unconverted_total",
            "Values a configured transform could not convert, indexed as they were",
            "counter",
            &src,
            self.transform_unconverted_total.load(Ordering::Relaxed),
        );
        for (table, count) in self.schema_drift_total.lock().unwrap().iter() {
            out.push(
                "pg2osync_schema_drift_total",
                "Times a table's columns changed under the running pipeline; the change is \
                 never applied, so the index and the table disagree until the index is rebuilt",
                "counter",
                &format!("{src},table=\"{table}\""),
                count.load(Ordering::Relaxed),
            );
        }
        for (result, count) in self.config_reloads_total.lock().unwrap().iter() {
            out.push(
                "pg2osync_config_reloads_total",
                "Configuration reloads by outcome: applied, invalid, refused or failed",
                "counter",
                &format!("{src},result=\"{result}\""),
                count.load(Ordering::Relaxed),
            );
        }
        out.push(
            "pg2osync_reconnects_total",
            "Source reconnect attempts",
            "counter",
            &src,
            self.reconnects_total.load(Ordering::Relaxed),
        );
        out.push(
            "pg2osync_source_connected",
            "1 while the source is streaming, 0 while reconnecting",
            "gauge",
            &src,
            self.source_connected.load(Ordering::Relaxed),
        );
        let state = self.state();
        for known in SourceState::ALL {
            // A state set rather than a number: an alert wants to name
            // `halted`, not remember which integer stood for it.
            out.push(
                "pg2osync_source_state",
                "Where a source is: starting, loading, streaming, reconnecting, halted or stopped",
                "gauge",
                &format!("{src},state=\"{}\"", known.as_str()),
                u8::from(known == state),
            );
        }
        for slot in self.slots.lock().unwrap().iter() {
            // the slot carries its source as well as its name: two hosts can
            // each have a slot called pg2osync_slot
            let labels = format!("{src},slot=\"{}\"", slot.name);
            if let Some(bytes) = slot.retained_bytes {
                out.push(
                    "pg2osync_slot_retained_bytes",
                    "WAL bytes a replication slot forbids the source from recycling",
                    "gauge",
                    &labels,
                    bytes,
                );
            }
            if let Some(bytes) = slot.safe_wal_size {
                out.push(
                    "pg2osync_slot_safe_wal_size_bytes",
                    "WAL bytes that may still be written before the slot is lost; \
                     absent when max_slot_wal_keep_size is unlimited",
                    "gauge",
                    &labels,
                    bytes,
                );
            }
            out.push(
                "pg2osync_slot_active",
                "1 while something is streaming the slot",
                "gauge",
                &labels,
                u8::from(slot.active),
            );
            for known in ["reserved", "extended", "unreserved", "lost"] {
                out.push(
                    "pg2osync_slot_wal_status",
                    "The source's own assessment of the slot: reserved, extended, \
                     unreserved or lost",
                    "gauge",
                    &format!("{labels},status=\"{known}\""),
                    u8::from(slot.wal_status == known),
                );
            }
        }
        let lat = self.latencies_ms.lock().unwrap();
        if !lat.is_empty() {
            let mut sorted = lat.clone();
            sorted.sort_unstable();
            let q = |p: usize| sorted[(p * (sorted.len() - 1)) / 100];
            // one line per quantile: a summary with several quantiles on a
            // single line is not valid exposition format
            for (label, p) in [("0.5", 50usize), ("0.9", 90), ("0.99", 99)] {
                out.push(
                    "pg2osync_latency_ms",
                    "End-to-end sync latency (commit->indexed)",
                    "summary",
                    &format!("{src},quantile=\"{label}\""),
                    q(p),
                );
            }
            // `_count` belongs to the summary rather than being a family of
            // its own, so it carries no HELP or TYPE and stays in this block
            out.push_line(
                "pg2osync_latency_ms",
                format!("pg2osync_latency_ms_count{{{src}}} {}", sorted.len()),
            );
        }
        drop(lat);
        let current = self.position_current.load(Ordering::Relaxed);
        let confirmed = self.position_confirmed.load(Ordering::Relaxed);
        out.push(
            "pg2osync_position_current",
            "Highest source position token received (WAL LSN or binlog offset)",
            "gauge",
            &src,
            current,
        );
        out.push(
            "pg2osync_position_confirmed",
            "Highest source position token durably checkpointed",
            "gauge",
            &src,
            confirmed,
        );
        out.push(
            "pg2osync_position_lag",
            "Position tokens received but not yet checkpointed",
            "gauge",
            &src,
            current.saturating_sub(confirmed),
        );
    }

    /// One source's exposition on its own. The process serves
    /// [`Registry::render`]; this is what a test of one source's series reads.
    pub fn render(&self, source: &str) -> String {
        let mut out = Exposition::default();
        self.write_series(&mut out, source);
        out.render()
    }
}

/// A Prometheus text exposition being built by several sources at once.
///
/// A family is declared by the first series that needs it and every later
/// series joins it, which is what keeps one `# HELP`/`# TYPE` per family and
/// its series contiguous however many sources contribute to it.
#[derive(Default)]
pub struct Exposition {
    families: Vec<Family>,
    index: HashMap<String, usize>,
}

struct Family {
    name: String,
    help: String,
    kind: &'static str,
    lines: Vec<String>,
}

impl Exposition {
    /// One series of `name`, declaring the family if this is the first.
    /// `labels` is the label set without its braces.
    pub fn push(
        &mut self,
        name: &str,
        help: &str,
        kind: &'static str,
        labels: &str,
        value: impl std::fmt::Display,
    ) {
        let line = format!("{name}{{{labels}}} {value}");
        self.family(name, help, kind).lines.push(line);
    }

    /// A line that belongs to `name`'s family under a different metric name —
    /// a summary's `_count`, which must not carry HELP or TYPE of its own.
    pub fn push_line(&mut self, name: &str, line: String) {
        if let Some(&at) = self.index.get(name) {
            self.families[at].lines.push(line);
        }
    }

    fn family(&mut self, name: &str, help: &str, kind: &'static str) -> &mut Family {
        let families = &mut self.families;
        let at = *self.index.entry(name.to_string()).or_insert_with(|| {
            families.push(Family {
                name: name.to_string(),
                // a help string wrapped across source lines is one line here:
                // a newline inside it would end the HELP and orphan the rest
                help: help.replace('\n', " "),
                kind,
                lines: Vec::new(),
            });
            families.len() - 1
        });
        &mut self.families[at]
    }

    /// Families in the order they were first declared: an exposition that
    /// reorders itself between scrapes is one nobody can diff by eye.
    pub fn render(&self) -> String {
        let mut out = String::new();
        for family in &self.families {
            out.push_str(&format!(
                "# HELP {} {}\n# TYPE {} {}\n",
                family.name, family.help, family.name, family.kind
            ));
            for line in &family.lines {
                out.push_str(line);
                out.push('\n');
            }
        }
        out
    }
}

/// Every source the process is running, and the metrics each of them keeps.
///
/// One process serves one `/metrics`, so the exposition is assembled here
/// rather than by whichever source happened to answer the scrape.
#[derive(Default)]
pub struct Registry {
    sources: Mutex<Vec<(String, SharedMetrics)>>,
}

impl Registry {
    /// Give a source its metrics. Registration happens before the source
    /// connects, so a source that never comes up is still reported — as
    /// `starting`, and then as `halted`.
    pub fn register(&self, name: &str) -> SharedMetrics {
        let metrics: SharedMetrics = Arc::new(Metrics::default());
        self.sources
            .lock()
            .unwrap()
            .push((name.to_string(), metrics.clone()));
        metrics
    }

    pub fn render(&self) -> String {
        let mut out = Exposition::default();
        for (name, metrics) in self.sources.lock().unwrap().iter() {
            metrics.write_series(&mut out, name);
        }
        out.render()
    }

    /// The state of one source, or `None` when no source has that name.
    pub fn state_of(&self, name: &str) -> Option<SourceState> {
        self.sources
            .lock()
            .unwrap()
            .iter()
            .find(|(known, _)| known == name)
            .map(|(_, metrics)| metrics.state())
    }

    pub fn names(&self) -> Vec<String> {
        self.sources
            .lock()
            .unwrap()
            .iter()
            .map(|(name, _)| name.clone())
            .collect()
    }
}

/// What the metrics port answers, and with what.
///
/// Pure so the routing can be tested without a socket: the endpoint is the one
/// piece of the process an operator points the outside world at.
fn respond(request: &str, token: Option<&str>, registry: &Registry) -> Response {
    let Some(target) = crate::http::request_target(request) else {
        return Response::text("405 Method Not Allowed", "only GET is served here");
    };
    let (path, _) = crate::http::split_target(target);
    // probes have to reach the process before it can prove who is asking, and a
    // liveness check that fails closed on a missing token would restart a
    // perfectly healthy pipeline
    if path == "/healthz" {
        // Liveness, and only liveness: this answers 200 while the process is
        // up, whatever its sources are doing. Failing it because one source of
        // thirty is halted would have the kubelet restart the other
        // twenty-nine, in a loop that cannot fix a permanent rejection.
        return Response::text("200 OK", "ok");
    }
    if let Some(name) = path.strip_prefix("/healthz/") {
        return match registry.state_of(name) {
            Some(state) if state.is_live() => Response::text("200 OK", state.as_str()),
            Some(state) => Response::text("503 Service Unavailable", state.as_str()),
            None => Response::text(
                "404 Not Found",
                &format!("no source is called {name:?}; this process runs: {}", {
                    let names = registry.names();
                    if names.is_empty() {
                        "none".to_string()
                    } else {
                        names.join(", ")
                    }
                }),
            ),
        };
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
        body: registry.render(),
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
/// `token`, when set, is required on /metrics. Neither `/healthz` nor
/// `/healthz/<name>` is ever authenticated: a probe has nowhere to keep a
/// token, and a health check that fails closed restarts a healthy pipeline.
pub async fn serve(bind: &str, registry: Arc<Registry>, token: Option<String>) {
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
        let registry = registry.clone();
        let token = token.clone();
        tokio::spawn(async move {
            let mut buf = [0u8; 2048];
            let Ok(read) = sock.read(&mut buf).await else {
                return;
            };
            let request = String::from_utf8_lossy(&buf[..read]);
            let response = respond(
                &request,
                token.as_deref().map(String::as_str),
                registry.as_ref(),
            );
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

    /// A registry with one source that has flushed one batch, which is enough
    /// for a routing test to tell an exposition from anything else.
    fn one_source() -> Registry {
        let registry = Registry::default();
        registry
            .register("orders")
            .batches_flushed
            .fetch_add(1, Ordering::Relaxed);
        registry
    }

    fn body(request: &str, token: Option<&str>) -> (&'static str, String) {
        let r = respond(request, token, &one_source());
        (r.status, r.body)
    }

    #[test]
    fn metrics_are_served_when_no_token_is_configured() {
        let (status, body) = body(&get("/metrics", ""), None);
        assert_eq!(status, "200 OK");
        assert!(body.contains("pg2osync_batches_flushed{source=\"orders\"} 1"));
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
        assert!(!body.contains("pg2osync_batches_flushed"));
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
    fn liveness_is_not_the_health_of_the_sources() {
        let registry = Registry::default();
        registry.register("orders").set_state(SourceState::Halted);
        registry.register("users").set_state(SourceState::Streaming);
        let health = |path: &str| respond(&get(path, ""), None, &registry).status;
        assert_eq!(
            health("/healthz"),
            "200 OK",
            "a halted source must not restart the ones that are working"
        );
        assert_eq!(health("/healthz/orders"), "503 Service Unavailable");
        assert_eq!(health("/healthz/users"), "200 OK");
    }

    #[test]
    fn an_unknown_source_is_a_404_that_names_the_known_ones() {
        let registry = Registry::default();
        registry.register("orders");
        let response = respond(&get("/healthz/typo", ""), None, &registry);
        assert_eq!(response.status, "404 Not Found");
        assert!(response.body.contains("orders"), "{}", response.body);
    }

    #[test]
    fn a_starting_source_is_healthy() {
        // it has not failed; a probe that called this unhealthy would stop
        // every deployment from ever becoming ready
        let registry = Registry::default();
        registry.register("orders");
        assert_eq!(
            respond(&get("/healthz/orders", ""), None, &registry).status,
            "200 OK"
        );
    }

    #[test]
    fn only_get_is_answered() {
        let post = "POST /metrics HTTP/1.1\r\nHost: h\r\n\r\n";
        assert_eq!(body(post, None).0, "405 Method Not Allowed");
    }

    fn slot(name: &str, retained: Option<u64>, safe: Option<u64>, status: &str) -> SlotState {
        SlotState {
            name: name.into(),
            active: false,
            retained_bytes: retained,
            safe_wal_size: safe,
            wal_status: status.into(),
        }
    }

    #[test]
    fn a_slot_reports_what_it_holds_and_how_the_server_judges_it() {
        let metrics = Metrics::default();
        metrics.set_slots(vec![slot("s", Some(4096), None, "lost")]);
        let out = metrics.render("orders");
        assert!(out.contains("pg2osync_slot_retained_bytes{source=\"orders\",slot=\"s\"} 4096"));
        assert!(
            out.contains(
                "pg2osync_slot_wal_status{source=\"orders\",slot=\"s\",status=\"lost\"} 1"
            )
        );
        assert!(out.contains(
            "pg2osync_slot_wal_status{source=\"orders\",slot=\"s\",status=\"reserved\"} 0"
        ));
        assert!(
            !out.contains("pg2osync_slot_safe_wal_size_bytes"),
            "the server leaves it null when nothing bounds the slot, and a zero \
             there would read as no headroom left"
        );
    }

    #[test]
    fn a_dropped_slot_stops_being_reported() {
        let metrics = Metrics::default();
        metrics.set_slots(vec![slot("gone", Some(1), None, "reserved")]);
        metrics.set_slots(vec![slot("kept", Some(2), Some(9), "reserved")]);
        let out = metrics.render("orders");
        assert!(
            !out.contains("slot=\"gone\""),
            "a stale gauge keeps an alert firing against something that is gone"
        );
        assert!(
            out.contains("pg2osync_slot_safe_wal_size_bytes{source=\"orders\",slot=\"kept\"} 9")
        );
    }

    /// Every `# HELP` line in the exposition, in the order it appears.
    fn help_lines(out: &str) -> Vec<&str> {
        out.lines()
            .filter_map(|line| line.strip_prefix("# HELP "))
            .filter_map(|line| line.split_whitespace().next())
            .collect()
    }

    #[test]
    fn two_sources_render_one_exposition() {
        let registry = Registry::default();
        let orders = registry.register("orders");
        let users = registry.register("users");
        orders.incr_event("insert");
        orders.set_slots(vec![slot("s", Some(1), None, "reserved")]);
        users.incr_event("update");
        users.record_latency(7);
        let out = registry.render();

        let helps = help_lines(&out);
        let mut unique = helps.clone();
        unique.sort_unstable();
        unique.dedup();
        assert_eq!(
            helps.len(),
            unique.len(),
            "a family declared twice is not valid exposition:\n{out}"
        );

        // every series sits under the family it was declared in, which is what
        // "contiguous" means to a scraper reading the exposition top to bottom
        let mut family = "";
        for line in out.lines() {
            if let Some(rest) = line.strip_prefix("# HELP ") {
                family = rest.split_whitespace().next().unwrap_or_default();
                continue;
            }
            if line.starts_with('#') {
                continue;
            }
            assert!(
                line.starts_with(family),
                "{line:?} is not part of the {family} block:\n{out}"
            );
        }

        for line in out.lines().filter(|line| !line.starts_with('#')) {
            assert!(line.contains("source=\""), "unlabelled series {line:?}");
        }
        assert!(out.contains("pg2osync_events_total{source=\"orders\",type=\"insert\"} 1"));
        assert!(out.contains("pg2osync_events_total{source=\"users\",type=\"update\"} 1"));
        assert!(out.contains("pg2osync_latency_ms_count{source=\"users\"} 1"));
    }

    #[test]
    fn a_source_is_in_exactly_one_state() {
        let metrics = Metrics::default();
        metrics.set_state(SourceState::Reconnecting);
        let out = metrics.render("orders");
        let set: Vec<&str> = out
            .lines()
            .filter(|line| line.starts_with("pg2osync_source_state{"))
            .collect();
        assert_eq!(set.len(), SourceState::ALL.len(), "{out}");
        assert_eq!(
            set.iter().filter(|line| line.ends_with(" 1")).count(),
            1,
            "{out}"
        );
        assert!(out.contains("pg2osync_source_state{source=\"orders\",state=\"reconnecting\"} 1"));
        assert!(out.contains("pg2osync_source_state{source=\"orders\",state=\"halted\"} 0"));
    }

    #[test]
    fn a_summary_count_carries_no_help_of_its_own() {
        let metrics = Metrics::default();
        metrics.record_latency(3);
        let out = metrics.render("orders");
        assert!(!out.contains("# HELP pg2osync_latency_ms_count"), "{out}");
        assert_eq!(
            help_lines(&out)
                .iter()
                .filter(|f| **f == "pg2osync_latency_ms")
                .count(),
            1
        );
    }
}
