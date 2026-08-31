//! Sink implementations: OpenSearch (reference), Elasticsearch, Meilisearch,
//! PostgreSQL and Qdrant.

pub mod elasticsearch;
pub mod mapping;
pub mod meilisearch;
pub mod postgres;
pub mod qdrant;

use async_trait::async_trait;
use opensearch::auth::Credentials;
use opensearch::http::response::Response;
use opensearch::http::transport::{SingleNodeConnectionPool, TransportBuilder};
use opensearch::ingest::IngestGetPipelineParts;
use opensearch::params::{ExpandWildcards, VersionType};
use opensearch::{BulkOperation, BulkParts, GetParts, IndexParts, OpenSearch};
use pg2osync_core::checkpoint::Checkpoint;
use pg2osync_core::error::CoreError;
use pg2osync_core::lsn::Lsn;
use pg2osync_core::sink::{
    BulkLoadSettings, DocumentOp, Health, IndexSpec, LsnOp, Rejection, Sink, SinkAck, StoredReject,
    index_matches_pattern,
};
use serde_json::{Value, json};
use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, MutexGuard, PoisonError};

pub const META_INDEX: &str = ".pg2osync_meta";
/// Where documents the target refused are kept.
///
/// Separate from the meta index on purpose: an operator can read, count or drop
/// this without going anywhere near a checkpoint, and its contents are bounded
/// by `max_rejects` rather than by one small document per stream.
pub const REJECTS_INDEX: &str = ".pg2osync_rejects";
/// Single checkpoint document per pipeline; per-document atomicity is what
/// makes the write crash-safe without any compare-and-swap.
/// The document every pipeline used to write its checkpoint to, whatever
/// stream it belonged to. Still read as a fallback so an existing deployment
/// does not re-run its initial load on upgrade; never written any more.
pub const CHECKPOINT_DOC_ID: &str = "default";

/// Where one stream's checkpoint lives.
///
/// Two pipelines writing to the same target used to share a single document,
/// so each overwrote the other's position — which is exactly what the
/// documented reindex recipe asks you to run, and what "split tables across
/// instances" means as well. Either instance restarting then found a
/// checkpoint belonging to the other, rejected it, and re-ran a full load.
pub fn checkpoint_doc_id(stream: &pg2osync_core::checkpoint::StreamId) -> String {
    let tame: String = format!("{}-{}", stream.source, stream.stream)
        .chars()
        .map(|c| {
            if c.is_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect();
    tame
}
/// Bumped when the document layout changes; v1 stored only `confirmed_lsn`.
pub const CHECKPOINT_SCHEMA_VERSION: u32 = 2;

/// Serialize a checkpoint into its stored document form.
pub fn checkpoint_doc(ckpt: &Checkpoint) -> Value {
    json!({
        "source": ckpt.stream.source,
        "stream": ckpt.stream.stream,
        "slot_name": ckpt.stream.stream,
        "publication": ckpt.stream.publication,
        "position": ckpt.position,
        "token": ckpt.token,
        "instance_id": std::env::var("PG2OSYNC_INSTANCE_ID").unwrap_or_default(),
        "updated_at_epoch": chrono_now(),
        "schema_version": CHECKPOINT_SCHEMA_VERSION,
    })
}

/// Parse a stored checkpoint document.
///
/// Accepts the v1 layout (`confirmed_lsn`, no `source`) so an existing
/// deployment resumes instead of re-running a full backfill after an upgrade.
pub fn checkpoint_from_doc(src: &Value) -> Option<Checkpoint> {
    use pg2osync_core::checkpoint::{SOURCE_POSTGRES, StreamId};

    let stream = StreamId {
        source: src["source"]
            .as_str()
            .unwrap_or(SOURCE_POSTGRES)
            .to_string(),
        stream: src["slot_name"]
            .as_str()
            .or(src["stream"].as_str())
            .unwrap_or_default()
            .to_string(),
        publication: src["publication"].as_str().unwrap_or_default().to_string(),
    };
    let position = src["position"]
        .as_str()
        .or(src["confirmed_lsn"].as_str())?
        .to_string();
    let token = match src["token"].as_u64() {
        Some(t) => t,
        // v1 documents carry only the textual LSN
        None => position.parse::<Lsn>().ok()?.0,
    };
    Some(Checkpoint {
        stream,
        token,
        position,
    })
}

pub struct OpenSearchSink {
    client: OpenSearch,
    retry: RetryPolicy,
    /// Globs recorded by `ensure_ready` for templated tables, with the mapping
    /// an index they claim should be created with. Written once there and
    /// only read afterwards; a mutex because `ensure_ready` takes `&self`.
    templates: Mutex<Vec<(String, Option<Value>)>>,
    /// Index names known to exist, so the ordinary batch costs no request.
    known_indexes: Mutex<HashSet<String>>,
    /// Whether every bulk action has to land on an alias.
    require_alias: bool,
}

#[derive(Debug, Clone)]
pub struct OpenSearchSinkConfig {
    pub url: String,
    /// service does not support; index policies must exist beforehand.
    pub username: Option<String>,
    pub password: Option<String>,
    pub tls_verify: bool,
    pub retry: RetryPolicy,
    /// `[target] require_alias`: refuse a write whose target is an index
    /// rather than an alias.
    pub require_alias: bool,
}

/// Backoff is capped so a long outage settles into a steady retry rhythm
/// instead of drifting into hours.
const MAX_BACKOFF: std::time::Duration = std::time::Duration::from_secs(30);

/// Retry policy for transient failures; tunable via `[engine]` config.
///
/// Atomics rather than plain fields because the sink is behind an `Arc<dyn
/// Sink>` shared by every task by the time an operator changes their mind:
/// there is no `&mut` to reach it through, and rebuilding the sink to widen a
/// backoff would drop the connection pool and the index cache with it. Each
/// value is read at the top of an attempt, so a change reaches the next one.
#[derive(Debug)]
pub struct RetryPolicy {
    max_attempts: std::sync::atomic::AtomicU32,
    base_backoff_ms: AtomicU64,
    /// Ceiling on the time spent retrying one request, measured from its first
    /// failure. Zero is `None`: no ceiling, leaving the attempt count as the
    /// only limit. A ceiling of zero would mean "never retry", which is what
    /// `retry_max = 1` says, so the sentinel costs nothing expressible.
    max_elapsed_ms: AtomicU64,
}

impl RetryPolicy {
    pub fn new(max_attempts: u32, base_backoff_ms: u64, max_elapsed_ms: Option<u64>) -> Self {
        Self {
            max_attempts: std::sync::atomic::AtomicU32::new(max_attempts),
            base_backoff_ms: AtomicU64::new(base_backoff_ms),
            max_elapsed_ms: AtomicU64::new(max_elapsed_ms.unwrap_or(0)),
        }
    }

    pub fn max_attempts(&self) -> u32 {
        self.max_attempts.load(Ordering::Relaxed)
    }

    pub fn base_backoff_ms(&self) -> u64 {
        self.base_backoff_ms.load(Ordering::Relaxed)
    }

    pub fn max_elapsed_ms(&self) -> Option<u64> {
        match self.max_elapsed_ms.load(Ordering::Relaxed) {
            0 => None,
            ms => Some(ms),
        }
    }

    /// Every value at once, so a retry loop in flight cannot pair the old
    /// ceiling with the new backoff.
    pub fn set(&self, max_attempts: u32, base_backoff_ms: u64, max_elapsed_ms: Option<u64>) {
        self.max_attempts
            .store(max_attempts.max(1), Ordering::Relaxed);
        self.base_backoff_ms
            .store(base_backoff_ms.max(1), Ordering::Relaxed);
        self.max_elapsed_ms
            .store(max_elapsed_ms.unwrap_or(0), Ordering::Relaxed);
    }
}

impl Clone for RetryPolicy {
    fn clone(&self) -> Self {
        Self::new(
            self.max_attempts(),
            self.base_backoff_ms(),
            self.max_elapsed_ms(),
        )
    }
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self::new(10, 500, None)
    }
}

/// Which of the two limits ended a retry loop.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetryLimit {
    Attempts,
    Elapsed,
}

impl RetryPolicy {
    /// How long to wait before the try that follows `failures` consecutive
    /// failures, or which limit ends the retrying instead.
    ///
    /// The wait is clipped to what is left of the ceiling, so the worst case a
    /// runbook has to reason about really is `min(attempts × backoff schedule,
    /// elapsed ceiling)` rather than that plus one last backoff.
    pub fn backoff_after(
        &self,
        failures: u32,
        elapsed: std::time::Duration,
    ) -> Result<std::time::Duration, RetryLimit> {
        if failures >= self.max_attempts() {
            return Err(RetryLimit::Attempts);
        }
        let doubled = self
            .base_backoff_ms()
            .saturating_mul(2u64.saturating_pow(failures.saturating_sub(1).min(16)));
        let backoff = std::time::Duration::from_millis(doubled).min(MAX_BACKOFF);
        let Some(ceiling) = self.max_elapsed_ms() else {
            return Ok(backoff);
        };
        let remaining = std::time::Duration::from_millis(ceiling)
            .checked_sub(elapsed)
            .filter(|left| !left.is_zero())
            .ok_or(RetryLimit::Elapsed)?;
        Ok(backoff.min(remaining))
    }
}

/// Run one request until it succeeds, fails permanently, or hits a limit.
///
/// Shared by both bulk sinks so the two answer to the same policy. The clock is
/// tokio's, which a paused-time test can drive without waiting for it.
pub(crate) async fn retry_transient<T, F, Fut>(
    retry: &RetryPolicy,
    mut attempt: F,
) -> Result<T, CoreError>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, CoreError>>,
{
    let mut failures = 0u32;
    let mut first_failure = None;
    loop {
        let error = match attempt().await {
            Ok(done) => return Ok(done),
            Err(e) if is_retryable(&e) => e,
            Err(e) => return Err(e),
        };
        failures += 1;
        let since = *first_failure.get_or_insert_with(tokio::time::Instant::now);
        let elapsed = since.elapsed();
        match retry.backoff_after(failures, elapsed) {
            Ok(backoff) => {
                tracing::warn!(target: "pg2osync::sink",
                    "bulk attempt {failures} failed ({error}); backing off {}ms",
                    backoff.as_millis());
                tokio::time::sleep(backoff).await;
            }
            Err(limit) => return Err(gave_up(&error, limit, failures, elapsed)),
        }
    }
}

/// The error that ends the pipeline, saying which limit ended the retrying and
/// how long it went on — the two things a runbook asks first.
fn gave_up(
    error: &CoreError,
    limit: RetryLimit,
    attempts: u32,
    elapsed: std::time::Duration,
) -> CoreError {
    let reason = match limit {
        RetryLimit::Attempts => "the attempt limit [engine] retry_max",
        RetryLimit::Elapsed => "the elapsed ceiling [engine] retry_max_elapsed_ms",
    };
    let message = format!(
        "bulk gave up after {attempts} attempts over {:.1}s, ended by {reason} ({error})",
        elapsed.as_secs_f64()
    );
    // the classification survives the wrapping: whoever reads this error still
    // has to tell a target that was merely unreachable from one that refused
    match error {
        CoreError::SinkTransient(_) => CoreError::SinkTransient(message),
        _ => CoreError::Sink(message),
    }
}

/// Whether a URL points straight at an OpenSearch Serverless collection.
///
/// Serverless is not a supported target: every request to a collection has to be
/// signed with SigV4, which this client does not do, and the service rejects the
/// refresh and settings calls the pipeline relies on. Saying so at startup is
/// better than an afternoon of 403s.
fn is_serverless_endpoint(url: &str) -> bool {
    let after_scheme = url.split_once("://").map_or(url, |(_, rest)| rest);
    let host = after_scheme.split(['/', ':']).next().unwrap_or("");
    host.to_ascii_lowercase().ends_with(".aoss.amazonaws.com")
}

impl OpenSearchSink {
    pub fn new(cfg: OpenSearchSinkConfig) -> Result<Self, CoreError> {
        if is_serverless_endpoint(&cfg.url) {
            return Err(CoreError::Sink(format!(
                "{} is an Amazon OpenSearch Serverless endpoint, which pg2osync does not \
                 support: every request to a collection must be signed with SigV4, and the \
                 service rejects the refresh and index-settings calls the pipeline needs. \
                 Use a provisioned OpenSearch domain, Elasticsearch or Meilisearch",
                cfg.url
            )));
        }
        let pool_url = cfg
            .url
            .parse()
            .map_err(|e| CoreError::Sink(format!("invalid target url: {e}")))?;
        let mut builder = TransportBuilder::new(SingleNodeConnectionPool::new(pool_url));
        if let (Some(u), Some(p)) = (&cfg.username, &cfg.password) {
            builder = builder.auth(Credentials::Basic(u.clone(), p.clone()));
        }
        let transport = builder
            .build()
            .map_err(|e| CoreError::Sink(format!("transport build failed: {e}")))?;
        Ok(Self {
            client: OpenSearch::new(transport),
            retry: cfg.retry,
            templates: Mutex::new(Vec::new()),
            known_indexes: Mutex::new(HashSet::new()),
            require_alias: cfg.require_alias,
        })
    }

    /// Create the hidden checkpoint index if missing.
    /// Say so when an index is still not refreshing.
    ///
    /// An initial load suspends refresh and puts it back afterwards. A load
    /// killed in between leaves it suspended, and the symptom is the worst
    /// kind: writes are accepted, the pipeline looks healthy, and searches
    /// return nothing.
    /// Clear an index by deleting each document at the truncate's own position.
    ///
    /// Reads a page of ids and deletes them, repeating until a round deletes
    /// nothing. That termination rule is what makes it correct rather than
    /// merely bounded: a document written *after* the truncate has a higher
    /// version, so its delete is refused and it stays — and once every
    /// remaining document is one of those, the round deletes nothing and the
    /// loop is done.
    ///
    /// `index` may be the glob of a templated table. The search expands it,
    /// and each hit is deleted from the index it was found in.
    async fn truncate_at_version(
        &self,
        index: &str,
        version: i64,
        query: &Value,
    ) -> Result<(), CoreError> {
        const PAGE: usize = 1000;
        loop {
            // a glob whose rows have not rendered a single index yet matches
            // nothing, which is an empty table rather than a missing one
            let resp = self
                .client
                .search(opensearch::SearchParts::Index(&[index]))
                .allow_no_indices(true)
                .expand_wildcards(&[ExpandWildcards::Open])
                .body(json!({"size": PAGE, "_source": false, "query": query}))
                .send()
                .await
                .map_err(http_err)?;
            let body: Value = resp.json().await.map_err(http_err)?;
            // a join child lives on its parent's shard, and the delete has to
            // name that shard the way the write did; a hit without an index
            // is skipped rather than deleted from the glob, which would fail
            let hits: Vec<(String, String, Option<String>)> = body["hits"]["hits"]
                .as_array()
                .map(|hits| {
                    hits.iter()
                        .filter_map(|hit| {
                            let found_in = hit["_index"].as_str()?;
                            hit["_id"].as_str().map(|id| {
                                (
                                    found_in.to_string(),
                                    id.to_string(),
                                    hit["_routing"].as_str().map(str::to_string),
                                )
                            })
                        })
                        .collect()
                })
                .unwrap_or_default();
            if hits.is_empty() {
                return Ok(());
            }

            let ops: Vec<BulkOperation<Value>> = hits
                .iter()
                .map(|(found_in, id, routing)| {
                    let op = BulkOperation::delete(id.clone())
                        .index(found_in.clone())
                        .version(version)
                        .version_type(VersionType::ExternalGte);
                    match routing {
                        Some(r) => op.routing(r.clone()).into(),
                        None => op.into(),
                    }
                })
                .collect();
            let resp = self
                .client
                .bulk(opensearch::BulkParts::None)
                .body(ops)
                .refresh(opensearch::params::Refresh::True)
                .send()
                .await
                .map_err(http_err)?;
            let body: Value = resp.json().await.map_err(http_err)?;
            let deleted = body["items"]
                .as_array()
                .map(|items| {
                    items
                        .iter()
                        .filter(|item| {
                            item["delete"]["status"]
                                .as_u64()
                                .is_some_and(|s| (200..300).contains(&s))
                        })
                        .count()
                })
                .unwrap_or(0);
            if deleted == 0 {
                // everything left was written after the truncate
                return Ok(());
            }
        }
    }

    /// Remove every child of one parent, at the parent's own position.
    ///
    /// The children are found by the parent-id subfield the join field keeps
    /// for each parent relation, minus the parent's own relation — the parent
    /// document is deleted by the bulk action ahead of this, and `has_parent`
    /// would find nothing once it is gone. Everything is routed to the
    /// parent's shard, because that is the one shard a join child can be on.
    ///
    /// Terminates like `truncate_at_version`: a child written after the
    /// parent's delete carries a higher version, its delete is refused, and
    /// once only those remain a round deletes nothing. That survivor is the
    /// ordering rule working, not a leak.
    async fn delete_children(
        &self,
        index: &str,
        field: &str,
        parent_name: &str,
        parent_id: &str,
        version: Option<i64>,
    ) -> Result<(), CoreError> {
        const PAGE: usize = 1000;
        // a search only sees refreshed segments, so a child written moments
        // ago — in this very batch, even — would outlive the parent that
        // owns it
        let resp = self
            .client
            .indices()
            .refresh(opensearch::indices::IndicesRefreshParts::Index(&[index]))
            .send()
            .await
            .map_err(http_err)?;
        check_status(resp, "pre-cascade refresh").await?;

        let query = json!({"bool": {
            "filter": [{"term": {format!("{field}#{parent_name}"): parent_id}}],
            "must_not": [{"term": {field: parent_name}}]
        }});
        loop {
            let resp = self
                .client
                .search(opensearch::SearchParts::Index(&[index]))
                .routing(&[parent_id])
                .body(json!({"size": PAGE, "_source": false, "query": query}))
                .send()
                .await
                .map_err(http_err)?;
            let body: Value = resp.json().await.map_err(http_err)?;
            if let Some(error) = body.get("error") {
                return Err(CoreError::Sink(format!(
                    "find children of {index}/{parent_id}: {error}"
                )));
            }
            let ids: Vec<String> = body["hits"]["hits"]
                .as_array()
                .map(|hits| {
                    hits.iter()
                        .filter_map(|hit| hit["_id"].as_str().map(str::to_string))
                        .collect()
                })
                .unwrap_or_default();
            if ids.is_empty() {
                return Ok(());
            }

            let ops: Vec<BulkOperation<Value>> = ids
                .iter()
                .map(|id| {
                    let op = BulkOperation::delete(id.clone())
                        .index(index.to_string())
                        .routing(parent_id.to_string());
                    match version {
                        Some(v) => op.version(v).version_type(VersionType::ExternalGte).into(),
                        None => op.into(),
                    }
                })
                .collect();
            let resp = self
                .client
                .bulk(opensearch::BulkParts::None)
                .body(ops)
                .refresh(opensearch::params::Refresh::True)
                .send()
                .await
                .map_err(http_err)?;
            let body: Value = resp.json().await.map_err(http_err)?;
            let deleted = body["items"]
                .as_array()
                .map(|items| {
                    items
                        .iter()
                        .filter(|item| {
                            item["delete"]["status"]
                                .as_u64()
                                .is_some_and(|s| (200..300).contains(&s))
                        })
                        .count()
                })
                .unwrap_or(0);
            if deleted == 0 {
                return Ok(());
            }
        }
    }

    async fn warn_on_suspended_refresh(&self, names: &[&str]) {
        if names.is_empty() {
            return;
        }
        let Ok(resp) = self
            .client
            .indices()
            .get_settings(opensearch::indices::IndicesGetSettingsParts::Index(names))
            .send()
            .await
        else {
            return;
        };
        let Ok(body) = resp.json::<Value>().await else {
            return;
        };
        for name in names.iter().copied() {
            if body[name]["settings"]["index"]["refresh_interval"] == json!("-1") {
                tracing::warn!(target: "pg2osync::sink",
                    "index {name} has refresh_interval = -1, so nothing written to it is \
                     searchable. An initial load that was interrupted leaves it this way; \
                     restore it with PUT /{name}/_settings {{\"index\":{{\"refresh_interval\":null}}}}");
            }
        }
    }

    async fn put_settings(&self, index: &str, body: Value) -> Result<(), CoreError> {
        let resp = self
            .client
            .indices()
            .put_settings(opensearch::indices::IndicesPutSettingsParts::Index(&[
                index,
            ]))
            .body(body)
            .send()
            .await
            .map_err(http_err)?;
        check_status(resp, &format!("settings for {index}")).await
    }

    /// Create `name` with `mapping` unless it exists; whether this call made it.
    ///
    /// Losing a race to create it — two writers, or an operator — is the same
    /// end state as finding it, so `resource_already_exists_exception` is not
    /// an error here.
    async fn create_index_if_absent(
        &self,
        name: &str,
        mapping: Option<&Value>,
    ) -> Result<bool, CoreError> {
        let exists = self
            .client
            .indices()
            .exists(opensearch::indices::IndicesExistsParts::Index(&[name]))
            .send()
            .await
            .map_err(http_err)?;
        if exists.status_code().is_success() {
            return Ok(false);
        }
        // an empty body leaves the index to whatever the target infers, or to
        // whatever index template the operator already manages
        let body = mapping.map_or_else(|| json!({}), crate::mapping::create_body);
        let resp = self
            .client
            .indices()
            .create(opensearch::indices::IndicesCreateParts::Index(name))
            .body(body)
            .send()
            .await
            .map_err(http_err)?;
        if resp.status_code().is_success() {
            return Ok(true);
        }
        let status = resp.status_code();
        let body: Value = resp.json().await.unwrap_or(Value::Null);
        if already_exists(&body) {
            return Ok(false);
        }
        Err(CoreError::Sink(format!(
            "create index {name} failed: {status} {body}"
        )))
    }

    /// Create any index this batch writes to that does not exist yet and that a
    /// recorded template claims.
    ///
    /// Here rather than in `ensure_ready`, because the set of indices is not
    /// known until the rows are: pre-creating every index a template *could*
    /// render means enumerating a column's values, which nothing can do.
    ///
    /// Upserts only: creating an index to delete from it is work with no
    /// document to show for it, and a delete against a missing index is
    /// tolerated by the bulk triage instead.
    async fn ensure_batch_indexes(&self, batch: &[LsnOp]) -> Result<(), CoreError> {
        // neither guard is held across an await: the mutexes are std, and
        // the creates below are the slow part
        let unknown: HashSet<&str> = {
            let known = lock(&self.known_indexes);
            batch
                .iter()
                .filter_map(|op| match &op.op {
                    DocumentOp::Upsert { index, .. } if !known.contains(index) => {
                        Some(index.as_str())
                    }
                    _ => None,
                })
                .collect()
        };
        if unknown.is_empty() {
            return Ok(());
        }
        let claimed: Vec<(String, Option<Value>)> = {
            let templates = lock(&self.templates);
            unknown
                .into_iter()
                .filter_map(|name| {
                    claiming_template(&templates, name).map(|m| (name.to_string(), m.clone()))
                })
                .collect()
        };
        for (name, mapping) in claimed {
            if self.create_index_if_absent(&name, mapping.as_ref()).await? {
                tracing::info!(target: "pg2osync::sink",
                    "created index {name} for the first row that chose it");
            }
            lock(&self.known_indexes).insert(name);
        }
        Ok(())
    }

    pub async fn ensure_meta_index(&self) -> Result<(), CoreError> {
        let exists = self
            .client
            .indices()
            .exists(opensearch::indices::IndicesExistsParts::Index(&[
                META_INDEX,
            ]))
            .send()
            .await
            .map_err(http_err)?;
        if exists.status_code().is_success() {
            return Ok(());
        }
        let resp = self
            .client
            .indices()
            .create(opensearch::indices::IndicesCreateParts::Index(META_INDEX))
            .body(json!({
                "settings": {"index": {"hidden": true, "number_of_shards": 1}}
            }))
            .send()
            .await
            .map_err(http_err)?;
        check_created(resp, "create meta index").await
    }

    async fn ensure_rejects_index(&self) -> Result<(), CoreError> {
        let exists = self
            .client
            .indices()
            .exists(opensearch::indices::IndicesExistsParts::Index(&[
                REJECTS_INDEX,
            ]))
            .send()
            .await
            .map_err(http_err)?;
        if exists.status_code().is_success() {
            return Ok(());
        }
        let resp = self
            .client
            .indices()
            .create(opensearch::indices::IndicesCreateParts::Index(
                REJECTS_INDEX,
            ))
            .body(json!({
                "settings": {"index": {"hidden": true, "number_of_shards": 1}},
                // The refused document is stored but never searched by its own
                // fields, and indexing it is what would refuse it a second time
                // — the mapping that rejected it applies here too.
                "mappings": {"properties": {"document": {"type": "object", "enabled": false}}}
            }))
            .send()
            .await
            .map_err(http_err)?;
        check_created(resp, "create rejects index").await
    }

    /// One bulk request. Returns how far it got and, for each document the
    /// target refused permanently, its position in `batch` and why.
    ///
    /// The position rather than the id: there is exactly one bulk action per
    /// operation, in order, so the position identifies the operation even when a
    /// batch holds two writes for the same document.
    async fn bulk_once(&self, batch: &[LsnOp]) -> Result<(Lsn, Vec<(usize, String)>), CoreError> {
        let ops = batch
            .iter()
            .map(|op| bulk_action(&op.op))
            .collect::<Result<Vec<_>, _>>()?;

        // On the request rather than on each action because the generated
        // client offers it there and its `BulkOperation` has no field for it.
        // The two are the same rule: the target applies the flag to every
        // action of the request either way.
        let mut request = self.client.bulk(BulkParts::None);
        if self.require_alias {
            request = request.require_alias(true);
        }
        let resp = request
            .body(ops)
            .send()
            .await
            .map_err(|e| CoreError::SinkTransient(format!("bulk request failed: {e}")))?;

        let status = resp.status_code().as_u16();
        if status == 429 || status >= 500 {
            return Err(CoreError::SinkTransient(format!("http status {status}")));
        }
        let body: Value = resp
            .json()
            .await
            .map_err(|e| CoreError::Sink(e.to_string()))?;
        if body["errors"] != json!(true) {
            return Ok((batch.last().expect("nonempty checked").lsn, vec![]));
        }

        // item-level triage: retryable statuses re-queue, permanent ones surface
        let mut retryable_http = false;
        let mut permanent = vec![];
        for (nth, item) in body["items"]
            .as_array()
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .enumerate()
        {
            // the action name keys the result, so a delete's outcome lives
            // under "delete" — reading only "index" hid every failed delete
            let entry = item
                .as_object()
                .and_then(|fields| fields.values().next())
                .cloned()
                .unwrap_or(Value::Null);
            let item_status = entry["status"].as_u64().unwrap_or(0);
            let error_type = entry["error"]["type"].as_str().unwrap_or("unknown");
            if item_status == 429 || item_status >= 500 {
                retryable_http = true;
            } else if let Some(reason) = require_alias_refusal(&entry) {
                permanent.push((nth, reason));
            } else if error_type == "version_conflict_engine_exception" {
                // Not a failure: a later position already holds this document,
                // so declining this write is the ordering rule working. Treating
                // it as permanent would halt the pipeline on a race it just won.
                tracing::debug!(target: "pg2osync::sink",
                    "version conflict on {} left the newer document in place: {}",
                    entry["_id"].as_str().unwrap_or("?"),
                    entry["error"]["reason"].as_str().unwrap_or("?"));
            } else if is_absent(&item) {
                // A delete with nothing to delete, which is the desired end
                // state and arrives 404. Delivery is at-least-once, so a replay
                // after any restart re-sends deletes whose documents are already
                // gone — counting those as refusals halted the pipeline on its
                // own correctness. The index a row's template chose may not
                // exist at all, which is the same end state.
                tracing::debug!(target: "pg2osync::sink",
                    "{} was already absent", entry["_id"].as_str().unwrap_or("?"));
            } else if !(200..300).contains(&item_status) {
                // the reason, not only the type: a mapping error's detail is
                // what tells an operator which field to fix
                // An item with no error object would otherwise read
                // "unknown: unknown", which says nothing about what happened
                let reason = match entry["error"]["reason"].as_str() {
                    Some(reason) => format!("{error_type}: {reason}"),
                    None => format!("http {item_status}"),
                };
                permanent.push((nth, reason));
            }
        }
        if retryable_http {
            return Err(CoreError::SinkTransient(
                "item-level 429/5xx in bulk response".into(),
            ));
        }
        let max_lsn = batch.last().expect("nonempty checked").lsn;
        Ok((max_lsn, permanent))
    }

    async fn bulk_with_retry(
        &self,
        batch: &[LsnOp],
        retry: &RetryPolicy,
    ) -> Result<(Lsn, Vec<(usize, String)>), CoreError> {
        retry_transient(retry, || self.bulk_once(batch)).await
    }
}

/// One operation as its bulk action.
///
/// Upserts are `index` actions: last write wins by `_id`, which is what makes
/// a replay idempotent. Every action carries its own `_index` header, so the
/// request needs no URL-level index. A cascade is not a bulk action at all —
/// `write` segments the batch around it, so one reaching here is a bug in the
/// caller and is reported as such rather than silently skipped.
fn bulk_action(op: &DocumentOp) -> Result<BulkOperation<Value>, CoreError> {
    match op {
        DocumentOp::Upsert {
            index,
            id,
            routing,
            doc,
            version,
            pipeline,
        } => {
            let mut action = BulkOperation::index(doc.clone())
                .id(id.clone())
                .index(index.clone());
            if let Some(routing) = routing {
                action = action.routing(routing.clone());
            }
            if let Some(pipeline) = pipeline {
                action = action.pipeline(pipeline.clone());
            }
            // external_gte, not external: a replay after a crash writes the
            // same position again, and `external` rejects an equal version
            // while `external_gte` accepts it
            Ok(match external_version(*version) {
                Some(v) => action
                    .version(v)
                    .version_type(VersionType::ExternalGte)
                    .into(),
                None => action.into(),
            })
        }
        DocumentOp::Delete {
            index,
            id,
            routing,
            version,
        } => {
            let mut action = BulkOperation::delete(id.clone()).index(index.clone());
            if let Some(routing) = routing {
                action = action.routing(routing.clone());
            }
            Ok(match external_version(*version) {
                Some(v) => action
                    .version(v)
                    .version_type(VersionType::ExternalGte)
                    .into(),
                None => action.into(),
            })
        }
        DocumentOp::DeleteChildren {
            index, parent_id, ..
        } => Err(CoreError::Sink(format!(
            "the cascade for {index}/{parent_id} reached a bulk request; \
             write must run it between bulk requests"
        ))),
    }
}

fn is_retryable(e: &CoreError) -> bool {
    matches!(e, CoreError::SinkTransient(_))
}

fn http_err(e: opensearch::Error) -> CoreError {
    CoreError::Sink(format!("http request failed: {e}"))
}

/// A create that lost a race is a create that succeeded.
///
/// The hidden indices are the target's, not one pipeline's, so every source in
/// a process ensures the same two at startup and one of them is always second.
/// The target saying it is already there is the outcome the caller asked for.
async fn check_created(resp: Response, what: &str) -> Result<(), CoreError> {
    if resp.status_code().is_success() {
        return Ok(());
    }
    let status = resp.status_code();
    let body: Value = resp.json().await.unwrap_or(Value::Null);
    if already_exists(&body) {
        return Ok(());
    }
    Err(CoreError::Sink(format!("{what} failed: {status} {body}")))
}

/// Whether a refused create says the index is already there.
pub(crate) fn already_exists(body: &Value) -> bool {
    body["error"]["type"] == "resource_already_exists_exception"
}

async fn check_status(resp: Response, what: &str) -> Result<(), CoreError> {
    if !resp.status_code().is_success() {
        let status = resp.status_code();
        let body: Value = resp.json().await.unwrap_or(Value::Null);
        return Err(CoreError::Sink(format!("{what} failed: {status} {}", body)));
    }
    Ok(())
}

/// A guard on one of the sink's registries. A poisoned lock is still usable:
/// nothing panics between taking a guard and finishing one insert, so the
/// registry behind it is never half-written.
fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// The mapping the first recorded glob that claims `name` was configured
/// with, or `None` when no template claims it. Config refuses two templates
/// that overlap, so "first" is the only one.
pub(crate) fn claiming_template<'a>(
    templates: &'a [(String, Option<Value>)],
    name: &str,
) -> Option<&'a Option<Value>> {
    templates
        .iter()
        .find(|(pattern, _)| index_matches_pattern(pattern, name))
        .map(|(_, mapping)| mapping)
}

/// Whether a bulk item is a delete that found nothing to delete: the document
/// was already gone, or the index a row's template chose was never created —
/// creation is on demand and only an upsert asks for it. Read off the `delete`
/// action only, so an upsert into a missing index stays the rejection it is.
pub(crate) fn is_absent(item: &Value) -> bool {
    let entry = &item["delete"];
    entry["result"] == "not_found" || entry["error"]["type"] == "index_not_found_exception"
}

/// The refusal `require_alias` produced, if that is what this bulk item is.
///
/// Both targets report it as an `index_not_found_exception` naming the flag,
/// which reads as "the index vanished" to anyone who did not set the option —
/// so it is translated into the one sentence that says what to do about it.
///
/// Permanent rather than transient wherever it is used: nothing about a name
/// that is an index and not an alias changes on a retry, and the whole point of
/// the option is that the pipeline stops instead of writing past the alias.
pub(crate) fn require_alias_refusal(entry: &Value) -> Option<String> {
    if entry["error"]["type"] != "index_not_found_exception" {
        return None;
    }
    let reason = entry["error"]["reason"].as_str()?;
    if !reason.contains("[require_alias]") {
        return None;
    }
    let index = entry["error"]["index"]
        .as_str()
        .or(entry["_index"].as_str())
        .unwrap_or("the target index");
    Some(format!(
        "{index} is not an alias; with require_alias every write must go through one"
    ))
}

fn chrono_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

/// Where one refused document is filed.
///
/// Keyed by document rather than by attempt: if the source changes a broken row
/// and the target refuses it again, the newer refusal replaces the older one, so
/// a replay submits the current value and the count means "how many documents
/// are broken" rather than "how many times we tried".
pub fn reject_doc_id(index: &str, doc_id: &str) -> String {
    let tame: String = format!("{index}-{doc_id}")
        .chars()
        .map(|c| {
            if c.is_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect();
    tame
}

/// Serialize a rejection into its stored document form.
///
/// A cascade never becomes a rejection — its failure surfaces as an error from
/// `write`, since there is no single document to set aside — but the arm is
/// here so the stored form can say what every variant is, rather than the
/// type promising a round trip it cannot deliver.
pub fn reject_doc(r: &Rejection) -> Value {
    let (action, document, version) = match &r.op {
        DocumentOp::Upsert { doc, version, .. } => ("upsert", doc.clone(), *version),
        DocumentOp::Delete { version, .. } => ("delete", Value::Null, *version),
        DocumentOp::DeleteChildren { version, .. } => ("delete_children", Value::Null, *version),
    };
    let mut doc = json!({
        "index": r.index,
        "doc_id": r.doc_id,
        "reason": r.reason,
        "position": r.lsn.0,
        "action": action,
        "document": document,
        "version": version,
        "at_epoch": chrono_now(),
    });
    match &r.op {
        DocumentOp::Upsert {
            routing, pipeline, ..
        } => {
            // a replay has to land on the shard the original write named,
            // and go through the pipeline the section named
            doc["routing"] = json!(routing);
            doc["pipeline"] = json!(pipeline);
        }
        DocumentOp::Delete { routing, .. } => {
            doc["routing"] = json!(routing);
        }
        DocumentOp::DeleteChildren {
            field,
            parent_name,
            parent_id,
            ..
        } => {
            doc["field"] = json!(field);
            doc["parent_name"] = json!(parent_name);
            doc["parent_id"] = json!(parent_id);
        }
    }
    doc
}

/// Read a stored rejection back, or `None` if the document is not one.
pub fn reject_from_doc(id: &str, src: &Value) -> Option<StoredReject> {
    let index = src["index"].as_str()?.to_string();
    let doc_id = src["doc_id"].as_str()?.to_string();
    let version = src["version"].as_u64();
    let routing = src["routing"].as_str().map(str::to_string);
    let op = match src["action"].as_str()? {
        "upsert" => DocumentOp::Upsert {
            index: index.clone(),
            id: doc_id.clone(),
            routing,
            doc: src["document"].clone(),
            version,
            pipeline: src["pipeline"].as_str().map(str::to_string),
        },
        "delete" => DocumentOp::Delete {
            index: index.clone(),
            id: doc_id.clone(),
            routing,
            version,
        },
        "delete_children" => DocumentOp::DeleteChildren {
            index: index.clone(),
            field: src["field"].as_str()?.to_string(),
            parent_name: src["parent_name"].as_str()?.to_string(),
            parent_id: src["parent_id"].as_str()?.to_string(),
            version,
        },
        _ => return None,
    };
    Some(StoredReject {
        id: id.to_string(),
        rejection: Rejection {
            index,
            doc_id,
            reason: src["reason"].as_str().unwrap_or_default().to_string(),
            lsn: Lsn(src["position"].as_u64()?),
            op,
        },
        at_epoch: src["at_epoch"].as_u64().unwrap_or_default(),
    })
}

/// Pair each refused bulk item with the operation that produced it.
///
/// Shared by both Elasticsearch-family sinks: the rule that a response item with
/// no operation behind it is an error rather than a guess is the same for both,
/// and guessing which document was refused is how one gets lost.
pub fn rejections(
    batch: &[LsnOp],
    permanent: Vec<(usize, String)>,
) -> Result<Vec<Rejection>, CoreError> {
    let mut out = Vec::with_capacity(permanent.len());
    for (nth, reason) in permanent {
        let op = batch.get(nth).ok_or_else(|| {
            CoreError::Sink(format!(
                "bulk response has {} items for a batch of {}",
                nth + 1,
                batch.len()
            ))
        })?;
        let (index, doc_id) = match &op.op {
            DocumentOp::Upsert { index, id, .. } | DocumentOp::Delete { index, id, .. } => {
                (index.clone(), id.clone())
            }
            // the parent is the one document a cascade can be filed under
            DocumentOp::DeleteChildren {
                index, parent_id, ..
            } => (index.clone(), parent_id.clone()),
        };
        tracing::error!(target: "pg2osync::sink",
            "PERMANENT rejection {index}/{doc_id} at {}: {reason}", op.lsn);
        out.push(Rejection {
            index,
            doc_id,
            reason,
            lsn: op.lsn,
            op: op.op.clone(),
        });
    }
    Ok(out)
}

/// The `_mget` request for a set of `(id, routing)` pairs.
///
/// The plain `ids` form whenever nothing is routed — the overwhelming case,
/// and keeping the wire shape identical for existing deployments is worth the
/// branch. Both forms answer in request order, which the caller relies on.
fn mget_body(ids: &[(String, Option<String>)]) -> Value {
    if ids.iter().all(|(_, routing)| routing.is_none()) {
        let ids: Vec<&str> = ids.iter().map(|(id, _)| id.as_str()).collect();
        return json!({"ids": ids});
    }
    let docs: Vec<Value> = ids
        .iter()
        .map(|(id, routing)| match routing {
            Some(routing) => json!({"_id": id, "routing": routing}),
            None => json!({"_id": id}),
        })
        .collect();
    json!({"docs": docs})
}

/// A source position as the target's external document version.
///
/// The version is what makes a write's order at the target independent of the
/// order it arrives in: a document already at a later position rejects an
/// earlier write instead of being overwritten by it. That is what allows the
/// initial load and the change stream to write the same document concurrently.
///
/// Positions past `i64::MAX` cannot be expressed as a version. A PostgreSQL LSN
/// would have to pass 8 exabytes of WAL to get there, so this returns None
/// rather than pretending, and the write simply goes unversioned.
pub(crate) fn external_version(position: Option<u64>) -> Option<i64> {
    position
        .filter(|p| *p > 0)
        .and_then(|p| i64::try_from(p).ok())
}

/// Compare a configured mapping against a live index and act on the answer.
///
/// A field mapped to a different type means every document carrying it will be
/// rejected, and a rejection halts the pipeline — so it is worth finding at
/// startup rather than mid-batch. A field the index simply lacks is not fatal:
/// the target will map it dynamically from the first value that arrives, which
/// may be exactly what was wanted and may not.
fn report_mapping(index: &str, configured: &Value, live: &Value) -> Result<(), CoreError> {
    let report = crate::mapping::compare(configured, live);
    if !report.missing.is_empty() {
        tracing::warn!(target: "pg2osync::sink",
            "index {index} already exists and does not declare {}; \
             those fields will be mapped from whatever arrives first",
            report.missing.join(", "));
    }
    if !report.conflicting.is_empty() {
        return Err(CoreError::Sink(format!(
            "index {index} disagrees with the configured mapping: {}. \
             A mapping is not changed in place; reindex into a new index name",
            report.conflicting.join("; ")
        )));
    }
    Ok(())
}

#[async_trait]
impl Sink for OpenSearchSink {
    fn set_retry_policy(
        &self,
        max_attempts: u32,
        base_backoff_ms: u64,
        max_elapsed_ms: Option<u64>,
    ) {
        self.retry
            .set(max_attempts, base_backoff_ms, max_elapsed_ms);
    }

    async fn ensure_ready(&self, tables: &[IndexSpec]) -> Result<(), CoreError> {
        self.ensure_meta_index().await?;
        let mut fixed: Vec<&str> = Vec::new();
        for spec in tables {
            if spec.pattern {
                // nothing exists to create or compare yet: the names come from
                // rows, and the first batch that writes one creates it
                lock(&self.templates).push((spec.name.clone(), spec.mapping.clone()));
                continue;
            }
            fixed.push(&spec.name);
            let created = self
                .create_index_if_absent(&spec.name, spec.mapping.as_ref())
                .await?;
            lock(&self.known_indexes).insert(spec.name.clone());
            if created {
                continue;
            }
            if let Some(mapping) = &spec.mapping {
                let resp = self
                    .client
                    .indices()
                    .get_mapping(opensearch::indices::IndicesGetMappingParts::Index(&[
                        &spec.name
                    ]))
                    .send()
                    .await
                    .map_err(http_err)?;
                let body: Value = resp.json().await.map_err(http_err)?;
                report_mapping(&spec.name, mapping, &body[&spec.name])?;
            }
        }
        // fixed names only: settings for a glob come back one body per index,
        // and there is no index behind a glob yet anyway
        self.warn_on_suspended_refresh(&fixed).await;
        Ok(())
    }

    async fn get_documents(
        &self,
        index: &str,
        ids: &[(String, Option<String>)],
    ) -> Result<Vec<Option<Value>>, CoreError> {
        if ids.is_empty() {
            return Ok(vec![]);
        }
        let body = mget_body(ids);
        let resp = self
            .client
            .mget(opensearch::MgetParts::Index(index))
            .body(body)
            .send()
            .await
            .map_err(http_err)?;
        if !resp.status_code().is_success() {
            let status = resp.status_code();
            let err_body: Value = resp.json().await.unwrap_or(Value::Null);
            return Err(CoreError::Sink(format!("mget failed: {status} {err_body}")));
        }
        let body: Value = resp
            .json()
            .await
            .map_err(|e| CoreError::Sink(e.to_string()))?;
        // mget returns docs in request order; missing ids have no _source
        let mut out: Vec<Option<Value>> = Vec::with_capacity(ids.len());
        for doc in body["docs"].as_array().cloned().unwrap_or_default() {
            out.push(doc.get("_source").cloned());
        }
        Ok(out)
    }

    async fn write(&self, batch: Vec<LsnOp>) -> Result<SinkAck, CoreError> {
        if batch.is_empty() {
            return Err(CoreError::Sink(
                "engine must never send empty batches".into(),
            ));
        }
        self.ensure_batch_indexes(&batch).await?;
        // A cascade is not a bulk action, and it has to run after the parent's
        // own delete and before anything that follows it, so the batch is
        // written in the runs between cascades, in order.
        let mut permanent = Vec::new();
        let mut start = 0;
        for (nth, op) in batch.iter().enumerate() {
            let DocumentOp::DeleteChildren {
                index,
                field,
                parent_name,
                parent_id,
                version,
            } = &op.op
            else {
                continue;
            };
            if nth > start {
                let (_, perm) = self
                    .bulk_with_retry(&batch[start..nth], &self.retry)
                    .await?;
                // a rejection is paired with its operation by position, so a
                // run's positions have to be put back where the batch has them
                permanent.extend(perm.into_iter().map(|(i, why)| (start + i, why)));
            }
            self.delete_children(
                index,
                field,
                parent_name,
                parent_id,
                external_version(*version),
            )
            .await?;
            start = nth + 1;
        }
        if start < batch.len() {
            let (_, perm) = self.bulk_with_retry(&batch[start..], &self.retry).await?;
            permanent.extend(perm.into_iter().map(|(i, why)| (start + i, why)));
        }
        // the batch is non-empty, checked above
        let max_lsn = batch.last().expect("nonempty checked").lsn;
        let rejected = rejections(&batch, permanent)?;
        Ok(SinkAck { max_lsn, rejected })
    }

    async fn truncate_index(
        &self,
        index: &str,
        version: Option<u64>,
        only: Option<(&str, &str)>,
    ) -> Result<(), CoreError> {
        let query = match only {
            Some((field, value)) => json!({"term": {field: value}}),
            None => json!({"match_all": {}}),
        };
        // delete_by_query only removes documents a search can see, so writes
        // still sitting in the translog would survive the TRUNCATE and
        // resurrect rows the source has already dropped. `index` may be a
        // templated table's glob; refresh expands one itself.
        let resp = self
            .client
            .indices()
            .refresh(opensearch::indices::IndicesRefreshParts::Index(&[index]))
            .send()
            .await
            .map_err(http_err)?;
        check_status(resp, "pre-truncate refresh").await?;

        // Where documents carry versions, the truncate must carry one too.
        // delete_by_query cannot: it is internally versioned, so it leaves a
        // tombstone one above whatever the document held, and a replay of that
        // same write after a crash is then rejected forever — the row is lost.
        // A versioned bulk delete puts the tombstone at the truncate's own
        // position instead, which is the honest ordering: earlier writes lose,
        // later ones survive.
        if let Some(version) = external_version(version) {
            return self.truncate_at_version(index, version, &query).await;
        }

        // a glob is expanded by delete_by_query itself
        let resp = self
            .client
            .delete_by_query(opensearch::DeleteByQueryParts::Index(&[index]))
            .refresh(true)
            .conflicts(opensearch::params::Conflicts::Proceed)
            .body(json!({"query": query}))
            .send()
            .await
            .map_err(http_err)?;
        check_status(resp, &format!("truncate index {index}")).await
    }

    async fn refresh(&self, indices: &[String]) -> Result<(), CoreError> {
        if indices.is_empty() {
            return Ok(());
        }
        let names: Vec<&str> = indices.iter().map(String::as_str).collect();
        let resp = self
            .client
            .indices()
            .refresh(opensearch::indices::IndicesRefreshParts::Index(&names))
            .send()
            .await
            .map_err(http_err)?;
        check_status(resp, "refresh").await
    }

    async fn begin_bulk_load(&self, indices: &[String]) -> Result<BulkLoadSettings, CoreError> {
        if indices.is_empty() {
            return Ok(BulkLoadSettings::default());
        }
        let names: Vec<&str> = indices.iter().map(String::as_str).collect();
        let resp = self
            .client
            .indices()
            .get_settings(opensearch::indices::IndicesGetSettingsParts::Index(&names))
            .send()
            .await
            .map_err(http_err)?;
        let body: Value = resp.json().await.map_err(http_err)?;

        let mut saved = Vec::new();
        for index in indices {
            let settings = &body[index]["settings"]["index"];
            // "-1" means a previous load was interrupted before it could put
            // the setting back; restoring that would make the damage
            // permanent, so it is treated as "whatever the default is"
            let refresh = settings["refresh_interval"]
                .as_str()
                .map(str::to_string)
                .filter(|v| v != "-1");
            let replicas = settings["number_of_replicas"].as_str().map(str::to_string);
            saved.push((index.clone(), refresh, replicas));
        }
        for (index, _, _) in &saved {
            self.put_settings(
                index,
                json!({"index": {"refresh_interval": "-1", "number_of_replicas": 0}}),
            )
            .await?;
        }
        tracing::info!(target: "pg2osync::sink",
            "refresh and replicas suspended on {} index(es) for the initial load",
            saved.len());
        Ok(BulkLoadSettings(saved))
    }

    async fn end_bulk_load(&self, saved: &BulkLoadSettings) -> Result<(), CoreError> {
        for (index, refresh, replicas) in &saved.0 {
            // null restores the target's own default, which is the honest
            // answer when the index did not set the value itself
            self.put_settings(
                index,
                json!({"index": {
                    "refresh_interval": refresh,
                    "number_of_replicas": replicas
                }}),
            )
            .await?;
        }
        if !saved.0.is_empty() {
            tracing::info!(target: "pg2osync::sink",
                "refresh and replicas restored on {} index(es)", saved.0.len());
        }
        Ok(())
    }

    async fn scan_keys(
        &self,
        index: &str,
        key_field: &str,
        only: Option<(&str, &str)>,
        after: Option<&Value>,
        size: usize,
    ) -> Result<Vec<(String, Value, Option<String>)>, CoreError> {
        let mut body = json!({
            "size": size,
            "sort": [{ key_field: "asc" }],
            "_source": [key_field],
        });
        if let Some((field, value)) = only {
            body["query"] = json!({"term": {field: value}});
        }
        if let Some(after) = after {
            body["search_after"] = json!([after]);
        }
        let resp = self
            .client
            .search(opensearch::SearchParts::Index(&[index]))
            .body(body)
            .send()
            .await
            .map_err(http_err)?;
        let body: Value = resp.json().await.map_err(http_err)?;
        if let Some(error) = body.get("error") {
            return Err(CoreError::Sink(format!("scan {index}: {error}")));
        }
        Ok(body["hits"]["hits"]
            .as_array()
            .map(|hits| {
                hits.iter()
                    .filter_map(|hit| {
                        let id = hit["_id"].as_str()?.to_string();
                        // the sort value rather than _source: it is what
                        // search_after needs back, in the form the index holds
                        let key = hit["sort"].as_array()?.first()?.clone();
                        // present only on a routed document, which is where a
                        // delete of it has to be routed too
                        let routing = hit["_routing"].as_str().map(str::to_string);
                        Some((id, key, routing))
                    })
                    .collect()
            })
            .unwrap_or_default())
    }

    async fn count_documents(&self, index: &str) -> Result<Option<u64>, CoreError> {
        let resp = self
            .client
            .count(opensearch::CountParts::Index(&[index]))
            .send()
            .await
            .map_err(http_err)?;
        if resp.status_code() == 404 {
            return Ok(None);
        }
        let body: Value = resp.json().await.map_err(http_err)?;
        body["count"]
            .as_u64()
            .map(Some)
            .ok_or_else(|| CoreError::Sink(format!("count {index}: {body}")))
    }

    async fn index_exists(&self, name: &str) -> Result<bool, CoreError> {
        let resp = self
            .client
            .indices()
            .exists(opensearch::indices::IndicesExistsParts::Index(&[name]))
            .send()
            .await
            .map_err(http_err)?;
        Ok(resp.status_code().is_success())
    }

    async fn delete_index(&self, name: &str) -> Result<(), CoreError> {
        let resp = self
            .client
            .indices()
            .delete(opensearch::indices::IndicesDeleteParts::Index(&[name]))
            .send()
            .await
            .map_err(http_err)?;
        // gone already is the state the caller asked for
        if resp.status_code() == 404 {
            return Ok(());
        }
        check_status(resp, &format!("delete index {name}")).await
    }

    async fn switch_alias(&self, alias: &str, index: &str) -> Result<(), CoreError> {
        // where it points now, so the remove names real indices: a remove
        // against a wildcard errors when the alias does not exist yet
        let resp = self
            .client
            .indices()
            .get_alias(opensearch::indices::IndicesGetAliasParts::Name(&[alias]))
            .send()
            .await
            .map_err(http_err)?;
        // an alias that does not exist yet answers 404 with an error body, and
        // its keys are not index names — reading them would ask the target to
        // remove the alias from an index called "error"
        let current: Value = if resp.status_code().is_success() {
            resp.json().await.map_err(http_err)?
        } else {
            json!({})
        };

        let mut actions: Vec<Value> = current
            .as_object()
            .map(|indices| {
                indices
                    .keys()
                    .filter(|name| name.as_str() != index)
                    .map(|name| json!({"remove": {"index": name, "alias": alias}}))
                    .collect()
            })
            .unwrap_or_default();
        actions.push(json!({"add": {"index": index, "alias": alias}}));

        let resp = self
            .client
            .indices()
            .update_aliases()
            .body(json!({"actions": actions}))
            .send()
            .await
            .map_err(http_err)?;
        check_status(resp, &format!("point alias {alias} at {index}")).await
    }

    fn can_quarantine(&self) -> bool {
        true
    }

    async fn quarantine(&self, rejected: &[Rejection]) -> Result<(), CoreError> {
        if rejected.is_empty() {
            return Ok(());
        }
        self.ensure_rejects_index().await?;
        // One request per rejection rather than a bulk: a partial bulk failure
        // here would leave the caller unable to say which documents are safe to
        // acknowledge, and rejections are rare enough that the round trips cost
        // nothing worth having.
        for r in rejected {
            let resp = self
                .client
                .index(IndexParts::IndexId(
                    REJECTS_INDEX,
                    &reject_doc_id(&r.index, &r.doc_id),
                ))
                .body(reject_doc(r))
                .send()
                .await
                .map_err(http_err)?;
            check_status(resp, &format!("quarantine {}/{}", r.index, r.doc_id)).await?;
        }
        Ok(())
    }

    async fn list_rejects(&self, limit: usize) -> Result<(Vec<StoredReject>, u64), CoreError> {
        let exists = self
            .client
            .indices()
            .exists(opensearch::indices::IndicesExistsParts::Index(&[
                REJECTS_INDEX,
            ]))
            .send()
            .await
            .map_err(http_err)?;
        // Nothing has ever been quarantined, which is not the same as an error
        if !exists.status_code().is_success() {
            return Ok((Vec::new(), 0));
        }
        // A search only sees refreshed segments, and this total is what bounds
        // the quarantine: reading it stale would hand back budget that has
        // already been spent, and would hide a document from `rejects`.
        let resp = self
            .client
            .indices()
            .refresh(opensearch::indices::IndicesRefreshParts::Index(&[
                REJECTS_INDEX,
            ]))
            .send()
            .await
            .map_err(http_err)?;
        check_status(resp, "refresh rejects index").await?;
        let resp = self
            .client
            .search(opensearch::SearchParts::Index(&[REJECTS_INDEX]))
            .body(json!({
                "size": limit,
                "track_total_hits": true,
                "sort": [{"at_epoch": {"order": "desc"}}],
                "query": {"match_all": {}}
            }))
            .send()
            .await
            .map_err(http_err)?;
        if !resp.status_code().is_success() {
            return Err(CoreError::Sink(format!(
                "list rejects: {}",
                resp.status_code()
            )));
        }
        let body: Value = resp.json().await.map_err(http_err)?;
        let total = body["hits"]["total"]["value"].as_u64().unwrap_or(0);
        let stored = body["hits"]["hits"]
            .as_array()
            .cloned()
            .unwrap_or_default()
            .iter()
            .filter_map(|hit| {
                reject_from_doc(hit["_id"].as_str().unwrap_or_default(), &hit["_source"])
            })
            .collect();
        Ok((stored, total))
    }

    async fn clear_reject(&self, id: &str) -> Result<(), CoreError> {
        let resp = self
            .client
            .delete(opensearch::DeleteParts::IndexId(REJECTS_INDEX, id))
            .send()
            .await
            .map_err(http_err)?;
        // already gone is success
        if resp.status_code().as_u16() == 404 {
            return Ok(());
        }
        check_status(resp, &format!("clear reject {id}")).await
    }

    async fn read_state(&self, key: &str) -> Result<Option<Value>, CoreError> {
        let resp = self
            .client
            .get(GetParts::IndexId(META_INDEX, key))
            .send()
            .await
            .map_err(http_err)?;
        if resp.status_code().as_u16() == 404 {
            return Ok(None);
        }
        if !resp.status_code().is_success() {
            return Err(CoreError::Sink(format!(
                "read state {key}: {}",
                resp.status_code()
            )));
        }
        let body: Value = resp
            .json()
            .await
            .map_err(|e| CoreError::Sink(e.to_string()))?;
        Ok(Some(body["_source"].clone()))
    }

    async fn write_state(&self, key: &str, doc: &Value) -> Result<(), CoreError> {
        self.ensure_meta_index().await?;
        let resp = self
            .client
            .index(IndexParts::IndexId(META_INDEX, key))
            .body(doc.clone())
            .send()
            .await
            .map_err(http_err)?;
        check_status(resp, &format!("write state {key}")).await
    }

    async fn clear_state(&self, key: &str) -> Result<(), CoreError> {
        let resp = self
            .client
            .delete(opensearch::DeleteParts::IndexId(META_INDEX, key))
            .send()
            .await
            .map_err(http_err)?;
        if resp.status_code().as_u16() == 404 {
            return Ok(());
        }
        check_status(resp, &format!("clear state {key}")).await
    }

    async fn write_checkpoint(&self, checkpoint: &Checkpoint) -> Result<(), CoreError> {
        self.ensure_meta_index().await?;
        let doc_id = crate::checkpoint_doc_id(&checkpoint.stream);
        let resp = self
            .client
            .index(IndexParts::IndexId(META_INDEX, &doc_id))
            .body(checkpoint_doc(checkpoint))
            .send()
            .await
            .map_err(http_err)?;
        check_status(resp, "write checkpoint").await
    }

    async fn read_checkpoint(
        &self,
        stream: &pg2osync_core::checkpoint::StreamId,
    ) -> Result<Option<Checkpoint>, CoreError> {
        let doc_id = crate::checkpoint_doc_id(stream);
        let resp = self
            .client
            .get(GetParts::IndexId(META_INDEX, &doc_id))
            .send()
            .await
            .map_err(http_err)?;
        if resp.status_code().as_u16() == 404 {
            // written before checkpoints were kept per stream; the caller
            // still checks that it belongs to this one
            let legacy = self
                .client
                .get(GetParts::IndexId(META_INDEX, CHECKPOINT_DOC_ID))
                .send()
                .await
                .map_err(http_err)?;
            if !legacy.status_code().is_success() {
                return Ok(None);
            }
            let body: Value = legacy
                .json()
                .await
                .map_err(|e| CoreError::Sink(e.to_string()))?;
            return Ok(checkpoint_from_doc(&body["_source"]));
        }
        if !resp.status_code().is_success() {
            let status = resp.status_code();
            let err_body: Value = resp.json().await.unwrap_or(Value::Null);
            return Err(CoreError::Sink(format!(
                "read checkpoint failed: {status} {err_body}"
            )));
        }
        let body: Value = resp
            .json()
            .await
            .map_err(|e| CoreError::Sink(e.to_string()))?;
        Ok(checkpoint_from_doc(&body["_source"]))
    }

    async fn health(&self) -> Result<Health, CoreError> {
        let resp = self
            .client
            .ping()
            .send()
            .await
            .map_err(|e| CoreError::Sink(format!("health request failed: {e}")))?;
        if resp.status_code().is_success() {
            Ok(Health::Up)
        } else {
            Ok(Health::Down(format!("status {}", resp.status_code())))
        }
    }

    async fn has_pipeline(&self, name: &str) -> Result<bool, CoreError> {
        let resp = self
            .client
            .ingest()
            .get_pipeline(IngestGetPipelineParts::Id(name))
            .send()
            .await
            .map_err(|e| CoreError::Sink(format!("ingest pipeline {name:?} lookup failed: {e}")))?;
        match resp.status_code().as_u16() {
            200 => Ok(true),
            404 => Ok(false),
            status => Err(CoreError::Sink(format!(
                "ingest pipeline {name:?} lookup failed: status {status}"
            ))),
        }
    }

    async fn is_alias(&self, name: &str) -> Result<bool, CoreError> {
        // `/_alias/<name>` looks the name up in the alias namespace alone, so
        // an index of that name answers 404 exactly as a name nothing holds
        // does — which is the distinction `require_alias` turns on.
        let resp = self
            .client
            .indices()
            .get_alias(opensearch::indices::IndicesGetAliasParts::Name(&[name]))
            .send()
            .await
            .map_err(|e| CoreError::Sink(format!("alias {name:?} lookup failed: {e}")))?;
        match resp.status_code().as_u16() {
            200 => Ok(true),
            404 => Ok(false),
            status => Err(CoreError::Sink(format!(
                "alias {name:?} lookup failed: status {status}"
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn a_create_that_lost_the_race_is_not_a_failure() {
        // two sources in one process ensure the hidden indices at once, and
        // the second one halting on it would be a pipeline lost to a startup
        // order nobody controls
        assert!(super::already_exists(&serde_json::json!({
            "error": {"type": "resource_already_exists_exception"}
        })));
        assert!(!super::already_exists(&serde_json::json!({
            "error": {"type": "illegal_argument_exception"}
        })));
        assert!(!super::already_exists(&serde_json::Value::Null));
    }

    #[test]
    fn a_position_becomes_the_documents_external_version() {
        assert_eq!(external_version(Some(4096)), Some(4096));
    }

    #[test]
    fn a_write_with_nothing_to_version_by_goes_unversioned() {
        assert_eq!(external_version(None), None);
        // position zero is the initial load's marker for "no position"
        assert_eq!(external_version(Some(0)), None);
    }

    #[test]
    fn a_position_too_large_to_express_is_left_unversioned() {
        // rather than wrapping into a negative version and silently reordering
        // writes; a WAL would have to pass 8 exabytes to get here
        assert_eq!(external_version(Some(u64::MAX)), None);
        assert_eq!(external_version(Some(i64::MAX as u64)), Some(i64::MAX));
    }

    use super::*;
    use pg2osync_core::checkpoint::{SOURCE_MYSQL, SOURCE_POSTGRES, StreamId};

    #[test]
    fn the_first_template_claiming_a_name_lends_it_its_mapping() {
        let events = Some(json!({"properties": {"at": {"type": "date"}}}));
        let templates = vec![
            ("events-*".to_string(), events.clone()),
            ("*-archive".to_string(), None),
        ];
        assert_eq!(claiming_template(&templates, "events-acme"), Some(&events));
        assert_eq!(claiming_template(&templates, "orders-archive"), Some(&None));
        assert_eq!(claiming_template(&templates, "orders-live"), None);
        assert_eq!(claiming_template(&[], "events-acme"), None);
    }

    #[test]
    fn a_delete_that_found_nothing_is_absent_whether_the_document_or_the_index_was_missing() {
        assert!(is_absent(
            &json!({"delete": {"_id": "1", "status": 404, "result": "not_found"}})
        ));
        assert!(is_absent(&json!({"delete": {"_id": "1", "status": 404,
            "error": {"type": "index_not_found_exception", "reason": "no such index [events-acme]"}}})));
    }

    #[test]
    fn a_missing_index_on_an_upsert_is_not_absence() {
        // an index action with nowhere to go is a refusal, not a no-op
        assert!(!is_absent(&json!({"index": {"_id": "1", "status": 404,
            "error": {"type": "index_not_found_exception", "reason": "no such index [events-acme]"}}})));
        assert!(!is_absent(
            &json!({"delete": {"_id": "1", "status": 200, "result": "deleted"}})
        ));
        assert!(!is_absent(&json!({"delete": {"_id": "1", "status": 400,
            "error": {"type": "illegal_argument_exception", "reason": "bad"}}})));
    }

    fn checkpoint() -> Checkpoint {
        Checkpoint {
            stream: StreamId {
                source: SOURCE_POSTGRES.into(),
                stream: "pg2osync".into(),
                publication: "pg2osync_pub".into(),
            },
            token: 0x1B4_F2A8,
            position: "0/1B4F2A8".into(),
        }
    }

    #[test]
    fn checkpoint_documents_round_trip() {
        let doc = checkpoint_doc(&checkpoint());
        assert_eq!(checkpoint_from_doc(&doc), Some(checkpoint()));
    }

    #[test]
    fn mysql_positions_round_trip_unchanged() {
        let mysql = Checkpoint {
            stream: StreamId {
                source: SOURCE_MYSQL.into(),
                stream: "424242".into(),
                publication: String::new(),
            },
            token: (4u64 << 32) | 1234,
            position: "binlog.000004:1234".into(),
        };
        let doc = checkpoint_doc(&mysql);
        assert_eq!(checkpoint_from_doc(&doc), Some(mysql));
    }

    #[test]
    fn v1_documents_still_resume() {
        // pre-0.7 deployments stored only the textual LSN; refusing to read
        // them would force a full re-index on upgrade
        let legacy = json!({
            "slot_name": "pg2osync",
            "publication": "pg2osync_pub",
            "confirmed_lsn": "0/1B4F2A8",
            "schema_version": 1
        });
        let parsed = checkpoint_from_doc(&legacy).expect("v1 document must parse");
        assert_eq!(parsed.stream.source, SOURCE_POSTGRES);
        assert_eq!(parsed.token, 0x1B4_F2A8);
    }

    #[test]
    fn documents_without_a_position_are_ignored() {
        assert_eq!(checkpoint_from_doc(&json!({})), None);
        assert_eq!(checkpoint_from_doc(&Value::Null), None);
    }

    #[test]
    fn only_transient_errors_are_retried() {
        assert!(is_retryable(&CoreError::SinkTransient("429".into())));
        assert!(!is_retryable(&CoreError::Sink("bad mapping".into())));
    }

    #[test]
    fn a_delete_of_something_already_gone_is_not_a_refusal() {
        // Delivery is at-least-once, so a restart replays deletes whose
        // documents are already gone. The target answers 404 with no error
        // object, and counting that as a refusal halted the pipeline on its own
        // correctness.
        let item = json!({"delete": {"_id": "7", "status": 404, "result": "not_found"}});
        let entry = item
            .as_object()
            .and_then(|f| f.values().next())
            .cloned()
            .unwrap();
        assert_eq!(entry["result"], "not_found");
        assert!(
            !(200..300).contains(&entry["status"].as_u64().unwrap()),
            "which is exactly why the status alone cannot decide it"
        );
    }

    #[test]
    fn a_refused_item_is_paired_with_the_operation_that_caused_it() {
        // By position, not by id: a batch may hold two writes for one document,
        // and a replay needs the operation that was actually refused.
        let op = |id: &str, doc: Value| LsnOp {
            lsn: Lsn(0x100),
            op: DocumentOp::Upsert {
                index: "i".into(),
                id: id.into(),
                routing: None,
                doc,
                version: Some(0x100),
                pipeline: None,
            },
        };
        let batch = vec![op("1", json!({"v": 1})), op("2", json!({"v": 2}))];
        let out =
            rejections(&batch, vec![(1, "mapper_parsing_exception: nope".into())]).expect("paired");
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].doc_id, "2");
        assert_eq!(
            out[0].op, batch[1].op,
            "the refused write, not its neighbour"
        );

        // a response that does not line up with what was sent is an error
        assert!(rejections(&batch, vec![(9, "?".into())]).is_err());
    }

    #[test]
    fn a_quarantined_document_round_trips() {
        let r = Rejection {
            index: "orders".into(),
            doc_id: "7".into(),
            reason: "mapper_parsing_exception: amount".into(),
            lsn: Lsn(0x2A),
            op: DocumentOp::Delete {
                index: "orders".into(),
                id: "7".into(),
                routing: None,
                version: Some(0x2A),
            },
        };
        let stored = reject_from_doc("orders-7", &reject_doc(&r)).expect("readable");
        assert_eq!(stored.rejection, r, "a delete keeps being a delete");
        assert_eq!(stored.id, "orders-7");
    }

    #[test]
    fn a_quarantined_document_keeps_its_routing() {
        // A join child lives on its parent's shard; a replay that forgot the
        // routing would write a second copy of the document somewhere else.
        let rejection = |op| Rejection {
            index: "shop".into(),
            doc_id: "order-7".into(),
            reason: "mapper_parsing_exception: amount".into(),
            lsn: Lsn(0x2A),
            op,
        };
        let upsert = rejection(DocumentOp::Upsert {
            index: "shop".into(),
            id: "order-7".into(),
            routing: Some("customer-1".into()),
            doc: json!({"amount": "lots"}),
            version: Some(0x2A),
            pipeline: None,
        });
        let doc = reject_doc(&upsert);
        assert_eq!(doc["routing"], "customer-1");
        let stored = reject_from_doc("shop-order-7", &doc).expect("readable");
        assert_eq!(stored.rejection, upsert);

        let delete = rejection(DocumentOp::Delete {
            index: "shop".into(),
            id: "order-7".into(),
            routing: Some("customer-1".into()),
            version: Some(0x2A),
        });
        let stored = reject_from_doc("shop-order-7", &reject_doc(&delete)).expect("readable");
        assert_eq!(stored.rejection, delete);

        let cascade = rejection(DocumentOp::DeleteChildren {
            index: "shop".into(),
            field: "relation".into(),
            parent_name: "customer".into(),
            parent_id: "customer-1".into(),
            version: Some(0x2A),
        });
        let stored = reject_from_doc("shop-customer-1", &reject_doc(&cascade)).expect("readable");
        assert_eq!(
            stored.rejection, cascade,
            "a cascade is not silently un-replayable"
        );
    }

    /// The ndjson a bulk action serialises to, header line first.
    fn action_lines(op: &DocumentOp) -> Vec<Value> {
        use opensearch::http::request::Body as _;
        let mut ops = opensearch::BulkOperations::new();
        ops.push(bulk_action(op).expect("a bulk action"))
            .expect("serialisable");
        let bytes = ops.bytes().expect("buffered");
        let text = std::str::from_utf8(&bytes).expect("utf-8");
        text.lines()
            .map(|line| serde_json::from_str(line).expect("json line"))
            .collect()
    }

    #[test]
    fn a_routed_upsert_names_its_shard_in_the_bulk_header() {
        let lines = action_lines(&DocumentOp::Upsert {
            index: "shop".into(),
            id: "order-7".into(),
            routing: Some("customer-1".into()),
            doc: json!({"amount": 3}),
            version: Some(0x2A),
            pipeline: None,
        });
        assert_eq!(
            lines[0],
            json!({"index": {"_index": "shop", "_id": "order-7", "routing": "customer-1",
                             "version": 42, "version_type": "external_gte"}})
        );
        assert_eq!(lines[1], json!({"amount": 3}));
    }

    #[test]
    fn a_routed_delete_names_its_shard_in_the_bulk_header() {
        let lines = action_lines(&DocumentOp::Delete {
            index: "shop".into(),
            id: "order-7".into(),
            routing: Some("customer-1".into()),
            version: None,
        });
        assert_eq!(
            lines,
            vec![json!({"delete": {"_index": "shop", "_id": "order-7", "routing": "customer-1"}})]
        );
    }

    #[test]
    fn an_unrouted_operation_carries_no_routing_at_all() {
        // the wire shape every existing deployment sends stays byte-identical
        let lines = action_lines(&DocumentOp::Upsert {
            index: "users".into(),
            id: "1".into(),
            routing: None,
            doc: json!({"id": 1}),
            version: None,
            pipeline: None,
        });
        assert_eq!(lines[0], json!({"index": {"_index": "users", "_id": "1"}}));
    }

    #[test]
    fn an_upsert_names_its_pipeline_in_the_bulk_header_and_a_delete_does_not() {
        let lines = action_lines(&DocumentOp::Upsert {
            index: "products".into(),
            id: "1".into(),
            routing: None,
            doc: json!({"id": 1}),
            version: None,
            pipeline: Some("embed-products".into()),
        });
        assert_eq!(
            lines[0],
            json!({"index": {"_index": "products", "_id": "1", "pipeline": "embed-products"}})
        );
        assert_eq!(lines[1], json!({"id": 1}));

        // an ingest pipeline runs on index actions only, and the delete
        // header has no field for one
        let lines = action_lines(&DocumentOp::Delete {
            index: "products".into(),
            id: "1".into(),
            routing: None,
            version: None,
        });
        assert_eq!(
            lines,
            vec![json!({"delete": {"_index": "products", "_id": "1"}})]
        );
    }

    #[test]
    fn a_quarantined_document_keeps_its_pipeline() {
        // a replay that forgot the pipeline would write the document without
        // the fields the target was supposed to compute
        let upsert = Rejection {
            index: "products".into(),
            doc_id: "7".into(),
            reason: "mapper_parsing_exception: embedding".into(),
            lsn: Lsn(0x2A),
            op: DocumentOp::Upsert {
                index: "products".into(),
                id: "7".into(),
                routing: None,
                doc: json!({"name": "lamp"}),
                version: Some(0x2A),
                pipeline: Some("embed-products".into()),
            },
        };
        let doc = reject_doc(&upsert);
        assert_eq!(doc["pipeline"], "embed-products");
        let stored = reject_from_doc("products-7", &doc).expect("readable");
        assert_eq!(stored.rejection, upsert);
    }

    #[test]
    fn a_cascade_is_never_a_bulk_action() {
        let cascade = DocumentOp::DeleteChildren {
            index: "shop".into(),
            field: "relation".into(),
            parent_name: "customer".into(),
            parent_id: "customer-1".into(),
            version: None,
        };
        assert!(bulk_action(&cascade).is_err());
    }

    #[test]
    fn a_readback_keeps_the_plain_form_until_something_is_routed() {
        let plain = vec![("1".to_string(), None), ("2".to_string(), None)];
        assert_eq!(mget_body(&plain), json!({"ids": ["1", "2"]}));

        let routed = vec![
            ("customer-1".to_string(), None),
            ("order-7".to_string(), Some("customer-1".to_string())),
        ];
        assert_eq!(
            mget_body(&routed),
            json!({"docs": [{"_id": "customer-1"}, {"_id": "order-7", "routing": "customer-1"}]})
        );
    }

    #[test]
    fn a_collection_endpoint_is_refused_rather_than_left_to_403() {
        assert!(super::is_serverless_endpoint(
            "https://abc123.eu-west-1.aoss.amazonaws.com"
        ));
        assert!(super::is_serverless_endpoint(
            "https://ABC123.US-EAST-1.AOSS.AMAZONAWS.COM/"
        ));
    }

    #[test]
    fn every_other_target_still_passes() {
        assert!(!super::is_serverless_endpoint("http://localhost:9200"));
        // a provisioned domain is a different service, and is supported
        assert!(!super::is_serverless_endpoint(
            "https://search-mine-xyz.eu-west-1.es.amazonaws.com"
        ));
    }

    #[test]
    fn a_refusal_that_names_the_flag_becomes_the_sentence_that_says_what_to_do() {
        let entry = json!({"_index": "orders", "status": 404, "error": {
            "type": "index_not_found_exception", "index": "orders",
            "reason": "no such index [orders] and [require_alias] request flag is [true] \
                       and [orders] is not an alias"}});
        assert_eq!(
            require_alias_refusal(&entry).as_deref(),
            Some("orders is not an alias; with require_alias every write must go through one")
        );

        // an index that really is missing is a different failure and keeps its
        // own reason, which is what tells an operator to create it
        let missing = json!({"error": {"type": "index_not_found_exception", "index": "orders",
                                       "reason": "no such index [orders]"}});
        assert_eq!(require_alias_refusal(&missing), None);
        assert_eq!(require_alias_refusal(&json!({"status": 201})), None);
    }

    /// The request lines an `OpenSearchSink` sends to a loopback listener that
    /// answers every one of them with an empty, error-free bulk response.
    ///
    /// The flag goes on the request rather than on each action here, so a
    /// serialised action cannot show it and only the URL can.
    async fn request_lines_of_a_write(require_alias: bool, batch: Vec<LsnOp>) -> Vec<String> {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind a loopback port");
        let url = format!("http://{}", listener.local_addr().expect("local address"));
        let seen = std::sync::Arc::new(Mutex::new(Vec::new()));
        let record = std::sync::Arc::clone(&seen);
        tokio::spawn(async move {
            while let Ok((mut stream, _)) = listener.accept().await {
                let record = std::sync::Arc::clone(&record);
                tokio::spawn(async move {
                    let mut buf = [0u8; 8192];
                    let Ok(n) = stream.read(&mut buf).await else {
                        return;
                    };
                    let head = String::from_utf8_lossy(&buf[..n]).into_owned();
                    let mut words = head.lines().next().unwrap_or_default().split_whitespace();
                    lock(&record).push(format!(
                        "{} {}",
                        words.next().unwrap_or_default(),
                        words.next().unwrap_or_default()
                    ));
                    let body = r#"{"errors":false,"items":[]}"#;
                    let response = format!(
                        "HTTP/1.1 200 Fake\r\nContent-Type: application/json\r\n\
                         Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    );
                    let _ = stream.write_all(response.as_bytes()).await;
                    let _ = stream.shutdown().await;
                });
            }
        });

        let sink = OpenSearchSink::new(OpenSearchSinkConfig {
            url,
            username: None,
            password: None,
            tls_verify: true,
            retry: RetryPolicy::default(),
            require_alias,
        })
        .expect("a sink over plain http");
        sink.write(batch).await.expect("the fake accepts the batch");
        lock(&seen).clone()
    }

    fn an_upsert_and_a_delete() -> Vec<LsnOp> {
        vec![
            LsnOp {
                lsn: Lsn(0x2A),
                op: DocumentOp::Upsert {
                    index: "users".into(),
                    id: "1".into(),
                    routing: None,
                    doc: json!({"id": 1}),
                    version: Some(0x2A),
                    pipeline: None,
                },
            },
            LsnOp {
                lsn: Lsn(0x2A),
                op: DocumentOp::Delete {
                    index: "users".into(),
                    id: "2".into(),
                    routing: None,
                    version: Some(0x2A),
                },
            },
        ]
    }

    #[tokio::test]
    async fn every_bulk_action_of_a_write_is_told_to_require_an_alias() {
        let lines = request_lines_of_a_write(true, an_upsert_and_a_delete()).await;
        assert_eq!(
            lines,
            vec!["POST /_bulk?require_alias=true".to_string()],
            "one request carries the flag for the upsert and the delete alike"
        );
    }

    #[tokio::test]
    async fn a_target_nobody_asked_to_require_an_alias_sends_what_it_always_sent() {
        let lines = request_lines_of_a_write(false, an_upsert_and_a_delete()).await;
        assert_eq!(lines, vec!["POST /_bulk".to_string()]);
    }
}

#[cfg(test)]
mod retry_tests {
    use super::*;
    use std::cell::Cell;
    use std::time::Duration;

    fn policy(max_attempts: u32, max_elapsed_ms: Option<u64>) -> RetryPolicy {
        RetryPolicy::new(max_attempts, 500, max_elapsed_ms)
    }

    #[test]
    fn without_a_ceiling_the_attempt_count_is_the_only_limit() {
        let policy = policy(3, None);
        assert_eq!(
            policy.backoff_after(1, Duration::from_secs(3600)),
            Ok(Duration::from_millis(500))
        );
        assert_eq!(
            policy.backoff_after(2, Duration::ZERO),
            Ok(Duration::from_secs(1))
        );
        assert_eq!(
            policy.backoff_after(3, Duration::ZERO),
            Err(RetryLimit::Attempts)
        );
    }

    #[test]
    fn the_ceiling_ends_the_loop_while_attempts_are_left() {
        let policy = policy(100, Some(2_000));
        assert_eq!(
            policy.backoff_after(1, Duration::ZERO),
            Ok(Duration::from_millis(500))
        );
        assert_eq!(
            policy.backoff_after(4, Duration::from_millis(1_900)),
            Ok(Duration::from_millis(100)),
            "the last wait is clipped to what is left of the ceiling"
        );
        assert_eq!(
            policy.backoff_after(5, Duration::from_millis(2_000)),
            Err(RetryLimit::Elapsed)
        );
    }

    #[test]
    fn a_reloaded_policy_is_read_by_the_next_wait() {
        // The sink is behind an Arc by the time an operator changes their
        // mind, so the budget has to be readable through `&self` and change
        // under a loop that is already running.
        let policy = policy(3, None);
        assert_eq!(
            policy.backoff_after(3, Duration::ZERO),
            Err(RetryLimit::Attempts)
        );
        policy.set(10, 100, Some(5_000));
        assert_eq!(
            policy.backoff_after(3, Duration::ZERO),
            Ok(Duration::from_millis(400)),
            "the attempt that was out of budget now has one, at the new backoff"
        );
        assert_eq!(policy.max_elapsed_ms(), Some(5_000));
        policy.set(10, 100, None);
        assert_eq!(policy.max_elapsed_ms(), None, "the ceiling can be removed");
        // a floor of one, so a reload cannot turn retrying off by accident
        policy.set(0, 0, None);
        assert_eq!(policy.max_attempts(), 1);
        assert_eq!(policy.base_backoff_ms(), 1);
    }

    #[test]
    fn a_huge_backoff_cannot_overflow_into_a_short_wait() {
        let policy = RetryPolicy::new(100, u64::MAX, None);
        assert_eq!(policy.backoff_after(5, Duration::ZERO), Ok(MAX_BACKOFF));
    }

    /// A request that never succeeds, so only the policy can end the loop.
    async fn always_transient(retry: &RetryPolicy) -> CoreError {
        let tries = Cell::new(0u32);
        let error = retry_transient(retry, || {
            tries.set(tries.get() + 1);
            std::future::ready(Err::<(), _>(CoreError::SinkTransient(
                "target unreachable".into(),
            )))
        })
        .await
        .expect_err("nothing ever succeeds");
        assert!(tries.get() > 1, "it did retry");
        error
    }

    #[tokio::test(start_paused = true)]
    async fn attempts_run_out_first_and_the_error_says_so() {
        let error = always_transient(&policy(4, Some(3_600_000))).await;
        let message = error.to_string();
        assert!(message.contains("retry_max"), "{message}");
        assert!(!message.contains("retry_max_elapsed_ms"), "{message}");
        assert!(message.contains("after 4 attempts"), "{message}");
    }

    #[tokio::test(start_paused = true)]
    async fn the_ceiling_runs_out_first_and_the_error_says_so() {
        let error = always_transient(&policy(1_000, Some(4_000))).await;
        let message = error.to_string();
        assert!(message.contains("retry_max_elapsed_ms"), "{message}");
        assert!(message.contains("over 4.0s"), "{message}");
        assert!(
            matches!(error, CoreError::SinkTransient(_)),
            "the failure is still a transient one"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_permanent_failure_is_returned_untouched() {
        let error = retry_transient(&policy(10, Some(10_000)), || {
            std::future::ready(Err::<(), _>(CoreError::Sink("mapping refuses it".into())))
        })
        .await
        .expect_err("it cannot succeed");
        assert_eq!(error.to_string(), "sink error: mapping refuses it");
    }
}
