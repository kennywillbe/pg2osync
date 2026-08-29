//! Elasticsearch implementation of the `core::Sink` trait.
//!
//! Deliberately a thin raw-REST client instead of the `elasticsearch` crate:
//! we need only ~6 endpoints and avoid pulling a second generated-client HTTP
//! stack into the binary.

use async_trait::async_trait;
use base64::Engine as _;
use pg2osync_core::checkpoint::Checkpoint;
use pg2osync_core::error::CoreError;
use pg2osync_core::lsn::Lsn;
use pg2osync_core::sink::{BulkLoadSettings, DocumentOp, Health, IndexSpec, LsnOp, Sink, SinkAck};
use serde_json::{Value, json};
use std::collections::HashSet;
use std::sync::Mutex;

pub const META_INDEX: &str = ".pg2osync_meta";

pub struct ElasticsearchSink {
    http: reqwest::Client,
    base_url: String,
    retry: crate::RetryPolicy,
    /// Globs recorded by `ensure_ready` for templated tables, with the mapping
    /// an index they claim should be created with. Written once there and
    /// only read afterwards; a mutex because `ensure_ready` takes `&self`.
    templates: Mutex<Vec<(String, Option<Value>)>>,
    /// Index names known to exist, so the ordinary batch costs no request.
    known_indexes: Mutex<HashSet<String>>,
}

#[derive(Debug, Clone)]
pub struct ElasticsearchSinkConfig {
    pub url: String,
    pub username: Option<String>,
    pub password: Option<String>,
    /// ES Cloud / API-key auth: base64 "id:api_key".
    pub api_key: Option<String>,
    pub tls_verify: bool,
    pub retry: crate::RetryPolicy,
}

/// The version fields of a bulk action header, or nothing when the source has
/// no position to version by.
///
/// `external_gte` rather than `external`: a replay after a crash writes the
/// same position again, which `external` rejects and `external_gte` accepts.
fn version_fields(position: Option<u64>) -> String {
    match crate::external_version(position) {
        Some(v) => format!(",\"version\":{v},\"version_type\":\"external_gte\""),
        None => String::new(),
    }
}

/// The routing field of a bulk action header, or nothing for a document that
/// lives on the shard its own id picks.
fn routing_field(routing: Option<&str>) -> String {
    match routing {
        Some(r) => format!(
            ",\"routing\":{}",
            serde_json::to_string(r).unwrap_or_default()
        ),
        None => String::new(),
    }
}

/// The ingest pipeline field of a bulk action header, or nothing for a
/// document the target indexes as it arrives.
fn pipeline_field(pipeline: Option<&str>) -> String {
    match pipeline {
        Some(p) => format!(
            ",\"pipeline\":{}",
            serde_json::to_string(p).unwrap_or_default()
        ),
        None => String::new(),
    }
}

/// `/_ingest/pipeline/<name>` with the name percent-encoded as a path
/// segment, so a `/` or `?` in it cannot turn the lookup into another request.
fn pipeline_path(name: &str) -> String {
    let Ok(mut url) = reqwest::Url::parse("http://query.invalid/_ingest/pipeline") else {
        return String::new();
    };
    if let Ok(mut segments) = url.path_segments_mut() {
        segments.push(name);
    }
    url.path().to_string()
}

/// `key=value&…` with every value percent-encoded, so a parent id holding
/// `&` or `#` cannot cut the query short.
///
/// Built on the URL type reqwest already ships rather than a dependency of
/// its own; the host is a placeholder and only the query is kept.
fn query_string(pairs: &[(&str, &str)]) -> String {
    let Ok(mut url) = reqwest::Url::parse("http://query.invalid/") else {
        return String::new();
    };
    url.query_pairs_mut().extend_pairs(pairs);
    url.query().unwrap_or_default().to_string()
}

/// One bulk action header line: `{"<action>":{"_index":…,"_id":…,…}}`.
fn action_header(
    action: &str,
    index: &str,
    id: &str,
    routing: Option<&str>,
    version: Option<u64>,
    pipeline: Option<&str>,
) -> String {
    format!(
        "{{\"{action}\":{{\"_index\":{},\"_id\":{}{}{}{}}}}}\n",
        serde_json::to_string(index).unwrap_or_default(),
        serde_json::to_string(id).unwrap_or_default(),
        routing_field(routing),
        version_fields(version),
        pipeline_field(pipeline)
    )
}

impl ElasticsearchSink {
    /// Clear an index by deleting each document at the truncate's own position.
    ///
    /// The loop stops when a round deletes nothing, which is also what makes it
    /// correct: a document written after the truncate carries a higher version,
    /// its delete is refused, and it survives.
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
            let (_, body) = self
                .send(
                    reqwest::Method::POST,
                    &format!("/{index}/_search?allow_no_indices=true&expand_wildcards=open"),
                    Some(json!({"size": PAGE, "_source": false, "query": query}).to_string()),
                )
                .await?;
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
            let mut ndjson = String::new();
            for (found_in, id, routing) in &hits {
                ndjson.push_str(&format!(
                    "{{\"delete\":{{\"_index\":{},\"_id\":{}{},\"version\":{version},\
                     \"version_type\":\"external_gte\"}}}}\n",
                    serde_json::to_string(found_in).unwrap_or_default(),
                    serde_json::to_string(id).unwrap_or_default(),
                    routing_field(routing.as_deref()),
                ));
            }
            let (status, body) = self
                .send(reqwest::Method::POST, "/_bulk?refresh=true", Some(ndjson))
                .await?;
            if status != 200 {
                return Err(CoreError::Sink(format!("truncate {index}: {status}")));
            }
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

    /// Remove every child of one parent, at the parent's own position; the
    /// same loop as `truncate_at_version`, scoped by the join field's
    /// parent-id subfield and routed to the parent's shard.
    async fn delete_children(
        &self,
        index: &str,
        field: &str,
        parent_name: &str,
        parent_id: &str,
        version: Option<u64>,
    ) -> Result<(), CoreError> {
        const PAGE: usize = 1000;
        // a search only sees refreshed segments, so a child written moments
        // ago — in this very batch, even — would outlive the parent that
        // owns it
        self.refresh_target(index).await?;
        let query = json!({"bool": {
            "filter": [{"term": {format!("{field}#{parent_name}"): parent_id}}],
            "must_not": [{"term": {field: parent_name}}]
        }});
        let routing = query_string(&[("routing", parent_id)]);
        loop {
            let (status, body) = self
                .send(
                    reqwest::Method::POST,
                    &format!("/{index}/_search?{routing}"),
                    Some(json!({"size": PAGE, "_source": false, "query": query}).to_string()),
                )
                .await?;
            if status != 200 {
                return Err(CoreError::Sink(format!(
                    "find children of {index}/{parent_id}: {status} {body}"
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
            let mut ndjson = String::new();
            for id in &ids {
                ndjson.push_str(&action_header(
                    "delete",
                    index,
                    id,
                    Some(parent_id),
                    version,
                    None,
                ));
            }
            let (status, body) = self
                .send(reqwest::Method::POST, "/_bulk?refresh=true", Some(ndjson))
                .await?;
            if status != 200 {
                return Err(CoreError::Sink(format!(
                    "delete children of {index}/{parent_id}: {status}"
                )));
            }
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

    async fn put_settings(&self, index: &str, body: serde_json::Value) -> Result<(), CoreError> {
        let (status, body) = self
            .send(
                reqwest::Method::PUT,
                &format!("/{index}/_settings"),
                Some(body.to_string()),
            )
            .await?;
        if status != 200 {
            return Err(CoreError::Sink(format!(
                "settings for {index}: {status} {body}"
            )));
        }
        Ok(())
    }

    pub fn new(cfg: ElasticsearchSinkConfig) -> Result<Self, CoreError> {
        let mut headers = reqwest::header::HeaderMap::new();
        match (&cfg.api_key, &cfg.username, &cfg.password) {
            (Some(key), ..) => {
                let v = format!("ApiKey {key}")
                    .parse()
                    .map_err(|e| CoreError::Sink(format!("bad api key: {e}")))?;
                headers.insert(reqwest::header::AUTHORIZATION, v);
            }
            (_, Some(u), Some(p)) => {
                let v = format!(
                    "Basic {}",
                    base64::engine::general_purpose::STANDARD.encode(format!("{u}:{p}"))
                )
                .parse()
                .map_err(|e| CoreError::Sink(format!("bad credentials: {e}")))?;
                headers.insert(reqwest::header::AUTHORIZATION, v);
            }
            _ => {}
        }
        let http = reqwest::Client::builder()
            .default_headers(headers)
            .danger_accept_invalid_certs(!cfg.tls_verify)
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .map_err(|e| CoreError::Sink(e.to_string()))?;
        Ok(Self {
            http,
            base_url: cfg.url.trim_end_matches('/').to_string(),
            retry: cfg.retry,
            templates: Mutex::new(Vec::new()),
            known_indexes: Mutex::new(HashSet::new()),
        })
    }

    /// Create `name` with `mapping` unless it exists; whether this call made it.
    ///
    /// One PUT rather than an exists check first: Elasticsearch answers a
    /// second create with `resource_already_exists_exception`, which is the
    /// same end state as finding it — whoever won the race, the index is
    /// there.
    async fn create_index_if_absent(
        &self,
        name: &str,
        mapping: Option<&Value>,
    ) -> Result<bool, CoreError> {
        // without a body the index takes whatever Elasticsearch infers, or
        // whatever index template the operator already manages
        let create = mapping.map(|m| crate::mapping::create_body(m).to_string());
        let (status, body) = self
            .send(reqwest::Method::PUT, &format!("/{name}"), create)
            .await?;
        let already = body["error"]["type"] == json!("resource_already_exists_exception");
        if !(status == 200 || already) {
            return Err(CoreError::Sink(format!("create index {name}: {status}")));
        }
        Ok(!already)
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
            let known = crate::lock(&self.known_indexes);
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
            let templates = crate::lock(&self.templates);
            unknown
                .into_iter()
                .filter_map(|name| {
                    crate::claiming_template(&templates, name)
                        .map(|m| (name.to_string(), m.clone()))
                })
                .collect()
        };
        for (name, mapping) in claimed {
            if self.create_index_if_absent(&name, mapping.as_ref()).await? {
                tracing::info!(target: "pg2osync::sink",
                    "created index {name} for the first row that chose it");
            }
            crate::lock(&self.known_indexes).insert(name);
        }
        Ok(())
    }

    async fn send(
        &self,
        method: reqwest::Method,
        path: &str,
        body: Option<String>,
    ) -> Result<(u16, Value), CoreError> {
        let url = format!("{}{}", self.base_url, path);
        let mut req = self.http.request(method, &url);
        if let Some(b) = body {
            req = req.header("Content-Type", "application/json").body(b);
        }
        let resp = req
            .send()
            .await
            .map_err(|e| CoreError::Sink(format!("request failed: {e}")))?;
        let status = resp.status().as_u16();
        let text = resp
            .text()
            .await
            .map_err(|e| CoreError::Sink(e.to_string()))?;
        let body = serde_json::from_str(&text).unwrap_or(Value::Null);
        Ok((status, body))
    }

    /// Make every write to `target` — an index, a glob, or a comma-separated
    /// list — visible to the search that follows.
    ///
    /// A refused refresh is an error, not a warning: each caller runs a
    /// search whose result is only right once the refresh has happened, and
    /// acting on a stale one deletes too little or hands out budget twice.
    async fn refresh_target(&self, target: &str) -> Result<(), CoreError> {
        let (status, body) = self
            .send(reqwest::Method::POST, &format!("/{target}/_refresh"), None)
            .await?;
        if !(200..300).contains(&status) {
            return Err(CoreError::Sink(format!(
                "refresh {target}: {status} {body}"
            )));
        }
        Ok(())
    }

    /// One bulk request. Returns how far it got and, for each refused document,
    /// its position in `batch` and why — the position because there is exactly
    /// one action per operation, in order, so it identifies the operation even
    /// when a batch holds two writes for the same document.
    async fn bulk_once(&self, batch: &[LsnOp]) -> Result<(Lsn, Vec<(usize, String)>), CoreError> {
        let ndjson = ndjson_body(batch)?;

        let resp = self
            .http
            .post(format!("{}/_bulk", self.base_url))
            .header("Content-Type", "application/x-ndjson")
            .body(ndjson)
            .send()
            .await
            .map_err(|e| CoreError::SinkTransient(format!("bulk request failed: {e}")))?;
        let status = resp.status().as_u16();
        if status == 429 || status >= 500 {
            return Err(CoreError::SinkTransient(format!("http status {status}")));
        }
        let body: Value = resp
            .json()
            .await
            .map_err(|e| CoreError::Sink(e.to_string()))?;
        if body["errors"] != json!(true) {
            return Ok((batch.last().expect("nonempty").lsn, vec![]));
        }
        let mut retryable = false;
        let mut permanent = vec![];
        for (nth, item) in body["items"]
            .as_array()
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .enumerate()
        {
            // first key is the action name ("index"/"create"/"delete")
            let entry = item
                .as_object()
                .and_then(|o| o.values().next())
                .cloned()
                .unwrap_or(Value::Null);
            let s = entry["status"].as_u64().unwrap_or(0);
            let error_type = entry["error"]["type"].as_str().unwrap_or("unknown");
            if s == 429 || s >= 500 {
                retryable = true;
            } else if error_type == "version_conflict_engine_exception" {
                // a later position already holds this document, so declining
                // the write is the ordering rule working rather than a failure
                tracing::trace!(target: "pg2osync::sink",
                    "version conflict on {} left the newer document in place",
                    entry["_id"].as_str().unwrap_or("?"));
            } else if crate::is_absent(&item) {
                // a delete with nothing to delete: the end state we wanted, and
                // at-least-once delivery replays deletes after every restart;
                // an index a row's template chose may not exist at all, which
                // is the same end state
                tracing::trace!(target: "pg2osync::sink",
                    "{} was already absent", entry["_id"].as_str().unwrap_or("?"));
            } else if !(200..300).contains(&s) {
                let reason = match entry["error"]["reason"].as_str() {
                    Some(reason) => format!("{error_type}: {reason}"),
                    None => format!("http {s}"),
                };
                permanent.push((nth, reason));
            }
        }
        if retryable {
            return Err(CoreError::SinkTransient(
                "item-level 429/5xx in bulk response".into(),
            ));
        }
        Ok((batch.last().expect("nonempty").lsn, permanent))
    }

    async fn bulk_with_retry(
        &self,
        batch: &[LsnOp],
    ) -> Result<(Lsn, Vec<(usize, String)>), CoreError> {
        let mut attempt = 0u32;
        loop {
            attempt += 1;
            match self.bulk_once(batch).await {
                Ok(done) => return Ok(done),
                Err(e) if attempt < self.retry.max_attempts && is_retryable(&e) => {
                    let backoff = self.retry.base_backoff_ms * 2u64.saturating_pow(attempt - 1);
                    tracing::warn!(target: "pg2osync::sink",
                        "bulk attempt {attempt} failed ({e}); backing off {backoff}ms");
                    tokio::time::sleep(std::time::Duration::from_millis(backoff.min(30_000))).await;
                }
                Err(e) => return Err(e),
            }
        }
    }
}

/// The ndjson body of one bulk request, one action per operation in order.
///
/// A cascade is not a bulk action — `write` runs it between bulk requests —
/// so one reaching here is a bug in the caller and is reported rather than
/// skipped, which would silently lose it.
fn ndjson_body(batch: &[LsnOp]) -> Result<String, CoreError> {
    let mut ndjson = String::new();
    for op in batch {
        match &op.op {
            DocumentOp::Upsert {
                index,
                id,
                routing,
                doc,
                version,
                pipeline,
            } => {
                ndjson.push_str(&action_header(
                    "index",
                    index,
                    id,
                    routing.as_deref(),
                    *version,
                    pipeline.as_deref(),
                ));
                ndjson.push_str(&serde_json::to_string(doc).unwrap_or_default());
                ndjson.push('\n');
            }
            DocumentOp::Delete {
                index,
                id,
                routing,
                version,
            } => {
                ndjson.push_str(&action_header(
                    "delete",
                    index,
                    id,
                    routing.as_deref(),
                    *version,
                    None,
                ));
            }
            DocumentOp::DeleteChildren {
                index, parent_id, ..
            } => {
                return Err(CoreError::Sink(format!(
                    "the cascade for {index}/{parent_id} reached a bulk request; \
                     write must run it between bulk requests"
                )));
            }
        }
    }
    ndjson.push('\n');
    Ok(ndjson)
}

#[async_trait]
impl Sink for ElasticsearchSink {
    async fn ensure_ready(&self, tables: &[IndexSpec]) -> Result<(), CoreError> {
        let (status, _) = self
            .send(
                reqwest::Method::PUT,
                &format!("/{META_INDEX}"),
                Some(
                    json!({
                        "settings": {"index": {"hidden": true, "number_of_shards": 1}}
                    })
                    .to_string(),
                ),
            )
            .await?;
        // 400 with resource_already_exists is fine
        if !(status == 200 || status == 400) {
            return Err(CoreError::Sink(format!("ensure meta index: {status}")));
        }
        for spec in tables {
            if spec.pattern {
                // nothing exists to create or compare yet: the names come from
                // rows, and the first batch that writes one creates it
                crate::lock(&self.templates).push((spec.name.clone(), spec.mapping.clone()));
                continue;
            }
            let created = self
                .create_index_if_absent(&spec.name, spec.mapping.as_ref())
                .await?;
            crate::lock(&self.known_indexes).insert(spec.name.clone());
            if !created && let Some(mapping) = &spec.mapping {
                let (_, live) = self
                    .send(
                        reqwest::Method::GET,
                        &format!("/{}/_mapping", spec.name),
                        None,
                    )
                    .await?;
                crate::report_mapping(&spec.name, mapping, &live[&spec.name])?;
            }
        }
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
        let (status, body) = self
            .send(
                reqwest::Method::POST,
                &format!("/{index}/_mget"),
                Some(crate::mget_body(ids).to_string()),
            )
            .await?;
        if status != 200 {
            return Err(CoreError::Sink(format!("mget failed: {status}")));
        }
        Ok(body["docs"]
            .as_array()
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .map(|d| d.get("_source").cloned())
            .collect())
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
                let (_, perm) = self.bulk_with_retry(&batch[start..nth]).await?;
                // a rejection is paired with its operation by position, so a
                // run's positions have to be put back where the batch has them
                permanent.extend(perm.into_iter().map(|(i, why)| (start + i, why)));
            }
            self.delete_children(index, field, parent_name, parent_id, *version)
                .await?;
            start = nth + 1;
        }
        if start < batch.len() {
            let (_, perm) = self.bulk_with_retry(&batch[start..]).await?;
            permanent.extend(perm.into_iter().map(|(i, why)| (start + i, why)));
        }
        // the batch is non-empty, checked above
        let max_lsn = batch.last().expect("nonempty").lsn;
        Ok(SinkAck {
            max_lsn,
            rejected: crate::rejections(&batch, permanent)?,
        })
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
        // an unrefreshed write is invisible to _delete_by_query and would
        // outlive the TRUNCATE that is supposed to remove it. `index` may be a
        // templated table's glob; _refresh and _delete_by_query expand one
        // themselves.
        self.refresh_target(index).await?;
        // delete_by_query is internally versioned, so it leaves a tombstone
        // above the document's own version and a replayed write is then
        // rejected forever. A versioned bulk delete puts the tombstone at the
        // truncate's position instead.
        if let Some(version) = crate::external_version(version) {
            return self.truncate_at_version(index, version, &query).await;
        }
        let (status, _) = self
            .send(
                reqwest::Method::POST,
                &format!("/{index}/_delete_by_query?refresh=true&conflicts=proceed"),
                Some(json!({"query": query}).to_string()),
            )
            .await?;
        if status != 200 {
            return Err(CoreError::Sink(format!("truncate {index}: {status}")));
        }
        Ok(())
    }

    async fn refresh(&self, indices: &[String]) -> Result<(), CoreError> {
        if indices.is_empty() {
            return Ok(());
        }
        self.refresh_target(&indices.join(",")).await
    }

    async fn begin_bulk_load(&self, indices: &[String]) -> Result<BulkLoadSettings, CoreError> {
        if indices.is_empty() {
            return Ok(BulkLoadSettings::default());
        }
        let (_, body) = self
            .send(
                reqwest::Method::GET,
                &format!("/{}/_settings", indices.join(",")),
                None,
            )
            .await?;
        let mut saved = Vec::new();
        for index in indices {
            let settings = &body[index]["settings"]["index"];
            // "-1" means an earlier load never got to put it back; restoring
            // that value would make the damage permanent
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

    async fn read_state(&self, key: &str) -> Result<Option<Value>, CoreError> {
        let (status, body) = self
            .send(
                reqwest::Method::GET,
                &format!("/{META_INDEX}/_doc/{key}"),
                None,
            )
            .await?;
        match status {
            200 => Ok(Some(body["_source"].clone())),
            404 => Ok(None),
            other => Err(CoreError::Sink(format!("read state {key}: {other}"))),
        }
    }

    fn can_quarantine(&self) -> bool {
        true
    }

    async fn quarantine(
        &self,
        rejected: &[pg2osync_core::sink::Rejection],
    ) -> Result<(), CoreError> {
        // Created explicitly rather than left to auto-creation: the refused
        // document is stored but never searched by its own fields, and indexing
        // it would let the very mapping conflict that refused it refuse it here
        // too.
        let body = json!({
            "settings": {"index": {"hidden": true, "number_of_shards": 1}},
            "mappings": {"properties": {
                "document": {"type": "object", "enabled": false}
            }}
        });
        self.create_index_if_absent(crate::REJECTS_INDEX, Some(&body))
            .await?;
        for r in rejected {
            let id = crate::reject_doc_id(&r.index, &r.doc_id);
            let (status, _) = self
                .send(
                    reqwest::Method::PUT,
                    &format!("/{}/_doc/{id}", crate::REJECTS_INDEX),
                    Some(crate::reject_doc(r).to_string()),
                )
                .await?;
            if status != 200 && status != 201 {
                return Err(CoreError::Sink(format!(
                    "quarantine {}/{}: {status}",
                    r.index, r.doc_id
                )));
            }
        }
        Ok(())
    }

    async fn list_rejects(
        &self,
        limit: usize,
    ) -> Result<(Vec<pg2osync_core::sink::StoredReject>, u64), CoreError> {
        // A search only sees refreshed segments, and this total is what bounds
        // the quarantine: reading it stale would hand back budget already spent.
        let (status, body) = self
            .send(
                reqwest::Method::POST,
                &format!("/{}/_refresh", crate::REJECTS_INDEX),
                None,
            )
            .await?;
        // nothing has ever been quarantined, which is not an error
        if status == 404 {
            return Ok((Vec::new(), 0));
        }
        if !(200..300).contains(&status) {
            return Err(CoreError::Sink(format!(
                "refresh {}: {status} {body}",
                crate::REJECTS_INDEX
            )));
        }
        let (status, body) = self
            .send(
                reqwest::Method::POST,
                &format!("/{}/_search", crate::REJECTS_INDEX),
                Some(
                    json!({
                        "size": limit,
                        "track_total_hits": true,
                        "sort": [{"at_epoch": {"order": "desc"}}],
                        "query": {"match_all": {}}
                    })
                    .to_string(),
                ),
            )
            .await?;
        if status != 200 {
            return Err(CoreError::Sink(format!("list rejects: {status}")));
        }
        let total = body["hits"]["total"]["value"].as_u64().unwrap_or(0);
        let stored = body["hits"]["hits"]
            .as_array()
            .cloned()
            .unwrap_or_default()
            .iter()
            .filter_map(|hit| {
                crate::reject_from_doc(hit["_id"].as_str().unwrap_or_default(), &hit["_source"])
            })
            .collect();
        Ok((stored, total))
    }

    async fn clear_reject(&self, id: &str) -> Result<(), CoreError> {
        let (status, _) = self
            .send(
                reqwest::Method::DELETE,
                &format!("/{}/_doc/{id}", crate::REJECTS_INDEX),
                None,
            )
            .await?;
        if status != 200 && status != 404 {
            return Err(CoreError::Sink(format!("clear reject {id}: {status}")));
        }
        Ok(())
    }

    async fn write_state(&self, key: &str, doc: &Value) -> Result<(), CoreError> {
        let (status, _) = self
            .send(
                reqwest::Method::PUT,
                &format!("/{META_INDEX}/_doc/{key}"),
                Some(doc.to_string()),
            )
            .await?;
        if status != 200 && status != 201 {
            return Err(CoreError::Sink(format!("write state {key}: {status}")));
        }
        Ok(())
    }

    async fn clear_state(&self, key: &str) -> Result<(), CoreError> {
        let (status, _) = self
            .send(
                reqwest::Method::DELETE,
                &format!("/{META_INDEX}/_doc/{key}"),
                None,
            )
            .await?;
        if status != 200 && status != 404 {
            return Err(CoreError::Sink(format!("clear state {key}: {status}")));
        }
        Ok(())
    }

    async fn write_checkpoint(&self, checkpoint: &Checkpoint) -> Result<(), CoreError> {
        let doc_id = crate::checkpoint_doc_id(&checkpoint.stream);
        let (status, _) = self
            .send(
                reqwest::Method::PUT,
                &format!("/{META_INDEX}/_doc/{doc_id}"),
                Some(crate::checkpoint_doc(checkpoint).to_string()),
            )
            .await?;
        if status != 200 && status != 201 {
            return Err(CoreError::Sink(format!("write checkpoint: {status}")));
        }
        Ok(())
    }

    async fn read_checkpoint(
        &self,
        stream: &pg2osync_core::checkpoint::StreamId,
    ) -> Result<Option<Checkpoint>, CoreError> {
        let doc_id = crate::checkpoint_doc_id(stream);
        let (status, body) = self
            .send(
                reqwest::Method::GET,
                &format!("/{META_INDEX}/_doc/{doc_id}"),
                None,
            )
            .await?;
        if status == 404 {
            // written before checkpoints were kept per stream; the caller
            // still checks that it belongs to this one
            let (status, body) = self
                .send(
                    reqwest::Method::GET,
                    &format!("/{META_INDEX}/_doc/{}", crate::CHECKPOINT_DOC_ID),
                    None,
                )
                .await?;
            if status != 200 {
                return Ok(None);
            }
            return Ok(crate::checkpoint_from_doc(&body["_source"]));
        }
        if status != 200 {
            return Err(CoreError::Sink(format!("read checkpoint: {status}")));
        }
        Ok(crate::checkpoint_from_doc(&body["_source"]))
    }

    async fn health(&self) -> Result<Health, CoreError> {
        let (status, _) = self.send(reqwest::Method::GET, "/", None).await?;
        if status == 200 {
            Ok(Health::Up)
        } else {
            Ok(Health::Down(format!("status {status}")))
        }
    }

    async fn has_pipeline(&self, name: &str) -> Result<bool, CoreError> {
        let (status, _) = self
            .send(reqwest::Method::GET, &pipeline_path(name), None)
            .await?;
        match status {
            200 => Ok(true),
            404 => Ok(false),
            status => Err(CoreError::Sink(format!(
                "ingest pipeline {name:?} lookup failed: status {status}"
            ))),
        }
    }
}

fn is_retryable(e: &CoreError) -> bool {
    matches!(e, CoreError::SinkTransient(_))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    fn lines(batch: &[LsnOp]) -> Vec<Value> {
        ndjson_body(batch)
            .expect("bulk actions")
            .lines()
            // the body ends with the blank line the bulk API requires
            .filter(|line| !line.is_empty())
            .map(|line| serde_json::from_str(line).expect("json line"))
            .collect()
    }

    #[test]
    fn a_routed_action_header_carries_routing_and_version() {
        let batch = vec![
            LsnOp {
                lsn: Lsn(0x2A),
                op: DocumentOp::Upsert {
                    index: "shop".into(),
                    id: "order-7".into(),
                    routing: Some("7".into()),
                    doc: json!({"amount": 3}),
                    version: Some(0x2A),
                    pipeline: None,
                },
            },
            LsnOp {
                lsn: Lsn(0x2A),
                op: DocumentOp::Delete {
                    index: "shop".into(),
                    id: "order-8".into(),
                    routing: Some("7".into()),
                    version: Some(0x2A),
                },
            },
        ];
        assert_eq!(
            lines(&batch),
            vec![
                json!({"index": {"_index": "shop", "_id": "order-7", "routing": "7",
                                 "version": 42, "version_type": "external_gte"}}),
                json!({"amount": 3}),
                json!({"delete": {"_index": "shop", "_id": "order-8", "routing": "7",
                                  "version": 42, "version_type": "external_gte"}}),
            ]
        );
    }

    #[test]
    fn a_pipeline_name_is_encoded_before_it_reaches_the_url() {
        assert_eq!(
            pipeline_path("embed-products"),
            "/_ingest/pipeline/embed-products"
        );
        assert_eq!(pipeline_path("a/b?c"), "/_ingest/pipeline/a%2Fb%3Fc");
    }

    #[test]
    fn a_parent_id_is_encoded_before_it_reaches_the_url() {
        assert_eq!(query_string(&[("routing", "a&b#c")]), "routing=a%26b%23c");
        assert_eq!(
            query_string(&[("routing", "customer-1")]),
            "routing=customer-1"
        );
    }

    #[test]
    fn an_upsert_names_its_pipeline_and_a_delete_does_not() {
        let batch = vec![
            LsnOp {
                lsn: Lsn(0x2A),
                op: DocumentOp::Upsert {
                    index: "products".into(),
                    id: "7".into(),
                    routing: None,
                    doc: json!({"name": "lamp"}),
                    version: Some(0x2A),
                    pipeline: Some("embed-products".into()),
                },
            },
            LsnOp {
                lsn: Lsn(0x2A),
                op: DocumentOp::Delete {
                    index: "products".into(),
                    id: "8".into(),
                    routing: None,
                    version: Some(0x2A),
                },
            },
        ];
        assert_eq!(
            lines(&batch),
            vec![
                json!({"index": {"_index": "products", "_id": "7", "version": 42,
                                 "version_type": "external_gte", "pipeline": "embed-products"}}),
                json!({"name": "lamp"}),
                json!({"delete": {"_index": "products", "_id": "8", "version": 42,
                                  "version_type": "external_gte"}}),
            ]
        );
    }

    #[test]
    fn an_unrouted_action_header_is_unchanged() {
        let batch = vec![LsnOp {
            lsn: Lsn(1),
            op: DocumentOp::Delete {
                index: "users".into(),
                id: "1".into(),
                routing: None,
                version: None,
            },
        }];
        assert_eq!(
            lines(&batch),
            vec![json!({"delete": {"_index": "users", "_id": "1"}})]
        );
    }

    /// A stand-in for Elasticsearch: `answer` maps each request line
    /// (`"POST /orders/_refresh"`) to the status and body it replies with, and
    /// `seen` records every request line in the order it arrived.
    ///
    /// Hand-rolled over the TCP listener tokio already ships rather than a
    /// mocking crate: the sink speaks plain HTTP/1.1 through reqwest, and a
    /// canned status per path is all a refresh test needs.
    struct FakeTarget {
        url: String,
        seen: Arc<Mutex<Vec<String>>>,
    }

    impl FakeTarget {
        async fn start(answer: fn(&str) -> (u16, &'static str)) -> Self {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .expect("bind a loopback port");
            let url = format!("http://{}", listener.local_addr().expect("local address"));
            let seen = Arc::new(Mutex::new(Vec::new()));
            let record = Arc::clone(&seen);
            tokio::spawn(async move {
                while let Ok((mut stream, _)) = listener.accept().await {
                    let record = Arc::clone(&record);
                    tokio::spawn(async move {
                        let Some(line) = read_request(&mut stream).await else {
                            return;
                        };
                        crate::lock(&record).push(line.clone());
                        let (status, body) = answer(&line);
                        let response = format!(
                            "HTTP/1.1 {status} Fake\r\nContent-Type: application/json\r\n\
                             Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
                            body.len()
                        );
                        let _ = stream.write_all(response.as_bytes()).await;
                        let _ = stream.shutdown().await;
                    });
                }
            });
            Self { url, seen }
        }

        fn sink(&self) -> ElasticsearchSink {
            ElasticsearchSink::new(ElasticsearchSinkConfig {
                url: self.url.clone(),
                username: None,
                password: None,
                api_key: None,
                tls_verify: true,
                retry: crate::RetryPolicy::default(),
            })
            .expect("a sink over plain http")
        }

        fn seen(&self) -> Vec<String> {
            crate::lock(&self.seen).clone()
        }
    }

    /// The request line of one HTTP/1.1 request, once the whole request —
    /// headers and the body Content-Length announces — has been read, so the
    /// reply never races the client's own write.
    async fn read_request(stream: &mut tokio::net::TcpStream) -> Option<String> {
        let mut buf = Vec::new();
        let mut chunk = [0u8; 4096];
        loop {
            let n = stream.read(&mut chunk).await.ok()?;
            if n == 0 {
                return None;
            }
            buf.extend_from_slice(&chunk[..n]);
            let Some(end) = buf.windows(4).position(|w| w == b"\r\n\r\n") else {
                continue;
            };
            let head = String::from_utf8_lossy(&buf[..end]).into_owned();
            let announced = head
                .lines()
                .find_map(|l| {
                    l.split_once(':')
                        .filter(|(k, _)| k.eq_ignore_ascii_case("content-length"))
                })
                .and_then(|(_, v)| v.trim().parse::<usize>().ok())
                .unwrap_or(0);
            if buf.len() - end - 4 >= announced {
                let mut words = head.lines().next().unwrap_or_default().split_whitespace();
                return Some(format!(
                    "{} {}",
                    words.next().unwrap_or_default(),
                    words.next().unwrap_or_default()
                ));
            }
        }
    }

    fn refresh_is_refused(line: &str) -> (u16, &'static str) {
        if line.ends_with("/_refresh") {
            (503, r#"{"error":{"type":"unavailable_shards_exception"}}"#)
        } else {
            (200, r#"{"hits":{"hits":[],"total":{"value":0}}}"#)
        }
    }

    fn assert_stopped_at_refresh(result: Result<(), CoreError>, seen: &[String]) {
        match result {
            Err(CoreError::Sink(msg)) => assert!(msg.contains("refresh"), "{msg}"),
            other => panic!("a refused refresh was not an error: {other:?}"),
        }
        assert_eq!(
            seen.iter().filter(|l| l.ends_with("/_refresh")).count(),
            1,
            "{seen:?}"
        );
        assert_eq!(seen.len(), 1, "a search ran on a stale index: {seen:?}");
    }

    #[tokio::test]
    async fn a_refused_refresh_stops_a_cascade_delete_before_it_searches() {
        let target = FakeTarget::start(refresh_is_refused).await;
        let result = target
            .sink()
            .delete_children("orders", "relation", "customer", "7", Some(42))
            .await;
        assert_stopped_at_refresh(result, &target.seen());
    }

    #[tokio::test]
    async fn a_refused_refresh_stops_a_truncate_before_it_deletes() {
        let target = FakeTarget::start(refresh_is_refused).await;
        let result = target.sink().truncate_index("orders", None, None).await;
        assert_stopped_at_refresh(result, &target.seen());
    }

    #[tokio::test]
    async fn a_refused_refresh_stops_the_rejects_listing_before_it_counts() {
        let target = FakeTarget::start(refresh_is_refused).await;
        let result = target.sink().list_rejects(10).await.map(|_| ());
        assert_stopped_at_refresh(result, &target.seen());
    }

    #[tokio::test]
    async fn a_rejects_index_that_was_never_created_lists_nothing() {
        let target = FakeTarget::start(|line| {
            if line.ends_with("/_refresh") {
                (404, r#"{"error":{"type":"index_not_found_exception"}}"#)
            } else {
                (200, "{}")
            }
        })
        .await;
        let listed = target
            .sink()
            .list_rejects(10)
            .await
            .expect("an empty quarantine");
        assert_eq!(listed.1, 0);
        assert!(listed.0.is_empty());
        assert_eq!(target.seen().len(), 1, "{:?}", target.seen());
    }

    fn a_rejection() -> pg2osync_core::sink::Rejection {
        pg2osync_core::sink::Rejection {
            index: "orders".into(),
            doc_id: "7".into(),
            reason: "mapper_parsing_exception".into(),
            lsn: Lsn(0x2A),
            op: DocumentOp::Upsert {
                index: "orders".into(),
                id: "7".into(),
                routing: None,
                doc: json!({"amount": 3}),
                version: Some(0x2A),
                pipeline: None,
            },
        }
    }

    #[tokio::test]
    async fn a_rejects_index_that_cannot_be_created_fails_the_quarantine() {
        let target = FakeTarget::start(|line| match line {
            "PUT /.pg2osync_rejects" => (500, r#"{"error":{"type":"illegal_state_exception"}}"#),
            _ => (201, "{}"),
        })
        .await;
        let result = target.sink().quarantine(&[a_rejection()]).await;
        assert!(
            matches!(result, Err(CoreError::Sink(_))),
            "an index that was never created took a document: {result:?}"
        );
        assert_eq!(target.seen(), vec!["PUT /.pg2osync_rejects".to_string()]);
    }

    #[tokio::test]
    async fn a_rejects_index_that_already_exists_is_not_created_twice() {
        let target = FakeTarget::start(|line| match line {
            "PUT /.pg2osync_rejects" => (
                400,
                r#"{"error":{"type":"resource_already_exists_exception"}}"#,
            ),
            _ => (201, "{}"),
        })
        .await;
        target
            .sink()
            .quarantine(&[a_rejection()])
            .await
            .expect("the existing index takes the document");
        assert_eq!(target.seen().len(), 2, "{:?}", target.seen());
    }
}
