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

pub const META_INDEX: &str = ".pg2osync_meta";

pub struct ElasticsearchSink {
    http: reqwest::Client,
    base_url: String,
    retry: crate::RetryPolicy,
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

impl ElasticsearchSink {
    /// Clear an index by deleting each document at the truncate's own position.
    ///
    /// The loop stops when a round deletes nothing, which is also what makes it
    /// correct: a document written after the truncate carries a higher version,
    /// its delete is refused, and it survives.
    async fn truncate_at_version(&self, index: &str, version: i64) -> Result<(), CoreError> {
        const PAGE: usize = 1000;
        loop {
            let (_, body) = self
                .send(
                    reqwest::Method::POST,
                    &format!("/{index}/_search"),
                    Some(
                        json!({"size": PAGE, "_source": false, "query": {"match_all": {}}})
                            .to_string(),
                    ),
                )
                .await?;
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
                ndjson.push_str(&format!(
                    "{{\"delete\":{{\"_index\":{},\"_id\":{},\"version\":{version},\
                     \"version_type\":\"external_gte\"}}}}\n",
                    serde_json::to_string(index).unwrap(),
                    serde_json::to_string(id).unwrap(),
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
        })
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

    /// One bulk request. Returns how far it got and, for each refused document,
    /// its position in `batch` and why — the position because there is exactly
    /// one action per operation, in order, so it identifies the operation even
    /// when a batch holds two writes for the same document.
    async fn bulk_once(&self, batch: &[LsnOp]) -> Result<(Lsn, Vec<(usize, String)>), CoreError> {
        let mut ndjson = String::new();
        for op in batch {
            match &op.op {
                DocumentOp::Upsert {
                    index,
                    id,
                    doc,
                    version,
                } => {
                    ndjson.push_str(&format!(
                        "{{\"index\":{{\"_index\":{},\"_id\":{}{}}}}}\n",
                        serde_json::to_string(index).unwrap(),
                        serde_json::to_string(id).unwrap(),
                        version_fields(*version)
                    ));
                    ndjson.push_str(&serde_json::to_string(doc).unwrap());
                    ndjson.push('\n');
                }
                DocumentOp::Delete { index, id, version } => {
                    ndjson.push_str(&format!(
                        "{{\"delete\":{{\"_index\":{},\"_id\":{}{}}}}}\n",
                        serde_json::to_string(index).unwrap(),
                        serde_json::to_string(id).unwrap(),
                        version_fields(*version)
                    ));
                }
            }
        }
        ndjson.push('\n');

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
            } else if !(200..300).contains(&s) {
                let reason = entry["error"]["reason"].as_str().unwrap_or(error_type);
                permanent.push((nth, format!("{error_type}: {reason}")));
            }
        }
        if retryable {
            return Err(CoreError::SinkTransient(
                "item-level 429/5xx in bulk response".into(),
            ));
        }
        Ok((batch.last().expect("nonempty").lsn, permanent))
    }
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
            // without a body the index takes whatever Elasticsearch infers, or
            // whatever index template the operator already manages
            let create = spec
                .mapping
                .as_ref()
                .map(|m| crate::mapping::create_body(m).to_string());
            let (status, body) = self
                .send(reqwest::Method::PUT, &format!("/{}", spec.name), create)
                .await?;
            let already = body["error"]["type"] == json!("resource_already_exists_exception");
            if !(status == 200 || already) {
                return Err(CoreError::Sink(format!(
                    "create index {}: {status}",
                    spec.name
                )));
            }
            if already && let Some(mapping) = &spec.mapping {
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
        ids: &[String],
    ) -> Result<Vec<Option<Value>>, CoreError> {
        if ids.is_empty() {
            return Ok(vec![]);
        }
        let (status, body) = self
            .send(
                reqwest::Method::POST,
                &format!("/{index}/_mget"),
                Some(json!({"ids": ids}).to_string()),
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
        let mut attempt = 0u32;
        loop {
            attempt += 1;
            match self.bulk_once(&batch).await {
                Ok((lsn, permanent)) => {
                    return Ok(SinkAck {
                        max_lsn: lsn,
                        rejected: crate::rejections(&batch, permanent)?,
                    });
                }
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

    async fn truncate_index(&self, index: &str, version: Option<u64>) -> Result<(), CoreError> {
        // an unrefreshed write is invisible to _delete_by_query and would
        // outlive the TRUNCATE that is supposed to remove it
        let _ = self
            .send(reqwest::Method::POST, &format!("/{index}/_refresh"), None)
            .await;
        // delete_by_query is internally versioned, so it leaves a tombstone
        // above the document's own version and a replayed write is then
        // rejected forever. A versioned bulk delete puts the tombstone at the
        // truncate's position instead.
        if let Some(version) = crate::external_version(version) {
            return self.truncate_at_version(index, version).await;
        }
        let (status, _) = self
            .send(
                reqwest::Method::POST,
                &format!("/{index}/_delete_by_query?refresh=true&conflicts=proceed"),
                Some(json!({"query": {"match_all": {}}}).to_string()),
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
        let (status, _) = self
            .send(
                reqwest::Method::POST,
                &format!("/{}/_refresh", indices.join(",")),
                None,
            )
            .await?;
        if status != 200 {
            return Err(CoreError::Sink(format!("refresh: {status}")));
        }
        Ok(())
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
        let _ = self
            .send(
                reqwest::Method::PUT,
                &format!("/{}", crate::REJECTS_INDEX),
                Some(
                    json!({
                        "settings": {"index": {"hidden": true, "number_of_shards": 1}},
                        "mappings": {"properties": {
                            "document": {"type": "object", "enabled": false}
                        }}
                    })
                    .to_string(),
                ),
            )
            .await;
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
        let _ = self
            .send(
                reqwest::Method::POST,
                &format!("/{}/_refresh", crate::REJECTS_INDEX),
                None,
            )
            .await;
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
        // nothing has ever been quarantined, which is not an error
        if status == 404 {
            return Ok((Vec::new(), 0));
        }
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
}

fn is_retryable(e: &CoreError) -> bool {
    matches!(e, CoreError::SinkTransient(_))
}
