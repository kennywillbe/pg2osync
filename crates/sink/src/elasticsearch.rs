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

impl ElasticsearchSink {
    async fn put_settings(&self, index: &str, body: serde_json::Value) -> Result<(), CoreError> {
        let (status, body) = self
            .send(
                reqwest::Method::PUT,
                &format!("/{index}/_settings"),
                Some(body.to_string()),
            )
            .await?;
        if status != 200 {
            return Err(CoreError::Sink(format!("settings for {index}: {status} {body}")));
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

    async fn bulk_once(
        &self,
        batch: &[LsnOp],
    ) -> Result<(Lsn, Vec<(String, String, String)>), CoreError> {
        let mut ndjson = String::new();
        for op in batch {
            match &op.op {
                DocumentOp::Upsert { index, id, doc } => {
                    ndjson.push_str(&format!(
                        "{{\"index\":{{\"_index\":{},\"_id\":{}}}}}\n",
                        serde_json::to_string(index).unwrap(),
                        serde_json::to_string(id).unwrap()
                    ));
                    ndjson.push_str(&serde_json::to_string(doc).unwrap());
                    ndjson.push('\n');
                }
                DocumentOp::Delete { index, id } => {
                    ndjson.push_str(&format!(
                        "{{\"delete\":{{\"_index\":{},\"_id\":{}}}}}\n",
                        serde_json::to_string(index).unwrap(),
                        serde_json::to_string(id).unwrap()
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
        for item in body["items"].as_array().cloned().unwrap_or_default() {
            // first key is the action name ("index"/"create"/"delete")
            let entry = item
                .as_object()
                .and_then(|o| o.values().next())
                .cloned()
                .unwrap_or(Value::Null);
            let s = entry["status"].as_u64().unwrap_or(0);
            if s == 429 || s >= 500 {
                retryable = true;
            } else if !(200..300).contains(&s) {
                permanent.push((
                    entry["_id"].as_str().unwrap_or("?").into(),
                    entry["_index"].as_str().unwrap_or("?").into(),
                    entry["error"]["type"].as_str().unwrap_or("unknown").into(),
                ));
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
                    for (id, index, reason) in &permanent {
                        tracing::error!(target: "pg2osync::sink", "PERMANENT rejection id={id} {index}: {reason}");
                    }
                    if let Some(first) = permanent.first() {
                        return Err(CoreError::DocumentRejected {
                            doc_id: first.0.clone(),
                            index: first.1.clone(),
                            reason: first.2.clone(),
                        });
                    }
                    return Ok(SinkAck { max_lsn: lsn });
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

    async fn truncate_index(&self, index: &str) -> Result<(), CoreError> {
        // an unrefreshed write is invisible to _delete_by_query and would
        // outlive the TRUNCATE that is supposed to remove it
        let _ = self
            .send(reqwest::Method::POST, &format!("/{index}/_refresh"), None)
            .await;
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
