//! Elasticsearch implementation of the `core::Sink` trait.
//!
//! Deliberately a thin raw-REST client instead of the `elasticsearch` crate:
//! we need only ~6 endpoints and avoid pulling a second generated-client HTTP
//! stack into the binary (VISION §2.3).

use async_trait::async_trait;
use base64::Engine as _;
use pg2osync_core::error::CoreError;
use pg2osync_core::lsn::Lsn;
use pg2osync_core::sink::{DocumentOp, Health, IndexSpec, LsnOp, Sink, SinkAck};
use serde_json::{Value, json};

pub const META_INDEX: &str = ".pg2osync_meta";

pub struct ElasticsearchSink {
    http: reqwest::Client,
    base_url: String,
}

#[derive(Debug, Clone)]
pub struct ElasticsearchSinkConfig {
    pub url: String,
    pub username: Option<String>,
    pub password: Option<String>,
    /// ES Cloud / API-key auth: base64 "id:api_key".
    pub api_key: Option<String>,
    pub tls_verify: bool,
}

impl ElasticsearchSink {
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
            .map_err(|e| CoreError::Sink(format!("bulk request failed: {e}")))?;
        let status = resp.status().as_u16();
        if status == 429 || status >= 500 {
            return Err(CoreError::Sink(format!("retryable http status {status}")));
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
            return Err(CoreError::Sink("retryable item-level 429/5xx".into()));
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
            let (status, body) = self
                .send(reqwest::Method::PUT, &format!("/{}", spec.name), None)
                .await?;
            let already = body["error"]["type"] == json!("resource_already_exists_exception");
            if !(status == 200 || already) {
                return Err(CoreError::Sink(format!(
                    "create index {}: {status}",
                    spec.name
                )));
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
                Err(e) if attempt < 10 && is_retryable(&e) => {
                    let backoff = 500u64 * 2u64.saturating_pow(attempt - 1);
                    tokio::time::sleep(std::time::Duration::from_millis(backoff.min(30_000))).await;
                }
                Err(e) => return Err(e),
            }
        }
    }

    async fn truncate_index(&self, index: &str) -> Result<(), CoreError> {
        let (status, _) = self
            .send(
                reqwest::Method::POST,
                &format!("/{index}/_delete_by_query"),
                Some(
                    json!({
                        "query": {"match_all": {}}
                    })
                    .to_string(),
                ),
            )
            .await?;
        if status != 200 {
            return Err(CoreError::Sink(format!("truncate {index}: {status}")));
        }
        let _ = self
            .send(reqwest::Method::POST, &format!("/{index}/_refresh"), None)
            .await;
        Ok(())
    }

    async fn write_checkpoint(
        &self,
        slot_name: &str,
        publication: &str,
        lsn: Lsn,
    ) -> Result<(), CoreError> {
        let epoch = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let (status, _) = self
            .send(
                reqwest::Method::PUT,
                &format!("/{META_INDEX}/_doc/default"),
                Some(
                    json!({
                        "slot_name": slot_name,
                        "publication": publication,
                        "confirmed_lsn": lsn.to_string(),
                        "instance_id": std::env::var("PG2OSYNC_INSTANCE_ID").unwrap_or_default(),
                        "updated_at_epoch": epoch,
                        "schema_version": 1
                    })
                    .to_string(),
                ),
            )
            .await?;
        if status != 200 && status != 201 {
            return Err(CoreError::Sink(format!("write checkpoint: {status}")));
        }
        Ok(())
    }

    async fn read_checkpoint(&self) -> Result<Option<Lsn>, CoreError> {
        let (status, body) = self
            .send(
                reqwest::Method::GET,
                &format!("/{META_INDEX}/_doc/default"),
                None,
            )
            .await?;
        if status == 404 {
            return Ok(None);
        }
        if status != 200 {
            return Err(CoreError::Sink(format!("read checkpoint: {status}")));
        }
        match body["_source"]["confirmed_lsn"].as_str() {
            Some(s) => Ok(Some(s.parse().map_err(CoreError::from)?)),
            None => Ok(None),
        }
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
    matches!(e, CoreError::Sink(m) if m.contains("retryable") || m.contains("request failed"))
}
