//! Sink implementations: OpenSearch (reference), Elasticsearch, Meilisearch.

pub mod elasticsearch;
pub mod meilisearch;

use async_trait::async_trait;
use opensearch::auth::Credentials;
use opensearch::http::response::Response;
use opensearch::http::transport::{SingleNodeConnectionPool, TransportBuilder};
use opensearch::{BulkOperation, BulkParts, GetParts, IndexParts, OpenSearch};
use pg2osync_core::error::CoreError;
use pg2osync_core::lsn::Lsn;
use pg2osync_core::sink::{Health, IndexSpec, LsnOp, Sink, SinkAck};
use serde_json::{Value, json};

pub const META_INDEX: &str = ".pg2osync_meta";

pub struct OpenSearchSink {
    client: OpenSearch,
    serverless: bool,
}

#[derive(Debug, Clone)]
pub struct OpenSearchSinkConfig {
    pub url: String,
    /// Amazon OpenSearch Serverless: skip refresh/setting operations the
    /// service does not support; index policies must exist beforehand.
    pub serverless: bool,
    pub username: Option<String>,
    pub password: Option<String>,
    pub tls_verify: bool,
}

/// Retry policy for transient failures (ADR plan §4 [engine]).
#[derive(Debug, Clone)]
pub struct RetryPolicy {
    pub max_attempts: u32,
    pub base_backoff_ms: u64,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 10,
            base_backoff_ms: 500,
        }
    }
}

impl OpenSearchSink {
    pub fn new(cfg: OpenSearchSinkConfig) -> Result<Self, CoreError> {
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
            serverless: cfg.serverless,
        })
    }

    /// Create the hidden checkpoint index if missing (ADR #18).
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
        check_status(resp, "create meta index").await
    }

    /// Persist the checkpoint document (single-doc atomicity per ADR #18).
    pub async fn write_checkpoint(
        &self,
        slot_name: &str,
        publication: &str,
        lsn: Lsn,
    ) -> Result<(), CoreError> {
        let resp = self
            .client
            .index(IndexParts::IndexId(META_INDEX, "default"))
            .body(json!({
                "slot_name": slot_name,
                "publication": publication,
                "confirmed_lsn": lsn.to_string(),
                "instance_id": std::env::var("PG2OSYNC_INSTANCE_ID").unwrap_or_default(),
                "updated_at_epoch": chrono_now(),
                "schema_version": 1
            }))
            .send()
            .await
            .map_err(http_err)?;
        check_status(resp, "write checkpoint").await
    }

    /// Read last confirmed LSN; None when no checkpoint exists yet.
    pub async fn read_checkpoint(&self) -> Result<Option<Lsn>, CoreError> {
        let resp = self
            .client
            .get(GetParts::IndexId(META_INDEX, "default"))
            .send()
            .await
            .map_err(http_err)?;
        if resp.status_code().as_u16() == 404 {
            return Ok(None);
        }
        check_status(resp, "read checkpoint").await?;
        // re-issue as get to fetch body: simplest is a second request
        let resp = self
            .client
            .get(GetParts::IndexId(META_INDEX, "default"))
            .send()
            .await
            .map_err(http_err)?;
        let body: Value = resp
            .json()
            .await
            .map_err(|e| CoreError::Sink(e.to_string()))?;
        match body["_source"]["confirmed_lsn"].as_str() {
            Some(s) => Ok(Some(s.parse().map_err(CoreError::from)?)),
            None => Ok(None),
        }
    }

    async fn bulk_once(
        &self,
        batch: &[LsnOp],
    ) -> Result<(Lsn, Vec<(String, String, String)>), CoreError> {
        // Build operations; upserts use index (last-write-wins by _id)
        // every operation carries its own _index header, so no URL-level index
        let ops: Vec<BulkOperation<Value>> = batch
            .iter()
            .map(|op| match &op.op {
                pg2osync_core::sink::DocumentOp::Upsert { index, id, doc } => {
                    BulkOperation::index(doc.clone())
                        .id(id.clone())
                        .index(index.clone())
                        .into()
                }
                pg2osync_core::sink::DocumentOp::Delete { index, id } => {
                    BulkOperation::delete(id.clone())
                        .index(index.clone())
                        .into()
                }
            })
            .collect();

        let resp = self
            .client
            .bulk(BulkParts::None)
            .body(ops)
            .send()
            .await
            .map_err(|e| CoreError::Sink(format!("bulk request failed: {e}")))?;

        let status = resp.status_code().as_u16();
        if status == 429 || status == 503 || status >= 500 {
            return Err(CoreError::Sink(format!("retryable http status {status}")));
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
        for item in body["items"].as_array().cloned().unwrap_or_default() {
            let entry = &item["index"];
            let item_status = entry["status"].as_u64().unwrap_or(0);
            if item_status == 429 || item_status >= 500 {
                retryable_http = true;
            } else if !(200..300).contains(&item_status) {
                permanent.push((
                    entry["_id"].as_str().unwrap_or("?").to_string(),
                    entry["_index"].as_str().unwrap_or("?").to_string(),
                    entry["error"]["type"]
                        .as_str()
                        .unwrap_or("unknown")
                        .to_string(),
                ));
            }
        }
        if retryable_http {
            return Err(CoreError::Sink("retryable item-level 429/5xx".into()));
        }
        let max_lsn = batch.last().expect("nonempty checked").lsn;
        Ok((max_lsn, permanent))
    }

    async fn bulk_with_retry(
        &self,
        batch: &[LsnOp],
        retry: &RetryPolicy,
    ) -> Result<(Lsn, Vec<(String, String, String)>), CoreError> {
        let mut attempt = 0u32;
        loop {
            attempt += 1;
            match self.bulk_once(batch).await {
                Ok((lsn, perm)) => return Ok((lsn, perm)),
                Err(e) if attempt < retry.max_attempts && is_retryable(&e) => {
                    let backoff = retry.base_backoff_ms * 2u64.saturating_pow(attempt - 1);
                    tracing::warn!(target: "pg2osync::sink", "bulk attempt {attempt} failed ({e}); backing off {backoff}ms");
                    tokio::time::sleep(std::time::Duration::from_millis(backoff.min(30_000))).await;
                }
                Err(e) => return Err(e),
            }
        }
    }
}

fn is_retryable(e: &CoreError) -> bool {
    matches!(e, CoreError::Sink(msg) if msg.contains("retryable") || msg.contains("request failed"))
}

fn http_err(e: opensearch::Error) -> CoreError {
    CoreError::Sink(format!("http request failed: {e}"))
}

async fn check_status(resp: Response, what: &str) -> Result<(), CoreError> {
    if !resp.status_code().is_success() {
        let status = resp.status_code();
        let body: Value = resp.json().await.unwrap_or(Value::Null);
        return Err(CoreError::Sink(format!("{what} failed: {status} {}", body)));
    }
    Ok(())
}

fn chrono_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[async_trait]
impl Sink for OpenSearchSink {
    async fn ensure_ready(&self, tables: &[IndexSpec]) -> Result<(), CoreError> {
        self.ensure_meta_index().await?;
        for spec in tables {
            let exists = self
                .client
                .indices()
                .exists(opensearch::indices::IndicesExistsParts::Index(
                    &[&spec.name],
                ))
                .send()
                .await
                .map_err(http_err)?;
            if !exists.status_code().is_success() {
                // dynamic mapping for v0.1; explicit mappings arrive with 0.3 config work
                let resp = self
                    .client
                    .indices()
                    .create(opensearch::indices::IndicesCreateParts::Index(&spec.name))
                    .send()
                    .await
                    .map_err(http_err)?;
                check_status(resp, &format!("create index {}", spec.name)).await?;
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
        let body = json!({"ids": ids});
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
        let retry = RetryPolicy::default();
        let (max_lsn, permanent) = self.bulk_with_retry(&batch, &retry).await?;
        for (id, index, reason) in &permanent {
            tracing::error!(target: "pg2osync::sink", "PERMANENT rejection id={id} {index}: {reason}");
        }
        if !permanent.is_empty() {
            // correctness-first failure policy (M4): halt pipeline on permanent errors
            return Err(CoreError::DocumentRejected {
                index: permanent[0].1.clone(),
                doc_id: permanent[0].0.clone(),
                reason: permanent[0].2.clone(),
            });
        }
        Ok(SinkAck { max_lsn })
    }

    async fn truncate_index(&self, index: &str) -> Result<(), CoreError> {
        let resp = self
            .client
            .delete_by_query(opensearch::DeleteByQueryParts::Index(&[index]))
            .body(json!({"query": {"match_all": {}}}))
            .send()
            .await
            .map_err(http_err)?;
        check_status(resp, &format!("truncate index {index}")).await?;
        if self.serverless {
            return Ok(()); // Serverless manages visibility itself
        }
        // refresh so subsequent reads see an empty index immediately
        let resp = self
            .client
            .indices()
            .refresh(opensearch::indices::IndicesRefreshParts::Index(&[index]))
            .send()
            .await
            .map_err(http_err)?;
        check_status(resp, "post-truncate refresh").await
    }

    async fn write_checkpoint(
        &self,
        slot_name: &str,
        publication: &str,
        lsn: Lsn,
    ) -> Result<(), CoreError> {
        self.ensure_meta_index().await?;
        let instance_id = std::env::var("PG2OSYNC_INSTANCE_ID").unwrap_or_default();
        let epoch = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let resp = self
            .client
            .index(IndexParts::IndexId(META_INDEX, "default"))
            .body(json!({
                "slot_name": slot_name,
                "publication": publication,
                "confirmed_lsn": lsn.to_string(),
                "instance_id": instance_id,
                "updated_at_epoch": epoch,
                "schema_version": 1
            }))
            .send()
            .await
            .map_err(http_err)?;
        check_status(resp, "write checkpoint").await
    }

    async fn read_checkpoint(&self) -> Result<Option<Lsn>, CoreError> {
        let resp = self
            .client
            .get(GetParts::IndexId(META_INDEX, "default"))
            .send()
            .await
            .map_err(http_err)?;
        if resp.status_code().as_u16() == 404 {
            return Ok(None);
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
        match body["_source"]["confirmed_lsn"].as_str() {
            Some(s) => Ok(Some(s.parse().map_err(CoreError::from)?)),
            None => Ok(None),
        }
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
}
