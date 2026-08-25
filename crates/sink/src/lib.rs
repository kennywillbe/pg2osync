//! Sink implementations: OpenSearch (reference), Elasticsearch, Meilisearch.

pub mod elasticsearch;
pub mod meilisearch;

use async_trait::async_trait;
use opensearch::auth::Credentials;
use opensearch::http::response::Response;
use opensearch::http::transport::{SingleNodeConnectionPool, TransportBuilder};
use opensearch::{BulkOperation, BulkParts, GetParts, IndexParts, OpenSearch};
use pg2osync_core::checkpoint::Checkpoint;
use pg2osync_core::error::CoreError;
use pg2osync_core::lsn::Lsn;
use pg2osync_core::sink::{Health, IndexSpec, LsnOp, Sink, SinkAck};
use serde_json::{Value, json};

pub const META_INDEX: &str = ".pg2osync_meta";
/// Single checkpoint document per pipeline; per-document atomicity is what
/// makes the write crash-safe without any compare-and-swap.
pub const CHECKPOINT_DOC_ID: &str = "default";
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
    serverless: bool,
    retry: RetryPolicy,
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
    pub retry: RetryPolicy,
}

/// Retry policy for transient failures; tunable via `[engine]` config.
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
            retry: cfg.retry,
        })
    }

    /// Create the hidden checkpoint index if missing.
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
    matches!(e, CoreError::SinkTransient(_))
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
        let (max_lsn, permanent) = self.bulk_with_retry(&batch, &self.retry).await?;
        for (id, index, reason) in &permanent {
            tracing::error!(target: "pg2osync::sink", "PERMANENT rejection id={id} {index}: {reason}");
        }
        if !permanent.is_empty() {
            // correctness-first failure policy: a document the sink will never
            // accept must stop the pipeline instead of being skipped silently
            return Err(CoreError::DocumentRejected {
                index: permanent[0].1.clone(),
                doc_id: permanent[0].0.clone(),
                reason: permanent[0].2.clone(),
            });
        }
        Ok(SinkAck { max_lsn })
    }

    async fn truncate_index(&self, index: &str) -> Result<(), CoreError> {
        // delete_by_query only removes documents a search can see, so writes
        // still sitting in the translog would survive the TRUNCATE and
        // resurrect rows the source has already dropped
        if !self.serverless {
            let resp = self
                .client
                .indices()
                .refresh(opensearch::indices::IndicesRefreshParts::Index(&[index]))
                .send()
                .await
                .map_err(http_err)?;
            check_status(resp, "pre-truncate refresh").await?;
        }
        let resp = self
            .client
            .delete_by_query(opensearch::DeleteByQueryParts::Index(&[index]))
            .refresh(true)
            .conflicts(opensearch::params::Conflicts::Proceed)
            .body(json!({"query": {"match_all": {}}}))
            .send()
            .await
            .map_err(http_err)?;
        check_status(resp, &format!("truncate index {index}")).await
    }

    async fn refresh(&self, indices: &[String]) -> Result<(), CoreError> {
        if self.serverless || indices.is_empty() {
            // Serverless manages visibility itself and rejects the call
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

    async fn write_checkpoint(&self, checkpoint: &Checkpoint) -> Result<(), CoreError> {
        self.ensure_meta_index().await?;
        let resp = self
            .client
            .index(IndexParts::IndexId(META_INDEX, CHECKPOINT_DOC_ID))
            .body(checkpoint_doc(checkpoint))
            .send()
            .await
            .map_err(http_err)?;
        check_status(resp, "write checkpoint").await
    }

    async fn read_checkpoint(&self) -> Result<Option<Checkpoint>, CoreError> {
        let resp = self
            .client
            .get(GetParts::IndexId(META_INDEX, CHECKPOINT_DOC_ID))
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use pg2osync_core::checkpoint::{SOURCE_MYSQL, SOURCE_POSTGRES, StreamId};

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
        assert!(!is_retryable(&CoreError::DocumentRejected {
            index: "i".into(),
            doc_id: "1".into(),
            reason: "mapper_parsing_exception".into(),
        }));
    }
}
