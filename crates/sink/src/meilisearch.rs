//! Meilisearch sink: a schema-less target.
//!
//! Deviations from the OpenSearch contract:
//! - no mappings: `ensure_ready` creates the index with primaryKey "id"
//! - writes are async server-side tasks; we wait for completion before acking
//!   so at-least-once semantics hold
//! - no arbitrary-document storage exists, so the checkpoint falls back to a
//!   local JSON file instead of an in-cluster document (documented limitation)

use async_trait::async_trait;
use pg2osync_core::checkpoint::Checkpoint;
use pg2osync_core::error::CoreError;
use pg2osync_core::sink::{DocumentOp, Health, IndexSpec, LsnOp, Sink, SinkAck};
use serde_json::{Value, json};

pub struct MeilisearchSink {
    http: reqwest::Client,
    base_url: String,
    state_dir: String,
}

#[derive(Debug, Clone)]
pub struct MeilisearchSinkConfig {
    pub url: String,
    pub api_key: Option<String>,
    /// Directory for the checkpoint file (no in-cluster storage available).
    pub state_dir: String,
}

impl MeilisearchSink {
    pub fn new(cfg: MeilisearchSinkConfig) -> Result<Self, CoreError> {
        let mut headers = reqwest::header::HeaderMap::new();
        if let Some(key) = &cfg.api_key {
            let v = format!("Bearer {key}")
                .parse()
                .map_err(|e| CoreError::Sink(format!("bad key: {e}")))?;
            headers.insert(reqwest::header::AUTHORIZATION, v);
        }
        let http = reqwest::Client::builder()
            .default_headers(headers)
            .timeout(std::time::Duration::from_secs(60))
            .build()
            .map_err(|e| CoreError::Sink(e.to_string()))?;
        std::fs::create_dir_all(&cfg.state_dir)
            .map_err(|e| CoreError::Sink(format!("state dir {}: {e}", cfg.state_dir)))?;
        Ok(Self {
            http,
            base_url: cfg.url.trim_end_matches('/').to_string(),
            state_dir: cfg.state_dir,
        })
    }

    async fn send(
        &self,
        method: reqwest::Method,
        path: &str,
        body: Option<Value>,
    ) -> Result<(u16, Value), CoreError> {
        let mut req = self
            .http
            .request(method, format!("{}{}", self.base_url, path));
        if let Some(b) = body {
            req = req.json(&b);
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

    /// Meilisearch operations are async tasks; block (bounded) until success.
    async fn wait_task(&self, task_uid: u64) -> Result<(), CoreError> {
        for _ in 0..600 {
            let (status, body) = self
                .send(reqwest::Method::GET, &format!("/tasks/{task_uid}"), None)
                .await?;
            if status == 200 {
                match body["status"].as_str() {
                    Some("succeeded") => return Ok(()),
                    Some("failed") => {
                        return Err(CoreError::Sink(format!(
                            "task {task_uid} failed: {}",
                            body["error"]
                        )));
                    }
                    _ => {}
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        Err(CoreError::Sink("task timed out after 30s".into()))
    }

    /// Named after the stream, so two pipelines sharing a state directory do
    /// not overwrite each other's position.
    fn checkpoint_path(&self, stream: &pg2osync_core::checkpoint::StreamId) -> String {
        format!(
            "{}/checkpoint-{}.json",
            self.state_dir,
            crate::checkpoint_doc_id(stream)
        )
    }

    /// Named state lives beside the checkpoint, for the same reason: there is
    /// no arbitrary-document storage in Meilisearch to keep it in.
    fn state_path(&self, key: &str) -> String {
        format!("{}/{key}.json", self.state_dir)
    }

    /// Where checkpoints lived before they were kept per stream.
    fn legacy_checkpoint_path(&self) -> String {
        format!("{}/checkpoint.json", self.state_dir)
    }
}

#[async_trait]
impl Sink for MeilisearchSink {
    /// Meilisearch keeps whichever write it applied last, so two requests open
    /// at once could settle a document either way round.
    fn orders_by_version(&self) -> bool {
        false
    }

    async fn ensure_ready(&self, tables: &[IndexSpec]) -> Result<(), CoreError> {
        for spec in tables {
            if spec.mapping.is_some() {
                // Meilisearch has no field types to declare; its equivalent is
                // the settings API, which is a different feature than this one
                return Err(CoreError::Sink(format!(
                    "index {} has a mapping configured, which Meilisearch has no \
                     equivalent for; remove mapping_file for this target",
                    spec.name
                )));
            }
            // "id" as primary key matches our pk_to_id rendering contract
            let (status, body) = self
                .send(
                    reqwest::Method::POST,
                    "/indexes",
                    Some(json!({
                        "uid": spec.name,
                        "primaryKey": "id"
                    })),
                )
                .await?;
            if status == 202 {
                if let Some(uid) = body["taskUid"].as_u64() {
                    self.wait_task(uid).await?;
                }
            } else if status != 200 && status != 201 {
                // index_already_exists is acceptable
                let err = body.to_string();
                if !err.contains("already_exists") && !err.contains("existing") {
                    return Err(CoreError::Sink(format!(
                        "create index {}: {status} {err}",
                        spec.name
                    )));
                }
            }
        }
        Ok(())
    }

    async fn get_documents(
        &self,
        index: &str,
        // routing is ignored: an id-only document model has no shards
        ids: &[(String, Option<String>)],
    ) -> Result<Vec<Option<Value>>, CoreError> {
        let mut out = Vec::with_capacity(ids.len());
        for (id, _) in ids {
            let (status, body) = self
                .send(
                    reqwest::Method::GET,
                    &format!("/indexes/{index}/documents/{id}"),
                    None,
                )
                .await?;
            out.push(if status == 200 { Some(body) } else { None });
        }
        Ok(out)
    }

    async fn write(&self, batch: Vec<LsnOp>) -> Result<SinkAck, CoreError> {
        if batch.is_empty() {
            return Err(CoreError::Sink(
                "engine must never send empty batches".into(),
            ));
        }
        // group by index: meili endpoints are per-index
        let mut by_index: HashMap<String, (Vec<Value>, Vec<String>)> = HashMap::new();
        for op in &batch {
            let index = match &op.op {
                DocumentOp::Upsert { index, .. } | DocumentOp::Delete { index, .. } => index,
                DocumentOp::DeleteChildren { .. } => {
                    // an error and not a no-op: a cascade dropped here would be
                    // indistinguishable from one that ran
                    return Err(CoreError::Sink(
                        "a join field's cascade cannot be expressed in Meilisearch, which \
                         has no parent-child data model; this operation should have been \
                         refused at startup"
                            .into(),
                    ));
                }
            };
            let entry = by_index
                .entry(index.clone())
                .or_insert_with(|| (vec![], vec![]));
            match &op.op {
                DocumentOp::Upsert { doc, .. } => entry.0.push(doc.clone()),
                DocumentOp::Delete { id, .. } => entry.1.push(id.clone()),
                DocumentOp::DeleteChildren { .. } => {}
            }
        }
        for (index, (docs, del_ids)) in by_index {
            if !docs.is_empty() {
                let (status, body) = self
                    .send(
                        reqwest::Method::POST,
                        &format!("/indexes/{index}/documents?primaryKey=id"),
                        Some(Value::Array(docs)),
                    )
                    .await?;
                if status != 202 {
                    return Err(CoreError::Sink(format!(
                        "add documents to {index}: {status}"
                    )));
                }
                if let Some(uid) = body["taskUid"].as_u64() {
                    self.wait_task(uid).await?;
                }
            }
            if !del_ids.is_empty() {
                let (status, body) = self
                    .send(
                        reqwest::Method::POST,
                        &format!("/indexes/{index}/documents/delete-batch"),
                        Some(Value::Array(
                            del_ids.into_iter().map(Value::String).collect(),
                        )),
                    )
                    .await?;
                if status != 202 {
                    return Err(CoreError::Sink(format!(
                        "delete documents from {index}: {status}"
                    )));
                }
                if let Some(uid) = body["taskUid"].as_u64() {
                    self.wait_task(uid).await?;
                }
            }
        }
        Ok(SinkAck::written(batch.last().expect("nonempty").lsn))
    }

    async fn truncate_index(
        &self,
        index: &str,
        // Meilisearch has no document versions, so there is no ordering to
        // preserve and nothing to carry here
        _version: Option<u64>,
        only: Option<(&str, &str)>,
    ) -> Result<(), CoreError> {
        // a scoped clear only exists for a join pair, which is refused for
        // this target at config load
        if only.is_some() {
            return Err(CoreError::Sink(
                "a scoped truncate cannot be expressed in Meilisearch; this should have been \
                 refused at startup"
                    .into(),
            ));
        }
        let (status, body) = self
            .send(
                reqwest::Method::DELETE,
                &format!("/indexes/{index}/documents"),
                None,
            )
            .await?;
        if status != 202 {
            return Err(CoreError::Sink(format!("truncate {index}: {status}")));
        }
        if let Some(uid) = body["taskUid"].as_u64() {
            self.wait_task(uid).await?;
        }
        Ok(())
    }

    async fn refresh(&self, _indices: &[String]) -> Result<(), CoreError> {
        // writes are server-side tasks and the sink already waits for them to
        // complete, so an accepted write is searchable by then
        Ok(())
    }

    async fn read_state(&self, key: &str) -> Result<Option<Value>, CoreError> {
        match std::fs::read(self.state_path(key)) {
            Ok(bytes) => serde_json::from_slice::<Value>(&bytes)
                .map(Some)
                .map_err(|e| CoreError::Sink(format!("read state {key}: {e}"))),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(CoreError::Sink(format!("read state {key}: {e}"))),
        }
    }

    async fn write_state(&self, key: &str, doc: &Value) -> Result<(), CoreError> {
        let path = self.state_path(key);
        let tmp = format!("{path}.tmp");
        let bytes = serde_json::to_vec_pretty(doc).map_err(|e| CoreError::Sink(e.to_string()))?;
        // write-then-rename, as for the checkpoint: a truncated progress
        // document would be unreadable, and unreadable means reload
        std::fs::write(&tmp, bytes)
            .map_err(|e| CoreError::Sink(format!("write state {key}: {e}")))?;
        std::fs::rename(&tmp, &path)
            .map_err(|e| CoreError::Sink(format!("rename state {key}: {e}")))
    }

    async fn clear_state(&self, key: &str) -> Result<(), CoreError> {
        match std::fs::remove_file(self.state_path(key)) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(CoreError::Sink(format!("clear state {key}: {e}"))),
        }
    }

    async fn write_checkpoint(&self, checkpoint: &Checkpoint) -> Result<(), CoreError> {
        let path = self.checkpoint_path(&checkpoint.stream);
        let tmp = format!("{path}.tmp");
        let bytes = serde_json::to_vec_pretty(&crate::checkpoint_doc(checkpoint))
            .map_err(|e| CoreError::Sink(e.to_string()))?;
        // write-then-rename: a crash mid-write must not leave a truncated
        // checkpoint that would silently restart the pipeline from zero
        std::fs::write(&tmp, bytes)
            .map_err(|e| CoreError::Sink(format!("checkpoint write: {e}")))?;
        std::fs::rename(&tmp, &path).map_err(|e| CoreError::Sink(format!("checkpoint rename: {e}")))
    }

    async fn read_checkpoint(
        &self,
        stream: &pg2osync_core::checkpoint::StreamId,
    ) -> Result<Option<Checkpoint>, CoreError> {
        let read = |path: String| match std::fs::read(path) {
            Ok(bytes) => serde_json::from_slice::<Value>(&bytes)
                .map(|v| crate::checkpoint_from_doc(&v))
                .map_err(|e| CoreError::Sink(e.to_string())),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(CoreError::Sink(format!("checkpoint read: {e}"))),
        };
        match read(self.checkpoint_path(stream))? {
            Some(checkpoint) => Ok(Some(checkpoint)),
            // written before checkpoints were kept per stream; the caller
            // still checks that it belongs to this one
            None => read(self.legacy_checkpoint_path()),
        }
    }

    async fn health(&self) -> Result<Health, CoreError> {
        let (status, _) = self.send(reqwest::Method::GET, "/health", None).await?;
        if status == 200 {
            Ok(Health::Up)
        } else {
            Ok(Health::Down(format!("status {status}")))
        }
    }
}

use std::collections::HashMap;
