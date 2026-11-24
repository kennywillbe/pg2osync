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

/// The body of a `POST /swap-indexes`: one swap, of the two uids named.
///
/// The endpoint takes a list of swaps and each swap a pair, so the shape is two
/// levels deeper than the one operation it expresses — which is why it is built
/// in one place and asserted on.
fn swap_body(left: &str, right: &str) -> Value {
    json!([{ "indexes": [left, right] }])
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
        self.wait_task_tolerating(task_uid, None).await
    }

    /// `tolerated` is a Meilisearch error code that counts as success: the
    /// outcome of an operation is reported on its task, so an error meaning
    /// "already done" arrives here rather than on the request that enqueued it.
    async fn wait_task_tolerating(
        &self,
        task_uid: u64,
        tolerated: Option<&str>,
    ) -> Result<(), CoreError> {
        for _ in 0..600 {
            let (status, body) = self
                .send(reqwest::Method::GET, &format!("/tasks/{task_uid}"), None)
                .await?;
            if status == 200 {
                match body["status"].as_str() {
                    Some("succeeded") => return Ok(()),
                    Some("failed") => {
                        if tolerated.is_some() && body["error"]["code"].as_str() == tolerated {
                            return Ok(());
                        }
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

    /// The alias step of a rebuild, in the only form this target has.
    ///
    /// There is no alias namespace here: the name readers use is an index uid,
    /// so what makes it resolve to freshly built documents is `swap-indexes`,
    /// which exchanges the contents of two uids in a single task. The exchange
    /// runs both ways, so `index` ends up holding what `alias` held — the
    /// previous documents, which is exactly what a rollback wants.
    async fn swap_indexes(&self, alias: &str, index: &str) -> Result<(), CoreError> {
        if alias == index {
            return Err(CoreError::Sink(format!(
                "swapping {alias} with itself would leave every document where it is"
            )));
        }
        // both uids have to exist for a swap, and the live name does not yet on
        // the first rebuild of an index nothing ever wrote to
        self.ensure_ready(&[IndexSpec {
            name: alias.to_string(),
            mapping: None,
            pattern: false,
        }])
        .await?;
        let (status, body) = self
            .send(
                reqwest::Method::POST,
                "/swap-indexes",
                Some(swap_body(alias, index)),
            )
            .await?;
        if status != 202 {
            return Err(CoreError::Sink(format!(
                "swap of {alias} with {index}: {status} {body}"
            )));
        }
        let Some(uid) = body["taskUid"].as_u64() else {
            return Err(CoreError::Sink(format!(
                "swap of {alias} with {index} was accepted without a task: {body}"
            )));
        };
        // the swap either happened or it did not, and a caller told only that
        // "a task failed" cannot tell which two names are now in doubt
        self.wait_task(uid).await.map_err(|e| match e {
            CoreError::Sink(why) => CoreError::Sink(format!("swap of {alias} with {index}: {why}")),
            other => other,
        })
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

    /// Where 1.x kept every stream's checkpoint before they were per stream.
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

    /// And it stores no position either, so a truncate here clears the index
    /// whole rather than up to a point in the stream.
    fn truncates_at_a_position(&self) -> bool {
        false
    }

    async fn ensure_ready(&self, tables: &[IndexSpec]) -> Result<(), CoreError> {
        for spec in tables {
            // refused by config before it gets here; kept so a new caller
            // cannot create an index literally named after a glob
            if spec.pattern {
                return Err(CoreError::Sink(format!(
                    "index {:?} is chosen per row, which Meilisearch cannot create on demand",
                    spec.name
                )));
            }
            if spec.mapping.is_some() {
                // Meilisearch has no field types to declare; its equivalent is
                // the settings API, which is a different feature than this one
                return Err(CoreError::Sink(format!(
                    "index {} has a mapping configured, which Meilisearch has no \
                     equivalent for; remove mapping_file for this target",
                    spec.name
                )));
            }
            // Asking first is what makes a restart work: the index the
            // previous run created is left alone instead of being created
            // again, which Meilisearch reports as a failed task.
            let (status, _) = self
                .send(
                    reqwest::Method::GET,
                    &format!("/indexes/{}", spec.name),
                    None,
                )
                .await?;
            if status == 200 {
                continue;
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
                    // two pipelines starting at once, or a replay, can still
                    // create the same index between the check above and here
                    self.wait_task_tolerating(uid, Some("index_already_exists"))
                        .await?;
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
        // Meilisearch endpoints are per index and per kind, so consecutive ops
        // of one kind on one index share a request. Runs keep batch order
        // because the engine coalesces transactions without collapsing them
        // per id: a delete followed by a re-insert must land in that order.
        let mut runs: Vec<(String, bool, Vec<Value>)> = Vec::new();
        for op in &batch {
            let (index, upsert, item) = match &op.op {
                DocumentOp::Upsert { index, doc, .. } => (index, true, doc.clone()),
                DocumentOp::Delete { index, id, .. } => (index, false, Value::String(id.clone())),
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
            match runs.last_mut() {
                Some((last, kind, items)) if last == index && *kind == upsert => items.push(item),
                _ => runs.push((index.clone(), upsert, vec![item])),
            }
        }
        for (index, upsert, items) in runs {
            let (path, what) = if upsert {
                (
                    format!("/indexes/{index}/documents?primaryKey=id"),
                    "add documents to",
                )
            } else {
                (
                    format!("/indexes/{index}/documents/delete-batch"),
                    "delete documents from",
                )
            };
            let (status, body) = self
                .send(reqwest::Method::POST, &path, Some(Value::Array(items)))
                .await?;
            if status != 202 {
                return Err(CoreError::Sink(format!("{what} {index}: {status}")));
            }
            if let Some(uid) = body["taskUid"].as_u64() {
                self.wait_task(uid).await?;
            }
        }
        Ok(SinkAck::written(
            batch.last().expect("nonempty").position.clone(),
        ))
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

    async fn count_documents(&self, index: &str) -> Result<Option<u64>, CoreError> {
        let (status, body) = self
            .send(
                reqwest::Method::GET,
                &format!("/indexes/{index}/stats"),
                None,
            )
            .await?;
        match status {
            404 => Ok(None),
            200 => body["numberOfDocuments"]
                .as_u64()
                .map(Some)
                .ok_or_else(|| CoreError::Sink(format!("count {index}: {body}"))),
            other => Err(CoreError::Sink(format!("count {index}: {other} {body}"))),
        }
    }

    async fn index_exists(&self, name: &str) -> Result<bool, CoreError> {
        let (status, body) = self
            .send(reqwest::Method::GET, &format!("/indexes/{name}"), None)
            .await?;
        match status {
            200 => Ok(true),
            404 => Ok(false),
            other => Err(CoreError::Sink(format!(
                "does index {name} exist: {other} {body}"
            ))),
        }
    }

    async fn delete_index(&self, name: &str) -> Result<(), CoreError> {
        let (status, body) = self
            .send(reqwest::Method::DELETE, &format!("/indexes/{name}"), None)
            .await?;
        match status {
            // gone already is the state the caller asked for
            404 => Ok(()),
            202 => match body["taskUid"].as_u64() {
                // and an index another caller removed between the request and
                // the task reaches us the same way every other outcome does
                Some(uid) => {
                    self.wait_task_tolerating(uid, Some("index_not_found"))
                        .await
                }
                None => Ok(()),
            },
            other => Err(CoreError::Sink(format!(
                "delete index {name}: {other} {body}"
            ))),
        }
    }

    async fn switch_alias(&self, alias: &str, index: &str) -> Result<(), CoreError> {
        self.swap_indexes(alias, index).await
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
        match std::fs::read(self.checkpoint_path(stream)) {
            Ok(bytes) => serde_json::from_slice::<Value>(&bytes)
                .map(|v| crate::checkpoint_from_doc(&v))
                .map_err(|e| CoreError::Sink(e.to_string())),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(CoreError::Sink(format!("checkpoint read: {e}"))),
        }
    }

    async fn read_legacy_position_text(
        &self,
        stream: &pg2osync_core::checkpoint::StreamId,
    ) -> Result<Option<String>, CoreError> {
        // 1.x read the shared file only when the stream's own was absent, so
        // that is the order its checkpoints can be found in
        for path in [self.checkpoint_path(stream), self.legacy_checkpoint_path()] {
            match std::fs::read(&path) {
                Ok(bytes) => {
                    let doc: Value = serde_json::from_slice(&bytes)
                        .map_err(|e| CoreError::Sink(format!("1.x checkpoint {path}: {e}")))?;
                    return Ok(crate::legacy_position_text(&doc, stream));
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                Err(e) => return Err(CoreError::Sink(format!("1.x checkpoint {path}: {e}"))),
            }
        }
        Ok(None)
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    /// A loopback HTTP server answering by request line, so what the
    /// assertions read is the sequence of requests the sink really made.
    struct FakeTarget {
        url: String,
        state_dir: String,
        seen: Arc<Mutex<Vec<String>>>,
    }

    impl FakeTarget {
        async fn start(answer: fn(&str) -> (u16, &'static str)) -> Self {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .expect("bind a loopback port");
            let address = listener.local_addr().expect("local address");
            let url = format!("http://{address}");
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
            let state_dir = std::env::temp_dir()
                .join(format!("pg2osync-meili-test-{}", address.port()))
                .to_string_lossy()
                .into_owned();
            Self {
                url,
                state_dir,
                seen,
            }
        }

        fn sink(&self) -> MeilisearchSink {
            MeilisearchSink::new(MeilisearchSinkConfig {
                url: self.url.clone(),
                api_key: None,
                state_dir: self.state_dir.clone(),
            })
            .expect("a sink over plain http")
        }

        fn seen(&self) -> Vec<String> {
            crate::lock(&self.seen).clone()
        }
    }

    impl Drop for FakeTarget {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.state_dir);
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

    #[tokio::test]
    async fn a_checkpoint_file_round_trips_and_the_shared_file_is_not_read() {
        use pg2osync_core::checkpoint::{SOURCE_MYSQL, StreamId};
        let target = FakeTarget::start(|_| (200, "{}")).await;
        let sink = target.sink();
        let stream = StreamId {
            source: SOURCE_MYSQL.into(),
            stream: "424242".into(),
            publication: String::new(),
        };
        // what checkpoints were kept in before they were per stream; nothing
        // writes it any more, so nothing may resume from it either
        std::fs::write(
            format!("{}/checkpoint.json", target.state_dir),
            serde_json::to_vec(&crate::checkpoint_doc(&Checkpoint {
                stream: stream.clone(),
                ordering: pg2osync_core::Position::from_bytes([0, 0, 0, 1, 0, 0, 0, 4]),
                position: "binlog.000001:4".into(),
            }))
            .unwrap(),
        )
        .unwrap();
        assert_eq!(sink.read_checkpoint(&stream).await.unwrap(), None);

        let checkpoint = Checkpoint {
            stream: stream.clone(),
            ordering: pg2osync_core::Position::from_bytes([0, 0, 0, 4, 0, 0, 0x04, 0xD2]),
            position: "binlog.000004:1234;base=4096".into(),
        };
        sink.write_checkpoint(&checkpoint).await.unwrap();
        assert_eq!(
            sink.read_checkpoint(&stream).await.unwrap(),
            Some(checkpoint)
        );
    }

    #[tokio::test]
    async fn a_1x_checkpoint_file_is_read_as_text_from_the_stream_file_then_the_shared_one() {
        let target = FakeTarget::start(|_| (200, "{}")).await;
        let sink = target.sink();
        let stream = crate::tests::mysql_stream();
        let legacy = serde_json::to_vec(&crate::tests::mysql_1x_doc()).unwrap();
        assert_eq!(sink.read_legacy_position_text(&stream).await.unwrap(), None);

        let shared = format!("{}/checkpoint.json", target.state_dir);
        std::fs::write(&shared, &legacy).unwrap();
        assert_eq!(
            sink.read_legacy_position_text(&stream)
                .await
                .unwrap()
                .as_deref(),
            Some("binlog.000004:1234;base=1099511627776")
        );
        let other = pg2osync_core::checkpoint::StreamId {
            stream: "7".into(),
            ..stream.clone()
        };
        assert_eq!(sink.read_legacy_position_text(&other).await.unwrap(), None);
        std::fs::remove_file(&shared).unwrap();

        let own = sink.checkpoint_path(&stream);
        std::fs::write(&own, &legacy).unwrap();
        assert_eq!(
            sink.read_legacy_position_text(&stream)
                .await
                .unwrap()
                .as_deref(),
            Some("binlog.000004:1234;base=1099511627776")
        );
        // the current layout replaces the 1.x one in the same file
        sink.write_checkpoint(&Checkpoint {
            stream: stream.clone(),
            ordering: pg2osync_core::Position::from_bytes([0, 0, 0, 4, 0, 0, 0x04, 0xD2]),
            position: "binlog.000004:1234;base=1099511627776".into(),
        })
        .await
        .unwrap();
        assert_eq!(sink.read_legacy_position_text(&stream).await.unwrap(), None);
    }

    fn users() -> Vec<IndexSpec> {
        vec![IndexSpec {
            name: "users".into(),
            mapping: None,
            pattern: false,
        }]
    }

    #[tokio::test]
    async fn an_index_that_already_exists_is_not_created_again() {
        let target = FakeTarget::start(|line| match line {
            "GET /indexes/users" => (200, r#"{"uid":"users","primaryKey":"id"}"#),
            _ => (500, r#"{"code":"internal"}"#),
        })
        .await;
        target
            .sink()
            .ensure_ready(&users())
            .await
            .expect("a restart over the index the previous run created");
        assert_eq!(target.seen(), vec!["GET /indexes/users".to_string()]);
    }

    #[tokio::test]
    async fn a_create_task_that_lost_the_race_is_success() {
        let target = FakeTarget::start(|line| match line {
            "GET /indexes/users" => (404, r#"{"code":"index_not_found"}"#),
            "POST /indexes" => (202, r#"{"taskUid":6,"status":"enqueued"}"#),
            "GET /tasks/6" => (
                200,
                r#"{"status":"failed","error":{"code":"index_already_exists"}}"#,
            ),
            _ => (500, r#"{"code":"internal"}"#),
        })
        .await;
        target
            .sink()
            .ensure_ready(&users())
            .await
            .expect("an index someone else created in the meantime");
        assert_eq!(target.seen().len(), 3, "{:?}", target.seen());
    }

    #[test]
    fn a_swap_is_one_pair_inside_a_list_of_swaps() {
        assert_eq!(
            swap_body("users", "users-1756512345"),
            json!([{"indexes": ["users", "users-1756512345"]}])
        );
    }

    #[tokio::test]
    async fn switching_a_name_onto_a_rebuilt_index_swaps_the_two_uids() {
        let target = FakeTarget::start(|line| match line {
            "GET /indexes/users" => (200, r#"{"uid":"users","primaryKey":"id"}"#),
            "POST /swap-indexes" => (202, r#"{"taskUid":11,"status":"enqueued"}"#),
            "GET /tasks/11" => (200, r#"{"status":"succeeded"}"#),
            _ => (500, r#"{"code":"internal"}"#),
        })
        .await;
        target
            .sink()
            .switch_alias("users", "users-1756512345")
            .await
            .expect("the live name now holds the rebuilt documents");
        assert_eq!(
            target.seen(),
            vec![
                "GET /indexes/users".to_string(),
                "POST /swap-indexes".to_string(),
                "GET /tasks/11".to_string(),
            ]
        );
    }

    #[tokio::test]
    async fn a_failed_swap_task_names_both_uids() {
        let target = FakeTarget::start(|line| match line {
            "GET /indexes/users" => (200, r#"{"uid":"users","primaryKey":"id"}"#),
            "POST /swap-indexes" => (202, r#"{"taskUid":12,"status":"enqueued"}"#),
            "GET /tasks/12" => (
                200,
                r#"{"status":"failed","error":{"code":"index_not_found"}}"#,
            ),
            _ => (500, r#"{"code":"internal"}"#),
        })
        .await;
        let result = target
            .sink()
            .switch_alias("users", "users-1756512345")
            .await;
        let Err(CoreError::Sink(message)) = result else {
            panic!("a swap that did not happen looked like one that did: {result:?}");
        };
        assert!(message.contains("users"), "{message}");
        assert!(message.contains("users-1756512345"), "{message}");
    }

    fn op(position: u64, op: DocumentOp) -> LsnOp {
        LsnOp {
            position: crate::test_position(position),
            op,
        }
    }

    fn upsert(id: &str) -> DocumentOp {
        DocumentOp::Upsert {
            index: "users".into(),
            id: id.into(),
            routing: None,
            doc: json!({"id": id}),
            version: None,
            pipeline: None,
            alias: None,
        }
    }

    fn delete(id: &str) -> DocumentOp {
        DocumentOp::Delete {
            index: "users".into(),
            id: id.into(),
            routing: None,
            version: None,
            alias: None,
        }
    }

    #[tokio::test]
    async fn a_reinsert_after_a_delete_in_one_batch_lands_last() {
        // the engine coalesces transactions into one batch without collapsing
        // them per id, so a row deleted and inserted again arrives as both ops
        let target = FakeTarget::start(|line| match line {
            "POST /indexes/users/documents?primaryKey=id"
            | "POST /indexes/users/documents/delete-batch" => {
                (202, r#"{"taskUid":1,"status":"enqueued"}"#)
            }
            "GET /tasks/1" => (200, r#"{"status":"succeeded"}"#),
            _ => (500, r#"{"code":"internal"}"#),
        })
        .await;
        target
            .sink()
            .write(vec![
                op(1, upsert("7")),
                op(2, delete("7")),
                op(3, upsert("7")),
            ])
            .await
            .expect("a batch the target accepted");
        let writes: Vec<String> = target
            .seen()
            .into_iter()
            .filter(|line| line.starts_with("POST"))
            .collect();
        assert_eq!(
            writes,
            vec![
                "POST /indexes/users/documents?primaryKey=id".to_string(),
                "POST /indexes/users/documents/delete-batch".to_string(),
                "POST /indexes/users/documents?primaryKey=id".to_string(),
            ]
        );
    }

    #[tokio::test]
    async fn any_other_create_task_failure_is_an_error() {
        let target = FakeTarget::start(|line| match line {
            "GET /indexes/users" => (404, r#"{"code":"index_not_found"}"#),
            "POST /indexes" => (202, r#"{"taskUid":7,"status":"enqueued"}"#),
            "GET /tasks/7" => (
                200,
                r#"{"status":"failed","error":{"code":"invalid_index_uid"}}"#,
            ),
            _ => (500, r#"{"code":"internal"}"#),
        })
        .await;
        let result = target.sink().ensure_ready(&users()).await;
        assert!(
            matches!(result, Err(CoreError::Sink(ref e)) if e.contains("invalid_index_uid")),
            "an index that was never created looked ready: {result:?}"
        );
    }
}
