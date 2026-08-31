//! Qdrant sink: a vector database used as a search backend.
//!
//! One section writes one collection. The operator supplies that collection's
//! creation body — `mapping_file` pointing at the JSON Qdrant's
//! `PUT /collections/<name>` takes — and the sink creates the collection when
//! it is absent. What the body declares as a *named vector* is what makes a
//! document field a vector: a field whose name matches one of them is written
//! as that vector, and every other field becomes payload. The sink computes
//! nothing.
//!
//! Deviations from the OpenSearch contract, and why:
//! - point ids are `u64` or UUID only, so every document id is mapped to a
//!   UUIDv5 over a fixed namespace and the id itself is kept in the
//!   `_pg2osync_id` payload field
//! - an upsert overwrites unconditionally: Qdrant has no external versioning,
//!   so two writes of one document settle by arrival order, and
//!   `orders_by_version` says so — which is what refuses `write_concurrency`
//!   above 1 against this target
//! - `_version` is still recorded on every point, because a truncate has to
//!   clear at a position, and that is a comparison against stored payload
//!   rather than a race between two writes
//! - no shards and no parent-child model, so `routing` and `join` are refused
//! - no rebuild: Qdrant does have collection aliases, but the switch a
//!   `reindex` ends in is not implemented here, so `require_alias` has nothing
//!   to protect and is refused with it

use async_trait::async_trait;
use pg2osync_core::checkpoint::Checkpoint;
use pg2osync_core::error::CoreError;
use pg2osync_core::sink::{
    DocumentOp, Health, IndexSpec, LsnOp, Rejection, Sink, SinkAck, StoredReject,
};
use serde_json::{Map, Value, json};
use std::collections::HashMap;

/// Where one stream's checkpoint and the initial-load progress live.
pub const STATE_COLLECTION: &str = "pg2osync_state";
/// Where documents the target refused are kept, for the reason the rejects
/// index exists on the other targets: an operator can read or drop it without
/// going anywhere near a checkpoint.
pub const REJECTS_COLLECTION: &str = "pg2osync_rejects";
/// The payload field carrying the document id a point was built from.
///
/// A point id is a `u64` or a UUID, so the id the pipeline files a document
/// under cannot be one. It is kept here instead, which is what makes a
/// read-back and a quarantined document name the document rather than a hash
/// of it.
pub const ID_FIELD: &str = "_pg2osync_id";
/// The payload field carrying the source position a point was written at.
pub const VERSION_FIELD: &str = "_version";

/// The namespace every document id is hashed under, fixed for the life of this
/// sink: a different namespace would file every document under a new point and
/// leave the old one behind, so it is a constant rather than a setting.
const NAMESPACE: [u8; 16] = [
    0xf8, 0xf0, 0xb0, 0xa2, 0x6b, 0x3c, 0x4d, 0x7e, 0x9a, 0x1f, 0x2c, 0x4d, 0x6e, 0x8a, 0x0b, 0x13,
];

pub struct QdrantSink {
    http: reqwest::Client,
    base_url: String,
    retry: crate::RetryPolicy,
    /// The named vectors `ensure_ready` read off each collection, so a write
    /// knows which fields are vectors without asking the server.
    shapes: std::sync::Mutex<HashMap<String, CollectionShape>>,
    /// Whether this process has already made the state and rejects
    /// collections. Checked before every checkpoint, which is often enough
    /// that three round trips to be told "yes, still there" is a cost worth
    /// paying once.
    own_collections: std::sync::atomic::AtomicBool,
}

#[derive(Debug, Clone)]
pub struct QdrantSinkConfig {
    pub url: String,
    pub api_key: Option<String>,
    pub retry: crate::RetryPolicy,
}

/// What a write needs to know about one collection: the vectors it declares,
/// by name and dimension.
#[derive(Debug, Clone, PartialEq, Eq)]
struct CollectionShape {
    vectors: Vec<(String, usize)>,
}

impl CollectionShape {
    fn size_of(&self, field: &str) -> Option<usize> {
        self.vectors
            .iter()
            .find(|(name, _)| name == field)
            .map(|(_, size)| *size)
    }
}

/// The UUIDv5 a document id is filed under.
///
/// Deterministic on purpose: delivery is at-least-once, so the same document
/// arrives again after every restart and has to land on the same point. A
/// counter or a random id would duplicate it instead of overwriting it.
///
/// One rule for every id, including one that reads as an integer: a special
/// case for those would file `7` and `"7"` under different points depending on
/// which the source produced.
fn point_id(doc_id: &str) -> String {
    use sha1::{Digest, Sha1};
    let mut hasher = Sha1::new();
    hasher.update(NAMESPACE);
    hasher.update(doc_id.as_bytes());
    let digest = hasher.finalize();
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    // RFC 4122: version 5 in the high nibble of byte 6, the variant in byte 8
    bytes[6] = (bytes[6] & 0x0f) | 0x50;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    let hex: String = bytes.iter().map(|b| format!("{b:02x}")).collect();
    format!(
        "{}-{}-{}-{}-{}",
        &hex[0..8],
        &hex[8..12],
        &hex[12..16],
        &hex[16..20],
        &hex[20..32]
    )
}

/// The point Qdrant will store, or why the document cannot be one.
///
/// A field named after one of the collection's vectors becomes that vector and
/// everything else becomes payload, which is the whole mapping rule. The
/// dimension is checked here rather than left to the server so a refusal names
/// the field and both numbers.
fn point_body(
    shape: &CollectionShape,
    id: &str,
    doc: &Value,
    version: Option<u64>,
) -> Result<Value, String> {
    let Some(fields) = doc.as_object() else {
        return Err("a document that is not a JSON object has no payload to write".into());
    };
    let mut payload = Map::with_capacity(fields.len() + 2);
    let mut vectors = Map::new();
    for (field, value) in fields {
        let Some(size) = shape.size_of(field) else {
            payload.insert(field.clone(), value.clone());
            continue;
        };
        // A row whose embedding has not been computed yet is a document like
        // any other: it is stored, found by id and filtered on, and it joins
        // the similarity search as soon as the vector arrives. Refusing it
        // would quarantine every row an embedding worker has not reached.
        if value.is_null() {
            continue;
        }
        vectors.insert(field.clone(), numeric_vector(field, value, size)?);
    }
    payload.insert(ID_FIELD.to_string(), json!(id));
    payload.insert(
        VERSION_FIELD.to_string(),
        match version {
            Some(at) => json!(at),
            None => Value::Null,
        },
    );
    Ok(json!({
        "id": point_id(id),
        "payload": Value::Object(payload),
        "vector": Value::Object(vectors),
    }))
}

/// One declared vector's value, or why it is not one.
fn numeric_vector(field: &str, value: &Value, size: usize) -> Result<Value, String> {
    let Some(items) = value.as_array() else {
        return Err(format!(
            "{field} is a vector of this collection, so it has to be an array of numbers"
        ));
    };
    if items.len() != size {
        return Err(format!(
            "{field} has {} dimensions and the collection declares {size}",
            items.len()
        ));
    }
    if let Some(odd) = items.iter().find(|item| !item.is_number()) {
        return Err(format!("{field} holds {odd}, which is not a number"));
    }
    Ok(value.clone())
}

/// The document a point stands for: its payload without the sink's own two
/// fields, with every vector it carries back as an array.
fn document_of(point: &Value) -> Value {
    let mut doc = point["payload"].as_object().cloned().unwrap_or_default();
    doc.remove(ID_FIELD);
    doc.remove(VERSION_FIELD);
    if let Some(vectors) = point["vector"].as_object() {
        for (name, value) in vectors {
            doc.insert(name.clone(), value.clone());
        }
    }
    Value::Object(doc)
}

/// Everything written at or before `version`, or everything at all.
///
/// A point with no position loses either way: it was written by a load or a
/// poll that had none, so there is nothing to say it came after the truncate.
fn truncate_filter(version: Option<u64>) -> Value {
    match version {
        Some(at) => json!({"should": [
            {"key": VERSION_FIELD, "range": {"lte": at}},
            {"is_empty": {"key": VERSION_FIELD}},
        ]}),
        None => json!({}),
    }
}

/// One request's worth of work: consecutive operations of one kind against one
/// collection.
///
/// Consecutive rather than grouped by collection, because a batch may hold an
/// upsert and a delete of the same document and the later one has to win; a
/// grouping that reordered them would resurrect a deleted row.
enum Run {
    Upsert {
        index: String,
        points: Vec<(usize, Value)>,
    },
    Delete {
        index: String,
        points: Vec<(usize, String)>,
    },
}

impl Run {
    fn index(&self) -> &str {
        match self {
            Run::Upsert { index, .. } | Run::Delete { index, .. } => index,
        }
    }

    fn is_upsert(&self) -> bool {
        matches!(self, Run::Upsert { .. })
    }

    fn is_empty(&self) -> bool {
        match self {
            Run::Upsert { points, .. } => points.is_empty(),
            Run::Delete { points, .. } => points.is_empty(),
        }
    }
}

impl QdrantSink {
    pub fn new(cfg: QdrantSinkConfig) -> Result<Self, CoreError> {
        let mut headers = reqwest::header::HeaderMap::new();
        if let Some(key) = &cfg.api_key {
            let mut value: reqwest::header::HeaderValue = key
                .parse()
                .map_err(|e| CoreError::Sink(format!("bad api key: {e}")))?;
            // so a header dump or a debug log of the client cannot print it
            value.set_sensitive(true);
            headers.insert("api-key", value);
        }
        let http = reqwest::Client::builder()
            .default_headers(headers)
            .timeout(std::time::Duration::from_secs(60))
            .build()
            .map_err(|e| CoreError::Sink(e.to_string()))?;
        Ok(Self {
            http,
            base_url: cfg.url.trim_end_matches('/').to_string(),
            retry: cfg.retry,
            shapes: std::sync::Mutex::new(HashMap::new()),
            own_collections: std::sync::atomic::AtomicBool::new(false),
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
        let resp = req.send().await.map_err(|e| {
            // a connection that never answered is the blip the engine retries
            CoreError::SinkTransient(format!("request to the target failed: {e}"))
        })?;
        let status = resp.status().as_u16();
        let text = resp
            .text()
            .await
            .map_err(|e| CoreError::SinkTransient(format!("reading the target's answer: {e}")))?;
        let body = serde_json::from_str(&text).unwrap_or(Value::Null);
        Ok((status, body))
    }

    /// A request that has to have worked, as the taxonomy the engine retries
    /// on: an overloaded or restarting server is transient, a refusal is not.
    async fn call(
        &self,
        method: reqwest::Method,
        path: &str,
        body: Option<Value>,
        what: &str,
    ) -> Result<Value, CoreError> {
        let (status, body) = self.send(method, path, body).await?;
        if (200..300).contains(&status) {
            return Ok(body);
        }
        Err(status_error(status, what, &body))
    }

    /// The collections this sink keeps its own bookkeeping in.
    ///
    /// Created here rather than asked of the operator because they are ours:
    /// the JSON an operator writes describes documents, and a checkpoint is not
    /// one. Neither holds a vector, which Qdrant expresses as a collection with
    /// no named vectors at all.
    async fn ensure_own_collections(&self) -> Result<(), CoreError> {
        use std::sync::atomic::Ordering;
        if self.own_collections.load(Ordering::Relaxed) {
            return Ok(());
        }
        for name in [STATE_COLLECTION, REJECTS_COLLECTION] {
            if self.collection_exists(name).await? {
                continue;
            }
            let (status, body) = self
                .send(
                    reqwest::Method::PUT,
                    &format!("/collections/{name}"),
                    Some(json!({"vectors": {}})),
                )
                .await?;
            // two pipelines starting at once can create it between the check
            // above and here, and that is the state this asked for
            if !(200..300).contains(&status) && !self.collection_exists(name).await? {
                return Err(status_error(status, &format!("create {name}"), &body));
            }
        }
        // Newest first is a scroll ordered by a payload field, which Qdrant
        // only offers over an index.
        self.call(
            reqwest::Method::PUT,
            &format!("/collections/{REJECTS_COLLECTION}/index?wait=true"),
            Some(json!({"field_name": "at_epoch", "field_schema": "integer"})),
            "index the quarantine store",
        )
        .await?;
        self.own_collections.store(true, Ordering::Relaxed);
        Ok(())
    }

    async fn collection_exists(&self, name: &str) -> Result<bool, CoreError> {
        let (status, body) = self
            .send(reqwest::Method::GET, &format!("/collections/{name}"), None)
            .await?;
        match status {
            200 => Ok(true),
            404 => Ok(false),
            other => Err(status_error(
                other,
                &format!("does collection {name} exist"),
                &body,
            )),
        }
    }

    /// The named vectors a collection declares, read off the server rather than
    /// off the configured body: a collection an earlier run created is the one
    /// documents are written into, whatever the file now says.
    async fn read_shape(&self, name: &str) -> Result<CollectionShape, CoreError> {
        let body = self
            .call(
                reqwest::Method::GET,
                &format!("/collections/{name}"),
                None,
                &format!("read collection {name}"),
            )
            .await?;
        let declared = &body["result"]["config"]["params"]["vectors"];
        if declared.get("size").is_some() {
            return Err(CoreError::Sink(format!(
                "collection {name} declares a single unnamed vector, and this target maps a \
                 document field onto a vector by its name. Declare it named: \
                 \"vectors\": {{\"embedding\": {{\"size\": …, \"distance\": …}}}}"
            )));
        }
        let mut vectors: Vec<(String, usize)> = Vec::new();
        for (vector, spec) in declared.as_object().into_iter().flatten() {
            let Some(size) = spec["size"].as_u64() else {
                return Err(CoreError::Sink(format!(
                    "collection {name} declares vector {vector} without a size"
                )));
            };
            vectors.push((vector.clone(), size as usize));
        }
        Ok(CollectionShape { vectors })
    }

    fn shape_of(&self, index: &str) -> Result<CollectionShape, CoreError> {
        crate::lock(&self.shapes)
            .get(index)
            .cloned()
            .ok_or_else(|| {
                CoreError::Sink(format!(
                    "collection {index} was never prepared; every section's collection is \
                     created or checked before the first batch"
                ))
            })
    }

    /// One run of upserts, or — when the target refuses the request as a whole
    /// — the same points one at a time, so the refusal is attributed to the
    /// document that caused it and the rest of the run still lands.
    ///
    /// A batch upsert is all-or-nothing, which is the difference from a bulk
    /// API that answers per item: the second pass is this target's savepoint.
    async fn upsert_run(
        &self,
        index: &str,
        points: &[(usize, Value)],
    ) -> Result<Vec<(usize, String)>, CoreError> {
        let path = format!("/collections/{index}/points?wait=true");
        let body: Vec<Value> = points.iter().map(|(_, point)| point.clone()).collect();
        let (status, answer) = self
            .send(reqwest::Method::PUT, &path, Some(json!({"points": body})))
            .await?;
        if (200..300).contains(&status) {
            return Ok(Vec::new());
        }
        if !is_refusal(status) {
            return Err(status_error(status, &format!("write to {index}"), &answer));
        }
        let mut refused = Vec::new();
        for (nth, point) in points {
            let (status, answer) = self
                .send(
                    reqwest::Method::PUT,
                    &path,
                    Some(json!({"points": [point]})),
                )
                .await?;
            if (200..300).contains(&status) {
                continue;
            }
            if !is_refusal(status) {
                return Err(status_error(status, &format!("write to {index}"), &answer));
            }
            refused.push((*nth, refusal(&answer)));
        }
        Ok(refused)
    }

    /// One attempt at a run. Idempotent whichever way it ends: an upsert
    /// overwrites and a delete of an absent point is success, so a retry after
    /// a transient failure re-applies the same run rather than half of it.
    async fn apply_run(&self, run: &Run) -> Result<Vec<(usize, String)>, CoreError> {
        match run {
            Run::Upsert { index, points } => self.upsert_run(index, points).await,
            Run::Delete { index, points } => {
                let ids: Vec<&str> = points.iter().map(|(_, id)| id.as_str()).collect();
                self.call(
                    reqwest::Method::POST,
                    &format!("/collections/{index}/points/delete?wait=true"),
                    Some(json!({"points": ids})),
                    &format!("delete from {index}"),
                )
                .await
                .map(|_| Vec::new())
            }
        }
    }

    /// The state document `key` is filed under, or `None` when nothing ever
    /// wrote one.
    async fn state_doc(&self, key: &str) -> Result<Option<Value>, CoreError> {
        let (status, body) = self
            .send(
                reqwest::Method::POST,
                &format!("/collections/{STATE_COLLECTION}/points"),
                Some(json!({"ids": [point_id(key)], "with_payload": true})),
            )
            .await?;
        match status {
            // nothing has ever been written to this target, which is not the
            // same as an error
            404 => Ok(None),
            200 => Ok(body["result"]
                .as_array()
                .and_then(|found| found.first())
                .map(|point| point["payload"]["doc"].clone())
                .filter(|doc| !doc.is_null())),
            other => Err(status_error(other, &format!("read state {key}"), &body)),
        }
    }

    async fn put_state_doc(&self, key: &str, doc: &Value) -> Result<(), CoreError> {
        self.ensure_own_collections().await?;
        self.call(
            reqwest::Method::PUT,
            &format!("/collections/{STATE_COLLECTION}/points?wait=true"),
            Some(json!({"points": [{
                "id": point_id(key),
                "payload": {"key": key, "doc": doc},
                "vector": {},
            }]})),
            &format!("write state {key}"),
        )
        .await
        .map(|_| ())
    }

    async fn drop_point(&self, collection: &str, key: &str) -> Result<(), CoreError> {
        let (status, body) = self
            .send(
                reqwest::Method::POST,
                &format!("/collections/{collection}/points/delete?wait=true"),
                Some(json!({"points": [point_id(key)]})),
            )
            .await?;
        match status {
            // a collection nothing was ever written to holds nothing to remove
            404 => Ok(()),
            s if (200..300).contains(&s) => Ok(()),
            other => Err(status_error(other, &format!("clear {key}"), &body)),
        }
    }
}

/// Whether the target refused the request rather than failed at it.
///
/// 429 is the one 4xx that is not a refusal: it is the server saying "later",
/// and a document is not wrong for having arrived while it was busy.
fn is_refusal(status: u16) -> bool {
    (400..500).contains(&status) && status != 429
}

/// An unsuccessful response as the taxonomy the engine retries on.
fn status_error(status: u16, what: &str, body: &Value) -> CoreError {
    let reason = format!("{what}: {status} {}", refusal(body));
    if is_refusal(status) {
        CoreError::Sink(reason)
    } else {
        CoreError::SinkTransient(reason)
    }
}

/// The sentence Qdrant put in the answer, which is the only thing that says
/// which field to fix.
fn refusal(body: &Value) -> String {
    body["status"]["error"]
        .as_str()
        .map(str::to_string)
        .unwrap_or_else(|| body.to_string())
}

#[async_trait]
impl Sink for QdrantSink {
    /// An upsert overwrites whatever the point held: Qdrant takes no external
    /// version to compare against, so two write requests open at once could
    /// settle one document either way round.
    fn orders_by_version(&self) -> bool {
        false
    }

    async fn ensure_ready(&self, tables: &[IndexSpec]) -> Result<(), CoreError> {
        self.ensure_own_collections().await?;
        for spec in tables {
            // refused by config before it gets here; kept so a new caller
            // cannot create a collection literally named after a glob
            if spec.pattern {
                return Err(CoreError::Sink(format!(
                    "collection {:?} is chosen per row, and this target has no way to know what \
                     vectors a name a row renders should be created with",
                    spec.name
                )));
            }
            let Some(body) = spec.mapping.clone() else {
                return Err(CoreError::Sink(format!(
                    "collection {} has no configuration; a collection cannot be created without \
                     the vectors it holds, so the section needs mapping_file pointing at the \
                     JSON of PUT /collections/{}",
                    spec.name, spec.name
                )));
            };
            if !self.collection_exists(&spec.name).await? {
                // Only when it is absent: a collection that already holds
                // points cannot take a new vector configuration, and doing it
                // implicitly would be a rebuild nobody asked for — the rule the
                // other targets apply to a mapping.
                let (status, answer) = self
                    .send(
                        reqwest::Method::PUT,
                        &format!("/collections/{}", spec.name),
                        Some(body),
                    )
                    .await?;
                if !(200..300).contains(&status) && !self.collection_exists(&spec.name).await? {
                    return Err(status_error(
                        status,
                        &format!("create collection {}", spec.name),
                        &answer,
                    ));
                }
            }
            let shape = self.read_shape(&spec.name).await?;
            if shape.vectors.is_empty() {
                return Err(CoreError::Sink(format!(
                    "collection {} declares no vectors; this target stores documents to search \
                     by similarity, so the section's mapping_file has to name at least one",
                    spec.name
                )));
            }
            // A TRUNCATE clears at a position, which is a filter over
            // `_version`; Qdrant filters an unindexed field by scanning every
            // point, so the index is part of being ready rather than a tuning
            // step.
            self.call(
                reqwest::Method::PUT,
                &format!("/collections/{}/index?wait=true", spec.name),
                Some(json!({"field_name": VERSION_FIELD, "field_schema": "integer"})),
                &format!("index {VERSION_FIELD} of {}", spec.name),
            )
            .await?;
            crate::lock(&self.shapes).insert(spec.name.clone(), shape);
        }
        Ok(())
    }

    async fn get_documents(
        &self,
        index: &str,
        // routing is ignored: a collection has no shards to find a point on
        ids: &[(String, Option<String>)],
    ) -> Result<Vec<Option<Value>>, CoreError> {
        if ids.is_empty() {
            return Ok(vec![]);
        }
        let points: Vec<String> = ids.iter().map(|(id, _)| point_id(id)).collect();
        let body = self
            .call(
                reqwest::Method::POST,
                &format!("/collections/{index}/points"),
                Some(json!({"ids": points, "with_payload": true, "with_vector": true})),
                &format!("read documents of {index}"),
            )
            .await?;
        let found: HashMap<String, Value> = body["result"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|point| {
                point["id"]
                    .as_str()
                    .map(|id| (id.to_string(), document_of(point)))
            })
            .collect();
        // in request order, with a hole where the point is not there, which is
        // what the caller completing a TOAST marker relies on
        Ok(points.iter().map(|id| found.get(id).cloned()).collect())
    }

    fn set_retry_policy(
        &self,
        max_attempts: u32,
        base_backoff_ms: u64,
        max_elapsed_ms: Option<u64>,
    ) {
        self.retry
            .set(max_attempts, base_backoff_ms, max_elapsed_ms);
    }

    async fn write(&self, batch: Vec<LsnOp>) -> Result<SinkAck, CoreError> {
        if batch.is_empty() {
            return Err(CoreError::Sink(
                "engine must never send empty batches".into(),
            ));
        }
        let mut shapes: HashMap<String, CollectionShape> = HashMap::new();
        let mut runs: Vec<Run> = Vec::new();
        let mut refusals: Vec<(usize, String)> = Vec::new();
        for (nth, op) in batch.iter().enumerate() {
            let (index, upsert) = match &op.op {
                DocumentOp::Upsert { index, .. } => (index.clone(), true),
                DocumentOp::Delete { index, .. } => (index.clone(), false),
                DocumentOp::DeleteChildren { .. } => {
                    // an error and not a no-op: a cascade dropped here would be
                    // indistinguishable from one that ran
                    return Err(CoreError::Sink(
                        "a join field's cascade cannot be expressed against Qdrant, which has \
                         no parent-child document model; this operation should have been \
                         refused at startup"
                            .into(),
                    ));
                }
            };
            if !shapes.contains_key(&index) {
                shapes.insert(index.clone(), self.shape_of(&index)?);
            }
            if !runs
                .last()
                .is_some_and(|run| run.index() == index && run.is_upsert() == upsert)
            {
                runs.push(match upsert {
                    true => Run::Upsert {
                        index: index.clone(),
                        points: Vec::new(),
                    },
                    false => Run::Delete {
                        index: index.clone(),
                        points: Vec::new(),
                    },
                });
            }
            // present: a run of the right kind was just pushed if there was none
            let run = runs.last_mut().expect("a run for every operation");
            match (&op.op, run) {
                (
                    DocumentOp::Upsert {
                        id, doc, version, ..
                    },
                    Run::Upsert { points, .. },
                ) => {
                    // present: inserted above for every index of the batch
                    let shape = shapes.get(&index).expect("a shape for every index");
                    match point_body(shape, id, doc, *version) {
                        Ok(point) => points.push((nth, point)),
                        Err(why) => refusals.push((nth, why)),
                    }
                }
                (DocumentOp::Delete { id, .. }, Run::Delete { points, .. }) => {
                    points.push((nth, point_id(id)))
                }
                _ => unreachable!("a run holds the kind of operation it was opened for"),
            }
        }
        // A run can end up empty when every document in it was refused before
        // it was built, and a request carrying no points is one nobody needs.
        for run in runs.iter().filter(|run| !run.is_empty()) {
            refusals.extend(crate::retry_transient(&self.retry, || self.apply_run(run)).await?);
        }
        refusals.sort_by_key(|(nth, _)| *nth);
        // the batch is non-empty, checked above
        let max_lsn = batch.last().expect("nonempty checked").lsn;
        let rejected = crate::rejections(&batch, refusals)?;
        Ok(SinkAck { max_lsn, rejected })
    }

    async fn truncate_index(
        &self,
        index: &str,
        version: Option<u64>,
        only: Option<(&str, &str)>,
    ) -> Result<(), CoreError> {
        // a scoped clear only exists for a join pair, which is refused for
        // this target at config load
        if only.is_some() {
            return Err(CoreError::Sink(
                "a scoped truncate belongs to a join pair, which this target has no data model \
                 for; this should have been refused at startup"
                    .into(),
            ));
        }
        let (status, body) = self
            .send(
                reqwest::Method::POST,
                &format!("/collections/{index}/points/delete?wait=true"),
                Some(json!({"filter": truncate_filter(version)})),
            )
            .await?;
        match status {
            // a collection that was never created holds nothing to clear
            404 => Ok(()),
            s if (200..300).contains(&s) => Ok(()),
            other => Err(status_error(other, &format!("truncate {index}"), &body)),
        }
    }

    async fn refresh(&self, _indices: &[String]) -> Result<(), CoreError> {
        // every write asks for wait=true, so the points of one that was
        // acknowledged are already in the segments a search reads
        Ok(())
    }

    async fn count_documents(&self, index: &str) -> Result<Option<u64>, CoreError> {
        let (status, body) = self
            .send(
                reqwest::Method::POST,
                &format!("/collections/{index}/points/count"),
                Some(json!({"exact": true})),
            )
            .await?;
        match status {
            // a collection that was never created is a different answer from an
            // empty one, which is why this is Option and not zero
            404 => Ok(None),
            200 => body["result"]["count"]
                .as_u64()
                .map(Some)
                .ok_or_else(|| CoreError::Sink(format!("count {index}: {body}"))),
            other => Err(status_error(other, &format!("count {index}"), &body)),
        }
    }

    async fn index_exists(&self, name: &str) -> Result<bool, CoreError> {
        self.collection_exists(name).await
    }

    async fn delete_index(&self, name: &str) -> Result<(), CoreError> {
        let (status, body) = self
            .send(
                reqwest::Method::DELETE,
                &format!("/collections/{name}"),
                None,
            )
            .await?;
        match status {
            // gone already is the state the caller asked for
            404 => Ok(()),
            s if (200..300).contains(&s) => Ok(()),
            other => Err(status_error(
                other,
                &format!("delete collection {name}"),
                &body,
            )),
        }
    }

    fn can_quarantine(&self) -> bool {
        true
    }

    async fn quarantine(&self, rejected: &[Rejection]) -> Result<(), CoreError> {
        if rejected.is_empty() {
            return Ok(());
        }
        self.ensure_own_collections().await?;
        let points: Vec<Value> = rejected
            .iter()
            .map(|r| {
                let doc = crate::reject_doc(r);
                let id = crate::reject_doc_id(&r.index, &r.doc_id);
                json!({
                    "id": point_id(&id),
                    "payload": {
                        "key": id,
                        "doc": doc.clone(),
                        "at_epoch": doc["at_epoch"].as_u64().unwrap_or_default(),
                    },
                    "vector": {},
                })
            })
            .collect();
        self.call(
            reqwest::Method::PUT,
            &format!("/collections/{REJECTS_COLLECTION}/points?wait=true"),
            Some(json!({"points": points})),
            "quarantine the refused documents",
        )
        .await
        .map(|_| ())
    }

    async fn list_rejects(&self, limit: usize) -> Result<(Vec<StoredReject>, u64), CoreError> {
        // A page of nothing is what `validate` asks for to learn the size of
        // the store, and a scroll of nothing is a validation error here rather
        // than an empty page.
        if limit == 0 {
            return Ok((
                vec![],
                self.count_documents(REJECTS_COLLECTION)
                    .await?
                    .unwrap_or_default(),
            ));
        }
        let (status, body) = self
            .send(
                reqwest::Method::POST,
                &format!("/collections/{REJECTS_COLLECTION}/points/scroll"),
                Some(json!({
                    "limit": limit,
                    "with_payload": true,
                    "order_by": {"key": "at_epoch", "direction": "desc"},
                })),
            )
            .await?;
        // nothing was ever quarantined, which is not an error
        if status == 404 {
            return Ok((vec![], 0));
        }
        if status != 200 {
            return Err(status_error(status, "list rejects", &body));
        }
        let stored: Vec<StoredReject> = body["result"]["points"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|point| {
                let payload = &point["payload"];
                crate::reject_from_doc(payload["key"].as_str()?, &payload["doc"])
            })
            .collect();
        // The page carries the whole store's size with it, which a caller
        // bounding itself against the store needs and a page cannot say.
        let total = self
            .count_documents(REJECTS_COLLECTION)
            .await?
            .unwrap_or_default();
        Ok((stored, total))
    }

    async fn clear_reject(&self, id: &str) -> Result<(), CoreError> {
        self.drop_point(REJECTS_COLLECTION, id).await
    }

    async fn read_state(&self, key: &str) -> Result<Option<Value>, CoreError> {
        self.state_doc(&format!("state-{key}")).await
    }

    async fn write_state(&self, key: &str, doc: &Value) -> Result<(), CoreError> {
        self.put_state_doc(&format!("state-{key}"), doc).await
    }

    async fn clear_state(&self, key: &str) -> Result<(), CoreError> {
        self.drop_point(STATE_COLLECTION, &format!("state-{key}"))
            .await
    }

    async fn write_checkpoint(&self, checkpoint: &Checkpoint) -> Result<(), CoreError> {
        self.put_state_doc(
            &checkpoint_key(&checkpoint.stream),
            &crate::checkpoint_doc(checkpoint),
        )
        .await
    }

    async fn read_checkpoint(
        &self,
        stream: &pg2osync_core::checkpoint::StreamId,
    ) -> Result<Option<Checkpoint>, CoreError> {
        Ok(self
            .state_doc(&checkpoint_key(stream))
            .await?
            .as_ref()
            .and_then(crate::checkpoint_from_doc))
    }

    async fn health(&self) -> Result<Health, CoreError> {
        match self.send(reqwest::Method::GET, "/healthz", None).await {
            Ok((200, _)) => Ok(Health::Up),
            Ok((status, _)) => Ok(Health::Down(format!("status {status}"))),
            Err(e) => Ok(Health::Down(e.to_string())),
        }
    }
}

/// Named after the stream, so two pipelines against one target do not
/// overwrite each other's position.
fn checkpoint_key(stream: &pg2osync_core::checkpoint::StreamId) -> String {
    format!("checkpoint-{}", crate::checkpoint_doc_id(stream))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn documents() -> CollectionShape {
        CollectionShape {
            vectors: vec![("embedding".into(), 3)],
        }
    }

    #[test]
    fn a_document_id_always_maps_to_the_same_point() {
        // at-least-once delivery means this batch arrives again after every
        // restart, and it has to land on the point it landed on before
        assert_eq!(point_id("kit-1"), point_id("kit-1"));
        assert_ne!(point_id("kit-1"), point_id("kit-2"));
    }

    #[test]
    fn a_point_id_is_a_version_5_uuid() {
        let id = point_id("kit-1");
        assert_eq!(id.len(), 36, "{id}");
        assert_eq!(id.as_bytes()[14], b'5', "{id}");
        assert!(
            matches!(id.as_bytes()[19], b'8' | b'9' | b'a' | b'b'),
            "{id}"
        );
    }

    #[test]
    fn an_integer_id_is_mapped_by_the_same_rule_as_any_other() {
        // no u64 special case: `7` and `"7"` are one document however the
        // source spelled it
        assert_eq!(point_id("7"), point_id("7"));
        assert_eq!(point_id("7").len(), 36);
    }

    #[test]
    fn a_declared_vector_becomes_a_vector_and_everything_else_payload() {
        let point = point_body(
            &documents(),
            "doc-1",
            &json!({"title": "alpha", "embedding": [1.0, 2.0, 3.0]}),
            Some(42),
        )
        .expect("a document of the declared shape");
        assert_eq!(point["vector"], json!({"embedding": [1.0, 2.0, 3.0]}));
        assert_eq!(point["payload"]["title"], json!("alpha"));
        assert_eq!(point["payload"][ID_FIELD], json!("doc-1"));
        assert_eq!(point["payload"][VERSION_FIELD], json!(42));
        assert_eq!(point["id"], json!(point_id("doc-1")));
    }

    #[test]
    fn a_row_whose_embedding_is_not_computed_yet_is_still_a_point() {
        let point = point_body(&documents(), "doc-1", &json!({"embedding": null}), None)
            .expect("a document with no vector yet");
        assert_eq!(point["vector"], json!({}));
        assert_eq!(point["payload"][VERSION_FIELD], Value::Null);
    }

    #[test]
    fn a_vector_of_the_wrong_shape_is_refused_by_name() {
        let why = point_body(
            &documents(),
            "doc-1",
            &json!({"embedding": [1.0, 2.0]}),
            None,
        )
        .expect_err("a vector the collection cannot hold");
        assert!(why.contains("embedding"), "{why}");
        assert!(why.contains('3'), "{why}");
        let why = point_body(&documents(), "doc-1", &json!({"embedding": "nope"}), None)
            .expect_err("a vector that is not an array");
        assert!(why.contains("embedding"), "{why}");
    }

    #[test]
    fn a_read_back_document_carries_its_vector_and_none_of_the_bookkeeping() {
        let doc = document_of(&json!({
            "id": "6ba7b810-9dad-11d1-80b4-00c04fd430c8",
            "payload": {"title": "alpha", ID_FIELD: "doc-1", VERSION_FIELD: 42},
            "vector": {"embedding": [1.0, 2.0, 3.0]},
        }));
        assert_eq!(doc, json!({"title": "alpha", "embedding": [1.0, 2.0, 3.0]}));
    }

    #[test]
    fn a_truncate_clears_what_came_before_its_position_and_keeps_what_came_after() {
        let filter = truncate_filter(Some(400));
        assert_eq!(filter["should"][0]["range"]["lte"], json!(400));
        assert_eq!(filter["should"][1]["is_empty"]["key"], json!(VERSION_FIELD));
        // a truncate with no position of its own clears the collection
        assert_eq!(truncate_filter(None), json!({}));
    }

    #[test]
    fn an_overloaded_target_is_retried_and_a_refused_document_is_not() {
        assert!(is_refusal(400));
        assert!(!is_refusal(429));
        assert!(!is_refusal(503));
        assert!(matches!(
            status_error(400, "write", &json!({"status": {"error": "Wrong input"}})),
            CoreError::Sink(why) if why.contains("Wrong input")
        ));
        assert!(matches!(
            status_error(503, "write", &Value::Null),
            CoreError::SinkTransient(_)
        ));
    }
}
