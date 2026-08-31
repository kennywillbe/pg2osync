# Qdrant sink

[Qdrant](https://qdrant.tech) is a vector database, and this target treats it as
a search backend like any other: each section writes one collection, documents
become points, and the embedding your source already produced is carried like
any other value.

```toml
[target]
flavor = "qdrant"
url = "http://qdrant.internal:6333"
api_key_env = "QDRANT_API_KEY"    # optional; the key goes in the environment

[sync.documents]
table = "public.documents"
mapping_file = "documents.json"   # the collection this section writes
```

## The collection is yours to declare

`mapping_file` points at the JSON body of `PUT /collections/<name>` for this
target — what an index mapping is to OpenSearch, the collection configuration is
here. pg2osync creates the collection **only when it is absent** (the same rule
the other targets apply to a mapping: a collection that already holds points
cannot take a new vector configuration, and rebuilding it implicitly is a
migration nobody asked for).

```json
{
  "vectors": { "embedding": { "size": 768, "distance": "Cosine" } },
  "hnsw_config": { "m": 32 }
}
```

The vectors must be **named**. The name is the whole mapping rule: a document
field called `embedding` becomes that vector, and every other field becomes
payload. A single unnamed vector (`"vectors": {"size": 768, …}`) has no name to
match a field against and is refused at startup, saying so.

## Documents become points

- **A point id is a UUIDv5 of the document id**, over a namespace fixed for the
  life of this sink. Qdrant accepts only `u64` and UUID point ids, and the ids
  this pipeline uses — a primary key, a derived `tenant-{tenant_id}-{id}`, a
  content hash — are neither. The mapping is deterministic, which is what makes
  a replay overwrite rather than duplicate. There is no special case for an id
  that looks like an integer: one rule, so `7` and `"7"` are one document.
- **The id itself is kept in the `_pg2osync_id` payload field**, which is how
  you find a document by the id you know:

  ```bash
  curl -s -XPOST "$QDRANT/collections/documents/points/scroll" \
    -H 'content-type: application/json' \
    -d '{"filter":{"must":[{"key":"_pg2osync_id","match":{"value":"42"}}]},
         "with_payload":true}'
  ```

- **A write is `PUT /collections/<name>/points?wait=true`**, and the position is
  acknowledged only after that returns. `wait=false` would answer
  `acknowledged` before the operation is applied, which is not a position it is
  safe to checkpoint.
- **A batch holding one document the target refuses still writes the rest.** A
  Qdrant batch upsert is all-or-nothing, so a refused request is retried one
  point at a time; each refusal is reported against the document that caused it
  and takes the configured `on_permanent_rejection` path.

## Vectors are bring-your-own-embedding

The field holding a vector is a JSON array of numbers, whichever way it was
produced — a `real[]` column in the source database, an embedding your
application already writes there. **The sink computes nothing:** it calls no
model and has no embedding configuration, so the vector in the collection is the
vector you produced.

A document whose vector field is absent or null is stored as a **point without
that vector**: it is retrievable by id, filterable on its payload, and it joins
the similarity search the moment the embedding lands. A value that is there but
is not an array of the declared length is a permanent rejection naming the
field.

## Truncate

A source-side `TRUNCATE` has to clear the documents written before it and keep
the ones written after — a row re-inserted a moment later must survive. Every
point carries a `_version` payload field with the source position it was written
at, and the truncate is a delete-by-filter of `_version <= <the truncate's
position>` plus the points that carry none. `ensure_ready` creates a payload
index on `_version`, so that filter is an index lookup rather than a scan of the
collection.

`_version` and `_pg2osync_id` are the sink's bookkeeping, not fields of your
document: both are stripped from every read-back.

## Ordering, and why `write_concurrency` stays at 1

Qdrant has no external document version: an upsert overwrites whatever the point
held. Two write requests open at once could therefore settle one document either
way round, so this target reports that it does not order by version and
`[engine] write_concurrency` above 1 is refused at startup — the same answer
Meilisearch gives. With one request in flight the engine delivers a document's
operations in order, and because every write is the whole document, a replay
after a restart writes the same document again.

## The checkpoint lives in the target

Two collections are created and are pg2osync's own:

| Collection | What it holds |
|---|---|
| `pg2osync_state` | one point per stream's checkpoint, plus initial-load progress |
| `pg2osync_rejects` | documents the target refused, when quarantine is on |

Neither holds a vector. So a restart resumes from a position only the target can
have moved, and `on_permanent_rejection = "quarantine"` works here — unlike
Meilisearch, which has nowhere to keep a refused document.

## What is refused, and why

Everything below is refused by name at config load or startup, not ignored:

| Configuration | Why |
|---|---|
| `[sync.*] join` | no parent-child document model |
| `[sync.*] routing` | no shards to co-locate anything on |
| `[sync.*] pipeline` | no ingest pipelines; the embedding is computed by whatever holds the model |
| `index = "events-{tenant}"` | nothing can say what vectors a name a row renders should be created with |
| a section with no `mapping_file` | a collection cannot be created without the vectors it holds |
| `[target] require_alias` | it refuses every write to a name that is not an alias, and the rebuild that produces one is not implemented here |
| `pg2osync reindex` | a rebuild ends in an atomic switch of the name readers use; Qdrant has collection aliases, but this sink does not drive them |
| `[engine] write_concurrency > 1` | see [Ordering](#ordering-and-why-write_concurrency-stays-at-1) above |

`reconcile` is not available either: it walks an index in key order, which this
sink does not offer.

Rebuilding a collection therefore means building the new one beside the old with
a second instance of the whole config, then pointing your readers at it — the
same recipe [operations.md](../operations.md) gives for a shared index.

## Version targeting

`dev/e2e-qdrant.sh` runs nightly against `qdrant/qdrant:v1.15.1` (see
[compatibility](../compatibility.md)): the initial load, live
INSERT/UPDATE/DELETE, an embedding the source produced answering a similarity
search, a fanned list that really deletes the elements it drops, a `kill -9`
resuming from the state collection, `TRUNCATE` at a position, and the refusal of
an OpenSearch-only option. The same suite runs the sink conformance kit, which
asserts the part of the contract every target shares, with no check skipped.
