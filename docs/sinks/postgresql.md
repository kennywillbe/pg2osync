# PostgreSQL (pgvector) sink

A PostgreSQL database with [pgvector](https://github.com/pgvector/pgvector) is a
search backend, and this target treats it as one: each section writes one table,
documents become rows, and an embedding column is carried like any other value.

**This is not database replication.** No schema is mirrored, no DDL is
propagated, and nothing here promises more than the document model every other
target gets. If what you want is a copy of your database, use logical
replication; this is the same pipeline that feeds OpenSearch, pointed at a
different store.

```toml
[target]
flavor = "postgres"
url_env = "PG2OSYNC_TARGET_URL"   # postgres://user:pass@host:5432/searchdb

[sync.documents]
table = "public.documents"
mapping_file = "documents.sql"    # the DDL of the table this section writes
```

`url_env` rather than `url`, because a database URL carries its password. An
inline `url` still works and `sslmode` in it is honoured the way `[source]`
honours it.

## The table is yours to declare

`mapping_file` points at a `.sql` file for this target — what an index mapping is
to OpenSearch, the `CREATE TABLE` is here. pg2osync applies it **only when the
table is absent** (the same rule the other targets apply to a mapping: changing
one in place is a migration nobody asked for), then checks the two things a
write depends on:

- a **single-column primary key** — a document has one id, and that is where it
  is filed
- a **`_version bigint` column** — see [Truncate](#truncate) below

Nothing else is compared: this sink does not parse SQL. Every other disagreement
between the DDL and the documents arrives as a per-document rejection naming the
column, which is where you can act on it.

```sql
-- documents.sql
CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE documents (
  id         text PRIMARY KEY,
  title      text,
  body       text,
  tags       jsonb,
  embedding  vector(768),
  _version   bigint
);
CREATE INDEX ON documents USING hnsw (embedding vector_cosine_ops);
```

The file may hold any DDL — extensions, indexes, constraints — and it runs as one
batch.

## Documents become rows

- **Fields map to columns by name.** A field the table has no column for is a
  permanent rejection naming the column, taking the configured
  `on_permanent_rejection` path like any other refusal. There is no catch-all
  and no dynamic mapping: the DDL is the contract, and adding the column
  ourselves is the schema mirroring this sink is not. Project the document with
  `columns` / `exclude_columns` to match what the table declares.
- **The id lands in the primary key column**, whatever the document calls it. So
  a derived id (`id = "tenant-{tenant_id}-{id}"`) is what the row is filed under,
  and the key column is naturally `text`.
- **A write is `INSERT … ON CONFLICT (pk) DO UPDATE`**, a delete is a `DELETE`,
  and one `write` is one transaction: the position is acknowledged only after the
  commit returns. A batch holding one document the target refuses still writes
  the rest — each operation runs on a savepoint — and every refusal in it is
  reported.

## Vectors are bring-your-own-embedding

A vector column is not special. The document field holding it is a JSON array of
numbers, whichever way it was produced — a `real[]` column in the source
database, an embedding your application already writes there — and PostgreSQL's
own input function turns it into a `vector` on the way in. **The sink computes
nothing:** it calls no model and has no embedding configuration, so the value in
the index is the value you produced.

```sql
-- in the target, once the pipeline has run
SELECT id, title FROM documents ORDER BY embedding <-> '[0.1, 0.2, …]' LIMIT 10;
```

Read back through `get_documents` — which the pipeline does to complete a
document with unchanged-TOAST markers — a `vector` column comes back in
pgvector's own text form (`"[1,2,3]"`) rather than the array that went in.
Writing it again is accepted, and nothing is lost.

## Truncate

A source-side `TRUNCATE` has to clear the documents written before it and keep
the ones written after — a row re-inserted a moment later must survive. That
comparison is what the `_version bigint` column is for: it carries the source
position each row was written at, and the truncate is
`DELETE … WHERE _version <= <the truncate's position>`. The same comparison
guards the upsert, so two write requests in flight at once still settle on the
later document — which is why `write_concurrency` above 1 is accepted here
rather than refused as it is on Meilisearch. It buys nothing yet: this sink
holds one connection and serializes batches on it, so raising it is safe and
idle.

`_version` is the sink's bookkeeping, not a field of your document: it is
stripped from every read-back, and nothing writes to it but the sink.

## The checkpoint lives in the target

Two tables are created in the target database and are pg2osync's own:

| Table | What it holds |
|---|---|
| `pg2osync_state` | one row per stream's checkpoint, plus initial-load progress |
| `pg2osync_rejects` | documents the target refused, when quarantine is on |

So a restart resumes from a position only the target can have moved, and
`on_permanent_rejection = "quarantine"` works here — unlike Meilisearch, which
has nowhere to keep a refused document.

## What is refused, and why

Everything below is refused by name at config load or startup, not ignored:

| Configuration | Why |
|---|---|
| `[target] require_alias` | no alias namespace: the name a section writes to is a table |
| `[sync.*] join` | no parent-child document model |
| `[sync.*] routing` | no shards to co-locate anything on |
| `[sync.*] pipeline` | no ingest pipelines |
| `index = "events-{tenant}"` | nothing can say what DDL a name a row renders should be created with |
| `pg2osync reindex` | a rebuild ends in an atomic switch of the name readers use, and there is no such step here |
| a section with no `mapping_file` | this target creates nothing on its own |

`reconcile` is not available either: it walks an index in key order, which this
sink does not offer.

Rebuilding a table therefore means building the new one beside the old with a
second instance of the whole config, then pointing your readers at it — the same
recipe [operations.md](../operations.md) gives for a shared index.

## Version targeting

`dev/e2e-postgres-sink.sh` runs on every pull request against
`pgvector/pgvector:pg17` (see [compatibility](../compatibility.md)): the initial
load, live INSERT/UPDATE/DELETE, an embedding the source produced ordering a
nearest-neighbour query in the target, a `kill -9` resuming from the checkpoint
table, `TRUNCATE`, `resnapshot`, and the refusal of an OpenSearch-only option.
The same suite runs the sink conformance kit, which asserts the part of the
contract every target shares.
