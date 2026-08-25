# Configuration reference

One TOML file describes the whole pipeline. Unknown keys are rejected at load
time, so a typo fails immediately instead of silently doing nothing.

Full example: [examples/pg2osync.example.toml](../examples/pg2osync.example.toml).

Everything structural is checked by `pg2osync validate`, which also connects to
both ends and verifies server prerequisites.

## Secrets

Credentials belong in environment variables. Every secret has an `*_env` form:

```toml
[source]
url_env = "PG2OSYNC_SOURCE_URL"      # preferred

[target]
password_env = "PG2OSYNC_TARGET_PASSWORD"
```

Plain `url` and `password` keys still work but log a deprecation warning on
startup. Secrets never appear in logs or error messages.

## `[source]`

| Option | Default | Description |
|---|---|---|
| `flavor` | `"postgres"` | `"postgres"` or `"mysql"` (also covers MariaDB) |
| `mode` | `"wal"` | `"wal"` (replication log) or `"poll"`. PostgreSQL only |
| `url_env` | — | Environment variable holding the connection URL |
| `url` | — | Inline URL; warns as deprecated |
| `sslmode` | from the URL, else `prefer` | `disable`, `prefer`, `require`, `verify-ca`, `verify-full` |
| `sslrootcert` | — | PEM bundle of trusted roots for the verifying modes |
| `admin_url_env` | falls back to the source URL | Separate connection for catalog and nested-child queries |
| `reconnect_max` | `10` | Consecutive stream failures tolerated before exiting; `0` exits on the first |
| `reconnect_backoff_ms` | `1000` | Initial reconnect delay, doubled per failure, capped at 30 s |
| `load_workers` | `1` | Ranges of the initial load read at once, each on its own connection. PostgreSQL only. Worth raising only for tables with nested children, where the server does per-row work: measured +53% there, +5–8% on ordinary tables for four times the read load |
| `load_chunk_rows` | `50000` | Rows one initial-load piece covers: a sampled range on PostgreSQL, a keyset chunk on MySQL. On PostgreSQL it is also how often the load can react to WAL pressure, since a range cannot be interrupted |
| `slot_name` | `"pg2osync"` | PostgreSQL replication slot |
| `publication` | `"pg2osync_pub"` | PostgreSQL publication |
| `server_id` | `424242` | MySQL: replica id, unique across the server's replicas |
| `poll_column` | `"updated_at"` | Poll mode: default timestamp column |
| `poll_interval_secs` | `30` | Poll mode: seconds between cycles |
| `poll_page_size` | `5000` | Poll mode: rows per table per cycle |

URL formats:

```
postgres://user:pass@host:5432/dbname
mysql://user:pass@host:3306/dbname
```

Percent-encoded credentials are decoded, so a password containing `@` or `:`
works if you encode it.

`admin_url_env` exists so the replication connection and ordinary queries can
use different users — the replication role needs `REPLICATION`, the admin role
needs `SELECT` on the synced tables.

### TLS

`sslmode` follows libpq exactly, and applies to every connection pg2osync opens
— the replication stream and the MySQL binlog dump included, so a source can
never end up half encrypted.

| Mode | Encrypted | Certificate checked | Hostname checked |
|---|---|---|---|
| `disable` | no | — | — |
| `prefer` *(default)* | if the server offers it | no | no |
| `require` | yes | no | no |
| `verify-ca` | yes | yes | no |
| `verify-full` | yes | yes | yes |

An explicit `sslmode` in the config wins over one in the connection URL, so a
URL pasted from a provider cannot weaken a deployment that pinned its mode.

`prefer` is the default because libpq uses it and it improves an unconfigured
deployment without breaking a server that has no certificate. It is not a
guarantee: a server that does not offer TLS is silently accepted. Anything
crossing a network you do not control wants `verify-full`.

With `verify-ca` and `verify-full`, `sslrootcert` points at the CA bundle; when
it is omitted the bundled Mozilla roots are used, which is what public managed
providers chain to.

### Poll mode

For managed PostgreSQL instances where logical replication cannot be enabled.
It re-reads rows whose timestamp column advanced since the last cycle.

- **Deletes are invisible.** There is no log to read them from.
- Requires a monotonically increasing timestamp column per table.
- Each start re-runs the initial load: there is no position to resume from, and
  re-indexing is harmless under idempotent writes. Existing WAL checkpoints are
  ignored in this mode so a gap can never be skipped.

## `[target]`

| Option | Default | Description |
|---|---|---|
| `flavor` | `"opensearch"` | `"opensearch"`, `"elasticsearch"` or `"meilisearch"` |
| `url` | *(required)* | Base URL, e.g. `http://localhost:9200` |
| `username` | — | Basic-auth user |
| `password` / `password_env` | — | Basic-auth password |
| `api_key_env` | — | Elasticsearch API key, or Meilisearch master key |
| `tls_verify` | `true` | Only disable for self-signed development certificates |
| `serverless` | `false` | Amazon OpenSearch Serverless profile: skips the refresh and settings calls it rejects. Needs a **search** collection and your own SigV4 proxy — an `*.aoss.amazonaws.com` url is refused at startup, since nothing here can sign a request. [Never verified against a real collection](sinks/opensearch.md#amazon-opensearch-serverless) |
| `state_dir` | `./.pg2osync-state` | Meilisearch only: directory for the checkpoint file |

Meilisearch has no place to store an arbitrary document, so its checkpoint is a
local file. Give that directory persistent storage, or a restart re-runs the
initial load.

## `[sync.<key>]`

One section per table. `<key>` is the index name when `index` is omitted.

| Option | Description |
|---|---|
| `table` | **Required.** `schema.table` for PostgreSQL, `database.table` for MySQL |
| `index` | Target index or collection; lowercase `[a-z0-9_-]`, not starting with `_` or `.` |
| `primary_key` | Overrides key detection; also the join column for nested children |
| `columns` | Only these columns are indexed |
| `exclude_columns` | All columns except these; mutually exclusive with `columns` |
| `transform` | Map of column to `"hash"` or `"redact"` |
| `poll_column` | Poll mode: overrides `[source] poll_column` for this table |
| `soft_delete` | SQL predicate marking a row as deleted, e.g. `deleted_at IS NOT NULL` |
| `mapping_file` | JSON mapping to create the index with, see below |
| `children` | Nested child collections, see below |

Projection and transforms apply to every path — initial load, live streaming and
poll mode — so an excluded column never reaches the target. The primary key is
read before projection, so excluding a key column is rejected at load time
(it would collide document ids).

`hash` replaces the value with a truncated SHA-256 digest, stable across runs so
it can still be grouped on. `redact` replaces it with `***`. Null values are
left alone in both cases.

Two tables may not map to the same index: document identity would be ambiguous.

### Soft deletes

Poll mode has no replication log, so a row that is simply gone leaves nothing
to poll and cannot be seen. A row *marked* deleted can be:

```toml
[sync.users]
table = "public.users"
soft_delete = "deleted_at IS NOT NULL"
```

A row matching the predicate is removed from the index instead of upserted, and
the initial load skips it rather than indexing it only to delete it on the
first cycle. The predicate is evaluated by the database — it is the only party
that knows the column's type — so any boolean expression over the row's own
columns works, `status = 'archived'` as much as a timestamp check.

It is poll-mode only, and configuring it elsewhere is rejected rather than
ignored. WAL mode sees a soft delete as the ordinary `UPDATE` it is, and
turning that into a delete would mean evaluating the predicate here rather than
in the database — which is where the column's type lives. Until that is worth
building, a WAL-mode pipeline should filter on the column at query time.

### Index mappings

Without `mapping_file` the index is created empty and the target infers field
types from the first document that carries each field. That is enough to get
started and not enough for real search: analyzers, `keyword` subfields for
aggregation, explicit date formats and vector fields all have to exist before
the first document lands.

```toml
[sync.users]
table = "public.users"
index = "users"
mapping_file = "users-mapping.json"
```

The path is resolved relative to the config file, and the file is read at
startup so a missing or malformed one fails before anything connects. It holds
either a full index-creation body or just the mapping:

```json
{
  "mappings": {
    "properties": {
      "id":         { "type": "long" },
      "name":       { "type": "text", "fields": { "raw": { "type": "keyword" } } },
      "created_at": { "type": "date" }
    }
  },
  "settings": { "number_of_shards": 1 }
}
```

Three rules, and the reasoning behind each:

- **It applies only when the index does not exist.** A target refuses to change
  an existing field's type — that is a reindex — so applying a mapping to a
  live index would either fail or quietly do half the job.
- **An existing index is compared against it at startup.** A field the index
  maps to a different type is an error: every document carrying it would be
  rejected, and a permanent rejection halts the pipeline. A field the index
  does not declare is a warning: it will be mapped from whatever value arrives
  first, which may be what you wanted.
- **Only the fields you name are checked.** The target normalises what it is
  given and dynamic mapping legitimately adds fields you never declared, so an
  equality check would report differences on a mapping that is exactly right.

If you would rather manage an index template, leave `mapping_file` unset: the
index is then created without a body and your template applies. Configuring
both means the creation body wins and the template is ignored for these
indices.

Meilisearch has no field types to declare; `mapping_file` is refused for that
target rather than ignored.

### Nested children

Embed a one-to-many relation as a JSON array on the parent document:

```toml
[sync.customers]
table = "public.customers"
index = "customers"
primary_key = "id"

[[sync.customers.children]]
table = "public.orders"      # child table
field = "orders"             # array field on the parent document
foreign_key = "customer_id"  # column on the CHILD referencing the parent key
# max_rows = 1000            # optional: embed at most this many, see below
```

- PostgreSQL and MySQL/MariaDB alike.
- One level deep only.
- Children are fetched during the initial load and re-fetched whenever the
  parent or any of its children changes, so the array is never stale.
- Child tables are added to the publication automatically.
- The initial load reads each collection once and joins it, so it costs one
  query per table no matter how many parents there are.
- **Streamed changes cost one query per collection per transaction**, not per
  row. Rows are held until the transaction commits, the distinct parents they
  affect are collected, and each collection is read once for the whole group — so
  a transaction touching 2,000 children of 20 parents issues 3 queries and writes
  20 documents, where per-row resolution issued 4,001 and wrote 2,000.
  **Index the foreign key on the child table**: those lookups compare the key in
  its own type, and without an index each one scans the whole child table.
- The array is ordered by the child table's primary key, so the initial load and
  a later re-fetch embed it identically and a re-snapshot does not rewrite
  documents for no reason. A child table with no primary key has no such order,
  and says so at startup.
#### How many children to embed

`max_rows` is unset by default, so the whole collection is embedded however large
it is. That is deliberate: a cap loses data, and the bound that matters is already
the target's. Past `index.mapping.nested_objects.limit` — 10,000 by default —
OpenSearch **refuses** a document whose field is mapped `nested`, because every
element becomes a hidden Lucene sub-document; that refusal names the parent and is
quarantined rather than lost (see `on_permanent_rejection`). Below the limit the
cost is gradual. An array past 10,000 is logged, naming the parent, so the
decision is visible rather than a surprise later.

Setting `max_rows` trades a complete array for a bounded document. A document
whose array was cut says so, in two extra fields:

```json
{ "id": 42, "orders": [ /* max_rows of them */ ],
  "orders_truncated": true, "orders_total": 100000 }
```

They appear only when something was left out, so `orders_truncated: true` finds
every affected parent in one query. Which rows are kept is decided by the child
table's primary key, so the same ones are kept every time and the initial load and
a streamed re-fetch agree — a cap without that order would keep a different subset
on each run. `max_rows` on a child table with no primary key is refused at
startup for the same reason.

- The field name must not collide with a column of the parent table; the initial
  load refuses to start rather than shadow a real column.
- **Give child tables `REPLICA IDENTITY FULL`**
  (`ALTER TABLE public.orders REPLICA IDENTITY FULL`). Without it a DELETE
  carries no foreign key, so the parent cannot be located; pg2osync warns at
  startup and fails on such a delete rather than silently going stale.
- Not supported for the MySQL source yet.

## `[engine]`

Defaults are production-sane; tune only against measurements.

| Option | Default | Description |
|---|---|---|
| `batch_size` | `500` | Rows per sink request |
| `batch_max_bytes` | `10485760` | Approximate byte ceiling per request; whichever limit hits first splits the batch |
| `write_concurrency` | `1` | Write requests open against the target at once. One at a time is what the initial load is limited by, not the source read; raising it multiplies the load on the target, and it needs a target that orders by document version, so Meilisearch refuses anything above 1 |
| `txn_buffer_cap_mb` | `256` | Warning threshold for one open transaction |
| `retry_max` | `10` | Attempts per request before the pipeline stops |
| `retry_backoff_ms` | `500` | Initial backoff, doubled per attempt, capped at 30 s |
| `checkpoint_interval_ms` | `500` | How often the position is persisted |
| `on_permanent_rejection` | `"halt"` | `"halt"` stops the pipeline on a document the target will never accept. `"quarantine"` records it in a hidden `.pg2osync_rejects` index, with its position, and carries on |
| `max_rejects` | `100` | Quarantined documents allowed before the pipeline halts anyway. Counted against what the store holds, so a restart does not reset it |

`checkpoint_interval_ms` is the ceiling on replayed work after a crash: a lower
value means less replay and more writes to the target.

A transaction larger than `txn_buffer_cap_mb` is split across requests, which
means the target briefly holds part of it. Everything is idempotent, so the end
state is correct, but a reader can observe the transaction half-applied.

Transient failures (HTTP 429, 5xx, connection resets) are retried with
exponential backoff. A permanent rejection — a mapping conflict, for example —
stops the pipeline instead of skipping the document, because skipping is silent
data loss. `on_permanent_rejection = "quarantine"` trades that for availability:
the document is recorded with its position before the position is acknowledged,
so nothing is lost, but the transaction it belonged to is applied without it.
`pg2osync rejects --replay` puts it back once the mapping is fixed. Only the
OpenSearch and Elasticsearch targets can quarantine; configuring it against
Meilisearch fails at startup.

## `[api]`

The read-your-writes endpoint. Off by default: it is a surface applications
call, not an operational one.

| Option | Default | Description |
|---|---|---|
| `enabled` | `false` | Serve the endpoint |
| `bind` | `127.0.0.1:9101` | Listen address |
| `token_env` | — | Env var holding a bearer token required on every request |

### `GET /synced`

Blocks until everything committed before the request is written to the target,
then answers. A query made after it returns is guaranteed to see those writes.

| Parameter | Default | Description |
|---|---|---|
| `position` | read from the source | Where to wait for; omit and pg2osync reads it itself |
| `timeout` | `5000` | Milliseconds to wait, capped at 30 s |
| `refresh` | `false` | Also make the writes searchable, not merely stored |

```
GET /synced?refresh=true&timeout=2000
200 {"synced":true,"requested":"0/1B4F2A8","confirmed":"0/1B4F2B0","waited_ms":5}
408 {"synced":false,…}   still behind when the timeout elapsed
400 the position could not be parsed
```

Leave `position` out unless you have a reason not to. Reading it requires
`REPLICATION CLIENT` on MySQL — a privilege an application account should not
hold — and pg2osync already has a connection that does.

`refresh=true` is what separates *stored* from *searchable*: OpenSearch and
Elasticsearch only expose a write to search after a refresh, on their own
interval. Without it the document is retrievable by id but a search may not
find it yet.

The wait costs nothing on the write path. A background job that does not care
never calls this and pays nothing.

## `[metrics]`

| Option | Default | Description |
|---|---|---|
| `enabled` | `true` | Serve the Prometheus endpoint |
| `bind` | `127.0.0.1:9100` | Listen address; use `0.0.0.0:9100` in a container |
| `token_env` | unset | Variable holding a bearer token required on `/metrics` |

Only `GET /metrics` and `GET /healthz` are served; anything else is a 404.
`/healthz` is never authenticated, because a kubelet probe has nowhere to keep
a token and a liveness check that fails on a missing one would restart a
healthy pipeline.

With `token_env` set, Prometheus sends the same token:

```yaml
scrape_configs:
  - job_name: pg2osync
    authorization:
      type: Bearer
      credentials_file: /etc/prometheus/pg2osync-token
    static_configs:
      - targets: ["pg2osync:9100"]
```

```
pg2osync_events_total{type="row|truncate"}
pg2osync_batches_flushed
pg2osync_toast_readbacks_total                  # reads to complete TOASTed columns
pg2osync_sink_errors_total
pg2osync_reconnects_total
pg2osync_latency_ms{quantile="0.5|0.9|0.99"}   # source commit to indexed
pg2osync_latency_ms_count
pg2osync_position_current                       # highest position received
pg2osync_position_confirmed                     # highest position checkpointed
pg2osync_position_lag                           # difference between the two
```

## Environment variables

| Variable | Purpose |
|---|---|
| `RUST_LOG` | Log filter, e.g. `pg2osync=debug` |
| `PG2OSYNC_INSTANCE_ID` | Recorded in the checkpoint document; identifies the writer |
| whatever `*_env` names | The credentials themselves |

## Complete example

```toml
[source]
flavor = "postgres"
url_env = "PG2OSYNC_SOURCE_URL"
slot_name = "pg2osync"
publication = "pg2osync_pub"

[target]
flavor = "opensearch"
url = "https://opensearch.internal:9200"
username = "pg2osync"
password_env = "PG2OSYNC_TARGET_PASSWORD"
tls_verify = true

[engine]
batch_size = 500
batch_max_bytes = 10485760
write_concurrency = 1
checkpoint_interval_ms = 500

[metrics]
enabled = true
bind = "127.0.0.1:9100"

[sync.users]
table = "public.users"
index = "users"
exclude_columns = ["password_hash"]

[sync.users.transform]
email = "redact"

[sync.customers]
table = "public.customers"
index = "customers"
primary_key = "id"

[[sync.customers.children]]
table = "public.orders"
field = "orders"
foreign_key = "customer_id"
```
