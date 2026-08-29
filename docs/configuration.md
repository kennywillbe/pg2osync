# Configuration reference

One TOML file describes the whole pipeline. Unknown keys are rejected at load
time, so a typo fails immediately instead of silently doing nothing.

You do not have to write it from this page. `pg2osync init --table users` writes
the smallest config that runs, qualifying the table name from the source's own
catalogue and refusing a table with no primary key; this reference is for the
options you add afterwards. Every command defaults to `pg2osync.toml`, which is
what `init` writes.

Full example: [examples/pg2osync.example.toml](https://github.com/kennywillbe/pg2osync/blob/main/examples/pg2osync.example.toml).

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
| `id` | Derived document id, e.g. `tenant-{tenant_id}-{id}`; see [Document ids](#document-ids) |
| `fan_out` | One row becomes one document per element of an array column; see [Fan-out](#fan-out) |
| `columns` | Only these columns are indexed |
| `exclude_columns` | All columns except these; mutually exclusive with `columns` |
| `transform` | Map of column to an operation, see [Transforms](#transforms) |
| `fields` | Map of source column to target field name; applied last, see [Field names](#field-names) |
| `constants` | Map of field name to a literal value added to every document; `{schema}`/`{table}` in a string render at startup, see [Constant fields](#constant-fields) |
| `where` | Restricted SQL predicate deciding which rows are indexed, e.g. `status = 'active' AND deleted_at IS NULL`; see [Row filters](#row-filters) |
| `poll_column` | Poll mode: overrides `[source] poll_column` for this table |
| `soft_delete` | SQL predicate marking a row as deleted, e.g. `deleted_at IS NOT NULL` |
| `mapping_file` | JSON mapping to create the index with, see below |
| `children` | Nested child collections, see below |

Projection and transforms apply to every path — initial load, live streaming and
poll mode — so an excluded column never reaches the target. The primary key is
read before projection, so excluding a key column is rejected at load time
(it would collide document ids). Ids, likewise, render from the row's raw
values: before projection and before transforms. `fields` renames run after
projection and transforms, and `constants` are added after that; every other
option names the column as the source knows it.

### Document ids

By default a document's `_id` is its row's primary key, exactly as it always
has been — configuring nothing changes nothing, and an existing index needs no
rebuild. `id` overrides the shape:

```toml
[sync.orders]
table = "public.orders"
id = "tenant-{tenant_id}-{id}"
```

Literals plus `{column}` placeholders. The name has to be a column of the
table, and the value renders like a key does: strings unquoted, numbers and
booleans as text.

- A NULL in a column the id references **halts the pipeline** — the id cannot
  be invented, and the document the row already owns would be stranded.
  `validate` warns up front for nullable columns.
- An id naming only key columns works everywhere. An id that references
  columns **outside** the key needs the row's before-image to delete and move
  its documents, so on PostgreSQL the table must be
  `REPLICA IDENTITY FULL`; `run` refuses to start otherwise. MySQL already
  guarantees it (`binlog_row_image = FULL`).

### Fan-out

One row whose array column holds N elements can become N documents:

```toml
[sync.tickets]
table = "public.tickets"
id = "ticket-{id}"

[sync.tickets.fan_out]
field = "tags"            # a PostgreSQL array column, or jsonb holding an array
id = "ticket-{id}-{tags}"
```

Each element document is the parent document **minus the array**, merged with
the element: an object element's fields are merged in and win on collision, a
scalar element lands under the array's own field name. The element `id`
renders from that merged document, so its placeholders can name parent columns
and element fields alike.

- A row with an **empty or missing** array emits nothing; a row whose array is
  **NULL** keeps one parent document under the plain `id`.
- Updates diff before against after: elements that left the array have their
  documents deleted, the rest are rewritten. Deletes remove every element
  document the row owned. All of it as ordinary versioned writes, in the same
  order as everything else — `write_concurrency` keeps working.
- PostgreSQL: the table needs `REPLICA IDENTITY FULL` (checked at startup),
  because deletes and diffs come from the row's old values. Poll mode and
  `[[children]]` on the same table are refused; so is naming the fan-out
  column in `columns`/`exclude_columns`, which would cut the array before
  identity and fan-out ever see it. `reconcile` and `resnapshot` do not
  support fanned tables yet: both page by key, and one row now has many
  documents.

Two tables may not map to the same index: document identity would be ambiguous.

### Transforms

A column can be reshaped on its way into the document. `transform` maps a
source column to one of six named operations: a string for an op that takes
no parameter, an inline table for one that does.

```toml
[sync.users]
table = "public.users"

[sync.users.transform]
email   = "hash"
phone   = "redact"
payload = "json"                             # or { op = "json" }
price   = "number"
tags    = { op = "split", by = "," }
born    = { op = "date", from = "%d/%m/%Y" }
```

`hash` replaces the value with a truncated SHA-256 digest, stable across runs so
it can still be grouped on. `redact` replaces it with `***`. The other four turn
a string into something more structured:

| op | takes | turns | into |
|---|---|---|---|
| `hash` | — | any value | a truncated SHA-256 digest |
| `redact` | — | any value | `***` |
| `json` | — | a string holding JSON | that JSON value, an object or a bare number alike |
| `split` | `by`, required and non-empty | a delimited string | an array of its trimmed, non-empty pieces: `"a, b ,c"` → `["a","b","c"]`, `""` → `[]` |
| `number` | — | a string holding a number | a JSON number: an integer when it is one, otherwise a double |
| `date` | `from`, a `strptime`-style format, required and non-empty | a string in that format | ISO 8601: `YYYY-MM-DD` for a date, `YYYY-MM-DDTHH:MM:SS` for a date-time, RFC 3339 with the offset kept when the format carries one |

NULL is left alone by every op, and so is a value already in the target shape:
a parsed `json`/`jsonb`/`JSON` column under `json`, an array under `split`, a
number under `number`. That is what keeps the ops idempotent when
at-least-once delivery replays a row, and it is why the three exist for
*text* columns that hold something more structured. `number` is also the
explicit opt-out of the rule that `numeric`/`DECIMAL` arrive as strings to
keep their precision — for an index that sorts or range-queries on the value
and accepts the double.

A value an op cannot convert — `"abc"` under `number`, a date that does not
match `from` — is indexed **exactly as it arrived**, counted in
`pg2osync_transform_unconverted_total`, and logged once per table and column.
The pipeline never halts on it: the target's mapping is the arbiter of what a
field holds, and a document the mapping refuses takes the ordinary rejection
path (see `on_permanent_rejection`). A fanned row counts once per element
document.

- If one field will hold both converted and unconverted values, `mapping_file`
  should type it as `text`. Otherwise dynamic mapping types the field from the
  first document and refuses the second — and that refusal is the halt or
  quarantine path, not this policy.
- Transforms name the **source** column and run after projection, before
  `fields` renames and `constants`.
- `split` cannot feed `fan_out`: fan-out reads the raw row, before any
  transform, so it needs a real array column.

Refused at load: an unknown op, a parameter the op does not take, `split`
without a non-empty `by`, `date` without a non-empty `from`, and a transform
on the `fan_out.field`.

### Field names

An index that already exists is rarely named after the database. `fields`
stores a column under another name:

```toml
[sync.users]
table = "public.users"

[sync.users.fields]
usr_nm = "username"
```

The rename is the **last** shaping step — identity, fan-out, projection and
transforms all run first — so every other option (`columns`,
`exclude_columns`, `transform`, `id`, `fan_out.field`, `primary_key`,
`soft_delete`, `poll_column`) keeps naming the column as the source knows it.
The new name applies on the initial load, the stream, poll mode and a
re-snapshot alike, and inside embedded child arrays through the child's own
`fields` (see [Nested children](#nested-children)).

Refused at load: an empty name, renaming a column to itself, two columns to
the same name, renaming an excluded column or one missing from `columns`, a
target that equals a non-renamed column in `columns`, and a parent rename that
names or targets a child `field` (or its `_truncated`/`_total`). `validate`
warns when a renamed column does not exist — a stale config, as with
`exclude_columns` — and refuses a target that equals a live column that is not
itself renamed away.

- TOAST completion reads the stored document, so it finds the column under its
  new name.
- `mapping_file` must declare the **renamed** names: the mapping is compared
  against the index, never against the table.

### Constant fields

A tag several indices can be queried by, or a marker of where a document came
from, needs no column. `constants` adds a literal value to every document of
the section:

```toml
[sync.users]
table = "public.users"

[sync.users.constants]
entity = "user"
tenant = "eu"
origin = "{schema}.{table}"
rank = 3
active = true
```

Scalars only — string, integer, float, boolean; arrays, tables and datetimes
are refused at load. `{schema}` and `{table}` are the only placeholders,
allowed only inside a string and rendered **once at startup**, so a string
naming any other placeholder (or with a malformed `{`) is refused at load. A
string without `{` is taken verbatim; there is no way to write a literal `{`.

Constants are added **last** — after identity, fan-out, projection, transforms
and renames — because `columns` would otherwise strip a field that is not a
column. Every fanned element document carries them; child arrays do not.

Refused at load: a name that is a rename target, a surviving entry of
`columns` (one not itself renamed away), a child `field` (or its
`_truncated`/`_total`), or the `fan_out.field`. `validate` additionally
refuses a name that equals a live column the projection keeps. A name equal
to a rename *key* is fine: that column leaves the document first. At write
time the constant wins.

- `mapping_file` is compared for containment, so a constant it does not name
  gets whatever dynamic mapping infers from the first document; declare it
  there if the type matters.

### Row filters

Not every row of a table belongs in the index. `where` is a predicate in a
restricted SQL subset that decides which rows do:

```toml
[sync.users]
table = "public.users"
where = "status = 'active' AND tenant IN ('eu', 'us') AND deleted_at IS NULL"
```

| form | example |
|---|---|
| comparison, the column always on the left | `status = 'active'`, `tier <> 'free'` (or `!=`), `price > 10`, `<`, `<=`, `>=` |
| null test | `deleted_at IS NULL`, `parent_id IS NOT NULL` |
| membership | `tenant IN ('eu', 'us')`, `kind NOT IN (1, 2)` |
| connectives | `AND`, `OR`, `NOT`, parentheses |
| literals | `'text'` (`''` for a quote), integers, decimals, `true`/`false` |

Keywords are case-insensitive. There are no functions, no `LIKE` and no
column-to-column comparison; anything outside the subset is refused at config
load with a message listing what is supported.

The initial load pushes the predicate into its query — the COPY on PostgreSQL,
the chunk reads on MySQL — so a row that does not match is never read, shipped
or indexed; `resnapshot --where` ANDs with it, and `reconcile` treats a row
that no longer matches as gone. The engine then evaluates the same predicate
on every streamed and polled row: one whose new state matches is written, one
whose new state does not is deleted from the index — every element document of
a fanned row, the id a moved row used to own. That is what makes a row that
leaves the filter disappear and one that enters it appear. The predicate sees
the **raw** row, before projection, so a column that `columns` excludes can
still be filtered on.

- NULL follows SQL: a comparison against NULL is unknown, `NOT` of unknown is
  unknown, and a row matches only when the predicate is TRUE. `IS NULL` also
  matches a column the source did not send.
- Strings compare byte-wise. Equality is exact everywhere; ordering is exact
  for ASCII and ISO 8601, which is what makes `created_at >= '2024-01-01'`
  work against the textual timestamps the sources hand over.
- A number compared against a string holding a number compares numerically:
  `numeric`/`DECIMAL` reach the engine as strings to keep their precision, and
  SQL would compare them as numbers, so `price > 10` matches `10.01`.
- Nothing new is asked of the source: a key-only id renders its delete from
  the key, and non-key ids and `fan_out` already required the before-image.
- `validate` refuses a predicate naming a column the table does not have, and
  runs `SELECT 1 FROM t WHERE (predicate) LIMIT 0` against the live table to
  catch what the grammar cannot, such as a type error.
- Poll mode does not push the predicate into its query, on purpose: a row that
  has left the filter must keep arriving so the engine can turn it into the
  delete it now is. See [Soft deletes](#soft-deletes) for how the two compose.
- The cost, stated plainly: a WAL insert of a row that never matched still
  produces one idempotent delete, which the target answers not-found, and a
  non-matching parent of a child collection produces one such delete per
  child change.
- A filter selects rows; it computes no values. There is still no
  transformation language.

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
first cycle. The predicate is evaluated by the database — poll mode has a query
to put it in — so any boolean expression over the row's own columns works,
`status = 'archived'` as much as a timestamp check.

It is poll-mode only, and configuring it elsewhere is rejected rather than
ignored. The general form is a [row filter](#row-filters):
`where = "deleted_at IS NULL"` works in WAL, binlog and poll mode alike, and
turns the `UPDATE` that marks a row deleted into the delete it means. What
`soft_delete` keeps for poll mode is the database's evaluation, and with it
any expression the grammar of `where` does not accept. The two compose —
`soft_delete` deletes, `where` gates — and naming the same column in both is
redundant rather than wrong.

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

A child's columns are renamed the same way, on the child element rather than
on the parent:

```toml
[[sync.customers.children]]
table = "public.orders"
field = "orders"
foreign_key = "customer_id"

[sync.customers.children.fields]
total = "amount"             # every element of `orders` carries `amount`
```

`<field>_truncated` and `<field>_total` follow the child `field`, not a rename.

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
- MySQL/MariaDB: the child table is streamed from the binlog automatically;
  `binlog_row_image = FULL` (already required) is what lets a child DELETE
  carry its foreign key, so there is no REPLICA IDENTITY caveat.

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
pg2osync_rejected_total                         # documents the target refused, quarantined instead of written
pg2osync_transform_unconverted_total            # values a transform could not convert, indexed as they were
pg2osync_reconnects_total
pg2osync_source_connected                       # 1 while the source is streaming, 0 while reconnecting
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
where = "deleted_at IS NULL"

[sync.users.transform]
email = "redact"
interests = { op = "split", by = "," }

[sync.customers]
table = "public.customers"
index = "customers"
primary_key = "id"

[[sync.customers.children]]
table = "public.orders"
field = "orders"
foreign_key = "customer_id"
```
