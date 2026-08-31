# Configuration reference

One TOML file describes the whole pipeline. Unknown keys are rejected at load
time, so a typo fails immediately instead of silently doing nothing.

You do not have to write it from this page. `pg2osync init --table users` writes
the smallest config that runs, qualifying the table name from the source's own
catalogue and declaring a table with no primary key `append_only`; this
reference is for the options you add afterwards. Every command defaults to
`pg2osync.toml`, which is what `init` writes.

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
| `name` | the file's stem, fitted to the grammar | What this source is called; `A-Z a-z 0-9 _ -` only. See [A directory of configs](#a-directory-of-configs) |
| `url_env` | — | Environment variable holding the connection URL |
| `url` | — | Inline URL; warns as deprecated |
| `sslmode` | from the URL, else `prefer` | `disable`, `prefer`, `require`, `verify-ca`, `verify-full` |
| `sslrootcert` | — | PEM bundle of trusted roots for the verifying modes |
| `sslcert` | — | PEM client certificate chain presented to the server; requires `sslkey` |
| `sslkey` | — | PEM private key for `sslcert`; PKCS#8, RSA or EC, unencrypted |
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
It is also the only one of the two that may sit behind a connection pooler;
the stream cannot, and both must reach the primary — see
[Proxies and connection poolers](proxies.md).

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

`sslcert` and `sslkey` are the other direction: the certificate this process
presents so the server knows who is connecting, for a PostgreSQL `pg_hba.conf`
with `clientcert=verify-full` or a MySQL account declared `REQUIRE X509`. Set
both or neither — half a client identity is refused before anything connects.
The key must be unencrypted, in PKCS#8, RSA (PKCS#1) or EC (SEC1) form; one
file holding both the chain and the key works for both options. They apply to
every connection the process opens: the replication stream, the catalog and
admin queries, and the initial load.

This is orthogonal to `sslmode`. `require` plus a client certificate is a real
combination — encrypt, prove who I am, do not check who you are — and is what a
self-signed managed instance that still demands a client certificate needs. The
URL may carry `sslcert=` and `sslkey=` as well; the config wins per option.

### Poll mode

For managed PostgreSQL instances where logical replication cannot be enabled.
It re-reads rows whose timestamp column advanced since the last cycle.

- **Deletes are invisible.** There is no log to read them from.
- Requires a monotonically increasing timestamp column per table.
- Each start re-runs the initial load: there is no position to resume from, and
  re-indexing is harmless under idempotent writes. Existing WAL checkpoints are
  ignored in this mode so a gap can never be skipped.
- A row whose `id`, index template or [`routing`](#routing) column changed
  leaves its old document behind: a cycle sees the row's new state and has no
  before-image to find the old one by.

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
| `require_alias` | `false` | Refuse any write whose target is an index rather than an alias — see [Rebuilding an index](#rebuilding-an-index) |

Meilisearch has no place to store an arbitrary document, so its checkpoint is a
local file. Give that directory persistent storage, or a restart re-runs the
initial load.

## `[sync.<key>]`

One section per table. `<key>` is the index name when `index` is omitted.

| Option | Description |
|---|---|
| `table` | **Required.** `schema.table` for PostgreSQL, `database.table` for MySQL |
| `index` | Target index or collection; lowercase `[a-z0-9_-]`, not starting with `_` or `.`; several sections may name the same one, see [Sharing an index](#sharing-an-index); may contain `{column}` placeholders, see [Per-row indices](#per-row-indices) |
| `primary_key` | Overrides key detection; also the join column for nested children; contradicts `append_only` |
| `append_only` | The table has no key and only ever gains rows; documents are filed under a content hash, see [Append-only tables](#append-only-tables) |
| `id` | Derived document id, e.g. `tenant-{tenant_id}-{id}`; see [Document ids](#document-ids) |
| `fan_out` | One row becomes one document per element of an array column; see [Fan-out](#fan-out) |
| `join` | This table's place in a join field shared with another section: its relation name and, on the child, the parent column; see [Join fields](#join-fields) |
| `columns` | Only these columns are indexed |
| `exclude_columns` | All columns except these; mutually exclusive with `columns` |
| `transform` | Map of column to an operation, see [Transforms](#transforms) |
| `fields` | Map of source column to target field name; applied last, see [Field names](#field-names) |
| `constants` | Map of field name to a literal value added to every document; `{schema}`/`{table}` in a string render at startup, see [Constant fields](#constant-fields) |
| `where` | Restricted SQL predicate deciding which rows are indexed, e.g. `status = 'active' AND deleted_at IS NULL`; see [Row filters](#row-filters) |
| `poll_column` | Poll mode: overrides `[source] poll_column` for this table |
| `soft_delete` | SQL predicate marking a row as deleted, e.g. `deleted_at IS NOT NULL` |
| `mapping_file` | JSON mapping to create the index with, see below |
| `pipeline` | Ingest pipeline the target runs on every document of this section, e.g. `"embed-products"`; OpenSearch and Elasticsearch only, see [Ingest pipelines](#ingest-pipelines) |
| `routing` | Column whose value decides the shard this section's documents live on, e.g. `"tenant_id"`; OpenSearch and Elasticsearch only, see [Routing](#routing) |
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
rebuild. A table with no key can still be synced insert-only, under a hash of
the row, see [Append-only tables](#append-only-tables). `id` overrides the
shape:

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

### Append-only tables

A table with no primary key can be synced as long as it only ever gains rows
— an event log, an audit trail, a metrics table. Declare it:

```toml
[sync.events_log]
table = "public.events_log"
append_only = true
```

Without a key nothing can say which document a row is, so the document id is
a **content hash**: sha256 of the row's raw values as canonical JSON, hex,
32 characters. The same row hashes the same on the initial load, the stream
and in poll mode, so a replay lands on the document it already wrote — and
two identical rows are **one document**, which is the right answer for an
append-only table. If the table carries a unique column such as an
`event_id`, set `id` and the document is named from it instead.

- An `UPDATE` or `DELETE` on the table **halts the pipeline**:
  `public.events_log: an UPDATE arrived on an append-only table; nothing can
  say which document it is`. There is no document to move or remove, so the
  pipeline stops at that change rather than guess; an append-only table is
  one on which it never arrives.
- `where`, `columns`, `exclude_columns`, `transform`, `fields`, `constants`,
  `index` templates and `pipeline` all work. A row that a `where` filter
  excludes is deleted under its own hash, which is a no-op on the first pass.
- `primary_key` contradicts the declaration and is refused; so are `fan_out`,
  `join`, `[[children]]` and `soft_delete`, each of which needs a key to
  address a document by.
- `reconcile` refuses an append-only table — it pages the index by a key
  column the table does not have. `resnapshot` works and writes the same
  hashes, and so does [`reindex`](#rebuilding-an-index), which checks its
  count as `documents <= rows`: rows the source cannot tell apart are one
  document.
- `init` writes `append_only = true` for a table it finds without a primary
  key, so the generated config runs unedited.

### Sharing an index

An index built before pg2osync is usually a union of several tables, and
several sections may name the same index:

```toml
[sync.users]
table = "public.users"
index = "search"
id = "user-{id}"

[sync.orders]
table = "public.orders"
index = "search"
id = "order-{id}"
```

- **Every section sharing the index declares an `id`.** The default id is the
  row's key, and two tables that both have a row `1` would be one document by
  accident; an explicit template on each section is the declaration that they
  are not. A shared index with a section that omits `id` is refused at config
  load. The templates themselves are not compared: `user-{id}` on both
  sections collides, and that is the operator's own declaration — nothing
  checks the values, because nothing can see them.
- **At most one of the sections sets `mapping_file`.** An index is created
  once; a second section describing it is refused.
- **`reconcile` refuses a shared index.** It pages the index by one table's
  key column and cannot tell one table's documents from another's, so every
  other table's documents would be reported as orphans — and removed by
  `--delete`.
- **`TRUNCATE` on any of the tables is not applied.** Clearing the index
  would wipe the tables the source never truncated, and halting would replay
  the same `TRUNCATE` from the slot at every restart with nothing to change to
  get past it — so the truncated table's documents are left in place, the
  pipeline logs `TRUNCATE not applied to index search, which other tables also
  feed; its documents are left in place` and counts a `truncate_skipped` event
  in `pg2osync_events_total`. Clear them by hand, or give the table an index
  of its own. A [join pair](#join-fields) is different: its halves are told
  apart by the join field, so a truncate there clears one relation exactly.
- **`resnapshot` works** on any one of the tables for the same reason: it
  writes by id and touches nothing else in the index.
- **[`reindex`](#rebuilding-an-index) refuses a shared index,** including a
  join pair's. It reads one table, so the fresh index it built would hold
  that table's documents and nothing else — and the alias would then hide the
  others. Rebuild a shared index with a second instance of the whole config.

A [join pair](#join-fields) is the other way two sections share an index:
there the join field scopes every document to its relation, which is why
`reconcile` can check either half of the pair against its own table.

### Per-row indices

`index` may carry `{column}` placeholders, so each row chooses the index it
lands in. Two shapes cover most of what this is for. Time-based retention,
where an old month is dropped as one index instead of deleted row by row:

```toml
[sync.events]
table = "public.events"
index = "events-{created_month}"    # a column holding e.g. 2026-08
```

And per-tenant isolation, where every tenant is searched, sized and secured
on its own:

```toml
[sync.events]
table = "public.events"
index = "{tenant}-events"
```

The rules are the [`id`](#document-ids) rules, because a name derived from a
column is the same problem as an id derived from one: the column can change,
and the document is then in the old index.

- **Same grammar, same row.** Literals plus `{column}` placeholders, rendered
  from the row's raw values — before projections and transforms — exactly as
  `id` is. Every placeholder must name a column of the table, and `validate`
  checks that against the catalogue. A fanned row's element documents all go
  where the row goes, so the template may not name the `fan_out` column.
- **A rendered name that is not a legal index halts the pipeline.** An
  uppercase letter, an empty value, a NULL in a named column: none of these
  can become an index the target accepts, so the pipeline stops and names
  the template, the column and the value it rendered. `validate` warns up
  front for nullable columns.
- **Non-key columns need the before-image.** A template naming only key
  columns works everywhere. One naming a column outside the key needs the
  old row to find the index a changed row was in, so on PostgreSQL the table
  must be `REPLICA IDENTITY FULL`; `run` refuses to start otherwise. MySQL
  already guarantees it (`binlog_row_image = FULL`).
- **The index is created on demand,** at the first document that needs it,
  with the section's `mapping_file` if one is set. Nothing is created at
  startup, because the set of indices is not known until the rows are.
- **A template must have a literal part,** and may not overlap another
  section's index or be shared. Each placeholder stands for `*` in what a
  `TRUNCATE` clears, so `index = "{tenant}"` — a claim on the whole cluster —
  is refused at config load; so is a template whose pattern also matches an
  index another section writes to, and a template two sections name.
- **`TRUNCATE` clears the pattern.** Every index the template claims is
  searched, and each hit is deleted under its own index as a versioned write,
  so a row committed after the truncate is not swept away with it.
- **A row that changes its index-choosing column moves.** It is written in
  the new index and deleted from the old — the same move `id` makes for a row
  whose id changed.
- **`reconcile`, `switch-alias` and `reindex` refuse a templated table.**
  Reconcile pages one index by its key column, and the table's documents are
  spread over every index the template renders; an alias points at one index,
  and so does a rebuild. `resnapshot` works.
- **Meilisearch refuses a template** at startup: it has no mappings to
  create an index with.
- **Bulk-load settings are not relaxed** for a templated index. An index
  created during the initial load takes the target's defaults.

### Rebuilding an index

A mapping cannot be changed on an index that already exists, so changing one
means building a new index and moving the traffic to it. `reindex` is that in
one command:

```sh
pg2osync reindex -c pg2osync.toml --table public.users --alias users
```

It creates `users-<unix seconds>` with the section's `mapping_file`, loads the
table into it, compares what it wrote against the source's row count, and
points the alias at the new index in one atomic request. `--drop-old` deletes
the index the alias came off; by default it is kept, because it is the
rollback — one alias flip away.

- **Stop the pipeline first; the command refuses to run beside it.** A
  re-snapshot is safe beside the stream because a copied row and a streamed
  change meet in the same index and the higher position wins. A fresh index the
  stream is not writing to has no second document to compare against, so a row
  that changed during the rebuild would be wrong there for good — and the count
  would still add up. The refusal is evidence, not a flag: an active
  replication slot on PostgreSQL, and on either source a checkpoint seen
  moving. There is no `--force`.
- **The checkpoint does not move,** by construction: the rows carry position
  `0`, exactly as a re-snapshot's do. So everything committed while the
  pipeline was stopped is still in the log, and the restart replays it into the
  new index. That is also what proves the *contents*: the count only proves how
  many.
- **Two follow-ups are yours,** and the command prints both: set `index` to the
  new name in the section, and start the pipeline again.
- **A count the source does not explain leaves the alias where it is.** The
  source is counted before and after the load; anything outside that range is
  reported with all three numbers and a non-zero exit, and the rebuilt index is
  left for you to look at.
- **Refused for** a [templated](#per-row-indices) index, a
  [shared](#sharing-an-index) index or a join pair, a [fanned](#fan-out)
  table, and an `--alias` equal to the index the section already writes to.
- **On Meilisearch the alias *is* the index,** so `--alias` there must be the
  name the section already writes to and every other value is refused. There is
  no alias namespace on that target; the rebuilt index is swapped into the live
  name with `POST /swap-indexes` instead, which is atomic in the same way. Two
  things differ afterwards: no config edit is needed, only the restart, and it
  is `<index>-<unix seconds>` that ends up holding the documents from before the
  rebuild — see [the Meilisearch sink](sinks/meilisearch.md#rebuilding-an-index).
- **A live cutover with no freshness gap at all is still two instances,** as
  [operations.md](operations.md) describes. A rebuild trades the gap for one
  command.

#### Keeping every write behind the alias

The follow-up that is yours — set `index` to the new name — is also the one
that goes wrong quietly. A section left pointing at the raw index keeps
writing, keeps its checkpoint moving and never says anything; the alias simply
stops seeing new rows, and the first sign of it is a search result that is
weeks stale.

`[target] require_alias = true` makes that impossible. Every bulk action then
tells OpenSearch or Elasticsearch that its target has to be an alias, and a
write to a plain index comes back refused. The refusal is a **permanent**
rejection — nothing about a name that is an index and not an alias changes on
a retry — so the pipeline halts, or quarantines, exactly as it does for a
document the mapping refuses:

```
orders is not an alias; with require_alias every write must go through one
```

```toml
[target]
url = "http://localhost:9200"
require_alias = true

[sync.orders]
table = "public.orders"
index = "orders_live"   # the alias, never the index it resolves to
```

`pg2osync validate` checks the same thing before the first batch, so the
misconfiguration is caught by the command you already run after editing the
config rather than by a halted pipeline an hour later:

```
✓ require_alias: every configured index is an alias
```

Two configurations are refused outright rather than left to fail per write:
Meilisearch, which has no alias namespace at all — the live name there *is* an
index uid — and a [templated](#per-row-indices) index, whose per-row index is
created on demand and is therefore always a raw one.

The target enforces the flag on writes that create or replace a document.
A delete against a plain index is not refused by either engine, so the option
is a guard on the writes that make an index diverge, not an access control.

**A rebuild needs the flag off for the duration.** With `require_alias` the
section names the alias, and `reindex` needs two names: a fresh index to fill
and a separate one to point at it. So unset the option for the rebuild — the
pipeline is stopped for it anyway — and afterwards leave `index` naming the
alias rather than the new index the command prints, then set the option again.
The alias now resolves to the rebuilt index and nothing in the section changed,
which is the drift the option existed to prevent, gone at the source.

### Retention

An index per month is only half of time-based retention: something still has
to delete August once nothing searches it any more. pg2osync does not, and
neither target needs it to — both have an index-lifecycle feature that acts on
an index pg2osync created on demand without pg2osync knowing anything about
it. The policy is one PUT the operator makes once; the target owns the
lifecycle of its own indices, and a sync tool that also deleted them would be
a second, weaker copy of a scheduler that is already there.

**Elasticsearch: name an ILM policy in the mapping's `settings`.** The
`mapping_file` is the index-creation body, `settings` included, and it is sent
verbatim for every index the template renders — so the policy is attached to
`events-2026-08` at the moment the first August row creates it, and to
`events-2026-09` a month later, with no template to maintain:

```json
{
  "settings": {
    "index.lifecycle.name": "events-30d"
  },
  "mappings": {
    "properties": {
      "created_at": { "type": "date" }
    }
  }
}
```

The policy itself is created once, by hand:

```json
PUT _ilm/policy/events-30d
{
  "policy": {
    "phases": {
      "delete": {
        "min_age": "30d",
        "actions": { "delete": {} }
      }
    }
  }
}
```

`index.lifecycle.name` is all that is needed here: `min_age` counts from the
index's creation date for an index that never rolls over, which is exactly
what a month bucket is. `index.lifecycle.rollover_alias` belongs to the
`rollover` action — a write alias moving from one index to the next — and a
row-chosen index has no such alias: the row's own column says where it goes.
Setting it without a rollover action makes the policy fail on an index it
cannot roll over. (See
[ILM index settings](https://www.elastic.co/docs/reference/elasticsearch/configuration-reference/index-lifecycle-management-settings)
and the [delete action](https://www.elastic.co/docs/reference/elasticsearch/index-lifecycle-actions/ilm-delete).)

**OpenSearch: match the indices from an ISM policy.** ISM attaches itself. A
policy carrying an `ism_template` is applied to every index created after it
whose name matches one of the template's patterns, so nothing goes in
`mapping_file` at all:

```json
PUT _plugins/_ism/policies/events-30d
{
  "policy": {
    "description": "Delete an events index 30 days after it was created.",
    "default_state": "hot",
    "states": [
      {
        "name": "hot",
        "actions": [],
        "transitions": [
          { "state_name": "delete", "conditions": { "min_index_age": "30d" } }
        ]
      },
      {
        "name": "delete",
        "actions": [{ "delete": {} }],
        "transitions": []
      }
    ],
    "ism_template": [
      { "index_patterns": ["events-*"], "priority": 100 }
    ]
  }
}
```

The legacy way of attaching a policy — an index template setting named
`plugins.index_state_management.policy_id` (`opendistro.` before that) — is
deprecated in favour of `ism_template`. It still works on the 2.x line, but
it needs an index template to carry the setting, which is the maintenance the
`ism_template` field removes. (See
[Index State Management](https://docs.opensearch.org/latest/im-plugin/ism/index/)
and the [ISM API](https://docs.opensearch.org/latest/im-plugin/ism/api/).)

Both mechanisms have the same two edges:

- **The policy has to exist before the index does.** An ISM template is
  consulted at index creation only, and an ES index created before the policy
  existed carries no `index.lifecycle.name`. Indices already in the cluster
  are attached by hand — `POST _plugins/_ism/add/events-2026-07` on
  OpenSearch, `PUT /events-2026-07/_settings` with `index.lifecycle.name` on
  Elasticsearch — and every index created from then on is covered.
- **Nothing deletes on the stroke of the boundary.** ILM checks its indices
  every `indices.lifecycle.poll_interval` (10 minutes by default), ISM on its
  own job schedule; an index outlives its `min_age` by that much.

Keep the pattern narrow enough to miss pg2osync's own state. `events-*` is
fine; a policy matching `*` would also claim the hidden `.pg2osync_meta`
checkpoint index and eventually delete the position the pipeline resumes
from.

**Data streams are not supported.** A data stream accepts only `create`
actions in a bulk request, and pg2osync writes `index` actions: every document
is keyed by its id and rewritten, because at-least-once delivery means a
replayed change has to overwrite the document it already wrote rather than be
rejected or duplicated. That holds even for an
[`append_only`](#append-only-tables) table, whose content hash exists precisely
so that a re-delivered row lands on the same document again. A time-bucketed
index with a lifecycle policy is what a data stream would have given here
anyway: whole indices dropped by age, never documents deleted one at a time.

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
  documents. [`reindex`](#rebuilding-an-index) refuses one too — it checks
  what it wrote against the source's row count, and here one row is many
  documents.

Two tables may map to the same index once each declares its `id`; see
[Sharing an index](#sharing-an-index).

### Transforms

A column can be reshaped on its way into the document. `transform` maps a
source column to one of eight named operations: a string for an op that takes
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
status  = { op = "lookup", map = { "1" = "active", "2" = "closed" }, default = "unknown" }
ssn     = { op = "pseudonym", key_env = "PG2OSYNC_PSEUDONYM_KEY" }
```

`hash` replaces the value with a truncated SHA-256 digest, stable across runs so
it can still be grouped on. `redact` replaces it with `***`. `pseudonym` is
[keyed and reversible](#pseudonym). `lookup` maps a value through a dictionary
the configuration declares. The other four turn a string into something more
structured:

| op | takes | turns | into |
|---|---|---|---|
| `hash` | — | any value | a truncated SHA-256 digest |
| `redact` | — | any value | `***` |
| `pseudonym` | `key_env`, required and non-empty; `scope`, optional | a string, number or bool | a deterministic base64url token under an AES-SIV key |
| `json` | — | a string holding JSON | that JSON value, an object or a bare number alike |
| `split` | `by`, required and non-empty | a delimited string | an array of its trimmed, non-empty pieces: `"a, b ,c"` → `["a","b","c"]`, `""` → `[]` |
| `number` | — | a string holding a number | a JSON number: an integer when it is one, otherwise a double |
| `date` | `from`, a `strptime`-style format, required and non-empty | a string in that format | ISO 8601: `YYYY-MM-DD` for a date, `YYYY-MM-DDTHH:MM:SS` for a date-time, RFC 3339 with the offset kept when the format carries one |
| `lookup` | `map`, required and non-empty; `default`, optional | a value whose text form is a key of `map` | that key's label; a value the map does not name keeps its own, or becomes `default` |

NULL is left alone by every op, and so is a value already in the target shape:
a parsed `json`/`jsonb`/`JSON` column under `json`, an array under `split`, a
number under `number`. That is what keeps the ops idempotent when
at-least-once delivery replays a row, and it is why the three exist for
*text* columns that hold something more structured. `number` is also the
explicit opt-out of the rule that `numeric`/`DECIMAL` arrive as strings to
keep their precision — for an index that sorts or range-queries on the value
and accepts the double.

`lookup` compares the value's text form against the keys, which are strings:
the number `1`, the string `"1"` and — through `"true"`/`"false"` — a boolean
all find their key, while an array or an object has no text form and misses.
A miss is a value the dictionary does not cover, so it is counted like any
other unconverted value, whether it keeps its own value or takes `default`.

A value a *reshaping* op cannot convert — `"abc"` under `number`, a code no
`lookup` map names — is indexed **exactly as it arrived** (or as `default`),
counted in
`pg2osync_transform_unconverted_total`, and logged once per table and column.
`pseudonym` is the exception: it is protective, so a value it cannot render is
replaced with `***`, counted the same way and logged as redacted. Indexing the
value that op was asked to hide would be worse than losing it.
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
without a non-empty `by`, `date` without a non-empty `from`, `lookup` without a
non-empty `map`, `pseudonym` without a non-empty `key_env`, and a transform on
the `fan_out.field`. Refused at start-up: a `pseudonym` whose `key_env` names a
variable that is unset or does not hold a well-formed key.

#### Pseudonym

`hash` is one-way and truncated: two values can collide, so it is unsafe on a
unique or a foreign-key column, and nobody can get the value back. `pseudonym`
encrypts the value with AES-SIV (RFC 5297) instead, which is deterministic — so
equal values give equal tokens, across tables and across runs, and a join on the
pseudonymised column still joins.

```toml
[sync.users.transform]
email   = { op = "pseudonym", key_env = "PG2OSYNC_PSEUDONYM_KEY" }

[sync.orders.transform]
user_id = { op = "pseudonym", key_env = "PG2OSYNC_PSEUDONYM_KEY", scope = "public.users.id" }
```

**The key** comes only from the environment: `key_env` names a variable holding
**128 hex characters** — a 64-byte AES-256-SIV key. There is no inline form, and
the key never appears in a log, an error or a dump of the configuration.

```sh
export PG2OSYNC_PSEUDONYM_KEY=$(openssl rand -hex 64)
pg2osync validate -c pg2osync.toml     # ✓ pseudonym key present (64 bytes) from PG2OSYNC_PSEUDONYM_KEY
```

**The scope** is the associated data, and it defaults to the column's own
`schema.table.column`, so the same value in two columns gives two different
tokens and one cannot be replayed into another context. That default is fully
qualified, which is the trap a foreign key has to step around: for
`orders.user_id` to produce the same token as `users.id`, it must name that
column's scope explicitly, exactly as spelled above. Two columns that must join
have to carry the same `scope`.

**The construction**, for whoever holds the key and needs the value back:

```
token = base64url_nopad( AES-SIV-Encrypt(K, headers = [scope], plaintext) )
  K:         64 bytes — the 128 hex characters. RFC 5297 order: K[0..32] is the
             CMAC key, K[32..64] the CTR key. Some libraries take the halves the
             other way round; check yours against a test vector.
  headers:   exactly one item, the scope string as UTF-8
  plaintext: the value's text as UTF-8 — the string itself, or "123" / "true"
             for a number or a bool, never JSON-quoted
  output:    the 16-byte synthetic IV, then the ciphertext (16 + len bytes)
```

There is deliberately no `decrypt` subcommand: it would want the key on a
command line and the scope beside it, and the four lines above are all any RFC
5297 library needs.

**What it does and does not protect.** This is pseudonymisation, not
anonymisation. It is reversible by the key holder, so under GDPR the index still
holds personal data — keep the index and the key in different blast radii,
because together they are the plaintext. Being deterministic is the point and
also the cost: equal values are visibly equal, so a low-cardinality column
(a country, a status, a gender) is de-anonymised by counting. Use it on
high-cardinality identifiers. The token is not padded either, so its length
reveals the plaintext's. Where none of that is acceptable and no one ever needs
the value back, `hash` and `redact` remain.

**Where it does not reach.** `_id` renders from the raw row, before transforms,
so a pseudonymised column still appears in plaintext in the document id if `id`
names it — pseudonymise the field and keep the identifier off it, or exclude the
column. `where` and `fan_out` read the raw row for the same reason. And a
TOAST-completed column is copied from the stored document rather than
transformed again, the same rule that keeps `hash` stable.

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
names or targets a child `field` (or its `_truncated`/`_total`, which a
`single` child does not write and so does not claim). `validate`
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
`_truncated`/`_total`, which a `single` child does not claim), or the
`fan_out.field`. `validate` additionally
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

### Ingest pipelines

A vector field is the one thing a mapping can declare that no row can fill:
the embedding has to be computed by something that holds the model. pg2osync
does not; the target does. `pipeline` names an ingest pipeline on the target,
and every document the section writes carries it on its bulk action, so the
target runs the pipeline's processors on the way in:

```toml
[sync.products]
table = "public.products"
index = "products"
mapping_file = "products-mapping.json"
pipeline = "embed-products"
```

The pipeline is yours to create, before the first document lands. A
`text_embedding` processor (OpenSearch's neural-search plugin; Elasticsearch
has the `inference` processor) reads a text field and writes the vector into a
field the mapping declares as `knn_vector`:

```json
{
  "mappings": {
    "properties": {
      "name":        { "type": "text" },
      "description": { "type": "text" },
      "embedding":   { "type": "knn_vector", "dimension": 384 }
    }
  },
  "settings": { "index.knn": true }
}
```

`pipeline = "embed-products"` is the whole of pg2osync's part; the model, the
processor and the `dimension` above belong to the pipeline and the mapping,
and a `set` processor or any other works the same way.

`validate` asks the target for the pipeline (`GET _ingest/pipeline/<name>`)
and refuses a name it does not have, because every document would otherwise
be rejected at the first write, with the pipeline named but the config already
running. A pipeline that exists is reported by name, one line per section.

Three things follow from the pipeline riding on the operation rather than on
the index:

- **It is per section, not per index.** Two tables feeding one index (see
  [Sharing an index](#sharing-an-index)) may name different pipelines, so each
  embeds its own columns; a section without `pipeline` writes to the same
  index with none.
- **A delete carries no pipeline.** Ingest pipelines run on index actions
  only, which is also what the target does.
- **A quarantined document is replayed through the pipeline again.** The
  record kept by `on_permanent_rejection = "quarantine"` stores the pipeline
  with the operation, so `pg2osync rejects --replay` submits it the way it
  was first submitted, and the document does not land without its vector.

Meilisearch has no ingest pipelines; `pipeline` is refused for that target at
config load rather than ignored.

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
# single = true              # optional: a 1:1 relation, see below
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

A child collection is projected the same way as a section, with `columns` or
`exclude_columns` on the child rather than on the parent:

```toml
[[sync.customers.children]]
table = "public.orders"
field = "orders"
foreign_key = "customer_id"
exclude_columns = ["internal_notes"]   # every element leaves this column out
# columns = ["id", "total"]            # or list what to keep — not both
```

The two are mutually exclusive, as on a section, and an empty `columns` list is
refused. The projection happens **in the read**: the initial load and the
per-transaction re-fetch are built from the same expression, so they cannot
embed different shapes, and PostgreSQL never reads a column the element does not
name. `fields` runs after the projection and names the source column, so a
column that is excluded — or left out of `columns` — cannot also be renamed;
that is refused at startup rather than silently dropping the rename. The
`foreign_key` is kept only if you list it: it is read as its own column beside
the element, so leaving it out of the array changes nothing but the array.

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
- **A `TRUNCATE` of a child table — or of a junction — is not applied to the
  parents.** A truncate clears the index of the table it names, and a child has
  no index of its own; the parents keep the arrays they had until something
  else changes them. `pg2osync resnapshot --table public.customers` rebuilds
  them.

#### Many-to-many through a junction table

A books ↔ authors relation lives in a third table, and the rows worth embedding
are one table further than the one carrying the parent's key. `through` names
that junction:

```toml
[sync.books]
table = "public.books"
index = "books"

[[sync.books.children]]
table   = "public.authors"      # the rows that get embedded
field   = "authors"
through = "public.book_author"  # the junction
foreign_key = "book_id"         # junction column referencing the PARENT key
through_key = "author_id"       # junction column referencing the CHILD key
```

```json
{ "id": 1, "title": "…", "authors": [ { "id": 7, "name": "…" } ] }
```

`foreign_key` keeps its meaning — the column that names the parent — and only
changes its home: with `through` set it is a column of the junction, not of the
child. Everything else on a child works unchanged: `fields`, `columns`,
`exclude_columns`, `max_rows` with its `_truncated`/`_total` fields, and
`single` for a one-to-one relation that happens to be recorded in a junction.
The junction contributes no field of its own, and this is still one level deep.

- The aggregation is the same one, with one join added, so the initial load and
  a streamed re-fetch cannot embed different arrays.
- **Both tables are watched**: the junction and the child are added to the
  publication (PostgreSQL) or the streamed set (MySQL/MariaDB) automatically.
  A junction row is what makes or breaks the relation; a child row changes what
  is embedded.
- **Index the junction on both columns.** A primary key of
  `(book_id, author_id)` covers the aggregation; add an index led by
  `author_id` as well, because a changed *child* row is looked back up by it.
  `validate` says so when it is missing.
- A changed child row costs one extra small query per collection per
  transaction — a single `SELECT DISTINCT` over the junction for all of that
  transaction's changed children — and then the parent is read and its
  collections aggregated exactly once, as for any other change.
- PostgreSQL: the junction needs `foreign_key` in its replica identity for a
  junction DELETE to find the parent, which the usual `(book_id, author_id)`
  primary key already gives it; only a junction keyed some other way needs
  `REPLICA IDENTITY FULL`, and pg2osync warns at startup when that is the case.
  The **child** table needs nothing: it is located by its own primary key.
- The child needs a single-column primary key, refused at startup otherwise:
  it is what `through_key` points at.
- A duplicate `(book_id, author_id)` pair embeds the author twice. Give the
  junction a unique key on the pair.
- One junction table belongs to one relation: two sections naming the same
  junction for different parents are refused, because a streamed junction row
  could not say which parent it meant.

#### A one-to-one relation

A `users` → `profiles` relation is one child, and an array of one makes every
query and every mapping carry an index that is always zero. `single = true`
embeds the element itself:

```toml
[[sync.customers.children]]
table = "public.profiles"
field = "profile"
foreign_key = "customer_id"
single = true
```

```json
{ "id": 42, "profile": { "customer_id": 42, "bio": "..." } }
```

The field is always present: a parent with no child gets `"profile": null`, so a
query need not know whether this parent happens to have one. `fields`, `columns`
and `exclude_columns` work exactly as they do on an array child.

**Map the field as `object`, not `nested`.** `nested` exists to keep the
elements of an *array* from being flattened into one another; there is no array
here, so it buys nothing and `index.mapping.nested_objects.limit` — the whole
reason the array form has a cap — does not apply.

- `max_rows` is refused with `single`: a relation declared one-to-one has
  nothing to cap. `<field>_truncated` and `<field>_total` are never written, and
  the two names are free for a column, a constant or a rename.
- The child table needs a primary key, refused at startup otherwise: with no
  order there is no first row, so two runs could embed different ones.
- A **second** matching row does not fail the run — a duplicate that exists for
  the length of a migration must not halt an index. The lowest-keyed row is
  embedded, which is the same one a re-snapshot picks, and each batch logs one
  warning naming the collection, how many parents matched twice and the worst of
  them. Fix the data, or drop `single`, and the next change to those parents
  rewrites them.

### Aggregates

When only the *number* of related rows matters, embedding every row to count it
client-side is waste and leaving it out forces a second query at search time.
An aggregate embeds the number itself, kept live by the same machinery the
embedded children use:

```toml
[sync.contacts]
table = "public.contacts"
index = "contacts"
primary_key = "id"

[[sync.contacts.aggregates]]
field       = "open_deals"       # field on the parent document
table       = "public.deals"     # the table whose rows are counted
foreign_key = "contact_id"       # column on THAT table naming the parent
op          = "count"            # the only operation; the default
where       = "status_type = 1"  # optional: which rows count
```

```json
{ "id": 42, "name": "…", "open_deals": 3 }
```

An aggregate is one more shape of child, not a computation: `children` embeds a
child table's rows, an aggregate embeds a single number derived from them. There
is no expression language — a fixed operation over one foreign key, narrowed by
the [row-filter subset](#row-filters), the same predicate `[sync.x] where`
takes and rendered through the same parser for both dialects.

- PostgreSQL and MySQL/MariaDB alike.
- `count` is the only operation. `sum`, `min` and `max` are a change of their
  own, when somebody asks for one; an `op` that is not `count` is refused at
  startup, naming what is supported.
- **A parent no row matched carries `0`, never a missing field**, on every path
  — otherwise `open_deals: 0` could not be queried for at all.
- The initial load joins one grouped read per aggregate, so it costs one query
  per table however many parents there are.
- **Streamed changes cost one grouped query per aggregate per transaction.**
  The aggregated table is watched exactly as a child table is — added to the
  publication (PostgreSQL) or the streamed set (MySQL/MariaDB) automatically —
  and a changed row names the parent to count again rather than becoming a
  document. **Index the foreign key on that table.**
- An INSERT, a DELETE and an UPDATE all name the parent, so a row entering or
  leaving `where` moves the number. An UPDATE that moves a row to another
  parent names **both**: the parent it left is as wrong as the one it joined.
- PostgreSQL: **give the aggregated table `REPLICA IDENTITY FULL`**
  (`ALTER TABLE public.deals REPLICA IDENTITY FULL`) unless `foreign_key` is
  part of its primary key. Without it a DELETE carries no foreign key and an
  UPDATE carries no before-image, so the parent — or the parent the row left —
  cannot be located; pg2osync warns at startup with the `ALTER` to run.
  MySQL/MariaDB need nothing: `binlog_row_image = FULL` is already required and
  carries both images.
- The field name must not collide with another aggregate, a child's `field`, a
  constant, a rename target or the join field; each is refused when the config
  is read. A collision with a *column* of the parent is refused by the initial
  load, which is where the catalogue is known — as for a child's `field`.
- An `append_only` parent is refused — a changed row of the aggregated table
  names the parent to count again *by its key*, which such a table has none of
  — and so is a `fan_out` parent, whose row is not one document to count for.
- The table an aggregate counts may have a `[sync]` section of its own, exactly
  as a child table may — with the same caveat, which pg2osync says at startup:
  its rows are read as a re-count of the parent, so its own index receives the
  initial load and no streamed change.
- Map the field as a `long`. Nothing else about the target changes: it is one
  more number on the document.
- A `TRUNCATE` of the aggregated table is not applied to the parents, exactly as
  for a child table; `pg2osync resnapshot --table public.contacts` rebuilds them.

### Join fields

The embedded array above is one document and one write, and it is the right
choice nearly always. When the children are many, change far more often than
the parent, or have to be searched in their own right, a join field keeps each
child a document of its own — on its parent's shard, so `has_child` and
`has_parent` queries work — instead of re-fetching the whole collection on
every child change:

```toml
[sync.customers]
table = "public.customers"
index = "shop"
id = "customer-{id}"
mapping_file = "shop-mapping.json"

[sync.customers.join]
field = "relation"           # the join field the mapping declares
name = "customer"            # this table's relation name inside it

[sync.orders]
table = "public.orders"
index = "shop"
id = "order-{id}"

[sync.orders.join]
field = "relation"
name = "order"
parent = "customer_id"       # column on THIS table holding the parent's key
```

`parent` is what makes a section the child: it names the column whose value,
rendered through the parent section's `id`, is the parent document's id — and
the child's routing. The parent omits it. Each document carries the join field
in the shape the target expects, `"customer"` on a parent and
`{"name": "order", "parent": "customer-1"}` on a child. It is written after
projection and renames, like a constant, so nothing can strip it or move a
document to another shard. OpenSearch and Elasticsearch only.

- **The mapping lives on the parent, and only there.** A join pair is two
  sections and one index; the parent's `mapping_file` creates it and has to
  declare the field:

  ```json
  { "mappings": { "properties": {
      "relation": { "type": "join", "relations": { "customer": ["order"] } } } } }
  ```

  A child that sets `mapping_file` is refused. Dynamic mapping cannot invent a
  join field, so without this mapping — or an index template that declares
  the field — the first document is rejected.
- **Ids must be unique across the shared index.** Parent `customers.id = 1`
  and child `orders.id = 1` would both render `_id = "1"`, and the child would
  overwrite the parent. Configuration cannot see it; give each section an `id`
  with its own prefix, as `customer-{id}` and `order-{id}` above.
- **The parent's `id` may name only its key.** The child holds one column and
  has to compute the parent's id from it alone, so an id naming anything else
  is refused at load, and a parent with a composite key at startup.
- **PostgreSQL: the child needs `REPLICA IDENTITY FULL`** unless its parent
  column is part of its own key. A delete has to reach the shard that holds
  the document, and the routing comes from the old row — the same rule a
  non-key `id` follows; `run` refuses to start otherwise. A child whose parent
  column changes moves: written under the new parent, deleted under the old.
  MySQL already guarantees the before-image.
- **A parent delete cascades.** The engine does not know which children the
  target holds, so the sink refreshes the index and searches for them, after
  the parent's own delete and at the parent's position. The refresh is what
  makes it correct — a child written seconds earlier would otherwise survive —
  and it is the cost: one per deleted parent, which the batch waits for. Each
  batch that carries one counts a `join_cascade` event in
  `pg2osync_events_total`.
- **`TRUNCATE` on either table clears its relation only.** The join field
  tells the halves apart, so a truncate of the orders removes every `order`
  document — routed to its parent's shard — and leaves the customers, unlike
  a [plain shared index](#sharing-an-index), where nothing can tell one
  table's documents from another's. PostgreSQL makes you
  truncate tables that reference each other together anyway, so usually both.
- **A `where` filter is the operator's to keep consistent across the pair.**
  A child whose parent the parent section's `where` filters out is indexed
  with a `parent` that no document has.
- A NULL in the parent column halts the pipeline, as a NULL in an `id` column
  does; `validate` warns when the column is nullable, and refuses a column
  the table does not have.
- `reconcile` scopes its scan to one relation, so it checks either half of
  the pair against its own table and routes the deletes it makes.

Refused at config load: two sections on one index without `join` on every one
of them and without an `id` on every one of them (see
[Sharing an index](#sharing-an-index)); sections of one index naming
different join fields; an index with no
parent, or with two; two sections sharing a relation name; a child that only
names a parent no other section provides; a child with `mapping_file`;
`fan_out` together with `join`; a parent `id` naming a column outside its key;
`join` against Meilisearch, which has no parent-child model; a `fields`
rename, a constant, a `columns` entry or a `[[children]]` field that collides
with the join field. `[[children]]` on a join child is allowed: embedding an
array on this document and filing this document under a parent are unrelated.
A table that is both an embedded child of another section and a section of
its own is warned about at startup, not refused: the replication runner reads
its rows only as a re-fetch of the owner, so its own index receives the initial
load and no streamed change.

### Routing

A document's shard is chosen from its `_id` unless something says otherwise.
`routing` says otherwise: it names a column whose value decides the shard, so
every document sharing that value lands on one shard and a query for it reads
one shard instead of all of them.

```toml
[sync.documents]
table = "public.documents"
index = "documents"
routing = "tenant_id"
```

A tenant column is the usual case: hundreds of small tenants in one index,
each query scoped to one of them. An index per tenant would be the
alternative, and it is the wrong one when tenants are many and small — every
index costs shards, and shards cost memory whether they hold ten documents or
ten million.

- **The value is the column's raw value**, read before projection and
  transforms, like an `id` is: a projection must not be able to move a
  document to another shard. The column stays an ordinary field of the
  document; routing adds nothing to it.
- **NULL, missing, or empty halts the pipeline.** The target rejects an empty
  routing outright, and quietly writing the document to its default shard
  would hide it from every routed query. `validate` refuses a column the
  table does not have and warns when it is nullable.
- **PostgreSQL: a routing column outside the key needs
  `REPLICA IDENTITY FULL`.** A delete has to reach the shard that holds the
  document, and the old value comes from the before-image — the same rule a
  non-key `id` follows; `run` refuses to start otherwise. MySQL already
  guarantees the before-image with `binlog_row_image = FULL`.
- **A changed value moves the document**: written under the new routing
  first, deleted under the old second, exactly as a changed `id` or a changed
  index template moves it.
- **Fanned-out elements inherit the row's routing**, and a row that changes
  its routing takes them all with it.
- **A routing column that is both projected away and TOASTable halts on an
  update that does not resend it**, like a non-key `id` column: the read-back
  fills the document, not the row the routing renders from.
- **`reconcile` and `TRUNCATE` are unaffected.** Both work index-wide and
  take each document's routing from the hit itself, so neither has to derive
  one. A document duplicated under a stale routing is not something reconcile
  collects: the row it belongs to is still there.
- **Poll mode leaves the old copy.** A poll cycle sees the row's new state
  and nothing else, so a changed routing value writes a second copy under the
  new routing and never deletes the first — the same limitation a changed
  `id` has in poll mode.

Refused at config load: `routing` together with `join`, which already routes
a child to its parent's shard, and `routing` against Meilisearch, which
ignores routing entirely.

## `[engine]`

Defaults are production-sane; tune only against measurements.

| Option | Default | Description |
|---|---|---|
| `batch_size` | `500` | Rows per sink request |
| `batch_max_bytes` | `10485760` | Approximate byte ceiling per request; whichever limit hits first splits the batch |
| `write_concurrency` | `1` | Write requests open against the target at once. One at a time is what the initial load is limited by, not the source read; raising it multiplies the load on the target, and it needs a target that orders by document version, so Meilisearch refuses anything above 1 |
| `load_max_rows_per_sec` | unset | Ceiling on how many rows a second the initial load, a re-snapshot and a rebuild take in. Unset means unlimited; `0` is refused. Load rows only — the stream is never held back |
| `txn_buffer_cap_mb` | `256` | Warning threshold for one open transaction |
| `retry_max` | `10` | Attempts per request before the pipeline stops |
| `retry_backoff_ms` | `500` | Initial backoff, doubled per attempt, capped at 30 s |
| `retry_max_elapsed_ms` | — | Ceiling on the time one request spends being retried, measured from its first failure. Unset leaves the attempt count as the only limit; `0` is refused |
| `checkpoint_interval_ms` | `500` | How often the position is persisted |
| `on_permanent_rejection` | `"halt"` | `"halt"` stops the pipeline on a document the target will never accept. `"quarantine"` records it in a hidden `.pg2osync_rejects` index, with its position, and carries on |
| `max_rejects` | `100` | Quarantined documents allowed before the pipeline halts anyway. Counted against what the store holds, so a restart does not reset it |

`checkpoint_interval_ms` is the ceiling on replayed work after a crash: a lower
value means less replay and more writes to the target.

`load_max_rows_per_sec` is the way to be gentle with a production primary
without waiting for the night. It is a token bucket in front of the engine's
intake of load rows, refilled every second and holding at most one second of
allowance, so an idle stretch cannot be spent as a burst; the loaders are slowed
by it without knowing it exists, because the channel feeding the engine is
bounded. The pause on the replication slot's `wal_status` stays where it is —
that one protects the slot once the server is already straining, this one keeps
the load off the source's CPU and IO before anything strains. It never applies
to the stream: holding the stream back is what fills the slot. The load's
summary line names the ceiling, so a rate far below what the server could give
is explained where it is read:

```text
read 200 rows from public.orders in 4.0s (~50 rows/s) over 1 range(s), capped at 50 rows/s
```

A transaction larger than `txn_buffer_cap_mb` is split across requests, which
means the target briefly holds part of it. Everything is idempotent, so the end
state is correct, but a reader can observe the transaction half-applied.

Transient failures (HTTP 429, 5xx, connection resets) are retried with
exponential backoff. `retry_max` bounds that by attempts and
`retry_max_elapsed_ms` bounds it by wall clock; with both set, whichever is
reached first ends the retrying, and the error says which one it was and how
long the request had been retried. A permanent rejection — a mapping conflict, for example —
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
| `source` | the only source | Which pipeline to wait on, by [name](#names); required once the process runs more than one |
| `position` | read from the source | Where to wait for; omit and pg2osync reads it itself |
| `timeout` | `5000` | Milliseconds to wait, capped at 30 s |
| `refresh` | `false` | Also make the writes searchable, not merely stored |

```
GET /synced?refresh=true&timeout=2000
200 {"source":"orders","synced":true,"requested":"0/1B4F2A8","confirmed":"0/1B4F2B0","waited_ms":5}
408 {"synced":false,…}   still behind when the timeout elapsed
400 the position could not be parsed
400 several sources and no source=; the body names them
404 no source of that name; the body names the known ones
503 the source has not connected yet
```

A process running one source answers without `source=` exactly as before; one
running several refuses to guess, because answering for whichever pipeline
came first would tell a caller its write is visible when the pipeline carrying
it has written nothing. `503 … has not connected yet` is a source that exists
but has not yet registered with the endpoint — it does so once it can render
a position, which for MySQL is after a round trip to the server — and is
distinct from a `404`, so a caller can retry the one and fix the other.

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

Only `GET /metrics`, `GET /healthz` and `GET /healthz/<name>` are served;
anything else is a 404. Neither health endpoint is ever authenticated, because
a kubelet probe has nowhere to keep a token and a liveness check that fails on
a missing one would restart a healthy pipeline. `/healthz` is the process
being up and nothing more; `/healthz/<name>` is one source's readiness — see
[operations](operations.md#health) for what each answers.

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

Every series carries `source="<name>"` — the [name](#names) of the config it
came from — so one process running a directory of them is one scrape, and a
single-file process reads the same way; [operations](operations.md#the-source-label)
says what that means for an existing query. Omitted below for brevity.

```
pg2osync_events_total{type="row|truncate|join_cascade"}   # join_cascade: a batch that removed a deleted parent's children
pg2osync_batches_flushed
pg2osync_toast_readbacks_total                  # reads to complete TOASTed columns
pg2osync_sink_errors_total
pg2osync_rejected_total                         # documents the target refused, quarantined instead of written
pg2osync_transform_unconverted_total            # values a transform could not convert, indexed as they were
pg2osync_schema_drift_total{table="schema.table"}  # a table changed shape; the index keeps the old one until rebuilt
pg2osync_reconnects_total
pg2osync_source_connected                       # 1 while the source is streaming, 0 while reconnecting
pg2osync_source_state{state="starting|loading|streaming|reconnecting|halted|stopped"}   # exactly one 1 per source
pg2osync_latency_ms{quantile="0.5|0.9|0.99"}   # source commit to indexed
pg2osync_latency_ms_count
pg2osync_position_current                       # highest position received
pg2osync_position_confirmed                     # highest position checkpointed
pg2osync_position_lag                           # difference between the two
```

## A directory of configs

One file is one source: its own slot, its own checkpoint, its own target.
`--config-dir` gives a whole directory of them to one command:

```sh
pg2osync validate --config-dir /etc/pg2osync
pg2osync status   --config-dir /etc/pg2osync
pg2osync run      --config-dir /etc/pg2osync                  # every source, one process
pg2osync run      --config-dir /etc/pg2osync --source orders  # one of them
```

`--config` and `--config-dir` are alternatives — passing both is an error.
`run` with a directory starts one pipeline per file inside one process. Each
source keeps its own sink, engine, checkpoint, retry policy and initial load;
what they share is the process and its two listeners, so every metric carries
a `source` label, `/healthz/<name>` answers for one source and `/synced` takes
`?source=` — [operations](operations.md#health) has the endpoints,
[architecture](architecture.md#several-sources-in-one-process) the shape.
`--source <name>` narrows the directory to that one source, which is how a
single tenant is restarted or debugged without moving files.

There is deliberately no write budget across the files. The sum of every
`[engine] write_concurrency` and `batch_size` in the directory is what the
process costs, and sizing it is the operator's — a shared cap would make
every source wait on every other's slowest target, which is the coupling
running them apart avoided.

### Which commands take a `--source`

Every subcommand but `init` takes `--config` or `--config-dir`; `init` writes
the first config file, so a directory means nothing to it. What differs is what
they do with several sources:

| Command | Without `--source` | With it |
|---|---|---|
| `run`, `bootstrap` | every source in the directory, in one process | that source only |
| `validate`, `status` | every source, reported one after another, and a failure anywhere is the exit code | that source only |
| `resnapshot`, `reindex`, `switch-alias`, `reconcile`, `drop-slot`, `setup-sql`, `rejects` | refused when the directory holds more than one, naming the choices | that source |

The second group changes what one source owns — its index, its slot, its
documents — and a directory does not say which. Rather than acting on the first
file or on all of them, they ask:

```
drop-slot acts on one source, and this directory has 2: billing, orders. Name
one with --source
```

`rejects` is the exception worth knowing: the quarantine store belongs to the
**target**, not to a source, so every source writing to that target shares it.
`--source` there picks the configuration the command reaches the target with,
not which documents are listed.

What the directory adds is everything two files can disagree about, none of
which is visible from inside either one.

Which files are read:

- every `*.toml` **directly** in the directory, in name order; subdirectories
  are not descended into
- entries whose name begins with `.` are skipped, and so is anything that is
  not a file. A Kubernetes ConfigMap mounts as a directory of symlinks beside a
  `..data` symlink to a timestamped directory holding the files themselves, so
  a loader that recursed would read every config twice
- anything that is not `.toml` is skipped, so a `mapping_file` JSON can sit
  beside the config that names it
- a directory with no `*.toml` in it is an error, and so is a directory in
  which any file fails to load — the message lists **every** failing file, not
  the first

Each file is loaded and validated exactly as `--config` loads it. Nothing about
a config changes because it has neighbours.

### Names

A source is called what `[source] name` says, or, with the key left out, the
file's stem — `orders.toml` is `orders`. The grammar is `A-Z a-z 0-9 _ -` and
nothing else, because the name has to survive both a metrics label and a
command line unescaped. A name you write is held to that grammar and refused
otherwise; a stem is fitted to it instead — every other character becomes `-`,
runs collapse, the ends are trimmed — so `orders v2.beta.toml` is
`orders-v2-beta` and no config fails to load over the path it was handed under.
Two sources of the same name are refused, naming both files.

### What two files may not share

**A stream.** A checkpoint is bound to its stream, and a stream is identified
by its `slot_name` (PostgreSQL) or `server_id` (MySQL) — not by the host it
reads. Thirty configs copied from one template therefore name one stream, share
the one `.pg2osync_meta` document, and each resume from the others' position.
The refusal names the document:

```
duplicate stream identity: tenant-a.toml and tenant-b.toml both name replication
slot "pg2osync", so both would keep their position in the one checkpoint document
.pg2osync_meta/postgres-pg2osync and each would resume from the other's. Give
every source a [source] slot_name of its own
```

**An index, unless every section says who its documents are.** The
[shared-index rule](#sharing-an-index) runs over the whole directory as well as
inside each file: sections feeding one index each declare an `id`, or form a
join pair. An index does not know which file a document came from, so two files
with a row `1` are otherwise one document.

**An index a template claims.** A [per-row index](#per-row-indices) claims
every name it can render, in any file: a fixed `events-2024` inside another
file's `events-{tenant}` is refused, because a re-snapshot of the templated
section would clear it.

**A listener, and the log.** `[metrics]`, `[api]` and `[log]` describe the
process, not a source: one process opens one metrics port, one `/synced` port
and one log subscriber. A file that leaves one of these sections out takes
whatever the files that declare it say; two files declaring it differently are
refused, naming both.

## `[log]`

| Option | Default | Description |
|---|---|---|
| `filter` | unset | `tracing` filter, e.g. `pg2osync=debug`. Ignored while `RUST_LOG` is set |

`RUST_LOG` wins wherever both are set — the environment is what a container
runtime, a systemd unit and a shell all reach for, and a file that quietly
overrode it would make the variable a lie. This key exists for the case the
variable cannot serve: the environment is fixed when the process is executed,
so this is the only way to turn a level up on a pipeline that is already
running, and it is one of the things a [reload](#reloading) applies. An
unparsable filter fails validation, which means it fails the whole reload.
Removing the key puts the default back.

## Reloading

`SIGHUP` re-reads this file. The whole file is validated first, so a file with a
mistake anywhere in it changes nothing at all:

```sh
pg2osync validate -c pg2osync.toml && kill -HUP "$(pidof pg2osync)"
```

`validate` checks exactly what a reload checks, which is what makes that one
line the workflow. There is no `pg2osync reload` subcommand: a second process
would have to find the first one, and the only portable way is a pidfile —
state outside the target, which is the one place pg2osync keeps state. Under
Kubernetes the step is `kubectl exec … -- kill -HUP 1`; see
[Deployment](deployment.md#reloading-the-configuration) for doing it on a
config change automatically. Note that installing a handler changes what the
signal does: SIGHUP's default disposition is to terminate, so a deployment
using `kill -HUP` as a blunt restart now gets a reload instead.

What a reload does with each option:

| Option | On reload |
|---|---|
| `[engine] batch_size`, `batch_max_bytes`, `txn_buffer_cap_mb`, `load_max_rows_per_sec` | **Applied** — the next batch reads it |
| `[engine] checkpoint_interval_ms` | **Applied**, one interval late: the sleep already under way is the old one |
| `[engine] retry_max`, `retry_backoff_ms`, `retry_max_elapsed_ms` | **Applied** — the next attempt reads it |
| `[log] filter` | **Applied**, unless `RUST_LOG` is set |
| `[engine] write_concurrency`, `on_permanent_rejection`, `max_rejects` | **Restart** — the sink task is built with them |
| `[source] *`, `[target] *` | **Restart** — they name the stream, and the checkpoint is bound to it. `[source] name` is the exception: only a [directory of configs](#a-directory-of-configs) reads it, so a running pipeline is indifferent to it |
| `[metrics] *`, `[api] *`, any `*_env` | **Restart** — a listener is bound once, and the environment is fixed at exec |
| `[sync.<key>] table`, `index`, `id`, `primary_key`, `append_only`, `routing`, `fan_out`, `join`, `children` | **Rebuild** — see below |
| `[sync.<key>] columns`, `exclude_columns`, `transform`, `fields`, `constants`, `where`, `pipeline`, `soft_delete`, `mapping_file` | **Re-snapshot** — see below |
| `[sync.<key>] poll_column` | **Restart** — the poll query is built when an attempt starts |
| a `[sync.<key>]` added or removed | **Restart** |

Anything not applied is refused *in place*: the section keeps running exactly as
it was, and one `ERROR` line names the field, both values and what it would
take. Nothing is half-applied, and a refusal elsewhere does not hold back the
settings that can change.

The two `[sync]` classes differ in what it costs to put right. The first changes
what a row is **filed as**, so every document the section already wrote is filed
the old way — the fix is a re-index into a new name with the alias moved onto
it ([Zero-downtime re-index](operations.md#zero-downtime-re-index)). The second
only changes the **shape** of the document, so the index would end up holding a
mixture with nothing recording which is which — the fix is `pg2osync resnapshot
--table …` after the restart. Adding or removing a section is a restart: a table
joins a running pipeline only once its rows have been loaded beside the stream
and, on PostgreSQL, its name is in the publication. Removing one leaves its
index exactly as it is; nothing here deletes documents.

Every reload is counted as `pg2osync_config_reloads_total{result}`, with
`result` one of `applied`, `refused`, `invalid` (the file did not load, so
nothing changed) or `failed`. Nothing a reload does moves the checkpoint.

## Environment variables

| Variable | Purpose |
|---|---|
| `RUST_LOG` | Log filter, e.g. `pg2osync=debug`. Wins over `[log] filter`, and cannot be changed without a restart |
| `PG2OSYNC_LOG_FORMAT` | `text` (default) or `json` for one JSON object per line |
| `PG2OSYNC_INSTANCE_ID` | Recorded in the checkpoint document; identifies the writer |
| `PG2OSYNC_OTLP_ENDPOINT` | OTLP/gRPC collector for traces. Unset means none are built or sent — [Traces](operations.md#traces) |
| `PG2OSYNC_OTLP_SAMPLE_RATIO` | Fraction of traces kept, `0.0` to `1.0` (default `1.0`) |
| `PG2OSYNC_OTLP_SERVICE_NAME` | `service.name` on every span (default `pg2osync`) |
| whatever `*_env` names | The credentials themselves |

Filling those credentials from Vault, AWS Secrets Manager or the External
Secrets Operator: [Deployment](deployment.md#secrets).

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
