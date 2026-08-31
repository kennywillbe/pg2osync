# pg2osync

**Keep your search index in sync with your database, in real time, from one
binary.** No Logstash, no Kafka, no Redis, no JVM.

pg2osync reads changes straight from the database's replication stream —
PostgreSQL's WAL or MySQL's binlog — and writes them to OpenSearch,
Elasticsearch or Meilisearch within milliseconds. Inserts, updates, deletes and
truncates included. One static Rust binary, one TOML file.

```sh
git clone https://github.com/kennywillbe/pg2osync && cd pg2osync
cargo build --release

export PG2OSYNC_SOURCE_URL="postgres://user:pass@db-host/mydb"
./target/release/pg2osync init --table users   # writes pg2osync.toml, checks the table exists
./target/release/pg2osync validate             # checks both ends and the server's settings
./target/release/pg2osync run                  # initial load, then streaming
```

`init` reads your database to write the config, so an unqualified `users` comes
out as `public.users` and a table without a primary key comes out declared
`append_only` rather than failing at the first row of the load. `validate` is
worth reading rather than skipping:

```
✓ config structure valid (1 table mappings)
✓ connected to PostgreSQL (sslmode=prefer)
✓ wal_level = logical
✓ table public.products exists (3 columns)
✓ privileges sufficient to create the missing objects
✓ opensearch reachable at http://localhost:9200

all checks passed
```

Then `run` loads the table once and streams every change after it, resuming
exactly where it left off if the process dies. Measured on a laptop: 1.1 seconds
from starting it to the first document being searchable.

The config it writes is this small:

```toml
[source]
url_env = "PG2OSYNC_SOURCE_URL"

[target]
url = "http://localhost:9200"

[sync.users]
table = "public.users"
index = "users"
```

## Why this instead of the usual stack

|  | pg2osync | Debezium + Kafka | Logstash JDBC input |
|---|---|---|---|
| Moving parts | **1 binary** | Kafka + Connect + connectors | 1 process |
| Real-time deletes | ✅ from the replication log | ✅ | ❌ polling cannot see them |
| Initial load | ✅ consistent snapshot, automatic | ✅ | manual orchestration |
| Setup | 1 TOML file | topics, registry, connector configs | pipeline config plus caveats |
| Memory footprint | tens of MB | GBs of JVM | hundreds of MB |

The trade-off is deliberate: pg2osync is a single-purpose pipeline, not a
streaming platform. There is no fan-out to multiple consumers, no message
replay, and no transformation language (a fixed set of named column reshapes,
not expressions) — if you need those, you want Kafka.

## Features

| | Status |
|---|---|
| **PostgreSQL → OpenSearch** (logical replication) | ✅ full suite on every pull request (PostgreSQL 17, OpenSearch 2.19) |
| PostgreSQL → Elasticsearch 8.x | ✅ full suite nightly, [one known gap](docs/compatibility.md) |
| PostgreSQL → Meilisearch 1.x | ✅ smoke suite nightly (file-based checkpoint), [one known gap](docs/compatibility.md) |
| **MySQL 8.0 / MariaDB 10.6+ → any of the above** | ✅ MySQL 8.0 on every pull request; MySQL 8.4 and MariaDB 10.6/11.8 nightly |
| Consistent initial load, then live streaming | ✅ |
| Crash recovery with no data loss (`kill -9` safe) | ✅ verified by the e2e suite |
| Nested child collections (one level) | ✅ PostgreSQL and MySQL/MariaDB; the parent document embeds child arrays, resolved once per collection per transaction, with their own `columns` / `exclude_columns` projection, or `single = true` to embed a 1:1 relation as an object |
| Many-to-many children (`through`) | ✅ PostgreSQL and MySQL/MariaDB; a junction table is joined inside the same aggregation, and both the junction and the child are streamed |
| Aggregate children (`aggregates`) | ✅ PostgreSQL and MySQL/MariaDB; a count from a child table lands on the parent document and is kept live by the same machinery, one grouped query per aggregate per transaction |
| Parent-child as a join field (`join`) | ✅ OpenSearch and Elasticsearch; shared index, per-document routing, parent delete cascades to its children |
| Per-document routing from a column (`routing = "tenant_id"`) | ✅ OpenSearch and Elasticsearch; co-locates a tenant on one shard; a changed value moves the document; non-key columns need `REPLICA IDENTITY FULL` |
| One index fed by several tables | ✅ each section declares an explicit id; reconcile refuses it, a TRUNCATE is skipped and counted |
| A row chooses its index (`index = "events-{tenant}"`) | ✅ created on demand with your mapping; TRUNCATE clears the pattern; reconcile refuses; old buckets expire through [ILM or ISM](docs/configuration.md#retention) |
| Column projection (`columns` / `exclude_columns`) | ✅ |
| Derived document ids (`id = "tenant-{tenant_id}-{id}"`) | ✅ default stays the primary key; non-key columns need `REPLICA IDENTITY FULL` |
| One row to many documents (`fan_out` over a JSON-array column, or a delimited string with `by = ","`) | ✅ elements added, moved and removed as versioned writes; with `join.parent = "{element}"` each element is filed under itself |
| Append-only tables without a primary key (`append_only`) | ✅ content-hash ids; an UPDATE or DELETE halts |
| Column transforms (`hash`, `redact`, `pseudonym`, `json`, `split`, `number`, `date`, `lookup`) | ✅ eight named ops, no expression language; `pseudonym` is keyed AES-SIV, so joins survive; a value that will not convert is indexed as it is and counted (a pseudonym is redacted) |
| Row filters (`where`) | ✅ a SQL subset the load pushes down and the stream evaluates; a row that leaves the filter is deleted |
| Field renames (`fields`) | ✅ source column to target field, on every path and inside child arrays |
| Fields that come from no column (`constants`) | ✅ literals plus `{schema}`/`{table}`, no expression language |
| TRUNCATE propagation | ✅ PostgreSQL and MySQL/MariaDB |
| Polling fallback for managed databases without replication | ✅ upserts, plus deletes via `soft_delete` |
| Index mappings you define (`mapping_file`) | ✅ applied at creation, compared at startup |
| Vector fields through an ingest pipeline (`pipeline`) | ✅ the target embeds; pg2osync names the pipeline |
| Reconcile an index against its table (`reconcile`) | ✅ names or removes documents whose row is gone |
| Re-snapshot one table (`resnapshot`) | ✅ on demand, `--where` to narrow it, safe beside the stream |
| Survive one document the target refuses | ✅ opt-in quarantine with its position, bounded, replayable |
| **Read-your-writes** (`/synced`) | ✅ wait for your own commit to be searchable |
| Prometheus metrics | ✅ built-in endpoint, with a Grafana dashboard to import |

Which versions CI actually runs, and which are expected to work but are not
exercised: [docs/compatibility.md](docs/compatibility.md).

### Known limitations

Stated up front, because finding these out in production is expensive:

- **One process per replication slot.** Two processes on the same slot fight
  over its position. Several source databases do fit in one process
  (`run --config-dir`), each with a slot of its own; scale beyond that by
  splitting the files across processes.
- **No schema migration.** A column added, dropped or retyped under a running
  pipeline is reported, never applied: new documents take the new shape and
  every document written before it keeps the old one until the index is
  rebuilt. `validate` refuses a `columns` list naming a column that no longer
  exists.
- **Poll mode cannot see a hard delete.** It has no access to the replication
  log. A soft delete it can see (`soft_delete`), and `pg2osync reconcile`
  removes index documents whose row is gone.
- **A many-to-many relation embeds through its junction table**: `through` adds
  one join inside the same aggregation, so the array is read the way any other
  child collection is.
- **Nested children are one level deep**, re-fetched once per collection per
  transaction rather than per changed row, ordered by the child's primary key.
  `max_rows` bounds a collection, and a document whose array was cut says so.
  A child collection projects its own columns, in the read, so the load and the
  re-fetch cannot embed different shapes. `single = true` embeds a one-to-one
  relation as the object itself, `null` when absent.
- **Two tables share an index only as a join pair or when every section
  declares its id.** Otherwise document identity would be ambiguous, and
  even then the ids must be unique across the tables — `customer-{id}` and
  `order-{id}`, not the bare key. `reconcile` refuses a shared index, and a
  `TRUNCATE` on one of its tables is skipped and logged rather than applied
  (a join pair clears its own relation).
- **A per-row index (`index = "events-{tenant}"`) cannot be reconciled or
  aliased.** `reconcile`, `switch-alias` and `reindex` refuse a templated
  table, and Meilisearch refuses the template at startup: it has no mappings
  to create an index with.
- **MySQL needs `binlog_row_image = FULL`**, and refuses
  `binlog_row_value_options = PARTIAL_JSON`.
- Ordering is guaranteed per row, not across tables.

## Requirements

**PostgreSQL source** — 15 or newer, `wal_level = logical`, a user with
`REPLICATION`, and a primary key on every synced table — or `append_only` on
one that has none. TLS is supported on
every connection via `[source] sslmode` (libpq semantics, `prefer` by default),
and client certificates via `sslcert`/`sslkey`.

**MySQL source** — MySQL 8.0+ or MariaDB 10.6+ with `log_bin = ON`,
`binlog_format = ROW`, `binlog_row_image = FULL`, and a user holding `SELECT`,
`REPLICATION SLAVE` and `REPLICATION CLIENT`. Both `caching_sha2_password` and
`mysql_native_password` work, and TLS is supported through the same
`[source] sslmode` setting, with client certificates via `sslcert`/`sslkey`.

**Network** — a direct connection for the stream: a pooler or query-routing
proxy cannot carry a replication or binlog-dump connection. The SQL connection
may be pooled but must reach the primary. See [docs/proxies.md](docs/proxies.md).

**Target** — OpenSearch 2.x, Elasticsearch 8.x or Meilisearch 1.x.

The exact versions CI runs are listed in
[docs/compatibility.md](docs/compatibility.md); the rest of the range is
expected to work but untested.

`pg2osync validate` checks all of this and tells you exactly what to fix.

## Install

**From a release.** Every [release](https://github.com/kennywillbe/pg2osync/releases)
ships a static binary for Linux and macOS on x86-64 and arm64, each as
`pg2osync-<tag>-<target>.tar.gz` with a `.sha256` beside it. The archive holds
the one `pg2osync` executable and nothing else:

```sh
v=v1.3.0 t=x86_64-unknown-linux-musl   # or aarch64-unknown-linux-musl,
                                       # x86_64-apple-darwin, aarch64-apple-darwin
curl -fsSLO "https://github.com/kennywillbe/pg2osync/releases/download/$v/pg2osync-$v-$t.tar.gz"
curl -fsSLO "https://github.com/kennywillbe/pg2osync/releases/download/$v/pg2osync-$v-$t.tar.gz.sha256"
sha256sum -c "pg2osync-$v-$t.tar.gz.sha256"   # shasum -a 256 -c on macOS
tar -xzf "pg2osync-$v-$t.tar.gz" && sudo install pg2osync /usr/local/bin/
```

The same release publishes a container image, tagged with the version and with
`major.minor`:

```sh
docker run --rm \
  -e PG2OSYNC_SOURCE_URL="postgres://user:pass@db:5432/appdb" \
  -v "$PWD/pg2osync.toml:/etc/pg2osync/pg2osync.toml:ro" \
  -p 9100:9100 \
  ghcr.io/kennywillbe/pg2osync:1.3.0
```

**From source.**

```sh
git clone https://github.com/kennywillbe/pg2osync && cd pg2osync
cargo build --release          # ./target/release/pg2osync
# or put it on PATH:
cargo install --path crates/bin
```

Rust 1.98 or newer. The binary links no C libraries, so the build needs nothing
but a toolchain.

Kubernetes manifests are in [deploy/kubernetes](deploy/kubernetes)
(`kubectl apply -k deploy/kubernetes`); see
[docs/deployment.md](docs/deployment.md) for probes, scaling, systemd and
the recipes that feed the credentials from Vault, AWS Secrets Manager or the
External Secrets Operator.

## Try it locally

Both ends in containers and a seeded table, from a clone of this repository:

```sh
docker compose -f dev/docker-compose.yml up -d          # PostgreSQL :15432, OpenSearch :9200
docker exec -i dev-postgres-1 psql -U postgres -d sourcedb < dev/seed.sql
cargo build --release

export PG2OSYNC_SOURCE_URL="postgres://postgres:postgres@localhost:15432/sourcedb"
./target/release/pg2osync init --table users
./target/release/pg2osync validate
./target/release/pg2osync run
```

In another shell, watch a change land:

```sh
docker exec dev-postgres-1 psql -U postgres -d sourcedb \
  -c "UPDATE users SET name = 'renamed' WHERE id = 1;"
curl -s localhost:9200/users/_doc/1 | jq .
```

Against **your own** database the only difference is the URL: `init` finds the
tables, and `validate` names anything the server still needs — `wal_level`, a
replication role, a primary key or `append_only` in its place.
`pg2osync setup-sql` prints the SQL for a DBA to run when you do not hold
those privileges yourself.

## A fuller configuration

```toml
[source]
url_env = "PG2OSYNC_SOURCE_URL"
slot_name = "pg2osync"
publication = "pg2osync_pub"

[target]
url = "https://opensearch.internal:9200"
username = "pg2osync"
password_env = "PG2OSYNC_TARGET_PASSWORD"

[metrics]
bind = "127.0.0.1:9100"

[sync.customers]
table = "public.customers"
index = "customers"
exclude_columns = ["password_hash"]     # never leaves the database

[sync.customers.transform]
email = "redact"                        # or "hash"
status = { op = "lookup", map = { "1" = "active", "2" = "closed" } }

[sync.customers.fields]
signup_dt = "signed_up_at"              # stored under this name; every other option keeps the source name

[sync.customers.constants]
entity = "customer"                     # every document carries it; no column needed
origin = "{schema}.{table}"             # rendered once at startup

[[sync.customers.children]]
table = "public.orders"
field = "orders"                        # embedded as a JSON array
foreign_key = "customer_id"
```

MySQL is the same file with two lines changed:

```toml
[source]
flavor = "mysql"
url_env = "PG2OSYNC_SOURCE_URL"         # mysql://user:pass@host:3306/db
server_id = 424242                      # unique among the server's replicas

[sync.users]
table = "appdb.users"                    # database.table for MySQL
index = "users"
```

Every option is documented in
[docs/configuration.md](docs/configuration.md).

## Commands

Every one takes `-c <file>` and defaults to `pg2osync.toml`, so the common case
needs no flag at all.

| Command | What it does |
|---|---|
| `init --table T` | Write a starter config, qualifying `T` and checking the source has it |
| `validate` | Config, connectivity and server prerequisites, one line each |
| `run` | Initial load plus continuous streaming (main mode) |
| `bootstrap` | Create the slot, publication and target indices, then exit |
| `status` | Checkpoint position versus the source's, and what each slot retains |
| `setup-sql` | Print the SQL a DBA needs, derived from your config |
| `resnapshot --table T` | Read one table again into its index, without reloading the rest |
| `reconcile` | Name index documents whose row is gone; `--delete` removes them |
| `rejects` | What the target refused, and `--replay` to submit it again |
| `reindex --table T --alias A` | Rebuild the table's index under a fresh name and flip the alias onto it (a swap of the two names on Meilisearch, which has no aliases) |
| `switch-alias --alias A` | Point an alias at this config's index, atomically |
| `drop-slot` | Drop the slot and publication when decommissioning |

## Operating it

**Metrics** — `GET http://127.0.0.1:9100/metrics`: events by type, batches
flushed, sink errors, reconnects, tables that changed shape under the running
pipeline, commit-to-indexed latency quantiles, and the current/confirmed source
position with the lag between them. Import
[deploy/grafana/pg2osync.json](deploy/grafana/pg2osync.json) for the dashboard
built from those series. `PG2OSYNC_LOG_FORMAT=json` writes one JSON object per
log line for the collector alongside it. Set `PG2OSYNC_OTLP_ENDPOINT` and one
trace per batch — decode, transform, write, checkpoint — goes to Jaeger, Tempo
or any OTLP collector, continuing your application's own trace when it calls
`/synced`; unset, nothing is built or sent.

**Read-your-writes** — the pipeline is asynchronous, so a page that writes and
then reads from search can race it. Enable `[api]` and call `GET /synced` after
your commit: it returns once your write is searchable, and only the requests
that need the guarantee pay for it. Measured at 5 ms median on the dev stack.

**Reconnects** — a dropped connection, a failover or a terminated backend is
retried in process with backoff, rebuilding from the last checkpoint. After
`[source] reconnect_max` consecutive failures it exits so a real outage still
surfaces. Configuration and privilege errors are never retried.

**MySQL failover** — with `gtid_mode = ON` the checkpoint records which
transactions have been consumed, not just a file and offset, so pointing the
pipeline at a promoted replica resumes instead of reloading. `dev/failover-probe.sh`
promotes a real replica and proves it. MariaDB needs no setting for this.

**Crash safety** — restart the process; it resumes from the last checkpoint.
Delivery is at-least-once with idempotent writes (`_id` is the primary key, an
id shaped by configuration, or a content hash on an `append_only` table), so
replays overwrite rather than duplicate. The acknowledgement sent back to the
source is clamped to the durable checkpoint, so the database never recycles
history for rows that are not indexed yet.

**Watch the slot** — an abandoned replication slot fills the source's disk, and
that is the one failure that takes the *database* down rather than the pipeline.
`pg2osync_slot_retained_bytes` reports what every slot on the server is pinning,
with the server's own `wal_status` beside it, so it can be alerted on before it
matters. Measured: a 110-byte row retains 238 bytes of WAL, about 820 MB an hour
at a thousand writes a second.

While the pipeline is *down* nothing is there to report, which is exactly when it
grows — so `pg2osync status --max-retained-mb 10240` exits non-zero over a limit
and a cron job can own the check. If you stop syncing for good, run `drop-slot`.

More in [docs/operations.md](docs/operations.md).

## Performance

Measured with `dev/benchmark.sh` on an Apple M2 laptop (8 cores, 16 GB) against
dockerized PostgreSQL 17.11 and OpenSearch 2.19 — a single-node dev stack, so
read this as an order of magnitude, not a capacity plan:

| Metric | Value |
|---|---|
| Initial load | 200K docs in ~5.8 s (**~35,000 docs/s**) |
| Initial load, 2M rows | ~43,000 rows/s with one write request open, **~87,000 with four** (`[engine] write_concurrency`) |
| Initial load, 10M rows | ~43,000 rows/s at one, **~90,000 at four** — the ratio holds at scale |
| Initial load with nested children | 27,400 parents/s with one reader, **42,100 with four** (`[source] load_workers`) |
| Pipeline latency, commit to indexed | **p50 2 ms**, p99 3 ms |
| Commit to searchable, single row | ~80 ms including the client round-trip and a forced index refresh |
| One 50K-row transaction | propagated in ~1.4 s |
| Resident memory under load | ~90 MB |
| Cores the pipeline needs | **1** — 51,900 rows/s capped at one core, 54,500 at two, and 9 MB of memory when starved to a quarter |
| What the **source database** pays while replicated | **−0.4% throughput, 0.19 of a core** at ~14,000 tps ([how that was separated from co-location](docs/database-impact.md#load-while-streaming)) |

Under sustained concurrent writers (`dev/load-test.sh`, 8 clients on the same
machine), where the limits actually sit:

| Metric | Value |
|---|---|
| Single-row transactions | **~11,800 rows/s** with WAL lag at zero — the writers ran out of speed first |
| 100-row transactions | **~57,700 rows/s** |
| Target paused for 15 s at 2,000 rows/s | memory stayed at 38 MB, the slot retained ~510 MB of WAL |
| Recovery after `kill -9` at load | caught up in ~2 s, source and index counts equal |

A commit is what forces a batch, so a stream of single-row transactions used to
cost one request each and topped out near 1,100 rows/s. Whole transactions now
accumulate for up to 10 ms before the batch is written, which is where the rest
of that number comes from; end-to-end latency moved from p50 1 ms to 2 ms.

The initial load is bounded by the target, not by the source: one `COPY` produces
rows more than twenty times faster than the pipeline can index them, and the
figure above is what a single open write request buys. `write_concurrency` opens
more of them, which is why it roughly doubles the load and does almost nothing
past four.

Every figure here is what the load does when nothing holds it back.
`[engine] load_max_rows_per_sec` is the ceiling for when that is not what you
want on a production primary — load rows only, never the stream.

The two latency rows measure different things on purpose. "Commit to indexed" is
what pg2osync controls, taken from its own `pg2osync_latency_ms`. "Commit to
searchable" adds your client's round-trip and the target's refresh interval,
which is what a reader actually waits for — and is dominated by the search
engine, not by this tool.

Reproduce it yourself:

```sh
docker compose -f dev/docker-compose.yml up -d
cargo build --release
ROWS=200000 ./dev/benchmark.sh
./dev/load-test.sh          # sustained writers, backpressure, recovery
```

Tuning knobs live under `[engine]`: batch size, byte ceiling, transaction
buffer cap, retry policy, checkpoint interval.

## Documentation

All of it, browsable and searchable, is at
**<https://kennywillbe.github.io/pg2osync/>** — the same files as below, built
from `docs/` on every change.

- [Architecture](docs/architecture.md) — how the pipeline works
- [Database impact](docs/database-impact.md) — connections, privileges and the load it puts on your source
- [Configuration](docs/configuration.md) — every option
- [Deployment](docs/deployment.md) — Docker, Kubernetes, systemd, secrets managers
- [Operations](docs/operations.md) — metrics, failure modes, recovery
- [Design decisions](docs/decisions.md) — why it is built this way
- Sources: [PostgreSQL](docs/sources/postgresql.md) · [MySQL/MariaDB](docs/sources/mysql.md)
- Sinks: [OpenSearch](docs/sinks/opensearch.md) · [Elasticsearch](docs/sinks/elasticsearch.md) · [Meilisearch](docs/sinks/meilisearch.md)

## Development

```sh
cargo build
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings

cargo build --release
./dev/e2e-test.sh          # PostgreSQL -> OpenSearch, full pipeline
./dev/e2e-mysql-test.sh    # MySQL/MariaDB source
```

Before pushing, run `./dev/ci-local.sh`: it runs on your machine exactly what
CI runs on the pull request, so CI is never the first to find a red.

Workspace layout:

```
crates/
├── core/          ChangeEvent model, Sink trait, checkpoint types, errors
├── source/        PostgreSQL: pgoutput decoder, catalog, poll fallback
├── source-mysql/  MySQL/MariaDB: wire protocol, binlog decoder, catalog
├── engine/        transaction buffering, batching, projections, metrics
├── sink/          OpenSearch / Elasticsearch / Meilisearch writers
└── bin/           CLI and pipeline wiring
```

[CONTRIBUTING.md](CONTRIBUTING.md) covers the architecture rules a change has
to respect.

## License

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE).
