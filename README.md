# pg2osync

**Keep your search index in sync with your database, in real time, from one
binary.** No Logstash, no Kafka, no Redis, no JVM.

pg2osync reads changes straight from the database's replication stream —
PostgreSQL's WAL or MySQL's binlog — and writes them to OpenSearch,
Elasticsearch or Meilisearch within milliseconds. Inserts, updates, deletes and
truncates included. One static Rust binary, one TOML file.

```toml
[source]
url_env = "PG2OSYNC_SOURCE_URL"

[target]
url = "http://localhost:9200"

[sync.users]
table = "public.users"
index = "users"
```

```sh
export PG2OSYNC_SOURCE_URL="postgres://user:pass@db-host/mydb"
pg2osync setup-sql -c pg2osync.toml  # the SQL your DBA needs to run, if any
pg2osync validate -c pg2osync.toml   # check connections and prerequisites
pg2osync run -c pg2osync.toml        # consistent initial load, then streaming
```

That's the whole setup. `run` loads the table once from a consistent snapshot,
then streams every subsequent change until you stop it — resuming exactly where
it left off if the process dies.

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
replay, and no transformation language — if you need those, you want Kafka.

## Features

| | Status |
|---|---|
| **PostgreSQL → OpenSearch** (logical replication) | ✅ verified end to end |
| PostgreSQL → Elasticsearch 8.x | ✅ verified end to end |
| PostgreSQL → Meilisearch 1.x | ✅ verified end to end (file-based checkpoint) |
| PostgreSQL → Amazon OpenSearch Serverless | ⚠️ profile exists, never run against a real collection — [see the caveats](docs/sinks/opensearch.md#amazon-opensearch-serverless) |
| **MySQL 8.0 / MariaDB 10.6+ → any of the above** | ✅ verified end to end |
| Consistent initial load, then live streaming | ✅ |
| Crash recovery with no data loss (`kill -9` safe) | ✅ verified by the e2e suite |
| Nested child collections (one level) | ✅ parent document embeds child arrays |
| Column projection (`columns` / `exclude_columns`) | ✅ |
| Column transforms (`hash`, `redact`) | ✅ |
| TRUNCATE propagation | ✅ PostgreSQL and MySQL/MariaDB |
| Polling fallback for managed databases without replication | ✅ upserts, plus deletes via `soft_delete` |
| Index mappings you define (`mapping_file`) | ✅ applied at creation, compared at startup |
| Reconcile an index against its table (`reconcile`) | ✅ names or removes documents whose row is gone |
| **Read-your-writes** (`/synced`) | ✅ wait for your own commit to be searchable |
| Prometheus metrics | ✅ built-in endpoint |

### Known limitations

Stated up front, because finding these out in production is expensive:

- **One instance per replication slot.** Two processes on the same slot fight
  over its position. Scale by splitting tables across instances.
- **No schema migration.** A column added, dropped or retyped under a running
  pipeline is reported, never applied: new documents take the new shape and
  every document written before it keeps the old one until the index is
  rebuilt. `validate` refuses a `columns` list naming a column that no longer
  exists.
- **Poll mode cannot see a hard delete.** It has no access to the replication
  log. A soft delete it can see (`soft_delete`), and `pg2osync reconcile`
  removes index documents whose row is gone.
- **Nested children are one level deep**, re-fetched with a query per changed
  parent — a wide fan-out slows the initial load.
- **MySQL needs `binlog_row_image = FULL`**, and refuses
  `binlog_row_value_options = PARTIAL_JSON`.
- **MySQL nested children are not supported yet.**
- Ordering is guaranteed per row, not across tables.

## Requirements

**PostgreSQL source** — 15 or newer, `wal_level = logical`, a user with
`REPLICATION`, and a primary key on every synced table. TLS is supported on
every connection via `[source] sslmode` (libpq semantics, `prefer` by default).

**MySQL source** — MySQL 8.0+ or MariaDB 10.6+ with `log_bin = ON`,
`binlog_format = ROW`, `binlog_row_image = FULL`, and a user holding `SELECT`,
`REPLICATION SLAVE` and `REPLICATION CLIENT`. Both `caching_sha2_password` and
`mysql_native_password` work, and TLS is supported through the same
`[source] sslmode` setting.

**Target** — OpenSearch 2.x, Elasticsearch 8.x or Meilisearch 1.x.

`pg2osync validate` checks all of this and tells you exactly what to fix.

## Install

```sh
# from source
cargo install --path crates/bin

# container
docker run --rm \
  -e PG2OSYNC_SOURCE_URL="postgres://user:pass@db:5432/appdb" \
  -v "$PWD/pg2osync.toml:/etc/pg2osync/pg2osync.toml:ro" \
  -p 9100:9100 \
  ghcr.io/kennywillbe/pg2osync:0.6.0
```

Kubernetes manifests are in [deploy/kubernetes](deploy/kubernetes)
(`kubectl apply -k deploy/kubernetes`); see
[docs/deployment.md](docs/deployment.md) for probes, scaling and systemd.

## Try it locally

```sh
# PostgreSQL on :15432 with logical replication, OpenSearch on :9200
docker compose -f dev/docker-compose.yml up -d
docker exec -i dev-postgres-1 psql -U postgres -d sourcedb < dev/seed.sql

cargo build --release
cat > pg2osync.toml <<'TOML'
[source]
url_env = "PG2OSYNC_SOURCE_URL"

[target]
url = "http://localhost:9200"

[sync.users]
table = "public.users"
index = "users"
TOML

export PG2OSYNC_SOURCE_URL="postgres://postgres:postgres@localhost:15432/sourcedb"
./target/release/pg2osync validate -c pg2osync.toml
./target/release/pg2osync run -c pg2osync.toml
```

In another shell, watch a change land:

```sh
docker exec dev-postgres-1 psql -U postgres -d sourcedb \
  -c "UPDATE users SET name = 'renamed' WHERE id = 1;"
curl -s localhost:9200/users/_doc/1 | jq .
```

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

| Command | What it does |
|---|---|
| `run -c <cfg>` | Initial load plus continuous streaming (main mode) |
| `validate -c <cfg>` | Config, connectivity and server prerequisites |
| `bootstrap -c <cfg>` | Create the slot, publication and target indices, then exit |
| `status -c <cfg>` | Checkpoint position versus the source's current position |
| `drop-slot -c <cfg>` | Drop the slot and publication when decommissioning |

## Operating it

**Metrics** — `GET http://127.0.0.1:9100/metrics`: events by type, batches
flushed, sink errors, reconnects, commit-to-indexed latency quantiles, and the
current/confirmed source position with the lag between them.

**Read-your-writes** — the pipeline is asynchronous, so a page that writes and
then reads from search can race it. Enable `[api]` and call `GET /synced` after
your commit: it returns once your write is searchable, and only the requests
that need the guarantee pay for it. Measured at 5 ms median on the dev stack.

**Reconnects** — a dropped connection, a failover or a terminated backend is
retried in process with backoff, rebuilding from the last checkpoint. After
`[source] reconnect_max` consecutive failures it exits so a real outage still
surfaces. Configuration and privilege errors are never retried.

**Crash safety** — restart the process; it resumes from the last checkpoint.
Delivery is at-least-once with idempotent writes (`_id` is the primary key), so
replays overwrite rather than duplicate. The acknowledgement sent back to the
source is clamped to the durable checkpoint, so the database never recycles
history for rows that are not indexed yet.

**Watch the slot** — `pg2osync status` shows the checkpoint against the
source's position. A growing gap means WAL or binlogs are accumulating on the
database. If you stop syncing for good, run `drop-slot`; an abandoned slot will
fill the source's disk.

More in [docs/operations.md](docs/operations.md).

## Performance

Measured with `dev/benchmark.sh` on an Apple M2 laptop (8 cores, 16 GB) against
dockerized PostgreSQL 17.11 and OpenSearch 2.19 — a single-node dev stack, so
read this as an order of magnitude, not a capacity plan:

| Metric | Value |
|---|---|
| Initial load | 200K docs in ~5.8 s (**~35,000 docs/s**) |
| Pipeline latency, commit to indexed | **p50 2 ms**, p99 3 ms |
| Commit to searchable, single row | ~80 ms including the client round-trip and a forced index refresh |
| One 50K-row transaction | propagated in ~1.4 s |
| Resident memory under load | ~90 MB |

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

- [Architecture](docs/architecture.md) — how the pipeline works
- [Database impact](docs/database-impact.md) — connections, privileges and the load it puts on your source
- [Configuration](docs/configuration.md) — every option
- [Deployment](docs/deployment.md) — Docker, Kubernetes, systemd
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
