# pg2osync

**PostgreSQL → OpenSearch in one binary. No Logstash. No Kafka. No Redis.**

pg2osync keeps search indices in sync with your database in real time using
logical replication (WAL). A single static Rust binary, one TOML config file,
zero external services.

```toml
[source]
url_env = "PG2OSYNC_SOURCE_URL"

[target]
url = "http://localhost:9200"

[sync.users]
table = "public.users"
index = "users_index"
```

```sh
export PG2OSYNC_SOURCE_URL="postgres://user:pass@db-host/mydb"
pg2osync validate -c pg2osync.toml   # check connections & prerequisites
pg2osync run -c pg2osync.toml        # consistent backfill + live streaming
```

Every insert, update, delete and truncate on `public.users` is indexed into
`users_index` within milliseconds — deletes included.

## Feature matrix

| | Status | Notes |
|---|---|---|
| **PostgreSQL → OpenSearch** (WAL) | ✅ production-ready | Live-verified end-to-end |
| PostgreSQL → Elasticsearch | ✅ | Same engine, REST sink |
| PostgreSQL → Meilisearch | ✅ | File-based checkpointing |
| PostgreSQL → OS Serverless | ✅ | Serverless-safe profile |
| PostgreSQL polling fallback | ✅ | Upsert-only, for managed DBs without replication |
| Nested child collections | ✅ | One level: parent doc embeds child arrays |
| Column transforms (`hash`, `redact`) | ✅ | Applied before indexing |
| Crash recovery, zero data loss | ✅ | kill -9 safe, verified by e2e suite |
| Prometheus metrics | ✅ | Built-in `/metrics` endpoint |
| **MySQL / MariaDB source** | 🔶 preview | Binlog transport + row decoder live-verified against MySQL 8.0 **and** MariaDB 11.8; CLI integration in progress — see [docs/sources/mysql.md](docs/sources/mysql.md) |

## Why not X?

| | pg2osync | Debezium + Kafka | Logstash JDBC input |
|---|---|---|---|
| Moving parts | **1 binary** | Kafka cluster + Connect + connectors | 1 process, but… |
| Real-time deletes | ✅ WAL-based | ✅ | ❌ polling can't see them |
| Initial load | ✅ consistent snapshot | ✅ | manual orchestration |
| Setup effort | 1 TOML file | topics, registry, connectors | pipeline config + caveats |

## Documentation

- [Architecture](docs/architecture.md) — how the pipeline works
- [Configuration reference](docs/configuration.md) — every option explained
- **Sources:** [PostgreSQL](docs/sources/postgresql.md) · [MySQL/MariaDB](docs/sources/mysql.md)
- **Sinks:** [OpenSearch](docs/sinks/opensearch.md) · [Elasticsearch](docs/sinks/elasticsearch.md) · [Meilisearch](docs/sinks/meilisearch.md)

## Quick start

```sh
# 1. Local test environment (PG on :15432 with wal_level=logical, OS on :9200)
docker compose -f dev/docker-compose.yml up -d

# 2. Configure — see examples/pg2osync.example.toml for all options
cat > pg2osync.toml << 'EOF'
[source]
url_env = "PG2OSYNC_SOURCE_URL"

[target]
url = "http://localhost:9200"

[sync.users]
table = "public.users"
index = "users_index"
EOF

# 3. Run
export PG2OSYNC_SOURCE_URL="postgres://postgres:postgres@localhost:15432/sourcedb"
pg2osync validate -c pg2osync.toml
pg2osync run -c pg2osync.toml
```

A nested-documents example with column transforms:

```toml
[sync.customers]
table = "public.customers"
index = "customers_index"
exclude_columns = ["password_hash"]

[sync.customers.transform]
email = "redact"   # or "hash"

[[sync.customers.children]]
table = "public.orders"
field = "orders"          # embedded as a JSON array on each customer doc
foreign_key = "customer_id"
```

## Commands

| Command | Purpose |
|---|---|
| `run -c <cfg>` | Backfill + continuous streaming (main mode) |
| `validate -c <cfg>` | Config syntax, connectivity, `wal_level` checks |
| `status -c <cfg>` | Checkpoint vs. replication slot position |
| `bootstrap -c <cfg>` | Create publication + slot only (no streaming) |
| `drop-slot -c <cfg>` | Clean teardown of slot and publication |

## Operations

- **Metrics**: `GET http://127.0.0.1:9100/metrics` (configurable) — event
  counters per type, batches flushed, sink errors, reconnects, commit→indexed
  latency histogram, current/confirmed LSN.
- **Checkpoints**: written every 500 ms into a hidden `.pg2osync_meta` index
  (OpenSearch/Elasticsearch) or a local state directory (Meilisearch).
- **Crash safety**: restart the process; it resumes from the last checkpoint.
  Delivery is at-least-once with idempotent writes (`_id` = primary key), so
  replays are harmless. See the crash-recovery step in `dev/e2e-test.sh`.
- **Monitoring slot lag**: `pg2osync status` shows checkpoint vs. slot
  position; large gaps mean WAL is accumulating on the database.

## Performance

Measured with the release binary against dockerized PostgreSQL 17 +
OpenSearch 2.19 on a laptop:

| Metric | Value |
|---|---|
| Backfill throughput | ~21,000 docs/s sustained (210K docs in 10 s) |
| Live sync latency p50 | **4 ms** |
| Single-row commit → indexed | ~70 ms end-to-end incl. PG round-trip |
| 50K-row single transaction | propagated in ~2 s |

Tuning knobs live under `[engine]` — batch size, max bytes, flush interval,
transaction buffer cap, retry policy. Defaults are sane; see
[configuration.md](docs/configuration.md).

## Requirements

- PostgreSQL 15+ with `wal_level = logical` and a user with `REPLICATION`
  privilege
- OpenSearch 2.x / Elasticsearch 8.x / Meilisearch 1.x
- Linux/macOS static binary; official Docker image available (`Dockerfile`
  in repo root)

## Development

```sh
cargo build --release
cargo test --workspace                                  # unit tests
cargo clippy --workspace --all-targets -- -D warnings   # lint gate
./dev/e2e-test.sh                                       # full e2e against live containers
```

The workspace layout:

```
crates/
├── core/          ChangeEvent model, LSN type, Sink trait, error taxonomy
├── source/        PostgreSQL transport, pgoutput decoder, catalog, poll mode
├── source-mysql/  MySQL binlog transport + decoder (preview)
├── engine/        batching, transaction buffering, transforms, metrics
├── sink/          OpenSearch / Elasticsearch / Meilisearch writers
└── bin/           CLI: run, validate, status, bootstrap, drop-slot
```

## License

Licensed under [Apache-2.0](LICENSE-APACHE).
