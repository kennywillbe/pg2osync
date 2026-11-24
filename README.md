# pg2osync

**Keep your search index in sync with your database, in real time, from one
binary.** No Logstash, no Kafka, no Redis, no JVM.

pg2osync reads changes straight from the database's replication stream —
PostgreSQL's WAL or MySQL's binlog — and writes them to OpenSearch,
Elasticsearch, Meilisearch, a pgvector table or Qdrant within milliseconds.
Inserts, updates, deletes and truncates included. One static Rust binary, one
TOML file. It is a single-purpose pipeline, not a streaming platform: no
fan-out to multiple consumers, no message replay, no transformation language.

## Features

- PostgreSQL 15+, MySQL 8.0+ and MariaDB 10.6+ as sources, plus
  PostgreSQL-derived services; which versions CI runs is in
  [Compatibility](docs/compatibility.md).
- A consistent initial load, then live streaming; `kill -9` safe, resuming
  from the last durable checkpoint.
- Nested children, many-to-many through a junction table, aggregates and
  parent-child join fields, all kept live.
- Column projection, row filters, field renames, constants and eight named
  transforms, including keyed pseudonyms.
- Derived ids, per-document routing, one row to many documents, a per-row
  index and several tables feeding one index.
- Read-your-writes: `GET /synced` returns once your commit is searchable.
- Reconcile, re-snapshot, zero-downtime re-index and a quarantine for
  documents the target refuses.
- Prometheus metrics, a Grafana dashboard, JSON logs and OTLP traces.

## Install

Every [release](https://github.com/kennywillbe/pg2osync/releases) ships a
static binary for Linux and macOS on x86-64 and arm64, with a `.sha256` beside
each archive:

<!-- x-release-please-start-version -->
```sh
v=v1.5.2 t=x86_64-unknown-linux-musl   # or aarch64-unknown-linux-musl,
                                       # x86_64-apple-darwin, aarch64-apple-darwin
curl -fsSLO "https://github.com/kennywillbe/pg2osync/releases/download/$v/pg2osync-$v-$t.tar.gz"
curl -fsSLO "https://github.com/kennywillbe/pg2osync/releases/download/$v/pg2osync-$v-$t.tar.gz.sha256"
sha256sum -c "pg2osync-$v-$t.tar.gz.sha256"   # shasum -a 256 -c on macOS
tar -xzf "pg2osync-$v-$t.tar.gz" && sudo install pg2osync /usr/local/bin/
```
<!-- x-release-please-end -->

<!-- x-release-please-start-version -->
Or run the multi-architecture image, tagged with the version, `major.minor` and
`latest`: `docker run ghcr.io/kennywillbe/pg2osync:1.5.2`.
<!-- x-release-please-end -->

The archive holds the one `pg2osync` executable and nothing else. Or build from
source with Rust 1.90 or newer — the version CI enforces; the binary links no C
libraries, so a toolchain is all it needs:

```sh
git clone https://github.com/kennywillbe/pg2osync && cd pg2osync
cargo build --release          # ./target/release/pg2osync
cargo install --path crates/bin  # or put it on PATH
```

Kubernetes manifests, the Helm chart, systemd and secrets managers are covered
in [Deployment](https://kennywillbe.github.io/pg2osync/deployment.html).

## Quickstart

No database of your own and no toolchain: PostgreSQL, OpenSearch and pg2osync
come up together, a seeded table is loaded, and the rows are searchable.

```sh
docker compose -f examples/docker-compose.yml up -d --wait   # --wait: until it is streaming
curl -s 'localhost:19200/products/_search?q=espresso' | jq '.hits.hits[]._source'
docker compose -f examples/docker-compose.yml down -v
```

```json
{ "id": 1, "name": "espresso machine", "price": "249.00" }
```

Nothing indexed that document directly: pg2osync read it out of the WAL. Write
another row and it is searchable before you can run the query again —
`docker compose -f examples/docker-compose.yml exec postgres psql -U postgres -d shop`.
The ports are unusual on purpose, so the stack comes up beside whatever you already
run; its whole configuration is
[examples/quickstart/pg2osync.toml](examples/quickstart/pg2osync.toml).

Against your own database the only difference is the URL:

```sh
export PG2OSYNC_SOURCE_URL="postgres://user:pass@db-host/mydb"
pg2osync init --table users   # writes pg2osync.toml, checks the table exists
pg2osync validate             # checks both ends and the server's settings
pg2osync run                  # initial load, then streaming
```

## Documentation

Everything else — configuration, sources, sinks, operations, performance and
design decisions — is at **<https://kennywillbe.github.io/pg2osync/>**, built
from [`docs/`](docs/SUMMARY.md). [CONTRIBUTING.md](CONTRIBUTING.md) covers
development.

## License

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE).
