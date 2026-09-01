# What pg2osync is

pg2osync keeps a search index in sync with a database, in real time, from one
binary. It reads changes straight from the database's replication stream —
PostgreSQL's WAL or MySQL's binlog — and writes them to OpenSearch,
Elasticsearch, Meilisearch, a pgvector table or Qdrant within milliseconds. No
Logstash, no Kafka, no Redis, no JVM.

```sh
export PG2OSYNC_SOURCE_URL="postgres://user:pass@db-host/mydb"
pg2osync init --table users   # writes pg2osync.toml, checks the table exists
pg2osync validate             # checks both ends and the server's settings
pg2osync run                  # initial load, then streaming
```

Installing it, the comparison against Debezium and Logstash, and the current
feature matrix are in the
[README](https://github.com/kennywillbe/pg2osync#readme). This site is the rest:

- **[The five-minute quickstart](https://github.com/kennywillbe/pg2osync/blob/main/examples/docker-compose.yml)**
  — PostgreSQL, OpenSearch and pg2osync in one `docker compose up`, seeded, with
  a searchable row at the end of it; and
  [a browser demo](https://github.com/kennywillbe/pg2osync/tree/main/examples/nextjs-demo)
  that times how long a write takes to become searchable.
- **[Configuration](configuration.md)** — every option, with what each one costs.
- **Sources** — what [PostgreSQL](sources/postgresql.md) and
  [MySQL/MariaDB](sources/mysql.md) each need switched on, and how much of the
  server's behaviour leaks into the pipeline.
- **Sinks** — [OpenSearch](sinks/opensearch.md),
  [Elasticsearch](sinks/elasticsearch.md), [Meilisearch](sinks/meilisearch.md),
  [PostgreSQL with pgvector](sinks/postgresql.md) and
  [Qdrant](sinks/qdrant.md), including where they differ in what they can
  guarantee.
- **[Deployment](deployment.md)** — Docker, Kubernetes, systemd, probes.
- **[Kubernetes operator](operator.md)** — a `Pg2osync` per source database,
  reconciled into the deployment the page above describes.
- **[Operations](operations.md)** — metrics, every failure mode, and the
  recovery for each.
- **Guides** — the tasks rather than the subsystems:
  [migrating from Logstash](guides/migrating-from-logstash.md), [setting up a
  managed PostgreSQL](guides/managed-postgres.md), [adding a source table
  without a restart](guides/add-a-source-table-without-a-restart.md),
  [choosing a rebuild](guides/choosing-a-rebuild.md) and [rotating a pseudonym
  key](guides/rotating-a-pseudonym-key.md).
- **[What it costs your database](database-impact.md)** — connections,
  privileges, and the measured load on a busy source.
- **[Architecture](architecture.md)** and
  **[design decisions](decisions.md)** — how it works, and why it was built
  this way rather than another.

Every number in these pages was measured by a script in
[`dev/`](https://github.com/kennywillbe/pg2osync/tree/main/dev), and the page
says which one. Where a limit has not been measured, it says that instead.
