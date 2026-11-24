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

`validate` is worth reading rather than skipping:

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
exactly where it left off if the process dies. Against your own database,
`validate` names anything the server still needs — `wal_level`, a replication
role, a primary key or `append_only` in its place — and `pg2osync setup-sql`
prints the SQL for a DBA to run when you do not hold those privileges yourself.
The config `init` writes is this small:

```toml
[source]
url_env = "PG2OSYNC_SOURCE_URL"

[target]
url = "http://localhost:9200"

[sync.users]
table = "public.users"
index = "users"
```

Installing it is in the
[README](https://github.com/kennywillbe/pg2osync#install). This site is the rest:

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
| PostgreSQL → Elasticsearch 8.x | ✅ full suite nightly |
| PostgreSQL → Meilisearch 1.x | ✅ smoke suite nightly (file-based checkpoint), [one known gap](compatibility.md) |
| PostgreSQL → PostgreSQL with pgvector | ✅ full suite on every pull request (pgvector pg17); a table per section, bring-your-own-embedding |
| PostgreSQL → Qdrant 1.x | ✅ suite of its own nightly (Qdrant v1.15); a collection per section, bring-your-own-embedding, the checkpoint in a state collection |
| PostgreSQL-derived services | ✅ TimescaleDB and Supabase's image run the full suite nightly (plain tables; hypertables are not published); RDS, Aurora and Neon speak the same pgoutput and `validate` names each one's switch — CockroachDB is wire-compatible, not replication-compatible: [what is proven and what is argued](compatibility.md#postgresql-derived-services) |
| **MySQL 8.0 / MariaDB 10.6+ → any of the above** | ✅ MySQL 8.0 on every pull request; MySQL 8.4 and MariaDB 10.6/11.8 nightly |
| Consistent initial load, then live streaming | ✅ |
| Crash recovery with no data loss (`kill -9` safe) | ✅ verified by the e2e suite |
| Nested child collections (two levels) | ✅ PostgreSQL and MySQL/MariaDB; the parent document embeds child arrays, and each element may embed one more level of its own, resolved once per collection per transaction, with their own `columns` / `exclude_columns` projection, or `single = true` to embed a 1:1 relation as an object, or `flatten = true` with it to lift the element's columns onto the parent document |
| Many-to-many children (`through`) | ✅ PostgreSQL and MySQL/MariaDB; a junction table is joined inside the same aggregation, and both the junction and the child are streamed |
| Aggregate children (`aggregates`) | ✅ PostgreSQL and MySQL/MariaDB; a count from a child table lands on the parent document and is kept live by the same machinery, one grouped query per aggregate per transaction |
| Vector columns on a pgvector target | ✅ bring-your-own-embedding: a vector the source database or the application produced is carried like any other column and orders a nearest-neighbour query in the target; the sink computes nothing |
| Vectors on a Qdrant target | ✅ the collection's named vectors are the mapping: a document field of the same name becomes that vector and everything else becomes payload; a document id becomes a deterministic UUID point and is kept in the `_pg2osync_id` payload field; the sink computes nothing |
| Parent-child as a join field (`join`) | ✅ OpenSearch and Elasticsearch; shared index, per-document routing, parent delete cascades to its children |
| Per-document routing from a column (`routing = "tenant_id"`) | ✅ OpenSearch and Elasticsearch; co-locates a tenant on one shard; a changed value moves the document; non-key columns need `REPLICA IDENTITY FULL` |
| One index fed by several tables | ✅ each section declares an explicit id; reconcile refuses it, a TRUNCATE is skipped and counted |
| A row chooses its index (`index = "events-{tenant}"`) | ✅ created on demand with your mapping; TRUNCATE clears the pattern; reconcile walks every matching index; old buckets expire through [ILM or ISM](configuration.md#retention) |
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
exercised: [Compatibility](compatibility.md).
