# Compatibility

Which versions CI actually runs, as opposed to which ones are expected to
work. "Not tested" is not "known broken" — it is a version no job exercises,
so a regression there would ship unnoticed.

| Component | Version | Covered by |
|---|---|---|
| PostgreSQL | 17 | every pull request |
| PostgreSQL | 15 (the declared floor) | nightly |
| PostgreSQL | 16, 18 | not tested |
| TimescaleDB | 2.29.2 on PostgreSQL 17 | nightly, full suite (plain tables) |
| Supabase's PostgreSQL image | 17.6 | nightly, full suite |
| Amazon RDS, Aurora, Neon | any PostgreSQL 15+ | not tested, [argued below](#postgresql-derived-services) |
| CockroachDB | any | [not a PostgreSQL source](#the-cockroachdb-trap) |
| MySQL | 8.0 | every pull request |
| MySQL | 8.4 LTS | nightly |
| MariaDB | 10.6 (the declared floor) | nightly |
| MariaDB | 11.8 LTS | nightly |
| OpenSearch | 2.19.6 | every pull request |
| OpenSearch | other 2.x | not tested |
| Elasticsearch | 8.19.20 | nightly, full suite |
| Elasticsearch | 7.x | not tested, known gaps |
| pgvector | pg17 (PostgreSQL 17) | every pull request |
| pgvector | other PostgreSQL majors | not tested |
| Meilisearch | v1.53.1 | nightly, smoke suite only — advisory until [#122](https://github.com/kennywillbe/pg2osync/issues/122) |
| Qdrant | v1.15.1 | nightly, suite of its own |
| Qdrant | other 1.x | not tested |

## What the nightly suite runs

`.github/workflows/compat.yml` builds the release binary once and hands it to
every cell. Three scripts do the work:

- `dev/e2e-test.sh` — the full PostgreSQL suite. `TARGET_FLAVOR` picks
  OpenSearch or Elasticsearch; everything else is identical, because the two
  differ only in REST dialect details the sink hides.
- `dev/e2e-mysql-test.sh` — the full MySQL/MariaDB suite. `MYSQL_CLIENT`
  picks the client binary the container ships.
- `dev/e2e-meili-smoke.sh` — Meilisearch. Not the full suite: that one
  asserts over mappings, join fields and per-row indices, none of which
  Meilisearch has. The smoke suite covers the initial load, live
  INSERT/UPDATE/DELETE, the file-based checkpoint resuming after a restart,
  and a `reindex` swapping a rebuilt index into the live name.

- `dev/e2e-qdrant.sh` — Qdrant, for the same reason: no mappings, no joins, no
  per-row collections. It covers the initial load, live INSERT/UPDATE/DELETE, an
  embedding the source produced answering a similarity search, a fanned list
  that really deletes the elements it drops, a `kill -9` resuming from the state
  collection, `TRUNCATE` at a position, the refusal of an OpenSearch-only
  option, and the sink conformance kit with no check skipped. That last section
  is a `cargo test`, which is why this cell needs a toolchain where the others
  need only the binary.

A fifth script, `dev/e2e-postgres-sink.sh`, runs on every pull request rather
than nightly: the PostgreSQL target is exercised by the `e2e PostgreSQL to
pgvector` job in `ci.yml`, so it has no cell here.

Beside the cells there is one job that runs no script at all. `sink
conformance kit` starts an Elasticsearch and a Meilisearch side by side and
runs the kit against both in a single `cargo test`: the kit is a test rather
than a section of a suite, and the two cells above run a prebuilt binary, so
asking them for it would have compiled the workspace once per cell for what
compiles once here. With OpenSearch and pgvector answering it on every pull
request and Qdrant inside its own cell, all five targets answer the same
contract.

One cell is marked advisory. The Meilisearch smoke suite reaches the restart,
which fails because that sink cannot start twice against one index
([#122](https://github.com/kennywillbe/pg2osync/issues/122)); the cell is kept
red rather than trimmed, because the gap is the finding. The Elasticsearch cell
was advisory for the same reason until the last write it could not make went
green: a section pointed at an alias, which `require_alias` makes the normal
shape, halted before its first write
([#178](https://github.com/kennywillbe/pg2osync/issues/178)).

The matrix also runs on a pull request that touches the workflow, those
scripts, a sink crate or the `Sink` contract, so the change that would break it
is tested before the night it would be reported — and a sink change is answered
by the cells that are the only witness of that target.

`./dev/ci-local.sh` runs the same ten cells and that job on your machine, and
runs them automatically for exactly the changes a pull request would;
`--matrix` forces them. Each cell is a throwaway container on a port of its
own — PostgreSQL 15433, OpenSearch 9201, Elasticsearch 9202, Meilisearch 7701,
Qdrant 6334, MySQL/MariaDB 13307 — so the dev stack on 15432/9200/13306 keeps
running beside it, and the containers are removed however the cell ends. Those
ports are one set, so the cells go one at a time. `--isolated` gives every cell
containers and ports of its own instead, which lets the run take two at a time
and lets it run beside another run on the same machine.

## PostgreSQL-derived services

Every service in this section speaks the same pgoutput protocol over the same
replication connection, so what pg2osync needs from it is always the same three
things: `wal_level = logical`, a role with `REPLICATION`, and ownership of the
published tables. What differs is the switch, and only the switch.

### Proven in CI

- **TimescaleDB** (`timescale/timescaledb:2.29.2-pg17`) — the full
  `dev/e2e-test.sh` suite, nightly, with the extension loaded through
  `shared_preload_libraries`.
- **Supabase's PostgreSQL image** (`supabase/postgres:17.6.1.167`) — the full
  suite, nightly, connecting as the built-in `postgres` role, which is not a
  superuser but does carry `REPLICATION`.

Both are started the way any other cell is, and neither needs a code path of
its own: the point of the cells is that the suite passes unchanged.

The two images do insist on something at container start. Supabase's builds
its roles as `supabase_admin`, so `POSTGRES_USER` has to stay unset; it ships
`listen_addresses = localhost`, which no published port can reach, so
`-c listen_addresses=0.0.0.0` is what makes the container usable; and the
database it creates belongs to `supabase_admin`, so the cell hands it to the
role the suite connects as before seeding — publishing a table requires owning
it. None of the three is a property of the hosted service, where the project's
own role already owns its database.

### TimescaleDB and hypertables

**Plain tables only. Hypertables are not supported, and that was measured, not
assumed.** A hypertable's rows live in chunks, which are inheritance children
rather than declarative partitions, so `CREATE PUBLICATION ... FOR TABLE
metrics` publishes the root and nothing writes to the root: on a 2.29.2 server,
an `INSERT` into a hypertable produced **zero** changes on a pgoutput slot
subscribed to that publication, while an insert into a plain table in the same
publication produced its four messages. `publish_via_partition_root` does not
help — it applies to declarative partitioning, which a hypertable is not.

So a `[sync]` section naming a hypertable would report a healthy pipeline and
index nothing. The cell exercises the plain tables in `dev/seed.sql`, which is
exactly what it claims: TimescaleDB as a PostgreSQL server, not TimescaleDB's
own storage.

### Amazon RDS and Aurora

Not in CI — neither can be a container — but the equivalence is narrow enough
to state. Both run PostgreSQL with the standard `pgoutput` output plugin and
the standard replication protocol; what they take away is `postgresql.conf` and
the `REPLICATION` attribute, and each has a named replacement:

| | RDS for PostgreSQL | Aurora PostgreSQL |
|---|---|---|
| Logical decoding | `rds.logical_replication = 1` in the **instance** parameter group, then reboot | `rds.logical_replication = 1` in the **DB cluster** parameter group, then reboot the writer |
| Slot creation | `GRANT rds_replication TO <role>` as the master user | the same, on the writer |
| Where to connect | the instance endpoint | the **writer** endpoint; a reader cannot hold a slot |

`ALTER ROLE ... REPLICATION` is refused on both: `rds_replication` is the
membership that stands in for it. `validate` names these for you — it
recognises RDS by the `rds.logical_replication` setting existing and Aurora by
`aurora_version()`, and appends the service's own remedy to the `wal_level` and
slot messages. An unrecognised server keeps the plain message.

### Supabase, the hosted service

`wal_level` is already `logical` on a Supabase project, and the built-in
`postgres` role already has `REPLICATION`, so the setup is the publication and
the slot and nothing else. `validate` recognises a Supabase server by its
`supabase_admin` role. Two things to know: the pooler port (6543, transaction
pooling) cannot carry a replication connection — use the direct port 5432, as
[proxies and connection poolers](proxies.md) explains for every pooler — and a
project that is paused stops advancing the slot, which retains WAL.

### Neon

Not in CI: Neon separates storage from compute, so there is no image to run a
cell against, and the compute suspends when idle. It supports logical
replication as a publisher — the switch is per-project, in the Neon console —
and the protocol is unchanged. The caveat is the suspend: an idle compute stops
the stream, and pg2osync resumes from its slot when the compute wakes, so the
retained WAL grows for as long as nobody is connected. Size
`max_slot_wal_keep_size` accordingly, and expect a restart to be a reconnect
rather than an error.

### The CockroachDB trap

CockroachDB is PostgreSQL **wire**-compatible: it speaks the frontend/backend
protocol, so `psql` connects and queries run. It is not PostgreSQL
**replication**-compatible. There is no `pgoutput`, no
`pg_create_logical_replication_slot`, no `pg_publication` behaving as
PostgreSQL's — its change feeds are a different feature with a different
protocol. The failure shows up at the first boot as a refused slot, and nothing
about the successful connection before it predicts it.

Being "PostgreSQL-derived" is a claim each service has to prove. The way to
prove it for a service not listed here is `pg2osync validate`, which asks the
server rather than the marketing page.

## When a nightly run fails

The workflow opens one issue labelled `nightly-compat` and comments the run
URL and the failed cells on it every night it stays red, rather than opening a
new issue each time. Fix the cell or, if the version genuinely is not
supported, say so here and in the README — a claim no job checks is the thing
this page exists to prevent.
