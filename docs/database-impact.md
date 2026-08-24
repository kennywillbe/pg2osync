# What pg2osync costs your database

Every number here was measured with `dev/db-impact.sh` against dockerized
PostgreSQL 17 on an Apple M2. Re-run it against your own instance before
trusting any of it for capacity planning.

## How it connects

pg2osync opens ordinary client connections plus one replication connection. It
holds them for the life of the process; there is no pool and no reconnect storm.

| Connection | When | Purpose |
|---|---|---|
| replication (`walsender`) | always, for the whole run | `START_REPLICATION SLOT … LOGICAL` — the change stream |
| client | always | catalog lookups, publication and slot management, column metadata |
| client | during the initial load only | holds the `REPEATABLE READ` snapshot and runs `COPY` |
| client | only when nested children are configured | re-fetches parent and child rows |

Measured: **2 connections** in steady state, **3** with nested children
configured, **3** at the peak of the initial load. `max_connections` is not a
concern; `max_wal_senders` and `max_replication_slots` must have room for one
each per instance.

MySQL is the same shape: one connection for `COM_BINLOG_DUMP` and one for
`information_schema` lookups.

## Privileges

Two different things are needed, and they are usually held by different roles.

**To run the pipeline** (stream, initial load, index):

```sql
CREATE USER pg2osync WITH REPLICATION PASSWORD '…';
GRANT CONNECT ON DATABASE appdb TO pg2osync;
GRANT USAGE ON SCHEMA public TO pg2osync;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO pg2osync;
```

**To create the publication and slot**, PostgreSQL additionally requires:

- `CREATE` on the database, and
- **ownership of every published table** — a `GRANT` cannot substitute for it.

That is a deliberate PostgreSQL restriction, not something pg2osync can work
around. Verified: with `REPLICATION` and `SELECT` but without ownership,
`CREATE PUBLICATION` fails with `must be owner of table users`.

So on a database whose tables are owned by someone else, a privileged role
creates the objects once:

```sql
CREATE PUBLICATION pg2osync_pub FOR TABLE public.users
  WITH (publish_via_partition_root = true);
SELECT pg_create_logical_replication_slot('pg2osync', 'pgoutput');
```

…and from then on the sync role only consumes them. Verified end to end: with
the objects pre-created, a role holding just `REPLICATION`, `CONNECT`, `USAGE`
and `SELECT` completes the initial load and replicates inserts, updates and
deletes.

`pg2osync validate` reports exactly which of these you are missing and prints
the statements to hand to a DBA. It no longer passes just because it could read
the tables.

| Capability | Needs |
|---|---|
| Open the replication stream | `REPLICATION` attribute (or superuser) |
| Read tables for the initial load | `SELECT` on each table, `USAGE` on the schema |
| Create the replication slot | `REPLICATION` attribute |
| Create the publication | `CREATE` on the database **and** ownership of every table |
| `status` | read access to `pg_replication_slots` (public by default) |
| `drop-slot` | `REPLICATION`, plus ownership of the publication |

MySQL: `SELECT`, `REPLICATION SLAVE`, `REPLICATION CLIENT`, and a
`mysql_native_password` user. Nothing has to be created server-side, so there is
no ownership requirement.

## Load while streaming

**Idle streaming costs nothing measurable.** Over 20 seconds with no writes,
pg2osync issued **0 queries**. Logical replication is push-based: the server
sends changes over the replication connection, and there is no polling.

**pg2osync writes no WAL.** It only reads. The WAL your writes generate is
charged to the database whether pg2osync runs or not — with one exception below.

**Nested children are the one real query cost.** They re-fetch, so:

| Situation | Queries |
|---|---|
| One changed row, no children | 0 |
| One changed parent, one child collection | 1 per collection |
| One changed child row | 1 parent re-fetch + 1 per collection |
| Initial load of N parents with children | **N × collections** |

Measured: the initial load of 20,000 parents with one child collection ran
20,000 child queries. That is the dominant cost of nested children, and it is
why they are documented as a feature you opt into per table.

Without children, the initial load runs exactly one `COPY` per table plus a
handful of catalog queries:

```
1x  SELECT setting FROM pg_settings WHERE name = 'wal_level'
1x  SELECT pubname FROM pg_publication WHERE pubname = $1
1x  SELECT confirmed_flush_lsn::text FROM pg_replication_slots …
1x  COPY (SELECT … FROM public.users) TO STDOUT (FORMAT text)
```

## Cost of `REPLICA IDENTITY FULL`

pg2osync recommends `REPLICA IDENTITY FULL` on child tables (a delete otherwise
carries no foreign key) and for tables whose primary keys change. It is not
free: the whole old row goes into the WAL on every update.

Measured on a table with a 200-byte text column, 5,000 updates:

| Replica identity | WAL written |
|---|---|
| `DEFAULT` | 2.1 MB |
| `FULL` | 3.1 MB (**1.5×**) |

The multiplier grows with row width. Set it per table where you need it, not
database-wide.

## Initial load impact

The load reads inside one `REPEATABLE READ READ ONLY` transaction, so it is a
long-running transaction for its duration. On a large table that has two
consequences worth knowing:

- `VACUUM` cannot clean up rows that became dead after the snapshot started,
  for as long as it runs.
- The snapshot connection holds one `max_connections` slot.

Measured: 20,000 rows loaded in under a second; the snapshot transaction never
exceeded 1 second. For a table where the load takes minutes, schedule it like
you would any long analytical read.

The initial load reads sequentially with `COPY`, which competes for I/O with
your workload but takes no locks that block writers.

## The one real risk: retained WAL

An active pipeline retains only what it has not yet confirmed — measured at
**176 kB** while streaming.

A slot with nothing consuming it retains WAL **forever**, and that fills the
database's disk. This is the failure mode to alert on:

```sql
SELECT slot_name, active,
       pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) AS retained
FROM pg_replication_slots;
```

- Alert when `retained` grows over hours, or when `active` is false for a slot
  you expect to be running.
- Run `pg2osync drop-slot` when you decommission an instance for good.
- Consider `max_slot_wal_keep_size` (PostgreSQL 13+) as a backstop: the slot is
  invalidated instead of filling the disk, and pg2osync then falls back to a
  full initial load, which is safe.

MySQL has no equivalent: it keeps binlogs on its own schedule
(`binlog_expire_logs_seconds`). The trade-off is reversed — nothing accumulates
because of pg2osync, but if it is down longer than the retention window the
position is gone and the next start re-runs the initial load.

## Reproducing these numbers

```sh
docker compose -f dev/docker-compose.yml up -d
cargo build --release
ROWS=20000 ./dev/db-impact.sh
```

The script prints connections, per-query call counts from
`pg_stat_statements`, WAL deltas and retained WAL.
