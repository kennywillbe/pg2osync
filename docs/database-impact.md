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
| client | during the initial load only | runs one `COPY` per key range, concurrently with the stream |
| client | only when nested children are configured | re-fetches parent and child rows |

Measured: **2 connections** in steady state, **3** with nested children
configured, **3** at the peak of the initial load. `max_connections` is not a
concern; `max_wal_senders` and `max_replication_slots` must have room for one
each per instance.

MySQL is the same shape: one connection for `COM_BINLOG_DUMP` and one for
`information_schema` lookups.

All PostgreSQL connections share one TLS configuration (`[source] sslmode`), so
a source cannot end up with an encrypted query connection and a plaintext
replication stream.

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

**What a busy database pays: about 0.2 of a core, and no measurable throughput.**
Measured with `dev/db-load-impact.sh`: pgbench, 8 clients, 30 seconds a phase,
against a database whose hot table is the one being replicated.

| | foreground tps | average latency |
|---|---|---|
| nothing replicating | 13,775 | 0.581 ms |
| **the same logical decoding, nothing behind it** | **13,716 (−0.4%)** | 0.583 ms (+0.3%) |
| pg2osync streaming, on the same machine | 8,160 (−40.8%) | 0.980 ms |
| initial load beside the workload | 7,774 (−43.6%) | 1.029 ms |

The second row is the one to quote, and the difference between it and the third
is the point of measuring both. It is `pg_recvlogical` doing byte-for-byte the
same decoding through the same publication with its output going to
`/dev/null` — so it is what the *database* pays, and it is under half a percent.
The walsender burned 5.7 s of CPU over 30 s, **0.19 of a core**, to decode a
workload of ~14,000 transactions a second.

The 40% below it is not replication's cost. It is one laptop running the
database, the pipeline and OpenSearch on the same eight cores; the walsender's
own CPU only rises from 0.19 to 0.32 cores between those rows, which is nowhere
near 40% of anything. A deployment where the pipeline and the target are not on
the database's cores does not pay it — and if yours is that deployment, the
control row is your number.

Two things follow for capacity planning. Give the pipeline its own cores, and
expect the *database* side of CDC to cost a fraction of a core per ~10,000
transactions a second, growing with write volume rather than with table size.

**A table costs about 46 ms, whatever it holds.** Measured with
`dev/many-tables.sh`: the same 500,000 rows loaded once as a single table and
once spread over fifty.

| | wall time | rows/s | peak RSS |
|---|---|---|---|
| 1 table, 500,000 rows | 3.9 s | 128,800 | 57 MB |
| 50 tables, 10,000 rows each | 6.2 s | 81,200 | 30 MB |

The 2.3 s difference over fifty tables is the fixed cost of a table: one boundary
sample, one column lookup, one index to create and one progress document to
write, once each, however few rows it holds. A child collection adds roughly
100 ms more for that table, since its `COPY` aggregates per parent row.

Two things it is worth noticing in that table. The cost is **linear** — five
hundred tables would be around 23 s of setup, not a wall — and memory is *lower*
with fifty tables than with one, because a single large table keeps more rows in
flight in the copy channel. Nothing here grows with the number of tables.

Nor does the streaming side: fifty relations written round-robin keep up, and a
single commit touching all fifty propagates in **0.19 s**, so per-transaction
bookkeeping does not fan out either.

**Idle streaming costs nothing measurable.** Over 20 seconds with no writes,
pg2osync issued **0 queries**. Logical replication is push-based: the server
sends changes over the replication connection, and there is no polling.

**pg2osync writes no WAL.** It only reads. The WAL your writes generate is
charged to the database whether pg2osync runs or not — with one exception below.

**Nested children cost one query per changed parent, and nothing extra during
the initial load.** The load reads each child collection once, aggregated, and
joins it to the parent in the same `COPY`:

| Situation | Queries |
|---|---|
| One changed row, no children | 0 |
| One changed parent, one child collection | 1 per collection |
| One changed child row | 1 parent re-fetch + 1 per collection |
| Initial load of N parents with children | **1 per table**, whatever N is |

Measured: loading 20,000 parents with one child collection issued **20,000
child queries** before this was fixed and **zero** afterwards.

The join compares the key in its own type. That matters more than the query
count: casting either side to text makes the index unusable. On 50,000 parents,
the same work took 165s with a text cast and 74ms without it. The live
re-fetches compare in their own type for the same reason — **index the child's
foreign key**, or every changed parent scans the whole child table.

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

The load reads a table in primary-key pieces, each its own short statement, so
the longest read view it holds is one piece rather than the whole load — one
`COPY` per range on PostgreSQL, one keyset `SELECT` per chunk on MySQL. That
matters for exactly one reason: a read view open across a long load stops the
engine reclaiming anything that died after it started, and the load is the
operation that takes an hour.

The cost is worse on MySQL than on PostgreSQL, which is why the MySQL load was
changed too. A pinned `xmin` horizon delays `VACUUM`; a long InnoDB read view
makes purge *block*, and the undo it cannot discard accumulates in the buffer
pool — Percona measured 382,969 of ~391,000 buffer-pool pages given over to undo
on a 1B-row table, with foreground throughput down to single-digit TPS for as
long as the view lived. MySQL's own manual warns about this for read-only
transactions unprompted.

What it costs the source:

- One `max_connections` slot for the duration.
- Sequential reads that compete for I/O with your workload, taking no locks
  that block writers.
- PostgreSQL: one cheap `pg_class` lookup and one `TABLESAMPLE` read per table
  to decide where to cut the ranges, plus one `pg_current_wal_lsn()` per range.
- MySQL: nothing to decide — the cursor comes out of the rows already read —
  plus one `SHOW BINARY LOG STATUS` per chunk.

Measured: 20,000 rows loaded in under a second; a 200,000-row table read in six
ranges at ~55,000 rows/s, no single transaction lasting longer than a range.

The load also runs *beside* the stream rather than before it, which is what keeps
retained WAL bounded on a long one — see
[architecture](architecture.md#why-the-load-and-the-stream-overlap). The cost to
the source is that the copy and the change stream compete for the same target,
so a PostgreSQL load under WAL pressure deliberately pauses and takes longer.
The MySQL load never pauses, because there is no retention of ours to protect
and waiting would only widen the window for a purge.

`[source] load_workers` is the other direction: it costs the *source* one
concurrent `COPY` per worker for the duration of the load. Worth paying only
where the server is doing per-row work for the read, which in practice means a
table with a nested collection — its `COPY` runs an aggregate subquery per parent
row, and more backends run them in parallel. Measured on the dev stack, 200,000
parents with five children each: 27,400 parents/s with one reader, 42,100 with
four. On an ordinary table the same four readers buy 5–8%, which is not worth
four times the read load.

`[engine] write_concurrency` costs the source nothing and the *target*
proportionally: it is how many write requests stay open at once, so raising it to
four means four concurrent bulk requests against a cluster that may be serving
queries as well. The source read is untouched — it was never the limit, and one
`COPY` already outruns the pipeline by more than twenty times.

A PostgreSQL table smaller than one range, or one with a composite primary key,
is still read in a single `COPY`, so the common case has none of the extra round
trips. On MySQL a composite key is chunked like any other — that is what the
expanded cursor comparison is for — and only a key whose text is not a faithful
literal (binary, blob, bit, geometry, float) falls back to one statement.

## The one real risk: retained WAL

An active pipeline retains only what it has not yet confirmed — measured at
**176 kB** while streaming.

A slot with nothing consuming it retains WAL **forever**, and that fills the
database's disk. This is the failure mode to alert on.

### What it costs, measured

A row of about 110 bytes retains **238 bytes** of WAL — the row plus its
overhead, and the same figure whether the writes arrive as one transaction of
100,000 rows or as 20,000 separate ones. So for a workload of that shape, with
nothing reading the slot:

| write rate | retained after 1 hour | after a day |
|---|---|---|
| 100 rows/s | ~82 MB | ~2 GB |
| 1,000 rows/s | ~820 MB | ~19 GB |
| 10,000 rows/s | ~8 GB | ~192 GB |

Wider rows cost proportionally more, and `REPLICA IDENTITY FULL` multiplies the
update half by about 1.5. The point of the table is the shape: retention is
linear in write volume and unbounded in time, so the question is never *whether*
a stopped pipeline fills the disk but *when*.

### Alerting on it

pg2osync reports this itself, for every slot on the server and not only its own:

```
pg2osync_slot_retained_bytes{slot="pg2osync"}      4096
pg2osync_slot_wal_status{slot="pg2osync",status="lost"} 0
pg2osync_slot_active{slot="pg2osync"}              1
```

`pg2osync_slot_safe_wal_size_bytes` appears only when `max_slot_wal_keep_size`
is set: the server leaves it null when nothing bounds the slot, so its absence
says the retention is unbounded. The same numbers by hand:

```sql
SELECT slot_name, active, wal_status,
       pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) AS retained
FROM pg_replication_slots;
```

The awkward case is a pipeline that has been *down* for a week, because nothing
is running to report anything. For that, `pg2osync status --max-retained-mb`
exits non-zero when any slot is over the limit, which makes it something a cron
job or a Kubernetes `CronJob` can check without the pipeline being up.

- Alert when `retained` grows over hours, or when `active` is false for a slot
  you expect to be running.
- Run `pg2osync drop-slot` when you decommission an instance for good.
- Set `max_slot_wal_keep_size` (PostgreSQL 13+) as a backstop: a full disk
  becomes a recoverable incident instead, and the initial load now watches the
  same signal — while the slot is past its budget the copy pauses and gives the
  stream the throughput, so the load is what slows down rather than the slot
  being invalidated. Without the setting there is nothing to watch and nothing
  to protect: `wal_status` stays `reserved` however much WAL piles up.

MySQL has no equivalent: it keeps binlogs on its own schedule
(`binlog_expire_logs_seconds`), and its automatic purge does not spare a file a
consumer still needs. The trade-off is reversed — nothing accumulates because of
pg2osync, but if it is down longer than the retention window the position is
gone and the next start re-runs the initial load. That is also why the MySQL
load does not pause the way the PostgreSQL one does: holding the load back
cannot protect a position MySQL was never keeping for us, and it lengthens the
window in which the file we still need can be purged.

## Reproducing these numbers

```sh
docker compose -f dev/docker-compose.yml up -d
cargo build --release
ROWS=20000 ./dev/db-impact.sh
```

The script prints connections, per-query call counts from
`pg_stat_statements`, WAL deltas and retained WAL.
