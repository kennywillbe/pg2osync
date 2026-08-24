# PostgreSQL source

pg2osync's primary source. Uses **logical replication** (pgoutput protocol)
for real-time change capture with a consistent-snapshot backfill.

## Requirements

- PostgreSQL 15 or newer
- `wal_level = logical` in `postgresql.conf` (restart required)
- Sync user needs:
  - `REPLICATION` privilege (or superuser)
  - `SELECT` on all synced tables (used by backfill and child queries)
  - schema usage rights

```sql
CREATE USER sync_user WITH REPLICATION PASSWORD '...';
GRANT SELECT ON ALL TABLES IN SCHEMA public TO sync_user;
```

Verify readiness:

```sh
pg2osync validate -c pg2osync.toml
# ✓ connected to PostgreSQL
# ✓ wal_level = logical
# ✓ table public.users exists
```

## WAL mode (default)

```toml
[source]
url_env = "PG2OSYNC_SOURCE_URL"
slot_name = "pg2osync"          # optional, this is the default
publication = "pg2osync_pub"    # optional, this is the default
```

What pg2osync creates automatically on first run:

- `CREATE PUBLICATION pg2osync_pub FOR TABLE <your tables>`
- `CREATE REPLICATION SLOT pg2osync LOGICAL pgoutput`

You can also create them beforehand with `pg2osync bootstrap` — useful when
the sync user can't run DDL and a DBA provisions the objects instead.

### Row identity

- Documents are keyed by the table's **primary key** (`_id = pk`). Composite
  PKs are supported.
- `UPDATE`/`DELETE` events only carry the old row if the table has
  `REPLICA IDENTITY FULL`. Default (`DEFAULT`) is enough as long as you don't
  change primary keys; if PKs can change, set:

```sql
ALTER TABLE users REPLICA IDENTITY FULL;
```

pg2osync reads the actual setting from `pg_class.relreplident` and warns at
startup when a table cannot support what your configuration asks of it.

### Column selection

```toml
[sync.users]
table = "public.users"
index = "users_index"
exclude_columns = ["password_hash", "internal_notes"]
# ...or whitelist instead:
# columns = ["id", "name", "email"]
```

Projection applies to the initial load and to live streaming alike, so an
excluded column never reaches the target.

TOASTed columns (very large values) that an UPDATE did not modify arrive as
markers rather than values. pg2osync completes them from the old tuple when the
table has `REPLICA IDENTITY FULL`, and otherwise reads the previously indexed
document back from the target — so the document is never written with a hole in
it.

## Poll mode (fallback)

For managed databases where you can't enable logical replication (some
RDS/Cloud SQL tiers, shared hosting):

```toml
[source]
mode = "poll"
url_env = "PG2OSYNC_SOURCE_URL"
poll_column = "updated_at"        # timestamp column maintained by triggers/app
poll_interval_secs = 30
```

Limitations:

- **Upsert-only**: deletes are invisible to polling. A soft-delete column plus
  a filter in your queries is the usual workaround.
- Rows need a reliably bumped, monotonically increasing timestamp column.
- The latency floor is the poll interval.
- There is no position to resume from, so every start re-runs the initial load.
  WAL checkpoints left by a previous `mode = "wal"` run are ignored on purpose:
  using one would skip rows that changed while the process was down.
- `poll_page_size` (default 5000) bounds how many rows one cycle reads per
  table; a large backlog drains over several cycles.

## Truncates and deletes

`TRUNCATE` on a synced table clears the target index. It is ordered against
writes still queued for the target, so a row written just before the truncate
cannot survive it.

`DELETE` needs the row's key, which the default replica identity provides. A
table with `REPLICA IDENTITY NOTHING` cannot replicate updates or deletes at
all; pg2osync fails with the exact `ALTER TABLE` to run.

## Slot hygiene

A replication slot that isn't consumed retains WAL **forever** and will fill
the database disk. Operational rules:

- Monitor with `pg_replication_slots` (`restart_lsn`, `confirmed_flush_lsn`)
  or just `pg2osync status`.
- Decommissioning an environment: always run `pg2osync drop-slot`.
- If a slot was dropped while pg2osync was down, the next start detects the
  missing slot and re-backfills safely (idempotent writes).
