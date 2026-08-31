# Setting up a managed PostgreSQL

A managed PostgreSQL gives you the same server and takes away two things: the
`postgresql.conf` you would edit, and the `REPLICATION` attribute you would
grant. Every provider has a named replacement for both, and once they are in
place nothing about the pipeline is provider-specific — it is the same
`pgoutput` protocol over the same replication connection.

This is the order to do it in. Steps 1 and 2 usually need someone with the
master credentials, so they are worth batching into one handover.

## 1. Turn logical decoding on

| | What to change | What it costs |
|---|---|---|
| **RDS for PostgreSQL** | `rds.logical_replication = 1` in the **instance** parameter group | a reboot |
| **Aurora PostgreSQL** | `rds.logical_replication = 1` in the **DB cluster** parameter group | a reboot of the writer |
| **Supabase** | nothing — `wal_level` is already `logical` | — |
| **Neon** | logical replication is a per-project switch in the Neon console | — |

`ALTER SYSTEM SET wal_level = logical` is not available on any of them; the
parameter group *is* the change. [Compatibility](../compatibility.md#amazon-rds-and-aurora)
has the full argument for why the equivalence is narrow enough to state without
a CI cell.

## 2. Hand the DBA the SQL

```sh
pg2osync setup-sql -c pg2osync.toml
```

That prints the whole script for your config — role, grants, publication, and
the `wal_level` change with the restart it needs — so it can go to whoever
holds the privileges as one thing rather than as a conversation. What it comes
down to is a role with `REPLICATION`, `CONNECT` on the database, `USAGE` on the
schema and `SELECT` on the tables; [the PostgreSQL source
page](../sources/postgresql.md#requirements) spells it out and
[database impact](../database-impact.md#privileges) has the privilege matrix.

Two substitutions the managed services impose:

- **`ALTER ROLE … REPLICATION` is refused on RDS and Aurora.** The membership
  that stands in for it is `GRANT rds_replication TO <role>`, run as the master
  user — on the writer, for Aurora.
- **Publication creation needs ownership of every published table,** which is a
  PostgreSQL restriction a grant cannot work around. Where the tables belong to
  someone else, have a privileged role create the publication and slot once;
  `validate` prints the exact statements. This is the same branch a
  [reload that adds a table](add-a-source-table-without-a-restart.md#when-the-role-does-not-own-the-publication)
  runs into later.

On Supabase the built-in `postgres` role already carries `REPLICATION`, so the
setup is the publication and the slot and nothing else.

## 3. Check the connection is not going through a pooler

This is the step people skip and then debug for an afternoon. The replication
connection is not a query with a result set: after `START_REPLICATION` the
server streams WAL until the client ends COPY mode, so it needs one backend for
the life of the connection. **Transaction and statement pooling cannot carry
it**, and neither can a query-parsing router.

- **Supabase:** the pooler port **6543** (transaction pooling) cannot carry a
  replication connection. Use the direct port **5432**.
- **RDS Proxy:** documented as not supporting streaming replication mode on
  PostgreSQL. Connect to the instance endpoint, or the Aurora **writer**
  endpoint — a reader cannot hold a slot.
- **PgBouncer** proxies replication connections from 1.23.0 onward, pinned and
  unpooled whatever `pool_mode` says; earlier versions reject them.

[Proxies and connection poolers](../proxies.md#the-stream-connection-must-be-direct)
is the table for every pooler, and the reasoning from the wire protocols rather
than from a test. The SQL connection — `admin_url_env`, or the source URL when
that is unset — *may* be pooled, but it must still reach the primary.

## 4. Run `validate` and read what it says

```sh
pg2osync validate -c pg2osync.toml
```

`validate` asks the server rather than the marketing page, and it recognises
which server it is talking to: RDS by the `rds.logical_replication` setting
existing, Aurora by `aurora_version()`, Supabase by its `supabase_admin` role.
When one of them is recognised, the service's own remedy is appended to the
`wal_level` and slot messages, so a red line names the parameter group or the
`rds_replication` grant rather than the `postgresql.conf` you cannot edit. An
unrecognised server keeps the plain message.

Expect the three lines the [PostgreSQL source
page](../sources/postgresql.md#requirements) shows — connected, `wal_level =
logical`, the table exists — plus one per configured check.

## 5. First run

```sh
pg2osync run -c pg2osync.toml
```

The initial load runs beside the stream, and `[engine] load_max_rows_per_sec`
is the way to be gentle with a production primary without waiting for the
night. What the load and the stream actually cost the server is measured in
[What it costs your database](../database-impact.md), and the one real risk
there — a slot retaining WAL — is worth an alert before the first busy day:
`pg2osync status --max-retained-mb` makes that check something a scheduler can
run even while the pipeline is down, which is the dangerous case.

## Per-provider caveats worth knowing before you start

- **Neon** suspends an idle compute, which stops the stream. pg2osync resumes
  from its slot when the compute wakes, so a restart is a reconnect rather than
  an error — but the retained WAL grows for as long as nobody is connected.
  Size `max_slot_wal_keep_size` accordingly.
- **A paused Supabase project** stops advancing the slot, with the same
  consequence.
- **TimescaleDB: plain tables only.** A hypertable's rows live in chunks, which
  are inheritance children rather than declarative partitions, so a publication
  on the root sees nothing — a section naming a hypertable would report a
  healthy pipeline and index nothing. This was
  [measured](../compatibility.md#timescaledb-and-hypertables), not assumed.
- **CockroachDB is not a PostgreSQL source.** It is wire-compatible, so `psql`
  connects and queries run; it has no `pgoutput`, no
  `pg_create_logical_replication_slot`, and its change feeds are a different
  feature with a different protocol. The failure shows up at the first boot as
  a refused slot, and nothing about the successful connection before it
  predicts it. See [the CockroachDB
  trap](../compatibility.md#the-cockroachdb-trap).
- **RDS, Aurora and Neon are not in CI** — none of them can be a container.
  [Compatibility](../compatibility.md#postgresql-derived-services) says which
  claims are proven by a nightly cell and which are argued from the protocol;
  the way to prove one for a service not listed there is `pg2osync validate`.

Where logical replication genuinely cannot be enabled,
[poll mode](../configuration.md#poll-mode) is the documented fallback. It
cannot see deletes, which is what
[`reconcile`](../operations.md#reconciling-an-index-against-its-source) is for.
