# Operations

## Metrics

`GET http://<bind>/metrics` returns Prometheus text exposition. Defaults to
`127.0.0.1:9100`; set `[metrics] bind = "0.0.0.0:9100"` in a container.

Moving off loopback is what containers and Kubernetes need, and it is also the
moment the endpoint becomes reachable by anything that can route to the pod.
The exposition holds no credentials and no row data, but it does name every
table being synced and how far behind the pipeline is. Two ways to close that,
and they compose:

- `[metrics] token_env = "PG2OSYNC_METRICS_TOKEN"` requires a bearer token on
  `/metrics`. The process warns at startup when it is bound off loopback
  without one.
- A `NetworkPolicy` that admits only the Prometheus namespace to port 9100.
  This is the baseline Kubernetes pattern and is worth having either way.

Probes use `/healthz`, which is never authenticated.

| Series | Type | Meaning |
|---|---|---|
| `pg2osync_events_total{type}` | counter | Change events received from the source |
| `pg2osync_batches_flushed` | counter | Requests the target accepted |
| `pg2osync_toast_readbacks_total` | counter | Reads of the target to complete unchanged TOASTed columns |
| `pg2osync_sink_errors_total` | counter | Requests that failed permanently |
| `pg2osync_reconnects_total` | counter | Source reconnect attempts |
| `pg2osync_source_connected` | gauge | 1 while streaming, 0 while reconnecting |
| `pg2osync_latency_ms{quantile}` | summary | Source commit to indexed |
| `pg2osync_position_current` | gauge | Highest source position received |
| `pg2osync_position_confirmed` | gauge | Highest position durably checkpointed |
| `pg2osync_position_lag` | gauge | Difference between the two |

### What to alert on

```promql
# nothing has been checkpointed for five minutes while events keep arriving
increase(pg2osync_events_total[5m]) > 0
  and increase(pg2osync_position_confirmed[5m]) == 0

# the pipeline is falling behind instead of catching up
deriv(pg2osync_position_lag[10m]) > 0

# a permanent rejection stops the pipeline
increase(pg2osync_sink_errors_total[5m]) > 0

# the process is gone
up{job="pg2osync"} == 0

# the source has been disconnected rather than briefly interrupted
pg2osync_source_connected == 0

# it keeps losing the connection instead of settling
increase(pg2osync_reconnects_total[15m]) > 5
```

Alert on **source disk** too. An unconsumed PostgreSQL replication slot retains
WAL indefinitely; that fills the database's disk long before it inconveniences
pg2osync.

Set `max_slot_wal_keep_size` before you need it:

```sql
ALTER SYSTEM SET max_slot_wal_keep_size = '10GB';
SELECT pg_reload_conf();
```

It is the one setting that turns a full disk into a recoverable incident. Past
the limit PostgreSQL invalidates the slot instead of retaining more WAL, and
pg2osync then falls back to a full initial load — expensive, but the database
stays up. PostgreSQL 13+.

The initial load watches the same signal. While the slot is past its budget
(`wal_status` anything but `reserved`) the load pauses and lets the change stream
have the throughput, logging `pausing the load: slot … is at wal_status = …`.
A load that takes noticeably longer than expected with that line in the log is
telling you the target cannot absorb the copy and the stream at once: give the
target more capacity, or accept the slower load. If the slot is invalidated
anyway the load fails with `wal_status = lost` and says what to raise, rather
than continuing into a gap.

## Day-to-day commands

```sh
pg2osync setup-sql -c pg2osync.toml  # the SQL a DBA needs, from your config
pg2osync reconcile -c pg2osync.toml  # find index documents whose row is gone
pg2osync validate -c pg2osync.toml   # config, connectivity, server settings
pg2osync status   -c pg2osync.toml   # checkpoint vs the source's position
pg2osync bootstrap -c pg2osync.toml  # create slot/publication/indices, then exit
pg2osync switch-alias -c pg2osync.toml --alias users   # point an alias here
pg2osync drop-slot -c pg2osync.toml  # teardown; --publication drops that too
```

`status` output:

```
checkpoint: source=postgres stream=pg2osync position=0/C174158
slot pg2osync (configured): active=true retained_wal=4 kB
slot pg2osync_old: active=false retained_wal=3 GB

1 inactive slot(s) not named in this config: pg2osync_old
each holds WAL until it is dropped. If one is a former slot_name of this
pipeline: SELECT pg_drop_replication_slot('pg2osync_old');
```

`retained_wal` growing over hours means the target is not keeping up, or the
process is not running while the slot still exists.

Every logical slot on the server is listed, not only the configured one.
Changing `slot_name` leaves the old slot behind, still pinning WAL with nothing
reading it, and that orphan is invisible to anyone who only asks about the name
in the config. `drop-slot` only ever touches the configured slot, so an orphan
is dropped with the SQL above — deliberately, since a slot may belong to
another consumer.

## Reconciling an index against its source

```sh
pg2osync reconcile -c pg2osync.toml            # report only
pg2osync reconcile -c pg2osync.toml --delete   # and remove them
```

Each index is paged in primary-key order and each page of keys is checked
against the table. A document whose row is gone is named; with `--delete` it is
removed. Only keys move between the two sides, which is what makes this far
cheaper than a reindex.

Reporting is the default because a wrong `primary_key` would otherwise empty an
index, and that is not a recoverable mistake.

Run it when the pipeline is caught up. A row inserted seconds ago and not yet
indexed looks exactly like an orphan, so a reconcile mid-load will name rows
that are simply on their way.

It is the tool for three situations: poll mode, which cannot see a hard delete
at all; after an incident where the index and the database might have diverged;
and answering "are these two actually in step" at all, which nothing else here
does. PostgreSQL sources only for now.

## Logging

```sh
RUST_LOG=pg2osync=info    # default
RUST_LOG=pg2osync=debug   # per-event decisions
RUST_LOG=pg2osync::sink=debug,pg2osync=info   # narrow to one component
```

Targets: `pg2osync::source`, `::engine`, `::sink`, `::checkpoint`, `::backfill`,
`::catalog`, `::config`, `::metrics`, `::run`. Credentials are never logged.

## Failure modes

| Symptom | Cause | What to do |
|---|---|---|
| `wal_level is 'replica' but must be 'logical'` | Server not configured | Set it in `postgresql.conf` and restart |
| `publication … covers X but config wants Y` | You changed the table list | Drop and recreate the publication, or align the config. Drift is never auto-applied |
| `… changed shape: added/removed/retyped …` | A column changed under the running pipeline | Nothing breaks, but documents written earlier keep the old shape. Re-index when you want them to agree |
| `table … has REPLICA IDENTITY NOTHING` | Updates/deletes cannot be replicated | Run the `ALTER TABLE … REPLICA IDENTITY FULL` from the message |
| `child row carries NULL <fk>` | Child table lacks `REPLICA IDENTITY FULL` | Set it; a delete without the key cannot find its parent |
| `halting pipeline: permanent rejection …` | The target refuses the document (usually a mapping conflict) | Fix the mapping or exclude the column; the retry then gets through. Or quarantine it — see below |
| `halting pipeline: … reached the max_rejects limit` | Enough documents were refused that it is systematic | Fix the mapping, then `pg2osync rejects --replay` |
| `… were refused and could not be quarantined` | The quarantine store is unwritable | The batch is unacknowledged, so nothing is lost. Fix target access and restart |
| `binlog_format is "STATEMENT"` | MySQL not row-based | `binlog_format = ROW` in `my.cnf`, restart |
| `server switched to 'caching_sha2_password'` | MySQL user's auth plugin | Recreate the user with `mysql_native_password` |
| `bogus data in log event` | Resuming at an invalid binlog offset | Delete the checkpoint document to force a fresh initial load |
| Checkpoint ignored, full load runs | Checkpoint belongs to another slot/`server_id`/source | Expected. Restore the original identifier or accept the reload |

A permanent rejection stops the pipeline on purpose: skipping the document would
be silent data loss, and every batch after it would widen the divergence. It stops
by making no progress rather than by exiting — the attempt fails and is retried
like any other, so the position never passes the document and fixing the mapping
lets the next attempt through without a restart.

### Carrying on past one refused document

One malformed row otherwise stops replication for every table. With

```toml
[engine]
on_permanent_rejection = "quarantine"
max_rejects = 100
```

the refused document is recorded in a hidden `.pg2osync_rejects` index — with its
position and the write itself — and the pipeline continues. `pg2osync_rejected_total`
moving is the signal to alert on: it means data is in the quarantine store and not
in the index.

```sh
pg2osync rejects -c pg2osync.toml            # what was refused, where, and why
pg2osync rejects -c pg2osync.toml --replay   # after fixing the mapping
```

A replay submits each document again with its original position as its version, so
a row the source has changed since loses to the newer value, and a record is
cleared only once the target has accepted it. Anything still refused is reported
and left in place.

Two things this deliberately does not do. It does not acknowledge a position
before the document behind it is either written or recorded — if the quarantine
store cannot be written, the pipeline halts and the source replays the batch. And
it does not carry on for ever: past `max_rejects` it halts anyway, because that
many refusals is a mapping problem rather than a bad row. Nothing is lost when it
does — whatever is not recorded is also not acknowledged, so the source sends it
again once you have fixed the mapping.

## What retries and what does not

A broken stream — a dropped connection, a failover, a terminated backend — is
retried in process. The pipeline is rebuilt from the last checkpoint each time,
with exponential backoff capped at 30 seconds, and the attempt counter resets
once a connection has lasted longer than that cap. After
`[source] reconnect_max` consecutive failures (10 by default, roughly five
minutes) the process exits and hands over.

A MySQL **failover** is the case where reconnecting is not enough by itself: the
address may now point at a server whose binlog file names and offsets mean
nothing here. With GTIDs on, the checkpoint carries a position that any member of
the topology can honour, and the pipeline resumes rather than reloading —
see [surviving a failover](sources/mysql.md#surviving-a-failover) for what the
server needs and what the log says when it happens.

Configuration and privilege problems are **not** retried: `wal_level`,
publication drift, a missing table, insufficient privileges and target setup all
fail at startup and stay fatal. Retrying those is a crash loop wearing a
costume.

| Exit code | Meaning |
|---|---|
| 0 | Clean shutdown — `SIGINT`/`SIGTERM`, or `bootstrap`/`validate` finishing |
| non-zero | Fatal: bad configuration, insufficient privileges, a permanent document rejection, or reconnection gave up |

A supervisor is still worth having — it just no longer has to catch every
network blip.

## Recovery

**Process crashed.** Start it again. It resumes from the last checkpoint and
replays at most `checkpoint_interval_ms` worth of work. Replays overwrite
documents by primary key, so they are invisible in the result.

**Target lost its data.** Delete the checkpoint and restart to re-index from
scratch:

```sh
# the document is named after the stream: <source>-<slot_name|server_id>
curl -XDELETE "$OS/.pg2osync_meta/_doc/postgres-pg2osync"   # OpenSearch / Elasticsearch
rm .pg2osync-state/checkpoint-postgres-pg2osync.json        # Meilisearch
```

**Source history expired** (WAL recycled, binlog purged). The next start detects
the unusable position and runs a full initial load. Nothing to do, but expect
the load time.

**Suspected divergence.** Compare counts, then re-index if they disagree:

```sh
psql -c "SELECT count(*) FROM public.users"
curl -s "$OS/users/_count"
```

Counts can differ transiently while a batch is in flight; check twice before
concluding anything.

**One table is wrong.** Read it again, without reloading everything else:

```sh
pg2osync resnapshot -c pg2osync.toml --table public.users
pg2osync resnapshot -c pg2osync.toml --table public.users --where "tenant_id = 42"
```

Safe to run while the pipeline is streaming, and it never moves the checkpoint —
its rows carry the position they were read at, so a change committed after that
wins as it always does. Use it after a mapping change, or after fixing something
that wrote a wrong value.

Three things it does not do:

- **It does not delete.** A row gone from the source keeps its document; deciding
  that is `reconcile`'s job, and keeping the two apart keeps each explainable.
- **It does not resume.** An interruption means running it again, which costs the
  read and nothing else. Recording progress would leave bookkeeping the next
  pipeline start would read as an unfinished initial load.
- **It does not overwrite a document whose version is above the position it read
  at.** That is the same rule that lets it run beside the stream. Documents the
  pipeline wrote are always at or below the current position, so this only bites
  if you have edited the index by hand: a plain `PUT` or `DELETE` through the
  target's own API uses internal versioning, which leaves a version one past the
  source's current position, and the re-snapshot declines it until the source
  moves on. One write to the table is enough — or reach for the zero-downtime
  re-index below, which builds a fresh index and has nothing to argue with.

## Zero-downtime re-index

The index name is configuration, so a re-index is a second instance. This
sequence has been rehearsed end to end against the dev stack with a reader
polling the alias throughout:

There are two ways in. A second instance, which is what the rehearsal below
covers, or — when the mapping is the only thing changing and a gap in freshness is
acceptable — a re-snapshot into the new index name, which needs no second slot:

```sh
# point a copy of the config at users_v2, then fill it from the source
pg2osync resnapshot -c users-v2.toml --table public.users
pg2osync switch-alias -c users-v2.toml --alias users
```

That leaves the new index static from the moment the re-snapshot finished, so it
suits a one-off rebuild rather than a live cutover. For a live cutover:

```sh
# 1. Copy the config. Change `index` to users_v2 and `slot_name` to a new
#    value. Keep the same publication: both instances read it.
# 2. Start it. It runs its own initial load, then streams.
pg2osync run -c users-v2.toml

# 3. Wait for it to catch up — an exit code rather than a metric to watch.
pg2osync status -c users-v2.toml --caught-up --timeout 300

# 4. Move the alias. This is one atomic request: a reader resolving it never
#    sees a moment where it points nowhere.
pg2osync switch-alias -c users-v2.toml --alias users

# 5. Stop the old instance, then drop its slot.
pg2osync drop-slot -c users-v1.toml
```

Four things the rehearsal turned up, all of which are now handled:

- **The two instances must not share a checkpoint.** They no longer do —
  each stream keeps its own document in the target. Before that they overwrote
  each other, and whichever restarted first found a checkpoint belonging to the
  other, rejected it, and re-ran a full initial load.
- **`drop-slot` no longer drops the publication.** Both instances read the same
  one, so dropping it with the old slot took it out from under the *new*
  pipeline. Pass `--publication` when you really are decommissioning.
- **A publication dropped under a running slot is not repaired by recreating
  it.** Logical decoding reads the catalog as it was at the position being
  replayed, and at that position the new publication does not exist. The
  recovery is to drop the slot and let the pipeline run a fresh initial load.
- **Two slots mean two lots of retained WAL** until the old one is dropped, so
  do step 5 promptly. `pg2osync status` lists every slot and what each retains.

Two instances must never share a slot — each needs its own `slot_name` (or
`server_id` for MySQL).

## Upgrades

Checkpoints are forward-compatible: a newer binary reads what an older one
wrote. Rolling back across a format change makes the older binary ignore the
checkpoint and re-run the initial load, which is safe but expensive.

## Capacity notes

- Memory is bounded by the channel depths and `batch_max_bytes`; measured at
  ~90 MB resident while loading 200K docs, and lower at steady state.
- One instance is single-threaded in effect — the source, engine and sink tasks
  pipeline but do not shard. Scale by splitting tables across instances.
- The initial load's cost is dominated by the target's indexing throughput, not
  by pg2osync. `dev/benchmark.sh` reproduces the numbers in the README against a
  local stack.
- **That ceiling is what `[engine] write_concurrency` moves.** One request open
  at a time is the default and is what the figures above are measured at; the
  source is not the constraint, since a single `COPY` hands over rows more than
  twenty times faster than the pipeline consumes them. On the dev stack a 2M-row
  load went 43,000 → 87,000 rows/s from one open request to four, with little
  left past that. Raise it against your own target and watch its indexing load,
  not ours: the setting multiplies concurrent requests to a cluster that may be
  serving queries at the same time.
- **Sustained writes**: `dev/load-test.sh` drives concurrent writers and reports
  where the pipeline stops keeping up, what happens while the target is
  unavailable, and whether a `kill -9` at load loses anything. On an 8-core
  laptop against a single-node stack it keeps up with ~11,800 rows/s of
  single-row transactions and ~57,700 rows/s at a hundred rows per transaction.
  At the first figure the writers ran out of speed before the pipeline did, so
  it is a floor rather than a ceiling.
- Past that limit nothing grows without bound. The bounded channels held memory
  at 38 MB through a paused target, and the backlog accumulated as retained WAL
  on the source instead — which is the pressure `max_slot_wal_keep_size` caps.
