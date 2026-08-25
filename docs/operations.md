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

## Day-to-day commands

```sh
pg2osync validate -c pg2osync.toml   # config, connectivity, server settings
pg2osync status   -c pg2osync.toml   # checkpoint vs the source's position
pg2osync bootstrap -c pg2osync.toml  # create slot/publication/indices, then exit
pg2osync drop-slot -c pg2osync.toml  # teardown when decommissioning
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
| `halting pipeline: permanent rejection …` | The target refuses the document (usually a mapping conflict) | Fix the mapping or exclude the column, then restart |
| `binlog_format is "STATEMENT"` | MySQL not row-based | `binlog_format = ROW` in `my.cnf`, restart |
| `server switched to 'caching_sha2_password'` | MySQL user's auth plugin | Recreate the user with `mysql_native_password` |
| `bogus data in log event` | Resuming at an invalid binlog offset | Delete the checkpoint document to force a fresh initial load |
| Checkpoint ignored, full load runs | Checkpoint belongs to another slot/`server_id`/source | Expected. Restore the original identifier or accept the reload |

A permanent rejection stops the pipeline on purpose: skipping the document would
be silent data loss, and every batch after it would widen the divergence.

## What retries and what does not

A broken stream — a dropped connection, a failover, a terminated backend — is
retried in process. The pipeline is rebuilt from the last checkpoint each time,
with exponential backoff capped at 30 seconds, and the attempt counter resets
once a connection has lasted longer than that cap. After
`[source] reconnect_max` consecutive failures (10 by default, roughly five
minutes) the process exits and hands over.

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
curl -XDELETE "$OS/.pg2osync_meta/_doc/default"    # OpenSearch / Elasticsearch
rm .pg2osync-state/checkpoint.json                 # Meilisearch
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

## Zero-downtime re-index

The index name is configuration, so a re-index is a second instance:

1. Copy the config, change `index` to `users_v2` and `slot_name` to a new value.
2. Run it. It performs its own initial load and then streams.
3. When `pg2osync_position_lag` is near zero, move your search alias to
   `users_v2`.
4. Stop the old instance and `drop-slot` it.

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
