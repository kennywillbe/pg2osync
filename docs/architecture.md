# Architecture

## The pipeline

```
  source task            engine task              sink task
┌──────────────┐  mpsc  ┌──────────────┐  mpsc  ┌──────────────┐
│ decode WAL / │ 10k    │ buffer until │ 64     │ bulk write   │
│ binlog into  ├───────►│ COMMIT, then ├───────►│ + truncate,  │
│ ChangeEvents │  ▲     │ batch + map  │        │ in order     │
└──────────────┘  │     └──────┬───────┘        └──────┬───────┘
       ▲          │            │                       │
       │      backpressure     ▼                       ▼
       │                 checkpoint task ◄──── acknowledged position
       └──────── durable position (clamps what may be acked)
```

Three tokio tasks joined by **bounded** channels. The bounds are the entire
backpressure mechanism: a slow sink fills the batch channel, which blocks the
engine, which fills the event channel, which stops the source from reading —
so the database retains its log instead of the process growing without limit.

### Source task

Owns one replication connection and turns the wire protocol into
`core::ChangeEvent` values: `Row` (insert/update/delete), `TableTruncated`, and
`Transaction(Commit)` boundaries carrying a position token.

Everything protocol-specific stays here. PostgreSQL pgoutput decoding lives in
`crates/source`, MySQL binlog decoding in `crates/source-mysql`.

A source error tears the pipeline down and rebuilds it from the last
checkpoint — new channels, a new engine task, a fresh connection. Rebuilding
fully is the point rather than the cost: a partially buffered transaction is
invalid once the stream repositions, and the teardown is what discards it.
Retries back off exponentially and stop after `[source] reconnect_max`
consecutive failures, so a real outage still reaches whatever supervises the
process instead of being hidden by an endless retry loop.

### Engine task

Source-agnostic by construction — it knows `ChangeEvent`, the `Sink` trait, and
nothing else. It:

1. buffers rows until their `Commit` arrives, so a partial transaction is never
   handed to a sink;
2. splits a transaction that exceeds `batch_size` or `batch_max_bytes` across
   requests (safe because every write is idempotent, and the commit position
   lands on the final piece), and conversely lets whole transactions accumulate
   for up to 10 ms when more events are already waiting — a commit is what
   forces a batch, so without it a stream of single-row transactions costs one
   request each. The batch's highest position stays the last commit in it, so
   an ack can never run past a transaction that was not fully written;
3. applies column projection and transforms;
4. completes unchanged-TOAST columns by reading the previously indexed
   documents — for a whole group of rows in one request, not one round-trip
   per row;
5. maps `(schema, table)` to a target index.

### Sink task

Executes writes and truncates **in the order the engine produced them**. Both
travel through the same channel: running a truncate directly would let writes
still queued ahead of it land afterwards and resurrect rows the source has
already dropped.

### Checkpoint task

Every `checkpoint_interval_ms` it persists the highest acknowledged position.
Only after that write succeeds does the *durable position* advance — and that
value is what the source is allowed to acknowledge upstream.

## Positions

The engine treats a source position as an opaque, monotonically increasing
`u64` token:

| Source | Token | Stored text |
|---|---|---|
| PostgreSQL | WAL LSN as `u64` | `0/1B4F2A8` |
| MySQL/MariaDB | `(binlog file index << 32) \| offset` | `binlog.000004:1234` |

Packing the file index into the high bits makes a rotation compare greater than
any offset in the previous file, so ordering holds across rotations. The binary
supplies a closure that renders the token into the source's own textual form,
which is what a restart parses to resume.

## Delivery semantics

- **At-least-once.** A crash between a sink write and the checkpoint replays the
  last batch on restart. Correctness rests on idempotent writes: a document's
  `_id` is its row's primary key, so a replay overwrites to the same value.
- **Never acknowledge early.** The position reported to the source is clamped to
  the durable checkpoint. Acknowledging further would let the database recycle
  history for rows that are not indexed yet — the classic way CDC pipelines
  lose data on crash-restart.
- **Ordering** is guaranteed per row. Across tables there is none, as with any
  CDC system without global serialization.
- **TRUNCATE** clears the target index, ordered against pending writes.
- **A changed primary key is a move**: the document is written at its new
  identity and the old one is deleted, in that order. A crash between the two
  leaves a duplicate that the replay repairs, where the reverse order would
  leave a document nothing ever collects.

## Read-your-writes

The pipeline is asynchronous, so a caller that just committed cannot assume its
change is searchable. `GET /synced` closes that gap on request: it waits until
the acknowledged position passes the source's position and, with
`refresh=true`, until the target has made the writes searchable.

Waiting on the *acknowledged* position rather than the checkpoint is deliberate.
The checkpoint exists for crash recovery and is written on an interval; the
acknowledgement is the moment the target accepted the write, which is what the
caller actually cares about.

Two source-specific details make this work at all:

- **PostgreSQL skips transactions that touch no published table.** The position
  a caller reads from `pg_current_wal_lsn()` therefore includes activity the
  pipeline will never see, and on a quiet database the gap never closes. When
  the endpoint finds itself behind, it emits a logical decoding message
  (`pg_logical_emit_message`) — a marker the stream does carry, written without
  touching any table and without needing DDL privileges.
- **MySQL's binlog is server-wide**, so the position a caller reads is reached
  by the commit's own `XID` event. A heartbeat period is requested on the dump
  connection as a fallback for a genuinely idle server.

Measured on the dev stack: 20 writes each followed immediately by a search, zero
misses, `/synced` returning in 5 ms at the median. MySQL and MariaDB the same.

## Crash safety

On startup pg2osync reads the checkpoint and refuses to use one that does not
belong to this stream — a different source kind, slot or `server_id` means a
full initial load instead of resuming into the wrong position space.

For PostgreSQL it additionally compares the checkpoint with the slot's
`confirmed_flush_lsn`. A checkpoint *behind* the slot is unusable: streaming
would resume at the slot's position and the gap between them would be lost, so
the initial load runs again.

`dev/e2e-test.sh` verifies this by `SIGKILL`ing the process, writing rows during
the downtime, restarting, and asserting nothing is missing.

## Initial load

**PostgreSQL:** the slot is created first, then a second connection opens a
`REPEATABLE READ READ ONLY` transaction and reads each table with
`COPY (SELECT …) TO STDOUT (FORMAT text)`. Because the snapshot is taken after
the slot exists, the overlap between snapshot and stream can only produce
duplicates, never a gap — and duplicates are harmless. Synthetic commit
boundaries every few thousand rows keep the engine flushing during large loads.

**MySQL:** `START TRANSACTION WITH CONSISTENT SNAPSHOT`, then the binlog
coordinate is read *inside* that transaction and every table is selected. InnoDB
establishes the read view at the start of the transaction, so streaming from
that coordinate can only re-deliver rows.

Rows produced by an initial load carry position token `0`, which flushes batches
without ever advancing the checkpoint — they have no position of their own.

### Target-side cost

One read of the table, and the rest of the cost on the target: 15M rows at the
default `batch_size` of 500 is one `COPY` and roughly 30,000 bulk requests.

For the duration of the load `refresh_interval` is set to `-1` and
`number_of_replicas` to `0`, and both are put back afterwards — the standard
bulk-load recipe. Nothing searches an index that is still being filled, so
refreshing it every second and writing replicas is work nobody is waiting for.
Two honest caveats:

- **It is not always worth anything.** Measured on a single-node development
  cluster loading 200k narrow rows, it made no measurable difference: with one
  node the replicas are unassigned and a two-second load spans two refreshes.
  The work it removes only bills on a cluster that really has replicas and a
  load that runs for minutes.
- **An interrupted load leaves refresh suspended**, and the symptom is nasty:
  writes are accepted, the pipeline looks healthy, and searches return nothing.
  A later load treats `-1` as "no saved value" rather than restoring it, and
  startup warns about any configured index still in that state.

Serverless targets skip both, since they manage refresh and replication
themselves and reject the call.

## Row fidelity

- **Unchanged TOAST** (PostgreSQL): an UPDATE omits large unchanged columns. If
  the table has `REPLICA IDENTITY FULL` the old tuple supplies the value;
  otherwise the previously indexed document is read back through
  `Sink::get_documents`.

  The engine takes every row already waiting on its channel before doing that,
  so the read is one request for the group rather than one per row. It waits
  for nothing, so a row arriving alone is unaffected. `dev/toast-cost.sh`
  measures it: 20,000 updates to a table with an 8 kB out-of-line column ran at
  1,800 rows/s when each row read on its own and 4,800 rows/s batched, against
  4,400 rows/s for the same table at `REPLICA IDENTITY FULL`. The read-back is
  therefore no longer a reason to pay FULL's 1.5x WAL, and the counter
  `pg2osync_toast_readbacks_total` says how often it happens.
- **Types.** `numeric` and `decimal` become JSON strings, because a float
  round-trip loses precision. `bytea`, MySQL blobs and geometry become base64.
  `json`/`jsonb` are parsed into real JSON. Unknown types fall back to strings.
- **Partitioned tables** (PostgreSQL): publications are created with
  `publish_via_partition_root = true`, so events arrive under the parent
  relation and match the configuration.
- **MySQL row images** require `binlog_row_image = FULL`. The null bitmap is
  indexed by position among *present* columns, which is what makes partial
  images decode correctly at all.

## Extension points

Adding a target means implementing `Sink` (`ensure_ready`, `get_documents`,
`write`, `truncate_index`, `write_checkpoint`, `read_checkpoint`, `health`). No
engine code changes, and the engine never matches on a sink kind.

Adding a source means producing `ChangeEvent`s with position tokens. The engine
cannot tell the difference.

Rationale for these boundaries is in [decisions.md](decisions.md).
