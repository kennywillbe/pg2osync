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
process instead of being hidden by an endless retry loop. The setup that runs
before the first stream retries under the same policy, so a source whose
database is not up yet waits for it in `reconnecting` rather than halting —
unless the failure is a refusal, which no amount of waiting changes.

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
3. filters rows against the table's `where` predicate on the raw row, then
   applies column projection, transforms, field renames and constants;
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

## Several sources in one process

`run --config-dir` starts the pipeline above once per file, in one process:

```
                    ┌─ one process ──────────────────────────────────────┐
  orders.toml  ───► │  source ─► engine ─► sink ─► checkpoint            │
  users.toml   ───► │  source ─► engine ─► sink ─► checkpoint            │
  tenant-42.toml ─► │  source ─► engine ─► sink ─► checkpoint            │
                    │                                                    │
                    │  /metrics  /healthz  /healthz/<name>  one listener │
                    │  /synced?source=<name>                one listener │
                    └────────────────────────────────────────────────────┘
```

Each row is the whole pipeline: its own channels, its own engine and sink
tasks, its own checkpoint document, its own slot or `server_id`, its own retry
policy and its own initial load. Nothing a pipeline is made of is shared,
which is what keeps the failure modes above local — a target outage on one
source is not one on another, the bounded channels of one never block the
source task of another, and a source that halts (a permanent rejection, an
exhausted quarantine, a `reconnect_max` run out) is a state rather than an
exit: it sets `pg2osync_source_state{state="halted"}`, `/healthz/<name>`
turns 503, and the rest keep streaming. The process exits non-zero only when
every source has halted, which for one config is what it always did.

There is deliberately no shared write budget either. A semaphore across the
sources would make every pipeline wait on the slowest target, which is
precisely the coupling that running them apart avoided; what the process
costs is the sum of the configurations, and sizing that is the operator's.

What is shared is the process and its two listeners. One exposition, because
concatenating one per source is not valid Prometheus text — a family's `HELP`
and `TYPE` may appear once — so every series carries `source="<name>"`
instead. `/healthz` stays an unconditional liveness answer, since failing it
for one halted source would restart the healthy ones. `/synced` names its
source per request, and a source registers with it once it can render a
position rather than before the listener opens. [operations](operations.md#health)
has the endpoints; [decisions.md](decisions.md#operating-limits) has the reasoning
behind a directory of files rather than one file of sources.

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
  `_id` is its row's primary key — or the id `[sync.x] id` renders from the
  row's raw values, and for a `fan_out` table one id per array element — so a
  replay overwrites to the same value.
- **Never acknowledge early.** The position reported to the source is clamped to
  the durable checkpoint. Acknowledging further would let the database recycle
  history for rows that are not indexed yet — the classic way CDC pipelines
  lose data on crash-restart.
- **Written or recorded, before acknowledged.** A document the target refuses
  permanently stops the pipeline by default. Where `on_permanent_rejection =
  "quarantine"` is set it is written to a hidden `.pg2osync_rejects` index
  *before* its batch's position is acknowledged, and a failure to record it halts
  instead — so the position can never sit past a document that was neither
  written nor kept. Dead-lettering while the offset advances regardless is how
  other pipelines lose the document.
- **Ordering** is guaranteed per row. Across tables there is none, as with any
  CDC system without global serialization.
- **TRUNCATE** clears the target index, ordered against pending writes.
- **A changed primary key is a move**: the document is written at its new
  identity and the old one is deleted, in that order. A crash between the two
  leaves a duplicate that the replay repairs, where the reverse order would
  leave a document nothing ever collects. An id derived from columns outside
  the key moves the same way when those columns change, which is why such a
  table requires the row's before-image from the source.
- **A `fan_out` row owns one document per array element**, id rendered from
  the merged child. Updates diff the element sets and deletes come from the
  before-image, all as ordinary versioned ops; the sink never learns the
  difference.

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

**PostgreSQL:** the slot is created first, then the load runs *beside* the
stream — not before it — on a second connection, reading each table in
primary-key ranges, each range one
`COPY (SELECT … WHERE key >= a AND key < b) TO STDOUT (FORMAT text)` in its own
short transaction. No transaction spans the load: one that did would pin the
xmin horizon for its whole duration, and autovacuum could clean nothing that
died meanwhile.

What makes that safe is not snapshot consistency. The slot exists before the
first range and nothing advances it during the load, so streaming afterwards
resumes from a position that predates every range — anything a range missed or
read stale is still in the WAL and replays onto an idempotent write.

Ranges are cut at boundaries sampled from the table itself
(`percentile_disc` over `TABLESAMPLE SYSTEM`), so they follow the real key
distribution and work for any orderable key type. They are read *unordered* on
purpose: `ORDER BY key LIMIT n` forbids a bitmap heap scan, so a row-estimate
miss degrades to a sort per range, and index order costs random heap access on
any key that is not physically correlated. A table below the range size, or one
with a composite key, is read in one piece as before.

**MySQL:** the binlog coordinate is read *before* the first chunk, then each
table is read in primary-key chunks — `WHERE key > cursor ORDER BY key LIMIT n`,
one statement each, no transaction spanning them. InnoDB's clustered index is
the table, so that walks the rows in key order and each chunk's last key is the
next chunk's cursor; the comparison is expanded rather than written as a row
constructor, which MySQL plans without a usable key. A key column whose text
form is not a faithful literal — binary, blob, bit, geometry, float — is read in
one statement instead.

Rows produced by an initial load carry position token `0`, which flushes batches
without ever advancing the checkpoint — they have no position of their own. They
do carry a document *version*: the source position read before their range or
chunk, so a row that was already stale when it was copied cannot overwrite the
streamed change that superseded it.

### Why the load and the stream overlap

Loading first and streaming afterwards has a failure that gets worse the larger
the table is: nothing acknowledges a position for the load's whole duration, so
the slot's retained WAL grows monotonically until the load ends. Past
`max_slot_wal_keep_size` PostgreSQL invalidates the slot (`wal_status = lost`),
which is unrecoverable and forces exactly the full reload that was in progress.

Running both at once is what removes that, and it needs one thing to be safe:
every document carries the position it became visible at, so a copied row that
was already stale loses to the streamed change at the target regardless of which
arrives first. Without that, a chunk read at position 100 and written after a
streamed event at position 150 for the same key leaves the row silently stale
until something touches it again.

Four rules keep the overlap from turning into a different problem:

- **Change events have strict priority over copy rows.** They arrive on separate
  channels and the engine drains the stream first. WAL is retained until it is
  consumed, so the stream cannot wait; a copy range can.
- **The copy yields under source pressure.** Before each range the slot's
  `wal_status` is checked, and while it is anything but `reserved` the copy
  pauses and lets the stream have the throughput. That is PostgreSQL's own
  signal rather than a threshold of ours — with no `max_slot_wal_keep_size`
  configured the status stays `reserved`, which is honest: there is no line to
  stay behind, and no protection either. `wal_status = lost` fails the load with
  an explanation instead of continuing into a gap.
- **A write the stream has already removed is dropped, not offered.** This is
  where versioning alone is not enough. A versioned delete leaves a tombstone
  rather than nothing, the target keeps it for `index.gc_deletes` (60s by
  default), and once it is gone `external_gte` accepts any version at all — so a
  copy row starved past that would put the document back. The engine remembers
  what the stream removed for the length of one chunk and drops such a row
  itself. The window closes at each load mark, which is sound because the load
  waits for its mark before reading the next chunk.
- **Pausing happens between ranges, never inside one.** A `COPY` held mid-stream
  would keep its snapshot open for the length of the pause, which is the long
  transaction this design exists to avoid. A range is under a second of work at
  measured rates, so waiting for one to finish costs nothing worth having.

Measured on the dev stack, 1M rows (361 MB) loading while a writer churned a
second table throughout: retained WAL oscillated and *fell* while the load was
still running — 83 MB down to 46 MB — which cannot happen in the sequential
design. With `max_slot_wal_keep_size` deliberately cut to 48 MB the slot went
`unreserved`, the load paused for 29 s, the slot recovered to 6.5 MB and
`reserved`, and the load then finished: 1,000,000 rows indexed, every streamed
update intact. The incident stayed recoverable, which is the whole point.

**MySQL overlaps too, and deliberately never pauses.** The middle rule has no
analogue there because the hazard is reversed: a slot retains WAL until it is
consumed, so a slow consumer is what invalidates it, while MySQL purges binlogs
on its own time and space policy and ignores consumers entirely. Nothing
accumulates because of pg2osync, and the thing that can go wrong — the file we
still need being purged — is made likelier by holding the load back. There is
also nothing to keep open between chunks: each chunk is one statement, and the
session runs `READ COMMITTED` so no read view outlives it.

Measured on the dev stack, 1,048,576 rows in 53 chunks while a writer churned a
second table throughout: 17.2 s at ~61,000 rows/s, no transaction older than
0 s at any sample, and `History list length` oscillating between 11 and 49
rather than climbing — the purge keeps up, which is the whole claim. The old
snapshot held one read view for the entire load, and a blocked purge is what
Percona measured filling 382,969 of ~391,000 buffer-pool pages with undo on a
1B-row table.

For the duration of the load `refresh_interval` is suspended on every
configured index, so ordinary searches see nothing new while it runs even though
the stream is live. `/synced` forces a refresh, so read-your-writes still works.

### Resuming an interrupted load

Progress is recorded per chunk in the target, one document per stream and table
in `.pg2osync_meta`. PostgreSQL stores the boundaries the table was cut at and
how many leading ranges are durably written; MySQL stores the last key written,
which is all a keyset cursor needs. Both carry whether the table finished. The order is
strict — rows, then a mark the sink reports once they are written, then the
progress document — so a crash can lose forward progress and redo a range, but
can never claim a range that was not written.

PostgreSQL's boundaries are stored rather than recomputed because they come from
a random sample: a second run would cut the table elsewhere, and "two ranges
done" would then name a different span of rows. A keyset cursor has no such
problem — the key it names is the key it names — which is why MySQL stores
nothing else.

A checkpoint is not proof that the load finished. Startup checks both, and an
unfinished table is carried on even when a checkpoint exists — trusting the
checkpoint alone is how a pipeline silently skips its load and reports success.
`dev/e2e-test.sh` and `dev/e2e-mysql-test.sh` each kill a load mid-chunk, change
the source while nothing is watching, restart, and assert both that the load
resumed and that the index matches the source exactly.

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

## Checkpoints

One document per stream in the target, named `<source>-<slot_name>` for
PostgreSQL and `<source>-<server_id>` for MySQL. Per stream rather than one
shared document, because two pipelines writing to the same target — a
zero-downtime re-index, or tables split across instances — otherwise overwrite
each other's position, and either one restarting then finds a checkpoint
belonging to the other and re-runs a full initial load.

A checkpoint written before that change lives under `default`, and is still
read when a stream has no document of its own, so an upgrade does not re-load.

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
