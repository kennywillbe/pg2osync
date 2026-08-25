# Design decisions

Why pg2osync is built the way it is. Code that contradicts a decision here is a
bug: change this document first, then the code.

## Change capture

**Read the replication log, not triggers or timestamps.** Logical replication
(PostgreSQL) and the binlog (MySQL) see every change including deletes, add no
write overhead to the source, and are available on managed databases. Triggers
tax every transaction; timestamp polling cannot see deletes at all — which is
why poll mode exists only as a documented fallback.

**pgoutput, not a plugin.** PostgreSQL's built-in output plugin needs nothing
installed server-side, unlike `wal2json` or `decoderbufs`.

## Protocol code is ours

**Transport is a dependency, decoding is not.** PostgreSQL replication uses the
`pgwire-replication` crate purely as a transport that hands us raw `XLogData`
frames; `tokio-postgres` does not support replication. For MySQL the whole wire
protocol is implemented in-house, because the only Rust binlog client is
unmaintained, blocking, and cannot handle split packets.

Everything above the transport — decoding, slot and publication management,
transaction buffering, checkpointing — is ours, behind our own boundary. No CDC
framework, and the transport stays swappable.

## Boundaries

**`core` depends on nothing.** It holds `ChangeEvent`, the `Sink` trait,
checkpoint types and the error taxonomy. Everything else depends on it and not
on each other, so the compiler enforces the architecture instead of discipline.

**The `Sink` trait lives in `core`, not in the sink crate.** The engine must
never import a sink implementation; the contract has to sit next to the shared
types for that to hold.

**The engine is source-agnostic.** It knows `ChangeEvent` and an opaque `u64`
position token. Whether that token is a WAL LSN or a packed binlog coordinate is
the source's business, and the binary injects a closure that renders it back to
text for the checkpoint. This is what made adding MySQL a small change rather
than a fork.

**New targets implement `Sink`.** No `match sink_kind` anywhere in the engine.

## Correctness

**At-least-once with idempotent writes.** Exactly-once across two systems
without a shared transaction is a fiction. Every document's `_id` is its row's
primary key, so a replay overwrites to the same value. Duplicates are therefore
invisible, which is what makes the snapshot-then-stream overlap safe.

**Never acknowledge a position before it is durable.** The value reported to the
source is clamped to the persisted checkpoint. Acknowledging further lets the
database recycle history for rows that are not indexed yet — the classic way a
CDC pipeline loses data on crash-restart.

**Buffer until commit.** Rows are held until their commit boundary so a partial
transaction is never presented as complete. A transaction that exceeds the batch
limits is split, which is a deliberate, documented exception: an unbounded
buffer is a worse failure than briefly observable partial state.

**Writes and truncates share one ordered channel.** Executing a truncate
directly would let writes still queued ahead of it land afterwards and
resurrect rows the source has already dropped. The target is also refreshed
before the truncate, because `delete_by_query` only removes documents a search
can see.

**Stop on permanent rejection.** A document the target will never accept halts
the pipeline. Skipping it would be silent data loss, and every later batch would
widen the divergence.

**A checkpoint is bound to its stream.** It records the source kind, the slot or
`server_id`, and the publication. A checkpoint from another stream is rejected
rather than used to resume into an unrelated position space.

## Initial load

**No exported snapshot; short transactions and replay instead.** The obvious
design exports one transaction snapshot and reads every chunk from it, which
keeps a read view open for the whole load: `VACUUM` cannot then clean anything
that died after it started, and on MySQL a long read view makes purge block
rather than lag. What makes our load safe is not snapshot
consistency but that streaming resumes from a position that predates every
chunk: on PostgreSQL because the slot exists before the first range and nothing
advances it during the load, on MySQL because the binlog coordinate is read
before the first chunk. Anything a chunk missed or read stale is still in the
log and replays onto an idempotent write. Two conditions that argument rests on: writes are
whole-document upserts keyed by the row's primary key, and an update whose
unchanged TOASTed columns arrive as markers is completed from the stored
document — without that, a replayed update would erase a value a range read
correctly.

**The table is cut the way the storage engine reads it.** PostgreSQL's heap
order says nothing about the key, so ranges are sampled in advance and read
unordered: `ORDER BY key LIMIT n` forbids a bitmap heap scan, and index order
costs random heap access on any key that is not physically correlated. InnoDB's
clustered index *is* the table, so MySQL does the opposite — `WHERE key > cursor
ORDER BY key LIMIT n` walks the rows themselves, nothing is sampled, and each
chunk's last key is the next chunk's cursor. That also makes the MySQL resume
point exact, where PostgreSQL has to store its boundaries because a second
sample would cut the table elsewhere.

**The cursor comparison is never a row constructor.** `(a, b) > (x, y)` says
exactly what the expanded `(a > x) OR (a = x AND b > y)` says, and MySQL plans
it as `type: index` with no usable key while the expansion plans as
`type: range`. Measured on a composite key, for 1000 rows returned: 1000 rows
read expanded, 2000 read as a row constructor, restarting at the head of the
index every chunk — so the multiplier grows with how far the cursor has
travelled. MySQL bug #111952, closed as not-a-bug with a worklog in its place.
No `IS NOT NULL` guard accompanies the comparison even though MySQL sorts NULLs
first: a `PRIMARY KEY` column is `NOT NULL` whether it was declared so or not.

**Every document carries the position it became visible at, as a target
document version.** Streamed rows carry their commit position, copied rows the
position read before their range. A copied row that is already stale therefore
loses to the streamed change at the target, whichever order the two arrive in,
and a version conflict is success rather than a rejection. It is deliberately
separate from the checkpoint token: a copied row needs a version and must never
advance a position. Poll mode, which has no position at all, writes no version
and relies on ordering alone.

**MySQL versions by its binlog coordinate, not by a GTID.** `(file index << 32)
| offset` is monotonic across rotation and was already the ordering token, and a
transaction's events are written to the binlog as one group at commit — so no
position inside a group can predate a coordinate a reader saw earlier, which is
what makes an event's own offset a sound version. A GTID could not be one: it is
`source_uuid:N` with `N` restarting at 1 for each UUID, so a GTID set has no
order as an integer. The cost of using the offset is that the space is per
server and per binlog history: if that history restarts, the target holds
versions from a numbering that no longer exists and would reject everything
written under the new one, so a position behind the checkpoint refuses to start
rather than reloading into silence.

**A write the stream has already removed is dropped, not offered.** Versioning
alone does not make the overlap safe, and this is the one place it does not. A
versioned delete leaves a tombstone carrying the delete's version, the target
keeps that tombstone only for `index.gc_deletes` — 60s by default — and once it
is gone `external_gte` accepts *any* version, including one below the delete's.
So a copied row starved behind a busy stream for longer than that would put the
document back. Measured against a real target at `gc_deletes = 1s`: the same
write is refused with a 409 immediately and accepted two seconds later. `TRUNCATE`
has the same shape, since it clears an index with versioned deletes.

The engine therefore remembers what the stream removed and drops a copied row
that is older than the removal, rather than asking the target for a comparison it
cannot make. This is the move DBLog makes for the same problem — it buffers a
chunk and removes every key the log touched between two watermarks — except that
watermarks exist there to substitute for versions, and versions already order
everything else here, so only the case they cannot express needs it.

What bounds the state is the load's own protocol: a chunk's rows, then a mark,
then a *wait* for that mark to be written. When a mark arrives, every row of its
chunk has been handed over and none of the next chunk can exist yet, so the
window closes and the memory is one chunk's worth of deletes rather than the
load's. A loader that sent the next chunk before its mark was confirmed would
reopen the hole silently, which is why the ordering is stated in both places.

Raising `index.gc_deletes` for the duration of the load was the alternative and
is rejected: it moves the window instead of closing it, costs target heap for
every tombstone it holds, and an interrupted load would leave the setting raised
the way one already leaves `refresh_interval` at `-1`.

**The load runs beside the stream, not before it.** Loading first means nothing
acknowledges a position for the load's whole duration, so retained WAL grows
monotonically and a large enough table invalidates the slot — which forces the
full reload the load was trying to finish. Alternating copy and catch-up phases
does not fix it either: on PostgreSQL a paused consumer freezes `restart_lsn`
whether it detaches or merely stops reading, so the only thing that releases WAL
is continuing to consume it. Document versioning is what makes the overlap safe;
change events take strict priority over copy rows, and the copy pauses between
ranges while the slot's `wal_status` is anything but `reserved`.

**On MySQL the load overlaps the stream but never pauses for it.** The hazard
runs the other way there: a slot retains WAL until it is consumed, so a slow
consumer is what invalidates it, while MySQL purges binlogs on its own time and
space policy and ignores consumers entirely. Nothing accumulates because of us,
and what can go wrong — the file we still need being purged — is made likelier
by holding the load back, not less. So there is no `wal_status` analogue to
watch and deliberately no pause.

**Load progress is recorded per range, in the target, behind a durability
barrier.** The order is strict: rows, then a mark the sink reports once they are
written, then the progress document. A crash can therefore lose forward
progress and redo a range, which idempotent writes make free, but can never
claim a range that was not written. What the progress says depends on how the
table was cut: PostgreSQL stores its sampled boundaries alongside a count of
finished ranges, because recomputing them would cut the table elsewhere and the
count would name a different span of rows; MySQL stores the last key written,
which needs nothing else to be exact. A checkpoint alone is not proof the
load finished — the two are separate facts, and conflating them is what
silently skips a load.

## Checkpoints

**State lives in the target.** A hidden `.pg2osync_meta` index holds one
document per stream; per-document atomicity gives the crash safety for free,
with no compare-and-swap. Per stream rather than one shared document, because
two pipelines writing to the same target otherwise overwrite each other's
position — which is what a zero-downtime re-index runs, and what splitting
tables across instances means. A local file breaks on ephemeral containers, and a table in
the source database pollutes the user's schema and risks replicating itself.
Meilisearch has nowhere to put an arbitrary document, so it uses a
write-then-rename state file — the documented exception.

**One position format for every source.** The document stores an ordering token
plus the source's own textual position. Documents written by earlier versions,
which stored only an LSN, are still readable: refusing them would force a full
re-index on upgrade.

## Types

**`numeric` and `decimal` become JSON strings.** A float round-trip loses
precision, and these columns are usually money. MySQL decimals keep their
declared scale so a streamed value matches what the initial load read.

**Unknown types become strings.** Enums, domains, ranges and composites are
passed through as text rather than guessed at.

**`bytea`, blobs and geometry become base64.** Binary cannot go into JSON any
other way.

## Scope

**One-way replication only.** No bidirectional sync, no conflict resolution.

**Schema drift is reported, never applied.** pg2osync will not run DDL on the
target's behalf. A publication that does not match the configuration is an
error, not something to silently fix. A table whose columns change under a
running pipeline is logged, naming what was added, removed or retyped — the
index and the database now disagree about what a row looks like, and only a
rebuild closes that. Which is why the index name is configuration.

**No relational sources beyond PostgreSQL and MySQL/MariaDB**, and no
non-relational sources. The value is depth on these, not breadth.

**Nested children stay one level deep.** Deeper denormalization belongs in a
view or in the application.

## Implementation choices

**Hand-rolled metrics endpoint.** Six counters and one summary do not justify a
Prometheus client plus an HTTP framework in a binary that advertises having no
dependencies.

**Batch reads with `COPY … (FORMAT text)`.** Text parsing measured fast enough
(~21k docs/s end to end) that binary format's complexity is not yet justified.

**Secrets from the environment.** Every secret has an `*_env` form. Inline
values still work but warn, because config files end up in version control.

**Errors: `thiserror` in libraries, `anyhow` only in the binary.** Callers get
matchable variants; the CLI gets readable messages.

**YAGNI on configuration.** An option that does nothing is worse than a missing
one, because it implies a guarantee. Options that had no effect were removed
rather than documented.
