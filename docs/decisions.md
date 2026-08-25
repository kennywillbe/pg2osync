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

## Checkpoints

**State lives in the target.** A hidden `.pg2osync_meta` index holds one
document; per-document atomicity gives the crash safety for free, with no
compare-and-swap. A local file breaks on ephemeral containers, and a table in
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
