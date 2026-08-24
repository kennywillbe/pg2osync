# Architecture

## Overview

```
PostgreSQL (logical replication / pgoutput)
        │
        ▼
   Source task ────► Engine task ────► Sink task ────► OpenSearch
   pgoutput decode    txn buffering     _bulk API
   → ChangeEvents     coalescing        checkpoint write
        ▲                 │                  │
        └──── LSN ack / checkpoint ◄────────┘
```

Three async tasks connected by bounded channels:

1. **Source** holds one replication connection, decodes pgoutput into
   `ChangeEvent`s and forwards them with their LSN.
2. **Engine** buffers events until it sees the transaction's COMMIT, then
   releases the whole transaction at once — a partial transaction is never
   indexed. It coalesces multiple updates to the same row within a batch,
   groups rows into `_bulk`-sized batches, applies column transforms, and
   embeds nested children for insert/update documents.
3. **Sink** writes batches to OpenSearch/Elasticsearch via `_bulk` and
   persists the checkpoint (highest durably-flushed LSN) into a hidden
   `.pg2osync_meta` index. The confirmed LSN is fed back to the source so
   PostgreSQL knows which WAL can be recycled.

## Delivery semantics

- **At-least-once**: a crash between sink write and checkpoint means the last
  batch is replayed on restart. Correctness relies on idempotent writes:
  every document's `_id` is the row's primary key, so replays overwrite to
  the same value.
- **Ordering**: per-row ordering is preserved because coalescing keeps only
  the newest image; across tables there is no ordering guarantee (same as
  every CDC system without global serialization).
- **Truncate**: mapped to an index-level delete-by-query / index drop in the
  sink.

## Crash safety

- Checkpoints are written every `checkpoint_interval_ms` (default 500 ms).
- On startup, if the stored checkpoint predates the slot's restart LSN
  (WAL was recycled or the slot was recreated), pg2osync refuses to stream a
  gap and falls back to a full backfill — idempotent writes make this safe.
- `kill -9` at any point loses nothing: either the batch was flushed and
  checkpointed, or it will be replayed.

The e2e suite (`dev/e2e-test.sh`) verifies this by killing the process with
SIGKILL, writing a row during downtime, restarting, and asserting zero data
loss.

## Backfill

On first run (no usable checkpoint), the source:

1. Creates publication + replication slot (`CREATE PUBLICATION ... FOR TABLE
   ..., CREATE REPLICATION SLOT ... LOGICAL pgoutput`).
2. Opens a second connection and reads a consistent snapshot
   (`pg_export_snapshot`) taken at the slot's consistent point.
3. Streams the table contents via `COPY (SELECT ...) TO STDOUT (BINARY)`.
4. Switches to WAL streaming; because the snapshot matches the slot's LSN,
   no row is missed or double-applied.

## Source abstraction

The engine consumes only `core::ChangeEvent`. Sources are swappable behind
this boundary:

- **PostgreSQL** (`crates/source`): transport via the `pgwire-replication`
  crate (raw frames only); all pgoutput decoding, type mapping and relation
  catalog handling lives in-house.
- **MySQL/MariaDB** (`crates/source-mysql`, preview): own wire-protocol
  implementation — handshake, native/caching-sha2 auth, binlog dump — plus an
  in-house binlog event decoder. See [sources/mysql.md](sources/mysql.md).

Sinks implement the small `Sink` trait (`ensure_ready`, `write`,
`truncate_index`, `write_checkpoint`, `read_checkpoint`). Adding a new target
never touches engine code.

## Design principles

- **No CDC framework**: protocol logic is in-house, dependencies are limited
  to transport primitives.
- **Secrets env-first**: config files reference environment variables
  (`url_env`, `password_env`); plain-text secrets warn as deprecated.
- **YAGNI**: six metrics don't justify a prometheus dependency; the exposition
  endpoint is hand-rolled. Every abstraction must pay for itself.
