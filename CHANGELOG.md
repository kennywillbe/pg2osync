# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); this project is
pre-1.0, so breaking changes may land in any 0.x release.

## [Unreleased]

### Added

- TLS for MySQL and MariaDB connections, including the binlog dump, through the
  same `[source] sslmode` setting.
- `caching_sha2_password` support, so the MySQL 8 default plugin no longer
  requires creating a legacy `mysql_native_password` account. Full
  authentication sends the password inside the TLS session, or encrypted with
  the server's public key when the connection is plaintext.
- TLS for every PostgreSQL connection, including the replication stream, via
  `[source] sslmode` and `sslrootcert` with libpq semantics (`disable`,
  `prefer`, `require`, `verify-ca`, `verify-full`). Connections were previously
  always unencrypted, which made managed instances that require TLS unusable.
  **The default is now `prefer` rather than plaintext**: a server offering TLS
  gets an encrypted connection without any configuration change.

- MySQL/MariaDB source wired into the CLI end to end: prerequisite checks,
  consistent-snapshot initial load, binlog streaming with real column names
  and primary keys, and resumable binlog checkpoints. Verified against
  MySQL 8.0 and MariaDB 11.8.
- Checkpoints now record the source kind and a source-specific position, so
  PostgreSQL LSNs and MySQL binlog coordinates share one format. Checkpoints
  written by earlier versions are still readable.
- A checkpoint belonging to a different slot, server or source is rejected
  instead of being used to resume the wrong stream.
- `columns` and `exclude_columns` are now applied to every document, on both
  the initial load and live streaming.
- Nested children are embedded during the initial load, not just on later
  updates.
- `batch_max_bytes` and `txn_buffer_cap_mb` are honoured; `retry_max` and
  `retry_backoff_ms` now configure the sinks' retry behaviour.
- Helm chart, plain Kubernetes manifests, a container image and deployment docs.
- `dev/benchmark.sh`: reproducible initial-load, latency and large-transaction
  measurements; the README numbers come from it.
- Prometheus gauges `pg2osync_position_current`, `pg2osync_position_confirmed`
  and `pg2osync_position_lag`.

### Fixed

- **Checkpoints could stall indefinitely:** a commit whose rows had already
  been handed to the sink — an empty transaction, or one whose row count was an
  exact multiple of `batch_size` — advanced the in-memory position but was never
  persisted. PostgreSQL then retained WAL until the next differently-shaped
  transaction, and a restart replayed everything since the last persisted
  position. Positions now travel through the sink queue, so they are recorded in
  order and after the writes they follow.
- **Data loss on crash-restart:** the acknowledgement sent to PostgreSQL was
  not clamped to the durable checkpoint, so the server could recycle WAL for
  rows that were not indexed yet.
- **TRUNCATE was never propagated:** the WAL decoder parsed the message but
  the source dropped it, leaving deleted-in-bulk rows in the index.
- TRUNCATE is now ordered against writes already queued for the sink, and the
  target index is refreshed first — an unrefreshed write used to survive the
  truncate and resurrect a deleted row.
- Column transforms (`hash`, `redact`) were skipped on UPDATE, so a redacted
  column leaked its real value after any row update.
- MySQL binlog headers were read with the event size in place of the next-event
  position, which produced unusable resume positions.
- MySQL decimals lost their declared scale when streamed (`8.50` became `8.5`).
- MySQL row events could decode trailing padding into a phantom all-NULL row.
- MySQL null bitmaps are indexed by position among present columns, which
  matters for partial row images.
- `bootstrap` now only creates source objects and target indices instead of
  running the whole pipeline.
- Poll mode no longer breaks on an empty table, supports composite and
  non-`id` primary keys, flushes on every cycle, and ignores WAL checkpoints
  that would skip rows changed while the process was down.
- `drop-slot` also drops the publication and is idempotent.
- Retryable sink failures are classified by error type instead of by matching
  strings in messages.
- The Prometheus latency summary is valid exposition format, its quantile labels
  are `0.5`/`0.9`/`0.99` rather than `0.50`/`0.90`, and sink errors are actually
  counted.
- A never-used replication slot with a NULL `confirmed_flush_lsn` no longer
  panics on startup.

### Removed

- `[sync.*] replica_identity_full` and `[engine] flush_interval_ms`: neither
  had any effect. Replica identity is inspected from the catalog, and every
  transaction is flushed at its commit.
- `PLAN.md` and `VISION.md`: superseded by the docs in `docs/`.
