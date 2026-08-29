# Changelog

**Maintained by [release-please](https://github.com/googleapis/release-please)
from here on.** It reads the commit subjects and keeps one open pull request that
bumps the versions and writes the entry below; merging that pull request tags and
publishes. The 1.0.0 entry was written by hand because no commit before it
followed the convention. See [docs/releasing.md](../../docs/releasing.md).

Notable changes, newest first. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the versions
follow [Semantic Versioning](https://semver.org/) — the version is `1.0.0`, the
git tag is `v1.0.0`, which is the convention SemVer's own FAQ describes.

What a major version promises here: the **configuration file** and the
**checkpoint format** stay readable across minor releases. A change that would
make an existing `pg2osync.toml` stop loading, or make a running pipeline
re-read a table from the start, is a major version.

## [1.2.0](https://github.com/kennywillbe/pg2osync/compare/v1.1.0...v1.2.0) (2026-08-29)


### Features

* rename columns in the target document ([#77](https://github.com/kennywillbe/pg2osync/issues/77)) ([6ba3bb1](https://github.com/kennywillbe/pg2osync/commit/6ba3bb1803db7f67ce18fb4d0ff757bb4dbc8dcc))
* add fields that come from no column ([#80](https://github.com/kennywillbe/pg2osync/issues/80)) ([3569e08](https://github.com/kennywillbe/pg2osync/commit/3569e087150bcebba6dc38121abd50aa6bf9c8a1))


### Bug Fixes

* do not transform a TOAST-completed value a second time ([#80](https://github.com/kennywillbe/pg2osync/issues/80)) ([3569e08](https://github.com/kennywillbe/pg2osync/commit/3569e087150bcebba6dc38121abd50aa6bf9c8a1))

## [1.1.0](https://github.com/kennywillbe/pg2osync/compare/v1.0.0...v1.1.0) (2026-08-28)


### Features

* configurable document ids and one-row-to-many fan-out ([#72](https://github.com/kennywillbe/pg2osync/issues/72)) ([8615c1b](https://github.com/kennywillbe/pg2osync/commit/8615c1b04a71daca33a925de6548155b32c68ed5))

## 1.0.0 — not yet tagged

Pushing `v1.0.0` is what publishes it: the release workflow builds static
binaries for Linux and macOS on x86-64 and arm64 and pushes the container image.
Until that tag exists there is no downloadable release, whatever the version
numbers in `Cargo.toml` say. This first one is tagged by hand; releases after it
come from merging the release pull request.

### Added

- `pg2osync init` writes a starter config, qualifying table names from the
  source's catalogue and refusing a table with no primary key. Every command now
  defaults to `pg2osync.toml`, so the whole sequence is `init`, `validate`, `run`
  with no flags.
- Nested child collections for MySQL and MariaDB, on the same terms as
  PostgreSQL: one query per collection per transaction, deterministic order, and
  a document that says so when its array was cut short.
- GTID checkpoints for MySQL and MariaDB, so a promoted replica can be resumed
  from rather than reloaded. Document versions carry a generation, which is what
  makes writes after a failover outrank what came before them.
- `[engine] write_concurrency`: more than one write request open against the
  target. Measured 43,000 → 87,000 rows/s on a 2M-row load at four.
- `[source] load_workers`: ranges of the initial load read in waves. Worth it
  for tables with a nested collection, where the server aggregates per parent
  row — 53% there, 5–8% on ordinary tables.
- Replication-slot retention is reported (`pg2osync_slot_retained_bytes`,
  `pg2osync_slot_wal_status`), and `status --max-retained-mb` exits non-zero over
  a limit so a scheduled check catches a pipeline that has been down.

### Fixed

- Renaming a replicated table panicked a worker thread and put the pipeline into
  a crash loop, replaying the same row on every reconnect. Its rows are now
  dropped with one warning naming the cause.
- A tombstone expiring under copy starvation could let a loaded row resurrect a
  deleted document.
- MySQL binary and blob columns decoded through `from_utf8_lossy`, so their
  base64 was wrong.

### Removed

- The Amazon OpenSearch Serverless profile. It was never run against the
  service, and two of its obstacles are structural: SigV4 is the only
  authentication a collection accepts, and a custom document id — which every
  document here carries — works only on a search collection. An
  `*.aoss.amazonaws.com` url is now refused at startup with the reason.

### Documented, with measurements

- What the source database pays while busy: **0.4%** of foreground throughput
  and **0.19 of a core** at ~14,000 transactions a second. The 40% drop seen
  when everything shares one machine is co-location, not replication.
- The pipeline needs **one core**; starved to a quarter it is five times slower
  on 9 MB of memory, because falling behind costs latency and retained WAL
  rather than growing memory.
- A table costs about **46 ms** of fixed setup, and neither memory nor
  per-transaction bookkeeping grows with the number of tables.
- What a distant source or target costs is **not** documented, because it could
  not be measured honestly on one machine: adding 50 ms of delay reproducibly
  makes the load *faster*, which is contention relieving itself.
