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

## [1.4.0](https://github.com/kennywillbe/pg2osync/compare/v1.3.0...v1.4.0) (2026-08-30)


### Features

* a Grafana dashboard ([#121](https://github.com/kennywillbe/pg2osync/issues/121)) ([ff4a56e](https://github.com/kennywillbe/pg2osync/commit/ff4a56e92f6d8771aaccbca4a2047f6dccf4398a)), closes [#105](https://github.com/kennywillbe/pg2osync/issues/105)
* a schema drift metric, reported on MySQL too ([#126](https://github.com/kennywillbe/pg2osync/issues/126)) ([149af68](https://github.com/kennywillbe/pg2osync/commit/149af681c4f7e5d4e2c0ecd8b506591da566c4b8)), closes [#104](https://github.com/kennywillbe/pg2osync/issues/104)
* an atomic full rebuild on Meilisearch through swap-indexes ([#136](https://github.com/kennywillbe/pg2osync/issues/136)) ([353f3d9](https://github.com/kennywillbe/pg2osync/commit/353f3d95d9ec67f5e5f51eaa1310b150135c527e)), closes [#108](https://github.com/kennywillbe/pg2osync/issues/108)
* client-certificate authentication for the source connections ([#125](https://github.com/kennywillbe/pg2osync/issues/125)) ([2ef18e0](https://github.com/kennywillbe/pg2osync/commit/2ef18e09ac96ea1ef8130044b7c3c5311dfb1653)), closes [#103](https://github.com/kennywillbe/pg2osync/issues/103)
* column projection inside a child collection ([#132](https://github.com/kennywillbe/pg2osync/issues/132)) ([e5d09e3](https://github.com/kennywillbe/pg2osync/commit/e5d09e308f17dcc1d06b3942d9aa34bee811ad9f)), closes [#111](https://github.com/kennywillbe/pg2osync/issues/111)
* embed a one-to-one child as an object ([#134](https://github.com/kennywillbe/pg2osync/issues/134)) ([1dae947](https://github.com/kennywillbe/pg2osync/commit/1dae9478327bc997b0316e464e13ca5deace8b8f)), closes [#110](https://github.com/kennywillbe/pg2osync/issues/110) [#128](https://github.com/kennywillbe/pg2osync/issues/128)
* JSON log lines ([#130](https://github.com/kennywillbe/pg2osync/issues/130)) ([331c21c](https://github.com/kennywillbe/pg2osync/commit/331c21cba2e26ebcf79c02c89bd98b2d5248a07d)), closes [#102](https://github.com/kennywillbe/pg2osync/issues/102)
* per-document routing from a column ([#129](https://github.com/kennywillbe/pg2osync/issues/129)) ([89946a5](https://github.com/kennywillbe/pg2osync/commit/89946a53ffcb478a8d881e5e909aa2c0eaf90791)), closes [#109](https://github.com/kennywillbe/pg2osync/issues/109)
* rebuild one table's index and flip an alias onto it ([#135](https://github.com/kennywillbe/pg2osync/issues/135)) ([ada1cef](https://github.com/kennywillbe/pg2osync/commit/ada1cefff433896d7906eba27a35175bc2940212)), closes [#107](https://github.com/kennywillbe/pg2osync/issues/107)


### Bug Fixes

* a DDL replayed after a crash no longer wedges the MySQL stream ([#137](https://github.com/kennywillbe/pg2osync/issues/137)) ([593ffab](https://github.com/kennywillbe/pg2osync/commit/593ffab34ea6c1663716c6d862badf19c422054b)), closes [#133](https://github.com/kennywillbe/pg2osync/issues/133)
* a Meilisearch pipeline can be restarted ([#124](https://github.com/kennywillbe/pg2osync/issues/124)) ([67825c5](https://github.com/kennywillbe/pg2osync/commit/67825c5ced06102b4859af5efcf3e98def6aee5e)), closes [#122](https://github.com/kennywillbe/pg2osync/issues/122)
* a quoted "NULL" array element stays a string ([#113](https://github.com/kennywillbe/pg2osync/issues/113)) ([3b255fe](https://github.com/kennywillbe/pg2osync/commit/3b255fee4c2a20164e584bebd44d58ebf7ba3536)), closes [#96](https://github.com/kennywillbe/pg2osync/issues/96)
* reassemble MySQL messages that span more than one packet ([#114](https://github.com/kennywillbe/pg2osync/issues/114)) ([8826e14](https://github.com/kennywillbe/pg2osync/commit/8826e148a893e08977d520940ffa0c1ce0366070)), closes [#95](https://github.com/kennywillbe/pg2osync/issues/95)
* reconcile and switch-alias work on Elasticsearch ([#127](https://github.com/kennywillbe/pg2osync/issues/127)) ([6237b53](https://github.com/kennywillbe/pg2osync/commit/6237b533f7f24e9d977c71bf8dafe30a51687225)), closes [#118](https://github.com/kennywillbe/pg2osync/issues/118)
* SIGTERM drains and checkpoints like SIGINT ([#119](https://github.com/kennywillbe/pg2osync/issues/119)) ([475a12d](https://github.com/kennywillbe/pg2osync/commit/475a12d4d6686e443701d6edf3f43beba13e6d74)), closes [#98](https://github.com/kennywillbe/pg2osync/issues/98)
* the Elasticsearch sink no longer ignores a failed refresh ([#115](https://github.com/kennywillbe/pg2osync/issues/115)) ([a7aa7d9](https://github.com/kennywillbe/pg2osync/commit/a7aa7d99943d9645ccb63f1e5bd930ec2e54dfa9)), closes [#97](https://github.com/kennywillbe/pg2osync/issues/97)

## [1.3.0](https://github.com/kennywillbe/pg2osync/compare/v1.2.0...v1.3.0) (2026-08-29)


### Features

* a row chooses its index ([#91](https://github.com/kennywillbe/pg2osync/issues/91)) ([0939822](https://github.com/kennywillbe/pg2osync/commit/09398227e1c7343af93b4cd16fb0d1c7b794931b))
* an ingest pipeline per section, so the target computes vector fields ([#92](https://github.com/kennywillbe/pg2osync/issues/92)) ([1da79dc](https://github.com/kennywillbe/pg2osync/commit/1da79dccf1510af7e12ac9c7da6dcccf2b51bbb5))
* append-only tables without a primary key ([#93](https://github.com/kennywillbe/pg2osync/issues/93)) ([e6ab837](https://github.com/kennywillbe/pg2osync/commit/e6ab837964abc10fe17abc46b6c7c6558e02fe7e))
* join field and per-document routing ([#88](https://github.com/kennywillbe/pg2osync/issues/88)) ([16d2b56](https://github.com/kennywillbe/pg2osync/commit/16d2b566555c0e9d7cea6a8139886cdfa07efe8b))
* one index fed by several tables ([#89](https://github.com/kennywillbe/pg2osync/issues/89)) ([7efef40](https://github.com/kennywillbe/pg2osync/commit/7efef40a6da58897e58fb62abdf0b83e1dda2228))
* parse, split, number and date transforms ([#85](https://github.com/kennywillbe/pg2osync/issues/85)) ([7a46aff](https://github.com/kennywillbe/pg2osync/commit/7a46aff54e44c237a006a6aa72e2505510124f4b))
* row filters that the load pushes down and the stream evaluates ([#87](https://github.com/kennywillbe/pg2osync/issues/87)) ([c392efe](https://github.com/kennywillbe/pg2osync/commit/c392efe940dcd267a455c8b278deb9ec18d66756))


### Bug Fixes

* file a REPLICA IDENTITY FULL row under its primary key ([#90](https://github.com/kennywillbe/pg2osync/issues/90)) ([9eabc04](https://github.com/kennywillbe/pg2osync/commit/9eabc045ab87ae2ad1224b30a4fcfbb66e275c1a))

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
