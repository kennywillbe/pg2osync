# Compatibility

Which versions CI actually runs, as opposed to which ones are expected to
work. "Not tested" is not "known broken" — it is a version no job exercises,
so a regression there would ship unnoticed.

| Component | Version | Covered by |
|---|---|---|
| PostgreSQL | 17 | every pull request |
| PostgreSQL | 15 (the declared floor) | nightly |
| PostgreSQL | 16, 18 | not tested |
| MySQL | 8.0 | every pull request |
| MySQL | 8.4 LTS | nightly |
| MariaDB | 10.6 (the declared floor) | nightly |
| MariaDB | 11.8 LTS | nightly |
| OpenSearch | 2.19.6 | every pull request |
| OpenSearch | other 2.x | not tested |
| Elasticsearch | 8.19.20 | nightly, full suite — advisory until [#118](https://github.com/kennywillbe/pg2osync/issues/118) |
| Elasticsearch | 7.x | not tested, known gaps |
| Meilisearch | v1.53.1 | nightly, smoke suite only — advisory until [#122](https://github.com/kennywillbe/pg2osync/issues/122) |

## What the nightly suite runs

`.github/workflows/compat.yml` builds the release binary once and hands it to
every cell. Three scripts do the work:

- `dev/e2e-test.sh` — the full PostgreSQL suite. `TARGET_FLAVOR` picks
  OpenSearch or Elasticsearch; everything else is identical, because the two
  differ only in REST dialect details the sink hides.
- `dev/e2e-mysql-test.sh` — the full MySQL/MariaDB suite. `MYSQL_CLIENT`
  picks the client binary the container ships.
- `dev/e2e-meili-smoke.sh` — Meilisearch. Not the full suite: that one
  asserts over mappings, join fields and per-row indices, none of which
  Meilisearch has. The smoke suite covers the initial load, live
  INSERT/UPDATE/DELETE, the file-based checkpoint resuming after a restart,
  and a `reindex` swapping a rebuilt index into the live name.

Two cells are marked advisory, because the first nightly matrix found a bug
in each of them. The Elasticsearch suite reaches `reconcile`, which that sink
cannot run ([#118](https://github.com/kennywillbe/pg2osync/issues/118)), so
everything after that section is unverified there. The Meilisearch smoke suite
reaches the restart, which fails because that sink cannot start twice against
one index ([#122](https://github.com/kennywillbe/pg2osync/issues/122)). Both
cells are kept red rather than trimmed: the gap is the finding.

The matrix also runs on a pull request that touches the workflow or those
scripts, so a change to the matrix is tested before the night it would break.

## When a nightly run fails

The workflow opens one issue labelled `nightly-compat` and comments the run
URL and the failed cells on it every night it stays red, rather than opening a
new issue each time. Fix the cell or, if the version genuinely is not
supported, say so here and in the README — a claim no job checks is the thing
this page exists to prevent.
