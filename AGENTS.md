# AGENTS.md — Guide for contributors and AI agents

Read this before changing anything in this repository. It is the short version
of [CONTRIBUTING.md](CONTRIBUTING.md) plus the rules that are easy to break by
accident.

## What this project is

pg2osync keeps a search index in sync with a relational database in real time:
PostgreSQL (logical replication) or MySQL/MariaDB (binlog) into OpenSearch,
Elasticsearch, Meilisearch, a pgvector table or Qdrant. One static binary, one
TOML file, no Kafka, no Logstash, no Redis.

- How the pipeline works: [docs/architecture.md](docs/architecture.md)
- Every config option: [docs/configuration.md](docs/configuration.md)
- Why things are built the way they are: [docs/decisions.md](docs/decisions.md)

## Architecture invariants

Breaking one of these is a bug even if the tests pass:

- `core` depends on no other workspace crate. `ChangeEvent` and the `Sink`
  trait are the only things crossing module boundaries.
- The engine is source-agnostic. Nothing PostgreSQL- or MySQL-specific belongs
  in `crates/engine`; source positions reach it as an opaque `u64` token.
- Protocol decoding stays inside its source crate — pgoutput in
  `crates/source`, binlog in `crates/source-mysql`. No CDC framework.
- New targets implement `Sink`. The engine never matches on a sink kind, and a
  new sink is not done until it answers `pg2osync_core::testkit` — the
  conformance kit, `crates/sink/tests/conformance.rs` — against a live
  instance of its target. A check it cannot answer is reported as skipped,
  with the reason in the trait: the kit gates the truncate check on
  `truncates_at_a_position`, and the partial-batch one on there being a
  document the target refuses.
- Delivery is at-least-once with idempotent writes: `_id` is the primary key,
  an id the configuration derives, or — for an `append_only` table — a content
  hash, so a replay overwrites rather than duplicates.
- A partial transaction must never be visible as a complete one.
- A source position is acknowledged only after the data is durably written and
  checkpointed. Acknowledging early loses data on crash-restart.
- Checkpoint state lives in the target (a hidden index, a state table, or a
  state file for Meilisearch), never in the source database.

If a change contradicts [docs/decisions.md](docs/decisions.md), update that
document in the same change and say why.

## Code rules

- Comments explain **why**, never what. No commented-out code, no changelog
  comments, no bare `TODO` without an issue reference.
- SOLID and YAGNI: build what the change needs. A trait with one implementation
  and no second caller in sight is not an improvement.
- Errors: `thiserror` in libraries, `anyhow` only in `crates/bin`.
- No `unwrap()`/`expect()` outside tests unless a comment proves the invariant.
- Async everywhere with tokio; never block the runtime.
- Secrets never appear in logs, errors, or fixtures.
- English for all code, comments, commits and documentation.
- Issues and pull requests use the forms in `.github/` — an issue follows
  the matching template's headings and labels, a PR description keeps
  `pull_request_template.md`'s structure. CLI tools do not apply them for
  you.
- Never edit `crates/bin/CHANGELOG.md` by hand: release-please writes it from
  the commit subjects at release time, so the commit message is where a change
  explains itself.

## Definition of done

```sh
./dev/ci-local.sh          # before every push, no exceptions
./dev/failover-probe.sh    # when the MySQL checkpoint or version logic changed; not part of CI
```

That one script runs, locally, exactly what GitHub Actions runs on a pull
request: the same commands, the same job names, and the same pinned versions —
it reads the images, the mdBook version and the MSRV out of the workflow files
at run time, so it cannot drift from CI. CI must never be the first thing that
finds a red.

What it covers, job by job:

| Job | Workflow |
|---|---|
| `fmt + clippy + unit tests` | `ci.yml` |
| `minimum supported Rust version` | `ci.yml` |
| `e2e PostgreSQL to OpenSearch` | `ci.yml` |
| `e2e MySQL to OpenSearch` | `ci.yml` |
| `e2e several sources in one process` | `ci.yml` |
| `e2e PostgreSQL to pgvector` | `ci.yml` |
| `container image builds` | `ci.yml` |
| `helm chart lints` | `ci.yml` |
| `the book builds` | `docs.yml` |
| `the title is a conventional commit` | `pr-title.yml` |
| `dependencies have no known advisories` | `audit.yml`, when a Cargo file moved |
| the ten compatibility cells and `sink conformance kit` | `compat.yml`, when it, `dev/e2e-*.sh`, a sink crate, the `Sink` contract, the operator or the `Dockerfile` changed |

The cell `compat.yml` marks `continue-on-error` is reported as advisory (`!`)
here too: a known gap being tracked does not make the run red.

It brings the dev stack up if it is down and seeds it. The e2e suites share
that stack, so they queue on one machine-wide lock (`dev/e2e-lock.sh`); a
second run waits. `--isolated` instead gives the run throwaway containers of
its own, `pg2osync-ci-<run id>-*`, on ports Docker assigns — it takes no lock,
never touches the dev stack, and is how you run beside someone else's run. An
8 GB Docker VM fits about two isolated runs next to the dev stack. Because each
cell there is a namespace of its own, an isolated run also takes the ten
compatibility cells two at a time (`--jobs <n>`), which halves the matrix.
Every run prints its mode and run id first, and logs land in
`/tmp/pg2osync-ci-local/<run id>`, one file per job plus the pipeline log of
each suite; the run ends in a `RESULT` line.

Selectors: `--only <job>` / `--skip <job>` for one job, `--matrix` /
`--no-matrix` for the compatibility cells, `--isolated` for a run beside
another one, `--jobs <n>` for how many cells go at once, `--title "..."` to
check a pull request title before it exists. `--fast` skips the e2e suites,
the image build and the matrix — it is a quick loop while you work, **not**
the definition of done.

Tools it needs: Docker, `helm`, `kubectl`, `mdbook`, `rustup`/`cargo`, `curl`,
`python3`, and `kind` for the operator cell. `gh` is optional (only to read
the title of an existing pull request); `cargo-audit` and a missing MSRV
toolchain are installed on demand.

Every bug fix ships with a regression test that fails without the fix. For
protocol decoders, that means a byte-level test vector.

Commits: short imperative subject, reasoning in the body when the diff does not
show it. One logical change per commit.
