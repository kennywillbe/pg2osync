# AGENTS.md — Guide for contributors and AI agents

Read this before changing anything in this repository. It is the short version
of [CONTRIBUTING.md](CONTRIBUTING.md) plus the rules that are easy to break by
accident.

## What this project is

pg2osync keeps a search index in sync with a relational database in real time:
PostgreSQL (logical replication) or MySQL/MariaDB (binlog) into OpenSearch,
Elasticsearch or Meilisearch. One static binary, one TOML file, no Kafka, no
Logstash, no Redis.

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
- New targets implement `Sink`. The engine never matches on a sink kind.
- Delivery is at-least-once with idempotent writes (`_id` = primary key).
- A partial transaction must never be visible as a complete one.
- A source position is acknowledged only after the data is durably written and
  checkpointed. Acknowledging early loses data on crash-restart.
- Checkpoint state lives in the target (a hidden index, or a state file for
  Meilisearch), never in the source database.

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
- Never edit `crates/bin/CHANGELOG.md` by hand: release-please writes it from
  the commit subjects at release time, so the commit message is where a change
  explains itself.

## Definition of done

```sh
cargo fmt --all
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
./dev/e2e-test.sh          # when the pipeline changed
```

Every bug fix ships with a regression test that fails without the fix. For
protocol decoders, that means a byte-level test vector.

Commits: short imperative subject, reasoning in the body when the diff does not
show it. One logical change per commit.
