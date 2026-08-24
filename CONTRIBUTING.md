# Contributing to pg2osync

Thanks for considering a contribution. This project replicates production
databases, so correctness beats features: a change that risks silent data loss
will not be merged, however convenient it is.

## Getting set up

```sh
git clone https://github.com/kennywillbe/pg2osync
cd pg2osync
cargo build

# local PostgreSQL (:15432, wal_level=logical) + OpenSearch (:9200)
docker compose -f dev/docker-compose.yml up -d
docker exec -i dev-postgres-1 psql -U postgres -d sourcedb < dev/seed.sql
```

Rust stable is required; the MSRV is pinned in `Cargo.toml` (`rust-version`)
and enforced by CI.

## Before opening a pull request

```sh
cargo fmt --all
cargo clippy --workspace --all-targets -- -D warnings   # must be clean
cargo test --workspace

cargo build --release
./dev/e2e-test.sh                    # PostgreSQL -> OpenSearch, full pipeline
./dev/e2e-mysql-test.sh              # MySQL source (needs a MySQL container)
```

Every bug fix needs a regression test that fails without the fix. Protocol
decoders (`pgoutput.rs`, `binlog.rs`) are tested with byte-level vectors — add
one for the case you fix rather than only testing through the pipeline.

## Architecture rules

These boundaries are what keep new sources and sinks cheap to add. A pull
request that crosses them will be sent back:

- `core` depends on nothing in the workspace. Everything flows through
  `core::ChangeEvent` and the `Sink` trait.
- The engine is source-agnostic: nothing PostgreSQL- or MySQL-specific may
  appear in `crates/engine`. Source positions reach it as an opaque `u64`.
- Sources own their protocol. pgoutput decoding stays in `crates/source`,
  binlog decoding in `crates/source-mysql`. No CDC framework dependency.
- New targets implement `Sink`. The engine must never match on a sink kind.
- A partial transaction must never be visible as a complete one, and a
  position must never be acknowledged before the data is durable.

Design rationale lives in [docs/decisions.md](docs/decisions.md). If your
change contradicts a recorded decision, update that document in the same pull
request and explain why.

## Style

- Comments explain **why**, not what. No commented-out code, no changelog
  comments, no bare `TODO` without an issue link.
- Errors: `thiserror` in libraries, `anyhow` only in `crates/bin`.
- No `unwrap()`/`expect()` outside tests unless the invariant is proven in a
  comment right there.
- Build only what the change needs. An abstraction with one implementation and
  no second caller in sight is not an improvement.
- Secrets never reach logs, errors, or test fixtures.

## Commit messages and pull requests

Write short, descriptive commit subjects in the imperative mood
(`fix truncate ordering against pending writes`). Explain the reasoning in the
body when it is not obvious from the diff. Keep one logical change per commit.

Pull requests should say what changed, why, and how you verified it — include
the e2e output when you touched the pipeline.

## Reporting bugs

Include the source and target versions, your config with secrets redacted, the
relevant log lines (`RUST_LOG=pg2osync=debug`), and what you expected instead.
Data-loss and data-corruption reports get priority over everything else.
