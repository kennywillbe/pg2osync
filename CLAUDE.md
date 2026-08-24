# CLAUDE.md

[AGENTS.md](AGENTS.md) is the canonical guide for this repository — read it
first. The rules below are the ones that must never be violated.

1. **Architecture invariants** (AGENTS.md): source-agnostic engine, decoding
   inside its source crate, sinks behind the `Sink` trait, at-least-once with
   idempotent writes, no partial transactions, no acknowledgement before the
   checkpoint is durable, checkpoint state in the target.
2. **Comments explain why, never what.** No commented-out code, no changelog
   comments, no bare `TODO`s.
3. **SOLID + YAGNI:** extend through existing traits; build only what the
   current change requires.
4. **English** for code, comments, commits and documentation.
5. Contradicting [docs/decisions.md](docs/decisions.md) is a bug: update the
   decision record first, then the code.
6. Before calling a change done: `cargo fmt --all`,
   `cargo clippy --workspace --all-targets -- -D warnings`,
   `cargo test --workspace`, and `./dev/e2e-test.sh` when the pipeline changed.
