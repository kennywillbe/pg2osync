# AGENTS.md — Guide for AI Agents and Human Contributors

This document defines the rules every AI agent (Claude, Copilot, Cursor, etc.)
and human contributor MUST follow when working on this repository. Read it fully
before making any change.

## Project Overview

**pg2osync** is a single-binary, zero-dependency (no Logstash, no Kafka, no
Redis) tool that keeps an OpenSearch index in sync with PostgreSQL tables in
real time using logical replication (WAL). Written in Rust.

- Full product vision (multi-source/multi-sink end state): [VISION.md](VISION.md)
- Full plan, milestones, and decision record: see [PLAN.md](PLAN.md)
- Current status: **planning phase — no production code exists yet**

## Current Phase Rules

While the project is in the planning/design phase:

1. Do NOT write implementation code unless explicitly asked.
2. Do NOT run `cargo init`, `cargo add`, or scaffold a project skeleton unless
   explicitly asked.
3. Design work happens in PLAN.md first; code follows only after the relevant
   milestone's design is agreed upon.
4. All documentation, comments, commit messages, PR descriptions, and issue
   discussions are written in **English**.

## Architecture (high level)

```
PostgreSQL (logical replication / pgoutput)
        │
        ▼
   Source reader ──► Engine (batching, dedup, backpressure) ──► Sink trait ──► OpenSearch
        ▲                                                            │
        └──────────────── LSN ack / checkpoint ◄──── State store ◄───┘
```

Key architectural invariants (see PLAN.md §9 for full rationale):

- The replication **transport** is the `pgwire-replication` crate (transport-only:
  raw `XLogData` frames), accessed through our own transport trait. All
  replication **logic** (pgoutput decoding, slot/publication management,
  transaction buffering) is implemented in-house. Never introduce a CDC
  *framework*; never let pgoutput decoding leak outside the source module.
- The Engine consumes sinks through a `Sink` **trait**, never through a concrete
  OpenSearch type. OpenSearch is one implementation of that trait.
- The Engine is **source-agnostic**: it knows only `core::ChangeEvent`. The fact
  that the source is PostgreSQL lives entirely inside the `source` crate.
- Delivery is at-least-once; correctness relies on idempotent writes
  (`_id` = primary key).
- A partial transaction must never reach a sink: events buffer until COMMIT.
- Checkpoint state lives inside OpenSearch (hidden `_pg2osync_meta` index).

## Comment Policy (strict)

Comments must answer **why**, never **what**.

FORBIDDEN — "what" comments (the code already says this):

```rust
// Set the retry count to 3
let retries = 3;

// Loop over all users
for user in &users {
```

ALLOWED — "why" comments (context the code cannot express):

```rust
// 3 retries because a single OS node restart can take ~2s to accept connections again
let retries = 3;

// Skip system catalogs: pgoutput emits RELATION messages for them during slot creation
for rel in relations.iter().filter(|r| !r.is_system_catalog()) {
```

Additional comment rules:

- No commented-out code. Delete it; git remembers.
- No changelog-style comments ("added X on date Y"). Git history is the log.
- No placeholder comments like `// TODO` without an issue reference
  (`// TODO(#42): ...`). Prefer opening an issue over leaving TODOs.
- Doc comments (`///`) describe contracts, invariants, error conditions — not
  implementations.

## Engineering Principles (strict)

### SOLID

- **Single Responsibility:** each module/type has exactly one reason to change.
  The pgoutput parser must not know about batching; the batcher must not know
  about HTTP.
- **Open/Closed:** new targets (Elasticsearch, etc.) are added by implementing
  `Sink`, never by editing engine logic with `match sink_kind { ... }`.
- **Liskov Substitution:** any `Sink` implementation must be substitutable;
  behavior contracts (at-least-once, ack semantics) are defined on the trait.
- **Interface Segregation:** small, purposeful traits. Don't force mock sinks to
  implement admin APIs they don't need.
- **Dependency Inversion:** high-level modules (engine) depend on traits;
  low-level details (opensearch-rs, tokio-postgres) live behind them.

### YAGNI

- Build what the current milestone requires. Nothing else.
- No config options "for the future", no feature flags without a consumer,
  no abstraction layers with a single planned implementation *unless* the trait
  boundary is already justified by the architecture (e.g., `Sink`).
- When in doubt between flexible and simple: choose simple, note the extension
  point in PLAN.md instead of building it.

## Code Conventions

- Rust stable, edition 2024+; MSRV per PLAN.md §9.
- `cargo fmt` and `cargo clippy -- -D warnings` must pass before every commit.
- Errors: `thiserror` for library-level typed errors, `anyhow` only in `main.rs`.
- No `unwrap()`/`expect()` outside tests and truly-impossible invariants (and
  invariants require a why-comment proving it).
- Async: tokio everywhere; do not block the runtime.
- Secrets never appear in logs, errors, or test fixtures.

## Testing Expectations

- Unit tests live next to the code they test.
- Integration tests use `testcontainers` and are gated behind an
  `integration-tests` feature flag.
- Every bug fix comes with a regression test that fails without the fix.

## Workflow

1. Read the relevant section of PLAN.md before starting any task.
2. New architectural decisions get appended to the Decision Record (PLAN.md §9)
   — code that contradicts a recorded decision is a bug.
3. Milestones are the unit of delivery; keep main branch shippable.
