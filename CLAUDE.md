# CLAUDE.md

Read and follow [AGENTS.md](AGENTS.md) — it is the canonical guide for all
agents working on this repository. This file only highlights the rules Claude
must never violate.

## Critical rules (from AGENTS.md)

1. **Planning phase:** no implementation code, no `cargo init`/scaffolding
   unless explicitly requested. Design work happens in PLAN.md first.
2. **Language:** everything (docs, comments, commits, PRs) in English.
3. **Comments:** explain *why*, never *what*. No commented-out code, no
   changelog comments, no bare TODOs (use issue references).
4. **SOLID + YAGNI, strictly:** new targets extend via traits; build only what
   the current milestone needs.
5. **Architecture invariants** (PLAN.md §9): replication transport via
   `pgwire-replication` behind our own trait, pgoutput decoding in-house (no CDC
   framework), Engine → `Sink` trait → OpenSearch impl, engine is source-agnostic
   (knows only `core::ChangeEvent`), at-least-once delivery with idempotent
   writes, no partial transactions to sinks, checkpoint state inside OpenSearch.
6. Contradicting a recorded decision is a bug: update the Decision Record first,
   then the code.

## Quick orientation

- Project plan, milestones, decisions: [PLAN.md](PLAN.md)
- Current status: planning phase — see PLAN.md §9 "Open decisions" for what
  must be settled before any code is written.
