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

## Before every push

```sh
./dev/ci-local.sh
```

One script, and it is the only thing you have to remember: it runs on your
machine exactly what GitHub Actions runs on your pull request — the same
commands, under the same job names, with the same pinned versions, which it
reads out of `.github/workflows/*` and `Cargo.toml` at run time so it cannot
drift from CI. Push on a green `RESULT` line; CI should never be the first to
tell you a change is red.

It covers `fmt + clippy + unit tests`, the MSRV check, `e2e PostgreSQL to
OpenSearch`, `e2e MySQL to OpenSearch`, `e2e several sources in one process`
(both databases in one `run --config-dir`, which is where per-source metrics,
health and isolation are proved), `e2e PostgreSQL to pgvector`, the container
image build, the helm
lints, the book (`docs.yml`), your pull request title (`pr-title.yml`),
`cargo audit` when a Cargo file moved, and the ten compatibility cells when
CI would run them — that is, when you touched `.github/workflows/compat.yml`,
`dev/e2e-*.sh`, a sink, the `Sink` contract, the operator or the `Dockerfile`.
`--matrix` runs those cells anyway; `--no-matrix` skips
them, which is worth it only when you know they cannot be affected. They are
throwaway containers on ports of their own (PostgreSQL 15433, OpenSearch 9201,
Elasticsearch 9202, Meilisearch 7701, Qdrant 6334, MySQL/MariaDB 13307), so the
dev stack keeps running beside them.

The Meilisearch cell is advisory — `compat.yml` marks it `continue-on-error`
while [#122](https://github.com/kennywillbe/pg2osync/issues/122) is open — so a
failure there prints `!` and does not make your run red.

Two other failures are known to be timing rather than breakage, and the
etiquette for both is the same: **re-run the job once before you start
triaging**, and if it fails twice it is real. The first is the junction-DELETE
assertion in `e2e MySQL to OpenSearch` — step 22, "a junction DELETE takes them
away again" — which reads the index before the re-fetch it triggered has
landed. The second is the Supabase compatibility cell, whose image occasionally
restarts while the cell is seeding it; that one says `the database system is
shutting down` in the log, which is the tell. The third is another children
timing window in the same MySQL suite — "one statement of 10 children lands
whole", which polls the parent before one grouped re-fetch has been written.
A failure that reproduces on the second run is a bug and wants an issue, not
a third run.

The script starts the dev stack and the `mysql-test` container if they are
down and seeds both. Those are shared, and so are the table and index names the
suites use, so the e2e jobs queue on one machine-wide lock: a second run waits
rather than overwriting the first one's state. Logs go to a directory of this
run's own, `/tmp/pg2osync-ci-local/<run id>`: one file per job, and the
pipeline log of each suite beside it (the suites take that path in `E2E_LOG`,
so two runs never read each other's log lines).

To run two at once, give one of them a stack of its own:

```sh
./dev/ci-local.sh --isolated
```

Every e2e and compatibility job of an isolated run gets throwaway containers
named `pg2osync-ci-<run id>-*` on ports Docker assigns, seeded exactly the way
CI seeds its service containers and removed when the job ends; the pipelines
the suites start get a block of localhost ports of their own too, because they
run on your machine rather than in a container. Such a run takes no lock and
leaves the dev stack alone, so it can go beside a shared run or another
isolated one. It also runs the ten compatibility cells two at a time
rather than one after the other, because each of them now has containers and
ports of its own; `--jobs <n>` sets how many, and more than two is what a
larger Docker VM buys you. A cell measures about 1 GB — 0.9 of it OpenSearch
on its 512 MB heap, half a gigabyte more for a MySQL one — against a dev stack
of 2.6 GB, so an 8 GB Docker VM carries about two isolated runs beside it; past
that Docker's OOM killer takes a container down and the job waiting on it fails
naming it, within seconds rather than hanging. The default stays the shared
stack: it is already running and pulls nothing.

While you are still working, `./dev/ci-local.sh --fast` skips the e2e suites,
the image build and the matrix, which is about a minute instead of the better
part of an hour. It is a loop, not a definition of done: run the whole script
before you push.

You need Docker, `helm`, `kubectl`, `mdbook`, `rustup`/`cargo`, `curl` and
`python3`. `gh` is optional — without it, pass your title as
`--title "fix: ..."`. `cargo-audit` and the MSRV toolchain are installed on
demand.

Deeper probes are not part of CI and stay manual:

```sh
./dev/failover-probe.sh              # MySQL failover; builds its own primary and replica
./dev/mtls-probe.sh                  # client certificates; builds its own PostgreSQL with a CA
./dev/db-load-impact.sh              # what the source database pays while busy
./dev/many-tables.sh                 # what a table costs, apart from its rows
./dev/resource-limits.sh             # how many cores it needs (needs the container image)
./dev/soak.sh 4h                     # hours of sustained load with scheduled chaos, sampled and asserted
```

`e2e-postgres-sink.sh` is the suite to run when the PostgreSQL sink changes.
It is also where the sink conformance kit runs against that target; the same
kit runs against OpenSearch as a step of the `e2e PostgreSQL to OpenSearch`
job. Every target answers it, and answering it is what finishes a new sink:
Elasticsearch and Meilisearch in the `sink conformance kit` job of
`compat.yml` — one `cargo test` beside both containers, because their own
cells run a prebuilt binary and would each have compiled the workspace for it
— and Qdrant inside its own suite. So a change to `crates/core/src/testkit.rs`
is answered by five targets, none of them the only witness.

`e2e-qdrant.sh` is the suite to run when the Qdrant sink changes, and where the
kit answers for that target. The full PostgreSQL suite cannot cover that
target either — no mappings, no joins, no per-row collections — so this is
where its load, its similarity search, its state collection and its versioned
truncate are exercised.

`e2e-meili-smoke.sh` is the suite to run when the Meilisearch sink changes.
The full PostgreSQL suite cannot cover that target — it asserts over mappings,
join fields and per-row indices, none of which Meilisearch has — so the smoke
suite is the only place its load, its file checkpoint and its rebuild are
exercised.

`failover-probe.sh` is separate because it is the only check that needs two
servers: it promotes a replica and asserts that the stream resumes *and* that
documents written afterwards still land. Run it when the MySQL checkpoint or
version logic changed.

Commit subjects follow [Conventional Commits](https://www.conventionalcommits.org/)
— `feat:`, `fix:`, `docs:`, `chore:`, and `feat!:` for anything that breaks a
config or a checkpoint. Only the subject line: the body stays prose, and for
anything non-obvious it should say why. That subject is what writes the changelog
and picks the next version; see [docs/releasing.md](docs/releasing.md).

**Give the pull request itself a conventional title too.** Pull requests are
squash-merged, so the title — not your commit subjects — is what lands on `main`
and what the changelog is written from. A check fails the pull request if it does
not parse.

Every bug fix needs a regression test that fails without the fix. Protocol
decoders (`pgoutput.rs`, `binlog.rs`) are tested with byte-level vectors — add
one for the case you fix rather than only testing through the pipeline.


A pull request merges once its checks are green; `gh pr merge --squash --auto`
lands it the moment they are. The branch does not have to be up to date with
`main` — what guards a batch of pull requests landing together is that they are
verified locally as one stack (see AGENTS.md), not a re-run per merge.

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

`docs/` is also the source of <https://kennywillbe.github.io/pg2osync/>, built
with mdBook. A new page has to be listed in
[docs/SUMMARY.md](docs/SUMMARY.md) or CI fails, since a page nothing links to is
a page nobody finds. Build it locally with `mdbook build` and read it with
`mdbook serve`.

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

Pull request descriptions follow [.github/pull_request_template.md](.github/pull_request_template.md)
— keep its headings and checklists. The web UI pre-fills it; `gh pr create`
does not, so paste it into `--body` yourself. Put what changed and why under
*What and why*, and the `RESULT` line of `./dev/ci-local.sh` under *How it was
verified*.

## Reporting bugs

Include the source and target versions, your config with secrets redacted, the
relevant log lines (`RUST_LOG=pg2osync=debug`), and what you expected instead.
Data-loss and data-corruption reports get priority over everything else.
