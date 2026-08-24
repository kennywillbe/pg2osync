# pg2osync Next.js demo

A small app that makes the pg2osync pipeline tangible: you write to
PostgreSQL in one panel and watch the change land in OpenSearch in the other,
with the propagation time on screen.

**The architectural point:** this app writes only to PostgreSQL
(`lib/db.ts`) and reads only from OpenSearch (`lib/opensearch.ts`). It never
issues an index/update/delete request against OpenSearch. Every document you
see on the right exists because a separate `pg2osync` process tailed the
PostgreSQL WAL and wrote it there.

## What the demo exercises

Every feature below maps to a pg2osync capability the UI makes observable:

| Demo action | pg2osync feature under test |
|---|---|
| Create / edit / delete one row | row-level insert, update, delete propagation |
| **Bulk create N**, **±% price update**, delete oldest / selected | multi-row transactions, engine batching |
| **Mixed transaction** (insert + rename + delete in one tx) | partial transactions never visible as complete ones |
| **Reviews** per product | nested children: embedded `reviews` array, parent re-indexed on child change |
| "internal note" field | `exclude_columns`: never reaches the index |
| "supplier email" field | `transform.redact`: indexed as `***` |
| TRUNCATE button | TRUNCATE decoded from the WAL, not seen as row deletes |
| Search with typo tolerance | fuzziness; highlights show why a document matched |
| **Inspect document** | raw `_source` view asserting exclusion/redaction/embedding |

The header keeps a history of measured propagation times (min/avg/max) for
the session, so trying several operations shows the distribution rather than
a single number.

## Prerequisites

- The local dev stack running (from the repo root):
  ```sh
  docker compose -f dev/docker-compose.yml up -d
  ```
  This provides PostgreSQL on `localhost:15432` and OpenSearch on
  `localhost:9200`.
- `pg2osync` built:
  ```sh
  cargo build --release
  ```
- Node.js 20+.

## Run it

From `examples/nextjs-demo/`:

```sh
# 1. Create the demo tables
docker exec -i dev-postgres-1 psql -U postgres -d sourcedb < schema.sql

# 2. Start pg2osync against the demo config (from the repo root, in another shell)
cd ../..
export PG2OSYNC_SOURCE_URL="postgres://postgres:postgres@localhost:15432/sourcedb"
./target/release/pg2osync run -c examples/nextjs-demo/pg2osync.demo.toml

# 3. Install deps and start the app (back in examples/nextjs-demo/)
npm install
npm run dev
```

Open http://localhost:3000.

## What you should see

- The left panel ("PostgreSQL") lists rows from `demo_products` and lets you
  create, edit, delete, bulk-create, bulk-update prices, bulk-delete,
  truncate, run the mixed-transaction demo, and manage each product's reviews.
- The right panel ("OpenSearch") searches the `demo_products` index, with an
  optional 2-second auto-refresh so writes appear without manual clicks.
- After every write, a banner at the top shows how many milliseconds it took
  for that change to become visible in OpenSearch — the app polls the search
  index until it sees the effect of the write it just made.
- Clicking **Inspect document** opens the exact `_source` pg2osync wrote and
  asserts on it: `internal_note` absent, `supplier_email` equal to `***`,
  `reviews` embedded as an array.
- Before `pg2osync run` has ever executed, the right panel says the index
  doesn't exist yet, instead of erroring.

## Configuration

Environment variables, all optional (defaults match the local dev stack):

| Variable | Default |
|---|---|
| `DEMO_PG_URL` | `postgres://postgres:postgres@localhost:15432/sourcedb` |
| `DEMO_OPENSEARCH_URL` | `http://localhost:9200` |
| `DEMO_OS_INDEX` | `demo_products` |

## Files

- `schema.sql` — the `demo_products` and `demo_reviews` tables (kept separate
  from `public.users`, which the e2e suites truncate).
- `pg2osync.demo.toml` — sync config: excludes `internal_note`, redacts
  `supplier_email`, embeds `demo_reviews` as `reviews`.
- `lib/db.ts` — the only place this app writes anything, and it writes only
  to PostgreSQL.
- `lib/opensearch.ts` — the only place this app reads OpenSearch, with plain
  `fetch`; no client library.
- `lib/propagation.ts` — polls OpenSearch after a write and times how long
  until the change is visible; `measureBulkPropagation` verifies sampled
  documents for batch operations.
- `app/api/*` — API routes wiring the two together.
- `app/components/*` — product panel, search panel, document inspector modal.
- `app/page.tsx` — layout, state and the propagation history.
