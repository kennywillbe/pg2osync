# pg2osync Next.js demo

A small app that makes the pg2osync pipeline tangible: you write to
PostgreSQL in one panel and watch the change land in OpenSearch in the other,
with the propagation time on screen.

**The architectural point:** this app writes only to PostgreSQL
(`lib/db.ts`) and reads only from OpenSearch (`lib/opensearch.ts`). It never
issues an index/update/delete request against OpenSearch. Every document you
see on the right exists because a separate `pg2osync` process tailed the
PostgreSQL WAL and wrote it there.

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
# 1. Create the demo table
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
  create, edit, delete, or truncate them.
- The right panel ("OpenSearch") searches the `demo_products` index.
- After every write, a banner at the top shows how many milliseconds it took
  for that change to become visible in OpenSearch — the app polls the search
  index until it sees the effect of the write it just made.
- Before `pg2osync run` has ever executed, the right panel says the index
  doesn't exist yet, instead of erroring.
- TRUNCATE is called out on its own button because it is the one change that
  never appears as a row-level event: pg2osync decodes the WAL's TRUNCATE
  record directly and clears the whole index.

## Configuration

Environment variables, all optional (defaults match the local dev stack):

| Variable | Default |
|---|---|
| `DEMO_PG_URL` | `postgres://postgres:postgres@localhost:15432/sourcedb` |
| `DEMO_OPENSEARCH_URL` | `http://localhost:9200` |
| `DEMO_OS_INDEX` | `demo_products` |

## Files

- `schema.sql` — the `demo_products` table (kept separate from `public.users`,
  which the e2e suites truncate).
- `pg2osync.demo.toml` — syncs `public.demo_products` to the `demo_products`
  index.
- `lib/db.ts` — the only place this app writes anything, and it writes only
  to PostgreSQL.
- `lib/opensearch.ts` — the only place this app reads OpenSearch, with plain
  `fetch`; no client library.
- `lib/propagation.ts` — polls OpenSearch after a write and times how long
  until the change is visible.
- `app/api/*` — API routes wiring the two together.
- `app/page.tsx` — the two-panel UI.
