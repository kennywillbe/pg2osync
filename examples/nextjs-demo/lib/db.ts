import { Pool } from "pg";

// The demo writes only here. Nothing in this file, or anywhere else in the
// app, ever talks to OpenSearch to write a document — that is pg2osync's job,
// running as a separate process against the WAL.
const connectionString =
  process.env.DEMO_PG_URL ??
  "postgres://postgres:postgres@localhost:15432/sourcedb";

// Next.js dev mode reloads this module on every edit; stashing the pool on
// `globalThis` avoids leaking a new connection pool per reload.
const globalForPg = globalThis as unknown as { pgPool?: Pool };

export const pool =
  globalForPg.pgPool ?? new Pool({ connectionString, max: 5 });

if (process.env.NODE_ENV !== "production") {
  globalForPg.pgPool = pool;
}

export type DemoProduct = {
  id: number;
  name: string;
  description: string;
  price: string; // numeric comes back as a string from `pg`; keep it exact.
  tags: unknown;
  updated_at: string;
};
