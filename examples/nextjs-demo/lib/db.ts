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

export type DemoReview = {
  id: number;
  product_id: number;
  author: string;
  rating: number;
  comment: string;
  created_at: string;
};

const PRODUCT_COLUMNS = "id, name, description, price, tags, updated_at";

export async function listProducts(): Promise<DemoProduct[]> {
  const { rows } = await pool.query(
    `SELECT ${PRODUCT_COLUMNS} FROM demo_products ORDER BY id`,
  );
  return rows;
}

export async function listReviews(): Promise<DemoReview[]> {
  const { rows } = await pool.query(
    `SELECT id, product_id, author, rating, comment, created_at
     FROM demo_reviews ORDER BY product_id, id`,
  );
  return rows;
}

// A small word pool so bulk-created rows look like products instead of
// "row 17"; the point of bulk create is watching pg2osync batch them.
const ADJECTIVES = ["classic", "compact", "deluxe", "portable", "rugged", "smart", "vintage", "wireless"];
const NOUNS = ["keyboard", "lamp", "mug", "backpack", "speaker", "notebook", "kettle", "tripod"];

export function randomProduct() {
  const name = `${ADJECTIVES[Math.floor(Math.random() * ADJECTIVES.length)]} ${
    NOUNS[Math.floor(Math.random() * NOUNS.length)]}`;
  return {
    name,
    description: `Auto-generated ${name}, batch insert via the demo app.`,
    price: (Math.random() * 100).toFixed(2),
    tags: ["bulk", "auto"],
  };
}
