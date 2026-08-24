-- Demo schema for the pg2osync Next.js example.
--
-- Kept separate from public.users because the e2e suites truncate that table;
-- these objects belong only to the demo app. Every statement is idempotent so
-- the file can be re-run after pulling changes.

CREATE TABLE IF NOT EXISTS demo_products (
    id             serial PRIMARY KEY,
    name           text NOT NULL,
    description    text NOT NULL DEFAULT '',
    price          numeric(10, 2) NOT NULL DEFAULT 0,
    tags           jsonb NOT NULL DEFAULT '[]'::jsonb,
    updated_at     timestamptz NOT NULL DEFAULT now()
);

-- internal_note exercises exclude_columns: it must never appear in OpenSearch.
-- supplier_email exercises transforms (redact): it reaches the index only as ***.
ALTER TABLE demo_products ADD COLUMN IF NOT EXISTS internal_note text NOT NULL DEFAULT '';
ALTER TABLE demo_products ADD COLUMN IF NOT EXISTS supplier_email text NOT NULL DEFAULT '';

CREATE TABLE IF NOT EXISTS demo_reviews (
    id          serial PRIMARY KEY,
    product_id  integer NOT NULL REFERENCES demo_products(id) ON DELETE CASCADE,
    author      text NOT NULL,
    rating      integer NOT NULL CHECK (rating BETWEEN 1 AND 5),
    comment     text NOT NULL DEFAULT '',
    created_at  timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS demo_reviews_product_id_idx ON demo_reviews (product_id);

-- Without this a review DELETE carries no product_id in the WAL record, so
-- pg2osync could not locate the parent document to re-index.
ALTER TABLE demo_reviews REPLICA IDENTITY FULL;
