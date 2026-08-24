-- Demo table for the pg2osync Next.js example.
--
-- Kept separate from public.users because the e2e suites truncate that table;
-- this one belongs only to the demo app.
CREATE TABLE IF NOT EXISTS demo_products (
    id          serial PRIMARY KEY,
    name        text NOT NULL,
    description text NOT NULL DEFAULT '',
    price       numeric(10, 2) NOT NULL DEFAULT 0,
    tags        jsonb NOT NULL DEFAULT '[]'::jsonb,
    updated_at  timestamptz NOT NULL DEFAULT now()
);
