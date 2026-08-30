-- Development schema and seed data for the local docker-compose stack.
-- Apply: docker exec -i dev-postgres-1 psql -U postgres -d sourcedb < dev/seed.sql

CREATE TABLE IF NOT EXISTS users (
    id            bigint PRIMARY KEY,
    name          text NOT NULL,
    email         text,
    password_hash text,
    metadata      jsonb,
    created_at    timestamptz NOT NULL DEFAULT now(),
    updated_at    timestamptz NOT NULL DEFAULT now()
);

-- Older dev databases predate these columns; keep re-runs working.
ALTER TABLE users ADD COLUMN IF NOT EXISTS password_hash text;
ALTER TABLE users ADD COLUMN IF NOT EXISTS updated_at timestamptz NOT NULL DEFAULT now();

CREATE TABLE IF NOT EXISTS customers (
    id   bigint PRIMARY KEY,
    name text NOT NULL
);

CREATE TABLE IF NOT EXISTS orders (
    id             bigint PRIMARY KEY,
    customer_id    bigint NOT NULL REFERENCES customers(id),
    total          numeric(12, 2) NOT NULL,
    internal_notes text
);

ALTER TABLE orders ADD COLUMN IF NOT EXISTS internal_notes text;

-- integer rather than bigint on purpose: a child key narrower than i64 is the
-- common case and once broke the typed comparison
CREATE TABLE IF NOT EXISTS tickets (
    id          bigint PRIMARY KEY,
    customer_id integer NOT NULL REFERENCES customers(id),
    subject     text NOT NULL
);

-- A one-to-one child: at most one row per customer, embedded as an object.
CREATE TABLE IF NOT EXISTS profiles (
    id          bigint PRIMARY KEY,
    customer_id bigint NOT NULL REFERENCES customers(id),
    bio         text
);

-- Child deletes carry the foreign key only under REPLICA IDENTITY FULL, which
-- is what lets the parent document be refreshed.
ALTER TABLE orders REPLICA IDENTITY FULL;
ALTER TABLE tickets REPLICA IDENTITY FULL;
ALTER TABLE profiles REPLICA IDENTITY FULL;

INSERT INTO users (id, name, email, password_hash, metadata)
VALUES
    (1, 'ada', 'ada@example.com', 'hash-1', '{"role": "admin"}'),
    (2, 'grace', 'grace@example.com', 'hash-2', '{"role": "user"}'),
    (3, 'linus', 'linus@example.com', 'hash-3', '{"role": "user"}')
ON CONFLICT (id) DO NOTHING;

INSERT INTO customers (id, name) VALUES (1, 'acme'), (2, 'globex')
ON CONFLICT (id) DO NOTHING;

INSERT INTO orders (id, customer_id, total)
VALUES (10, 1, 99.90), (11, 1, 5.00), (12, 2, 42.00)
ON CONFLICT (id) DO NOTHING;
