-- M0 spike schema + seed.
-- Apply: docker exec -i $(docker compose -f dev/docker-compose.yml ps -q postgres) \
--        psql -U postgres -d sourcedb < dev/seed.sql

CREATE TABLE IF NOT EXISTS users (
    id         bigint PRIMARY KEY,
    name       text NOT NULL,
    email      text,
    metadata   jsonb,
    created_at timestamptz NOT NULL DEFAULT now()
);

-- Keep publication creation idempotent across re-runs.
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = 'spike_pub') THEN
        CREATE PUBLICATION spike_pub FOR TABLE users;
    END IF;
END $$;

INSERT INTO users (id, name, email, metadata)
VALUES
    (1, 'ada', 'ada@example.com', '{"role": "admin"}'),
    (2, 'grace', 'grace@example.com', '{"role": "user"}'),
    (3, 'linus', 'linus@example.com', '{"role": "user"}')
ON CONFLICT (id) DO NOTHING;

-- Sanity probes used by the spike acceptance checks:
-- SELECT wal_level FROM pg_settings WHERE name = 'wal_level';  -- expect: logical
-- SELECT * FROM pg_publication;                               -- expect: spike_pub
