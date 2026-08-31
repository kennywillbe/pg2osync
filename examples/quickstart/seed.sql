-- Seed for the quickstart stack: one table, three rows, applied by the
-- postgres image's own init hook the first time its volume is created.

CREATE TABLE products (
    id    bigint PRIMARY KEY,
    name  text NOT NULL,
    price numeric(10, 2) NOT NULL
);

INSERT INTO products (id, name, price) VALUES
    (1, 'espresso machine', 249.00),
    (2, 'burr grinder', 129.50),
    (3, 'gooseneck kettle', 59.90);
