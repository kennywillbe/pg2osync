-- Development schema for the MySQL/MariaDB source.
-- Apply: docker exec -i mysql-test mysql -uroot -pmysqlpw sourcedb < dev/mysql-seed.sql

CREATE TABLE IF NOT EXISTS shop_users (
    id            bigint PRIMARY KEY,
    name          varchar(120) NOT NULL,
    email         varchar(190),
    password_hash varchar(190),
    balance       decimal(12, 2) NOT NULL DEFAULT 0,
    metadata      json,
    created_at    timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP
);
