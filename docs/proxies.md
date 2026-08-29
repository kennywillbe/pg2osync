# Proxies and connection poolers

pg2osync opens two kinds of connection to the source. Which of them a proxy can
carry follows from the wire protocols, not from a test: nothing here has been
run against a proxy, and CI does not run one. A "yes" below means the protocol
does not forbid it.

| | PostgreSQL | MySQL / MariaDB |
|---|---|---|
| **Stream** | `url_env`: a logical-replication connection (`replication=database`) that sends `START_REPLICATION` and receives WAL until it closes | `url_env`: sends `COM_BINLOG_DUMP` and receives binlog events until it closes |
| **SQL** | `admin_url_env`, falling back to `url_env`: catalog reads, publication and slot setup, `pg_replication_slots`, the initial-load readers, child re-fetches | `url_env` again — MySQL has no separate admin URL: prerequisite checks, `information_schema`, `SHOW BINARY LOG STATUS`, the initial load, child re-fetches |

## The stream connection must be direct

After `START_REPLICATION` the server answers with `CopyBothResponse` and streams
WAL until the client ends COPY mode ([streaming replication protocol](https://www.postgresql.org/docs/current/protocol-replication.html));
`COM_BINLOG_DUMP` requests "a Binlog Network Stream" that runs until the
connection closes ([MySQL internals](https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_binlog_dump.html)).
Neither is a query with a result set; both need one backend for the life of
the connection.

- **Transaction and statement pooling** cannot carry it. A backend "is assigned
  to a client only during a transaction" ([PgBouncer features](https://www.pgbouncer.org/features.html)),
  and the stream is not one. RDS Proxy multiplexes "after each transaction"
  ([concepts](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/rds-proxy.howitworks.html#rds-proxy-transactions))
  and says so outright: "RDS Proxy currently doesn't support streaming
  replication mode" ([PostgreSQL limitations](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/rds-proxy.html#rds-proxy.limitations-pg)).
  Its MySQL limitations say nothing about the binlog dump; treat it the same way.
- **Query-parsing routers** do not know the command. Pgpool-II "does not
  recognize replication protocol"; its maintainer's advice for `pg_basebackup`,
  connect to PostgreSQL directly, applies here too ([pgpool-general, April 2016](https://www.pgpool.net/pipermail/pgpool-general/2016-April/004649.html)).
  ProxySQL's query layer rejects binlog clients; the maintainer's answer is
  `mysql_users.fast_forward=1` for that user alone ([issue #3580](https://github.com/sysown/proxysql/issues/3580)),
  which "bypasses the query processing layer (rewriting, caching) and passes
  through the query directly to the backend server" ([mysql_users](https://proxysql.com/documentation/main-runtime/mysql-tables/)).
- **PgBouncer 1.23.0 and later** proxies replication connections
  ([changelog, 2024-07-03](https://www.pgbouncer.org/changelog.html)), bypassing
  pooling whatever `pool_mode` says: client and server connection "form a
  strong pair, as soon as one is closed the other is closed too", and are never
  cached ([PR #876](https://github.com/pgbouncer/pgbouncer/pull/876)). Earlier
  versions reject them.
- **TCP pass-throughs** carry it because they carry anything: HAProxy in
  `mode tcp`, where "no layer 7 examination will be performed"
  ([manual](https://docs.haproxy.org/3.0/configuration.html#4.2-mode)), and
  MySQL Router's connection routing, where "MySQL packets are routed in their
  entirety without inspection" ([docs](https://dev.mysql.com/doc/mysql-router/8.4/en/mysql-router-general-features-connection-routing.html)),
  with `connection_sharing` left at its default `0` ([options](https://dev.mysql.com/doc/mysql-router/8.4/en/mysql-router-conf-options.html#option_mysqlrouter_connection_sharing)).
  A TCP connection cannot move between backends, so the stream is pinned for
  its life; when the proxy or its backend changes, the connection drops and
  pg2osync reconnects.

## The SQL connection may be pooled, but must reach the primary

It is ordinary SQL, so a pooler can carry it: in session mode without
conditions, in transaction mode only where the pooler handles the named
prepared statements the PostgreSQL driver issues — PgBouncer 1.21 and later
with `max_prepared_statements` ([config](https://www.pgbouncer.org/config.html#max_prepared_statements)).
It also has to land on the server the stream reads, and that is the primary:
the initial load versions every range with the position it reads here, and
`pg_current_wal_lsn()` "cannot be executed during recovery" ([backup control functions](https://www.postgresql.org/docs/current/functions-admin.html#FUNCTIONS-ADMIN-BACKUP)),
while `SHOW BINARY LOG STATUS` on a replica names a binlog the stream never
uses; a child re-fetch reads the parent row right after its change arrived on
the stream, and on a lagging replica the row is old or missing; and the
publication and slot are created here for the stream to open.

So anything whose purpose is to send reads elsewhere — Pgpool-II load balancing
([load_balance_mode](https://www.pgpool.net/docs/latest/en/html/runtime-config-load-balancing.html)),
MySQL Router's read-only or [read/write-splitting](https://dev.mysql.com/doc/mysql-router/8.4/en/router-read-write-splitting.html)
ports, a reader endpoint — must stay out of this connection's path. An RDS
Proxy default endpoint is fine on that count: a proxy "can associate only with
the writer DB instance, not a read replica" ([limitations](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/rds-proxy.html#rds-proxy.limitations)).

## Summary

| Proxy | Stream connection | SQL connection |
|---|---|---|
| PgBouncer ≥ 1.23, any `pool_mode` | yes, pinned and unpooled | session: yes; transaction: with `max_prepared_statements` |
| PgBouncer < 1.23 | rejected | as above |
| Pgpool-II | no | only with `load_balance_mode = off` |
| RDS Proxy | no on PostgreSQL (documented); unstated on MySQL, assume no | yes; it targets the writer |
| ProxySQL | only with `fast_forward = 1` for the user | yes, if the user's hostgroup holds only the primary |
| MySQL Router | connection routing to the primary, `connection_sharing = 0` | the read/write port only |
| HAProxy `mode tcp` | yes | yes; every backend in the pool must be the primary |

## What pg2osync does not do

Nothing proxy-specific: it does not detect a proxy, set pooler parameters, or
pin anything itself, and no test runs against one — `dev/e2e-test.sh` and
`dev/failover-probe.sh` dial the database directly. A proxy restart is a
dropped stream like any other: the pipeline is rebuilt from the last checkpoint
with backoff, up to `[source] reconnect_max` attempts
([operations](operations.md#what-retries-and-what-does-not)); the slot on
PostgreSQL, or the GTID in the checkpoint on MySQL, keeps the position, so it is
a reconnect, not a loss. A reconnect that lands on a *different* server is a
failover, covered in [surviving a failover](sources/mysql.md#surviving-a-failover).
