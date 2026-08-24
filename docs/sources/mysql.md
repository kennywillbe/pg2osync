# MySQL / MariaDB source

Change capture through the **binary log**. Verified end to end against
MySQL 8.0 and MariaDB 11.8: consistent initial load, live INSERT/UPDATE/DELETE
streaming with real column names, resumable positions and crash recovery
(`dev/e2e-mysql-test.sh`).

## Requirements

```sql
-- row-based logging with full row images: MINIMAL and NOBLOB omit unchanged
-- columns, which silently loses data on update
SET GLOBAL binlog_format = ROW;
SET GLOBAL binlog_row_image = FULL;

-- the sync user needs to read the tables, the catalog and the binlog
CREATE USER 'pg2osync'@'%' IDENTIFIED WITH mysql_native_password BY '...';
GRANT SELECT, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'pg2osync'@'%';
```

`log_bin` must be enabled (it is by default on MySQL 8.0+ and MariaDB 10.6+;
`binlog_format`/`binlog_row_image` are usually already `ROW`/`FULL`). Put the
settings in `my.cnf` as well — `SET GLOBAL` does not survive a restart.

`pg2osync validate` checks all three settings, the connection and every
configured table before you run anything.

**`mysql_native_password` is required.** The `caching_sha2_password` full-auth
exchange needs TLS or an RSA key exchange, which is not implemented yet. The
server's default plugin does not matter, only the sync user's.

Every synced table needs a **primary key**; it becomes the document `_id`.

## Configuration

```toml
[source]
flavor = "mysql"
url_env = "PG2OSYNC_SOURCE_URL"     # mysql://user:pass@host:3306/appdb
server_id = 424242                  # unique across the server's replicas

[target]
url = "http://localhost:9200"

[sync.users]
table = "appdb.users"               # database.table
index = "users"
```

`server_id` must not collide with any other replica or CDC tool attached to the
same server, and it is what the checkpoint is keyed on — changing it forces a
full initial load.

## How it works

1. **Prerequisite check** on a plain connection (`log_bin`, `binlog_format`,
   `binlog_row_image`), plus column and primary-key resolution from
   `information_schema`.
2. **Initial load** inside `START TRANSACTION WITH CONSISTENT SNAPSHOT`. The
   binlog coordinate is read *inside* that transaction, so streaming from it can
   only re-deliver rows, never skip them.
3. **`COM_BINLOG_DUMP`** from that coordinate on a second connection, after
   `SET @master_binlog_checksum = @@global.binlog_checksum` so CRC32-checksummed
   events are usable.
4. **Event decoding**: FORMAT_DESCRIPTION (checksum length), ROTATE (file
   changes), TABLE_MAP (column types), WRITE/UPDATE/DELETE_ROWS (row images),
   XID (commit boundaries), QUERY (DDL detection).

Column names come from `information_schema` over a second connection, because
binlog row events identify columns only by ordinal — and the dump connection
cannot run queries while streaming. When the server runs
`binlog_row_metadata = FULL`, the names in TABLE_MAP are used directly.

### Positions instead of slots

MySQL has no server-side position tracking. pg2osync stores
`(binlog file, position)` in the checkpoint and resumes from it; a replay after
a crash is harmless because writes are idempotent.

The engine orders positions as `(file index << 32) | offset`, so a rotation
always compares greater than any offset in the previous file.

Consequence: the server must still hold the binlog you stopped at. Past
`binlog_expire_logs_seconds` the position is gone and the next start runs a full
initial load. Keep enough retention to cover your worst expected outage.

### MySQL vs MariaDB wire differences

Handled transparently:

| Aspect | MySQL 8.x | MariaDB 10/11.x |
|---|---|---|
| `WRITE_ROWS_V2` event type | `30` | `23` |
| `UPDATE_ROWS_V2` event type | `31` | `24` |
| `DELETE_ROWS_V2` event type | `32` | `25` |
| v2 extra-data-length field | present | absent |
| Client binary | `mysql` | `mariadb` |
| Default binlog prefix | `binlog`/`mysql-bin` | `mariadb-bin` |

## Type mapping

| MySQL type | JSON |
|---|---|
| `TINYINT`…`BIGINT`, `YEAR` | number |
| `DECIMAL`/`NUMERIC` | **string**, with the declared scale preserved (`8.50` stays `8.50`) |
| `FLOAT`, `DOUBLE` | number |
| `DATE`, `DATETIME`, `TIMESTAMP`, `TIME` | string |
| `CHAR`, `VARCHAR`, `TEXT` | string |
| `BIT`, `BINARY`, `BLOB`, `GEOMETRY` | base64 string |
| `JSON` | parsed JSON on the initial load; hex placeholder when streamed |

Decimals stay strings on purpose: a float round-trip loses precision on money.

## DDL

An `ALTER` or `RENAME` in the binlog invalidates the cached schema, so the next
row event resolves column names from the catalog again. If the binlog reports a
different column count than the catalog — a DDL in flight — the process stops
with a clear error rather than writing shifted values. Restart it to
resynchronize.

Column renames and drops need a re-index: existing documents keep the old
field names.

## Known limitations

| Limitation | Detail | Workaround |
|---|---|---|
| Nested children | Not implemented for MySQL | Use PostgreSQL, or denormalize in a view |
| `JSON` while streaming | MySQL's binary JSON format is not parsed; a hex placeholder is stored | Store JSON as `TEXT`, or re-run the initial load |
| `caching_sha2_password` | Full auth needs TLS/RSA | Create the sync user with `mysql_native_password` |
| GTID positions | Checkpoints use file and offset, not GTID sets | Fine for a single server; failover to a replica needs a fresh initial load |
| Timezone edge cases | `DATETIME` values decode naive | Prefer `TIMESTAMP`, or verify your setup |

## Verifying your setup

```sh
docker run -d --name mysql-test -p 13306:3306 \
  -e MYSQL_ROOT_PASSWORD=secret -e MYSQL_DATABASE=appdb mysql:8.0

MYSQL_CONTAINER=mysql-test MYSQL_PORT=13306 \
  MYSQL_ROOT_PASSWORD=secret ./dev/e2e-mysql-test.sh
```

The suite covers the snapshot, live CRUD, decimal fidelity, projections,
transforms, the checkpoint format and crash recovery. For MariaDB add
`MYSQL_CLIENT=mariadb`.
