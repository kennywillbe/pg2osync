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

`pg2osync setup-sql -c pg2osync.toml` prints the whole script for your config —
the `my.cnf` block, the user and the grants — so it can be handed to whoever
holds the privileges. `pg2osync validate` then checks the settings, the
connection and every configured table before you run anything.

Every synced table needs a **primary key**; it becomes the document `_id`.

## TLS

Every MySQL connection honours `[source] sslmode`, using the same five levels
as the PostgreSQL source. MySQL's own vocabulary maps onto them directly:

| pg2osync | MySQL |
|---|---|
| `disable` | `DISABLED` |
| `prefer` *(default)* | `PREFERRED` |
| `require` | `REQUIRED` |
| `verify-ca` | `VERIFY_CA` |
| `verify-full` | `VERIFY_IDENTITY` |

```toml
[source]
flavor = "mysql"
url_env = "PG2OSYNC_SOURCE_URL"
sslmode = "verify-ca"
sslrootcert = "/etc/mysql/ca.pem"
```

MySQL's auto-generated certificates are issued to
`MySQL_Server_<version>_Auto_Generated_Server_Certificate`, not to your
hostname, so `verify-full` rejects them by design. Use `verify-ca` with the
server's own `ca.pem`, or install certificates issued for the real hostname.

### Authentication

`caching_sha2_password` (the MySQL 8 default) and `mysql_native_password` are
both supported.

The first authentication of an account needs *full* authentication, because the
server has nothing cached yet. pg2osync handles both routes:

- **On a TLS connection** the password is sent as cleartext inside the
  encrypted session, which is what the server expects.
- **On a plaintext connection** it asks for the server's public key, XORs the
  password with the nonce and encrypts it, so the password is never recoverable
  from the wire even without TLS.

Later connections take the fast path while the account remains in the server's
cache. `FLUSH PRIVILEGES` empties that cache and forces full authentication
again — which is exactly how the full-auth paths above were tested.

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
   `binlog_row_image`, `binlog_row_value_options`), plus column and
   primary-key resolution from
   `information_schema`.
2. **Initial load** in primary-key chunks, each one statement, with the binlog
   coordinate read *before* the first chunk and the stream running from it — so
   anything a chunk missed or read stale is replayed onto an idempotent write.
   The load runs beside the stream, not before it.
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
initial load. Keep enough retention to cover your worst expected outage —
MySQL's automatic purge does not spare files a consumer still needs.

The same token is each document's version at the target, which is what lets the
initial load run beside the stream. A version only ever goes up, so a binlog
history that restarts under a running pipeline — `RESET BINARY LOGS AND GTIDS`,
or the same address answered by a different server — leaves the target holding
versions from a numbering that no longer exists. pg2osync refuses to start in
that case rather than writing into silence; the fix is a fresh index name, since
a reload cannot undo versions already in the target.

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
| `end_log_pos` inside a transaction | filled in on every event | left at `0` except on the GTID and XID events |

That last row is the one with teeth: a MariaDB group's final position is not
known until the group is written, and not needing it per event is what lets the
checksums be computed in advance. `binlog_legacy_event_pos` restores the old
behaviour and is documented as costing binlog scalability, so pg2osync tracks
the position itself instead — a stated position wins wherever one appears, a
zero advances by the event size, and the two events that state a position
without having moved the stream (the heartbeat, which is not even in the file,
and the format description of a file resumed into the middle of) move nothing.

## Type mapping

| MySQL type | JSON |
|---|---|
| `TINYINT`…`BIGINT`, `YEAR` | number |
| `DECIMAL`/`NUMERIC` | **string**, with the declared scale preserved (`8.50` stays `8.50`) |
| `FLOAT`, `DOUBLE` | number |
| `DATE`, `DATETIME`, `TIMESTAMP`, `TIME` | string |
| `CHAR`, `VARCHAR`, `TEXT` family | string |
| `BINARY`, `VARBINARY`, `BLOB` family, `GEOMETRY` and the spatial subtypes | base64 string |
| `BIT` | number (MySQL caps it at 64 bits) |
| `ENUM` | its label, e.g. `"medium"` |
| `SET` | an array of its labels, e.g. `["a","c"]` |
| `JSON` | parsed JSON, whichever path wrote it |

Both readers decide from the declared type rather than from what the wire
carries, because neither wire format says enough. A binlog row image gives a
string column no charset — `char` and `binary` share a type code, as do `text`
and `blob` — and gives an enum an ordinal with its labels nowhere; the text
protocol the initial load reads gives every value as bytes and nothing else. The
shape is resolved from `information_schema` once and consulted by both.

An index built before this holds the older shapes for `TEXT` (base64 when it came
from the stream), `BIT`, `ENUM` and `SET`, and for `BINARY`/`VARBINARY` a base64
of mangled text. There was no consistent value to preserve, so those columns are
only correct after a rebuild.

Decimals stay strings on purpose: a float round-trip loses precision on money.
A decimal *inside* a `JSON` document is the exception: MySQL renders it as a
bare number in the JSON text the initial load reads, so the streamed value
matches that rather than the column rule.

## JSON columns

`binlog_row_value_options = PARTIAL_JSON` is refused at startup. It makes the
server log a JSON update as a diff in an event type of its own, which is not
decoded here; refusing says so rather than dropping those updates silently.

MySQL stores `JSON` in its own binary form, which the binlog carries verbatim.
It is decoded here, so a row keeps the same shape whether it arrived through
the initial load or through an update. Dates, times and decimals that JSON has
no type for are rendered exactly as the initial load renders them; anything
else opaque is base64.

Two things worth knowing before pointing this at a target:

- A document that cannot be decoded is stored as `__mysql_json_hex:<hex>` and
  logged. The bytes stay recoverable rather than being guessed at, and the log
  names the size so the row can be found.
- OpenSearch's dynamic mapping rejects some perfectly valid JSON — an array
  mixing scalars and objects, or an integer larger than a `long`. That is a
  target-side limit, not a decoding one, and it applies to the initial load
  just as much. Define the mapping yourself, or map the field as
  `{"type": "object", "enabled": false}` to store it without indexing.

MariaDB is unaffected: it stores `JSON` as `LONGTEXT`, which already arrives as
text.

## TRUNCATE and DROP

`TRUNCATE` is logged as a statement rather than as row events, so it is read out
of the SQL and turned into an index clear, ordered against the writes queued
before it — the same behaviour as the PostgreSQL source.

`DROP TABLE` is only warned about. Clearing the index would be presumptuous when
the table may be about to be recreated, but a dropped table whose index still
holds its documents is worth saying out loud.

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
