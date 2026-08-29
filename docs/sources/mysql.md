# MySQL / MariaDB source

Change capture through the **binary log**. `dev/e2e-mysql-test.sh` runs
against MySQL 8.0 on every pull request, and against MySQL 8.4, MariaDB 10.6
and MariaDB 11.8 nightly (see [compatibility](../compatibility.md)):
consistent initial load, live INSERT/UPDATE/DELETE
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

`log_bin` must be enabled. MySQL 8.0+ enables it by default; MariaDB does
not — it needs `log_bin` in the configuration file (or `--log-bin` on the
command line) and a restart. `binlog_format`/`binlog_row_image` are usually
already `ROW`/`FULL`. Put the settings in `my.cnf` as well — `SET GLOBAL` does
not survive a restart.

`pg2osync setup-sql -c pg2osync.toml` prints the whole script for your config —
the `my.cnf` block, the user and the grants — so it can be handed to whoever
holds the privileges. `pg2osync validate` then checks the settings, the
connection and every configured table before you run anything.

Every synced table needs a **primary key** or is declared `append_only`. The
key becomes the document `_id`, unless the table configures `id` to derive one
from its columns or `fan_out` to turn an array column into one document per
element; `binlog_row_image = FULL` already guarantees the before-images both
need. An `append_only` table is filed under a hash of each row, and an
`UPDATE` or `DELETE` on it halts the pipeline — see
[Append-only tables](../configuration.md#append-only-tables).

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

The URL is also what the binlog dump connection uses, so it must reach the
server directly: a query-routing proxy does not carry `COM_BINLOG_DUMP` — see
[Proxies and connection poolers](../proxies.md).

## How it works

1. **Prerequisite check** on a plain connection (`log_bin`, `binlog_format`,
   `binlog_row_image`, `binlog_row_value_options`), plus column and
   primary-key resolution from
   `information_schema`.
2. **Initial load** in primary-key chunks, each one statement, with the binlog
   coordinate read *before* the first chunk and the stream running from it — so
   anything a chunk missed or read stale is replayed onto an idempotent write.
   The load runs beside the stream, not before it. The table's `where`
   predicate is pushed into each chunk statement and evaluated again on every
   binlog row, so a non-matching row is never read and one that stops matching
   is deleted.
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

CI runs MySQL 8.0 and 8.4, and MariaDB 10.6 and 11.8, over both dialects.

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
| `DECIMAL`/`NUMERIC` | **string**, with the declared scale preserved (`8.50` stays `8.50`); `transform = "number"` converts it if you accept float precision |
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
field names. A `fields` entry in the config can absorb a source-side rename
(`new_column = "old_field"`) so the index keeps its field name without one.

## Nested children

`[[sync.x.children]]` works here as it does for PostgreSQL: the parent document
embeds the collection as an array, refreshed whenever the parent or any of its
children changes. Child tables are added to the streamed set automatically, and
their row events resolve to a parent instead of becoming documents of their own.

Two things are easier here than on PostgreSQL:

- **A deleted child is always locatable.** `binlog_row_image = FULL` is already a
  requirement, so a delete carries its whole before-image and the foreign key is
  always present. PostgreSQL needs `REPLICA IDENTITY FULL` on the child for the
  same guarantee and warns when it is missing.
- **There is no TOAST equivalent**, so no value ever arrives as a marker that has
  to be completed from the target.

The array is built from ordinary rows rather than by `JSON_ARRAYAGG(JSON_OBJECT(…))`,
which would not agree with the rest of the pipeline. `JSON_OBJECT` renders a
`varbinary` as `base64:type15:…` on MySQL and as raw escaped bytes on MariaDB, a
`set` as `"a,b"` rather than an array, a `decimal` as a number rather than a
precision-preserving string, and a `bit` as *invalid JSON* on MariaDB — its own
`JSON_VALID` says so. Reading the rows and converting them with the same code that
builds a parent document means a value inside an array is the same JSON as the
same value on its own. The cost is unchanged: one query per collection per batch,
with the server still doing the ordering, the cap and the count.

## Surviving a failover

A binlog file name and offset only mean anything on the server they were read
from, so a checkpoint also records which transactions have been consumed:

```
mysql-bin.000003:2278;gtid=3c63db20-a0cd-11f1-bc85-32cf1c33a72f:1-14
```

With that, pointing the pipeline at a promoted replica resumes rather than
reloading. Two things make it work, and both need the server's cooperation:

- **GTIDs have to be on.** MySQL needs `gtid_mode = ON`; `ON_PERMISSIVE` is not
  enough, because a transaction may then be written with no GTID at all and a
  position built from the stream would silently omit it. MariaDB always has
  them, and needs nothing.
- **The replica has to write its own binlog** — `log_replica_updates = ON` — or
  there is nothing to stream from it once it is promoted.

The new primary's coordinates are a different, usually lower, numbering than the
one the target already holds versions from. Rather than refuse to continue,
pg2osync opens a new *generation*: the version becomes `base + coordinate` with
a base past everything already written, and the log says so —
`versioning documents from a new generation at …`. Documents written after the
promotion therefore outrank what came before them, which is what stops a
failover from leaving the index quietly stale.

`dev/failover-probe.sh` builds a primary and a replica, promotes the replica and
asserts both halves: that the stream resumes without an initial load, and that a
row only the new primary ever had actually lands in the index.

If the GTID position has been purged from the new primary's binlogs, the server
refuses the request rather than starting somewhere else, and the pipeline stops
with what the server said. A fresh initial load is then the only honest repair.

Without GTIDs, a checkpoint still resumes exactly — but only against the server
it was written on. A coordinate behind the checkpoint stops the pipeline instead
of reloading into silence, because the target's versions come from a numbering
that no longer exists.

## Known limitations

| Limitation | Detail | Workaround |
|---|---|---|
| Tagged GTIDs | MySQL 8.4's tagged GTID events are not decoded | Checkpoints fall back to the coordinate and say so; untagged GTIDs are unaffected |
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

The GTID section needs a server that has them. MySQL's image starts with them
off, so a run that exercises it wants the flags:

```sh
docker run -d --name mysql-gtid -p 13308:3306 \
  -e MYSQL_ROOT_PASSWORD=secret -e MYSQL_DATABASE=sourcedb mysql:8.0 \
  --log-bin=mysql-bin --binlog-format=ROW --binlog-row-image=FULL \
  --server-id=77 --gtid-mode=ON --enforce-gtid-consistency=ON
```

Against a server without them the section says it skipped and why, rather than
passing without having tested anything. MariaDB needs none of this.
