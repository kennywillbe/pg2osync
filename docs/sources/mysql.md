# MySQL / MariaDB source (preview)

> **Status: preview.** The binlog transport and row decoder are complete and
> live-verified against both **MySQL 8.0.46** and **MariaDB 11.8** —
> INSERT/UPDATE/DELETE events with full before/after images decode correctly
> from real replication streams. CLI integration into `pg2osync run` is in
> progress; until it ships, use the `dump-live` example to observe your
> binlog stream decoded.

## How it works

MySQL/MariaDB expose changes through the **binary log** (binlog). pg2osync
implements the replication protocol in-house:

1. **Handshake** — native password and `caching_sha2_password` fast-auth,
   including server-initiated AuthSwitch.
2. **Checksum negotiation** — `SET @master_binlog_checksum = @@global.binlog_checksum`
   so servers with CRC32 checksums stream usable events.
3. **COM_BINLOG_DUMP** — the server streams raw events from a given
   file + position.
4. **Event decoding** — FORMAT_DESCRIPTION, ROTATE, QUERY (transaction
   boundaries), TABLE_MAP (column types + optional metadata), WRITE_ROWS /
   UPDATE_ROWS / DELETE_ROWS v2 (row images), XID (commits).

### MySQL vs. MariaDB wire differences

The decoder handles both dialects transparently:

| Aspect | MySQL 8.x | MariaDB 10/11.x |
|---|---|---|
| WRITE_ROWS_V2 event type | `30` | `23` |
| UPDATE_ROWS_V2 event type | `31` | `24` |
| DELETE_ROWS_V2 event type | `32` | `25` |
| v2 extra-data-length field | present | absent |
| FDE body size | 103 B | 233 B |

Unlike PostgreSQL logical replication, MySQL has **no server-side position
tracking**: there are no slots. pg2osync checkpoints `(binlog file, position)`
after durable flushes and resumes from there; replays after a crash are
harmless thanks to idempotent writes.

## Configuration

```toml
[source]
flavor = "mysql"
url_env = "MYSQL_SOURCE_URL"        # mysql://user:pass@host:3306/mydb
server_id = 424242                  # must be unique in the replica topology

[sync.users]
table = "mydb.users"
index = "users_index"
```

Requirements on the server:

```sql
-- row-based binlog with full row images (required for UPDATE before-images
-- and DELETE payloads)
SET GLOBAL binlog_format = ROW;
SET GLOBAL binlog_row_image = FULL;

-- optional but recommended: puts column names + PK info into TABLE_MAP
SET GLOBAL binlog_row_metadata = FULL;   -- MySQL only

-- replication privileges for the sync user (MySQL syntax)
GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'sync_user'@'%';
```

## Known limitations

| Limitation | Detail | Workaround |
|---|---|---|
| JSON columns (MySQL) | Binary JSON decodes as hex placeholder; MariaDB TEXT/BLOB decode as base64 of the payload | Native JSON parser planned |
| `binlog_row_metadata=MINIMAL` | No column names/PK in TABLE_MAP | Resolved from information_schema at bootstrap |
| DATETIME2 timezones | Decoded values verified naive; TZ edge cases untested | Use TIMESTAMP or test your TZ setup |
| `caching_sha2_password` full auth | Requires TLS/RSA over the wire | Use TLS, or `mysql_native_password` for the sync user |

## Trying it now

The `dump-live` example connects to a real server and prints decoded events:

```sh
cargo build --release --examples
./target/release/examples/dump-live <binlog-file> <position>
# then INSERT/UPDATE/DELETE some rows and watch them decode
```

Example output from live runs:

```
[TABLE_MAP] id=115 sourcedb cols=users types=[8, 15, 15, 3, 246, 1, 18, 245]
[INSERT sourcedb.users] id=200, name="live-insert", email="li@x.io", ...
[UPDATE sourcedb.users] BEFORE(email="li@x.io") AFTER(email="upd@x.io")
[XID] commit xid=1003
```
