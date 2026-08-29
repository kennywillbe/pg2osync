#!/usr/bin/env bash
# End-to-end test suite for the MySQL/MariaDB source.
#
# Runs one at a time: stopping the pipeline kills every pg2osync process, so
# two suites at once take each other's down and report failures that are not.
#
# Prerequisites: a MySQL 8.0+ or MariaDB 10.6+ server with log_bin,
# binlog_format=ROW, binlog_row_image=FULL and a mysql_native_password user
# holding SELECT, REPLICATION SLAVE and REPLICATION CLIENT.
#
# Usage: MYSQL_PORT=13306 MYSQL_CONTAINER=mysql-test ./dev/e2e-mysql-test.sh
set -euo pipefail

cd "$(dirname "$0")/.."
BIN=./target/release/pg2osync
OS=${OS_URL:-http://localhost:9200}
CONTAINER=${MYSQL_CONTAINER:-mysql-test}
PORT=${MYSQL_PORT:-13306}
USER=${MYSQL_USER:-repl}
PASSWORD=${MYSQL_PASSWORD:-replpw}
ROOT_PASSWORD=${MYSQL_ROOT_PASSWORD:-mysqlpw}
# MariaDB images ship the client as `mariadb`, MySQL images as `mysql`
CLIENT=${MYSQL_CLIENT:-mysql}
# BSD mktemp only substitutes X's at the *end* of the template: with a
# suffix after them it creates the literal name instead, and one killed run
# then breaks every later one with "File exists".
CONFIG=$(mktemp /tmp/pg2osync-mysql.XXXXXX)
LOG=/tmp/pg2osync-mysql-e2e.log
export PG2OSYNC_MYSQL_URL="mysql://$USER:$PASSWORD@localhost:$PORT/sourcedb"
PASS=0; FAIL=0

say()   { printf "\n\033[1m== %s ==\033[0m\n" "$1"; }
ok()    { printf "  \033[32m✓ %s\033[0m\n" "$1"; PASS=$((PASS + 1)); }
bad()   { printf "  \033[31m✗ %s\033[0m\n" "$1"; FAIL=$((FAIL + 1)); }
check() { if [ "$2" = "$3" ]; then ok "$1 ($2)"; else bad "$1 (got '$2', want '$3')"; fi }

jqf()       { python3 -c "import sys,json;d=json.load(sys.stdin);print($1)"; }
os_count()  { curl -s "$OS/$1/_count" | jqf "d.get('count', 0)"; }
os_field()  { curl -s "$OS/$1/_doc/$2" | jqf "d.get('_source',{}).get('$3','<missing>')"; }
os_has()    { curl -s "$OS/$1/_doc/$2" | jqf "'$3' in d.get('_source',{})"; }
os_len()    { curl -s "$OS/$1/_doc/$2" | jqf "len(d.get('_source',{}).get('$3') or [])"; }
os_status() { curl -s -o /dev/null -w "%{http_code}" "$OS/$1/_doc/$2"; }
my()        { docker exec "$CONTAINER" "$CLIENT" -uroot -p"$ROOT_PASSWORD" -N -B sourcedb -e "$1" 2>/dev/null; }
refresh()   { curl -s -XPOST "$OS/_refresh" > /dev/null; }
# Every table loads beside the stream, so one table reaching the source's count
# says nothing about another one; each index has to be waited for on its own.
await_count() {
  for _ in $(seq 1 180); do
    refresh
    [ "$(os_count "$1")" = "$2" ] && return 0
    sleep 1
  done
}
synced()    { curl -s "http://127.0.0.1:9132/synced?refresh=true&timeout=10000" > /dev/null; refresh; }

start_sync() { nohup $BIN run -c "$CONFIG" >> "$LOG" 2>&1 < /dev/null & disown; }
stop_sync()  { pkill -f "pg2osync run" 2> /dev/null || true; }
cleanup()    { stop_sync; rm -f "$CONFIG"; }
trap cleanup EXIT

cat > "$CONFIG" <<TOML
[source]
flavor = "mysql"
url_env = "PG2OSYNC_MYSQL_URL"
server_id = 990001

[target]
url = "$OS"

[metrics]
bind = "127.0.0.1:9112"

[api]
enabled = true
bind = "127.0.0.1:9132"

[sync.shop_users]
table = "sourcedb.shop_users"
index = "e2e_mysql_users"
exclude_columns = ["password_hash"]

[sync.shop_users.transform]
email = "redact"

[sync.shop_users.fields]
balance = "credit"

[sync.shop_users.constants]
origin = "{schema}.{table}"
TOML

say "0. Reset state"
stop_sync
my "TRUNCATE shop_users;"
my "INSERT INTO shop_users (id,name,email,password_hash,balance,metadata) VALUES
      (1,'alice','alice@test.io','secret-1',10.25,'{\"role\":\"admin\"}'),
      (2,'bob','bob@test.io','secret-2',0.00,'{\"role\":\"user\"}'),
      (3,'carol','carol@test.io','secret-3',99.99,NULL);"
curl -s -XDELETE "$OS/e2e_mysql_users,.pg2osync_meta?ignore_unavailable=true" > /dev/null
ok "seeded 3 rows, indices cleared"

say "1. validate"
if $BIN validate -c "$CONFIG" 2>&1 | grep -q "all checks passed"; then
  ok "validate passes"
else
  bad "validate failed"
  $BIN validate -c "$CONFIG" 2>&1 | tail -5 | sed 's/^/    /'
fi

say "2. initial load"
start_sync
sleep 6; refresh
check "the load indexed all rows" "$(os_count e2e_mysql_users)" "3"
check "named columns, not ordinals" "$(os_field e2e_mysql_users 1 name)" "alice"
check "decimal keeps precision" "$(os_field e2e_mysql_users 3 credit)" "99.99"
check "the source name is gone after the rename" "$(os_has e2e_mysql_users 3 balance)" "False"
check "the origin placeholder is rendered" "$(os_field e2e_mysql_users 1 origin)" "sourcedb.shop_users"
check "excluded column absent" "$(os_has e2e_mysql_users 1 password_hash)" "False"
check "transform applied" "$(os_field e2e_mysql_users 1 email)" "***"

say "3. live binlog streaming"
my "INSERT INTO shop_users (id,name,email,balance) VALUES (4,'dave','dave@test.io',7.00);"
sleep 3; refresh
check "INSERT propagated" "$(os_count e2e_mysql_users)" "4"
check "insert uses named columns" "$(os_field e2e_mysql_users 4 name)" "dave"

my "UPDATE shop_users SET name='dave-renamed', balance=8.50 WHERE id=4;"
sleep 3; refresh
check "UPDATE propagated" "$(os_field e2e_mysql_users 4 name)" "dave-renamed"
check "UPDATE keeps decimals" "$(os_field e2e_mysql_users 4 credit)" "8.50"

my "INSERT INTO shop_users (id,name,metadata) VALUES (7,'jane','{\"tier\":\"gold\",\"tags\":[1,2]}');"
synced
case "$(os_field e2e_mysql_users 7 metadata)" in
  *__mysql_json_hex*) bad "JSON streamed as a hex placeholder" ;;
  *) ok "JSON decoded rather than left as hex" ;;
esac
if [ "$CLIENT" = "mysql" ]; then
  # MariaDB stores JSON as LONGTEXT, so only MySQL produces a nested document
  found=$(curl -s "$OS/e2e_mysql_users/_search" -H 'Content-Type: application/json' \
    -d '{"query":{"term":{"metadata.tier":"gold"}}}' | jqf "d['hits']['total']['value']")
  check "a streamed JSON field is searchable by subfield" "$found" "1"
fi
my "DELETE FROM shop_users WHERE id=7;"
synced

my "DELETE FROM shop_users WHERE id=3;"
sleep 3; refresh
check "DELETE propagated" "$(os_status e2e_mysql_users 3)" "404"

say "4. changing a primary key moves the document"
my "UPDATE shop_users SET id = 40 WHERE id = 4;"
sleep 3; refresh
check "row lives at its new id" "$(os_field e2e_mysql_users 40 name)" "dave-renamed"
check "old document removed" "$(os_status e2e_mysql_users 4)" "404"
my "DELETE FROM shop_users WHERE id = 40;"
sleep 3; refresh
check "deleting the moved row leaves nothing" "$(os_status e2e_mysql_users 40)" "404"

say "5. TRUNCATE clears the index"
my "TRUNCATE TABLE shop_users;"
# wait on the pipeline rather than on a guess: /synced returns once everything
# committed before it has been applied, which keeps this step deterministic
synced
check "index cleared after TRUNCATE" "$(os_count e2e_mysql_users)" "0"
my "INSERT INTO shop_users (id,name,email) VALUES (7,'after-truncate','g@test.io');"
synced
check "streaming continues after TRUNCATE" "$(os_count e2e_mysql_users)" "1"

say "6. checkpoint format"
source_kind=$(curl -s "$OS/.pg2osync_meta/_doc/mysql-990001" | jqf "d['_source']['source']")
position=$(curl -s "$OS/.pg2osync_meta/_doc/mysql-990001" | jqf "d['_source']['position']")
check "checkpoint source" "$source_kind" "mysql"
if [[ "$position" == *":"* ]]; then
  ok "binlog position stored ($position)"
else
  bad "binlog position malformed ($position)"
fi

say "7. reconnects after the server kills the dump thread"
before_pid=$(pgrep -f "pg2osync run" | head -1)
# information_schema.PROCESSLIST rather than performance_schema.threads:
# MariaDB ships with performance_schema disabled, so the latter finds nothing
# and the kill silently becomes a no-op
dump_id=$(my "SELECT ID FROM information_schema.PROCESSLIST WHERE COMMAND LIKE 'Binlog Dump%' LIMIT 1;")
if [ -z "$dump_id" ]; then
  bad "no Binlog Dump connection found to kill"
else
  my "KILL ${dump_id};" || true
fi
my "INSERT INTO shop_users (id,name,email) VALUES (9,'written-while-disconnected','w@test.io');"
sleep 8; refresh
check "same process recovered" "$(pgrep -f "pg2osync run" | head -1)" "$before_pid"
check "row written while disconnected arrived" "$(os_field e2e_mysql_users 9 name)" "written-while-disconnected"
metrics=$(curl -s http://127.0.0.1:9112/metrics)
reconnects=$(awk '$1 == "pg2osync_reconnects_total" {print $2}' <<< "$metrics")
if [ "${reconnects:-0}" -ge 1 ]; then ok "reconnects_total counted it ($reconnects)"; else bad "reconnects_total still zero"; fi

say "8. read-your-writes"
my "INSERT INTO shop_users (id,name,email) VALUES (11,'ryw','r@test.io');"
synced=$(curl -s "http://127.0.0.1:9132/synced?refresh=true&timeout=8000")
found=$(curl -s "$OS/e2e_mysql_users/_search" -H 'Content-Type: application/json' \
  -d '{"query":{"term":{"id":11}}}' | jqf "d['hits']['total']['value']")
check "the row is searchable the moment /synced returns" "$found" "1"
ok "waited $(jqf "d['waited_ms']" <<< "$synced")ms"

say "9. crash recovery resumes from the binlog position"
loads_before=$(grep -c 'rows from sourcedb' "$LOG")
pkill -9 -f "pg2osync run"; sleep 1
my "INSERT INTO shop_users (id,name,email) VALUES (5,'eve-during-downtime','eve@test.io');"
start_sync
sleep 6; refresh
check "row written while down is recovered" "$(os_field e2e_mysql_users 5 name)" "eve-during-downtime"
check "no full reload needed" "$(( $(grep -c 'rows from sourcedb' "$LOG") - loads_before ))" "0"

say "10. final consistency"
check "row counts match" "$(my 'SELECT count(*) FROM shop_users;')" "$(os_count e2e_mysql_users)"

say "11. status"
$BIN status -c "$CONFIG" | sed 's/^/    /'

say "12. chunked load: resumed mid-load, and running beside the stream"
stop_sync
RCONFIG=$(mktemp /tmp/pg2osync-mysql-resume.XXXXXX)
RSID=990002
cat > "$RCONFIG" <<TOML
[source]
flavor = "mysql"
url_env = "PG2OSYNC_MYSQL_URL"
server_id = $RSID
load_chunk_rows = 5000

[target]
url = "$OS"

[metrics]
bind = "127.0.0.1:9113"

[sync.big]
table = "sourcedb.resume_probe"
index = "e2e_mysql_resume"

[sync.composite]
table = "sourcedb.composite_probe"
index = "e2e_mysql_composite"
TOML
resume_cleanup() {
  pkill -9 -f "pg2osync run" 2> /dev/null || true
  my "DROP TABLE IF EXISTS resume_probe; DROP TABLE IF EXISTS composite_probe;" > /dev/null 2>&1 || true
  rm -f "$RCONFIG"
}
trap 'cleanup; resume_cleanup' EXIT

# Doubling rather than a recursive CTE: MySQL caps recursion at
# cte_max_recursion_depth and MariaDB spells that setting differently.
#
# The keys are computed rather than left to auto_increment, whose default lock
# mode in MySQL 8.0 is interleaved and which is documented to leave gaps for
# bulk inserts — that would leave the very rows this section names absent. The
# offset is a shell literal because a subquery on the table being inserted into
# is error 1093; selecting from the target table itself is allowed.
my "DROP TABLE IF EXISTS resume_probe;
    CREATE TABLE resume_probe(id bigint primary key, v varchar(255));"
my "INSERT INTO resume_probe VALUES (1, REPEAT('x',200));"
grown=1
for _ in $(seq 1 17); do
  my "INSERT INTO resume_probe SELECT id + $grown, v FROM resume_probe;"
  grown=$((grown * 2))
done
# a composite key is what forces the expanded cursor predicate rather than a
# row constructor, which MySQL plans as type: index
my "DROP TABLE IF EXISTS composite_probe;
    CREATE TABLE composite_probe(tenant varchar(16), id bigint, v varchar(64),
                                 primary key (tenant, id));"
my "INSERT INTO composite_probe SELECT IF(id%2=0,'acme','globex'), id, CONCAT('c-', id)
      FROM resume_probe WHERE id <= 5000;"
curl -s -XDELETE "$OS/e2e_mysql_resume,e2e_mysql_composite?ignore_unavailable=true" > /dev/null
PROG=load-mysql-$RSID-sourcedb_resume_probe
curl -s -XDELETE "$OS/.pg2osync_meta/_doc/$PROG" > /dev/null
big_rows=$(my 'SELECT count(*) FROM resume_probe;')
composite_rows=$(my 'SELECT count(*) FROM composite_probe;')

cursor_len() { curl -s "$OS/.pg2osync_meta/_doc/$PROG" | jqf "len((d.get('_source') or {}).get('after') or [])"; }
nohup $BIN run -c "$RCONFIG" >> "$LOG" 2>&1 < /dev/null & disown
for _ in $(seq 1 120); do
  [ "$(cursor_len)" -ge 1 ] 2> /dev/null && break
  sleep 0.2
done
cursor_at_kill=$(cursor_len)
pkill -9 -f "pg2osync run"; sleep 1
if [ "$cursor_at_kill" -ge 1 ]; then
  ok "progress recorded per chunk, as a key"
else
  bad "no per-chunk progress recorded (cursor length '$cursor_at_kill')"
fi

# the interesting part: the source moves while nothing is watching it, so the
# replay argument the chunked load rests on is what has to repair the result
my "DELETE FROM resume_probe WHERE id IN (5, 100000);"
my "INSERT INTO resume_probe(id,v) VALUES (400001,'added-while-down');"
my "UPDATE resume_probe SET v='updated-while-down' WHERE id = 77;"
src_rows=$(my 'SELECT count(*) FROM resume_probe;')

nohup $BIN run -c "$RCONFIG" >> "$LOG" 2>&1 < /dev/null & disown
# The load now runs beside the stream, so a row can change while the chunk
# holding it is still being read. The streamed change carries a higher binlog
# position than the chunk did, so it has to win — the version is the only thing
# stopping the stale copied row from landing on top of it.
sleep 1
my "UPDATE resume_probe SET v='changed-during-the-load' WHERE id = 131000;"
# And a row deleted while the load is still running. The version cannot protect
# this one on its own: a delete leaves a tombstone that lives for gc_deletes, and
# a copy row starved past that would be accepted back. The engine drops such a
# row instead of offering it. This case only pins the ordering — the starvation
# that breaks it cannot be staged from a shell script.
my "DELETE FROM resume_probe WHERE id = 130000;"
# one row fewer to wait for, now that the load itself is racing a delete
src_rows=$((src_rows - 1))
await_count e2e_mysql_resume "$src_rows"
check "every row is indexed after the restart" "$(os_count e2e_mysql_resume)" "$src_rows"
if grep -q "resuming the load of sourcedb.resume_probe after key" "$LOG"; then
  ok "the load resumed from its cursor instead of starting over"
else
  bad "the load restarted from the beginning"
fi
await_count e2e_mysql_composite "$composite_rows"
check "a composite key loads completely" "$(os_count e2e_mysql_composite)" "$composite_rows"
# no /synced endpoint on this config, and the row count matching only proves the
# changes it counts have landed — the rest of the stream needs a moment
sleep 3; refresh
check "a row deleted while down is gone" "$(os_status e2e_mysql_resume 100000)" "404"
check "a row added while down arrived" "$(os_field e2e_mysql_resume 400001 v)" "added-while-down"
check "a row updated while down is current" "$(os_field e2e_mysql_resume 77 v)" "updated-while-down"
check "a row changed during the load is not overwritten by it" \
  "$(os_field e2e_mysql_resume 131000 v)" "changed-during-the-load"
check "a row deleted during the load is not resurrected by it" \
  "$(os_status e2e_mysql_resume 130000)" "404"
# the load holding one read view for its whole duration is what this replaced
open_trx=$(my "SELECT count(*) FROM information_schema.innodb_trx;")
check "no transaction outlives the load" "$open_trx" "0"
stop_sync

say "13. the load and the stream agree on every column type"
stop_sync
TCONFIG=$(mktemp /tmp/pg2osync-mysql-types.XXXXXX)
cat > "$TCONFIG" <<TOML
[source]
flavor = "mysql"
url_env = "PG2OSYNC_MYSQL_URL"
server_id = 990003

[target]
url = "$OS"

[metrics]
bind = "127.0.0.1:9114"

[sync.types]
table = "sourcedb.types_probe"
index = "e2e_mysql_types"
TOML
types_cleanup() {
  pkill -9 -f "pg2osync run" 2> /dev/null || true
  my "DROP TABLE IF EXISTS types_probe;" > /dev/null 2>&1 || true
  rm -f "$TCONFIG"
}
trap 'cleanup; resume_cleanup; types_cleanup' EXIT

# The row image says nothing about whether a string column holds characters or
# bytes, and nothing about what an enum ordinal means, so these are exactly the
# types the two readers used to disagree on. Row 1 arrives through the initial
# load and row 2 through the binlog, from identical values.
my "DROP TABLE IF EXISTS types_probe;
    CREATE TABLE types_probe(
      id     bigint PRIMARY KEY,
      txt    text,
      bin    varbinary(16),
      blb    blob,
      bits   bit(16),
      grade  enum('low','medium','high'),
      tags   set('a','b','c'));"
my "INSERT INTO types_probe VALUES (1,'hello',0x00FF10,0x0102,b'0000000011111111','medium','a,c');"
curl -s -XDELETE "$OS/e2e_mysql_types" > /dev/null
curl -s -XDELETE "$OS/.pg2osync_meta/_doc/checkpoint-mysql-990003" > /dev/null
curl -s -XDELETE "$OS/.pg2osync_meta/_doc/load-mysql-990003-sourcedb_types_probe" > /dev/null

nohup $BIN run -c "$TCONFIG" >> "$LOG" 2>&1 < /dev/null & disown
for _ in $(seq 1 60); do
  refresh
  [ "$(os_count e2e_mysql_types)" = "1" ] && break
  sleep 0.5
done
# same values again, this time reaching the target through the binlog
my "INSERT INTO types_probe VALUES (2,'hello',0x00FF10,0x0102,b'0000000011111111','medium','a,c');"
for _ in $(seq 1 60); do
  refresh
  [ "$(os_count e2e_mysql_types)" = "2" ] && break
  sleep 0.5
done
check "both rows arrived" "$(os_count e2e_mysql_types)" "2"
for col in txt bin blb bits grade tags; do
  loaded=$(os_field e2e_mysql_types 1 $col)
  streamed=$(os_field e2e_mysql_types 2 $col)
  if [ "$loaded" = "$streamed" ]; then
    ok "$col reads the same from the load and the stream ($loaded)"
  else
    bad "$col disagrees (load '$loaded', stream '$streamed')"
  fi
done
# and the shapes themselves, so "agreeing on the wrong thing" still fails
check "text is characters" "$(os_field e2e_mysql_types 1 txt)" "hello"
check "varbinary is base64 of its bytes" "$(os_field e2e_mysql_types 1 bin)" "AP8Q"
check "blob is base64 of its bytes" "$(os_field e2e_mysql_types 1 blb)" "AQI="
check "a two-byte bit is a number" "$(os_field e2e_mysql_types 1 bits)" "255"
check "an enum is its label" "$(os_field e2e_mysql_types 1 grade)" "medium"
check "a set is its labels" "$(os_field e2e_mysql_types 1 tags)" "['a', 'c']"
stop_sync

say "14. re-snapshot one table on demand"
# The MySQL loader is a separate implementation from PostgreSQL's, so the command
# is exercised on both rather than assumed to work from one.
nohup $BIN run -c "$RCONFIG" >> "$LOG" 2>&1 < /dev/null & disown
sleep 4
curl -s -XDELETE "$OS/e2e_mysql_resume/_doc/500" > /dev/null
# Corrupting by hand uses internal versioning, which bumps the version one past
# the source's current position, so the binlog has to move on before a
# re-snapshot can replace it — as it always has by the time a fix is deployed.
curl -s -XPUT "$OS/e2e_mysql_resume/_doc/600" -H 'Content-Type: application/json' \
  -d '{"id":600,"v":"CORRUPT"}' > /dev/null
my "INSERT INTO resume_probe VALUES (500001,'moves-the-binlog-on');"
sleep 2
refresh
check "a document was removed from the index" "$(os_status e2e_mysql_resume 500)" "404"

$BIN resnapshot -c "$RCONFIG" --table sourcedb.resume_probe >> "$LOG" 2>&1
refresh
check "the missing document is back" "$(os_status e2e_mysql_resume 500)" "200"
if [ "$(os_field e2e_mysql_resume 600 v)" != "CORRUPT" ]; then
  ok "the wrong value was replaced by the source's"
else
  bad "the wrong value survived the re-snapshot"
fi
check "the other table was left alone" \
  "$(os_count e2e_mysql_composite)" "$composite_rows"

curl -s -XDELETE "$OS/e2e_mysql_resume/_doc/700" > /dev/null
curl -s -XDELETE "$OS/e2e_mysql_resume/_doc/701" > /dev/null
# the hand deletion leaves a tombstone one version past the source, so the source
# moves on before the re-snapshot can replace it
my "INSERT INTO resume_probe VALUES (500003,'moves-the-binlog-on-again');"
sleep 2
refresh
$BIN resnapshot -c "$RCONFIG" --table sourcedb.resume_probe --where "id = 700" >> "$LOG" 2>&1
refresh
check "--where restored the row it names" "$(os_status e2e_mysql_resume 700)" "200"
check "--where left the others alone" "$(os_status e2e_mysql_resume 701)" "404"
check "no load progress left behind" \
  "$(curl -s "$OS/.pg2osync_meta/_search?q=_id:load*&size=20" | jqf "len(d['hits']['hits'])")" "0"

# Attributable only with the pipeline stopped: while it streams the position
# advances on its own from whatever else the server is doing.
stop_sync
sleep 1
before=$($BIN status -c "$RCONFIG" | grep -o 'position=[^ ]*' | head -1)
$BIN resnapshot -c "$RCONFIG" --table sourcedb.resume_probe >> "$LOG" 2>&1
check "a re-snapshot does not move the checkpoint" \
  "$($BIN status -c "$RCONFIG" | grep -o 'position=[^ ]*' | head -1)" "$before"

say "15. nested children"
stop_sync
CCONFIG=$(mktemp /tmp/pg2osync-mysql-child.XXXXXX)
CSID=990004
cat > "$CCONFIG" <<TOML
[source]
flavor = "mysql"
url_env = "PG2OSYNC_MYSQL_URL"
server_id = $CSID

[target]
url = "$OS"

[metrics]
bind = "127.0.0.1:9116"

[sync.kid_parent]
table = "sourcedb.kid_parent"
index = "e2e_mysql_kid"
primary_key = "id"

[[sync.kid_parent.children]]
table = "sourcedb.kid_item"
field = "items"
foreign_key = "parent_id"

# the same child table as its own index, so an embedded object can be compared
# against the very row it came from
[sync.kid_item]
table = "sourcedb.kid_item"
index = "e2e_mysql_kid_item"
TOML
child_cleanup() {
  pkill -9 -f "pg2osync run" 2> /dev/null || true
  my "DROP TABLE IF EXISTS kid_item; DROP TABLE IF EXISTS kid_parent;" > /dev/null 2>&1 || true
  rm -f "$CCONFIG"
}
trap 'cleanup; resume_cleanup; types_cleanup; child_cleanup' EXIT

my "DROP TABLE IF EXISTS kid_item; DROP TABLE IF EXISTS kid_parent;"
my "CREATE TABLE kid_parent(id bigint PRIMARY KEY, name varchar(32));"
# the types MySQL's own JSON functions render differently from this pipeline:
# varbinary, bit, set and decimal. Embedding them is the case that would break
# silently if the array were built by JSON_ARRAYAGG(JSON_OBJECT(...)).
my "CREATE TABLE kid_item(id bigint PRIMARY KEY, parent_id bigint,
                          b varbinary(8), d decimal(10,2), bt bit(16),
                          s set('a','b','c'), label varchar(32), INDEX(parent_id));"
my "INSERT INTO kid_parent VALUES (1,'one'),(2,'two');"
my "INSERT INTO kid_item VALUES (10,1,0x00FF10,12.34,b'0000000011111111','a,c','i10'),
                                (11,1,0x01,0.10,b'1','','i11');"
curl -s -XDELETE "$OS/e2e_mysql_kid,e2e_mysql_kid_item?ignore_unavailable=true" > /dev/null
curl -s -XDELETE "$OS/.pg2osync_meta/_doc/mysql-$CSID" > /dev/null

nohup $BIN run -c "$CCONFIG" >> "$LOG" 2>&1 < /dev/null & disown
for _ in $(seq 1 60); do
  refresh
  [ "$(os_count e2e_mysql_kid)" = "2" ] && break
  sleep 0.5
done
check "children attached during the load" "$(os_len e2e_mysql_kid 1 items)" "2"
check "a childless parent gets an empty array" "$(os_len e2e_mysql_kid 2 items)" "0"
check "and has the field at all" "$(os_has e2e_mysql_kid 2 items)" "True"

my "INSERT INTO kid_item VALUES (12,1,0x02,1.00,b'10','b','i12');"
sleep 3; refresh
check "child INSERT refreshes the parent" "$(os_len e2e_mysql_kid 1 items)" "3"
my "DELETE FROM kid_item WHERE id = 12;"
sleep 3; refresh
# binlog_row_image = FULL is what carries the foreign key on a delete, so the
# parent is always locatable — no REPLICA IDENTITY caveat as on PostgreSQL
check "child DELETE refreshes the parent" "$(os_len e2e_mysql_kid 1 items)" "2"

# many children of one parent in one statement: deduplicated to one document
my "INSERT INTO kid_item SELECT 1000+seq, 2, 0x03, 2.00, b'11', 'c', CONCAT('m',seq)
      FROM (SELECT 1 seq UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5
            UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9
            UNION SELECT 10) t;"
sleep 4; refresh
check "one statement of 10 children lands whole" "$(os_len e2e_mysql_kid 2 items)" "10"

# The assertion the Rust-side aggregation exists for: an embedded child object
# must be the same JSON as the row it came from indexed on its own.
in_array=$(curl -s "$OS/e2e_mysql_kid/_doc/1" \
  | jqf "[{k:v for k,v in i.items() if k in ('b','d','bt','s')} for i in ((d.get('_source') or {}).get('items') or []) if i.get('id')==10]")
as_doc=$(curl -s "$OS/e2e_mysql_kid_item/_doc/10" \
  | jqf "[{k:v for k,v in (d.get('_source') or {}).items() if k in ('b','d','bt','s')}]")
if [ "$in_array" = "$as_doc" ]; then
  ok "an embedded child is the same JSON as the row itself ($in_array)"
else
  bad "an embedded child differs from the row itself (array $in_array, doc $as_doc)"
fi
check "and it is the pipeline's own binary form, not MySQL's" \
  "$(curl -s "$OS/e2e_mysql_kid_item/_doc/10" | jqf "(d.get('_source') or {}).get('b')")" "AP8Q"
stop_sync

say "16. GTID positions in the checkpoint"
# A file name and offset only mean anything on the server they came from, so the
# checkpoint has to say which transactions were consumed. MariaDB always has
# GTIDs; MySQL only when it was started with them, which is why this section
# says what it skipped rather than passing on a server that cannot show it.
# Asked in two steps: MariaDB has no `gtid_mode` at all, and naming it in a
# branch that MariaDB never takes still fails when the statement is resolved.
if [ "$(my "SELECT VERSION() LIKE '%MariaDB%';")" = "1" ]; then
  gtid_capable=yes
elif [ "$(my "SELECT @@global.gtid_mode;")" = "ON" ]; then
  gtid_capable=yes
else
  gtid_capable=no
fi
if [ "$gtid_capable" != "yes" ]; then
  echo "  - skipped: this server has GTIDs off (gtid_mode = $(my "SELECT @@global.gtid_mode;"))"
else
  my "DROP TABLE IF EXISTS gtid_probe; CREATE TABLE gtid_probe(id int primary key, v varchar(40));"
  my "INSERT INTO gtid_probe VALUES (1,'loaded');"
  # Written out rather than derived from $CONFIG: rewriting its [sync.*] header
  # renames every one of them to the same key, which is a duplicate-key error.
  GCONFIG=$(mktemp /tmp/pg2osync-mysql-gtid.XXXXXX)
  cat > "$GCONFIG" <<TOML
[source]
flavor = "mysql"
url_env = "PG2OSYNC_MYSQL_URL"
server_id = 990016

[target]
url = "$OS"

[metrics]
enabled = false

[sync.gtid_probe]
table = "sourcedb.gtid_probe"
index = "e2e_mysql_gtid"
TOML
  curl -s -XDELETE "$OS/e2e_mysql_gtid,.pg2osync_meta?ignore_unavailable=true" > /dev/null
  nohup $BIN run -c "$GCONFIG" >> "$LOG" 2>&1 < /dev/null & disown
  for _ in $(seq 1 30); do
    refresh
    [ "$(os_count e2e_mysql_gtid)" = "1" ] && break
    sleep 1
  done
  # a streamed transaction, which is what a recorded position has to cover
  my "INSERT INTO gtid_probe VALUES (2,'streamed');"
  for _ in $(seq 1 30); do
    refresh
    [ "$(os_count e2e_mysql_gtid)" = "2" ] && break
    sleep 1
  done
  # Read while the pipeline is still running, and waited for rather than taken
  # once: the checkpoint task persists on its own interval, so killing the
  # process first and comparing immediately races it.
  checkpoint_gtid() {
    curl -s "$OS/.pg2osync_meta/_search?q=*:*&size=10" \
      | jqf "next((h['_source'].get('position','').split(';gtid=')[-1].split(';')[0] \
              for h in d.get('hits',{}).get('hits',[]) \
              if 'gtid=' in h['_source'].get('position','')), '')"
  }
  mariadb_source=$([ "$(my "SELECT VERSION() LIKE '%MariaDB%';")" = "1" ] && echo yes || echo no)
  for _ in $(seq 1 30); do
    position=$(checkpoint_gtid)
    [ -n "$position" ] || { sleep 1; continue; }
    # The server's own position is the only external check on ours: a set built
    # from the stream has to converge on what the server says it has written.
    [ "$mariadb_source" != "yes" ] && break
    [ "$position" = "$(my "SELECT @@global.gtid_binlog_pos;")" ] && break
    sleep 1
  done
  if [ -n "$position" ]; then
    ok "the checkpoint carries a GTID position ($position)"
  else
    bad "the checkpoint has no GTID position"
  fi
  if [ "$mariadb_source" = "yes" ]; then
    check "it converged on the server's own position" \
      "$position" "$(my "SELECT @@global.gtid_binlog_pos;")"
  fi
  stop_sync; sleep 1
  # And it resumes from it, without losing what happened while it was down
  my "INSERT INTO gtid_probe VALUES (3,'while-down');"
  nohup $BIN run -c "$GCONFIG" >> "$LOG" 2>&1 < /dev/null & disown
  for _ in $(seq 1 30); do
    refresh
    [ "$(os_count e2e_mysql_gtid)" = "3" ] && break
    sleep 1
  done
  check "a row written while down arrived after the resume" \
    "$(os_field e2e_mysql_gtid 3 v)" "while-down"
  if grep -q "binlog dump from gtid" "$LOG"; then
    ok "the stream was asked for by GTID, not by coordinate"
  else
    bad "the stream was asked for by coordinate despite a GTID position"
  fi
  stop_sync
  rm -f "$GCONFIG"
fi

printf "\n\033[1mRESULT: %d passed, %d failed\033[0m\n" "$PASS" "$FAIL"
[ "$FAIL" = "0" ]
