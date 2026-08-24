#!/usr/bin/env bash
# End-to-end test suite for the MySQL/MariaDB source.
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
CONFIG=$(mktemp /tmp/pg2osync-mysql-XXXX.toml)
LOG=/tmp/pg2osync-mysql-e2e.log
export PG2OSYNC_MYSQL_URL="mysql://$USER:$PASSWORD@localhost:$PORT/sourcedb"
PASS=0; FAIL=0

say()   { printf "\n\033[1m== %s ==\033[0m\n" "$1"; }
ok()    { printf "  \033[32m✓ %s\033[0m\n" "$1"; PASS=$((PASS + 1)); }
bad()   { printf "  \033[31m✗ %s\033[0m\n" "$1"; FAIL=$((FAIL + 1)); }
check() { if [ "$2" = "$3" ]; then ok "$1 ($2)"; else bad "$1 (got '$2', want '$3')"; fi }

jqf()       { python3 -c "import sys,json;d=json.load(sys.stdin);print($1)"; }
os_count()  { curl -s "$OS/$1/_count" | jqf "d['count']"; }
os_field()  { curl -s "$OS/$1/_doc/$2" | jqf "d.get('_source',{}).get('$3','<missing>')"; }
os_has()    { curl -s "$OS/$1/_doc/$2" | jqf "'$3' in d.get('_source',{})"; }
os_status() { curl -s -o /dev/null -w "%{http_code}" "$OS/$1/_doc/$2"; }
my()        { docker exec "$CONTAINER" "$CLIENT" -uroot -p"$ROOT_PASSWORD" -N -B sourcedb -e "$1" 2>/dev/null; }
refresh()   { curl -s -XPOST "$OS/_refresh" > /dev/null; }

start_sync() { nohup $BIN run -c "$CONFIG" &> "$LOG" < /dev/null & disown; }
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

[sync.shop_users]
table = "sourcedb.shop_users"
index = "e2e_mysql_users"
exclude_columns = ["password_hash"]

[sync.shop_users.transform]
email = "redact"
TOML

say "0. Reset state"
stop_sync
my "TRUNCATE shop_users;"
my "INSERT INTO shop_users (id,name,email,password_hash,balance,metadata) VALUES
      (1,'alice','alice@test.io','secret-1',10.25,'{\"role\":\"admin\"}'),
      (2,'bob','bob@test.io','secret-2',0.00,'{\"role\":\"user\"}'),
      (3,'carol','carol@test.io','secret-3',99.99,NULL);"
curl -s -XDELETE "$OS/e2e_mysql_users,.pg2osync_meta" > /dev/null
ok "seeded 3 rows, indices cleared"

say "1. validate"
if $BIN validate -c "$CONFIG" 2>&1 | grep -q "all checks passed"; then
  ok "validate passes"
else
  bad "validate failed"
  $BIN validate -c "$CONFIG" 2>&1 | tail -5 | sed 's/^/    /'
fi

say "2. snapshot"
start_sync
sleep 6; refresh
check "snapshot indexed all rows" "$(os_count e2e_mysql_users)" "3"
check "named columns, not ordinals" "$(os_field e2e_mysql_users 1 name)" "alice"
check "decimal keeps precision" "$(os_field e2e_mysql_users 3 balance)" "99.99"
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
check "UPDATE keeps decimals" "$(os_field e2e_mysql_users 4 balance)" "8.50"

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

say "5. checkpoint format"
source_kind=$(curl -s "$OS/.pg2osync_meta/_doc/default" | jqf "d['_source']['source']")
position=$(curl -s "$OS/.pg2osync_meta/_doc/default" | jqf "d['_source']['position']")
check "checkpoint source" "$source_kind" "mysql"
if [[ "$position" == *":"* ]]; then
  ok "binlog position stored ($position)"
else
  bad "binlog position malformed ($position)"
fi

say "6. reconnects after the server kills the dump thread"
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

say "7. crash recovery resumes from the binlog position"
pkill -9 -f "pg2osync run"; sleep 1
my "INSERT INTO shop_users (id,name,email) VALUES (5,'eve-during-downtime','eve@test.io');"
start_sync
sleep 6; refresh
check "row written while down is recovered" "$(os_field e2e_mysql_users 5 name)" "eve-during-downtime"
check "no full re-snapshot needed" "$(grep -c 'snapshot of' "$LOG")" "0"

say "8. final consistency"
check "row counts match" "$(my 'SELECT count(*) FROM shop_users;')" "$(os_count e2e_mysql_users)"

say "9. status"
$BIN status -c "$CONFIG" | sed 's/^/    /'

printf "\n\033[1mRESULT: %d passed, %d failed\033[0m\n" "$PASS" "$FAIL"
[ "$FAIL" = "0" ]
