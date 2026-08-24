#!/usr/bin/env bash
# End-to-end test suite for the PostgreSQL -> OpenSearch pipeline.
#
# Prerequisites:
#   docker compose -f dev/docker-compose.yml up -d
#   docker exec -i dev-postgres-1 psql -U postgres -d sourcedb < dev/seed.sql
#   cargo build --release
#
# Usage: ./dev/e2e-test.sh
set -euo pipefail

cd "$(dirname "$0")/.."
BIN=./target/release/pg2osync
OS=${OS_URL:-http://localhost:9200}
PG_CONTAINER=${PG_CONTAINER:-dev-postgres-1}
CONFIG=$(mktemp /tmp/pg2osync-e2e-XXXX.toml)
LOG=/tmp/pg2osync-e2e.log
export PG2OSYNC_SOURCE_URL="postgres://postgres:postgres@localhost:15432/sourcedb"
PASS=0; FAIL=0

say()   { printf "\n\033[1m== %s ==\033[0m\n" "$1"; }
ok()    { printf "  \033[32m✓ %s\033[0m\n" "$1"; PASS=$((PASS + 1)); }
bad()   { printf "  \033[31m✗ %s\033[0m\n" "$1"; FAIL=$((FAIL + 1)); }
check() { if [ "$2" = "$3" ]; then ok "$1 ($2)"; else bad "$1 (got '$2', want '$3')"; fi }

jqf()        { python3 -c "import sys,json;d=json.load(sys.stdin);print($1)"; }
os_count()   { curl -s "$OS/$1/_count" | jqf "d['count']"; }
os_field()   { curl -s "$OS/$1/_doc/$2" | jqf "d.get('_source',{}).get('$3','<missing>')"; }
os_has()     { curl -s "$OS/$1/_doc/$2" | jqf "'$3' in d.get('_source',{})"; }
os_status()  { curl -s -o /dev/null -w "%{http_code}" "$OS/$1/_doc/$2"; }
os_len()     { curl -s "$OS/$1/_doc/$2" | jqf "len(d.get('_source',{}).get('$3',[]))"; }
pg()         { docker exec "$PG_CONTAINER" psql -U postgres -d sourcedb -qtAc "$1"; }
refresh()    { curl -s -XPOST "$OS/_refresh" > /dev/null; }

start_sync() {
  nohup $BIN run -c "$CONFIG" &> "$LOG" < /dev/null & disown
}
stop_sync() { pkill -f "pg2osync run" 2> /dev/null || true; }
cleanup()   { stop_sync; rm -f "$CONFIG"; }
trap cleanup EXIT

cat > "$CONFIG" <<'TOML'
[source]
url_env = "PG2OSYNC_SOURCE_URL"
slot_name = "pg2osync_e2e"
publication = "pg2osync_e2e_pub"

[target]
url = "http://localhost:9200"

[metrics]
bind = "127.0.0.1:9111"

[sync.users]
table = "public.users"
index = "e2e_users"
exclude_columns = ["password_hash"]

[sync.users.transform]
email = "redact"

[sync.customers]
table = "public.customers"
index = "e2e_customers"

[[sync.customers.children]]
table = "public.orders"
field = "orders"
foreign_key = "customer_id"
TOML

say "0. Reset state"
stop_sync
pg "DROP PUBLICATION IF EXISTS pg2osync_e2e_pub;" > /dev/null
pg "SELECT pg_drop_replication_slot('pg2osync_e2e') WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name='pg2osync_e2e');" > /dev/null
pg "TRUNCATE users; TRUNCATE orders, customers;" > /dev/null
pg "INSERT INTO users (id,name,email,password_hash,metadata) VALUES
      (1,'alice','alice@test.io','secret-1','{\"role\":\"admin\"}'),
      (2,'bob','bob@test.io','secret-2','{\"role\":\"user\"}'),
      (3,'carol','carol@test.io','secret-3','{}');" > /dev/null
pg "INSERT INTO customers (id,name) VALUES (1,'acme'),(2,'globex');" > /dev/null
pg "INSERT INTO orders (id,customer_id,total) VALUES (10,1,99.90),(11,1,5.00),(12,2,42.00);" > /dev/null
curl -s -XDELETE "$OS/e2e_users,e2e_customers,.pg2osync_meta" > /dev/null
ok "seeded 3 users, 2 customers, 3 orders; indices cleared"

say "1. validate"
if $BIN validate -c "$CONFIG" 2>&1 | grep -q "all checks passed"; then
  ok "validate passes"
else
  bad "validate failed"
fi

say "2. bootstrap creates objects without streaming"
$BIN bootstrap -c "$CONFIG" > /dev/null
check "slot exists" "$(pg "SELECT count(*) FROM pg_replication_slots WHERE slot_name='pg2osync_e2e';")" "1"
check "publication exists" "$(pg "SELECT count(*) FROM pg_publication WHERE pubname='pg2osync_e2e_pub';")" "1"
refresh
check "bootstrap indexed nothing" "$(os_count e2e_users)" "0"

say "3. initial load"
start_sync
sleep 5
refresh
check "users backfilled" "$(os_count e2e_users)" "3"
check "customers backfilled" "$(os_count e2e_customers)" "2"
check "excluded column absent" "$(os_has e2e_users 1 password_hash)" "False"
check "transform applied on backfill" "$(os_field e2e_users 1 email)" "***"
check "children attached during backfill" "$(os_len e2e_customers 1 orders)" "2"

say "4. live streaming"
pg "INSERT INTO users (id,name,email,password_hash) VALUES (4,'dave','dave@test.io','secret-4');" > /dev/null
sleep 2; refresh
check "INSERT propagated" "$(os_count e2e_users)" "4"
check "excluded column still absent" "$(os_has e2e_users 4 password_hash)" "False"

pg "UPDATE users SET name='dave-renamed', email='new@test.io' WHERE id=4;" > /dev/null
sleep 2; refresh
check "UPDATE propagated" "$(os_field e2e_users 4 name)" "dave-renamed"
check "transform applied on update" "$(os_field e2e_users 4 email)" "***"

pg "DELETE FROM users WHERE id=3;" > /dev/null
sleep 2; refresh
check "DELETE propagated" "$(os_status e2e_users 3)" "404"

say "5. nested children stay fresh"
pg "INSERT INTO orders (id,customer_id,total) VALUES (13,2,7.50);" > /dev/null
sleep 2; refresh
check "child INSERT refreshes parent" "$(os_len e2e_customers 2 orders)" "2"
pg "DELETE FROM orders WHERE id=13;" > /dev/null
sleep 2; refresh
check "child DELETE refreshes parent" "$(os_len e2e_customers 2 orders)" "1"

say "6. TRUNCATE clears the index"
pg "TRUNCATE users;" > /dev/null
sleep 3; refresh
check "index cleared after TRUNCATE" "$(os_count e2e_users)" "0"
pg "INSERT INTO users (id,name,email) VALUES (7,'grace','grace@test.io');" > /dev/null
sleep 2; refresh
check "streaming continues after TRUNCATE" "$(os_count e2e_users)" "1"

say "7. checkpoint and WAL safety"
checkpoint=$(curl -s "$OS/.pg2osync_meta/_doc/default" | jqf "d['_source']")
echo "    $checkpoint"
check "checkpoint source" "$(curl -s "$OS/.pg2osync_meta/_doc/default" | jqf "d['_source']['source']")" "postgres"
ckpt_lsn=$(curl -s "$OS/.pg2osync_meta/_doc/default" | jqf "d['_source']['position']")
# Acknowledging past the checkpoint would let PostgreSQL recycle WAL for rows
# that are not indexed yet, which is exactly what loses data on crash-restart.
behind=$(pg "SELECT pg_wal_lsn_diff('$ckpt_lsn'::pg_lsn, confirmed_flush_lsn) >= 0 FROM pg_replication_slots WHERE slot_name='pg2osync_e2e';")
check "slot never acked past the checkpoint" "$behind" "t"

say "8. metrics endpoint"
metrics=$(curl -s http://127.0.0.1:9111/metrics)
if grep -q "pg2osync_position_confirmed" <<< "$metrics" && grep -q "pg2osync_events_total" <<< "$metrics"; then
  ok "metrics expose position and event counters"
else
  bad "metrics missing expected series"
fi

say "9. crash recovery"
pkill -9 -f "pg2osync run"; sleep 1
pg "INSERT INTO users (id,name,email) VALUES (8,'eve-during-downtime','eve@test.io');" > /dev/null
start_sync
sleep 6; refresh
check "row written while down is recovered" "$(os_field e2e_users 8 name)" "eve-during-downtime"

say "10. final consistency"
check "row counts match" "$(pg "SELECT count(*) FROM users;")" "$(os_count e2e_users)"

say "11. status and teardown"
$BIN status -c "$CONFIG" | sed 's/^/    /'
stop_sync; sleep 1
$BIN drop-slot -c "$CONFIG" > /dev/null
check "slot dropped" "$(pg "SELECT count(*) FROM pg_replication_slots WHERE slot_name='pg2osync_e2e';")" "0"
check "publication dropped" "$(pg "SELECT count(*) FROM pg_publication WHERE pubname='pg2osync_e2e_pub';")" "0"

printf "\n\033[1mRESULT: %d passed, %d failed\033[0m\n" "$PASS" "$FAIL"
[ "$FAIL" = "0" ]
