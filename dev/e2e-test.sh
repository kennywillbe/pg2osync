#!/usr/bin/env bash
# End-to-end test suite for the PostgreSQL -> OpenSearch/Elasticsearch pipeline.
#
# One suite at a time per stack: the section names, the tables and the indices
# are fixed, so two suites against the same PostgreSQL and OpenSearch overwrite
# each other's state. dev/e2e-lock.sh enforces that on the shared dev stack
# with a machine-wide lock; a second suite waits. A run with a stack of its own
# — ci-local --isolated — passes E2E_LOCK=none and takes no lock, because a
# stop only ever signals the pipelines this run started (dev/e2e-pipeline.sh).
#
# Prerequisites:
#   docker compose -f dev/docker-compose.yml up -d
#   docker exec -i dev-postgres-1 psql -U postgres -d sourcedb < dev/seed.sql
#   cargo build --release
#
# Usage: ./dev/e2e-test.sh
#   OS_URL         target base URL          (default http://localhost:9200)
#   TARGET_FLAVOR  opensearch|elasticsearch (default opensearch)
#   PG_CONTAINER   psql container name      (default dev-postgres-1)
#   PG_PORT        source port on localhost (default 15432)
#   E2E_LOG        pipeline log file        (default /tmp/pg2osync-e2e.log)
#   E2E_LOCK       lock directory, or none  (default /tmp/pg2osync-e2e.lock)
#   E2E_PORT_BASE  first metrics/API port   (default 9100, a 40-port block)
set -euo pipefail
# shellcheck source=dev/e2e-lock.sh
source "$(dirname "$0")/e2e-lock.sh"
# shellcheck source=dev/e2e-pipeline.sh
source "$(dirname "$0")/e2e-pipeline.sh"
e2e_lock

cd "$(dirname "$0")/.."
BIN=./target/release/pg2osync
OS=${OS_URL:-http://localhost:9200}
TARGET_FLAVOR=${TARGET_FLAVOR:-opensearch}
PG_CONTAINER=${PG_CONTAINER:-dev-postgres-1}
PG_PORT=${PG_PORT:-15432}
# BSD mktemp only substitutes X's at the *end* of the template: with a
# suffix after them it creates the literal name instead, and one killed run
# then breaks every later one with "File exists".
CONFIG=$(mktemp /tmp/pg2osync-e2e.XXXXXX)
# mktemp's suffix, reused for every other file this run writes: two suites at
# once — each against a stack of its own — must not read or delete each other's
# mapping files and probe logs.
TAG=${CONFIG##*.}
MAPPING=$(dirname "$CONFIG")/pg2osync-e2e-mapping-$TAG.json
# The suite counts lines it wrote to this log, so a second run appending to
# the same file makes those assertions read another run's output. E2E_LOG
# gives a caller running suites back to back a file of its own.
LOG=${E2E_LOG:-/tmp/pg2osync-e2e.log}
export PG2OSYNC_SOURCE_URL="postgres://postgres:postgres@localhost:$PG_PORT/sourcedb"
# Every pipeline the suite starts binds its metrics and API ports on this
# machine rather than inside a container, so two suites at once need two
# blocks: E2E_PORT_BASE moves every port this one uses.
PORT_BASE=${E2E_PORT_BASE:-9100}

PASS=0; FAIL=0

say()   { printf "\n\033[1m== %s ==\033[0m\n" "$1"; }
ok()    { printf "  \033[32m✓ %s\033[0m\n" "$1"; PASS=$((PASS + 1)); }
bad()   { printf "  \033[31m✗ %s\033[0m\n" "$1"; FAIL=$((FAIL + 1)); }
check() { if [ "$2" = "$3" ]; then ok "$1 ($2)"; else bad "$1 (got '$2', want '$3')"; fi }

jqf()        { python3 -c "import sys,json;d=json.load(sys.stdin);print($1)"; }
os_count()   { curl -s "$OS/$1/_count" | jqf "d.get('count', 0)"; }
os_field()   { curl -s "$OS/$1/_doc/$2" | jqf "d.get('_source',{}).get('$3','<missing>')"; }
os_has()     { curl -s "$OS/$1/_doc/$2" | jqf "'$3' in d.get('_source',{})"; }
os_status()  { curl -s -o /dev/null -w "%{http_code}" "$OS/$1/_doc/$2"; }
# a join child lives on its parent's shard, so reading it needs the routing
os_routed()  { curl -s "$OS/$1/_doc/$2?routing=$3" | jqf "d.get('_source',{}).get('$4','<missing>')"; }
os_rstatus() { curl -s -o /dev/null -w "%{http_code}" "$OS/$1/_doc/$2?routing=$3"; }
os_len()     { curl -s "$OS/$1/_doc/$2" | jqf "len(d.get('_source',{}).get('$3',[]))"; }
pg()         { docker exec "$PG_CONTAINER" psql -U postgres -d sourcedb -qtAc "$1"; }
refresh()    { curl -s -XPOST "$OS/_refresh" > /dev/null; }
synced()     { curl -s "http://127.0.0.1:$((PORT_BASE + 31))/synced?refresh=true&timeout=10000" > /dev/null; refresh; }

start_sync() { sync_spawn "$CONFIG"; }
stop_sync()  { sync_stop; }
drop_own_slot() { pg "SELECT pg_drop_replication_slot('pg2osync_e2e') WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name='pg2osync_e2e');" > /dev/null 2>&1 || true; }
# Every probe section keeps its slot until the trap at exit, and the dev
# database allows ten; a late section would otherwise start with "all
# replication slots are in use". Only idle slots go — a running pipeline's is
# in use and stays.
drop_idle_probe_slots() { pg "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name LIKE 'pg2osync\\_e2e\\_%' AND NOT active;" > /dev/null 2>&1 || true; }
cleanup()   { stop_sync; drop_own_slot; rm -f "$CONFIG" "$MAPPING"; e2e_unlock; }
trap cleanup EXIT

cat > "$CONFIG" <<TOML
[source]
url_env = "PG2OSYNC_SOURCE_URL"
slot_name = "pg2osync_e2e"
publication = "pg2osync_e2e_pub"

[target]
url = "$OS"
flavor = "$TARGET_FLAVOR"

[metrics]
bind = "127.0.0.1:$((PORT_BASE + 11))"

[api]
enabled = true
bind = "127.0.0.1:$((PORT_BASE + 31))"

[sync.users]
table = "public.users"
index = "e2e_users"
exclude_columns = ["password_hash"]
mapping_file = "pg2osync-e2e-mapping-$TAG.json"

[sync.users.transform]
email = "redact"

[sync.users.fields]
metadata = "meta"
email = "contact"

[sync.users.constants]
entity = "user"
origin = "{schema}.{table}"

[sync.customers]
table = "public.customers"
index = "e2e_customers"

[[sync.customers.children]]
table = "public.orders"
field = "orders"
foreign_key = "customer_id"
exclude_columns = ["internal_notes"]

[sync.customers.children.fields]
total = "amount"

[[sync.customers.children]]
table = "public.tickets"
field = "tickets"
foreign_key = "customer_id"

[[sync.customers.children]]
table = "public.profiles"
field = "profile"
foreign_key = "customer_id"
single = true
TOML

say "0. Reset state"
stop_sync
pg "DROP PUBLICATION IF EXISTS pg2osync_e2e_pub;" > /dev/null
pg "SELECT pg_drop_replication_slot('pg2osync_e2e') WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name='pg2osync_e2e');" > /dev/null
pg "TRUNCATE users; TRUNCATE tickets, orders, profiles, customers;" > /dev/null
# a run killed part way through leaves the drift probe's column behind, and the
# shape change it exists to cause would then not happen
pg "ALTER TABLE users DROP COLUMN IF EXISTS drift_probe;" > /dev/null
pg "INSERT INTO users (id,name,email,password_hash,metadata) VALUES
      (1,'alice','alice@test.io','secret-1','{\"role\":\"admin\"}'),
      (2,'bob','bob@test.io','secret-2','{\"role\":\"user\"}'),
      (3,'carol','carol@test.io','secret-3','{}');" > /dev/null
pg "INSERT INTO customers (id,name) VALUES (1,'acme'),(2,'globex'),(3,'no-children');" > /dev/null
pg "INSERT INTO orders (id,customer_id,total,internal_notes) VALUES
      (10,1,99.90,'do not index'),(11,1,5.00,'nor this'),(12,2,42.00,'nor that');" > /dev/null
pg "INSERT INTO tickets (id,customer_id,subject) VALUES (20,1,'late delivery');" > /dev/null
pg "INSERT INTO profiles (id,customer_id,bio) VALUES (30,1,'first profile');" > /dev/null
# ignore_unavailable, because a multi-index delete where one index is absent
# returns 404 and deletes *none* of them — which silently leaves the previous
# run's documents behind and fails the counts below by however many there were
curl -s -XDELETE "$OS/e2e_users,e2e_customers,.pg2osync_meta?ignore_unavailable=true" > /dev/null
ok "seeded 3 users, 2 customers, 3 orders; indices cleared"

cat > "$MAPPING" <<'JSON'
{ "mappings": { "properties": { "name": { "type": "keyword" } } } }
JSON

say "1. validate"
if $BIN validate -c "$CONFIG" 2>&1 | grep -q "all checks passed"; then
  ok "validate passes"
else
  bad "validate failed"
fi
# a plain-text url is the one thing validate reliably logs, so it is what shows
# the log format a collector would see
sed "s|^url_env = \"PG2OSYNC_SOURCE_URL\"|url = \"$PG2OSYNC_SOURCE_URL\"|" "$CONFIG" > "${CONFIG}.plain"
json_log=$(PG2OSYNC_LOG_FORMAT=json $BIN validate -c "${CONFIG}.plain" 2>&1 | grep -m1 '^{' || true)
check "PG2OSYNC_LOG_FORMAT=json writes JSON log lines" \
  "$(printf '%s' "$json_log" | jqf "d.get('level', '<missing>')" 2> /dev/null || echo '<unparsed>')" "WARN"
rm -f "${CONFIG}.plain"
# a rename of a column that projection drops can never take effect; refuse it
# where it can still be fixed rather than let it pass silently
sed 's/^metadata = "meta"/password_hash = "pw"/' "$CONFIG" > "${CONFIG}.bad"
if $BIN validate -c "${CONFIG}.bad" > /dev/null 2>&1; then
  bad "validate accepted a rename of an excluded column"
else
  ok "validate refuses renaming a column that is excluded"
fi
rm -f "${CONFIG}.bad"
# users projects with exclude_columns, so config load cannot see that a
# constant collides with the real name column — only the catalogue can
sed 's/^entity = "user"/name = "x"/' "$CONFIG" > "${CONFIG}.bad"
if $BIN validate -c "${CONFIG}.bad" > /dev/null 2>&1; then
  bad "validate accepted a constant that would bury a column"
else
  ok "validate refuses a constant that would bury a column"
fi
rm -f "${CONFIG}.bad"

say "2. bootstrap creates objects without streaming"
$BIN bootstrap -c "$CONFIG" > /dev/null
check "slot exists" "$(pg "SELECT count(*) FROM pg_replication_slots WHERE slot_name='pg2osync_e2e';")" "1"
check "publication exists" "$(pg "SELECT count(*) FROM pg_publication WHERE pubname='pg2osync_e2e_pub';")" "1"
refresh
check "bootstrap indexed nothing" "$(os_count e2e_users)" "0"
check "the index was created with the configured mapping" \
  "$(curl -s "$OS/e2e_users/_mapping" | jqf "d['e2e_users']['mappings']['properties']['name']['type']")" \
  "keyword"

say "3. initial load"
start_sync
sleep 5
refresh
check "users backfilled" "$(os_count e2e_users)" "3"
check "customers backfilled" "$(os_count e2e_customers)" "3"
check "excluded column absent" "$(os_has e2e_users 1 password_hash)" "False"
check "transform applied on backfill" "$(os_field e2e_users 1 contact)" "***"
check "the source name is gone after the rename" "$(os_has e2e_users 1 email)" "False"
check "a child column is renamed inside the array" \
  "$(curl -s "$OS/e2e_customers/_doc/1" | jqf "sorted(k for k in d['_source']['orders'][0] if k in ('total','amount'))")" \
  "['amount']"
check "a child column is excluded from the array" \
  "$(curl -s "$OS/e2e_customers/_doc/1" | jqf "'internal_notes' in d['_source']['orders'][0]")" \
  "False"
check "a constant is added to every document" "$(os_field e2e_users 1 entity)" "user"
check "the origin placeholder is rendered" "$(os_field e2e_users 1 origin)" "public.users"
check "children attached during backfill" "$(os_len e2e_customers 1 orders)" "2"
# a second collection exercises the multi-join path of the initial load
check "second collection attached too" "$(os_len e2e_customers 1 tickets)" "1"
# a parent with no children must get an empty array, never null
check "childless parent gets an empty array" "$(os_len e2e_customers 3 orders)" "0"
check "childless parent has the field at all" "$(os_has e2e_customers 3 orders)" "True"
# single = true: the element itself, not an array of one
check "a one-to-one child is an object" \
  "$(curl -s "$OS/e2e_customers/_doc/1" | jqf "d['_source']['profile']['bio']")" \
  "first profile"
check "a parent with no one-to-one child gets null" \
  "$(curl -s "$OS/e2e_customers/_doc/3" | jqf "d['_source']['profile']")" "None"
check "and still has the field" "$(os_has e2e_customers 3 profile)" "True"

say "4. live streaming"
pg "INSERT INTO users (id,name,email,password_hash) VALUES (4,'dave','dave@test.io','secret-4');" > /dev/null
synced
check "INSERT propagated" "$(os_count e2e_users)" "4"
check "excluded column still absent" "$(os_has e2e_users 4 password_hash)" "False"

pg "UPDATE users SET name='dave-renamed', email='new@test.io' WHERE id=4;" > /dev/null
synced
check "UPDATE propagated" "$(os_field e2e_users 4 name)" "dave-renamed"
check "transform applied on update" "$(os_field e2e_users 4 contact)" "***"

# a wide, incompressible value is stored out of line, so an update that does
# not touch it sends a marker instead of the value and the engine has to
# complete the document from the one already indexed
pg "UPDATE users SET metadata = (SELECT jsonb_build_object('blob', string_agg(md5(random()::text), ''))
                                 FROM generate_series(1, 400)) WHERE id = 4;" > /dev/null
synced
toast_len=$(curl -s "$OS/e2e_users/_doc/4" | jqf "len(d['_source']['meta']['blob'])")
pg "UPDATE users SET name = 'dave-toast' WHERE id = 4;" > /dev/null
synced
check "an update that leaves a TOASTed column alone keeps its value" \
  "$(curl -s "$OS/e2e_users/_doc/4" | jqf "len(d['_source']['meta']['blob'])")" "$toast_len"
check "the rest of that update still applied" "$(os_field e2e_users 4 name)" "dave-toast"

pg "DELETE FROM users WHERE id=3;" > /dev/null
synced
check "DELETE propagated" "$(os_status e2e_users 3)" "404"

say "5. changing a primary key moves the document"
pg "UPDATE users SET id = 40 WHERE id = 4;" > /dev/null
synced
check "row lives at its new id" "$(os_field e2e_users 40 name)" "dave-toast"
# the old document must not survive: nothing would ever collect it
check "old document removed" "$(os_status e2e_users 4)" "404"
pg "DELETE FROM users WHERE id = 40;" > /dev/null
synced
check "deleting the moved row leaves nothing" "$(os_status e2e_users 40)" "404"

say "6. nested children stay fresh"
pg "INSERT INTO orders (id,customer_id,total) VALUES (13,2,7.50);" > /dev/null
synced
check "child INSERT refreshes parent" "$(os_len e2e_customers 2 orders)" "2"
pg "DELETE FROM orders WHERE id=13;" > /dev/null
synced
check "child DELETE refreshes parent" "$(os_len e2e_customers 2 orders)" "1"

# the re-fetch shares the load's element expression, so the projection holds on
# the streamed path too — a change to the excluded column may not leak it
pg "UPDATE orders SET internal_notes='still secret' WHERE id=12;" > /dev/null
synced
check "the excluded child column stays out on the stream" \
  "$(curl -s "$OS/e2e_customers/_doc/2" | jqf "'internal_notes' in d['_source']['orders'][0]")" \
  "False"

# a one-to-one child that goes away leaves the field present and null, so a
# query for it need not know whether this parent ever had one
pg "DELETE FROM profiles WHERE id=30;" > /dev/null
synced
check "a deleted one-to-one child becomes null" \
  "$(curl -s "$OS/e2e_customers/_doc/1" | jqf "d['_source']['profile']")" "None"
check "the field is still there" "$(os_has e2e_customers 1 profile)" "True"

# a second matching row is a warning, not a halt: the lowest-keyed row stands
dup_lines=$(wc -l < "$LOG")
pg "INSERT INTO profiles (id,customer_id,bio) VALUES (31,1,'kept'),(32,1,'extra');" > /dev/null
synced
check "the lowest-keyed row stands" \
  "$(curl -s "$OS/e2e_customers/_doc/1" | jqf "d['_source']['profile']['bio']")" "kept"
if tail -n +$((dup_lines + 1)) "$LOG" | grep -q "public.profiles"; then
  ok "the run warns that a one-to-one child matched twice"
else
  bad "a second matching row was embedded without a word"
fi
pg "DELETE FROM profiles WHERE id IN (31,32);" > /dev/null
synced

# Many children of one parent in one transaction: the parent is re-read once for
# the group, not once per row, and the array that lands is the whole collection.
before_reads=$(pg "SELECT COALESCE(sum(calls),0) FROM pg_stat_statements
                   WHERE query LIKE '%FROM \"public\".\"orders\"%'
                     AND query NOT LIKE '%pg_stat_statements%';" 2>/dev/null || echo 0)
pg "INSERT INTO orders (id,customer_id,total)
      SELECT 1000 + g, 2, g FROM generate_series(1, 40) g;" > /dev/null
synced
check "one transaction of 40 children lands whole" "$(os_len e2e_customers 2 orders)" "41"
after_reads=$(pg "SELECT COALESCE(sum(calls),0) FROM pg_stat_statements
                  WHERE query LIKE '%FROM \"public\".\"orders\"%'
                    AND query NOT LIKE '%pg_stat_statements%';" 2>/dev/null || echo 0)
# 40 rows resolved per row would be 40 fetches; per batch it is a small constant.
# Asserting "fewer than half" rather than an exact number keeps this about the
# cost model rather than about how the engine happened to split the batch.
if [ "$before_reads" = "0" ] && [ "$after_reads" = "0" ]; then
  echo "    (pg_stat_statements unavailable; query count not asserted)"
elif [ "$((after_reads - before_reads))" -lt 20 ]; then
  ok "children resolved per batch, not per row ($((after_reads - before_reads)) fetches for 40 rows)"
else
  bad "children still resolved per row ($((after_reads - before_reads)) fetches for 40 rows)"
fi
pg "DELETE FROM orders WHERE id > 1000;" > /dev/null
synced
check "the parent is back to its own children" "$(os_len e2e_customers 2 orders)" "1"

say "7. TRUNCATE clears the index"
pg "TRUNCATE users;" > /dev/null
synced
check "index cleared after TRUNCATE" "$(os_count e2e_users)" "0"
pg "INSERT INTO users (id,name,email) VALUES (7,'grace','grace@test.io');" > /dev/null
synced
check "streaming continues after TRUNCATE" "$(os_count e2e_users)" "1"

say "8. checkpoint and WAL safety"
checkpoint=$(curl -s "$OS/.pg2osync_meta/_doc/postgres-pg2osync_e2e" | jqf "d['_source']")
echo "    $checkpoint"
check "checkpoint source" "$(curl -s "$OS/.pg2osync_meta/_doc/postgres-pg2osync_e2e" | jqf "d['_source']['source']")" "postgres"
ckpt_lsn=$(curl -s "$OS/.pg2osync_meta/_doc/postgres-pg2osync_e2e" | jqf "d['_source']['position']")
# Acknowledging past the checkpoint would let PostgreSQL recycle WAL for rows
# that are not indexed yet, which is exactly what loses data on crash-restart.
behind=$(pg "SELECT pg_wal_lsn_diff('$ckpt_lsn'::pg_lsn, confirmed_flush_lsn) >= 0 FROM pg_replication_slots WHERE slot_name='pg2osync_e2e';")
check "slot never acked past the checkpoint" "$behind" "t"

say "9. metrics endpoint"
metrics=$(curl -s http://127.0.0.1:$((PORT_BASE + 11))/metrics)
if grep -q "pg2osync_position_confirmed" <<< "$metrics" && grep -q "pg2osync_events_total" <<< "$metrics"; then
  ok "metrics expose position and event counters"
else
  bad "metrics missing expected series"
fi
# Retained WAL is the number that takes the source down, and nothing else here
# reports it: position_lag stays kilobytes while a slot can pin gigabytes. The
# poller runs on its own interval, so this waits for the first sample.
for _ in $(seq 1 40); do
  metrics=$(curl -s http://127.0.0.1:$((PORT_BASE + 11))/metrics)
  grep -qE "^pg2osync_slot_retained_bytes\{source=\"[^\"]*\",slot=\"pg2osync_e2e\"\}" <<< "$metrics" && break
  sleep 1
done
if grep -qE "^pg2osync_slot_retained_bytes\{source=\"[^\"]*\",slot=\"pg2osync_e2e\"\}" <<< "$metrics"; then
  ok "the configured slot's retained WAL is reported"
else
  bad "no retained-WAL series for the configured slot"
fi
if grep -qE "^pg2osync_slot_wal_status\{source=\"[^\"]*\",slot=\"pg2osync_e2e\",status=\"lost\"\} 0" <<< "$metrics"; then
  ok "and the server's own verdict on it"
else
  bad "no wal_status series for the configured slot"
fi
# The check that has to work when the pipeline is *not* running, which is the
# case that fills a disk. A limit of 0 MB is over by definition.
if $BIN status -c "$CONFIG" --max-retained-mb 0 > /dev/null 2>&1; then
  bad "a slot over the retention limit exited zero"
else
  ok "status exits non-zero over the retention limit"
fi
if $BIN status -c "$CONFIG" --max-retained-mb 1048576 > /dev/null 2>&1; then
  ok "and zero under it"
else
  bad "status failed under the retention limit"
fi
check "health answers for probes" "$(curl -s http://127.0.0.1:$((PORT_BASE + 11))/healthz)" "ok"
check "an unknown path is not the exposition" \
  "$(curl -s -o /dev/null -w '%{http_code}' http://127.0.0.1:$((PORT_BASE + 11))/)" "404"

say "9a. a column added under the pipeline is counted as drift"
# PostgreSQL re-sends a RELATION message before the first row event that
# depends on the new shape. The change is never applied, so the counter is the
# only alertable report that the index and the table stopped agreeing.
pg "ALTER TABLE users ADD COLUMN drift_probe text;" > /dev/null
pg "INSERT INTO users (id,name,email,drift_probe) VALUES (91,'drift','drift@test.io','later');" > /dev/null
synced
metrics=$(curl -s http://127.0.0.1:$((PORT_BASE + 11))/metrics)
check "the shape change is counted" \
  "$(awk '/^pg2osync_schema_drift_total\{source="[^"]*",table="public.users"\} /{print $2}' <<< "$metrics")" "1"
check "and the row after it still lands" "$(os_field e2e_users 91 drift_probe)" "later"
pg "DELETE FROM users WHERE id=91;" > /dev/null
synced
pg "ALTER TABLE users DROP COLUMN drift_probe;" > /dev/null

say "9b. reconcile finds a document whose row is gone"
# the count here depends on what earlier steps left behind, so it is measured
# rather than assumed
before_reconcile=$(os_count e2e_users)
curl -s -XPUT "$OS/e2e_users/_doc/9999" -H 'Content-Type: application/json' \
  -d '{"id":9999,"name":"ghost"}' > /dev/null
refresh
out=$($BIN reconcile -c "$CONFIG" 2>&1)
case "$out" in
  *"1 found with no row"*) ok "reconcile named the orphan and nothing else" ;;
  *) bad "reconcile did not find exactly the orphan: $out" ;;
esac
$BIN reconcile -c "$CONFIG" --delete > /dev/null
refresh
check "reconcile --delete removed it" "$(os_status e2e_users 9999)" "404"
check "reconcile left the real documents alone" "$(os_count e2e_users)" "$before_reconcile"

say "10. reconnects after the server drops the stream"
before_pid=$SYNC_PID
pg "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE backend_type='walsender';" > /dev/null
pg "INSERT INTO users (id,name,email) VALUES (9,'written-while-disconnected','w@test.io');" > /dev/null
synced
# the same process must still be running: recovery happens in process, not by
# a supervisor restarting us
check "same process recovered" "$(sync_pid)" "$before_pid"
check "row written while disconnected arrived" "$(os_field e2e_users 9 name)" "written-while-disconnected"
metrics=$(curl -s http://127.0.0.1:$((PORT_BASE + 11))/metrics)
reconnects=$(awk '/^pg2osync_reconnects_total\{/ {print $2}' <<< "$metrics")
if [ "${reconnects:-0}" -ge 1 ]; then ok "reconnects_total counted it ($reconnects)"; else bad "reconnects_total still zero"; fi
check "source reports connected again" "$(awk '/^pg2osync_source_connected\{/ {print $2}' <<< "$metrics")" "1"

say "11. read-your-writes"
pg "INSERT INTO users (id,name,email) VALUES (11,'ryw','r@test.io');" > /dev/null
# no position, no sleep, no retry: the endpoint returns only once the write is
# searchable, so a single query afterwards must find it
synced=$(curl -s "http://127.0.0.1:$((PORT_BASE + 31))/synced?refresh=true&timeout=8000")
found=$(curl -s "$OS/e2e_users/_search" -H 'Content-Type: application/json' \
  -d '{"query":{"term":{"id":11}}}' | jqf "d['hits']['total']['value']")
check "the row is searchable the moment /synced returns" "$found" "1"
check "and it says so" "$(jqf "d['synced']" <<< "$synced")" "True"
waited=$(jqf "d['waited_ms']" <<< "$synced")
ok "waited ${waited}ms"
# a position nothing will ever reach must time out rather than hang
code=$(curl -s -o /dev/null -w "%{http_code}" "http://127.0.0.1:$((PORT_BASE + 31))/synced?position=FFFF%2FFFFFFFFF&timeout=300")
check "an unreachable position times out" "$code" "408"

say "12. crash recovery"
sync_kill; sleep 1
pg "INSERT INTO users (id,name,email) VALUES (8,'eve-during-downtime','eve@test.io');" > /dev/null
start_sync
sleep 6; refresh
check "row written while down is recovered" "$(os_field e2e_users 8 name)" "eve-during-downtime"

say "13. final consistency"
check "row counts match" "$(pg "SELECT count(*) FROM users;")" "$(os_count e2e_users)"

say "14. SIGTERM drains, then status and teardown"
# docker stop and Kubernetes send SIGTERM, so it has to end the way Ctrl-C
# does: with the last acknowledged position checkpointed, not with the default
# handler's immediate exit and a replay on the next start
pg "INSERT INTO users (id,name,email) VALUES (12,'term','t@test.io');" > /dev/null
confirmed=$(curl -s "http://127.0.0.1:$((PORT_BASE + 31))/synced?refresh=true&timeout=8000" | jqf "d['confirmed']")
log_lines=$(wc -l < "$LOG")
kill -TERM "$SYNC_PID"
wait "$SYNC_PID" && code=0 || code=$?
check "SIGTERM exits cleanly" "$code" "0"
check "and is logged as the reason" \
  "$(tail -n +$((log_lines + 1)) "$LOG" | grep -c 'shutdown signal received (SIGTERM)')" "1"
# at or past rather than equal: a keepalive landing between /synced and the
# signal moves the acknowledged position, and the checkpoint follows it
ckpt_lsn=$(curl -s "$OS/.pg2osync_meta/_doc/postgres-pg2osync_e2e" | jqf "d['_source']['position']")
check "the final checkpoint holds the last acknowledged position" \
  "$(pg "SELECT pg_wal_lsn_diff('$ckpt_lsn'::pg_lsn, '$confirmed'::pg_lsn) >= 0;")" "t"
$BIN status -c "$CONFIG" | sed 's/^/    /'
$BIN drop-slot -c "$CONFIG" > /dev/null
check "slot dropped" "$(pg "SELECT count(*) FROM pg_replication_slots WHERE slot_name='pg2osync_e2e';")" "0"
# a re-index runs two pipelines on one publication, so dropping it with the old
# slot would take it out from under the new one
check "publication left alone by default" "$(pg "SELECT count(*) FROM pg_publication WHERE pubname='pg2osync_e2e_pub';")" "1"
$BIN drop-slot -c "$CONFIG" --publication > /dev/null
check "publication dropped when asked" "$(pg "SELECT count(*) FROM pg_publication WHERE pubname='pg2osync_e2e_pub';")" "0"

say "15. an interrupted initial load resumes where it stopped"
# Its own config, slot and table: the table has to be large enough to be read
# in several ranges, and every other section would then pay for it on restart.
RCONFIG=$(mktemp /tmp/pg2osync-e2e-resume.XXXXXX)
RSLOT=pg2osync_e2e_resume
cat > "$RCONFIG" <<TOML
[source]
url_env = "PG2OSYNC_SOURCE_URL"
slot_name = "$RSLOT"
publication = "${RSLOT}_pub"

[target]
url = "$OS"
flavor = "$TARGET_FLAVOR"

[metrics]
bind = "127.0.0.1:$((PORT_BASE + 12))"

[sync.big]
table = "public.resume_probe"
index = "e2e_resume"
TOML
resume_cleanup() {
  sync_kill
  pg "SELECT pg_drop_replication_slot('$RSLOT') WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name='$RSLOT');" > /dev/null 2>&1 || true
  pg "DROP PUBLICATION IF EXISTS ${RSLOT}_pub; DROP TABLE IF EXISTS resume_probe;" > /dev/null 2>&1 || true
  rm -f "$RCONFIG"
}
trap 'cleanup; resume_cleanup' EXIT

pg "DROP TABLE IF EXISTS resume_probe; CREATE TABLE resume_probe(id bigint primary key, v text);" > /dev/null 2>&1
pg "INSERT INTO resume_probe SELECT g, repeat('x',200)||g FROM generate_series(1,200000) g;" > /dev/null
# reltuples is what decides how many ranges to read in, and it is only set by ANALYZE
pg "ANALYZE resume_probe;" > /dev/null
pg "DROP PUBLICATION IF EXISTS ${RSLOT}_pub; CREATE PUBLICATION ${RSLOT}_pub FOR TABLE resume_probe;" > /dev/null 2>&1
curl -s -XDELETE "$OS/e2e_resume" > /dev/null
PROG=load-postgres-$RSLOT-public_resume_probe
curl -s -XDELETE "$OS/.pg2osync_meta/_doc/$PROG" > /dev/null

progress_done() { curl -s "$OS/.pg2osync_meta/_doc/$PROG" | jqf "(d.get('_source') or {}).get('done', -1)"; }
sync_spawn "$RCONFIG"
for _ in $(seq 1 120); do
  [ "$(progress_done)" -ge 2 ] 2> /dev/null && break
  sleep 0.5
done
done_at_kill=$(progress_done)
sync_kill; sleep 1
if [ "$done_at_kill" -ge 2 ]; then ok "progress recorded per range ($done_at_kill done)"; else bad "no per-range progress recorded (got '$done_at_kill')"; fi

# the interesting part: the source moves while nothing is watching it, so the
# replay argument the chunked load rests on is what has to repair the result
pg "DELETE FROM resume_probe WHERE id IN (5, 100000);" > /dev/null
pg "INSERT INTO resume_probe VALUES (400001,'added-while-down');" > /dev/null
pg "UPDATE resume_probe SET v='updated-while-down' WHERE id = 77;" > /dev/null
src_rows=$(pg "SELECT count(*) FROM resume_probe;")

sync_spawn "$RCONFIG"
# The copy now runs beside the stream, so a row can be changed while the range
# holding it is still being read. The streamed change carries a higher position
# than the copy did, so it has to win — the version is the only thing stopping
# the stale copied row from landing on top of it.
sleep 1
pg "UPDATE resume_probe SET v='changed-during-the-copy' WHERE id = 199000;" > /dev/null
# And a row deleted while the copy is still running. The version cannot protect
# this one on its own: a delete leaves a tombstone that lives for gc_deletes, and
# a copy row starved past that would be accepted back. The engine drops such a
# row instead of offering it. This case only pins the ordering — the starvation
# that breaks it cannot be staged from a shell script.
pg "DELETE FROM resume_probe WHERE id = 198000;" > /dev/null
# one row fewer to wait for, now that the load itself is racing a delete
src_rows=$((src_rows - 1))
for _ in $(seq 1 180); do
  refresh
  [ "$(os_count e2e_resume)" = "$src_rows" ] && break
  sleep 1
done
check "every row is indexed after the restart" "$(os_count e2e_resume)" "$src_rows"
if grep -q "resuming the load of public.resume_probe" "$LOG"; then
  ok "the load resumed instead of starting over"
else
  bad "the load restarted from the beginning"
fi
check "a row deleted while down is gone" "$(os_status e2e_resume 100000)" "404"
check "a row added while down arrived" "$(os_field e2e_resume 400001 v)" "added-while-down"
check "a row updated while down is current" "$(os_field e2e_resume 77 v)" "updated-while-down"
# no /synced endpoint on this config, so give the stream a moment to catch up
sleep 3; refresh
check "a row changed during the copy is not overwritten by it" \
  "$(os_field e2e_resume 199000 v)" "changed-during-the-copy"
check "a row deleted during the copy is not resurrected by it" \
  "$(os_status e2e_resume 198000)" "404"
stop_sync

say "14. a document the target refuses"
# The rejection path is entirely source-independent — it lives in the engine and
# the sink — so it is exercised once here rather than duplicated for MySQL.
stop_sync
QSLOT=pg2osync_e2e_reject
QCONFIG=$(mktemp /tmp/pg2osync-reject.XXXXXX)
HCONFIG=$(mktemp /tmp/pg2osync-halt.XXXXXX)
q_body() {
  cat <<TOML
[source]
url_env = "PG2OSYNC_SOURCE_URL"
slot_name = "$QSLOT"
publication = "${QSLOT}_pub"

[target]
url = "$OS"
flavor = "$TARGET_FLAVOR"

[metrics]
bind = "127.0.0.1:$((PORT_BASE + 15))"

$1

[sync.t]
table = "public.reject_probe"
index = "e2e_reject"
TOML
}
q_body '[engine]
on_permanent_rejection = "quarantine"
max_rejects = 100' > "$QCONFIG"
q_body '' > "$HCONFIG"
reject_cleanup() {
  sync_kill
  pg "SELECT pg_drop_replication_slot('$QSLOT') WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name='$QSLOT');" > /dev/null 2>&1 || true
  pg "DROP PUBLICATION IF EXISTS ${QSLOT}_pub; DROP TABLE IF EXISTS reject_probe;" > /dev/null 2>&1 || true
  rm -f "$QCONFIG" "$HCONFIG"
}
trap 'cleanup; resume_cleanup; reject_cleanup' EXIT

pg "DROP TABLE IF EXISTS reject_probe; CREATE TABLE reject_probe(id bigint primary key, amount text);" > /dev/null 2>&1
pg "DROP PUBLICATION IF EXISTS ${QSLOT}_pub; CREATE PUBLICATION ${QSLOT}_pub FOR TABLE reject_probe;" > /dev/null 2>&1
curl -s -XDELETE "$OS/e2e_reject,.pg2osync_rejects?ignore_unavailable=true" > /dev/null
curl -s -XDELETE "$OS/.pg2osync_meta/_doc/postgres-$QSLOT" > /dev/null
# amount is declared a long, so a row holding text is refused permanently
curl -s -XPUT "$OS/e2e_reject" -H 'Content-Type: application/json' \
  -d '{"mappings":{"properties":{"amount":{"type":"long"}}}}' > /dev/null
pg "INSERT INTO reject_probe VALUES (1, '100');" > /dev/null

# halt is the default: no progress past the bad row, and nothing recorded. The
# attempt dies and is retried, so the process stays up and the document is tried
# again — which is what lets a mapping fix unblock it without a restart.
sync_spawn "$HCONFIG"
sleep 5
halts_before=$(grep -c "halting pipeline: permanent rejection" "$LOG" || true)
pg "INSERT INTO reject_probe VALUES (2, 'not-a-number');" > /dev/null
for _ in $(seq 1 20); do
  [ "$(grep -c 'halting pipeline: permanent rejection' "$LOG" || true)" -gt "$halts_before" ] && break
  sleep 1
done
if [ "$(grep -c 'halting pipeline: permanent rejection' "$LOG" || true)" -gt "$halts_before" ]; then
  ok "halt is the default: the refused document stops the pipeline, naming itself"
else
  bad "a refused document did not halt the pipeline with on_permanent_rejection unset"
fi
refresh
check "the refused row is not indexed under halt" "$(os_status e2e_reject 2)" "404"
check "nothing was quarantined under halt" \
  "$(curl -s "$OS/.pg2osync_rejects/_count" | jqf "d.get('count', 0)")" "0"
stop_sync

# now quarantine: the same row is recorded and the pipeline carries on
sync_spawn "$QCONFIG"
sleep 5
pg "INSERT INTO reject_probe VALUES (3, '300');" > /dev/null
for _ in $(seq 1 30); do
  refresh
  [ "$(os_status e2e_reject 3)" = "200" ] && break
  sleep 1
done
check "a later row still arrives" "$(os_status e2e_reject 3)" "200"
check "the refused row is not in the index" "$(os_status e2e_reject 2)" "404"
curl -s -XPOST "$OS/.pg2osync_rejects/_refresh" > /dev/null
check "it is in the quarantine store instead" \
  "$(curl -s "$OS/.pg2osync_rejects/_count" | jqf "d.get('count', 0)")" "1"
# at least one, not exactly one: the store is keyed by document id, while the
# counter counts arrivals — and a restart replays the row, so the same refusal
# is legitimately filed again under at-least-once
if curl -s http://127.0.0.1:$((PORT_BASE + 15))/metrics | grep -qE "^pg2osync_rejected_total\{source=\"[^\"]*\"\} [1-9]"; then
  ok "pg2osync_rejected_total reports it"
else
  bad "pg2osync_rejected_total did not move"
fi
# Elasticsearch 8 names the same refusal document_parsing_exception, and the
# listing is right either way: the assertion is about what rejects prints, not
# about which word the target chose
if $BIN rejects -c "$QCONFIG" 2>&1 \
  | grep -qE "e2e_reject/2 at .*(mapper_parsing_exception|document_parsing_exception)"; then
  ok "rejects names the document, its position and why"
else
  bad "rejects did not list the refused document"
  $BIN rejects -c "$QCONFIG" 2>&1 | tail -3 | sed 's/^/    /'
fi
stop_sync

# fix the mapping, then replay what was held back
curl -s -XDELETE "$OS/e2e_reject" > /dev/null
curl -s -XPUT "$OS/e2e_reject" -H 'Content-Type: application/json' \
  -d '{"mappings":{"properties":{"amount":{"type":"text"}}}}' > /dev/null
if $BIN rejects -c "$QCONFIG" --replay 2>&1 | grep -q "replayed 1, still refused 0"; then
  ok "replay indexed the document the target now accepts"
else
  bad "replay did not accept the document"
fi
refresh
check "the once-refused row is indexed" "$(os_field e2e_reject 2 amount)" "not-a-number"
curl -s -XPOST "$OS/.pg2osync_rejects/_refresh" > /dev/null
check "and the quarantine store is empty" \
  "$(curl -s "$OS/.pg2osync_rejects/_count" | jqf "d.get('count', 0)")" "0"

say "15. re-snapshot one table on demand"
# resume_probe is still here from section 12, streaming under $RCONFIG, with a
# composite-key table beside it — so this exercises one table out of two.
sync_spawn "$RCONFIG"
sleep 4
# A document that went missing: no version at all, so a re-snapshot restores it.
curl -s -XDELETE "$OS/e2e_resume/_doc/500" > /dev/null
# And one whose value is wrong. Corrupting it by hand uses internal versioning,
# which bumps the version one past the source's current position — so the source
# has to move on before a re-snapshot can replace it, exactly as it always has by
# the time a fix is deployed.
curl -s -XPUT "$OS/e2e_resume/_doc/600" -H 'Content-Type: application/json' \
  -d '{"id":600,"v":"CORRUPT"}' > /dev/null
pg "INSERT INTO resume_probe VALUES (500001,'moves-the-wal-on');" > /dev/null
sleep 2
refresh
check "a document was removed from the index" "$(os_status e2e_resume 500)" "404"
check "and another holds a wrong value" "$(os_field e2e_resume 600 v)" "CORRUPT"

$BIN resnapshot -c "$RCONFIG" --table public.resume_probe >> "$LOG" 2>&1
refresh
check "the missing document is back" "$(os_status e2e_resume 500)" "200"
if [ "$(os_field e2e_resume 600 v)" != "CORRUPT" ]; then
  ok "the wrong value was replaced by the source's"
else
  bad "the wrong value survived the re-snapshot"
fi
# streaming is unaffected by a re-snapshot having run beside it
pg "INSERT INTO resume_probe VALUES (500002,'after-the-resnapshot');" > /dev/null
for _ in $(seq 1 20); do
  refresh
  [ "$(os_status e2e_resume 500002)" = "200" ] && break
  sleep 1
done
check "streaming continues afterwards" "$(os_field e2e_resume 500002 v)" "after-the-resnapshot"

# --where narrows what is re-read
curl -s -XDELETE "$OS/e2e_resume/_doc/700" > /dev/null
curl -s -XDELETE "$OS/e2e_resume/_doc/701" > /dev/null
# the hand deletion leaves a tombstone one version past the source, so the source
# moves on before the re-snapshot can replace it
pg "INSERT INTO resume_probe VALUES (500003,'moves-the-wal-on-again');" > /dev/null
sleep 2
refresh
$BIN resnapshot -c "$RCONFIG" --table public.resume_probe --where "id = 700" >> "$LOG" 2>&1
refresh
check "--where restored the row it names" "$(os_status e2e_resume 700)" "200"
check "--where left the others alone" "$(os_status e2e_resume 701)" "404"

# a re-snapshot must not leave bookkeeping a later start would read as an
# unfinished initial load
progress_left=$(curl -s "$OS/.pg2osync_meta/_search?q=_id:load*&size=20" | jqf "len(d['hits']['hits'])")
check "no load progress left behind" "$progress_left" "0"
# captured rather than piped: the command exits non-zero on purpose, and under
# `set -o pipefail` that fails the `if` however the grep went
refusal=$($BIN resnapshot -c "$RCONFIG" --table public.not_configured 2>&1 || true)
case "$refusal" in
  *"not in this config"*) ok "a table with no index mapping is refused, naming the configured ones" ;;
  *) bad "an unconfigured table was accepted: $refusal" ;;
esac

# The claim is that a re-snapshot does not move the checkpoint, and it can only
# be attributed with the pipeline stopped: while it streams the position advances
# on its own from whatever else the database is doing.
stop_sync
sleep 1
before=$($BIN status -c "$RCONFIG" | grep -o 'position=[^ ]*' | head -1)
$BIN resnapshot -c "$RCONFIG" --table public.resume_probe >> "$LOG" 2>&1
check "a re-snapshot does not move the checkpoint" \
  "$($BIN status -c "$RCONFIG" | grep -o 'position=[^ ]*' | head -1)" "$before"

echo -e "\n\033[1m== 16. concurrent write requests ==\033[0m"
# The write window is what the initial load is actually limited by, so the load
# has to stay correct with several requests open at once — including the two
# things concurrency could plausibly break: a streamed change landing while the
# copy is still running, and a delete the copy must not undo.
WCONFIG=$(mktemp /tmp/pg2osync-e2e-conc.XXXXXX)
WSLOT=pg2osync_e2e_conc
sed -e "s/^slot_name = .*/slot_name = \"$WSLOT\"/" \
    -e "s/^publication = .*/publication = \"${WSLOT}_pub\"/" \
    -e "s/^index = .*/index = \"e2e_conc\"/" \
    -e "s#^bind = .*#bind = \"127.0.0.1:$((PORT_BASE + 18))\"#" \
    -e "s/^\[target\]/[engine]\nwrite_concurrency = 4\n\n[target]/" \
    "$RCONFIG" > "$WCONFIG"
conc_cleanup() {
  sync_kill
  pg "SELECT pg_drop_replication_slot('$WSLOT') WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name='$WSLOT');" > /dev/null 2>&1 || true
  pg "DROP PUBLICATION IF EXISTS ${WSLOT}_pub;" > /dev/null 2>&1 || true
  rm -f "$WCONFIG"
}
trap 'cleanup; resume_cleanup; reject_cleanup; conc_cleanup' EXIT
pg "DROP PUBLICATION IF EXISTS ${WSLOT}_pub; CREATE PUBLICATION ${WSLOT}_pub FOR TABLE resume_probe;" > /dev/null 2>&1
curl -s -XDELETE "$OS/e2e_conc" > /dev/null
src_rows=$(pg "SELECT count(*) FROM resume_probe;")
sync_spawn "$WCONFIG"
sleep 1
pg "UPDATE resume_probe SET v='changed-under-concurrency' WHERE id = 150000;" > /dev/null
pg "DELETE FROM resume_probe WHERE id = 150001;" > /dev/null
src_rows=$((src_rows - 1))
for _ in $(seq 1 180); do
  refresh
  [ "$(os_count e2e_conc)" = "$src_rows" ] && break
  sleep 1
done
check "every row is indexed with four requests open" "$(os_count e2e_conc)" "$src_rows"
sleep 3; refresh
check "a streamed change still outranks the copy" \
  "$(os_field e2e_conc 150000 v)" "changed-under-concurrency"
check "a deleted row is not resurrected by a concurrent write" \
  "$(os_status e2e_conc 150001)" "404"
# The checkpoint may only pass what is durable, and with several requests open
# that is the property most at risk. A restart that loses nothing proves it.
stop_sync
sleep 1
pg "INSERT INTO resume_probe VALUES (900001,'after-the-restart');" > /dev/null
sync_spawn "$WCONFIG"
for _ in $(seq 1 60); do
  refresh
  [ "$(os_status e2e_conc 900001)" = "200" ] && break
  sleep 1
done
check "streaming resumes from a position written under concurrency" \
  "$(os_field e2e_conc 900001 v)" "after-the-restart"
stop_sync

echo -e "\n\033[1m== 17. a renamed table does not take the pipeline down ==\033[0m"
# A publication follows a table through a rename, so its rows keep arriving
# under a name nothing maps. That used to reach an assertion in the engine and
# panic a worker thread, which the replay then repeated on every reconnect.
RNCONFIG=$(mktemp /tmp/pg2osync-e2e-rename.XXXXXX)
RNSLOT=pg2osync_e2e_rename
sed -e "s/^slot_name = .*/slot_name = \"$RNSLOT\"/" \
    -e "s/^publication = .*/publication = \"${RNSLOT}_pub\"/" \
    -e "s/^index = .*/index = \"e2e_rename\"/" \
    -e "s#^bind = .*#bind = \"127.0.0.1:$((PORT_BASE + 19))\"#" \
    "$RCONFIG" > "$RNCONFIG"
rename_cleanup() {
  sync_kill
  pg "SELECT pg_drop_replication_slot('$RNSLOT') WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name='$RNSLOT');" > /dev/null 2>&1 || true
  pg "DROP PUBLICATION IF EXISTS ${RNSLOT}_pub;" > /dev/null 2>&1 || true
  pg "ALTER TABLE IF EXISTS renamed_probe RENAME TO rename_probe;" > /dev/null 2>&1 || true
  pg "DROP TABLE IF EXISTS rename_probe;" > /dev/null 2>&1 || true
  rm -f "$RNCONFIG"
}
trap 'cleanup; resume_cleanup; reject_cleanup; conc_cleanup; rename_cleanup' EXIT
pg "DROP TABLE IF EXISTS rename_probe, renamed_probe;" > /dev/null 2>&1
pg "CREATE TABLE rename_probe(id bigint primary key, v text);" > /dev/null
pg "INSERT INTO rename_probe VALUES (1,'before');" > /dev/null
pg "DROP PUBLICATION IF EXISTS ${RNSLOT}_pub; CREATE PUBLICATION ${RNSLOT}_pub FOR TABLE rename_probe;" > /dev/null 2>&1
# point the config at this table rather than the resume probe's
python3 - "$RNCONFIG" <<'PYEOF'
import re, sys
path = sys.argv[1]
text = open(path).read()
text = re.sub(r"table = .*", 'table = "public.rename_probe"', text)
open(path, "w").write(text)
PYEOF
curl -s -XDELETE "$OS/e2e_rename?ignore_unavailable=true" > /dev/null
sync_spawn "$RNCONFIG"
for _ in $(seq 1 30); do
  refresh
  [ "$(os_count e2e_rename)" = "1" ] && break
  sleep 1
done
check "the table is synced before the rename" "$(os_count e2e_rename)" "1"
pg "ALTER TABLE rename_probe RENAME TO renamed_probe;" > /dev/null
pg "INSERT INTO renamed_probe VALUES (2,'after-rename');" > /dev/null
sleep 4
# The process has to still be there. A panic in a worker thread takes it down
# and the replay repeats it, so "is it alive" is the whole assertion.
if sync_pid > /dev/null; then
  ok "the pipeline survived the rename"
else
  bad "the pipeline died on the rename"
fi
if grep -q "unmapped table reached the engine" "$LOG"; then
  bad "it panicked on a table it could not map"
else
  ok "no panic on a table nothing maps"
fi
if grep -q "renamed_probe is in publication" "$LOG"; then
  ok "and it named the table whose rows it is dropping"
else
  bad "it dropped the rows without saying which table"
fi
# A row under the old name is still there: nothing was undone, it just stopped
# being updated — the same rule a dropped column already follows.
check "what was indexed before the rename is untouched" \
  "$(os_field e2e_rename 1 v)" "before"
# And the stream is still working, which a crash loop would not be
pg "ALTER TABLE renamed_probe RENAME TO rename_probe;" > /dev/null
pg "INSERT INTO rename_probe VALUES (3,'after-rename-back');" > /dev/null
for _ in $(seq 1 30); do
  refresh
  [ "$(os_status e2e_rename 3)" = "200" ] && break
  sleep 1
done
check "streaming resumes once the name matches again" \
  "$(os_field e2e_rename 3 v)" "after-rename-back"
stop_sync

echo -e "\n\033[1m== 18. the load read by several connections at once ==\033[0m"
# Waves, not a free-running pool: progress counts *leading* ranges written, and
# the engine forgets its record of stream-removed keys on every load mark. Both
# hold only if a wave is contiguous and finished before its mark.
PWCONFIG=$(mktemp /tmp/pg2osync-e2e-workers.XXXXXX)
PWSLOT=pg2osync_e2e_workers
# Its own log: the shared one already holds two dozen "resuming the load" lines
# from earlier sections, so grepping it would prove nothing about this run.
LOG18=/tmp/pg2osync-e2e-workers-$TAG.log
: > "$LOG18"
sed -e "s/^slot_name = .*/slot_name = \"$PWSLOT\"/" \
    -e "s/^publication = .*/publication = \"${PWSLOT}_pub\"/" \
    -e "s/^index = .*/index = \"e2e_workers\"/" \
    -e "s#^bind = .*#bind = \"127.0.0.1:$((PORT_BASE + 20))\"#" \
    -e "s/^\[source\]/[source]\nload_workers = 4\nload_chunk_rows = 2000/" \
    "$RCONFIG" > "$PWCONFIG"
workers_cleanup() {
  sync_kill
  pg "SELECT pg_drop_replication_slot('$PWSLOT') WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name='$PWSLOT');" > /dev/null 2>&1 || true
  pg "DROP PUBLICATION IF EXISTS ${PWSLOT}_pub;" > /dev/null 2>&1 || true
  rm -f "$PWCONFIG" "$LOG18"
}
trap 'cleanup; resume_cleanup; reject_cleanup; conc_cleanup; rename_cleanup; workers_cleanup' EXIT
pg "DROP PUBLICATION IF EXISTS ${PWSLOT}_pub; CREATE PUBLICATION ${PWSLOT}_pub FOR TABLE resume_probe;" > /dev/null 2>&1
curl -s -XDELETE "$OS/e2e_workers?ignore_unavailable=true" > /dev/null
PWPROG=load-postgres-$PWSLOT-public_resume_probe
curl -s -XDELETE "$OS/.pg2osync_meta/_doc/$PWPROG" > /dev/null
src_rows=$(pg "SELECT count(*) FROM resume_probe;")
pw_done() { curl -s "$OS/.pg2osync_meta/_doc/$PWPROG" | jqf "(d.get('_source') or {}).get('done', -1)"; }
sync_spawn "$PWCONFIG" "$LOG18"
# Caught while it runs: the document is removed once the load finishes, so a
# slow poll sees nothing rather than something wrong.
done_at_kill=-1
for _ in $(seq 1 400); do
  seen=$(pw_done)
  if [ "$seen" -gt 0 ] 2> /dev/null; then
    done_at_kill=$seen
    break
  fi
  sleep 0.05
done
sync_kill; sleep 1
if [ "$done_at_kill" -gt 0 ]; then
  if [ $((done_at_kill % 4)) = 0 ]; then
    ok "progress advances a wave at a time ($done_at_kill ranges, a multiple of 4)"
  else
    bad "progress recorded $done_at_kill ranges, which no wave of 4 could produce"
  fi
else
  echo "  - not asserted: the load finished before any progress could be read"
fi
if grep -q "reading the load with 4 connections" "$LOG18"; then
  ok "it really used four connections"
else
  bad "it did not report reading with four connections"
fi
sync_spawn "$PWCONFIG" "$LOG18"
for _ in $(seq 1 180); do
  refresh
  [ "$(os_count e2e_workers)" = "$src_rows" ] && break
  sleep 1
done
check "every row is indexed after a parallel load" "$(os_count e2e_workers)" "$src_rows"
if [ "$done_at_kill" -gt 0 ]; then
  if grep -q "resuming the load of public.resume_probe" "$LOG18"; then
    ok "and it resumed from the wave it had finished"
  else
    bad "the parallel load restarted from the beginning"
  fi
fi
stop_sync

echo -e "\n\033[1m== 19. init writes a config that runs ==\033[0m"
# The first thing a new user needs, and the one thing no subcommand did: every
# other one takes a `-c FILE` that has to exist first. Measured before this
# existed, the first hand-written config failed on an unqualified table name.
#
# Nothing is edited after `init` here, deliberately: the out-of-the-box path is
# what is being tested, down to the default config name and target url.
ABIN="$(pwd)/$BIN"
INITDIR=$(mktemp -d /tmp/pg2osync-e2e-init.XXXXXX)
pg "DROP TABLE IF EXISTS init_probe, init_no_pk;" > /dev/null 2>&1
pg "CREATE TABLE init_probe(id bigint primary key, v text);" > /dev/null
pg "INSERT INTO init_probe VALUES (1,'from-init');" > /dev/null
pg "CREATE TABLE init_no_pk(v text);" > /dev/null
init_cleanup() {
  rm -rf "$INITDIR"
  pg "DROP TABLE IF EXISTS init_probe, init_no_pk;" > /dev/null 2>&1 || true
}
trap 'cleanup; resume_cleanup; reject_cleanup; conc_cleanup; rename_cleanup; workers_cleanup; init_cleanup' EXIT

# unqualified on purpose: the point is that it comes out qualified
if (cd "$INITDIR" && "$ABIN" init --table init_probe > /dev/null 2>&1); then
  ok "init wrote a config without being told the schema"
else
  bad "init failed against a live source"
fi
check "and qualified the table from the catalogue" \
  "$(grep -c 'table = "public.init_probe"' "$INITDIR/pg2osync.toml" 2> /dev/null || echo 0)" "1"
# No primary key used to be refused here. It now comes out declared
# append_only (#70), so an event log's smallest config runs unedited. Written
# to its own file so the init_probe config stays untouched for the beats
# below.
if (cd "$INITDIR" && "$ABIN" init -c no_pk.toml --table init_no_pk > /dev/null 2>&1); then
  ok "init wrote a config for a table with no primary key"
else
  bad "init refused a table with no primary key"
fi
check "and declared it append_only" \
  "$(grep -c '^append_only = true' "$INITDIR/no_pk.toml" 2> /dev/null || echo 0)" "1"
if (cd "$INITDIR" && "$ABIN" init --table init_probe > /dev/null 2>&1); then
  bad "init overwrote an existing config without --force"
else
  ok "init refuses to overwrite without --force"
fi
# The whole point: what it writes validates, unedited and with no -c flag.
if (cd "$INITDIR" && "$ABIN" validate > /dev/null 2>&1); then
  ok "validate passes on the generated config, unedited"
else
  bad "the generated config does not validate"
fi
init_cleanup
trap 'cleanup; resume_cleanup; reject_cleanup; conc_cleanup; rename_cleanup; workers_cleanup' EXIT

echo -e "\n\033[1m== 20. a derived id and a row that fans out into many documents ==\033[0m"
# The identity question end to end (#62): a document id shaped by config, and
# one row owning a document per element of a jsonb array — added, moved and
# removed by the ordinary stream.
FCONFIG=$(mktemp /tmp/pg2osync-e2e-fan.XXXXXX)
FSLOT=pg2osync_e2e_fan
cat > "$FCONFIG" <<TOML
[source]
url_env = "PG2OSYNC_SOURCE_URL"
slot_name = "$FSLOT"
publication = "${FSLOT}_pub"

[target]
url = "$OS"
flavor = "$TARGET_FLAVOR"

[metrics]
bind = "127.0.0.1:$((PORT_BASE + 21))"

[sync.fan]
table = "public.fan_probe"
index = "e2e_fan"
id = "fan-{id}"

[sync.fan.fan_out]
field = "tags"
id = "fan-{id}-{tags}"

[sync.fan.constants]
kind = "tag"
TOML
fan_cleanup() {
  sync_kill
  pg "SELECT pg_drop_replication_slot('$FSLOT') WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name='$FSLOT');" > /dev/null 2>&1 || true
  pg "DROP PUBLICATION IF EXISTS ${FSLOT}_pub; DROP TABLE IF EXISTS fan_probe;" > /dev/null 2>&1 || true
  rm -f "$FCONFIG"
}
trap 'cleanup; resume_cleanup; reject_cleanup; conc_cleanup; rename_cleanup; workers_cleanup; init_cleanup; fan_cleanup' EXIT

pg "DROP TABLE IF EXISTS fan_probe; CREATE TABLE fan_probe(id bigint primary key, tags jsonb);" > /dev/null 2>&1
pg "ALTER TABLE fan_probe REPLICA IDENTITY FULL;" > /dev/null 2>&1
pg "INSERT INTO fan_probe VALUES (1,'[\"a\",\"b\"]'::jsonb), (2,'[]'::jsonb), (3,NULL);" > /dev/null
pg "DROP PUBLICATION IF EXISTS ${FSLOT}_pub; CREATE PUBLICATION ${FSLOT}_pub FOR TABLE fan_probe;" > /dev/null 2>&1
curl -s -XDELETE "$OS/e2e_fan" > /dev/null
# an id naming a column the table does not have must be refused where it can
# still be fixed, not on the first row of the load
sed 's/^id = "fan-{id}"/id = "fan-{nope}"/' "$FCONFIG" > "${FCONFIG}.bad"
if $BIN validate -c "${FCONFIG}.bad" > /dev/null 2>&1; then
  bad "validate accepted an id that fan_probe has no column for"
else
  ok "validate refuses an id naming a column the table does not have"
fi
rm -f "${FCONFIG}.bad"
sync_spawn "$FCONFIG"
for _ in $(seq 1 60); do
  refresh
  [ "$(os_count e2e_fan)" = "3" ] && break
  sleep 1
done
check "the load fans each array out into its own document" "$(os_count e2e_fan)" "3"
check "element documents carry the configured id" "$(os_status e2e_fan fan-1-a)" "200"
check "every fanned element carries the constants" "$(os_field e2e_fan fan-1-a kind)" "tag"
check "a NULL array keeps the row itself, under the base id" "$(os_status e2e_fan fan-3)" "200"
check "an empty array emits nothing" "$(os_status e2e_fan fan-2)" "404"

pg "UPDATE fan_probe SET tags='[\"a\",\"c\"]'::jsonb WHERE id = 1;" > /dev/null
for _ in $(seq 1 30); do
  refresh
  [ "$(os_status e2e_fan fan-1-c)" = "200" ] && break
  sleep 1
done
check "a new element arrives" "$(os_status e2e_fan fan-1-c)" "200"
check "the kept element is still there" "$(os_status e2e_fan fan-1-a)" "200"
synced 2> /dev/null || { sleep 2; refresh; }
check "the dropped element's document is gone" "$(os_status e2e_fan fan-1-b)" "404"

pg "DELETE FROM fan_probe WHERE id = 1;" > /dev/null
for _ in $(seq 1 30); do
  refresh
  [ "$(os_count e2e_fan)" = "1" ] && break
  sleep 1
done
check "a row delete removes every element document it owned" "$(os_count e2e_fan)" "1"
stop_sync

echo -e "\n\033[1m== 21. a derived id that needs the before-image ==\033[0m"
BCONFIG=$(mktemp /tmp/pg2osync-e2e-bid.XXXXXX)
BSLOT=pg2osync_e2e_bid
cat > "$BCONFIG" <<TOML
[source]
url_env = "PG2OSYNC_SOURCE_URL"
slot_name = "$BSLOT"
publication = "${BSLOT}_pub"

[target]
url = "$OS"
flavor = "$TARGET_FLAVOR"

[metrics]
bind = "127.0.0.1:$((PORT_BASE + 22))"

[sync.bid]
table = "public.bid_probe"
index = "e2e_bid"
id = "{tenant}-u{id}"
TOML
bid_cleanup() {
  sync_kill
  pg "SELECT pg_drop_replication_slot('$BSLOT') WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name='$BSLOT');" > /dev/null 2>&1 || true
  pg "DROP PUBLICATION IF EXISTS ${BSLOT}_pub; DROP TABLE IF EXISTS bid_probe;" > /dev/null 2>&1 || true
  rm -f "$BCONFIG" /tmp/pg2osync-e2e-bid-$TAG.log
}
trap 'cleanup; resume_cleanup; reject_cleanup; conc_cleanup; rename_cleanup; workers_cleanup; init_cleanup; fan_cleanup; bid_cleanup' EXIT

pg "DROP TABLE IF EXISTS bid_probe; CREATE TABLE bid_probe(id bigint primary key, tenant text);" > /dev/null 2>&1
# without the old row the pipeline could not find the document an id change
# moves out of, so the tool refuses to start rather than strand it
pg "INSERT INTO bid_probe VALUES (1,'acme');" > /dev/null
pg "DROP PUBLICATION IF EXISTS ${BSLOT}_pub; CREATE PUBLICATION ${BSLOT}_pub FOR TABLE bid_probe;" > /dev/null 2>&1
curl -s -XDELETE "$OS/e2e_bid" > /dev/null
: > /tmp/pg2osync-e2e-bid-$TAG.log
sync_spawn "$BCONFIG" /tmp/pg2osync-e2e-bid-$TAG.log
sleep 3
sync_stop
if grep -q "REPLICA IDENTITY FULL" /tmp/pg2osync-e2e-bid-$TAG.log; then
  ok "a non-key id on a non-FULL table is refused with the ALTER to run"
else
  bad "the pipeline started despite an id it cannot delete against"
fi
pg "ALTER TABLE bid_probe REPLICA IDENTITY FULL;" > /dev/null 2>&1
sync_spawn "$BCONFIG"
for _ in $(seq 1 60); do
  refresh
  [ "$(os_status e2e_bid acme-u1)" = "200" ] && break
  sleep 1
done
check "the id is rendered from the configured template" "$(os_status e2e_bid acme-u1)" "200"
pg "UPDATE bid_probe SET tenant='globex' WHERE id = 1;" > /dev/null
for _ in $(seq 1 30); do
  refresh
  [ "$(os_status e2e_bid globex-u1)" = "200" ] && break
  sleep 1
done
check "an update that moves the id writes the new document" "$(os_status e2e_bid globex-u1)" "200"
synced 2> /dev/null || { sleep 2; refresh; }
check "and removes the old one" "$(os_status e2e_bid acme-u1)" "404"
pg "DELETE FROM bid_probe WHERE id = 1;" > /dev/null
for _ in $(seq 1 30); do
  refresh
  [ "$(os_status e2e_bid globex-u1)" = "404" ] && break
  sleep 1
done
check "the delete finds the document by its derived id" "$(os_status e2e_bid globex-u1)" "404"
stop_sync
bid_cleanup

echo -e "\n\033[1m== 22. named transforms reshape a column, and leave alone what they cannot ==\033[0m"
# Seven named ops, no expression language (#63, #142). Row 1 converts on every column;
# row 2 converts on none of them and has to land exactly as it arrived, counted,
# rather than halt the pipeline or be nulled.
SCONFIG=$(mktemp /tmp/pg2osync-e2e-shape.XXXXXX)
SMAPPING=$(dirname "$SCONFIG")/pg2osync-e2e-shape-mapping-$TAG.json
SSLOT=pg2osync_e2e_shape
cat > "$SCONFIG" <<TOML
[source]
url_env = "PG2OSYNC_SOURCE_URL"
slot_name = "$SSLOT"
publication = "${SSLOT}_pub"

[target]
url = "$OS"
flavor = "$TARGET_FLAVOR"

[metrics]
bind = "127.0.0.1:$((PORT_BASE + 23))"

[sync.shape]
table = "public.shape_probe"
index = "e2e_shape"
mapping_file = "pg2osync-e2e-shape-mapping-$TAG.json"

[sync.shape.transform]
payload = "json"
price = "number"
tags = { op = "split", by = "," }
born = { op = "date", from = "%d/%m/%Y" }
status = { op = "lookup", map = { "1" = "active", "2" = "suspended" } }
TOML
# Row 2 keeps `price` and `born` as the strings they arrived as, so under dynamic
# mapping the second document would be a mapping rejection — the quarantine
# path, not the policy under test — hence the fields are typed text up front.
cat > "$SMAPPING" <<'JSON'
{ "mappings": { "properties": { "price": { "type": "text" }, "born": { "type": "text" }, "tags": { "type": "keyword" } } } }
JSON
shape_cleanup() {
  sync_kill
  pg "SELECT pg_drop_replication_slot('$SSLOT') WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name='$SSLOT');" > /dev/null 2>&1 || true
  pg "DROP PUBLICATION IF EXISTS ${SSLOT}_pub; DROP TABLE IF EXISTS shape_probe;" > /dev/null 2>&1 || true
  rm -f "$SCONFIG" "$SMAPPING"
}
trap 'cleanup; resume_cleanup; reject_cleanup; conc_cleanup; rename_cleanup; workers_cleanup; init_cleanup; fan_cleanup; bid_cleanup; shape_cleanup' EXIT

pg "DROP TABLE IF EXISTS shape_probe; CREATE TABLE shape_probe(id bigint primary key, tags text, price text, born text, payload text, status text);" > /dev/null 2>&1
pg "INSERT INTO shape_probe VALUES (1,'a, b ,c','19.99','01/03/2024','{\"k\":1}','1'), (2,'x','abc','not-a-date','{\"k\":2}','9');" > /dev/null
pg "DROP PUBLICATION IF EXISTS ${SSLOT}_pub; CREATE PUBLICATION ${SSLOT}_pub FOR TABLE shape_probe;" > /dev/null 2>&1
curl -s -XDELETE "$OS/e2e_shape" > /dev/null
# a split with nothing to split by is a config mistake, refused where it can
# still be fixed rather than on the first row of the load
sed 's/by = ","/by = ""/' "$SCONFIG" > "${SCONFIG}.bad"
if $BIN validate -c "${SCONFIG}.bad" > /dev/null 2>&1; then
  bad "validate accepted a split with an empty delimiter"
else
  ok "validate refuses a split with nothing to split by"
fi
rm -f "${SCONFIG}.bad"
sync_spawn "$SCONFIG"
for _ in $(seq 1 60); do
  refresh
  [ "$(os_count e2e_shape)" = "2" ] && break
  sleep 1
done
check "a delimited string became an array" "$(os_field e2e_shape 1 tags)" "['a', 'b', 'c']"
check "a numeric string became a number" "$(curl -s "$OS/e2e_shape/_doc/1" | jqf "type(d['_source']['price']).__name__")" "float"
check "a formatted date became ISO 8601" "$(os_field e2e_shape 1 born)" "2024-03-01"
check "a JSON string became an object" "$(curl -s "$OS/e2e_shape/_doc/1" | jqf "type(d['_source']['payload']).__name__")" "dict"
check "a code became the label the dictionary names" "$(os_field e2e_shape 1 status)" "active"
check "an unconvertible value is indexed as it was" "$(os_field e2e_shape 2 price)" "abc"
check "and so is an unparseable date" "$(os_field e2e_shape 2 born)" "not-a-date"
# -ge rather than =: at-least-once delivery may hand row 2 over more than once,
# and every pass counts what it left alone again
left=$(curl -s 127.0.0.1:$((PORT_BASE + 23))/metrics | awk '/^pg2osync_transform_unconverted_total\{/{print $2}')
if [ "${left:-0}" -ge 3 ]; then
  ok "the counter reports the values left as they were ($left)"
else
  bad "the counter reports ${left:-0} values left as they were, want at least 3"
fi
stop_sync

echo -e "\n\033[1m== 23. a row filter decides what is indexed, on the load and on the stream ==\033[0m"
# One SQL subset, pushed into the COPY and evaluated again on every WAL row
# (#64). The load must never read a row that does not match; the stream must
# turn a row that leaves the filter into a delete and one that enters it into
# a write, and a non-matching insert must cost nothing but a not-found delete.
RFCONFIG=$(mktemp /tmp/pg2osync-e2e-where.XXXXXX)
RFSLOT=pg2osync_e2e_where
cat > "$RFCONFIG" <<TOML
[source]
url_env = "PG2OSYNC_SOURCE_URL"
slot_name = "$RFSLOT"
publication = "${RFSLOT}_pub"

[target]
url = "$OS"
flavor = "$TARGET_FLAVOR"

[metrics]
bind = "127.0.0.1:$((PORT_BASE + 24))"

[sync.where_probe]
table = "public.where_probe"
index = "e2e_where"
where = "status = 'active' AND price > 10 AND deleted_at IS NULL"
TOML
where_cleanup() {
  sync_kill
  pg "SELECT pg_drop_replication_slot('$RFSLOT') WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name='$RFSLOT');" > /dev/null 2>&1 || true
  pg "DROP PUBLICATION IF EXISTS ${RFSLOT}_pub; DROP TABLE IF EXISTS where_probe;" > /dev/null 2>&1 || true
  rm -f "$RFCONFIG"
}
trap 'cleanup; resume_cleanup; reject_cleanup; conc_cleanup; rename_cleanup; workers_cleanup; init_cleanup; fan_cleanup; bid_cleanup; shape_cleanup; where_cleanup' EXIT

pg "DROP TABLE IF EXISTS where_probe; CREATE TABLE where_probe(id bigint primary key, status text, price numeric(10,2), deleted_at timestamptz);" > /dev/null 2>&1
# 10.00 against 10.01 is the numeric-string rule: numeric arrives as a JSON
# string in the WAL, and a byte-wise '10.00' > '10' would let row 3 through.
pg "INSERT INTO where_probe VALUES (1,'active',20.00,NULL), (2,'archived',20.00,NULL), (3,'active',10.00,NULL), (4,'active',10.01,NULL), (5,'active',20.00,now());" > /dev/null
pg "DROP PUBLICATION IF EXISTS ${RFSLOT}_pub; CREATE PUBLICATION ${RFSLOT}_pub FOR TABLE where_probe;" > /dev/null 2>&1
curl -s -XDELETE "$OS/e2e_where" > /dev/null
# outside the subset: refused by the grammar, where it can still be fixed
sed "s/^where = .*/where = \"status LIKE 'a%'\"/" "$RFCONFIG" > "${RFCONFIG}.bad"
if $BIN validate -c "${RFCONFIG}.bad" > /dev/null 2>&1; then
  bad "validate accepted a LIKE the engine could not evaluate"
else
  ok "validate refuses a predicate outside the subset"
fi
rm -f "${RFCONFIG}.bad"
# inside the subset but naming no column: only the live table can tell
sed 's/^where = .*/where = "nope = 1"/' "$RFCONFIG" > "${RFCONFIG}.bad"
if $BIN validate -c "${RFCONFIG}.bad" > /dev/null 2>&1; then
  bad "validate accepted a predicate on a column the table does not have"
else
  ok "validate refuses a predicate naming a column the table does not have"
fi
rm -f "${RFCONFIG}.bad"
sync_spawn "$RFCONFIG"
for _ in $(seq 1 60); do
  refresh
  [ "$(os_count e2e_where)" = "2" ] && break
  sleep 1
done
check "the load indexed only the matching rows" "$(os_count e2e_where)" "2"
check "a matching row is indexed" "$(os_status e2e_where 1)" "200"
check "a numeric string above the bound is indexed" "$(os_status e2e_where 4)" "200"
check "a row the status excludes was never read" "$(os_status e2e_where 2)" "404"
check "a numeric string at the bound was never read" "$(os_status e2e_where 3)" "404"
check "a row the NULL test excludes was never read" "$(os_status e2e_where 5)" "404"
pg "UPDATE where_probe SET status='archived' WHERE id = 1;" > /dev/null
for _ in $(seq 1 60); do
  refresh
  [ "$(os_status e2e_where 1)" = "404" ] && break
  sleep 1
done
check "a row that leaves the filter is deleted" "$(os_status e2e_where 1)" "404"
pg "UPDATE where_probe SET status='active' WHERE id = 2;" > /dev/null
for _ in $(seq 1 60); do
  refresh
  [ "$(os_status e2e_where 2)" = "200" ] && break
  sleep 1
done
check "a row that enters the filter is written" "$(os_status e2e_where 2)" "200"
pg "INSERT INTO where_probe VALUES (6,'archived',30,NULL);" > /dev/null
pg "INSERT INTO where_probe VALUES (7,'active',30,NULL);" > /dev/null
for _ in $(seq 1 60); do
  refresh
  [ "$(os_status e2e_where 7)" = "200" ] && break
  sleep 1
done
check "a matching insert is indexed" "$(os_status e2e_where 7)" "200"
check "a non-matching insert is not indexed and does not stop the pipeline" "$(os_status e2e_where 6)" "404"
pg "UPDATE where_probe SET deleted_at=now() WHERE id = 4;" > /dev/null
for _ in $(seq 1 60); do
  refresh
  [ "$(os_status e2e_where 4)" = "404" ] && break
  sleep 1
done
check "a NULL test decides too" "$(os_status e2e_where 4)" "404"
stop_sync

echo -e "\n\033[1m== 24. a parent and its children as a join field in one index ==\033[0m"
# Two tables, one index, every child on its parent's shard (#60). Routing has
# to ride on every write, a re-parented child has to change shard, and a
# parent delete has to take its children with it — through a search, because
# the engine does not know which children the target holds.
JCONFIG=$(mktemp /tmp/pg2osync-e2e-join.XXXXXX)
JMAPPING=$(dirname "$JCONFIG")/pg2osync-e2e-join-mapping-$TAG.json
JSLOT=pg2osync_e2e_join
drop_idle_probe_slots
cat > "$JCONFIG" <<TOML
[source]
url_env = "PG2OSYNC_SOURCE_URL"
slot_name = "$JSLOT"
publication = "${JSLOT}_pub"

[target]
url = "$OS"
flavor = "$TARGET_FLAVOR"

[metrics]
bind = "127.0.0.1:$((PORT_BASE + 25))"

[sync.jcust]
table = "public.jcust"
index = "e2e_shop"
id = "customer-{id}"
mapping_file = "pg2osync-e2e-join-mapping-$TAG.json"

[sync.jcust.join]
field = "relation"
name = "customer"

[sync.jord]
table = "public.jord"
index = "e2e_shop"
id = "order-{id}"

[sync.jord.join]
field = "relation"
name = "order"
parent = "customer_id"
TOML
# Three shards rather than the default one: on a single shard an unrouted GET
# reaches a child by construction, and the routing assertions below would pass
# with no routing at all. Measured on the dev stack, three is the smallest
# count that puts customer-1, customer-2 and every order-N on separate shards.
cat > "$JMAPPING" <<'JSON'
{"settings":{"number_of_shards":3},"mappings":{"properties":{"relation":{"type":"join","relations":{"customer":["order"]}}}}}
JSON
join_cleanup() {
  sync_kill
  pg "SELECT pg_drop_replication_slot('$JSLOT') WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name='$JSLOT');" > /dev/null 2>&1 || true
  pg "DROP PUBLICATION IF EXISTS ${JSLOT}_pub; DROP TABLE IF EXISTS jord, jcust;" > /dev/null 2>&1 || true
  rm -f "$JCONFIG" "$JMAPPING"
}
trap 'cleanup; resume_cleanup; reject_cleanup; conc_cleanup; rename_cleanup; workers_cleanup; init_cleanup; fan_cleanup; bid_cleanup; shape_cleanup; where_cleanup; join_cleanup' EXIT

pg "DROP TABLE IF EXISTS jord, jcust;" > /dev/null 2>&1
pg "CREATE TABLE jcust(id bigint primary key, name text);" > /dev/null
pg "CREATE TABLE jord(id bigint primary key, customer_id bigint, total numeric(10,2));" > /dev/null
# a child's delete is routed by its parent column, which only the old row carries
pg "ALTER TABLE jord REPLICA IDENTITY FULL;" > /dev/null 2>&1
pg "INSERT INTO jcust VALUES (1,'acme'),(2,'globex');" > /dev/null
pg "INSERT INTO jord VALUES (10,1,5.00),(11,1,7.00),(12,2,1.00);" > /dev/null
pg "DROP PUBLICATION IF EXISTS ${JSLOT}_pub; CREATE PUBLICATION ${JSLOT}_pub FOR TABLE jcust, jord;" > /dev/null 2>&1
curl -s -XDELETE "$OS/e2e_shop?ignore_unavailable=true" > /dev/null
curl -s -XDELETE "$OS/.pg2osync_meta/_doc/postgres-$JSLOT" > /dev/null
# two sections on one index are a join pair or each declare an id (#61): the
# same file with the join blocks and one id removed has to be refused where it
# can still be fixed
sed -e '/^\[sync\.[a-z]*\.join\]/,/^$/d' -e '/^id = "order-{id}"$/d' "$JCONFIG" > "${JCONFIG}.bad"
if $BIN validate -c "${JCONFIG}.bad" > /dev/null 2>&1; then
  bad "validate accepted two sections on one index with neither a join nor ids"
else
  ok "validate refuses two sections on one index with neither a join nor ids"
fi
rm -f "${JCONFIG}.bad"
if $BIN validate -c "$JCONFIG" 2>&1 | grep -q "all checks passed"; then
  ok "validate accepts the join pair"
else
  bad "validate refused the join pair"
fi
sync_spawn "$JCONFIG"
for _ in $(seq 1 60); do
  refresh
  [ "$(os_count e2e_shop)" = "5" ] && break
  sleep 1
done
check "the load writes both tables into one index" "$(os_count e2e_shop)" "5"
check "a parent is found without routing" "$(os_status e2e_shop customer-1)" "200"
# a child lives on its parent's shard: only a GET routed there reaches it
check "a child is found under its parent's routing" "$(os_rstatus e2e_shop order-10 customer-1)" "200"
check "and not without it" "$(os_status e2e_shop order-10)" "404"
check "the parent carries its relation name" "$(os_field e2e_shop customer-1 relation)" "customer"
check "the child carries its name and its parent" \
  "$(curl -s "$OS/e2e_shop/_doc/order-10?routing=customer-1" | jqf "json.dumps(d['_source']['relation'], sort_keys=True)")" \
  '{"name": "order", "parent": "customer-1"}'
# the acceptance test for the whole feature: the target can answer the
# parent-child question at all
check "has_child finds every parent with an order" \
  "$(curl -s "$OS/e2e_shop/_search" -H 'Content-Type: application/json' \
      -d '{"query":{"has_child":{"type":"order","query":{"match_all":{}}}}}' | jqf "d['hits']['total']['value']")" "2"

pg "INSERT INTO jord VALUES (13,2,2.00);" > /dev/null
for _ in $(seq 1 30); do
  [ "$(os_rstatus e2e_shop order-13 customer-2)" = "200" ] && break
  sleep 1
done
check "a streamed child lands under its parent's routing" "$(os_rstatus e2e_shop order-13 customer-2)" "200"
check "with its own columns" "$(os_routed e2e_shop order-13 customer-2 total)" "2.00"

# a child whose parent changes changes shard: the same id has to be written
# under the new routing and removed under the old, or the index holds it twice
pg "UPDATE jord SET customer_id=2 WHERE id=10;" > /dev/null
for _ in $(seq 1 30); do
  [ "$(os_rstatus e2e_shop order-10 customer-2)" = "200" ] && [ "$(os_rstatus e2e_shop order-10 customer-1)" = "404" ] && break
  sleep 1
done
check "a re-parented child is found under its new parent" "$(os_rstatus e2e_shop order-10 customer-2)" "200"
check "and is gone from the old one" "$(os_rstatus e2e_shop order-10 customer-1)" "404"

# the cascade: the parent goes, and every child still filed under it goes with
# it — found by a search on the join field's parent-id subfield, refreshed first
pg "DELETE FROM jcust WHERE id=1;" > /dev/null
for _ in $(seq 1 30); do
  [ "$(os_status e2e_shop customer-1)" = "404" ] && [ "$(os_rstatus e2e_shop order-11 customer-1)" = "404" ] && break
  sleep 1
done
check "a deleted parent is gone" "$(os_status e2e_shop customer-1)" "404"
check "and so is the child it still had" "$(os_rstatus e2e_shop order-11 customer-1)" "404"
check "the child that had moved to another parent is untouched" "$(os_rstatus e2e_shop order-10 customer-2)" "200"
check "and so is the other parent" "$(os_status e2e_shop customer-2)" "200"
if curl -s 127.0.0.1:$((PORT_BASE + 25))/metrics | grep -qE '^pg2osync_events_total\{source="[^"]*",type="join_cascade"\} [1-9]'; then
  ok "the cascade is counted"
else
  bad "pg2osync_events_total{type=\"join_cascade\"} did not move"
fi

# reconcile pages one relation at a time, so each half of the pair is checked
# against its own table; nothing is an orphan yet — the cascade left none
refresh
out=$($BIN reconcile -c "$JCONFIG" 2>&1)
if grep -q "Re-run with --delete" <<< "$out"; then
  bad "reconcile found orphans in a consistent join index: $out"
elif grep -q "0 found with no row in public.jcust" <<< "$out" && grep -q "0 found with no row in public.jord" <<< "$out"; then
  ok "reconcile finds no orphan on either side of the pair"
else
  bad "reconcile did not report both tables: $out"
fi
# a row gone while nothing was watching: reconcile has to remove exactly that
# child, routed to the shard that holds it
stop_sync
sleep 1
pg "DELETE FROM jord WHERE id=12;" > /dev/null
refresh
out=$($BIN reconcile -c "$JCONFIG" --delete 2>&1)
case "$out" in
  *"1 removed with no row in public.jord"*) ok "reconcile --delete named the child whose row is gone" ;;
  *) bad "reconcile --delete did not name exactly the missing child: $out" ;;
esac
check "and removed it under its parent's routing" "$(os_rstatus e2e_shop order-12 customer-2)" "404"
check "leaving its sibling alone" "$(os_rstatus e2e_shop order-13 customer-2)" "200"

# a TRUNCATE of one half of the pair clears that relation only: the join
# field is what tells the halves apart, so the customers stay
sync_spawn "$JCONFIG"
sleep 3
pg "TRUNCATE jord;" > /dev/null
for _ in $(seq 1 30); do refresh; [ "$(os_rstatus e2e_shop order-13 customer-2)" = "404" ] && break; sleep 1; done
check "a TRUNCATE of the children clears every child" "$(os_rstatus e2e_shop order-13 customer-2)" "404"
check "and leaves the parents" "$(os_status e2e_shop customer-2)" "200"
stop_sync

echo -e "\n\033[1m== 25. one index fed by two tables ==\033[0m"
# Two unrelated tables, one index, no join (#61). Each section's id is what
# keeps the two tables' documents apart, so the same config with one id
# removed has to be refused; and the two things a shared index cannot do —
# reconcile, and clearing it on TRUNCATE — have to refuse rather than damage
# the table that did not change.
UCONFIG=$(mktemp /tmp/pg2osync-e2e-union.XXXXXX)
USLOT=pg2osync_e2e_union
drop_idle_probe_slots
cat > "$UCONFIG" <<TOML
[source]
url_env = "PG2OSYNC_SOURCE_URL"
slot_name = "$USLOT"
publication = "${USLOT}_pub"

[target]
url = "$OS"
flavor = "$TARGET_FLAVOR"

[metrics]
bind = "127.0.0.1:$((PORT_BASE + 26))"

[sync.user_probe]
table = "public.user_probe"
index = "e2e_union"
id = "user-{id}"

[sync.order_probe]
table = "public.order_probe"
index = "e2e_union"
id = "order-{id}"
TOML
union_cleanup() {
  sync_kill
  pg "SELECT pg_drop_replication_slot('$USLOT') WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name='$USLOT');" > /dev/null 2>&1 || true
  pg "DROP PUBLICATION IF EXISTS ${USLOT}_pub; DROP TABLE IF EXISTS user_probe, order_probe;" > /dev/null 2>&1 || true
  rm -f "$UCONFIG"
}
trap 'cleanup; resume_cleanup; reject_cleanup; conc_cleanup; rename_cleanup; workers_cleanup; init_cleanup; fan_cleanup; bid_cleanup; shape_cleanup; where_cleanup; join_cleanup; union_cleanup' EXIT

pg "DROP TABLE IF EXISTS user_probe, order_probe;" > /dev/null 2>&1
pg "CREATE TABLE user_probe(id bigint primary key, name text);" > /dev/null
pg "CREATE TABLE order_probe(id bigint primary key, total numeric(10,2));" > /dev/null
# the same key in both tables: only the id prefixes keep them two documents
pg "INSERT INTO user_probe VALUES (1,'acme');" > /dev/null
pg "INSERT INTO order_probe VALUES (1,5.00);" > /dev/null
pg "DROP PUBLICATION IF EXISTS ${USLOT}_pub; CREATE PUBLICATION ${USLOT}_pub FOR TABLE user_probe, order_probe;" > /dev/null 2>&1
curl -s -XDELETE "$OS/e2e_union?ignore_unavailable=true" > /dev/null
curl -s -XDELETE "$OS/.pg2osync_meta/_doc/postgres-$USLOT" > /dev/null
# the id is the whole declaration: the same file with one of them removed has
# to be refused where it can still be fixed
sed '/^id = "order-{id}"$/d' "$UCONFIG" > "${UCONFIG}.bad"
# captured first: a refusal exits non-zero, which under pipefail would hide a
# grep that matched
out=$($BIN validate -c "${UCONFIG}.bad" 2>&1 || true)
if grep -q "explicit id template" <<< "$out"; then
  ok "validate refuses a shared index with a section that has no id"
else
  bad "validate accepted a shared index with a section that has no id"
fi
rm -f "${UCONFIG}.bad"
sync_spawn "$UCONFIG"
for _ in $(seq 1 60); do
  refresh
  [ "$(os_count e2e_union)" = "2" ] && break
  sleep 1
done
check "the load writes both tables into one index" "$(os_count e2e_union)" "2"
check "the user is filed under its own prefix" "$(os_status e2e_union user-1)" "200"
check "and so is the order with the same key" "$(os_status e2e_union order-1)" "200"

pg "DELETE FROM order_probe WHERE id=1;" > /dev/null
for _ in $(seq 1 30); do
  [ "$(os_status e2e_union order-1)" = "404" ] && break
  sleep 1
done
check "a deleted order is gone" "$(os_status e2e_union order-1)" "404"
check "and the user with the same key is untouched" "$(os_status e2e_union user-1)" "200"

# reconcile pages by one table's key column: every other table's document
# would look like an orphan, so it has to refuse before it can call one that
out=$($BIN reconcile -c "$UCONFIG" 2>&1 || true)
if grep -q "fed by more than one table" <<< "$out"; then
  ok "reconcile refuses an index more than one table feeds"
else
  bad "reconcile did not refuse the shared index"
fi

# clearing the index on TRUNCATE would take the users with it, which the
# source never truncated, and halting would replay the same TRUNCATE at every
# restart: the truncate is skipped, logged and counted, and the pipeline goes
# on. The log is cumulative across sections, so only a new line counts.
skips_before=$(grep -c "its documents are left in place" "$LOG" || true)
pg "TRUNCATE order_probe;" > /dev/null
for _ in $(seq 1 30); do
  [ "$(grep -c 'its documents are left in place' "$LOG" || true)" -gt "$skips_before" ] && break
  sleep 1
done
if [ "$(grep -c 'its documents are left in place' "$LOG" || true)" -gt "$skips_before" ]; then
  ok "a TRUNCATE of one table is skipped and said so instead of clearing the index"
else
  bad "the TRUNCATE of a table sharing its index was neither skipped nor logged"
fi
check "the other table's document survived the TRUNCATE" "$(os_status e2e_union user-1)" "200"
pg "INSERT INTO user_probe VALUES (2,'still-streaming');" > /dev/null
for _ in $(seq 1 30); do refresh; [ "$(os_status e2e_union user-2)" = "200" ] && break; sleep 1; done
check "and the pipeline is still streaming after it" "$(os_status e2e_union user-2)" "200"
check "the skip is counted" "$(curl -s 127.0.0.1:$((PORT_BASE + 26))/metrics | awk '/^pg2osync_events_total\{source="[^"]*",type="truncate_skipped"\} /{print $2}')" "1"
stop_sync

echo -e "\n\033[1m== 26. a row chooses the index it lands in ==\033[0m"
# `index` with a placeholder is the id problem again (#69): the column can
# change, and the document is then in the old index. So everything section
# 21 holds for a derived id has to hold here — the before-image is required,
# a change moves the document, an unusable name halts — plus what is new:
# the index is created the first time a row needs it, with the configured
# mapping, and nothing that pages one index can run against the table.
ECONFIG=$(mktemp /tmp/pg2osync-e2e-events.XXXXXX)
EMAPPING=$(dirname "$ECONFIG")/pg2osync-e2e-events-mapping-$TAG.json
ESLOT=pg2osync_e2e_events
drop_idle_probe_slots
cat > "$ECONFIG" <<TOML
[source]
url_env = "PG2OSYNC_SOURCE_URL"
slot_name = "$ESLOT"
publication = "${ESLOT}_pub"

[target]
url = "$OS"
flavor = "$TARGET_FLAVOR"

[metrics]
bind = "127.0.0.1:$((PORT_BASE + 27))"

[sync.events]
table = "public.events_probe"
index = "e2e-events-{tenant}"
mapping_file = "pg2osync-e2e-events-mapping-$TAG.json"
TOML
cat > "$EMAPPING" <<'JSON'
{"mappings":{"properties":{"tenant":{"type":"keyword"}}}}
JSON
events_cleanup() {
  sync_kill
  pg "SELECT pg_drop_replication_slot('$ESLOT') WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name='$ESLOT');" > /dev/null 2>&1 || true
  pg "DROP PUBLICATION IF EXISTS ${ESLOT}_pub; DROP TABLE IF EXISTS events_probe;" > /dev/null 2>&1 || true
  curl -s -XDELETE "$OS/e2e-events-*?ignore_unavailable=true" > /dev/null 2>&1 || true
  rm -f "$ECONFIG" "$EMAPPING"
}
trap 'cleanup; resume_cleanup; reject_cleanup; conc_cleanup; rename_cleanup; workers_cleanup; init_cleanup; fan_cleanup; bid_cleanup; shape_cleanup; where_cleanup; join_cleanup; union_cleanup; events_cleanup' EXIT

pg "DROP TABLE IF EXISTS events_probe; CREATE TABLE events_probe(id bigint primary key, tenant text, at timestamptz);" > /dev/null 2>&1
pg "INSERT INTO events_probe VALUES (1,'acme',now());" > /dev/null
pg "DROP PUBLICATION IF EXISTS ${ESLOT}_pub; CREATE PUBLICATION ${ESLOT}_pub FOR TABLE events_probe;" > /dev/null 2>&1
curl -s -XDELETE "$OS/e2e-events-*?ignore_unavailable=true" > /dev/null
curl -s -XDELETE "$OS/.pg2osync_meta/_doc/postgres-$ESLOT" > /dev/null
# a template with no literal part claims `*`, and a TRUNCATE of the table
# would then clear the cluster: refused where it can still be fixed. Captured
# first: a refusal exits non-zero, which under pipefail would hide a grep
# that matched.
sed 's/^index = "e2e-events-{tenant}"$/index = "{tenant}"/' "$ECONFIG" > "${ECONFIG}.bad"
out=$($BIN validate -c "${ECONFIG}.bad" 2>&1 || true)
if grep -q "is all placeholders" <<< "$out"; then
  ok "validate refuses an index template with no literal part"
else
  bad "validate accepted an index template that would claim every index"
fi
rm -f "${ECONFIG}.bad"
# without the old row the pipeline could not find the index a changed row
# was in, so the tool refuses to start rather than strand the document. The
# log is cumulative across sections, so only a new line counts.
full_before=$(grep -c "REPLICA IDENTITY FULL" "$LOG" || true)
sync_spawn "$ECONFIG"
for _ in $(seq 1 30); do
  [ "$(grep -c 'REPLICA IDENTITY FULL' "$LOG" || true)" -gt "$full_before" ] && break
  sleep 1
done
if [ "$(grep -c 'REPLICA IDENTITY FULL' "$LOG" || true)" -gt "$full_before" ]; then
  ok "a per-row index on a non-FULL table is refused with the ALTER to run"
else
  bad "the pipeline started despite an index it could not find the old document in"
fi
stop_sync
pg "ALTER TABLE events_probe REPLICA IDENTITY FULL;" > /dev/null 2>&1
sync_spawn "$ECONFIG"
for _ in $(seq 1 60); do
  refresh
  [ "$(os_status e2e-events-acme 1)" = "200" ] && break
  sleep 1
done
check "the row lands in the index its tenant column names" "$(os_status e2e-events-acme 1)" "200"
# the index did not exist before the row did, so a keyword here says the
# on-demand creation used the section's mapping rather than dynamic mapping
check "the index was created on demand with the configured mapping" \
  "$(curl -s "$OS/e2e-events-acme/_mapping" | jqf "d.get('e2e-events-acme',{}).get('mappings',{}).get('properties',{}).get('tenant',{}).get('type','<missing>')")" "keyword"
pg "UPDATE events_probe SET tenant='globex' WHERE id = 1;" > /dev/null
for _ in $(seq 1 30); do
  refresh
  [ "$(os_status e2e-events-globex 1)" = "200" ] && break
  sleep 1
done
check "an update that changes the index writes the document there" "$(os_status e2e-events-globex 1)" "200"
for _ in $(seq 1 30); do
  refresh
  [ "$(os_status e2e-events-acme 1)" = "404" ] && break
  sleep 1
done
check "and removes it from the index it was in" "$(os_status e2e-events-acme 1)" "404"
pg "DELETE FROM events_probe WHERE id = 1;" > /dev/null
for _ in $(seq 1 30); do
  refresh
  [ "$(os_status e2e-events-globex 1)" = "404" ] && break
  sleep 1
done
check "the delete finds the document in the index the old row names" "$(os_status e2e-events-globex 1)" "404"
check "exactly the indices the rows named exist" "$(curl -s "$OS/_cat/indices/e2e-events-*?h=index" | wc -l | tr -d ' ')" "2"
# an uppercase letter cannot become an index, and inventing a name would
# file the row where nothing looks for it: the pipeline halts, naming the
# template, the column and the value it rendered. Only a new line counts.
halts_before=$(grep -c "not a usable index name" "$LOG" || true)
pg "INSERT INTO events_probe VALUES (2,'ACME',now());" > /dev/null
for _ in $(seq 1 30); do
  [ "$(grep -c 'not a usable index name' "$LOG" || true)" -gt "$halts_before" ] && break
  sleep 1
done
if [ "$(grep -c 'not a usable index name' "$LOG" || true)" -gt "$halts_before" ]; then
  ok "a rendered name that is not a legal index halts the pipeline"
else
  bad "the pipeline accepted an index name the target could not have created"
fi
if grep "not a usable index name" "$LOG" | tail -1 | grep -q "ACME"; then
  ok "and the halt names the value it rendered"
else
  bad "the halt did not say which value was refused"
fi
check "no index was created for the refused name" "$(curl -s "$OS/_cat/indices/e2e-events-*?h=index" | wc -l | tr -d ' ')" "2"
stop_sync
# reconcile pages one index by its key column, and this table's documents
# are spread over every index the template renders
out=$($BIN reconcile -c "$ECONFIG" 2>&1 || true)
if grep -q "chosen per row" <<< "$out"; then
  ok "reconcile refuses a table whose index is chosen per row"
else
  bad "reconcile did not refuse the templated table"
fi

echo -e "\n\033[1m== 27. the target runs an ingest pipeline on every document of a section ==\033[0m"
# The vector is the target's to compute (#68): the section names an ingest
# pipeline, the bulk action carries it, and the target fills the field. A
# `set` processor stands in for the embedding model — what has to hold is
# that the load and the stream both go through the pipeline, and that a
# pipeline the target does not have is refused where it can still be fixed.
PCONFIG=$(mktemp /tmp/pg2osync-e2e-pipe.XXXXXX)
PSLOT=pg2osync_e2e_pipe
drop_idle_probe_slots
cat > "$PCONFIG" <<TOML
[source]
url_env = "PG2OSYNC_SOURCE_URL"
slot_name = "$PSLOT"
publication = "${PSLOT}_pub"

[target]
url = "$OS"
flavor = "$TARGET_FLAVOR"

[metrics]
bind = "127.0.0.1:$((PORT_BASE + 28))"

[sync.pipe]
table = "public.pipe_probe"
index = "e2e_pipe"
pipeline = "e2e-tag"
TOML
pipe_cleanup() {
  sync_kill
  pg "SELECT pg_drop_replication_slot('$PSLOT') WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name='$PSLOT');" > /dev/null 2>&1 || true
  pg "DROP PUBLICATION IF EXISTS ${PSLOT}_pub; DROP TABLE IF EXISTS pipe_probe;" > /dev/null 2>&1 || true
  curl -s -XDELETE "$OS/_ingest/pipeline/e2e-tag" > /dev/null 2>&1 || true
  curl -s -XDELETE "$OS/e2e_pipe?ignore_unavailable=true" > /dev/null 2>&1 || true
  rm -f "$PCONFIG"
}
trap 'cleanup; resume_cleanup; reject_cleanup; conc_cleanup; rename_cleanup; workers_cleanup; init_cleanup; fan_cleanup; bid_cleanup; shape_cleanup; where_cleanup; join_cleanup; union_cleanup; events_cleanup; pipe_cleanup' EXIT

pg "DROP TABLE IF EXISTS pipe_probe; CREATE TABLE pipe_probe(id bigint primary key, name text);" > /dev/null 2>&1
pg "INSERT INTO pipe_probe VALUES (1,'one');" > /dev/null
pg "DROP PUBLICATION IF EXISTS ${PSLOT}_pub; CREATE PUBLICATION ${PSLOT}_pub FOR TABLE pipe_probe;" > /dev/null 2>&1
curl -s -XDELETE "$OS/e2e_pipe?ignore_unavailable=true" > /dev/null
curl -s -XDELETE "$OS/.pg2osync_meta/_doc/postgres-$PSLOT" > /dev/null
curl -s -XPUT "$OS/_ingest/pipeline/e2e-tag" -H 'Content-Type: application/json' -d '{"processors":[{"set":{"field":"tagged","value":"yes"}}]}' > /dev/null
# a pipeline the target does not have would reject every document at the
# first write, so validate asks for it by name and refuses. Captured first: a
# refusal exits non-zero, which under pipefail would hide a grep that matched.
sed 's/^pipeline = "e2e-tag"$/pipeline = "e2e-missing"/' "$PCONFIG" > "${PCONFIG}.bad"
out=$($BIN validate -c "${PCONFIG}.bad" 2>&1 || true)
if grep -q "does not exist on the target" <<< "$out"; then
  ok "validate refuses a pipeline the target does not have"
else
  bad "validate accepted a pipeline the target does not have"
fi
rm -f "${PCONFIG}.bad"
out=$($BIN validate -c "$PCONFIG" 2>&1 || true)
if grep -q "ingest pipeline e2e-tag exists" <<< "$out"; then
  ok "validate names the pipeline it found"
else
  bad "validate did not report the pipeline the target has"
fi
sync_spawn "$PCONFIG"
for _ in $(seq 1 60); do
  refresh
  [ "$(os_status e2e_pipe 1)" = "200" ] && break
  sleep 1
done
# the field exists in no row: only the pipeline could have put it there
check "the load goes through the pipeline" "$(os_field e2e_pipe 1 tagged)" "yes"
pg "INSERT INTO pipe_probe VALUES (2,'two');" > /dev/null
for _ in $(seq 1 30); do
  refresh
  [ "$(os_status e2e_pipe 2)" = "200" ] && break
  sleep 1
done
check "and so does a streamed row" "$(os_field e2e_pipe 2 tagged)" "yes"
stop_sync

echo -e "\n\033[1m== 28. a table with no primary key syncs insert-only, under a content hash ==\033[0m"
# An event log has no key and never needs one (#70): declared append_only,
# each row is filed under a hash of its raw values, so the load and the
# stream agree on the id without a position, and a row the source cannot
# tell from another is one document. What has to hold: validate takes the
# declaration and refuses a key beside it, a duplicate never doubles the
# count on either path, and an UPDATE — which nothing can address — halts
# by name.
ACONFIG=$(mktemp /tmp/pg2osync-e2e-append.XXXXXX)
ASLOT=pg2osync_e2e_append
drop_idle_probe_slots
cat > "$ACONFIG" <<TOML
[source]
url_env = "PG2OSYNC_SOURCE_URL"
slot_name = "$ASLOT"
publication = "${ASLOT}_pub"

[target]
url = "$OS"
flavor = "$TARGET_FLAVOR"

[metrics]
bind = "127.0.0.1:$((PORT_BASE + 29))"

[sync.events_log]
table = "public.events_log"
index = "e2e_append"
append_only = true
TOML
append_cleanup() {
  sync_kill
  pg "SELECT pg_drop_replication_slot('$ASLOT') WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name='$ASLOT');" > /dev/null 2>&1 || true
  pg "DROP PUBLICATION IF EXISTS ${ASLOT}_pub; DROP TABLE IF EXISTS events_log;" > /dev/null 2>&1 || true
  curl -s -XDELETE "$OS/e2e_append?ignore_unavailable=true" > /dev/null 2>&1 || true
  rm -f "$ACONFIG"
}
trap 'cleanup; resume_cleanup; reject_cleanup; conc_cleanup; rename_cleanup; workers_cleanup; init_cleanup; fan_cleanup; bid_cleanup; shape_cleanup; where_cleanup; join_cleanup; union_cleanup; events_cleanup; pipe_cleanup; append_cleanup' EXIT

pg "DROP TABLE IF EXISTS events_log; CREATE TABLE events_log(at timestamptz, kind text, payload text);" > /dev/null 2>&1
# the third row is the first one again, byte for byte: same hash, same document
pg "INSERT INTO events_log VALUES ('2024-01-01T00:00:00Z','login','alice'), ('2024-01-01T00:00:01Z','logout','alice'), ('2024-01-01T00:00:00Z','login','alice');" > /dev/null
pg "DROP PUBLICATION IF EXISTS ${ASLOT}_pub; CREATE PUBLICATION ${ASLOT}_pub FOR TABLE events_log;" > /dev/null 2>&1
curl -s -XDELETE "$OS/e2e_append?ignore_unavailable=true" > /dev/null
curl -s -XDELETE "$OS/.pg2osync_meta/_doc/postgres-$ASLOT" > /dev/null
if $BIN validate -c "$ACONFIG" 2>&1 | grep -q "all checks passed"; then
  ok "validate accepts a keyless table declared append_only"
else
  bad "validate refused an append_only table with no primary key"
fi
# a key and the declaration cannot both be true of one table. Captured
# first: a refusal exits non-zero, which under pipefail would hide a grep
# that matched.
printf 'primary_key = "kind"\n' | cat "$ACONFIG" - > "${ACONFIG}.bad"
out=$($BIN validate -c "${ACONFIG}.bad" 2>&1 || true)
if grep -q "contradict" <<< "$out"; then
  ok "validate refuses primary_key beside append_only"
else
  bad "validate accepted primary_key on an append_only table"
fi
rm -f "${ACONFIG}.bad"
sync_spawn "$ACONFIG"
for _ in $(seq 1 60); do
  refresh
  [ "$(os_count e2e_append)" = "2" ] && break
  sleep 1
done
check "the load files three rows as two documents: the duplicate is the same one" "$(os_count e2e_append)" "2"
pg "INSERT INTO events_log VALUES ('2024-01-01T00:00:02Z','login','bob');" > /dev/null
for _ in $(seq 1 30); do
  refresh
  [ "$(os_count e2e_append)" = "3" ] && break
  sleep 1
done
check "a streamed row is a new document" "$(os_count e2e_append)" "3"
# the same row again, then a fresh one to wait on: the stream hashes as the
# load did, so the count moves by one, not two
pg "INSERT INTO events_log VALUES ('2024-01-01T00:00:02Z','login','bob');" > /dev/null
pg "INSERT INTO events_log VALUES ('2024-01-01T00:00:03Z','logout','bob');" > /dev/null
for _ in $(seq 1 30); do
  refresh
  [ "$(os_count e2e_append)" -ge 4 ] && break
  sleep 1
done
check "a streamed duplicate is the document it already is" "$(os_count e2e_append)" "4"
# Nothing can say which document a changed row is, so the pipeline halts by
# name rather than guess or skip. PostgreSQL itself refuses an UPDATE on a
# published table with no replica identity, so the one way an UPDATE can
# reach the pipeline is under FULL — set while stopped, as in section 26.
# The log is cumulative across sections, so only a new line counts.
stop_sync
pg "ALTER TABLE events_log REPLICA IDENTITY FULL;" > /dev/null 2>&1
halts_before=$(grep -c "an UPDATE arrived on an append-only table" "$LOG" || true)
sync_spawn "$ACONFIG"
pg "UPDATE events_log SET kind='x';" > /dev/null
for _ in $(seq 1 30); do
  [ "$(grep -c 'an UPDATE arrived on an append-only table' "$LOG" || true)" -gt "$halts_before" ] && break
  sleep 1
done
if [ "$(grep -c 'an UPDATE arrived on an append-only table' "$LOG" || true)" -gt "$halts_before" ]; then
  ok "an UPDATE on an append-only table halts the pipeline"
else
  bad "an UPDATE on an append-only table was not refused"
fi
stop_sync

echo -e "\n\033[1m== 29. a column routes a section's documents to one shard ==\033[0m"
# One tenant per shard (#109): `routing = "tenant"` puts every document of a
# tenant on the shard that value hashes to, so a document is only found with
# its routing, a changed tenant moves it, and everything that already carried
# a routing for a join child — deletes, reconcile, TRUNCATE — has to keep
# working for a section that has no join at all.
RTCONFIG=$(mktemp /tmp/pg2osync-e2e-routing.XXXXXX)
RTSLOT=pg2osync_e2e_routing
drop_idle_probe_slots
cat > "$RTCONFIG" <<TOML
[source]
url_env = "PG2OSYNC_SOURCE_URL"
slot_name = "$RTSLOT"
publication = "${RTSLOT}_pub"

[target]
url = "$OS"

[metrics]
bind = "127.0.0.1:$((PORT_BASE + 30))"

[sync.tenanted]
table = "public.tenanted"
index = "e2e_routing"
routing = "tenant"
TOML
routing_cleanup() {
  sync_kill
  pg "SELECT pg_drop_replication_slot('$RTSLOT') WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name='$RTSLOT');" > /dev/null 2>&1 || true
  pg "DROP PUBLICATION IF EXISTS ${RTSLOT}_pub; DROP TABLE IF EXISTS tenanted;" > /dev/null 2>&1 || true
  curl -s -XDELETE "$OS/e2e_routing?ignore_unavailable=true" > /dev/null 2>&1 || true
  rm -f "$RTCONFIG" /tmp/pg2osync-e2e-routing-$TAG.log
}
trap 'cleanup; resume_cleanup; reject_cleanup; conc_cleanup; rename_cleanup; workers_cleanup; init_cleanup; fan_cleanup; bid_cleanup; shape_cleanup; where_cleanup; join_cleanup; union_cleanup; events_cleanup; pipe_cleanup; append_cleanup; routing_cleanup' EXIT

pg "DROP TABLE IF EXISTS tenanted; CREATE TABLE tenanted(id bigint primary key, tenant text, name text);" > /dev/null 2>&1
pg "INSERT INTO tenanted VALUES (1,'acme','first'),(2,'globex','second');" > /dev/null
pg "DROP PUBLICATION IF EXISTS ${RTSLOT}_pub; CREATE PUBLICATION ${RTSLOT}_pub FOR TABLE tenanted;" > /dev/null 2>&1
curl -s -XDELETE "$OS/e2e_routing?ignore_unavailable=true" > /dev/null
curl -s -XDELETE "$OS/.pg2osync_meta/_doc/postgres-$RTSLOT" > /dev/null
# Three shards rather than the default one: on a single shard every document
# is reachable without its routing, and the assertions below would pass with
# no routing at all.
curl -s -XPUT "$OS/e2e_routing" -H 'Content-Type: application/json' \
  -d '{"settings":{"number_of_shards":3}}' > /dev/null
if $BIN validate -c "$RTCONFIG" 2>&1 | grep -q "all checks passed"; then
  ok "validate accepts a routing column"
else
  bad "validate refused a routing column"
fi
# a join child is already routed to its parent; a second owner of the shard
# has to be refused where it can still be fixed
printf '[sync.tenanted.join]\nfield = "rel"\nname = "doc"\n' | cat "$RTCONFIG" - > "${RTCONFIG}.bad"
out=$($BIN validate -c "${RTCONFIG}.bad" 2>&1 || true)
if grep -q "routing and join cannot be combined" <<< "$out"; then
  ok "validate refuses routing beside join"
else
  bad "validate accepted routing beside join: $out"
fi
rm -f "${RTCONFIG}.bad"
# the routing column is not the key, so a delete carries it only under FULL:
# the pipeline refuses to start rather than strand documents on their shards
: > /tmp/pg2osync-e2e-routing-$TAG.log
sync_spawn "$RTCONFIG" /tmp/pg2osync-e2e-routing-$TAG.log
sleep 3
sync_stop
if grep -q "REPLICA IDENTITY FULL" /tmp/pg2osync-e2e-routing-$TAG.log; then
  ok "a non-key routing column on a non-FULL table is refused with the ALTER to run"
else
  bad "the pipeline started despite a routing it could not derive on a delete"
fi
pg "ALTER TABLE tenanted REPLICA IDENTITY FULL;" > /dev/null 2>&1

sync_spawn "$RTCONFIG"
for _ in $(seq 1 60); do
  refresh
  [ "$(os_count e2e_routing)" = "2" ] && break
  sleep 1
done
check "the load writes both rows" "$(os_count e2e_routing)" "2"
check "a document is found under its tenant's routing" "$(os_rstatus e2e_routing 1 acme)" "200"
check "and not without it" "$(os_status e2e_routing 1)" "404"
check "the target reports the routing it filed the document under" \
  "$(curl -s "$OS/e2e_routing/_doc/1?routing=acme" | jqf "d.get('_routing','<missing>')")" "acme"
check "the routing column stays an ordinary field" "$(os_routed e2e_routing 1 acme name)" "first"

pg "INSERT INTO tenanted VALUES (3,'acme','third');" > /dev/null
for _ in $(seq 1 30); do
  [ "$(os_rstatus e2e_routing 3 acme)" = "200" ] && break
  sleep 1
done
check "a streamed row lands under its tenant's routing" "$(os_rstatus e2e_routing 3 acme)" "200"

# the acceptance test: the value that decides the shard can change, so the
# document has to be written under the new routing and removed under the old
pg "UPDATE tenanted SET tenant='globex' WHERE id=1;" > /dev/null
for _ in $(seq 1 30); do
  [ "$(os_rstatus e2e_routing 1 globex)" = "200" ] && [ "$(os_rstatus e2e_routing 1 acme)" = "404" ] && break
  sleep 1
done
check "a moved document is found under its new routing" "$(os_rstatus e2e_routing 1 globex)" "200"
check "and is gone from the old one" "$(os_rstatus e2e_routing 1 acme)" "404"
refresh
check "one copy, not two" "$(os_count e2e_routing)" "3"

pg "DELETE FROM tenanted WHERE id=3;" > /dev/null
for _ in $(seq 1 30); do
  [ "$(os_rstatus e2e_routing 3 acme)" = "404" ] && break
  sleep 1
done
check "a delete removes the document on the shard that holds it" "$(os_rstatus e2e_routing 3 acme)" "404"

# reconcile reads each hit's own routing, so it needs none of this derived:
# nothing is an orphan yet, and a row that goes while nothing is watching is
# removed where it is
refresh
out=$($BIN reconcile -c "$RTCONFIG" 2>&1)
if grep -q "Re-run with --delete" <<< "$out"; then
  bad "reconcile found orphans in a consistent routed index: $out"
else
  ok "reconcile finds no orphan in a routed index"
fi
stop_sync
sleep 1
pg "DELETE FROM tenanted WHERE id=2;" > /dev/null
refresh
out=$($BIN reconcile -c "$RTCONFIG" --delete 2>&1)
case "$out" in
  *"1 removed with no row in public.tenanted"*) ok "reconcile --delete named the document whose row is gone" ;;
  *) bad "reconcile --delete did not name exactly the missing document: $out" ;;
esac
check "and removed it under its own routing" "$(os_rstatus e2e_routing 2 globex)" "404"

# a TRUNCATE is index-wide, so routing has nothing to do with it
sync_spawn "$RTCONFIG"
sleep 3
pg "TRUNCATE tenanted;" > /dev/null
for _ in $(seq 1 30); do refresh; [ "$(os_count e2e_routing)" = "0" ] && break; sleep 1; done
check "a TRUNCATE clears the index whatever the documents were routed by" "$(os_count e2e_routing)" "0"
stop_sync

echo -e "\n\033[1m== 30. a rebuild into a fresh index, then the alias onto it ==\033[0m"
# A mapping cannot change on a live index, so a rebuild is a new index and an
# alias flip (#107). What has to hold: the fresh index is created with the
# mapping the section names *now*, the count is checked against the source,
# the checkpoint does not move — so the restart replays what changed while the
# pipeline was stopped — and the command refuses everything it cannot do
# safely, a live stream first among them.
XCONFIG=$(mktemp /tmp/pg2osync-e2e-reindex.XXXXXX)
XMAPPING=$(dirname "$XCONFIG")/pg2osync-e2e-reindex-mapping-$TAG.json
XSLOT=pg2osync_e2e_reindex
XALIAS=e2e_reindex_live
drop_idle_probe_slots
cat > "$XCONFIG" <<TOML
[source]
url_env = "PG2OSYNC_SOURCE_URL"
slot_name = "$XSLOT"
publication = "${XSLOT}_pub"

[target]
url = "$OS"
flavor = "$TARGET_FLAVOR"

[metrics]
bind = "127.0.0.1:$((PORT_BASE + 32))"

[sync.reindex]
table = "public.reindex_probe"
index = "e2e_reindex"
mapping_file = "pg2osync-e2e-reindex-mapping-$TAG.json"
TOML
cat > "$XMAPPING" <<'JSON'
{ "mappings": { "properties": { "v": { "type": "text" } } } }
JSON
# a wildcard DELETE is refused by both targets by default, so the rebuilt
# indices are named one at a time
drop_reindex_indices() {
  for name in $(curl -s "$OS/_cat/indices/e2e_reindex*?h=index" 2> /dev/null); do
    curl -s -XDELETE "$OS/$name" > /dev/null 2>&1 || true
  done
}
reindex_cleanup() {
  sync_kill
  pg "SELECT pg_drop_replication_slot('$XSLOT') WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name='$XSLOT');" > /dev/null 2>&1 || true
  pg "DROP PUBLICATION IF EXISTS ${XSLOT}_pub; DROP TABLE IF EXISTS reindex_probe;" > /dev/null 2>&1 || true
  drop_reindex_indices
  rm -f "$XCONFIG" "$XMAPPING" "${XCONFIG}.new"
}
trap 'cleanup; resume_cleanup; reject_cleanup; conc_cleanup; rename_cleanup; workers_cleanup; init_cleanup; fan_cleanup; bid_cleanup; shape_cleanup; where_cleanup; join_cleanup; union_cleanup; events_cleanup; pipe_cleanup; append_cleanup; routing_cleanup; reindex_cleanup' EXIT

pg "DROP TABLE IF EXISTS reindex_probe; CREATE TABLE reindex_probe(id bigint primary key, v text);" > /dev/null 2>&1
pg "INSERT INTO reindex_probe VALUES (1,'one'),(2,'two'),(3,'three');" > /dev/null
pg "DROP PUBLICATION IF EXISTS ${XSLOT}_pub; CREATE PUBLICATION ${XSLOT}_pub FOR TABLE reindex_probe;" > /dev/null 2>&1
drop_reindex_indices
curl -s -XDELETE "$OS/.pg2osync_meta/_doc/postgres-$XSLOT" > /dev/null

sync_spawn "$XCONFIG"
for _ in $(seq 1 60); do
  refresh
  [ "$(os_count e2e_reindex)" = "3" ] && break
  sleep 1
done
check "the original index is loaded" "$(os_count e2e_reindex)" "3"

# a rebuild beside a running pipeline would fill an index the stream is not
# writing to, so a row changing under it would be wrong there for good
refusal=$($BIN reindex -c "$XCONFIG" --table public.reindex_probe --alias "$XALIAS" 2>&1 || true)
case "$refusal" in
  *"$XSLOT"*"reader attached"*) ok "a rebuild refuses while the pipeline is running, naming the slot" ;;
  *) bad "a rebuild ran beside a live stream: $refusal" ;;
esac
stop_sync

# the mapping the rebuild has to pick up: v is a keyword from here on, which
# is the change no live index would accept
cat > "$XMAPPING" <<'JSON'
{ "mappings": { "properties": { "v": { "type": "keyword" } } } }
JSON
ckpt_before=$(curl -s "$OS/.pg2osync_meta/_doc/postgres-$XSLOT" | jqf "d['_source']['position']")

out=$($BIN reindex -c "$XCONFIG" --table public.reindex_probe --alias "$XALIAS" 2>&1)
echo "$out" >> "$LOG"
new_index=$(curl -s "$OS/_alias/$XALIAS" | jqf "sorted(d.keys())[0] if d else 'none'")
case "$new_index" in
  e2e_reindex-*) ok "the rebuild created $new_index and the alias resolves to it" ;;
  *) bad "the alias does not resolve to a rebuilt index (got '$new_index'): $out" ;;
esac
refresh
check "the rebuilt index holds every row" "$(os_count "$new_index")" "$(pg 'SELECT count(*) FROM reindex_probe;')"
check "the alias reads the rebuilt index" "$(os_count "$XALIAS")" "3"
check "the rebuilt index took the new mapping" \
  "$(curl -s "$OS/$new_index/_mapping" | jqf "d['$new_index']['mappings']['properties']['v']['type']")" "keyword"
check "the checkpoint did not move" \
  "$(curl -s "$OS/.pg2osync_meta/_doc/postgres-$XSLOT" | jqf "d['_source']['position']")" "$ckpt_before"
check "the old index is kept as the rollback" "$(curl -s -o /dev/null -w '%{http_code}' "$OS/e2e_reindex")" "200"
if grep -q "DELETE /e2e_reindex" <<< "$out"; then
  ok "the output says how to remove the old index"
else
  bad "the output does not name the delete: $out"
fi

# The crux. A rebuild fills an index the stream is not writing to, so it can
# only be as fresh as the moment the load read. What closes the gap is the
# checkpoint standing still: the restart replays everything committed since.
pg "UPDATE reindex_probe SET v='changed-while-stopped' WHERE id=1;" > /dev/null
pg "INSERT INTO reindex_probe VALUES (4,'added-while-stopped');" > /dev/null
refresh
check "the rebuilt index cannot know about them yet" "$(os_field "$new_index" 1 v)" "one"
sed "s#^index = \"e2e_reindex\"\$#index = \"$new_index\"#" "$XCONFIG" > "${XCONFIG}.new"
sync_spawn "${XCONFIG}.new"
for _ in $(seq 1 60); do
  refresh
  [ "$(os_status "$new_index" 4)" = "200" ] && break
  sleep 1
done
check "the restart replays what changed while the pipeline was stopped" \
  "$(os_field "$new_index" 1 v)" "changed-while-stopped"
check "and what was inserted while it was stopped" "$(os_field "$new_index" 4 v)" "added-while-stopped"
stop_sync

# an alias points at one index, and a rebuild reads one table
sed "s#^index = \"e2e_reindex\"\$#index = \"e2e_reindex_{id}\"#" "$XCONFIG" > "${XCONFIG}.new"
refusal=$($BIN reindex -c "${XCONFIG}.new" --table public.reindex_probe --alias "$XALIAS" 2>&1 || true)
case "$refusal" in
  *"per row"*) ok "a rebuild refuses a templated index" ;;
  *) bad "a templated index was accepted: $refusal" ;;
esac
cat "$XCONFIG" > "${XCONFIG}.new"
cat >> "${XCONFIG}.new" <<'TOML'
id = "p-{id}"

[sync.reindex_two]
table = "public.users"
index = "e2e_reindex"
id = "u-{id}"
TOML
refusal=$($BIN reindex -c "${XCONFIG}.new" --table public.reindex_probe --alias "$XALIAS" 2>&1 || true)
case "$refusal" in
  *"more than one table"*) ok "a rebuild refuses an index two tables feed" ;;
  *) bad "a shared index was accepted: $refusal" ;;
esac

# --drop-old removes the index the alias came off; the default kept it above
sed "s#^index = \"e2e_reindex\"\$#index = \"$new_index\"#" "$XCONFIG" > "${XCONFIG}.new"
$BIN reindex -c "${XCONFIG}.new" --table public.reindex_probe --alias "$XALIAS" --drop-old >> "$LOG" 2>&1
check "--drop-old removed the index the alias came off" \
  "$(curl -s -o /dev/null -w '%{http_code}' "$OS/$new_index")" "404"
newer_index=$(curl -s "$OS/_alias/$XALIAS" | jqf "sorted(d.keys())[0] if d else 'none'")
refresh
check "and the alias points at the newest rebuild" "$(os_count "$newer_index")" "4"

echo -e "\n\033[1m== 31. the load obeys a rate limit the operator asked for ==\033[0m"
# A ceiling on load rows a second (#144), so being gentle with a production
# primary does not mean waiting for the night. It is enforced once, at the
# engine's intake of load rows, which is where both sources and all three
# commands that run a load converge — so a re-snapshot is the cheapest way to
# see it, and it needs no slot and no pipeline.
RTCONFIG=$(mktemp /tmp/pg2osync-e2e-rate.XXXXXX)
RTLOG=/tmp/pg2osync-e2e-rate-$TAG.log
: > "$RTLOG"
cat > "$RTCONFIG" <<TOML
[source]
url_env = "PG2OSYNC_SOURCE_URL"
slot_name = "pg2osync_e2e_rate"
publication = "pg2osync_e2e_rate_pub"

[target]
url = "$OS"
flavor = "$TARGET_FLAVOR"

[engine]
load_max_rows_per_sec = 50

[sync.rate]
table = "public.rate_probe"
index = "e2e_rate"
TOML
rate_cleanup() {
  pg "DROP TABLE IF EXISTS rate_probe;" > /dev/null 2>&1 || true
  curl -s -XDELETE "$OS/e2e_rate" > /dev/null 2>&1 || true
  rm -f "$RTCONFIG" "$RTLOG"
}
trap 'cleanup; resume_cleanup; reject_cleanup; conc_cleanup; rename_cleanup; workers_cleanup; init_cleanup; fan_cleanup; bid_cleanup; shape_cleanup; where_cleanup; join_cleanup; union_cleanup; events_cleanup; pipe_cleanup; append_cleanup; routing_cleanup; reindex_cleanup; rate_cleanup' EXIT

pg "DROP TABLE IF EXISTS rate_probe; CREATE TABLE rate_probe(id bigint primary key, v text);" > /dev/null 2>&1
pg "INSERT INTO rate_probe SELECT g, 'v' || g FROM generate_series(1, 200) g;" > /dev/null
curl -s -XDELETE "$OS/e2e_rate" > /dev/null 2>&1

started=$SECONDS
$BIN resnapshot -c "$RTCONFIG" --table public.rate_probe >> "$RTLOG" 2>&1
took=$((SECONDS - started))
refresh
check "every row of a capped load is still indexed" "$(os_count e2e_rate)" "200"
# 200 rows at 50 a second is four seconds of intake however fast everything else is
if [ "$took" -ge 3 ]; then
  ok "200 rows at 50 rows/s took ${took}s"
else
  bad "the cap did not hold the load back: 200 rows in ${took}s"
fi
if grep -q "capped at 50 rows/s" "$RTLOG"; then
  ok "the load's summary line names the ceiling"
else
  bad "the summary line does not name the ceiling: $(grep 'rows from' "$RTLOG" || true)"
fi

echo -e "\n\033[1m== 32. a many-to-many relation embeds through its junction table ==\033[0m"
# `through` (#141): the rows worth embedding are one table further than the one
# carrying the parent's key, so the aggregation gains one join and both tables
# are streamed. What has to hold: the junction keys the array, a junction row
# makes and breaks the relation without REPLICA IDENTITY FULL on it (its key
# carries the column that names the parent), a changed author reaches every one
# of their books, and a transaction full of junction rows still costs a
# constant number of reads.
TCONFIG=$(mktemp /tmp/pg2osync-e2e-through.XXXXXX)
TSLOT=pg2osync_e2e_through
drop_idle_probe_slots
cat > "$TCONFIG" <<TOML
[source]
url_env = "PG2OSYNC_SOURCE_URL"
slot_name = "$TSLOT"
publication = "${TSLOT}_pub"

[target]
url = "$OS"
flavor = "$TARGET_FLAVOR"

[metrics]
bind = "127.0.0.1:$((PORT_BASE + 33))"

[sync.books]
table = "public.books"
index = "e2e_through"

[[sync.books.children]]
table = "public.authors"
field = "authors"
through = "public.book_author"
foreign_key = "book_id"
through_key = "author_id"
TOML
through_cleanup() {
  sync_kill
  pg "SELECT pg_drop_replication_slot('$TSLOT') WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name='$TSLOT');" > /dev/null 2>&1 || true
  pg "DROP PUBLICATION IF EXISTS ${TSLOT}_pub; DROP TABLE IF EXISTS book_author; DROP TABLE IF EXISTS authors; DROP TABLE IF EXISTS books;" > /dev/null 2>&1 || true
  curl -s -XDELETE "$OS/e2e_through,e2e_through_capped?ignore_unavailable=true" > /dev/null 2>&1 || true
  rm -f "$TCONFIG" "${TCONFIG}.capped"
}
trap 'cleanup; resume_cleanup; reject_cleanup; conc_cleanup; rename_cleanup; workers_cleanup; init_cleanup; fan_cleanup; bid_cleanup; shape_cleanup; where_cleanup; join_cleanup; union_cleanup; events_cleanup; pipe_cleanup; append_cleanup; routing_cleanup; reindex_cleanup; rate_cleanup; through_cleanup' EXIT

pg "DROP TABLE IF EXISTS book_author; DROP TABLE IF EXISTS authors; DROP TABLE IF EXISTS books;" > /dev/null 2>&1
pg "CREATE TABLE books(id bigint primary key, title text);
    CREATE TABLE authors(id bigint primary key, name text);
    CREATE TABLE book_author(book_id bigint, author_id bigint,
                             PRIMARY KEY (book_id, author_id));
    CREATE INDEX book_author_author_idx ON book_author(author_id);" > /dev/null
pg "INSERT INTO books VALUES (1,'first'),(2,'second'),(3,'unwritten');" > /dev/null
pg "INSERT INTO authors VALUES (7,'ada'),(8,'grace'),(9,'edsger');" > /dev/null
pg "INSERT INTO book_author VALUES (1,7),(1,8),(2,7);" > /dev/null
pg "DROP PUBLICATION IF EXISTS ${TSLOT}_pub;" > /dev/null 2>&1
curl -s -XDELETE "$OS/e2e_through?ignore_unavailable=true" > /dev/null
curl -s -XDELETE "$OS/.pg2osync_meta/_doc/postgres-$TSLOT" > /dev/null

vout=$($BIN validate -c "$TCONFIG" 2>&1 || true)
if grep -q "all checks passed" <<< "$vout"; then
  ok "validate accepts a many-to-many child"
else
  bad "validate refused a many-to-many child: $vout"
fi
if grep -q "child public.authors through public.book_author" <<< "$vout"; then
  ok "validate names the junction and both of its columns"
else
  bad "validate says nothing about the junction: $vout"
fi

sync_spawn "$TCONFIG"
for _ in $(seq 1 60); do
  refresh
  [ "$(os_count e2e_through)" = "3" ] && break
  sleep 1
done
# the junction carries half the relation, so it has to be published as well
check "the junction joined the publication" \
  "$(pg "SELECT count(*) FROM pg_publication_tables WHERE pubname='${TSLOT}_pub' AND tablename='book_author';")" "1"
check "the load embeds both authors of a book" "$(os_len e2e_through 1 authors)" "2"
check "a book nobody wrote gets an empty array" "$(os_len e2e_through 3 authors)" "0"
check "and has the field at all" "$(os_has e2e_through 3 authors)" "True"
check "the embedded element is the child row, not the junction row" \
  "$(curl -s "$OS/e2e_through/_doc/2" | jqf "d['_source']['authors'][0]['name']")" "ada"

# The junction row is the relation: inserting one is what adds an author, and
# deleting one is what removes them. The junction is left on the default
# replica identity on purpose — book_id is half of its primary key, so a delete
# already carries what names the parent.
check "the junction is on the default replica identity" \
  "$(pg "SELECT relreplident FROM pg_class WHERE relname='book_author';")" "d"
pg "INSERT INTO book_author VALUES (2,9);" > /dev/null
sleep 2; refresh
check "a junction INSERT adds the author to the book" "$(os_len e2e_through 2 authors)" "2"
pg "DELETE FROM book_author WHERE book_id=2 AND author_id=9;" > /dev/null
sleep 2; refresh
check "a junction DELETE takes them away again" "$(os_len e2e_through 2 authors)" "1"

# The other half: a changed CHILD row names no parent at all, so the junction is
# asked which parents it belongs to — and every one of them is rewritten.
pg "UPDATE authors SET name='ada lovelace' WHERE id=7;" > /dev/null
sleep 3; refresh
check "a renamed author reaches their first book" \
  "$(curl -s "$OS/e2e_through/_doc/1" | jqf "sorted(a['name'] for a in d['_source']['authors'])[0]")" \
  "ada lovelace"
check "and their second one" \
  "$(curl -s "$OS/e2e_through/_doc/2" | jqf "d['_source']['authors'][0]['name']")" "ada lovelace"

# One transaction of many junction rows: the parent is read once for the group
# and the collection aggregated once, not once per row.
before_reads=$(pg "SELECT COALESCE(sum(calls),0) FROM pg_stat_statements
                   WHERE query LIKE '%FROM \"public\".\"authors\"%'
                     AND query NOT LIKE '%pg_stat_statements%';" 2>/dev/null || echo 0)
pg "INSERT INTO authors SELECT 100 + g, 'writer ' || g FROM generate_series(1, 40) g;
    INSERT INTO book_author SELECT 3, 100 + g FROM generate_series(1, 40) g;" > /dev/null
sleep 4; refresh
check "one transaction of 40 junction rows lands whole" "$(os_len e2e_through 3 authors)" "40"
after_reads=$(pg "SELECT COALESCE(sum(calls),0) FROM pg_stat_statements
                  WHERE query LIKE '%FROM \"public\".\"authors\"%'
                    AND query NOT LIKE '%pg_stat_statements%';" 2>/dev/null || echo 0)
if [ "$before_reads" = "0" ] && [ "$after_reads" = "0" ]; then
  echo "    (pg_stat_statements unavailable; query count not asserted)"
elif [ "$((after_reads - before_reads))" -lt 20 ]; then
  ok "the collection is resolved per batch, not per junction row ($((after_reads - before_reads)) fetches for 40 rows)"
else
  bad "the collection is still resolved per row ($((after_reads - before_reads)) fetches for 40 rows)"
fi
stop_sync

# max_rows caps a many-to-many collection the way it caps any other, and the
# rows kept are the lowest-keyed ones, so a re-snapshot keeps the same forty.
{ sed 's#^index = "e2e_through"$#index = "e2e_through_capped"#' "$TCONFIG"; echo 'max_rows = 2'; } > "${TCONFIG}.capped"
curl -s -XDELETE "$OS/e2e_through_capped?ignore_unavailable=true" > /dev/null
curl -s -XDELETE "$OS/.pg2osync_meta/_doc/postgres-$TSLOT" > /dev/null
sync_spawn "${TCONFIG}.capped"
for _ in $(seq 1 60); do
  refresh
  [ "$(os_count e2e_through_capped)" = "3" ] && break
  sleep 1
done
check "a capped collection embeds max_rows of them" "$(os_len e2e_through_capped 3 authors)" "2"
check "and says it is not the whole collection" \
  "$(os_field e2e_through_capped 3 authors_truncated)" "True"
check "naming how many there are" "$(os_field e2e_through_capped 3 authors_total)" "40"
check "the rows kept are the lowest-keyed ones" \
  "$(curl -s "$OS/e2e_through_capped/_doc/3" | jqf "[a['id'] for a in d['_source']['authors']]")" \
  "[101, 102]"
check "an uncapped book says nothing extra" \
  "$(os_has e2e_through_capped 1 authors_truncated)" "False"
stop_sync
echo -e "\n\033[1m== 33. require_alias refuses a write that bypasses the alias ==\033[0m"
# A rebuild leaves one step to the operator: point the section at the new name.
# A section left on the raw index keeps writing, keeps its checkpoint moving and
# says nothing while the alias goes stale (#150). require_alias makes that a
# refusal instead of a silence, and validate catches it before the first write.
RACONFIG=$(mktemp /tmp/pg2osync-e2e-require-alias.XXXXXX)
ra_cleanup() {
  sync_kill
  rm -f "$RACONFIG" "${RACONFIG}.raw"
}
trap 'cleanup; resume_cleanup; reject_cleanup; conc_cleanup; rename_cleanup; workers_cleanup; init_cleanup; fan_cleanup; bid_cleanup; shape_cleanup; where_cleanup; join_cleanup; union_cleanup; events_cleanup; pipe_cleanup; append_cleanup; routing_cleanup; reindex_cleanup; rate_cleanup; through_cleanup; ra_cleanup' EXIT

cat > "$RACONFIG" <<TOML
[source]
url_env = "PG2OSYNC_SOURCE_URL"
slot_name = "$XSLOT"
publication = "${XSLOT}_pub"

[target]
url = "$OS"
flavor = "$TARGET_FLAVOR"
require_alias = true

[metrics]
bind = "127.0.0.1:$((PORT_BASE + 34))"

[sync.reindex]
table = "public.reindex_probe"
index = "$XALIAS"
TOML

out=$($BIN validate -c "$RACONFIG" 2>&1 || true)
case "$out" in
  *"require_alias: every configured index is an alias"*)
    ok "validate confirms every configured index is an alias" ;;
  *) bad "validate did not confirm the alias: $out" ;;
esac

sync_spawn "$RACONFIG"
sleep 3
pg "INSERT INTO reindex_probe VALUES (5,'through-the-alias');" > /dev/null
for _ in $(seq 1 60); do
  refresh
  [ "$(os_status "$XALIAS" 5)" = "200" ] && break
  sleep 1
done
check "a write through the alias is accepted with require_alias set" \
  "$(os_field "$XALIAS" 5 v)" "through-the-alias"
stop_sync

# the same configuration one edit away from correct: the raw index the alias
# resolves to, which is exactly what a rebuild leaves an operator holding
sed "s#^index = \"$XALIAS\"\$#index = \"$newer_index\"#" "$RACONFIG" > "${RACONFIG}.raw"
refusal=$($BIN validate -c "${RACONFIG}.raw" 2>&1 || true)
case "$refusal" in
  *"$newer_index"*"not an alias"*)
    ok "validate refuses the raw index a rebuild left behind, naming it" ;;
  *) bad "validate accepted a raw index under require_alias: $refusal" ;;
esac

# started anyway, the target refuses the first write itself, and a name that is
# an index and not an alias never becomes one — so it halts rather than retries
halts_before=$(grep -c "must go through one" "$LOG" || true)
sync_spawn "${RACONFIG}.raw"
sleep 3
pg "INSERT INTO reindex_probe VALUES (6,'past-the-alias');" > /dev/null
for _ in $(seq 1 30); do
  [ "$(grep -c 'must go through one' "$LOG" || true)" -gt "$halts_before" ] && break
  sleep 1
done
if [ "$(grep -c 'must go through one' "$LOG" || true)" -gt "$halts_before" ]; then
  ok "a run started anyway halts on the first write, naming the index and the flag"
else
  bad "a write past the alias was neither refused nor logged"
fi
refresh
check "and the row never reached the index behind the alias" \
  "$(os_status "$newer_index" 6)" "404"
stop_sync

# the section now names the alias, so the one rebuild invocation left needs the
# flag off: a rebuild fills a fresh index and points a second name at it
refusal=$($BIN reindex -c "$RACONFIG" --table public.reindex_probe --alias "$XALIAS" 2>&1 || true)
case "$refusal" in
  *"require_alias is set"*) ok "a rebuild says why require_alias has to come off first" ;;
  *) bad "a rebuild under require_alias did not explain itself: $refusal" ;;
esac

echo -e "\n\033[1m== 34. a keyed pseudonym, so a join survives it ==\033[0m"
# The whole point of AES-SIV over a hash (#143): equal values give equal
# tokens, so a foreign key still finds its parent — provided both sides name
# the same scope, since the default is the column's own schema.table.column.
PCONFIG=$(mktemp /tmp/pg2osync-e2e-pseudonym.XXXXXX)
PSLOT=pg2osync_e2e_pseudonym
PKEY=$(python3 -c "print('ab' * 64)")
drop_idle_probe_slots
cat > "$PCONFIG" <<TOML
[source]
url_env = "PG2OSYNC_SOURCE_URL"
slot_name = "$PSLOT"
publication = "${PSLOT}_pub"

[target]
url = "$OS"
flavor = "$TARGET_FLAVOR"

[metrics]
bind = "127.0.0.1:$((PORT_BASE + 35))"

[sync.people]
table = "public.pseudo_people"
index = "e2e_pseudo_people"

[sync.people.transform]
email = { op = "pseudonym", key_env = "PG2OSYNC_E2E_PSEUDONYM_KEY", scope = "public.pseudo_people.email" }

[sync.orders]
table = "public.pseudo_orders"
index = "e2e_pseudo_orders"

[sync.orders.transform]
owner_email = { op = "pseudonym", key_env = "PG2OSYNC_E2E_PSEUDONYM_KEY", scope = "public.pseudo_people.email" }
TOML
pseudo_cleanup() {
  sync_kill
  pg "SELECT pg_drop_replication_slot('$PSLOT') WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name='$PSLOT');" > /dev/null 2>&1 || true
  pg "DROP PUBLICATION IF EXISTS ${PSLOT}_pub; DROP TABLE IF EXISTS pseudo_people, pseudo_orders;" > /dev/null 2>&1 || true
  curl -s -XDELETE "$OS/e2e_pseudo_people" > /dev/null 2>&1 || true
  curl -s -XDELETE "$OS/e2e_pseudo_orders" > /dev/null 2>&1 || true
  rm -f "$PCONFIG"
}
trap 'cleanup; resume_cleanup; reject_cleanup; conc_cleanup; rename_cleanup; workers_cleanup; init_cleanup; fan_cleanup; bid_cleanup; shape_cleanup; where_cleanup; join_cleanup; union_cleanup; events_cleanup; pipe_cleanup; append_cleanup; routing_cleanup; reindex_cleanup; rate_cleanup; through_cleanup; ra_cleanup; pseudo_cleanup' EXIT

# validate refuses a key that is not there and one that is not a key, and
# neither message may carry the value
unset PG2OSYNC_E2E_PSEUDONYM_KEY
refusal=$($BIN validate -c "$PCONFIG" 2>&1 || true)
case "$refusal" in
  *"PG2OSYNC_E2E_PSEUDONYM_KEY"*missing*) ok "validate refuses a pseudonym whose key variable is unset" ;;
  *) bad "a missing key was accepted: $refusal" ;;
esac
short=$(python3 -c "print('ab' * 32)")
refusal=$(PG2OSYNC_E2E_PSEUDONYM_KEY="$short" $BIN validate -c "$PCONFIG" 2>&1 || true)
case "$refusal" in
  *"128 hex characters"*) ok "validate refuses a 64-character key and says the length wanted" ;;
  *) bad "a short key was accepted: $refusal" ;;
esac
if grep -q "$short" <<< "$refusal"; then
  bad "the error echoes the key material"
else
  ok "the error names no key material"
fi

pg "DROP TABLE IF EXISTS pseudo_people, pseudo_orders;" > /dev/null 2>&1
pg "CREATE TABLE pseudo_people(id bigint primary key, email text); CREATE TABLE pseudo_orders(id bigint primary key, owner_email text);" > /dev/null
pg "INSERT INTO pseudo_people VALUES (1,'alice@example.com'),(2,'alice@example.com'),(3,'bob@example.com');" > /dev/null
pg "INSERT INTO pseudo_orders VALUES (10,'alice@example.com');" > /dev/null
pg "DROP PUBLICATION IF EXISTS ${PSLOT}_pub; CREATE PUBLICATION ${PSLOT}_pub FOR TABLE pseudo_people, pseudo_orders;" > /dev/null 2>&1
curl -s -XDELETE "$OS/e2e_pseudo_people" > /dev/null 2>&1 || true
curl -s -XDELETE "$OS/e2e_pseudo_orders" > /dev/null 2>&1 || true
curl -s -XDELETE "$OS/.pg2osync_meta/_doc/postgres-$PSLOT" > /dev/null

export PG2OSYNC_E2E_PSEUDONYM_KEY="$PKEY"
# captured rather than piped: the slot does not exist until the pipeline below
# creates it, so validate reports the key and then exits non-zero, and under
# `pipefail` that exit would sink a grep that had already matched
accepted=$($BIN validate -c "$PCONFIG" 2>&1 || true)
case "$accepted" in
  *"pseudonym key present (64 bytes) from PG2OSYNC_E2E_PSEUDONYM_KEY"*)
    ok "validate reports the key by the name of its variable" ;;
  *) bad "validate does not report the key: $accepted" ;;
esac

sync_spawn "$PCONFIG"
for _ in $(seq 1 60); do
  refresh
  [ "$(os_count e2e_pseudo_people)" = "3" ] && [ "$(os_count e2e_pseudo_orders)" = "1" ] && break
  sleep 1
done
check "both tables loaded" "$(os_count e2e_pseudo_people)" "3"

one=$(os_field e2e_pseudo_people 1 email)
two=$(os_field e2e_pseudo_people 2 email)
three=$(os_field e2e_pseudo_people 3 email)
order=$(os_field e2e_pseudo_orders 10 owner_email)
check "two rows sharing an address share a token" "$one" "$two"
check "the foreign key joins back to the parent" "$order" "$one"
if [ "$one" = "$three" ]; then
  bad "two different addresses produced the same token"
else
  ok "a different address gives a different token"
fi
case "$one" in
  *@*) bad "the token is the plaintext ($one)" ;;
  "") bad "the token is empty" ;;
  *) ok "the token is not the address ($one)" ;;
esac
# 16-byte synthetic IV plus the 17-byte address, base64url unpadded
check "the token is the documented length" "${#one}" "44"

# a live change goes through the same op, so the stream and the load agree
pg "UPDATE pseudo_people SET email='alice@example.com' WHERE id=3;" > /dev/null
for _ in $(seq 1 30); do
  refresh
  [ "$(os_field e2e_pseudo_people 3 email)" = "$one" ] && break
  sleep 1
done
check "a streamed update produces the token the load did" \
  "$(os_field e2e_pseudo_people 3 email)" "$one"
stop_sync
unset PG2OSYNC_E2E_PSEUDONYM_KEY

echo -e "\n\033[1m== 35. SIGHUP re-reads the configuration without a restart ==\033[0m"
# #145: a reload applies the settings a batch consults each time round, and
# refuses everything else in place. What has to hold: the process is the same
# process afterwards (same pid, no reconnect), a changed batch_size actually
# moves the batch boundary, an identity change is refused by name while the
# stream keeps running, and a file that does not parse leaves everything as it
# was rather than taking the pipeline down with it.
RLCONFIG=$(mktemp /tmp/pg2osync-e2e-reload.XXXXXX)
RLSLOT=pg2osync_e2e_reload
RLLOG=/tmp/pg2osync-e2e-reload-$TAG.log
: > "$RLLOG"
drop_idle_probe_slots
rl_metric() { curl -s 127.0.0.1:$((PORT_BASE + 36))/metrics | grep -v '^#' | grep -E "$1" | awk '{print $2}' | head -1; }
rl_write_config() {
  cat > "$RLCONFIG" <<TOML
[source]
url_env = "PG2OSYNC_SOURCE_URL"
slot_name = "$RLSLOT"
publication = "${RLSLOT}_pub"

[target]
url = "$OS"
flavor = "$TARGET_FLAVOR"

[metrics]
bind = "127.0.0.1:$((PORT_BASE + 36))"

[engine]
batch_size = $1
checkpoint_interval_ms = 200

[sync.reload]
table = "public.reload_probe"
index = "e2e_reload"
$2
TOML
}
rl_cleanup() {
  sync_kill
  pg "SELECT pg_drop_replication_slot('$RLSLOT') WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name='$RLSLOT');" > /dev/null 2>&1 || true
  pg "DROP PUBLICATION IF EXISTS ${RLSLOT}_pub; DROP TABLE IF EXISTS reload_probe;" > /dev/null 2>&1 || true
  curl -s -XDELETE "$OS/e2e_reload" > /dev/null 2>&1 || true
  rm -f "$RLCONFIG" "$RLLOG"
}
trap 'cleanup; resume_cleanup; reject_cleanup; conc_cleanup; rename_cleanup; workers_cleanup; init_cleanup; fan_cleanup; bid_cleanup; shape_cleanup; where_cleanup; join_cleanup; union_cleanup; events_cleanup; pipe_cleanup; append_cleanup; routing_cleanup; reindex_cleanup; rate_cleanup; through_cleanup; ra_cleanup; pseudo_cleanup; rl_cleanup' EXIT

pg "DROP TABLE IF EXISTS reload_probe; CREATE TABLE reload_probe(id bigint primary key, v text);" > /dev/null 2>&1
pg "INSERT INTO reload_probe VALUES (1,'seed');" > /dev/null
curl -s -XDELETE "$OS/e2e_reload" > /dev/null 2>&1
rl_write_config 500 ""

sync_spawn "$RLCONFIG" "$RLLOG"
RLPID=$SYNC_PID
for _ in $(seq 1 60); do
  refresh
  [ "$(os_count e2e_reload)" = "1" ] && break
  sleep 1
done
check "the pipeline is up on the batch_size it started with" "$(os_count e2e_reload)" "1"
batches_before=$(rl_metric '^pg2osync_batches_flushed\{')
reconnects_before=$(rl_metric '^pg2osync_reconnects_total\{')

# batch_size 500 -> 5. One transaction of 200 rows is the proof: no commit
# boundary cuts it, so the only thing that can split it into batches is the
# size the engine is reading now.
rl_write_config 5 ""
kill -HUP "$RLPID"
for _ in $(seq 1 30); do
  grep -q "applied: batch_size=5" "$RLLOG" && break
  sleep 1
done
if grep -q "applied: batch_size=5" "$RLLOG"; then
  ok "SIGHUP applied the new batch_size and said so"
else
  bad "the reload did not apply: $(grep -i reload "$RLLOG" | tail -3)"
fi
check "the process is the same process" "$(kill -0 "$RLPID" 2> /dev/null && echo alive)" "alive"

# The engine copies the settings at the top of a batch turn, so a turn that was
# already waiting when the HUP landed still runs on the old size — one turn of
# lag, the documented cost. A sentinel row spends that turn, so the big
# transaction below is guaranteed to meet the reloaded size.
pg "INSERT INTO reload_probe VALUES (999, 'sentinel');" > /dev/null
for _ in $(seq 1 60); do
  refresh
  [ "$(os_count e2e_reload)" = "2" ] && break
  sleep 1
done
check "the sentinel row went through on the old turn" "$(os_count e2e_reload)" "2"
batches_before=$(rl_metric '^pg2osync_batches_flushed\{')

pg "BEGIN; INSERT INTO reload_probe SELECT g, 'v' || g FROM generate_series(1000, 1199) g; COMMIT;" > /dev/null
for _ in $(seq 1 90); do
  refresh
  [ "$(os_count e2e_reload)" = "202" ] && break
  sleep 1
done
check "every row of the transaction is indexed" "$(os_count e2e_reload)" "202"
batches_after=$(rl_metric '^pg2osync_batches_flushed\{')
grew=$((batches_after - batches_before))
# 200 rows at 5 a batch is around forty requests; at 500 it would have been one
if [ "$grew" -ge 20 ]; then
  ok "the reloaded batch_size cut the transaction into $grew batches, not one"
else
  bad "the batch boundary did not move: $grew batches for 200 rows"
fi
check "and nothing reconnected to do it" "$(rl_metric '^pg2osync_reconnects_total\{')" "$reconnects_before"
check "the reload is counted as applied" "$(rl_metric '^pg2osync_config_reloads_total\{source="[^"]*",result="applied"\}')" "1"

# An identity change is refused in place: the section keeps running as it was,
# because every document already written is filed the old way.
rl_write_config 5 'id = "reload-{id}"'
kill -HUP "$RLPID"
for _ in $(seq 1 30); do
  grep -q "id changed from None" "$RLLOG" && break
  sleep 1
done
if grep -qF '[sync.reload] id changed from None to Some("reload-{id}")' "$RLLOG" \
  && grep -qF "pg2osync reindex --table public.reload_probe" "$RLLOG"; then
  ok "an identity change is refused by name, pointing at the rebuild"
else
  bad "the identity change was not refused clearly: $(grep -i 'id changed' "$RLLOG" | tail -1)"
fi
check "and it is counted as refused" "$(rl_metric '^pg2osync_config_reloads_total\{source="[^"]*",result="refused"\}')" "1"

pg "INSERT INTO reload_probe VALUES (2001,'after-the-refusal');" > /dev/null
for _ in $(seq 1 60); do
  refresh
  [ "$(os_status e2e_reload 2001)" = "200" ] && break
  sleep 1
done
check "the section is still filing rows under the id it was running with" \
  "$(os_field e2e_reload 2001 v)" "after-the-refusal"
check "so the refused template was never used" "$(os_status e2e_reload 'reload-2001')" "404"

# A file that does not parse changes nothing, and above all does not stop the
# process: a config pushed by a deployment tool must not be able to take a
# pipeline down.
printf '[source]\nthis is not toml' > "$RLCONFIG"
kill -HUP "$RLPID"
for _ in $(seq 1 30); do
  grep -q "was not reloaded and nothing changed" "$RLLOG" && break
  sleep 1
done
if grep -q "was not reloaded and nothing changed" "$RLLOG"; then
  ok "a file that does not parse is reported and applied to nothing"
else
  bad "a broken file was not reported: $(grep -i reload "$RLLOG" | tail -3)"
fi
check "the process is still alive" "$(kill -0 "$RLPID" 2> /dev/null && echo alive)" "alive"
check "and the broken read is counted as invalid" \
  "$(rl_metric '^pg2osync_config_reloads_total\{source="[^"]*",result="invalid"\}')" "1"

pg "INSERT INTO reload_probe VALUES (2002,'still-streaming');" > /dev/null
for _ in $(seq 1 60); do
  refresh
  [ "$(os_status e2e_reload 2002)" = "200" ] && break
  sleep 1
done
check "the stream is still advancing after the bad file" \
  "$(os_field e2e_reload 2002 v)" "still-streaming"
stop_sync

echo -e "\n\033[1m== 36. traces are opt-in and cannot break the pipeline ==\033[0m"
# The point of the feature is that it costs nothing when it fails: an endpoint
# nothing is listening on is the worst case, and rows still have to reach the
# index while the export failure is reported once and not once per batch (#152).
drop_idle_probe_slots
OTCONFIG=$(mktemp /tmp/pg2osync-e2e-otel.XXXXXX)
OTSLOT=pg2osync_e2e_otel
otel_cleanup() {
  sync_kill
  pg "SELECT pg_drop_replication_slot('$OTSLOT') WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name='$OTSLOT');" > /dev/null 2>&1 || true
  rm -f "$OTCONFIG"
}
trap 'cleanup; resume_cleanup; reject_cleanup; conc_cleanup; rename_cleanup; workers_cleanup; init_cleanup; fan_cleanup; bid_cleanup; shape_cleanup; where_cleanup; join_cleanup; union_cleanup; events_cleanup; pipe_cleanup; append_cleanup; routing_cleanup; reindex_cleanup; rate_cleanup; through_cleanup; ra_cleanup; pseudo_cleanup; rl_cleanup; otel_cleanup' EXIT

cat > "$OTCONFIG" <<TOML
[source]
url_env = "PG2OSYNC_SOURCE_URL"
slot_name = "$OTSLOT"
publication = "${OTSLOT}_pub"

[target]
url = "$OS"
flavor = "$TARGET_FLAVOR"

[metrics]
bind = "127.0.0.1:$((PORT_BASE + 37))"

[sync.users]
table = "public.users"
index = "e2e_otel"
exclude_columns = ["password_hash"]
TOML

curl -s -XDELETE "$OS/e2e_otel?ignore_unavailable=true" > /dev/null
curl -s -XDELETE "$OS/.pg2osync_meta/_doc/postgres-$OTSLOT" > /dev/null
# nothing listens on 4417; the exporter can only fail, which is the case under test
drops_before=$(grep -c "spans are not reaching the OTLP endpoint" "$LOG" || true)
export PG2OSYNC_OTLP_ENDPOINT=http://127.0.0.1:4417
export PG2OSYNC_OTLP_SAMPLE_RATIO=1.0
export PG2OSYNC_OTLP_SERVICE_NAME=e2e-otel
sync_spawn "$OTCONFIG"
unset PG2OSYNC_OTLP_ENDPOINT PG2OSYNC_OTLP_SAMPLE_RATIO PG2OSYNC_OTLP_SERVICE_NAME
for _ in $(seq 1 60); do
  refresh
  [ "$(os_count e2e_otel)" -gt 0 ] && break
  sleep 1
done
loaded=$(os_count e2e_otel)
if [ "$loaded" -gt 0 ]; then
  ok "the initial load indexed rows with an unreachable collector configured ($loaded)"
else
  bad "an unreachable collector stopped the initial load"
fi
pg "INSERT INTO users (id, email, name) VALUES (9152, 'otel@example.com', 'otel') ON CONFLICT (id) DO UPDATE SET name = 'otel';" > /dev/null
for _ in $(seq 1 30); do
  refresh
  [ "$(os_status e2e_otel 9152)" = "200" ] && break
  sleep 1
done
check "and a streamed row reaches the index too" "$(os_field e2e_otel 9152 name)" "otel"
# the drain flushes the exporter, so by the time the process is gone the failure
# has been reported if it is going to be
stop_sync
dropped=$(($(grep -c "spans are not reaching the OTLP endpoint" "$LOG" || true) - drops_before))
if [ "$dropped" = "1" ]; then
  ok "the export failure is reported exactly once, not once per batch"
else
  bad "the export failure was reported $dropped times, want 1"
fi
if grep -q "replication is unaffected" "$LOG"; then
  ok "and the line says the pipeline is unaffected"
else
  bad "the export failure does not say what it means for replication"
fi

# a bad ratio is a refusal at startup rather than a pipeline that runs on a
# value nobody chose
refusal=$(PG2OSYNC_OTLP_ENDPOINT=http://127.0.0.1:4417 PG2OSYNC_OTLP_SAMPLE_RATIO=7 \
  $BIN validate -c "$OTCONFIG" 2>&1 || true)
case "$refusal" in
  *PG2OSYNC_OTLP_SAMPLE_RATIO*) ok "a sampling ratio outside 0.0-1.0 is refused, naming the variable" ;;
  *) bad "a bad sampling ratio was accepted: $refusal" ;;
esac

echo -e "\n\033[1m== 37. a count from a child table, kept live ==\033[0m"
# An aggregate is one more shape of child (#179): the parent document carries a
# number derived from a child table, and the same machinery keeps it fresh. What
# has to hold is that the load counts correctly including the zero, that a child
# change moves the number without the parent changing, and that a row crossing
# the `where` boundary or moving to another parent is counted where it now
# belongs — and no longer where it was.
AGCONFIG=$(mktemp /tmp/pg2osync-e2e-agg.XXXXXX)
AGSLOT=pg2osync_e2e_agg
AGLOG=/tmp/pg2osync-e2e-agg-$TAG.log
drop_idle_probe_slots
cat > "$AGCONFIG" <<TOML
[source]
url_env = "PG2OSYNC_SOURCE_URL"
slot_name = "$AGSLOT"
publication = "${AGSLOT}_pub"

[target]
url = "$OS"
flavor = "$TARGET_FLAVOR"

[metrics]
bind = "127.0.0.1:$((PORT_BASE + 38))"

[sync.contacts]
table = "public.agg_contact"
index = "e2e_agg"

[[sync.contacts.aggregates]]
field = "open_deals"
table = "public.agg_deal"
foreign_key = "contact_id"
op = "count"
where = "status_type = 1"
TOML
agg_cleanup() {
  sync_kill
  pg "SELECT pg_drop_replication_slot('$AGSLOT') WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name='$AGSLOT');" > /dev/null 2>&1 || true
  pg "DROP PUBLICATION IF EXISTS ${AGSLOT}_pub; DROP TABLE IF EXISTS agg_deal, agg_contact;" > /dev/null 2>&1 || true
  curl -s -XDELETE "$OS/e2e_agg?ignore_unavailable=true" > /dev/null 2>&1 || true
  rm -f "$AGCONFIG" "$AGLOG"
}
trap 'cleanup; resume_cleanup; reject_cleanup; conc_cleanup; rename_cleanup; workers_cleanup; init_cleanup; fan_cleanup; bid_cleanup; shape_cleanup; where_cleanup; join_cleanup; union_cleanup; events_cleanup; pipe_cleanup; append_cleanup; routing_cleanup; reindex_cleanup; rate_cleanup; through_cleanup; ra_cleanup; pseudo_cleanup; rl_cleanup; otel_cleanup; agg_cleanup' EXIT

pg "DROP TABLE IF EXISTS agg_deal, agg_contact;" > /dev/null 2>&1
pg "CREATE TABLE agg_contact(id bigint primary key, name text);" > /dev/null 2>&1
pg "CREATE TABLE agg_deal(id bigint primary key, contact_id bigint, status_type int);" > /dev/null 2>&1
pg "INSERT INTO agg_contact VALUES (1,'acme'),(2,'globex'),(3,'initech');" > /dev/null
# contact 1 has two open deals and one that the filter leaves out, contact 2 one
# closed deal, contact 3 nothing at all
pg "INSERT INTO agg_deal VALUES (1,1,1),(2,1,1),(3,1,2),(4,2,2);" > /dev/null
pg "DROP PUBLICATION IF EXISTS ${AGSLOT}_pub; CREATE PUBLICATION ${AGSLOT}_pub FOR TABLE agg_contact, agg_deal;" > /dev/null 2>&1
curl -s -XDELETE "$OS/e2e_agg?ignore_unavailable=true" > /dev/null
curl -s -XDELETE "$OS/.pg2osync_meta/_doc/postgres-$AGSLOT" > /dev/null

# captured first: a failure exits non-zero, which under pipefail would hide a
# grep that matched
out=$($BIN validate -c "$AGCONFIG" 2>&1 || true)
if grep -q "aggregate open_deals counts public.agg_deal" <<< "$out"; then
  ok "validate runs the aggregate against the table"
else
  bad "validate did not report the aggregate it checked: $out"
fi
# an operation that does not exist is refused where it can still be fixed.
# Captured first: a refusal exits non-zero, which under pipefail would hide a
# grep that matched.
sed 's/^op = "count"$/op = "sum"/' "$AGCONFIG" > "${AGCONFIG}.bad"
out=$($BIN validate -c "${AGCONFIG}.bad" 2>&1 || true)
if grep -q "supported: count" <<< "$out"; then
  ok "validate refuses an operation that does not exist, naming what there is"
else
  bad "validate accepted an unknown aggregate op: $out"
fi
rm -f "${AGCONFIG}.bad"

# the foreign key is not the deal's own key, so a DELETE carries it only under
# FULL: the warning has to name the ALTER before anything goes stale
: > "$AGLOG"
sync_spawn "$AGCONFIG" "$AGLOG"
sleep 3
sync_stop
if grep -q "ALTER TABLE public.agg_deal REPLICA IDENTITY FULL" "$AGLOG"; then
  ok "an aggregated table without FULL is warned about, with the ALTER to run"
else
  bad "nothing warned that a delete could not name the parent to count again"
fi
pg "ALTER TABLE agg_deal REPLICA IDENTITY FULL;" > /dev/null 2>&1

sync_spawn "$AGCONFIG"
for _ in $(seq 1 60); do
  refresh
  [ "$(os_count e2e_agg)" = "3" ] && break
  sleep 1
done
check "the load counts what the filter matches" "$(os_field e2e_agg 1 open_deals)" "2"
check "a parent whose rows the filter leaves out counts none" "$(os_field e2e_agg 2 open_deals)" "0"
check "and a parent with no rows at all still carries the field" \
  "$(os_field e2e_agg 3 open_deals)" "0"

# every assertion below changes only the child table: the parent row is never
# touched, and the number still moves
pg "INSERT INTO agg_deal VALUES (5,2,1);" > /dev/null
for _ in $(seq 1 30); do
  refresh
  [ "$(os_field e2e_agg 2 open_deals)" = "1" ] && break
  sleep 1
done
check "an inserted child row bumps the parent's count" "$(os_field e2e_agg 2 open_deals)" "1"

pg "DELETE FROM agg_deal WHERE id = 1;" > /dev/null
for _ in $(seq 1 30); do
  refresh
  [ "$(os_field e2e_agg 1 open_deals)" = "1" ] && break
  sleep 1
done
check "a deleted child row drops it" "$(os_field e2e_agg 1 open_deals)" "1"

# the row was always there; it only just started matching `where`
pg "UPDATE agg_deal SET status_type = 1 WHERE id = 3;" > /dev/null
for _ in $(seq 1 30); do
  refresh
  [ "$(os_field e2e_agg 1 open_deals)" = "2" ] && break
  sleep 1
done
check "a row crossing into the filter is counted" "$(os_field e2e_agg 1 open_deals)" "2"
pg "UPDATE agg_deal SET status_type = 2 WHERE id = 3;" > /dev/null
for _ in $(seq 1 30); do
  refresh
  [ "$(os_field e2e_agg 1 open_deals)" = "1" ] && break
  sleep 1
done
check "and one crossing back out is not" "$(os_field e2e_agg 1 open_deals)" "1"

# the acceptance test: the parent it left is as wrong as the one it joined
pg "UPDATE agg_deal SET contact_id = 3 WHERE id = 5;" > /dev/null
for _ in $(seq 1 30); do
  refresh
  [ "$(os_field e2e_agg 3 open_deals)" = "1" ] && [ "$(os_field e2e_agg 2 open_deals)" = "0" ] && break
  sleep 1
done
check "a row moved to another parent counts there" "$(os_field e2e_agg 3 open_deals)" "1"
check "and no longer where it was" "$(os_field e2e_agg 2 open_deals)" "0"
stop_sync

echo -e "\n\033[1m== 38. a delimited column fanned out, each element its own join parent ==\033[0m"
# The two lifts of #180 in one shape: fan_out cuts a text column on `by`, and
# `parent = "{element}"` files every element document under the parent the
# element names. The selling point is the removal path — a member dropped from
# the list loses its document without a rebuild — so that is what is asserted.
drop_idle_probe_slots
ECONFIG=$(mktemp /tmp/pg2osync-e2e-element.XXXXXX)
EMAPPING=$(dirname "$ECONFIG")/pg2osync-e2e-element-mapping-$TAG.json
ESLOT=pg2osync_e2e_element
cat > "$ECONFIG" <<TOML
[source]
url_env = "PG2OSYNC_SOURCE_URL"
slot_name = "$ESLOT"
publication = "${ESLOT}_pub"

[target]
url = "$OS"
flavor = "$TARGET_FLAVOR"

[metrics]
bind = "127.0.0.1:$((PORT_BASE + 39))"

[sync.enote]
table = "public.enotes"
index = "e2e_element"
id = "note-{id}"
mapping_file = "pg2osync-e2e-element-mapping-$TAG.json"

[sync.enote.join]
field = "relation"
name = "note"

[sync.elink]
table = "public.elinks"
index = "e2e_element"
id = "link-{id}"

[sync.elink.fan_out]
field = "member_ids"
by = ","
id = "link-{id}-{member_ids}"

[sync.elink.join]
field = "relation"
name = "member"
parent = "{element}"
TOML
# Three shards, for the reason section 24 gives: on one shard every document
# is reachable unrouted, and a routing assertion would prove nothing.
cat > "$EMAPPING" <<'JSON'
{"settings":{"number_of_shards":3},"mappings":{"properties":{"relation":{"type":"join","relations":{"note":["member"]}}}}}
JSON
element_cleanup() {
  sync_kill
  pg "SELECT pg_drop_replication_slot('$ESLOT') WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name='$ESLOT');" > /dev/null 2>&1 || true
  pg "DROP PUBLICATION IF EXISTS ${ESLOT}_pub; DROP TABLE IF EXISTS elinks, enotes;" > /dev/null 2>&1 || true
  rm -f "$ECONFIG" "$EMAPPING"
}
trap 'cleanup; resume_cleanup; reject_cleanup; conc_cleanup; rename_cleanup; workers_cleanup; init_cleanup; fan_cleanup; bid_cleanup; shape_cleanup; where_cleanup; join_cleanup; union_cleanup; events_cleanup; pipe_cleanup; append_cleanup; routing_cleanup; reindex_cleanup; rate_cleanup; through_cleanup; ra_cleanup; pseudo_cleanup; rl_cleanup; otel_cleanup; agg_cleanup; element_cleanup' EXIT

pg "DROP TABLE IF EXISTS elinks, enotes;" > /dev/null 2>&1
pg "CREATE TABLE enotes(id bigint primary key, title text);" > /dev/null
pg "CREATE TABLE elinks(id bigint primary key, member_ids text);" > /dev/null
# the diff that removes a dropped member comes from the old row
pg "ALTER TABLE elinks REPLICA IDENTITY FULL;" > /dev/null 2>&1
pg "INSERT INTO enotes VALUES (1,'first'),(2,'second'),(3,'third');" > /dev/null
# spaces on purpose: fan-out trims each piece, as the split transform does
pg "INSERT INTO elinks VALUES (9,'1, 2');" > /dev/null
pg "DROP PUBLICATION IF EXISTS ${ESLOT}_pub; CREATE PUBLICATION ${ESLOT}_pub FOR TABLE enotes, elinks;" > /dev/null 2>&1
curl -s -XDELETE "$OS/e2e_element?ignore_unavailable=true" > /dev/null
curl -s -XDELETE "$OS/.pg2osync_meta/_doc/postgres-$ESLOT" > /dev/null
# the element parent is the only shape fan_out and join combine in: without
# the fan_out block the same file has to be refused
sed '/^\[sync\.elink\.fan_out\]/,/^$/d' "$ECONFIG" > "${ECONFIG}.bad"
if $BIN validate -c "${ECONFIG}.bad" > /dev/null 2>&1; then
  bad "validate accepted parent = \"{element}\" on a section that does not fan out"
else
  ok "validate refuses an element parent without fan_out"
fi
rm -f "${ECONFIG}.bad"
if $BIN validate -c "$ECONFIG" 2>&1 | grep -q "all checks passed"; then
  ok "validate accepts a fanned section whose elements are the parents"
else
  bad "validate refused the element-parent pair"
fi
sync_spawn "$ECONFIG"
for _ in $(seq 1 60); do
  refresh
  [ "$(os_count e2e_element)" = "5" ] && break
  sleep 1
done
check "the load writes three notes and one document per member" "$(os_count e2e_element)" "5"
check "an element document is found under the note it names" "$(os_rstatus e2e_element link-9-1 note-1)" "200"
check "and so is the second one, on the other note's shard" "$(os_rstatus e2e_element link-9-2 note-2)" "200"
check "the element is the trimmed piece, under the fan-out field" \
  "$(os_routed e2e_element link-9-2 note-2 member_ids)" "2"
check "and it names its own parent" \
  "$(curl -s "$OS/e2e_element/_doc/link-9-2?routing=note-2" | jqf "json.dumps(d['_source']['relation'], sort_keys=True)")" \
  '{"name": "member", "parent": "note-2"}'
check "has_child finds exactly the notes a member document points at" \
  "$(curl -s "$OS/e2e_element/_search" -H 'Content-Type: application/json' \
      -d '{"query":{"has_child":{"type":"member","query":{"match_all":{}}}}}' | jqf "d['hits']['total']['value']")" "2"

# the acceptance test for the feature: a member leaves the list and its
# document goes with it, on the shard it was on, while the rest stay
pg "UPDATE elinks SET member_ids='1,3' WHERE id=9;" > /dev/null
for _ in $(seq 1 30); do
  refresh
  [ "$(os_rstatus e2e_element link-9-3 note-3)" = "200" ] && break
  sleep 1
done
check "a member added to the list gains a document" "$(os_rstatus e2e_element link-9-3 note-3)" "200"
synced 2> /dev/null || { sleep 2; refresh; }
check "the member dropped from the list loses its document" "$(os_rstatus e2e_element link-9-2 note-2)" "404"
check "and the member that stayed is untouched" "$(os_rstatus e2e_element link-9-1 note-1)" "200"
refresh
check "one document per member, and no orphan" "$(os_count e2e_element)" "5"

pg "DELETE FROM elinks WHERE id=9;" > /dev/null
for _ in $(seq 1 30); do
  refresh
  [ "$(os_count e2e_element)" = "3" ] && break
  sleep 1
done
check "a deleted row takes every element document with it" "$(os_count e2e_element)" "3"
stop_sync

echo -e "\n\033[1m== 39. a one-to-one child flattened onto the parent document ==\033[0m"
# #181: the same watching and re-fetching a `single` child already has, with
# the element lifted onto the parent instead of nested. What has to hold is
# that the lifted field is live: a change to the child alone moves it, and a
# parent whose child row is gone carries none of it.
drop_idle_probe_slots
FLCONFIG=$(mktemp /tmp/pg2osync-e2e-flatten.XXXXXX)
FLSLOT=pg2osync_e2e_flatten
cat > "$FLCONFIG" <<TOML
[source]
url_env = "PG2OSYNC_SOURCE_URL"
slot_name = "$FLSLOT"
publication = "${FLSLOT}_pub"

[target]
url = "$OS"
flavor = "$TARGET_FLAVOR"

[metrics]
bind = "127.0.0.1:$((PORT_BASE + 40))"

[sync.contacts]
table = "public.flat_contact"
index = "e2e_flat"

[[sync.contacts.children]]
table = "public.flat_company"
field = "company"
foreign_key = "contact_id"
single = true
flatten = true
columns = ["customer_name"]

[sync.contacts.children.fields]
customer_name = "company_name"
TOML
flatten_cleanup() {
  sync_kill
  pg "SELECT pg_drop_replication_slot('$FLSLOT') WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name='$FLSLOT');" > /dev/null 2>&1 || true
  pg "DROP PUBLICATION IF EXISTS ${FLSLOT}_pub; DROP TABLE IF EXISTS flat_company, flat_contact;" > /dev/null 2>&1 || true
  curl -s -XDELETE "$OS/e2e_flat?ignore_unavailable=true" > /dev/null 2>&1 || true
  rm -f "$FLCONFIG"
}
trap 'cleanup; resume_cleanup; reject_cleanup; conc_cleanup; rename_cleanup; workers_cleanup; init_cleanup; fan_cleanup; bid_cleanup; shape_cleanup; where_cleanup; join_cleanup; union_cleanup; events_cleanup; pipe_cleanup; append_cleanup; routing_cleanup; reindex_cleanup; rate_cleanup; through_cleanup; ra_cleanup; pseudo_cleanup; rl_cleanup; otel_cleanup; agg_cleanup; element_cleanup; flatten_cleanup' EXIT

pg "DROP TABLE IF EXISTS flat_company, flat_contact;" > /dev/null 2>&1
pg "CREATE TABLE flat_contact(id bigint primary key, name text);" > /dev/null
pg "CREATE TABLE flat_company(id bigint primary key, contact_id bigint, customer_name text);" > /dev/null
# the foreign key is not the company's own key, so a DELETE names the parent
# only under FULL
pg "ALTER TABLE flat_company REPLICA IDENTITY FULL;" > /dev/null 2>&1
pg "INSERT INTO flat_contact VALUES (1,'ada'),(2,'grace');" > /dev/null
# contact 2 has no company at all: nothing is lifted onto it
pg "INSERT INTO flat_company VALUES (100,1,'acme');" > /dev/null
pg "DROP PUBLICATION IF EXISTS ${FLSLOT}_pub; CREATE PUBLICATION ${FLSLOT}_pub FOR TABLE flat_contact, flat_company;" > /dev/null 2>&1
curl -s -XDELETE "$OS/e2e_flat?ignore_unavailable=true" > /dev/null
curl -s -XDELETE "$OS/.pg2osync_meta/_doc/postgres-$FLSLOT" > /dev/null

# captured first: a refusal exits non-zero, which under pipefail would hide a
# grep that matched
sed '/^single = true$/d' "$FLCONFIG" > "${FLCONFIG}.bad"
out=$($BIN validate -c "${FLCONFIG}.bad" 2>&1 || true)
if grep -q "flatten needs single = true" <<< "$out"; then
  ok "validate refuses flatten on a relation nothing declares one-to-one"
else
  bad "validate accepted flatten without single: $out"
fi
rm -f "${FLCONFIG}.bad"
if $BIN validate -c "$FLCONFIG" 2>&1 | grep -q "all checks passed"; then
  ok "validate accepts the flattened child"
else
  bad "validate refused the flattened child"
fi

sync_spawn "$FLCONFIG"
for _ in $(seq 1 60); do
  refresh
  [ "$(os_count e2e_flat)" = "2" ] && break
  sleep 1
done
check "the load lifts the child column under its new name" \
  "$(os_field e2e_flat 1 company_name)" "acme"
check "the source name of the lifted column is gone" "$(os_has e2e_flat 1 customer_name)" "False"
check "and so is the field the child would have nested under" \
  "$(os_has e2e_flat 1 company)" "False"
check "a parent with no child row carries none of it" \
  "$(os_has e2e_flat 2 company_name)" "False"

# the acceptance test: the parent row is never touched, and the flat field moves
pg "UPDATE flat_company SET customer_name = 'acme-renamed' WHERE id = 100;" > /dev/null
for _ in $(seq 1 30); do
  refresh
  [ "$(os_field e2e_flat 1 company_name)" = "acme-renamed" ] && break
  sleep 1
done
check "a change to the child alone moves the parent's flat field" \
  "$(os_field e2e_flat 1 company_name)" "acme-renamed"

pg "INSERT INTO flat_company VALUES (101,2,'globex');" > /dev/null
for _ in $(seq 1 30); do
  refresh
  [ "$(os_has e2e_flat 2 company_name)" = "True" ] && break
  sleep 1
done
check "a child row appearing lifts onto the parent that had none" \
  "$(os_field e2e_flat 2 company_name)" "globex"

pg "DELETE FROM flat_company WHERE id = 100;" > /dev/null
for _ in $(seq 1 30); do
  refresh
  [ "$(os_has e2e_flat 1 company_name)" = "False" ] && break
  sleep 1
done
check "and the deleted child row takes the lifted field with it" \
  "$(os_has e2e_flat 1 company_name)" "False"
check "the parent document is still there" "$(os_field e2e_flat 1 name)" "ada"
stop_sync

printf "\n\033[1mRESULT: %d passed, %d failed\033[0m\n" "$PASS" "$FAIL"
[ "$FAIL" = "0" ]
