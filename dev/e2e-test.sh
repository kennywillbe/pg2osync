#!/usr/bin/env bash
# End-to-end test suite for the PostgreSQL -> OpenSearch pipeline.
#
# Runs one at a time: stopping the pipeline kills every pg2osync process, so
# two suites at once take each other's down and report failures that are not.
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
# BSD mktemp only substitutes X's at the *end* of the template: with a
# suffix after them it creates the literal name instead, and one killed run
# then breaks every later one with "File exists".
CONFIG=$(mktemp /tmp/pg2osync-e2e.XXXXXX)
MAPPING=$(dirname "$CONFIG")/pg2osync-e2e-mapping.json
LOG=/tmp/pg2osync-e2e.log
export PG2OSYNC_SOURCE_URL="postgres://postgres:postgres@localhost:15432/sourcedb"
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
synced()     { curl -s "http://127.0.0.1:9131/synced?refresh=true&timeout=10000" > /dev/null; refresh; }

start_sync() {
  nohup $BIN run -c "$CONFIG" >> "$LOG" 2>&1 < /dev/null & disown
}
stop_sync() { pkill -f "pg2osync run" 2> /dev/null || true; }
drop_own_slot() { pg "SELECT pg_drop_replication_slot('pg2osync_e2e') WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name='pg2osync_e2e');" > /dev/null 2>&1 || true; }
# Every probe section keeps its slot until the trap at exit, and the dev
# database allows ten; a late section would otherwise start with "all
# replication slots are in use". Only idle slots go — a running pipeline's is
# in use and stays.
drop_idle_probe_slots() { pg "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name LIKE 'pg2osync\\_e2e\\_%' AND NOT active;" > /dev/null 2>&1 || true; }
cleanup()   { stop_sync; drop_own_slot; rm -f "$CONFIG" "$MAPPING"; }
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

[api]
enabled = true
bind = "127.0.0.1:9131"

[sync.users]
table = "public.users"
index = "e2e_users"
exclude_columns = ["password_hash"]
mapping_file = "pg2osync-e2e-mapping.json"

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

[sync.customers.children.fields]
total = "amount"

[[sync.customers.children]]
table = "public.tickets"
field = "tickets"
foreign_key = "customer_id"
TOML

say "0. Reset state"
stop_sync
pg "DROP PUBLICATION IF EXISTS pg2osync_e2e_pub;" > /dev/null
pg "SELECT pg_drop_replication_slot('pg2osync_e2e') WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name='pg2osync_e2e');" > /dev/null
pg "TRUNCATE users; TRUNCATE tickets, orders, customers;" > /dev/null
pg "INSERT INTO users (id,name,email,password_hash,metadata) VALUES
      (1,'alice','alice@test.io','secret-1','{\"role\":\"admin\"}'),
      (2,'bob','bob@test.io','secret-2','{\"role\":\"user\"}'),
      (3,'carol','carol@test.io','secret-3','{}');" > /dev/null
pg "INSERT INTO customers (id,name) VALUES (1,'acme'),(2,'globex'),(3,'no-children');" > /dev/null
pg "INSERT INTO orders (id,customer_id,total) VALUES (10,1,99.90),(11,1,5.00),(12,2,42.00);" > /dev/null
pg "INSERT INTO tickets (id,customer_id,subject) VALUES (20,1,'late delivery');" > /dev/null
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
check "a constant is added to every document" "$(os_field e2e_users 1 entity)" "user"
check "the origin placeholder is rendered" "$(os_field e2e_users 1 origin)" "public.users"
check "children attached during backfill" "$(os_len e2e_customers 1 orders)" "2"
# a second collection exercises the multi-join path of the initial load
check "second collection attached too" "$(os_len e2e_customers 1 tickets)" "1"
# a parent with no children must get an empty array, never null
check "childless parent gets an empty array" "$(os_len e2e_customers 3 orders)" "0"
check "childless parent has the field at all" "$(os_has e2e_customers 3 orders)" "True"

say "4. live streaming"
pg "INSERT INTO users (id,name,email,password_hash) VALUES (4,'dave','dave@test.io','secret-4');" > /dev/null
sleep 2; refresh
check "INSERT propagated" "$(os_count e2e_users)" "4"
check "excluded column still absent" "$(os_has e2e_users 4 password_hash)" "False"

pg "UPDATE users SET name='dave-renamed', email='new@test.io' WHERE id=4;" > /dev/null
sleep 2; refresh
check "UPDATE propagated" "$(os_field e2e_users 4 name)" "dave-renamed"
check "transform applied on update" "$(os_field e2e_users 4 contact)" "***"

# a wide, incompressible value is stored out of line, so an update that does
# not touch it sends a marker instead of the value and the engine has to
# complete the document from the one already indexed
pg "UPDATE users SET metadata = (SELECT jsonb_build_object('blob', string_agg(md5(random()::text), ''))
                                 FROM generate_series(1, 400)) WHERE id = 4;" > /dev/null
sleep 2; refresh
toast_len=$(curl -s "$OS/e2e_users/_doc/4" | jqf "len(d['_source']['meta']['blob'])")
pg "UPDATE users SET name = 'dave-toast' WHERE id = 4;" > /dev/null
sleep 2; refresh
check "an update that leaves a TOASTed column alone keeps its value" \
  "$(curl -s "$OS/e2e_users/_doc/4" | jqf "len(d['_source']['meta']['blob'])")" "$toast_len"
check "the rest of that update still applied" "$(os_field e2e_users 4 name)" "dave-toast"

pg "DELETE FROM users WHERE id=3;" > /dev/null
sleep 2; refresh
check "DELETE propagated" "$(os_status e2e_users 3)" "404"

say "5. changing a primary key moves the document"
pg "UPDATE users SET id = 40 WHERE id = 4;" > /dev/null
sleep 2; refresh
check "row lives at its new id" "$(os_field e2e_users 40 name)" "dave-toast"
# the old document must not survive: nothing would ever collect it
check "old document removed" "$(os_status e2e_users 4)" "404"
pg "DELETE FROM users WHERE id = 40;" > /dev/null
sleep 2; refresh
check "deleting the moved row leaves nothing" "$(os_status e2e_users 40)" "404"

say "6. nested children stay fresh"
pg "INSERT INTO orders (id,customer_id,total) VALUES (13,2,7.50);" > /dev/null
sleep 2; refresh
check "child INSERT refreshes parent" "$(os_len e2e_customers 2 orders)" "2"
pg "DELETE FROM orders WHERE id=13;" > /dev/null
sleep 2; refresh
check "child DELETE refreshes parent" "$(os_len e2e_customers 2 orders)" "1"

# Many children of one parent in one transaction: the parent is re-read once for
# the group, not once per row, and the array that lands is the whole collection.
before_reads=$(pg "SELECT COALESCE(sum(calls),0) FROM pg_stat_statements
                   WHERE query LIKE '%FROM \"public\".\"orders\"%'
                     AND query NOT LIKE '%pg_stat_statements%';" 2>/dev/null || echo 0)
pg "INSERT INTO orders (id,customer_id,total)
      SELECT 1000 + g, 2, g FROM generate_series(1, 40) g;" > /dev/null
sleep 3; refresh
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
sleep 3; refresh
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
metrics=$(curl -s http://127.0.0.1:9111/metrics)
if grep -q "pg2osync_position_confirmed" <<< "$metrics" && grep -q "pg2osync_events_total" <<< "$metrics"; then
  ok "metrics expose position and event counters"
else
  bad "metrics missing expected series"
fi
# Retained WAL is the number that takes the source down, and nothing else here
# reports it: position_lag stays kilobytes while a slot can pin gigabytes. The
# poller runs on its own interval, so this waits for the first sample.
for _ in $(seq 1 40); do
  metrics=$(curl -s http://127.0.0.1:9111/metrics)
  grep -q "pg2osync_slot_retained_bytes{slot=\"pg2osync_e2e\"}" <<< "$metrics" && break
  sleep 1
done
if grep -q "pg2osync_slot_retained_bytes{slot=\"pg2osync_e2e\"}" <<< "$metrics"; then
  ok "the configured slot's retained WAL is reported"
else
  bad "no retained-WAL series for the configured slot"
fi
if grep -q "pg2osync_slot_wal_status{slot=\"pg2osync_e2e\",status=\"lost\"} 0" <<< "$metrics"; then
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
check "health answers for probes" "$(curl -s http://127.0.0.1:9111/healthz)" "ok"
check "an unknown path is not the exposition" \
  "$(curl -s -o /dev/null -w '%{http_code}' http://127.0.0.1:9111/)" "404"

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
before_pid=$(pgrep -f "pg2osync run" | head -1)
pg "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE backend_type='walsender';" > /dev/null
pg "INSERT INTO users (id,name,email) VALUES (9,'written-while-disconnected','w@test.io');" > /dev/null
sleep 8; refresh
# the same process must still be running: recovery happens in process, not by
# a supervisor restarting us
check "same process recovered" "$(pgrep -f "pg2osync run" | head -1)" "$before_pid"
check "row written while disconnected arrived" "$(os_field e2e_users 9 name)" "written-while-disconnected"
metrics=$(curl -s http://127.0.0.1:9111/metrics)
reconnects=$(awk '$1 == "pg2osync_reconnects_total" {print $2}' <<< "$metrics")
if [ "${reconnects:-0}" -ge 1 ]; then ok "reconnects_total counted it ($reconnects)"; else bad "reconnects_total still zero"; fi
check "source reports connected again" "$(awk '$1 == "pg2osync_source_connected" {print $2}' <<< "$metrics")" "1"

say "11. read-your-writes"
pg "INSERT INTO users (id,name,email) VALUES (11,'ryw','r@test.io');" > /dev/null
# no position, no sleep, no retry: the endpoint returns only once the write is
# searchable, so a single query afterwards must find it
synced=$(curl -s "http://127.0.0.1:9131/synced?refresh=true&timeout=8000")
found=$(curl -s "$OS/e2e_users/_search" -H 'Content-Type: application/json' \
  -d '{"query":{"term":{"id":11}}}' | jqf "d['hits']['total']['value']")
check "the row is searchable the moment /synced returns" "$found" "1"
check "and it says so" "$(jqf "d['synced']" <<< "$synced")" "True"
waited=$(jqf "d['waited_ms']" <<< "$synced")
ok "waited ${waited}ms"
# a position nothing will ever reach must time out rather than hang
code=$(curl -s -o /dev/null -w "%{http_code}" "http://127.0.0.1:9131/synced?position=FFFF%2FFFFFFFFF&timeout=300")
check "an unreachable position times out" "$code" "408"

say "12. crash recovery"
pkill -9 -f "pg2osync run"; sleep 1
pg "INSERT INTO users (id,name,email) VALUES (8,'eve-during-downtime','eve@test.io');" > /dev/null
start_sync
sleep 6; refresh
check "row written while down is recovered" "$(os_field e2e_users 8 name)" "eve-during-downtime"

say "13. final consistency"
check "row counts match" "$(pg "SELECT count(*) FROM users;")" "$(os_count e2e_users)"

say "14. status and teardown"
$BIN status -c "$CONFIG" | sed 's/^/    /'
stop_sync; sleep 1
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
url = "http://localhost:9200"

[metrics]
bind = "127.0.0.1:9112"

[sync.big]
table = "public.resume_probe"
index = "e2e_resume"
TOML
resume_cleanup() {
  pkill -9 -f "pg2osync run" 2> /dev/null || true
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
nohup $BIN run -c "$RCONFIG" >> "$LOG" 2>&1 < /dev/null & disown
for _ in $(seq 1 120); do
  [ "$(progress_done)" -ge 2 ] 2> /dev/null && break
  sleep 0.5
done
done_at_kill=$(progress_done)
pkill -9 -f "pg2osync run"; sleep 1
if [ "$done_at_kill" -ge 2 ]; then ok "progress recorded per range ($done_at_kill done)"; else bad "no per-range progress recorded (got '$done_at_kill')"; fi

# the interesting part: the source moves while nothing is watching it, so the
# replay argument the chunked load rests on is what has to repair the result
pg "DELETE FROM resume_probe WHERE id IN (5, 100000);" > /dev/null
pg "INSERT INTO resume_probe VALUES (400001,'added-while-down');" > /dev/null
pg "UPDATE resume_probe SET v='updated-while-down' WHERE id = 77;" > /dev/null
src_rows=$(pg "SELECT count(*) FROM resume_probe;")

nohup $BIN run -c "$RCONFIG" >> "$LOG" 2>&1 < /dev/null & disown
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

[metrics]
bind = "127.0.0.1:9115"

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
  pkill -9 -f "pg2osync run" 2> /dev/null || true
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
nohup $BIN run -c "$HCONFIG" >> "$LOG" 2>&1 < /dev/null & disown
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
nohup $BIN run -c "$QCONFIG" >> "$LOG" 2>&1 < /dev/null & disown
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
if curl -s http://127.0.0.1:9115/metrics | grep -qE "^pg2osync_rejected_total [1-9]"; then
  ok "pg2osync_rejected_total reports it"
else
  bad "pg2osync_rejected_total did not move"
fi
if $BIN rejects -c "$QCONFIG" 2>&1 | grep -q "e2e_reject/2 at .*mapper_parsing_exception"; then
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
nohup $BIN run -c "$RCONFIG" >> "$LOG" 2>&1 < /dev/null & disown
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
    -e "s#^bind = .*#bind = \"127.0.0.1:9118\"#" \
    -e "s/^\[target\]/[engine]\nwrite_concurrency = 4\n\n[target]/" \
    "$RCONFIG" > "$WCONFIG"
conc_cleanup() {
  pkill -9 -f "pg2osync run" 2> /dev/null || true
  pg "SELECT pg_drop_replication_slot('$WSLOT') WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name='$WSLOT');" > /dev/null 2>&1 || true
  pg "DROP PUBLICATION IF EXISTS ${WSLOT}_pub;" > /dev/null 2>&1 || true
  rm -f "$WCONFIG"
}
trap 'cleanup; resume_cleanup; reject_cleanup; conc_cleanup' EXIT
pg "DROP PUBLICATION IF EXISTS ${WSLOT}_pub; CREATE PUBLICATION ${WSLOT}_pub FOR TABLE resume_probe;" > /dev/null 2>&1
curl -s -XDELETE "$OS/e2e_conc" > /dev/null
src_rows=$(pg "SELECT count(*) FROM resume_probe;")
nohup $BIN run -c "$WCONFIG" >> "$LOG" 2>&1 < /dev/null & disown
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
nohup $BIN run -c "$WCONFIG" >> "$LOG" 2>&1 < /dev/null & disown
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
    -e "s#^bind = .*#bind = \"127.0.0.1:9119\"#" \
    "$RCONFIG" > "$RNCONFIG"
rename_cleanup() {
  pkill -9 -f "pg2osync run" 2> /dev/null || true
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
nohup $BIN run -c "$RNCONFIG" >> "$LOG" 2>&1 < /dev/null & disown
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
if pgrep -f "pg2osync run" > /dev/null; then
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
LOG18=/tmp/pg2osync-e2e-workers.log
: > "$LOG18"
sed -e "s/^slot_name = .*/slot_name = \"$PWSLOT\"/" \
    -e "s/^publication = .*/publication = \"${PWSLOT}_pub\"/" \
    -e "s/^index = .*/index = \"e2e_workers\"/" \
    -e "s#^bind = .*#bind = \"127.0.0.1:9120\"#" \
    -e "s/^\[source\]/[source]\nload_workers = 4\nload_chunk_rows = 2000/" \
    "$RCONFIG" > "$PWCONFIG"
workers_cleanup() {
  pkill -9 -f "pg2osync run" 2> /dev/null || true
  pg "SELECT pg_drop_replication_slot('$PWSLOT') WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name='$PWSLOT');" > /dev/null 2>&1 || true
  pg "DROP PUBLICATION IF EXISTS ${PWSLOT}_pub;" > /dev/null 2>&1 || true
  rm -f "$PWCONFIG"
}
trap 'cleanup; resume_cleanup; reject_cleanup; conc_cleanup; rename_cleanup; workers_cleanup' EXIT
pg "DROP PUBLICATION IF EXISTS ${PWSLOT}_pub; CREATE PUBLICATION ${PWSLOT}_pub FOR TABLE resume_probe;" > /dev/null 2>&1
curl -s -XDELETE "$OS/e2e_workers?ignore_unavailable=true" > /dev/null
PWPROG=load-postgres-$PWSLOT-public_resume_probe
curl -s -XDELETE "$OS/.pg2osync_meta/_doc/$PWPROG" > /dev/null
src_rows=$(pg "SELECT count(*) FROM resume_probe;")
pw_done() { curl -s "$OS/.pg2osync_meta/_doc/$PWPROG" | jqf "(d.get('_source') or {}).get('done', -1)"; }
nohup $BIN run -c "$PWCONFIG" > "$LOG18" 2>&1 < /dev/null & disown
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
pkill -9 -f "pg2osync run"; sleep 1
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
nohup $BIN run -c "$PWCONFIG" >> "$LOG18" 2>&1 < /dev/null & disown
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
url = "http://localhost:9200"

[metrics]
bind = "127.0.0.1:9121"

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
  pkill -9 -f "pg2osync run" 2> /dev/null || true
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
nohup $BIN run -c "$FCONFIG" >> "$LOG" 2>&1 < /dev/null & disown
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
url = "http://localhost:9200"

[metrics]
bind = "127.0.0.1:9122"

[sync.bid]
table = "public.bid_probe"
index = "e2e_bid"
id = "{tenant}-u{id}"
TOML
bid_cleanup() {
  pkill -9 -f "pg2osync run" 2> /dev/null || true
  pg "SELECT pg_drop_replication_slot('$BSLOT') WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name='$BSLOT');" > /dev/null 2>&1 || true
  pg "DROP PUBLICATION IF EXISTS ${BSLOT}_pub; DROP TABLE IF EXISTS bid_probe;" > /dev/null 2>&1 || true
  rm -f "$BCONFIG"
}
trap 'cleanup; resume_cleanup; reject_cleanup; conc_cleanup; rename_cleanup; workers_cleanup; init_cleanup; fan_cleanup; bid_cleanup' EXIT

pg "DROP TABLE IF EXISTS bid_probe; CREATE TABLE bid_probe(id bigint primary key, tenant text);" > /dev/null 2>&1
# without the old row the pipeline could not find the document an id change
# moves out of, so the tool refuses to start rather than strand it
pg "INSERT INTO bid_probe VALUES (1,'acme');" > /dev/null
pg "DROP PUBLICATION IF EXISTS ${BSLOT}_pub; CREATE PUBLICATION ${BSLOT}_pub FOR TABLE bid_probe;" > /dev/null 2>&1
curl -s -XDELETE "$OS/e2e_bid" > /dev/null
if $BIN run -c "$BCONFIG" > /tmp/pg2osync-e2e-bid.log 2>&1 & then
  sleep 3
  pkill -f "pg2osync run" 2>/dev/null || true
  wait 2> /dev/null || true
fi
if grep -q "REPLICA IDENTITY FULL" /tmp/pg2osync-e2e-bid.log; then
  ok "a non-key id on a non-FULL table is refused with the ALTER to run"
else
  bad "the pipeline started despite an id it cannot delete against"
fi
pg "ALTER TABLE bid_probe REPLICA IDENTITY FULL;" > /dev/null 2>&1
nohup $BIN run -c "$BCONFIG" >> "$LOG" 2>&1 < /dev/null & disown
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
# Six named ops, no expression language (#63). Row 1 converts on every column;
# row 2 converts on none of them and has to land exactly as it arrived, counted,
# rather than halt the pipeline or be nulled.
SCONFIG=$(mktemp /tmp/pg2osync-e2e-shape.XXXXXX)
SMAPPING=$(dirname "$SCONFIG")/pg2osync-e2e-shape-mapping.json
SSLOT=pg2osync_e2e_shape
cat > "$SCONFIG" <<TOML
[source]
url_env = "PG2OSYNC_SOURCE_URL"
slot_name = "$SSLOT"
publication = "${SSLOT}_pub"

[target]
url = "http://localhost:9200"

[metrics]
bind = "127.0.0.1:9123"

[sync.shape]
table = "public.shape_probe"
index = "e2e_shape"
mapping_file = "pg2osync-e2e-shape-mapping.json"

[sync.shape.transform]
payload = "json"
price = "number"
tags = { op = "split", by = "," }
born = { op = "date", from = "%d/%m/%Y" }
TOML
# Row 2 keeps `price` and `born` as the strings they arrived as, so under dynamic
# mapping the second document would be a mapping rejection — the quarantine
# path, not the policy under test — hence the fields are typed text up front.
cat > "$SMAPPING" <<'JSON'
{ "mappings": { "properties": { "price": { "type": "text" }, "born": { "type": "text" }, "tags": { "type": "keyword" } } } }
JSON
shape_cleanup() {
  pkill -9 -f "pg2osync run" 2> /dev/null || true
  pg "SELECT pg_drop_replication_slot('$SSLOT') WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name='$SSLOT');" > /dev/null 2>&1 || true
  pg "DROP PUBLICATION IF EXISTS ${SSLOT}_pub; DROP TABLE IF EXISTS shape_probe;" > /dev/null 2>&1 || true
  rm -f "$SCONFIG" "$SMAPPING"
}
trap 'cleanup; resume_cleanup; reject_cleanup; conc_cleanup; rename_cleanup; workers_cleanup; init_cleanup; fan_cleanup; bid_cleanup; shape_cleanup' EXIT

pg "DROP TABLE IF EXISTS shape_probe; CREATE TABLE shape_probe(id bigint primary key, tags text, price text, born text, payload text);" > /dev/null 2>&1
pg "INSERT INTO shape_probe VALUES (1,'a, b ,c','19.99','01/03/2024','{\"k\":1}'), (2,'x','abc','not-a-date','{\"k\":2}');" > /dev/null
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
nohup $BIN run -c "$SCONFIG" >> "$LOG" 2>&1 < /dev/null & disown
for _ in $(seq 1 60); do
  refresh
  [ "$(os_count e2e_shape)" = "2" ] && break
  sleep 1
done
check "a delimited string became an array" "$(os_field e2e_shape 1 tags)" "['a', 'b', 'c']"
check "a numeric string became a number" "$(curl -s "$OS/e2e_shape/_doc/1" | jqf "type(d['_source']['price']).__name__")" "float"
check "a formatted date became ISO 8601" "$(os_field e2e_shape 1 born)" "2024-03-01"
check "a JSON string became an object" "$(curl -s "$OS/e2e_shape/_doc/1" | jqf "type(d['_source']['payload']).__name__")" "dict"
check "an unconvertible value is indexed as it was" "$(os_field e2e_shape 2 price)" "abc"
check "and so is an unparseable date" "$(os_field e2e_shape 2 born)" "not-a-date"
# -ge rather than =: at-least-once delivery may hand row 2 over more than once,
# and every pass counts what it left alone again
left=$(curl -s 127.0.0.1:9123/metrics | awk '/^pg2osync_transform_unconverted_total /{print $2}')
if [ "${left:-0}" -ge 2 ]; then
  ok "the counter reports the values left as they were ($left)"
else
  bad "the counter reports ${left:-0} values left as they were, want at least 2"
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
url = "http://localhost:9200"

[metrics]
bind = "127.0.0.1:9124"

[sync.where_probe]
table = "public.where_probe"
index = "e2e_where"
where = "status = 'active' AND price > 10 AND deleted_at IS NULL"
TOML
where_cleanup() {
  pkill -9 -f "pg2osync run" 2> /dev/null || true
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
nohup $BIN run -c "$RFCONFIG" >> "$LOG" 2>&1 < /dev/null & disown
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
JMAPPING=$(dirname "$JCONFIG")/pg2osync-e2e-join-mapping.json
JSLOT=pg2osync_e2e_join
drop_idle_probe_slots
cat > "$JCONFIG" <<TOML
[source]
url_env = "PG2OSYNC_SOURCE_URL"
slot_name = "$JSLOT"
publication = "${JSLOT}_pub"

[target]
url = "http://localhost:9200"

[metrics]
bind = "127.0.0.1:9125"

[sync.jcust]
table = "public.jcust"
index = "e2e_shop"
id = "customer-{id}"
mapping_file = "pg2osync-e2e-join-mapping.json"

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
  pkill -9 -f "pg2osync run" 2> /dev/null || true
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
nohup $BIN run -c "$JCONFIG" >> "$LOG" 2>&1 < /dev/null & disown
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
if curl -s 127.0.0.1:9125/metrics | grep -qE '^pg2osync_events_total\{type="join_cascade"\} [1-9]'; then
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
nohup $BIN run -c "$JCONFIG" >> "$LOG" 2>&1 < /dev/null & disown
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
url = "http://localhost:9200"

[metrics]
bind = "127.0.0.1:9126"

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
  pkill -9 -f "pg2osync run" 2> /dev/null || true
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
nohup $BIN run -c "$UCONFIG" >> "$LOG" 2>&1 < /dev/null & disown
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
check "the skip is counted" "$(curl -s 127.0.0.1:9126/metrics | awk '/^pg2osync_events_total\{type="truncate_skipped"\} /{print $2}')" "1"
stop_sync

echo -e "\n\033[1m== 26. a row chooses the index it lands in ==\033[0m"
# `index` with a placeholder is the id problem again (#69): the column can
# change, and the document is then in the old index. So everything section
# 21 holds for a derived id has to hold here — the before-image is required,
# a change moves the document, an unusable name halts — plus what is new:
# the index is created the first time a row needs it, with the configured
# mapping, and nothing that pages one index can run against the table.
ECONFIG=$(mktemp /tmp/pg2osync-e2e-events.XXXXXX)
EMAPPING=$(dirname "$ECONFIG")/pg2osync-e2e-events-mapping.json
ESLOT=pg2osync_e2e_events
drop_idle_probe_slots
cat > "$ECONFIG" <<TOML
[source]
url_env = "PG2OSYNC_SOURCE_URL"
slot_name = "$ESLOT"
publication = "${ESLOT}_pub"

[target]
url = "http://localhost:9200"

[metrics]
bind = "127.0.0.1:9127"

[sync.events]
table = "public.events_probe"
index = "e2e-events-{tenant}"
mapping_file = "pg2osync-e2e-events-mapping.json"
TOML
cat > "$EMAPPING" <<'JSON'
{"mappings":{"properties":{"tenant":{"type":"keyword"}}}}
JSON
events_cleanup() {
  pkill -9 -f "pg2osync run" 2> /dev/null || true
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
nohup $BIN run -c "$ECONFIG" >> "$LOG" 2>&1 < /dev/null & disown
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
nohup $BIN run -c "$ECONFIG" >> "$LOG" 2>&1 < /dev/null & disown
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
url = "http://localhost:9200"

[metrics]
bind = "127.0.0.1:9128"

[sync.pipe]
table = "public.pipe_probe"
index = "e2e_pipe"
pipeline = "e2e-tag"
TOML
pipe_cleanup() {
  pkill -9 -f "pg2osync run" 2> /dev/null || true
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
nohup $BIN run -c "$PCONFIG" >> "$LOG" 2>&1 < /dev/null & disown
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
url = "http://localhost:9200"

[metrics]
bind = "127.0.0.1:9129"

[sync.events_log]
table = "public.events_log"
index = "e2e_append"
append_only = true
TOML
append_cleanup() {
  pkill -9 -f "pg2osync run" 2> /dev/null || true
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
nohup $BIN run -c "$ACONFIG" >> "$LOG" 2>&1 < /dev/null & disown
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
nohup $BIN run -c "$ACONFIG" >> "$LOG" 2>&1 < /dev/null & disown
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

printf "\n\033[1mRESULT: %d passed, %d failed\033[0m\n" "$PASS" "$FAIL"
[ "$FAIL" = "0" ]
