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
os_len()     { curl -s "$OS/$1/_doc/$2" | jqf "len(d.get('_source',{}).get('$3',[]))"; }
pg()         { docker exec "$PG_CONTAINER" psql -U postgres -d sourcedb -qtAc "$1"; }
refresh()    { curl -s -XPOST "$OS/_refresh" > /dev/null; }
synced()     { curl -s "http://127.0.0.1:9131/synced?refresh=true&timeout=10000" > /dev/null; refresh; }

start_sync() {
  nohup $BIN run -c "$CONFIG" >> "$LOG" 2>&1 < /dev/null & disown
}
stop_sync() { pkill -f "pg2osync run" 2> /dev/null || true; }
drop_own_slot() { pg "SELECT pg_drop_replication_slot('pg2osync_e2e') WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name='pg2osync_e2e');" > /dev/null 2>&1 || true; }
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

[sync.customers]
table = "public.customers"
index = "e2e_customers"

[[sync.customers.children]]
table = "public.orders"
field = "orders"
foreign_key = "customer_id"

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
curl -s -XDELETE "$OS/e2e_users,e2e_customers,.pg2osync_meta" > /dev/null
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
check "transform applied on backfill" "$(os_field e2e_users 1 email)" "***"
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
check "transform applied on update" "$(os_field e2e_users 4 email)" "***"

# a wide, incompressible value is stored out of line, so an update that does
# not touch it sends a marker instead of the value and the engine has to
# complete the document from the one already indexed
pg "UPDATE users SET metadata = (SELECT jsonb_build_object('blob', string_agg(md5(random()::text), ''))
                                 FROM generate_series(1, 400)) WHERE id = 4;" > /dev/null
sleep 2; refresh
toast_len=$(curl -s "$OS/e2e_users/_doc/4" | jqf "len(d['_source']['metadata']['blob'])")
pg "UPDATE users SET name = 'dave-toast' WHERE id = 4;" > /dev/null
sleep 2; refresh
check "an update that leaves a TOASTed column alone keeps its value" \
  "$(curl -s "$OS/e2e_users/_doc/4" | jqf "len(d['_source']['metadata']['blob'])")" "$toast_len"
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

printf "\n\033[1mRESULT: %d passed, %d failed\033[0m\n" "$PASS" "$FAIL"
[ "$FAIL" = "0" ]
