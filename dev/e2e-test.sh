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
trap 'cleanup; resume_cleanup; conc_cleanup' EXIT
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

printf "\n\033[1mRESULT: %d passed, %d failed\033[0m\n" "$PASS" "$FAIL"
[ "$FAIL" = "0" ]
