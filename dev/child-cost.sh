#!/usr/bin/env bash
# What a change to a nested child collection costs.
#
# A child row does not become a document: the parent it belongs to is re-read and
# re-emitted with its whole array. The question this measures is how often that
# read happens — once per changed row, or once per batch — and the case that
# hurts is a wide fan-out: one parent with thousands of children, all touched in
# one transaction.
#
# Prerequisites:
#   docker compose -f dev/docker-compose.yml up -d
#   cargo build --release
#
# Usage: [PARENTS=50] [CHILDREN=200] ./dev/child-cost.sh
set -euo pipefail

cd "$(dirname "$0")/.."
BIN=./target/release/pg2osync
OS=${OS_URL:-http://localhost:9200}
PG_CONTAINER=${PG_CONTAINER:-dev-postgres-1}
PARENTS=${PARENTS:-50}
CHILDREN=${CHILDREN:-200}
CONFIG=$(mktemp /tmp/pg2osync-child.XXXXXX)
LOG=/tmp/pg2osync-child.log
export PG2OSYNC_SOURCE_URL="postgres://postgres:postgres@localhost:15432/sourcedb"

pg()      { docker exec "$PG_CONTAINER" psql -U postgres -d sourcedb -qtAc "$1"; }
refresh() { curl -s -XPOST "$OS/child_bench/_refresh" > /dev/null; }
count()   { curl -s "$OS/child_bench/_count" | python3 -c "import sys,json;print(json.load(sys.stdin).get('count',0))"; }
metric()  { curl -s http://127.0.0.1:9116/metrics | awk -v k="$1" '$1 == k {print $2}'; }
now()     { python3 -c "import time;print(time.time())"; }

# Queries the source actually received, which is the number this is about.
# pg_stat_statements normalises literals, so one entry covers every execution.
child_queries() {
  pg "SELECT COALESCE(sum(calls), 0) FROM pg_stat_statements
      WHERE query LIKE '%child_bench_items%' AND query NOT LIKE '%pg_stat_statements%';" 2>/dev/null || echo 0
}
parent_queries() {
  pg "SELECT COALESCE(sum(calls), 0) FROM pg_stat_statements
      WHERE query LIKE '%child_bench%' AND query NOT LIKE '%child_bench_items%'
        AND query NOT LIKE '%pg_stat_statements%';" 2>/dev/null || echo 0
}

stop_sync()  { pkill -f "pg2osync run" 2> /dev/null || true; }
drop_slot()  { pg "SELECT pg_drop_replication_slot('pg2osync_child') WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name='pg2osync_child');" > /dev/null 2>&1 || true; }
cleanup()    { stop_sync; drop_slot; rm -f "$CONFIG"; }
trap cleanup EXIT

cat > "$CONFIG" <<'TOML'
[source]
url_env = "PG2OSYNC_SOURCE_URL"
slot_name = "pg2osync_child"
publication = "pg2osync_child_pub"

[target]
url = "http://localhost:9200"

[metrics]
bind = "127.0.0.1:9116"

[sync.child_bench]
table = "public.child_bench"
index = "child_bench"
primary_key = "id"

[[sync.child_bench.children]]
table = "public.child_bench_items"
field = "items"
foreign_key = "parent_id"
TOML

if [ "$(pg "SELECT count(*) FROM pg_extension WHERE extname = 'pg_stat_statements';")" != "1" ]; then
  echo "pg_stat_statements is not installed; query counts will read 0."
  echo "  shared_preload_libraries = 'pg_stat_statements' in postgresql.conf, then"
  echo "  CREATE EXTENSION pg_stat_statements;"
fi

stop_sync
drop_slot
pg "DROP PUBLICATION IF EXISTS pg2osync_child_pub;" > /dev/null
pg "DROP TABLE IF EXISTS child_bench_items; DROP TABLE IF EXISTS child_bench;" > /dev/null
pg "CREATE TABLE child_bench (id bigint PRIMARY KEY, name text NOT NULL);" > /dev/null
pg "CREATE TABLE child_bench_items (
      id bigint PRIMARY KEY,
      parent_id bigint NOT NULL,
      qty int NOT NULL,
      note text NOT NULL);" > /dev/null
# without this index every lookup scans the whole child table, which is a
# different problem and would drown the one being measured
pg "CREATE INDEX ON child_bench_items (parent_id);" > /dev/null
pg "INSERT INTO child_bench SELECT g, 'parent-' || g FROM generate_series(1, $PARENTS) g;" > /dev/null
pg "INSERT INTO child_bench_items
      SELECT (p - 1) * $CHILDREN + c, p, 0, 'item'
      FROM generate_series(1, $PARENTS) p, generate_series(1, $CHILDREN) c;" > /dev/null
curl -s -XDELETE "$OS/child_bench,.pg2osync_meta" > /dev/null

nohup $BIN run -c "$CONFIG" &> "$LOG" < /dev/null & disown
while :; do refresh; [ "$(count)" -ge "$PARENTS" ] && break; sleep 1; done
echo "== $PARENTS parents x $CHILDREN children each, loaded =="

pg "SELECT pg_stat_statements_reset();" > /dev/null 2>&1 || true
rows_before=$(metric 'pg2osync_events_total{type="row"}')
start=$(now)
# one transaction touching every child row: the fan-out case
pg "UPDATE child_bench_items SET qty = qty + 1;" > /dev/null
total=$((PARENTS * CHILDREN))
# Done when no document still holds a child at the old value: every parent's
# whole array has to have landed, not just the first one's, or the numbers below
# describe a run that was still in flight.
stale() {
  curl -s "$OS/child_bench/_count" -H 'Content-Type: application/json' \
    -d '{"query":{"term":{"items.qty":0}}}' \
    | python3 -c "import sys,json;print(json.load(sys.stdin).get('count',-1))"
}
while :; do
  refresh
  [ "$(stale)" = "0" ] && break
  sleep 0.5
done
elapsed=$(python3 -c "print($(now) - $start)")

python3 -c "
print(f'   {$total} child rows in one transaction -> {$elapsed:.1f}s, {$total/$elapsed:,.0f} rows/s')
print(f'   parent re-reads : {$(parent_queries)}')
print(f'   child fetches   : {$(child_queries)}')
print(f'   documents emitted: {$(metric 'pg2osync_events_total{type=\"row\"}') - $rows_before}')
print()
print(f'   one query per changed row would be {$total} of each; one per batch is')
print(f'   bounded by the number of distinct parents, which is {$PARENTS}.')
"
stop_sync
pg "DROP TABLE IF EXISTS child_bench_items; DROP TABLE IF EXISTS child_bench;" > /dev/null
pg "DROP PUBLICATION IF EXISTS pg2osync_child_pub;" > /dev/null
curl -s -XDELETE "$OS/child_bench" > /dev/null
