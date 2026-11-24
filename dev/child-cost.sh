#!/usr/bin/env bash
# What a change to a nested child collection costs.
#
# A child row does not become a document: the parent it belongs to is re-read and
# re-emitted with its whole array. The question this measures is how often that
# read happens — once per changed row, or once per batch — and the case that
# hurts is a wide fan-out: one parent with thousands of children, all touched in
# one transaction.
#
# A second level makes it one step longer: a changed grandchild row names the
# child row it sits in, which is looked up to find the parent. The second phase
# touches every grandchild in one transaction, and the walk-up has to stay one
# query per batch as well, not one per row.
#
# Prerequisites:
#   docker compose -f dev/docker-compose.yml up -d
#   cargo build --release
#
# Usage: [PARENTS=50] [CHILDREN=200] [GRANDCHILDREN=5] ./dev/child-cost.sh
#   OS_URL        target base URL                (default http://localhost:9200)
#   PG_URL        source URL                     (default the dev stack's)
#   PG_CONTAINER  container psql runs in         (default dev-postgres-1)
#   METRICS_PORT  the pipeline's metrics port    (default 9116)
#   GRANDCHILDREN 0 skips the second level
set -euo pipefail

cd "$(dirname "$0")/.."
BIN=./target/release/pg2osync
OS=${OS_URL:-http://localhost:9200}
PG_CONTAINER=${PG_CONTAINER:-dev-postgres-1}
PARENTS=${PARENTS:-50}
CHILDREN=${CHILDREN:-200}
GRANDCHILDREN=${GRANDCHILDREN:-5}
METRICS_PORT=${METRICS_PORT:-9116}
CONFIG=$(mktemp /tmp/pg2osync-child.XXXXXX)
LOG=/tmp/pg2osync-child.log
SLOT=pg2osync_child
SYNC_PID=
export PG2OSYNC_SOURCE_URL=${PG_URL:-postgres://postgres:postgres@localhost:15432/sourcedb}

pg()      { docker exec "$PG_CONTAINER" psql -U postgres -d sourcedb -qtAc "$1"; }
refresh() { curl -s -XPOST "$OS/child_bench/_refresh" > /dev/null; }
count()   { curl -s "$OS/child_bench/_count" | python3 -c "import sys,json;print(json.load(sys.stdin).get('count',0))"; }
# every row event the pipeline counted, whatever other labels the series carries
rows_seen() {
  curl -s "http://127.0.0.1:$METRICS_PORT/metrics" \
    | awk '/^pg2osync_events_total\{/ && /type="row"/ {s += $2} END {print s + 0}'
}
now()     { python3 -c "import time;print(time.time())"; }

# Queries the source actually received, which is the number this is about.
# pg_stat_statements normalises literals, so one entry covers every execution.
calls() {
  pg "SELECT COALESCE(sum(calls), 0) FROM pg_stat_statements
      WHERE $1 AND query NOT LIKE '%pg_stat_statements%';" 2>/dev/null || echo 0
}
child_queries()   { calls "query LIKE '%child_bench_items%' AND query NOT LIKE 'SELECT DISTINCT%'"; }
grand_queries()   { calls "query LIKE '%child_bench_opts%'"; }
walk_up_queries() { calls "query LIKE 'SELECT DISTINCT%child_bench_items%'"; }
parent_queries() {
  calls "query LIKE '%child_bench%' AND query NOT LIKE '%child_bench_items%'
         AND query NOT LIKE '%child_bench_opts%'"
}

# Only the pipeline this script started: a pattern would also stop every other
# run on the machine.
stop_sync() {
  if [ -n "$SYNC_PID" ] && kill -0 "$SYNC_PID" 2> /dev/null; then
    kill "$SYNC_PID" 2> /dev/null || true
    wait "$SYNC_PID" 2> /dev/null || true
  fi
  SYNC_PID=
}
drop_slot()   { pg "SELECT pg_drop_replication_slot('$SLOT') WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name='$SLOT');" > /dev/null 2>&1 || true; }
drop_tables() { pg "DROP TABLE IF EXISTS child_bench_opts, child_bench_items, child_bench;" > /dev/null; }
# This stream's checkpoint only: the meta index holds the checkpoint of every
# other pipeline sharing the target.
drop_state() {
  curl -s -XDELETE "$OS/child_bench?ignore_unavailable=true" > /dev/null
  curl -s -XDELETE "$OS/.pg2osync_meta/_doc/postgres-$SLOT" > /dev/null
}
cleanup()     { stop_sync; drop_slot; rm -f "$CONFIG"; }
trap cleanup EXIT

cat > "$CONFIG" <<TOML
[source]
url_env = "PG2OSYNC_SOURCE_URL"
slot_name = "$SLOT"
publication = "${SLOT}_pub"

[target]
url = "$OS"

[metrics]
bind = "127.0.0.1:$METRICS_PORT"

[sync.child_bench]
table = "public.child_bench"
index = "child_bench"
primary_key = "id"

[[sync.child_bench.children]]
table = "public.child_bench_items"
field = "items"
foreign_key = "parent_id"
TOML
if [ "$GRANDCHILDREN" -gt 0 ]; then
  cat >> "$CONFIG" <<'TOML'

[[sync.child_bench.children.children]]
table = "public.child_bench_opts"
field = "opts"
foreign_key = "item_id"
TOML
fi

if [ "$(pg "SELECT count(*) FROM pg_extension WHERE extname = 'pg_stat_statements';")" != "1" ]; then
  echo "pg_stat_statements is not installed; query counts will read 0."
  echo "  shared_preload_libraries = 'pg_stat_statements' in postgresql.conf, then"
  echo "  CREATE EXTENSION pg_stat_statements;"
fi

drop_slot
pg "DROP PUBLICATION IF EXISTS ${SLOT}_pub;" > /dev/null
drop_tables
pg "CREATE TABLE child_bench (id bigint PRIMARY KEY, name text NOT NULL);" > /dev/null
pg "CREATE TABLE child_bench_items (
      id bigint PRIMARY KEY,
      parent_id bigint NOT NULL,
      qty int NOT NULL,
      note text NOT NULL);" > /dev/null
pg "CREATE TABLE child_bench_opts (
      id bigint PRIMARY KEY,
      item_id bigint NOT NULL,
      qty int NOT NULL);" > /dev/null
pg "ALTER TABLE child_bench_items REPLICA IDENTITY FULL;
    ALTER TABLE child_bench_opts REPLICA IDENTITY FULL;" > /dev/null
# without these indexes every lookup scans the whole child table, which is a
# different problem and would drown the one being measured
pg "CREATE INDEX ON child_bench_items (parent_id);
    CREATE INDEX ON child_bench_opts (item_id);" > /dev/null
pg "INSERT INTO child_bench SELECT g, 'parent-' || g FROM generate_series(1, $PARENTS) g;" > /dev/null
pg "INSERT INTO child_bench_items
      SELECT (p - 1) * $CHILDREN + c, p, 0, 'item'
      FROM generate_series(1, $PARENTS) p, generate_series(1, $CHILDREN) c;" > /dev/null
if [ "$GRANDCHILDREN" -gt 0 ]; then
  pg "INSERT INTO child_bench_opts
        SELECT (i - 1) * $GRANDCHILDREN + o, i, 0
        FROM generate_series(1, $((PARENTS * CHILDREN))) i, generate_series(1, $GRANDCHILDREN) o;" > /dev/null
fi
drop_state

nohup $BIN run -c "$CONFIG" &> "$LOG" < /dev/null &
SYNC_PID=$!
while :; do
  kill -0 "$SYNC_PID" 2> /dev/null || { echo "the pipeline exited; see $LOG"; exit 1; }
  refresh; [ "$(count)" -ge "$PARENTS" ] && break; sleep 1
done
echo "== $PARENTS parents x $CHILDREN children x $GRANDCHILDREN grandchildren each, loaded =="

# Done when no document still holds a row at the old value: every parent's whole
# array has to have landed, not just the first one's, or the numbers below
# describe a run that was still in flight.
stale() {
  curl -s "$OS/child_bench/_count" -H 'Content-Type: application/json' \
    -d "{\"query\":{\"term\":{\"$1\":0}}}" \
    | python3 -c "import sys,json;print(json.load(sys.stdin).get('count',-1))"
}
# one transaction touching every row of one level, timed until it has landed
touch_all() {
  local table=$1 field=$2 rows_before start
  pg "SELECT pg_stat_statements_reset();" > /dev/null 2>&1 || true
  rows_before=$(rows_seen)
  start=$(now)
  pg "UPDATE $table SET qty = qty + 1;" > /dev/null
  while :; do
    refresh
    [ "$(stale "$field")" = "0" ] && break
    sleep 0.5
  done
  elapsed=$(python3 -c "print($(now) - $start)")
  emitted=$(( $(rows_seen) - rows_before ))
}

total=$((PARENTS * CHILDREN))
touch_all child_bench_items items.qty
python3 -c "
print(f'   {$total} child rows in one transaction -> {$elapsed:.1f}s, {$total/$elapsed:,.0f} rows/s')
print(f'   parent re-reads : {$(parent_queries)}')
print(f'   child fetches   : {$(child_queries)}')
print(f'   documents emitted: {$emitted}')
print()
print(f'   one query per changed row would be {$total} of each; one per batch is')
print(f'   bounded by the number of distinct parents, which is {$PARENTS}.')
"

if [ "$GRANDCHILDREN" -gt 0 ]; then
  grand_total=$((total * GRANDCHILDREN))
  touch_all child_bench_opts items.opts.qty
  python3 -c "
print()
print(f'   {$grand_total} grandchild rows in one transaction -> {$elapsed:.1f}s, {$grand_total/$elapsed:,.0f} rows/s')
print(f'   walk-ups to the parent: {$(walk_up_queries)}')
print(f'   parent re-reads       : {$(parent_queries)}')
print(f'   grandchild fetches    : {$(grand_queries)}')
print(f'   documents emitted     : {$emitted}')
print()
print(f'   one walk-up per changed row would be {$grand_total}; one per batch is')
print(f'   bounded by the batches the transaction is resolved in, not by its rows.')
"
fi
stop_sync
drop_tables
pg "DROP PUBLICATION IF EXISTS ${SLOT}_pub;" > /dev/null
drop_state
