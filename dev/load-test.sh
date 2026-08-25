#!/usr/bin/env bash
# Where the pipeline stops keeping up, and what happens past that point.
#
# Everything else we measure runs against a quiet database: a bulk load, then
# single-row commits with nothing else happening. Under that shape the bounded
# channels never fill, so backpressure — the mechanism the design leans on
# hardest — never engages. This drives concurrent writers instead.
#
# Prerequisites:
#   docker compose -f dev/docker-compose.yml up -d
#   cargo build --release
#
# Usage: [CLIENTS=8] [STEP_SECONDS=20] [RATES="200 1000 5000 10000"] ./dev/load-test.sh
set -euo pipefail

cd "$(dirname "$0")/.."
BIN=./target/release/pg2osync
OS=${OS_URL:-http://localhost:9200}
PG_CONTAINER=${PG_CONTAINER:-dev-postgres-1}
OS_CONTAINER=${OS_CONTAINER:-dev-opensearch-1}
CLIENTS=${CLIENTS:-8}
STEP_SECONDS=${STEP_SECONDS:-20}
RATES=${RATES:-"200 1000 5000 10000"}
# BSD mktemp only substitutes X's at the *end* of the template: with a
# suffix after them it creates the literal name instead, and one killed run
# then breaks every later one with "File exists".
CONFIG=$(mktemp /tmp/pg2osync-load.XXXXXX)
LOG=/tmp/pg2osync-load.log
export PG2OSYNC_SOURCE_URL="postgres://postgres:postgres@localhost:15432/sourcedb"

pg()      { docker exec "$PG_CONTAINER" psql -U postgres -d sourcedb -qtAc "$1"; }
# a series with no samples yet is absent from the exposition, and every caller
# here wants to do arithmetic on the answer
metric()  { local v; v=$(curl -s http://127.0.0.1:9115/metrics | awk -v k="$1" '$1 == k {print $2}'); echo "${v:-0}"; }
count()   { curl -s "$OS/load_test/_count" | python3 -c "import sys,json;print(json.load(sys.stdin).get('count',0))"; }
refresh() { curl -s -XPOST "$OS/load_test/_refresh" > /dev/null; }
now()     { python3 -c "import time;print(time.time())"; }
rss()     { ps -o rss= -p "$(pgrep -f 'pg2osync run' | head -1)" 2>/dev/null | awk '{printf "%.0f", $1/1024}'; }
slot_wal() { pg "SELECT pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)) FROM pg_replication_slots WHERE slot_name='pg2osync_load_test';"; }

# Wait until the pipeline has caught up. A freshly started process serves no
# samples at all, and an absent series reads as zero, so waiting on the lag
# alone would return immediately and measure nothing.
wait_for_drain() {
  until curl -sf http://127.0.0.1:9115/metrics > /dev/null; do sleep 1; done
  until [ "$(metric pg2osync_position_current)" != "0" ]; do sleep 1; done
  until [ "$(metric pg2osync_position_lag)" = "0" ]; do sleep 1; done
}

stop_sync() { pkill -f "pg2osync run" 2> /dev/null || true; }
drop_own_slot() { pg "SELECT pg_drop_replication_slot('pg2osync_load_test') WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name='pg2osync_load_test');" > /dev/null 2>&1 || true; }
unpause() { docker unpause "$OS_CONTAINER" > /dev/null 2>&1 || true; }
cleanup() { stop_sync; unpause; drop_own_slot; rm -f "$CONFIG" /tmp/pg2osync-load-*.sql; }
trap cleanup EXIT

cat > "$CONFIG" <<'TOML'
[source]
url_env = "PG2OSYNC_SOURCE_URL"
slot_name = "pg2osync_load_test"
publication = "pg2osync_load_test_pub"

[target]
url = "http://localhost:9200"

[metrics]
bind = "127.0.0.1:9115"

[sync.load_test]
table = "public.load_test"
index = "load_test"
TOML

# pgbench scripts: same row rate, different transaction shapes
cat > /tmp/pg2osync-load-small.sql <<'SQL'
\set id random(1, 1000000000)
INSERT INTO load_test (id, payload, n) VALUES (:id, 'p', 1)
  ON CONFLICT (id) DO UPDATE SET n = load_test.n + 1;
SQL
cat > /tmp/pg2osync-load-big.sql <<'SQL'
\set base random(1, 1000000000)
BEGIN;
INSERT INTO load_test (id, payload, n)
  SELECT :base + g, 'p', 1 FROM generate_series(1, 100) g
  ON CONFLICT (id) DO UPDATE SET n = load_test.n + 1;
COMMIT;
SQL

bench() { # rate script seconds  -> runs pgbench, prints nothing
  local rate=$1 script=$2 secs=$3
  docker exec -i "$PG_CONTAINER" pgbench -U postgres -d sourcedb \
    -c "$CLIENTS" -j "$CLIENTS" -T "$secs" -R "$rate" -f "/tmp/$script" -n \
    > /dev/null 2>&1 || true
}

echo "== 0. prepare =="
stop_sync
drop_own_slot
pg "DROP PUBLICATION IF EXISTS pg2osync_load_test_pub;" > /dev/null
pg "DROP TABLE IF EXISTS load_test;" > /dev/null
pg "CREATE TABLE load_test (id bigint PRIMARY KEY, payload text NOT NULL, n int NOT NULL);" > /dev/null
curl -s -XDELETE "$OS/load_test,.pg2osync_meta?ignore_unavailable=true" > /dev/null
docker cp /tmp/pg2osync-load-small.sql "$PG_CONTAINER":/tmp/pg2osync-load-small.sql > /dev/null
docker cp /tmp/pg2osync-load-big.sql "$PG_CONTAINER":/tmp/pg2osync-load-big.sql > /dev/null
nohup $BIN run -c "$CONFIG" &> "$LOG" < /dev/null & disown
until curl -sf http://127.0.0.1:9115/metrics > /dev/null; do sleep 1; done
echo "   $CLIENTS writers, ${STEP_SECONDS}s per step"

echo
echo "== 1. where lag stops returning to zero =="
# the lag metric counts WAL bytes between received and checkpointed, not rows
printf "   %-9s %-14s %-18s %-8s\n" rate/s "rows applied" "wal lag after 5s" rss
for rate in $RATES; do
  before=$(metric 'pg2osync_events_total{type="row"}')
  start=$(now)
  bench "$rate" pg2osync-load-small.sql "$STEP_SECONDS"
  applied=$(python3 -c "print(int($(metric 'pg2osync_events_total{type=\"row\"}') - $before))")
  # before the settle sleep: including it would deflate the rate it reports
  elapsed=$(python3 -c "print($(now) - $start)")
  sleep 5
  printf "   %-9s %-14s %-18s %-8s\n" \
    "$rate" \
    "$(python3 -c "print(f'{$applied/$elapsed:,.0f}/s')")" \
    "$(metric pg2osync_position_lag)" \
    "$(rss) MB"
done

echo
echo "== 2. same rate, few large transactions instead of many small =="
for script in small big; do
  before=$(metric 'pg2osync_events_total{type="row"}')
  start=$(now)
  bench 2000 "pg2osync-load-$script.sql" "$STEP_SECONDS"
  applied=$(python3 -c "print(int($(metric 'pg2osync_events_total{type=\"row\"}') - $before))")
  python3 -c "print(f'   {\"$script\":<6} {$applied:,} rows in {$(now)-$start:.0f}s -> {$applied/($(now)-$start):,.0f} rows/s')"
done

echo
echo "== 3. target unavailable: does anything grow without bound =="
docker pause "$OS_CONTAINER" > /dev/null
bench 2000 pg2osync-load-small.sql "$STEP_SECONDS"
echo "   while paused: rss=$(rss) MB, slot retains $(slot_wal)"
unpause
start=$(now)
wait_for_drain
python3 -c "print(f'   caught up {$(now)-$start:.0f}s after the target came back')"

echo
echo "== 4. kill at load, restart, check nothing is lost =="
bench 2000 pg2osync-load-small.sql 5 &
sleep 3
pkill -9 -f "pg2osync run" || true
wait || true
sleep 1
rows_before=$(pg "SELECT count(*) FROM load_test;")
nohup $BIN run -c "$CONFIG" &> "$LOG" < /dev/null & disown
start=$(now)
wait_for_drain
# the source row count is the answer; a document count that never reaches it
# is the loss this step exists to detect
for _ in $(seq 1 120); do
  refresh
  [ "$(count)" -ge "$rows_before" ] && break
  sleep 1
done
python3 -c "print(f'   caught up {$(now)-$start:.0f}s after restart')"
echo "   pg rows=$rows_before  os docs=$(count)"

echo
echo "== 5. steady state =="
# counters reset with the process, so these are since the restart above
echo "   since restart: rows=$(metric 'pg2osync_events_total{type="row"}') batches=$(metric pg2osync_batches_flushed)"
echo "   sink errors: $(metric pg2osync_sink_errors_total)  reconnects: $(metric pg2osync_reconnects_total)"
echo "   rss: $(rss) MB  slot retains $(slot_wal)"
