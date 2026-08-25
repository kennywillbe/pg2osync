#!/usr/bin/env bash
# What an unchanged TOASTed column costs.
#
# An UPDATE that does not touch a large column sends a marker instead of the
# value, and the engine reads the current document back from the target to fill
# the gap. This measures that read: throughput and read-back count with the
# default replica identity, then the same table at REPLICA IDENTITY FULL, where
# the value arrives in the row image and no read happens.
#
# Prerequisites:
#   docker compose -f dev/docker-compose.yml up -d
#   cargo build --release
#
# Usage: [ROWS=20000] [WIDTH=8000] ./dev/toast-cost.sh
set -euo pipefail

cd "$(dirname "$0")/.."
BIN=./target/release/pg2osync
OS=${OS_URL:-http://localhost:9200}
PG_CONTAINER=${PG_CONTAINER:-dev-postgres-1}
ROWS=${ROWS:-20000}
WIDTH=${WIDTH:-8000}
# BSD mktemp only substitutes X's at the *end* of the template: with a
# suffix after them it creates the literal name instead, and one killed run
# then breaks every later one with "File exists".
CONFIG=$(mktemp /tmp/pg2osync-toast.XXXXXX)
LOG=/tmp/pg2osync-toast.log
export PG2OSYNC_SOURCE_URL="postgres://postgres:postgres@localhost:15432/sourcedb"

pg()      { docker exec "$PG_CONTAINER" psql -U postgres -d sourcedb -qtAc "$1"; }
refresh() { curl -s -XPOST "$OS/toast_bench/_refresh" > /dev/null; }
count()   { curl -s "$OS/toast_bench/_count" | python3 -c "import sys,json;print(json.load(sys.stdin).get('count',0))"; }
metric()  { curl -s http://127.0.0.1:9114/metrics | awk -v k="$1" '$1 == k {print $2}'; }
now()     { python3 -c "import time;print(time.time())"; }

stop_sync() { pkill -f "pg2osync run" 2> /dev/null || true; }
drop_own_slot() { pg "SELECT pg_drop_replication_slot('pg2osync_toast') WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name='pg2osync_toast');" > /dev/null 2>&1 || true; }
cleanup()   { stop_sync; drop_own_slot; rm -f "$CONFIG"; }
trap cleanup EXIT

cat > "$CONFIG" <<'TOML'
[source]
url_env = "PG2OSYNC_SOURCE_URL"
slot_name = "pg2osync_toast"
publication = "pg2osync_toast_pub"

[target]
url = "http://localhost:9200"

[metrics]
bind = "127.0.0.1:9114"

[sync.toast_bench]
table = "public.toast_bench"
index = "toast_bench"
TOML

# One pass: load, stream, update every row once, report.
run_case() {
  local identity="$1"
  stop_sync
  drop_own_slot
  pg "DROP PUBLICATION IF EXISTS pg2osync_toast_pub;" > /dev/null
  pg "DROP TABLE IF EXISTS toast_bench;" > /dev/null
  pg "CREATE TABLE toast_bench (
        id bigint PRIMARY KEY,
        small int NOT NULL,
        big text NOT NULL);" > /dev/null
  # random bytes, not repeated ones: PostgreSQL compresses before it TOASTs, so
  # a repetitive value stays inline and never arrives as a marker at all
  pg "INSERT INTO toast_bench (id, small, big)
      SELECT g, 0, string_agg(md5(random()::text), '')
      FROM generate_series(1, $ROWS) g, generate_series(1, $WIDTH / 32) c
      GROUP BY g;" > /dev/null
  local toasted
  toasted=$(pg "SELECT count(*) FROM pg_toast.pg_toast_$(pg "SELECT 'toast_bench'::regclass::oid;");" 2>/dev/null || echo 0)
  echo "   ($toasted toast chunks stored out of line)"
  pg "ALTER TABLE toast_bench REPLICA IDENTITY $identity;" > /dev/null
  curl -s -XDELETE "$OS/toast_bench,.pg2osync_meta" > /dev/null

  nohup $BIN run -c "$CONFIG" &> "$LOG" < /dev/null & disown
  while :; do refresh; [ "$(count)" -ge "$ROWS" ] && break; sleep 1; done

  local before_reads start elapsed
  before_reads=$(metric pg2osync_toast_readbacks_total)
  start=$(now)
  pg "UPDATE toast_bench SET small = small + 1;" > /dev/null
  while :; do
    [ "$(metric 'pg2osync_events_total{type="row"}')" -ge $((ROWS * 2)) ] && break
    sleep 0.2
  done
  elapsed=$(python3 -c "print($(now) - $start)")

  python3 -c "print(f'   {\"$identity\":<8} {$ROWS} updates in {$elapsed:.1f}s -> {$ROWS/$elapsed:,.0f} rows/s, \
read-backs: {$(metric pg2osync_toast_readbacks_total) - $before_reads}')"
  stop_sync
}

echo "== $ROWS rows, a ${WIDTH}-byte column left untouched by every update =="
run_case DEFAULT
run_case FULL

echo
echo "WAL written per case is what REPLICA IDENTITY FULL costs in exchange;"
echo "see dev/db-impact.sh for that side of the trade."
pg "DROP TABLE IF EXISTS toast_bench;" > /dev/null
pg "DROP PUBLICATION IF EXISTS pg2osync_toast_pub;" > /dev/null
curl -s -XDELETE "$OS/toast_bench" > /dev/null
