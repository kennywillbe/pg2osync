#!/usr/bin/env bash
# Reproducible benchmark: initial load throughput, live latency and the cost of
# one large transaction.
#
# Prerequisites:
#   docker compose -f dev/docker-compose.yml up -d
#   cargo build --release
#
# Usage: [ROWS=200000] [SAMPLES=200] ./dev/benchmark.sh
set -euo pipefail

cd "$(dirname "$0")/.."
BIN=./target/release/pg2osync
OS=${OS_URL:-http://localhost:9200}
PG_CONTAINER=${PG_CONTAINER:-dev-postgres-1}
ROWS=${ROWS:-200000}
SAMPLES=${SAMPLES:-200}
BIG_TXN=${BIG_TXN:-50000}
# BSD mktemp only substitutes X's at the *end* of the template: with a
# suffix after them it creates the literal name instead, and one killed run
# then breaks every later one with "File exists".
CONFIG=$(mktemp /tmp/pg2osync-bench.XXXXXX)
LOG=/tmp/pg2osync-bench.log
export PG2OSYNC_SOURCE_URL="postgres://postgres:postgres@localhost:15432/sourcedb"

pg()      { docker exec "$PG_CONTAINER" psql -U postgres -d sourcedb -qtAc "$1"; }
refresh() { curl -s -XPOST "$OS/bench_docs/_refresh" > /dev/null; }
count()   { curl -s "$OS/bench_docs/_count" | python3 -c "import sys,json;print(json.load(sys.stdin).get('count',0))"; }
# exact series match: a substring match would also pick up the HELP/TYPE lines
metric()  { curl -s http://127.0.0.1:9113/metrics | awk -v k="$1" '$1 == k {print $2}'; }
now()     { python3 -c "import time;print(time.time())"; }

stop_sync() { pkill -f "pg2osync run" 2> /dev/null || true; }
drop_own_slot() { pg "SELECT pg_drop_replication_slot('pg2osync_bench') WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name='pg2osync_bench');" > /dev/null 2>&1 || true; }
cleanup()   { stop_sync; drop_own_slot; rm -f "$CONFIG"; }
trap cleanup EXIT

cat > "$CONFIG" <<'TOML'
[source]
url_env = "PG2OSYNC_SOURCE_URL"
slot_name = "pg2osync_bench"
publication = "pg2osync_bench_pub"

[target]
url = "http://localhost:9200"

[metrics]
bind = "127.0.0.1:9113"

[sync.bench_docs]
table = "public.bench_docs"
index = "bench_docs"
TOML

echo "== 0. prepare $ROWS rows =="
stop_sync
pg "DROP PUBLICATION IF EXISTS pg2osync_bench_pub;" > /dev/null
pg "SELECT pg_drop_replication_slot('pg2osync_bench') WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name='pg2osync_bench');" > /dev/null
pg "DROP TABLE IF EXISTS bench_docs;" > /dev/null
pg "CREATE TABLE bench_docs (
      id bigint PRIMARY KEY,
      name text NOT NULL,
      email text,
      city text,
      score numeric(10,2),
      payload jsonb,
      created_at timestamptz NOT NULL DEFAULT now());" > /dev/null
pg "INSERT INTO bench_docs (id,name,email,city,score,payload)
    SELECT g, 'user_' || g, 'u' || g || '@example.com',
           (ARRAY['istanbul','berlin','lisbon','osaka'])[1 + g % 4],
           (g % 10000)::numeric / 100,
           jsonb_build_object('tier', g % 5, 'tags', ARRAY['a','b'])
    FROM generate_series(1, $ROWS) g;" > /dev/null
curl -s -XDELETE "$OS/bench_docs,.pg2osync_meta?ignore_unavailable=true" > /dev/null
echo "   $(pg 'SELECT count(*) FROM bench_docs;') rows ready"

echo "== 1. initial load =="
start=$(now)
nohup $BIN run -c "$CONFIG" &> "$LOG" < /dev/null & disown
while :; do
  refresh
  indexed=$(count)
  [ "$indexed" -ge "$ROWS" ] && break
  sleep 1
done
elapsed=$(python3 -c "print($(now) - $start)")
python3 -c "print(f'   {$ROWS} docs in {$elapsed:.1f}s -> {$ROWS/$elapsed:,.0f} docs/s (process start to last doc searchable)')"

echo "== 2. live latency, $SAMPLES single-row commits =="
python3 - "$SAMPLES" "$OS" <<'PY'
import json, subprocess, sys, time, urllib.request
samples, os_url = int(sys.argv[1]), sys.argv[2]
psql = ["docker", "exec", "-i", "dev-postgres-1", "psql", "-U", "postgres", "-d", "sourcedb", "-qtA"]
searchable = []
for i in range(samples):
    doc_id = 90_000_000 + i
    t0 = time.time()
    subprocess.run(psql, input=(
        f"INSERT INTO bench_docs (id,name,email,city,score) "
        f"VALUES ({doc_id},'lat_{i}','l{i}@x.io','istanbul',1.00);"
    ).encode(), capture_output=True, check=True)
    while True:
        # a refresh per poll: without it the wait would just measure the
        # index's 1s refresh interval instead of the pipeline
        urllib.request.urlopen(urllib.request.Request(
            f"{os_url}/bench_docs/_refresh", method="POST"))
        try:
            with urllib.request.urlopen(f"{os_url}/bench_docs/_doc/{doc_id}") as r:
                if json.load(r).get("found"):
                    break
        except urllib.error.HTTPError:
            pass
    searchable.append((time.time() - t0) * 1000)

searchable.sort()
n = len(searchable)
q = lambda p: searchable[min(n - 1, int(n * p / 100))]
# includes the psql client round-trip and a forced refresh, so it is an upper
# bound on what a reader observes, not a measure of the pipeline alone
print(f"   commit to searchable ms (incl. client + refresh): "
      f"p50={q(50):.0f} p90={q(90):.0f} p99={q(99):.0f}")
PY
echo -n "   pipeline commit-to-indexed ms (from /metrics): "
for q in 0.5 0.9 0.99; do
  printf "p%s=%s " "$(python3 -c "print(int(float('$q')*100))")" "$(metric "pg2osync_latency_ms{quantile=\"$q\"}")"
done
echo

echo "== 3. one transaction of $BIG_TXN rows =="
before=$(count)
start=$(now)
pg "INSERT INTO bench_docs (id,name,email,city,score)
    SELECT 100000000 + g, 'bulk_' || g, 'b' || g || '@x.io', 'berlin', 2.50
    FROM generate_series(1, $BIG_TXN) g;" > /dev/null
while :; do
  refresh
  [ "$(count)" -ge $((before + BIG_TXN)) ] && break
  sleep 0.2
done
python3 -c "print(f'   {$BIG_TXN} rows propagated in {$(now) - $start:.1f}s')"

echo "== 4. steady state =="
echo "   events: $(metric 'pg2osync_events_total{type="row"}')"
echo "   batches: $(metric pg2osync_batches_flushed)"
# sampled after a checkpoint interval: between an ack and the next checkpoint
# write there is always a gap, and reading it immediately measures that gap
sleep 2
echo "   position lag: $(metric pg2osync_position_lag) (current=$(metric pg2osync_position_current) confirmed=$(metric pg2osync_position_confirmed))"
echo "   rss: $(ps -o rss= -p "$(pgrep -f 'pg2osync run' | head -1)" | awk '{printf "%.0f MB", $1/1024}')"
echo "   pg rows=$(pg 'SELECT count(*) FROM bench_docs;') os docs=$(count)"

stop_sync
pg "SELECT pg_drop_replication_slot('pg2osync_bench') WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name='pg2osync_bench');" > /dev/null
pg "DROP PUBLICATION IF EXISTS pg2osync_bench_pub;" > /dev/null
echo "== done =="
