#!/usr/bin/env bash
# What does pg2osync cost the source database *while the database is busy*?
#
# `db-impact.sh` measures the cost in connections, queries and WAL. It says
# nothing about the question an operator actually asks before turning this on:
# does my database get slower, and by how much. So this drives a write workload
# with pgbench and reports foreground throughput and latency while the pipeline
# runs beside it.
#
# The number that comes out of that alone is misleading, and the control phase is
# the point of this script. On one machine the database, the pipeline and the
# target share the same cores, so "pipeline running" measures co-location as much
# as replication. `pg_recvlogical` does the identical decoding work with no
# pipeline and no target behind it, which separates what the *database* pays from
# what a laptop pays. The walsender's own CPU is reported for the same reason.
#
# The pipeline replicates the very table pgbench is hammering, which is the
# expensive arrangement rather than a flattering one: every write is decoded and
# streamed.
#
# Prerequisites:
#   docker compose -f dev/docker-compose.yml up -d
#   cargo build --release
#
# Usage: [CLIENTS=8] [DURATION=30] [SCALE=10] ./dev/db-load-impact.sh
set -euo pipefail

cd "$(dirname "$0")/.."
BIN=./target/release/pg2osync
OS=${OS_URL:-http://localhost:9200}
PG_CONTAINER=${PG_CONTAINER:-dev-postgres-1}
CLIENTS=${CLIENTS:-8}
# Not SECONDS: bash owns that name and it holds the shell's own uptime, which
# reads as a valid duration and silently becomes `-T 0`.
DURATION=${DURATION:-30}
SCALE=${SCALE:-10}
SLOT=pg2osync_loadimpact
INDEX=loadimpact
CONFIG=$(mktemp /tmp/pg2osync-loadimpact.XXXXXX)
LOG=/tmp/pg2osync-loadimpact.log
RESULTS=/tmp/pg2osync-loadimpact-results.txt

pg()   { docker exec "$PG_CONTAINER" psql -U postgres -d sourcedb -qtAc "$1"; }
bench(){ docker exec "$PG_CONTAINER" pgbench -U postgres -d sourcedb "$@" 2>&1; }
stop_sync() { pkill -f "pg2osync run" 2> /dev/null || true; sleep 1; }
drop_slot() { pg "SELECT pg_drop_replication_slot('$SLOT') WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name='$SLOT');" > /dev/null 2>&1 || true; }
cleanup() {
  stop_sync
  drop_slot
  pg "DROP PUBLICATION IF EXISTS ${SLOT}_pub;" > /dev/null 2>&1 || true
  rm -f "$CONFIG"
}
trap cleanup EXIT

say() { printf "\n\033[1m== %s ==\033[0m\n" "$1"; }

# One phase: run the workload, print tps and latency, and keep them for the
# comparison at the end.
phase() {
  local name=$1
  local out
  # -N skips the branch and teller updates: at this scale they serialise on a
  # handful of rows and the number would measure lock contention rather than
  # anything about replication.
  out=$(bench -c "$CLIENTS" -j 4 -T "$DURATION" -N -r)
  local tps lat
  tps=$(awk '/^tps =/ {print $3; exit}' <<< "$out")
  lat=$(awk '/^latency average/ {print $4; exit}' <<< "$out")
  printf "  %-34s %10.0f tps   %8.3f ms average\n" "$name" "$tps" "$lat"
  echo "$name|$tps|$lat" >> "$RESULTS"
}

# Core-seconds the walsender for one slot has burned since it started.
#
# Read from the container's own /proc rather than `docker stats`: the latter
# samples the instant it is called, which after a phase has ended is the instant
# nothing is happening.
walsender_cpu_ticks() {
  local slot=$1 pid
  pid=$(pg "SELECT coalesce(active_pid, 0) FROM pg_replication_slots WHERE slot_name = '$slot';")
  [ "${pid:-0}" -gt 0 ] 2> /dev/null || { echo 0; return; }
  docker exec "$PG_CONTAINER" awk '{print $14 + $15}' "/proc/$pid/stat" 2> /dev/null || echo 0
}

# What that many ticks means as a share of one core over the phase.
report_walsender() {
  local before=$1 after=$2
  python3 -c "
before, after, secs = $before, $after, $DURATION
# Linux reports these in clock ticks; 100 a second on every platform this runs on
cores = (after - before) / 100.0 / secs
print(f'  walsender CPU: {(after - before) / 100.0:.1f}s over {secs}s = {cores:.2f} cores')
"
}

: > "$RESULTS"

say "setup: pgbench scale $SCALE, $CLIENTS clients, ${DURATION}s per phase"
stop_sync
drop_slot
bench -i -s "$SCALE" --quiet > /dev/null 2>&1
pg "DROP PUBLICATION IF EXISTS ${SLOT}_pub; CREATE PUBLICATION ${SLOT}_pub FOR TABLE pgbench_accounts;" > /dev/null
rows=$(pg "SELECT count(*) FROM pgbench_accounts;")
echo "  pgbench_accounts: $rows rows"

cat > "$CONFIG" <<TOML
[source]
url_env = "PG2OSYNC_SOURCE_URL"
slot_name = "$SLOT"
publication = "${SLOT}_pub"

[target]
url = "$OS"

[metrics]
enabled = false

[sync.$INDEX]
table = "public.pgbench_accounts"
index = "$INDEX"
primary_key = "aid"
TOML
export PG2OSYNC_SOURCE_URL="postgres://postgres:postgres@localhost:15432/sourcedb"

say "1. baseline — nothing replicating"
phase "no pipeline"

say "2. control — the same decoding, with nothing behind it"
# pgoutput and the same publication, so the database does exactly the work it
# would do for us; the output goes nowhere, so nothing competes for the cores.
drop_ctrl() { pg "SELECT pg_drop_replication_slot('${SLOT}_ctrl') WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name='${SLOT}_ctrl');" > /dev/null 2>&1 || true; }
drop_ctrl
docker exec "$PG_CONTAINER" pg_recvlogical -U postgres -d sourcedb \
  --slot "${SLOT}_ctrl" --create-slot -P pgoutput > /dev/null 2>&1
docker exec -d "$PG_CONTAINER" pg_recvlogical -U postgres -d sourcedb \
  --slot "${SLOT}_ctrl" --start -o proto_version=1 \
  -o "publication_names=${SLOT}_pub" -f /dev/null
sleep 3
ctrl_before=$(walsender_cpu_ticks "${SLOT}_ctrl")
phase "logical decoding only"
ctrl_after=$(walsender_cpu_ticks "${SLOT}_ctrl")
report_walsender "$ctrl_before" "$ctrl_after"
docker exec "$PG_CONTAINER" pkill -f pg_recvlogical > /dev/null 2>&1 || true
drop_ctrl

say "3. steady state — the stream running, table already loaded"
curl -s -XDELETE "$OS/$INDEX,.pg2osync_meta?ignore_unavailable=true" > /dev/null
nohup $BIN run -c "$CONFIG" > "$LOG" 2>&1 < /dev/null & disown
# wait for the initial load to finish before measuring, so this phase is about
# decoding and streaming rather than about the copy
for _ in $(seq 1 300); do
  curl -s -XPOST "$OS/$INDEX/_refresh" > /dev/null 2>&1 || true
  n=$(curl -s "$OS/$INDEX/_count" | python3 -c "import sys,json;print(json.load(sys.stdin).get('count',0))" 2>/dev/null || echo 0)
  [ "$n" -ge "$rows" ] && break
  sleep 1
done
echo "  initial load finished ($n rows indexed)"
stream_before=$(walsender_cpu_ticks "$SLOT")
phase "streaming"
stream_after=$(walsender_cpu_ticks "$SLOT")
report_walsender "$stream_before" "$stream_after"
stop_sync

# The load is the expensive half, so it gets measured at both widths. Each run
# starts from no checkpoint, which is what makes it load again.
for workers in 1 4; do
  say "4. initial load beside the workload — load_workers = $workers"
  drop_slot
  curl -s -XDELETE "$OS/$INDEX,.pg2osync_meta?ignore_unavailable=true" > /dev/null
  sed "s/^slot_name = .*/slot_name = \"$SLOT\"\nload_workers = $workers/" "$CONFIG" > "$CONFIG.$workers"
  nohup $BIN run -c "$CONFIG.$workers" > "$LOG.$workers" 2>&1 < /dev/null & disown
  sleep 2
  phase "loading, $workers reader(s)"
  grep -oE "read [0-9]+ rows from [a-z_.]+ in [0-9.]+s \(~[0-9]+ rows/s\)" "$LOG.$workers" | tail -1 | sed 's/^/  /' || true
  stop_sync
  rm -f "$CONFIG.$workers"
done

say "what it cost"
python3 - "$RESULTS" <<'PY'
import sys
rows = [l.strip().split("|") for l in open(sys.argv[1]) if l.strip()]
base_tps = float(rows[0][1])
base_lat = float(rows[0][2])
print(f"  baseline: {base_tps:,.0f} tps, {base_lat:.3f} ms")
for name, tps, lat in rows[1:]:
    tps, lat = float(tps), float(lat)
    print(
        f"  {name:<24} {tps:>9,.0f} tps  ({(tps/base_tps-1)*100:+.1f}%)"
        f"   {lat:>7.3f} ms  ({(lat/base_lat-1)*100:+.1f}%)"
    )
PY
echo
echo "The control line is the one to quote. It is the same decoding work with no"
echo "pipeline and no target behind it, so the gap between it and the baseline is"
echo "what the *database* pays. Everything below it also pays for the pipeline and"
echo "the target sharing this machine's cores, which a real deployment does not."
