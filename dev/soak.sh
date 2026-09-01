#!/usr/bin/env bash
# Does it still hold after hours, and after being interfered with?
#
# Every other probe here is a sprint. dev/load-test.sh finds where the pipeline
# stops keeping up and then interferes with it once — a pause, a kill — in a run
# measured in tens of seconds. Nothing answers the question an operator actually
# asks before leaving a process alone over a weekend: does memory stay flat,
# does the slot keep coming back to nothing, and does a fault at hour three end
# the same way a fault at minute three does.
#
# So this is that run, generalised. Sustained pgbench writes for as long as you
# ask, a chaos operation on a schedule, a sample every few seconds appended to a
# CSV, and — because a harness whose red means nothing is not worth running —
# a set of hard invariants asserted at the end, ending in a RESULT line.
#
# The chaos rotation, in order, repeating until the clock runs out:
#
#   1. pause the target        the promise is bounded memory: the backlog goes
#                              to retained WAL, not to the heap, and drains when
#                              the target comes back
#   2. terminate the walsender the promise is an in-process reconnect: the same
#                              pid carries on and reconnects_total moves
#   3. schema drift            the promise is that drift is counted, never
#                              applied, and never stops the rows behind it
#   4. one large transaction   the promise is that it is split across requests
#                              and still lands as a whole
#   5. kill -9 and restart     the promise is at-least-once: the source row
#                              count and the document count agree afterwards
#
# in that order because the last one is the expensive one: catching up from a
# checkpoint while the load continues takes minutes, so a run short enough to
# see only some of the rotation sees the cheap ones.
#
# and, once, as the last act before teardown, SIGTERM: it must drain and exit 0,
# and what it wrote must survive the restart that follows.
#
# The load underneath is three shapes at once, because they stress different
# things: many small commits (the channel), a 100-row commit every tenth
# transaction (the batcher), a periodic 20k-row transaction (the splitter), and
# a periodic pulse of wide incompressible rows whose narrow column is then
# updated — that last one is dev/toast-cost.sh's pattern, and it is what makes
# the read-back path run continuously rather than in a benchmark.
#
# It brings up throwaway containers of its own, named for the run and on ports
# Docker picks, the way dev/ci-local.sh --isolated does. It takes no lock, never
# touches the dev stack, and can run beside anything else on the machine. One
# stack is about 1 GB, so an 8 GB Docker VM carries two of these.
#
# It kills only the process it started, by pid. The `pkill -f "pg2osync run"`
# the shorter probes use would take a colleague's pipeline down with it, and a
# run that lasts hours will overlap with one.
#
# Prerequisites:
#   cargo build --release
#
# Usage:
#   ./dev/soak.sh                              # 30 minutes, chaos every 5
#   ./dev/soak.sh 4h --chaos-interval 10m
#   ./dev/soak.sh 10m --rate 300 --rss-ceiling 300
#
# Not part of CI: an hour of load on one machine belongs in a terminal someone
# is watching, like ./dev/failover-probe.sh. Run it before a release, after
# anything that touches buffering, retries, checkpointing or the slot, and
# whenever a leak is suspected.
set -euo pipefail

cd "$(dirname "$0")/.."

BIN=./target/release/pg2osync
PG_IMAGE=${PG_IMAGE:-postgres:17}
OS_IMAGE=${OS_IMAGE:-opensearchproject/opensearch:2.19.6}
DURATION=30m
RATE=200
CHAOS_INTERVAL=300
# Ten times what the documentation claims steady state costs (tens of MB). A
# ceiling that close to the claim would flake on a garbage collector's timing;
# this one only fires on something that grows.
RSS_CEILING=300
SAMPLE_SECONDS=${SAMPLE_SECONDS:-15}
CLIENTS=${CLIENTS:-4}
# How much of a wide row is out of line: PostgreSQL compresses before it TOASTs,
# so the value has to be incompressible to leave the table at all.
TOAST_WIDTH=${TOAST_WIDTH:-8000}
TOAST_ROWS=${TOAST_ROWS:-200}
BIG_TXN_ROWS=${BIG_TXN_ROWS:-20000}
# What the slot may still hold once everything has drained. Not zero: the
# database keeps writing WAL of its own — checkpoints, autovacuum of a table
# this run has just put millions of rows through — and the slot's confirmed
# position only moves when the pipeline next sends feedback, so a live source
# is legitimately a fraction of a segment behind its own WAL head. One segment
# is generous enough not to flake and far below the tens of megabytes a real
# backlog reaches.
RETAINED_CEILING=$((16 * 1024 * 1024))

while [ $# -gt 0 ]; do
  case "$1" in
    --rate) RATE=$2; shift 2 ;;
    --chaos-interval) CHAOS_INTERVAL=$2; shift 2 ;;
    --rss-ceiling) RSS_CEILING=$2; shift 2 ;;
    -h | --help) sed -n '2,60p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    -*) echo "unknown option $1" >&2; exit 2 ;;
    *) DURATION=$1; shift ;;
  esac
done

seconds_of() { # 30m | 4h | 90s | 90
  local v=$1
  case "$v" in
    *h) echo $(( ${v%h} * 3600 )) ;;
    *m) echo $(( ${v%m} * 60 )) ;;
    *s) echo "${v%s}" ;;
    *[!0-9]*) echo "cannot read a duration from '$v'" >&2; return 1 ;;
    *) echo "$v" ;;
  esac
}
TOTAL=$(seconds_of "$DURATION")
CHAOS_INTERVAL=$(seconds_of "$CHAOS_INTERVAL")
[ "$TOTAL" -ge 60 ] || { echo "a soak shorter than a minute measures nothing" >&2; exit 2; }

# Identifies the run in its log directory and in every container it starts; the
# pid is what keeps two runs started in the same second apart.
RUN_ID=$(date +%Y%m%d-%H%M%S)-$$
RUN_DIR=${PG2OSYNC_SOAK_LOG_DIR:-/tmp/pg2osync-soak/$RUN_ID}
CSV=$RUN_DIR/timeline.csv
LOG=$RUN_DIR/pipeline.log
PG=pg2osync-soak-$RUN_ID-pg
OS_CONTAINER=pg2osync-soak-$RUN_ID-os
SLOT=pg2osync_soak
TABLE=soak_load
INDEX=soak_load
SOURCE_NAME=soak
# BSD mktemp only substitutes X's at the *end* of the template: with a
# suffix after them it creates the literal name instead, and one killed run
# then breaks every later one with "File exists".
CONFIG=$(mktemp /tmp/pg2osync-soak.XXXXXX)
SNAP=$(mktemp /tmp/pg2osync-soak-metrics.XXXXXX)

PASS=0
FAIL=0
ok()   { printf "  \033[32m✓ %s\033[0m\n" "$1"; PASS=$((PASS+1)); }
bad()  { printf "  \033[31m✗ %s\033[0m\n" "$1"; FAIL=$((FAIL+1)); }
say()  { printf "\n\033[1m== %s ==\033[0m\n" "$1"; }
note() { printf "  %s\n" "$1"; }
check() { if [ "$2" = "$3" ]; then ok "$1 ($2)"; else bad "$1 (got '$2', want '$3')"; fi; }

SYNC_PID=
PGBENCH_PID=
# Set while the harness is the reason the process is gone, so the sampler can
# tell a restart it asked for from one it did not.
EXPECTED_DOWN=0

stop_sync() {
  [ -n "$SYNC_PID" ] || return 0
  kill "$SYNC_PID" 2> /dev/null || true
  wait "$SYNC_PID" 2> /dev/null || true
  SYNC_PID=
}

# The client is what this shell can signal; pgbench itself runs inside the
# container, so its backends are ended where they are. A straggler writing while
# the final counts are taken would make the source and the index disagree for a
# reason that is not a fault.
stop_load() {
  if [ -n "$PGBENCH_PID" ]; then
    kill "$PGBENCH_PID" 2> /dev/null || true
    wait "$PGBENCH_PID" 2> /dev/null || true
    PGBENCH_PID=
  fi
  pg "SELECT pg_terminate_backend(pid) FROM pg_stat_activity
      WHERE application_name = 'pgbench';" > /dev/null 2>&1 || true
}

cleanup() {
  EXPECTED_DOWN=1
  if [ -n "$PGBENCH_PID" ]; then kill "$PGBENCH_PID" 2> /dev/null || true; fi
  if [ -n "$SYNC_PID" ]; then
    kill -9 "$SYNC_PID" 2> /dev/null || true
    # Reaped here so the shell does not report the signal on its own after the
    # RESULT line, where it reads as part of the result.
    wait "$SYNC_PID" 2> /dev/null || true
  fi
  docker unpause "$OS_CONTAINER" > /dev/null 2>&1 || true
  # The slot and the publication live in a container that is about to be
  # removed, so dropping them is only for the case where it is not.
  docker exec "$PG" psql -U postgres -d sourcedb -qtAc \
    "SELECT pg_drop_replication_slot('$SLOT') WHERE EXISTS
       (SELECT 1 FROM pg_replication_slots WHERE slot_name='$SLOT');" > /dev/null 2>&1 || true
  docker rm -f "$PG" "$OS_CONTAINER" > /dev/null 2>&1 || true
  rm -f "$CONFIG" "$SNAP"
}
trap cleanup EXIT

now()  { python3 -c 'import time;print(f"{time.time():.1f}")'; }
stamp() { date +%H:%M:%S; }
pg()   { docker exec "$PG" psql -U postgres -d sourcedb -qtAc "$1"; }
port_free() { ! (exec 3<> "/dev/tcp/127.0.0.1/$1") 2> /dev/null; }
published_port() { docker port "$1" "$2/tcp" | head -1 | sed 's/.*://'; }

# Probing and then binding leaves a window, but the pipeline binds within a
# second of the choice and the scan starts at this run's pid, so two runs
# looking at the same moment do not both take the first free port.
pick_port() {
  local i p
  for i in $(seq 0 199); do
    p=$(( 9400 + ($$ + i) % 200 ))
    if port_free "$p"; then echo "$p"; return 0; fi
  done
  echo "no free port for the metrics endpoint" >&2
  return 1
}

# A series with no samples yet is absent from the exposition, and every caller
# here does arithmetic on the answer. Every family carries a source label, so
# these match by name and sum: one source today, and a total that stays correct
# if the config ever grows a second.
snapshot() { curl -s --max-time 5 "$METRICS/metrics" > "$SNAP" 2> /dev/null || : > "$SNAP"; }
msum() { # name [label substring]
  awk -v n="$1" -v f="${2:-}" '
    (index($1, n "{") == 1 || $1 == n) && (f == "" || index($1, f) > 0) { s += $2 }
    END { printf "%d", s + 0 }' "$SNAP"
}
wal_lost() { awk '/^pg2osync_slot_wal_status\{.*status="lost"\} 1$/ { found = 1 }
                  END { print found ? 1 : 0 }' "$SNAP"; }
rss_kb() { [ -n "$SYNC_PID" ] && ps -o rss= -p "$SYNC_PID" 2> /dev/null | tr -d ' ' || echo 0; }
http_code() { curl -s -o /dev/null --max-time 5 -w '%{http_code}' "$1" 2> /dev/null || echo 000; }
os_count() { curl -s "$OS_URL/$INDEX/_count" \
  | python3 -c 'import sys,json;print(json.load(sys.stdin).get("count",0))' 2> /dev/null || echo 0; }
os_refresh() { curl -s -XPOST "$OS_URL/$INDEX/_refresh" > /dev/null 2>&1 || true; }

# What the run has seen, for the summary and for the invariants.
PEAK_RSS=0
PEAK_LAG=0
PEAK_RETAINED=0
HEALTH_FAILS=0
WAL_LOST_SEEN=0
UNEXPECTED_EXITS=0
CALM_SINK_ERRORS=0
CHAOS_OPS=0
SAMPLES=0
# Sink errors are counted per failed write to the target, so the pause phase is
# expected to move this counter; a calm minute is not. The baseline is retaken
# after every chaos operation has drained, and the counter itself restarts with
# the process, which is why this is a stored value rather than a total.
SINK_BASE=0
DRAIN_LOG=""
# Every counter restarts with the process, and this run restarts it on purpose,
# so a total over the whole soak has to be accumulated here: a value below the
# last one read is a new process rather than a counter going backwards.
CARRY_EVENTS=0; LAST_EVENTS=0
CARRY_RECONNECTS=0; LAST_RECONNECTS=0
CARRY_READBACKS=0; LAST_READBACKS=0
CARRY_DRIFT=0; LAST_DRIFT=0

sample() { # phase [event]
  local phase=$1 event=${2:-} rss lag retained events batches reconnects errors \
    rejected p99 health readbacks drift
  snapshot
  rss=$(rss_kb); rss=${rss:-0}
  lag=$(msum pg2osync_position_lag)
  retained=$(msum pg2osync_slot_retained_bytes)
  events=$(msum pg2osync_events_total)
  batches=$(msum pg2osync_batches_flushed)
  reconnects=$(msum pg2osync_reconnects_total)
  errors=$(msum pg2osync_sink_errors_total)
  rejected=$(msum pg2osync_rejected_total)
  p99=$(msum pg2osync_latency_ms 'quantile="0.99"')
  readbacks=$(msum pg2osync_toast_readbacks_total)
  drift=$(msum pg2osync_schema_drift_total)
  health=$(http_code "$METRICS/healthz")
  SAMPLES=$((SAMPLES + 1))

  # An unreachable endpoint reads as zero everywhere, which would look exactly
  # like a restart; only a snapshot that arrived may move these.
  if [ -s "$SNAP" ]; then
    if [ "$events" -lt "$LAST_EVENTS" ]; then CARRY_EVENTS=$((CARRY_EVENTS + LAST_EVENTS)); fi
    if [ "$reconnects" -lt "$LAST_RECONNECTS" ]; then
      CARRY_RECONNECTS=$((CARRY_RECONNECTS + LAST_RECONNECTS))
    fi
    if [ "$readbacks" -lt "$LAST_READBACKS" ]; then
      CARRY_READBACKS=$((CARRY_READBACKS + LAST_READBACKS))
    fi
    if [ "$drift" -lt "$LAST_DRIFT" ]; then CARRY_DRIFT=$((CARRY_DRIFT + LAST_DRIFT)); fi
    LAST_EVENTS=$events
    LAST_RECONNECTS=$reconnects
    LAST_READBACKS=$readbacks
    LAST_DRIFT=$drift
  fi

  if [ "$rss" -gt "$PEAK_RSS" ]; then PEAK_RSS=$rss; fi
  if [ "$lag" -gt "$PEAK_LAG" ]; then PEAK_LAG=$lag; fi
  if [ "$retained" -gt "$PEAK_RETAINED" ]; then PEAK_RETAINED=$retained; fi
  if [ "$(wal_lost)" = 1 ]; then WAL_LOST_SEEN=1; fi
  if [ "$EXPECTED_DOWN" = 0 ] && [ -n "$SYNC_PID" ] && ! kill -0 "$SYNC_PID" 2> /dev/null; then
    UNEXPECTED_EXITS=$((UNEXPECTED_EXITS + 1))
  fi
  if [ "$phase" = steady ]; then
    # Liveness only outside a chaos window: /healthz answers for the process,
    # and the process is deliberately absent inside one.
    [ "$health" = 200 ] || HEALTH_FAILS=$((HEALTH_FAILS + 1))
    [ "$(http_code "$METRICS/healthz/$SOURCE_NAME")" = 200 ] || HEALTH_FAILS=$((HEALTH_FAILS + 1))
    if [ "$errors" -gt "$SINK_BASE" ]; then
      CALM_SINK_ERRORS=$((CALM_SINK_ERRORS + errors - SINK_BASE))
    fi
    SINK_BASE=$errors
  fi

  printf '%s,%d,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' \
    "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$(elapsed)" "$phase" "$event" \
    "$rss" "$lag" "$retained" "$events" "$batches" "$reconnects" \
    "$errors" "$rejected" "$p99" "$readbacks" "$drift" "$health" >> "$CSV"
}

START=
elapsed() { python3 -c "print(int($(now) - $START))"; }

# Sample for this many seconds, so a chaos window is measured rather than slept
# through. Returns as soon as the time is up, never before a whole sample.
wait_sampling() { # seconds phase [event]
  local until_s=$1 phase=$2 event=${3:-} deadline
  deadline=$(( $(elapsed) + until_s ))
  while [ "$(elapsed)" -lt "$deadline" ]; do
    sample "$phase" "$event"
    event=""
    sleep "$SAMPLE_SECONDS"
  done
}

# Wait until the pipeline has caught up, sampling while it does, and leave how
# long it took in DRAIN_TOOK. A freshly started process serves no samples at all
# and an absent series reads as zero, so the position has to move before the lag
# means anything.
#
# The load never stops for it, so this is a race rather than a wait: what it
# measures is how long catching up takes while the writers carry on, and the
# timeout is the answer when catching up loses. That is also why a chaos
# interval shorter than a drain simply runs the rotation back to back — over
# hours it comes round many times either way.
#
# It reports through a variable rather than stdout because a command
# substitution would run it in a subshell, and everything it accumulates on the
# way — the peaks, the health failures — would be discarded with that subshell.
DRAIN_TOOK=0
drain() { # phase label [timeout]
  local phase=$1 label=$2 timeout=${3:-600} start
  start=$(now)
  while [ "$(python3 -c "print(int($(now) - $start))")" -lt "$timeout" ]; do
    snapshot
    if [ -s "$SNAP" ] && [ "$(msum pg2osync_position_current)" != 0 ] &&
      [ "$(msum pg2osync_position_lag)" = 0 ]; then
      break
    fi
    sample "$phase" ""
    sleep 2
  done
  DRAIN_TOOK=$(python3 -c "print(int($(now) - $start))")
  DRAIN_LOG="$DRAIN_LOG$label ${DRAIN_TOOK}s
"
  sample "$phase" "drained:$label:${DRAIN_TOOK}s"
}

start_sync() {
  EXPECTED_DOWN=0
  nohup "$BIN" run -c "$CONFIG" >> "$LOG" 2>&1 < /dev/null &
  SYNC_PID=$!
  local i
  for i in $(seq 1 60); do
    if [ "$(http_code "$METRICS/healthz")" = 200 ]; then return 0; fi
    sleep 1
  done
  echo "the pipeline never answered on $METRICS" >&2
  return 1
}

# ------------------------------------------------------------------- the stack
mkdir -p "$RUN_DIR"
[ -x "$BIN" ] || { echo "$BIN is missing. Build it first: cargo build --release" >&2; exit 2; }

say "0. a stack of this run's own"
note "run id     $RUN_ID"
note "logs       $RUN_DIR"
note "duration   ${TOTAL}s, chaos every ${CHAOS_INTERVAL}s, ${RATE} txn/s from $CLIENTS writers"
docker run -d --name "$PG" -p 0:5432 \
  -e POSTGRES_USER=postgres -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=sourcedb \
  "$PG_IMAGE" -c wal_level=logical -c max_wal_senders=10 -c max_replication_slots=10 > /dev/null
# The heap dev/docker-compose.yml gives its node: without it OpenSearch sizes
# itself off the whole Docker VM and this stack stops fitting beside the others.
docker run -d --name "$OS_CONTAINER" -p 0:9200 \
  -e discovery.type=single-node -e DISABLE_SECURITY_PLUGIN=true \
  -e OPENSEARCH_JAVA_OPTS="-Xms512m -Xmx512m" "$OS_IMAGE" > /dev/null
PG_PORT=$(published_port "$PG" 5432)
OS_PORT=$(published_port "$OS_CONTAINER" 9200)
OS_URL=http://localhost:$OS_PORT
METRICS=http://127.0.0.1:$(pick_port)
for _ in $(seq 1 90); do
  if docker exec "$PG" pg_isready -U postgres -d sourcedb > /dev/null 2>&1; then break; fi
  sleep 2
done
for _ in $(seq 1 90); do
  if curl -s "$OS_URL/_cluster/health" | grep -q '"status":"\(green\|yellow\)"'; then break; fi
  sleep 2
done
note "postgres   localhost:$PG_PORT      opensearch $OS_URL"
note "metrics    $METRICS"

pg "CREATE TABLE $TABLE (
      id bigint PRIMARY KEY,
      payload text NOT NULL,
      big text,
      n int NOT NULL);" > /dev/null

cat > "$RUN_DIR/small.sql" <<'SQL'
\set id random(1, 1000000000)
INSERT INTO soak_load (id, payload, n) VALUES (:id, 'p', 1)
  ON CONFLICT (id) DO UPDATE SET n = soak_load.n + 1;
SQL
cat > "$RUN_DIR/batch.sql" <<'SQL'
\set base random(1, 1000000000)
BEGIN;
INSERT INTO soak_load (id, payload, n)
  SELECT :base + g, 'p', 1 FROM generate_series(1, 100) g
  ON CONFLICT (id) DO UPDATE SET n = soak_load.n + 1;
COMMIT;
SQL
docker cp "$RUN_DIR/small.sql" "$PG:/tmp/small.sql" > /dev/null
docker cp "$RUN_DIR/batch.sql" "$PG:/tmp/batch.sql" > /dev/null

cat > "$CONFIG" <<TOML
[source]
name = "$SOURCE_NAME"
url_env = "PG2OSYNC_SOAK_URL"
slot_name = "$SLOT"
publication = "${SLOT}_pub"

[target]
url = "$OS_URL"

[metrics]
bind = "${METRICS#http://}"

[sync.$INDEX]
table = "public.$TABLE"
index = "$INDEX"
TOML
export PG2OSYNC_SOAK_URL="postgres://postgres:postgres@localhost:$PG_PORT/sourcedb"

echo 'timestamp,elapsed_s,phase,event,rss_kb,position_lag,slot_retained_bytes,events_total,batches_flushed,reconnects_total,sink_errors_total,rejected_total,latency_p99_ms,toast_readbacks_total,schema_drift_total,healthz' > "$CSV"

say "1. the pipeline, and the load it will carry throughout"
START=$(now)
start_sync
note "pipeline pid $SYNC_PID"
# Nine small commits to one hundred-row commit: the same row rate through two
# transaction shapes, which is what dev/load-test.sh found the batcher cares
# about.
docker exec -i "$PG" pgbench -U postgres -d sourcedb -n \
  -c "$CLIENTS" -j "$CLIENTS" -T "$TOTAL" -R "$RATE" \
  -f /tmp/small.sql@9 -f /tmp/batch.sql@1 > "$RUN_DIR/pgbench.log" 2>&1 &
PGBENCH_PID=$!
note "pgbench running for ${TOTAL}s"

# ------------------------------------------------------------- chaos operations
chaos_pause_target() {
  local before after took
  note "$(stamp)  pause the target for 75s"
  sample chaos "pause:begin"
  docker pause "$OS_CONTAINER" > /dev/null
  wait_sampling 75 chaos "paused"
  before=$(msum pg2osync_slot_retained_bytes)
  docker unpause "$OS_CONTAINER" > /dev/null
  sample chaos "pause:end"
  drain chaos pause
  took=$DRAIN_TOOK
  snapshot
  after=$(msum pg2osync_slot_retained_bytes)
  note "     backlog reached $((before / 1024)) kB of retained WAL, drained in ${took}s to $((after / 1024)) kB"
}

chaos_terminate_walsender() {
  local pid_before reconnects_before killed i
  pid_before=$SYNC_PID
  reconnects_before=$(msum pg2osync_reconnects_total)
  killed=$(pg "SELECT count(*) FROM (SELECT pg_terminate_backend(pid)
              FROM pg_stat_activity WHERE backend_type = 'walsender') t;")
  note "$(stamp)  terminate the walsender ($killed backend(s))"
  sample chaos "walsender:terminated"
  for i in $(seq 1 60); do
    snapshot
    if [ "$(msum pg2osync_reconnects_total)" -gt "$reconnects_before" ]; then break; fi
    sleep 1
  done
  if kill -0 "$pid_before" 2> /dev/null; then
    note "     reconnected in process: pid $pid_before still running"
  else
    bad "the walsender terminating took the process with it"
  fi
  drain chaos walsender
}

chaos_kill_restart() {
  local rows docs i
  note "$(stamp)  kill -9 and restart"
  sample chaos "kill:begin"
  EXPECTED_DOWN=1
  kill -9 "$SYNC_PID" 2> /dev/null || true
  wait "$SYNC_PID" 2> /dev/null || true
  SYNC_PID=
  sleep 2
  rows=$(pg "SELECT count(*) FROM $TABLE;")
  start_sync
  local took
  drain chaos "kill-restart"
  took=$DRAIN_TOOK
  docs=0
  for i in $(seq 1 120); do
    os_refresh
    docs=$(os_count)
    if [ "$docs" -ge "$rows" ]; then break; fi
    sleep 1
  done
  # The count at the moment of the kill is the floor, not the answer: the load
  # never stopped, so the index legitimately holds more by now.
  if [ "$docs" -ge "$rows" ]; then
    note "     resumed in ${took}s with nothing lost ($rows rows at the kill, $docs docs now)"
  else
    bad "rows written before the kill did not come back ($docs docs against $rows rows)"
  fi
}

chaos_schema_drift() {
  local before after events_before events_after
  before=$(msum pg2osync_schema_drift_total)
  events_before=$(msum pg2osync_events_total)
  note "$(stamp)  add and drop a column under the running pipeline"
  pg "ALTER TABLE $TABLE ADD COLUMN soak_probe int;" > /dev/null
  pg "INSERT INTO $TABLE (id, payload, n, soak_probe)
      SELECT 3000000000 + g, 'drift', 1, g FROM generate_series(1, 100) g
      ON CONFLICT (id) DO UPDATE SET n = $TABLE.n + 1;" > /dev/null
  sample chaos "drift:added"
  sleep 10
  pg "ALTER TABLE $TABLE DROP COLUMN soak_probe;" > /dev/null
  drain chaos drift
  snapshot
  after=$(msum pg2osync_schema_drift_total)
  events_after=$(msum pg2osync_events_total)
  if [ "$after" -gt "$before" ] && [ "$events_after" -gt "$events_before" ]; then
    note "     drift counted ($before -> $after) and rows kept landing"
  else
    bad "schema drift was not counted, or the rows behind it stopped"
  fi
}

chaos_big_transaction() {
  local batches_before took
  batches_before=$(msum pg2osync_batches_flushed)
  note "$(stamp)  one transaction of $BIG_TXN_ROWS rows"
  pg "INSERT INTO $TABLE (id, payload, n)
      SELECT 4000000000 + g, 'big', 1 FROM generate_series(1, $BIG_TXN_ROWS) g
      ON CONFLICT (id) DO UPDATE SET n = $TABLE.n + 1;" > /dev/null
  sample chaos "big-txn:committed"
  drain chaos "big-txn"
  took=$DRAIN_TOOK
  snapshot
  note "     $(( $(msum pg2osync_batches_flushed) - batches_before )) batches, drained in ${took}s"
}

# Wide, incompressible values whose narrow column is then updated: under the
# default replica identity the unchanged wide value arrives as a marker and the
# engine reads the current document back to fill it in. dev/toast-cost.sh
# measures that path; here it just has to keep running for hours.
toast_pulse() {
  local base=$(( 5000000000 + RANDOM * 1000 ))
  pg "INSERT INTO $TABLE (id, payload, big, n)
      SELECT $base + g, 'toast', string_agg(md5(random()::text), ''), 1
      FROM generate_series(1, $TOAST_ROWS) g, generate_series(1, $TOAST_WIDTH / 32) c
      GROUP BY g ON CONFLICT (id) DO NOTHING;" > /dev/null
  pg "UPDATE $TABLE SET n = n + 1 WHERE id BETWEEN $base + 1 AND $base + $TOAST_ROWS;" > /dev/null
}

CHAOS_OPERATIONS=(chaos_pause_target chaos_terminate_walsender chaos_schema_drift
  chaos_big_transaction chaos_kill_restart)

# ------------------------------------------------------------------- the soak
say "2. ${TOTAL}s of load, chaos every ${CHAOS_INTERVAL}s"
printf "   %-8s %-8s %-10s %-12s %-10s %-8s\n" elapsed "rss MB" "wal lag" "retained kB" events "p99 ms"
next_chaos=$CHAOS_INTERVAL
next_toast=60
next_print=0
while [ "$(elapsed)" -lt "$TOTAL" ]; do
  sample steady ""
  if [ "$(elapsed)" -ge "$next_print" ]; then
    printf "   %-8s %-8s %-10s %-12s %-10s %-8s\n" \
      "$(elapsed)s" "$(( $(rss_kb) / 1024 ))" "$(msum pg2osync_position_lag)" \
      "$(( $(msum pg2osync_slot_retained_bytes) / 1024 ))" \
      "$(msum pg2osync_events_total)" "$(msum pg2osync_latency_ms 'quantile="0.99"')"
    next_print=$(( $(elapsed) + 120 ))
  fi
  if [ "$(elapsed)" -ge "$next_toast" ]; then
    toast_pulse
    next_toast=$(( $(elapsed) + 120 ))
  fi
  if [ "$(elapsed)" -ge "$next_chaos" ]; then
    "${CHAOS_OPERATIONS[$(( CHAOS_OPS % ${#CHAOS_OPERATIONS[@]} ))]}"
    CHAOS_OPS=$((CHAOS_OPS + 1))
    # Retaken after the operation has drained: what it cost is its own, and only
    # a quiet minute that moves this counter is a finding.
    snapshot
    SINK_BASE=$(msum pg2osync_sink_errors_total)
    next_chaos=$(( $(elapsed) + CHAOS_INTERVAL ))
  fi
  sleep "$SAMPLE_SECONDS"
done

say "3. SIGTERM: it must drain and exit 0"
stop_load
sample chaos "sigterm:begin"
EXPECTED_DOWN=1
term_start=$(now)
kill -TERM "$SYNC_PID"
term_status=0
wait "$SYNC_PID" || term_status=$?
term_took=$(python3 -c "print(int($(now) - $term_start))")
SYNC_PID=
CHAOS_OPS=$((CHAOS_OPS + 1))
DRAIN_LOG="${DRAIN_LOG}sigterm ${term_took}s
"
check "SIGTERM exited cleanly after ${term_took}s" "$term_status" "0"
start_sync
drain steady "final"

# The slot gauges come from a poll every 30 seconds, so the value standing at
# the end of a drain is up to that old — long enough for a run to accuse the
# slot of holding a backlog that was released before it was asked. Two poll
# intervals of settling, or an answer that is already low.
settle_deadline=$(( $(elapsed) + 90 ))
while [ "$(elapsed)" -lt "$settle_deadline" ]; do
  snapshot
  if [ "$(msum pg2osync_slot_retained_bytes)" -le "$RETAINED_CEILING" ]; then break; fi
  sample steady "settling"
  sleep 10
done
snapshot
SINK_BASE=$(msum pg2osync_sink_errors_total)

# The load is over, so these can be compared: the row count is the answer and a
# document count that never reaches it is the loss this run exists to detect.
rows=$(pg "SELECT count(*) FROM $TABLE;")
docs=0
for _ in $(seq 1 180); do
  os_refresh
  docs=$(os_count)
  if [ "$docs" -ge "$rows" ]; then break; fi
  sleep 1
done
sample steady "final-count"
snapshot

say "4. what the run saw"
final_retained=$(msum pg2osync_slot_retained_bytes)
printf "   %-26s %s\n" "samples" "$SAMPLES over $(elapsed)s"
printf "   %-26s %s MB (ceiling $RSS_CEILING MB)\n" "peak rss" "$((PEAK_RSS / 1024))"
printf "   %-26s %s bytes\n" "peak wal lag" "$PEAK_LAG"
printf "   %-26s %s kB (now $((final_retained / 1024)) kB)\n" "peak retained wal" "$((PEAK_RETAINED / 1024))"
printf "   %-26s %s\n" "rows in the source" "$rows"
printf "   %-26s %s\n" "documents in the index" "$docs"
printf "   %-26s %s\n" "events, all processes" "$((CARRY_EVENTS + LAST_EVENTS))"
printf "   %-26s %s\n" "toast read-backs" "$((CARRY_READBACKS + LAST_READBACKS))"
printf "   %-26s %s\n" "reconnects" "$((CARRY_RECONNECTS + LAST_RECONNECTS))"
printf "   %-26s %s\n" "schema drift" "$((CARRY_DRIFT + LAST_DRIFT))"
printf "   %-26s %s\n" "chaos operations" "$CHAOS_OPS"
printf "   %-26s\n" "drain times"
printf '%s' "$DRAIN_LOG" | sed 's/^/     /'

say "5. what has to have held"
check "every row reached the index" "$docs" "$rows"
if [ "$((PEAK_RSS / 1024))" -le "$RSS_CEILING" ]; then
  ok "memory stayed bounded (peak $((PEAK_RSS / 1024)) MB, ceiling $RSS_CEILING MB)"
else
  bad "memory grew past the ceiling (peak $((PEAK_RSS / 1024)) MB, ceiling $RSS_CEILING MB)"
fi
if [ "$final_retained" -le "$RETAINED_CEILING" ]; then
  ok "the slot released its backlog ($((final_retained / 1024)) kB retained, \
peak $((PEAK_RETAINED / 1024)) kB)"
else
  bad "the slot is still retaining $((final_retained / 1024)) kB after the final drain"
fi
check "the slot was never lost" "$WAL_LOST_SEEN" "0"
check "no sink errors outside a chaos window" "$CALM_SINK_ERRORS" "0"
check "the process only ever went down on request" "$UNEXPECTED_EXITS" "0"
check "/healthz answered while the pipeline was up" "$HEALTH_FAILS" "0"
if [ "$((CARRY_READBACKS + LAST_READBACKS))" -gt 0 ]; then
  ok "unchanged TOASTed columns were read back ($((CARRY_READBACKS + LAST_READBACKS)))"
else
  bad "no TOAST read-back happened, so that path was never exercised"
fi

printf "\n   timeline: %s\n" "$CSV"
printf "\033[1mRESULT: %d passed, %d failed\033[0m\n" "$PASS" "$FAIL"
[ "$FAIL" = 0 ]
