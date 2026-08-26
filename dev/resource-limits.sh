#!/usr/bin/env bash
# How many cores does the pipeline need, and what happens when it does not get
# them?
#
# `db-load-impact.sh` found that running the database, the pipeline and the
# target on one laptop costs the database 40% of its foreground throughput while
# logical decoding itself costs 0.4%. That is a scheduling result, and it is only
# useful once it becomes advice: give the pipeline its own cores, this many. So
# the pipeline runs in its own container here, capped with `--cpus`.
#
# The latency blocks are kept, and their numbers must not be quoted. Four
# attempts at measuring a distant target on this stack produced four different
# answers, and the last two — reproducible to within 3% — say that adding 50 ms
# of delay makes the initial load *faster*. That cannot be a latency effect. It
# is what saturation looks like: everything shares eight cores, so pacing one
# participant relieves the others and total throughput rises. Measuring what a
# network costs needs the target on hardware that is not already the bottleneck,
# which is not this machine. The harness is left correct and the conclusion is
# left unmade.
#
set -euo pipefail

cd "$(dirname "$0")/.."
IMAGE=${IMAGE:-pg2osync:limits}
NETWORK=${NETWORK:-dev_default}
PG_CONTAINER=${PG_CONTAINER:-dev-postgres-1}
OS_CONTAINER=${OS_CONTAINER:-dev-opensearch-1}
ROWS=${ROWS:-500000}
CPUS=${CPUS:-"0.25 0.5 1 2 4"}
LATENCY_MS=${LATENCY_MS:-"0 10 50"}
# Configurations to try against a distant target, and how many times each.
LATENCY_SPECS=${LATENCY_SPECS:-"4:500 16:2000"}
REPEATS=${REPEATS:-3}
SLOT=pg2osync_limits
TABLE=limits_probe
INDEX=limits_probe
RUNNER=pg2osync-limits-run
NETEM_IMAGE=pg2osync-netem:local
CONFIG_DIR=$(mktemp -d /tmp/pg2osync-limits.XXXXXX)
# The pipeline waits for a file here before it starts. Shaping traffic means
# running two helper containers, which takes seconds; a timed grace period let
# the load begin during that window and the clock started after it, so the
# shaped phases came out *faster* than the unshaped ones. A sentinel makes
# starting late impossible rather than unlikely.
GO_DIR=$(mktemp -d /tmp/pg2osync-limits-go.XXXXXX)

pg()  { docker exec "$PG_CONTAINER" psql -U postgres -d sourcedb -qtAc "$1"; }
now() { python3 -c 'import time;print(time.time())'; }
say() { printf "\n\033[1m== %s ==\033[0m\n" "$1"; }
osq() { curl -s "http://localhost:9200/$1"; }

drop_slot() { pg "SELECT pg_drop_replication_slot('$SLOT') WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name='$SLOT');" > /dev/null 2>&1 || true; }
stop_runner() { docker rm -f "$RUNNER" > /dev/null 2>&1 || true; }

cleanup() {
  stop_runner
  drop_slot
  pg "DROP PUBLICATION IF EXISTS ${SLOT}_pub; DROP TABLE IF EXISTS $TABLE;" > /dev/null 2>&1 || true
  rm -rf "$CONFIG_DIR" "$GO_DIR"
}
trap cleanup EXIT

# Inside the network the containers address each other by name, so the pipeline
# reaches the database and the target exactly as a deployed one would.
write_config() {
  local path=$1 concurrency=${2:-4} batch=${3:-500}
  cat > "$path" <<TOML
[source]
url_env = "PG2OSYNC_SOURCE_URL"
slot_name = "$SLOT"
publication = "${SLOT}_pub"

[engine]
write_concurrency = $concurrency
batch_size = $batch

[target]
url = "http://$OS_CONTAINER:9200"

[metrics]
enabled = false

[sync.$INDEX]
table = "public.$TABLE"
index = "$INDEX"
TOML
}

# Build the helper that can shape traffic. The runtime image is a static binary
# on Alpine with no iproute2, and asking it for `tc` fails quietly.
build_netem() {
  docker image inspect "$NETEM_IMAGE" > /dev/null 2>&1 && return
  printf 'FROM alpine:3.20\nRUN apk add --no-cache iproute2\n' \
    | docker build -q -t "$NETEM_IMAGE" - > /dev/null
}

# Start the pipeline in a container, capped, and hold it before it connects so a
# delay can be applied to the namespace it is about to use.
start_runner() {
  local cpus=$1
  stop_runner
  rm -f "$GO_DIR/start"
  docker run -d --name "$RUNNER" --network "$NETWORK" --cpus "$cpus" \
    -e PG2OSYNC_SOURCE_URL="postgres://postgres:postgres@$PG_CONTAINER:5432/sourcedb" \
    -v "$CONFIG_DIR:/etc/pg2osync:ro" -v "$GO_DIR:/go:ro" \
    --entrypoint sh "$IMAGE" -c \
    "while [ ! -f /go/start ]; do sleep 0.05; done; \
     exec pg2osync run -c /etc/pg2osync/config.toml" > /dev/null
}

# Add one-way delay to the running container's interface, from outside it.
#
# Returns what the kernel says is in place, so a phase that failed to shape
# anything cannot be mistaken for a phase that measured no effect — which is
# exactly what happened when this was attempted inside the runtime image.
apply_delay() {
  local delay_ms=$1
  [ "$delay_ms" = "0" ] && { echo "none"; return; }
  build_netem
  docker run --rm --net "container:$RUNNER" --cap-add=NET_ADMIN \
    --entrypoint tc "$NETEM_IMAGE" \
    qdisc add dev eth0 root netem delay "${delay_ms}ms" > /dev/null 2>&1 || {
      echo "FAILED"
      return
    }
  docker run --rm --net "container:$RUNNER" --cap-add=NET_ADMIN \
    --entrypoint tc "$NETEM_IMAGE" qdisc show dev eth0 2>/dev/null \
    | grep -o "delay [0-9.]*m*s" | head -1
}

# Wait for the load, and report what it cost. A cap low enough to matter shows
# up as wall time; one low enough to break something shows up as a timeout.
#
# The clock starts once the container is shaped and past its grace period, so
# neither the handshake nor the qdisc is counted as load time.
time_load() {
  local label=$1 cpus=$2 delay_ms=$3
  drop_slot
  curl -s -XDELETE "http://localhost:9200/$INDEX,.pg2osync_meta?ignore_unavailable=true" > /dev/null
  local shaped start end indexed peak=0 rss
  start_runner "$cpus"
  shaped=$(apply_delay "$delay_ms")
  if [ "$delay_ms" != "0" ]; then
    case "$shaped" in
      delay*) ;;
      *) echo "  $label: could not shape traffic ($shaped); not measured"; stop_runner; return ;;
    esac
  fi
  # Everything is in place, so release the pipeline and start the clock in the
  # same breath. Both phases pay exactly the same setup: none of it.
  start=$(now)
  : > "$GO_DIR/start"
  indexed=0
  for _ in $(seq 1 1200); do
    curl -s -XPOST "http://localhost:9200/$INDEX/_refresh" > /dev/null 2>&1 || true
    indexed=$(osq "$INDEX/_count" | python3 -c 'import sys,json;print(json.load(sys.stdin).get("count",0))' 2>/dev/null || echo 0)
    rss=$(docker stats --no-stream --format '{{.MemUsage}}' "$RUNNER" 2>/dev/null | awk '{print $1}' | sed 's/MiB//;s/GiB/*1024/' | bc 2>/dev/null || echo 0)
    rss=${rss%%.*}
    if [ "${rss:-0}" -gt "$peak" ] 2> /dev/null; then peak=$rss; fi
    [ "$indexed" -ge "$ROWS" ] && break
    sleep 0.5
  done
  end=$(now)
  python3 -c '
import sys
label, indexed, expected, peak, start, end, shaped = (
    sys.argv[1], int(sys.argv[2]), int(sys.argv[3]), sys.argv[4],
    float(sys.argv[5]), float(sys.argv[6]), sys.argv[7],
)
secs = end - start
note = "" if indexed >= expected else f"  INCOMPLETE ({indexed:,}/{expected:,})"
shape = "" if shaped in ("none", "") else f"   [{shaped}]"
print(f"  {label:<24} {secs:7.1f}s   {indexed/secs:>9,.0f} rows/s   peak {peak} MB{shape}{note}")
' "$label" "$indexed" "$ROWS" "$peak" "$start" "$end" "$shaped"
  stop_runner
}

say "setup: $ROWS rows, pipeline in its own container on $NETWORK"
docker image inspect "$IMAGE" > /dev/null 2>&1 || {
  echo "  $IMAGE is missing. Build it first: docker build -t $IMAGE ."
  exit 1
}
stop_runner
drop_slot
pg "DROP TABLE IF EXISTS $TABLE;" > /dev/null 2>&1
pg "CREATE TABLE $TABLE(id bigint primary key, name text, email text, payload jsonb);" > /dev/null
pg "INSERT INTO $TABLE SELECT g, 'user_'||g, 'u'||g||'@example.com',
      jsonb_build_object('tier', g % 5, 'tags', ARRAY['a','b'])
    FROM generate_series(1,$ROWS) g;" > /dev/null
pg "ANALYZE $TABLE;" > /dev/null
pg "DROP PUBLICATION IF EXISTS ${SLOT}_pub; CREATE PUBLICATION ${SLOT}_pub FOR TABLE $TABLE;" > /dev/null
write_config "$CONFIG_DIR/config.toml"
echo "  ready"

say "1. how many cores it needs"
for cpus in $CPUS; do
  time_load "--cpus=$cpus" "$cpus" 0
done

say "2. latency, which this machine cannot isolate — see the header"
echo "  (at 2 cores, so the cap is not what is being measured)"
for delay in $LATENCY_MS; do
  # netem delay is one-way, so the round trip is twice this
  time_load "${delay}ms one way" 2 "$delay"
done

say "3. the same thing, interleaved, which does not rescue it either"
# Interleaved and repeated, not blocked, because the first two attempts at this
# produced tables that disagreed with each other: identical configurations
# measured 40% apart when run in different blocks, while repeats inside a block
# agreed to within 1%. Something slow-moving on this stack — OpenSearch segment
# state after twenty index create/delete cycles is the likeliest — drifts between
# blocks. So every comparison here is adjacent, and the opening case is repeated
# at the end to show how far the ground moved underneath it.
for round in 1 2; do
  write_config "$CONFIG_DIR/config.toml" 4 500
  time_load "near, conc=4  (round $round)" 2 0
  time_load "far,  conc=4  (round $round)" 2 50
  write_config "$CONFIG_DIR/config.toml" 16 500
  time_load "far,  conc=16 (round $round)" 2 50
done
write_config "$CONFIG_DIR/config.toml"

say "reading this"
echo "  The first block is the number to put in a deployment guide: the point"
echo "  where more cores stop buying anything is what the pipeline needs, and"
echo "  anything below it is where a starved pipeline lands — check the peak"
echo "  memory column there, because falling behind must not mean growing."
echo "  The second block is a source and target across a network. The delay in"
echo "  brackets is read back from the kernel after it was applied, because a"
echo "  phase that shaped nothing would otherwise read as a phase that found no"
echo "  effect. Both connections pay it: replication and the writes to the target."
echo "  Blocks 2 and 3 are a negative result, not a measurement. Interleaving and"
echo "  repeating removed the noise and the head start, and what survived is that"
echo "  50 ms of added delay makes the load faster and more concurrency makes it"
echo "  slower. Both are contention, not latency. What a distant source or target"
echo "  costs needs a target that is not already the bottleneck — separate hosts,"
echo "  and ideally shaping only the source connection so the two can be told"
echo "  apart."
