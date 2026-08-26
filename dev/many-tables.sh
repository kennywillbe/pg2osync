#!/usr/bin/env bash
# What does a table cost, apart from its rows?
#
# Every measurement here is taken against one or two tables, and the documented
# way to scale is "split tables across instances" — advice with no number behind
# it. This loads the same number of rows twice, once as a single table and once
# spread over many, so the difference is the per-table cost rather than the
# per-row cost. Then it streams writes across all of them, and commits one
# transaction that touches every one, which is where per-transaction bookkeeping
# would show up if it were going to.
#
# Prerequisites:
#   docker compose -f dev/docker-compose.yml up -d
#   cargo build --release
#
# Usage: [TABLES=50] [ROWS_TOTAL=500000] [CHILDREN=5] ./dev/many-tables.sh
set -euo pipefail

cd "$(dirname "$0")/.."
BIN=./target/release/pg2osync
OS=${OS_URL:-http://localhost:9200}
PG_CONTAINER=${PG_CONTAINER:-dev-postgres-1}
TABLES=${TABLES:-50}
ROWS_TOTAL=${ROWS_TOTAL:-500000}
CHILDREN=${CHILDREN:-5}
SLOT=pg2osync_manytables
CONFIG=$(mktemp /tmp/pg2osync-many.XXXXXX)
LOG=/tmp/pg2osync-many.log
export PG2OSYNC_SOURCE_URL="postgres://postgres:postgres@localhost:15432/sourcedb"

pg()  { docker exec "$PG_CONTAINER" psql -U postgres -d sourcedb -qtAc "$1"; }
now() { python3 -c 'import time;print(time.time())'; }
say() { printf "\n\033[1m== %s ==\033[0m\n" "$1"; }
stop_sync() { pkill -f "pg2osync run" 2> /dev/null || true; sleep 1; }
drop_slot() { pg "SELECT pg_drop_replication_slot('$SLOT') WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name='$SLOT');" > /dev/null 2>&1 || true; }

cleanup() {
  stop_sync
  drop_slot
  pg "DROP PUBLICATION IF EXISTS ${SLOT}_pub;" > /dev/null 2>&1 || true
  pg "DO \$\$ DECLARE t text; BEGIN FOR t IN SELECT tablename FROM pg_tables WHERE tablename LIKE 'many_%' LOOP EXECUTE format('DROP TABLE IF EXISTS %I CASCADE', t); END LOOP; END \$\$;" > /dev/null 2>&1 || true
  rm -f "$CONFIG"
}
trap cleanup EXIT

# Indices are deleted by prefix so a re-run starts clean without listing them.
drop_indices() {
  curl -s -XDELETE "$OS/many_*,.pg2osync_meta?ignore_unavailable=true" > /dev/null
}

# Peak resident memory of the pipeline, in MB.
rss_mb() {
  local pid=$1
  ps -o rss= -p "$pid" 2> /dev/null | awk '{printf "%.0f", $1/1024}' || echo "?"
}

# Run a load to completion and report how long it took.
#
# Waits on the documents rather than on the log, so it measures what an operator
# would see: the point at which the index holds the table. Sets LOADED_ROWS and
# prints one line; values go to python as arguments rather than being spliced
# into its source, which is where the first version of this went wrong.
LOADED_ROWS=0
load_and_time() {
  local expected=$1 label=$2
  drop_slot
  drop_indices
  local start pid indexed peak rss end
  start=$(now)
  nohup $BIN run -c "$CONFIG" > "$LOG" 2>&1 < /dev/null & disown
  pid=$(pgrep -f "pg2osync run" | head -1)
  peak=0
  indexed=0
  for _ in $(seq 1 600); do
    curl -s -XPOST "$OS/many_*/_refresh" > /dev/null 2>&1 || true
    indexed=$(curl -s "$OS/many_*/_count" | python3 -c 'import sys,json;print(json.load(sys.stdin).get("count",0))' 2>/dev/null || echo 0)
    rss=$(rss_mb "$pid")
    if [ "${rss:-0}" -gt "$peak" ] 2> /dev/null; then peak=$rss; fi
    if [ "$indexed" -ge "$expected" ]; then break; fi
    sleep 0.5
  done
  end=$(now)
  LOADED_ROWS=$indexed
  python3 -c '
import sys
label, indexed, peak, start, end = sys.argv[1], int(sys.argv[2]), sys.argv[3], float(sys.argv[4]), float(sys.argv[5])
secs = end - start
print(f"  {label:<28} {secs:6.1f}s   {indexed/secs:>9,.0f} rows/s   peak RSS {peak} MB")
' "$label" "$indexed" "$peak" "$start" "$end"
}

say "setup"
stop_sync
cleanup > /dev/null 2>&1 || true
per_table=$((ROWS_TOTAL / TABLES))
echo "  $TABLES tables x $per_table rows, and one table with $ROWS_TOTAL rows"
echo "  $CHILDREN of the many will carry a child collection"

# The single-table case, for the comparison.
pg "CREATE TABLE many_one(id bigint primary key, v text, n int);" > /dev/null
pg "INSERT INTO many_one SELECT g, 'v'||g, g % 1000 FROM generate_series(1,$ROWS_TOTAL) g;" > /dev/null

# The many-table case: same rows, spread out.
for i in $(seq 1 "$TABLES"); do
  pg "CREATE TABLE many_t$i(id bigint primary key, v text, n int);" > /dev/null
  pg "INSERT INTO many_t$i SELECT g, 'v'||g, g % 1000 FROM generate_series(1,$per_table) g;" > /dev/null
done
# A few child tables, because a collection is the one thing that costs a query
# per parent rather than nothing. Guarded because BSD `seq 1 0` counts *down*
# and emits "1 0", which would create tables the config knows nothing about.
for i in $(if [ "$CHILDREN" -gt 0 ]; then seq 1 "$CHILDREN"; fi); do
  pg "CREATE TABLE many_kid$i(id bigserial primary key, parent_id bigint, v text);" > /dev/null
  pg "INSERT INTO many_kid$i(parent_id, v) SELECT 1 + (g % $per_table), 'k'||g FROM generate_series(1,$((per_table * 2))) g;" > /dev/null
  pg "CREATE INDEX ON many_kid$i(parent_id);" > /dev/null
done
pg "ANALYZE;" > /dev/null
echo "  rows in place"

config_for_one() {
  cat > "$CONFIG" <<TOML
[source]
url_env = "PG2OSYNC_SOURCE_URL"
slot_name = "$SLOT"
publication = "${SLOT}_pub"

[engine]
write_concurrency = 4

[target]
url = "$OS"

[metrics]
enabled = false

[sync.many_one]
table = "public.many_one"
index = "many_one"
TOML
  pg "DROP PUBLICATION IF EXISTS ${SLOT}_pub; CREATE PUBLICATION ${SLOT}_pub FOR TABLE many_one;" > /dev/null
}

config_for_many() {
  {
    cat <<TOML
[source]
url_env = "PG2OSYNC_SOURCE_URL"
slot_name = "$SLOT"
publication = "${SLOT}_pub"

[engine]
write_concurrency = 4

[target]
url = "$OS"

[metrics]
enabled = false
TOML
    for i in $(seq 1 "$TABLES"); do
      printf '\n[sync.many_t%s]\ntable = "public.many_t%s"\nindex = "many_t%s"\n' "$i" "$i" "$i"
      if [ "$i" -le "$CHILDREN" ]; then
        printf '\n[[sync.many_t%s.children]]\ntable = "public.many_kid%s"\nfield = "kids"\nforeign_key = "parent_id"\n' "$i" "$i"
      fi
    done
  } > "$CONFIG"
  local list
  list=$(for i in $(seq 1 "$TABLES"); do printf "many_t%s," "$i"; done)
  list="$list$(for i in $(if [ "$CHILDREN" -gt 0 ]; then seq 1 "$CHILDREN"; fi); do printf "many_kid%s," "$i"; done)"
  pg "DROP PUBLICATION IF EXISTS ${SLOT}_pub; CREATE PUBLICATION ${SLOT}_pub FOR TABLE ${list%,};" > /dev/null
}

say "1. the same rows, one table against $TABLES"
config_for_one
load_and_time "$ROWS_TOTAL" "1 table"
one_rows=$LOADED_ROWS
stop_sync
config_for_many
# the child collections add their rows to no index, so the expected count is
# still the parents' rows
load_and_time "$ROWS_TOTAL" "$TABLES tables"
many_rows=$LOADED_ROWS
stop_sync

say "2. bootstrap alone, with $TABLES tables and indices"
drop_slot
drop_indices
start=$(now)
$BIN bootstrap -c "$CONFIG" > /dev/null 2>&1
end=$(now)
python3 -c "print(f'  creating the slot, publication and $TABLES indices: {$end - $start:.1f}s')"

say "3. a write reaching every table, end to end"
drop_slot
drop_indices
nohup $BIN run -c "$CONFIG" > "$LOG" 2>&1 < /dev/null & disown
for _ in $(seq 1 600); do
  curl -s -XPOST "$OS/many_*/_refresh" > /dev/null 2>&1 || true
  n=$(curl -s "$OS/many_*/_count" | python3 -c "import sys,json;print(json.load(sys.stdin).get('count',0))" 2>/dev/null || echo 0)
  [ "$n" -ge "$ROWS_TOTAL" ] && break
  sleep 0.5
done
echo "  loaded, now writing"
# One row into each table, round robin, so every relation stays warm and the
# decoder cannot amortise over a single hot table.
#
# This is not a throughput figure and must not be quoted as one: the clock
# includes the plpgsql loop generating the rows, which is the database's work
# rather than the pipeline's. What it does show is that spreading writes over
# fifty relations does not fall off a cliff — `load-test.sh` is where
# throughput is measured.
start=$(now)
pg "DO \$\$
DECLARE i int; t int;
BEGIN
  FOR i IN 1..200 LOOP
    FOR t IN 1..$TABLES LOOP
      EXECUTE format('INSERT INTO many_t%s(id, v, n) VALUES (%s, %L, %s)',
                     t, 1000000 + i, 'streamed', i);
    END LOOP;
  END LOOP;
END \$\$;" > /dev/null
written=$((200 * TABLES))
target=$((ROWS_TOTAL + written))
for _ in $(seq 1 600); do
  curl -s -XPOST "$OS/many_*/_refresh" > /dev/null 2>&1 || true
  n=$(curl -s "$OS/many_*/_count" | python3 -c "import sys,json;print(json.load(sys.stdin).get('count',0))" 2>/dev/null || echo 0)
  [ "$n" -ge "$target" ] && break
  sleep 0.2
done
end=$(now)
python3 -c "print(f'  $written rows across $TABLES tables, written and indexed: {$end - $start:.1f}s')"

say "4. one transaction touching every table"
start=$(now)
pg "BEGIN;
$(for i in $(seq 1 "$TABLES"); do echo "INSERT INTO many_t$i(id, v, n) VALUES (2000000, 'one-txn', 1);"; done)
COMMIT;" > /dev/null
target=$((target + TABLES))
for _ in $(seq 1 300); do
  curl -s -XPOST "$OS/many_*/_refresh" > /dev/null 2>&1 || true
  n=$(curl -s "$OS/many_*/_count" | python3 -c "import sys,json;print(json.load(sys.stdin).get('count',0))" 2>/dev/null || echo 0)
  [ "$n" -ge "$target" ] && break
  sleep 0.1
done
end=$(now)
python3 -c "print(f'  {$TABLES} tables in one commit: propagated in {$end - $start:.2f}s')"
pid=$(pgrep -f "pg2osync run" | head -1)
echo "  resident memory now: $(rss_mb "$pid") MB"
stop_sync

say "what a table costs"
echo "  Same $one_rows rows against $many_rows, one table versus $TABLES — the two"
echo "  lines under section 1 are the comparison. Whatever gap they show is fixed"
echo "  cost per table: a boundary sample, a column lookup, an index to create and"
echo "  a progress document to write, once each, however few rows the table holds."
