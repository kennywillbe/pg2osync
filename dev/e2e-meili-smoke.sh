#!/usr/bin/env bash
# Smoke suite for the PostgreSQL -> Meilisearch pipeline.
#
# Meilisearch cannot run dev/e2e-test.sh: that suite asserts over mappings,
# joins and per-row indices, none of which this target has. What is left is
# what Meilisearch does support — an initial load, live changes, and the
# file-based checkpoint that stands in for the hidden meta index.
#
# Runs one at a time: stopping the pipeline kills every pg2osync process, so
# two suites at once take each other's down and report failures that are not.
#
# Prerequisites:
#   a PostgreSQL with logical replication, seeded with dev/seed.sql
#   a Meilisearch reachable at MEILI_URL
#   cargo build --release
#
# Usage: ./dev/e2e-meili-smoke.sh
#   MEILI_URL         Meilisearch base URL  (default http://localhost:7700)
#   MEILI_MASTER_KEY  master key            (default e2e-master-key)
#   PG_CONTAINER      psql container name   (default dev-postgres-1)
set -euo pipefail

cd "$(dirname "$0")/.."
BIN=./target/release/pg2osync
MEILI=${MEILI_URL:-http://localhost:7700}
PG_CONTAINER=${PG_CONTAINER:-dev-postgres-1}
# the sink reads the key from this variable's name, so it has to be exported
export MEILI_MASTER_KEY=${MEILI_MASTER_KEY:-e2e-master-key}
SLOT=pg2osync_e2e_meili
INDEX=e2e_meili_users
# BSD mktemp only substitutes X's at the *end* of the template: with a
# suffix after them it creates the literal name instead, and one killed run
# then breaks every later one with "File exists".
CONFIG=$(mktemp /tmp/pg2osync-meili.XXXXXX)
STATE_DIR=$(mktemp -d /tmp/pg2osync-meili-state.XXXXXX)
LOG=/tmp/pg2osync-meili-smoke.log
export PG2OSYNC_SOURCE_URL="postgres://postgres:postgres@localhost:15432/sourcedb"
PASS=0; FAIL=0

say()   { printf "\n\033[1m== %s ==\033[0m\n" "$1"; }
ok()    { printf "  \033[32m✓ %s\033[0m\n" "$1"; PASS=$((PASS + 1)); }
bad()   { printf "  \033[31m✗ %s\033[0m\n" "$1"; FAIL=$((FAIL + 1)); }
check() { if [ "$2" = "$3" ]; then ok "$1 ($2)"; else bad "$1 (got '$2', want '$3')"; fi }

jqf()      { python3 -c "import sys,json;d=json.load(sys.stdin);print($1)"; }
meili()    { curl -s -H "Authorization: Bearer $MEILI_MASTER_KEY" "$@"; }
mi_count() { meili "$MEILI/indexes/$INDEX/documents?limit=0" | jqf "d.get('total', 0)"; }
mi_field() { meili "$MEILI/indexes/$INDEX/documents/$1" | jqf "d.get('$2', '<missing>')"; }
mi_has()   { meili "$MEILI/indexes/$INDEX/documents/$1" | jqf "'$2' in d"; }
mi_status() { curl -s -o /dev/null -w "%{http_code}" -H "Authorization: Bearer $MEILI_MASTER_KEY" "$MEILI/indexes/$INDEX/documents/$1"; }
pg()       { docker exec "$PG_CONTAINER" psql -U postgres -d sourcedb -qtAc "$1"; }

start_sync() { nohup $BIN run -c "$CONFIG" >> "$LOG" 2>&1 < /dev/null & disown; }
# A restart has to wait for the old process to let go of the replication slot:
# starting on a slot the previous run still holds fails outright, and the
# failure looks exactly like a checkpoint that did not resume.
stop_sync() {
  pkill -f "pg2osync run" 2> /dev/null || true
  for _ in $(seq 1 30); do
    pgrep -f "pg2osync run" > /dev/null || break
    sleep 1
  done
  for _ in $(seq 1 30); do
    [ "$(pg "SELECT count(*) FROM pg_replication_slots WHERE slot_name='$SLOT' AND active;")" = "0" ] && break
    sleep 1
  done
}
# Meilisearch applies a write as an asynchronous task, so a document appears
# some time after the sink acknowledged it; every assertion polls for it.
await_count() {
  for _ in $(seq 1 60); do
    [ "$(mi_count)" = "$1" ] && break
    sleep 1
  done
}
await_field() {
  for _ in $(seq 1 60); do
    [ "$(mi_field "$1" "$2")" = "$3" ] && break
    sleep 1
  done
}
cleanup() {
  stop_sync
  pg "SELECT pg_drop_replication_slot('$SLOT') WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name='$SLOT');" > /dev/null 2>&1 || true
  pg "DROP PUBLICATION IF EXISTS ${SLOT}_pub;" > /dev/null 2>&1 || true
  rm -rf "$CONFIG" "$STATE_DIR"
}
trap cleanup EXIT

cat > "$CONFIG" <<TOML
[source]
url_env = "PG2OSYNC_SOURCE_URL"
slot_name = "$SLOT"
publication = "${SLOT}_pub"

[target]
flavor = "meilisearch"
url = "$MEILI"
api_key_env = "MEILI_MASTER_KEY"
state_dir = "$STATE_DIR"

[metrics]
bind = "127.0.0.1:9121"

[sync.users]
table = "public.users"
index = "$INDEX"
exclude_columns = ["password_hash"]
TOML

say "0. Reset state"
stop_sync
pg "DROP PUBLICATION IF EXISTS ${SLOT}_pub;" > /dev/null
pg "SELECT pg_drop_replication_slot('$SLOT') WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name='$SLOT');" > /dev/null
pg "TRUNCATE users;" > /dev/null
pg "INSERT INTO users (id,name,email,password_hash) VALUES
      (1,'alice','alice@test.io','secret-1'),
      (2,'bob','bob@test.io','secret-2'),
      (3,'carol','carol@test.io','secret-3');" > /dev/null
meili -XDELETE "$MEILI/indexes/$INDEX" > /dev/null
rm -f "$STATE_DIR"/*.json
: > "$LOG"
ok "seeded 3 users, index and checkpoint cleared"

say "1. validate"
if $BIN validate -c "$CONFIG" 2>&1 | grep -q "all checks passed"; then
  ok "validate passes"
else
  bad "validate failed"
fi

say "2. initial load"
start_sync
await_count 3
check "every row is indexed" "$(mi_count)" "3"
check "a value arrives intact" "$(mi_field 1 name)" "alice"
check "an excluded column is absent" "$(mi_has 1 password_hash)" "False"

say "3. live streaming"
pg "INSERT INTO users (id,name,email) VALUES (4,'dave','dave@test.io');" > /dev/null
await_count 4
check "INSERT propagated" "$(mi_count)" "4"
pg "UPDATE users SET name='dave-renamed' WHERE id=4;" > /dev/null
await_field 4 name dave-renamed
check "UPDATE propagated" "$(mi_field 4 name)" "dave-renamed"
pg "DELETE FROM users WHERE id=4;" > /dev/null
await_count 3
check "DELETE propagated" "$(mi_status 4)" "404"

say "4. the file checkpoint resumes instead of reloading"
stop_sync
if [ -n "$(ls -A "$STATE_DIR" 2>/dev/null)" ]; then
  ok "a checkpoint file was written to the state directory"
else
  bad "the state directory is empty"
fi
# the source moves while nothing is watching it: a reload would find these
# anyway, so the log line below is what separates a resume from a reload
pg "INSERT INTO users (id,name,email) VALUES (5,'erin','erin@test.io');" > /dev/null
pg "UPDATE users SET name='bob-while-down' WHERE id=2;" > /dev/null
: > "$LOG"
start_sync
await_count 4
check "a row added while down arrived" "$(mi_field 5 name)" "erin"
check "a row updated while down is current" "$(mi_field 2 name)" "bob-while-down"
if grep -q "resuming from checkpoint" "$LOG"; then
  ok "the pipeline resumed from the file checkpoint"
else
  bad "the pipeline did not resume from the file checkpoint"
fi
stop_sync

printf "\n\033[1m%d passed, %d failed\033[0m\n" "$PASS" "$FAIL"
# the pipeline's own log is the only place a failure explains itself, and on CI
# nobody can go and read the file afterwards
[ "$FAIL" -eq 0 ] || tail -40 "$LOG"
[ "$FAIL" -eq 0 ]
