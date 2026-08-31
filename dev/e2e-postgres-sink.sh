#!/usr/bin/env bash
# End-to-end suite for the PostgreSQL -> pgvector pipeline.
#
# The target here is a database, so everything the other suites read over HTTP
# is read with psql instead. What it proves is the part of the contract that is
# this target's own: the operator's DDL creates the table, documents land in
# columns by name, an embedding the *source* produced arrives in a `vector`
# column and orders a nearest-neighbour query, the checkpoint lives in a state
# table of the target database, and the features that belong to a search engine
# are refused by name instead of ignored.
#
# It also runs the sink conformance kit (crates/sink/tests/conformance.rs)
# against the same database, which is where the contract shared with every
# other target is asserted.
#
# One suite at a time per stack: the slot, the tables and the state table are
# fixed, so two suites against the same pair overwrite each other's state.
# dev/e2e-lock.sh enforces that on the shared dev stack with a machine-wide
# lock; a run with a stack of its own — ci-local --isolated — passes
# E2E_LOCK=none, because a stop only ever signals the pipelines this run
# started (dev/e2e-pipeline.sh).
#
# Prerequisites:
#   docker compose -f dev/docker-compose.yml up -d
#   docker exec -i dev-postgres-1 psql -U postgres -d sourcedb < dev/seed.sql
#   cargo build --release
#
# Usage: ./dev/e2e-postgres-sink.sh
#   PG_CONTAINER      source psql container  (default dev-postgres-1)
#   PG_PORT           source port            (default 15432)
#   SINK_CONTAINER    target psql container  (default dev-postgres-sink-1)
#   SINK_PORT         target port            (default 15434)
#   E2E_LOG           pipeline log file      (default /tmp/pg2osync-pgsink.log)
#   E2E_LOCK          lock directory, or none (default /tmp/pg2osync-e2e.lock)
#   E2E_PORT_BASE     first metrics port     (default 9100, a 40-port block)
set -euo pipefail
# shellcheck source=dev/e2e-lock.sh
source "$(dirname "$0")/e2e-lock.sh"
# shellcheck source=dev/e2e-pipeline.sh
source "$(dirname "$0")/e2e-pipeline.sh"
e2e_lock

cd "$(dirname "$0")/.."
BIN=./target/release/pg2osync
PG_CONTAINER=${PG_CONTAINER:-dev-postgres-1}
PG_PORT=${PG_PORT:-15432}
SINK_CONTAINER=${SINK_CONTAINER:-dev-postgres-sink-1}
SINK_PORT=${SINK_PORT:-15434}
SLOT=pg2osync_e2e_pgsink
TABLE=e2e_pgsink_docs
# BSD mktemp only substitutes X's at the *end* of the template: with a suffix
# after them it creates the literal name instead, and one killed run then
# breaks every later one with "File exists".
WORK=$(mktemp -d /tmp/pg2osync-pgsink.XXXXXX)
CONFIG=$WORK/pg2osync.toml
REFUSED_CONFIG=$WORK/refused.toml
DDL=$WORK/docs.sql
LOG=${E2E_LOG:-/tmp/pg2osync-pgsink.log}
export PG2OSYNC_SOURCE_URL="postgres://postgres:postgres@localhost:$PG_PORT/sourcedb"
# The sink reads the target URL from this variable's name, so it has to be
# exported: a database URL carries its password, which is why it is not in the
# config file.
export PG2OSYNC_TARGET_URL="postgres://postgres:postgres@localhost:$SINK_PORT/targetdb"
# Every pipeline the suite starts binds its metrics port on this machine rather
# than inside a container, so two suites at once need two blocks.
PORT_BASE=${E2E_PORT_BASE:-9100}

PASS=0; FAIL=0

say()   { printf "\n\033[1m== %s ==\033[0m\n" "$1"; }
ok()    { printf "  \033[32m✓ %s\033[0m\n" "$1"; PASS=$((PASS + 1)); }
bad()   { printf "  \033[31m✗ %s\033[0m\n" "$1"; FAIL=$((FAIL + 1)); }
check() { if [ "$2" = "$3" ]; then ok "$1 ($2)"; else bad "$1 (got '$2', want '$3')"; fi }

pg()   { docker exec "$PG_CONTAINER" psql -U postgres -d sourcedb -qtAc "$1"; }
sink() { docker exec "$SINK_CONTAINER" psql -U postgres -d targetdb -qtAc "$1"; }

count()  { sink "SELECT count(*) FROM $TABLE"; }
field()  { sink "SELECT coalesce($2::text, '<null>') FROM $TABLE WHERE id = '$1'"; }
exists() { sink "SELECT count(*) FROM $TABLE WHERE id = '$1'"; }

start_sync() { sync_spawn "$CONFIG"; }
# A restart has to wait for the old process to let go of the replication slot:
# starting on a slot the previous run still holds fails outright, and the
# failure looks exactly like a checkpoint that did not resume.
stop_sync() {
  sync_stop
  for _ in $(seq 1 30); do
    [ "$(pg "SELECT count(*) FROM pg_replication_slots WHERE slot_name='$SLOT' AND active;")" = "0" ] && break
    sleep 1
  done
}
await_count() { for _ in $(seq 1 60); do [ "$(count)" = "$1" ] && return 0; sleep 1; done; }
await_field() { for _ in $(seq 1 60); do [ "$(field "$1" "$2")" = "$3" ] && return 0; sleep 1; done; }

cleanup() {
  stop_sync
  pg "SELECT pg_drop_replication_slot('$SLOT') WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name='$SLOT');" > /dev/null 2>&1 || true
  pg "DROP PUBLICATION IF EXISTS ${SLOT}_pub;" > /dev/null 2>&1 || true
  pg "DROP TABLE IF EXISTS $TABLE;" > /dev/null 2>&1 || true
  sink "DROP TABLE IF EXISTS $TABLE;" > /dev/null 2>&1 || true
  sink "DROP TABLE IF EXISTS pg2osync_state;" > /dev/null 2>&1 || true
  sink "DROP TABLE IF EXISTS pg2osync_rejects;" > /dev/null 2>&1 || true
  sink "DROP TABLE IF EXISTS conformance_kit;" > /dev/null 2>&1 || true
  rm -rf "$WORK"
  e2e_unlock
}
trap cleanup EXIT

# The DDL is the operator's, not something derived from the source: the target
# names its own types, and `vector(3)` is one the source has never heard of.
cat > "$DDL" <<SQL
CREATE EXTENSION IF NOT EXISTS vector;
CREATE TABLE $TABLE (
  id text PRIMARY KEY,
  title text,
  embedding vector(3),
  _version bigint
);
SQL

cat > "$CONFIG" <<TOML
[source]
url_env = "PG2OSYNC_SOURCE_URL"
slot_name = "$SLOT"
publication = "${SLOT}_pub"

[target]
flavor = "postgres"
url_env = "PG2OSYNC_TARGET_URL"

[metrics]
bind = "127.0.0.1:$((PORT_BASE + 22))"

[sync.$TABLE]
table = "public.$TABLE"
mapping_file = "docs.sql"
exclude_columns = ["secret"]
TOML

# The same pipeline with one OpenSearch-only option, so the refusal is checked
# against a configuration that is otherwise valid.
sed -e 's/^exclude_columns = .*/exclude_columns = ["secret"]\nrouting = "title"/' \
  "$CONFIG" > "$REFUSED_CONFIG"

say "0. Reset state"
stop_sync
pg "DROP PUBLICATION IF EXISTS ${SLOT}_pub;" > /dev/null
pg "SELECT pg_drop_replication_slot('$SLOT') WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name='$SLOT');" > /dev/null
pg "DROP TABLE IF EXISTS $TABLE;" > /dev/null
# An embedding the source database produced: a plain float array, which is what
# a team without pgvector on the source already has. The sink computes nothing.
pg "CREATE TABLE $TABLE (
      id bigint PRIMARY KEY,
      title text,
      embedding real[],
      secret text);" > /dev/null
pg "INSERT INTO $TABLE (id,title,embedding,secret) VALUES
      (1,'alpha','{1,0,0}','s1'),
      (2,'beta','{0,1,0}','s2'),
      (3,'gamma','{0,0,1}','s3');" > /dev/null
sink "DROP TABLE IF EXISTS $TABLE;" > /dev/null
sink "DROP TABLE IF EXISTS pg2osync_state;" > /dev/null
sink "DROP TABLE IF EXISTS pg2osync_rejects;" > /dev/null
: > "$LOG"
ok "seeded 3 rows, target table and checkpoint cleared"

say "1. validate"
if $BIN validate -c "$CONFIG" 2>&1 | grep -q "all checks passed"; then
  ok "validate passes"
else
  bad "validate failed"
fi
# captured rather than piped: the refusal is a non-zero exit, and `pipefail`
# would report that as the pipeline's own failure
REFUSED=$($BIN validate -c "$REFUSED_CONFIG" 2>&1 || true)
if grep -q "routing" <<< "$REFUSED" && grep -q "PostgreSQL" <<< "$REFUSED"; then
  ok "an OpenSearch-only option is refused by name"
else
  bad "routing was accepted against a PostgreSQL target: $REFUSED"
fi

say "2. initial load"
start_sync
await_count 3
check "the DDL created the table and every row is in it" "$(count)" "3"
check "a value arrives intact" "$(field 1 title)" "alpha"
check "an excluded column is not a column of the target" \
  "$(sink "SELECT count(*) FROM information_schema.columns WHERE table_name='$TABLE' AND column_name='secret'")" "0"
check "the position lands in the version column" \
  "$(sink "SELECT count(*) FROM $TABLE WHERE _version IS NOT NULL")" "3"

say "3. an embedding the source produced orders a nearest-neighbour query"
check "the array arrived as a vector" "$(field 2 embedding)" "[0,1,0]"
check "the nearest neighbour of the second basis vector is its own row" \
  "$(sink "SELECT id FROM $TABLE ORDER BY embedding <-> '[0,0.9,0]' LIMIT 1")" "2"

say "4. live streaming"
pg "INSERT INTO $TABLE (id,title,embedding) VALUES (4,'delta','{0,0,0.5}');" > /dev/null
await_count 4
check "INSERT propagated" "$(count)" "4"
pg "UPDATE $TABLE SET title='delta-renamed', embedding='{0.5,0,0}' WHERE id=4;" > /dev/null
await_field 4 title delta-renamed
check "UPDATE propagated" "$(field 4 title)" "delta-renamed"
check "and so did the changed embedding" "$(field 4 embedding)" "[0.5,0,0]"
pg "DELETE FROM $TABLE WHERE id=4;" > /dev/null
await_count 3
check "DELETE propagated" "$(exists 4)" "0"

say "5. the checkpoint lives in the target database"
check "a checkpoint row was written to the state table" \
  "$(sink "SELECT count(*) FROM pg2osync_state WHERE key LIKE 'checkpoint-%'")" "1"
check "and it carries this stream's slot" \
  "$(sink "SELECT doc->>'slot_name' FROM pg2osync_state WHERE key LIKE 'checkpoint-%'")" "$SLOT"

say "6. kill -9 resumes from that checkpoint"
sync_kill; sleep 1
# the source moves while nothing is watching it: a reload would find these
# anyway, so the log line below is what separates a resume from a reload
pg "INSERT INTO $TABLE (id,title,embedding) VALUES (5,'epsilon','{1,1,1}');" > /dev/null
pg "UPDATE $TABLE SET title='beta-while-down' WHERE id=2;" > /dev/null
: > "$LOG"
start_sync
await_count 4
check "a row added while down arrived" "$(field 5 title)" "epsilon"
await_field 2 title beta-while-down
check "a row updated while down is current" "$(field 2 title)" "beta-while-down"
if grep -q "resuming from checkpoint" "$LOG"; then
  ok "the pipeline resumed from the checkpoint in the target"
else
  bad "the pipeline did not resume from the checkpoint in the target"
fi

say "7. a TRUNCATE clears the table at the position it happened at"
pg "TRUNCATE $TABLE;" > /dev/null
await_count 0
check "the target table is empty" "$(count)" "0"
pg "INSERT INTO $TABLE (id,title,embedding) VALUES (9,'after-truncate','{1,1,0}');" > /dev/null
await_count 1
check "a row inserted after the truncate survives it" "$(field 9 title)" "after-truncate"

say "8. resnapshot rebuilds what was removed behind the pipeline's back"
stop_sync
sink "DELETE FROM $TABLE WHERE id = '9';" > /dev/null
check "the row is gone from the target" "$(exists 9)" "0"
$BIN resnapshot -c "$CONFIG" --table "public.$TABLE" >> "$LOG" 2>&1
check "resnapshot put it back" "$(exists 9)" "1"
check "with its value" "$(field 9 title)" "after-truncate"

say "9. the sink conformance kit"
# The contract every target shares, asserted against this one. It uses a table
# of its own, so it can run beside the sections above.
if PG2OSYNC_TEST_PG_SINK_URL="$PG2OSYNC_TARGET_URL" \
  cargo test --release -p pg2osync-sink --test conformance -- --nocapture >> "$LOG" 2>&1; then
  ok "the sink honours the shared contract"
else
  bad "the sink conformance kit failed"
fi

printf "\n\033[1m%d passed, %d failed\033[0m\n" "$PASS" "$FAIL"
# the pipeline's own log is the only place a failure explains itself, and on CI
# nobody can go and read the file afterwards
[ "$FAIL" -eq 0 ] || tail -60 "$LOG"
[ "$FAIL" -eq 0 ]
