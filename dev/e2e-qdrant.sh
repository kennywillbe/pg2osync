#!/usr/bin/env bash
# End-to-end suite for the PostgreSQL -> Qdrant pipeline.
#
# Qdrant cannot run dev/e2e-test.sh: that suite asserts over mappings, joins and
# per-row indices, none of which this target has. What is left is what a vector
# database does have, and what is this target's own: the operator's JSON creates
# the collection, a document id becomes a UUID point with the id itself in the
# `_pg2osync_id` payload field, an embedding the *source* produced answers a
# similarity search, a fanned list shrinks by really deleting the points it
# dropped, the checkpoint lives in a collection of the target, and the features
# that belong to a search engine are refused by name instead of ignored.
#
# It also runs the sink conformance kit (crates/sink/tests/conformance.rs)
# against the same instance, which is where the contract shared with every other
# target is asserted, with no check skipped.
#
# One suite at a time per stack: the slots, the collections and the state
# collection are fixed, so two suites against the same pair overwrite each
# other's state. dev/e2e-lock.sh enforces that on the shared dev stack with a
# machine-wide lock; a run with a stack of its own — ci-local --isolated —
# passes E2E_LOCK=none, because a stop only ever signals the pipelines this run
# started (dev/e2e-pipeline.sh).
#
# Prerequisites:
#   a PostgreSQL with logical replication, seeded with dev/seed.sql
#   a Qdrant reachable at QDRANT_URL
#   cargo build --release
#
# Usage: ./dev/e2e-qdrant.sh
#   QDRANT_URL        Qdrant base URL       (default http://localhost:6333)
#   QDRANT_API_KEY    api key, or empty     (default e2e-api-key)
#   PG_CONTAINER      psql container name   (default dev-postgres-1)
#   PG_PORT           source port           (default 15432)
#   E2E_LOG           pipeline log file     (default /tmp/pg2osync-qdrant.log)
#   E2E_LOCK          lock directory, or none (default /tmp/pg2osync-e2e.lock)
#   E2E_PORT_BASE     first metrics port    (default 9100, a 40-port block)
set -euo pipefail
# shellcheck source=dev/e2e-lock.sh
source "$(dirname "$0")/e2e-lock.sh"
# shellcheck source=dev/e2e-pipeline.sh
source "$(dirname "$0")/e2e-pipeline.sh"
e2e_lock

cd "$(dirname "$0")/.."
BIN=./target/release/pg2osync
QDRANT=${QDRANT_URL:-http://localhost:6333}
PG_CONTAINER=${PG_CONTAINER:-dev-postgres-1}
PG_PORT=${PG_PORT:-15432}
# the sink reads the key from this variable's name, so it has to be exported
export QDRANT_API_KEY=${QDRANT_API_KEY:-e2e-api-key}
SLOT=pg2osync_e2e_qdrant
FSLOT=pg2osync_e2e_qdrant_fan
TABLE=e2e_qdrant_docs
FAN_TABLE=e2e_qdrant_fan
# BSD mktemp only substitutes X's at the *end* of the template: with a suffix
# after them it creates the literal name instead, and one killed run then
# breaks every later one with "File exists".
WORK=$(mktemp -d /tmp/pg2osync-qdrant.XXXXXX)
CONFIG=$WORK/pg2osync.toml
REFUSED_CONFIG=$WORK/refused.toml
FAN_CONFIG=$WORK/fan.toml
COLLECTION_JSON=$WORK/docs.json
FAN_JSON=$WORK/fan.json
LOG=${E2E_LOG:-/tmp/pg2osync-qdrant.log}
export PG2OSYNC_SOURCE_URL="postgres://postgres:postgres@localhost:$PG_PORT/sourcedb"
# Every pipeline the suite starts binds its metrics port on this machine rather
# than inside a container, so two suites at once need two blocks.
PORT_BASE=${E2E_PORT_BASE:-9100}

PASS=0; FAIL=0

say()   { printf "\n\033[1m== %s ==\033[0m\n" "$1"; }
ok()    { printf "  \033[32m✓ %s\033[0m\n" "$1"; PASS=$((PASS + 1)); }
bad()   { printf "  \033[31m✗ %s\033[0m\n" "$1"; FAIL=$((FAIL + 1)); }
check() { if [ "$2" = "$3" ]; then ok "$1 ($2)"; else bad "$1 (got '$2', want '$3')"; fi }

jqf() { python3 -c "import sys,json;d=json.load(sys.stdin);print($1)"; }
qd()  { curl -s -H "api-key: $QDRANT_API_KEY" -H 'content-type: application/json' "$@"; }
pg()  { docker exec "$PG_CONTAINER" psql -U postgres -d sourcedb -qtAc "$1"; }

# A collection the pipeline has not created yet answers 404, which is a count
# of nothing rather than an error: the loops below are waiting for it to exist.
count_of() { qd -XPOST "$QDRANT/collections/$1/points/count" -d '{"exact":true}' | jqf "d.get('result', {}).get('count', 0)"; }
count()    { count_of "$TABLE"; }
# A point id is a UUID, so a document is looked up by the id the sink kept in
# the payload — which is what that field exists for.
point_of() {
  qd -XPOST "$QDRANT/collections/$1/points/scroll" \
    -d "{\"filter\":{\"must\":[{\"key\":\"_pg2osync_id\",\"match\":{\"value\":\"$2\"}}]},\"limit\":1,\"with_payload\":true,\"with_vector\":true}"
}
found()  { jqf "(d.get('result', {}).get('points') or [{}])[0]$1"; }
field()  { point_of "$TABLE" "$1" | found ".get('payload', {}).get('$2', '<missing>')"; }
vector() { point_of "$TABLE" "$1" | found ".get('vector', {}).get('embedding', '<missing>')"; }
exists() { point_of "$1" "$2" | jqf "len(d.get('result', {}).get('points') or [])"; }
nearest() {
  qd -XPOST "$QDRANT/collections/$TABLE/points/search" \
    -d "{\"vector\":{\"name\":\"embedding\",\"vector\":$1},\"limit\":1,\"with_payload\":true}" |
    jqf "(d['result'] or [{}])[0].get('payload', {}).get('_pg2osync_id', '<none>')"
}
drop_collections() {
  for c in "$TABLE" "$FAN_TABLE" conformance_kit pg2osync_state pg2osync_rejects; do
    qd -XDELETE "$QDRANT/collections/$c" > /dev/null
  done
}

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
await_gone()  { for _ in $(seq 1 60); do [ "$(exists "$1" "$2")" = "0" ] && return 0; sleep 1; done; }

drop_slot() {
  pg "SELECT pg_drop_replication_slot('$1') WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name='$1');" > /dev/null 2>&1 || true
  pg "DROP PUBLICATION IF EXISTS ${1}_pub;" > /dev/null 2>&1 || true
}

cleanup() {
  stop_sync
  drop_slot "$SLOT"
  drop_slot "$FSLOT"
  pg "DROP TABLE IF EXISTS $TABLE;" > /dev/null 2>&1 || true
  pg "DROP TABLE IF EXISTS $FAN_TABLE;" > /dev/null 2>&1 || true
  drop_collections > /dev/null 2>&1 || true
  rm -rf "$WORK"
  e2e_unlock
}
trap cleanup EXIT

# The collection is the operator's, not something derived from the source: only
# it can say how many dimensions the embedding has and how they are compared.
# `embedding` is a *named* vector, and that name is what makes the document
# field of the same name a vector instead of payload.
cat > "$COLLECTION_JSON" <<JSON
{"vectors": {"embedding": {"size": 3, "distance": "Dot"}}}
JSON
cat > "$FAN_JSON" <<JSON
{"vectors": {"embedding": {"size": 3, "distance": "Dot"}}}
JSON

cat > "$CONFIG" <<TOML
[source]
url_env = "PG2OSYNC_SOURCE_URL"
slot_name = "$SLOT"
publication = "${SLOT}_pub"

[target]
flavor = "qdrant"
url = "$QDRANT"
api_key_env = "QDRANT_API_KEY"

[metrics]
bind = "127.0.0.1:$((PORT_BASE + 23))"

[sync.$TABLE]
table = "public.$TABLE"
mapping_file = "docs.json"
exclude_columns = ["secret"]
TOML

# The same pipeline with one OpenSearch-only option, so the refusal is checked
# against a configuration that is otherwise valid.
sed -e 's/^exclude_columns = .*/exclude_columns = ["secret"]\nrouting = "title"/' \
  "$CONFIG" > "$REFUSED_CONFIG"

cat > "$FAN_CONFIG" <<TOML
[source]
url_env = "PG2OSYNC_SOURCE_URL"
slot_name = "$FSLOT"
publication = "${FSLOT}_pub"

[target]
flavor = "qdrant"
url = "$QDRANT"
api_key_env = "QDRANT_API_KEY"

[metrics]
bind = "127.0.0.1:$((PORT_BASE + 24))"

[sync.fan]
table = "public.$FAN_TABLE"
index = "$FAN_TABLE"
id = "fan-{id}"
mapping_file = "fan.json"

[sync.fan.fan_out]
field = "tags"
id = "fan-{id}-{tags}"
TOML

say "0. Reset state"
stop_sync
drop_slot "$SLOT"
drop_slot "$FSLOT"
pg "DROP TABLE IF EXISTS $TABLE;" > /dev/null
# An embedding the source database produced: a plain float array, which is what
# a team without a vector store on the source already has. The sink computes
# nothing.
pg "CREATE TABLE $TABLE (
      id bigint PRIMARY KEY,
      title text,
      embedding real[],
      secret text);" > /dev/null
pg "INSERT INTO $TABLE (id,title,embedding,secret) VALUES
      (1,'alpha','{1,0,0}','s1'),
      (2,'beta','{0,1,0}','s2'),
      (3,'gamma','{0,0,1}','s3');" > /dev/null
drop_collections > /dev/null
: > "$LOG"
ok "seeded 3 rows, collections and checkpoint cleared"

say "1. validate"
if $BIN validate -c "$CONFIG" 2>&1 | grep -q "all checks passed"; then
  ok "validate passes"
else
  bad "validate failed"
fi
# captured rather than piped: the refusal is a non-zero exit, and `pipefail`
# would report that as the pipeline's own failure
REFUSED=$($BIN validate -c "$REFUSED_CONFIG" 2>&1 || true)
if grep -q "routing" <<< "$REFUSED" && grep -q "Qdrant" <<< "$REFUSED"; then
  ok "an OpenSearch-only option is refused by name"
else
  bad "routing was accepted against a Qdrant target: $REFUSED"
fi

say "2. initial load"
start_sync
await_count 3
check "the configuration created the collection and every row is in it" "$(count)" "3"
check "a value arrives intact" "$(field 1 title)" "alpha"
check "the document id is kept in the payload" "$(field 1 _pg2osync_id)" "1"
check "an excluded column never reaches the target" "$(field 1 secret)" "<missing>"
check "the position lands in the version field" \
  "$(qd -XPOST "$QDRANT/collections/$TABLE/points/count" -d '{"exact":true,"filter":{"must":[{"key":"_version","range":{"gt":0}}]}}' | jqf "d['result']['count']")" "3"

say "3. an embedding the source produced answers a similarity search"
check "the array arrived as the named vector" "$(vector 2)" "[0.0, 1.0, 0.0]"
check "the nearest neighbour of the second basis vector is its own row" "$(nearest '[0,0.9,0]')" "2"

say "4. live streaming"
pg "INSERT INTO $TABLE (id,title,embedding) VALUES (4,'delta','{0,0,0.5}');" > /dev/null
await_count 4
check "INSERT propagated" "$(count)" "4"
pg "UPDATE $TABLE SET title='delta-renamed', embedding='{0.5,0,0}' WHERE id=4;" > /dev/null
await_field 4 title delta-renamed
check "UPDATE propagated" "$(field 4 title)" "delta-renamed"
check "and so did the changed embedding" "$(vector 4)" "[0.5, 0.0, 0.0]"
pg "DELETE FROM $TABLE WHERE id=4;" > /dev/null
await_count 3
check "DELETE propagated" "$(exists "$TABLE" 4)" "0"

say "5. the checkpoint lives in the target"
STATE=$(qd -XPOST "$QDRANT/collections/pg2osync_state/points/scroll" -d '{"limit":100,"with_payload":true}')
check "a checkpoint point was written to the state collection" \
  "$(jqf "len([p for p in d['result']['points'] if p['payload']['key'].startswith('checkpoint-')])" <<< "$STATE")" "1"
check "and it carries this stream's slot" \
  "$(jqf "[p['payload']['doc']['slot_name'] for p in d['result']['points'] if p['payload']['key'].startswith('checkpoint-')][0]" <<< "$STATE")" "$SLOT"

say "6. kill -9 resumes from the state collection"
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

say "7. a TRUNCATE clears the collection at the position it happened at"
pg "TRUNCATE $TABLE;" > /dev/null
await_count 0
check "the collection is empty" "$(count)" "0"
pg "INSERT INTO $TABLE (id,title,embedding) VALUES (9,'after-truncate','{1,1,0}');" > /dev/null
await_count 1
check "a row inserted after the truncate survives it" "$(field 9 title)" "after-truncate"
stop_sync

say "8. a fanned list really deletes the elements it drops"
pg "DROP TABLE IF EXISTS $FAN_TABLE;" > /dev/null
pg "CREATE TABLE $FAN_TABLE(id bigint PRIMARY KEY, tags jsonb);" > /dev/null
pg "ALTER TABLE $FAN_TABLE REPLICA IDENTITY FULL;" > /dev/null
pg "INSERT INTO $FAN_TABLE VALUES (1,'[\"a\",\"b\"]'::jsonb);" > /dev/null
pg "CREATE PUBLICATION ${FSLOT}_pub FOR TABLE $FAN_TABLE;" > /dev/null
sync_spawn "$FAN_CONFIG"
for _ in $(seq 1 60); do [ "$(count_of "$FAN_TABLE")" = "2" ] && break; sleep 1; done
check "each element of the list is a point of its own" "$(count_of "$FAN_TABLE")" "2"
pg "UPDATE $FAN_TABLE SET tags='[\"a\",\"c\"]'::jsonb WHERE id = 1;" > /dev/null
for _ in $(seq 1 60); do [ "$(exists "$FAN_TABLE" fan-1-c)" = "1" ] && break; sleep 1; done
check "a new element arrives" "$(exists "$FAN_TABLE" fan-1-c)" "1"
await_gone "$FAN_TABLE" fan-1-b
check "the dropped element's point is gone" "$(exists "$FAN_TABLE" fan-1-b)" "0"
check "the kept element is still there" "$(exists "$FAN_TABLE" fan-1-a)" "1"
sync_stop

say "9. the sink conformance kit"
# The contract every target shares, asserted against this one. It uses a
# collection of its own, so it can run beside the sections above, and the test
# itself fails if any check reports itself as skipped.
if PG2OSYNC_TEST_QDRANT_URL="$QDRANT" PG2OSYNC_TEST_QDRANT_KEY="$QDRANT_API_KEY" \
  cargo test --release -p pg2osync-sink --test conformance -- --nocapture >> "$LOG" 2>&1; then
  ok "the sink honours the shared contract, with nothing skipped"
else
  bad "the sink conformance kit failed"
fi

printf "\n\033[1m%d passed, %d failed\033[0m\n" "$PASS" "$FAIL"
# the pipeline's own log is the only place a failure explains itself, and on CI
# nobody can go and read the file afterwards
[ "$FAIL" -eq 0 ] || tail -60 "$LOG"
[ "$FAIL" -eq 0 ]
