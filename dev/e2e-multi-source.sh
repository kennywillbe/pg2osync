#!/usr/bin/env bash
# End-to-end suite for one process serving several source databases.
#
# PostgreSQL and MySQL in a single `pg2osync run --config-dir`: both index,
# both report themselves, and — the point of the whole feature — one of them
# going away does not touch the other.
#
# One suite at a time per stack: the slot, the tables and the indices are
# fixed, so two suites against the same PostgreSQL, MySQL and OpenSearch
# overwrite each other's state. dev/e2e-lock.sh enforces that on the shared dev
# stack with a machine-wide lock; a second suite waits. A run with a stack of
# its own — ci-local --isolated — passes E2E_LOCK=none and takes no lock,
# because a stop only ever signals the pipelines this run started
# (dev/e2e-pipeline.sh).
#
# Prerequisites:
#   docker compose -f dev/docker-compose.yml up -d
#   docker exec -i dev-postgres-1 psql -U postgres -d sourcedb < dev/seed.sql
#   a MySQL container (dev/ci-local.sh brings one up as `mysql-test`)
#   cargo build --release
#
# Usage: ./dev/e2e-multi-source.sh
#   OS_URL          target base URL          (default http://localhost:9200)
#   TARGET_FLAVOR   opensearch|elasticsearch (default opensearch)
#   PG_CONTAINER    psql container name      (default dev-postgres-1)
#   PG_PORT         source port on localhost (default 15432)
#   MYSQL_CONTAINER MySQL container name     (default mysql-test)
#   MYSQL_PORT      MySQL port on localhost  (default 13306)
#   E2E_LOG         pipeline log file        (default /tmp/pg2osync-multi-e2e.log)
#   E2E_LOCK        lock directory, or none  (default /tmp/pg2osync-e2e.lock)
#   E2E_PORT_BASE   first metrics/API port   (default 9100, a 40-port block)
set -euo pipefail
# shellcheck source=dev/e2e-lock.sh
source "$(dirname "$0")/e2e-lock.sh"
# shellcheck source=dev/e2e-pipeline.sh
source "$(dirname "$0")/e2e-pipeline.sh"
e2e_lock

cd "$(dirname "$0")/.."
BIN=./target/release/pg2osync
OS=${OS_URL:-http://localhost:9200}
TARGET_FLAVOR=${TARGET_FLAVOR:-opensearch}
PG_CONTAINER=${PG_CONTAINER:-dev-postgres-1}
PG_PORT=${PG_PORT:-15432}
MYSQL_CONTAINER=${MYSQL_CONTAINER:-mysql-test}
MYSQL_PORT=${MYSQL_PORT:-13306}
MYSQL_USER=${MYSQL_USER:-repl}
MYSQL_PASSWORD=${MYSQL_PASSWORD:-replpw}
MYSQL_ROOT_PASSWORD=${MYSQL_ROOT_PASSWORD:-mysqlpw}
# MariaDB images ship the client as `mariadb`, MySQL images as `mysql`
MYSQL_CLIENT=${MYSQL_CLIENT:-mysql}
# BSD mktemp only substitutes X's at the *end* of the template, so a directory
# template with a suffix would be created literally and one killed run would
# break every later one with "File exists".
CONFIG_DIR=$(mktemp -d /tmp/pg2osync-multi.XXXXXX)
LOG=${E2E_LOG:-/tmp/pg2osync-multi-e2e.log}
# The process binds its metrics and API ports on this machine rather than
# inside a container, so two suites at once need two blocks: E2E_PORT_BASE
# moves both ports this one uses.
PORT_BASE=${E2E_PORT_BASE:-9100}
METRICS=127.0.0.1:$((PORT_BASE + 38))
API=127.0.0.1:$((PORT_BASE + 39))
export PG2OSYNC_SOURCE_URL="postgres://postgres:postgres@localhost:$PG_PORT/sourcedb"
export PG2OSYNC_MYSQL_URL="mysql://$MYSQL_USER:$MYSQL_PASSWORD@localhost:$MYSQL_PORT/sourcedb"
PASS=0; FAIL=0

say()   { printf "\n\033[1m== %s ==\033[0m\n" "$1"; }
ok()    { printf "  \033[32m✓ %s\033[0m\n" "$1"; PASS=$((PASS + 1)); }
bad()   { printf "  \033[31m✗ %s\033[0m\n" "$1"; FAIL=$((FAIL + 1)); }
check() { if [ "$2" = "$3" ]; then ok "$1 ($2)"; else bad "$1 (got '$2', want '$3')"; fi }

jqf()       { python3 -c "import sys,json;d=json.load(sys.stdin);print($1)"; }
os_count()  { curl -s "$OS/$1/_count" | jqf "d.get('count', 0)"; }
os_status() { curl -s -o /dev/null -w "%{http_code}" "$OS/$1/_doc/$2"; }
pg()        { docker exec "$PG_CONTAINER" psql -U postgres -d sourcedb -qtAc "$1"; }
my()        { docker exec "$MYSQL_CONTAINER" "$MYSQL_CLIENT" -uroot -p"$MYSQL_ROOT_PASSWORD" -N -B sourcedb -e "$1" 2>/dev/null; }
refresh()   { curl -s -XPOST "$OS/_refresh" > /dev/null; }
metrics()   { curl -s "http://$METRICS/metrics"; }
http_code() { curl -s -o /dev/null -w "%{http_code}" "$1"; }
# every source loads on its own, so one index reaching its count says nothing
# about the other
await_count() {
  for _ in $(seq 1 120); do
    refresh
    [ "$(os_count "$1")" = "$2" ] && return 0
    sleep 1
  done
}
# a state set reports one 1 and five 0s, so the assertion is on the whole line
await_state() {
  for _ in $(seq 1 120); do
    metrics | grep -q "pg2osync_source_state{source=\"$1\",state=\"$2\"} 1" && return 0
    sleep 1
  done
  return 1
}
state_of() {
  metrics | sed -n "s/^pg2osync_source_state{source=\"$1\",state=\"\\([a-z]*\\)\"} 1$/\\1/p"
}

# remember_sync sets SYNC_PID, which the drain section signals and waits on.
start_sync() {
  nohup "$BIN" run --config-dir "$CONFIG_DIR" >> "$LOG" 2>&1 < /dev/null &
  remember_sync $!
}
# SIGTERM rather than SIGKILL: draining is what the last section is about
stop_sync() { sync_stop; }
# How many of the pipelines this run started are still up. Counting every
# pg2osync on the machine would count the suite running beside this one.
sync_count() { live_syncs || true; printf '%s' "$LIVE_SYNCS" | wc -w | tr -d ' '; }
drop_own_slot() { pg "SELECT pg_drop_replication_slot('pg2osync_multi') WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name='pg2osync_multi');" > /dev/null 2>&1 || true; }
# a suite that stopped MySQL and then failed must not leave it stopped for the
# suites that follow
cleanup() {
  stop_sync
  docker start "$MYSQL_CONTAINER" > /dev/null 2>&1 || true
  drop_own_slot
  rm -rf "$CONFIG_DIR"
  e2e_unlock
}
trap cleanup EXIT

# One file per source, exactly as an operator mounts them. [metrics] and [api]
# describe the process, so both files declare the same one — which is also what
# the cross-file validation insists on.
cat > "$CONFIG_DIR/pgsrc.toml" <<TOML
[source]
url_env = "PG2OSYNC_SOURCE_URL"
slot_name = "pg2osync_multi"
publication = "pg2osync_multi_pub"

[target]
url = "$OS"
flavor = "$TARGET_FLAVOR"

[metrics]
bind = "$METRICS"

[api]
enabled = true
bind = "$API"

[sync.users]
table = "public.users"
index = "e2e_multi_pg_users"
exclude_columns = ["password_hash"]
TOML

# reconnect_max is small on purpose: the isolation section stops the server and
# waits for this source to give up, and the default policy would keep it
# reconnecting for far longer than a test can wait.
cat > "$CONFIG_DIR/mysrc.toml" <<TOML
[source]
flavor = "mysql"
url_env = "PG2OSYNC_MYSQL_URL"
server_id = 990002
reconnect_max = 2
reconnect_backoff_ms = 500

[target]
url = "$OS"
flavor = "$TARGET_FLAVOR"

[metrics]
bind = "$METRICS"

[api]
enabled = true
bind = "$API"

[sync.shop_users]
table = "sourcedb.shop_users"
index = "e2e_multi_my_users"
exclude_columns = ["password_hash"]
TOML

# a mapping file beside the configs: the loader reads *.toml and nothing else,
# and a directory of configs is where such a file actually lives
echo '{"properties":{"name":{"type":"keyword"}}}' > "$CONFIG_DIR/mapping.json"

say "0. Reset state"
stop_sync
docker start "$MYSQL_CONTAINER" > /dev/null 2>&1 || true
for _ in $(seq 1 60); do my "SELECT 1;" > /dev/null 2>&1 && break; sleep 1; done
drop_own_slot
pg "TRUNCATE users CASCADE;"
pg "INSERT INTO users (id,name,email,password_hash) VALUES
      (1,'alice','alice@test.io','secret-1'),
      (2,'bob','bob@test.io','secret-2');"
my "TRUNCATE shop_users;"
my "INSERT INTO shop_users (id,name,email,password_hash,balance) VALUES
      (1,'mysql-alice','ma@test.io','secret-1',1.00),
      (2,'mysql-bob','mb@test.io','secret-2',2.00),
      (3,'mysql-carol','mc@test.io','secret-3',3.00);"
curl -s -XDELETE "$OS/e2e_multi_pg_users,e2e_multi_my_users,.pg2osync_meta?ignore_unavailable=true" > /dev/null
ok "two sources seeded, indices cleared"

say "1. validate --config-dir"
if $BIN validate --config-dir "$CONFIG_DIR" 2>&1 | grep -q "all checks passed"; then
  ok "both configs validate as one set"
else
  bad "validate --config-dir failed"
  $BIN validate --config-dir "$CONFIG_DIR" 2>&1 | tail -8 | sed 's/^/    /'
fi

say "2. one process, both sources"
start_sync
await_count e2e_multi_pg_users 2
await_count e2e_multi_my_users 3
check "PostgreSQL rows indexed" "$(os_count e2e_multi_pg_users)" "2"
check "MySQL rows indexed" "$(os_count e2e_multi_my_users)" "3"
# both sources are served by the one --config-dir process, not by two
check "one process" "$(sync_count)" "1"

say "3. one exposition, a source label on every series"
await_state pgsrc streaming || true
await_state mysrc streaming || true
EXPO=$(metrics)
check "one HELP for the event counter" "$(echo "$EXPO" | grep -c '^# HELP pg2osync_events_total')" "1"
check "one TYPE for the event counter" "$(echo "$EXPO" | grep -c '^# TYPE pg2osync_events_total')" "1"
check "one HELP for the position gauge" "$(echo "$EXPO" | grep -c '^# HELP pg2osync_position_lag')" "1"
check "the PostgreSQL source is labelled" \
  "$(echo "$EXPO" | grep -c '^pg2osync_position_lag{source="pgsrc"}')" "1"
check "the MySQL source is labelled" \
  "$(echo "$EXPO" | grep -c '^pg2osync_position_lag{source="mysrc"}')" "1"
check "the slot gauge carries its source too" \
  "$(echo "$EXPO" | grep -c '^pg2osync_slot_retained_bytes{source="pgsrc",slot=')" "1"
# a series with no source label would be two sources' numbers added together
check "no unlabelled series" \
  "$(echo "$EXPO" | grep -v '^#' | grep -cv 'source="')" "0"
check "PostgreSQL is streaming" "$(state_of pgsrc)" "streaming"
check "MySQL is streaming" "$(state_of mysrc)" "streaming"

say "4. /synced names its source"
check "the PostgreSQL source answers" \
  "$(http_code "http://$API/synced?source=pgsrc&timeout=10000")" "200"
check "the MySQL source answers" \
  "$(http_code "http://$API/synced?source=mysrc&timeout=10000")" "200"
# answering for whichever source came first would tell a caller its write is
# visible when the pipeline carrying it has written nothing
check "no source named is refused" "$(http_code "http://$API/synced")" "400"
BARE=$(curl -s "http://$API/synced")
check "the refusal names both sources" \
  "$(echo "$BARE" | grep -c 'pgsrc.*mysrc\|mysrc.*pgsrc')" "1"
check "an unknown source is a 404" "$(http_code "http://$API/synced?source=typo")" "404"
check "the answer says which source it is about" \
  "$(curl -s "http://$API/synced?source=mysrc&timeout=10000" | jqf "d.get('source')")" "mysrc"

say "5. health is per source, liveness is not"
check "liveness" "$(http_code "http://$METRICS/healthz")" "200"
check "the PostgreSQL source is ready" "$(http_code "http://$METRICS/healthz/pgsrc")" "200"
check "the MySQL source is ready" "$(http_code "http://$METRICS/healthz/mysrc")" "200"
check "an unknown source is a 404" "$(http_code "http://$METRICS/healthz/typo")" "404"

say "6. one source going away is not the other's problem"
docker stop "$MYSQL_CONTAINER" > /dev/null
# the whole point: PostgreSQL keeps indexing while MySQL is gone
pg "INSERT INTO users (id,name,email,password_hash) VALUES (3,'carol','carol@test.io','secret-3');"
await_count e2e_multi_pg_users 3
check "PostgreSQL rows keep landing" "$(os_count e2e_multi_pg_users)" "3"
if await_state mysrc halted; then
  ok "the MySQL source halted after its reconnects ran out"
else
  bad "the MySQL source is $(state_of mysrc), not halted"
fi
check "the halted source fails its own probe" "$(http_code "http://$METRICS/healthz/mysrc")" "503"
# a 503 here would have the kubelet restart the pipeline that is working
check "liveness stays up" "$(http_code "http://$METRICS/healthz")" "200"
check "the working source stays ready" "$(http_code "http://$METRICS/healthz/pgsrc")" "200"
check "the process is still running" "$(sync_count)" "1"
check "/synced still answers for the source that is up" \
  "$(http_code "http://$API/synced?source=pgsrc&timeout=10000")" "200"

say "7. the halted source resumes from its checkpoint"
docker start "$MYSQL_CONTAINER" > /dev/null
for _ in $(seq 1 90); do my "SELECT 1;" > /dev/null 2>&1 && break; sleep 1; done
# A container Docker chose the host port for — an isolated run's — is published
# on a different one every time it starts, so the URL the restarted process
# reads has to be read back rather than remembered.
MYSQL_PORT=$(docker port "$MYSQL_CONTAINER" 3306/tcp | head -1 | sed 's/.*://')
export PG2OSYNC_MYSQL_URL="mysql://$MYSQL_USER:$MYSQL_PASSWORD@localhost:$MYSQL_PORT/sourcedb"
my "INSERT INTO shop_users (id,name,email,password_hash,balance) VALUES (4,'mysql-dave','md@test.io','secret-4',4.00);"
# a halted source is not restarted on its own: that is the documented contract
check "it stays halted until the process is restarted" "$(state_of mysrc)" "halted"
stop_sync
# only the lines this restart writes, so an earlier section's are not counted
MARK=$(wc -l < "$LOG")
start_sync
await_count e2e_multi_my_users 4
check "the row written while it was down arrives" "$(os_count e2e_multi_my_users)" "4"
# each source resumes from its own checkpoint; a shared one would have sent one
# of them back to a full load. The two sources say it in their own words: a WAL
# position is a checkpoint, a binlog coordinate is a file and an offset.
check "both sources resumed from a checkpoint" \
  "$(tail -n "+$((MARK + 1))" "$LOG" | grep -cE "resuming from checkpoint|resuming binlog from")" "2"
await_state mysrc streaming || true
check "the MySQL source is streaming again" "$(state_of mysrc)" "streaming"
check "the PostgreSQL source came back too" "$(state_of pgsrc)" "streaming"

say "8. SIGTERM drains every source"
pg "INSERT INTO users (id,name,email,password_hash) VALUES (4,'dan','dan@test.io','secret-4');"
my "INSERT INTO shop_users (id,name,email,password_hash,balance) VALUES (5,'mysql-erin','me@test.io','secret-5',5.00);"
# long enough for both streams to have carried the change into the pipeline;
# what the signal then has to prove is that the drain writes what it holds
sleep 5
kill -TERM "$SYNC_PID" 2> /dev/null || true
EXIT=0
wait "$SYNC_PID" || EXIT=$?
check "a clean drain exits 0" "$EXIT" "0"
refresh
check "the PostgreSQL write was drained" "$(os_count e2e_multi_pg_users)" "4"
check "the MySQL write was drained" "$(os_count e2e_multi_my_users)" "5"
# one document per source, keyed by the stream rather than by the process: a
# shared checkpoint is the failure the whole feature is guarding against
check "the PostgreSQL checkpoint is its own document" \
  "$(os_status .pg2osync_meta postgres-pg2osync_multi)" "200"
check "the MySQL checkpoint is its own document" \
  "$(os_status .pg2osync_meta mysql-990002)" "200"

say "9. a subcommand acts on the source it was given"
# The process is down, so these talk to the databases and the target directly.
BOTH=$($BIN status --config-dir "$CONFIG_DIR" 2>&1 || true)
check "status reports every source" "$(echo "$BOTH" | grep -c '^checkpoint: ')" "2"
ONE=$($BIN status --config-dir "$CONFIG_DIR" --source pgsrc 2>&1 || true)
check "--source reports that one" \
  "$(echo "$ONE" | grep -c 'stream=pg2osync_multi')" "1"
check "and not the other" "$(echo "$ONE" | grep -c 'stream=990002')" "0"
# drop-slot destroys what one source owns; over a directory it has to be told
# which, and the refusal comes before anything connects
REFUSED=$($BIN drop-slot --config-dir "$CONFIG_DIR" 2>&1 || true)
check "drop-slot refuses to guess" \
  "$(echo "$REFUSED" | grep -c 'drop-slot acts on one source')" "1"
check "the refusal names the choices" \
  "$(echo "$REFUSED" | grep -c 'mysrc, pgsrc')" "1"
check "the slot it did not drop is still there" \
  "$(pg "SELECT count(*) FROM pg_replication_slots WHERE slot_name='pg2osync_multi';")" "1"

say "Result"
printf "  %d passed, %d failed\n" "$PASS" "$FAIL"
[ "$FAIL" -eq 0 ]
