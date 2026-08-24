#!/usr/bin/env bash
# pg2osync end-to-end test suite
#
# Prerequisites:
#   - docker containers from dev/docker-compose.yml running (PG 15432, OS 9200)
#   - release binary built: cargo build --release
#
# Usage: ./dev/e2e-test.sh
set -euo pipefail

cd "$(dirname "$0")/.."
BIN=./target/release/pg2osync
export PG2OSYNC_SOURCE_URL="postgres://postgres:postgres@localhost:15432/sourcedb"
OS=http://localhost:9200
PASS=0; FAIL=0

say()  { printf "\n\033[1m== %s ==\033[0m\n" "$1"; }
ok()   { printf "  \033[32m✓ %s\033[0m\n" "$1"; PASS=$((PASS+1)); }
bad()  { printf "  \033[31m✗ %s\033[0m\n" "$1"; FAIL=$((FAIL+1)); }
check(){ if [ "$2" = "$3" ]; then ok "$1 ($2)"; else bad "$1 (got $2, want $3)"; fi }

os_count()   { curl -s "$OS/$1/_count" | python3 -c "import sys,json;print(json.load(sys.stdin)['count'])"; }
os_field()   { curl -s "$OS/$1/_doc/$2" | python3 -c "import sys,json;print(json.load(sys.stdin).get('_source',{}).get('$3','<missing>'))"; }
os_exists()  { curl -s -o /dev/null -w "%{http_code}" "$OS/$1/_doc/$2"; }
pg()         { docker exec dev-postgres-1 psql -U postgres -d sourcedb -qtc "$1" | tr -d " "; }

cleanup() { pkill -f "pg2osync run" 2>/dev/null || true; }
trap cleanup EXIT

say "0. Reset state"
pg "DROP PUBLICATION IF EXISTS pg2osync_pub;"
pg "SELECT pg_drop_replication_slot('pg2osync') WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name='pg2osync');" > /dev/null
pg "TRUNCATE users;"
pg "INSERT INTO users VALUES (1,'alice','alice@test.io','{\"role\":\"admin\"}',NOW());
    INSERT INTO users VALUES (2,'bob','bob@test.io','{\"role\":\"user\"}',NOW());
    INSERT INTO users VALUES (3,'charlie','charlie@test.io','{}',NOW());" > /dev/null
curl -s -XDELETE "$OS/users_index,.pg2osync_meta" > /dev/null
ok "seeded 3 rows, indices cleared"

say "1. validate"
if $BIN validate -c /tmp/pg2osync-e2e.toml 2>&1 | grep -q "all checks passed"; then
  ok "validate passes"
else
  bad "validate failed"
fi

say "2. run + backfill"
nohup $BIN run -c /tmp/pg2osync-e2e.toml &> /tmp/pg2osync-e2e.log < /dev/null & disown
sleep 4
check "backfilled docs" "$(os_count users_index)" "3"

say "3. live streaming CRUD"
pg "INSERT INTO users VALUES (4,'dave','dave@test.io','{}',NOW());" > /dev/null; sleep 2
check "INSERT id=4" "$(os_count users_index)" "4"

pg "UPDATE users SET email='upd@test.io' WHERE id=4;" > /dev/null; sleep 2
check "UPDATE email propagated" "$(os_field users_index 4 email)" "upd@test.io"

pg "DELETE FROM users WHERE id=3;" > /dev/null; sleep 2
check "DELETE id=3 (404 expected)" "$(os_exists users_index 3)" "404"

say "4. crash recovery"
pkill -9 -f "pg2osync run"; sleep 1
pg "INSERT INTO users VALUES (5,'eve-during-downtime','eve@test.io','{}',NOW());" > /dev/null
before=$(os_count users_index)
echo "    during downtime: OS=$before (id=5 missing)"
nohup $BIN run -c /tmp/pg2osync-e2e.toml &> /tmp/pg2osync-e2e-restart.log < /dev/null & disown
sleep 5
check "recovered after restart" "$(os_count users_index)" "4"
check "id=5 present" "$(os_field users_index 5 name)" "eve-during-downtime"

say "5. final consistency"
pg_n=$(pg "SELECT count(*) FROM users;")
os_n=$(os_count users_index)
check "PG=$pg_n == OS=$os_n" "$pg_n" "$os_n"

printf "\n\033[1mRESULT: %d passed, %d failed\033[0m\n" "$PASS" "$FAIL"
[ "$FAIL" = "0" ]
