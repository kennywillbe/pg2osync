#!/usr/bin/env bash
# Measures what pg2osync costs the source database: connections, queries, WAL
# and the effect of REPLICA IDENTITY FULL.
#
# Prerequisites:
#   docker compose -f dev/docker-compose.yml up -d   (loads pg_stat_statements)
#   cargo build --release
#
# Usage: [ROWS=20000] ./dev/db-impact.sh
set -euo pipefail

cd "$(dirname "$0")/.."
BIN=./target/release/pg2osync
OS=${OS_URL:-http://localhost:9200}
PG_CONTAINER=${PG_CONTAINER:-dev-postgres-1}
ROWS=${ROWS:-20000}
IDLE_SECS=${IDLE_SECS:-30}
# BSD mktemp only substitutes X's at the *end* of the template: with a
# suffix after them it creates the literal name instead, and one killed run
# then breaks every later one with "File exists".
CONFIG=$(mktemp /tmp/pg2osync-impact.XXXXXX)
export PG2OSYNC_SOURCE_URL="postgres://postgres:postgres@localhost:15432/sourcedb"

pg()   { docker exec "$PG_CONTAINER" psql -U postgres -d sourcedb -qtAc "$1"; }
wal()  { pg "SELECT pg_current_wal_lsn() - '0/0'::pg_lsn;"; }
human(){ python3 -c "import sys;n=int(sys.argv[1]);print(f'{n/1048576:.1f} MB' if n>1048576 else f'{n/1024:.0f} kB')" "$1"; }

stop_sync() { pkill -f "pg2osync run" 2> /dev/null || true; }
drop_own_slot() { pg "SELECT pg_drop_replication_slot('pg2osync_impact') WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name='pg2osync_impact');" > /dev/null 2>&1 || true; }
cleanup()   { stop_sync; drop_own_slot; rm -f "$CONFIG"; }
trap cleanup EXIT

cat > "$CONFIG" <<'TOML'
[source]
url_env = "PG2OSYNC_SOURCE_URL"
slot_name = "pg2osync_impact"
publication = "pg2osync_impact_pub"

[target]
url = "http://localhost:9200"

[metrics]
enabled = false

[sync.impact_parent]
table = "public.impact_parent"
index = "impact_parent"

[[sync.impact_parent.children]]
table = "public.impact_child"
field = "children"
foreign_key = "parent_id"
TOML

echo "== setup: $ROWS parents, $ROWS children =="
stop_sync
pg "DROP PUBLICATION IF EXISTS pg2osync_impact_pub;" > /dev/null
pg "SELECT pg_drop_replication_slot('pg2osync_impact') WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name='pg2osync_impact');" > /dev/null
pg "DROP TABLE IF EXISTS impact_child, impact_parent;" > /dev/null
pg "CREATE TABLE impact_parent (id bigint PRIMARY KEY, name text, blob text);" > /dev/null
pg "CREATE TABLE impact_child (id bigint PRIMARY KEY, parent_id bigint NOT NULL, amount numeric(12,2));" > /dev/null
pg "ALTER TABLE impact_child REPLICA IDENTITY FULL;" > /dev/null
pg "INSERT INTO impact_parent SELECT g, 'p'||g, repeat('x', 200) FROM generate_series(1,$ROWS) g;" > /dev/null
pg "INSERT INTO impact_child SELECT g, g, 1.00 FROM generate_series(1,$ROWS) g;" > /dev/null
curl -s -XDELETE "$OS/impact_parent,.pg2osync_meta?ignore_unavailable=true" > /dev/null

echo
echo "== 1. WAL written by the source, with and without REPLICA IDENTITY FULL =="
pg "ALTER TABLE impact_parent REPLICA IDENTITY DEFAULT;" > /dev/null
before=$(wal); pg "UPDATE impact_parent SET name = name || '.' WHERE id <= 5000;" > /dev/null
default_wal=$(( $(wal) - before ))
pg "ALTER TABLE impact_parent REPLICA IDENTITY FULL;" > /dev/null
before=$(wal); pg "UPDATE impact_parent SET name = name || '.' WHERE id <= 5000;" > /dev/null
full_wal=$(( $(wal) - before ))
pg "ALTER TABLE impact_parent REPLICA IDENTITY DEFAULT;" > /dev/null
echo "   5000 updates, REPLICA IDENTITY DEFAULT: $(human $default_wal)"
echo "   5000 updates, REPLICA IDENTITY FULL:    $(human $full_wal)"
python3 -c "print(f'   FULL costs {$full_wal/$default_wal:.1f}x the WAL of DEFAULT')"
echo "   (pg2osync itself writes no WAL; this is what your own writes cost"
echo "    once a table is published, and it is charged to the database)"

echo
echo "== 2. initial load: connections, queries and snapshot duration =="
pg "SELECT pg_stat_statements_reset();" > /dev/null
nohup $BIN run -c "$CONFIG" &> /tmp/pg2osync-impact.log < /dev/null & disown
peak_conns=0
while :; do
  conns=$(pg "SELECT count(*) FROM pg_stat_activity WHERE datname='sourcedb' AND pid <> pg_backend_pid();")
  [ "$conns" -gt "$peak_conns" ] && peak_conns=$conns
  curl -s -XPOST "$OS/impact_parent/_refresh" > /dev/null
  indexed=$(curl -s "$OS/impact_parent/_count" | python3 -c "import sys,json;print(json.load(sys.stdin).get('count',0))")
  [ "$indexed" -ge "$ROWS" ] && break
  sleep 1
done
echo "   peak connections held on the source: $peak_conns"
echo "   longest transaction seen while loading:"
pg "SELECT '     ' || coalesce(max(round(extract(epoch FROM now() - xact_start)))::text, 'none') || 's'
    FROM pg_stat_activity WHERE datname='sourcedb' AND xact_start IS NOT NULL;"
echo "   top queries by call count during the load:"
pg "SELECT '     ' || calls || 'x  ' || left(regexp_replace(query, '\s+', ' ', 'g'), 90)
    FROM pg_stat_statements s JOIN pg_database d ON d.oid = s.dbid
    WHERE d.datname = 'sourcedb' AND query NOT LIKE '%pg_stat_statements%'
    ORDER BY calls DESC LIMIT 6;"

echo
echo "== 3. steady state: ${IDLE_SECS}s with no writes =="
pg "SELECT pg_stat_statements_reset();" > /dev/null
xact_before=$(pg "SELECT xact_commit FROM pg_stat_database WHERE datname='sourcedb';")
sleep "$IDLE_SECS"
xact_after=$(pg "SELECT xact_commit FROM pg_stat_database WHERE datname='sourcedb';")
queries=$(pg "SELECT coalesce(sum(calls),0) FROM pg_stat_statements s JOIN pg_database d ON d.oid = s.dbid
              WHERE d.datname='sourcedb' AND query NOT LIKE '%pg_stat_statements%';")
echo "   queries issued while idle: $queries"
echo "   transactions committed:    $(( xact_after - xact_before )) (includes this script's own probes)"
echo "   connections held:          $(pg "SELECT count(*) FROM pg_stat_activity WHERE datname='sourcedb' AND pid <> pg_backend_pid();")"

echo
echo "== 4. cost of one changed row =="
pg "SELECT pg_stat_statements_reset();" > /dev/null
pg "UPDATE impact_parent SET name='changed' WHERE id = 1;" > /dev/null
sleep 3
echo "   queries pg2osync ran to rebuild one parent document (nested children):"
pg "SELECT '     ' || calls || 'x  ' || left(regexp_replace(query, '\s+', ' ', 'g'), 80)
    FROM pg_stat_statements s JOIN pg_database d ON d.oid = s.dbid
    WHERE d.datname='sourcedb' AND query NOT LIKE '%pg_stat_statements%'
      AND query NOT LIKE '%pg_stat_activity%' AND query NOT LIKE 'UPDATE impact_parent%'
    ORDER BY calls DESC LIMIT 4;"

echo
echo "== 5. retained WAL while the pipeline runs =="
pg "SELECT '   slot ' || slot_name || ': retained ' || pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn))
    FROM pg_replication_slots WHERE slot_name='pg2osync_impact';"
echo "   (stop the process and this grows without bound: that is the one real"
echo "    operational risk an unconsumed slot puts on the database)"

stop_sync
pg "SELECT pg_drop_replication_slot('pg2osync_impact') WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name='pg2osync_impact');" > /dev/null
pg "DROP PUBLICATION IF EXISTS pg2osync_impact_pub;" > /dev/null
pg "DROP TABLE IF EXISTS impact_child, impact_parent;" > /dev/null
echo
echo "== done =="
