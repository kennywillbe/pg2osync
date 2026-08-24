#!/bin/bash
# pg2osync benchmark: fresh lift from zero, measure backfill + live latency + DB impact.
set -e
cd "$(dirname "$0")/.."

PSQL="docker exec -i dev-postgres-1 psql -U postgres -d sourcedb"
OS="http://localhost:9200"
CFG=/tmp/bench.toml

echo "=== 0. reset state ==="
pkill -f "pg2osync run" 2>/dev/null || true; sleep 1
$PSQL -q << 'SQL'
DROP PUBLICATION IF EXISTS pg2osync_pub;
SELECT pg_drop_replication_slot('pg2osync') WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name='pg2osync');
SQL
curl -s -XDELETE "$OS/users_index,bench_a_index,users_index_v2,bench_users_index,bench_products_index,bench_events_index,bench_wide_index,.pg2osync_meta" > /dev/null || true

cat > $CFG << 'EOF'
[source]
url_env = "PG2OSYNC_SOURCE_URL"
slot_name = "pg2osync"
publication = "pg2osync_pub"

[target]
url = "http://localhost:9200"

[sync.users]
table = "public.users"
index = "users_index"

[sync.bench_a]
table = "public.bench_a"
index = "bench_a_index"

[sync.bench_users]
table = "public.bench_users"
index = "bench_users_index"

[sync.bench_products]
table = "public.bench_products"
index = "bench_products_index"

[sync.bench_events]
table = "public.bench_events"
index = "bench_events_index"

[sync.bench_wide]
table = "public.bench_wide"
index = "bench_wide_index"
EOF

os_count() {
  local total=0
  for idx in users_index bench_a_index bench_users_index bench_products_index bench_events_index bench_wide_index; do
    c=$(curl -s "$OS/$idx/_count" | python3 -c "import json,sys
try: print(json.load(sys.stdin).get('count',0))
except: print(0)")
    total=$((total + c))
  done
  echo $total
}

db_stats() {
  $PSQL -tAc "SELECT xact_commit, wal_bytes FROM pg_stat_database, LATERAL (SELECT pg_wal_lsn_diff(pg_current_wal_lsn(),'0/0')::bigint) w(wal_bytes) WHERE datname='sourcedb'"
}

echo "=== 1. start binary (release) ==="
export PG2OSYNC_SOURCE_URL="postgres://postgres:postgres@localhost:15432/sourcedb"
START=$(date +%s)
RUST_LOG=info ./target/release/pg2osync run -c $CFG &> /tmp/bench.log &
BIN_PID=$!

TARGET_DOCS=$(($($PSQL -tAc "SELECT sum(cnt) FROM (SELECT count(*) cnt FROM users UNION ALL SELECT count(*) FROM bench_a UNION ALL SELECT count(*) FROM bench_users UNION ALL SELECT count(*) FROM bench_products UNION ALL SELECT count(*) FROM bench_events UNION ALL SELECT count(*) FROM bench_wide) s") | tr -d ' '))
echo "target docs: $TARGET_DOCS"

echo "=== 2. wait for backfill ==="
PREV=0; STABLE=0
while true; do
  NOW=$(date +%s); ELAPSED=$((NOW-START))
  C=$(os_count)
  echo "t=${ELAPSED}s indexed=$C/$TARGET_DOCS"
  if [ "$C" -ge "$TARGET_DOCS" ]; then break; fi
  if [ "$C" == "$PREV" ]; then STABLE=$((STABLE+1)); else STABLE=0; fi
  if [ $STABLE -ge 5 ] && [ $ELAPSED -gt 30 ]; then echo "STALL detected"; break; fi
  PREV=$C
  sleep 3
done
BACKFILL_END=$(date +%s)
BACKFILL_SECS=$((BACKFILL_END-START))
echo "backfill complete in ${BACKFILL_SECS}s -> $(( TARGET_DOCS / (BACKFILL_SECS>0?BACKFILL_SECS:1) )) docs/sec"

echo "=== 3. db impact snapshot after backfill ==="
$PSQL -tAc "SELECT slot_name, confirmed_flush_lsn, pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn) AS lag_bytes FROM pg_replication_slots WHERE slot_name='pg2osync'"

echo "=== 4. live DML latency test ==="
# 200 inserts measured individually end-to-end (commit -> visible in OS)
LATENCIES=()
for i in $(seq 1 200); do
  ID=$((500000+i))
  T0=$(python3 -c 'import time; print(time.time())')
  $PSQL -q -c "INSERT INTO bench_users (id,name,email,city,country,age,score,active,created_at,tags) VALUES ($ID,'lat_$i','l$i@x.io','x','y',30,'1.0',true,now(),'{}')"
  # poll until visible
  while true; do
    FOUND=$(curl -s "$OS/bench_users_index/_doc/$ID" | python3 -c "import json,sys
print(1 if json.load(sys.stdin).get('found') else 0)")
    [ "$FOUND" == "1" ] && break
    sleep 0.05
  done
  T1=$(python3 -c 'import time; print(time.time())')
  LATENCIES+=("$(python3 -c "print(round(($T1-$T0)*1000))")")
done
python3 -c "
lat = sorted(int(x) for x in ['$LATENCIES'.replace('\n','')][0].split(',')) if False else sorted(int(x) for x in '''${LATENCIES[@]}'''.split())
n=len(lat)
print(f'live insert latency ms: min={lat[0]} p50={lat[n//2]} p90={lat[int(n*0.9)]} p99={lat[int(n*0.99)]} max={lat[-1]} mean={sum(lat)/n:.0f}')
"

echo "=== 5. update/delete propagation ==="
UPD_START=$(date +%s)
$PSQL -q -c "UPDATE bench_users SET city='UPDATED' WHERE id <= 1000"
sleep 3
UPD_COUNT=$(curl -s "$OS/bench_users_index/_search" -H 'Content-Type: application/json' -d '{"query":{"term":{"city":"UPDATED"}}}' | python3 -c "import json,sys; print(json.load(sys.stdin)['hits']['total']['value'])")
UPD_END=$(date +%s)
echo "updated docs propagated: $UPD_COUNT/1000 in ~$((UPD_END-UPD_START))s"

DEL_START=$(date +%s)
$PSQL -q -c "DELETE FROM bench_users WHERE id > 400000 AND id <= 401000"
SQL
sleep 3
echo "=== 6. final consistency check ==="
PG_TOTAL=$($PSQL -tAc "SELECT sum(cnt) FROM (SELECT count(*) cnt FROM users UNION ALL SELECT count(*) FROM bench_a UNION ALL SELECT count(*) FROM bench_users UNION ALL SELECT count(*) FROM bench_products UNION ALL SELECT count(*) FROM bench_events UNION ALL SELECT count(*) FROM bench_wide) s" | tr -d ' ')
OS_TOTAL=$(os_count)
echo "pg_rows=$PG_TOTAL os_docs=$OS_TOTAL delta=$((PG_TOTAL-OS_TOTAL))"

kill $BIN_PID 2>/dev/null || true
echo "=== benchmark complete ==="
