#!/usr/bin/env python3
"""pg2osync benchmark driver: fresh lift, backfill throughput, live latency, DB impact."""
import json
import subprocess
import sys
import time
import urllib.request

PSQL = ["docker", "exec", "-i", "dev-postgres-1", "psql", "-U", "postgres", "-d", "sourcedb", "-tA"]
OS = "http://localhost:9200"
CFG = "/tmp/bench.toml"
INDICES = ["users_index", "bench_a_index", "bench_users_index",
           "bench_products_index", "bench_events_index", "bench_wide_index"]


def psql(sql: str) -> str:
    r = subprocess.run(PSQL, input=sql.encode(), capture_output=True)
    if r.returncode != 0:
        raise RuntimeError(r.stderr.decode())
    return r.stdout.decode().strip()


def os_req(method: str, path: str, body=None):
    req = urllib.request.Request(OS + path, method=method)
    if body is not None:
        req.add_header("Content-Type", "application/json")
        req.data = json.dumps(body).encode()
    try:
        with urllib.request.urlopen(req) as resp:
            return json.load(resp)
    except urllib.error.HTTPError as e:
        return json.load(e)


def os_count(index):
    try:
        return os_req("GET", f"/{index}/_count").get("count", 0)
    except Exception:
        return 0


def os_total():
    return sum(os_count(i) for i in INDICES)


def pg_target():
    return int(psql(
        "SELECT sum(c) FROM (SELECT count(*) c FROM users UNION ALL "
        "SELECT count(*) FROM bench_a UNION ALL SELECT count(*) FROM bench_users "
        "UNION ALL SELECT count(*) FROM bench_products UNION ALL SELECT count(*) "
        "FROM bench_events UNION ALL SELECT count(*) FROM bench_wide) s"))


def db_stats():
    out = psql("SELECT xact_commit, pg_wal_lsn_diff(pg_current_wal_lsn(),'0/0')::bigint "
               "FROM pg_stat_database WHERE datname='sourcedb'")
    xacts, wal = out.split("|")
    return int(xacts), int(wal)


print("=== 0. reset ===")
subprocess.run(["pkill", "-f", "pg2osync run"], capture_output=True)
time.sleep(1)
psql("DROP PUBLICATION IF EXISTS pg2osync_pub;")
psql("SELECT pg_drop_replication_slot('pg2osync') WHERE EXISTS "
     "(SELECT 1 FROM pg_replication_slots WHERE slot_name='pg2osync');")
for idx in INDICES + [".pg2osync_meta"]:
    os_req("DELETE", f"/{idx}")

with open(CFG, "w") as f:
    f.write("""
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
""")

target = pg_target()
xacts0, wal0 = db_stats()
print(f"target docs: {target}")

print("=== 1. lift from zero (release binary) ===")
import os
env = dict(os.environ, PG2OSYNC_SOURCE_URL="postgres://postgres:postgres@localhost:15432/sourcedb",
           RUST_LOG="info")
logf = open("/tmp/bench.log", "w")
proc = subprocess.Popen(["./target/release/pg2osync", "run", "-c", CFG],
                        stdout=logf, stderr=subprocess.STDOUT, env=env)

t0 = time.time()
last, stable = 0, 0
while True:
    time.sleep(2)
    elapsed = time.time() - t0
    c = os_total()
    print(f"  t={elapsed:.0f}s indexed={c}/{target}")
    if c >= target:
        break
    stable = stable + 1 if c == last else 0
    last = c
    if stable >= 6 and elapsed > 30:
        print("STALL — aborting wait")
        break
backfill_secs = time.time() - t0
rate = target / backfill_secs if backfill_secs else 0
print(f"BACKFILL: {target} docs in {backfill_secs:.1f}s -> {rate:.0f} docs/s")

print("=== 2. slot lag after backfill ===")
print(psql("SELECT slot_name || ' lag_bytes=' || pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn) "
           "FROM pg_replication_slots WHERE slot_name='pg2osync'"))

print("=== 3. live insert latency (200 single-row commits) ===")
lat = []
for i in range(200):
    rid = 500000 + i
    t0 = time.time()
    psql(f"INSERT INTO bench_users (id,name,email,city,country,age,score,active,created_at,tags) "
         f"VALUES ({rid},'lat_{i}','l{i}@x.io','x','y',30,'1.0',true,now(),'{{}}')")
    while True:
        doc = os_req("GET", f"/bench_users_index/_doc/{rid}")
        if doc.get("found"):
            break
        time.sleep(0.02)
    lat.append((time.time() - t0) * 1000)
lat.sort()
n = len(lat)
print(f"live insert latency ms: min={lat[0]:.0f} p50={lat[n//2]:.0f} p90={lat[int(n*.9)]:.0f} "
      f"p99={lat[int(n*.99)]:.0f} max={lat[-1]:.0f} mean={sum(lat)/n:.0f}")

print("=== 4. bulk update propagation (1000 updates, one transaction) ===")
psql("UPDATE bench_users SET city='BENCH_UPDATED' WHERE id <= 1000 AND id > 400000")
t0 = time.time()
deadline = t0 + 60
upd = 0
while time.time() < deadline:
    res = os_req("GET", "/bench_users_index/_search?size=0",
                 {"query": {"term": {"city": "BENCH_UPDATED"}}})
    upd = res["hits"]["total"]["value"]
    if upd >= 1000:
        break
    time.sleep(0.25)
print(f"updates propagated: {upd}/1000 in {time.time()-t0:.1f}s")

print("=== 5. delete propagation ===")
ids_to_del = [r for r in psql(
    "SELECT string_agg(id::text, ',') FROM (SELECT id FROM bench_users WHERE id <= 400000 ORDER BY id LIMIT 500) s").split(",") if r]
psql(f"DELETE FROM bench_users WHERE id IN ({','.join(ids_to_del)})")
t0 = time.time()
gone = -1
while time.time() < t0 + 60:
    gone = sum(1 for i in ids_to_del
               if not os_req("GET", f"/bench_users_index/_doc/{i}").get("found"))
    if gone == len(ids_to_del):
        break
    time.sleep(0.25)
print(f"deletes propagated: {gone}/{len(ids_to_del)} in {time.time()-t0:.1f}s")

print("=== 6. final consistency ===")
final_pg = pg_target()
final_os = os_total()
print(f"pg_rows={final_pg} os_docs={final_os} delta={final_pg - final_os}")
xacts1, wal1 = db_stats()
print(f"db impact during benchmark: xact_commits={xacts1-xacts0}, wal_bytes={(wal1-wal0)/1e6:.1f}MB")

proc.terminate()
try:
    proc.wait(timeout=10)
except subprocess.TimeoutExpired:
    proc.kill()
print("=== done ===")
