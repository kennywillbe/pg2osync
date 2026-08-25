#!/usr/bin/env bash
# Does a MySQL failover actually resume, or does it silently stop working?
#
# Two things have to hold at once, and neither is provable against a single
# server. The stream has to continue from a GTID position the new primary can
# honour, and the documents written after the promotion have to *land* — the
# version generation exists because the new server's binlog coordinates are a
# different, usually lower, numbering than the target already holds.
#
# Builds its own pair of containers, promotes the replica, and asserts both.
#
# Usage: ./dev/failover-probe.sh
set -euo pipefail

cd "$(dirname "$0")/.."
BIN=./target/release/pg2osync
OS=${OS_URL:-http://localhost:9200}
PRIMARY=pg2osync-fo-primary
REPLICA=pg2osync-fo-replica
PW=mysqlpw
INDEX=fo_probe
CONFIG=$(mktemp /tmp/pg2osync-failover.XXXXXX)
LOG=/tmp/pg2osync-failover.log
# The second run needs its own log: the first legitimately reports an initial
# load, and grepping one file for both would credit that line to the resume.
LOG2=/tmp/pg2osync-failover-resumed.log
PASS=0
FAIL=0

ok()   { printf "  \033[32m✓ %s\033[0m\n" "$1"; PASS=$((PASS+1)); }
bad()  { printf "  \033[31m✗ %s\033[0m\n" "$1"; FAIL=$((FAIL+1)); }
say()  { printf "\n\033[1m== %s ==\033[0m\n" "$1"; }
check() { if [ "$2" = "$3" ]; then ok "$1 ($2)"; else bad "$1 (got '$2', want '$3')"; fi; }

on_primary() { docker exec "$PRIMARY" mysql -uroot -p$PW -N -B ${2:-sourcedb} -e "$1" 2>/dev/null; }
on_replica() { docker exec "$REPLICA" mysql -uroot -p$PW -N -B ${2:-sourcedb} -e "$1" 2>/dev/null; }
refresh()    { curl -s -XPOST "$OS/$INDEX/_refresh" > /dev/null 2>&1 || true; }
os_field()   { curl -s "$OS/$INDEX/_doc/$1" | python3 -c "import sys,json;print(json.load(sys.stdin).get('_source',{}).get('v','<missing>'))"; }
os_count()   { curl -s "$OS/$INDEX/_count" | python3 -c "import sys,json;print(json.load(sys.stdin).get('count',0))"; }
stop_sync()  { pkill -f "pg2osync run" 2> /dev/null || true; sleep 1; }

cleanup() {
  stop_sync
  docker rm -f "$PRIMARY" "$REPLICA" > /dev/null 2>&1 || true
  rm -f "$CONFIG"
}
trap cleanup EXIT

mysqld_flags() {
  echo "--log-bin=mysql-bin --binlog-format=ROW --binlog-row-image=FULL \
        --gtid-mode=ON --enforce-gtid-consistency=ON --log-replica-updates=ON"
}

say "0. a primary and a replica, both with GTIDs on"
docker rm -f "$PRIMARY" "$REPLICA" > /dev/null 2>&1 || true
# shellcheck disable=SC2046
docker run -d --name "$PRIMARY" -p 13401:3306 \
  -e MYSQL_ROOT_PASSWORD=$PW -e MYSQL_DATABASE=sourcedb mysql:8.0 \
  $(mysqld_flags) --server-id=401 > /dev/null
# shellcheck disable=SC2046
docker run -d --name "$REPLICA" -p 13402:3306 \
  -e MYSQL_ROOT_PASSWORD=$PW -e MYSQL_DATABASE=sourcedb mysql:8.0 \
  $(mysqld_flags) --server-id=402 > /dev/null
for container in "$PRIMARY" "$REPLICA"; do
  for _ in $(seq 1 60); do
    docker exec "$container" mysqladmin ping -h 127.0.0.1 -p$PW > /dev/null 2>&1 && break
    sleep 2
  done
done
primary_ip=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$PRIMARY")
ok "both servers up, primary at $primary_ip"

on_primary "CREATE TABLE fo(id int primary key, v varchar(40));" > /dev/null
on_primary "INSERT INTO fo VALUES (1,'one'),(2,'two');" > /dev/null
# caching_sha2_password over a plain connection needs the key fetched; the
# native plugin keeps the probe about failover rather than about auth
on_primary "CREATE USER repl@'%' IDENTIFIED WITH mysql_native_password BY 'repl'; \
            GRANT REPLICATION SLAVE ON *.* TO repl@'%';" mysql > /dev/null
on_replica "CHANGE REPLICATION SOURCE TO SOURCE_HOST='$primary_ip', SOURCE_PORT=3306, \
            SOURCE_USER='repl', SOURCE_PASSWORD='repl', SOURCE_AUTO_POSITION=1; \
            START REPLICA;" mysql > /dev/null
for _ in $(seq 1 60); do
  [ "$(on_replica "SELECT count(*) FROM fo;")" = "2" ] && break
  sleep 1
done
check "the replica has the primary's rows" "$(on_replica "SELECT count(*) FROM fo;")" "2"

# Push the primary's file index well past the replica's, so that after the
# promotion the coordinate really is *behind* the checkpoint. That is the case
# the version generation exists for, and a probe where the numbering happens to
# line up would pass without testing it.
for _ in $(seq 1 6); do on_primary "FLUSH BINARY LOGS;" mysql > /dev/null; done
ok "primary rotated to $( { on_primary "SHOW MASTER STATUS;" mysql || true; } | awk '{print $1}')"

say "1. stream from the primary and take a checkpoint"
cat > "$CONFIG" <<TOML
[source]
flavor = "mysql"
url_env = "PG2OSYNC_FO_URL"
server_id = 940401

[target]
url = "$OS"

[metrics]
enabled = false

[sync.fo]
table = "sourcedb.fo"
index = "$INDEX"
TOML
curl -s -XDELETE "$OS/$INDEX,.pg2osync_meta?ignore_unavailable=true" > /dev/null
export PG2OSYNC_FO_URL="mysql://root:$PW@localhost:13401/sourcedb"
nohup $BIN run -c "$CONFIG" > "$LOG" 2>&1 < /dev/null & disown
sleep 5
on_primary "INSERT INTO fo VALUES (3,'before-failover');" > /dev/null
for _ in $(seq 1 30); do
  refresh
  [ "$(os_count)" = "3" ] && break
  sleep 1
done
check "everything the primary had is indexed" "$(os_count)" "3"
stop_sync
position=$(curl -s "$OS/.pg2osync_meta/_search?q=*:*&size=5" \
  | python3 -c "import sys,json;print(next((h['_source'].get('position','') for h in json.load(sys.stdin)['hits']['hits'] if 'position' in h['_source']), ''))")
if [[ "$position" == *";gtid="* ]]; then
  ok "the checkpoint carries a GTID position (${position#*;gtid=})"
else
  bad "the checkpoint has no GTID position: '$position'"
fi
primary_token=$(echo "$position" | sed 's/;.*//')

say "2. promote the replica, exactly as a failover would"
on_replica "STOP REPLICA; RESET REPLICA ALL;" mysql > /dev/null
# The statement was renamed in 8.4, so try both and tolerate the one this server
# does not have: under `pipefail` an unrecognised statement would otherwise take
# the whole probe down.
replica_file=$( { on_replica "SHOW MASTER STATUS;" mysql \
                    || on_replica "SHOW BINARY LOG STATUS;" mysql || true; } \
                | awk 'NR==1 {print $1":"$2}')
ok "promoted; its own coordinate is $replica_file against the checkpoint's $primary_token"
# The row that only the new primary ever had. If the stream cannot resume, this
# never arrives; if the versions cannot advance, it arrives and is refused.
on_replica "INSERT INTO fo VALUES (4,'after-failover');" > /dev/null
on_replica "UPDATE fo SET v='changed-after-failover' WHERE id = 1;" > /dev/null

say "3. point the pipeline at the new primary"
export PG2OSYNC_FO_URL="mysql://root:$PW@localhost:13402/sourcedb"
nohup $BIN run -c "$CONFIG" > "$LOG2" 2>&1 < /dev/null & disown
for _ in $(seq 1 40); do
  refresh
  [ "$(os_count)" = "4" ] && break
  sleep 1
done
if grep -q "no usable checkpoint; initial load" "$LOG2"; then
  bad "it ran a full initial load instead of resuming"
else
  ok "no initial load: the checkpoint was usable against a different server"
fi
if grep -q "binlog dump from gtid" "$LOG2"; then
  ok "it asked for the stream by GTID"
else
  bad "it asked by coordinate, which the new primary cannot honour"
fi
check "the row only the new primary has arrived" "$(os_field 4)" "after-failover"
check "an update after the promotion landed" "$(os_field 1)" "changed-after-failover"
check "nothing was lost across the failover" "$(os_count)" "4"
if grep -q "versioning documents from a new generation" "$LOG2"; then
  ok "a new version generation opened, which is what let those writes land"
else
  bad "no generation opened, so nothing proves a lower coordinate is survivable"
fi
stop_sync

printf "\n\033[1mRESULT: %d passed, %d failed\033[0m\n" "$PASS" "$FAIL"
[ "$FAIL" = "0" ]
