#!/usr/bin/env bash
# Does a client certificate actually reach the source, on every connection?
#
# This is deliberately not part of dev/e2e-test.sh. A server that demands a
# client certificate has to be built for it — its own CA, a server key the
# postmaster will accept, a pg_hba.conf that leaves no plaintext route in — and
# every other script in dev/ assumes the plain trust-authenticated container in
# docker-compose.yml. So this one builds and destroys its own postgres.
#
# The interesting assertion is the last: `validate` proves the SQL connection,
# but the replication stream is a second connection through an entirely
# different transport, and it is the one the feature existed to fix.
#
# Prerequisites: docker, openssl, a target at $OS_URL, cargo build --release.
#
# Usage: ./dev/mtls-probe.sh
set -euo pipefail

cd "$(dirname "$0")/.."
BIN=./target/release/pg2osync
OS=${OS_URL:-http://localhost:9200}
PG=pg2osync-mtls
PORT=15433
DBUSER=mtlsuser
INDEX=mtls_probe
CERTS=$(mktemp -d /tmp/pg2osync-mtls.XXXXXX)
CONFIG=$(mktemp /tmp/pg2osync-mtls-conf.XXXXXX)
LOG=/tmp/pg2osync-mtls.log
PASS=0
FAIL=0

ok()   { printf "  \033[32m✓ %s\033[0m\n" "$1"; PASS=$((PASS+1)); }
bad()  { printf "  \033[31m✗ %s\033[0m\n" "$1"; FAIL=$((FAIL+1)); }
say()  { printf "\n\033[1m== %s ==\033[0m\n" "$1"; }
check() { if [ "$2" = "$3" ]; then ok "$1 ($2)"; else bad "$1 (got '$2', want '$3')"; fi; }

psql_local() { docker exec "$PG" psql -U postgres -qtAc "$1"; }

cleanup() {
  pkill -f "pg2osync run" 2> /dev/null || true
  docker rm -f "$PG" > /dev/null 2>&1 || true
  curl -s -XDELETE "$OS/$INDEX" > /dev/null 2>&1 || true
  rm -rf "$CERTS" "$CONFIG"
}
trap cleanup EXIT

say "certificates"
# Everything below is thrown away with $CERTS: nothing here is ever committed
# and nothing outlives the run.
openssl req -x509 -newkey rsa:2048 -nodes -days 1 -sha256 \
  -keyout "$CERTS/ca.key" -out "$CERTS/ca.crt" -subj "/CN=pg2osync-probe-ca" 2> /dev/null
openssl req -newkey rsa:2048 -nodes -keyout "$CERTS/server.key" -out "$CERTS/server.csr" \
  -subj "/CN=localhost" 2> /dev/null
openssl x509 -req -in "$CERTS/server.csr" -CA "$CERTS/ca.crt" -CAkey "$CERTS/ca.key" \
  -CAcreateserial -days 1 -sha256 -out "$CERTS/server.crt" \
  -extfile <(printf 'subjectAltName=DNS:localhost,IP:127.0.0.1\n') 2> /dev/null
# the CN is the database role: `cert` authentication maps one to the other, and
# clientcert=verify-full rejects a chain-valid certificate naming anyone else
openssl req -newkey rsa:2048 -nodes -keyout "$CERTS/client.key" -out "$CERTS/client.csr" \
  -subj "/CN=$DBUSER" 2> /dev/null
openssl x509 -req -in "$CERTS/client.csr" -CA "$CERTS/ca.crt" -CAkey "$CERTS/ca.key" \
  -CAcreateserial -days 1 -sha256 -out "$CERTS/client.crt" 2> /dev/null
# a second, unrelated CA: a well-formed certificate the server must still refuse
openssl req -x509 -newkey rsa:2048 -nodes -days 1 -sha256 \
  -keyout "$CERTS/rogue-ca.key" -out "$CERTS/rogue-ca.crt" -subj "/CN=pg2osync-rogue-ca" 2> /dev/null
openssl req -newkey rsa:2048 -nodes -keyout "$CERTS/rogue.key" -out "$CERTS/rogue.csr" \
  -subj "/CN=$DBUSER" 2> /dev/null
openssl x509 -req -in "$CERTS/rogue.csr" -CA "$CERTS/rogue-ca.crt" -CAkey "$CERTS/rogue-ca.key" \
  -CAcreateserial -days 1 -sha256 -out "$CERTS/rogue.crt" 2> /dev/null
ok "CA, server, client and rogue client certificates in $CERTS"

# The unix socket keeps working so the entrypoint can still initialise the
# cluster and this script can still administer it; every TCP route is mTLS.
cat > "$CERTS/pg_hba.conf" <<'HBA'
local all all trust
hostssl all all 0.0.0.0/0 cert clientcert=verify-full
hostssl all all ::/0      cert clientcert=verify-full
HBA

say "server"
# The key is copied out of the mount and chowned inside the container:
# PostgreSQL refuses a key the postmaster does not own at 0600, and bind-mount
# ownership is not something a host can promise across Docker platforms.
docker rm -f "$PG" > /dev/null 2>&1 || true
docker run -d --name "$PG" -p "$PORT:5432" \
  -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=sourcedb \
  -v "$CERTS:/certs:ro" postgres:17 \
  bash -c 'mkdir -p /pgcerts && cp /certs/ca.crt /certs/server.crt /certs/server.key /certs/pg_hba.conf /pgcerts/ \
    && chown -R postgres:postgres /pgcerts && chmod 600 /pgcerts/server.key \
    && exec docker-entrypoint.sh postgres \
      -c ssl=on -c ssl_cert_file=/pgcerts/server.crt -c ssl_key_file=/pgcerts/server.key \
      -c ssl_ca_file=/pgcerts/ca.crt -c hba_file=/pgcerts/pg_hba.conf \
      -c wal_level=logical -c max_replication_slots=8 -c max_wal_senders=8' > /dev/null

for _ in $(seq 1 60); do
  if docker exec "$PG" pg_isready -U postgres > /dev/null 2>&1; then break; fi
  sleep 1
done
docker exec "$PG" pg_isready -U postgres > /dev/null 2>&1 \
  || { bad "postgres never came up"; docker logs "$PG" | tail -20; exit 1; }
ok "postgres:17 on $PORT with ssl=on and clientcert=verify-full"

# SUPERUSER on purpose: this probe is about the transport, and a privilege
# error on CREATE PUBLICATION would look exactly like a certificate failure
psql_local "CREATE ROLE $DBUSER LOGIN SUPERUSER REPLICATION;" > /dev/null
docker exec "$PG" psql -U postgres -d sourcedb -qtAc \
  "CREATE TABLE items (id int PRIMARY KEY, name text);" > /dev/null
ok "role $DBUSER and table public.items"

export PG2OSYNC_SOURCE_URL="postgres://$DBUSER@localhost:$PORT/sourcedb"

write_config() { # $1 = extra [source] lines
  cat > "$CONFIG" <<TOML
[source]
url_env = "PG2OSYNC_SOURCE_URL"
sslmode = "verify-full"
sslrootcert = "$CERTS/ca.crt"
$1
slot_name = "pg2osync_mtls"
publication = "pg2osync_mtls_pub"

[target]
url = "$OS"

[metrics]
enabled = false

[sync.items]
table = "public.items"
index = "$INDEX"
TOML
}

say "without a client certificate"
write_config ""
out=$($BIN validate -c "$CONFIG" 2>&1 || true)
if printf '%s' "$out" | grep -qi "sslcert"; then
  ok "validate fails and names the missing options"
else
  bad "validate did not point at sslcert/sslkey: $(printf '%s' "$out" | tail -3)"
fi

say "with the client certificate"
write_config "sslcert = \"$CERTS/client.crt\"
sslkey = \"$CERTS/client.key\""
if out=$($BIN validate -c "$CONFIG" 2>&1); then
  printf '%s' "$out" | grep -q "client certificate presented" \
    && ok "validate reports the certificate it presented" \
    || bad "no 'client certificate presented' line"
  printf '%s' "$out" | grep -q "server accepted the client certificate" \
    && ok "the server reported the DN back through pg_stat_ssl" \
    || bad "no 'server accepted' line: $(printf '%s' "$out" | tail -3)"
else
  bad "validate failed: $(printf '%s' "$out" | tail -5)"
fi

say "with a certificate from another CA"
write_config "sslcert = \"$CERTS/rogue.crt\"
sslkey = \"$CERTS/rogue.key\""
if $BIN validate -c "$CONFIG" > /dev/null 2>&1; then
  bad "a certificate from an untrusted CA was accepted"
else
  ok "a certificate from an untrusted CA is refused"
fi

say "the replication stream"
write_config "sslcert = \"$CERTS/client.crt\"
sslkey = \"$CERTS/client.key\""
: > "$LOG"
nohup $BIN run -c "$CONFIG" >> "$LOG" 2>&1 < /dev/null & disown
sleep 8
docker exec "$PG" psql -U postgres -d sourcedb -qtAc \
  "INSERT INTO items VALUES (1, 'over mtls');" > /dev/null
sleep 6
curl -s -XPOST "$OS/$INDEX/_refresh" > /dev/null 2>&1 || true
got=$(curl -s "$OS/$INDEX/_doc/1" \
  | python3 -c "import sys,json;print(json.load(sys.stdin).get('_source',{}).get('name','<missing>'))")
check "a row streamed over the mTLS replication connection" "$got" "over mtls"
grep -q "client_cert=yes" "$LOG" \
  && ok "the startup log records client_cert=yes" \
  || bad "no client_cert=yes in $LOG"

say "result"
printf "  %d passed, %d failed\n\n" "$PASS" "$FAIL"
[ "$FAIL" -eq 0 ]
