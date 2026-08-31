#!/usr/bin/env bash
# End-to-end suite for the Kubernetes operator.
#
# What it proves is the operator's own claim and nothing the other suites
# already own: a `Pg2osync` becomes the ConfigMap `run --config-dir` expects
# and a Deployment of one replica, that pipeline really moves rows out of a
# PostgreSQL and into an OpenSearch — both inside the cluster, so the pod's own
# service names are what it resolves — an edited resource reaches the running
# pipeline, a spec that hides a credential is refused in `status` instead of
# deployed, and deleting the resource takes everything the operator made with
# it.
#
# It needs a cluster, so it is heavy: kind pulls a node image, starts a control
# plane and loads two images into it. That is why this runs nightly and on a
# change that touches it, not on every pull request.
#
# The cluster is this run's own — named, created and deleted here — so no lock
# is taken and nothing on the shared dev stack is touched. Two runs at once
# need two names (KIND_CLUSTER).
#
# Prerequisites:
#   docker, kubectl, and kind (https://kind.sigs.k8s.io — `brew install kind`
#   or `go install sigs.k8s.io/kind@v0.33.0`)
#
# Usage: ./dev/e2e-operator.sh
#   KIND_CLUSTER      cluster name          (default pg2osync-operator)
#   KIND_NODE_IMAGE   pinned node image     (default kindest/node v1.34.11)
#   E2E_KEEP_CLUSTER  1 to keep it for a post-mortem  (default empty)
#   E2E_SKIP_BUILD    1 to reuse images already built (default empty)
set -euo pipefail

cd "$(dirname "$0")/.."

CLUSTER=${KIND_CLUSTER:-pg2osync-operator}
NODE_IMAGE=${KIND_NODE_IMAGE:-kindest/node:v1.34.11@sha256:44e222ee2132dab25ff87301682f89eb82c7880ea3a1bf543bfe9708fd08d67d}
PIPELINE_IMAGE=pg2osync:e2e
OPERATOR_IMAGE=pg2osync-operator:e2e
PG_IMAGE=${E2E_PG_IMAGE:-postgres:17}
OS_IMAGE=${E2E_OS_IMAGE:-opensearchproject/opensearch:2.19.6}
NS=pg2osync
WORK=$(mktemp -d /tmp/pg2osync-operator.XXXXXX)
KUBECONFIG=$WORK/kubeconfig
export KUBECONFIG

PASS=0; FAIL=0; DONE=0

say()   { printf "\n\033[1m== %s ==\033[0m\n" "$1"; }
ok()    { printf "  \033[32m✓ %s\033[0m\n" "$1"; PASS=$((PASS + 1)); }
bad()   { printf "  \033[31m✗ %s\033[0m\n" "$1"; FAIL=$((FAIL + 1)); }
check() { if [ "$2" = "$3" ]; then ok "$1 ($2)"; else bad "$1 (got '$2', want '$3')"; fi }
# check for a value too long to read on one line, such as a whole config file.
same() {
  if [ "$2" = "$3" ]; then ok "$1"; else
    bad "$1"
    printf -- '--- got ---\n%s\n--- want ---\n%s\n' "$2" "$3"
  fi
}

k()   { kubectl -n "$NS" "$@"; }
psql_() { k exec deploy/postgres -- psql -U postgres -d sourcedb -qtAc "$1"; }
os()  { k exec deploy/opensearch -- curl -s "http://localhost:9200$1"; }

cleanup() {
  # A wait that times out ends the run through errexit, before the summary and
  # while the only evidence is still inside a cluster about to be deleted.
  if [ "$FAIL" -ne 0 ] || [ "$DONE" = 0 ]; then
    k get pods -o wide || true
    k logs deploy/pg2osync-operator --tail=60 || true
    k logs deploy/tenant-a --tail=60 || true
  fi
  if [ -n "${E2E_KEEP_CLUSTER:-}" ]; then
    echo "keeping the cluster $CLUSTER; KUBECONFIG=$KUBECONFIG"
    return
  fi
  kind delete cluster --name "$CLUSTER" > /dev/null 2>&1 || true
  rm -rf "$WORK"
}
trap cleanup EXIT

# The count a field of every document has to reach, or the last value seen.
await_count() {
  local index=$1 want=$2 got=""
  for _ in $(seq 1 90); do
    got=$(os "/$index/_count" | python3 -c \
      "import sys,json
try: print(json.load(sys.stdin)['count'])
except Exception: print('none')")
    [ "$got" = "$want" ] && break
    sleep 2
  done
  printf '%s' "$got"
}

await_gone() {
  local kind=$1 name=$2
  for _ in $(seq 1 60); do
    k get "$kind" "$name" > /dev/null 2>&1 || { printf 'gone'; return; }
    sleep 2
  done
  printf 'still there'
}

await_field() {
  local resource=$1 path=$2 want=$3 got=""
  for _ in $(seq 1 90); do
    got=$(k get "$resource" -o jsonpath="$path" 2> /dev/null || true)
    [ "$got" = "$want" ] && break
    sleep 2
  done
  printf '%s' "$got"
}

# "yes" once the field matches the pattern, so a section added to a file can be
# waited for without spelling the whole file out again.
await_match() {
  local resource=$1 path=$2 pattern=$3
  for _ in $(seq 1 90); do
    if k get "$resource" -o jsonpath="$path" 2> /dev/null | grep -q "$pattern"; then
      printf 'yes'
      return
    fi
    sleep 2
  done
  printf 'no'
}

for tool in docker kubectl kind; do
  command -v "$tool" > /dev/null || { echo "$tool is missing"; exit 1; }
done

say "1. a cluster of this run's own, and the images it runs"
kind create cluster --name "$CLUSTER" --image "$NODE_IMAGE" --kubeconfig "$KUBECONFIG" \
  --wait 120s > /dev/null
ok "kind cluster $CLUSTER"
if [ -z "${E2E_SKIP_BUILD:-}" ]; then
  DOCKER_BUILDKIT=1 docker build -t "$PIPELINE_IMAGE" . > "$WORK/build-pipeline.log" 2>&1 \
    || { bad "the pipeline image did not build"; tail -30 "$WORK/build-pipeline.log"; exit 1; }
  DOCKER_BUILDKIT=1 docker build --target operator -t "$OPERATOR_IMAGE" . \
    > "$WORK/build-operator.log" 2>&1 \
    || { bad "the operator image did not build"; tail -30 "$WORK/build-operator.log"; exit 1; }
fi
# A node has no registry to pull from, so both images are handed to it
# directly; the manifests name no pull policy, which for a tag that is not
# `latest` means the loaded copy is used.
kind load docker-image --name "$CLUSTER" "$PIPELINE_IMAGE" "$OPERATOR_IMAGE" > /dev/null
ok "both images loaded into the node"

say "2. a source and a target inside the cluster"
cat > "$WORK/stack.yaml" << YAML
apiVersion: v1
kind: Namespace
metadata:
  name: $NS
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: postgres
  namespace: $NS
spec:
  replicas: 1
  selector:
    matchLabels: { app: postgres }
  template:
    metadata:
      labels: { app: postgres }
    spec:
      containers:
        - name: postgres
          image: $PG_IMAGE
          args: ["-c", "wal_level=logical", "-c", "max_replication_slots=10", "-c", "max_wal_senders=10"]
          env:
            - { name: POSTGRES_USER, value: postgres }
            - { name: POSTGRES_PASSWORD, value: postgres }
            - { name: POSTGRES_DB, value: sourcedb }
            - { name: PGDATA, value: /var/lib/postgresql/data/pgdata }
          ports: [{ containerPort: 5432 }]
          readinessProbe:
            exec:
              command: ["pg_isready", "-U", "postgres", "-d", "sourcedb"]
            periodSeconds: 5
---
apiVersion: v1
kind: Service
metadata:
  name: postgres
  namespace: $NS
spec:
  selector: { app: postgres }
  ports: [{ port: 5432, targetPort: 5432 }]
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: opensearch
  namespace: $NS
spec:
  replicas: 1
  selector:
    matchLabels: { app: opensearch }
  template:
    metadata:
      labels: { app: opensearch }
    spec:
      containers:
        - name: opensearch
          image: $OS_IMAGE
          env:
            - { name: discovery.type, value: single-node }
            - { name: DISABLE_SECURITY_PLUGIN, value: "true" }
            - { name: DISABLE_INSTALL_DEMO_CONFIG, value: "true" }
            - { name: OPENSEARCH_JAVA_OPTS, value: "-Xms512m -Xmx512m" }
          ports: [{ containerPort: 9200 }]
          readinessProbe:
            httpGet: { path: /_cluster/health, port: 9200 }
            periodSeconds: 5
            failureThreshold: 60
---
apiVersion: v1
kind: Service
metadata:
  name: opensearch
  namespace: $NS
spec:
  selector: { app: opensearch }
  ports: [{ port: 9200, targetPort: 9200 }]
---
apiVersion: v1
kind: Secret
metadata:
  name: pg2osync-credentials
  namespace: $NS
stringData:
  # a throwaway cluster's throwaway database, so the suite proves the pipeline
  # reads its credentials from a Secret rather than from the spec
  PG2OSYNC_SOURCE_URL: postgres://postgres:postgres@postgres:5432/sourcedb
YAML
kubectl apply -f "$WORK/stack.yaml" > /dev/null
k wait --for=condition=available deploy/postgres --timeout=180s > /dev/null
k wait --for=condition=available deploy/opensearch --timeout=300s > /dev/null
ok "PostgreSQL and OpenSearch are up"
psql_ "CREATE TABLE orders (id bigint PRIMARY KEY, customer text NOT NULL);" > /dev/null
psql_ "INSERT INTO orders SELECT g, 'customer-' || g FROM generate_series(1, 5) g;" > /dev/null
psql_ "CREATE TABLE order_lines (id bigint PRIMARY KEY, sku text NOT NULL);" > /dev/null
psql_ "INSERT INTO order_lines SELECT g, 'sku-' || g FROM generate_series(1, 3) g;" > /dev/null
ok "the source has rows to load"

say "3. the operator"
# The manifests pin released images; this run has just built its own.
kubectl kustomize deploy/operator \
  | sed -E -e "s#ghcr\.io/kennywillbe/pg2osync-operator:[^\"[:space:]]+#$OPERATOR_IMAGE#" \
           -e "s#ghcr\.io/kennywillbe/pg2osync:[^\"[:space:]]+#$PIPELINE_IMAGE#" \
    > "$WORK/operator.yaml"
kubectl apply -f "$WORK/operator.yaml" > /dev/null
k wait --for=condition=available deploy/pg2osync-operator --timeout=180s > /dev/null
ok "the operator is running"
check "the definition is established" \
  "$(kubectl get crd pg2osyncs.pg2osync.io -o jsonpath='{.spec.group}')" "pg2osync.io"

say "4. one resource becomes one ConfigMap and one Deployment"
write_cr() {
  cat > "$WORK/cr.yaml" << YAML
apiVersion: pg2osync.io/v1alpha1
kind: Pg2osync
metadata:
  name: tenant-a
  namespace: $NS
spec:
  image: $PIPELINE_IMAGE
  secretRefs: [pg2osync-credentials]
  env:
    RUST_LOG: pg2osync=info
  configs:
    orders:
      source:
        url_env: PG2OSYNC_SOURCE_URL
        slot_name: pg2osync_orders
        publication: pg2osync_orders_pub
      target:
        url: http://opensearch:9200
      metrics:
        bind: 0.0.0.0:9100
      sync:
        orders:
          table: public.orders
          index: e2e-orders
$1
YAML
  kubectl apply -f "$WORK/cr.yaml" > /dev/null
}
write_cr ""
# Byte for byte, because the file is the contract with the pipeline: the unit
# tests assert the same rendering, this asserts the rendering reaches a cluster
# unchanged.
want=$(printf '[source]\npublication = "pg2osync_orders_pub"\nslot_name = "pg2osync_orders"\nurl_env = "PG2OSYNC_SOURCE_URL"\n\n[target]\nurl = "http://opensearch:9200"\n\n[metrics]\nbind = "0.0.0.0:9100"\n\n[sync.orders]\nindex = "e2e-orders"\ntable = "public.orders"\n')
same "the ConfigMap carries the file --config-dir reads" \
  "$(await_field cm/tenant-a '{.data.orders\.toml}' "$want")" "$want"
check "one replica, because one process owns the slot" \
  "$(await_field deploy/tenant-a '{.spec.replicas}' 1)" "1"
if k rollout status deploy/tenant-a --timeout=240s > /dev/null; then
  ok "the pipeline pod is ready"
else
  bad "the pipeline pod never became ready"
fi
check "the status reports what the operator can see" \
  "$(await_field pg2osync/tenant-a '{.status.ready}' true)" "true"
check "and how many sources it rendered" \
  "$(await_field pg2osync/tenant-a '{.status.sources}' 1)" "1"

say "5. rows reach the index"
check "the initial load arrived" "$(await_count e2e-orders 5)" "5"
psql_ "INSERT INTO orders VALUES (6, 'customer-6');" > /dev/null
check "a change streamed after it" "$(await_count e2e-orders 6)" "6"

say "6. a second source added to the resource propagates"
# What the operator exists for: a database arrives and it is an edit, not a
# release. A source of its own, because that is what a second database is —
# its own slot, its own publication, its own file — and in restart mode the
# rendered files move the pod's checksum, so the new file reaches a process
# that reads the directory at startup.
write_cr "$(cat << 'YAML'
    lines:
      source:
        url_env: PG2OSYNC_SOURCE_URL
        slot_name: pg2osync_lines
        publication: pg2osync_lines_pub
      target:
        url: http://opensearch:9200
      sync:
        lines:
          table: public.order_lines
          index: e2e-lines
YAML
)"
check "the ConfigMap grew a file" \
  "$(await_match cm/tenant-a '{.data}' 'lines\.toml')" "yes"
check "the status counts both sources" \
  "$(await_field pg2osync/tenant-a '{.status.sources}' 2)" "2"
k rollout status deploy/tenant-a --timeout=240s > /dev/null || true
check "the added source loaded its table" "$(await_count e2e-lines 3)" "3"
check "and the first one kept its documents" "$(await_count e2e-orders 6)" "6"

say "7. a spec that hides a credential is refused, not deployed"
# The throwaway cluster's own URL again: the assertion is that this never
# reaches a pod, so the value has to be the shape a real one would be.
cat > "$WORK/bad.yaml" << YAML
apiVersion: pg2osync.io/v1alpha1
kind: Pg2osync
metadata:
  name: tenant-bad
  namespace: $NS
spec:
  config:
    source:
      url: postgres://postgres:postgres@postgres:5432/sourcedb
    target:
      url: http://opensearch:9200
YAML
kubectl apply -f "$WORK/bad.yaml" > /dev/null
message=$(for _ in $(seq 1 30); do
  got=$(k get pg2osync/tenant-bad -o jsonpath='{.status.message}' 2> /dev/null || true)
  [ -n "$got" ] && { printf '%s' "$got"; break; }
  sleep 2
done)
case "$message" in
  *url_env*) ok "the reason names the way to do it right" ;;
  *) bad "the status says '$message'" ;;
esac
check "and nothing was deployed for it" \
  "$(k get deploy tenant-bad --ignore-not-found -o name)" ""
kubectl delete -f "$WORK/bad.yaml" > /dev/null

say "8. deleting the resource takes its objects with it"
kubectl delete -f "$WORK/cr.yaml" --wait=true > /dev/null
check "the Deployment is collected" "$(await_gone deploy tenant-a)" "gone"
check "the ConfigMap is collected" "$(await_gone cm tenant-a)" "gone"
check "the Service is collected" "$(await_gone svc tenant-a)" "gone"

DONE=1
printf "\n\033[1m%d passed, %d failed\033[0m\n" "$PASS" "$FAIL"
[ "$FAIL" -eq 0 ]
