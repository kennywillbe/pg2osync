#!/usr/bin/env bash
# Every check GitHub Actions runs on a pull request, run locally, one job at a
# time, with the same commands and the same pinned versions. CI is not allowed
# to be the first thing that finds a red: run this before every push.
#
# The versions are not written down here. They are read out of the workflow
# files and Cargo.toml at run time, so this script cannot drift from CI.
#
# Usage: ./dev/ci-local.sh [options]
#   --only <job>[,<job>]  run only these jobs
#   --skip <job>[,<job>]  run everything except these
#   --fast                skip the e2e suites, the image build and the matrix
#   --matrix              run the compatibility cells even if nothing asks for them
#   --no-matrix           never run them
#   --title "<text>"      the pull request title to check (default: the open
#                         pull request for this branch, via gh)
#   --list                print the job names and exit
#
# Jobs, named after the CI jobs they mirror:
#   lint                 fmt + clippy + unit tests             (ci.yml)
#   msrv                 minimum supported Rust version        (ci.yml)
#   e2e-postgres         e2e PostgreSQL to OpenSearch          (ci.yml)
#   e2e-mysql            e2e MySQL to OpenSearch               (ci.yml)
#   docker               container image builds                (ci.yml)
#   helm                 helm chart lints                      (ci.yml)
#   docs                 the book builds                       (docs.yml)
#   pr-title             the title is a conventional commit    (pr-title.yml)
#   audit                dependencies have no known advisories (audit.yml)
#   compat-*             the six compatibility cells           (compat.yml)
#
# Tools: docker, helm, kubectl, mdbook, rustup/cargo, curl, python3; gh only
# when --title is not given; cargo-audit is installed on demand.
# shellcheck disable=SC2329
# every job and probe below is reached by name from run_job, never literally
set -euo pipefail
# shellcheck source=dev/e2e-lock.sh
source "$(dirname "$0")/e2e-lock.sh"

cd "$(dirname "$0")/.."

CI_WF=.github/workflows/ci.yml
DOCS_WF=.github/workflows/docs.yml
TITLE_WF=.github/workflows/pr-title.yml
AUDIT_WF=.github/workflows/audit.yml
COMPAT_WF=.github/workflows/compat.yml

# One directory per run, stamped: a stale log read as this run's evidence is
# worse than no log at all, and two runs on one machine must not share a file.
RUN_DIR=${PG2OSYNC_CI_LOG_DIR:-/tmp/pg2osync-ci-local/$(date +%Y%m%d-%H%M%S)}

# ci.yml's env block. RUSTFLAGS is what makes a warning fail the build there,
# so leaving it out here would let a warning through to CI.
export CARGO_TERM_COLOR=always
export RUSTFLAGS=${RUSTFLAGS:--D warnings}

# The compatibility cells are throwaway containers, so they get ports of their
# own and never collide with the dev stack on 15432/9200 or a MySQL on 13306.
COMPAT_PG_PORT=15433
COMPAT_OS_PORT=9201
COMPAT_ES_PORT=9202
COMPAT_MEILI_PORT=7701
COMPAT_MYSQL_PORT=13307

ONLY=""
SKIP=""
FAST=0
MATRIX=auto
PR_TITLE=""
PR_TITLE_GIVEN=0

ALL_JOBS="lint msrv e2e-postgres e2e-mysql docker helm docs pr-title audit
compat-postgres15 compat-elasticsearch compat-meilisearch compat-mysql84
compat-mariadb106 compat-mariadb118"

bold() { printf '\033[1m%s\033[0m\n' "$*"; }
note() { printf '  %s\n' "$*"; }
warn() { printf '\033[33m!! %s\033[0m\n' "$*"; }

usage() { awk 'NR > 1 { if (/^# shellcheck/ || !/^#/) exit; sub(/^# ?/, ""); print }' "$0"; }

while [ $# -gt 0 ]; do
  case "$1" in
    --only) ONLY="$ONLY ${2//,/ }"; shift 2 ;;
    --skip) SKIP="$SKIP ${2//,/ }"; shift 2 ;;
    --fast) FAST=1; shift ;;
    --matrix) MATRIX=force; shift ;;
    --no-matrix) MATRIX=never; shift ;;
    --title) PR_TITLE=$2; PR_TITLE_GIVEN=1; shift 2 ;;
    --list) printf '%s\n' "$ALL_JOBS" | tr ' ' '\n'; exit 0 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown option: $1" >&2; usage >&2; exit 2 ;;
  esac
done

# --------------------------------------------------------------- the versions
# Read out of the workflows, never repeated here, so this cannot drift from CI.
first_match() { sed -n "$1" "$2" | head -1; }

PG_IMAGE=$(first_match 's|^ *image: \(postgres:[^ ]*\)$|\1|p' "$CI_WF")
OS_IMAGE=$(first_match 's|^ *image: \(opensearchproject/opensearch:[^ ]*\)$|\1|p' "$CI_WF")
MYSQL_IMAGE=$(first_match 's|^ *image: \(mysql:[^ ]*\)$|\1|p' "$CI_WF")
MSRV=$(first_match 's|^ *toolchain: "\([0-9.]*\)"$|\1|p' "$CI_WF")
CARGO_MSRV=$(first_match 's|^rust-version = "\([0-9.]*\)"$|\1|p' Cargo.toml)
MDBOOK_VERSION=$(first_match 's|^ *MDBOOK_VERSION: "\([0-9.]*\)"$|\1|p' "$DOCS_WF")
TITLE_PATTERN=$(first_match "s|^ *pattern='\(.*\)'\$|\\1|p" "$TITLE_WF")
AUDIT_CMD=$(first_match 's|^ *- run: \(cargo audit.*\)$|\1|p' "$AUDIT_WF")

# A cell compat.yml marks continue-on-error is a known gap being reported, not a
# red pull request, so a failure there must not read as one here either.
ADVISORY_JOBS=$(awk '/^  [a-z0-9-]+:/ { job = substr($1, 1, length($1) - 1) }
  /continue-on-error: true/ { print job }' "$COMPAT_WF" | tr '\n' ' ')

COMPAT_PG15_IMAGE=$(first_match 's|^ *pg_image: \(postgres:[^ ]*\)$|\1|p' "$COMPAT_WF")
COMPAT_PG_IMAGE=$(first_match 's|.*POSTGRES_DB=sourcedb \(postgres:[^ ]*\).*|\1|p' "$COMPAT_WF")
COMPAT_OS_IMAGE=$(first_match 's|^ *image: \(opensearchproject/opensearch:[^ ]*\)$|\1|p' "$COMPAT_WF")
COMPAT_ES_IMAGE=$(first_match 's|^ *image: \(docker.elastic.co/elasticsearch/elasticsearch:[^ ]*\)$|\1|p' "$COMPAT_WF")
COMPAT_MEILI_IMAGE=$(first_match 's|^ *image: \(getmeili/meilisearch:[^ ]*\)$|\1|p' "$COMPAT_WF")
COMPAT_MYSQL_IMAGE=$(first_match 's|^ *image: \(mysql:[^ ]*\)$|\1|p' "$COMPAT_WF")
COMPAT_MARIADB_1=$(sed -n 's|^ *image: \(mariadb:[^ ]*\)$|\1|p' "$COMPAT_WF" | sed -n 1p)
COMPAT_MARIADB_2=$(sed -n 's|^ *image: \(mariadb:[^ ]*\)$|\1|p' "$COMPAT_WF" | sed -n 2p)

for name in PG_IMAGE OS_IMAGE MYSQL_IMAGE MSRV CARGO_MSRV MDBOOK_VERSION \
  TITLE_PATTERN AUDIT_CMD COMPAT_PG15_IMAGE COMPAT_PG_IMAGE COMPAT_OS_IMAGE \
  COMPAT_ES_IMAGE COMPAT_MEILI_IMAGE COMPAT_MYSQL_IMAGE COMPAT_MARIADB_1 \
  COMPAT_MARIADB_2; do
  eval "value=\${$name}"
  [ -n "$value" ] || { echo "could not read $name out of the workflow files" >&2; exit 2; }
done

case "$MSRV" in
  "$CARGO_MSRV"*) ;;
  *) warn "ci.yml pins Rust $MSRV but Cargo.toml says rust-version = $CARGO_MSRV" ;;
esac

# ----------------------------------------------------------- what CI would run
CHANGED=$(git diff --name-only origin/main...HEAD 2> /dev/null || true)
CHANGED="$CHANGED
$(git status --porcelain | sed 's|^...||')"

changed_matches() { printf '%s\n' "$CHANGED" | grep -Eq "$1"; }

case "$MATRIX" in
  force) MATRIX_ON=1; MATRIX_REASON="--matrix" ;;
  never) MATRIX_ON=0; MATRIX_REASON="--no-matrix" ;;
  *)
    if changed_matches '^(\.github/workflows/compat\.yml|dev/e2e-.*\.sh)$'; then
      MATRIX_ON=1; MATRIX_REASON="compat.yml or dev/e2e-*.sh changed, so a pull request runs it"
    else
      MATRIX_ON=0; MATRIX_REASON="compat.yml and dev/e2e-*.sh are unchanged, as on CI"
    fi ;;
esac

if changed_matches '(^|/)Cargo\.(toml|lock)$'; then
  AUDIT_ON=1; AUDIT_REASON="a Cargo file moved"
else
  AUDIT_ON=0; AUDIT_REASON="no Cargo.toml or Cargo.lock change against origin/main"
fi

# -------------------------------------------------------------- the job runner
PASSED=0
FAILED=0
SKIPPED=0
ADVISORY=0
CLEANUP_CONTAINERS=""

cleanup_containers() {
  [ -n "$CLEANUP_CONTAINERS" ] || return 0
  # shellcheck disable=SC2086
  docker rm -f $CLEANUP_CONTAINERS > /dev/null 2>&1 || true
  CLEANUP_CONTAINERS=""
}
trap cleanup_containers EXIT

selected() {
  local slug=$1
  if [ -n "$ONLY" ]; then
    case " $ONLY " in *" $slug "*) return 0 ;; *) return 1 ;; esac
  fi
  case " $SKIP " in *" $slug "*) return 1 ;; esac
  return 0
}

# A job function returns 0 for green, 78 when CI itself would not have run it,
# anything else for red. errexit does not apply inside a function called from a
# condition, so every step in one ends in `|| return 1`.
run_job() {
  local slug=$1 name=$2 cell=${3:-} fn=job_${1//-/_} log start rc=0 took
  if ! selected "$slug"; then
    SKIPPED=$((SKIPPED + 1))
    printf -- '- %s (not selected)\n' "$name"
    return 0
  fi
  log=$RUN_DIR/$slug.log
  start=$SECONDS
  printf '\033[2m.. %s\033[0m\n' "$name"
  # Seeding the databases and running a suite must not interleave with another
  # run's suite, so the whole job holds the lock the suites queue on.
  case $slug in
    e2e-*|compat-*) e2e_lock ;;
  esac
  "$fn" > "$log" 2>&1 || rc=$?
  case $slug in
    e2e-*|compat-*) e2e_unlock ;;
  esac
  cleanup_containers
  took=$((SECONDS - start))
  printf '\033[1A\033[2K'
  case $rc in
    0) PASSED=$((PASSED + 1)); printf '\033[32m✓\033[0m %s (%ss)\n' "$name" "$took" ;;
    # a skipping job explains itself on its last line
    78) SKIPPED=$((SKIPPED + 1)); printf -- '- %s (%s)\n' "$name" "$(tail -1 "$log")" ;;
    *)
      case "${cell:+ $ADVISORY_JOBS }" in
        *" $cell "*)
          ADVISORY=$((ADVISORY + 1))
          printf '\033[33m!\033[0m %s (%ss, advisory on CI too, see %s)\n' "$name" "$took" "$log" ;;
        *)
          FAILED=$((FAILED + 1))
          printf '\033[31m✗\033[0m %s (%ss, see %s)\n' "$name" "$took" "$log" ;;
      esac ;;
  esac
}

# ------------------------------------------------------------------ containers
wait_for() {
  local what=$1 tries=$2
  shift 2
  for _ in $(seq 1 "$tries"); do
    if "$@" > /dev/null 2>&1; then return 0; fi
    sleep 2
  done
  echo "$what never became ready"
  return 1
}

pg_ready() { docker exec "$1" pg_isready -U postgres -d sourcedb; }
os_green() { curl -s "$1/_cluster/health" | grep -q green; }
# The dev stack's OpenSearch is a single node that outlives one run, so an index
# left behind with a replica keeps the cluster yellow for good. This is the
# healthcheck dev/docker-compose.yml uses; a throwaway cell still waits on green.
os_ready() { curl -s "$1/_cluster/health" | grep -q '"status":"\(green\|yellow\)"'; }
es_yellow() { curl -sf "$1/_cluster/health?wait_for_status=yellow"; }
meili_up() { curl -s "$1/health" | grep -q available; }
mysql_ready() { docker exec "$1" mysqladmin ping -h 127.0.0.1 -pmysqlpw; }
maria_ready() { docker exec "$1" mariadb-admin ping -h 127.0.0.1 -uroot -pmysqlpw; }

container_state() { docker inspect -f '{{.State.Status}}' "$1" 2> /dev/null || true; }
container_image() { docker inspect -f '{{.Config.Image}}' "$1" 2> /dev/null || true; }

# ci.yml creates this user once against a fresh service container; a container
# that outlives one run already has it, hence IF NOT EXISTS.
mysql_user() {
  local name=$1 client=$2 auth=$3
  docker exec "$name" "$client" -uroot -pmysqlpw -e "
    CREATE USER IF NOT EXISTS 'repl'@'%' IDENTIFIED $auth BY 'replpw';
    GRANT SELECT, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'repl'@'%';" || return 1
}

# MySQL's own online procedure for turning GTIDs on, from ci.yml: the images
# start with gtid_mode = OFF, which would have the suite skip that section.
mysql_enable_gtid() {
  local name=$1 mode count
  mode=$(docker exec "$name" mysql -uroot -pmysqlpw -N -B -e "SELECT @@GLOBAL.gtid_mode;" 2> /dev/null || true)
  if [ "$mode" = "ON" ]; then echo "gtid_mode is already ON"; return 0; fi
  docker exec "$name" mysql -uroot -pmysqlpw -e "
    SET @@GLOBAL.ENFORCE_GTID_CONSISTENCY = WARN;
    SET @@GLOBAL.ENFORCE_GTID_CONSISTENCY = ON;
    SET @@GLOBAL.GTID_MODE = OFF_PERMISSIVE;
    SET @@GLOBAL.GTID_MODE = ON_PERMISSIVE;" || return 1
  # every anonymous transaction has to finish before the last step
  for _ in $(seq 1 30); do
    count=$(docker exec "$name" mysql -uroot -pmysqlpw -N -B -e \
      "SELECT COUNT(*) FROM performance_schema.global_status \
       WHERE VARIABLE_NAME = 'ONGOING_ANONYMOUS_TRANSACTION_COUNT' \
         AND VARIABLE_VALUE <> '0';")
    [ "$count" = "0" ] && break
    sleep 1
  done
  docker exec "$name" mysql -uroot -pmysqlpw -e "SET @@GLOBAL.GTID_MODE = ON;" || return 1
  docker exec "$name" mysql -uroot -pmysqlpw -N -B -e "SELECT @@GLOBAL.gtid_mode;" || return 1
}

# The suites stop a pipeline by killing every pg2osync process, so a second one
# running anywhere on this machine makes them report failures that are not real.
# The suites queue on dev/e2e-lock.sh themselves; waiting here as well keeps a
# run from starting its e2e jobs into someone else's pipeline and reporting a
# red that was never about the change.
no_pipeline_running() {
  local waited=0 wait_max=${E2E_LOCK_WAIT:-5400}
  # The lock this run took itself is not someone else's run.
  while pgrep -f "pg2osync run" > /dev/null 2>&1 \
    || { [ -d "$E2E_LOCK" ] && [ "${E2E_LOCK_OWNER:-}" != "$$" ]; }; do
    if [ "$waited" -eq 0 ]; then
      echo "another pg2osync pipeline or e2e suite is running on this machine; waiting for it"
    fi
    if [ "$waited" -ge "$wait_max" ]; then
      echo "gave up after ${waited}s: a 'pg2osync run' process is still alive. Stop it and run again:"
      echo "  pkill -f 'pg2osync run'"
      return 1
    fi
    sleep 10
    waited=$((waited + 10))
  done
}

# CI gets PostgreSQL and OpenSearch as service containers; locally they are the
# dev stack, brought up here when it is not already running.
dev_stack_up() {
  local pg_state os_state
  pg_state=$(container_state dev-postgres-1)
  os_state=$(container_state dev-opensearch-1)
  if [ "$pg_state" != "running" ] || [ "$os_state" != "running" ]; then
    echo "bringing up dev/docker-compose.yml ($PG_IMAGE + $OS_IMAGE)"
    docker compose -f dev/docker-compose.yml up -d || return 1
  fi
  wait_for "PostgreSQL" 60 pg_ready dev-postgres-1 || return 1
  wait_for "OpenSearch" 60 os_ready http://localhost:9200 || return 1
  docker exec -i dev-postgres-1 psql -U postgres -d sourcedb < dev/seed.sql || return 1
}

mysql_stack_up() {
  local state image
  state=$(container_state mysql-test)
  image=$(container_image mysql-test)
  if [ -n "$state" ] && [ "$image" != "$MYSQL_IMAGE" ]; then
    echo "the container 'mysql-test' runs $image, but ci.yml runs $MYSQL_IMAGE."
    echo "Remove it and run again:  docker rm -f mysql-test"
    return 1
  fi
  case "$state" in
    running) echo "reusing the running mysql-test ($image)" ;;
    "") echo "starting mysql-test ($MYSQL_IMAGE)"
        docker run -d --name mysql-test -p 13306:3306 \
          -e MYSQL_ROOT_PASSWORD=mysqlpw -e MYSQL_DATABASE=sourcedb \
          "$MYSQL_IMAGE" > /dev/null || return 1 ;;
    *) echo "starting the stopped mysql-test ($image)"
       docker start mysql-test > /dev/null || return 1 ;;
  esac
  wait_for "MySQL" 90 mysql_ready mysql-test || return 1
  mysql_user mysql-test mysql "WITH mysql_native_password" || return 1
  mysql_enable_gtid mysql-test || return 1
  docker exec -i mysql-test mysql -uroot -pmysqlpw sourcedb < dev/mysql-seed.sql || return 1
}

release_build() {
  echo "cargo build --release --locked -p pg2osync"
  cargo build --release --locked -p pg2osync || return 1
}

# ---------------------------------------------------------------------- ci.yml
job_lint() {
  cargo fmt --all -- --check || return 1
  cargo clippy --workspace --all-targets -- -D warnings || return 1
  cargo test --workspace || return 1
}

job_msrv() {
  if ! rustup toolchain list | grep -q "^$MSRV"; then
    echo "installing the $MSRV toolchain"
    rustup toolchain install "$MSRV" --profile minimal || return 1
  fi
  cargo "+$MSRV" check --workspace --locked || return 1
}

job_e2e_postgres() {
  no_pipeline_running || return 1
  dev_stack_up || return 1
  release_build || return 1
  E2E_LOG=$RUN_DIR/e2e-postgres-pipeline.log ./dev/e2e-test.sh || return 1
}

job_e2e_mysql() {
  no_pipeline_running || return 1
  dev_stack_up || return 1
  mysql_stack_up || return 1
  release_build || return 1
  E2E_LOG=$RUN_DIR/e2e-mysql-pipeline.log ./dev/e2e-mysql-test.sh || return 1
}

job_docker() {
  # docker/build-push-action with push: false and no build args; its caches are
  # GitHub's and have no local equivalent.
  docker build --file Dockerfile --tag pg2osync:ci-local . || return 1
}

job_helm() {
  helm lint deploy/helm/pg2osync || return 1
  helm template pg2osync deploy/helm/pg2osync --set config.sync.users.table=public.users || return 1
  kubectl kustomize deploy/kubernetes || return 1
}

# -------------------------------------------------------------------- docs.yml
job_docs() {
  local missing=0 page installed
  # mdBook only warns about a file it never links to, which would silently
  # publish a page nobody can reach from the sidebar
  while IFS= read -r page; do
    case "$page" in docs/SUMMARY.md | docs/index.md) continue ;; esac
    grep -q "(${page#docs/})" docs/SUMMARY.md || { echo "not in SUMMARY.md: $page"; missing=1; }
  done < <(find docs -name '*.md' | sort)
  [ "$missing" = 0 ] || return 1
  if ! command -v mdbook > /dev/null; then
    echo "mdbook is missing: cargo install mdbook --version $MDBOOK_VERSION --locked"
    return 1
  fi
  installed=$(mdbook --version | sed 's/^mdbook v//')
  [ "$installed" = "$MDBOOK_VERSION" ] ||
    echo "warning: mdbook $installed here, docs.yml pins $MDBOOK_VERSION"
  mdbook build || return 1
}

# ---------------------------------------------------------------- pr-title.yml
job_pr_title() {
  local title=$PR_TITLE
  if [ "$PR_TITLE_GIVEN" = 0 ]; then
    if command -v gh > /dev/null; then
      title=$(gh pr view --json title --jq .title 2> /dev/null || true)
    fi
    if [ -z "$title" ]; then
      echo "no open pull request for this branch; pass --title \"...\" to check one"
      return 78
    fi
  fi
  if printf '%s' "$title" | grep -Eq "$TITLE_PATTERN"; then
    echo "ok: $title"
    return 0
  fi
  echo "The pull request title has to be a Conventional Commit, because it becomes"
  echo "the commit subject on main and the changelog line."
  echo "Got: $title"
  echo "Examples: fix: drop a renamed table's rows instead of panicking"
  echo "          feat(mysql): resume from a GTID set"
  echo "          feat!: rename the checkpoint index"
  return 1
}

# ------------------------------------------------------------------- audit.yml
job_audit() {
  if [ "$AUDIT_ON" = 0 ]; then echo "$AUDIT_REASON"; return 78; fi
  if ! command -v cargo-audit > /dev/null; then
    echo "installing cargo-audit"
    cargo install cargo-audit --locked || return 1
  fi
  # the ignore list and its reasoning live in .cargo/audit.toml
  $AUDIT_CMD || return 1
}

# ------------------------------------------------------------------ compat.yml
compat_opensearch() {
  docker run -d --name compat-opensearch -p "$COMPAT_OS_PORT:9200" \
    -e discovery.type=single-node -e DISABLE_SECURITY_PLUGIN=true \
    -e OPENSEARCH_JAVA_OPTS="-Xms512m -Xmx512m" \
    "$COMPAT_OS_IMAGE" > /dev/null || return 1
  wait_for "OpenSearch" 90 os_green "http://localhost:$COMPAT_OS_PORT" || return 1
}

# The suite needs logical replication from the first connection and a service
# container takes no server flags, so compat.yml starts this one by hand too.
compat_postgres() {
  docker run -d --name compat-postgres -p "$COMPAT_PG_PORT:5432" \
    -e POSTGRES_USER=postgres -e POSTGRES_PASSWORD=postgres \
    -e POSTGRES_DB=sourcedb "$1" \
    -c wal_level=logical -c max_wal_senders=10 -c max_replication_slots=10 > /dev/null || return 1
  wait_for "PostgreSQL" 60 pg_ready compat-postgres || return 1
  docker exec -i compat-postgres psql -U postgres -d sourcedb < dev/seed.sql || return 1
}

# A cell owns throwaway containers; naming them here has run_job remove them
# however the cell ends.
compat_start() {
  no_pipeline_running || return 1
  CLEANUP_CONTAINERS="$*"
  cleanup_containers
  CLEANUP_CONTAINERS="$*"
  release_build || return 1
}

job_compat_postgres15() {
  compat_start compat-postgres compat-opensearch || return 1
  compat_opensearch || return 1
  compat_postgres "$COMPAT_PG15_IMAGE" || return 1
  PG_CONTAINER=compat-postgres PG_PORT=$COMPAT_PG_PORT \
    OS_URL="http://localhost:$COMPAT_OS_PORT" E2E_LOG=$RUN_DIR/compat-postgres15-pipeline.log \
    ./dev/e2e-test.sh || return 1
}

job_compat_elasticsearch() {
  compat_start compat-postgres compat-elasticsearch || return 1
  docker run -d --name compat-elasticsearch -p "$COMPAT_ES_PORT:9200" \
    -e discovery.type=single-node -e xpack.security.enabled=false \
    -e xpack.security.enrollment.enabled=false -e ES_JAVA_OPTS="-Xms512m -Xmx512m" \
    "$COMPAT_ES_IMAGE" > /dev/null || return 1
  # not "green": a single-node Elasticsearch leaves every replica unassigned
  wait_for "Elasticsearch" 90 es_yellow "http://localhost:$COMPAT_ES_PORT" || return 1
  compat_postgres "$COMPAT_PG_IMAGE" || return 1
  PG_CONTAINER=compat-postgres PG_PORT=$COMPAT_PG_PORT \
    OS_URL="http://localhost:$COMPAT_ES_PORT" TARGET_FLAVOR=elasticsearch \
    E2E_LOG=$RUN_DIR/compat-elasticsearch-pipeline.log ./dev/e2e-test.sh || return 1
}

job_compat_meilisearch() {
  compat_start compat-postgres compat-meilisearch || return 1
  docker run -d --name compat-meilisearch -p "$COMPAT_MEILI_PORT:7700" \
    -e MEILI_MASTER_KEY=e2e-master-key -e MEILI_ENV=development \
    "$COMPAT_MEILI_IMAGE" > /dev/null || return 1
  wait_for "Meilisearch" 60 meili_up "http://localhost:$COMPAT_MEILI_PORT" || return 1
  compat_postgres "$COMPAT_PG_IMAGE" || return 1
  # Meilisearch has no mappings, no joins and no per-row indices, so the full
  # suite cannot run against it; this is what it does support.
  PG_CONTAINER=compat-postgres PG_PORT=$COMPAT_PG_PORT \
    MEILI_URL="http://localhost:$COMPAT_MEILI_PORT" MEILI_MASTER_KEY=e2e-master-key \
    E2E_LOG=$RUN_DIR/compat-meilisearch-pipeline.log ./dev/e2e-meili-smoke.sh || return 1
}

job_compat_mysql84() {
  compat_start compat-mysql compat-opensearch || return 1
  compat_opensearch || return 1
  docker run -d --name compat-mysql -p "$COMPAT_MYSQL_PORT:3306" \
    -e MYSQL_ROOT_PASSWORD=mysqlpw -e MYSQL_DATABASE=sourcedb \
    "$COMPAT_MYSQL_IMAGE" > /dev/null || return 1
  wait_for "MySQL" 90 mysql_ready compat-mysql || return 1
  # 8.4 dropped mysql_native_password from the default plugins, so this is also
  # the cell that proves the caching_sha2_password handshake.
  mysql_user compat-mysql mysql "WITH caching_sha2_password" || return 1
  mysql_enable_gtid compat-mysql || return 1
  docker exec -i compat-mysql mysql -uroot -pmysqlpw sourcedb < dev/mysql-seed.sql || return 1
  MYSQL_CONTAINER=compat-mysql MYSQL_PORT=$COMPAT_MYSQL_PORT \
    OS_URL="http://localhost:$COMPAT_OS_PORT" E2E_LOG=$RUN_DIR/compat-mysql84-pipeline.log \
    ./dev/e2e-mysql-test.sh || return 1
}

compat_mariadb() {
  compat_start compat-mysql compat-opensearch || return 1
  compat_opensearch || return 1
  # MariaDB writes no binlog unless it is told to
  docker run -d --name compat-mysql -p "$COMPAT_MYSQL_PORT:3306" \
    -e MARIADB_ROOT_PASSWORD=mysqlpw -e MARIADB_DATABASE=sourcedb "$1" \
    --log-bin --binlog-format=ROW --binlog-row-image=FULL --server-id=1 > /dev/null || return 1
  wait_for "MariaDB" 90 maria_ready compat-mysql || return 1
  mysql_user compat-mysql mariadb "" || return 1
  docker exec -i compat-mysql mariadb -uroot -pmysqlpw sourcedb < dev/mysql-seed.sql || return 1
  # MariaDB has no GTID position the suite can assert on; the script knows
  MYSQL_CONTAINER=compat-mysql MYSQL_PORT=$COMPAT_MYSQL_PORT MYSQL_CLIENT=mariadb \
    OS_URL="http://localhost:$COMPAT_OS_PORT" E2E_LOG=$RUN_DIR/compat-${1/:/-}-pipeline.log \
    ./dev/e2e-mysql-test.sh || return 1
}

job_compat_mariadb106() { compat_mariadb "$COMPAT_MARIADB_1"; }
job_compat_mariadb118() { compat_mariadb "$COMPAT_MARIADB_2"; }

# ------------------------------------------------------------------------ main
mkdir -p "$RUN_DIR"

bold "pg2osync — what a pull request runs on CI, run here"
note "logs        $RUN_DIR"
note "postgres    $PG_IMAGE ($CI_WF)"
note "opensearch  $OS_IMAGE ($CI_WF)"
note "mysql       $MYSQL_IMAGE ($CI_WF)"
note "msrv        $MSRV ($CI_WF; Cargo.toml says $CARGO_MSRV)"
note "mdbook      $MDBOOK_VERSION ($DOCS_WF)"
note "audit       $AUDIT_CMD ($AUDIT_WF) — $AUDIT_REASON"
note "matrix      $([ "$MATRIX_ON" = 1 ] && echo on || echo off) — $MATRIX_REASON"
if [ "$MATRIX_ON" = 1 ]; then
  note "  images    $COMPAT_PG15_IMAGE, $COMPAT_PG_IMAGE, $COMPAT_ES_IMAGE, $COMPAT_MEILI_IMAGE,"
  note "            $COMPAT_MYSQL_IMAGE, $COMPAT_MARIADB_1, $COMPAT_MARIADB_2"
  note "  ports     postgres $COMPAT_PG_PORT, opensearch $COMPAT_OS_PORT, elasticsearch $COMPAT_ES_PORT,"
  note "            meilisearch $COMPAT_MEILI_PORT, mysql/mariadb $COMPAT_MYSQL_PORT"
fi
if [ "$FAST" = 1 ]; then
  warn "--fast: the e2e suites, the image build and the matrix are skipped."
  warn "It is a quick loop, not the definition of done. Run the whole script"
  warn "before you push, or CI finds what this one did not look at."
  SKIP="$SKIP e2e-postgres e2e-mysql docker"
fi
if [ "$MATRIX" = never ]; then
  warn "--no-matrix: the compatibility cells are skipped. If they would have run,"
  warn "a version the documentation promises can break without anyone noticing."
fi
echo

run_job lint "fmt + clippy + unit tests"
run_job msrv "minimum supported Rust version"
run_job e2e-postgres "e2e PostgreSQL to OpenSearch"
run_job e2e-mysql "e2e MySQL to OpenSearch"
run_job docker "container image builds"
run_job helm "helm chart lints"
run_job docs "the book builds"
run_job pr-title "the title is a conventional commit"
run_job audit "dependencies have no known advisories"

if [ "$MATRIX_ON" = 1 ] && [ "$FAST" = 0 ]; then
  run_job compat-postgres15 "PostgreSQL 15 to OpenSearch" compat-postgres
  run_job compat-elasticsearch "PostgreSQL 17 to Elasticsearch" compat-elasticsearch
  run_job compat-meilisearch "PostgreSQL 17 to Meilisearch" compat-meilisearch
  run_job compat-mysql84 "MySQL 8.4 to OpenSearch" compat-mysql
  run_job compat-mariadb106 "MariaDB 10.6 to OpenSearch" compat-mariadb
  run_job compat-mariadb118 "MariaDB 11.8 to OpenSearch" compat-mariadb
fi

echo
ADVISORY_NOTE=""
[ "$ADVISORY" = 0 ] || ADVISORY_NOTE=", $ADVISORY advisory"
if [ "$FAILED" = 0 ]; then
  printf 'RESULT: green — %d passed, %d skipped%s\n' "$PASSED" "$SKIPPED" "$ADVISORY_NOTE"
  [ "$FAST" = 0 ] || warn "--fast, so this green is not the definition of done."
  exit 0
fi
printf 'RESULT: red — %d passed, %d failed, %d skipped%s. Logs in %s\n' \
  "$PASSED" "$FAILED" "$SKIPPED" "$ADVISORY_NOTE" "$RUN_DIR"
exit 1
