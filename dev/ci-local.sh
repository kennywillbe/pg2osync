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
#   --isolated            give this run containers of its own, on ports Docker
#                         picks, instead of the shared dev stack — that is what
#                         lets two runs go at once on one machine
#   --jobs <n>            compatibility cells to run at once (default 2 under
#                         --isolated, 1 on the shared stack)
#   --title "<text>"      the pull request title to check (default: the open
#                         pull request for this branch, via gh)
#   --list                print the job names and exit
#
# Jobs, named after the CI jobs they mirror:
#   lint                 fmt + clippy + unit tests             (ci.yml)
#   msrv                 minimum supported Rust version        (ci.yml)
#   e2e-postgres         e2e PostgreSQL to OpenSearch          (ci.yml)
#   e2e-mysql            e2e MySQL to OpenSearch               (ci.yml)
#   e2e-multi-source     e2e several sources in one process    (ci.yml)
#   docker               container image builds                (ci.yml)
#   helm                 helm chart lints                      (ci.yml)
#   docs                 the book builds                       (docs.yml)
#   pr-title             the title is a conventional commit    (pr-title.yml)
#   audit                dependencies have no known advisories (audit.yml)
#   compat-*             the eight compatibility cells         (compat.yml)
#
# By default the e2e jobs use the shared dev stack (dev/docker-compose.yml plus
# the mysql-test container): nothing to pull, nothing to start, and the suites
# queue on dev/e2e-lock.sh so two runs do not overwrite each other's tables.
# --isolated instead gives every e2e and compatibility job of this run its own
# throwaway containers, named pg2osync-ci-<run id>-*, on ports Docker assigns,
# and its own block of localhost ports for the pipelines the suites start.
# Such a run shares nothing, takes no lock and leaves the dev stack alone, so it
# can go beside another one, and its compatibility cells — a container set and a
# port block each — run two at a time instead of one after the other (--jobs).
# A cell measures about 1 GB — 0.9 for OpenSearch on its 512 MB heap, the rest
# PostgreSQL, half a gigabyte more for a MySQL one — against a dev stack of
# 2.6 GB, so an 8 GB Docker VM carries about two isolated runs beside it. Past
# that, Docker's OOM killer takes a container down, and the job waiting on it
# fails within seconds naming the container rather than hanging: readiness is
# polled, and a container that is no longer running ends the wait.
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

# Identifies this run in its log directory and, under --isolated, in the name
# of every container it starts. The pid is what keeps two runs started in the
# same second apart.
RUN_ID=$(date +%Y%m%d-%H%M%S)-$$

# One directory per run, stamped: a stale log read as this run's evidence is
# worse than no log at all, and two runs on one machine must not share a file.
RUN_DIR=${PG2OSYNC_CI_LOG_DIR:-/tmp/pg2osync-ci-local/$RUN_ID}

# ci.yml's env block. RUSTFLAGS is what makes a warning fail the build there,
# so leaving it out here would let a warning through to CI.
export CARGO_TERM_COLOR=always
export RUSTFLAGS=${RUSTFLAGS:--D warnings}

# The compatibility cells are throwaway containers, so they get ports of their
# own and never collide with the dev stack on 15432/9200 or a MySQL on 13306.
# Under --isolated a second run would collide with the first on exactly these,
# so there the ports are Docker's to pick and read back.
COMPAT_PG_PORT=15433
COMPAT_OS_PORT=9201
COMPAT_ES_PORT=9202
COMPAT_MEILI_PORT=7701
COMPAT_MYSQL_PORT=13307

ONLY=""
SKIP=""
FAST=0
ISOLATED=0
JOBS=
MATRIX=auto
PR_TITLE=""
PR_TITLE_GIVEN=0

ALL_JOBS="lint msrv e2e-postgres e2e-mysql e2e-multi-source docker helm docs
pr-title audit
compat-postgres15 compat-timescaledb compat-supabase compat-elasticsearch
compat-meilisearch compat-mysql84 compat-mariadb106 compat-mariadb118"

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
    --isolated) ISOLATED=1; shift ;;
    --jobs) JOBS=$2; shift 2 ;;
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
COMPAT_TIMESCALE_IMAGE=$(first_match 's|^ *pg_image: \(timescale/timescaledb:[^ ]*\)$|\1|p' "$COMPAT_WF")
COMPAT_SUPABASE_IMAGE=$(first_match 's|^ *pg_image: \(supabase/postgres:[^ ]*\)$|\1|p' "$COMPAT_WF")
COMPAT_OS_IMAGE=$(first_match 's|^ *image: \(opensearchproject/opensearch:[^ ]*\)$|\1|p' "$COMPAT_WF")
COMPAT_ES_IMAGE=$(first_match 's|^ *image: \(docker.elastic.co/elasticsearch/elasticsearch:[^ ]*\)$|\1|p' "$COMPAT_WF")
COMPAT_MEILI_IMAGE=$(first_match 's|^ *image: \(getmeili/meilisearch:[^ ]*\)$|\1|p' "$COMPAT_WF")
COMPAT_MYSQL_IMAGE=$(first_match 's|^ *image: \(mysql:[^ ]*\)$|\1|p' "$COMPAT_WF")
COMPAT_MARIADB_1=$(sed -n 's|^ *image: \(mariadb:[^ ]*\)$|\1|p' "$COMPAT_WF" | sed -n 1p)
COMPAT_MARIADB_2=$(sed -n 's|^ *image: \(mariadb:[^ ]*\)$|\1|p' "$COMPAT_WF" | sed -n 2p)

for name in PG_IMAGE OS_IMAGE MYSQL_IMAGE MSRV CARGO_MSRV MDBOOK_VERSION \
  TITLE_PATTERN AUDIT_CMD COMPAT_PG15_IMAGE COMPAT_PG_IMAGE \
  COMPAT_TIMESCALE_IMAGE COMPAT_SUPABASE_IMAGE COMPAT_OS_IMAGE \
  COMPAT_ES_IMAGE COMPAT_MEILI_IMAGE COMPAT_MYSQL_IMAGE COMPAT_MARIADB_1 \
  COMPAT_MARIADB_2; do
  eval "value=\${$name}"
  [ -n "$value" ] || { echo "could not read $name out of the workflow files" >&2; exit 2; }
done

case "$MSRV" in
  "$CARGO_MSRV"*) ;;
  *) warn "ci.yml pins Rust $MSRV but Cargo.toml says rust-version = $CARGO_MSRV" ;;
esac

# A cell is one set of throwaway containers with names and ports of its own.
# The compatibility cells have always been one; --isolated makes the e2e jobs
# cells too, and moves the names out of the way of the run next door.
if [ "$ISOLATED" = 1 ]; then
  CELL_BASE=pg2osync-ci-$RUN_ID
  # Nothing here is shared, so there is nothing to queue on — and queueing
  # would defeat the point of running beside another run.
  export E2E_LOCK=none
  JOBS=${JOBS:-2}
else
  CELL_BASE=compat
  # The cells here carry the fixed names and the fixed ports, and their suites
  # queue on the machine-wide lock, so two of them at once would collide on all
  # three. Running them side by side is what --isolated is for.
  if [ -n "$JOBS" ] && [ "$JOBS" != 1 ]; then
    echo "--jobs $JOBS needs --isolated: on the shared stack the cells share their" >&2
    echo "names, their ports and one lock, so they can only run one at a time." >&2
    exit 2
  fi
  JOBS=1
fi
case $JOBS in
  '' | *[!0-9]* | 0) echo "--jobs takes a number of 1 or more" >&2; exit 2 ;;
esac

# One namespace per cell, so two cells running at once never share a container
# name; on the shared stack there is one namespace and it is the old one.
cell_use() {
  if [ "$ISOLATED" = 1 ]; then
    CELL=$CELL_BASE-${1#compat-}
    # 0 means "Docker, pick one": probing for a free port and then binding it
    # leaves a window in which the cell beside this one binds the same port.
    CELL_PG_PORT=0
    CELL_OS_PORT=0
    CELL_ES_PORT=0
    CELL_MEILI_PORT=0
    CELL_MYSQL_PORT=0
  else
    CELL=$CELL_BASE
    CELL_PG_PORT=$COMPAT_PG_PORT
    CELL_OS_PORT=$COMPAT_OS_PORT
    CELL_ES_PORT=$COMPAT_ES_PORT
    CELL_MEILI_PORT=$COMPAT_MEILI_PORT
    CELL_MYSQL_PORT=$COMPAT_MYSQL_PORT
  fi
  CELL_PG=$CELL-postgres
  CELL_OS=$CELL-opensearch
  CELL_ES=$CELL-elasticsearch
  CELL_MEILI=$CELL-meilisearch
  CELL_MYSQL=$CELL-mysql
}
cell_use default

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

# A pooled cell registers its containers inside a subshell, where the parent
# cannot see the list, so its namespace is what gets removed instead. Only an
# isolated run has a prefix of its own; nothing else may be matched by name.
cleanup_cell() {
  local ids
  [ "$ISOLATED" = 1 ] || return 0
  ids=$(docker ps -aq --filter "name=^$1-" 2> /dev/null || true)
  [ -n "$ids" ] || return 0
  # shellcheck disable=SC2086
  docker rm -f $ids > /dev/null 2>&1 || true
}

cleanup_run() {
  cleanup_containers
  cleanup_cell "$CELL_BASE"
}
trap cleanup_run EXIT

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
report_job() {
  local name=$1 cell=$2 rc=$3 took=$4 log=$5
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
  cell_use "$slug"
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
  printf '\033[1A\033[2K'
  report_job "$name" "$cell" "$rc" "$((SECONDS - start))" "$log"
}

# ------------------------------------------------------------------- the pool
# Cells that share nothing can run at once. Each background cell gets a
# namespace and a port block of its own, indexed by the pool slot it holds, and
# is collected — log, exit code, containers — as soon as it ends.
POOL_PID=()
POOL_SLUG=()
POOL_NAME=()
POOL_CELL=()
POOL_START=()
FREED_SLOT=""

# A finished child is a zombie until it is waited for, and kill -0 still finds
# one, so the state is what says whether a slot is still busy.
slot_running() {
  case "$(ps -o state= -p "${1:-0}" 2> /dev/null)" in "" | Z*) return 1 ;; *) return 0 ;; esac
}

start_cell() {
  local slot=$1 slug=$2 name=$3 cell=$4 fn=job_${2//-/_}
  printf '\033[2m.. %s (started)\033[0m\n' "$name"
  (
    cell_use "$slug"
    # the shared stack has no block of its own: one cell at a time there, on
    # the ports the suites use by default
    export E2E_PORT_BASE=$((${E2E_PORT_BASE:-9100} + slot * 40))
    # As in run_job, and for the same reason: on the shared stack a cell still
    # seeds and streams on the names and ports every other run uses. Under
    # --isolated it owns all of them, and E2E_LOCK=none makes this a no-op.
    e2e_lock
    # as in run_job: the `||` is what keeps errexit out of the cell's own steps
    rc=0
    "$fn" || rc=$?
    # The parent cannot see the list a cell registered in here, and on the
    # shared stack it may not remove by name either — those names are not this
    # run's. So the cell removes its own containers, and before it lets the
    # lock go: on the shared stack the next run's cell claims the same names.
    cleanup_containers
    e2e_unlock
    exit $rc
  ) > "$RUN_DIR/$slug.log" 2>&1 &
  POOL_PID[$slot]=$!
  POOL_SLUG[$slot]=$slug
  POOL_NAME[$slot]=$name
  POOL_CELL[$slot]=$cell
  POOL_START[$slot]=$SECONDS
}

# Waits for one cell to end, reports it, removes its containers and leaves its
# slot number in FREED_SLOT.
collect_cell() {
  local slot rc
  while :; do
    for slot in $(seq 0 $((JOBS - 1))); do
      [ -n "${POOL_PID[$slot]:-}" ] || continue
      if ! slot_running "${POOL_PID[$slot]}"; then
        rc=0
        wait "${POOL_PID[$slot]}" || rc=$?
        report_job "${POOL_NAME[$slot]}" "${POOL_CELL[$slot]}" "$rc" \
          "$((SECONDS - POOL_START[slot]))" "$RUN_DIR/${POOL_SLUG[$slot]}.log"
        cleanup_cell "$CELL_BASE-${POOL_SLUG[$slot]#compat-}"
        POOL_PID[$slot]=""
        FREED_SLOT=$slot
        return 0
      fi
    done
    sleep 2
  done
}

# Runs the cells read from stdin, at most $JOBS of them at a time.
run_cells() {
  local slot slug name cell free=""
  for slot in $(seq 0 $((JOBS - 1))); do free="$free $slot"; done
  while IFS='|' read -r slug name cell; do
    [ -n "$slug" ] || continue
    if ! selected "$slug"; then
      SKIPPED=$((SKIPPED + 1))
      printf -- '- %s (not selected)\n' "$name"
      continue
    fi
    if [ -z "${free// /}" ]; then
      collect_cell
      free=" $FREED_SLOT"
    fi
    free=${free# }
    slot=${free%% *}
    case "$free" in *' '*) free=" ${free#* }" ;; *) free="" ;; esac
    start_cell "$slot" "$slug" "$name" "$cell"
  done
  while pool_busy; do collect_cell; done
}

pool_busy() {
  local slot
  for slot in $(seq 0 $((JOBS - 1))); do
    [ -z "${POOL_PID[$slot]:-}" ] || return 0
  done
  return 1
}

# ------------------------------------------------------------------ containers
# Polls the readiness check, and gives up early on a container that is no
# longer running: one Docker's OOM killer took is never going to answer, and
# waiting out the tries would turn a run past this machine's memory into three
# minutes of silence per container instead of an error naming it.
wait_for_container() {
  local name=$1 what=$2 tries=$3
  shift 3
  for _ in $(seq 1 "$tries"); do
    if "$@" > /dev/null 2>&1; then return 0; fi
    if [ "$(container_state "$name")" != "running" ]; then
      echo "$what stopped before it was ready: $(docker inspect \
        -f 'status={{.State.Status}} exit={{.State.ExitCode}} oom-killed={{.State.OOMKilled}}' \
        "$name" 2> /dev/null)"
      docker logs --tail 20 "$name" 2>&1 | sed 's/^/    /'
      return 1
    fi
    sleep 2
  done
  echo "$what never became ready"
  return 1
}

# Read back rather than assumed: under --isolated the host port is Docker's to
# choose, and in the shared case this returns the fixed port it was given.
published_port() { docker port "$1" "$2/tcp" | head -1 | sed 's/.*://'; }

port_free() { ! (exec 3<> "/dev/tcp/127.0.0.1/$1") 2> /dev/null; }

# The pipelines a suite starts bind their metrics and API ports on this machine,
# not inside the cell, so an isolated run needs a region of one 40-port block
# per pool slot. Regions do not overlap and the scan starts at this run's pid,
# because two runs looking at the same moment see the same ports unbound — the
# probe alone would have them both take the first region.
pick_port_base() {
  local want=$1 slots=64 i s k base ok
  for i in $(seq 0 $((slots - 1))); do
    s=$((($$ + i) % slots))
    base=$((9300 + s * 40 * want))
    ok=1
    for k in $(seq 0 $((want - 1))); do
      if port_free $((base + k * 40)) && port_free $((base + k * 40 + 20)) &&
        port_free $((base + k * 40 + 32)); then continue; fi
      ok=0
      break
    done
    if [ "$ok" = 1 ]; then echo "$base"; return 0; fi
  done
  echo "no free block of $((want * 40)) ports for this run's pipelines" >&2
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
  wait_for_container dev-postgres-1 "PostgreSQL" 60 pg_ready dev-postgres-1 || return 1
  wait_for_container dev-opensearch-1 "OpenSearch" 60 os_ready http://localhost:9200 || return 1
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
  wait_for_container mysql-test "MySQL" 90 mysql_ready mysql-test || return 1
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

# Isolated, these are a cell like any other, only with ci.yml's images: the
# same containers, the same seeding, and the suite told where to find them.
job_e2e_postgres() {
  if [ "$ISOLATED" = 1 ]; then
    cell_start "$CELL_PG" "$CELL_OS" || return 1
    cell_opensearch "$OS_IMAGE" || return 1
    cell_postgres "$PG_IMAGE" || return 1
    PG_CONTAINER=$CELL_PG PG_PORT=$CELL_PG_PORT OS_URL=$CELL_OS_URL \
      E2E_LOG=$RUN_DIR/e2e-postgres-pipeline.log ./dev/e2e-test.sh || return 1
    return 0
  fi
  dev_stack_up || return 1
  release_build || return 1
  E2E_LOG=$RUN_DIR/e2e-postgres-pipeline.log ./dev/e2e-test.sh || return 1
}

job_e2e_mysql() {
  if [ "$ISOLATED" = 1 ]; then
    cell_start "$CELL_MYSQL" "$CELL_OS" || return 1
    cell_opensearch "$OS_IMAGE" || return 1
    cell_mysql "$MYSQL_IMAGE" "WITH mysql_native_password" || return 1
    MYSQL_CONTAINER=$CELL_MYSQL MYSQL_PORT=$CELL_MYSQL_PORT OS_URL=$CELL_OS_URL \
      E2E_LOG=$RUN_DIR/e2e-mysql-pipeline.log ./dev/e2e-mysql-test.sh || return 1
    return 0
  fi
  dev_stack_up || return 1
  mysql_stack_up || return 1
  release_build || return 1
  E2E_LOG=$RUN_DIR/e2e-mysql-pipeline.log ./dev/e2e-mysql-test.sh || return 1
}

# The only cell that needs both sources at once: one process reads a PostgreSQL
# and a MySQL, so the isolated form is the two of them beside one target.
job_e2e_multi_source() {
  if [ "$ISOLATED" = 1 ]; then
    cell_start "$CELL_PG" "$CELL_MYSQL" "$CELL_OS" || return 1
    cell_opensearch "$OS_IMAGE" || return 1
    cell_postgres "$PG_IMAGE" || return 1
    cell_mysql "$MYSQL_IMAGE" "WITH mysql_native_password" || return 1
    PG_CONTAINER=$CELL_PG PG_PORT=$CELL_PG_PORT \
      MYSQL_CONTAINER=$CELL_MYSQL MYSQL_PORT=$CELL_MYSQL_PORT OS_URL=$CELL_OS_URL \
      E2E_LOG=$RUN_DIR/e2e-multi-source-pipeline.log ./dev/e2e-multi-source.sh || return 1
    return 0
  fi
  dev_stack_up || return 1
  mysql_stack_up || return 1
  release_build || return 1
  E2E_LOG=$RUN_DIR/e2e-multi-source-pipeline.log ./dev/e2e-multi-source.sh || return 1
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
# The heap is what dev/docker-compose.yml gives the dev stack's node: without
# it OpenSearch sizes itself off the whole Docker VM and two stacks do not fit.
cell_opensearch() {
  docker run -d --name "$CELL_OS" -p "$CELL_OS_PORT:9200" \
    -e discovery.type=single-node -e DISABLE_SECURITY_PLUGIN=true \
    -e OPENSEARCH_JAVA_OPTS="-Xms512m -Xmx512m" \
    "$1" > /dev/null || return 1
  CELL_OS_PORT=$(published_port "$CELL_OS" 9200)
  CELL_OS_URL="http://localhost:$CELL_OS_PORT"
  wait_for_container "$CELL_OS" "OpenSearch" 90 os_green "$CELL_OS_URL" || return 1
}

# The suite needs logical replication from the first connection and a service
# container takes no server flags, so compat.yml starts this one by hand too.
# $2 and $3 are what a derived image insists on, as compat.yml's matrix rows
# spell them out: Supabase creates its roles as supabase_admin and will not
# initialise when POSTGRES_USER names another one, and it ships
# listen_addresses = localhost, which no published port can reach.
cell_postgres() {
  local image=$1 env_args=${2--e POSTGRES_USER=postgres} flags=${3-} creates_db_as=${4-}
  # shellcheck disable=SC2086
  docker run -d --name "$CELL_PG" -p "$CELL_PG_PORT:5432" \
    $env_args -e POSTGRES_PASSWORD=postgres \
    -e POSTGRES_DB=sourcedb "$image" $flags \
    -c wal_level=logical -c max_wal_senders=10 -c max_replication_slots=10 > /dev/null || return 1
  CELL_PG_PORT=$(published_port "$CELL_PG" 5432)
  # a derived image carries a whole platform's bootstrap, so it takes longer
  # than a plain postgres to answer
  wait_for_container "$CELL_PG" "PostgreSQL" 90 pg_ready "$CELL_PG" || return 1
  # A hosted project hands you a database your own role owns; an image that
  # creates it as its own bootstrap role does not, and publishing a table
  # requires owning it.
  if [ -n "$creates_db_as" ]; then
    docker exec "$CELL_PG" psql -U "$creates_db_as" -d postgres \
      -c "ALTER DATABASE sourcedb OWNER TO postgres" > /dev/null || return 1
  fi
  docker exec -i "$CELL_PG" psql -U postgres -d sourcedb < dev/seed.sql || return 1
}

# Seeded the way ci.yml and compat.yml seed a service container: the user the
# suite connects as, GTIDs on, and dev/mysql-seed.sql.
cell_mysql() {
  docker run -d --name "$CELL_MYSQL" -p "$CELL_MYSQL_PORT:3306" \
    -e MYSQL_ROOT_PASSWORD=mysqlpw -e MYSQL_DATABASE=sourcedb \
    "$1" > /dev/null || return 1
  CELL_MYSQL_PORT=$(published_port "$CELL_MYSQL" 3306)
  wait_for_container "$CELL_MYSQL" "MySQL" 90 mysql_ready "$CELL_MYSQL" || return 1
  mysql_user "$CELL_MYSQL" mysql "$2" || return 1
  mysql_enable_gtid "$CELL_MYSQL" || return 1
  docker exec -i "$CELL_MYSQL" mysql -uroot -pmysqlpw sourcedb < dev/mysql-seed.sql || return 1
}

# A cell owns throwaway containers; naming them here has whatever runs the cell
# remove them however it ends, and clears away what a killed run left first.
cell_start() {
  CLEANUP_CONTAINERS="$*"
  cleanup_containers
  CLEANUP_CONTAINERS="$*"
  release_build || return 1
}

job_compat_postgres15() {
  cell_start "$CELL_PG" "$CELL_OS" || return 1
  cell_opensearch "$COMPAT_OS_IMAGE" || return 1
  cell_postgres "$COMPAT_PG15_IMAGE" || return 1
  PG_CONTAINER=$CELL_PG PG_PORT=$CELL_PG_PORT \
    OS_URL=$CELL_OS_URL E2E_LOG=$RUN_DIR/compat-postgres15-pipeline.log \
    ./dev/e2e-test.sh || return 1
}

# Same suite, same flags: what the cell proves is that a derived server
# decodes as the plain one does, so nothing here may be special-cased.
compat_derived() {
  cell_start "$CELL_PG" "$CELL_OS" || return 1
  cell_opensearch "$COMPAT_OS_IMAGE" || return 1
  cell_postgres "$1" "$2" "$3" "$4" || return 1
  PG_CONTAINER=$CELL_PG PG_PORT=$CELL_PG_PORT \
    OS_URL=$CELL_OS_URL E2E_LOG=$RUN_DIR/$5-pipeline.log \
    ./dev/e2e-test.sh || return 1
}

job_compat_timescaledb() {
  compat_derived "$COMPAT_TIMESCALE_IMAGE" "-e POSTGRES_USER=postgres" "" "" compat-timescaledb
}

job_compat_supabase() {
  compat_derived "$COMPAT_SUPABASE_IMAGE" "" "-c listen_addresses=0.0.0.0" supabase_admin compat-supabase
}

job_compat_elasticsearch() {
  cell_start "$CELL_PG" "$CELL_ES" || return 1
  docker run -d --name "$CELL_ES" -p "$CELL_ES_PORT:9200" \
    -e discovery.type=single-node -e xpack.security.enabled=false \
    -e xpack.security.enrollment.enabled=false -e ES_JAVA_OPTS="-Xms512m -Xmx512m" \
    "$COMPAT_ES_IMAGE" > /dev/null || return 1
  CELL_ES_PORT=$(published_port "$CELL_ES" 9200)
  # not "green": a single-node Elasticsearch leaves every replica unassigned
  wait_for_container "$CELL_ES" "Elasticsearch" 90 es_yellow "http://localhost:$CELL_ES_PORT" || return 1
  cell_postgres "$COMPAT_PG_IMAGE" || return 1
  PG_CONTAINER=$CELL_PG PG_PORT=$CELL_PG_PORT \
    OS_URL="http://localhost:$CELL_ES_PORT" TARGET_FLAVOR=elasticsearch \
    E2E_LOG=$RUN_DIR/compat-elasticsearch-pipeline.log ./dev/e2e-test.sh || return 1
}

job_compat_meilisearch() {
  cell_start "$CELL_PG" "$CELL_MEILI" || return 1
  docker run -d --name "$CELL_MEILI" -p "$CELL_MEILI_PORT:7700" \
    -e MEILI_MASTER_KEY=e2e-master-key -e MEILI_ENV=development \
    "$COMPAT_MEILI_IMAGE" > /dev/null || return 1
  CELL_MEILI_PORT=$(published_port "$CELL_MEILI" 7700)
  wait_for_container "$CELL_MEILI" "Meilisearch" 60 meili_up "http://localhost:$CELL_MEILI_PORT" || return 1
  cell_postgres "$COMPAT_PG_IMAGE" || return 1
  # Meilisearch has no mappings, no joins and no per-row indices, so the full
  # suite cannot run against it; this is what it does support.
  PG_CONTAINER=$CELL_PG PG_PORT=$CELL_PG_PORT \
    MEILI_URL="http://localhost:$CELL_MEILI_PORT" MEILI_MASTER_KEY=e2e-master-key \
    E2E_LOG=$RUN_DIR/compat-meilisearch-pipeline.log ./dev/e2e-meili-smoke.sh || return 1
}

job_compat_mysql84() {
  cell_start "$CELL_MYSQL" "$CELL_OS" || return 1
  cell_opensearch "$COMPAT_OS_IMAGE" || return 1
  # 8.4 dropped mysql_native_password from the default plugins, so this is also
  # the cell that proves the caching_sha2_password handshake.
  cell_mysql "$COMPAT_MYSQL_IMAGE" "WITH caching_sha2_password" || return 1
  MYSQL_CONTAINER=$CELL_MYSQL MYSQL_PORT=$CELL_MYSQL_PORT \
    OS_URL=$CELL_OS_URL E2E_LOG=$RUN_DIR/compat-mysql84-pipeline.log \
    ./dev/e2e-mysql-test.sh || return 1
}

compat_mariadb() {
  cell_start "$CELL_MYSQL" "$CELL_OS" || return 1
  cell_opensearch "$COMPAT_OS_IMAGE" || return 1
  # MariaDB writes no binlog unless it is told to
  docker run -d --name "$CELL_MYSQL" -p "$CELL_MYSQL_PORT:3306" \
    -e MARIADB_ROOT_PASSWORD=mysqlpw -e MARIADB_DATABASE=sourcedb "$1" \
    --log-bin --binlog-format=ROW --binlog-row-image=FULL --server-id=1 > /dev/null || return 1
  CELL_MYSQL_PORT=$(published_port "$CELL_MYSQL" 3306)
  wait_for_container "$CELL_MYSQL" "MariaDB" 90 maria_ready "$CELL_MYSQL" || return 1
  mysql_user "$CELL_MYSQL" mariadb "" || return 1
  docker exec -i "$CELL_MYSQL" mariadb -uroot -pmysqlpw sourcedb < dev/mysql-seed.sql || return 1
  # MariaDB has no GTID position the suite can assert on; the script knows
  MYSQL_CONTAINER=$CELL_MYSQL MYSQL_PORT=$CELL_MYSQL_PORT MYSQL_CLIENT=mariadb \
    OS_URL=$CELL_OS_URL E2E_LOG=$RUN_DIR/compat-${1/:/-}-pipeline.log \
    ./dev/e2e-mysql-test.sh || return 1
}

job_compat_mariadb106() { compat_mariadb "$COMPAT_MARIADB_1"; }
job_compat_mariadb118() { compat_mariadb "$COMPAT_MARIADB_2"; }

# ------------------------------------------------------------------------ main
mkdir -p "$RUN_DIR"

if [ "$ISOLATED" = 1 ]; then
  # One block per pool slot: two cells at once mean two pipelines binding on
  # this machine, and they cannot both have 9111.
  E2E_PORT_BASE=$(pick_port_base "$JOBS") || exit 1
  export E2E_PORT_BASE
fi

bold "pg2osync — what a pull request runs on CI, run here"
note "run id      $RUN_ID"
if [ "$ISOLATED" = 1 ]; then
  note "mode        isolated — containers of this run's own ($CELL_BASE-*), no shared lock"
  note "            pipelines on 127.0.0.1:$E2E_PORT_BASE-$((E2E_PORT_BASE + JOBS * 40 - 1))"
  note "cells        $JOBS at a time"
else
  note "mode        shared dev stack, under $E2E_LOCK"
  if [ -d "$E2E_LOCK" ] && kill -0 "$(cat "$E2E_LOCK/pid" 2> /dev/null || echo 0)" 2> /dev/null; then
    warn "the dev stack is busy: pid $(cat "$E2E_LOCK/pid") holds the lock and the e2e"
    warn "jobs will queue behind it. --isolated runs beside it instead."
  fi
fi
note "logs        $RUN_DIR"
note "postgres    $PG_IMAGE ($CI_WF)"
note "opensearch  $OS_IMAGE ($CI_WF)"
note "mysql       $MYSQL_IMAGE ($CI_WF)"
note "msrv        $MSRV ($CI_WF; Cargo.toml says $CARGO_MSRV)"
note "mdbook      $MDBOOK_VERSION ($DOCS_WF)"
note "audit       $AUDIT_CMD ($AUDIT_WF) — $AUDIT_REASON"
note "matrix      $([ "$MATRIX_ON" = 1 ] && echo on || echo off) — $MATRIX_REASON"
if [ "$MATRIX_ON" = 1 ]; then
  note "  images    $COMPAT_PG15_IMAGE, $COMPAT_PG_IMAGE, $COMPAT_TIMESCALE_IMAGE,"
  note "            $COMPAT_SUPABASE_IMAGE, $COMPAT_ES_IMAGE, $COMPAT_MEILI_IMAGE,"
  note "            $COMPAT_MYSQL_IMAGE, $COMPAT_MARIADB_1, $COMPAT_MARIADB_2"
  if [ "$ISOLATED" = 1 ]; then
    note "  ports     assigned by Docker"
  else
    note "  ports     postgres $COMPAT_PG_PORT, opensearch $COMPAT_OS_PORT, elasticsearch $COMPAT_ES_PORT,"
    note "            meilisearch $COMPAT_MEILI_PORT, mysql/mariadb $COMPAT_MYSQL_PORT"
  fi
fi
if [ "$FAST" = 1 ]; then
  warn "--fast: the e2e suites, the image build and the matrix are skipped."
  warn "It is a quick loop, not the definition of done. Run the whole script"
  warn "before you push, or CI finds what this one did not look at."
  SKIP="$SKIP e2e-postgres e2e-mysql e2e-multi-source docker"
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
run_job e2e-multi-source "e2e several sources in one process"
run_job docker "container image builds"
run_job helm "helm chart lints"
run_job docs "the book builds"
run_job pr-title "the title is a conventional commit"
run_job audit "dependencies have no known advisories"

if [ "$MATRIX_ON" = 1 ] && [ "$FAST" = 0 ]; then
  # slug, title, and the compat.yml job the advisory list is keyed by
  run_cells <<CELLS
compat-postgres15|PostgreSQL 15 to OpenSearch|compat-postgres
compat-timescaledb|TimescaleDB to OpenSearch|compat-derived
compat-supabase|Supabase PostgreSQL to OpenSearch|compat-derived
compat-elasticsearch|PostgreSQL 17 to Elasticsearch|compat-elasticsearch
compat-meilisearch|PostgreSQL 17 to Meilisearch|compat-meilisearch
compat-mysql84|MySQL 8.4 to OpenSearch|compat-mysql
compat-mariadb106|MariaDB 10.6 to OpenSearch|compat-mariadb
compat-mariadb118|MariaDB 11.8 to OpenSearch|compat-mariadb
CELLS
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
