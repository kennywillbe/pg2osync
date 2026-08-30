#!/usr/bin/env bash
# Sourced by the e2e suites: the pipelines this run started, and nothing else.
#
# A machine can carry several stacks at once — the shared dev stack and one per
# isolated ci-local run — so "stop the pipeline" cannot mean every pg2osync
# process on it: that would take down the suite running beside this one and
# report failures that are not real. Every spawn records its pid here, and
# every stop signals only those pids.

SYNC_PIDS=""
SYNC_PID=""
LIVE_SYNCS=""

# Started by this run, still running, and still pg2osync: a zombie has already
# exited, and a pid the kernel handed out again belongs to something else.
sync_alive() {
  local line
  line=$(ps -o state=,command= -p "${1:-0}" 2> /dev/null) || return 1
  case "$line" in "" | Z*) return 1 ;; esac
  case "$line" in *pg2osync*) return 0 ;; *) return 1 ;; esac
}

remember_sync() { SYNC_PID=$1; SYNC_PIDS="$SYNC_PIDS $1"; }

# Collects into LIVE_SYNCS rather than printing, so it can drop the pids that
# have exited: a subshell's pruning would be thrown away with the subshell.
live_syncs() {
  local pid
  LIVE_SYNCS=""
  for pid in $SYNC_PIDS; do
    if sync_alive "$pid"; then LIVE_SYNCS="$LIVE_SYNCS$pid "; fi
  done
  SYNC_PIDS=$LIVE_SYNCS
  [ -n "$LIVE_SYNCS" ]
}

# The pid of the pipeline last started, while it is still the same process: the
# recovery sections assert that recovery happened in process rather than by
# something restarting us, and an exited pipeline has to fail that comparison.
sync_pid() {
  if sync_alive "$SYNC_PID"; then printf '%s' "$SYNC_PID"; return 0; fi
  return 1
}

# It stays a child of this shell, so a section can still wait for its exit code.
sync_spawn() {
  nohup "$BIN" run -c "$1" >> "${2:-$LOG}" 2>&1 < /dev/null &
  remember_sync $!
}

sync_wait_gone() {
  local waited=0
  while live_syncs; do
    if [ "$waited" -ge "$1" ]; then return 1; fi
    sleep 0.1
    waited=$((waited + 1))
  done
  return 0
}

# Reaping a killed child is also what keeps bash from printing its own
# "Killed: 9" line over the suite's output.
sync_reap() {
  local pid
  for pid in "$@"; do wait "$pid" 2> /dev/null || true; done
}

# SIGTERM drains rather than kills: whatever follows a stop — a restart on the
# same slot, dropping it — needs the exit. A pipeline that will not drain
# inside the bound is killed, so a hung one fails the assertion after it
# instead of hanging the suite.
sync_stop() {
  local pids
  if live_syncs; then
    pids=$LIVE_SYNCS
    # shellcheck disable=SC2086
    kill $pids 2> /dev/null || true
    if ! sync_wait_gone 100; then
      # shellcheck disable=SC2086
      kill -9 $LIVE_SYNCS 2> /dev/null || true
    fi
    # shellcheck disable=SC2086
    sync_reap $pids
  fi
  SYNC_PIDS=""
}

# What the crash sections mean: gone without a chance to drain.
sync_kill() {
  local pids
  if live_syncs; then
    pids=$LIVE_SYNCS
    # shellcheck disable=SC2086
    kill -9 $pids 2> /dev/null || true
    # shellcheck disable=SC2086
    sync_reap $pids
  fi
  SYNC_PIDS=""
}
