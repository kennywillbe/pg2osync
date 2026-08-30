#!/usr/bin/env bash
# Sourced by the e2e suites so that only one runs on a machine at a time.
#
# Stopping a pipeline kills every pg2osync process, so two suites at once take
# each other's down and report failures that are not real. mkdir creates the
# lock directory atomically; the holder's pid inside it lets a lock left behind
# by a killed run be reclaimed instead of waited on until someone notices.
#
#   E2E_LOCK       lock directory   (default /tmp/pg2osync-e2e.lock)
#   E2E_LOCK_WAIT  seconds to wait  (default 5400)

E2E_LOCK=${E2E_LOCK:-/tmp/pg2osync-e2e.lock}
E2E_LOCK_WAIT=${E2E_LOCK_WAIT:-5400}

# A run that already holds the lock — ci-local around a suite — passes it down
# through E2E_LOCK_OWNER, so the suite it starts does not wait on its parent.
e2e_lock() {
  local waited=0 holder
  if [ -n "${E2E_LOCK_OWNER:-}" ] && kill -0 "$E2E_LOCK_OWNER" 2>/dev/null; then
    return 0
  fi
  until mkdir "$E2E_LOCK" 2>/dev/null; do
    holder=$(cat "$E2E_LOCK/pid" 2>/dev/null || true)
    if [ -n "$holder" ] && ! kill -0 "$holder" 2>/dev/null; then
      rm -rf "$E2E_LOCK"
      continue
    fi
    if [ "$waited" -eq 0 ]; then
      echo "waiting for $E2E_LOCK, held by pid ${holder:-unknown}"
    fi
    if [ "$waited" -ge "$E2E_LOCK_WAIT" ]; then
      echo "gave up waiting for $E2E_LOCK after ${waited}s"
      exit 1
    fi
    sleep 10
    waited=$((waited + 10))
  done
  echo $$ > "$E2E_LOCK/pid"
  export E2E_LOCK_OWNER=$$
}

# Only the holder releases: a suite that timed out waiting must not remove a
# lock that belongs to the run it was waiting for.
e2e_unlock() {
  if [ "$(cat "$E2E_LOCK/pid" 2>/dev/null)" = "$$" ]; then
    rm -rf "$E2E_LOCK"
  fi
}
