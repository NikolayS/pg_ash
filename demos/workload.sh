#!/usr/bin/env bash
# workload.sh — Generates a two-phase wait-event mix inside the demo container.
#
# Phase 1 (BASELINE_SEC):   light pgbench read-only traffic → Client:ClientRead,
#                           CPU*. Timeline looks "quiet".
# Phase 2 (SPIKE_SEC):      several concurrent writers contend on the SAME row.
#                           One holds pg_sleep() inside a transaction; the rest
#                           queue behind it on Lock:tuple / Lock:transactionid.
# Phase 3 (TAIL_SEC):       back to baseline so the ring-down is visible.
#
# The workload runs inside the container, talks to PG via the local UNIX socket,
# and writes only phase markers to stdout (not visible in the recorded pane).

set -euo pipefail

PSQL_BIN=psql
PSQL_ARGS=(-X -qAt -U postgres -d demo)
BASELINE_SEC="${BASELINE_SEC:-15}"
SPIKE_SEC="${SPIKE_SEC:-30}"
TAIL_SEC="${TAIL_SEC:-45}"
PGBENCH_CLIENTS="${PGBENCH_CLIENTS:-4}"
LOCK_WORKERS="${LOCK_WORKERS:-5}"
LOCK_SLEEP_SEC="${LOCK_SLEEP_SEC:-3}"
HOT_ROW_AID="${HOT_ROW_AID:-42}"

log() { printf '[workload %s] %s\n' "$(date +%H:%M:%S)" "$*"; }

cleanup() {
  # Kill everything this script launched, but don't noisily report failures —
  # the container keeps running and we just want a clean exit.
  jobs -p | xargs -r kill -TERM 2>/dev/null || true
  sleep 0.3
  jobs -p | xargs -r kill -KILL 2>/dev/null || true
  wait 2>/dev/null || true
}
trap cleanup EXIT INT TERM

# ---- Phase 1: baseline ------------------------------------------------------
log "phase 1: baseline pgbench (${BASELINE_SEC}s, ${PGBENCH_CLIENTS} clients, SELECT-only)"
pgbench -U postgres -d demo -S -c "$PGBENCH_CLIENTS" -j 2 -T "$BASELINE_SEC" -n \
  >/dev/null 2>&1 &
PGBENCH1=$!

wait "$PGBENCH1" 2>/dev/null || true

# ---- Phase 2: lock-contention spike -----------------------------------------
log "phase 2: row-lock spike on aid=${HOT_ROW_AID} (${SPIKE_SEC}s, ${LOCK_WORKERS} contenders)"

# Keep a light baseline running through the spike so the mix stays realistic
# (a few pgbench SELECTs for ClientRead/CPU* sprinkles).
pgbench -U postgres -d demo -S -c 2 -j 1 -T "$SPIKE_SEC" -n \
  >/dev/null 2>&1 &
PGBENCH2=$!

# The "holder" loop: acquires the row, sleeps LOCK_SLEEP_SEC, commits, repeat.
# Each iteration holds the row for LOCK_SLEEP_SEC; contenders queue on it.
(
  end=$(( $(date +%s) + SPIKE_SEC ))
  while [ "$(date +%s)" -lt "$end" ]; do
    "$PSQL_BIN" "${PSQL_ARGS[@]}" <<SQL >/dev/null 2>&1 || true
begin;
update pgbench_accounts set abalance = abalance + 1 where aid = ${HOT_ROW_AID};
select pg_sleep(${LOCK_SLEEP_SEC});
commit;
SQL
  done
) &

# N contender loops: each hammers the same row. With the holder occupying the
# row for LOCK_SLEEP_SEC at a time, every contender always finds a locked row
# and enters the wait queue → Lock:tuple + Lock:transactionid.
for i in $(seq 1 "$LOCK_WORKERS"); do
  (
    end=$(( $(date +%s) + SPIKE_SEC ))
    while [ "$(date +%s)" -lt "$end" ]; do
      "$PSQL_BIN" "${PSQL_ARGS[@]}" -c \
        "update pgbench_accounts set abalance = abalance - 1 where aid = ${HOT_ROW_AID};" \
        >/dev/null 2>&1 || true
    done
  ) &
done

# Wait out the spike duration. Then terminate any remaining workers.
sleep "$SPIKE_SEC"
jobs -p | xargs -r kill -TERM 2>/dev/null || true
sleep 0.5
jobs -p | xargs -r kill -KILL 2>/dev/null || true
wait 2>/dev/null || true

# ---- Phase 3: tail (quiet coda) --------------------------------------------
log "phase 3: tail pgbench (${TAIL_SEC}s)"
pgbench -U postgres -d demo -S -c "$PGBENCH_CLIENTS" -j 2 -T "$TAIL_SEC" -n \
  >/dev/null 2>&1 || true

log "workload complete"
