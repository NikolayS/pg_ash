#!/usr/bin/env bash
#
# lib/seed.sh — build a frozen incident window in a real PostgreSQL, fast.
#
# The trick, in one sentence: run real pgbench load, sample it with pg_ash's own
# ash.take_sample(), and then move the timestamps of the samples we just took
# into the virtual minute they represent. One real second of load becomes one
# virtual minute of history, so 28 minutes of believable ASH arrives in ~20
# seconds instead of ~28 real minutes.
#
# What is real: every sample, every wait event, every query id, every count.
# What is shaped: which samples exist, and when they are considered to have
# been taken. See the honesty boundary at the top of lib/seed.sql.
#
# The story the seeder tells, in virtual minutes:
#
#   1 .. 4    calm   read-only baseline    <- SLACK, outside the query window,
#                                             so the raw-retention guardrail in
#                                             the drill readers cannot trip as
#                                             the seed ages
#   5 .. 12   calm   read-only baseline    <- the compare() baseline window
#  13 .. 17   STORM  row-lock contention   <- the incident
#  18 .. 20   tail   mixed write recovery
#  21 .. 28   busy   read/IO load
#
# Exit codes: 3 backend, 4 seed assertion. (§2.6)

set -Eeuo pipefail

ASH_SEED_LIB_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)
# shellcheck source=lib/backend.sh
. "$ASH_SEED_LIB_DIR/backend.sh"

# ---------------------------------------------------------------------------
# Phase plan. Virtual-minute counts; they must sum to ASH_VMIN + ASH_VMIN_SLACK.
# ---------------------------------------------------------------------------
: "${ASH_PH_BASELINE:=12}"   # 4 slack + 8 in-window calm minutes
: "${ASH_PH_STORM:=5}"
: "${ASH_PH_RECOVERY:=3}"
: "${ASH_PH_READIO:=8}"

# pgbench shape. Kept as knobs because "make the spike legible" is a tuning
# problem and tuning through edits to a 200-line script is miserable.
#
# The calm phases are deliberately under-provisioned relative to the storm: the
# shape assertion demands storm peak_aas >= 3x the median calm minute, and a
# calm baseline that is itself busy eats the whole margin. Measured on this
# hardware: 4 calm clients -> ~3 AAS, 12 storm clients -> ~15 peak AAS, so the
# ratio lands near 5x with room for a loaded machine.
# These numbers are also a PICTURE decision, not just a load decision.
# ash.chart() ranks its series by total AAS over the whole window, so a heavy
# calm phase does not merely look busy — it pushes both Lock waits out of the
# top four and the flagship chart draws the five-minute incident as an anonymous
# column of "Other" dots. Measured here: baseline at 4 clients gave a 3.7 AAS
# calm floor and CPU* outranked Lock:transactionid over the 24-minute window.
# At 3/3 clients the calm floor stays below 2 AAS on faster Linux hosts and the
# storm owns the chart without relying on a platform-specific scheduler shape.
# lib/shape.sql assertion 7 fails the seed if this balance ever slips again.
: "${ASH_LOAD_BASELINE_CLIENTS:=3}"
: "${ASH_LOAD_BASELINE_JOBS:=2}"
: "${ASH_LOAD_STORM_CLIENTS:=12}"
: "${ASH_LOAD_STORM_JOBS:=4}"
: "${ASH_LOAD_STORM_BG_CLIENTS:=3}"
: "${ASH_LOAD_RECOVERY_CLIENTS:=4}"
: "${ASH_LOAD_READIO_CLIENTS:=3}"
: "${ASH_LOAD_READIO_JOBS:=2}"
# Rows scanned per read transaction. See lib/workload_read.sql for why the read
# phases are range aggregates rather than `-b select-only`.
: "${ASH_READ_SPAN_CALM:=1500}"
: "${ASH_READ_SPAN_TAIL:=5000}"
: "${ASH_PGBENCH_SCALE:=20}"

ASH_VMIN_TOTAL=$(( ASH_VMIN + ASH_VMIN_SLACK ))

# Hard ceiling (real seconds) on any single background pgbench. The phase ends
# when the SAMPLER says so and load_stop tears the client down; this is only a
# seatbelt against an orphaned client if the harness is killed outright.
if [ -z "${ASH_LOAD_CAP:-}" ]; then
  if [ "${ASH_REAL_TIME:-}" = "1" ]; then
    ASH_LOAD_CAP=$(( ASH_VMIN_TOTAL * 60 + 120 ))
  else
    ASH_LOAD_CAP=120
  fi
fi

# Real seconds slept between samples inside a virtual minute.
: "${ASH_SAMPLE_DELAY:=0.04}"

ASH_PGBENCH_LOG="$ASH_OUT/pgbench.log"

# ---------------------------------------------------------------------------
# Background load management
# ---------------------------------------------------------------------------
#
# bash 3.2: no arrays of substance needed, a space-separated pid list is plenty.
ASH_LOAD_PIDS=""

# load_start <label> <pgbench args...> — start a pgbench in the background.
# -T is deliberately generous: the phase ends when the SAMPLER says so, and
# load_stop tears the client down. Guessing a duration is how the old harness
# ended up with half-drawn frames.
load_start() {
  local label=$1; shift
  {
    echo "--- $label: pgbench $* ---"
  } >>"$ASH_PGBENCH_LOG"
  PGAPPNAME=ash_demo_load pgbench "$@" >>"$ASH_PGBENCH_LOG" 2>&1 &
  ASH_LOAD_PIDS="$ASH_LOAD_PIDS $!"
}

# load_stop — stop every background pgbench and reap it.
#
# SIGTERM, not SIGINT. POSIX says a non-interactive shell starts background
# jobs with SIGINT and SIGQUIT set to SIG_IGN, so `kill -INT` on a backgrounded
# pgbench is silently a no-op and `wait` then blocks for the whole -T duration.
# That cost one debugging cycle: the first run of this seeder sat for 600
# seconds waiting for a phase that had already finished sampling.
load_stop() {
  local pid alive
  for pid in $ASH_LOAD_PIDS; do
    kill -TERM "$pid" 2>/dev/null || true
  done
  # Give pgbench a beat to close its connections, then insist.
  alive=0
  for pid in $ASH_LOAD_PIDS; do
    if kill -0 "$pid" 2>/dev/null; then alive=1; fi
  done
  if [ "$alive" = "1" ]; then
    python3 -c 'import time; time.sleep(0.3)'
    for pid in $ASH_LOAD_PIDS; do
      kill -KILL "$pid" 2>/dev/null || true
    done
  fi
  for pid in $ASH_LOAD_PIDS; do
    wait "$pid" 2>/dev/null || true
  done
  ASH_LOAD_PIDS=""
  # Belt: a pgbench killed mid-transaction can leave a backend finishing its
  # statement. Terminate anything still tagged as demo load so the next phase
  # starts from a known state.
  ash_psql1 "
    select count(pg_terminate_backend(activity.pid))
    from pg_stat_activity as activity
    where activity.application_name = 'ash_demo_load'
      and activity.datname = current_database()
      and activity.pid <> pg_backend_pid()" >/dev/null 2>&1 || true
}

# load_wait_ready <min_clients> — block until the workload is actually running.
#
# Deterministic pacing applies to the LOAD too, not just to the terminal. A
# pgbench takes a few hundred milliseconds to connect and warm, and a virtual
# minute here is only ~0.5 real seconds: start sampling too early and the first
# virtual minute records an idle system, which is both a lie and (because
# restamp insists every minute carries samples) a hard failure.
load_wait_ready() {
  local want=$1 deadline seen
  deadline=$(( $(ash_now_ms) + 15000 ))
  while [ "$(ash_now_ms)" -lt "$deadline" ]; do
    seen=$(ash_psql1 "
      select count(*)
      from pg_stat_activity as activity
      where activity.datname = current_database()
        and activity.application_name = 'ash_demo_load'
        and activity.state = 'active'" 2>/dev/null || echo 0)
    if [ "${seen:-0}" -ge "$want" ]; then
      return 0
    fi
    python3 -c 'import time; time.sleep(0.05)'
  done
  ash_die 3 "workload never reached $want active backend(s) — is the cluster overloaded?"
}

trap 'load_stop' EXIT INT TERM

# ---------------------------------------------------------------------------
# The sampler: one session, one phase.
# ---------------------------------------------------------------------------
#
# PGAPPNAME tags it so ash_demo.reset_state() can find and terminate a
# straggler from an interrupted run. Exactly ONE of these may be alive at a
# time: a second sampler shows up in the first one's samples as Timeout:PgSleep
# at ~27% and destroys the story.
run_phase() {
  local start_idx=$1 n_minutes=$2
  PGAPPNAME=ash_demo_sampler psql -X -v ON_ERROR_STOP=1 -d "$ASH_DEMO_DB" -q -c \
    "call ash_demo.phase($ASH_BASE_TS, $start_idx, $n_minutes, $ASH_SPM,
                         $ASH_SAMPLE_DELAY, $ASH_RESTAMP_ON)" \
    || ash_die 4 "sampling phase starting at virtual minute $start_idx failed"
}

# ---------------------------------------------------------------------------
# seed_main
# ---------------------------------------------------------------------------
seed_main() {
  local t0 t_ready
  t0=$(ash_now_ms)

  [ $(( ASH_PH_BASELINE + ASH_PH_STORM + ASH_PH_RECOVERY + ASH_PH_READIO )) \
     -eq "$ASH_VMIN_TOTAL" ] \
    || ash_die 1 "phase plan sums to $(( ASH_PH_BASELINE + ASH_PH_STORM + ASH_PH_RECOVERY + ASH_PH_READIO )) virtual minutes but ASH_VMIN+ASH_VMIN_SLACK is $ASH_VMIN_TOTAL"

  : >"$ASH_PGBENCH_LOG"

  # -- Helper schema ---------------------------------------------------------
  ash_step "installing ash_demo helper schema"
  ash_psql -q -f "$ASH_SEED_LIB_DIR/seed.sql" \
    || ash_die 3 "could not install $ASH_SEED_LIB_DIR/seed.sql"

  # -- Sampling cadence ------------------------------------------------------
  #
  # sample_interval = 60 / ASH_SPM makes the AAS arithmetic exact:
  # backend_seconds = samples x interval, so ASH_SPM samples at that declared
  # interval is precisely one virtual minute of backend-seconds. Get this wrong
  # and every AAS in the demo is subtly, unfalsifiably off.
  ASH_INTERVAL_SECS=$(python3 -c "print(60.0/$ASH_SPM)")

  # -- Where the frozen window sits on the clock -----------------------------
  #
  # The window ENDS at the top of the current hour. That is not cosmetic: it
  # guarantees the seeded history lies entirely inside a COMPLETED clock hour,
  # which is the only thing ash.rollup_hour() will roll. Without a rollup_1h
  # row the wide periods (1h/1d/1w/1mo) in ash.periods() have no source and
  # come back empty — a silent, ugly failure.
  ASH_BASE_TS=$(ash_psql1 "
    select ash.ts_from_timestamptz(date_trunc('hour', now()))
           - $(( ASH_VMIN_TOTAL * 60 ))")
  [ -n "$ASH_BASE_TS" ] || ash_die 3 "could not resolve the virtual window base timestamp"

  if [ "${ASH_REAL_TIME:-}" = "1" ]; then
    # The honesty escape hatch: no restamping, real wall-clock pacing. One
    # virtual minute costs one real minute, so a full seed is ~28 minutes.
    ASH_RESTAMP_ON=false
    ASH_SAMPLE_DELAY=$ASH_INTERVAL_SECS
    ASH_BASE_TS=$(ash_psql1 "select ash.ts_from_timestamptz(date_trunc('minute', now()))")
    ash_warn "ASH_REAL_TIME=1: sampling in real time, this will take ~$ASH_VMIN_TOTAL minutes"
  else
    ASH_RESTAMP_ON=true
  fi

  # -- Load fixtures ---------------------------------------------------------
  #
  # pgbench -i first: the storm needs a real table with a real contended row,
  # and the read phases need enough data that IO waits are genuine rather than
  # a fully cached fiction.
  ash_step "initialising pgbench fixtures (scale $ASH_PGBENCH_SCALE)"
  PGAPPNAME=ash_demo_init pgbench -i -s "$ASH_PGBENCH_SCALE" -q \
    >>"$ASH_PGBENCH_LOG" 2>&1 \
    || ash_die 3 "pgbench -i failed; see $ASH_PGBENCH_LOG"

  # -- Reset -----------------------------------------------------------------
  ash_step "resetting pg_ash state (samples, rollups, rollup watermarks)"
  ash_psql -q -c "call ash_demo.reset_state($ASH_INTERVAL_SECS)" \
    || ash_die 3 "ash_demo.reset_state failed"
  # pg_stat_statements is reset here so the demo's query texts come only from
  # the demo's own workload; the installer's DDL never shows up in a drill.
  ash_psql -q -c "select pg_stat_statements_reset()" >/dev/null 2>&1 || true

  t_ready=$(ash_now_ms)
  ash_log "setup took $(( (t_ready - t0) )) ms"

  # =========================================================================
  # PHASE 1 — calm baseline, READ ONLY on purpose.
  #
  # READ ONLY, not the default TPC-B. Default TPC-B at a low scale gives
  # 6-9 AAS of Lock:transactionid because every client fights over the handful
  # of pgbench_branches rows, and then "calm" looks exactly like the incident.
  # A prototype shipped precisely that mistake. Calm must actually look calm.
  # =========================================================================
  ash_step "phase 1/4 baseline — $ASH_PH_BASELINE virtual minutes, read-only"
  load_start baseline -c "$ASH_LOAD_BASELINE_CLIENTS" -j "$ASH_LOAD_BASELINE_JOBS" \
    -T $ASH_LOAD_CAP -n -f "$ASH_SEED_LIB_DIR/workload_read.sql" \
    -D span="$ASH_READ_SPAN_CALM"
  load_wait_ready 1
  run_phase 1 "$ASH_PH_BASELINE"
  load_stop

  # =========================================================================
  # PHASE 2 — the incident: a row-lock storm on one contended row, plus a
  # little ordinary write traffic so the picture is a real system under stress
  # and not a synthetic monoculture.
  # =========================================================================
  ash_step "phase 2/4 lock storm — $ASH_PH_STORM virtual minutes, $ASH_LOAD_STORM_CLIENTS contending clients"
  load_start storm -c "$ASH_LOAD_STORM_CLIENTS" -j "$ASH_LOAD_STORM_JOBS" \
    -T $ASH_LOAD_CAP -n -f "$ASH_SEED_LIB_DIR/workload_lock.sql"
  load_start storm_bg -c "$ASH_LOAD_STORM_BG_CLIENTS" -j 1 -T $ASH_LOAD_CAP -n
  load_wait_ready 2
  run_phase $(( ASH_PH_BASELINE + 1 )) "$ASH_PH_STORM"
  load_stop

  # =========================================================================
  # PHASE 3 — recovery: ordinary read/write TPC-B. Contributes the
  # Client:ClientRead / IdleTx population (pgbench's explicit BEGIN..END leaves
  # backends idle-in-transaction between statements, which is a genuine and
  # very common production wait).
  # =========================================================================
  ash_step "phase 3/4 recovery — $ASH_PH_RECOVERY virtual minutes, mixed write load"
  load_start recovery -c "$ASH_LOAD_RECOVERY_CLIENTS" -j 2 -T $ASH_LOAD_CAP -n
  load_wait_ready 1
  run_phase $(( ASH_PH_BASELINE + ASH_PH_STORM + 1 )) "$ASH_PH_RECOVERY"
  load_stop

  # =========================================================================
  # PHASE 4 — busy read/IO tail.
  # =========================================================================
  ash_step "phase 4/4 read tail — $ASH_PH_READIO virtual minutes, $ASH_LOAD_READIO_CLIENTS readers"
  load_start readio -c "$ASH_LOAD_READIO_CLIENTS" -j "$ASH_LOAD_READIO_JOBS" \
    -T $ASH_LOAD_CAP -n -f "$ASH_SEED_LIB_DIR/workload_read.sql" \
    -D span="$ASH_READ_SPAN_TAIL"
  load_wait_ready 1
  run_phase $(( ASH_PH_BASELINE + ASH_PH_STORM + ASH_PH_RECOVERY + 1 )) "$ASH_PH_READIO"
  load_stop

  # =========================================================================
  # Rollups — through pg_ash's own functions, so the source-selection path
  # behaves exactly as it does in production. rollup_minute() caps its catch-up
  # per call, so call it twice; rollup_hour() then folds the completed hour.
  # =========================================================================
  ash_step "rolling up"
  ash_psql -q -c "select ash.rollup_minute($(( ASH_VMIN_TOTAL * 60 )))" >/dev/null
  ash_psql -q -c "select ash.rollup_minute($(( ASH_VMIN_TOTAL * 60 )))" >/dev/null
  ash_psql -q -c "select ash.rollup_hour()" >/dev/null

  # =========================================================================
  # Shape assertions (§6.3). A demo can be free of errors and still be boring
  # or wrong; these are the checks that say the STORY is present.
  # =========================================================================
  ash_step "asserting the shape of the seeded window"
  seed_assert_shape

  # =========================================================================
  # The frozen-window contract (§2.2) — written LAST, on purpose. Both capture
  # paths refuse to run without it, and refuse to run if it is older than the
  # newest row in ash.sample.
  # =========================================================================
  seed_write_window_env

  ash_step "seed complete in $(( ($(ash_now_ms) - t0) / 1000 )).$(( (($(ash_now_ms) - t0) / 100) % 10 ))s"
}

# ---------------------------------------------------------------------------
# seed_assert_shape — run lib/shape.sql with the window literals bound.
# ---------------------------------------------------------------------------
seed_assert_shape() {
  psql -X -v ON_ERROR_STOP=1 -d "$ASH_DEMO_DB" -q \
    -v base_ts="$ASH_BASE_TS" \
    -v vmin="$ASH_VMIN" \
    -v vmin_slack="$ASH_VMIN_SLACK" \
    -v vmin_total="$ASH_VMIN_TOTAL" \
    -v ph_baseline="$ASH_PH_BASELINE" \
    -v ph_storm="$ASH_PH_STORM" \
    -f "$ASH_SEED_LIB_DIR/shape.sql" \
    || ash_die 4 "seed shape assertions failed — the window does not tell the story"
}

# ---------------------------------------------------------------------------
# seed_write_window_env — emit out/window.env
# ---------------------------------------------------------------------------
#
# Every literal is produced by PostgreSQL from the same base timestamp the
# seeder used, formatted with an explicit UTC offset so it round-trips into a
# psql query without depending on the reader's TimeZone. No script may call
# now() in scene SQL. Ever.
seed_write_window_env() {
  local storm_event
  # The dominant wait event of the storm window, measured rather than assumed.
  # Exported so scenes.tsv can drill on it by name and so the marker assertions
  # test the real thing.
  storm_event=$(ash_psql1 "
    select top_row.key
    from ash.top('wait_event',
                 ash.ts_to_timestamptz($(( ASH_BASE_TS )) + $(( ASH_PH_BASELINE * 60 ))),
                 ash.ts_to_timestamptz($(( ASH_BASE_TS )) + $(( (ASH_PH_BASELINE + ASH_PH_STORM) * 60 ))),
                 n => 1) as top_row")

  ash_psql -tA -F '' -o "$ASH_WINDOW_ENV" -c "
    with bounds as (
      select
        ash.ts_to_timestamptz($ASH_BASE_TS + $(( ASH_VMIN_SLACK * 60 )))                                  as since,
        ash.ts_to_timestamptz($ASH_BASE_TS + $(( ASH_VMIN_TOTAL * 60 )))                                  as until_ts,
        ash.ts_to_timestamptz($ASH_BASE_TS + $(( ASH_VMIN_SLACK * 60 )))                                  as base_since,
        ash.ts_to_timestamptz($ASH_BASE_TS + $(( ASH_PH_BASELINE * 60 )))                                 as base_until,
        ash.ts_to_timestamptz($ASH_BASE_TS + $(( ASH_PH_BASELINE * 60 )))                                 as storm_since,
        ash.ts_to_timestamptz($ASH_BASE_TS + $(( (ASH_PH_BASELINE + ASH_PH_STORM) * 60 )))                as storm_until
    )
    select
      '# demos/out/window.env — the frozen incident window.'                                        || e'\n' ||
      '# Written by demos/lib/seed.sh. Sourced by BOTH capture paths.'                              || e'\n' ||
      '# Every scene SQL uses these literals; nothing in demos/ may call now().' || e'\n' ||
      'ASH_SINCE='        || quote_literal(to_char(bounds.since,       'YYYY-MM-DD HH24:MI:SSOF')) || e'\n' ||
      'ASH_UNTIL='        || quote_literal(to_char(bounds.until_ts,    'YYYY-MM-DD HH24:MI:SSOF')) || e'\n' ||
      'ASH_BASE_SINCE='   || quote_literal(to_char(bounds.base_since,  'YYYY-MM-DD HH24:MI:SSOF')) || e'\n' ||
      'ASH_BASE_UNTIL='   || quote_literal(to_char(bounds.base_until,  'YYYY-MM-DD HH24:MI:SSOF')) || e'\n' ||
      'ASH_STORM_SINCE='  || quote_literal(to_char(bounds.storm_since, 'YYYY-MM-DD HH24:MI:SSOF')) || e'\n' ||
      'ASH_STORM_UNTIL='  || quote_literal(to_char(bounds.storm_until, 'YYYY-MM-DD HH24:MI:SSOF')) || e'\n' ||
      'ASH_STORM_EVENT='  || quote_literal('$storm_event')                                         || e'\n' ||
      'ASH_STORM_QUERY_ID=' || quote_literal((
          select top_row.key
          from ash.top('query_id', bounds.storm_since, bounds.storm_until,
                       wait_event => '$storm_event', n => 1) as top_row))       || e'\n' ||
      'ASH_COMPRESSION='  || quote_literal(
          case when '${ASH_REAL_TIME:-0}' = '1' then 'real time (no compression)'
               else '1 real second = 1 virtual minute' end)                     || e'\n' ||
      'ASH_SEED_EPOCH='   || quote_literal(extract(epoch from now())::bigint::text) || e'\n' ||
      'ASH_SEED_MAX_TS='  || quote_literal((select max(sample_ts) from ash.sample)::text) || e'\n' ||
      'ASH_PG_VERSION='   || quote_literal(current_setting('server_version'))    || e'\n' ||
      'ASH_ASH_VERSION='  || quote_literal((select config.version from ash.config as config)) || e'\n' ||
      'ASH_DEMO_DB_NAME=' || quote_literal(current_database())
    from bounds"

  ash_log "wrote $ASH_WINDOW_ENV"
}

# ---------------------------------------------------------------------------
# window_env_load — used by the capture paths (and by `ash-demo check`).
# ---------------------------------------------------------------------------
#
# Exit 3 when the file is missing or stale. "Stale" means: the newest row in
# ash.sample is newer than the seed recorded, i.e. somebody re-seeded or the
# sampler ran again and the literals no longer describe the data.
window_env_load() {
  [ -f "$ASH_WINDOW_ENV" ] \
    || ash_die 3 "missing $ASH_WINDOW_ENV — run 'make seed' first"
  # shellcheck disable=SC1090
  . "$ASH_WINDOW_ENV"
  export ASH_SINCE ASH_UNTIL ASH_BASE_SINCE ASH_BASE_UNTIL \
         ASH_STORM_SINCE ASH_STORM_UNTIL ASH_STORM_EVENT ASH_STORM_QUERY_ID \
         ASH_COMPRESSION ASH_SEED_EPOCH ASH_PG_VERSION ASH_ASH_VERSION
}

# window_env_check_fresh — the database half of the freshness contract.
# Separated from window_env_load so a no-database consumer (make check) can
# read the literals without needing a server.
window_env_check_fresh() {
  local live_max
  live_max=$(ash_psql1 "select coalesce(max(sample_ts), -1) from ash.sample") \
    || ash_die 3 "cannot reach the demo database to validate $ASH_WINDOW_ENV"
  if [ "$live_max" != "${ASH_SEED_MAX_TS:-}" ]; then
    ash_die 3 "$ASH_WINDOW_ENV is stale: it was written for max(sample_ts)=${ASH_SEED_MAX_TS:-none} but the database holds $live_max — re-run 'make seed'"
  fi
}

# Executed directly (bin/ash-demo seed) rather than sourced?
if [ "${BASH_SOURCE[0]}" = "${0}" ]; then
  backend_up
  seed_main
fi
