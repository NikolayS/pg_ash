#!/usr/bin/env bash
# record.sh — End-to-end demo recorder for pg_ash.
#
# Pipeline (all local, no cloud):
#   1. Start postgres:18 in Docker with pg_cron + pg_stat_statements.
#   2. Install pg_ash via `\i sql/ash-install.sql`, seed pgbench, start sampling.
#   3. Kick off a workload that transitions baseline -> row-lock spike -> tail.
#   4. After the spike is well underway, start tmux + asciinema with psql
#      attached to the demo DB in the container.
#   5. Drive the tmux pane via per-character keystrokes (human-paced typing)
#      to walk the investigation.
#   6. Convert the .cast -> .gif via agg.
#
# Output: demos/ash_demo.cast, demos/ash_demo.gif
# Run:    ./demos/record.sh
# Clean:  ./demos/record.sh clean

set -Eeuo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
REPO="$(cd "$HERE/.." && pwd)"
CONTAINER="pg_ash_demo"
IMAGE="postgres:18"
SESSION="ash-demo"
CAST="$HERE/ash_demo.cast"
GIF="$HERE/ash_demo.gif"

# Terminal geometry: wider (140 cols) so wide wait-event names like
# `Lock:transactionid` and `Client:ClientRead` plus the colored bar charts
# fit on one line. Compensated by smaller font-size in agg below so the
# rendered GIF stays readable at GitHub's ~800px embed width.
COLS="${COLS:-140}"
ROWS="${ROWS:-32}"

# agg font-size in pixels. Lower = smaller text, more columns visible.
# 12 keeps a 140-col terminal under ~1100px and still legible.
AGG_FONT_SIZE="${AGG_FONT_SIZE:-12}"

# baseline + spike accumulate before we record. Much longer than the v1.x demo
# (was 30) for two 2.0-specific reasons:
#   1. chart/timeline bucket at a 1-minute minimum (sub-minute grains are gone),
#      so the colored ash.chart needs several minutes of history for a full bar
#      chart rather than one or two bars.
#   2. the leaf drills cross the wait<->query tie, which reads RAW samples and
#      raises if the window START predates raw retention. Raw retention here is
#      data-limited (it begins at the oldest sample = when ash.start() ran), so
#      the 5-minute reader windows below require > 5 minutes of prior sampling.
# 330 s (5.5 min) leaves a ~1 min margin at the first windowed query.
WARMUP_SEC="${WARMUP_SEC:-330}"
POST_WAIT="${POST_WAIT:-3}"

# Workload phase durations passed to demos/workload.sh. The spike must outlive
# WARMUP + the ~110 s recording (~440 s) so every reader query — especially the
# closing top('query_id', p_wait_event => 'Lock:tuple') -> top('wait_event',
# p_query_id => ...) leaf near the end — still sees fresh `Lock:tuple` in its
# 5-minute window. baseline is long enough (2 min) that the baseline->spike
# transition falls inside the trailing 5-minute chart window. Override to tune.
export SPIKE_SEC="${SPIKE_SEC:-480}"
export BASELINE_SEC="${BASELINE_SEC:-120}"
export TAIL_SEC="${TAIL_SEC:-30}"

# Human-typing pacing (milliseconds per character, with jitter).
# Average ~60 cps (16ms/char) is a brisk touch-typist; we use 30-120ms range
# so the pacing feels bursty rather than metronomic.
TYPE_MIN_MS="${TYPE_MIN_MS:-30}"
TYPE_MAX_MS="${TYPE_MAX_MS:-120}"
# Extra pause after punctuation (`,`, `;`, `.`, `(`, `)`) — humans pause to
# think at clause boundaries.
TYPE_PUNCT_MS="${TYPE_PUNCT_MS:-180}"

DOCKER="${DOCKER:-docker}"

log()  { printf '\033[36m[record %s]\033[0m %s\n' "$(date +%H:%M:%S)" "$*"; }
die()  { printf '\033[31m[record %s]\033[0m %s\n' "$(date +%H:%M:%S)" "$*" >&2; exit 1; }

require() { command -v "$1" >/dev/null 2>&1 || die "required tool not found: $1"; }

cleanup() {
  local rc=$?
  tmux kill-session -t "$SESSION" 2>/dev/null || true
  if [ "${KEEP_CONTAINER:-0}" != "1" ]; then
    log "stopping container $CONTAINER"
    $DOCKER rm -f "$CONTAINER" >/dev/null 2>&1 || true
  else
    log "KEEP_CONTAINER=1 — leaving $CONTAINER running"
  fi
  return $rc
}
trap cleanup EXIT INT TERM

if [ "${1:-}" = "clean" ]; then
  tmux kill-session -t "$SESSION" 2>/dev/null || true
  $DOCKER rm -f "$CONTAINER" >/dev/null 2>&1 || true
  rm -f "$CAST" "$GIF"
  log "cleaned container, cast, gif"
  trap - EXIT
  exit 0
fi

for t in docker tmux asciinema agg; do require "$t"; done

# --- 1. Start container ------------------------------------------------------
$DOCKER rm -f "$CONTAINER" >/dev/null 2>&1 || true

log "starting $IMAGE as $CONTAINER"
$DOCKER run -d --name "$CONTAINER" \
  -e POSTGRES_PASSWORD=postgres \
  -e POSTGRES_DB=postgres \
  -v "$REPO":/repo:ro \
  "$IMAGE" \
  -c track_activity_query_size=4096 \
  -c log_min_messages=warning \
  >/dev/null

log "waiting for first-boot PostgreSQL"
until $DOCKER exec "$CONTAINER" pg_isready -U postgres -q >/dev/null 2>&1; do sleep 0.5; done

log "installing pg_cron package + configuring shared_preload_libraries"
$DOCKER exec "$CONTAINER" bash -c '
  set -e
  apt-get update -qq >/dev/null 2>&1
  apt-get install -y -qq "postgresql-${PG_MAJOR}-cron" >/dev/null 2>&1
  cat >> "${PGDATA}/postgresql.conf" <<CONF

# --- pg_ash demo ---
shared_preload_libraries = '"'"'pg_cron,pg_stat_statements'"'"'
cron.database_name = '"'"'demo'"'"'
cron.use_background_workers = on
CONF
' >/dev/null 2>&1

log "restarting container so pg_cron + pg_stat_statements load"
$DOCKER restart "$CONTAINER" >/dev/null
until $DOCKER exec "$CONTAINER" pg_isready -U postgres -q >/dev/null 2>&1; do sleep 0.5; done

# --- 2. Install pg_ash + seed + start sampling + workload --------------------
log "installing pg_ash, seeding pgbench, starting sampling, kicking off workload"
log "  workload phases: baseline=${BASELINE_SEC}s spike=${SPIKE_SEC}s tail=${TAIL_SEC}s"
$DOCKER exec -d \
  -e BASELINE_SEC="$BASELINE_SEC" \
  -e SPIKE_SEC="$SPIKE_SEC" \
  -e TAIL_SEC="$TAIL_SEC" \
  "$CONTAINER" bash /repo/demos/container-entrypoint.sh

# --- 3. Warm up --------------------------------------------------------------
log "warming up (${WARMUP_SEC}s — baseline + spike accumulate)"
sleep "$WARMUP_SEC"

# Pre-stage a psqlrc inside the container that tightens the look of each query.
# Critically:
#   - Disable the default pager. psql's default `less`-based pager intercepts
#     keystrokes from the recorder, corrupting the demo. We always want every
#     row rendered straight to stdout.
#   - Enable `ash.color = on` for the session so the one render helper that
#     colors — `ash.chart` — emits ANSI escape codes in its `chart` column.
#     (In 2.0 the data readers `top`/`timeline`/`periods` are presentation-free;
#     `ash.chart` is the only reader that emits ANSI color. `ash.summary` is a
#     render helper too but returns plain key/value text.)
#   - Define a `:color` psql variable that re-runs the previous query through
#     `sed` to convert literal `\x1B` chars (which psql's aligned formatter
#     emits in place of real escapes) back into real ESC bytes, so the
#     terminal renders the colored bars. Mirrors the README pattern but
#     skips `less -R` so the output is non-interactive (the recorder cannot
#     drive an interactive pager).
$DOCKER exec "$CONTAINER" bash -c "cat >/root/.psqlrc <<'PSQLRC'
\\set QUIET on
\\pset pager off
\\pset border 2
\\pset linestyle unicode
\\pset unicode_border_linestyle single
\\pset unicode_column_linestyle single
\\pset unicode_header_linestyle single
\\pset null ''
\\pset format aligned
\\pset tuples_only off
set ash.color = on;
\\set color '\\\\g | sed ''s/\\\\\\\\x1B/\\\\x1b/g'''
\\unset QUIET
PSQLRC
"

rm -f "$CAST"
tmux kill-session -t "$SESSION" 2>/dev/null || true

# --- 4. Record ---------------------------------------------------------------
# asciinema CLI: --command for the command to record, --cols/--rows for
# geometry; positional arg is the output cast file. These flags work with the
# asciinema v2 CLI commonly packaged on Linux hosts.
# Ship a tiny splash script into the container. It prints a coloured banner
# (that becomes the GIF's first frame / GitHub thumbnail) and then execs psql.
# Banner is sized for the wider 140-col terminal (138-char inner box).
$DOCKER exec "$CONTAINER" bash -c 'cat >/tmp/ash_splash.sh <<"SPLASH"
#!/bin/bash
printf "\033[38;2;080;250;123m"
cat <<BANNER
╔════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════╗
║                                                                                                                                        ║
║   pg_ash v2.0   —   Active Session History for Postgres                                                                                ║
║                                                                                                                                        ║
║   Pure SQL. No extension. Installs via \i on RDS / Cloud SQL / Supabase / Neon.                                                        ║
║                                                                                                                                        ║
║   Investigation: something just spiked in the last minute. Let'"'"'s find out what.                                                        ║
║                                                                                                                                        ║
╚════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════╝
BANNER
printf "\033[0m\n"
exec psql -U postgres -d demo
SPLASH
chmod +x /tmp/ash_splash.sh
'

tmux new-session -d -s "$SESSION" -x "$COLS" -y "$ROWS" \
  "asciinema rec --overwrite --idle-time-limit 3 \
     --cols ${COLS} --rows ${ROWS} \
     --env TERM,SHELL \
     --command 'docker exec -it -e TERM=xterm-256color -e LC_ALL=C.UTF-8 -e LANG=C.UTF-8 $CONTAINER /tmp/ash_splash.sh' \
     '$CAST'"
sleep 3.0

# --- Human-paced typing ------------------------------------------------------
# Send a string one character at a time with randomized inter-key delays so the
# capture looks like a real person at the keyboard. Punctuation triggers a
# slightly longer pause (clause-boundary breath). Followed by Enter.
#
# Tmux quirks handled:
#   - `-l` is literal mode: passes the byte through without command parsing.
#     Required for `;`, `(`, `$`, etc.
#   - tmux drops a TRAILING `;` from `-l` even in literal mode (it doubles as
#     the tmux command-language separator). Because we're sending one byte
#     per call, every solo `;` IS trailing — we work around it below by
#     smuggling a trailing space along with the final `;` of the line.
#
# Jitter is seeded per-char from $RANDOM for low-correlation timings.
human_type_and_send() {
  local text="$1"
  local i ch ms send_str
  local span=$(( TYPE_MAX_MS - TYPE_MIN_MS ))
  local n=${#text}
  for (( i=0; i<n; i++ )); do
    ch="${text:$i:1}"
    # Random delay in [TYPE_MIN_MS, TYPE_MAX_MS] inclusive.
    if [ "$span" -gt 0 ]; then
      ms=$(( TYPE_MIN_MS + ( RANDOM % (span + 1) ) ))
    else
      ms=$TYPE_MIN_MS
    fi
    case "$ch" in
      ',' | ';' | '.' | '(' | ')')
        # Clause boundary — humans pause to think.
        ms=$(( ms + TYPE_PUNCT_MS ))
        ;;
      ' ')
        # Words flow fast; a space is mostly a beat.
        ms=$(( TYPE_MIN_MS + ( RANDOM % 40 ) ))
        ;;
    esac
    # tmux send-keys -l drops a TRAILING `;` (its command-language separator)
    # even in literal mode. Since we send one byte per call, EVERY solo `;`
    # is trailing in its own send-keys arg — so we always smuggle a trailing
    # space along with the semicolon to keep it from being eaten. That extra
    # space is a no-op everywhere in psql syntax.
    send_str="$ch"
    if [ "$ch" = ";" ]; then
      send_str="; "
    fi
    tmux send-keys -t "$SESSION" -l -- "$send_str"
    # Convert ms -> seconds for sleep. printf handles the decimal.
    sleep "$(printf '0.%03d' "$ms")"
  done
  # Submit the line.
  tmux send-keys -t "$SESSION" Enter
}

# Reseed RANDOM from /dev/urandom for non-deterministic jitter across runs.
RANDOM=$(( $(od -An -N2 -tu2 /dev/urandom | tr -d ' ') ))

# Convenience wrapper kept for any block that wants instant submission
# (currently unused in the script body, but handy for debugging).
send_instant() {
  tmux send-keys -t "$SESSION" -l "$1 "
  tmux send-keys -t "$SESSION" Enter
}

# Act 1 — the splash is already on screen from the wrapper. Let it breathe
# ~1.5s so GitHub's still-thumbnail shows the banner, then run the first
# investigation query.
#
# NOTE: psql treats any unterminated statement as continuation; every select
# MUST end with `;`. Reader functions are invoked with `p_color => true` so
# the bar charts emit ANSI codes; the `:color` psql variable runs the query
# through `sed` so the codes survive psql's aligned formatter.
sleep 1.5

# The 2.0 reader API: seven data functions + two human render helpers. The
# investigation arc below walks the designed path — triage (periods) -> locate
# + wait-class breakdown (chart, the colored render helper) -> drill (top on a
# dimension) -> leaf (top on query_id filtered by the guilty wait event). AAS
# (average active sessions) is the single load unit everywhere.
#
# Windows are `now() - interval '5 minutes'` .. `now()`: the 2.0 minimum bucket
# is 1 minute (sub-minute grains are gone), so the timeline/chart need a
# multi-minute window to show more than one bar. The workload spike outlives
# the recording (SPIKE_SEC), so every window still sees fresh Lock:tuple.

# Act 2 — status: already sampling, version 2.0, pg_cron wired.
human_type_and_send "select metric, value from ash.status() where metric in ('version','sampling_enabled','sample_interval','samples_total','pg_cron_available');"
sleep 3.8

# Act 3 — triage: is it bad right now, and is it a spike or sustained? periods()
# is the 2.0 "start here": one row per standard trailing window, in AAS, with
# peak/p99 next to avg so a spike (peak >> avg) is legible without a 2nd call.
human_type_and_send '\echo -- Q1: triage — is it bad right now? spike or sustained?'
sleep 1.0
human_type_and_send "select period, source, minutes_with_data as mins, avg_aas, peak_aas, p99_aas from ash.periods();"
sleep 4.6

# Act 4 — locate + wait-class breakdown: the colored stacked chart (the 2.0
# render helper that replaces timeline_chart). Shows WHEN the spike landed and
# WHICH wait class dominates (Lock in red). Color via p_color => true + :color.
human_type_and_send '\echo -- Q2: when did it land, and which wait class? (colored)'
sleep 1.0
human_type_and_send "select bucket_start::time as t, aas, chart from ash.chart(now() - interval '5 minutes', now(), '1 minute', 4, 40, true) :color"
sleep 4.8

# Act 5 — drill: which wait EVENT dominates? ash.top on the wait_event
# dimension — data-only now (AAS + peak + p99 per row, no presentation column).
human_type_and_send '\echo -- Q3: which wait event is dominating?'
sleep 1.0
human_type_and_send "select key, avg_aas, peak_aas, p99_aas, pct from ash.top('wait_event', now() - interval '5 minutes', now(), p_limit => 6);"
sleep 4.6

# Act 6 — leaf: which query is stuck on Lock:tuple? The 2.0 leaf drill composes
# the wait filter into top('query_id') — one grammar for what used to be
# event_queries(). This crosses the wait<->query tie, so it reads raw samples
# (the recent window is well within raw retention). \gset captures the top
# query_id into :top_qid for the closing move.
human_type_and_send '\echo -- Q4: which query is stuck on Lock:tuple?'
sleep 1.0
human_type_and_send "select key, avg_aas, peak_aas, pct, substr(query_text,1,40) as q from ash.top('query_id', now() - interval '5 minutes', now(), p_wait_event => 'Lock:tuple', p_limit => 3);"
sleep 4.6
human_type_and_send "select key as top_qid from ash.top('query_id', now() - interval '5 minutes', now(), p_wait_event => 'Lock:tuple', p_limit => 1) \\gset"
sleep 0.5

# Act 6b — closing the loop: full wait profile of that top query — top() again,
# this time the wait_event dimension filtered by :top_qid (the 2.0 unification
# of query_waits()). Same one grammar, opposite direction.
human_type_and_send '\echo -- Q5: full wait profile of the top guilty query?'
sleep 1.0
human_type_and_send "select key, avg_aas, peak_aas, pct from ash.top('wait_event', now() - interval '5 minutes', now(), p_query_id => :top_qid, p_limit => 5);"
sleep 4.8

# Act 7 — closing lines. Held on screen for the "still" moment viewers see
# when the gif loops back around.
human_type_and_send '\echo '
sleep 0.2
human_type_and_send '\echo -- Root cause: concurrent UPDATEs on the same row.'
sleep 1.4
human_type_and_send '\echo -- pg_ash: pure SQL. No extension. No restart. Works everywhere.'
sleep 3.5

# Quit psql cleanly.
human_type_and_send '\q'
sleep 0.6

tmux kill-session -t "$SESSION" 2>/dev/null || true
sleep "$POST_WAIT"

[ -s "$CAST" ] || die "asciinema did not produce $CAST"
log "cast written: $(du -h "$CAST" | cut -f1)"

# --- 5. Shift the first cast event to t=0 ------------------------------------
# asciinema records the command banner ~70 ms after the session starts; agg
# renders frame 0 at t=0, which is before that banner, so the resulting GIF
# opens with an empty terminal. That empty frame becomes GitHub's thumbnail
# for the embedded GIF. To avoid it, we rewrite the first event's timestamp
# from ~0.07 to 0.0 so the splash IS the first frame. Subsequent event
# timings are unchanged (they are relative deltas from the previous event).
python3 - "$CAST" <<'PY'
import json, sys
path = sys.argv[1]
with open(path) as f:
    lines = f.readlines()
header = lines[0]
events = [json.loads(line) for line in lines[1:] if line.strip()]
if events:
    events[0][0] = 0.0
with open(path, 'w') as f:
    f.write(header)
    for e in events:
        f.write(json.dumps(e) + '\n')
PY

# --- 6. Render GIF -----------------------------------------------------------
log "rendering gif via agg (cols=$COLS rows=$ROWS font-size=$AGG_FONT_SIZE)"
agg \
  --font-size "$AGG_FONT_SIZE" \
  --theme monokai \
  --speed 1.0 \
  --fps-cap 15 \
  --idle-time-limit 1.5 \
  --last-frame-duration 3 \
  "$CAST" "$GIF"

log "gif (pre-optim): $(du -h "$GIF" | cut -f1)"

# --- 7. Optional optimisation pass -------------------------------------------
# gifsicle halves the file size with no visible quality loss (palette merge +
# transparency optimisation). Skip gracefully if it's not installed.
if command -v gifsicle >/dev/null 2>&1 && [ "${SKIP_GIFSICLE:-0}" != "1" ]; then
  log "optimizing with gifsicle"
  gifsicle -O3 --lossy=40 --colors 128 "$GIF" -o "$GIF.opt" && mv "$GIF.opt" "$GIF"
  log "gif (final): $(du -h "$GIF" | cut -f1)"
fi

log "done."
log "cast: $CAST"
log "gif:  $GIF"
