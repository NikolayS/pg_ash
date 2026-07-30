#!/usr/bin/env bash
# driver.sh — the tmux terminal driver for the animation capture path.
#
# What this file buys: every scene advances when psql is ACTUALLY DONE, not
# when a guessed `sleep` expires. The old demos/record.sh choreographed the
# whole investigation with `sleep 3.8` / `sleep 4.6`; a query slower than the
# guess captured half-drawn output, a query faster than the guess captured dead
# air, and neither failed the build. Here the only surviving sleeps are
# drv_hold() calls — deliberate "let the viewer read this" beats, named as
# such, never used for synchronisation.
#
# ---------------------------------------------------------------------------
# How the synchronisation works
# ---------------------------------------------------------------------------
# lib/psqlrc.demo makes PROMPT1 emit a non-printing OSC-2 set-title escape whose
# payload is `$$` of the shell psql popen()s per prompt expansion — a fresh
# nonce every prompt. tmux parses OSC 2 into #{pane_title}. We poll that with
# `tmux display-message -p` at 20 ms; it is O(1), unlike capture-pane, which
# reads the whole scrollback and must never appear in a hot loop.
#
# The escape rides the SAME pty byte stream as the query output, so tmux cannot
# possibly see the new title before it has consumed every byte that preceded it.
# Observing the flip is proof of completion, not a heuristic. PROMPT2 emits a
# different nonce, so a scene that lost its semicolon fails in ~30 ms with
# exit 7 rather than hanging for the full timeout.
#
# ---------------------------------------------------------------------------
# Public API (spec §7)
# ---------------------------------------------------------------------------
#   drv_start <session> <cols> <rows> <command>   launch the recorded pane
#   drv_wait_prompt <prev-title> [timeout_ms]     0 PROMPT1 | 2 PROMPT2 | 1 timeout
#   drv_type <text>                               human-paced literal typing
#   drv_run <scene_name> [sql] [markers] [hold]   type -> sync -> verify -> shot -> hold
#   drv_hold <sec>                                a dramatic beat
#   drv_shot <path.ansi>                          tmux capture-pane -e -p
#   drv_die <msg>                                 red message, exit 7
#
# drv_run without explicit arguments reads DRV_SQL / DRV_MARKERS / DRV_HOLD,
# which is how bin/record-demo.sh passes a scene row through without needing
# associative arrays (bash 3.2 on macOS has none).
#
# Portability: bash 3.2 and bash 5. No `date +%s%N` (BSD date has no %N — it
# prints a literal "N" and every arithmetic comparison silently misbehaves);
# all millisecond timing goes through python3. No sed anywhere.

if [ -n "${ASH_DRIVER_SH_LOADED:-}" ]; then return 0 2>/dev/null || true; fi
ASH_DRIVER_SH_LOADED=1

DRV_TIMEOUT_MS="${DRV_TIMEOUT_MS:-60000}"
DRV_POLL="${DRV_POLL:-0.02}"

# A private tmux server is the structural guard that makes this driver safe
# when its caller is already inside tmux. Even a malformed target can only
# address this run's private socket, never the caller's server. The session
# still has its own unique name and ownership token: the private socket narrows
# the blast radius, while the identity checks prove what we are killing.
DRV_TMUX_SOCKET="pg-ash-demo-$$-${RANDOM}-${RANDOM}"
DRV_SESSION=""
DRV_SESSION_ID=""
DRV_SESSION_OWNER=""
DRV_SESSION_CREATED=0
DRV_SESSION_SEQUENCE=0

# Human-typing pacing (ms/char). The bursty-not-metronomic feel is the single
# best thing about the original record.sh and is preserved; the absolute values
# are faster, because the 2.0 scene SQL is much longer than the v1.x one-liners
# and at the old pacing a single statement cost twelve seconds of reel. This is
# a brisk touch-typist (~40 cps) rather than a leisurely one (~18 cps).
DRV_TYPE_MIN_MS="${DRV_TYPE_MIN_MS:-9}"
DRV_TYPE_MAX_MS="${DRV_TYPE_MAX_MS:-34}"
DRV_TYPE_PUNCT_MS="${DRV_TYPE_PUNCT_MS:-55}"    # clause-boundary breath

# Where per-scene verification snapshots go. VERIFY ONLY — never rendered into
# an asset. Rendering the tmux pane into a still is how a prototype shipped
# stills with scrollback from the previous scene bleeding in at the top.
DRV_SHOT_DIR="${DRV_SHOT_DIR:-${ASH_OUT:-.}/shots}"

# ---------------------------------------------------------------------------
# tiny helpers
# ---------------------------------------------------------------------------
drv_now_ms() { python3 -c 'import time; print(int(time.time()*1000))'; }
drv_tmux()   { TMUX= TMUX_PANE= tmux -L "$DRV_TMUX_SOCKET" "$@"; }
drv_title()  {
  [ "$DRV_SESSION_CREATED" = "1" ] || { printf ''; return 0; }
  drv_tmux display-message -p -t "$DRV_SESSION_ID" \
    '#{pane_title}' 2>/dev/null || printf ''
}
drv_log()    { printf '\033[38;2;98;114;164m[driver]\033[0m %s\n' "$*" >&2; }

# drv_die <msg> — exit 7 (animation sync failure) in the theme's Lock red so a
# failure is impossible to miss in a scrolling CI log.
drv_die() {
  printf '\033[38;2;255;85;85m[driver] FATAL:\033[0m %s\n' "$*" >&2
  exit 7
}

drv_identity() {
  drv_tmux display-message -p -t "$DRV_SESSION_ID" \
    '#{session_name}:#{session_id}:#{@pg_ash_driver_owner}' 2>/dev/null
}

drv_alive() {
  local identity
  [ "$DRV_SESSION_CREATED" = "1" ] || return 1
  identity="$(drv_identity)" || return 1
  [ "$identity" = "$DRV_SESSION:$DRV_SESSION_ID:$DRV_SESSION_OWNER" ]
}

# Roll back the narrow interval after new-session returned an ID but before its
# ownership option was stamped. The unique name plus captured ID on this run's
# private socket are the provisional proof; never target it if either changed.
drv_rollback_provisional() {
  local identity
  if [ -z "$DRV_TMUX_SOCKET" ] || [ -z "$DRV_SESSION" ] \
     || [ -z "$DRV_SESSION_ID" ]; then
    drv_log "FATAL: refusing provisional cleanup with an incomplete identity"
    return 7
  fi
  if ! drv_tmux has-session -t "$DRV_SESSION_ID" 2>/dev/null; then
    DRV_SESSION_CREATED=0
    return 0
  fi
  identity="$(drv_tmux display-message -p -t "$DRV_SESSION_ID" \
    '#{session_name}:#{session_id}' 2>/dev/null)" || {
    drv_log "FATAL: cannot read provisional driver identity"
    return 7
  }
  if [ "$identity" != "$DRV_SESSION:$DRV_SESSION_ID" ]; then
    drv_log "FATAL: refusing provisional cleanup: driver identity changed"
    return 7
  fi
  if ! drv_tmux kill-session -t "$DRV_SESSION_ID" 2>/dev/null; then
    drv_log "FATAL: could not roll back provisional driver $DRV_SESSION_ID"
    return 7
  fi
  DRV_SESSION_CREATED=0
}

# ---------------------------------------------------------------------------
# drv_start <session-prefix> <cols> <rows> <command>
# Detached tmux session with an EXACT geometry, running <command>. The geometry
# must be pinned here: tmux otherwise inherits the controlling terminal's size
# and the 100-column budget silently becomes 80 in CI.
# ---------------------------------------------------------------------------
drv_start() {
  local prefix="$1" cols="$2" rows="$3" cmd="$4"
  local attempt candidate session_id

  [ "$DRV_SESSION_CREATED" = "0" ] \
    || drv_die "refusing to start a second driver session before cleanup"
  case "$prefix" in
    "") drv_die "driver session prefix is empty" ;;
    *[!A-Za-z0-9_-]*)
      drv_die "driver session prefix has unsafe characters: $prefix" ;;
  esac

  DRV_SESSION_SEQUENCE=$((DRV_SESSION_SEQUENCE + 1))
  attempt=0
  session_id=""
  while [ "$attempt" -lt 5 ]; do
    attempt=$((attempt + 1))
    candidate="$prefix-$$-$DRV_SESSION_SEQUENCE-$RANDOM"
    if session_id="$(drv_tmux new-session -d -P -F '#{session_id}' \
        -s "$candidate" -x "$cols" -y "$rows" "$cmd" 2>/dev/null)"; then
      break
    fi
    session_id=""
  done
  [ -n "$session_id" ] \
    || drv_die "could not create a unique private tmux session"

  DRV_SESSION="$candidate"
  DRV_SESSION_ID="$session_id"
  DRV_SESSION_OWNER="pg-ash-demo-$$-${RANDOM}-${RANDOM}"
  DRV_SESSION_CREATED=1
  if ! drv_tmux set-option -t "$DRV_SESSION_ID" \
      @pg_ash_driver_owner "$DRV_SESSION_OWNER" 2>/dev/null; then
    drv_rollback_provisional \
      || drv_die "could not safely roll back driver session $DRV_SESSION"
    drv_die "could not stamp ownership on driver session $DRV_SESSION"
  fi
  # Do not let tmux rewrite the pane title from anything but OSC 2.
  drv_tmux set-option -t "$DRV_SESSION_ID" -p allow-rename on \
    2>/dev/null || true
  mkdir -p "$DRV_SHOT_DIR"
}

drv_kill() {
  local identity

  # The EXIT trap runs on dependency/preflight failures and in --render-only
  # mode, before drv_start. With no creation record there is nothing to clean.
  [ "$DRV_SESSION_CREATED" = "1" ] || return 0

  if [ -z "$DRV_TMUX_SOCKET" ] || [ -z "$DRV_SESSION" ] \
     || [ -z "$DRV_SESSION_ID" ] || [ -z "$DRV_SESSION_OWNER" ]; then
    drv_log "FATAL: refusing tmux cleanup with an incomplete ownership record"
    return 7
  fi

  # psql/asciinema may have exited naturally after \q. An absent private
  # session needs no kill; clear the ledger so cleanup stays idempotent.
  if ! drv_tmux has-session -t "$DRV_SESSION_ID" 2>/dev/null; then
    DRV_SESSION_CREATED=0
    return 0
  fi

  identity="$(drv_identity)" || {
    drv_log "FATAL: refusing tmux cleanup: cannot read the target identity"
    return 7
  }
  if [ "$identity" != "$DRV_SESSION:$DRV_SESSION_ID:$DRV_SESSION_OWNER" ]; then
    drv_log "FATAL: refusing tmux cleanup: ownership identity changed"
    return 7
  fi
  if ! drv_tmux kill-session -t "$DRV_SESSION_ID" 2>/dev/null; then
    drv_log "FATAL: could not kill owned driver session $DRV_SESSION_ID"
    return 7
  fi
  DRV_SESSION_CREATED=0
}

# ---------------------------------------------------------------------------
# drv_wait_prompt <previous-title> [timeout-ms]
#   prints the new title on stdout
#   returns 0 = PROMPT1 (statement complete)
#           2 = PROMPT2/PROMPT3 (continuation — unterminated statement)
#           1 = timeout
# The caller decides whether 2 is a bug (end of a statement) or expected (a
# deliberate mid-statement line break); this function only reports.
# ---------------------------------------------------------------------------
drv_wait_prompt() {
  local prev="$1" limit_ms="${2:-$DRV_TIMEOUT_MS}" t end
  end=$(( $(drv_now_ms) + limit_ms ))
  while :; do
    t="$(drv_title)"
    if [ -n "$t" ] && [ "$t" != "$prev" ]; then
      case "$t" in
        ashrdy-*)  printf '%s' "$t"; return 0 ;;
        ashcont-*) printf '%s' "$t"; return 2 ;;
      esac
    fi
    if [ "$(drv_now_ms)" -ge "$end" ]; then return 1; fi
    # If the pane died (psql crashed, container vanished) stop waiting for a
    # prompt that can never arrive.
    drv_alive || return 1
    sleep "$DRV_POLL"
  done
}

# ---------------------------------------------------------------------------
# drv_type <text> — send <text> one character at a time with jittered delays.
#
# Two tmux quirks are handled:
#   * `-l` is literal mode; required for `;`, `(`, `$`, `:` and friends, which
#     tmux would otherwise parse as command language.
#   * tmux drops a TRAILING `;` from `-l` even in literal mode, because `;` is
#     its command separator. Sending one byte per call makes EVERY solo `;`
#     trailing, so we smuggle a space along with it. A trailing space is a no-op
#     everywhere in psql syntax. This workaround is inherited from the original
#     record.sh and is still load-bearing.
# ---------------------------------------------------------------------------
drv_type() {
  local text="$1" i ch ms send span n
  span=$(( DRV_TYPE_MAX_MS - DRV_TYPE_MIN_MS ))
  n=${#text}
  for (( i=0; i<n; i++ )); do
    ch="${text:$i:1}"
    if [ "$span" -gt 0 ]; then ms=$(( DRV_TYPE_MIN_MS + RANDOM % (span + 1) ))
    else                       ms=$DRV_TYPE_MIN_MS; fi
    case "$ch" in
      ','|';'|'('|')') ms=$(( ms + DRV_TYPE_PUNCT_MS )) ;;
      ' ')             ms=$(( DRV_TYPE_MIN_MS + RANDOM % 18 )) ;;
    esac
    send="$ch"
    [ "$ch" = ";" ] && send="; "
    drv_tmux send-keys -t "$DRV_SESSION_ID" -l -- "$send"
    sleep "$(printf '0.%03d' "$ms")"
  done
}

drv_enter() { drv_tmux send-keys -t "$DRV_SESSION_ID" Enter; }

# ---------------------------------------------------------------------------
# drv_wrap <text> <width> — split SQL into typed display lines.
#
# Prints one line per output record, separated by \x1f. Breaks at ", " clause
# boundaries so a wrapped statement still reads like SQL someone typed, and
# never mid-token. Continuation lines carry three leading spaces which, added
# to psqlrc.demo's three-space PROMPT2, land the SQL at column 6 — exactly under
# the SQL on the "ash > " line. (Spec §3.4: 6-space hanging indent.)
# ---------------------------------------------------------------------------
drv_wrap() {
  ASH_WRAP_TEXT="$1" ASH_WRAP_W="$2" python3 - <<'PY'
import os, sys
text = os.environ['ASH_WRAP_TEXT']
width = int(os.environ['ASH_WRAP_W'])
PROMPT = 6      # "ash > " occupies six columns
INDENT = '   '  # + PROMPT2's three spaces == the six-column hanging indent

# Split into atoms that we are willing to break BETWEEN, never inside.
atoms, buf = [], ''
for ch in text:
    buf += ch
    if ch == ',':
        atoms.append(buf); buf = ''
if buf:
    atoms.append(buf)

lines, cur, budget = [], '', width - PROMPT
for a in atoms:
    piece = a if not cur else a
    if cur and len(cur) + len(piece) > budget:
        lines.append(cur.rstrip())
        cur = piece.lstrip()
        budget = width - PROMPT - len(INDENT)
    else:
        cur += piece
if cur:
    lines.append(cur.rstrip())

out = [lines[0]] + [INDENT + l for l in lines[1:]]
sys.stdout.write('\x1f'.join(out))
PY
}

# ---------------------------------------------------------------------------
# drv_submit <sql> — type <sql>, wrapped, and wait for the prompt to return.
#
# Intermediate line breaks are EXPECTED to land on PROMPT2 and are synchronised
# on the ashcont- nonce (still no blind sleeps). Only the final line must land
# on PROMPT1; a PROMPT2 there means the statement lost its terminator, and that
# fails in ~30 ms instead of hanging for the timeout.
# ---------------------------------------------------------------------------
drv_submit() {
  local sql="$1" cols="${2:-${ASH_COLS:-100}}"
  local wrapped prev rc newt line last_ix ix
  wrapped="$(drv_wrap "$sql" "$cols")"

  # bash 3.2: no mapfile. Split on \x1f with IFS.
  local OLDIFS="$IFS"; IFS=$'\037'
  # shellcheck disable=SC2206
  local parts=( $wrapped )
  IFS="$OLDIFS"

  # DRV_TERMINATOR: an optional final line typed after the statement body, used
  # for the colour scenes. psql's ALIGNED formatter escapes 0x1B into the FOUR
  # CHARACTER text \x1B and then measures column widths from the escaped text —
  # the flagship chart frame came out as 100-column garbage with visible
  # "\x1B[38;2;..." runs and a wrapped box. `\g (format=unaligned ...)` hands the
  # rows to the terminal with the real escape bytes intact and psql's aligner
  # never sees them. This is the animation-path half of the fix; the stills path
  # gets the same result from `psql -A`. Neither uses sed, which is where the
  # old record.sh had its latent BSD/GNU divergence.
  if [ -n "${DRV_TERMINATOR:-}" ]; then
    parts[${#parts[@]}]="   $DRV_TERMINATOR"
  fi
  last_ix=$(( ${#parts[@]} - 1 ))

  for (( ix=0; ix<=last_ix; ix++ )); do
    line="${parts[$ix]}"
    prev="$(drv_title)"
    drv_type "$line"
    drv_enter
    set +e; newt="$(drv_wait_prompt "$prev")"; rc=$?; set -e
    if [ "$ix" -lt "$last_ix" ]; then
      # Mid-statement: PROMPT2 is what we want. PROMPT1 here would mean psql
      # executed a fragment — a wrap bug, not a scene bug, but still fatal.
      case $rc in
        1) drv_die "timeout waiting for the continuation prompt: $line" ;;
        0) drv_die "psql executed a wrapped fragment (bad line break): $line" ;;
      esac
    else
      case $rc in
        1) drv_die "timeout after ${DRV_TIMEOUT_MS}ms waiting for the prompt: $sql" ;;
        2) drv_die "psql fell to the continuation prompt — unterminated statement: $sql" ;;
      esac
    fi
  done
}

# ---------------------------------------------------------------------------
# drv_shot <path.ansi> — truecolor snapshot of the VISIBLE pane.
#
# No `-S`: we want the screen as a viewer sees it, not the scrollback. This file
# is verification input only and is never rendered into a committed asset.
# ---------------------------------------------------------------------------
drv_shot() {
  mkdir -p "$(dirname "$1")"
  drv_tmux capture-pane -e -p -t "$DRV_SESSION_ID" > "$1"
}

# ---------------------------------------------------------------------------
# drv_hold <sec> — the ONLY surviving sleep. A dramatic beat, never a sync.
# ---------------------------------------------------------------------------
drv_hold() { sleep "$1"; }

# ---------------------------------------------------------------------------
# drv_run <scene_name> [sql] [markers] [hold]
# type -> sync -> verify (lib/verify.sh) -> snapshot -> hold.
# Falls back to DRV_SQL / DRV_MARKERS / DRV_HOLD when the extra arguments are
# omitted, so a caller can loop over a scene table without associative arrays.
# ---------------------------------------------------------------------------
drv_run() {
  local name="$1"
  local sql="${2:-${DRV_SQL:-}}"
  local markers="${3:-${DRV_MARKERS:-}}"
  local hold="${4:-${DRV_HOLD:-2.0}}"
  local terminator="${5:-${DRV_TERMINATOR:-}}"
  local shot="$DRV_SHOT_DIR/$name.ansi"
  local t0 t1

  [ -n "$sql" ] || drv_die "scene '$name' has no SQL"

  t0="$(drv_now_ms)"
  DRV_TERMINATOR="$terminator" drv_submit "$sql" "${ASH_COLS:-100}"
  t1="$(drv_now_ms)"

  # Snapshot BEFORE the hold: the hold is dead time on the pane, and taking the
  # shot first means a verification failure is reported without waiting it out.
  drv_shot "$shot"

  # The four shared assertions, identical to the ones the stills path runs.
  # vfy_* exit 5 on failure; an animation-specific failure (sync) is exit 7.
  vfy_scene "$shot" "$name" "$markers" "${ASH_COLS:-100}"

  drv_log "ok $name  $(( t1 - t0 ))ms  hold ${hold}s"
  drv_hold "$hold"
}

# ---------------------------------------------------------------------------
# drv_note <text> [hold] — a narration beat between scenes.
#
# Typed as a SQL COMMENT, not `\echo`. Both work, but `\echo` prints the
# command line AND its output, so every beat costs two lines of the frame and
# reads like a stutter ("ash > \echo -- Q3: ..." / "-- Q3: ..."). A `--` line
# leaves psql's buffer empty, prints one clean line, and still emits a fresh
# PROMPT1 nonce — so it is prompt-synchronised exactly like a query.
#
# Sent whole rather than typed character by character. Two reasons, one
# aesthetic and one hard: the human-typing flourish should be spent on the SQL,
# which is the thing a viewer reads and copies; and every typed character is a
# distinct terminal state and therefore a GIF frame — the narration and closing
# lines are ~450 characters, about a third of the reel's frames, spent on prose.
# Set DRV_TYPE_NARRATION=1 to type them out anyway.
# ---------------------------------------------------------------------------
drv_note() {
  local text="$1" hold="${2:-0.8}"
  local prev rc
  prev="$(drv_title)"
  if [ "${DRV_TYPE_NARRATION:-0}" = "1" ]; then
    drv_type "$text"
  else
    # Trailing space: tmux eats a terminal ';' even in literal mode, and a
    # trailing space is a no-op inside a comment.
    drv_tmux send-keys -t "$DRV_SESSION_ID" -l -- "$text "
  fi
  drv_enter
  set +e; drv_wait_prompt "$prev" 15000 >/dev/null; rc=$?; set -e
  [ "$rc" -eq 0 ] || drv_die "narration did not return to the prompt: $text"
  drv_hold "$hold"
}

# Backwards-compatible alias.
drv_echo() { drv_note "$@"; }
