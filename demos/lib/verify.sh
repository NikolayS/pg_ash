#!/usr/bin/env bash
# verify.sh — the shared assertion vocabulary, used by BOTH capture paths.
#
# bin/capture-stills.sh (scripted) and bin/record-demo.sh (interactive) call the
# same four functions on the same bytes, so "the demo is correct" means the same
# thing in a still and in the reel. These four names are an interface contract
# (spec §12); do not rename them.
#
#   vfy_nonempty  <file> <scene>              capture has content at all
#   vfy_no_errors <file> <scene>              no ERROR:/FATAL:/PANIC: anywhere
#   vfy_markers   <file> <scene> <markers>    every comma-separated literal present
#   vfy_width     <file> <scene> [cols]       every line fits the column budget
#
# All four exit 5 on failure (spec §2.6: capture verification failure) after
# printing the scene, what was expected, and enough of the capture to debug.
#
# Two things this file is deliberately careful about:
#
#   1. `grep -q` behind a pipe is BANNED repo-wide. A successful match makes
#      grep exit immediately, SIGPIPEs the upstream writer, and under
#      `set -o pipefail` the pipeline reports failure — i.e. a MATCH looks like
#      a MISS. This inverted 12 results in a prototype. We use `grep -c` with a
#      redirect, or `case`, never `grep -q` downstream of `|`.
#
#   2. Display width is measured in Python with East-Asian-width awareness and
#      SGR/OSC counted as zero. `awk length` counts UTF-8 BYTES and reported 393
#      for a 131-column table — it cannot see that █ is three bytes and one
#      column. Never measure width in awk.
#
# Portability: bash 3.2 (macOS default) and bash 5. No associative arrays, no
# mapfile, no GNU-only flags.

# Guard against double-sourcing (both capture paths may pull this in).
if [ -n "${ASH_VERIFY_SH_LOADED:-}" ]; then return 0 2>/dev/null || true; fi
ASH_VERIFY_SH_LOADED=1

VFY_ERR_RE='(ERROR|FATAL|PANIC):'

vfy__say()  { printf '\033[38;2;127;148;155m[verify]\033[0m %s\n' "$*" >&2; }
vfy__fail() {
  # vfy__fail <scene> <message>
  printf '\033[38;2;255;85;85m[verify] FAIL %s:\033[0m %s\n' "$1" "$2" >&2
  exit 5
}

# ---------------------------------------------------------------------------
# vfy_nonempty <file> <scene>
# An empty capture is the single easiest way to ship a beautiful picture of
# nothing. Treat "file exists" and "file has bytes" as different facts.
# ---------------------------------------------------------------------------
vfy_nonempty() {
  local file="$1" scene="$2"
  [ -f "$file" ] || vfy__fail "$scene" "capture file missing: $file"
  [ -s "$file" ] || vfy__fail "$scene" "capture is empty: $file"
  # A file of nothing but whitespace is empty in every sense that matters.
  local visible
  visible="$(tr -d ' \t\r\n' < "$file" | wc -c | tr -d ' ')"
  [ "$visible" -gt 0 ] || vfy__fail "$scene" "capture is whitespace only: $file"
}

# ---------------------------------------------------------------------------
# vfy_no_errors <file> <scene>
# psql writes ERROR:/FATAL:/PANIC: to stderr. The still path merges stderr into
# the capture (2>&1); the animation path tees the pty through to
# out/stderr.log AND scans the pane. `psql -L` does NOT log errors, so that tee
# is load-bearing, not belt-and-braces.
# ---------------------------------------------------------------------------
vfy_no_errors() {
  local file="$1" scene="$2" n
  # grep -c on a FILE (no upstream pipe) — no SIGPIPE hazard, and -c never
  # exits non-zero in a way that trips `set -e` because we swallow it.
  n="$(grep -Ec "$VFY_ERR_RE" "$file" 2>/dev/null || true)"
  [ -z "$n" ] && n=0
  if [ "$n" -gt 0 ]; then
    vfy__say "offending lines in $file:"
    grep -En "$VFY_ERR_RE" "$file" >&2 || true
    vfy__fail "$scene" "$n line(s) matched $VFY_ERR_RE"
  fi
}

# ---------------------------------------------------------------------------
# vfy_markers <file> <scene> <comma,separated,literals>
# This is the assertion that distinguishes "the query ran" from "the query
# showed the incident". Without it an empty result set ships as a pretty
# picture of nothing and CI stays green.
#
# Markers are LITERAL substrings, not regexes — a scene author should not have
# to escape `Lock:transactionid` or `█`. Matching is done in Python so UTF-8
# markers work identically on BSD and GNU userlands.
# ---------------------------------------------------------------------------
vfy_markers() {
  local file="$1" scene="$2" markers="$3"
  [ -n "$markers" ] || return 0
  local missing
  missing="$(ASH_VFY_FILE="$file" ASH_VFY_MARKERS="$markers" python3 - <<'PY'
import os, sys
data = open(os.environ['ASH_VFY_FILE'], 'rb').read().decode('utf-8', 'replace')
missing = [m for m in (x.strip() for x in os.environ['ASH_VFY_MARKERS'].split(','))
           if m and m not in data]
sys.stdout.write('\x1f'.join(missing))
PY
)"
  if [ -n "$missing" ]; then
    vfy__say "capture head:"
    head -20 "$file" >&2 || true
    vfy__fail "$scene" "marker(s) absent: $(printf '%s' "$missing" | tr '\037' ' ')"
  fi
}

# ---------------------------------------------------------------------------
# vfy_width <file> <scene> [cols]
# Hard gate on BOTH paths (spec §2.5). ash.chart(color => true) rendered 238
# columns in a prototype and wrapped silently in the flagship frame; that class
# of defect must be impossible to ship, so this runs in preflight BEFORE any
# renderer or recorder starts.
#
# Zero-width: CSI sequences (SGR colour), OSC sequences (our prompt sentinel),
# and combining marks. Double-width: East-Asian W/F. U+2588 and friends are
# 'N' (narrow) — one column each, three bytes each, which is exactly the
# distinction awk cannot make.
# ---------------------------------------------------------------------------
vfy_width() {
  local file="$1" scene="$2" cols="${3:-${ASH_COLS:-100}}"
  local bad
  bad="$(ASH_VFY_FILE="$file" ASH_VFY_COLS="$cols" python3 - <<'PY'
import os, re, sys, unicodedata

# CSI (colour), OSC (title sentinel, ST- or BEL-terminated), charset selects,
# and any other lone two-byte escape. All zero display width.
STRIP = re.compile(
    r'\x1b\][^\x07\x1b]*(?:\x07|\x1b\\)'   # OSC ... BEL | ST
    r'|\x1b\[[0-9;:?<>=]*[@-~]'            # CSI ... final
    r'|\x1b[()][A-Za-z0-9]'                # charset designators
    r'|\x1b[@-Z\\-_]'                      # other 2-byte escapes
)

def width(s):
    s = STRIP.sub('', s).replace('\r', '')
    n = 0
    for ch in s:
        if unicodedata.combining(ch):
            continue
        n += 2 if unicodedata.east_asian_width(ch) in ('W', 'F') else 1
    return n

limit = int(os.environ['ASH_VFY_COLS'])
raw = open(os.environ['ASH_VFY_FILE'], 'rb').read().decode('utf-8', 'replace')
out = []
for i, line in enumerate(raw.split('\n'), 1):
    w = width(line.rstrip())
    if w > limit:
        out.append('line %d: %d columns (limit %d)' % (i, w, limit))
sys.stdout.write('\x1f'.join(out[:5]))
PY
)"
  if [ -n "$bad" ]; then
    vfy__say "width overflow in $file (ASH_COLS=$cols):"
    printf '%s\n' "$bad" | tr '\037' '\n' >&2
    vfy__fail "$scene" "capture exceeds the $cols-column budget"
  fi
}

# ---------------------------------------------------------------------------
# vfy_scene <file> <scene> <markers> [cols] — all four, in the order that gives
# the most useful first failure (empty -> errors -> markers -> width).
# ---------------------------------------------------------------------------
vfy_scene() {
  local file="$1" scene="$2" markers="$3" cols="${4:-${ASH_COLS:-100}}"
  vfy_nonempty  "$file" "$scene"
  vfy_no_errors "$file" "$scene"
  vfy_markers   "$file" "$scene" "$markers"
  vfy_width     "$file" "$scene" "$cols"
}
