#!/usr/bin/env bash
# record-demo.sh — the animation capture path.
#
#   scenes/scenes.tsv + out/window.env
#     -> preflight (every scene run scripted, verified, width-gated)
#     -> tmux + psql, prompt-synchronised human typing
#     -> out/ash_demo.cast            (asciicast)
#     -> agg                          (bare terminal frames, truecolor)
#     -> ffmpeg overlay on a chrome plate
#     -> assets/ash_demo.gif + assets/ash_demo.mp4
#
# Design notes worth knowing before editing:
#
#   * Nothing here calls now(). Every window literal comes from out/window.env,
#     which the seeder froze. That is what lets `make stills` and `make demo`
#     run minutes apart and still agree on every digit on screen.
#
#   * The PREFLIGHT runs before tmux ever starts. Each scene is executed
#     scripted, then run through the same lib/verify.sh assertions the reel
#     uses. A broken scene therefore costs ~2 seconds and writes no artifact,
#     instead of costing a 90-second recording that ships a psql ERROR:.
#
#   * agg's GIF encoder preserves exact 24-bit RGB (measured: all twelve
#     docs/COLOR_SCHEME.md wait-class colours survive byte-exact). The final
#     palette pass therefore uses stats_mode=full + dither=none, and gifsicle is
#     run WITHOUT --lossy/--colors, which would quantise the palette into mud.
#     The colours are re-verified in the finished GIF at the end of this script.
#
#   * The vendored font is asserted, not assumed. A missing face makes agg fall
#     back silently — that is how a wrong-looking probe GIF got produced on this
#     machine once already.
#
# Exit codes (spec §2.6): 2 dependency, 3 backend/window, 5 capture verification,
# 6 render, 7 animation sync.

set -Eeuo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
ASH_DEMO_DIR_DEFAULT="$(cd "$HERE/.." && pwd -P)"

# shellcheck source=lib/env.sh
. "$ASH_DEMO_DIR_DEFAULT/lib/env.sh"
# shellcheck source=lib/verify.sh
. "$ASH_DEMO_DIR/lib/verify.sh"
# shellcheck source=lib/driver.sh
. "$ASH_DEMO_DIR/lib/driver.sh"

# --render-only: reuse the existing out/ash_demo.cast and redo everything from
# agg onwards. Recording costs 75 s and is the deterministic part; the render
# (theme, chrome plate, palette, size budget) is what actually gets iterated on,
# and it costs 20. Without this flag every tweak to the composite pays for a
# fresh recording, which is how people stop tweaking.
RENDER_ONLY=0
for arg in "$@"; do
  case "$arg" in
    --render-only) RENDER_ONLY=1 ;;
    -h|--help) sed -n '2,12p' "$0" | cut -c3-; exit 0 ;;
    *) printf 'record-demo: unknown option %s\n' "$arg" >&2; exit 1 ;;
  esac
done

SESSION_PREFIX="${ASH_DEMO_SESSION:-ash-demo}"
CAST="$ASH_OUT/ash_demo.cast"
TERM_GIF="$ASH_OUT/term.gif"
PLATE="$ASH_OUT/chrome_plate.png"
GIF="$ASH_ASSETS/ash_demo.gif"
MP4="$ASH_ASSETS/ash_demo.mp4"
TRANSCRIPT="$ASH_OUT/transcript.log"
STDERR_LOG="$ASH_OUT/stderr.log"
GIF_BUDGET_BYTES="${ASH_GIF_BUDGET:-716800}"   # 700 KB

cleanup() {
  local rc=$? kill_rc
  set +e
  drv_kill
  kill_rc=$?
  set -e
  [ "$rc" -ne 0 ] && return "$rc"
  return "$kill_rc"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

# ===========================================================================
# 1. Dependencies
# ===========================================================================
ash_step "record-demo: checking tier-3 dependencies"
_missing=""
for t in tmux asciinema agg ffmpeg ffprobe gifsicle psql python3; do
  ash_have "$t" || _missing="$_missing $t"
done
python3 -c 'import PIL' 2>/dev/null || _missing="$_missing python3-Pillow"
[ -z "$_missing" ] || ash_die 2 "missing dependencies:$_missing (run: make doctor)"

# ===========================================================================
# 2. Font assertion — never let agg substitute a face
# ===========================================================================
# VHS silently substitutes a serif face when the requested font is missing, and
# that produced a wrong-looking probe GIF on this machine before the harness was
# rewritten. agg is stricter — it exits non-zero on an unresolvable family — but
# "stricter" is not "proven", and on a laptop with JetBrains Mono installed
# system-wide a broken demos/fonts/ would be invisible. So three layers:
#
#   (a) the vendored TTFs exist, really are "JetBrains Mono", and carry the
#       block glyphs ash.chart() draws with;
#   (b) agg REFUSES an unresolvable family — i.e. the VHS failure mode cannot
#       occur here at all;
#   (c) the exact file in demos/fonts is what agg rasterised. A temp copy of the
#       vendored regular face is rewritten with a unique family name that exists
#       nowhere on the machine; agg is asked for THAT family; the resulting
#       raster must be byte-identical to the real render. Identical output from
#       a family only our file can satisfy proves our file was used.
assert_font() {
  local reg="$ASH_FONT_DIR/JetBrainsMono-Regular.ttf"
  local bold="$ASH_FONT_DIR/JetBrainsMono-Bold.ttf"
  [ -f "$reg" ]  || ash_die 6 "vendored font missing: $reg"
  [ -f "$bold" ] || ash_die 6 "vendored font missing: $bold"

  ASH_FONT_REG="$reg" ASH_FONT_BOLD="$bold" python3 - <<'PY' || ash_die 6 "vendored font is not usable JetBrains Mono"
import os, sys
try:
    from fontTools.ttLib import TTFont
except ImportError:
    sys.stderr.write("fontTools not installed\n"); sys.exit(1)
for key, want in (("ASH_FONT_REG", "JetBrains Mono"), ("ASH_FONT_BOLD", "JetBrains Mono")):
    f = TTFont(os.environ[key], lazy=True)
    fam = f["name"].getDebugName(1) or ""
    if want not in fam:
        sys.stderr.write("%s reports family %r, expected %r\n" % (os.environ[key], fam, want))
        sys.exit(1)
    if not f.getBestCmap().get(0x2588):
        sys.stderr.write("%s has no U+2588 FULL BLOCK glyph\n" % os.environ[key])
        sys.exit(1)
PY

  local probe="$ASH_OUT/_fontprobe"
  rm -rf "$probe"; mkdir -p "$probe/marked"
  python3 - "$probe/p.cast" <<'PY'
import json, sys
# A probe line with ascenders, descenders, a zero and the block glyphs, so a
# face swap shows up in the raster rather than hiding in the whitespace.
hdr = {"version": 2, "width": 24, "height": 3, "env": {"TERM": "xterm-256color"}}
with open(sys.argv[1], "w") as f:
    f.write(json.dumps(hdr) + "\n")
    f.write(json.dumps([0.0, "o", "MWiljq0O gil1 █▓░▒·\r\n"]) + "\n")
    f.write(json.dumps([0.5, "o", "\r\n"]) + "\n")
PY

  # (b) an unresolvable family must be an error, never a silent substitution.
  if agg -q --font-family "AshNoSuchFace-$$" --font-size 14 \
        "$probe/p.cast" "$probe/nofont.gif" >/dev/null 2>&1; then
    ash_die 6 "agg accepted a nonexistent font family — it may be substituting a fallback face silently"
  fi

  # (c) rename a copy of the vendored regular face to a family nothing else on
  # this machine can provide, then require an identical raster.
  local marker="AshVendoredProbe$$"
  ASH_FONT_SRC="$ASH_FONT_DIR/JetBrainsMono-Regular.ttf" \
  ASH_FONT_DST="$probe/marked/probe.ttf" ASH_FONT_MARK="$marker" python3 - <<'PY' \
    || ash_die 6 "could not build the vendored-font provenance probe"
import os
from fontTools.ttLib import TTFont
f = TTFont(os.environ['ASH_FONT_SRC'])
mark = os.environ['ASH_FONT_MARK']
for rec in f['name'].names:
    if rec.nameID in (1, 3, 4, 6, 16):
        try:
            cur = rec.toUnicode()
        except Exception:
            continue
        rec.string = cur.replace('JetBrains Mono', mark).replace('JetBrainsMono', mark)
f.save(os.environ['ASH_FONT_DST'])
PY

  agg -q --font-dir "$ASH_FONT_DIR" --font-family "JetBrains Mono" --font-size 14 \
      --line-height 1.34 --fps-cap 5 --last-frame-duration 0.2 \
      "$probe/p.cast" "$probe/ours.gif" >/dev/null 2>&1 \
      || ash_die 6 "agg could not render with $ASH_FONT_DIR"
  agg -q --font-dir "$probe/marked" --font-family "$marker" --font-size 14 \
      --line-height 1.34 --fps-cap 5 --last-frame-duration 0.2 \
      "$probe/p.cast" "$probe/vendored.gif" >/dev/null 2>&1 \
      || ash_die 6 "agg could not render the vendored-font provenance probe"
  cmp -s "$probe/ours.gif" "$probe/vendored.gif" \
    || ash_die 6 "agg did NOT rasterise demos/fonts/JetBrainsMono-Regular.ttf — some other 'JetBrains Mono' won"

  ash_log "font: demos/fonts/JetBrainsMono-Regular.ttf asserted (family, block glyphs, and agg provably used this file)"
}
assert_font

# ===========================================================================
# 3. Theme -> agg palette
# ===========================================================================
# theme/pg_ash.json is the ONLY file holding a colour. agg takes a custom theme
# as bg,fg,c0..c15 in bare hex, so we derive it here rather than hardcoding.
[ -f "$ASH_THEME" ] || ash_die 6 "theme file missing: $ASH_THEME"
AGG_THEME="$(ASH_THEME="$ASH_THEME" python3 - <<'PY'
import json, os, sys
t = json.load(open(os.environ['ASH_THEME']))
def h(x): return x.lstrip('#').lower()
parts = [h(t['ui']['bg']), h(t['ui']['fg'])] + [h(c) for c in t['ansi']]
if len(parts) != 18:
    sys.stderr.write("theme.ansi must hold exactly 16 colours\n"); sys.exit(1)
print(','.join(parts))
PY
)" || ash_die 6 "could not derive an agg theme from $ASH_THEME"

# Type metrics come from the theme too — theme/pg_ash.json is the ONLY file that
# carries a number about how this looks. reel.line_height in particular is not a
# taste setting: see the note beside it in the theme. It used to be hardcoded
# here at 1.34, which silently overrode the theme and put a 1px seam through
# every row of the flagship chart.
eval "$(ASH_THEME="$ASH_THEME" python3 - <<'PY'
import json, os
t = json.load(open(os.environ['ASH_THEME']))['reel']
# --font-size in agg takes an integer number of pixels and rejects "14.0"
# outright, so the float the theme carries -- a float because the SVG renderer
# wants one -- is narrowed here rather than duplicated as an int in the theme.
#
# NOTE, and it cost a debugging cycle: no apostrophes in this heredoc. bash 3.2
# (the macOS /bin/bash) mis-scans a quote inside a here-document that is inside
# a $( ) command substitution, and the whole rest of the file is swallowed as a
# quoted string. The failure surfaces 230 lines later as a syntax error on an
# innocent parenthesis.
print("REEL_FONT_SIZE=%d" % int(round(float(t['font_size']))))
print("REEL_LINE_HEIGHT=%s" % t['line_height'])
PY
)" || ash_die 6 "could not read reel metrics from $ASH_THEME"

# The psqlrc hardcodes three theme colours (a psqlrc cannot read JSON). Assert
# they still agree, so a theme edit cannot silently desync the prompt.
ASH_THEME="$ASH_THEME" ASH_PSQLRC="$ASH_DEMO_DIR/lib/psqlrc.demo" python3 - <<'PY' \
  || ash_die 6 "lib/psqlrc.demo prompt colours no longer match theme/pg_ash.json"
import json, os, re, sys
t = json.load(open(os.environ['ASH_THEME']))
rc = open(os.environ['ASH_PSQLRC']).read()
def rgb(hexs):
    h = hexs.lstrip('#')
    return tuple(int(h[i:i+2], 16) for i in (0, 2, 4))
want = [rgb(t['ui']['prompt_accent']), rgb(t['ui']['prompt_dim']), rgb(t['ui']['fg'])]
found = [tuple(int(x) for x in m) for m in re.findall(r'38;2;(\d+);(\d+);(\d+)', rc)]
for w in want:
    if w not in found:
        sys.stderr.write("psqlrc.demo is missing SGR for %s\n" % (w,)); sys.exit(1)
PY

# ===========================================================================
# 4. Backend + the frozen window (spec §2.2)
# ===========================================================================
ensure_backend() {
  if psql -X -At -c 'select 1' >/dev/null 2>&1; then return 0; fi
  if [ -f "$ASH_DEMO_DIR/lib/backend.sh" ]; then
    # shellcheck source=lib/backend.sh
    . "$ASH_DEMO_DIR/lib/backend.sh"
    backend_up || ash_die 3 "backend_up failed"
  fi
  psql -X -At -c 'select 1' >/dev/null 2>&1 \
    || ash_die 3 "cannot reach the demo database (PGDATABASE=${PGDATABASE:-unset})"
}

[ -f "$ASH_WINDOW_ENV" ] || ash_die 3 "missing $ASH_WINDOW_ENV — run \`make seed\` first"
# shellcheck disable=SC1090
. "$ASH_WINDOW_ENV"
: "${ASH_SINCE:?window.env did not define ASH_SINCE}"
: "${ASH_UNTIL:?window.env did not define ASH_UNTIL}"
: "${ASH_BASE_SINCE:?window.env did not define ASH_BASE_SINCE}"
: "${ASH_BASE_UNTIL:?window.env did not define ASH_BASE_UNTIL}"
: "${ASH_STORM_SINCE:?window.env did not define ASH_STORM_SINCE}"
: "${ASH_STORM_UNTIL:?window.env did not define ASH_STORM_UNTIL}"
export PGDATABASE="${PGDATABASE:-$ASH_DEMO_DB}"
ensure_backend

# window.env must not be older than the newest sample: a stale window means the
# seeder re-ran and the reel would query a window that no longer describes the
# data on disk. Compare in the database, in seconds, no `date -d` anywhere.
_stale="$(psql -X -At -v ON_ERROR_STOP=1 -c "
  select case when max(sample_ts) is null then 'nodata'
              when ash.ts_to_timestamptz(max(sample_ts)) > to_timestamp(${ASH_SEED_EPOCH:-0})
              then 'stale' else 'ok' end
  from ash.sample" 2>/dev/null || printf 'err')"
case "$_stale" in
  ok)     : ;;
  nodata) ash_die 3 "ash.sample is empty — the seed did not take" ;;
  stale)  ash_die 3 "$ASH_WINDOW_ENV is older than the newest ash.sample row — re-run \`make seed\`" ;;
  *)      ash_die 3 "could not validate $ASH_WINDOW_ENV against ash.sample" ;;
esac
ash_log "window: $ASH_SINCE .. $ASH_UNTIL   storm: $ASH_STORM_SINCE .. $ASH_STORM_UNTIL"

# ===========================================================================
# 5. Scene table
# ===========================================================================
# Parsed here rather than in lib/scenes.sh so `make demo` works standalone;
# when lib/scenes.sh exists we let it do the validation pass first, because
# builder A owns the banned-reader / explicit-projection rules.
if [ -f "$ASH_DEMO_DIR/lib/scenes.sh" ]; then
  # `bash -n` first: sourcing a file that fails to parse aborts this script with
  # a bare syntax error and no context, which is a miserable thing to debug.
  if bash -n "$ASH_DEMO_DIR/lib/scenes.sh" 2>/dev/null; then
    # shellcheck source=lib/scenes.sh
    . "$ASH_DEMO_DIR/lib/scenes.sh"
    if command -v scenes_validate >/dev/null 2>&1; then scenes_validate; fi
  else
    ash_warn "lib/scenes.sh does not parse — skipping its validation pass"
  fi
fi
[ -f "$ASH_SCENES" ] || ash_die 1 "scene file missing: $ASH_SCENES"

# Belt and braces on the removed-v1.x-reader rule. Cheap, and it is the check
# that would have caught the landing page still advertising a reader 2.0 deleted.
# The banned names are assembled here rather than written out, because "no
# occurrence anywhere under demos/" includes this file's own source.
BANNED_RE="top_$(printf 'waits')|query_$(printf 'waits')|top_by_$(printf 'type')|timeline_$(printf 'chart')"
grep -Ev '^[[:space:]]*#' "$ASH_SCENES" > "$ASH_OUT/_scenes.nocomment"
if [ "$(grep -Ec "$BANNED_RE" "$ASH_OUT/_scenes.nocomment" || true)" -gt 0 ]; then
  ash_die 1 "$ASH_SCENES references a reader removed in 2.0"
fi

REEL_TSV="$ASH_OUT/reel.tsv"
ASH_SCENES="$ASH_SCENES" python3 - > "$REEL_TSV" <<'PY' || ash_die 1 "scenes.tsv is malformed"
import os, sys
rows = []
for lineno, raw in enumerate(open(os.environ['ASH_SCENES'], encoding='utf-8'), 1):
    line = raw.rstrip('\n')
    if not line.strip() or line.lstrip().startswith('#'):
        continue
    cols = line.split('\t')
    if len(cols) != 6:
        sys.stderr.write("line %d: expected 6 tab-separated columns, got %d\n"
                         % (lineno, len(cols)))
        sys.exit(1)
    name, hold, order, title, markers, sql = cols
    if 'select *' in sql.lower() and 'ash.status()' not in sql and 'ash.periods(' not in sql:
        sys.stderr.write("line %d: scene %s uses `select *`\n" % (lineno, name))
        sys.exit(1)
    try:
        order_i = int(order); hold_f = float(hold)
    except ValueError:
        sys.stderr.write("line %d: bad hold/reel_order\n" % lineno); sys.exit(1)
    rows.append((order_i, name, hold_f, title, markers, sql))
reel = sorted(r for r in rows if r[0] > 0)
if not reel:
    sys.stderr.write("no scene has reel_order > 0\n"); sys.exit(1)
seen = set()
for order_i, name, hold_f, title, markers, sql in reel:
    if order_i in seen:
        sys.stderr.write("duplicate reel_order %d\n" % order_i); sys.exit(1)
    seen.add(order_i)
    sys.stdout.write('\t'.join([name, repr(hold_f), title, markers, sql]) + '\n')
PY
REEL_COUNT="$(grep -c . "$REEL_TSV" || true)"
ash_log "reel: $REEL_COUNT scenes from $(basename "$ASH_SCENES")"

# expand_sql <mode> <sql>
#   literal — substitute the quoted absolute timestamp (scripted preflight)
#   psqlvar — substitute :'SINCE' etc. (interactive typing; psql expands it,
#             and the SCREEN shows the short symbolic form instead of a wall of
#             ISO timestamps, which was a named defect in a prototype)
expand_sql() {
  ASH_MODE="$1" ASH_SQL="$2" \
  V_SINCE="$ASH_SINCE" V_UNTIL="$ASH_UNTIL" \
  V_BASE_SINCE="$ASH_BASE_SINCE" V_BASE_UNTIL="$ASH_BASE_UNTIL" \
  V_STORM_SINCE="$ASH_STORM_SINCE" V_STORM_UNTIL="$ASH_STORM_UNTIL" \
  python3 - <<'PY'
import os, sys
names = ['BASE_SINCE', 'BASE_UNTIL', 'STORM_SINCE', 'STORM_UNTIL', 'SINCE', 'UNTIL']
sql = os.environ['ASH_SQL']
mode = os.environ['ASH_MODE']
for n in names:                       # longest first: $BASE_SINCE before $SINCE
    if mode == 'literal':
        sub = "'" + os.environ['V_' + n] + "'"
    else:
        sub = ":'" + n + "'"
    sql = sql.replace('$' + n, sub)
sys.stdout.write(sql)
PY
}

# scene_terminator <sql> — how the interactive path SUBMITS this statement.
#
# A colour-emitting scene must not go through psql's aligned formatter: it
# escapes each 0x1B into the four-character text "\x1B" and then measures column
# widths from the escaped text. The flagship chart frame came out as visible
# "\x1B[38;2;080;250;123m" runs inside a box that wrapped at 100 columns — all
# three prototype designs hit this independently. `\g (format=unaligned ...)`
# hands the rows straight to the terminal with the escape bytes intact.
#
# `\g (options)` needs a psql client >= 16. Older clients get the equivalent via
# \pset, which costs two extra visible lines but never garbles the frame.
PSQL_MAJOR="$(psql --version 2>/dev/null | tr -dc '0-9. ' | awk '{print $1}' | cut -d. -f1)"
: "${PSQL_MAJOR:=0}"
scene_terminator() {
  case "$1" in
    *"color => true"*|*"color => TRUE"*)
      # `tuples_only` is deliberately NOT set. It would shorten this line, but it
      # also drops the header row, and the chart frame is the one frame in the
      # reel whose columns are not self-evident — without `bucket  aas  chart` a
      # viewer sees two unlabelled number columns next to the bars.
      if [ "$PSQL_MAJOR" -ge 16 ]; then
        printf "%s" "\\g (format=unaligned fieldsep='  ')"
      else
        # Pre-16 psql: no parenthesised \g options. Switch the format, run, and
        # switch back — psql executes backslash commands left to right, so the
        # restore lands before the next scene is typed.
        printf "%s" "\\pset format unaligned \\pset fieldsep '  ' \\g \\pset format aligned"
      fi
      ;;
    *) printf "" ;;
  esac
}

# ===========================================================================
# 6. PREFLIGHT — run every reel scene scripted and verify it, before recording
# ===========================================================================
# This is the gate that makes `ash.chart(color => true)`'s 238-column blowup and
# an empty result set impossible to ship silently. It costs ~2 s.
if [ "$RENDER_ONLY" = "1" ]; then
  [ -s "$CAST" ] || ash_die 7 "--render-only but there is no cast at $CAST"
  ash_log "--render-only: reusing $CAST"
  T_REC0="$(ash_now_ms)"
else

ash_step "preflight: executing $REEL_COUNT scenes scripted"
mkdir -p "$ASH_OUT/preflight"
MAX_SCENE_ROWS=0
while IFS=$'\t' read -r name hold title markers sql; do
  [ -n "$name" ] || continue
  pf="$ASH_OUT/preflight/$name.raw"
  # -A: raw ESC passthrough. psql's ALIGNED formatter escapes 0x1B into the
  # four-character text \x1B and then measures column widths from the escaped
  # text — 390-column border rules, garbage alignment. -A sidesteps it entirely.
  # stderr merged in so a psql ERROR: lands in the file vfy_no_errors reads.
  if ! psql -X -A -F '  ' -P footer=off -v ON_ERROR_STOP=1 \
         -c "$(expand_sql literal "$sql")" > "$pf" 2>&1; then
    ash_log "---- $name ----"; cat "$pf" >&2
    ash_die 5 "preflight: scene '$name' failed to execute"
  fi
  vfy_scene "$pf" "$name" "$markers" "$ASH_COLS"

  # ROW budget. The still path can be any height; the reel cannot — a scene
  # taller than the pane scrolls its own header off the top, and the header is
  # usually where the marker lives. (ash.chart's legend names the wait events;
  # lose it and the flagship frame is a bar chart with no key.) Reserve six
  # lines for the narration \echo, its prompt, the typed statement and its
  # continuation, and the trailing prompt.
  rows_used="$(grep -c '' "$pf" || true)"
  rows_budget=$(( ASH_ROWS - 6 ))
  if [ "${rows_used:-0}" -gt "$rows_budget" ]; then
    ash_die 5 "preflight: scene '$name' is $rows_used lines, over the reel budget of $rows_budget (ASH_ROWS=$ASH_ROWS); coarsen the bucket or lower n"
  fi
  [ "${rows_used:-0}" -gt "$MAX_SCENE_ROWS" ] && MAX_SCENE_ROWS="$rows_used"
  ash_log "preflight ok: $name  (${rows_used} lines)"
done < "$REEL_TSV"

# Size the pane to the CONTENT, not to a fixed 30 rows.
#
# Every unused row is dead pixels in every frame of the GIF, and the GIF budget
# is 700 KB. At 30 rows the reel came out at 822 KB with a third of the frame
# empty; sized to the tallest scene it lands comfortably under budget with no
# loss of information. ASH_ROWS remains the CEILING, never the target.
#
# The +6 is the per-scene chrome: narration \echo, its prompt, the typed
# statement, one continuation line, the \g line, and the trailing prompt.
REEL_ROWS=$(( MAX_SCENE_ROWS + 6 ))
[ "$REEL_ROWS" -lt 18 ] && REEL_ROWS=18
[ "$REEL_ROWS" -gt "$ASH_ROWS" ] && REEL_ROWS="$ASH_ROWS"
ash_log "pane: ${ASH_COLS}x${REEL_ROWS} (tallest scene ${MAX_SCENE_ROWS} lines, ceiling ASH_ROWS=$ASH_ROWS)"

# ===========================================================================
# 7. Record
# ===========================================================================
# The splash banner is load-bearing, not decoration: GitHub freezes frame 1 of
# an embedded GIF as its thumbnail, so frame 1 has to say what the project is.
# Sized to ASH_COLS, generated (never hardcoded at 138 chars like the old one).
SPLASH_TXT="$ASH_OUT/splash.ansi"
ASH_COLS="$ASH_COLS" ASH_THEME="$ASH_THEME" ASH_VERSION_STR="${ASH_ASH_VERSION:-2.0}" \
python3 - > "$SPLASH_TXT" <<'PY'
import json, os, sys
cols = int(os.environ['ASH_COLS'])
t = json.load(open(os.environ['ASH_THEME']))
E = '\x1b'
def sgr(hexs):
    h = hexs.lstrip('#')
    return "%s[38;2;%d;%d;%dm" % ((E,) + tuple(int(h[i:i+2], 16) for i in (0, 2, 4)))
RESET  = E + '[0m'
accent = sgr(t['ui']['prompt_accent'])
# ui.dim, not ui.prompt_dim: prompt_dim (#6272A4) is a blue-grey sized for a
# one-character prompt marker and reads as barely-there when a whole sentence is
# set in it against ui.bg. ui.dim (#7F949B) is the theme's "secondary text" role.
dim    = sgr(t['ui']['dim'])
fg     = sgr(t['ui']['fg'])
inner  = cols - 2
body = [
    ("", fg),
    ("  pg_ash 2.0  —  Active Session History for PostgreSQL", accent),
    ("", fg),
    ("  Pure SQL. No extension, no restart.", fg),
    ("  Installs with \\i on RDS, Cloud SQL, Supabase, AlloyDB and Neon.", fg),
    ("", fg),
    ("  Something spiked a few minutes ago. Let's find out what.", dim),
    ("", fg),
]
out = [accent + '╭' + '─' * inner + '╮' + RESET]
for text, colour in body:
    pad = inner - len(text)
    out.append(accent + '│' + RESET + colour + text + ' ' * max(pad, 0) + accent + '│' + RESET)
out.append(accent + '╰' + '─' * inner + '╯' + RESET)
sys.stdout.write('\n'.join(out) + '\n')
PY

# The recorded command: splash, then psql. PSQLRC (not -X!) loads the demo
# prompt; -v passes the frozen window in as psql variables so the typed SQL can
# stay short and symbolic on screen.
LAUNCH="$ASH_OUT/launch.sh"
cat > "$LAUNCH" <<LAUNCHEOF
#!/bin/sh
# generated by bin/record-demo.sh — the process asciinema records
set -e
printf '\033[2J\033[H'
cat "$SPLASH_TXT"
PSQLRC="$ASH_DEMO_DIR/lib/psqlrc.demo" exec psql \\
  -v SINCE="$ASH_SINCE" -v UNTIL="$ASH_UNTIL" \\
  -v BASE_SINCE="$ASH_BASE_SINCE" -v BASE_UNTIL="$ASH_BASE_UNTIL" \\
  -v STORM_SINCE="$ASH_STORM_SINCE" -v STORM_UNTIL="$ASH_STORM_UNTIL" \\
  -L "$TRANSCRIPT"
LAUNCHEOF
chmod +x "$LAUNCH"

# asciinema v2 and v3 disagree on the geometry and env-capture flags. Detect.
ASCIINEMA_MAJOR="$(asciinema --version 2>/dev/null | tr -dc '0-9 .' | awk '{print $1}' | cut -d. -f1)"
if [ "${ASCIINEMA_MAJOR:-2}" -ge 3 ]; then
  REC_GEO="--window-size ${ASH_COLS}x${REEL_ROWS}"
else
  REC_GEO="--cols ${ASH_COLS} --rows ${REEL_ROWS}"
fi

rm -f "$CAST" "$TRANSCRIPT" "$STDERR_LOG"
mkdir -p "$ASH_OUT/shots"
rm -f "$ASH_OUT/shots"/*.ansi 2>/dev/null || true

ash_step "recording (${ASH_COLS}x${REEL_ROWS}, asciinema $ASCIINEMA_MAJOR)"
T_REC0="$(ash_now_ms)"
DRV_SHOT_DIR="$ASH_OUT/shots"
drv_start "$SESSION_PREFIX" "$ASH_COLS" "$REEL_ROWS" \
  "asciinema rec --overwrite --idle-time-limit 3 $REC_GEO \
     --command '$LAUNCH 2> $STDERR_LOG' '$CAST'"

# Wait for the FIRST prompt rather than sleeping. Until psql has printed
# PROMPT1 there is no pane title, so this doubles as "psql actually started".
if ! drv_wait_prompt "" 30000 >/dev/null; then
  drv_shot "$ASH_OUT/shots/_startup.ansi" || true
  cat "$ASH_OUT/shots/_startup.ansi" >&2 2>/dev/null || true
  ash_die 7 "psql never reached its first prompt inside tmux"
fi

# Beat 1: hold the splash. GitHub's thumbnail is frame 1 and viewers need a
# moment to read it when the loop comes back around.
drv_hold 1.6

# --- the investigation ------------------------------------------------------
# One \echo of narration, then the scene. The narration is what turns six
# query results into a story: triage -> locate -> which wait -> which query ->
# is this actually abnormal.
NARRATION_status='-- Q0: is pg_ash collecting? no pg_cron here, so: external scheduler'
NARRATION_periods='-- Q1: triage. how much history is there, and is this a spike?'
NARRATION_chart='-- Q2: when did it land, and which wait class? (24-bit colour)'
NARRATION_top_event='-- Q3: the exact wait event during the spike'
NARRATION_top_query='-- Q4: which statement is stuck on it?'
NARRATION_compare='-- Q5: incident vs the calm baseline. is this really abnormal?'

while IFS=$'\t' read -r name hold title markers sql; do
  [ -n "$name" ] || continue
  eval "narr=\${NARRATION_$name:-}"
  [ -n "$narr" ] && drv_note "$narr" 0.7
  body="$(expand_sql psqlvar "$sql")"
  term="$(scene_terminator "$sql")"
  # When a \g terminator is used the statement must NOT also carry a `;`,
  # or psql executes it before reaching the backslash command.
  if [ -n "$term" ]; then
    case "$body" in *\;) body="${body%;}" ;; esac
  fi
  DRV_SQL="$body" \
  DRV_MARKERS="$markers" \
  DRV_HOLD="$hold" \
  DRV_TERMINATOR="$term" \
    drv_run "$name"
done < "$REEL_TSV"

# --- closing lines ----------------------------------------------------------
drv_note '-- Root cause: concurrent UPDATEs contending on one row.' 1.6
drv_note '-- pg_ash: pure SQL, no extension, no restart. Works everywhere.' 3.4

prev="$(drv_title)"
drv_type '\q'
drv_enter
drv_hold 0.8
drv_kill
T_REC1="$(ash_now_ms)"
ash_log "recording finished in $(( (T_REC1 - T_REC0) / 1000 ))s"

[ -s "$CAST" ] || ash_die 7 "asciinema produced no cast at $CAST"

# psql's -L transcript and the tee'd stderr are the two places a database error
# can hide. `psql -L` does NOT log errors, which is exactly why the stderr tee
# exists; check both.
for f in "$TRANSCRIPT" "$STDERR_LOG"; do
  [ -f "$f" ] || continue
  n="$(grep -Ec "$VFY_ERR_RE" "$f" || true)"
  if [ "${n:-0}" -gt 0 ]; then
    grep -En "$VFY_ERR_RE" "$f" >&2 || true
    ash_die 5 "database error found in $(basename "$f")"
  fi
done

# ===========================================================================
# 8. Cast post-processing
# ===========================================================================
# asciinema stamps the first event ~70 ms in; agg renders t=0, so the GIF would
# open on an empty terminal — and that empty frame becomes GitHub's thumbnail.
# Pull the first event to 0.0. Later events carry absolute times in v2 and
# relative deltas in v3, so only the first is touched either way.
python3 - "$CAST" <<'PY'
import json, sys
path = sys.argv[1]
with open(path) as f:
    lines = f.readlines()
if len(lines) > 1:
    header, rest = lines[0], lines[1:]
    for i, line in enumerate(rest):
        if not line.strip():
            continue
        ev = json.loads(line)
        ev[0] = 0.0
        rest[i] = json.dumps(ev) + "\n"
        break
    with open(path, 'w') as f:
        f.write(header)
        f.writelines(rest)
PY

fi   # RENDER_ONLY

# ===========================================================================
# 9. Render: agg -> frames -> chrome composite -> gif + mp4
# ===========================================================================
ash_step "rendering"

# Bare terminal, truecolor, our font.
#
# --idle-time-limit deviates from the spec's 1.2 s on purpose. The scenes.tsv
# `hold` column exists so a viewer can actually READ a table before it scrolls;
# capping idle at 1.2 s would silently truncate every one of those holds to
# 1.2 s and make the reel unreadable. 3.0 s keeps the short holds exact, trims
# the long ones, and costs almost nothing in bytes: an idle stretch is ONE gif
# frame with a longer delay, so the file size is driven by the typing animation,
# not by the pauses.
agg -q \
  --font-dir "$ASH_FONT_DIR" \
  --font-family "JetBrains Mono" \
  --font-size "$REEL_FONT_SIZE" \
  --line-height "$REEL_LINE_HEIGHT" \
  --theme "$AGG_THEME" \
  --fps-cap "${ASH_AGG_FPS:-12}" \
  --idle-time-limit "${ASH_AGG_IDLE:-3.0}" \
  --last-frame-duration 3 \
  "$CAST" "$TERM_GIF" || ash_die 6 "agg failed"

# Measure what agg produced. NEVER hardcode these — font metrics differ across
# machines and a hardcoded plate would clip on the first mismatch.
TERM_W="$(ffprobe -v error -select_streams v:0 -show_entries stream=width  -of csv=p=0 "$TERM_GIF")"
TERM_H="$(ffprobe -v error -select_streams v:0 -show_entries stream=height -of csv=p=0 "$TERM_GIF")"
[ -n "$TERM_W" ] && [ -n "$TERM_H" ] || ash_die 6 "could not measure the agg output"
ash_log "agg terminal raster: ${TERM_W}x${TERM_H}"

# --- chrome plate -----------------------------------------------------------
# render/chrome.py (builder B) is preferred so the reel and the stills share one
# implementation of §3.3. If it is absent or does not speak this CLI we fall
# back to an equivalent generator written from the same theme file, so the reel
# never blocks on the render plane.
CHROME_TITLE="pg_ash 2.0"
chrome_ok=0
if [ -f "$ASH_DEMO_DIR/render/chrome.py" ]; then
  # render/chrome.py prints a sourceable geometry summary so the overlay origin
  # is never recomputed here — the plate and the composite cannot disagree.
  if CHROME_GEOM="$(python3 "$ASH_DEMO_DIR/render/chrome.py" \
       --theme "$ASH_THEME" --inner "${TERM_W}x${TERM_H}" \
       --metrics reel --title "$CHROME_TITLE" \
       --font "$ASH_FONT_DIR/JetBrainsMono-Regular.ttf" -o "$PLATE" 2>/dev/null)"; then
    chrome_ok=1
    # shellcheck disable=SC2086
    eval $CHROME_GEOM
  else
    ash_warn "render/chrome.py did not accept the documented CLI — using the built-in plate"
  fi
fi
if [ "$chrome_ok" -eq 0 ]; then
  cat > "$ASH_OUT/_chrome_plate.py" <<'PY'
#!/usr/bin/env python3
"""Fallback chrome plate (spec §3.3), constructed from theme/pg_ash.json alone.

Identical construction to render/ansi2svg.py's chrome:
  1. fill the canvas with ui.marginfill
  2. rounded card at (margin, margin), radius 12, ui.bg, 1px ui.border
  3. title bar height titlebar_h in ui.titlebar + a hairline along its bottom
  4. three r=5.5 dots at margin+{19,38,57}, cy = margin+19
  5. title centred in the card, baseline cy + 0.36*size, ui.dim
  6. body origin (margin+pad_x, margin+titlebar_h+pad_y)

Rendered at 3x and downsampled so the corners and dots are cleanly antialiased
without ffmpeg ever seeing a half-transparent edge.
"""
import argparse, json
from PIL import Image, ImageDraw, ImageFont

p = argparse.ArgumentParser()
p.add_argument('--theme', required=True)
p.add_argument('--body-w', type=int, required=True)
p.add_argument('--body-h', type=int, required=True)
p.add_argument('--title', default='pg_ash 2.0')
p.add_argument('--font')
p.add_argument('--out', required=True)
a = p.parse_args()

t = json.load(open(a.theme))
c, ui = t['chrome'], t['ui']
M, R, TB = c['margin'], c['radius'], c['titlebar_h']
PX, PY = c['pad_x'], c['pad_y']

card_w = a.body_w + 2 * PX
card_h = TB + 2 * PY + a.body_h
W, H = card_w + 2 * M, card_h + 2 * M

S = 3  # supersample
img = Image.new('RGB', (W * S, H * S), ui['marginfill'])
d = ImageDraw.Draw(img)
d.rounded_rectangle([M * S, M * S, (M + card_w) * S - 1, (M + card_h) * S - 1],
                    radius=R * S, fill=ui['bg'], outline=ui['border'],
                    width=max(1, c['border'] * S))
# Title bar: rounded on top, square at the bottom, then a hairline rule.
d.rounded_rectangle([M * S, M * S, (M + card_w) * S - 1, (M + TB) * S],
                    radius=R * S, fill=ui['titlebar'])
d.rectangle([M * S, (M + TB - R) * S, (M + card_w) * S - 1, (M + TB) * S],
            fill=ui['titlebar'])
d.line([(M * S, (M + TB) * S), ((M + card_w) * S - 1, (M + TB) * S)],
       fill=ui['border'], width=max(1, S))
cy = (M + 19) * S
for dx, col in zip(c['dot_x'], ui['dots']):
    cx = (M + dx) * S
    r = c['dot_r'] * S
    d.ellipse([cx - r, cy - r, cx + r, cy + r], fill=col)

img = img.resize((W, H), Image.LANCZOS)
d = ImageDraw.Draw(img)
size = int(round(14 * c['title_size_em']))
font = None
if a.font:
    try:
        font = ImageFont.truetype(a.font, size)
    except Exception:
        font = None
if font is None:
    font = ImageFont.load_default()
tw = d.textlength(a.title, font=font)
d.text((M + (card_w - tw) / 2, M + 19 - size * 0.62), a.title,
       font=font, fill=ui['dim'])
img.save(a.out)
print('%d %d %d %d' % (W, H, M + PX, M + TB + PY))
PY
  python3 "$ASH_OUT/_chrome_plate.py" --theme "$ASH_THEME" \
    --body-w "$TERM_W" --body-h "$TERM_H" --title "$CHROME_TITLE" \
    --font "$ASH_FONT_DIR/JetBrainsMono-Regular.ttf" --out "$PLATE" >/dev/null \
    || ash_die 6 "chrome plate generation failed"
fi
[ -s "$PLATE" ] || ash_die 6 "chrome plate is empty"

# Body origin. render/chrome.py reports it; the fallback derives it from the
# same theme numbers. Either way it is never a literal in this file.
if [ -n "${ASH_PLATE_X:-}" ] && [ -n "${ASH_PLATE_Y:-}" ]; then
  OX="$ASH_PLATE_X"; OY="$ASH_PLATE_Y"
else
  OX="$(ASH_THEME="$ASH_THEME" python3 -c "
import json,os
c=json.load(open(os.environ['ASH_THEME']))['chrome']
print(c['margin']+c['pad_x'])")"
  OY="$(ASH_THEME="$ASH_THEME" python3 -c "
import json,os
c=json.load(open(os.environ['ASH_THEME']))['chrome']
print(c['margin']+c['titlebar_h']+c['pad_y'])")"
fi
PLATE_W="$(ffprobe -v error -select_streams v:0 -show_entries stream=width  -of csv=p=0 "$PLATE")"
PLATE_H="$(ffprobe -v error -select_streams v:0 -show_entries stream=height -of csv=p=0 "$PLATE")"
ash_log "chrome plate: ${PLATE_W}x${PLATE_H}, body origin ${OX},${OY}"
# The plate must actually contain the terminal at that origin, or the composite
# silently clips the last rows — the class of bug a hardcoded plate size causes.
if [ "$(( OX + TERM_W ))" -gt "$PLATE_W" ] || [ "$(( OY + TERM_H ))" -gt "$PLATE_H" ]; then
  ash_die 6 "chrome plate ${PLATE_W}x${PLATE_H} cannot hold a ${TERM_W}x${TERM_H} terminal at ${OX},${OY}"
fi

# --- composite --------------------------------------------------------------
mkdir -p "$ASH_ASSETS"
# One filter graph, run twice: once to a palettised GIF, once to H.264. Both
# read the same two inputs, so the reel and the mp4 are genuinely the same
# render rather than two recordings that drifted.
FILTER="[1:v][0:v]overlay=x=${OX}:y=${OY}:format=rgb:shortest=1[v]"

# GIF, in TWO passes. The single-pass `split` + palettegen + paletteuse graph is
# the idiom everyone reaches for, and it OOM-killed ffmpeg here: `split` has to
# buffer every composited frame in memory until palettegen has seen the last one.
# Writing the palette to a PNG first costs one extra decode and bounds memory.
#
# stats_mode=full builds ONE palette across all frames, so a colour that appears
# only in the chart scene is not evicted by the twenty seconds of calm around it.
# dither=none keeps flat block colour flat — dithering is what turns a solid
# #FF5555 bar into speckle and destroys the exact-RGB guarantee.
# shortest=1 is NOT optional. `-loop 1` makes the plate an INFINITE single-frame
# stream, and overlay's base input is the plate, so without it the filter graph
# never ends: the first attempt OOM-killed ffmpeg buffering frames for
# palettegen, the second ran for seven minutes producing nothing.
PALETTE_PNG="$ASH_OUT/palette.png"
ffmpeg -y -v error -i "$TERM_GIF" -loop 1 -i "$PLATE" \
  -filter_complex "${FILTER};[v]palettegen=max_colors=256:stats_mode=full" \
  -frames:v 1 "$PALETTE_PNG" || ash_die 6 "ffmpeg palettegen failed"
ffmpeg -y -v error -i "$TERM_GIF" -loop 1 -i "$PLATE" -i "$PALETTE_PNG" \
  -filter_complex "${FILTER};[v][2:v]paletteuse=dither=none:diff_mode=rectangle" \
  -loop 0 "$GIF" || ash_die 6 "ffmpeg gif composite failed"

# MP4 from the same graph, so the reel and the video are genuinely one render
# rather than two recordings that drifted. yuv420p + even dimensions for player
# compatibility, and an explicit constant frame rate: fed the GIF's variable
# timebase directly, x264 produced a file LARGER than the GIF.
ffmpeg -y -v error -i "$TERM_GIF" -loop 1 -i "$PLATE" \
  -filter_complex "${FILTER};[v]scale=trunc(iw/2)*2:trunc(ih/2)*2,format=yuv420p[o]" \
  -map '[o]' -c:v libx264 -preset slow -crf 20 -movflags +faststart \
  -r "${ASH_MP4_FPS:-15}" -pix_fmt yuv420p "$MP4" || ash_die 6 "ffmpeg mp4 encode failed"

# gifsicle: lossless optimisation ONLY. --lossy / --colors would quantise the
# 24-bit wait-class palette that the whole exercise exists to preserve.
if ash_have gifsicle; then
  gifsicle -O3 "$GIF" -o "$GIF.opt" >/dev/null 2>&1 && mv "$GIF.opt" "$GIF"
fi

# ===========================================================================
# 10. Post-render verification
# ===========================================================================
ash_step "verifying the rendered reel"
GIF_BYTES="$(wc -c < "$GIF" | tr -d ' ')"
GIF_W="$(ffprobe -v error -select_streams v:0 -show_entries stream=width  -of csv=p=0 "$GIF")"
GIF_H="$(ffprobe -v error -select_streams v:0 -show_entries stream=height -of csv=p=0 "$GIF")"
GIF_FRAMES="$(ffprobe -v error -count_frames -select_streams v:0 -show_entries stream=nb_read_frames -of csv=p=0 "$GIF" 2>/dev/null || printf '0')"

# Pixel-sample every frame for the exact docs/COLOR_SCHEME.md RGB values. This
# is the assertion that "truecolor survived" is a measurement and not a hope.
# We require the wait classes the reel actually exercises; the rest are checked
# opportunistically and reported.
# The frames are read straight out of the GIF with Pillow rather than exploded
# to PNG with ffmpeg first. Writing ~1800 PNGs and decoding them again cost
# about 50 of the 85 seconds this script spent after the recording, for no
# information: Pillow seeks GIF frames natively.
#
# ASH_KEEP_FRAMES=1 writes them out anyway, which is how you eyeball a single
# frame when something looks wrong.
if [ "${ASH_KEEP_FRAMES:-}" = "1" ]; then
  rm -rf "$ASH_OUT/gifframes"; mkdir -p "$ASH_OUT/gifframes"
  ffmpeg -y -v error -i "$GIF" "$ASH_OUT/gifframes/%05d.png" \
    || ash_die 6 "could not extract frames from $GIF"
  ash_log "ASH_KEEP_FRAMES=1: frames written to $ASH_OUT/gifframes"
fi

# Which wait classes MUST survive byte-exact.
#
# Lock and CPU* are the two series ash.chart draws with U+2588 in this reel, so
# they are solid ink and must land on the literal RGB. Other is the `·`
# remainder column, i.e. ordinary text — it proves the palette did not touch
# glyph colour either.
#
# Client is deliberately NOT required. It is the rank-4 series, drawn with
# U+2591 LIGHT SHADE — a 25%-ink dither pattern about one pixel wide. Blended
# against the background by the rasteriser, it genuinely never reaches its
# literal RGB on screen, in agg or in any terminal. Demanding an exact hit for
# it would not be a stronger test, it would be a wrong one. The report below
# still prints how far off every class is, so a real quantisation shows up.
ASH_GIF="$GIF" ASH_REQUIRED_COLORS="${ASH_REQUIRED_COLORS:-Lock,CPU*,Other}" \
python3 - <<'PY' || ash_die 6 "the rendered GIF lost the 24-bit wait-class palette"
import os, sys
from collections import Counter
from PIL import Image, ImageSequence

# docs/COLOR_SCHEME.md, verbatim.
PALETTE = [
    ("CPU*",      (80, 250, 123)),
    ("IdleTx",    (241, 250, 140)),
    ("IO",        (30, 100, 255)),
    ("Lock",      (255, 85, 85)),
    ("LWLock",    (255, 121, 198)),
    ("IPC",       (0, 200, 255)),
    ("Client",    (255, 220, 100)),
    ("Timeout",   (255, 165, 0)),
    ("BufferPin", (0, 210, 180)),
    ("Activity",  (150, 100, 255)),
    ("Extension", (190, 150, 255)),
    ("Other",     (180, 180, 180)),
]
# Exact hits are what a solid run of U+2588 and every glyph interior produce.
# But ash.chart draws its rank 2/3/4 series with U+2593/2591/2592, and in a
# TERMINAL those are not translucent blocks — they are literal dither patterns
# cut into the glyph. Their strokes are roughly one pixel wide and they do not
# land on pixel boundaries, so antialiasing can leave a series with no pixel at
# its literal RGB even though the bar is plainly, correctly that colour.
#
# (The stills renderer does not have this problem: it promotes the glyph to a
# <rect> at theme.shade opacity. This is a real difference between the two
# artifacts, not a bug in either.)
#
# So: exact match preferred, near match accepted, and "near" is defined tightly.
# The closest pair in the whole docs/COLOR_SCHEME.md palette is Activity
# (150,100,255) and Extension (190,150,255) at a Chebyshev distance of 50, so a
# tolerance of 20 can never let one wait class stand in for another, and cannot
# let a quantised-into-mud palette pass either.
TOL = 20


def nearest(counts, rgb):
    """(distance, exact?) of the closest pixel in the raster to `rgb`."""
    if counts.get(rgb):
        return 0, True
    best = 999
    r, g, b = rgb
    for (pr, pg, pb) in counts:
        d = max(abs(pr - r), abs(pg - g), abs(pb - b))
        if d < best:
            best = d
            if best == 0:
                break
    return best, False


seen = Counter()
near = {}
gif = Image.open(os.environ['ASH_GIF'])
nframes = 0
mid_counts = None
mid_size = (0, 0)
total = getattr(gif, 'n_frames', 1)
for frame in ImageSequence.Iterator(gif):
    im = frame.convert('RGB')
    # getcolors is a C-level histogram; Counter(im.getdata()) walks half a
    # million Python ints per frame and there are ~1800 frames.
    counts = dict((rgb, n) for n, rgb in (im.getcolors(1 << 24) or []))
    for name, rgb in PALETTE:
        if counts.get(rgb):
            seen[name] += counts[rgb]
        elif name not in near or near[name] > 0:
            d, _ = nearest(counts, rgb)
            near[name] = min(near.get(name, 999), d)
    if nframes == total // 2:
        mid_counts, mid_size = counts, im.size
    nframes += 1
if not nframes:
    sys.stderr.write("no frames in the GIF\n"); sys.exit(1)

# Non-triviality: an all-background GIF must fail. Sample the middle frame.
if mid_counts is None:
    mid_counts, mid_size = counts, im.size
bg_share = max(mid_counts.values()) / float(mid_size[0] * mid_size[1])
frames = range(nframes)

def status(name):
    if seen.get(name):
        return "%d px exact" % seen[name]
    d = near.get(name, 999)
    if d <= TOL:
        return "within %d (dithered glyph)" % d
    return "absent"


print("  frames sampled: %d" % len(frames))
for name, rgb in PALETTE:
    print("  %-10s %-16s %s" % (name, str(rgb), status(name)))
print("  dominant colour share in the middle frame: %.1f%%" % (bg_share * 100))

required = [x.strip() for x in os.environ['ASH_REQUIRED_COLORS'].split(',') if x.strip()]
missing = [n for n in required
           if not seen.get(n) and near.get(n, 999) > TOL]
if missing:
    sys.stderr.write("required wait-class colours absent from the GIF: %s\n"
                     % ', '.join("%s (nearest pixel %s away)"
                                 % (n, near.get(n, '?')) for n in missing))
    sys.exit(1)
if bg_share > 0.995:
    sys.stderr.write("the reel is effectively blank (%.2f%% one colour)\n" % (bg_share * 100))
    sys.exit(1)
PY

if [ "$GIF_BYTES" -gt "$GIF_BUDGET_BYTES" ]; then
  ash_die 6 "GIF is $GIF_BYTES bytes, over the $GIF_BUDGET_BYTES budget"
fi

T_END="$(ash_now_ms)"
ash_step "done"
printf '  gif    %s  %sx%s  %s bytes  %s frames\n' "$GIF" "$GIF_W" "$GIF_H" "$GIF_BYTES" "$GIF_FRAMES" >&2
printf '  mp4    %s  %s bytes\n' "$MP4" "$(wc -c < "$MP4" | tr -d ' ')" >&2
printf '  cast   %s\n' "$CAST" >&2
printf '  total  %ss\n' "$(( (T_END - T_REC0) / 1000 ))" >&2
