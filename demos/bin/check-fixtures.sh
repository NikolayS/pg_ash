#!/usr/bin/env bash
#
# bin/check-fixtures.sh -- the no-database gate (§6.5). Backs `make check`.
#
# WHAT THIS PROVES: the renderer still turns a known byte sequence into exactly
# the SVG it used to. It runs in about a second, needs only python3 + fontTools
# + brotli, and it is a genuine gate rather than a smoke test precisely because
# the SVG output is byte-deterministic.
#
# WHAT THIS CANNOT PROVE: that the frozen input still reflects what pg_ash does.
# If a reader is renamed, a column moves, or the installer path shifts, these
# fixtures keep passing while the live capture would fail. Only the nightly
# re-capture (`make stills` against a real database, then
# `git diff --exit-code demos/fixtures`) catches that. Say so in the Makefile
# help text; a gate people misunderstand is worse than no gate.
#
# The comparison is against fixtures/expected/, never against assets/. The
# committed assets come from a different seed and would never match.
#
set -Eeuo pipefail

HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)
DEMO_DIR=$(cd "$HERE/.." && pwd -P)

# env.sh is optional here on purpose: `make check` must run in a bare CI job
# with nothing configured.
if [ -f "$DEMO_DIR/lib/env.sh" ]; then
  # shellcheck source=../lib/env.sh
  . "$DEMO_DIR/lib/env.sh"
fi
: "${ASH_THEME:=$DEMO_DIR/theme/pg_ash.json}"
: "${ASH_FONT_DIR:=$DEMO_DIR/fonts}"
: "${ASH_COLS:=100}"

FIX="$DEMO_DIR/fixtures"
EXPECT="$FIX/expected"
MANIFEST="$FIX/manifest.tsv"
TMP="${TMPDIR:-/tmp}/ash-check.$$"
mkdir -p "$TMP"
trap 'rm -rf "$TMP"' EXIT

command -v python3 >/dev/null 2>&1 || { echo "check: python3 required" >&2; exit 2; }
python3 -c 'import fontTools, brotli' 2>/dev/null || {
  echo "check: python3 -m pip install fonttools brotli" >&2; exit 2; }
[ -f "$MANIFEST" ] || { echo "check: no $MANIFEST (run \`make fixtures\`)" >&2; exit 1; }

FONT="$ASH_FONT_DIR/JetBrainsMono-Regular.ttf"
BOLD="$ASH_FONT_DIR/JetBrainsMono-Bold.ttf"
[ -f "$FONT" ] || { echo "check: vendored font missing: $FONT" >&2; exit 6; }

fail=0
n=0

# fd 3, not stdin: see the note in bin/capture-stills.sh -- children inherit
# stdin and at least one of them reads it.
while IFS=$'\t' read -r name sha title <&3; do
  case "$name" in ""|\#*) continue ;; esac
  n=$((n + 1))
  src="$FIX/$name.ansi"
  want="$EXPECT/$name.svg"
  got="$TMP/$name.svg"

  [ -f "$src" ]  || { echo "check: missing fixture input $src" >&2; fail=1; continue; }
  [ -f "$want" ] || { echo "check: missing expected output $want" >&2; fail=1; continue; }

  # Width gate on the frozen bytes too: a fixture that no longer fits the
  # budget means the budget changed, and that must be a deliberate decision.
  python3 "$DEMO_DIR/render/dwidth.py" --max "$ASH_COLS" --quiet "$src" || fail=1

  python3 "$DEMO_DIR/render/ansi2svg.py" "$src" -o "$got" \
      --theme "$ASH_THEME" --font "$FONT" \
      ${BOLD:+--bold-font "$BOLD"} \
      --title "$title" --cols "$ASH_COLS" --quiet

  if ! cmp -s "$got" "$want"; then
    echo "check: RENDER DRIFT for $name" >&2
    echo "  expected $want" >&2
    echo "  got      $got (kept for inspection)" >&2
    cp "$got" "$DEMO_DIR/${name}.drift.svg" 2>/dev/null || true
    fail=1
    continue
  fi

  have=$(python3 - "$want" <<'PY'
import hashlib, sys
sys.stdout.write(hashlib.sha256(open(sys.argv[1], 'rb').read()).hexdigest())
PY
)
  if [ "$have" != "$sha" ]; then
    echo "check: manifest sha mismatch for $name" >&2
    echo "  manifest $sha" >&2
    echo "  actual   $have" >&2
    fail=1
  fi
done 3< "$MANIFEST"

if [ "$fail" -ne 0 ]; then
  echo "check: FAILED" >&2
  exit 6
fi
echo "check: $n fixture(s) re-rendered byte-identically" >&2
