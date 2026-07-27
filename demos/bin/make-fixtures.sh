#!/usr/bin/env bash
#
# bin/make-fixtures.sh -- refresh fixtures/ from the current capture. Backs
# `make fixtures`.
#
# Deliberate, manual and reviewable. NEVER run this automatically as part of
# `make stills`: a fixture set that regenerates itself whenever the renderer
# changes cannot detect a renderer change, which is the only thing it is for.
#
# The diff this produces is the thing a human reads. A changed capture shape
# means a reader was renamed, a column moved, or the installer path shifted --
# exactly the drift that let README.md go on advertising a colour reader for
# months after 2.0 removed it.
#
set -Eeuo pipefail

HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)
DEMO_DIR=$(cd "$HERE/.." && pwd -P)
# shellcheck source=../lib/env.sh
. "$DEMO_DIR/lib/env.sh"

FIX="$DEMO_DIR/fixtures"
EXPECT="$FIX/expected"
mkdir -p "$FIX" "$EXPECT"

FONT="$ASH_FONT_DIR/JetBrainsMono-Regular.ttf"
BOLD="$ASH_FONT_DIR/JetBrainsMono-Bold.ttf"
[ -d "$ASH_OUT/ansi" ] || {
  echo "make-fixtures: no $ASH_OUT/ansi -- run \`make capture\` first" >&2; exit 1; }

: > "$FIX/manifest.tsv.new"
: > "$FIX/shape.tsv.new"

while IFS=$'\t' read -r name hold reel title markers sql caption <&3; do
  case "$name" in ""|\#*) continue ;; esac
  src="$ASH_OUT/ansi/$name.ansi"
  [ -f "$src" ] || { echo "make-fixtures: no capture for $name" >&2; exit 1; }

  cp "$src" "$FIX/$name.ansi"

  # The reader CONTRACT, alongside the frozen bytes. bin/rot-check.sh compares
  # a fresh capture against this; see render/scene_shape.py for why a
  # fingerprint and not a diff. The raw (unit-separated) capture is the input,
  # because that is where the field names still exist as fields.
  raw="$ASH_OUT/raw/$name.raw"
  if [ -f "$raw" ]; then
    printf '%s\t%s\n' "$name" \
      "$(python3 "$DEMO_DIR/render/scene_shape.py" -F $'\x1f' "$raw")" \
      >> "$FIX/shape.tsv.new"
  else
    echo "make-fixtures: no raw capture for $name (shape not recorded)" >&2
  fi
  python3 "$DEMO_DIR/render/ansi2svg.py" "$FIX/$name.ansi" -o "$EXPECT/$name.svg" \
      --theme "$ASH_THEME" --font "$FONT" ${BOLD:+--bold-font "$BOLD"} \
      --title "$title" --cols "$ASH_COLS" --quiet

  sha=$(python3 - "$EXPECT/$name.svg" <<'PY'
import hashlib, sys
sys.stdout.write(hashlib.sha256(open(sys.argv[1], 'rb').read()).hexdigest())
PY
)
  printf '%s\t%s\t%s\n' "$name" "$sha" "$title" >> "$FIX/manifest.tsv.new"
done 3< "$ASH_SCENES"

mv "$FIX/manifest.tsv.new" "$FIX/manifest.tsv"
mv "$FIX/shape.tsv.new" "$FIX/shape.tsv"
echo "make-fixtures: refreshed $FIX (review the diff before committing)" >&2
