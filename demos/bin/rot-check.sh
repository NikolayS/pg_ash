#!/usr/bin/env bash
#
# bin/rot-check.sh -- the demo-rot alarm. Backs `make rot`, and it is the step
# CI runs after a live re-capture.
#
# `make check` proves the RENDERER still works on frozen bytes. This proves the
# BYTES still have the shape the frozen ones had -- i.e. that pg_ash's readers
# still project the same columns under the same names. See render/scene_shape.py
# for why this is a fingerprint comparison and not a diff; the short version is
# that every number in a capture is a real measurement, so a byte diff is red on
# every run and therefore ignored on every run.
#
# Requires a fresh capture in $ASH_OUT/raw (i.e. `make capture` or `make stills`
# first) and fixtures/shape.tsv, which `make fixtures` writes.
#
# Exit 0 clean, 1 usage, 5 on drift (§2.6: capture verification failure).
#
set -Eeuo pipefail

HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)
DEMO_DIR=$(cd "$HERE/.." && pwd -P)
# shellcheck source=../lib/env.sh
. "$DEMO_DIR/lib/env.sh"

SHAPE_TSV="$DEMO_DIR/fixtures/shape.tsv"
RAW_DIR="$ASH_OUT/raw"

[ -d "$RAW_DIR" ] || {
  echo "rot-check: no $RAW_DIR -- run \`make capture\` first" >&2; exit 1; }
[ -f "$SHAPE_TSV" ] || {
  echo "rot-check: no $SHAPE_TSV -- run \`make fixtures\` first" >&2; exit 1; }

drift=0
n=0

# fd 3, not stdin: children inherit stdin and at least one of them reads it.
while IFS=$'\t' read -r name want <&3; do
  case "$name" in ""|\#*) continue ;; esac
  raw="$RAW_DIR/$name.raw"
  if [ ! -f "$raw" ]; then
    printf 'rot-check: %s: no fresh capture at %s\n' "$name" "$raw" >&2
    drift=1
    continue
  fi
  n=$((n + 1))
  got=$(python3 "$DEMO_DIR/render/scene_shape.py" -F $'\x1f' "$raw")
  if [ "$got" != "$want" ]; then
    printf 'rot-check: %s: the reader contract changed\n' "$name" >&2
    printf '  committed: %s\n' "$want" >&2
    printf '  now:       %s\n' "$got" >&2
    drift=1
  fi
done 3< "$SHAPE_TSV"

if [ "$drift" -ne 0 ]; then
  cat >&2 <<'MSG'

A column was renamed, added, removed or reordered, or a report key moved.
That is a real change to what pg_ash prints, and every image in README.md is
now a picture of an older API. Review it, then accept it deliberately with:

  make -C demos fixtures
MSG
  exit 5
fi

printf 'rot-check: %d reader contract(s) unchanged\n' "$n" >&2
