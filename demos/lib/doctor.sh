#!/usr/bin/env bash
#
# lib/doctor.sh — dependency probe, by capability tier.
#
# The point of tiers: the stills path is the one that must work everywhere, and
# it needs almost nothing (python3 + two pure-python wheels + a client). The
# raster and reel paths need progressively more. Telling somebody "install
# ffmpeg" when all they wanted was an SVG is how a harness gets a reputation
# for being unrunnable.
#
# doctor_tier <1|2|3>   check one tier; exit 2 naming exactly what is missing
# doctor_report         print the full picture, including which backends work
#
# Sourced, never executed.

# shellcheck source=lib/backend.sh
. "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)/backend.sh"

# Accumulated failures for the current probe.
ASH_DOCTOR_MISSING=""

_doc_need_cmd() {
  # $1 = command, $2 = hint
  if ash_have "$1"; then
    printf '  \033[32mok\033[0m    %s\n' "$1" >&2
  else
    printf '  \033[31mMISS\033[0m  %s  — %s\n' "$1" "$2" >&2
    ASH_DOCTOR_MISSING="$ASH_DOCTOR_MISSING $1"
  fi
}

_doc_need_py() {
  # $1 = import name, $2 = pip name
  if python3 -c "import $1" >/dev/null 2>&1; then
    printf '  \033[32mok\033[0m    python:%s\n' "$1" >&2
  else
    printf '  \033[31mMISS\033[0m  python:%s  — pip install %s\n' "$1" "$2" >&2
    ASH_DOCTOR_MISSING="$ASH_DOCTOR_MISSING python:$1"
  fi
}

_doc_need_file() {
  # $1 = path, $2 = hint
  if [ -f "$1" ]; then
    printf '  \033[32mok\033[0m    %s\n' "${1#$ASH_DEMO_DIR/}" >&2
  else
    printf '  \033[31mMISS\033[0m  %s  — %s\n' "${1#$ASH_DEMO_DIR/}" "$2" >&2
    ASH_DOCTOR_MISSING="$ASH_DOCTOR_MISSING ${1##*/}"
  fi
}

# _doc_any_cmd <label> <hint> <cmd...> — satisfied by ANY one of the commands.
_doc_any_cmd() {
  local label=$1 hint=$2; shift 2
  local cmd
  for cmd in "$@"; do
    if ash_have "$cmd"; then
      printf '  \033[32mok\033[0m    %s (%s)\n' "$label" "$cmd" >&2
      return 0
    fi
  done
  printf '  \033[31mMISS\033[0m  %s  — %s\n' "$label" "$hint" >&2
  ASH_DOCTOR_MISSING="$ASH_DOCTOR_MISSING $label"
  return 1
}

# ---------------------------------------------------------------------------
# Tier 1 — stills. Everything else is optional.
# ---------------------------------------------------------------------------
doctor_tier1() {
  printf '\033[1mtier 1 (stills: SVG from real query output)\033[0m\n' >&2
  _doc_need_cmd python3 'required for every path; nothing here uses awk or sed for widths'
  _doc_need_py  fontTools fonttools
  _doc_need_py  brotli brotli
  _doc_need_cmd psql 'PostgreSQL client (libpq)'
  _doc_need_cmd pgbench 'ships with the PostgreSQL client packages'
  _doc_need_file "$ASH_FONT_DIR/JetBrainsMono-Regular.ttf" \
    'the font is vendored under demos/fonts — a missing font is a hard failure, never a silent fallback'
  _doc_need_file "$ASH_FONT_DIR/JetBrainsMono-Bold.ttf" 'vendored, OFL-1.1'
  _doc_need_file "$ASH_THEME" 'the theme file is the only source of colour and geometry'
}

# ---------------------------------------------------------------------------
# Tier 2 — PNG rasterisation of the SVGs.
# ---------------------------------------------------------------------------
doctor_tier2() {
  printf '\033[1mtier 2 (raster: PNG at %sx)\033[0m\n' "$ASH_SCALE" >&2
  # `command -v` accepts an absolute path, so the macOS .app bundles (which are
  # never on PATH) go in the same candidate list rather than in a special case.
  _doc_any_cmd 'svg_rasteriser' \
    'install resvg, or any Chromium-family browser; or set ASH_SVG_ONLY=1' \
    resvg rsvg-convert chromium chromium-browser google-chrome \
    '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome' \
    '/Applications/Chromium.app/Contents/MacOS/Chromium' || true
}

# ---------------------------------------------------------------------------
# Tier 3 — the animated reel.
# ---------------------------------------------------------------------------
doctor_tier3() {
  printf '\033[1mtier 3 (reel: GIF + MP4)\033[0m\n' >&2
  _doc_need_cmd tmux 'terminal driver for prompt-synchronised typing'
  _doc_need_cmd asciinema 'terminal recorder'
  _doc_need_cmd agg 'asciicast -> frames; single static binary from its GitHub release'
  _doc_need_cmd ffmpeg 'frames -> gif/mp4'
  _doc_need_cmd gifsicle 'gif optimisation'
  _doc_need_py  PIL pillow
}

# ---------------------------------------------------------------------------
# Backends
# ---------------------------------------------------------------------------
doctor_backends() {
  printf '\033[1mbackends usable on this machine\033[0m\n' >&2
  if backend_probe_local; then
    # `show server_version` rather than select current_setting('...'): the
    # single quotes would have to survive a shell single-quoted string, and
    # doubling them there concatenates instead of escaping. It silently
    # produced an empty version string for one round of this file.
    printf '  \033[32mok\033[0m    local   — PostgreSQL %s\n' \
      "$(psql -X -d "$ASH_MAINT_DB" -tAc 'show server_version' 2>/dev/null)" >&2
  else
    printf '  \033[33mno\033[0m    local   — the ambient PG* settings do not reach a server\n' >&2
  fi
  if backend_probe_docker; then
    printf '  \033[32mok\033[0m    docker  — image postgres:%s is present locally\n' "$ASH_PG_MAJOR" >&2
  elif ash_have docker; then
    printf '  \033[33mno\033[0m    docker  — daemon or image postgres:%s unavailable (the harness never pulls)\n' "$ASH_PG_MAJOR" >&2
  else
    printf '  \033[33mno\033[0m    docker  — not installed\n' >&2
  fi
  printf '  \033[36m--\033[0m    remote  — set PG* and ASH_BACKEND=remote; the database name must match ash_demo*\n' >&2
}

# ---------------------------------------------------------------------------
# Entry points
# ---------------------------------------------------------------------------

# doctor_tier <n> — probe up to tier n and exit 2 if anything in it is missing.
doctor_tier() {
  local want=${1:-1}
  ASH_DOCTOR_MISSING=""
  doctor_tier1
  # `if`, not `[ ... ] && cmd`: under `set -e` a false test makes the compound
  # statement the script's last status and kills the shell.
  if [ "$want" -ge 2 ]; then doctor_tier2; fi
  if [ "$want" -ge 3 ]; then doctor_tier3; fi
  if [ -n "$ASH_DOCTOR_MISSING" ]; then
    ash_die 2 "missing dependencies for tier $want:$ASH_DOCTOR_MISSING"
  fi
  return 0
}

# doctor_report — the human-facing `make doctor`. Never fails on tiers 2/3;
# it reports. Only a broken tier 1 is fatal, because without it nothing works.
doctor_report() {
  ASH_DOCTOR_MISSING=""
  doctor_tier1
  local tier1_missing=$ASH_DOCTOR_MISSING

  ASH_DOCTOR_MISSING=""
  doctor_tier2
  local tier2_missing=$ASH_DOCTOR_MISSING

  ASH_DOCTOR_MISSING=""
  doctor_tier3
  local tier3_missing=$ASH_DOCTOR_MISSING

  doctor_backends

  printf '\n' >&2
  if [ -n "$tier2_missing" ]; then
    ash_warn "tier 2 unavailable:$tier2_missing  (ASH_SVG_ONLY=1 skips it)"
  fi
  if [ -n "$tier3_missing" ]; then
    ash_warn "tier 3 unavailable:$tier3_missing  (\`make demo\` needs it; \`make stills\` does not)"
  fi
  if [ -n "$tier1_missing" ]; then
    ash_die 2 "tier 1 is incomplete:$tier1_missing"
  fi
  printf '\033[32mdoctor: tier 1 complete — stills can be built.\033[0m\n' >&2
  return 0
}
