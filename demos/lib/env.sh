#!/usr/bin/env bash
#
# lib/env.sh — the single place where every ASH_* knob is resolved.
#
# Sourced (never executed) by every other script in demos/. It is deliberately
# side-effect free apart from exporting variables and defining a handful of tiny
# helpers: sourcing it twice must be harmless, because bin/ash-demo sources it
# once and then each sub-script sources it again in its own process.
#
# Contract: §2.1 of the build spec. Anything that reads a raw PG* variable
# outside lib/backend.sh is a bug — backend.sh owns the connection, this file
# owns the harness configuration.
#
# bash 3.2 compatible (macOS ships 3.2): no associative arrays, no `mapfile`,
# no `${var,,}`.

# Guard against double-sourcing. Re-sourcing is harmless but pointless, and
# skipping it keeps `ASH_*` values a caller exported by hand from being
# recomputed.
if [ "${ASH_ENV_SOURCED:-}" = "1" ]; then
  return 0 2>/dev/null || true
fi
ASH_ENV_SOURCED=1

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
#
# Resolve the demos/ directory from this file's own location. No `readlink -f`
# (GNU-only) and no `realpath` (not on stock macOS): `cd` + `pwd -P` is POSIX
# and resolves symlinks on the way.
ASH_LIB_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)
ASH_DEMO_DIR=$(cd "$ASH_LIB_DIR/.." && pwd -P)

# ---------------------------------------------------------------------------
# Optional local overrides: demos/env.local
# ---------------------------------------------------------------------------
#
# Sourced BEFORE any default is applied, so it can set anything below. It exists
# for one reason: the harness is developed and reviewed out-of-tree (in a
# scratch directory) before it is merged into the repo, and out there
# `demos/../sql/ash-install.sql` does not exist. Rather than making every
# invocation carry three `ASH_*=` prefixes, put them in one gitignored file.
#
# In a normal checkout this file is absent and nothing changes.
if [ -f "$ASH_DEMO_DIR/env.local" ]; then
  # shellcheck disable=SC1090,SC1091
  . "$ASH_DEMO_DIR/env.local"
fi

# ---------------------------------------------------------------------------
# Repository root
# ---------------------------------------------------------------------------
#
# The repo root is where sql/ash-install.sql (or devel/scripts/ash_sql_chain.py)
# lives. Normally that is demos/.., but the harness may be run from a copy that
# sits outside the repo, so walk up a few levels looking for the marker before
# falling back. Explicit ASH_REPO_ROOT always wins.
if [ -z "${ASH_REPO_ROOT:-}" ]; then
  _ash_probe=$ASH_DEMO_DIR
  _ash_depth=0
  while [ "$_ash_depth" -lt 5 ]; do
    _ash_probe=$(cd "$_ash_probe/.." && pwd -P)
    if [ -f "$_ash_probe/sql/ash-install.sql" ] \
       || [ -f "$_ash_probe/devel/scripts/ash_sql_chain.py" ]; then
      ASH_REPO_ROOT=$_ash_probe
      break
    fi
    [ "$_ash_probe" = "/" ] && break
    _ash_depth=$(( _ash_depth + 1 ))
  done
  : "${ASH_REPO_ROOT:=$(cd "$ASH_DEMO_DIR/.." && pwd -P)}"
  unset _ash_probe _ash_depth
fi

export ASH_LIB_DIR ASH_DEMO_DIR ASH_REPO_ROOT

# Working dir (gitignored) and the committed asset dir.
#
# ASH_ASSETS is demos/../assets — a SIBLING of demos/, not $ASH_REPO_ROOT/assets.
# Those are the same directory in a real checkout, and deliberately different
# when the harness is run out-of-tree against a read-only repo copy: the
# installer is read from ASH_REPO_ROOT, but nothing is ever written there.
: "${ASH_OUT:=$ASH_DEMO_DIR/out}"
: "${ASH_ASSETS:=$(cd "$ASH_DEMO_DIR/.." && pwd -P)/assets}"
: "${ASH_FONT_DIR:=$ASH_DEMO_DIR/fonts}"
: "${ASH_THEME:=$ASH_DEMO_DIR/theme/pg_ash.json}"
: "${ASH_SCENES:=$ASH_DEMO_DIR/scenes/scenes.tsv}"
export ASH_OUT ASH_ASSETS ASH_FONT_DIR ASH_THEME ASH_SCENES

# ---------------------------------------------------------------------------
# Installer resolution
# ---------------------------------------------------------------------------
#
# NEVER hardcode devel/sql/. During a development cycle the installer lives
# under devel/sql/; at release-stamp time it is promoted to sql/ash-install.sql.
# devel/scripts/ash_sql_chain.py is the repo's own authority on which is
# current, so ask it when it exists and fall back to the released path when it
# does not (which is the state of a freshly tagged tree).
if [ -z "${ASH_INSTALL_SQL:-}" ]; then
  if [ -x "$ASH_REPO_ROOT/devel/scripts/ash_sql_chain.py" ] \
     || [ -f "$ASH_REPO_ROOT/devel/scripts/ash_sql_chain.py" ]; then
    _ash_chain_path=$(cd "$ASH_REPO_ROOT" \
      && python3 devel/scripts/ash_sql_chain.py fresh-install-path 2>/dev/null \
      || true)
  else
    _ash_chain_path=""
  fi
  if [ -n "$_ash_chain_path" ]; then
    case "$_ash_chain_path" in
      /*) ASH_INSTALL_SQL=$_ash_chain_path ;;
       *) ASH_INSTALL_SQL=$ASH_REPO_ROOT/$_ash_chain_path ;;
    esac
  else
    ASH_INSTALL_SQL=$ASH_REPO_ROOT/sql/ash-install.sql
  fi
  unset _ash_chain_path
fi
export ASH_INSTALL_SQL

# ---------------------------------------------------------------------------
# Backend selection
# ---------------------------------------------------------------------------
: "${ASH_BACKEND:=local}"          # local | docker | remote
: "${ASH_DEMO_DB:=ash_demo}"       # MUST match the ash_demo* house-rule glob
: "${ASH_DEMO_CONTAINER:=ash_demo_pg}"
: "${ASH_PG_MAJOR:=18}"
# ASH_DEMO_PORT is left unset on purpose: lib/backend.sh probes 5500-5599 for a
# free port at container-create time. Hardcoding a port is how two harness runs
# collide on a shared machine.
export ASH_BACKEND ASH_DEMO_DB ASH_DEMO_CONTAINER ASH_PG_MAJOR

# ---------------------------------------------------------------------------
# Capture / render geometry
# ---------------------------------------------------------------------------
: "${ASH_COLS:=100}"   # hard column ceiling for BOTH capture paths (§2.5)
: "${ASH_ROWS:=30}"    # terminal rows, animation path only
: "${ASH_SCALE:=2}"    # PNG raster multiplier
export ASH_COLS ASH_ROWS ASH_SCALE

# ---------------------------------------------------------------------------
# Seeding
# ---------------------------------------------------------------------------
: "${ASH_VMIN:=24}"    # virtual minutes inside the query window
: "${ASH_SPM:=12}"     # samples per virtual minute => sample_interval = 60/SPM
export ASH_VMIN ASH_SPM

# Slack: virtual minutes of raw history seeded BEFORE the query window so the
# raw-retention guardrail in ash.top()/ash.samples() cannot trip as the seed
# ages between `make seed` and `make demo`.
: "${ASH_VMIN_SLACK:=4}"
export ASH_VMIN_SLACK

# ---------------------------------------------------------------------------
# Behaviour switches
# ---------------------------------------------------------------------------
# ASH_SKIP_SEED=1   reuse an existing seed (the 8-second iteration loop)
# ASH_KEEP_DB=1     do not drop the database on teardown
# ASH_SVG_ONLY=1    skip PNG/GIF rasterisation (the fast CI gate)
# ASH_REAL_TIME=1   skip restamping: seed in real wall-clock time (slow, but it
#                   is the honesty escape hatch for release-grade assets)
: "${ASH_SKIP_SEED:=}"
: "${ASH_KEEP_DB:=}"
: "${ASH_SVG_ONLY:=}"
: "${ASH_REAL_TIME:=}"
export ASH_SKIP_SEED ASH_KEEP_DB ASH_SVG_ONLY ASH_REAL_TIME

# The frozen-window file (§2.2). NOT sourced here — seed.sh writes it, and the
# capture paths source it explicitly so a missing file is a loud exit 3 rather
# than a silently empty $ASH_SINCE.
: "${ASH_WINDOW_ENV:=$ASH_OUT/window.env}"
export ASH_WINDOW_ENV

# ---------------------------------------------------------------------------
# Tiny shared helpers
# ---------------------------------------------------------------------------

# ash_log <msg...> — progress on stderr, so stdout stays capture-clean.
ash_log() { printf '  %s\n' "$*" >&2; }

# ash_step <msg...> — a top-level step marker.
ash_step() { printf '\033[36m==>\033[0m %s\n' "$*" >&2; }

# ash_warn <msg...>
ash_warn() { printf '\033[33mwarn:\033[0m %s\n' "$*" >&2; }

# ash_die <exit-code> <msg...> — the ONLY way this harness exits non-zero.
# Exit codes are the §2.6 vocabulary:
#   1 usage/config  2 missing dependency  3 backend  4 seed assertion
#   5 capture verification  6 render  7 animation sync
ash_die() {
  _code=$1; shift
  printf '\033[31merror:\033[0m %s\n' "$*" >&2
  exit "$_code"
}

# ash_have <cmd> — quiet "is this on PATH" probe.
ash_have() { command -v "$1" >/dev/null 2>&1; }

# ash_now_ms — millisecond wall clock.
# NEVER `date +%s%N`: BSD date has no %N and silently prints the literal "N".
ash_now_ms() { python3 -c 'import time; print(int(time.time()*1000))'; }

# ash_free_port <from> <to> — first TCP port in the range with no listener.
# Pure python so it behaves the same on BSD and GNU userlands (`lsof`/`ss`
# output formats differ; a bind() probe does not).
ash_free_port() {
  python3 - "$1" "$2" <<'PY'
import socket, sys
lo, hi = int(sys.argv[1]), int(sys.argv[2])
for port in range(lo, hi + 1):
    s = socket.socket()
    try:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind(("127.0.0.1", port))
    except OSError:
        continue
    finally:
        s.close()
    print(port)
    sys.exit(0)
sys.exit(1)
PY
}

mkdir -p "$ASH_OUT"
