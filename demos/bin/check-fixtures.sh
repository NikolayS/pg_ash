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

# A chart can contain more than n named series because ash.chart() also keeps
# every per-bucket leader. The legend must fold when that makes it wider than
# the screen, while a bar remains a single visual bucket.
python3 - "$DEMO_DIR/render" "$ASH_COLS" <<'PY' || fail=1
import sys

sys.path.insert(0, sys.argv[1])
from ansitable import fold, render
from dwidth import strip_ansi, width

cols = int(sys.argv[2])
sep = "\x1f"
legend_entries = [
    "\x1b[38;2;255;82;112m█\x1b[0m Lock:transactionid",
    "\x1b[38;2;255;184;77m▓\x1b[0m CPU*",
    "\x1b[38;2;81;191;255m░\x1b[0m Client:ClientRead",
    "\x1b[38;2;168;140;255m▒\x1b[0m Lock:tuple",
    "\x1b[38;2;236;102;255m▒\x1b[0m LWLock:WALWrite",
    "\x1b[38;2;170;170;170m·\x1b[0m Other",
]
legend = "  ".join(legend_entries)
bar = "█" * 40
screen = render(
    [
        sep.join(("bucket", "aas", "chart")),
        sep.join(("", "", legend)),
        sep.join(("00:00", "1.00", bar)),
    ],
    sep,
    "",
    max_width=cols,
)
widest = max(width(line) for line in screen.splitlines())
if widest > cols:
    sys.stderr.write(
        f"check: dynamic chart legend is {widest} columns (limit {cols})\n"
    )
    raise SystemExit(1)
if fold("█" * (cols + 1), cols) != ["█" * (cols + 1)]:
    sys.stderr.write("check: chart bar folded into a false second bucket\n")
    raise SystemExit(1)

# A swatch and its label form one semantic legend entry. Exercise every useful
# fold width: ANSI colour escapes are zero-width, and no entry may be split
# even when the whole legend has to wrap.
plain_entries = [strip_ansi(entry) for entry in legend_entries]
minimum = max(width(entry) for entry in legend_entries)
for budget in range(minimum, width(legend)):
    lines = fold(legend, budget)
    plain_lines = [strip_ansi(line) for line in lines]
    if any(width(line) > budget for line in lines):
        sys.stderr.write(
            f"check: chart legend exceeded its {budget}-column fold budget\n"
        )
        raise SystemExit(1)
    for entry in plain_entries:
        if not any(entry in line for line in plain_lines):
            sys.stderr.write(
                f"check: chart legend split swatch from label at width {budget}: "
                f"{entry!r}\n"
            )
            raise SystemExit(1)
PY

# Driver session ownership is tested with a shell-level tmux fake so this stays
# in the tier-1 job: `make check` must not gain a real tmux dependency. The fake
# models an owned detached session and rejects an identity that was tampered
# with after creation.
if ! (
  fake_root="$TMP/tmux-driver"
  mkdir -p "$fake_root"
  printf '%s\n' '$901' > "$fake_root/id"
  : > "$fake_root/calls"
  : > "$fake_root/name"
  : > "$fake_root/owner"
  printf '0\n' > "$fake_root/alive"

  tmux() {
    local command_name
    if [ "${1:-}" != "-L" ] || [ -z "${2:-}" ]; then
      printf 'bad-socket\n' >> "$fake_root/calls"
      return 90
    fi
    shift 2
    command_name="${1:-}"
    shift || true
    case "$command_name" in
      display-message)
        case "$*" in
          *'#{session_name}:#{session_id}:#{@pg_ash_driver_owner}'*)
            printf '%s:%s:%s\n' \
              "$(<"$fake_root/name")" "$(<"$fake_root/id")" \
              "$(<"$fake_root/owner")" ;;
          *'#{session_name}:#{session_id}'*)
            printf '%s:%s\n' \
              "$(<"$fake_root/name")" "$(<"$fake_root/id")" ;;
          *'#{session_id}'*)
            printf '%s\n' "$(<"$fake_root/id")" ;;
        esac
        ;;
      new-session)
        while [ "$#" -gt 0 ]; do
          if [ "$1" = "-s" ]; then
            shift
            printf '%s\n' "$1" > "$fake_root/name"
          fi
          shift
        done
        printf '1\n' > "$fake_root/alive"
        printf 'new\n' >> "$fake_root/calls"
        printf '%s\n' "$(<"$fake_root/id")"
        ;;
      has-session)
        [ "$(<"$fake_root/alive")" = "1" ]
        ;;
      kill-session)
        [ "${1:-}" = "-t" ] || return 91
        printf '%s\n' "${2:-}" > "$fake_root/kill-target"
        printf 'kill\n' >> "$fake_root/calls"
        printf '0\n' > "$fake_root/alive"
        ;;
      set-option)
        case "$*" in
          *'@pg_ash_driver_owner'*)
            [ "${fake_fail_owner:-0}" = "0" ] || return 93
            printf '%s\n' "${4:-}" > "$fake_root/owner" ;;
        esac
        ;;
      *) return 92 ;;
    esac
  }

  # shellcheck source=../lib/driver.sh
  . "$DEMO_DIR/lib/driver.sh"

  drv_start "ash-check" 100 20 "sleep 30"
  case "$DRV_SESSION" in
    ash-check-*-*) : ;;
    *) echo "check: driver session name is not collision-resistant: $DRV_SESSION" >&2
       exit 1 ;;
  esac
  [ "$(grep -c '^new$' "$fake_root/calls" || true)" -eq 1 ] || exit 1
  [ "$(grep -c '^bad-socket$' "$fake_root/calls" || true)" -eq 0 ] || {
    echo "check: driver escaped its private tmux socket" >&2
    exit 1
  }
  [ "$(grep -c '^kill$' "$fake_root/calls" || true)" -eq 0 ] || {
    echo "check: drv_start tried to kill a pre-existing session" >&2
    exit 1
  }

  drv_kill
  [ "$(grep -c '^kill$' "$fake_root/calls" || true)" -eq 1 ] || exit 1
  [ "$(<"$fake_root/kill-target")" = "$(<"$fake_root/id")" ] || {
    echo "check: drv_kill did not target the created session id" >&2
    exit 1
  }

  # Cleanup is idempotent, and a forged ownership record must fail without
  # issuing another kill.
  drv_kill
  [ "$(grep -c '^kill$' "$fake_root/calls" || true)" -eq 1 ] || exit 1
  DRV_SESSION_CREATED=1
  printf '1\n' > "$fake_root/alive"
  DRV_SESSION="ash-check-forged"
  DRV_SESSION_ID="$(<"$fake_root/id")"
  DRV_SESSION_OWNER="expected-owner"
  printf '%s\n' "$DRV_SESSION" > "$fake_root/name"
  printf 'foreign-owner\n' > "$fake_root/owner"
  if (drv_kill >/dev/null 2>&1); then
    echo "check: driver accepted an unowned cleanup target" >&2
    exit 1
  fi
  [ "$(grep -c '^kill$' "$fake_root/calls" || true)" -eq 1 ] || exit 1

  # An empty prefix is a configuration error, never a request for tmux's
  # implicit current-session target.
  DRV_SESSION_CREATED=0
  if (drv_start "" 100 20 "sleep 30" >/dev/null 2>&1); then
    echo "check: driver accepted an empty session prefix" >&2
    exit 1
  fi
  [ "$(grep -c '^new$' "$fake_root/calls" || true)" -eq 1 ] || exit 1

  # If stamping the ownership option fails after new-session succeeds, the
  # captured name+ID on the private socket are a provisional ownership proof.
  # Startup must roll that session back rather than leaking it.
  DRV_SESSION_CREATED=0
  fake_fail_owner=1
  kills_before=$(grep -c '^kill$' "$fake_root/calls" || true)
  if (drv_start "ash-stamp" 100 20 "sleep 30" >/dev/null 2>&1); then
    echo "check: driver ignored an ownership-stamp failure" >&2
    exit 1
  fi
  fake_fail_owner=0
  kills_after=$(grep -c '^kill$' "$fake_root/calls" || true)
  [ "$kills_after" -eq $((kills_before + 1)) ] || {
    echo "check: driver leaked a session after ownership-stamp failure" >&2
    exit 1
  }
  [ "$(<"$fake_root/alive")" = "0" ] || exit 1
) then
  echo "check: tmux driver ownership regression" >&2
  fail=1
fi

# INT and TERM must turn into conventional non-zero exits. EXIT is the sole
# cleanup owner; trapping all three signals with cleanup makes Bash resume the
# interrupted script and can turn Ctrl-C into a successful build.
python3 - "$DEMO_DIR/bin/record-demo.sh" <<'PY' || fail=1
import pathlib
import sys

source = pathlib.Path(sys.argv[1]).read_text()
required = (
    "trap cleanup EXIT",
    "trap 'exit 130' INT",
    "trap 'exit 143' TERM",
)
if any(line not in source for line in required):
    sys.stderr.write("check: record-demo signal traps do not preserve interruption status\n")
    raise SystemExit(1)
if "trap cleanup EXIT INT TERM" in source:
    sys.stderr.write("check: cleanup still swallows INT/TERM\n")
    raise SystemExit(1)
PY

# The stock postgres images declare PGDATA as an anonymous volume. Removing a
# harness-owned container without `docker rm -v` leaks that volume (and several
# hundred MiB per matrix entry), so exercise the exact cleanup argv without
# requiring a Docker daemon in the tier-1 check.
if ! (
  fake_docker_log="$TMP/docker-cleanup.args"
  docker() {
    printf '%s\n' "$@" >"$fake_docker_log"
  }
  # shellcheck source=../lib/backend.sh
  . "$DEMO_DIR/lib/backend.sh"
  _bk_remove_owned_container ash_demo_fixture
  expected=$(printf '%s\n' rm -f -v ash_demo_fixture)
  [ "$(<"$fake_docker_log")" = "$expected" ]
) then
  echo "check: docker cleanup must remove anonymous volumes" >&2
  fail=1
fi

# Database teardown must obey the same ownership rule as container teardown.
# A local database can predate the harness, and the remote backend necessarily
# connects to a database it did not create. The ash_demo* name guard narrows
# the target; it does not confer ownership.
if ! (
  fake_root="$TMP/database-cleanup"
  mkdir -p "$fake_root"
  fake_calls="$fake_root/calls"
  : > "$fake_calls"

  # shellcheck source=../lib/backend.sh
  . "$DEMO_DIR/lib/backend.sh"
  ASH_STATE_FILE="$fake_root/backend.state"
  ASH_DEMO_DB=ash_demo_ambient
  ASH_KEEP_DB=

  if (_bk_assert_db_glob 'ash_demo;unsafe' >/dev/null 2>&1); then
    echo "check: database guard accepted SQL metacharacters" >&2
    exit 1
  fi

  ash_psql_maint() {
    printf 'psql:%s\n' "$*" >> "$fake_calls"
  }
  _bk_terminate_demo_backends() {
    printf 'terminate:%s\n' "${1:-}" >> "$fake_calls"
  }
  write_state() {
    printf '%s\n' \
      "ASH_STATE_BACKEND=$1" \
      "ASH_STATE_OWNERSHIP=$2" \
      "ASH_STATE_DB=$3" \
      "ASH_STATE_CONTAINER=" \
      "ASH_STATE_PORT=" > "$ASH_STATE_FILE"
  }

  for backend_kind in local remote; do
    : > "$fake_calls"
    write_state "$backend_kind" reused "ash_demo_${backend_kind}_reused"
    backend_down >/dev/null 2>&1
    [ ! -s "$fake_calls" ] || {
      echo "check: $backend_kind teardown touched a reused database" >&2
      exit 1
    }
    [ ! -e "$ASH_STATE_FILE" ] || exit 1
  done

  # Mutable ambient PGDATABASE state may not redirect a valid ledger to some
  # other ash_demo* target. Refuse the mismatch and retain the ledger.
  : > "$fake_calls"
  write_state local created ash_demo_owned
  if (backend_down >/dev/null 2>&1); then
    echo "check: database teardown accepted an ambient/ledger mismatch" >&2
    exit 1
  fi
  [ ! -s "$fake_calls" ] || exit 1
  [ -f "$ASH_STATE_FILE" ] || exit 1

  # With the exact recorded target selected, both termination and DROP use it.
  ASH_DEMO_DB=ash_demo_owned
  backend_down >/dev/null 2>&1
  grep -Fx 'terminate:ash_demo_owned' "$fake_calls" >/dev/null
  grep -F 'drop database if exists "ash_demo_owned" with (force)' \
    "$fake_calls" >/dev/null

  # A failed drop must stay loud and retain the ledger so a later `make down`
  # can retry instead of forgetting what this run created.
  ASH_DEMO_DB=ash_demo_drop_failure
  write_state local created ash_demo_drop_failure
  ash_psql_maint() { return 1; }
  if (backend_down >/dev/null 2>&1); then
    echo "check: database teardown swallowed a failed drop" >&2
    exit 1
  fi
  [ -f "$ASH_STATE_FILE" ]
) then
  echo "check: database teardown ownership regression" >&2
  fail=1
fi

# The real-time escape hatch keeps each pgbench process alive for its whole
# wall-clock phase. The compressed-path two-minute seatbelt would otherwise
# terminate the 12-minute baseline early and leave ten minutes of idle samples.
if ! (
  unset ASH_LOAD_CAP
  ASH_REAL_TIME=1
  export ASH_REAL_TIME
  # shellcheck source=../lib/seed.sh
  . "$DEMO_DIR/lib/seed.sh"
  [ "$ASH_LOAD_CAP" -gt $((ASH_PH_BASELINE * 60)) ]
) then
  echo "check: ASH_REAL_TIME load cap cannot cover the baseline phase" >&2
  fail=1
fi

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
