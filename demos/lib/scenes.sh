#!/usr/bin/env bash
#
# lib/scenes.sh — parse, validate and expand scenes/scenes.tsv.
#
# Shared by BOTH capture paths. bin/capture-stills.sh and bin/record-demo.sh
# must get their SQL from here and nowhere else; the moment one of them
# hardcodes a query the stills and the reel can disagree about what pg_ash
# says, which is precisely the failure this harness exists to remove.
#
# Public API (all bash 3.2 safe, all print to stdout):
#
#   scenes_validate                 validate the file; exit 1 on any violation
#   scenes_names                    every scene name, file order, one per line
#   scenes_reel_names               reel scenes only, ascending reel_order
#   scenes_field <name> <field>     hold | reel_order | title | markers | sql
#   scenes_sql <name>               SQL with the window literals substituted
#   scenes_template <name>          SQL with $SINCE etc. INTACT — this is what
#                                   the prompt line renders (§3.4); an expanded
#                                   ISO timestamp on the prompt line is noise
#                                   and was a named defect in a prototype
#   scenes_markers <name>           markers, one per line
#
# Sourced, never executed.

# shellcheck source=lib/env.sh
. "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)/env.sh"

# ---------------------------------------------------------------------------
# Reading
# ---------------------------------------------------------------------------
#
# One awk-free reader: `while IFS=$'\t' read -r` splits on tabs and nothing
# else, which is what a TSV means. Blank lines and `#` comments are skipped.
# Every consumer goes through _scenes_each so the skipping rules live once.
_scenes_each() {
  # Feeds "name<TAB>hold<TAB>order<TAB>title<TAB>markers<TAB>sql" on stdout.
  [ -f "$ASH_SCENES" ] || ash_die 1 "scene file not found: $ASH_SCENES"
  local name hold order title markers sql
  while IFS=$'\t' read -r name hold order title markers sql; do
    case "$name" in
      ''|'#'*) continue ;;
    esac
    printf '%s\t%s\t%s\t%s\t%s\t%s\n' \
      "$name" "$hold" "$order" "$title" "$markers" "$sql"
  done <"$ASH_SCENES"
}

scenes_names() {
  _scenes_each | while IFS=$'\t' read -r name _rest; do
    printf '%s\n' "$name"
  done
}

# Reel scenes, ascending reel_order. `sort -n` on a leading numeric key: no
# locale trouble because the key is digits only.
scenes_reel_names() {
  _scenes_each | while IFS=$'\t' read -r name hold order _rest; do
    case "$order" in
      0|'') continue ;;
    esac
    printf '%s\t%s\n' "$order" "$name"
  done | sort -n | while IFS=$'\t' read -r _order name; do
    printf '%s\n' "$name"
  done
}

# scenes_field <name> <hold|reel_order|title|markers|sql>
scenes_field() {
  local want=$1 field=$2 found=0
  local name hold order title markers sql
  while IFS=$'\t' read -r name hold order title markers sql; do
    [ "$name" = "$want" ] || continue
    found=1
    case "$field" in
      hold)       printf '%s\n' "$hold" ;;
      reel_order) printf '%s\n' "$order" ;;
      title)      printf '%s\n' "$title" ;;
      markers)    printf '%s\n' "$markers" ;;
      sql)        printf '%s\n' "$sql" ;;
      *) ash_die 1 "scenes_field: unknown field '$field'" ;;
    esac
    break
  done <<EOF
$(_scenes_each)
EOF
  [ "$found" = "1" ] || ash_die 1 "no such scene: $want"
}

scenes_template() { scenes_field "$1" sql; }

scenes_markers() {
  # markers is comma-separated; emit one per line, leading/trailing space
  # trimmed. A marker is matched as a LITERAL substring by verify.sh.
  scenes_field "$1" markers | tr ',' '\n' | while IFS= read -r marker; do
    marker=${marker#"${marker%%[![:space:]]*}"}
    marker=${marker%"${marker##*[![:space:]]}"}
    [ -n "$marker" ] && printf '%s\n' "$marker"
  done
  return 0
}

# ---------------------------------------------------------------------------
# Expansion
# ---------------------------------------------------------------------------
#
# $SINCE and friends become SQL literals. Done in python, not sed: a sed RHS
# containing a timestamp is fine, but the moment anything in this harness needs
# an escape in a replacement the BSD/GNU divergence bites, and the rule is
# simply "no sed" (§10).
#
# Longest name first, always: a naive left-to-right pass would rewrite the
# SINCE inside $BASE_SINCE and leave a stray `$BASE_'2026-...'`.
#
# The python body is a QUOTED heredoc, not `python3 -c '...'`: the code has to
# contain single quotes (it builds SQL literals), and a single-quoted shell
# string cannot hold one.
scenes_sql() {
  local template
  template=$(scenes_template "$1")
  ASH_TEMPLATE="$template" python3 - <<'ASH_SCENES_PY'
import os, sys

names = ["BASE_SINCE", "BASE_UNTIL", "STORM_SINCE", "STORM_UNTIL",
         "STORM_EVENT", "STORM_QUERY_ID", "SINCE", "UNTIL"]
# Longest first so $BASE_SINCE is never split into $BASE_ + $SINCE.
names.sort(key=len, reverse=True)

sql = os.environ["ASH_TEMPLATE"]
missing = []
for name in names:
    token = "$" + name
    if token not in sql:
        continue
    value = os.environ.get("ASH_" + name)
    if not value:
        missing.append(token)
        continue
    if name == "STORM_QUERY_ID":
        # numeric: a quoted literal would not match top()s bigint argument
        literal = value
    else:
        literal = "'" + value.replace("'", "''") + "'"
    sql = sql.replace(token, literal)
if missing:
    sys.stderr.write("scenes: unresolved window variable(s): %s\n"
                     % ", ".join(missing))
    sys.exit(3)
sys.stdout.write(sql + "\n")
ASH_SCENES_PY
}

# ---------------------------------------------------------------------------
# Validation (§2.3) — exit 1 on any violation
# ---------------------------------------------------------------------------
scenes_validate() {
  local errors=0
  local name hold order title markers sql seen=""

  while IFS=$'\t' read -r name hold order title markers sql; do
    case "$name" in
      *[!a-z0-9_]*|'')
        printf 'scenes: bad scene name "%s" (want [a-z0-9_]+)\n' "$name" >&2
        errors=$((errors + 1)) ;;
    esac

    case " $seen " in
      *" $name "*)
        printf 'scenes: duplicate scene name "%s"\n' "$name" >&2
        errors=$((errors + 1)) ;;
    esac
    seen="$seen $name"

    case "$hold" in
      ''|*[!0-9.]*)
        printf 'scenes[%s]: hold "%s" is not a number\n' "$name" "$hold" >&2
        errors=$((errors + 1)) ;;
    esac

    case "$order" in
      ''|*[!0-9]*)
        printf 'scenes[%s]: reel_order "%s" is not an integer\n' "$name" "$order" >&2
        errors=$((errors + 1)) ;;
    esac

    if [ -z "$title" ]; then
      printf 'scenes[%s]: empty title\n' "$name" >&2
      errors=$((errors + 1))
    fi

    if [ -z "$markers" ]; then
      printf 'scenes[%s]: no markers — a scene with no marker cannot fail, and a check that cannot fail is not a check\n' "$name" >&2
      errors=$((errors + 1))
    fi

    if [ -z "$sql" ]; then
      printf 'scenes[%s]: empty sql\n' "$name" >&2
      errors=$((errors + 1))
    fi

    # `select *` is banned: an unprojected reader blows the 100-column budget
    # the moment pg_ash adds a column, and it does so silently.
    case "$sql" in
      *'select *'*|*'SELECT *'*)
        printf 'scenes[%s]: `select *` is banned — project the columns you want\n' "$name" >&2
        errors=$((errors + 1)) ;;
    esac

    # now() in scene SQL destroys the frozen-window contract.
    case "$sql" in
      *now\(\)*|*clock_timestamp\(\)*)
        printf 'scenes[%s]: scene SQL may not call now()/clock_timestamp() — use the window literals\n' "$name" >&2
        errors=$((errors + 1)) ;;
    esac
  done <<EOF
$(_scenes_each)
EOF

  _scenes_check_banned_readers || errors=$((errors + 1))

  [ "$errors" = "0" ] || ash_die 1 "$errors scene-file violation(s)"
  return 0
}

# _scenes_check_banned_readers — the v1.x colour readers that 2.0 REMOVED must
# not be referenced anywhere under demos/. The landing page shipped screenshots
# of one of them for an entire release; a grep is cheap insurance.
#
# The pattern is assembled from fragments on purpose: written out whole, this
# very file would match it and the check would fail on itself.
_scenes_check_banned_readers() {
  local pattern hits
  pattern="top_wa""its|query_wa""its|top_by_t""ype|timeline_ch""art"
  # `grep -rl ... || true`, never `grep -q` behind a pipe: a successful -q
  # SIGPIPEs its writer and `pipefail` then inverts the result (§2.6).
  hits=$(grep -rlE "$pattern" "$ASH_DEMO_DIR" \
           --exclude-dir=out --exclude-dir=.git 2>/dev/null || true)
  if [ -n "$hits" ]; then
    printf 'scenes: removed v1.x reader referenced in:\n%s\n' "$hits" >&2
    return 1
  fi
  return 0
}
