#!/usr/bin/env bash
# Uses libpq connection environment; creates and removes only its own database.
set -euo pipefail

example_database="pgash_llm_${$}_${RANDOM}"
example_created=false
cleanup() {
  if "$example_created"; then
    dropdb --if-exists --force "$example_database"
  fi
}
trap cleanup EXIT
createdb "$example_database"
example_created=true
export PGDATABASE="$example_database"
installer_path="$(python3 devel/scripts/ash_sql_chain.py fresh-install-path)"
psql -X -v ON_ERROR_STOP=1 -f "$installer_path" >/dev/null
# No optional extensions in this database: query IDs must still work.
python3 devel/tests/llm_example_live.py
