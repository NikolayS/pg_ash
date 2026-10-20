#!/usr/bin/env bash
# Uses libpq connection environment; creates and removes only its own database.
set -euo pipefail

example_database="pgash_llm_${$}_${RANDOM}"
example_login="${example_database}_login"
example_owner="${example_database}_owner"
example_password="$(python3 -c 'import secrets; print(secrets.token_hex(24))')"
example_created=false
roles_created=false
cleanup() {
  local status=$?
  if "$example_created"; then
    dropdb --if-exists --force "$example_database" || status=$?
  fi
  if "$roles_created"; then
    psql -X -v ON_ERROR_STOP=1 -d postgres \
      -c "drop role $example_login, $example_owner" || status=$?
  fi
  return "$status"
}
trap cleanup EXIT
createdb "$example_database"
example_created=true
export PGDATABASE="$example_database"
export PGOPTIONS="${PGOPTIONS:+$PGOPTIONS }-c compute_query_id=on"
installer_path="$(python3 devel/scripts/ash_sql_chain.py fresh-install-path)"
psql -X -v ON_ERROR_STOP=1 -f "$installer_path" >/dev/null
# No optional extensions in this database: query IDs must still work.
python3 devel/tests/test_llm_example_cleanup.py
python3 devel/tests/llm_example_live.py

# A login assuming the schema owner must still signal its own server sessions.
dropdb --force "$example_database"
example_created=false
psql -X -v ON_ERROR_STOP=1 -d postgres \
  -v login="$example_login" -v owner="$example_owner" -v password="$example_password" <<'SQL'
begin;
create role :"owner";
create role :"login" login password :'password';
grant :"owner" to :"login";
grant pg_monitor to :"owner";
commit;
SQL
roles_created=true
createdb --owner="$example_owner" "$example_database"
example_created=true
psql -X -v ON_ERROR_STOP=1 -d postgres \
  -c "alter database $example_database set compute_query_id = on"
(
  export PGUSER="$example_login" PGPASSWORD="$example_password"
  export PGOPTIONS="-c role=$example_owner"
  psql -X -v ON_ERROR_STOP=1 <<'SQL'
do $$
begin
  assert current_user <> session_user;
  assert not (select rolsuper from pg_roles where rolname = session_user);
  assert not (select rolsuper from pg_roles where rolname = current_user);
  assert not pg_has_role(current_user, session_user, 'USAGE');
  assert not pg_has_role(current_user, 'pg_signal_backend', 'USAGE');
end $$;
SQL
  psql -X -v ON_ERROR_STOP=1 -f "$installer_path" >/dev/null
  python3 devel/tests/test_llm_example_cleanup.py
  python3 devel/tests/llm_example_live.py
)
