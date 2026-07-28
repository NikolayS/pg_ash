#!/usr/bin/env bash
# Issue #202: a failed 1.5-to-2.0 config normalization must not publish 2.0.
#
# Use the immutable v1.5 payload, not the development chain: historical shim
# migrations deliberately replay the current installer and therefore cannot
# establish a real released-1.5 precondition after a release stamp.

set -Eeuo pipefail
IFS=$'\n\t'

readonly REQUESTED_CASE="${1:-all}"
if [[ "${REQUESTED_CASE}" != "all" \
  && "${REQUESTED_CASE}" != "custom-trigger" \
  && "${REQUESTED_CASE}" != "dependent-view" ]]; then
  printf 'usage: %s [all|custom-trigger|dependent-view]\n' "$0" >&2
  exit 2
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly SCRIPT_DIR
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
readonly REPO_ROOT
TEST_TMP_DIR="$(mktemp -d)"
readonly TEST_TMP_DIR
readonly ARCHIVE_ROOT="${TEST_TMP_DIR}/v1.5"
readonly TEST_DATABASE="ash_config_atomicity_202_${BASHPID}"
if [[ ! "${TEST_DATABASE}" =~ ^[a-z0-9_]+$ ]]; then
  printf 'unsafe generated test database name: %s\n' "${TEST_DATABASE}" >&2
  exit 2
fi

readonly -a PSQL_MAINTENANCE=(
  psql
  --no-psqlrc
  --host="${PGHOST:-localhost}"
  --username="${PGUSER:-postgres}"
  --dbname="${PGDATABASE:-postgres}"
  --set=ON_ERROR_STOP=1
  --set=VERBOSITY=terse
)

readonly -a PSQL=(
  psql
  --no-psqlrc
  --host="${PGHOST:-localhost}"
  --username="${PGUSER:-postgres}"
  --dbname="${TEST_DATABASE}"
  --set=ON_ERROR_STOP=1
  --set=VERBOSITY=terse
)

cleanup_database() {
  "${PSQL[@]}" --quiet >/dev/null 2>&1 <<'SQL' || true
drop event trigger if exists fail_ash_install_transaction_marker;
drop function if exists public.fail_ash_install_transaction_marker();
do $cleanup$
begin
  if pg_catalog.to_regprocedure('ash.uninstall(text)') is not null then
    execute 'select ash.uninstall(''yes'')';
  end if;
exception
  when others then
    raise warning 'ash.uninstall cleanup failed: %', sqlerrm;
end
$cleanup$;
drop view if exists public.ash_config_atomicity_dependency;
drop function if exists public.ash_config_atomicity_trigger();
drop schema if exists ash cascade;
SQL
}

drop_test_database() {
  "${PSQL_MAINTENANCE[@]}" \
    --quiet \
    --command="drop database if exists ${TEST_DATABASE} with (force);" \
    >/dev/null 2>&1 || true
}

cleanup() {
  cleanup_database
  drop_test_database
  if [[ -d "${TEST_TMP_DIR}" ]]; then
    rm -rf -- "${TEST_TMP_DIR}"
  fi
}
trap cleanup EXIT

drop_test_database
"${PSQL_MAINTENANCE[@]}" \
  --quiet \
  --command="create database ${TEST_DATABASE};"

mkdir -p "${ARCHIVE_ROOT}"
git -C "${REPO_ROOT}" archive v1.5 -- sql | tar -x -C "${ARCHIVE_ROOT}"

readonly -a V15_CHAIN=(
  "${ARCHIVE_ROOT}/sql/ash-1.0.sql"
  "${ARCHIVE_ROOT}/sql/ash-1.0-to-1.1.sql"
  "${ARCHIVE_ROOT}/sql/ash-1.1-to-1.2.sql"
  "${ARCHIVE_ROOT}/sql/ash-1.2-to-1.3.sql"
  "${ARCHIVE_ROOT}/sql/ash-1.3-to-1.4.sql"
  "${ARCHIVE_ROOT}/sql/ash-1.4-to-1.5.sql"
)

install_actual_v15_chain() {
  local chain_log=$1
  local sql_file

  : >"${chain_log}"
  for sql_file in "${V15_CHAIN[@]}"; do
    if ! "${PSQL[@]}" --quiet --file="${sql_file}" >>"${chain_log}" 2>&1; then
      sed -n '1,240p' "${chain_log}" >&2
      return 1
    fi
  done
}

config_state() {
  "${PSQL[@]}" --tuples-only --no-align --command="
    select pg_catalog.concat_ws(
      '|',
      (select version from ash.config where singleton),
      (select value from ash.status() where metric = 'version'),
      (
        select attribute.attnum
        from pg_catalog.pg_attribute as attribute
        where attribute.attrelid = 'ash.config'::regclass
          and attribute.attname = 'num_partitions'
          and not attribute.attisdropped
      )
    );
  "
}

snapshot_schema() {
  local output_file=$1
  "${PSQL[@]}" --quiet \
    --file="${REPO_ROOT}/devel/tests/schema_snapshot.sql" >"${output_file}"
}

assert_failed_upgrade_rolled_back() {
  local case_name=$1
  local expected_error=$2
  local customization_check=$3
  local migration_log="${TEST_TMP_DIR}/${case_name}.migration.log"
  local before_snapshot="${TEST_TMP_DIR}/${case_name}.before.snapshot"
  local after_snapshot="${TEST_TMP_DIR}/${case_name}.after.snapshot"
  local snapshot_diff="${TEST_TMP_DIR}/${case_name}.snapshot.diff"
  local before_state
  local after_state
  local migration_exit
  local customization_survives

  before_state="$(config_state)"
  printf 'Issue #202 %s precondition: %s\n' "${case_name}" "${before_state}"
  if [[ "${before_state}" != "1.5|1.5|11" ]]; then
    printf 'unexpected released-v1.5 precondition: %s\n' "${before_state}" >&2
    return 1
  fi
  snapshot_schema "${before_snapshot}"

  set +e
  "${PSQL[@]}" \
    --file="${REPO_ROOT}/sql/ash-1.5-to-2.0.sql" \
    >"${migration_log}" 2>&1
  migration_exit=$?
  set -e

  printf 'Issue #202 %s migration_exit=%s\n' \
    "${case_name}" "${migration_exit}"
  if ((migration_exit != 3)); then
    sed -n '1,240p' "${migration_log}" >&2
    printf 'expected psql exit 3, got %s\n' "${migration_exit}" >&2
    return 1
  fi
  if ! grep -F -- "${expected_error}" "${migration_log}" >/dev/null; then
    sed -n '1,240p' "${migration_log}" >&2
    printf 'expected migration error not found: %s\n' "${expected_error}" >&2
    return 1
  fi

  # Use a new psql connection: no aborted transaction or session-local state
  # can make the post-failure observation look safer than the database is.
  after_state="$(config_state)"
  printf 'Issue #202 %s after failure: %s\n' "${case_name}" "${after_state}"
  if [[ "${after_state}" != "${before_state}" ]]; then
    printf 'failed migration changed release state: before=%s after=%s\n' \
      "${before_state}" "${after_state}" >&2
    return 1
  fi

  snapshot_schema "${after_snapshot}"
  if ! diff -u \
    "${before_snapshot}" "${after_snapshot}" >"${snapshot_diff}"; then
    sed -n '1,240p' "${snapshot_diff}" >&2
    printf 'failed migration changed the ash schema\n' >&2
    return 1
  fi

  customization_survives="$("${PSQL[@]}" \
    --tuples-only \
    --no-align \
    --command="${customization_check}")"
  if [[ "${customization_survives}" != "t" ]]; then
    printf 'failed migration did not preserve the blocking customization\n' >&2
    return 1
  fi

  printf 'Issue #202 %s rollback PASSED\n' "${case_name}"
}

assert_transaction_marker_does_not_leak() {
  local interactive_log="${TEST_TMP_DIR}/transaction-marker.interactive.log"
  local before_snapshot="${TEST_TMP_DIR}/transaction-marker.before.snapshot"
  local after_snapshot="${TEST_TMP_DIR}/transaction-marker.after.snapshot"
  local snapshot_diff="${TEST_TMP_DIR}/transaction-marker.snapshot.diff"
  local before_state
  local after_state
  local marker_after

  cleanup_database
  install_actual_v15_chain "${TEST_TMP_DIR}/transaction-marker.chain.log"
  "${PSQL[@]}" --quiet <<'SQL'
create function public.fail_ash_install_transaction_marker()
returns event_trigger
language plpgsql
as $event_trigger$
declare
  command record;
begin
  for command in
    select *
    from pg_catalog.pg_event_trigger_ddl_commands()
  loop
    if command.object_type = 'function'
       and command.schema_name = 'ash'
       and command.object_identity like 'ash.summary(%' then
      if coalesce(
        pg_catalog.current_setting(
          'pg_ash.install_in_migration_transaction',
          true
        ),
        ''
      ) = 'on' then
        raise exception 'forced migration installer-phase failure';
      else
        raise exception 'forced direct installer retry failure';
      end if;
    end if;
  end loop;
end
$event_trigger$;

create event trigger fail_ash_install_transaction_marker
on ddl_command_end
when tag in ('CREATE FUNCTION')
execute function public.fail_ash_install_transaction_marker();
SQL

  before_state="$(config_state)"
  if [[ "${before_state}" != "1.5|1.5|11" ]]; then
    printf 'unexpected transaction-marker precondition: %s\n' \
      "${before_state}" >&2
    return 1
  fi
  snapshot_schema "${before_snapshot}"

  # A pseudo-terminal makes psql follow its interactive ON_ERROR_STOP rule:
  # each failed \i returns to the prompt, so ROLLBACK and the direct retry run
  # in the same client process. The first failure must not leave client state
  # that makes the direct installer lose its owned transaction.
  printf '%s\n' \
    '\echo ISSUE202_MIGRATION_PHASE' \
    "\\i '${REPO_ROOT}/sql/ash-1.5-to-2.0.sql'" \
    'rollback;' \
    '\echo ISSUE202_DIRECT_PHASE' \
    "\\i '${REPO_ROOT}/sql/ash-install.sql'" \
    'rollback;' \
    '\q' |
    PGHOST="${PGHOST:-localhost}" \
    PGUSER="${PGUSER:-postgres}" \
    PGDATABASE="${TEST_DATABASE}" \
    script \
      --quiet \
      --return \
      --command "psql --no-psqlrc" \
      /dev/null >"${interactive_log}" 2>&1

  if ! grep -F -- \
    "forced migration installer-phase failure" \
    "${interactive_log}" >/dev/null; then
    sed -n '1,240p' "${interactive_log}" >&2
    printf 'interactive migration did not reach the installer-phase failure\n' \
      >&2
    return 1
  fi
  if ! grep -F -- \
    "forced direct installer retry failure" \
    "${interactive_log}" >/dev/null; then
    sed -n '1,240p' "${interactive_log}" >&2
    printf 'same-session direct installer retry did not reach its failure\n' \
      >&2
    return 1
  fi

  after_state="$(config_state)"
  printf 'Issue #202 same-session installer failures: %s\n' "${after_state}"
  if [[ "${after_state}" != "${before_state}" ]]; then
    sed -n '1,240p' "${interactive_log}" >&2
    printf 'installer transaction marker leaked: before=%s after=%s\n' \
      "${before_state}" "${after_state}" >&2
    return 1
  fi

  snapshot_schema "${after_snapshot}"
  if ! diff -u \
    "${before_snapshot}" "${after_snapshot}" >"${snapshot_diff}"; then
    sed -n '1,240p' "${snapshot_diff}" >&2
    printf 'same-session installer failures changed the ash schema\n' >&2
    return 1
  fi

  marker_after="$("${PSQL[@]}" \
    --tuples-only \
    --no-align \
    --command="
      select coalesce(
        pg_catalog.current_setting(
          'pg_ash.install_in_migration_transaction',
          true
        ),
        ''
      );
    ")"
  if [[ -n "${marker_after}" ]]; then
    printf 'transaction-local installer marker survived rollback: %s\n' \
      "${marker_after}" >&2
    return 1
  fi

  printf 'Issue #202 same-session transaction-marker rollback PASSED\n'
}

if [[ "${REQUESTED_CASE}" == "all" \
  || "${REQUESTED_CASE}" == "custom-trigger" ]]; then
  cleanup_database
  install_actual_v15_chain "${TEST_TMP_DIR}/custom-trigger.chain.log"
  "${PSQL[@]}" --quiet <<'SQL'
create function public.ash_config_atomicity_trigger()
returns trigger
language plpgsql
as $trigger$
begin
  return null;
end
$trigger$;

create trigger ash_config_atomicity_custom
after update on ash.config
for each row
execute function public.ash_config_atomicity_trigger();
SQL
  assert_failed_upgrade_rolled_back \
    "custom-trigger" \
    "cannot normalize ash.config: unsupported custom triggers are present" \
    "select exists (
       select
       from pg_catalog.pg_trigger
       where tgrelid = 'ash.config'::regclass
         and tgname = 'ash_config_atomicity_custom'
         and not tgisinternal
     );"
fi

if [[ "${REQUESTED_CASE}" == "all" \
  || "${REQUESTED_CASE}" == "dependent-view" ]]; then
  cleanup_database
  install_actual_v15_chain "${TEST_TMP_DIR}/dependent-view.chain.log"
  "${PSQL[@]}" --quiet <<'SQL'
create view public.ash_config_atomicity_dependency as
select version
from ash.config;
SQL
  assert_failed_upgrade_rolled_back \
    "dependent-view" \
    "cannot drop table ash.config_ordinal_legacy because other objects depend on it" \
    "select pg_catalog.to_regclass(
       'public.ash_config_atomicity_dependency'
     ) is not null;"
fi

if [[ "${REQUESTED_CASE}" == "all" ]]; then
  assert_transaction_marker_does_not_leak
fi

cleanup_database
printf 'Issue #202 failed-normalization release-state atomicity PASSED\n'
