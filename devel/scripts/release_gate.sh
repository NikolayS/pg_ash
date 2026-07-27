#!/usr/bin/env bash
# Run the pg_ash release-gate surfaces against disposable Docker PostgreSQL.
#
# Usage:
#   devel/scripts/release_gate.sh <14|15|16|17|18|19beta2> [surface|all]
#   PG_MAJORS="14 15 16 17 18 19beta2" devel/scripts/release_gate.sh all
#
# The GitHub Actions workflow remains the canonical source for the large
# regression and upgrade assertion sets. ci_step_script.py selects its exact
# run blocks, and this script executes every selected block in a fresh Bash
# process, matching GitHub Actions' process boundaries.

set -Eeuo pipefail
IFS=$'\n\t'

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly SCRIPT_DIR
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
readonly REPO_ROOT
readonly DOCKERFILE="${REPO_ROOT}/devel/docker/Dockerfile"
readonly CHAIN_HELPER="${SCRIPT_DIR}/ash_sql_chain.py"
readonly CI_STEP_HELPER="${SCRIPT_DIR}/ci_step_script.py"
readonly DEFAULT_MAJORS="14 15 16 17 18 19beta2"
readonly IMAGE_REPOSITORY="pg-ash-release-gate"
readonly POSTGRES_USER="postgres"
readonly POSTGRES_DATABASE="postgres"
readonly POSTGRES_PASSWORD="pg_ash_release_gate"

readonly -a SUPPORTED_MAJORS=(14 15 16 17 18 19beta2)
readonly -a SURFACES=(
  fresh-install
  upgrade-chain
  features
  degraded-no-cron
  degraded-no-pgss
  degraded-neither
  cron-path
)

declare -a owned_containers=()
declare -a owned_workers=()
output_dir=""
summary_file=""
surface_temp_dir=""
container_name=""
container_id=""
image_name=""
any_failure=0

err() {
  printf 'release_gate: %s\n' "$*" >&2
}

usage() {
  cat <<'EOF'
Usage:
  devel/scripts/release_gate.sh <major> [surface|all]
  devel/scripts/release_gate.sh all [surface|all]

Supported majors:
  14 15 16 17 18 19beta2

Supported surfaces:
  fresh-install
  upgrade-chain
  features
  degraded-no-cron
  degraded-no-pgss
  degraded-neither
  cron-path

Environment:
  PG_MAJORS                 Space-separated majors for the "all" command.
  RELEASE_GATE_JOBS         Parallel major workers (default: 2).
  RELEASE_GATE_PULL         Set to 0 to skip docker pull (default: 1).
  RELEASE_GATE_OUTPUT_DIR   Directory for logs and the default summary.
  RELEASE_GATE_SUMMARY      Path for the machine-readable TSV summary.
EOF
}

cleanup() {
  local exit_code=$?
  local cleanup_failed=0
  local id
  local owned_id
  local pid
  local candidate_is_owned=0
  local -a cleanup_ids=("${owned_containers[@]}")

  trap - EXIT INT TERM

  for pid in "${owned_workers[@]}"; do
    kill -TERM "${pid}" >/dev/null 2>&1 || true
  done
  for pid in "${owned_workers[@]}"; do
    wait "${pid}" >/dev/null 2>&1 || true
  done

  if [[ "${container_id}" =~ ^[a-f0-9]{12,64}$ ]]; then
    for owned_id in "${cleanup_ids[@]}"; do
      if [[ "${owned_id}" == "${container_id}" ]]; then
        candidate_is_owned=1
        break
      fi
    done
    if ((candidate_is_owned == 0)); then
      cleanup_ids+=("${container_id}")
    fi
  fi

  for id in "${cleanup_ids[@]}"; do
    if ! docker rm --force --volumes "${id}" >/dev/null 2>&1; then
      err "could not tear down owned container ${id}"
      cleanup_failed=1
    fi
  done
  if ((cleanup_failed != 0 && exit_code == 0)); then
    exit_code=1
  fi
  exit "${exit_code}"
}

signal_exit() {
  local exit_code=$1

  trap - INT TERM
  exit "${exit_code}"
}

require_commands() {
  local command_name
  local -a commands=(
    awk
    bash
    diff
    docker
    grep
    pg_isready
    psql
    python3
    sed
    sort
  )

  for command_name in "${commands[@]}"; do
    if ! command -v "${command_name}" >/dev/null 2>&1; then
      err "required command not found: ${command_name}"
      return 1
    fi
  done
}

contains_value() {
  local wanted=$1
  shift
  local value

  for value in "$@"; do
    if [[ "${value}" == "${wanted}" ]]; then
      return 0
    fi
  done
  return 1
}

validate_major() {
  local major=$1

  if ! contains_value "${major}" "${SUPPORTED_MAJORS[@]}"; then
    err "unsupported PostgreSQL major: ${major}"
    return 1
  fi
}

validate_surface() {
  local surface=$1

  if [[ "${surface}" != "all" ]] \
    && ! contains_value "${surface}" "${SURFACES[@]}"; then
    err "unknown surface: ${surface}"
    return 1
  fi
}

safe_component() {
  local value=$1
  value="${value//[^a-zA-Z0-9_.-]/_}"
  printf '%s\n' "${value}"
}

psql_gate() {
  PAGER="cat" psql \
    --no-psqlrc \
    --host=localhost \
    --port="${PGPORT}" \
    --username="${POSTGRES_USER}" \
    --dbname="${POSTGRES_DATABASE}" \
    --set=ON_ERROR_STOP=1 \
    "$@"
}

wait_for_postgres() {
  local attempt

  for ((attempt = 1; attempt <= 60; attempt++)); do
    if PGPASSWORD="${POSTGRES_PASSWORD}" pg_isready \
      --host=localhost \
      --port="${PGPORT}" \
      --username="${POSTGRES_USER}" \
      --dbname="${POSTGRES_DATABASE}" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done

  err "PostgreSQL did not become ready on container port ${PGPORT}"
  return 1
}

start_container() {
  local major=$1
  local surface=$2
  local preload=$3
  local port_mapping
  local -a postgres_args=(
    postgres
    -c
    "shared_preload_libraries=${preload}"
  )

  if [[ "${preload}" == *pg_cron* ]]; then
    postgres_args+=(
      -c
      "cron.database_name=${POSTGRES_DATABASE}"
      -c
      "cron.use_background_workers=on"
    )
  fi

  printf 'Starting PostgreSQL %s for %s as %s\n' \
    "${major}" "${surface}" "${container_name}"
  if ! container_id="$(docker create \
    --name="${container_name}" \
    --label="org.pg-ash.release-gate=true" \
    --env="POSTGRES_DB=${POSTGRES_DATABASE}" \
    --env="POSTGRES_PASSWORD=${POSTGRES_PASSWORD}" \
    --publish="127.0.0.1::5432" \
    "${image_name}" \
    "${postgres_args[@]}")"; then
    err "could not create the release-gate container"
    return 1
  fi
  if [[ ! "${container_id}" =~ ^[a-f0-9]{12,64}$ ]]; then
    err "docker create returned an invalid container ID: ${container_id}"
    return 1
  fi
  owned_containers+=("${container_id}")

  if ! docker start "${container_id}" >/dev/null; then
    err "could not start release-gate container ${container_id}"
    return 1
  fi

  port_mapping="$(docker port "${container_id}" 5432/tcp)"
  PGPORT="${port_mapping##*:}"
  if [[ ! "${PGPORT}" =~ ^[0-9]+$ ]]; then
    err "could not resolve ${container_name} port: ${port_mapping}"
    return 1
  fi

  export PGPORT
  export PGPASSWORD="${POSTGRES_PASSWORD}"
  export PGUSER="${POSTGRES_USER}"
  export PGDATABASE="${POSTGRES_DATABASE}"
  export PAGER=cat
  export PSQLRC=/dev/null
  export CONTAINER_ID="${container_id}"
  export CRON=on
  wait_for_postgres
}

create_extensions() {
  local want_cron=$1
  local want_pgss=$2

  if [[ "${want_cron}" == "t" ]]; then
    psql_gate --command="create extension pg_cron;"
  fi
  if [[ "${want_pgss}" == "t" ]]; then
    psql_gate --command="create extension pg_stat_statements;"
  fi
}

assert_extension_state() {
  local expected_cron=$1
  local expected_pgss=$2
  local actual
  local expected="${expected_cron}|${expected_pgss}"

  actual="$(psql_gate \
    --tuples-only \
    --no-align \
    --command="
      select
        case when exists (
          select from pg_extension where extname = 'pg_cron'
        ) then 't' else 'f' end
        || '|' ||
        case when exists (
          select from pg_extension where extname = 'pg_stat_statements'
        ) then 't' else 'f' end;
    ")"
  actual="${actual//$'\r'/}"

  if [[ "${actual}" != "${expected}" ]]; then
    err "extension precondition failed: expected ${expected}; got ${actual}"
    return 1
  fi
  printf 'Extension state: pg_cron=%s pg_stat_statements=%s\n' \
    "${expected_cron}" "${expected_pgss}"
}

install_fresh() {
  local install_path
  install_path="$(python3 "${CHAIN_HELPER}" fresh-install-path)"

  if [[ "${install_path}" == /* || ! -f "${REPO_ROOT}/${install_path}" ]]; then
    err "fresh-install-path returned an invalid path: ${install_path}"
    return 1
  fi
  printf 'Fresh installer: %s\n' "${install_path}"
  psql_gate --file="${REPO_ROOT}/${install_path}"
}

uninstall_if_present() {
  local schema_present

  schema_present="$(psql_gate \
    --tuples-only \
    --no-align \
    --command="
      select exists (
        select from pg_namespace where nspname = 'ash'
      )::text;
    ")"
  if [[ "${schema_present//$'\r'/}" == "true" ]]; then
    psql_gate --command="select ash.uninstall('yes');"
  fi
}

run_ci_selection() {
  local selection_file="${surface_temp_dir}/ci-selection.bin"
  local step_name
  local step_body
  local step_count=0
  local -a helper_args=("$@")

  PYTHONDONTWRITEBYTECODE=1 python3 "${CI_STEP_HELPER}" \
    "${helper_args[@]}" \
    --null >"${selection_file}"

  while IFS= read -r -d '' step_name \
    && IFS= read -r -d '' step_body; do
    ((step_count += 1))
    printf '[ci-step %s] %s\n' "${step_count}" "${step_name}"

    # Parallel major workers must not share the workflow's historical /tmp
    # filenames. This is the only transformation made to canonical step bodies.
    step_body="${step_body//\/tmp\//${surface_temp_dir}/}"
    if ! bash \
      --noprofile \
      --norc \
      -e \
      -o pipefail \
      -c "${step_body}" \
      ci-step; then
      err "canonical CI step failed: ${step_name}"
      return 1
    fi
  done <"${selection_file}"

  if ((step_count == 0)); then
    err "CI selection returned no executable steps"
    return 1
  fi
}

run_fresh_install() {
  assert_extension_state t t
  install_fresh
  run_ci_selection \
    range \
    "Test schema and infrastructure" \
    "Test uninstall" \
    --exclude \
    "H-CI-3: end-to-end pg_cron fires ash.take_sample (#46)"
}

run_upgrade_chain() {
  local major=$1
  local reapply_chain
  local reapply_count

  assert_extension_state t t
  reapply_chain="$(python3 "${CHAIN_HELPER}" reapply-chain)"
  reapply_count="$(printf '%s\n' "${reapply_chain}" \
    | awk 'NF { count++ } END { print count + 0 }')"
  printf 'Discovered re-apply-safe migration scripts: %s\n' "${reapply_count}"
  if ((reapply_count == 0)); then
    printf '%s\n' \
      'NOTE: reapply-chain is empty; installer re-apply regressions still run.'
  fi

  if [[ "${major}" == "17" ]]; then
    run_ci_selection \
      step \
      "Release upgrade path: actual v1.4 tag to v1.5"
  fi

  run_ci_selection \
    range \
    "Upgrade path: discovered full chain" \
    "Schema equivalence: fresh dev install vs full upgrade path"
}

run_features() {
  assert_extension_state t t
  run_ci_selection \
    step \
    "Behavioral feature coverage across extension modes"
}

run_feature_mode() {
  local feature_mode=$1
  local expected_cron=$2
  local expected_pgss=$3

  PG_ASH_FEATURE_MODE_ROWS="$(
    printf '%s\t%s\t%s\n' \
      "${feature_mode}" "${expected_cron}" "${expected_pgss}"
  )" run_ci_selection \
    step \
    "Behavioral feature coverage across extension modes"
}

run_degraded_no_cron() {
  assert_extension_state f t
  install_fresh
  psql_gate --file="${REPO_ROOT}/devel/tests/degraded_no_cron.sql"
  run_feature_mode no-cron false true
}

run_degraded_no_pgss() {
  assert_extension_state t f
  install_fresh
  psql_gate --file="${REPO_ROOT}/devel/tests/degraded_no_pgss.sql"
  run_feature_mode no-pgss true false
}

run_degraded_neither() {
  assert_extension_state f f
  install_fresh
  psql_gate --file="${REPO_ROOT}/devel/tests/degraded_no_pgss.sql"

  # Preserve the CI tests' fresh-install isolation between degraded bodies.
  uninstall_if_present
  install_fresh
  assert_extension_state f f
  psql_gate --file="${REPO_ROOT}/devel/tests/degraded_no_cron.sql"
  run_feature_mode neither false false
}

run_cron_path() {
  assert_extension_state t t
  install_fresh
  run_ci_selection \
    step \
    "Verify pg_cron wiring" \
    "Test start/stop" \
    "H-CI-3: end-to-end pg_cron fires ash.take_sample (#46)" \
    "Test all interval formats (issue #2)" \
    "Test #61: status() works for non-superuser without cron.job access"
}

surface_configuration() {
  local surface=$1

  case "${surface}" in
    fresh-install | upgrade-chain | features | cron-path)
      printf '%s\t%s\t%s\n' "pg_cron,pg_stat_statements" t t
      ;;
    degraded-neither)
      printf '%s\t%s\t%s\n' "<none>" f f
      ;;
    degraded-no-cron)
      printf '%s\t%s\t%s\n' "pg_stat_statements" f t
      ;;
    degraded-no-pgss)
      printf '%s\t%s\t%s\n' "pg_cron" t f
      ;;
    *)
      err "no extension configuration for surface: ${surface}"
      return 1
      ;;
  esac
}

execute_surface() {
  local major=$1
  local surface=$2
  local expected_cron=$3
  local expected_pgss=$4

  create_extensions "${expected_cron}" "${expected_pgss}"

  case "${surface}" in
    fresh-install)
      run_fresh_install
      ;;
    upgrade-chain)
      run_upgrade_chain "${major}"
      ;;
    features)
      run_features
      ;;
    degraded-no-cron)
      run_degraded_no_cron
      ;;
    degraded-no-pgss)
      run_degraded_no_pgss
      ;;
    degraded-neither)
      run_degraded_neither
      ;;
    cron-path)
      run_cron_path
      ;;
  esac
}

failure_detail() {
  local log_file=$1
  local detail

  detail="$(awk '
    /::error::|ERROR:|FAILED|assertion failed|FATAL:/ {
      sub(/^[[:space:]]+/, "")
      print
      exit
    }
  ' "${log_file}")"
  if [[ -z "${detail}" ]]; then
    detail="command failed; inspect ${log_file}"
  fi
  detail="${detail//$'\t'/ }"
  detail="${detail//$'\r'/}"
  printf '%.500s\n' "${detail}"
}

append_result() {
  local major=$1
  local surface=$2
  local result=$3
  local detail=$4
  local log_file=$5

  printf '%s\t%s\t%s\t%s\t%s\n' \
    "${major}" \
    "${surface}" \
    "${result}" \
    "${detail}" \
    "${log_file}" >>"${summary_file}"
}

remove_owned_container() {
  local id=$1
  local owned_id
  local -a remaining=()

  if ! docker rm --force --volumes "${id}" >/dev/null 2>&1; then
    err "could not tear down owned container ${id}"
    return 1
  fi
  for owned_id in "${owned_containers[@]}"; do
    if [[ "${owned_id}" != "${id}" ]]; then
      remaining+=("${owned_id}")
    fi
  done
  owned_containers=("${remaining[@]}")
  if [[ "${container_id}" == "${id}" ]]; then
    container_id=""
  fi
}

run_surface() {
  local major=$1
  local surface=$2
  local safe_major
  local safe_surface
  local log_file
  local exit_code
  local detail
  local configuration
  local preload
  local expected_cron
  local expected_pgss

  safe_major="$(safe_component "${major}")"
  safe_surface="$(safe_component "${surface}")"
  surface_temp_dir="${output_dir}/tmp_${safe_major}_${safe_surface}"
  mkdir -p "${surface_temp_dir}"
  log_file="${output_dir}/${safe_major}_${safe_surface}.log"
  container_name="pgash_gate_${safe_major}_${safe_surface}_$$_${RANDOM}"
  container_id=""
  image_name="${IMAGE_REPOSITORY}:${major}"
  configuration="$(surface_configuration "${surface}")"
  IFS=$'\t' read -r preload expected_cron expected_pgss <<<"${configuration}"
  if [[ "${preload}" == "<none>" ]]; then
    preload=""
  fi

  printf 'RUN  PostgreSQL %-7s %s\n' "${major}" "${surface}"
  set +e
  start_container \
    "${major}" "${surface}" "${preload}" >"${log_file}" 2>&1
  exit_code=$?
  set -e

  if ((exit_code == 0)); then
    set +e
    (
      trap - EXIT INT TERM
      set -Eeuo pipefail
      execute_surface \
        "${major}" "${surface}" "${expected_cron}" "${expected_pgss}"
    ) >>"${log_file}" 2>&1
    exit_code=$?
    set -e
  fi

  if ((exit_code != 0)) && [[ -n "${container_id}" ]]; then
    docker logs "${container_id}" >>"${log_file}" 2>&1 || true
  fi
  if [[ -n "${container_id}" ]]; then
    if ! remove_owned_container "${container_id}"; then
      printf 'ERROR: container teardown failed for %s\n' \
        "${container_id}" >>"${log_file}"
      exit_code=1
    fi
  fi

  if ((exit_code == 0)); then
    append_result \
      "${major}" "${surface}" PASS "all assertions passed" "${log_file}"
    printf 'PASS PostgreSQL %-7s %s\n' "${major}" "${surface}"
  else
    detail="$(failure_detail "${log_file}")"
    append_result "${major}" "${surface}" FAIL "${detail}" "${log_file}"
    err "FAIL PostgreSQL ${major} ${surface}: ${detail} (log: ${log_file})"
    any_failure=1
  fi
}

prepare_output() {
  local requested_dir=${RELEASE_GATE_OUTPUT_DIR:-}
  local requested_summary=${RELEASE_GATE_SUMMARY:-}

  if [[ -n "${requested_dir}" ]]; then
    output_dir="${requested_dir}"
    mkdir -p "${output_dir}"
    output_dir="$(cd "${output_dir}" && pwd -P)"
  else
    output_dir="$(mktemp -d /tmp/pg_ash-release-gate.XXXXXX)"
  fi
  if [[ ! "${output_dir}" =~ ^/[a-zA-Z0-9_./-]+$ ]]; then
    err "release-gate output path contains unsupported shell characters: ${output_dir}"
    return 2
  fi

  if [[ -n "${requested_summary}" ]]; then
    summary_file="${requested_summary}"
    mkdir -p "$(dirname "${summary_file}")"
  else
    summary_file="${output_dir}/results.tsv"
  fi
  printf 'major\tsurface\tresult\tdetail\tlog\n' >"${summary_file}"
}

build_image() {
  local major=$1
  local build_log
  build_log="${output_dir}/build_$(safe_component "${major}").log"

  if [[ "${RELEASE_GATE_PULL:-1}" == "1" ]]; then
    if ! docker pull "postgres:${major}" >"${build_log}" 2>&1; then
      err "docker pull failed for postgres:${major}"
      return 1
    fi
  elif [[ "${RELEASE_GATE_PULL}" != "0" ]]; then
    err "RELEASE_GATE_PULL must be 0 or 1"
    return 1
  fi

  if ! docker build \
    --build-arg="POSTGRES_TAG=${major}" \
    --tag="${IMAGE_REPOSITORY}:${major}" \
    "${REPO_ROOT}/devel/docker" >>"${build_log}" 2>&1; then
    err "docker build failed for ${IMAGE_REPOSITORY}:${major}"
    return 1
  fi
}

print_human_table() {
  printf '\n%-10s  %-21s  %-6s  %s\n' \
    "PG major" "surface" "result" "detail"
  printf '%-10s  %-21s  %-6s  %s\n' \
    "----------" "---------------------" "------" "------"
  awk -F'\t' '
    NR > 1 {
      printf "%-10s  %-21s  %-6s  %s\n", $1, $2, $3, $4
    }
  ' "${summary_file}"
  printf '\nMachine-readable TSV: %s\n' "${summary_file}"
  printf 'Logs: %s\n' "${output_dir}"
}

mark_build_failures() {
  local major=$1
  shift
  local surface
  local build_log
  build_log="${output_dir}/build_$(safe_component "${major}").log"

  for surface in "$@"; do
    append_result \
      "${major}" \
      "${surface}" \
      FAIL \
      "pg_cron image build failed" \
      "${build_log}"
  done
  any_failure=1
}

run_major() {
  local major=$1
  local selector=$2
  local surface
  local -a selected_surfaces=()

  prepare_output
  if [[ "${selector}" == "all" ]]; then
    selected_surfaces=("${SURFACES[@]}")
  else
    selected_surfaces=("${selector}")
  fi

  if ! build_image "${major}"; then
    err "could not build pg_cron image for PostgreSQL ${major}"
    mark_build_failures "${major}" "${selected_surfaces[@]}"
    print_human_table
    return 1
  fi

  for surface in "${selected_surfaces[@]}"; do
    run_surface "${major}" "${surface}"
  done

  print_human_table
  return "${any_failure}"
}

wait_for_worker() {
  local pid=$1
  local worker_pid
  local exit_code=0
  local -a remaining=()

  if ! wait "${pid}"; then
    exit_code=1
  fi
  for worker_pid in "${owned_workers[@]}"; do
    if [[ "${worker_pid}" != "${pid}" ]]; then
      remaining+=("${worker_pid}")
    fi
  done
  owned_workers=("${remaining[@]}")
  if ((exit_code != 0)); then
    return "${exit_code}"
  fi
}

run_all_majors() {
  local selector=$1
  local majors_text=${PG_MAJORS:-${DEFAULT_MAJORS}}
  local jobs=${RELEASE_GATE_JOBS:-2}
  local parent_output
  local parent_summary
  local major
  local safe_major
  local child_output
  local child_summary
  local worker_log
  local pid
  local worker_failed=0
  local header
  local result_row
  local row_count
  local surface
  local worker_major
  local major_problem
  local expected_failure
  local -a majors=()
  local -a active_pids=()
  local -a expected_surfaces=()
  local -a result_fields=()
  local -a validated_majors=()
  local -A worker_major_by_pid=()
  local -A worker_failed_by_major=()
  local -A result_rows=()
  local -A result_valid=()

  IFS=$' \t\n' read -r -a majors <<<"${majors_text}"
  if ((${#majors[@]} == 0)); then
    err "PG_MAJORS did not contain any majors"
    return 2
  fi
  if [[ ! "${jobs}" =~ ^[1-9][0-9]*$ ]]; then
    err "RELEASE_GATE_JOBS must be a positive integer"
    return 2
  fi
  for major in "${majors[@]}"; do
    validate_major "${major}"
    if contains_value "${major}" "${validated_majors[@]}"; then
      err "PG_MAJORS contains a duplicate major: ${major}"
      return 2
    fi
    validated_majors+=("${major}")
  done
  if [[ "${selector}" == "all" ]]; then
    expected_surfaces=("${SURFACES[@]}")
  else
    expected_surfaces=("${selector}")
  fi

  prepare_output
  parent_output="${output_dir}"
  parent_summary="${summary_file}"

  for major in "${majors[@]}"; do
    while ((${#active_pids[@]} >= jobs)); do
      pid="${active_pids[0]}"
      worker_major="${worker_major_by_pid[${pid}]}"
      if ! wait_for_worker "${pid}"; then
        worker_failed=1
        worker_failed_by_major["${worker_major}"]=1
      fi
      active_pids=("${active_pids[@]:1}")
    done

    safe_major="$(safe_component "${major}")"
    child_output="${parent_output}/pg_${safe_major}"
    child_summary="${parent_output}/pg_${safe_major}.tsv"
    RELEASE_GATE_OUTPUT_DIR="${child_output}" \
      RELEASE_GATE_SUMMARY="${child_summary}" \
      "${BASH_SOURCE[0]}" "${major}" "${selector}" \
      >"${parent_output}/worker_${safe_major}.log" 2>&1 &
    pid=$!
    active_pids+=("${pid}")
    owned_workers+=("${pid}")
    worker_major_by_pid["${pid}"]="${major}"
    worker_failed_by_major["${major}"]=0
    printf 'LAUNCH PostgreSQL %-7s worker pid=%s\n' "${major}" "${pid}"
  done

  for pid in "${active_pids[@]}"; do
    worker_major="${worker_major_by_pid[${pid}]}"
    if ! wait_for_worker "${pid}"; then
      worker_failed=1
      worker_failed_by_major["${worker_major}"]=1
    fi
  done

  for major in "${majors[@]}"; do
    safe_major="$(safe_component "${major}")"
    child_summary="${parent_output}/pg_${safe_major}.tsv"
    worker_log="${parent_output}/worker_${safe_major}.log"
    if [[ ! -f "${child_summary}" ]]; then
      for surface in "${expected_surfaces[@]}"; do
        append_result \
          "${major}" \
          "${surface}" \
          FAIL \
          "worker produced no summary" \
          "${worker_log}"
      done
      worker_failed=1
      continue
    fi

    major_problem="${worker_failed_by_major[${major}]}"
    header=""
    IFS= read -r header <"${child_summary}" || true
    if [[ "${header}" != $'major\tsurface\tresult\tdetail\tlog' ]]; then
      err "worker ${major} produced an invalid summary header"
      worker_failed=1
      major_problem=1
    fi
    row_count="$(awk 'NR > 1 { count++ } END { print count + 0 }' \
      "${child_summary}")"
    if [[ "${row_count}" != "${#expected_surfaces[@]}" ]]; then
      err "worker ${major} produced ${row_count} result rows; expected ${#expected_surfaces[@]}"
      worker_failed=1
      major_problem=1
    fi

    result_rows=()
    result_valid=()
    expected_failure=0
    for surface in "${expected_surfaces[@]}"; do
      result_row=""
      if [[ "${header}" == $'major\tsurface\tresult\tdetail\tlog' ]] \
        && result_row="$(awk -F'\t' \
          -v expected_major="${major}" \
          -v expected_surface="${surface}" '
            NR > 1 \
              && $1 == expected_major \
              && $2 == expected_surface \
              && ($3 == "PASS" || $3 == "FAIL") \
              && NF == 5 {
              count++
              row = $0
            }
            END {
              if (count == 1) {
                print row
              } else {
                exit 1
              }
            }
          ' "${child_summary}")"; then
        result_rows["${surface}"]="${result_row}"
        result_valid["${surface}"]=1
        IFS=$'\t' read -r -a result_fields <<<"${result_row}"
        if [[ "${result_fields[2]}" == "FAIL" ]]; then
          expected_failure=1
        fi
      else
        result_valid["${surface}"]=0
        expected_failure=1
        worker_failed=1
        major_problem=1
      fi
    done

    for surface in "${expected_surfaces[@]}"; do
      if ((major_problem != 0 && expected_failure == 0)) \
        && [[ "${surface}" == "${expected_surfaces[0]}" ]]; then
        append_result \
          "${major}" \
          "${surface}" \
          FAIL \
          "worker exited non-zero or produced an invalid summary" \
          "${worker_log}"
      elif [[ "${result_valid[${surface}]}" == "1" ]]; then
        printf '%s\n' "${result_rows[${surface}]}" >>"${parent_summary}"
      else
        append_result \
          "${major}" \
          "${surface}" \
          FAIL \
          "worker summary missing or duplicated this surface" \
          "${worker_log}"
      fi
    done
  done

  summary_file="${parent_summary}"
  output_dir="${parent_output}"
  print_human_table
  if awk -F'\t' 'NR > 1 && $3 == "FAIL" { found=1 } END { exit !found }' \
    "${summary_file}"; then
    worker_failed=1
  fi
  return "${worker_failed}"
}

main() {
  local target=${1:-}
  local selector=${2:-all}

  if [[ "${target}" == "--help" || "${target}" == "-h" ]]; then
    usage
    return 0
  fi
  if [[ -z "${target}" || $# -gt 2 ]]; then
    usage >&2
    return 2
  fi

  require_commands
  validate_surface "${selector}"
  [[ -f "${DOCKERFILE}" ]] || {
    err "Dockerfile not found: ${DOCKERFILE}"
    return 2
  }
  [[ -f "${CHAIN_HELPER}" && -f "${CI_STEP_HELPER}" ]] || {
    err "release-gate helper is missing"
    return 2
  }

  cd "${REPO_ROOT}"
  trap cleanup EXIT
  trap 'signal_exit 130' INT
  trap 'signal_exit 143' TERM

  if [[ "${target}" == "all" ]]; then
    run_all_majors "${selector}"
  else
    validate_major "${target}"
    run_major "${target}" "${selector}"
  fi
}

main "$@"
