#!/usr/bin/env bash
#
# lib/backend.sh — the only file in demos/ allowed to touch raw PG* variables.
#
# Three backends, one interface (§5):
#
#   local   DEFAULT and the CI path. Whatever cluster the ambient PG* variables
#           already reach. Needs psql + pgbench and nothing else: no Docker
#           daemon, no registry pull, no ALTER SYSTEM, no restart. pg_cron is
#           NOT required — the seeder drives ash.take_sample() itself. This
#           exercises external scheduling without assuming which extensions
#           a managed service or deployment permits.
#   docker  Optional. For pinning a specific major, or for a cluster that
#           really has pg_cron. Never on the critical path.
#   remote  Standard PG* variables against someone else's server, with two
#           guardrails that cannot be switched off.
#
# HOUSE RULES ARE ENFORCED HERE, IN CODE, NOT IN DOCUMENTATION. Every drop and
# every `docker rm` re-asserts the ash_demo* / ash_demo_* name globs and the
# ownership record this run wrote. A harness that can delete a database it did
# not create has no business running on anybody's laptop.
#
# Sourced, never executed.

# shellcheck source=lib/env.sh
. "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)/env.sh"

# Maintenance database used only to issue CREATE/DROP DATABASE. We connect to
# it; we never write to it.
: "${ASH_MAINT_DB:=postgres}"

# Ownership ledger. Written by backend_up, consulted by backend_down.
ASH_STATE_FILE="$ASH_OUT/backend.state"

# ---------------------------------------------------------------------------
# Guardrails
# ---------------------------------------------------------------------------

# _bk_assert_db_glob <dbname> — the harness may only create or drop databases
# whose name starts with `ash_demo`.
_bk_assert_db_glob() {
  case "$1" in
    ash_demo*) : ;;
    *) ash_die 1 "refusing to manage database '$1': the harness only touches ash_demo*" ;;
  esac
  case "$1" in
    *[!A-Za-z0-9_-]*)
      ash_die 1 "refusing unsafe database name '$1': use only letters, digits, underscores and hyphens"
      ;;
  esac
}

# _bk_assert_container_glob <name> — likewise for containers.
_bk_assert_container_glob() {
  case "$1" in
    ash_demo_*) : ;;
    *) ash_die 1 "refusing to manage container '$1': the harness only touches ash_demo_*" ;;
  esac
}

# The official postgres images declare PGDATA as an anonymous volume. `-v` is
# therefore part of correctness here, not an optional space-saving flag:
# removing only the container leaks roughly 600 MiB for every matrix entry.
_bk_remove_owned_container() {
  docker rm -f -v "$1" >/dev/null 2>&1
}

# ---------------------------------------------------------------------------
# psql wrappers — every SQL call in the harness goes through one of these
# ---------------------------------------------------------------------------

# ash_psql <psql args...> — the demo database, startup file suppressed,
# ON_ERROR_STOP armed. Callers that WANT a psqlrc (the capture paths) build
# their own command line; this one is for plumbing.
ash_psql() {
  psql -X -v ON_ERROR_STOP=1 -d "$ASH_DEMO_DB" "$@"
}

# ash_psql1 <sql> — one scalar, unaligned, no header, no footer.
ash_psql1() {
  psql -X -v ON_ERROR_STOP=1 -d "$ASH_DEMO_DB" -tAc "$1"
}

# ash_psql_maint <psql args...> — the maintenance database.
ash_psql_maint() {
  psql -X -v ON_ERROR_STOP=1 -d "$ASH_MAINT_DB" "$@"
}

# ---------------------------------------------------------------------------
# Backend probing (used by doctor, and by ASH_BACKEND=auto)
# ---------------------------------------------------------------------------

# backend_probe_local — 0 if the ambient PG* reach a live server.
backend_probe_local() {
  ash_have psql || return 1
  psql -X -d "$ASH_MAINT_DB" -tAc 'select 1' >/dev/null 2>&1
}

# backend_connect — resolve the connection WITHOUT creating or installing
# anything. For read-only consumers (preflight, `ash-demo sql`, the capture
# paths) that must not pay a 250 KB re-install to ask the database a question.
backend_connect() {
  case "$ASH_BACKEND" in
    docker)
      if [ -z "${ASH_DEMO_PORT:-}" ] && ash_have docker; then
        _bk_mapping=$(docker port "$ASH_DEMO_CONTAINER" 5432/tcp 2>/dev/null | head -1)
        ASH_DEMO_PORT=${_bk_mapping##*:}
        unset _bk_mapping
      fi
      export PGHOST=127.0.0.1 PGPORT="${ASH_DEMO_PORT:-5432}" PGUSER=postgres
      export PGPASSWORD=ash_demo
      ;;
  esac
  export PGDATABASE="$ASH_DEMO_DB"
  ash_psql1 'select 1' >/dev/null 2>&1 \
    || ash_die 3 "cannot reach database '$ASH_DEMO_DB' — run 'make up' first"
}

# backend_probe_docker — 0 if a docker daemon answers AND a usable postgres
# image is present locally. We deliberately do NOT count "docker can pull",
# because a registry that 503s in a sandbox is the root defect being fixed.
backend_probe_docker() {
  ash_have docker || return 1
  docker info >/dev/null 2>&1 || return 1
  docker image inspect "postgres:$ASH_PG_MAJOR" >/dev/null 2>&1
}

# ---------------------------------------------------------------------------
# backend_up
# ---------------------------------------------------------------------------
#
# Post-condition: PGHOST/PGPORT/PGUSER/PGDATABASE are exported and point at a
# database named $ASH_DEMO_DB that has pg_ash installed and (where possible)
# pg_stat_statements created.
backend_up() {
  case "$ASH_BACKEND" in
    auto)
      if backend_probe_local; then
        ASH_BACKEND=local
      elif backend_probe_docker; then
        ASH_BACKEND=docker
      else
        ash_die 3 "ASH_BACKEND=auto: no local cluster reachable and no usable docker postgres image"
      fi
      ash_log "backend auto-selected: $ASH_BACKEND"
      ;;
  esac

  case "$ASH_BACKEND" in
    local)  _bk_up_local ;;
    docker) _bk_up_docker ;;
    remote) _bk_up_remote ;;
    *) ash_die 1 "unknown ASH_BACKEND '$ASH_BACKEND' (want local|docker|remote|auto)" ;;
  esac

  _bk_install_pg_ash
  _bk_report
}

_bk_up_local() {
  _bk_assert_db_glob "$ASH_DEMO_DB"
  ash_have psql || ash_die 2 "psql not found on PATH"
  backend_probe_local \
    || ash_die 3 "cannot reach a local PostgreSQL cluster (tried database '$ASH_MAINT_DB' with the ambient PG* settings)"

  # Create the demo database if it is not already there, and remember whether
  # THIS run created it — backend_down only drops what it created.
  if [ "$(ash_psql_maint -tAc \
        "select count(*) from pg_database where datname = '$ASH_DEMO_DB'")" = "0" ]; then
    ash_log "creating database $ASH_DEMO_DB"
    ash_psql_maint -c "create database \"$ASH_DEMO_DB\"" >/dev/null
    _bk_record_state local created
  else
    ash_log "reusing existing database $ASH_DEMO_DB"
    _bk_record_state local reused
  fi

  export PGDATABASE="$ASH_DEMO_DB"
}

_bk_up_docker() {
  _bk_assert_db_glob "$ASH_DEMO_DB"
  _bk_assert_container_glob "$ASH_DEMO_CONTAINER"
  ash_have docker || ash_die 2 "docker not found on PATH"
  docker info >/dev/null 2>&1 || ash_die 3 "docker daemon is not answering"

  local created=reused
  if [ -z "$(docker ps -aq -f "name=^${ASH_DEMO_CONTAINER}$")" ]; then
    docker image inspect "postgres:$ASH_PG_MAJOR" >/dev/null 2>&1 \
      || ash_die 2 "image postgres:$ASH_PG_MAJOR is not present locally (pull it first; the harness never pulls on the critical path)"

    # Probe a free port. Never hardcode: two harness runs on one machine must
    # not fight over 5500.
    ASH_DEMO_PORT=${ASH_DEMO_PORT:-$(ash_free_port 5500 5599)} \
      || ash_die 3 "no free TCP port in 5500-5599"

    ash_log "starting container $ASH_DEMO_CONTAINER (postgres:$ASH_PG_MAJOR) on port $ASH_DEMO_PORT"
    # pg_stat_statements must be preloaded to exist at all. pg_cron is NOT
    # requested here: the stock postgres image does not ship it, and the whole
    # point of this harness is that the no-cron path is first class. A cluster
    # with real pg_cron needs a purpose-built image and an explicit image tag.
    docker run -d \
      --name "$ASH_DEMO_CONTAINER" \
      -e POSTGRES_PASSWORD=ash_demo \
      -e POSTGRES_DB="$ASH_DEMO_DB" \
      -p "127.0.0.1:$ASH_DEMO_PORT:5432" \
      "postgres:$ASH_PG_MAJOR" \
      -c shared_preload_libraries=pg_stat_statements \
      -c max_connections=100 \
      >/dev/null
    created=created
  else
    ash_log "reusing container $ASH_DEMO_CONTAINER"
    if [ -z "${ASH_DEMO_PORT:-}" ]; then
      # `docker port` prints "0.0.0.0:5531"; take everything after the last
      # colon with a parameter expansion rather than sed (§10: no sed).
      _bk_mapping=$(docker port "$ASH_DEMO_CONTAINER" 5432/tcp | head -1)
      ASH_DEMO_PORT=${_bk_mapping##*:}
      unset _bk_mapping
    fi
    docker start "$ASH_DEMO_CONTAINER" >/dev/null 2>&1 || true
  fi

  export PGHOST=127.0.0.1
  export PGPORT="$ASH_DEMO_PORT"
  export PGUSER=postgres
  export PGPASSWORD=ash_demo
  export PGDATABASE="$ASH_DEMO_DB"
  ASH_MAINT_DB=postgres

  # Readiness poll. `pg_isready` alone is not enough: the image restarts the
  # server once during first-boot initdb, so we also demand a real query.
  local deadline i
  deadline=$(( $(ash_now_ms) + 60000 ))
  i=0
  while [ "$(ash_now_ms)" -lt "$deadline" ]; do
    if psql -X -d "$ASH_DEMO_DB" -tAc 'select 1' >/dev/null 2>&1; then
      break
    fi
    i=$((i + 1))
    python3 -c 'import time; time.sleep(0.25)'
  done
  psql -X -d "$ASH_DEMO_DB" -tAc 'select 1' >/dev/null 2>&1 \
    || ash_die 3 "container $ASH_DEMO_CONTAINER never became ready on port $ASH_DEMO_PORT"

  _bk_record_state docker "$created"
}

_bk_up_remote() {
  # Guardrail 1: the target database name must match the house glob. This is
  # the difference between a demo harness and an accident.
  _bk_assert_db_glob "$ASH_DEMO_DB"
  export PGDATABASE="$ASH_DEMO_DB"

  psql -X -d "$ASH_DEMO_DB" -tAc 'select 1' >/dev/null 2>&1 \
    || ash_die 3 "cannot reach remote database '$ASH_DEMO_DB' with the ambient PG* settings"

  # Guardrail 2: refuse to seed on top of somebody else's samples. A remote
  # database that already holds ash.sample rows is either a real installation
  # or a previous run; either way the operator has to say so out loud.
  local existing
  existing=$(psql -X -d "$ASH_DEMO_DB" -tAc "
    select case when to_regclass('ash.sample') is null then 0
                else (select count(*) from ash.sample) end" 2>/dev/null || echo 0)
  if [ "${existing:-0}" != "0" ] && [ "${ASH_SKIP_SEED:-}" != "1" ] \
     && [ "${ASH_REMOTE_OVERWRITE:-}" != "1" ]; then
    ash_die 3 "remote database '$ASH_DEMO_DB' already holds $existing ash.sample rows; set ASH_REMOTE_OVERWRITE=1 if you really mean it"
  fi

  _bk_record_state remote reused
}

# ---------------------------------------------------------------------------
# pg_ash installation
# ---------------------------------------------------------------------------

_bk_install_pg_ash() {
  [ -f "$ASH_INSTALL_SQL" ] \
    || ash_die 1 "installer not found: $ASH_INSTALL_SQL (set ASH_INSTALL_SQL or ASH_REPO_ROOT)"

  # pg_stat_statements first: ash.top('query_id') and ash.report() are much
  # more interesting with real normalized query text, and the extension has to
  # exist before the seeder generates its query identities. Its absence is a
  # warning, never a failure — pg_ash's degraded path is a supported path.
  if ! ash_psql -c 'create extension if not exists pg_stat_statements' >/dev/null 2>&1; then
    ash_warn "pg_stat_statements is unavailable; query text will be omitted (degraded mode)"
  fi

  ash_log "installing pg_ash from ${ASH_INSTALL_SQL#$ASH_REPO_ROOT/}"
  # Install log goes to a file: the installer is ~250 KB of DDL and its NOTICE
  # traffic buries everything else. A failure prints the tail.
  if ! ash_psql -q -f "$ASH_INSTALL_SQL" >"$ASH_OUT/install.log" 2>&1; then
    tail -40 "$ASH_OUT/install.log" >&2
    ash_die 3 "pg_ash install failed; full log at $ASH_OUT/install.log"
  fi

  # pg_cron is optional by design. Report the mode so the operator knows which
  # path the demo is exercising, and so the `status` still can be read honestly.
  # `case` rather than `grep -q`: a successful `grep -q` behind a pipe SIGPIPEs
  # its writer, and under `set -o pipefail` that inverts the result (§2.6).
  case "$(ash_psql1 'select ash._pg_cron_available()')" in
    t) ash_log "pg_cron available: pg_ash could self-schedule (harness still drives sampling itself)" ;;
    *) ash_log "pg_cron unavailable: external-scheduler (degraded) mode — the default demo path" ;;
  esac
}

# ---------------------------------------------------------------------------
# State ledger + reporting
# ---------------------------------------------------------------------------

_bk_record_state() {
  # $1 = backend kind, $2 = created|reused
  #
  # Ownership is STICKY. `make seed` twice in a row hits the reuse path the
  # second time, and if that downgraded the record to "reused" then `make down`
  # would refuse to remove a container this harness had in fact created — which
  # is exactly what happened the first time this ran, leaving ash_demo_b1_pg
  # behind. Only backend_down clears the ledger.
  if [ -f "$ASH_STATE_FILE" ] && [ "$2" = "reused" ]; then
    local prev_own="" prev_db="" prev_container=""
    # shellcheck disable=SC1090
    . "$ASH_STATE_FILE"
    prev_own=${ASH_STATE_OWNERSHIP:-}
    prev_db=${ASH_STATE_DB:-}
    prev_container=${ASH_STATE_CONTAINER:-}
    if [ "$prev_own" = "created" ] \
       && [ "$prev_db" = "$ASH_DEMO_DB" ] \
       && [ "$prev_container" = "${ASH_DEMO_CONTAINER:-}" ]; then
      set -- "$1" created
    fi
  fi
  {
    echo "ASH_STATE_BACKEND=$1"
    echo "ASH_STATE_OWNERSHIP=$2"
    echo "ASH_STATE_DB=$ASH_DEMO_DB"
    echo "ASH_STATE_CONTAINER=${ASH_DEMO_CONTAINER:-}"
    echo "ASH_STATE_PORT=${ASH_DEMO_PORT:-}"
  } >"$ASH_STATE_FILE"
}

_bk_report() {
  local pgver
  pgver=$(ash_psql1 "select current_setting('server_version')")
  ash_log "backend=$ASH_BACKEND db=$ASH_DEMO_DB pg=$pgver ash=$(ash_psql1 'select version from ash.config')"
}

# ---------------------------------------------------------------------------
# backend_down
# ---------------------------------------------------------------------------

backend_down() {
  if [ "${ASH_KEEP_DB:-}" = "1" ]; then
    ash_log "ASH_KEEP_DB=1: leaving $ASH_DEMO_DB in place"
    return 0
  fi

  # Read the ownership ledger if we have one. Without it we still honour the
  # name globs, but we refuse to remove any resource we have no record of
  # creating. Keep sourced values local so repeated calls cannot inherit stale
  # state after the ledger has been cleared.
  local state_backend="" state_own="" state_db="" target_db=""
  local ASH_STATE_BACKEND="" ASH_STATE_OWNERSHIP="" ASH_STATE_DB=""
  local ASH_STATE_CONTAINER="" ASH_STATE_PORT=""
  if [ -f "$ASH_STATE_FILE" ]; then
    # shellcheck disable=SC1090
    . "$ASH_STATE_FILE"
    state_backend=${ASH_STATE_BACKEND:-}
    state_own=${ASH_STATE_OWNERSHIP:-}
    state_db=${ASH_STATE_DB:-}
  fi
  [ -n "$state_backend" ] || state_backend=$ASH_BACKEND

  case "$state_backend" in
    docker)
      _bk_assert_container_glob "${ASH_STATE_CONTAINER:-$ASH_DEMO_CONTAINER}"
      if [ "$state_own" = "created" ]; then
        ash_log "removing container ${ASH_STATE_CONTAINER:-$ASH_DEMO_CONTAINER}"
        _bk_remove_owned_container "${ASH_STATE_CONTAINER:-$ASH_DEMO_CONTAINER}" \
          || ash_die 3 "could not remove owned container ${ASH_STATE_CONTAINER:-$ASH_DEMO_CONTAINER} and its anonymous volumes"
      else
        ash_warn "container ${ASH_STATE_CONTAINER:-$ASH_DEMO_CONTAINER} was not created by this harness; leaving it alone"
      fi
      ;;
    local)
      target_db=${state_db:-$ASH_DEMO_DB}
      _bk_assert_db_glob "$target_db"
      if [ "$state_own" != "created" ]; then
        ash_warn "database $target_db was not created by this harness; leaving it alone"
      else
        [ "$target_db" = "$ASH_DEMO_DB" ] \
          || ash_die 3 "refusing database teardown: ledger owns '$target_db' but the selected database is '$ASH_DEMO_DB'"
        _bk_terminate_demo_backends "$target_db" || true
        ash_log "dropping database $target_db"
        ash_psql_maint -c \
          "drop database if exists \"$target_db\" with (force)" >/dev/null \
          || ash_die 3 "could not drop owned database $target_db; ownership ledger retained for retry"
      fi
      ;;
    remote)
      target_db=${state_db:-$ASH_DEMO_DB}
      _bk_assert_db_glob "$target_db"
      ash_warn "remote database $target_db was not created by this harness; leaving it alone"
      ;;
  esac

  rm -f "$ASH_STATE_FILE"
}

# _bk_terminate_demo_backends — server-side, filtered by database. `pkill -f`
# was measured unreliable (it races the shell that spawned the client and
# happily matches unrelated psql invocations).
_bk_terminate_demo_backends() {
  local target_db=${1:-$ASH_DEMO_DB}
  ash_psql_maint -tAc "
    select pg_terminate_backend(pid)
    from pg_stat_activity
    where datname = '$target_db'
      and pid <> pg_backend_pid()" >/dev/null 2>&1
}
