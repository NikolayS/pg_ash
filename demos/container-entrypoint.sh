#!/usr/bin/env bash
# container-entrypoint.sh — Runs inside the Postgres demo container.
#
# 1. Waits for Postgres to accept connections.
# 2. Creates the `demo` database, pg_stat_statements + pg_cron extensions.
# 3. Installs pg_ash from ASH_INSTALL_SQL (the host resolves this through
#    devel/scripts/ash_sql_chain.py; the released default is
#    /repo/sql/ash-install.sql).
# 4. Initializes pgbench.
# 5. Starts pg_ash sampling (1s).
# 6. Kicks off the workload.sh in the background — this produces the spike
#    that the recording will investigate moments later.
# 7. Writes ASH_DEMO_READY_FILE only after all setup above succeeds.
# 8. Sleeps forever so the container stays up for `docker exec` from the
#    tmux-driven recorder on the host.

set -Eeuo pipefail
IFS=$'\n\t'
export PAGER=cat

# UNIX-socket peer auth — no PGPASSWORD needed.
PSQL=(
  psql
  --no-psqlrc
  --quiet
  --no-align
  --tuples-only
  --set=ON_ERROR_STOP=1
  --username=postgres
  --host=/var/run/postgresql
)
INSTALL_SQL="${ASH_INSTALL_SQL:-/repo/sql/ash-install.sql}"
READY_FILE="${ASH_DEMO_READY_FILE:-/tmp/pg_ash_demo.ready}"

# A stale marker must never make the host mistake a failed retry for success.
rm -f "$READY_FILE"

echo "[entry] waiting for Postgres..."
until "${PSQL[@]}" \
  --dbname=postgres \
  --command="select 1" \
  >/dev/null 2>&1; do
  sleep 0.3
done
echo "[entry] Postgres ready"

# Create the database once. The remaining setup is deliberately re-apply-safe:
# if a prior attempt stopped after CREATE DATABASE, the next attempt repairs it
# instead of skipping installation forever.
if ! "${PSQL[@]}" \
  --dbname=postgres \
  --command="select 1 from pg_database where datname = 'demo'" \
  | grep -q 1; then
  echo "[entry] creating demo database"
  "${PSQL[@]}" \
    --dbname=postgres \
    --command="create database demo"
fi

# pg_cron's CREATE EXTENSION must run against the database named in
# cron.database_name (we set that to 'demo' in postgresql.conf).
echo "[entry] creating extensions"
"${PSQL[@]}" \
  --dbname=demo \
  --command="create extension if not exists pg_cron"
"${PSQL[@]}" \
  --dbname=demo \
  --command="create extension if not exists pg_stat_statements"

if [[ ! -r "$INSTALL_SQL" ]]; then
  echo "[entry] ERROR: pg_ash installer not readable: $INSTALL_SQL" >&2
  exit 1
fi

echo "[entry] installing pg_ash (\i $INSTALL_SQL)"
"${PSQL[@]}" \
  --dbname=demo \
  --file="$INSTALL_SQL" \
  >/dev/null

# Do not destroy an existing pgbench dataset when retrying setup in a kept
# container. A fresh demo database has no accounts table and is initialized.
if ! "${PSQL[@]}" \
  --dbname=demo \
  --command="select to_regclass('public.pgbench_accounts') is not null" \
  | grep -q t; then
  echo "[entry] initializing pgbench (scale 5)"
  pgbench -U postgres -d demo -i -s 5 -q >/dev/null 2>&1
fi

echo "[entry] starting pg_ash sampling (1s)"
"${PSQL[@]}" \
  --dbname=demo \
  --command="select ash.start('1 second')" \
  >/dev/null
if ! "${PSQL[@]}" \
  --dbname=demo \
  --command="select count(*) = 1 from cron.job where jobname = 'ash_sampler'" \
  | grep -q t; then
  echo "[entry] ERROR: ash.start() did not leave exactly one ash_sampler job" >&2
  exit 1
fi

echo "[entry] pg_ash status:"
"${PSQL[@]}" \
  --dbname=demo \
  --command="select metric, value from ash.status() where metric in ('version', 'sampling_enabled', 'pg_cron_available')"

# Kick off the workload. Send its logs to /tmp/workload.log for debugging;
# the investigation view never sees these.
echo "[entry] starting workload"
nohup bash /repo/demos/workload.sh >/tmp/workload.log 2>&1 &
WORKLOAD_PID=$!
echo "[entry] workload pid=$WORKLOAD_PID"

# Catch immediate launch failures before advertising readiness. Longer-running
# workload failures remain available in /tmp/workload.log for diagnostics.
sleep 0.1
if ! kill -0 "$WORKLOAD_PID" 2>/dev/null; then
  wait "$WORKLOAD_PID"
fi

touch "$READY_FILE"
echo "[entry] ready marker written: $READY_FILE"

echo "[entry] ready — sleeping forever so the recorder can exec psql"
# Keep container alive.
tail -f /dev/null
