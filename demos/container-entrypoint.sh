#!/usr/bin/env bash
# container-entrypoint.sh — Runs inside the postgres:18 container.
#
# 1. Waits for PostgreSQL to accept connections.
# 2. Creates the `demo` database, pg_stat_statements + pg_cron extensions.
# 3. Installs pg_ash (from the mounted /repo/sql/ash-install.sql).
# 4. Initializes pgbench.
# 5. Starts pg_ash sampling (1s).
# 6. Kicks off the workload.sh in the background — this produces the spike
#    that the recording will investigate moments later.
# 7. Sleeps forever so the container stays up for `docker exec` from the
#    tmux-driven recorder on the host.

set -euo pipefail

# UNIX-socket peer auth — no PGPASSWORD needed.
export PSQL="psql -X -qAt -U postgres -h /var/run/postgresql"

echo "[entry] waiting for PostgreSQL..."
until $PSQL -d postgres -c "select 1" >/dev/null 2>&1; do sleep 0.3; done
echo "[entry] PostgreSQL ready"

# Only set up once — safe for container restarts.
if ! $PSQL -d postgres -tc "select 1 from pg_database where datname='demo'" | grep -q 1; then
  echo "[entry] creating demo db + extensions"
  $PSQL -d postgres -c "create database demo"
  # pg_cron's CREATE EXTENSION must run against the database named in
  # cron.database_name (we set that to 'demo' in postgresql.conf).
  $PSQL -d demo -c "create extension if not exists pg_cron"
  $PSQL -d demo -c "create extension if not exists pg_stat_statements"

  echo "[entry] installing pg_ash (\i /repo/sql/ash-install.sql)"
  $PSQL -d demo -f /repo/sql/ash-install.sql >/dev/null

  echo "[entry] initializing pgbench (scale 5)"
  pgbench -U postgres -d demo -i -s 5 -q >/dev/null 2>&1

  echo "[entry] starting pg_ash sampling (1s)"
  $PSQL -d demo -c "select ash.start('1 second')" >/dev/null

  echo "[entry] pg_ash status:"
  $PSQL -d demo -c "select metric, value from ash.status() where metric in ('version','sampling_enabled','pg_cron_available')"
fi

# Kick off the workload. Send its logs to /tmp/workload.log for debugging;
# the investigation view never sees these.
echo "[entry] starting workload"
nohup bash /repo/demos/workload.sh >/tmp/workload.log 2>&1 &
echo "[entry] workload pid=$!"

echo "[entry] ready — sleeping forever so the recorder can exec psql"
# Keep container alive.
tail -f /dev/null
