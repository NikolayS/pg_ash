#!/usr/bin/env bash
#
# ash_sampler.sh — External sampler for pg_ash (alternative to pg_cron)
#
# Calls ash.take_sample() in a loop. Also handles ash.rotate() when due.
#
# Usage:
#   ./ash_sampler.sh                     # defaults: 1s interval, connect via libpq env vars
#   ./ash_sampler.sh -i 5                # sample every 5 seconds
#   ./ash_sampler.sh -d mydb             # specify database
#   ./ash_sampler.sh -d mydb -U postgres # specify database and user
#
# Runs until killed (Ctrl-C / SIGTERM). Suitable for:
#   - Direct execution:  nohup ./ash_sampler.sh &
#   - systemd service:   see README for unit file example
#   - screen / tmux
#
# Rotation: checks on each tick whether rotation is due and calls ash.rotate()
# automatically (ash.rotate() is a no-op if called too early).

set -euo pipefail

INTERVAL=1
PSQL_ARGS=()
ROTATION_CHECK_EVERY=3600  # check rotation every N seconds (default: hourly)

usage() {
  echo "Usage: $0 [-i SECONDS] [-d DBNAME] [-U USER] [-h HOST] [-p PORT]"
  echo "  -i  Sampling interval in seconds (default: 1)"
  echo "  -d  Database name"
  echo "  -U  Database user"
  echo "  -h  Database host"
  echo "  -p  Database port"
  exit 1
}

while getopts "i:d:U:h:p:" opt; do
  case $opt in
    i) INTERVAL="$OPTARG" ;;
    d) PSQL_ARGS+=("-d" "$OPTARG") ;;
    U) PSQL_ARGS+=("-U" "$OPTARG") ;;
    h) PSQL_ARGS+=("-h" "$OPTARG") ;;
    p) PSQL_ARGS+=("-p" "$OPTARG") ;;
    *) usage ;;
  esac
done

run_sql() {
  psql -qAtX "${PSQL_ARGS[@]}" -c "$1" 2>/dev/null
}

echo "pg_ash sampler: interval=${INTERVAL}s, psql args: ${PSQL_ARGS[*]:-<default>}"
echo "Press Ctrl-C to stop."

ticks_since_rotation_check=0

cleanup() {
  echo ""
  echo "Sampler stopped."
  exit 0
}
trap cleanup SIGINT SIGTERM

while true; do
  run_sql "SET statement_timeout = '500ms'; SELECT ash.take_sample();" || true

  # Periodic rotation check
  ticks_since_rotation_check=$((ticks_since_rotation_check + INTERVAL))
  if [ "$ticks_since_rotation_check" -ge "$ROTATION_CHECK_EVERY" ]; then
    run_sql "SELECT ash.rotate();" || true
    ticks_since_rotation_check=0
  fi

  sleep "$INTERVAL"
done
