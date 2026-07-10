#!/usr/bin/env bash
# smoke.sh — Short Docker-only validation of the pg_ash demo setup.
#
# This deliberately skips tmux/asciinema/agg and the five-minute warmup. It
# proves that the current discovered installer works in the baked demo image,
# pg_cron schedules the sampler, and at least one live sample is collected.

set -Eeuo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
REPO="$(cd "$HERE/.." && pwd)"
DOCKER="${DOCKER:-docker}"
PG_MAJOR="${PG_MAJOR:-18}"
IMAGE="${SMOKE_IMAGE:-pg_ash_demo:smoke-${PG_MAJOR}}"
CONTAINER="${SMOKE_CONTAINER:-pg_ash_demo_smoke_$$}"
TIMEOUT_SEC="${SMOKE_TIMEOUT_SEC:-120}"
READY_FILE="/tmp/pg_ash_demo.ready"
ENTRYPOINT_LOG="/tmp/pg_ash_demo-entrypoint.log"
ENTRYPOINT_STATUS="/tmp/pg_ash_demo-entrypoint.status"

log() { printf '[smoke %s] %s\n' "$(date +%H:%M:%S)" "$*"; }
die() { printf '[smoke %s] ERROR: %s\n' "$(date +%H:%M:%S)" "$*" >&2; exit 1; }

show_diagnostics() {
  log "demo entrypoint log:"
  $DOCKER exec "$CONTAINER" sh -c \
    "if [ -f '$ENTRYPOINT_LOG' ]; then cat '$ENTRYPOINT_LOG'; else echo '(entrypoint log missing)'; fi" \
    >&2 || true
  log "Postgres container log (last 100 lines):"
  $DOCKER logs --tail 100 "$CONTAINER" >&2 || true
}

cleanup() {
  local rc=$?
  $DOCKER rm -f "$CONTAINER" >/dev/null 2>&1 || true
  return "$rc"
}
trap cleanup EXIT INT TERM

command -v "$DOCKER" >/dev/null 2>&1 || die "Docker not found: $DOCKER"
command -v python3 >/dev/null 2>&1 || die "python3 not found"
[[ "$TIMEOUT_SEC" =~ ^[1-9][0-9]*$ ]] \
  || die "SMOKE_TIMEOUT_SEC must be a positive integer, got: $TIMEOUT_SEC"
INSTALL_REL="$(python3 "$REPO/devel/scripts/ash_sql_chain.py" fresh-install-path)"
INSTALL_SQL="${ASH_INSTALL_SQL:-/repo/$INSTALL_REL}"
EXPECTED_VERSION="$(python3 "$REPO/devel/scripts/ash_sql_chain.py" fresh-install-version)"

log "building $IMAGE (PG_MAJOR=$PG_MAJOR)"
$DOCKER build --build-arg "PG_MAJOR=$PG_MAJOR" -t "$IMAGE" "$HERE" >/dev/null

$DOCKER rm -f "$CONTAINER" >/dev/null 2>&1 || true
log "starting $CONTAINER"
$DOCKER run -d --name "$CONTAINER" \
  -e POSTGRES_PASSWORD=postgres \
  -e POSTGRES_DB=postgres \
  -v "$REPO":/repo:ro \
  "$IMAGE" \
  -c track_activity_query_size=4096 \
  -c log_min_messages=warning \
  >/dev/null

deadline=$(( SECONDS + TIMEOUT_SEC ))
until $DOCKER exec "$CONTAINER" pg_isready -U postgres -q >/dev/null 2>&1; do
  [ "$SECONDS" -lt "$deadline" ] \
    || { show_diagnostics; die "Postgres did not become ready within ${TIMEOUT_SEC}s"; }
  sleep 0.5
done

$DOCKER exec "$CONTAINER" rm -f \
  "$READY_FILE" "$ENTRYPOINT_LOG" "$ENTRYPOINT_STATUS"
# rc/status expansion belongs inside the container.
# shellcheck disable=SC2016
if ! $DOCKER exec -d \
  -e BASELINE_SEC=1 \
  -e SPIKE_SEC=4 \
  -e TAIL_SEC=1 \
  -e ASH_INSTALL_SQL="$INSTALL_SQL" \
  -e ASH_DEMO_READY_FILE="$READY_FILE" \
  "$CONTAINER" bash -c \
  'bash /repo/demos/container-entrypoint.sh > /tmp/pg_ash_demo-entrypoint.log 2>&1; rc=$?; printf "%s\n" "$rc" > /tmp/pg_ash_demo-entrypoint.status; exit "$rc"'; then
  show_diagnostics
  die "could not launch demo setup in container"
fi

log "waiting for setup readiness (installer=$INSTALL_SQL)"
while ! $DOCKER exec "$CONTAINER" test -f "$READY_FILE" 2>/dev/null; do
  if $DOCKER exec "$CONTAINER" test -f "$ENTRYPOINT_STATUS" 2>/dev/null; then
    setup_rc="$($DOCKER exec "$CONTAINER" cat "$ENTRYPOINT_STATUS" 2>/dev/null || echo unknown)"
    show_diagnostics
    die "demo setup exited before readiness (rc=$setup_rc)"
  fi
  container_running="$($DOCKER inspect -f '{{.State.Running}}' "$CONTAINER" 2>/dev/null || true)"
  if [ "$container_running" != "true" ]; then
    show_diagnostics
    die "demo container stopped before setup completed"
  fi
  [ "$SECONDS" -lt "$deadline" ] \
    || { show_diagnostics; die "demo setup did not become ready within ${TIMEOUT_SEC}s"; }
  sleep 0.5
done

psql_demo=(
  "$DOCKER" exec "$CONTAINER"
  psql -X -qAt -U postgres -d demo
)

actual_version="$("${psql_demo[@]}" -c "select version from ash.config where singleton")"
[ "$actual_version" = "$EXPECTED_VERSION" ] \
  || die "version mismatch: expected $EXPECTED_VERSION, got $actual_version"

cron_available="$("${psql_demo[@]}" -c "select value from ash.status() where metric='pg_cron_available'")"
[ "$cron_available" = "yes" ] \
  || die "pg_cron_available should be yes, got: $cron_available"

sampler_jobs="$("${psql_demo[@]}" -c "select count(*) from cron.job where jobname='ash_sampler'")"
[ "$sampler_jobs" = "1" ] \
  || die "expected one ash_sampler job, got: $sampler_jobs"

sample_deadline=$(( SECONDS + 15 ))
while :; do
  sample_count="$("${psql_demo[@]}" -c "select count(*) from ash.sample")"
  [ "$sample_count" -gt 0 ] && break
  [ "$SECONDS" -lt "$sample_deadline" ] \
    || { show_diagnostics; die "ash_sampler produced no samples within 15s"; }
  sleep 1
done

log "PASS: version=$actual_version pg_cron=$cron_available sampler_jobs=$sampler_jobs samples=$sample_count"
