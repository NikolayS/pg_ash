# demos/Dockerfile — pre-baked Postgres image for the pg_ash demo recorder.
#
# Bakes pg_cron (+ pg_stat_statements, which ships with contrib) and the
# shared_preload_libraries / cron config INTO the image, so record.sh no longer
# apt-installs pg_cron and restarts the container at record time. That removes
# the run-time dependency on the PGDG apt mirror (which occasionally lags) and
# shaves the install+restart round-trip off every recording.
#
# Build (record.sh does this automatically when this file is present):
#   docker build --build-arg PG_MAJOR=18 -t pg_ash_demo:18 demos/
#
# pg_stat_statements needs no package — it is part of the postgres contrib set
# already in the base image; only the preload line below activates it.

ARG PG_MAJOR=18
FROM postgres:${PG_MAJOR}
ARG PG_MAJOR

RUN apt-get update \
 && apt-get install -y --no-install-recommends "postgresql-${PG_MAJOR}-cron" \
 && rm -rf /var/lib/apt/lists/*

# Append to the sample config initdb copies into every fresh PGDATA, so a
# first-boot cluster comes up with the extensions preloaded — no restart.
# cron.database_name matches the demo DB created by container-entrypoint.sh.
RUN cat >> /usr/share/postgresql/postgresql.conf.sample <<'CONF'

# --- pg_ash demo (baked by demos/Dockerfile) ---
shared_preload_libraries = 'pg_cron,pg_stat_statements'
cron.database_name = 'demo'
cron.use_background_workers = on
CONF
