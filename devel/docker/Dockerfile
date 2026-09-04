# pg_cron-capable PostgreSQL image for the release gate.
#
# Keep the image tag separate from PostgreSQL's numeric major. In particular,
# postgres:19beta3 exports PG_MAJOR=19, which is the value required by the
# postgresql-19-cron package name.
ARG POSTGRES_TAG=18
FROM postgres:${POSTGRES_TAG}

# pg_cron versions differ by PostgreSQL major and follow the pinned base
# image's PGDG repository snapshot.
# hadolint ignore=DL3008
RUN apt-get update \
 && apt-get install -y --no-install-recommends "postgresql-${PG_MAJOR}-cron" \
 && rm -rf /var/lib/apt/lists/*

# initdb copies this sample into each new cluster, so both libraries are loaded
# on first start. cron.database_name is intentionally not baked in: callers can
# select the database at runtime with:
#
#   postgres -c cron.database_name=<database>
RUN printf '%s\n' \
    '' \
    '# pg_ash release-gate defaults' \
    "shared_preload_libraries = 'pg_cron,pg_stat_statements'" \
    'cron.use_background_workers = on' \
    >> /usr/share/postgresql/postgresql.conf.sample
