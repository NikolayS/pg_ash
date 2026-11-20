# Development SQL staging

Keep in-progress SQL for the next release here.

After a release tag, `sql/` is frozen at the tagged baseline. The first
SQL-changing PR for the next development cycle should add:

- `devel/sql/ash-install.sql` — future final installer
- `devel/sql/ash-X.Y-to-X.Z.sql` — future upgrade script, promoted to
  `sql/migrations/` at release stamp time

Release stamping promotes the installer into `sql/` and the upgrade script into
`sql/migrations/`, with a root-level compatibility wrapper left under `sql/`.

Do not edit `sql/*.sql` in feature or bug-fix PRs. CI allows released SQL
changes only from a release-stamp PR. See `docs/RELEASE_PROCESS.md`.

## PostgreSQL 19 development upgrade compatibility

`compat/ash-1.5-to-2.0.sql` is the development replacement for the frozen
migration on PostgreSQL 19. The chain helper selects it on that server major
or later. It permits only the server-owned `pg_attver` attribute option with
an unsigned decimal value. Other column options still block normalization.
The server assigns new attribute versions when the table is rebuilt.

Use `python3 devel/scripts/ash_sql_chain.py pinned-upgrade-chain 1.5` to generate
the development upgrade path, then run that file from the repository root
with `psql -X -v ON_ERROR_STOP=1 -f <path>`. The helper appends the development
installer. The frozen public wrapper is unchanged and does **not** support
this PostgreSQL 19 catalog representation until a release stamp promotes the
replacement. Older server majors still exercise the frozen migration.
