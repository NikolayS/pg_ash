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
