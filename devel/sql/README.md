# Development SQL staging

Keep in-progress SQL for the next release here.

After a release tag, `sql/` is frozen at the tagged baseline. The stamp
retains a byte-identical `devel/sql/ash-install.sql` baseline for the next
cycle. Subsequent SQL changes edit that development installer. After a final
release, the first SQL change also adds a connected
`devel/sql/ash-X.Y-to-X.Z.sql` migration, promoted to `sql/migrations/` at the
next stamp. Prerelease migration refreshes follow the rule below.

Release stamping promotes the installer into `sql/` and the upgrade script into
`sql/migrations/`, with a root-level compatibility wrapper left under `sql/`.

Do not edit `sql/*.sql` in feature or bug-fix PRs. CI allows released SQL
changes only from a release-stamp PR. See `docs/RELEASE_PROCESS.md`.

A current prerelease may also stage a same-named refresh of its released
cumulative migration here. Its include targets `ash-install.sql` in this
directory. Discovery permits only that exact current-prerelease edge with a
matching installer release line; older and finalized edges remain immutable.
The release stamp promotes the tested migration, changes its include to
`../ash-install.sql`, removes the staged migration, and retains the identical
development installer baseline. Public entrypoints are rehearsed beforehand
in a temporary promoted tree by `devel/tests/promoted_upgrade_paths.py`.
