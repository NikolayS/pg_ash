# Development SQL staging

Keep in-progress SQL for the next release here.

After a release tag, `sql/` is frozen at the tagged baseline. The first
SQL-changing PR for the next development cycle should add:

- `devel/sql/ash-install.sql` — future final installer
- `devel/sql/ash-X.Y-to-X.Z.sql` — future upgrade wrapper/script

Release stamping promotes those files into `sql/`.
