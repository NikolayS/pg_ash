# pg_ash development and release SQL process

## Between releases

After a release tag, `sql/` is frozen at the latest released baseline. Finalized
upgrade scripts are immutable, and `sql/ash-install.sql` represents the latest
tagged release.

For the next development cycle, create a `devel/sql/` area:

- `devel/sql/ash-install.sql` is the in-progress future final installer.
- `devel/sql/ash-X.Y-to-A.B.sql` is the in-progress future upgrade script.

All post-release SQL changes must be made in `devel/sql/`, not in released files
under `sql/`.

CI must not hardcode concrete version chains. It uses
`devel/scripts/ash_sql_chain.py` to discover released installers, released
upgrade scripts, and in-progress `devel/sql/` upgrade scripts from the files
present in the checkout.

CI must test both supported development paths discovered by that helper:

- fresh development install: the helper's `fresh-install-path`
- upgrade path: the helper's `full-upgrade-chain`

Schema-equivalence CI must compare those two paths.

## Release stamp

Right before tagging a release, use a release-stamp PR to promote the
development SQL into released core SQL:

1. Replace `sql/ash-install.sql` with `devel/sql/ash-install.sql`.
2. Move `devel/sql/ash-X.Y-to-A.B.sql` to `sql/ash-X.Y-to-A.B.sql`.
3. Bump `ash.config.version` and top-of-file version comments to the release
   version.
4. Update release notes and README install/upgrade instructions.
5. Leave CI on discovery-based helpers; remove only obsolete `devel/sql/`
   references if the helper no longer emits them after promotion.
6. Run the full release gate before tagging.

After the tag, keep an empty `devel/sql/` area as the next development-cycle
landing zone. The first SQL-changing PR after a release adds the next
`devel/sql/ash-install.sql` and `devel/sql/ash-X.Y-to-X.Z.sql` files there
before touching released `sql/` files.

## Legacy upgrade scripts

Finalized upgrade scripts are immutable. Do not rewrite `sql/ash-1.0-to-1.1.sql`
through the latest tagged upgrade script except for an explicit emergency
backport/re-release decision.
