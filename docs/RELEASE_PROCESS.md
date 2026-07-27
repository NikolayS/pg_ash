# pg_ash development and release SQL process

## Version identity and prereleases

Release tags use three-part semantic versions. The payload version is the tag
with its leading `v` removed:

| Stage | Git tag | `ash.config.version` |
|---|---|---|
| Development snapshot | `vX.Y.Z-dev.N` | `X.Y.Z-dev.N` |
| Alpha | `vX.Y.Z-alpha.N` | `X.Y.Z-alpha.N` |
| Beta | `vX.Y.Z-beta.N` | `X.Y.Z-beta.N` |
| Release candidate | `vX.Y.Z-rc.N` | `X.Y.Z-rc.N` |
| Final release | `vX.Y.Z` | `X.Y.Z` |

The progression is dev, alpha, beta, release candidate, then final. Alpha,
beta, and release-candidate tags are releases, not mutable development
markers. Their tags and payloads are immutable, and `sql/` freezes after each
one just as it does after a final release. If SQL changes after a prerelease,
the changed payload is staged under `devel/sql/` and receives a new identity in
the next release-stamp PR; it must not be published under the old tag.

Every release-stamp PR must make all release-identity surfaces agree:

- the `ash.config.version` column default, singleton-row update, and altered
  column default in the released installer;
- the installer and current-line migration comments;
- README and release-note version text and examples;
- security/support metadata, the git tag, and the GitHub release.

Only the git tag has the leading `v`. CI runs
`devel/scripts/check_release_stamp.py` in a manual pre-tag workflow and again
for every `v*` tag. It rejects a tag whose syntax is not one of the forms
above, whose identity differs from the released installer, or whose three
`ash.config.version` stamp sites disagree.

The historical `v2.0-alphaN` and `v2.0-beta1` tags predate the three-part,
dotted-stage syntax. They remain immutable but are not templates for new tags.
In particular, the final 2.0 identity is `v2.0.0` in git and `2.0.0` in the
payload.

## Between releases

After any release tag, including a prerelease, `sql/` is frozen at the latest
tagged baseline. Finalized upgrade scripts under `sql/migrations/` are
immutable, their compatibility wrappers under `sql/ash-X.Y-to-A.B.sql` are
immutable, and `sql/ash-install.sql` represents the latest tagged release.

For the next development cycle, create a `devel/sql/` area:

- `devel/sql/ash-install.sql` is the in-progress future final installer.
- For a new stable release line, `devel/sql/ash-X.Y-to-A.B.sql` is its
  in-progress cumulative upgrade script.

All post-release SQL changes must be made in `devel/sql/`, not in released files
under `sql/`. After a prerelease of the current line, its released cumulative
migration remains the public entry point and the next candidate installer is
staged under `devel/sql/`; that migration may change again only in a later
release-stamp PR.

CI must not hardcode concrete version chains. It uses
`devel/scripts/ash_sql_chain.py` to discover released installers, released
upgrade scripts under `sql/migrations/`, and in-progress `devel/sql/` upgrade
scripts from the files present in the checkout.

CI must test both supported development paths discovered by that helper:

- fresh development install: the helper's `fresh-install-path`
- fresh development install version: the helper's `fresh-install-version`
- upgrade path: the helper's `full-upgrade-chain`

After a prerelease, when a candidate development installer exists without a
new current-line migration, the helper appends that installer to the full
upgrade and re-apply chains as a re-apply-safe overlay. This makes the beta to
final path exercise the same candidate as a fresh install without inventing a
prerelease-specific migration filename. Schema-equivalence CI must compare
those two paths.

## CI guard

CI rejects ordinary PRs that modify any `sql/**/*.sql` file. To make an
intentional release-stamp PR, use a branch starting with `release/` or
`release-`, or a PR title starting with `release:`.

That guard is deliberately conservative: if unreleased SQL lands directly under
`sql/`, a user can copy `sql/ash-install.sql` from `main` and reasonably assume
it is the published release payload.

## Release stamp

Do not start a release-stamp PR until the development candidate has passed the
entire comprehensive release gate in `CLAUDE.md`. The stamp is the mechanical
promotion and identity step after that clean result, not a substitute for it.

Right before tagging any prerelease or final release, use a release-stamp PR to
promote the development SQL into released core SQL:

1. Replace `sql/ash-install.sql` with `devel/sql/ash-install.sql`.
2. For the first stamp of a new stable line, move
   `devel/sql/ash-X.Y-to-A.B.sql` to
   `sql/migrations/ash-X.Y-to-A.B.sql`. For a later prerelease or the final
   stamp of that line, retain the same cumulative migration entry point,
   refresh its release-identity comments on every stamp, and change its
   executable body only if the current-line contract changed.
3. Set all three `ash.config.version` stamps and top-of-file version comments
   to the exact tag identity without the leading `v`.
4. Update release notes and README install/upgrade instructions.
5. Add or update a root-level `sql/ash-X.Y-to-A.B.sql` compatibility wrapper
   that includes the canonical migration under `sql/migrations/`.
6. Leave CI on discovery-based helpers; remove only obsolete `devel/sql/`
   references if the helper no longer emits them after promotion.
7. From the exact candidate commit, dispatch `test.yml` with the intended tag
   as `release_tag`. Its stamp check and CI suite, followed by the full release
   gate, must pass before tagging; the tag push runs the same identity check
   again.

After the tag, keep an empty `devel/sql/` area as the next development-cycle
landing zone. The first SQL-changing PR after a final release adds the next
`devel/sql/ash-install.sql` and cumulative
`devel/sql/ash-X.Y-to-X.Z.sql` files there before touching released `sql/`
files. After a prerelease, stage the next installer there while retaining the
current line's cumulative migration path for the next stamp.

### Prerelease to final

A prerelease-to-final release-stamp promotes the final development installer,
keeps the current line's cumulative migration entry point, replaces
`X.Y.Z-stage.N` with `X.Y.Z` on every release-identity surface, and leaves all
older finalized migrations unchanged. The current-line migration must accept:

- the preceding stable release;
- every earlier prerelease of its target release, including a historical
  nonconforming stamp;
- the final stamp itself, so re-application remains safe.

Migration filenames name stable release lines (`X.Y`); `ash.config.version`
carries the complete `X.Y.Z-stage.N` or `X.Y.Z` payload identity. For the 2.0
line, beta and stable users therefore use the same canonical migration:

```sql
\i sql/migrations/ash-1.5-to-2.0.sql
```

The root `sql/ash-1.5-to-2.0.sql` file remains a compatibility wrapper. A user
with the historical `2.0-beta1` payload checks out the final `v2.0.0` release
and runs the canonical migration above. Its re-apply-safe installer accepts the
beta stamp as a valid source, stores `2.0.0` in both the config row and column
default, and makes the diagnostic report:

```text
 version | 2.0.0
```

from `select * from ash.status() where metric = 'version';`. No separate
beta-to-final migration filename is created.

## Legacy upgrade scripts

Finalized upgrade scripts are immutable. The five finalized 1.x transitions,
`sql/migrations/ash-1.0-to-1.1.sql` through
`sql/migrations/ash-1.4-to-1.5.sql`, and their five root-level compatibility
wrappers must not be rewritten except for an explicit emergency
backport/re-release decision.

The current-line `sql/migrations/ash-1.5-to-2.0.sql` migration is provisional
across 2.0 prereleases and becomes finalized and immutable at the stable
`v2.0.0` tag. Each prerelease tag still freezes its checked-in copy; changes
are allowed only in a subsequent release-stamp PR.
