# pg_ash development and release SQL process

CI triggers, the `ci-required` aggregator, and the branch-ruleset
required-status-checks configuration live in [CI.md](CI.md).

## Version identity and prereleases

Release tags use the repository's two-part version scheme. Prerelease stages
append the stage name and counter directly to that version. The payload version
is the tag with its leading `v` removed:

| Stage | Git tag | `ash.config.version` |
|---|---|---|
| Alpha | `vX.Y-alphaN` | `X.Y-alphaN` |
| Beta | `vX.Y-betaN` | `X.Y-betaN` |
| Release candidate | `vX.Y-rcN` | `X.Y-rcN` |
| Final release | `vX.Y` | `X.Y` |

The progression is alpha, beta, release candidate, then final. Alpha, beta,
and release-candidate tags are releases, not mutable development markers.
Their tags and payloads are immutable, and `sql/` freezes after each one just
as it does after a final release. If SQL changes after a prerelease, the
changed payload is staged under `devel/sql/` and receives a new identity in the
next release-stamp PR; it must not be published under the old tag.

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
`ash.config.version` stamp sites disagree. This preserves the scheme used by
all existing tags, including `v1.0` through `v1.5`, `v2.0-alpha1`, and
`v2.0-beta1`. The final 2.0 identity is `v2.0` in git and `2.0` in the payload.

## Between releases

After any release tag, including a prerelease, `sql/` is frozen at the latest
tagged baseline. Finalized upgrade scripts under `sql/migrations/` are
immutable, their compatibility wrappers under `sql/ash-X.Y-to-A.B.sql` are
immutable, and `sql/ash-install.sql` represents the latest tagged release.

For the next development cycle, create a `devel/sql/` area:

- `devel/sql/ash-install.sql` is the in-progress future final installer. The
  release-stamp PR recreates it immediately, as a **byte-identical copy** of
  the `sql/ash-install.sql` it just promoted.
- For a new stable release line, `devel/sql/ash-X.Y-to-A.B.sql` is its
  in-progress cumulative upgrade script.

Recreating the baseline copy in the stamp PR — rather than leaving the first
feature PR of the cycle to create it — matters for review, not tidiness. The
installer is ~8000 lines, so whichever PR introduces it shows an ~8000-line
addition that buries its actual change. Landing the copy on its own, where the
whole diff is provably a copy, keeps every later PR's diff equal to its real
delta.

A reviewer can verify a claimed copy in one command:

```bash
git diff origin/main:sql/ash-install.sql <branch>:devel/sql/ash-install.sql
```

Empty output means byte-identical. `shasum -a 256` on both paths is equivalent
and easier to quote in a PR description.

All post-release SQL changes must be made in `devel/sql/`, not in released files
under `sql/`. After a prerelease of the current line, its released cumulative
migration remains the public entry point and the next candidate installer is
staged under `devel/sql/`; that migration may change again only in a later
release-stamp PR.

When that current-line cumulative migration needs executable changes before
another prerelease, stage a same-named copy in `devel/sql/` alongside its
installer. Its relative include targets `ash-install.sql` in that directory.
Discovery permits this replacement only for the current prerelease's exact
source/target edge and a matching development installer release line; older
or finalized edges cannot be overridden. Full, pinned and reapply development
paths execute the staged transaction instead of first publishing the old
released wrapper's intermediate schema. `promoted_upgrade_paths.py` separately
rehearses the canonical and root public entrypoints in a temporary promoted
layout. During the stamp, promote the tested migration, change its include to
`../ash-install.sql`, remove the staged migration, and retain the byte-identical
development installer baseline.

CI must not hardcode concrete version chains. It uses
`devel/scripts/ash_sql_chain.py` to discover released installers, released
upgrade scripts under `sql/migrations/`, and in-progress `devel/sql/` upgrade
scripts from the files present in the checkout.

CI must test all supported development paths discovered by that helper:

- fresh development install: the helper's `fresh-install-path`
- fresh development install version: the helper's `fresh-install-version`
- upgrade path: the helper's `full-upgrade-chain`
- pinned previous-release path: the helper's `pinned-upgrade-chain X.Y`

After a prerelease, when a candidate development installer exists without a
new current-line migration, the helper first verifies that the released
migrations connect the requested starting version to the released head. It
then verifies that both installers remain on that prerelease's `X.Y` line and
appends the candidate to the full-upgrade, pinned public-wrapper, and re-apply
chains as a re-apply-safe overlay. A lone installer for another release line is
rejected before any SQL is emitted. Once the shipped payload is final, a
connected development migration is mandatory even when the candidate has not
yet received its next release identity. This makes the beta-to-final path
exercise the same candidate as a fresh install without allowing the overlay to
hide a missing migration or inventing a prerelease-specific migration filename.
The helper also requires the promoted released installer to name the head of
the released migration graph, so promotion cannot make compatibility shims hide
an omitted edge. Schema-equivalence CI must compare those paths.

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
6. Recreate `devel/sql/ash-install.sql` as a byte-identical copy of the
   `sql/ash-install.sql` promoted in step 1, in this same PR. Verify with
   `git diff HEAD:sql/ash-install.sql HEAD:devel/sql/ash-install.sql`
   (empty output). The next cycle then begins with the baseline already
   present, so no feature PR carries an ~8000-line copy alongside its own
   change.
   A no-op overlay is safe for CI: `ash_sql_chain.py` still discovers it and
   appends it to the full, re-apply and pinned upgrade paths, and applying an
   installer identical to the released one is exactly the idempotent re-apply
   those paths already assert.
7. Leave CI on discovery-based helpers; remove only obsolete `devel/sql/`
   references if the helper no longer emits them after promotion.
8. From the exact candidate commit, dispatch `test.yml` with the intended tag
   as `release_tag`. Its stamp check and CI suite, followed by the full release
   gate, must pass before tagging; the tag push runs the same identity check
   again.

After the tag, retain the byte-identical `devel/sql/ash-install.sql` baseline
created in the stamp PR. The first SQL-changing PR after a final release also
adds the next connected cumulative `devel/sql/ash-X.Y-to-X.Z.sql` migration.
After a prerelease, edit that development installer and stage any current-line
migration refresh there; retain the released entrypoint until the next stamp.
Never edit the tagged payload under its existing identity.

### Prerelease to final

A prerelease-to-final release-stamp promotes the final development installer,
keeps the current line's cumulative migration entry point, replaces
`X.Y-stageN` with `X.Y` on every release-identity surface, and leaves all
older finalized migrations unchanged. The current-line migration must accept:

- the preceding stable release;
- every earlier prerelease of its target release, including `2.0-beta1`;
- the final stamp itself, so re-application remains safe.

Migration filenames and `ash.config.version` both use the `X.Y` release line;
prerelease payloads add `-stageN` to the version stamp. For the 2.0 line, beta
and stable users therefore use the same canonical migration:

```sql
\i sql/migrations/ash-1.5-to-2.0.sql
```

The root `sql/ash-1.5-to-2.0.sql` file remains a compatibility wrapper. A user
with the `2.0-beta1` payload checks out the final `v2.0` release
and runs the canonical migration above. Its re-apply-safe installer accepts the
beta stamp as a valid source, stores `2.0` in both the config row and column
default, and makes the diagnostic report:

```text
 version | 2.0
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
`v2.0` tag. Each prerelease tag still freezes its checked-in copy; changes
are allowed only in a subsequent release-stamp PR.
