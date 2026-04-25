# CLAUDE.md

## Engineering Standards

Follow the rules at https://gitlab.com/postgres-ai/rules/-/tree/main/rules — always pull latest before starting work.

SQL style guide: https://gitlab.com/postgres-ai/rules/-/blob/main/rules/development__db-sql-style-guide.mdc

## Deployment

CI via GitHub Actions:

- **test.yml**: runs on push and PRs — tests across PostgreSQL 14, 15, 16, 17, 18
- Tests: fresh install, full upgrade chain up to the in-progress release (currently 1.0→…→1.5), schema equivalence between fresh install and upgrade chain, idempotent re-apply of each legacy upgrade script, degraded mode (no pgss/pg_cron)
- Version schema: `vMAJOR.MINOR` (e.g. `v1.3`). Tag on main after all PRs merged.

`ash-install.sql` is the source of truth. Upgrade scripts for the **in-progress** release (e.g. `ash-1.4-to-1.5.sql` while 1.5 is unreleased) must stay in lockstep with install.sql through the release cycle — any install.sql change that affects schema (new/modified function bodies, new columns, dropped objects) must be mirrored into the in-progress upgrade script. Schema-equivalence CI enforces this on every PR. **Finalized** legacy upgrade scripts (`ash-1.0-to-1.1.sql` through `ash-1.3-to-1.4.sql` today) are immutable: any signature-changing PR must avoid patterns that trip their idempotent re-apply, even if that means restructuring the fix (e.g. keeping an old function signature and using a new helper name instead of changing the return type).

## Testing

Red/green TDD: write failing tests first, then fix the code to make them pass.

- **Bug fixes**: always write a test that reproduces the bug (RED), then fix (GREEN). This proves the fix works and prevents regressions.
- **New features**: write tests for the expected behavior before or alongside the implementation. Run tests locally with `sudo -u postgres psql -v ON_ERROR_STOP=1 -f sql/ash-install.sql` and DO blocks with assertions.
- **CI tests** live in `.github/workflows/test.yml`. Each test section uses PL/pgSQL `DO $$ ... assert ... $$` blocks. Assert exact values, not just row existence — a test that only checks "row exists" can't distinguish correct aggregation from garbage.
- **Test locally on available PG version** before pushing. CI covers PG 14–18.

## Code Review

All changes go through PRs. Before merging, run a REV review (https://gitlab.com/postgres-ai/rev/) and post the report as a PR comment. REV is designed for GitLab but works on GitHub PRs too.

Never merge without explicit approval from the project owner.

## Release gate

**Tagging a release is blocked** until a comprehensive test pass against the release-candidate `main` returns a fully clean result. The pass MUST:

- Spawn parallel agents (one per test surface) — minimum coverage:
  - Fresh install + regression on every supported PG version
  - Full upgrade chain (1.0 → … → release candidate) + idempotent re-apply + schema equivalence
  - All new features introduced since the previous release, exercised behaviorally
  - All existing features (privilege hardening, rollups, edge cases — not just the new stuff)
  - Demo / docs reproducibility (e.g. `make -C demos record`)
- Test against real Postgres in Docker — CI green is necessary but not sufficient.
- If any agent reports a finding, file an issue, fix it, and re-run the **entire** pass. A partial re-run does not satisfy the gate; the whole suite must come back clean.

Only after the comprehensive pass returns clean: bump version, generate the upgrade script for the new release, write release notes, tag, and publish. Skipping the gate means shipping bugs that get filed against a tagged version (v1.4 hit this — issues #46, #49, #51, #52, #53, #54, #61, #63 were all surfaced post-tag and would have blocked it).

## Stack

- Pure SQL + PL/pgSQL (no extensions, no `.control` file)
- Anti-extension design: `\i` to install, works on RDS/Cloud SQL/Supabase/AlloyDB/Neon
- Optional pg_cron integration for automated sampling
- Optional pg_stat_statements for query text and execution metrics
