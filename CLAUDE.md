# CLAUDE.md

## Engineering Standards

Follow the rules at https://gitlab.com/postgres-ai/rules/-/tree/main/rules — always pull latest before starting work.

SQL style guide: https://gitlab.com/postgres-ai/rules/-/blob/main/rules/development__db-sql-style-guide.mdc

## Deployment

CI via GitHub Actions:

- **test.yml**: runs on push and PRs — tests across PostgreSQL 14, 15, 16, 17, 18
- Tests: fresh development install, discovered full upgrade chain up to the in-progress release, schema equivalence between fresh install and upgrade chain, idempotent re-apply of discovered re-apply-safe upgrade scripts, degraded mode (no pgss/pg_cron)
- Version schema: `vMAJOR.MINOR` (e.g. `v1.3`). Tag on main after all PRs merged.

After a release tag, keep `sql/` frozen at the latest released baseline until the next release-stamp PR. Current development SQL lives under `devel/sql/`: the future final installer and the future upgrade script. CI must discover version chains from files via `devel/scripts/ash_sql_chain.py`, not hardcode concrete version numbers. At release stamp time, promote the development SQL into `sql/`, bump `ash.config.version`, and remove or recreate `devel/sql/` for the next cycle. See `docs/RELEASE_PROCESS.md`.

## Testing

Red/green TDD: write failing tests first, then fix the code to make them pass.

- **Bug fixes**: always write a test that reproduces the bug (RED), then fix (GREEN). This proves the fix works and prevents regressions.
- **New features**: write tests for the expected behavior before or alongside the implementation. Run current development tests locally with `sudo -u postgres psql -v ON_ERROR_STOP=1 -f devel/sql/ash-install.sql` and DO blocks with assertions.
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
  - All new features introduced since the previous release, **exercised behaviorally** (i.e. call the function, assert the side effect — not just check the function exists in the catalog)
  - All existing features (privilege hardening, rollups, edge cases, **degraded mode without pg_cron and/or pg_stat_statements** — not just the new stuff)
  - Demo / docs reproducibility (e.g. `make -C demos record`)
- Test against real Postgres in Docker — CI green is necessary but not sufficient.
- If any agent reports a finding, file an issue, fix it, and re-run the **entire** pass. A partial re-run does not satisfy the gate; the whole suite must come back clean.

Only after the comprehensive pass returns clean: bump version, generate the upgrade script for the new release, write release notes, tag, and publish. Skipping the gate means shipping bugs against a tagged version — issues #46, #49, #51, #52, #53, #54, #61, #63 surfaced post-tag during pre-1.4 validation cycles and would have blocked the tag if this gate had been in place.

## Stack

- Pure SQL + PL/pgSQL (no extensions, no `.control` file)
- Anti-extension design: `\i` to install, works on RDS/Cloud SQL/Supabase/AlloyDB/Neon
- Optional pg_cron integration for automated sampling
- Optional pg_stat_statements for query text and execution metrics
