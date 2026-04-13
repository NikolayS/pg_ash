# CLAUDE.md

## Engineering Standards

Follow the rules at https://gitlab.com/postgres-ai/rules/-/tree/main/rules — always pull latest before starting work.

SQL style guide: https://gitlab.com/postgres-ai/rules/-/blob/main/rules/development__db-sql-style-guide.mdc

## Deployment

CI via GitHub Actions:

- **test.yml**: runs on push and PRs — tests across PostgreSQL 14, 15, 16, 17, 18
- Tests: fresh install, upgrade path (1.0→1.1→1.2→1.3), schema equivalence, degraded mode (no pgss/pg_cron)
- Version schema: `vMAJOR.MINOR` (e.g. `v1.3`). Tag on main after all PRs merged.

`ash-install.sql` is the source of truth. Upgrade scripts (`ash-X.Y-to-X.Z.sql`) are generated at release time, not in feature PRs.

## Testing

Red/green TDD: write failing tests first, then fix the code to make them pass.

- **Bug fixes**: always write a test that reproduces the bug (RED), then fix (GREEN). This proves the fix works and prevents regressions.
- **New features**: write tests for the expected behavior before or alongside the implementation. Run tests locally with `sudo -u postgres psql -v ON_ERROR_STOP=1 -f sql/ash-install.sql` and DO blocks with assertions.
- **CI tests** live in `.github/workflows/test.yml`. Each test section uses PL/pgSQL `DO $$ ... assert ... $$` blocks. Assert exact values, not just row existence — a test that only checks "row exists" can't distinguish correct aggregation from garbage.
- **Test locally on available PG version** before pushing. CI covers PG 14–18.

## Code Review

All changes go through PRs. Before merging, run a REV review (https://gitlab.com/postgres-ai/rev/) and post the report as a PR comment. REV is designed for GitLab but works on GitHub PRs too.

Never merge without explicit approval from the project owner.

## Stack

- Pure SQL + PL/pgSQL (no extensions, no `.control` file)
- Anti-extension design: `\i` to install, works on RDS/Cloud SQL/Supabase/AlloyDB/Neon
- Optional pg_cron integration for automated sampling
- Optional pg_stat_statements for query text and execution metrics
