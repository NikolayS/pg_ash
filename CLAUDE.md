# CLAUDE.md

## Engineering Standards

Follow the rules at https://gitlab.com/postgres-ai/rules/-/tree/main/rules — always pull latest before starting work.

SQL style guide: https://gitlab.com/postgres-ai/rules/-/blob/main/rules/development__db-sql-style-guide.mdc

## Deployment

CI via GitHub Actions:

- **test.yml**: runs on push and PRs — tests across PostgreSQL 14, 15, 16, 17, 18
- Tests: fresh install, full upgrade chain up to the in-progress release (currently 1.0→…→1.4), schema equivalence between fresh install and upgrade chain, idempotent re-apply of each legacy upgrade script, degraded mode (no pgss/pg_cron)
- Version schema: `vMAJOR.MINOR` (e.g. `v1.3`). Tag on main after all PRs merged.

`ash-install.sql` is the source of truth. Upgrade scripts for the **in-progress** release (e.g. `ash-1.3-to-1.4.sql` while 1.4 is unreleased) must stay in lockstep with install.sql through the release cycle — any install.sql change that affects schema (new/modified function bodies, new columns, dropped objects) must be mirrored into the in-progress upgrade script. Schema-equivalence CI enforces this on every PR. **Finalized** legacy upgrade scripts (`ash-1.0-to-1.1.sql` through `ash-1.2-to-1.3.sql` today) are immutable: any signature-changing PR must avoid patterns that trip their idempotent re-apply, even if that means restructuring the fix (e.g. keeping an old function signature and using a new helper name instead of changing the return type).

## Code Review

All changes go through PRs. Before merging, run a REV review (https://gitlab.com/postgres-ai/rev/) and post the report as a PR comment. REV is designed for GitLab but works on GitHub PRs too.

Never merge without explicit approval from the project owner.

## Stack

- Pure SQL + PL/pgSQL (no extensions, no `.control` file)
- Anti-extension design: `\i` to install, works on RDS/Cloud SQL/Supabase/AlloyDB/Neon
- Optional pg_cron integration for automated sampling
- Optional pg_stat_statements for query text and execution metrics
