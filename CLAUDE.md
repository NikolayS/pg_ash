# CLAUDE.md

## Engineering Standards

Follow the rules at https://gitlab.com/postgres-ai/rules/-/tree/main/rules — always pull latest before starting work.

## SQL Style

- Lowercase keywords (`select`, `from`, `where` — not `SELECT`, `FROM`, `WHERE`)
- `<>` not `!=`
- 2-space indent
- `where singleton`, `select from` pattern

## Deployment

CI via GitHub Actions:

- **test.yml**: runs on push and PRs — tests across PostgreSQL 14, 15, 16, 17, 18
- Tests: fresh install, upgrade path (1.0→1.1→1.2→1.3), schema equivalence, degraded mode (no pgss/pg_cron)
- Version schema: `vMAJOR.MINOR` (e.g. `v1.3`). Tag on main after all PRs merged.

`ash-install.sql` is the source of truth. Upgrade scripts (`ash-X.Y-to-X.Z.sql`) are generated at release time, not in feature PRs.

## Code Review

All changes go through PRs. Before merging, run a REV review (https://gitlab.com/postgres-ai/rev/) and post the report as a PR comment. REV is designed for GitLab but works on GitHub PRs too.

Never merge without explicit approval from the project owner.

## Stack

- Pure SQL + PL/pgSQL (no extensions, no `.control` file)
- Anti-extension design: `\i` to install, works on RDS/Cloud SQL/Supabase/AlloyDB/Neon
- Optional pg_cron integration for automated sampling
- Optional pg_stat_statements for query text enrichment
