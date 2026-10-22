# CLAUDE.md

## Engineering Standards

Follow the rules at https://gitlab.com/postgres-ai/rules/-/tree/main/rules — always pull latest before starting work.

SQL style guide: https://gitlab.com/postgres-ai/rules/-/blob/main/rules/development__db-sql-style-guide.mdc

### Language

At least 80% of sentences must follow ASD-STE100 Simplified Technical English.
Apply this to changed docs, comments, help text, reports, and PR descriptions.
Use at most 20 words per procedural sentence and 25 per descriptive sentence.
Use active voice, simple words, consistent terms, and one instruction per sentence.
Keep technical names and meaning exact. Preserve quoted text and logs.
Review changed prose against `writing__simplified-technical-english.mdc`; avoid unrelated rewrites.

## Deployment

CI via GitHub Actions:

- **test.yml**: runs on pushes, PRs, and manual pre-tag dispatches — tests
  across Postgres 14, 15, 16, 17, 18, and 19 RC1
- Tests: fresh development install, discovered full upgrade chain up to the in-progress release, schema equivalence between fresh install and upgrade chain, idempotent re-apply of discovered re-apply-safe upgrade scripts, degraded mode (no pgss/pg_cron)
- Version schema: `vMAJOR.MINOR` (e.g. `v1.3`). Tag on main after all PRs merged.

After a release tag, keep `sql/` frozen at the latest released baseline until the next release-stamp PR. During a development cycle, SQL changes live under `devel/sql/`: the future final installer and, for a new stable release line, the future cumulative upgrade script. After a prerelease, the discovery helper validates released-chain reachability and the same release line in both installers. It then appends the development installer to full, re-apply, and pinned public-wrapper upgrade paths. After a final release, only a byte-identical copy of `sql/ash-install.sql` may serve as that overlay. Any changed installer then requires a connected development migration, including comment or line-ending changes. CI must discover version chains from files via `devel/scripts/ash_sql_chain.py`, not hardcode concrete version numbers. At release stamp time, promote the development installer into `sql/ash-install.sql`, promote or update the current-line upgrade script under `sql/migrations/`, keep a root-level compatibility wrapper, bump `ash.config.version`, and **immediately recreate `devel/sql/ash-install.sql` as a byte-identical copy of the freshly promoted `sql/ash-install.sql`** so the next cycle starts with the baseline already in place and no feature PR ever has to carry it. See `docs/RELEASE_PROCESS.md`.

## Testing

Red/green TDD: write failing tests first, then fix the code to make them pass.

- **Bug fixes**: always write a test that reproduces the bug (RED), then fix (GREEN). This proves the fix works and prevents regressions.
- **New features**: write tests for the expected behavior before or alongside the implementation. Run current development tests locally with `sudo -u postgres psql -v ON_ERROR_STOP=1 -f "$(python3 devel/scripts/ash_sql_chain.py fresh-install-path)"` and DO blocks with assertions.
- **CI tests** live in `.github/workflows/test.yml`. Each test section uses PL/pgSQL `DO $$ ... assert ... $$` blocks. Assert exact values, not just row existence — a test that only checks "row exists" can't distinguish correct aggregation from garbage.
- **Test locally on available PG version** before pushing. CI covers PG 14–19.

## Code Review

### Row-invariant SQL work

Do not execute row-invariant helper work once per scanned or expanded row.
A `stable` declaration does not cache function results. SQL inlining and CTE inlining do not guarantee single evaluation.

- Classify each helper call by its inputs and required evaluation scope before adding or changing a query.
- Compute row-invariant work once at the appropriate scope. Use a PL/pgSQL variable, an uncorrelated scalar subquery, or a justified materialized CTE.
- Do not assume these forms guarantee the intended plan. Verify the executed query, including nested SQL and public callers.
- Keep genuinely row-dependent work correlated. Preserve volatility, privileges, null handling, empty-input behavior, and returned results.
- Check all equivalent call sites when fixing repeated evaluation. Include filters, joins, projections, lateral queries, and loops.
- Require real Postgres regression tests for changes to these execution paths. First prove that the old query fails the test.
- Use `explain (analyze, format json)` to check executed plan properties. Measure helper-call counts where outer plans hide nested execution.
- Test different fixture sizes and cached plan modes. Assert bounded invariant work and exact results, not elapsed-time thresholds.
- Run these checks in CI on supported Postgres versions. Avoid full-plan snapshots and requirements for one specific scan strategy.
- Treat static lint findings as review candidates, not proof of execution frequency. Document justified exceptions with plan and call-count evidence.

A review cannot pass on syntax alone. Repeated row-invariant work requires a fix or an explicit, evidence-backed justification.

All changes go through PRs. Before merging, run a REV review (https://gitlab.com/postgres-ai/rev/) and post the report as a PR comment. REV is designed for GitLab but works on GitHub PRs too.

Never auto-merge PRs. A PR can be merged only after all of these are true:

- CI is green, and the test shape is solid. Do not accept fake coverage, catalog-only smoke tests, or checks that can pass while behavior is broken.
- REV or samorev review is done, posted to the PR, and passes.
- Manual verification was done for the changed surface: functionality changes need actual manual testing; docs, screenshots, GIFs, and pictures need manual visual review for correctness and presentation.
- The project owner gives explicit human approval to merge.

If any item is missing, leave the PR open or draft. Green CI alone is not merge approval.

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

Only after the comprehensive pass returns clean: promote development SQL into a release-stamp PR, bump version, write release notes, tag, and publish. Skipping the gate means shipping bugs against a tagged version — issues #46, #49, #51, #52, #53, #54, #61, #63 surfaced post-tag during pre-1.4 validation cycles and would have blocked the tag if this gate had been in place.

## Stack

- Pure SQL + PL/pgSQL (no extensions, no `.control` file)
- Anti-extension design: `\i` to install, works on RDS/Cloud SQL/Supabase/AlloyDB/Neon
- Optional pg_cron integration for automated sampling
- Optional pg_stat_statements for query text and execution metrics
