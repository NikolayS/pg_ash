# pg_ash 2.0 rc 1 owner test guide

Use the exact candidate SHA and validation evidence recorded in [stamp PR #272](https://github.com/NikolayS/pg_ash/pull/272).
[PR #262](https://github.com/NikolayS/pg_ash/pull/262) preserves preparation history, not the final payload identity.
The owner authorized RC1 publication from `release/2.0-rc1` after all gates pass.
Final v2.0 publication requires separate owner approval.

## Promoted review fixes

[PR #267](https://github.com/NikolayS/pg_ash/pull/267) contains the accepted review fixes.
The accepted development source is `06b608abc4d93991c4fdae46a3cae37b7585f733`.
Its [complete development gate](https://github.com/NikolayS/pg_ash/pull/262#issuecomment-6283155076) passed all 42 Docker cells, recovery checks, and demo reproduction.

This release stamp copies that exact installer into `sql/ash-install.sql`.
The `devel/sql/ash-install.sql` baseline remains byte-identical.
Both fresh installs and the existing cumulative migration now contain the reader fixes.
Installer SHA256: `da1abdd506d2d37efdce58dc0e9df4c07dcb5311f344acf9e0c27bf5b22a4c06`.

The version string alone does not prove source identity.
Record the exact candidate commit and installer hash with test evidence.
The stamp PR must pass the complete gate again before publication.
The owner authorized `v2.0-rc1` from `release/2.0-rc1`; this does not merge main.

## Install and upgrade rehearsal

Use a disposable Postgres instance, not merely a quiet database.
pg_ash samples activity from every database. The test targets are Postgres 14–18
and 19 RC1. Prerelease testing does not establish Postgres 19 GA support.

```bash
export PGHOST=127.0.0.1 PGPORT=5432 PGUSER=postgres
export PGDATABASE=ash_rc_fresh
createdb "$PGDATABASE"
# From the exact RC candidate checkout:
git rev-parse HEAD
cmp sql/ash-install.sql devel/sql/ash-install.sql
psql -X -v ON_ERROR_STOP=1 -f sql/ash-install.sql
psql -X -c "select * from ash.status()"
```

Confirm `ash.status()` reports exactly `2.0-rc1`; a successful command alone does not certify the expected version. Run with schema-owner privileges. Lifecycle calls require that exact owner; another administrator can `SET ROLE` to the owner if authorized.

Before testing an upgrade, back up the source database and rehearse restoring on a separate disposable instance. Preserve global roles/ownership separately where needed. Check restored scheduler database targets before enabling jobs; the test instance must not connect to production. Example for a staging database:

```bash
PGDATABASE=ash_upgrade_source pg_dump -Fc -f /tmp/ash-before-rc.dump
createdb ash_rc_upgrade
PGDATABASE=ash_rc_upgrade pg_restore --exit-on-error /tmp/ash-before-rc.dump
export PGDATABASE=ash_rc_upgrade
psql -X -c "select * from ash.status() where metric = 'version'"
# From 1.5 or an earlier 2.0 prerelease:
psql -X -v ON_ERROR_STOP=1 -f sql/migrations/ash-1.5-to-2.0.sql
psql -X -c "select * from ash.status() where metric = 'version'"
```

For an older 1.x source, apply the missing 1.x edges listed in [README](../README.md#upgrade-to-20-rc-1) before the 1.5-to-2.0 migration. The root compatibility wrapper `sql/ash-1.5-to-2.0.sql` resolves to the same cumulative migration. Verify schema/version convergence, retained history, reader grants, cadence, persistence mode and scheduler jobs before/after. Reapply the same RC migration to verify idempotence. `2.0-rc1` is the expected installed row and column-default identity.

Rollback is restoration to a separate database/instance from the validated backup, with the previous application/API version. There is no promised SQL downgrade. Do not use `ash.uninstall()` as an upgrade rollback: it removes pg_ash state/history.

## Scheduler and readers

On the writable primary, call `select * from ash.start();` as the schema owner. Omitted cadence resumes the current setting. Check all managed cron jobs and `ash.status()`, then create known activity and verify raw samples actually arrive. `sampling_enabled`, stored row counts and rollup `minutes_with_data` are not a sampler heartbeat. Explicit start reactivates owned same-database jobs, preserves custom commands, and migrates recognized commands to `CALL`; cross-database or visible foreign-owner collisions fail atomically.

Without pg_cron, preserve and follow the guidance printed by start. For a manual one-second sampler rehearsal in psql:

```sql
select * from ash.start('1 second');
call ash.run_take_sample();
\watch 1
```

Run this on the primary; schedule `call ash.run_rotate()` at the configured rotation period, `call ash.run_rollup_minute()` every minute, `call ash.run_rollup_hour()` hourly, and `call ash.run_rollup_cleanup()` at the documented retention-maintenance cadence. Stop the test loop separately after `ash.stop()`; the database cannot remove an external scheduler. Verify disabled ticks collect no activity and resuming restores collection.

```sql
select * from ash.aas(now() - interval '10 minutes', now());
select * from ash.top('wait_event', now() - interval '10 minutes', now());
select * from ash.timeline(now() - interval '10 minutes', now(), '1 minute');
```

Capture stdout and diagnostic notices together. A `pg_ash partial source:` notice identifies newer completed raw observations omitted by a selected minute rollup. Wait for catch-up or narrow the window; values/source labels remain partial, and absence of the notice is not proof of completeness (#122 remains open).

## Cadence and primary/standby behavior

New intervals accept 1–60 whole seconds. Changing cadence with retained raw or either rollup history must fail without deleting history; `start()` resumes unchanged cadence. Old unsupported configurations survive upgrade but sampling skips and aggregate readers raise until an operator explicitly archives/resets/reconfigures. Commit successful changes promptly because their locks block history writers until transaction end. Coarse intervals can alias minute boundaries; even 59-second sampling can place two tick weights in one minute. These extrema remain estimates and #137 is not fully solved.

Use a real physical standby to verify reads and `ash.status()` recovery state. Sampling/rotation/rollup procedures on a standby emit an explanatory notice and do no writes; lifecycle/admin changes fail. `CALL` routing helps avoid statement-kind read routing but does not override an explicitly read-only connection.

The raw ring defaults to logged. Test `select ash.set_sample_persistence('unlogged');` only on a disposable primary: conversion rewrites affected partitions under ACCESS EXCLUSIVE locks. Raw history is absent on a physical standby and lost on crash/immediate shutdown; rollups remain logged. Verify real standby promotion and crash restart, then fresh primary sampling. Returning to `logged` does not restore lost data. No production-overhead or enable-by-default recommendation follows from these functional tests.

## LLM walkthrough and visual checks

```bash
export PGOPTIONS='-c compute_query_id=on'
python3 devel/tests/llm_example_live.py --output /tmp/llm-rc-output.txt
```

Run on the quiet disposable instance with pg_ash installed. The helper produces real lock waits, samples seven real ticks, waits for a complete minute, and runs `examples/llm-investigation.sql`. It preserves notices. Repeat without pg_stat_statements (query IDs and waits remain, SQL text is NULL), then with preloaded/created pg_stat_statements for best-effort current query text. Read the output using `examples/llm-prompt.md`; do not infer CPU utilization from AAS/vCPU, heartbeat from stored data, or blockers from waiting-query IDs. Report base totals cover five classes and averages cover observed rollup minutes; one-decimal event/query contributions may differ from two-decimal class values.

Rebuild the demos using the demo README prerequisites. For the tested macOS-to-Docker profile, run `ASH_BACKEND=docker ASH_READ_SPAN_CALM=50000 ASH_READ_SPAN_TAIL=50000 make -C demos all`. Defaults may produce an empty baseline when short reads are mostly idle at sampling instants over TCP; inspect the new sampler-state/counter hint before tuning real workload spans. The demo seeder deliberately restamps real samples into virtual minutes; its 5-second weight comes from 12 ticks per virtual minute, while compressed pauses are 0.04 real seconds plus execution. Elapsed compression is variable; it is not evidence of an observed 28-minute incident or production overhead. The LLM helper above does not restamp observations. Review chart/top-event/top-query/periods/status/report PNGs and representative reel frames for readable labels, clipping, terminal glyphs and honest semantics. Do not hand-edit output or recordings.

## Historical development evidence (superseded)

The historical development source `6dee7b8be4af99c80bfa398753360bd0e00e1515` passed all 42 Docker cells across PostgreSQL 14–18 and 19 beta 3, the real PG18 physical recovery suite, all eight screenshots and three reel frames, both real LLM variants, partial-warning export with the expected expired-attribution error, and the documented tuned demo profile. Its archive contains 682 files and verified live container/server/extension provenance for all 42 cells; SHA256 is `ddf72038d0ca80385010d34469fe3d0b0214deddfb628803d3373c2bec7a3641`.

## Current release gates

The historical pass above does not certify the latest review fixes or the final stamp.
[PR #262](https://github.com/NikolayS/pg_ash/pull/262) records historical results and the current release evidence.
[Stamp PR #272](https://github.com/NikolayS/pg_ash/pull/272) must record its exact SHA, complete repeated gate, hosted checks, and pre-tag dispatch.
Use `release_tag=v2.0-rc1` for that dispatch; it creates no tag.
The current authorization publishes RC1 from the release branch, not from main.

Preserve each model review report with its source SHA. Track current review status
in the pending-scope ledger linked from the RC PR. A successful reviewer
connection or health probe does not prove a passing review or complete source
coverage. Do not merge or tag while the review gate remains unresolved.
Resolve each concrete finding in the ledger. Repeat full exact-commit
verification before certification.

The owner packet includes the checksummed archive, manifests, real output artifacts, original default-demo failure diagnosis, commands/controllers and the review ledger. Bulk logs remain outside source history. This functional evidence does not establish production overhead, full heartbeat coverage, exact coarse-cadence extrema, or complete raw/rollup source composition.
