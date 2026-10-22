# pg_ash goals

pg_ash keeps active session history inside Postgres so operators can investigate
an incident after it ends. Application vendors can embed the same SQL interface
in their products. These goals guide v2 decisions; the
[release process](RELEASE_PROCESS.md) defines the evidence required to ship.

## Portable installation

Install through SQL in a dedicated `ash` schema without a pg_ash C extension,
preload setting, or restart. The role needs database object-creation privileges.
Cross-role query attribution also requires suitable statistics visibility;
optional pg_stat_statements and pg_cron have their own installation and
configuration requirements. Managed-service availability must be checked per
service and deployment rather than assumed.

Support external scheduling and operation without pg_stat_statements. Query
IDs come from `pg_stat_activity`; optional pg_stat_statements supplies current
SQL text. Missing text must not be confused with missing query attribution.

## Useful incident evidence

Make average and peak load, wait classes, specific waits, and query IDs
accessible through a small typed API and a documented machine report. AAS
provides concurrency evidence, not CPU utilization or a standalone health
score. CPU*, omitted report classes, differing class-peak times, retained
resolution, and partial query attribution must be explained to consumers.

Preserve the operator investigation sequence in executable examples that can
also guide an LLM. Distinguish observations from hypotheses; a waiting query
is not proof of the blocking query or the application's root cause.

## Measured operating cost

Measure sampler latency, WAL, storage growth, and missed or late sampling on
representative workloads, including already-saturated servers. Report the
hardware, Postgres version, active sessions, query diversity, database count,
cadence, and durability settings alongside each result. No portable v2
transaction, CPU, or storage budget is established yet (#226).

Logging remains the default for raw history. Optional unlogged partitions trade
raw crash recovery and replication for less WAL; logged rollups preserve only
the history already aggregated. Do not describe that tradeoff as free.

## Stable integration contracts

Settle public names, named arguments, output columns, status metrics, report
semantics, and privilege behavior before the v2 release candidate. Existing
status names remain supported; document what each measures. `ash.start()` and
`ash.stop()` express a configured collection state, including when an external
scheduler performs the ticks. Avoid unnecessary renames for integrators.

Report keys may be added within the v2 minor line; consumers must accept
unknown keys and documented optional attribution keys. Upgrade acceptance
requires preserved stored data, idempotent re-apply, and equivalent schemas
and privileges across supported paths. This is a tested release requirement,
not an unconditional promise that upgrades cannot fail or lose data.

## Scope and remaining gaps

pg_ash is a single-instance history, not a monitoring platform, alerting
system, wait-event tracer, or cross-node collector. Standby readers describe
replicated primary history. Sampling does not observe queries served only by
replicas.

| Work | State or acceptance requirement | Tracking |
|---|---|---|
| LLM investigation and optional dependencies | Executable walkthrough and honest prompt/matrix in this candidate; validate against its final SQL | #225, #230 |
| Collection lifecycle names | Retain start/stop and public status keys; document external-ticker behavior and prove resume semantics | #223 |
| Cadence and sampling evidence | Candidate safeguards must prevent history from being silently reweighted; idle activity and sampler outages still need independent scheduler evidence | #137 |
| Standby and procedure behavior | Candidate adds recovery guards and CALL wrappers; verify primary routing and promotion, without claiming replica-local sampling | #221, #222, #227, #228 |
| Raw durability | Candidate adds logged/unlogged choice; verify crash, clean restart, promotion, rebuild, and re-apply | #224 |
| Operating cost | Publish reproducible measurements, especially under saturation | #226 |
| Offline incident evidence | History inside an unavailable database is unavailable too; external export remains separate work | #229 |

Candidate features are staged under `devel/sql/`. The released `sql/` payload
remains frozen until a reviewed release-stamp PR promotes a tested candidate.
