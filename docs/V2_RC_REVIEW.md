# v2 RC preparation: review evidence, 2026-09-04

Verdict: **do not stamp or tag yet**. The [finalization plan](V2_RC_PLAN.md)
describes how to reach `v2.0-rc1`. This record separates completed review and
tests from the integration and comprehensive release gate still required.

## Scope and provenance

- Freshly fetched main: `2c3286ac2551a89ab6bf6e2c982b6988578d5640`.
- All 12 open PRs inspected through metadata, diffs, relevant source/tests,
  previous reviews and samorev. Three independent agents reviewed API/data
  correctness, docs/examples, and migration/release safety.
- samorev checkout: `24924b4900efdf1b11e43ae7c7477e0c0fc3d2d3` from
  [Tanya301/samorev](https://github.com/Tanya301/samorev).
- Each samorev invocation used `review URL --fetch --no-comment --blocking`.
  All 12 produced review reports and exited 1; none reported an unavailable
  or timed-out main reviewer. These are FAIL reports, not blanket merge
  approvals. Known false positives and tool limitations are adjudicated below.
- No PR was merged, retargeted, marked ready or commented on. No tag or GitHub
  release was created. Repository changes from this task are the plan and this
  review record. The one hosted mutation was the test workflow dispatch below.
- Historical PR-body runtime claims were read but are not counted as fresh
  tests performed in this audit.

## Completed verification

| Check | Result and interpretation |
|---|---|
| Current main extractor tests | **FAIL:** 7 tests, 1 error; `WorkflowError` at workflow line 8853. This reproduces #243 and blocks the local release-gate extraction path. |
| #244 extractor tests | **PASS:** all 13 tests on `4a948205`. A separate intermediate-indentation probe shows the parser still treats a nine-space comment as content after a ten-space literal-block line; track the supported YAML subset accurately. |
| Current main release-stamp tests | **PASS:** all 17 tests. |
| Hosted manual pre-tag mechanism | **PASS:** [run 33924051160](https://github.com/NikolayS/pg_ash/actions/runs/33924051160), `workflow_dispatch`, main SHA above, input `release_tag=v2.0-beta1`. Docs/stamp job, PG14/15/16/17/18/19beta2 with cron, PG17 without cron and `ci-required`: **9/9 succeeded**. This resolves the lack of a demonstrated successful dispatch mechanism in #247, but is not an RC payload gate. |
| Exact #240 CALL surface | **PASS:** `procedures_call_surface.sql` on isolated local PostgreSQL 18 with a tagged real workload backend, including effects and reader denials. |
| Exact #240 sample persistence suite | **PASS:** `sample_persistence.sql`, including installer reapply, rotation/rebuild, round trips, drift and negative cases. |
| UNLOGGED restart experiment | **PASS:** clean restart retained raw/rollup counts `1/1`; immediate shutdown/recovery yielded `0/1`. Matches the intended raw-loss/rollup-preservation contract. |
| Conversion lock-timeout experiment | **PASS for atomicity; documentation defect:** lock `sample_1`, attempt conversion, timeout after 5.01 seconds. Config remains `false`; all three partitions remain logged. No partially converted ring survives. |
| Cadence conservation experiment | **FAIL:** two true 1-AAS minutes become 5 and 1 after cadence changes to five seconds; total 360 backend-seconds instead of 120. Two-minute cadence produces a one-minute peak of 2 from one observed backend. |
| Complete Docker/visual release gate | **INITIAL AUDIT: NOT RUN** because Docker was unavailable. Docker is now available; CI #252 passed its seven-surface PG17 gate and #255 passed all-origin promotion rehearsals on PG18. The complete integrated gate and final visual/standby acceptance remain pending. |

The isolated PostgreSQL instance was stopped after the experiments. Relevant
SQL and transcripts, the raw samorev reports, PR metadata/diffs and checksums
are retained in the local evidence bundle described at the end.

## Confirmed release and integration findings

**P1 — historical AAS remains wrong (#137).** At #240 head,
`devel/sql/ash-install.sql:4459` reads the current interval;
`ash.start()` changes it without preserving past cadence. The live test
demonstrates the wrong values above. #201's calendar-divisor restriction
does not address this. Enforce the plan's persistence or containment contract,
including `stop(); start()` after a nondefault interval and all retained
rollup history.

**P1 — public migration is incompatible with the prospective #240 payload.**
Current `sql/migrations/ash-1.5-to-2.0.sql:63` defines a 22-column config
normalization contract and rejects unexpected columns at line 201; #240 adds
`sample_unlogged`. Simply promoting its installer will make this public
migration fail. Update every canonical column/type/nullability/table-copy
site and test the promoted canonical and root-wrapper entrypoints, including
reapply and preservation of an existing unlogged choice.

**P1 — migration transaction ownership needs both #200 and #205.** Current
migration starts normalization after installer replay has committed
(`sql/migrations/ash-1.5-to-2.0.sql:38`). #200 fixes that transaction split
but accepts an ambient literal `on` marker; #205 binds include mode to the
assigned transaction ID and caches the decision. Carry the coherent fix and
all fault-injection cases forward before stamping.

**P1 — lifecycle teardown is still capable of partial success.** Main
`devel/sql/ash-install.sql:2495` and #240 line 2971 swallow rollup unschedule
failures; rebuild/uninstall consume that result. #204 contains the atomic
failure fix, but is stale. It also overwrites custom commands and activation,
conflicting with #235's deliberate command preservation. Resolve this as a
single lifecycle state contract, not a file conflict.

**P1 — CI evidence is not fully enforced.** The inspected active ruleset
`13093534` lacks required status checks. `ci-required` accepts skipped
dependencies. The real-demo change detector only includes `demos` and `sql`,
missing `devel/sql`. #201/#204 carry workflows over the known size limit and
have no checks. Rebase, reduce workflow size, enforce actual checks, and
require artifact-backed demo execution for candidate changes.

**P2 — same-name cross-database cron collision.** #235/#240 filter existing
sampler lookup by database, then call `cron.schedule()` when no local match
exists. pg_cron's named-job upsert is keyed by `(jobname, username)` and
updates its target database. Thus the lookup filter does not protect a
same-owner job in another database. This is source-verified, not a live
multi-database reproduction here; add that regression and an explicit
collision/ownership policy. [Upstream pg_cron implementation](https://raw.githubusercontent.com/citusdata/pg_cron/main/src/job_metadata.c).

**P2 — report/LLM documentation would produce misleading conclusions.**
#233 README lines 381–419 treats load/core ratio as capacity utilization,
CPU* as measured CPU execution, report total as every wait class, independent
class maxima as the decomposition at the total peak, and attribution
availability as pgss resolution. The implementation contradicts those claims:
main `devel/sql/ash-install.sql:6489` chooses per-class peak minutes;
line 6574 sums the five report classes; line 6598 independently chooses the
total peak; line 6674 builds attribution availability from raw-covered
extreme minutes. Missing activity-bearing rows also do not prove missing
sampler ticks. Correct the prompt and restore the original investigation
walkthrough, not just a JSON handoff.

**P2 — dependency docs conflate text, IDs and report attribution.** #232
README line 532 promises SQL text from `report()`; line 533 associates its
attribution flag with pgss. IDs also require query-ID generation and adequate
visibility. PostgreSQL defaults `compute_query_id` to `auto`; `on` explicitly
enables generation, while `auto` lets a requesting module enable it. A seeded
ID fixture does not test capture from a live backend under those settings.
[PostgreSQL statistics configuration](https://www.postgresql.org/docs/current/runtime-config-statistics.html).

**P2 — #201 status/API changes break current consumers unless composed.**
Its status metric renames affect main `demos/scenes/scenes.tsv:35` and #234's
documented diagnostic. It modifies deleted demo scripts and a recorded cast;
those changes must be ported to the current harness and regenerated. Its
cadence, status and severity changes need a behavioral PR/review, not a
docs-only label. Until promotion, distinguish its new overlay behavior from
the released installer still used by README Quick Start.

**P2 — planned goals exceed verified guarantees.** #236 must qualify
installation/visibility permissions, overhead units and estimates, the
ability to distinguish idle from missing data, and absolute upgrade
data-preservation claims. Keep goals as requirements to verify rather than
statements of already proven behavior.

**P3 — UNLOGGED conversion comment is wrong.** #240 installer lines 739 and
777 claim timed-out conversion leaves partial persistence changes. The live
experiment proves rollback preserves the original config and every partition.
Correct those comments and keep the regression. A five-second lock timeout
bounds lock acquisition, not total rewrite duration; test/document the impact
of successful long conversion on sampling and readers.

## samorev results and adjudication

The raw report's “BLOCKING ISSUES” section can include LOW findings. Priority
below follows actual impact. Existing draft status is expected, but remains
a real merge gate. Missing CI is a gate even when samorev calls it `none`
without reporting it as a failure.

| PR / reviewed head | Raw findings | Disposition of substantive findings |
|---|---:|---|
| #200 `ba96f785` | 2 | Draft + ambient-marker atomicity defect. Confirmed; require #205 and recreate RC stamp. |
| #201 `f7a7c8ef` | 3 | Accept released/dev docs mismatch. Consolidate WARNING consequence/remediation so `client_min_messages=warning` does not hide the hint. Align counter wording after settling status labels. Main conflicts/no CI are additional blockers. |
| #204 `fc810ef7` | 3 | Draft; preserving useful error DETAIL/HINT is reasonable. The claim that specifying `database` to `alter_job` necessarily requires superuser is **not supported by current upstream source**: database changes check CONNECT; username changes require superuser. Still test supported pg_cron versions and routine ACLs with a non-superuser owner. |
| #205 `38af9f2f` | 2 | Draft; residual psql variable is a cleanup nit, recomputed on each entry. Do not undo the cached ownership decision. Missing CI and its held base remain gates. |
| #232 `aee733df` | 3 | Optional-skip CI artifact; qualify live ID-generation precondition and add actual no-pgss collection. Independent review found the additional report/text errors above. |
| #233 `f40b2772` | 1 | Only optional-skip CI artifact reported. This does **not** clear the documented semantic defects. Its defender incorrectly claimed `report()` prefers raw; source proves minute-rollup-only behavior. Fixture-isolation concern remains unverified and needs checking, not dismissal using that premise. |
| #234 `77afff73` | 2 | Optional-skip artifact; accept adding exact `status()` diagnostic assertions after final naming decision. Do not copy the tool's illustrative query blindly: status columns are `metric,value`. |
| #235 `793e4688` | 2 | Optional-skip artifact + cross-database named-job collision. Collision supported by source; add live regression. |
| #236 `e67aa8f7` | 4 | Optional-skip artifact; clarify ID prerequisites and transaction/time units. Reject the hypothetical missing `docs/RELEASE_PROCESS.md` finding: the file exists. Link it properly as normal polish. |
| #239 `23f53168` | 3 | Optional-skip artifact; accept explicit recovery tests for all five CALL wrappers. Primary-positive fixture writes are outside rollback while cleanup is PG18-only: normalize fixture cleanup across cells. This is a test-isolation gap, not proof of an observed reader failure. |
| #240 `9a704803` | 3 | Optional-skip artifact; conversion can block samplers during rewrite, so add operational evidence/runbook. Logical-publication interaction is **unverified**: test against supported configurations before presenting the tool's claimed restriction as fact. |
| #244 `4a948205` | 3 | Optional-skip artifact; add intermediate-indentation characterization and narrow conformance wording. Duplicate indentation code is nonblocking maintainability feedback. The tool's exact character-loss explanation was not reproduced; the separate probe showed altered deindentation with the comment retained. |

For the eight recent PRs with 17 checks, the observed rollup was 16 successes
plus one skipped duplicate `capture + reel` invocation. This explains the
tool's `unknown` artifact; it does **not** prove the successful duplicate did
database work. Inspect step execution/artifacts separately.

The CLI source, unlike its current README/runbook, invokes Claude and honors
`--blocking` with exit 1 on FAIL. It also caps source diff content at 40,000
characters, spread across files, so full-context review was necessary. Pin
the source revision, not just a tool name, when repeating this protocol.

## Remaining verification and publication boundary

The review establishes actionable defects and a tested path through the
manual CI trigger. It does not establish a clean combined candidate. Still
required: semantic integration, cadence correction, new migration contract,
every actual tagged upgrade origin, full Docker matrix, real promotion/resume,
external scheduler/router behavior, overhead measurements, executed restored
examples and regenerated visual inspection, followed by review/CI of the new
stamp and exact candidate main SHA.

No agent reported a fresh full release pass. Human merge/publish approval
remains required by the repository; ask only after the concrete candidate and
evidence are ready. Posted review summaries should contain this adjudication,
not the raw tool's unverified or disproven claims.

## Local evidence bundle

`/tmp/pg-ash-v2-audit/evidence.tar.gz` contains the downloaded PR metadata and
diffs, 12 samorev reports and stderr logs, the cadence reproduction and local
PG18 transcripts, the hosted dispatch result, and checksums. These temporary
artifacts are for review/reproduction and are not an immutable published
release record. Preserve them with the eventual release tracking artifacts;
the essential findings and outcome are recorded in this file.
