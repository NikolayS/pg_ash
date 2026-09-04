# pg_ash 2.0 release-candidate preparation plan

Audit date: 2026-09-04. Target: **`v2.0-rc1`**, with payload version
**`2.0-rc1`**. Status: **not ready to stamp or tag**.

Owner authorization: RC tags are permitted after verification. **Never create
the final `v2.0` tag or publish a final 2.0 release without the owner's explicit
approval.** The owner intends to test the RC fully first.

This plan covers all 12 open PRs, their integration, API decisions, automated
and manual verification, documentation, and the RC publication gate. It is
based on freshly fetched `origin/main` at
`2c3286ac2551a89ab6bf6e2c982b6988578d5640`, not the older local checkout.
Historical PR descriptions and passing checks are evidence for their recorded
commits, not certification of a future combined candidate.

See [the audit evidence and adjudication](V2_RC_REVIEW.md) for work completed
during this review, including all 12 samorev runs and the successful hosted
pre-tag mechanism rehearsal.

The central issue is correctness and integration, not the number of green
checks. Cadence changes still corrupt historical AAS; the held release stamp
has an installer-atomicity defect fixed only in its stacked follow-up; several
PRs have no CI; and independently reasonable lifecycle changes conflict.
Do not publish an RC with those unresolved.

## 1. Queue disposition and landing order

Links below identify the reviewed PRs; the short SHA pins the inspected head.
“Green” means the observed test matrix and aggregator passed, not that every
reported demo check executed its database steps.

| PR / head | Observed state | Disposition and required work |
|---|---|---|
| [#244](https://github.com/NikolayS/pg_ash/pull/244) `4a948205` | Green; GitHub reports blocked | Land first after review. Fixes the release-gate extractor on current main; 13 parser tests pass versus 1 error among 7 on main. Add the intermediate-indentation comment case found by samorev; avoid claiming full YAML conformance. Check the actual merge-block reason rather than assuming green CI permits merge. |
| [#235](https://github.com/NikolayS/pg_ash/pull/235) `793e4688` | Green | First feature-stack PR. Keep additive `run_*` procedures, explicit admin ACLs, command migration, and custom-command preservation. Recheck real pg_cron execution and routed external calls; `CALL` is not a universal router guarantee. Resolve its scheduler semantics jointly with #204. |
| [#239](https://github.com/NikolayS/pg_ash/pull/239) `23f53168` | Green; includes #235 | Land after #235. Review the incremental delta separately. Require both discriminating seam tests and physical standby/promotion tests, plus reader provenance and primary-positive controls. Do not equate a guard-generated no-op with local sampling. |
| [#240](https://github.com/NikolayS/pg_ash/pull/240) `9a704803` | Green; includes #235 and #239 | Land after #239 only after logged/unlogged, failure rollback, real standby, restart/crash/promotion and upgrade checks. Keep logged default. Update the cumulative migration for `sample_unlogged` at stamp time; current overlay tests are not proof of the promoted public-wrapper path. |
| [#204](https://github.com/NikolayS/pg_ash/pull/204) `fc810ef7` | Draft, conflicts, zero checks | Rebuild its lifecycle delta on the composed feature stack. Preserve exact-ID teardown, failure propagation and owner binding, while retaining #235's custom-command policy and #239's recovery guards. Document the owner-only behavior change and `SET ROLE` remedy. Move large inline regression bodies out of YAML. |
| [#201](https://github.com/NikolayS/pg_ash/pull/201) `f7a7c8ef` | Conflicts, zero checks; older approval exists | Split behavioral/API changes from prose. This is not a docs-only PR: it restricts cadences, renames status metrics and changes message severity. Rebase on the composed installer, retain deleted legacy demo files as deleted, port relevant changes to the current harness and regenerate assets. Re-review the new head. |
| [#232](https://github.com/NikolayS/pg_ash/pull/232) `aee733df` | Green | Land corrected dependency documentation. State query-ID generation and visibility prerequisites in the matrix itself. Remove the claim that `report()` returns SQL text; separate raw attribution availability from pgss text resolution. Add live collection without pgss, not just a pre-seeded ID test. |
| [#234](https://github.com/NikolayS/pg_ash/pull/234) `77afff73` | Green | Retain explanation of enablement, scheduling, restart persistence and external-ticker trap. Align names with the API decision and #201 status labels. Assert the documented `status()` diagnostic as well as config counters; demonstrate actual re-enabled collection with a concurrent workload. |
| [#233](https://github.com/NikolayS/pg_ash/pull/233) `f40b2772` | Green | Rewrite misleading report/AAS guidance, then restore the original investigation walkthrough using v2 APIs. Keep JSON-shape guards but permit additive keys as promised by the contract. Verify populated, unavailable and partial-attribution examples. |
| [#236](https://github.com/NikolayS/pg_ash/pull/236) `e67aa8f7` | Green | Land after reconciling goals with actual capabilities. Qualify permissions, AAS/vCPU interpretation and overhead estimates; specify one transaction per sampling tick plus maintenance. Update “known gaps” for the final integrated scope. |
| [#205](https://github.com/NikolayS/pg_ash/pull/205) `38af9f2f` | Draft, zero checks; base is `release/2.0` | Salvage with the atomicity part of #200. Keep transaction-ID-bound include ownership and the cached decision through the installer footer. Port the complete fresh/upgrade fault-injection suite. Do not merge this isolated delta into main without its migration-transaction prerequisite. |
| [#200](https://github.com/NikolayS/pg_ash/pull/200) `ba96f785` | Draft, conflicts; old July CI | Replace the stale final-`2.0` stamp after the development gate passes. Salvage migration atomicity and useful release notes first, together with #205. Recreate the mechanical stamp as `2.0-rc1`; never overwrite the composed development installer with this old full-file payload. |

The verified ancestry is **#235 → #239 → #240**, although all three PRs
currently target main. After each merge, rebase the remaining stack, keeping
main as the PR base (feature-parent bases are outside the workflow triggers),
so reviewers see only its remaining delta. A squash merge needs particular
care: remove already-landed ancestor commits instead of replaying them as new
work. Every rewritten head needs fresh checks and review.

Recommended sequence:

1. Repair CI/extractor reliability (#244 plus the CI work below).
2. Settle the API contracts below; integrate #235 → #239 → #240.
3. Port #204 onto that stack; extract and integrate #201's accepted behavioral
   changes; implement the cadence fix/containment from #137.
4. Port #200 + #205 atomicity as a coherent development change. Stage the complete current-line migration refresh, including the new
   config normalizer, in a development/test path. Test a temporary promoted
   layout whose public wrapper and relative includes resolve to the composed
   development installer. This is required before the development gate. Do
   not stamp yet.
5. Finish #201 prose, #232/#234/#233/#236, examples and regenerated demos.
6. Run the complete development-candidate gate; fix findings and repeat it.
7. Prepare and review a new RC stamp, then repeat verification on its final
   promoted payload and exact eventual main commit before tagging.

For each step record the implementing PR, current SHA, owner, review report,
manual transcript and CI URLs. Do not close superseded PRs until their unique
fixes and tests are accounted for in the replacement.

## 2. Decisions to freeze before the RC

### Historical AAS and cadence — release blocker #137

The latest #240 installer still weights history using today's
`ash.config.sample_interval`. Independent PostgreSQL 18 reproduction during
this audit used 60 observations collected at one second, followed by 12 at
five seconds. After `ash.start('5 seconds')`, the two true 1-AAS minutes become
5 and 1; the aggregate becomes 3 AAS / 360 backend-seconds instead of 1 AAS /
120 backend-seconds. A two-minute cadence also produces a one-minute peak of
2 from one observed backend.

**Preferred complete fix:** persist sampling weights/epoch information with
observations and propagate weighted backend time through minute/hour rollups.
Define what happens to older rows lacking that information; never infer a
past cadence from current configuration and claim exactness.

**Recommended bounded RC alternative:** enforce a constant supported cadence
while any retained raw or rollup history exists, and reject cadences that
cannot support the advertised one-minute statistics. Cover every supported
configuration path, including direct config updates, aliases, installer
reapply and external-scheduler setup. Rebuilding only the raw ring is not
sufficient to change cadence while old rollups survive. Do not silently purge
history to make a configuration update work. An explicit reset or segmented
history operation needs its own documented data-loss/interpretation contract.

Choose and implement one path before stamping. #201's cron-divisor correction
does not solve historical reweighting. A README caveat alone is insufficient.
Acceptance: fixed-history results do not change when present configuration
changes, mixed-cadence results are correct or the transition is refused, and
coarse observations never masquerade as exact one-minute peaks.

### Lifecycle and scheduling

Recommendation: keep `start`/`stop` callable for compatibility, add clear
sampling enable/disable operations if the responsibilities are split, and use
`schedule`/`unschedule` only for explicit scheduler management. Specify whether
unscheduling leaves sampling enabled for an external ticker. Avoid replacing
one ambiguous name with another that still changes hidden state. Do not adopt
#223's proposed removal of compatibility wrappers in 2.1 while promising a
stable 2.x interface; use a deprecation policy consistent with the public
compatibility commitment.

Before implementation, write a state table for each entry point covering:
`sampling_enabled`, interval, the five managed jobs, existing custom commands,
inactive jobs, job database/owner, missing pg_cron, recovery, errors and
rollback. The #204/#235 conflict is semantic: #204 force-repairs commands and
activation while #235 intentionally preserves custom commands. Recommended
contract: migrate recognized legacy commands, preserve custom commands, and
make any explicit repair/reset operation visible and separately tested.

Add a same-owner, same-job-name, different-database collision test. The current
lookup filter does not prevent `cron.schedule()` from updating that other
job: pg_cron keys named jobs by name and username and updates the database on
conflict. Reject the collision or explicitly establish managed-job ownership;
do not silently claim isolation. Verify behavior against each supported
pg_cron version.

Retain the additive `run_*` procedure names unless the complete call surface
is deliberately redesigned before RC. Document their NOTICE/result behavior,
admin privileges and lack of internal transaction control. Do not imply #235
implements the chunked-commit design in #228. Verify routing with a named
supported router configuration; syntax alone cannot promise primary routing.

### Reader, report and diagnostic contracts

Freeze routine signatures, argument names/defaults, result columns, allowed
dimensions, units, NULL/error behavior, SQLSTATEs and privilege requirements.
Verify positional and named calls. Keep raw evidence, aggregate data and
rendering functions distinct in documentation.

Preserve `report()`'s chosen minute-rollup-only contract: no raw/hourly fallback
that fabricates class-level one-minute detail. SQL NULL can mean no usable
minute-rollup data, even while another reader has data. Explain the NOTICE and
the next action. Its JSON contract permits additive keys; tests should require
the documented subset and types rather than forbid every future addition.

Choose #201's status-label policy explicitly. Recommendation: retain existing
labels as documented compatibility aliases while adding clearer labels, unless
the owner deliberately accepts their removal as a v2 breaking change. Update
README, tests, demos, fixtures, catalog comments and release notes together.
Configuration enabled, latest activity-bearing observation and a confirmed
sampler heartbeat are different facts; current storage cannot distinguish
every quiet period from an outage.

Document #204's schema-owner restriction, including the effect on a different
superuser and a permitted `SET ROLE` workaround. Validate the full reader
grant bundle and deny all new maintenance procedures/admin helpers to readers
and PUBLIC. Test useful reading as the positive control for each denied-write
test. Resolve #171/#195/#196 by reproducing the final documented grant/reapply
policy, not by carrying forward hardcoded bundle sizes from old reviews.

## 3. Make CI evidence dependable

Current main has a useful seven-cell PostgreSQL matrix (14–18 and 19beta2 with
cron, plus 17 without cron), release-stamp tests, behavior/upgrade/security
tests, an independent size workflow and a `ci-required` job. Preserve this
coverage while improving its enforcement.

Required work:

- Apply actual required status checks to the active main ruleset. The live
  `main-protected` ruleset `13093534` currently contains no
  `required_status_checks` rule. Require `ci-required`, the independent
  workflow-size check, renderer, real demo verification, and CodeQL with the
  correct app identities. Inspect any other rulesets/classic protection too;
  a JSON file in the repository is not proof of active enforcement.
- Make `ci-required` fail unless both mandatory dependencies succeeded.
  Current `success|skipped` acceptance permits a skipped test job to pass.
  Keep any genuinely optional job separate with a documented reason. Add
  failure/cancellation/skipped/missing-job probes for the aggregator and
  confirm the expected matrix cells materialized on the candidate SHA.
- Extend the independent size workflow to release branches as well as main.
  Retain its 460,000-byte guardrail. Reviewed `test.yml` sizes: #201 513,684;
  #204 530,255; #200/#205 509,529; #240 437,634. The first two exceed the
  repository's known 512,000-byte failure boundary; #200/#205 leave almost
  no room. Combining the pending inline bodies also threatens the guardrail.
  Move behavioral bodies into `devel/tests/` and keep YAML orchestration short.
- Include **`devel/sql`**, discovery helpers, relevant test/harness files and
  rendered documentation inputs in the demo gate's change detection. Today
  `git diff ... -- demos sql` misses development SQL; a successful
  `capture + reel` job may have skipped every database step. Force full
  capture for RC workflow dispatches and assert artifact production.
- Give database jobs explicit timeouts, bounded readiness/workload waits,
  reliable cleanup and failure artifacts. Preserve exact-value assertions and
  independent test databases. Avoid sleeps as evidence that a cron job fired.
- Run the extractor tests and stamp/discovery tests before any Docker gate.
  Assert extracted step names/counts, shell parseability and all seven local
  surfaces. Keep new report/procedure/standby/persistence tests in both hosted
  CI and the local gate; audit range-based selection after moving steps.
- Keep PG14–18 required; test the currently supported PG19 prerelease and
  record the exact image/package versions. Recheck the supported-version
  policy and available PG19 image before changing `19beta2`; do not silently
  drop a cell because its package setup is inconvenient.
- Prove manual `workflow_dispatch` end to end before stamp day. A rehearsal
  was started during this audit on current main using its existing
  `v2.0-beta1` stamp and passed all nine jobs in
  [run 33924051160](https://github.com/NikolayS/pg_ash/actions/runs/33924051160).
  It is a mechanism check, not approval of beta payload identity or an RC test.
  Diagnose queued/zero-job runs promptly; workflow size is a known cause,
  not the only possible cause of scheduling delay.

## 4. Review protocol using Tanya301/samorev

Pin samorev revision **`24924b4900efdf1b11e43ae7c7477e0c0fc3d2d3`** for this
audit. Its README/runbook are stale: source at this revision invokes Claude
analysis and an adversarial finding check; `--blocking` returns nonzero on
FAIL. The executed command is:

```sh
bun run samorev review https://github.com/NikolayS/pg_ash/pull/PR_NUMBER \
  --fetch --no-comment --blocking
```

Run on every finalized PR head and again on the composed candidate/stamp PR.
Keep the full local report, exit code, tool revision, PR head/base and CI
snapshot. Reports must distinguish tool execution failure, actual findings,
draft status, missing CI and a known optional skipped check.

Two limitations require an additional full-context review:

1. This revision can label green PRs `ci_status=unknown` because of the skipped
   duplicate demo invocation. Adjudicate against the individual mandatory
   jobs; never globally convert `unknown`, `skipped` or zero checks to PASS.
2. Its main model prompt caps source diff content at 40,000 characters,
   shared per file. Large SQL/workflow diffs are truncated. Review omitted
   bodies and tests explicitly; review #239/#240 incremental deltas and the
   composed installer. A no-code-findings report is not complete certification.

Use independent agents for security/privileges, bugs/data semantics, test
quality, migration/lifecycle behavior and documentation. Give them the actual
relevant source and tests, not only the proposed interpretation. Require a
reproduction or source-grounded argument for each blocking finding, and record
why contested findings are fixed, rejected or deferred. A prior PASS becomes
stale after a relevant change or conflict resolution.

The repository requires review reports on PRs and explicit owner merge
approval. This audit uses `--no-comment`; it has not posted reports. Prepare
the adjudicated reports before the publication step. Do not post raw tool
claims as independently verified conclusions.

## 5. Automated and manual release gate

Run against disposable real PostgreSQL instances. Hosted CI is necessary but
does not replace the repository's Docker release gate or human inspection.
The local Docker daemon was unavailable during the audit; isolated local PG18
reproductions are supplementary evidence, not the complete release pass.

| Surface | Required automated evidence | Manual acceptance |
|---|---|---|
| Fresh install / regression | PG14–18 + supported PG19 prerelease; all current regression tests; install/reapply success; deterministic late failure rolls back | Follow pinned-tag Quick Start from an empty database with realistic owner privileges; generate live activity and inspect status, periods, chart, top and report |
| Historical upgrades | Real tagged v1.0–v1.5; full discovered chain; v2.0-alpha1…alpha5 and beta1 to RC; canonical migration and root wrapper; wrapper reapply; fresh/upgrade schema equivalence | Upgrade a populated v1.5 and beta installation with nondefault config, complete/narrow grants, pg_monitor opt-out, custom/inactive cron jobs and retained raw/rollup data |
| Migration failure | Custom triggers, dependent view/DROP RESTRICT, externally preset transaction marker, same-session rollback, late installer failure, empty database and real v1.5 | Check nonzero psql exit and unchanged identity, config contents/OID, schema, blockers and jobs from a new connection; retry successfully after removing the deliberate blocker |
| AAS / reader correctness | Conservation of backend-seconds and percentages across raw/1m/1h, compacted residuals, stale watermark fallback, partial hours, retention boundaries, peaks/percentiles, fixed/mixed cadence, NULL/empty/inverted windows | Explain one known lock storm from raw evidence through aggregate/chart/report; reconcile values and identify the actual source/grain and coverage limits |
| Scheduling / lifecycle | Exact five-job state, no duplicate jobs, legacy-command migration, custom-command preservation, inactive/wrong-database/foreign-owner cases, teardown failure rollback, bounded lock/cancellation tests | Live cron tick and wall-clock external ticker; stop/re-enable; clean restart; restricted roles; deliberately blocked unschedule must not report success or partially uninstall |
| Dependencies / grants | Four cron×pgss modes; actual ID collection with query-ID generation enabled; unavailable ID/text cases; hostile search_path/schema; positive reader access and negative admin/procedure calls | Follow no-extension setup with realistic privileges, observe IDs without pgss, demonstrate when text is unavailable, and diagnose a disabled external ticker |
| Replica / persistence | Real physical standby as well as seam tests; logged/unlogged primary and standby; raw-only vs rollup readers; clean restart, crash/immediate shutdown and promotion; conversion failure rollback; rebuild and installer reapply | Demonstrate that standby output describes primary history; unlogged raw access reports unavailable with remedy; promotion resumes correct local writes; logged rollups survive while raw loss is disclosed |
| Docs / demos / examples | Renderer fixtures, live capture, shape/rot checks, real report samples and copied SQL, no removed v1 readers in active examples | Inspect README at desktop and narrow widths; view every image and the full animation; check labels, units, version and readability; run the LLM walkthrough verbatim |
| Overhead / resilience | Repeatable sampling-off/on measurements at idle, normal and saturated load; tick latency, skipped/failed ticks, CPU/TPS/latency impact, raw/rollup bytes, WAL and replication impact | Try an already-saturated small instance and a representative application-vendor setup; document measured limits instead of “safe for everyone” claims |

For overhead (#226), establish the acceptable budget before examining results,
record duration/concurrency/cadence/PG version/hardware and separate measured
results from estimates. Do not require an arbitrary percentage to manufacture
a PASS. No published “enable everywhere by default” recommendation until the
evidence supports it.

Assign independent agents to fresh/regression, upgrade/atomicity, new features,
existing/security/degraded behavior, and demos/docs. With four agent slots,
schedule these in waves while preserving independent ownership. Save logs,
TSV results, CI URLs, schema/ACL snapshots, workload setup and candidate hashes.
If any gate agent finds a defect, track it, fix it and rerun the **entire**
comprehensive pass; targeted green reruns alone do not meet repository policy.

## 6. README, API reference and restored LLM examples

Arrange the README around the first successful investigation: what pg_ash
measures; requirements/privileges; pinned install and upgrade; automatic or
external scheduling; periods → timeline/chart → waits → queries; JSON/LLM
analysis; API reference; retention, replicas and limitations. Keep the full
reference in linked documentation if the landing page becomes difficult to
scan. Distinguish released SQL from the development overlay until stamping.

Restore **both** use cases: the old step-by-step investigation and #233's
copyable JSON prompt. The original walkthrough was introduced by commit
[`28ecece`](https://github.com/NikolayS/pg_ash/commit/28ecece) and removed with
the beta README rewrite. Port its question/answer progression, not removed
function names or invented output. Map its API calls as follows:

| Original example | v2 replacement |
|---|---|
| `activity_summary` | `periods`, then `aas`/`summary` for the selected bounded window |
| `top_waits` | `top('wait_event', ...)` |
| `timeline_chart` | typed `timeline(...)` and optional human `chart(...)` |
| `event_queries_at` | `top('query_id', ..., wait_event => ...)` inside raw retention |
| `top_queries_with_text` | query-ID `top(...)` / `samples(...)` with text when available |

Reuse the current demo workload for reproducible output. Capture actual
timestamps, query IDs and result columns from the final candidate; show how
each result motivates the next query. Conclude with a supported hypothesis
and a verification step. Lock waiters are not automatically the blockers or
proof of a particular application-level root cause.

Correct the JSON prompt before freezing it:

- AAS/vCPU is an active-session-to-core ratio, not measured CPU utilization
  or a universal healthy/overloaded threshold. CPU*, locks, I/O and other
  waits have different implications. CPU* includes uninstrumented activity.
- `report.total` sums the five supported report classes; it is not necessarily
  every wait class represented by other readers.
- Per-class worst-minute values can come from different minutes. Do not read
  them as the decomposition at the minute of the total peak. Use a common
  timeline/window to establish simultaneity.
- Report averages/percentiles summarize stored activity-bearing minute rows;
  missing rows do not prove quiet time or missing sampler ticks.
  `minutes_with_data` is not a heartbeat or a sampling-completeness percentage.
- `top_queryids_available` indicates whether raw-covered extreme-minute
  attribution was possible. It does not test pgss availability. Query IDs,
  SQL text resolution and raw attribution coverage are separate dimensions.
- `raw_retention_start` is a planning/loss boundary, not an assurance of exact
  attribution at every timestamp. Drill-down can still be unavailable.
- Treat NULL report, partial coverage, missing vCPU, missing SQL text and
  absent/partial per-class query IDs explicitly. Ask for further evidence
  instead of forcing a health verdict.

Ship a plain JSON export recipe (no psql table framing), a provider-neutral
prompt and a small expected-response rubric. Treat embedded SQL text as data,
not instructions; remove sensitive values before sharing externally. Keep
pg_ash itself free of LLM credentials and external calls. Test the example on
a populated report, no-pgss report, old extreme outside raw retention, no
minute-rollup coverage and a mixed-wait incident. Never require exact generated
prose in CI; check the documented fields, SQL execution and semantic rubric.

Regenerate the existing demo harness outputs after final API/status changes.
Use its actual `make check`, `make all`, `make rot`, `make stills`/`make demo`
targets as documented. Do not restore deleted shell scripts or hand-edit a
recording to disguise API drift. Update screenshots/version text at the RC
stamp and rerun visual verification.

## 7. Stamp, tag and publish only after the gates

1. Finish scope triage. Verify apparently fixed issues (#122/#124/#128/#130/
   #136/#138/#146/#155/#167) against the integrated candidate and link tests
   before closing. Keep #137/#202/#203 and all unresolved review defects on
   the blocking list. Explicitly defer replica collection (#227), chunked
   commits (#228), log digests (#229), recovery counters (#246), chart styling
   (#176), and oversized-input wording (#177) unless a shipped contract
   requires them. #223 needs a compatibility decision; #226 needs measured
   evidence for overhead claims.
2. Obtain a clean comprehensive development gate on one recorded commit.
   Prepare a replacement RC stamp PR from it. Promote the composed installer,
   update all three version stamp sites and release-identity comments to
   `2.0-rc1`, refresh the current `1.5-to-2.0` migration and retain its root
   compatibility wrapper. Preserve the five finalized 1.x migrations and
   their wrappers byte-for-byte.
3. Promote the already-tested migration's complete normalization contract for all new
   columns, including `sample_unlogged`: expected columns/order, types,
   nullability, table recreation and preservation. Include #200/#205
   transaction ownership. Exercise the **promoted canonical migration and
   public wrapper**, not just released migration followed by a dev overlay.
4. Recreate `devel/sql/ash-install.sql` as the byte-identical promoted baseline,
   as the current release process now requires. Fix the later stale paragraph
   that still says to leave the directory empty. Verify discovery works for
   fresh, full, pinned and reapply paths after promotion.
5. Update README, release notes, SECURITY/support text, catalog comments,
   examples and generated assets consistently. Release notes must name the
   breaking API/owner/status/cadence changes, upgrade actions, data durability
   choices, remaining limitations and supported versions. Do not claim final
   `v2.0` is already released.
6. Review the stamp with samorev and full source context. Dispatch `test.yml`
   with `release_tag=v2.0-rc1` against the precise stamped revision and run
   the full Docker/manual/visual gate again. Require all expected jobs and
   artifacts, no blocking findings and owner acceptance of manual visuals.
7. After explicit owner approval, merge the reviewed stamp. If the merge
   changes the tested commit/tree, run CI and the complete gate on the exact
   resulting main candidate. Record the final SHA, payload hashes and results.
   Draft the GitHub prerelease body and upgrade instructions before approval
   to publish. No tag is a substitute for this pre-tag verification.
8. Create the immutable `v2.0-rc1` tag on that verified main SHA; publish a
   GitHub release marked **prerelease**, not latest stable. Verify tag-triggered
   checks, release links and a clean install/upgrade using the downloaded
   tagged payload. A post-tag fix gets `rc2`; never move `rc1` or replace its
   payload under the same identity.
9. Invite RC testing with a concrete matrix: fresh installs, real upgrades,
   no-extension operation, constrained privileges, replica topology and
   workload scale. Recommended soak: at least seven days, including daily
   maintenance and a retention/rotation boundary (time-accelerated tests still
   run beforehand). Advance to stable only after feedback is triaged, all
   blockers are fixed and the complete gate is repeated on the final stamp.

The release checklist is complete only when each item has an evidence link
and exact candidate identity. Downgrading the SQL payload is not a tested
rollback mechanism: document restore from a verified backup and forward-fix
options, particularly after schema normalization or unlogged raw-data loss.

## 8. Execution ownership and stop conditions

Use one release integrator to own the composed tree and merge order; assign
named owners for CI, API/data correctness, migrations/lifecycle, and docs/demo
acceptance. These are work assignments, not independent release authorities.
The project owner retains the required human merge and publication decision.

Stop RC publication for any wrong aggregate, partial-success install/teardown,
privilege regression, untested promoted upgrade path, missing/skipped required
test surface, unresolved semantic merge, or misleading core example. Record
lesser accepted limitations with scope and evidence rather than quietly
lowering the gate. This audit creates the plan and review evidence; it does
not certify the unfinished integrated candidate or authorize its tag.
