# Five-case blind investigation trial — results and audit

All five initial blind investigations are complete. **Five of five valid case investigations meet the frozen solved rule, each scoring 10/10.** This is a small functional usability sample, not a general success rate or a claim that an agent can recover every root cause. Independent scoring/audit used actual `gpt-6-astra`. No case answer was published before all five finished.

## Scope and frozen inputs

The controlling protocol is issue #285 and its pre-run scoring interpretation: https://github.com/NikolayS/pg_ash/issues/285 and https://github.com/NikolayS/pg_ash/issues/285#issuecomment-6327348832. The pre-handoff method clarification explicitly requires scoring observable evidence rather than hidden workload explanations: https://github.com/NikolayS/pg_ash/issues/285#issuecomment-6327371791.

Five criteria, each 0–2: observed incident window, dominant observed mechanism, query attribution when available, separation of evidence from unproven root cause, useful non-destructive next check. Solved requires at least 8, a correct dominant observed mechanism, at least partly correct location, and no confidently invented causal/blocker identity. No investigator penalty for refusing to infer hidden transaction state, host utilization, caller intent, or historical blocker identity.

Executable: `fe3e7b4dc56f5be9977624de7c549165bc180882`. Frozen investigator docs: `95e1ecf83ce68ab9423af2568af678b87c8d9e2f`. Later docs at `69d2703` are **not evaluated by these runs**. Current hashes of all four allowed documentation files, each launch-frozen wrapper, prompt and answer key match their recorded hashes. SQL reporting `2.0-rc1` is a development version label, not proof of released-RC1 byte identity. No database queries or fixture modifications were performed for this audit.

## Related reviews and exact source lineage

- [Independent Astra documentation/demo review](https://github.com/NikolayS/pg_ash/pull/284#issuecomment-6327356233).
- [Actual Claude Fable 5.1, medium-effort review](https://github.com/NikolayS/pg_ash/pull/284#issuecomment-6327393190).
- [Astra adjudication and integrated documentation changes](https://github.com/NikolayS/pg_ash/pull/284#issuecomment-6327637141), now at [`69d27037348b8c14713aac9db9ab0f5a410a0913`](https://github.com/NikolayS/pg_ash/commit/69d27037348b8c14713aac9db9ab0f5a410a0913). These trial runs used **older frozen `95e1ecf` docs**, not the revised standalone prompt. They therefore do not validate the new prompt or prove that the docs changes caused the outcomes.

The local combined executable `fe3e7b4dc56f5be9977624de7c549165bc180882` is a cherry-picked integration candidate, not a claim that this hash is a public release. Its public component PR heads, verified against the retained exact-head integration manifest and local Git objects, are:

- [#277, `4e190a0f60248553630378f55e21e40be8a3d256`](https://github.com/NikolayS/pg_ash/commit/4e190a0f60248553630378f55e21e40be8a3d256)
- [#280, `74879d9897a01835c3541c190c7c0a40096a94e1`](https://github.com/NikolayS/pg_ash/commit/74879d9897a01835c3541c190c7c0a40096a94e1)
- [#281, `4b43e056cab48dd704dd3c8b421a5414fa57f3da`](https://github.com/NikolayS/pg_ash/commit/4b43e056cab48dd704dd3c8b421a5414fa57f3da)
- [#282, `cb4e1527a630d83d6685e6d64f11d3fe326919db`](https://github.com/NikolayS/pg_ash/commit/cb4e1527a630d83d6685e6d64f11d3fe326919db)

Installer SHA-256: `94b3d80b6864999922bd9d70794dac4073e8e10e5d11f918d2a0947adf6b6444`; byte equality to the combined executable was recorded before handoff. The evidence set carries a source manifest so a local unpublished integration hash is not the sole provenance reference.

## Results and measured effort

| Case | Window | Mechanism | Query | Evidence | Next check | Total | Classification |
|---|---:|---:|---:|---:|---:|---:|---|
| abja9n39 | 2 | 2 | 2 | 2 | 2 | 10 | Solved |
| 3epj57xn | 2 | 2 | 2 | 2 | 2 | 10 | Solved |
| s49m2cnh | 2 | 2 | 2 | 2 | 2 | 10 | Solved |
| swe7j6wu | 2 | 2 | 2 | 2 | 2 | 10 | Solved |
| n6rq9wm3 | 2 | 2 | 2 | 2 | 2 | 10 | Solved |

| Case | Wall seconds | Orchestration calls | Nested exec calls | SQL wrapper calls | SELECT/WITH attempted | SQL errors | Doc content lookups (+ listing) |
|---|---:|---:|---:|---:|---:|---:|---:|
| abja9n39 | 92.446 | 11 | 11 | 7 | 26 | 1 | 3 (+1) |
| 3epj57xn | 93.990 | 10 | 12 | 5 | 28 | 0 | 5 (+1) |
| s49m2cnh | 64.367 | 7 | 10 | 4 | 17 | 0 | 5 (+1) |
| swe7j6wu | 67.904 | 8 | 7 | 5 | 17 | 0 | 1 (+1) |
| n6rq9wm3 | 72.615 | 9 | 13 | 6 | 19 | 1 | 4 (+1) |

All actual investigator runtime models are `gpt-6.1-sol`, verified from each runtime `turn_context`, not merely requested labels. Wall time is session creation through `task_complete`; all finished early within 15 minutes. This is agent elapsed time, not a database performance benchmark. The fourth also used one nested apply_patch call. Doc lookup counts measure executed read/search commands, not lines, tokens, or unique files.

SQL counts include successful evidence-script reruns and exclude transaction control/SET/metacommands. The fifth also made one read-only `\dt` catalog lookup, included in its six wrapper invocations but not counted as a submitted SELECT/WITH statement. The first submitted 27 SELECT/WITH statements; 26 were attempted, one failed, and one subsequent SELECT was not executed after the error. Others attempted all submitted reads. Total: 27 wrapper invocations, 107 attempted SELECT/WITH statements, 105 successful and two failed. These are not 107 independent requests: the wrapper batches statements. Wrapper wall totals were respectively 2.322, 2.006, 0.948, 0.812 and 1.734 seconds; they include shell/connection overhead and are not a pg_ash latency measurement. There were zero investigator resets/reruns; routine SQL exploration and successful evidence-script repetitions are not agent retries.

All wrapper metrics were bounded by actual investigator start/end timestamps. No out-of-window entries were included: one supervisor probe in the fifth wrapper log was excluded; the first four files required zero exclusions. Separately stored smoke/permission checks are not counted.

## Score rationale and evidence reconciliation

### abja9n39 — transaction-ID lock wait

- Window 2: discovered `[21:57,21:59)` and both retained bursts, explicitly identifying the gap between 21:57:31 and 21:57:52 rather than inventing continuous contention or known recovery.
- Mechanism 2: dominant `Lock:transactionid`, 64 of 97 backend observations (65.98%). Supervisor's independent certified export has exactly 64 lock observations at 32 ticks, plus 33 ClientRead observations.
- Query 2: identified `-1016721775295826782`, actual normalized text `update records set value = value + $2 where id = $1`. The expected template's literal `+ 1` is normalized to a parameter in observed pgss text; this is not an attribution discrepancy.
- Evidence 2: the co-occurring multi-row update is only a plausible blocker, explicitly not proven; no historical blocker PID, transaction state, CPU utilization or business impact invented.
- Next check 2: simultaneous live transaction state/age, `pg_blocking_pids()` and `pg_locks`, plus external sampler timing across the gap.

One documented error: `ash.timeline(..., bucket=>'1 second')` rejected the unsupported resolution. The agent retained the error, changed to raw samples and completed. This is API discoverability friction, not a fabricated answer or product correctness defect. The minimum one-minute timeline bucket is in the reference; README/entry-path discoverability can still be improved.

### 3epj57xn — client-input wait

- Window 2: located both observed intervals 21:59:20–33 and 21:59:54–22:00:11, enclosing `[21:59,22:01)`, with the gap correctly qualified.
- Mechanism 2: all 64 observations are `Client:ClientRead`; this is precisely the observable mechanism. Hidden idle-in-transaction state is not required for full credit.
- Query 2: correctly attributed `-9127212586606946335`, `select payload from records where id = $1`, while explaining that it can be the last/associated statement rather than an actively slow SELECT.
- Evidence 2: refuses to claim network failure, executing slow SELECT, proven idle-in-transaction state, a normal empty baseline, or a quantified regression. Exact compare notice returns NULL baseline/delta.
- Next check 2: timestamped transaction state, age, application/client identity and request tracing on recurrence, plus sampler tick evidence.

Supervisor export independently confirms 64 rows at 32 ticks for the identified event/query. Refusing the hidden root cause is a strength under the public frozen protocol, not partial credit.

### s49m2cnh — active/no-reported-wait

- Window 2: correctly locates all 30 application observations in `[22:01,22:03)`, 22:01:22–22:02:14 with the intervening 21-second timestamp jump. The report distinguishes later 22:03:57 rollup self-observation from the incident.
- Mechanism 2: `CPU*` is correctly described as active execution without a reported wait, not measured CPU saturation.
- Query 2: correctly identifies `2642533687498376244`, the MD5/generate_series aggregate.
- Evidence 2: declines to assert utilization, saturation, actual parameter bounds, per-execution duration, business latency or a verified baseline; compare NULLs/notices are faithfully reported.
- Next check 2: correlate backend CPU, runnable delay, container quota and application execution latency/bounds to distinguish on-CPU work from scheduling contention.

The earlier certified export has 30 matching application rows. Investigator output additionally contains one real later collector `select ash.rollup_minute()` observation, so the broad discovery total of 31 is supported and is not a conflict with the frozen 30-row application certificate. No timeline/history was rewritten for this audit.

### swe7j6wu — explicit server-side sleep

- Window 2: identifies both observed ranges 22:07:45–59 and 22:08:20–37 and the gap, within `[22:05,22:10)` discovery / `[22:07,22:09)` drill bounds.
- Mechanism 2: all 65 observed backend-seconds are `Timeout:PgSleep`, not CPU/disk/lock saturation.
- Query 2: correctly identifies `-7209068558248614225`, `select pg_sleep($1)`, and corroborates cumulative pgss calls/timing without pretending pgss is incident-timestamped history.
- Evidence 2: separates observed sleep from purpose, caller, parameters, end-user impact and sampler continuity. No unsupported remediation.
- Next check 2: match request traces and bound sleep duration/purpose; verify successful sampler ticks across the empty range.

Certified full sample export independently has 65 rows at 33 ticks, one backend at the first tick and two thereafter. The certificate's strict controller-window matching counts total 61 (27 + 34), because some stored sample timestamps extend just outside controller episode endpoints. This boundary difference does not make the agent's 65 raw observed rows false; score the actual retained observations. Controller vs sample boundary timestamps should be published honestly, not forced into exact agreement.

### n6rq9wm3 — relation-lock wait

- Window 2: discovered `[22:11,22:13)` with both observed bursts, 22:11:51–22:12:04 and 22:12:25–42; explicitly identified the 20 missing seconds rather than asserting continuous blocking or a healthy gap.
- Mechanism 2: dominant `Lock:relation`, 64 of 96 backend observations (66.67%), not CPU or IO pressure.
- Query 2: correctly attributed `-9127212586606946335`, `select payload from records where id = $1`, and distinguished it from the co-observed LOCK statement.
- Evidence 2: current text `lock table records in access exclusive mode` supports a plausible blocker hypothesis but the report explicitly refuses to treat it as a proven historical blocker/lock-owner graph. Exact lock mode of the blocking edge is not established. It also correctly notes the candidate normal baseline predates installation.
- Next check 2: on recurrence correlate waiting PIDs, `pg_blocking_pids()`, transaction/client state and granted/waiting `pg_locks`, plus scheduler execution across the gap; no session termination justified from history alone.

Supervisor export independently confirms 64 relation-wait rows plus 32 ClientRead rows, all at the same 32 timestamps. The pre-handoff certificate passes both original ≥8-sample and ≥8-independent-observation thresholds with query linkage. The same unsupported one-second timeline request failed and was corrected using raw timestamps. No other SQL error or agent retry.

## Blinding and data provenance

All tool inputs, including complete report-writing commands and all fifth-run inputs, were inspected. They access only the frozen public docs, the provided SQL wrapper and own report/evidence files. No connection-wrapper source, Docker, setup/controller/answer files, other case directory, GitHub or external context was accessed. No delegation. Queries were read-only. Prompt wording is identical after substituting opaque case/output paths; no incident interval or mechanism was supplied.

The fourth queried ordinary pg_stat_statements top-15 rows; these exposed generic installer and supervisor sampling/reader SQL through the permitted connection. No scenario-specific controller text or answer key appeared. Treat this as a disclosed shared-observability limitation, not a forbidden filesystem read. The first three relied on pg_ash's current query-text enrichment; that is expected product behavior.

These are procedurally blinded shared-host sessions, not security-isolated agents. Frozen before-launch hashes matching current files provide integrity checks, not a cryptographic historical proof that files could never have been transiently changed. Staggered collection/launch ages, current statistics enrichment, simple isolated workloads and constrained containers limit comparability and external validity.

## Invalid/pre-handoff attempts — not investigator failures

Retain separately and include in final methods/attempt ledger:

1. Installer adapter attempt failed before fixture generation because psycopg was given psql metacommands; corrected using exact installer with psql. No investigator attempt occurred.
2. Initial sleep workload attempt used an insufficient five-second worker join deadline and was incomplete. Original container/evidence retained through the trial; the scored sleep run used a fresh replacement container with a completed, certified fixture. No investigator saw the incomplete fixture.
3. CPU evidence initially failed an independent-observer normalization adapter: active + NULL wait type/event was not translated to CPU*. Existing real data was recertified after correcting only the mapping, not regenerating the workload or relaxing thresholds. This is a false-negative validator attempt, not an additional workload or agent attempt.
4. Original relation fixture `ssfjwgh2` retained the intended wait in both episodes but no historical query ID, violating the stricter private certification requirement. It was withheld, not scored as an agent failure. A separately owned replacement, `n6rq9wm3`, passed the unchanged certification and was the sole relation-case investigator run. The original invalid relation container/evidence was retained through the trial, with evidence preserved before final cleanup. The [pre-handoff replacement disclosure](https://github.com/NikolayS/pg_ash/issues/285#issuecomment-6327723731) records this decision. Do not count this as an investigator reset or quietly omit it.
5. Controller restarts/quiet collection gaps and ephemeral credential rotation with utility tracking disabled are method deviations already retained separately. Do not claim these were investigator failures or alter historical evidence to hide them.

**Attempt denominator:** five completed valid investigator runs, zero incomplete/contaminated investigator runs and zero investigator retries. Workload generation additionally included two rejected fixtures (incomplete original sleep and original relation without query linkage); neither received an investigator. The installer adapter failure and CPU validator false negative are separate setup/validation attempts, not extra investigated cases.

The public rubric allows unavailable attribution, while the private fixture certificate required it. This tension should be disclosed; a replacement is an explicit fixture-design decision, not proof that pg_ash always offers historical relation-wait query linkage.

## What this says about easy / convenient / fast

Supported: five fresh Sol contexts discovered the retained activity without supplied bounds, produced evidence-qualified mechanisms and next checks in roughly 64–94 seconds, and recovered from both SQL errors without coaching. `periods()` → timeline/chart → ranked waits → query/raw samples worked. Current query text and reader source/coverage metadata were useful. The explicit compare NULL/notice behavior prevented two runs from presenting an empty baseline as zero normal load.

**No case established a credible normal baseline.** All five correctly treated empty history as an evidence limitation; the trial therefore validates bounded mechanism/location/attribution investigations, not quantified before/after regressions, verified recovery, or production-impact causation. All five also encountered idle-versus-missing-tick ambiguity.

Not established: human ease-of-use, average production investigation time, repeated success probability, arbitrary incident root-cause recovery, throughput/overhead, or the effect of the later documentation revision. There is no control group, repeated trial or user rating.

Evidence-backed friction/improvement candidates:

- Make the one-minute aggregate floor and `ash.samples()` second-level fallback prominent at the first timeline entry point; two independent failed queries demonstrate discoverability cost.
- Make last-stored activity versus last-successful sampler tick distinction easy to find. All five spent report space on idle-versus-missed-tick uncertainty; current health counters cannot resolve it. A heartbeat is a possible product enhancement, not implemented or validated by this trial.
- Explain the zero scalar AAS / NULL timeline / NULL compare-baseline distinctions together, with an actual empty-baseline example. Agents handled this correctly but it is repeated cognitive overhead.
- Present current SQL text and absent historical state/blocker linkage as concise output caveats. ClientRead is not proof of a slow executing SELECT or network fault; CPU* is not CPU utilization.
- A shorter incident-report template could put event, exact observed intervals, query, key limitation and next check first. These long but correct reports are not proof of a concise human experience.

## Evidence bundle and operational closeout

`five-case-summary.json` is the machine-readable measurement/score summary. Each case subdirectory contains the exact neutral user-level investigator prompt, investigator report, available SQL/evidence files, allowlisted tool inputs/outputs, time-filtered SQL-wrapper outputs, SQL inventory, audit summary, frozen answer/certification and decoded sample export. Internal/system/developer prompts and private reasoning are deliberately excluded. One generic documentation password example is conservatively redacted; no actual credential was found in the audited tool I/O. Original case artifacts remain unchanged.

The full safe evidence set is staged for the parent's evidence PR; this issue comment is the complete assessment, not an assertion that a repository artifact has already been published. Parent integration owns reproducible harness packaging, raw independent observation/invalid-attempt archives and owned-resource cleanup. This evaluation made no database mutations. Parent cleanup recorded removal of all seven exact-owned containers (five valid plus two rejected fixtures) by 2026-10-21 22:18:23 UTC; the cleanup ledger is included. Raw JSON/SQL outputs and server logs remain retained locally. An optional full pg_dump attempt failed authentication and produced no database dump; no full-dump preservation is claimed.
