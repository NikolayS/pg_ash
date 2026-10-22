# Blind investigation trial evidence — 2026-10-21

Issue: [#285](https://github.com/NikolayS/pg_ash/issues/285). Published assessment: [complete five-case report](https://github.com/NikolayS/pg_ash/issues/285#issuecomment-6327804037).

Five valid initial investigations, two rejected workload fixtures, no investigator retries. This bundle preserves observed evidence, including failures. It is not a population success-rate estimate. All five cases lacked a credible normal baseline; missing samples could not distinguish idle time from missed external scheduler ticks.

## Start here

- [assessment.md](assessment.md): frozen scoring, five reports adjudicated, effort measurements, limitations and usability findings.
- [five-case-summary.json](five-case-summary.json): machine-readable outcomes, runtime models, times and SQL/tool metrics.
- [rubric-frozen.json](rubric-frozen.json): pre-handoff criteria, including the stricter private query-attribution certification.
- [source-verification.json](source-verification.json): exact executable/doc hashes and public component PR heads. Trial docs were `95e1ecf`; revised `69d2703` docs were not tested.
- [neutral-prompt-verification.json](neutral-prompt-verification.json): saved prompts match pre-launch frozen hashes; launch metadata verifies separate `fork_turns=none` Sol 6.1 contexts.
- [publication-safety.json](publication-safety.json): export allowlist and credential scan.
- [manifest-sha256.json](manifest-sha256.json): every other delivered file's SHA-256.
- [cleanup.json](cleanup.json): seven exact-owned containers removed after evidence collection. No full database dump was created; retained JSON/SQL outputs are the evidence.

## Valid investigations

Each directory contains the saved neutral prompt, report, SQL/evidence files where the investigator wrote them, tool-call inputs/outputs only, time-filtered SQL wrapper records, audit/SQL inventories, frozen answer and decoded sample export. The `independent/` subdirectory contains supervisor activity observations, sampler timings, controller phases, status and minute-rollup snapshots. Those supervisor observations were **not available to investigators** and are not used to demand unobservable causal detail in scoring.

| Directory | Observed mechanism | Separate independent evidence |
|---|---|---|
| [abja9n39](cases/abja9n39/) | Lock:transactionid | Activity/blocking state, timestamps and sampler records |
| [3epj57xn](cases/3epj57xn/) | Client:ClientRead | Independently observed idle-in-transaction state, not recoverable from the investigator's ASH output |
| [s49m2cnh](cases/s49m2cnh/) | CPU*: active/no reported wait | Active state and NULL wait type/event; no measured CPU utilization |
| [swe7j6wu](cases/swe7j6wu/) | Timeout:PgSleep | Actual sleeping sessions and phase boundaries |
| [n6rq9wm3](cases/n6rq9wm3/) | Lock:relation | Prepared/warmed query attribution and live blocking state |

`sample_time` comes from retained ASH; independent batches have their own observation times; controller boundaries are not interchangeable with either. For example, the sleep certificate counts 61 matching rows strictly within controller bounds while the full retained export contains 65. The investigator correctly reported the full observed 65. Nothing was restamped to remove this difference.

The SQL wrapper audit for the fifth case excludes one supervisor probe outside its runtime. Read-only `\dt` is counted as a wrapper/metadata call, not a submitted SELECT/WITH statement. Tool output truncation, where present, is retained as originally delivered; full wrapper SQL outputs are separately included.

## Rejected fixtures and controller corrections

- [Original incomplete sleep](invalid-attempts/sleep-incomplete/): first controller join deadline was five seconds despite a longer episode. `run.log` contains the actual exception; controller phases contain no completed spike. Samples, independent observations and sampler ticks preserve partial activity. No successful or failed certificate is invented for an attempt that raised before certification.
- [Original relation case without query ID](invalid-attempts/relation-no-query-id/): both observed Lock:relation episodes have enough real samples/independent observations, but sampled query IDs are NULL. Its frozen certificate is false. Independent text cannot supply missing historical ASH linkage.
- [CPU validator correction](method/cpu-validator-correction.json) and [original false-negative result](method/cpu-original-validator-result.json): mapping active + NULL/NULL to CPU* corrected the observer adapter; data and numeric thresholds were unchanged.
- [Installer adapter attempts](method/setup-attempts.json): initial psycopg/psql-metacommand mismatch occurred before workload generation.
- [Sleep recovery record](method/fixture-recovery.json) and [relation replacement method](method/relation-replacement-method-frozen.json) document the pre-investigation replacements. Historical records mention retained containers; the later cleanup ledger records their final removal.
- [Controller sources](controller-sources/): exact historical v1/v2/v3/final snapshots, relation replacement source, and two focused correction diffs. These are evidence snapshots, **not commands to execute from this results directory**. Use the parent harness's documented entry points for an authorized new trial. No embedded credentials are included.

## Offline certification replay

From this directory:

```sh
python3 replay-certification.py
```

This performs no network or database operations. It reproduces the five valid certificates' sampled/independent counts, the rejected relation certificate, the CPU adapter's original false negative, and verifies the incomplete sleep's recorded controller exception. It uses the frozen controller's exact timestamp comparison and event-normalization rules, including idle-in-transaction filtering for the supervisor-only ClientRead certificate. It also checks sampler errors only through each certificate time, excluding later collection.

The retained result is [certification-replay-results.json](certification-replay-results.json). This is an offline consistency check, not an independent new workload trial or a substitute for the preserved observations. Live service-session absence is retained as a certificate assertion, not retroactively re-proven by this script.

## Prompt verification boundary

Saved neutral prompts match their pre-launch hashes and differ only by opaque case/output paths. The retained parent launch `message` and child assignment are encrypted runtime payloads; their exact encrypted equality is verified for all five. The parent attests that each saved neutral prompt was passed verbatim. **Independent plaintext equality cannot be established from the encrypted retained transcripts.** No ciphertext, parent transcript, workspace user context, system/developer instructions or private reasoning is exported. This limitation does not imply a detected prompt mismatch.

## Publication boundary

Only explicitly selected evidence is included. No credentials, connection wrappers, private manifests, private connection URLs, full server logs, raw runtime transcripts or internal reasoning. Credential scanning recursively checks all available private manifest password collections and embedded wrapper credentials; raw private material is not copied into this bundle. Supervisor-independent state is disclosed only now that all blind investigations are complete.
