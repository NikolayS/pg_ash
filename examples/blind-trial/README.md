# Blind investigation fixtures

Supervisor-only harness for [#285](https://github.com/NikolayS/pg_ash/issues/285).
It creates five small functional fixtures, not a throughput or overhead benchmark.
Do not give this directory, its source, seed, manifests, or rubric to a blind investigator.

## Dependencies and inputs

Use Linux, Python 3.10+, `psycopg` 3 (`python3 -m pip install 'psycopg[binary]>=3,<4'`),
Git, Docker access, a locally pulled `postgres:18` image, and `psql` on `PATH`.
The image must include `pg_stat_statements`. Five containers each use 192 MiB,
0.25 CPU, 16 MiB shared buffers and at most 16 connections. Published ports bind
only to `127.0.0.1`; local and host authentication use SCRAM; connection logging
is enabled. Provision on an appropriate test host, not a production database.

The initial trial used development SQL from the local composition
`fe3e7b4dc56f5be9977624de7c549165bc180882`, not unchanged released RC1.
Its SHA256 is `94b3d80b6864999922bd9d70794dac4073e8e10e5d11f918d2a0947adf6b6444`.
Because that composition is not a remote ref, `prepare_inputs.py` reconstructs
its installer from the release base and the exact public commits in #277,
#281, #280 and #282. It applies only their development installer diffs and
requires the exact resulting hash. Documentation is frozen at #284 commit
`95e1ecf83ce68ab9423af2568af678b87c8d9e2f`.

From a pg_ash checkout, fetch the public objects if necessary:

```sh
git fetch origin release/2.0-rc1
git fetch origin pull/277/head:refs/remotes/origin/trial-277
git fetch origin pull/281/head:refs/remotes/origin/trial-281
git fetch origin pull/280/head:refs/remotes/origin/trial-280
git fetch origin pull/282/head:refs/remotes/origin/trial-282
git fetch origin pull/284/head:refs/remotes/origin/trial-284
python3 examples/blind-trial/prepare_inputs.py --repo . --out /tmp/trial-inputs
```

Alternatively provide an installer and the four documentation files from another
revision, recording their actual revisions with `--sql-revision` and
`--docs-revision`. The harness records content hashes; revision labels alone do
not prove source identity. Do not compare a changed executable silently against
an earlier trial.

## Run and preserve

Choose a new run directory and unique prefix for every trial. Do not reuse a
failed fixture, reset ASH, insert samples, restamp timestamps, or weaken a
certification check.

```sh
python3 examples/blind-trial/trial.py setup \
  --run-dir /tmp/blind-run --prefix pgash_blind_my_trial \
  --sql-file /tmp/trial-inputs/ash-install.sql \
  --docs-dir /tmp/trial-inputs/docs
python3 examples/blind-trial/trial.py run \
  --run-dir /tmp/blind-run --keep-sampling
```

Setup creates a fresh random 128-bit seed and records it in the private supervisor manifest.
The seed controls case IDs and mechanism order. Keep it private until all investigations finish.
Use `--seed <recorded-integer>` only for explicit replay, not for a new blind trial.
A published replay seed exposes its case mappings. The original archived trial remains unchanged.

Setup records exact container IDs, an independent run label, pinned local image
ID, input hashes and dedicated credentials. Every reader is granted
`ash.grant_reader` and `pg_read_all_stats` for query-text visibility. Setup tests
actual SELECT, DML, DDL, config-DML and administrative denial with advisory
read-only mode disabled. Passwords are random, local files are private, and
role-creation utility statements are excluded from pg_stat_statements.

Workloads run serially: 35 seconds calm, 14 seconds incident, 20 seconds calm,
18 seconds incident, 35 seconds recovery, then the current minute finishes.
A separate persistent connection samples each container once per real second;
late deadlines are recorded, not backfilled. Minute maintenance runs every
15 ticks. Defaults retain raw history for at least a day; no retention or
historical evidence edits are used. The relation fixture executes the same
extended-protocol prepared SELECT before taking its blocking lock, allowing
actual historical query attribution on PG18. CPU* is active/no reported wait,
not measured CPU utilization.

Each window must have at least eight matching ASH rows and eight matching
independent activity observations, plus a nonnull affected query ID. Idle-in-
transaction observations must have that state. Certification also requires no
remaining service sessions, no sampler errors/skipped deadlines, and a minute
rollup. Failure is an invalid fixture, not an investigator failure. The
controller refuses to rerun an incomplete case in place. Preserve the failed
run and use a new directory/prefix for a separately identified trial.

Successful certificates and ready markers stay in `supervisor/<case>/`.
Give an investigator only its own `investigators/<case>/handoff.json`, the
installed `psql` wrapper and frozen `investigators/docs/` bundle. Do not expose
supervisor paths. Shared-host isolation is procedural, not a security boundary.
The wrapper records SQL arguments/stdin/script contents, output, errors, return
codes and elapsed times. Do not use alternate connection tools. Audit transcripts
for boundary violations and classify contaminated runs explicitly.

Use the same model, no inherited transcript and one initial 15-minute run per
case. Record actual launch UTC, collection age, tool/file accesses, errors,
document lookups and final output. Staggered launch times are a limitation.
Do not supply time bounds, mechanisms, affected statements, other cases or
answers. Use the initial trial’s exact neutral task below, substituting only its
connection, documentation and report paths:

```text
Investigate the performance incident using pg_ash.

SQL connection command (psql-compatible): <case psql wrapper>
Public pg_ash documentation: <frozen docs directory>

Use only that SQL connection and documentation. Do not inspect connection-wrapper source, credentials, other filesystem material, Docker, workload/setup/controller files, other cases, GitHub, or answer keys. Read-only investigation; do not change database state. Do not delegate. You have up to 15 minutes; stop earlier when ready. Write your findings and supporting SQL/evidence, uncertainties, and useful next check to <case report.md> and return the report. You may write your own query scripts in your case directory. Do not seek additional context.
```

Score five criteria 0–2 each: observed window, dominant observed mechanism,
supported query attribution, separation of observation from unproved cause,
and useful next check. Solved requires at least 8/10, correct dominant mechanism,
at least partial window credit and no confidently invented causal/blocker
identity. Keep invalid fixtures, interruptions, contamination and product/tool
errors separate. Do not coach or quietly retry an investigation. Withhold all
answer keys until every investigation finishes. Five cases are an initial
usability sample, not an estimated general success rate.

## Stop and clean up

Stop the foreground sampler with Ctrl-C before cleanup. A run-directory lock
rejects concurrent controllers and cleanup while that process is active.
Cleanup validates every container's exact ID, name, prefix and unique run label
before removing any resource, then preserves server stdout/stderr and removes
only recorded immutable IDs and their volumes. A missing recorded ID is retained
as already absent; a different container reusing its name is never touched. An
atomic progress ledger after each resource makes interrupted cleanup resumable.
Never use a prefix-wide prune.

```sh
python3 examples/blind-trial/trial.py manifest --run-dir /tmp/blind-run
python3 examples/blind-trial/trial.py cleanup --run-dir /tmp/blind-run
```

The manifest command omits passwords but still reveals mechanisms; it is for the
supervisor only. Do not publish `.pgpass`, `manifest.json`, container environment,
unredacted pgss/SQL logs or credentials. Inspect and sanitize evidence before
publication; changing a published copy must never change the retained originals.

## Provenance and verification

`provenance.json` records hashes of the original historical controllers, including
superseded failures. Those originals remain in the initial trial's private
archive; this package is a refactor, not a claim that one uninterrupted script
produced that first trial. It integrates the corrected sleep join deadline,
CPU* independent classification and prepared-query relation replacement. The
rubric and minimum counts are unchanged. Existing first-trial results belong
to the original controllers, not to a rerun of this package.

Offline checks:

```sh
python3 -m py_compile examples/blind-trial/*.py
python3 examples/blind-trial/test_guards.py
python3 examples/blind-trial/trial.py --help
```

Fresh full-five-fixture validation passed on Postgres 18.6 with the exact frozen
SQL/doc inputs. All ten windows passed unchanged counts and attribution checks;
all five reader wrappers and 25 actual ACL denials passed. Eleven offline safety
tests passed. Final immutable-ID cleanup removed all five owned validation
containers, and an absent-ID retry succeeded.

[Validation record](validation-2026-10-21.json) includes exact source hashes:
the full replay used the pre-hardening controller; later setup ID-persistence
and cleanup changes were tested separately. Workload, connection, sampler,
phase, certification and run function ASTs are identical. This is a new harness
validation, not a retry or additional blind-agent trial. Raw originals and the
exact executed controller are retained in the supervisor's validation archive.
