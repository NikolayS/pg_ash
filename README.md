# pg_ash: Active Session History for Postgres

[![CI](https://github.com/NikolayS/pg_ash/actions/workflows/test.yml/badge.svg)](https://github.com/NikolayS/pg_ash/actions/workflows/test.yml)
[![Postgres 14-19](https://img.shields.io/badge/Postgres-14--19-336791?logo=postgresql&logoColor=white)](https://github.com/NikolayS/pg_ash)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)
[![Pure SQL](https://img.shields.io/badge/Pure_SQL-no_C_extension-green)](https://github.com/NikolayS/pg_ash)

**The spike is gone. Find what Postgres was waiting for.**

pg_ash keeps sampled activity history inside Postgres.
Use SQL to find when load rose, which waits dominated, and which statements experienced them.
It is plain SQL and PL/pgSQL, not a C extension.
pg_ash itself needs no `shared_preload_libraries` entry or restart.
Installation permissions and statistics visibility still apply.

![A sampled lock incident, followed by recovery](assets/chart.svg)

The chart shows average active sessions (AAS), grouped by wait event.
Three sessions waiting throughout ten seconds contribute 30 backend-seconds.
Across a 60-second window, that is 0.5 AAS.
AAS measures sampled activity, not CPU utilization or query latency.

## Watch, try, or operate

| Goal | Start here |
|---|---|
| See an investigation | [Watch the demo](#watch-an-incident-investigation) |
| Reproduce and change the workload | [Run the before/after lab](examples/README.md#beforeafter-lock-lab) |
| Investigate an existing installation | [Follow the SQL walkthrough](#investigate-an-incident) |
| Install pg_ash | [Quick start](#quick-start) |
| Configure production collection | [Scheduling](docs/REFERENCE.md#scheduling), [privileges](docs/REFERENCE.md#privileges), and [retention](docs/REFERENCE.md#retention-and-storage) |
| Look up a function or limitation | [Reference](docs/REFERENCE.md) |

## Watch an incident investigation

![A completed incident investigated with pg_ash](assets/ash_demo.gif)

The incident has ended before this investigation starts.
The demo finds its time window, ranks waits, identifies affected statements, and compares the incident with a calm baseline.
The conclusion is evidence of lock contention, not identification of the blocking session.

**Demo timing:** the reel uses real sampled backends with a compressed, restamped timeline.
Reader output and backend counts are not edited.
The [before/after lab](examples/README.md#beforeafter-lock-lab) uses real elapsed time without restamping.
See [demo reproduction](demos/README.md) for the full collection method.

## Try a measured change

The [lock lab](examples/README.md#beforeafter-lock-lab) moves client-side work outside a transaction.
Each phase commits the same four updates and records eight one-second sample calls.
Actual runs on Postgres 17 and 18 produced:

| Phase | Committed updates | Sampled lock-wait observations |
|---|---:|---:|
| Hold the transaction during client work | 4 | 24 |
| Commit before client work | 4 | 0 |

The lab investigates both phases after their workload connections exit.
It uses separate, completed 60-second windows without rewriting timestamps.
These are sampled observations from a controlled fixture, not latency or throughput measurements.
Read the [captured output](examples/lock-lab-output.txt), or reproduce it yourself.

## Quick start

This branch contains the `2.0-rc1` candidate.
For the latest stable release, check out `v1.5` instead.
Use the instructions and SQL from the same checkout.
Postgres 14 or newer is required.

Check out the release-candidate branch:

```bash
git clone --branch release/2.0-rc1 https://github.com/NikolayS/pg_ash.git
cd pg_ash
```

Install in the database that will store the history:

```bash
psql -X -v ON_ERROR_STOP=1 -d mydb -f sql/ash-install.sql
```

Enable collection:

```sql
select ash.start('1 second');
```

**`ash.start()` does not start a background process.**
With configured pg_cron, it registers collection and maintenance jobs.
Without pg_cron, configure an [external scheduler](docs/REFERENCE.md#scheduling) before expecting history.
Neither pg_cron nor pg_stat_statements is required for installation.

Generate application activity, then inspect the history:

```sql
select * from ash.periods();
select * from ash.top('wait_event');
```

No stored observation can mean idle activity, collection failure, or expired history.
Check the scheduler separately.

Optional pg_stat_statements supplies current, best-effort SQL text.
Query IDs come from Postgres activity statistics and require query-ID computation and sufficient statistics visibility.
See [requirements](docs/REFERENCE.md#requirements) and [privileges](docs/REFERENCE.md#privileges).

**Upgrading an existing installation?**
Test on a disposable instance first.
Keep a verified backup; SQL downgrades are not supported.
Do not use `ash.uninstall()` as rollback: it deletes collected data.
Follow the [upgrade procedure](docs/REFERENCE.md#upgrade-to-20-rc-1).

<a id="investigation-flow"></a>

## Investigate an incident

Use one fixed window for every step.
Replace these bounds with the incident time, including its time zone:

```sql
\set since '2026-10-21 14:00:00+00'
\set until '2026-10-21 14:10:00+00'
```

### 1. Where did load rise?

```sql
select bucket_start, aas, detail, chart
from ash.chart(since => :'since', until => :'until', color => false);
```

Find the interval where activity increased.
Narrow `since` and `until` to that interval before the next step.
Check the returned grain and diagnostics; hour-level history cannot locate a one-minute spike.
For colored terminal output, use the [psql setup](docs/REFERENCE.md#chart-rendering).

### 2. What were sessions waiting for?

```sql
select key, avg_aas, peak_aas, pct
from ash.top(
  'wait_event', since => :'since', until => :'until',
  order_by => 'peak', n => 5
);
```

`Lock:transactionid` means sessions waited for transactions to finish.
`CPU*` means active with no reported wait, not measured CPU utilization.
A dominant lock wait does not rule out other capacity constraints.

### 3. Which statements experienced that wait?

Use the wait event returned by the previous query:

```sql
\set wait_event 'Lock:transactionid'
select key, query_text, avg_aas, pct
from ash.top(
  'query_id', since => :'since', until => :'until',
  wait_event => :'wait_event', order_by => 'peak', n => 5
);
```

These are waiting statements, not necessarily blocking statements.
SQL text can be NULL without a readable, unambiguous pg_stat_statements entry.
The stored query IDs remain useful.
Exact wait-to-query drills require retained raw history.

### 4. What changed from normal?

Use the preceding, equal-duration window as a candidate baseline.
Check that it represents normal activity before interpreting the comparison:

```sql
select
  :'since'::timestamptz - (:'until'::timestamptz - :'since'::timestamptz) as baseline_since,
  :'since'::timestamptz as baseline_until \gset
select *
from ash.compare(
  :'baseline_since', :'baseline_until', :'since', :'until',
  dimension => 'wait_event', n => 5
) \gx
```

A positive `avg_delta` means more sampled activity in the incident window.
A NULL delta is not zero; check missing-history diagnostics before interpreting it.
It does not prove why application latency changed.
Check the effective bounds and source when comparing older history.

**Next action:** inspect transaction scope and live blocking evidence, then test a specific change.
The [before/after lab](examples/README.md#beforeafter-lock-lab) demonstrates that workflow.
The [read-only investigation script](examples/llm-investigation.sql) selects drill bounds from actual results.

## Use the same evidence with an LLM

```sql
select ash.report(since => :'since', until => :'until');
```

Give the report and [analysis prompt](examples/llm-prompt.md) to your chosen model.
pg_ash makes no LLM service calls.
Review SQL text before sharing it outside your environment.
The [captured walkthrough](examples/README.md) shows a supported conclusion and its limits.
The [report reference](docs/REFERENCE.md#machine-report) explains coverage and metric semantics.

## Know what the history cannot prove

- Sampling can miss short events and cannot establish subsecond event order.
- Stored activity does not identify the blocker or conflicting row values.
- SQL text is current best-effort text, not an archive of historical statements.
- Collection observes the primary, not reads served by physical standbys.
- Missing observations do not prove that the database was idle.
- Retention changes the detail available for older incidents.

See [known limits](docs/REFERENCE.md#known-limits) before interpreting production results.
No portable 2.0 overhead budget has been established.
Measure sampler latency, WAL, storage growth, and scheduler lateness on your target workload.

## Learn more

- [Reader and admin APIs](docs/REFERENCE.md#reader-api)
- [Install, schedule, and operate](docs/REFERENCE.md#admin-api)
- [Reproduce the demo assets](demos/README.md)
- [Release process](docs/RELEASE_PROCESS.md)
- [Security](SECURITY.md)
- [License: Apache 2.0](LICENSE)

pg_ash is part of SAMO: self-driving Postgres.
