# pg_ash incident investigation

## Finding

The retained activity is dominated by a compute-shaped statement in `appdb`, query ID `2642533687498376244`:

```sql
select sum(length(md5(i::text))) from generate_series($1, $2) as i
```

In the observed incident drill window **2026-10-21 22:01:00+00 through 22:03:00+00 (exclusive)**, all 30 retained backend observations are `CPU*` for this query. Each observation contains one active backend. This supports active execution with no reported wait, consistent with computation in the MD5/generate_series aggregate. It does **not** establish CPU saturation, a lock incident, measured CPU utilization, or the cause of application latency. No Lock, IO, IPC, or LWLock wait was returned for this window.

## Scope and provenance

Only the supplied SQL connection and supplied public documentation were used. Database queries were read-only; the reproducible investigation uses a repeatable-read, read-only transaction with a 30-second statement timeout and rolls back. No database state was changed.

Executed SQL is in `evidence.sql`; full stdout and NOTICE diagnostics are in `evidence.txt` in this directory. Initial discovery also executed `select * from ash.periods();`, which located activity only in three recent minutes. No incident bounds were supplied, so the investigation froze the discovered five-minute window `[2026-10-21 21:59:00+00, 2026-10-21 22:04:00+00)` and chose the two elevated minutes from returned timeline buckets.

## Load and timing evidence

The five-minute window returned source `rollup_1m`, effective bucket 1 minute, 5 expected buckets, 3 with data, 31 weighted backend-seconds, average AAS 0.10, and peak one-minute AAS 0.27. The minute timeline was:

- 21:59: no data; AAS NULL.
- 22:00: no data; AAS NULL.
- 22:01: AAS 0.27.
- 22:02: AAS 0.23.
- 22:03: AAS 0.02.

The drill window `[22:01,22:03)` returned source `raw`, effective bucket 1 minute, 2 expected / 2 activity-bearing buckets, 30 backend-seconds, average AAS 0.25, peak/p99 minute AAS 0.27. These are sample-weighted quantities, not measured query duration or instantaneous concurrency peaks.

Wait ranking returned `CPU*` alone: 100%, 30 backend-seconds. The CPU*-filtered query ranking returned only query `2642533687498376244`, with the same load. Its query-filtered wait profile also returned only CPU*.

A bounded `ash.samples(..., wait_event=>'CPU*', query_id=>2642533687498376244, n=>100)` returned all 30 matching rows, spanning **22:01:22 through 22:02:14 UTC**. All rows have database `appdb`, one active backend, and the query above. Observations are not continuous: for example there is a gap between 22:01:36 and 22:01:57. These timestamps cannot distinguish workload pauses from missed collection or identify separate executions.

The initial five-minute raw sample inspection returned 31 rows total: those 30 rows plus one CPU* observation at 22:03:57 for query `-649674272853049531`, current text `select ash.rollup_minute()`. Thus the tiny 22:03 tail is collector/rollup activity, not evidence that the application query remained active through that minute. Disappearance from samples alone does not establish application recovery.

## Baseline comparison and limits

The preceding equal-duration candidate baseline was `[2026-10-21 21:59:00+00, 2026-10-21 22:01:00+00)`. It had **zero activity-bearing buckets** and cannot be verified as normal. Although scalar `ash.aas()` displayed zeros, `ash.compare()` correctly returned NULL baseline values and NULL delta and emitted:

> ash.compare: window 1 (2026-10-21 21:59:00+00 to 2026-10-21 22:01:00+00) has no data coverage — its columns and avg_delta are NULL, not zero. Check retention with ash.status() before reading a delta as a change.

Therefore no quantitative change from a verified normal baseline is established.

`ash.status()` reported version 2.0-rc1, sampling enabled, configured interval 1 second, include_bg_workers=false, 31 raw rows, three minute-rollup rows from 22:01 through 22:03, and no hour rollups. Raw logical retention boundary was 2026-10-20 21:52 UTC; installation was 2026-10-21 21:52:45 UTC. Missed/skipped/insert-error counters were zero. These current counters and activity-bearing rows do not prove historical scheduler continuity. No sampler heartbeat is stored. No pg_cron was available; scheduling is external. No partial-source notice was emitted, but its absence does not prove completeness.

SQL text is current best-effort text, not archived historical SQL. Parameter values, execution count, per-execution elapsed time, host CPU quota/core count/utilization, application latency/throughput, and background-worker load are unknown. The apparent low AAS does not rule out latency or CPU constraints. No blocker or conflicting row can be inferred, and the observed result does not support an application change by itself.

## Hypothesis and useful next check

**Hypothesis:** CPU execution of the MD5 aggregate accounts for the observed active workload. The query shape and CPU* attribution support this, but CPU scheduling contention versus on-CPU computation remains unresolved.

**Next discriminating check:** during the same query's next occurrence, correlate its backend's per-process CPU consumption and scheduler runnable delay with host/container CPU usage and CPU quota, while recording execution latency and actual generate_series bounds. This would distinguish sustained on-CPU computation from CPU scheduling contention; pg_ash alone cannot. Separately obtain scheduler tick/heartbeat evidence and an activity-covered normal interval before claiming a baseline regression.
