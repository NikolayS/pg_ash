# Incident investigation

## Strongest supported finding

Retained raw history shows transaction-ID lock contention affecting the single-row update `update records set value = value + $2 where id = $1` (query ID `-1016721775295826782`) in database `appdb`. It accounts for all 64 sampled lock-wait backend observations and 65.98% of captured backend-seconds in the selected interval. This is an affected statement, not an identified blocker.

An important qualification is an internal history gap: samples jump from **2026-10-21 21:57:31+00 to 21:57:52+00**, leaving 20 intervening one-second timestamps without observations. The history does not distinguish an idle interval from stopped/missed collection. Thus the two bursts cannot be described as uninterrupted lock contention, and the AAS values are observed-sample-derived, not proof of complete-window activity.

## Windows, load, and source

No incident bounds were supplied, so I located activity using `ash.periods()` and a five-minute timeline ending at its returned 21:59 UTC boundary.

- Discovery window: [2026-10-21 21:54:00+00, 21:59:00+00). Rollup_1m, one-minute resolution; 2/5 buckets have data. Returned average 0.32 AAS, peak 1.12 AAS, 97 backend-seconds. Earlier three minutes have NULL timeline values, not verified idle activity.
- Selected drill window from observed buckets: **[2026-10-21 21:57:00+00, 21:59:00+00)**. Raw source, one-minute aggregate resolution; average 0.81 AAS, peak minute 1.12 AAS (21:57), then 0.50 AAS (21:58); 97 backend-seconds.
- Raw timestamp precision is one second. There are 33 distinct stored sample timestamps, 97 decoded backend observations, typically three backends per observed tick (first tick has one). Three observed backends is not the same metric as the returned 1.12 peak-minute AAS.
- First/last observations: 21:57:17–21:58:09 UTC. Lock observations: 21:57:18–21:58:09 UTC across only 32 observed ticks. No reliable exact incident onset/end is available.

## Wait and query evidence

For the fixed two-minute drill window:

| Wait | Observations / weighted backend-seconds | Average AAS | Peak minute AAS | Share |
|---|---:|---:|---:|---:|
| Lock:transactionid | 64 | 0.53 | 0.73 | 65.98% |
| Client:ClientRead | 33 | 0.28 | 0.38 | 34.02% |

The lock-filtered query ranking returns only query `-1016721775295826782`, the single-row update above, with 64 backend-seconds and 100% of that wait. Its complete sampled wait profile is also 100% Lock:transactionid.

The other captured query ID is `159435708526309938`, with current best-effort SQL text `update records set value = value + $1 where id in ($2 /*, ... */)`. Its 33 observations are Client:ClientRead. Bounded raw output for 21:58:06–09 shows one ClientRead observation and two transactionid-wait observations at each tick. SQL text is current pg_stat_statements attribution, not archived historical text.

## Hypothesis, not proven root cause

A transaction associated with the multi-row update may have held row locks while waiting for further client input, delaying concurrent single-row updates. Co-occurring ClientRead and transactionid waits make that plausible. However, pg_ash does not retain a blocker graph, transaction state, conflicting row values, or transaction identity sufficient to prove this. ClientRead is not by itself proof of an idle-in-transaction blocker, and the captured multi-row update must not be labeled the blocker as a fact.

A current `pg_stat_activity`/`pg_blocking_pids()` check found no live blocking edges: appdb had one active investigation connection and one idle ClientRead connection, both with empty blocker arrays. It cannot recover the historical blocker.

## Baseline and collection limitations

The preceding equal-duration candidate baseline [2026-10-21 21:55:00+00, 21:57:00+00) returns 0.00 AAS but **zero data-bearing buckets**. It is not a verified normal baseline. I did not interpret that zero as idle activity or run a misleading before/after comparison.

Status reports pg_ash 2.0-rc1 on a primary, configured interval one second, background workers excluded, raw retention one day plus current partial day, sampling enabled, zero skipped/missed/insert-error counters, 33 raw sample rows, two one-minute rollup rows, and no pg_cron (external scheduler required). Last stored activity is 21:58:09 UTC. These current status counters and settings do not establish historical scheduler execution or continuity, especially across the internal gap. No heartbeat is retained. A cadence change could also affect historical weights. No partial-source notices appeared.

An exploratory one-second `ash.timeline` bucket failed with `ERROR: bucket must be at least 1 minute, got 00:00:01`; that transaction aborted without changes. I used decoded raw samples to inspect second-level timestamps instead. There were no other query failures.

No CPU utilization, workload latency, throughput, successful-transaction counts, or resource-capacity evidence was available; no CPU saturation or user-visible latency magnitude is claimed.

## Useful next check

At recurrence, capture `pg_stat_activity` transaction states/start times together with `pg_blocking_pids()` and `pg_locks` at the same instant. Check whether the multi-row update's transaction is actually the blocker and idle in transaction awaiting client input. In parallel, verify external sampler execution logs specifically for **21:57:32–21:57:51 UTC**; this distinguishes a collection blind spot from an actual gap in captured activity before comparing load or duration. No production change is justified solely by these samples.

## Reproducible SQL and evidence

Executed read-only SQL with a 30-second statement timeout is saved in `evidence.sql`; exact SQL and stdout/stderr are in `evidence.txt` in this report's directory. Only the supplied SQL connection and public documentation were used. No database state was changed.

Core queries:

```sql
begin read only;
set local statement_timeout = '30s';
select * from ash.status();
select * from ash.timeline(
  since => '2026-10-21 21:54+00', until => '2026-10-21 21:59+00');
select * from ash.aas(
  since => '2026-10-21 21:57+00', until => '2026-10-21 21:59+00');
select * from ash.top('wait_event',
  since => '2026-10-21 21:57+00', until => '2026-10-21 21:59+00');
select * from ash.top('query_id',
  since => '2026-10-21 21:57+00', until => '2026-10-21 21:59+00',
  wait_event => 'Lock:transactionid');
select * from ash.samples(
  since => '2026-10-21 21:57+00', until => '2026-10-21 21:59+00', n => 12);
commit;
```
