# Incident investigation

## Strongest supported finding

Two sampled sessions executing query ID `-9127212586606946335`, currently resolved as `select payload from records where id = $1`, experienced **relation-lock waits** in `appdb`. This is not a CPU or IO signature. All 64 retained lock-wait backend-observations in the chosen two-minute window belong to that query.

A third session was sampled on `Client:ClientRead`, with query ID `-238073253591604315`, currently resolved as `lock table records in access exclusive mode`. A transaction holding an access-exclusive lock while waiting for its client is the leading explanation for the blocked reads. **This is a hypothesis, not a reconstructed blocking graph:** the decoded history has no PID, lock ownership, transaction state, or blocker edge. Do not identify the SELECT as the blocker, or claim the LOCK statement's session is conclusively the blocker from these samples alone.

## Window, load, and retained evidence

No incident bounds were supplied. I located the only activity-bearing minute buckets returned by the readers and fixed the main window to **2026-10-21 22:11:00+00 through 22:13:00+00**, exclusive end. The selected spike minute was **22:12:00 through 22:13:00+00**.

- `ash.aas` main window: source `raw`, effective bucket **1 minute**, 2 expected and 2 activity-bearing buckets; **0.80 average AAS**, **1.15 peak minute AAS**, p99 1.14, **96 sampled backend-seconds**.
- Minute series: 22:11 AAS **0.45**; 22:12 AAS **1.15**. Surrounding 22:09, 22:10, 22:13 and 22:14 buckets show zero stored activity, not verified idle time.
- Main-window `ash.top('wait_event')`: `Lock:relation` **64 backend-seconds**, **0.53 average AAS**, peak minute 0.77, **66.67%**; `Client:ClientRead` **32 backend-seconds**, **0.27 average AAS**, peak minute 0.38, **33.33%**.
- Lock-filtered spike-minute query ranking: SELECT ID above contributes **46 backend-seconds**, **0.77 AAS**, **100%** of relation-lock evidence in that minute.
- SELECT's main-window wait profile contains only `Lock:relation`: 64 backend-seconds, 0.53 average AAS, 100%.
- Bounded `ash.samples(..., n => 100)` returns **96 rows** across **32 distinct timestamps**, first **22:11:51**, last **22:12:42**. Every represented timestamp has two SELECT/Lock:relation observations and one LOCK/ClientRead observation, with `active_backends = 3`.

**Important gap:** timestamps run from 22:11:51 through 22:12:04, then resume at 22:12:25 through 22:12:42. The largest inter-observation gap is **21 seconds**, so 22:12:05–22:12:24 has no stored observations. Sampling cannot tell whether this reflects an idle interval, missed collection, or something else. Do not infer uninterrupted blocking for 52 seconds, a recovery during the gap, or complete incident backend-time. The reported 96 backend-seconds are weighted retained observations at the current one-second cadence, not a complete elapsed-time measurement.

## Coverage and baseline limits

`ash.status()` reports version 2.0-rc1, configured one-second interval, raw retention one day plus current partial, background workers excluded, 32 stored sample rows (each decoded into three backend observations here), two minute-rollup rows, no hour rollups. Installed timestamp is **22:11:14.542024+00**. Current missed/skipped/error counters are zero and pg_cron is unavailable; these facts do not establish historical scheduler completeness. Latest observed sample is 22:12:42.

I checked the preceding equal-duration candidate baseline **22:09:00–22:11:00+00**. `ash.aas` returns source `rollup_1m`, 1-minute bucket, zero activity-bearing buckets, zero stored backend-seconds. This precedes installation and is **not a verified normal baseline**. I did not run or interpret `ash.compare` against it. No defensible before/after improvement, regression magnitude, continuous collection, or recovery claim is available.

AAS measures sampled activity, not measured CPU utilization, statement latency, or throughput. No application metrics, resource measurements, exact lock acquisition/release time, conflicting relation OID, or historical blocking chain were available. SQL text is best-effort current pg_stat_statements attribution, not historical text preserved with each observation.

## Executed SQL and reproducible evidence

Exact executed investigation SQL and stdout, including effective bounds and status, are saved alongside this report:

- `evidence.sql` — read-only transaction, 30-second local statement timeout; all successful core evidence queries.
- `evidence.txt` — echoed SQL and complete outputs, including all 96 decoded rows; no NOTICE diagnostics appeared in this capture.

Core choices were driven by returned evidence: two nonempty minute buckets selected the main window; dominant `Lock:relation` selected the wait filter; 22:12's larger AAS selected the spike minute; the returned SELECT ID selected its wait profile.

Example executed attribution query:

```sql
select * from ash.top(
  'query_id',
  since => '2026-10-21 22:12:00+00',
  until => '2026-10-21 22:13:00+00',
  wait_event => 'Lock:relation',
  order_by => 'peak'
);
```

An exploratory request for `ash.timeline(..., bucket => '1 second')` failed with `ERROR: bucket must be at least 1 minute, got 00:00:01`, SQLSTATE context in timeline line 43. The read-only transaction aborted; I then used the supported one-minute reader grain and timestamped raw samples. Initial exploratory `ash.periods()` and `\dt` were also read-only. No data, configuration, or collection state was changed.

## Useful next check

On recurrence, capture `pg_blocking_pids()` for the waiting SELECT PIDs, join those blockers to `pg_stat_activity` (state, xact_start, last query, client/application), and inspect granted/waiting `pg_locks` relation/mode entries. This distinguishes the access-exclusive-client-wait hypothesis from another relation-lock holder and identifies transaction scope. Simultaneously verify scheduler execution across the 20-second missing-observation interval; current zero error counters are insufficient. Do not terminate a session or change application transaction scope solely on this sampled history.
