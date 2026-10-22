# pg_ash incident investigation

## Conclusion

The retained activity shows explicit SQL sleeping, not demonstrated CPU, disk, or lock saturation. All 65 sampled backend-seconds in `appdb` are `Timeout:PgSleep`, attributed to query ID `-7209068558248614225`, currently resolved as `select pg_sleep($1)`. There were up to two sleeping backends at observed instants. This explains the observed database-side elapsed time for that statement; whether it caused the reported user-facing incident requires application context.

Confidence is high in the observed wait attribution, but limited in coverage and business impact. No other performance mechanism is established by these samples. In particular, a gap in stored observations is not proof that the database was idle or that the incident ended.

## Time and magnitude (UTC)

Investigation database clock initially read `2026-10-21 22:10:03.229504+00`. Fixed main window: `[2026-10-21 22:05:00+00, 2026-10-21 22:10:00+00)`.

- Five-minute average AAS: 0.22; peak one-minute AAS: 0.60; 65 backend-seconds.
- 22:07 minute: 0.48 AAS (29 backend-seconds).
- 22:08 minute: 0.60 AAS (36 backend-seconds).
- All attributed load: `appdb`, `Timeout:PgSleep`, the single query ID above.
- Raw observations: 22:07:45 through 22:07:59, then 22:08:20 through 22:08:37. There are 33 stored sample instants and 65 decoded backend observations: one backend at 22:07:45 and two at every other stored instant.
- No stored observation during 22:08:00–22:08:19. This is a coverage uncertainty, not a known healthy interval.

The 0.60 peak is a minute average, not instantaneous maximum concurrency. Raw samples show two simultaneous active sleeping backends.

## Supporting evidence and reproducible SQL

Only the provided SQL connection and public documentation were used. No database-changing operations were performed.

```sql
select now();
select * from ash.status();
select * from ash.periods();
```

Status: version 2.0-rc1; primary (`in_recovery=false`); logged samples; enabled, supported one-second cadence; background workers excluded; 33 samples total; latest activity sample 22:08:37. `missed_samples=0`, `skipped_samples=0`, `insert_errors=0`. pg_cron unavailable, so an external scheduler is required. Installation time 22:01:49.092326. Two minute-rollup rows (22:07 and 22:08), no hourly rows, minute watermark 22:09. These counters/configuration are not sampler heartbeats.

```sql
select * from ash.timeline(
  '2026-10-21 22:05+00', '2026-10-21 22:10+00');
select * from ash.top(
  'wait_event', '2026-10-21 22:05+00', '2026-10-21 22:10+00',
  order_by => 'peak');
select * from ash.top(
  'query_id', '2026-10-21 22:05+00', '2026-10-21 22:10+00',
  order_by => 'peak');
select * from ash.top(
  'database', '2026-10-21 22:05+00', '2026-10-21 22:10+00');
```

These readers returned `source=rollup_1m`, effective bucket one minute, requested bounds unchanged. Wait, query, and database each account for 100% / 65 backend-seconds. Timeline has no stored observations in 22:05, 22:06, or 22:09. Expanding the top-wait window to 22:01–22:10 yielded the same sole wait and 65 backend-seconds.

```sql
select * from ash.top(
  'query_id', '2026-10-21 22:07+00', '2026-10-21 22:09+00',
  wait_event => 'Timeout:PgSleep');
select * from ash.aas(
  '2026-10-21 22:07+00', '2026-10-21 22:09+00');
select * from ash.samples(
  '2026-10-21 22:05+00', '2026-10-21 22:10+00', n => 1000);
select sample_time, count(*) as decoded_backend_rows,
       max(active_backends) as active_backends
from ash.samples(
  '2026-10-21 22:05+00', '2026-10-21 22:10+00', n => 1000)
group by sample_time
order by sample_time;
```

The exact wait-to-query drill used raw history and returned the same query, 65 backend-seconds, 0.54 AAS over two minutes, and 0.60 peak minute. All 65 decoded rows show the same database, wait, and query ID. Raw evidence agrees with minute rollups.

```sql
select queryid, calls, total_exec_time, mean_exec_time,
       min_exec_time, max_exec_time, rows, query
from pg_stat_statements
where queryid = -7209068558248614225;
```

Corroborating cumulative statement statistics: 4 calls, total execution time 64,040.212261 ms, mean 16,010.05306525 ms, minimum 14,003.111378 ms, maximum 18,016.602289 ms, 4 rows. These are cumulative statistics, not independently timestamped to the incident. Text is current pg_stat_statements text, not historical SQL captured by pg_ash. Sleep parameter values are not retained here.

```sql
select current_setting('compute_query_id') as compute_query_id,
       pg_has_role(current_user, 'pg_read_all_stats', 'member') as has_all_stats;
select pid, datname, state, wait_event_type, wait_event, query_id,
       query, pg_blocking_pids(pid) as blocking_pids
from pg_stat_activity
where backend_type = 'client backend';
```

Query IDs are enabled and reader has all-stats membership. At inspection there was no sleeping service backend or active blocker visible: only the investigator and another idle client; blocking PID arrays were empty. This is a current snapshot and does not prove historical absence of blocking.

## Uncertainties and useful next check

- The historical data supports intentional server-side sleep, not its caller, parameter values, purpose, or end-user impact. Do not infer a CPU or IO bottleneck from AAS, or recommend indexes/lock remedies for this evidence.
- Sampling was configured for one second, but absent observations cannot distinguish idle activity from missed external scheduler ticks. Zero missed/insert-error counters do not prove the scheduler ran. Independent external-scheduler timing/health evidence is not available here.
- Sampling is primary-local, client-backend-only. Unsampled short events, replica-served traffic, background-worker load, and periods before installation cannot be ruled out.

Most useful next check: correlate application request traces/logs around 22:07:45–22:08:37 UTC with this `pg_sleep` statement, including bound duration and why it was issued; independently verify the external sampler's successful tick timestamps across 22:08:00–22:08:19. This distinguishes deliberate delay in the request path from unrelated sleep activity and resolves the principal coverage gap. No database change is justified by the retained evidence alone.
