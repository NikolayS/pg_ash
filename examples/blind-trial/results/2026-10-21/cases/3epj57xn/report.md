# Incident investigation: 3epj57xn

## Strongest finding

The retained observations show **client-input waits, not demonstrated server execution, CPU saturation, I/O pressure, or lock contention**. All 64 sampled backend-seconds in the observed incident window were `Client:ClientRead`, attributed to query ID `-9127212586606946335`, with current best-effort text:

```sql
select payload from records where id = $1
```

`ClientRead` means the server was waiting to receive client input. The association with this SELECT does **not** prove this SELECT was executing slowly; the identifier/text can describe the associated/last statement while the connection is waiting for further client protocol input. A plausible explanation is application-side pacing, an open transaction awaiting its next command, or network/protocol input delay. The historical data available here does not distinguish these possibilities or establish an idle-in-transaction condition.

## Scope and measurements

No incident bounds were supplied, so I used `ash.periods()`, status, and a chart to locate retained activity. All bounds below are UTC and end-exclusive.

- Discovery window: `[2026-10-21 21:56:00, 22:01:00)`. Source `rollup_1m`, one-minute resolution, 2 of 5 buckets containing observations; average AAS 0.21, peak minute AAS 0.67, 64 backend-seconds. The three empty minutes are not a verified quiet baseline.
- Selected observed incident window: `[2026-10-21 21:59:00, 22:01:00)`. Source `raw`, effective one-minute bucket, 2 of 2 minutes containing observations; average AAS **0.53**, peak minute AAS **0.67**, p99 minute AAS 0.66, 64 backend-seconds.
- Minute breakdown: 21:59 = 0.67 AAS / 40 backend-seconds; 22:00 = 0.40 AAS / 24 backend-seconds.
- Actual raw observations: 32 distinct one-second timestamps, each decoding to two backend observations in database `appdb`, all on the selected query and ClientRead event. They run from 21:59:20–21:59:33 inclusive and 21:59:54–22:00:11 inclusive. No rows exist for the 20 intervening seconds. This is not evidence of continuously sampled activity throughout either minute.
- The momentary observed concurrency is two backends, whereas 0.67 is the peak **minute-average** AAS. Do not confuse the two or add each decoded row's `active_backends=2`: that field is the enclosing sample's reported backend count.

## Retention and instrumentation evidence

`ash.status()` reports version 2.0-rc1, sampling enabled at one second, client-only collection (`include_bg_workers=false`), 32 stored raw sample rows, 2 one-minute rollup rows (21:59 and 22:00), no hourly rollups, and zero current missed/skipped/insert-error counters. Raw retention is one day plus the current partial day; reported logical boundary is 2026-10-20 21:52:00 UTC. Last sample is 22:00:11 UTC. pg_cron is unavailable; an external scheduler is required.

These are current configuration/counters, not independent historical scheduler proof. Idle ticks write no sample; missing observations cannot distinguish idle workload, missed collection, or an observation gap. Sampling cadence changes, historical permissions, and activity outside retained observations remain unverified. AAS is sampled activity, not response latency or CPU utilization.

## Baseline check

I evaluated the preceding equal-duration candidate `[21:57:00, 21:59:00)` before interpreting a comparison:

- `ash.aas`: source `rollup_1m`, expected 2 buckets, **0 with data**; displayed zero AAS.
- `ash.chart`: no rows.
- `ash.compare`: baseline values and `avg_delta` **NULL**, incident ClientRead AAS 0.53, incident share 100%.
- Preserved diagnostic: `NOTICE: ash.compare: window 1 (2026-10-21 21:57:00+00 to 2026-10-21 21:59:00+00) has no data coverage — its columns and avg_delta are NULL, not zero. Check retention with ash.status() before reading a delta as a change.`

Thus no verified normal baseline or quantified increase from normal is available. The incident window is the location of observed activity, not a proven workload regression.

## Supporting SQL and captured results

Exact executed SQL is in `evidence.sql`; echoed SQL, stdout, and diagnostics are in `evidence.txt`, both alongside this report. All investigation queries used the supplied connection. The initial discovery `ash.periods()` was read-only SELECT; subsequent queries were enclosed in `begin read only` with a 30-second local statement timeout. No database state was changed.

Key drill:

```sql
select * from ash.top(
  'wait_event',
  since=>'2026-10-21 21:59:00+00', until=>'2026-10-21 22:01:00+00',
  order_by=>'peak'
);
-- Client:ClientRead; raw; avg_aas=0.53; peak_aas=0.67;
-- backend_seconds=64; pct=100.

select * from ash.top(
  'query_id',
  since=>'2026-10-21 21:59:00+00', until=>'2026-10-21 22:01:00+00',
  wait_event=>'Client:ClientRead'
);
-- -9127212586606946335; SELECT text above; raw;
-- avg_aas=0.53; backend_seconds=64; pct=100.

select * from ash.samples(
  since=>'2026-10-21 21:59:00+00', until=>'2026-10-21 22:01:00+00',
  wait_event=>'Client:ClientRead', n=>4
);
-- Four latest decoded backend observations, all appdb / ClientRead /
-- selected query ID, at 22:00:11 and 22:00:10 (two each).
```

The selected query's wait profile in the peak minute is 100% ClientRead (40 backend-seconds). A bounded 1,000-row raw query, grouped by timestamp, returned all 32 timestamps / 64 decoded rows described above. An additional current `pg_stat_activity` check found only an idle collector connection, so it does not reveal the historical incident sessions' transaction state or identity.

## Useful next discriminating check

At recurrence, correlate application request/connection tracing with a timestamped capture of `pg_stat_activity` containing `pid`, `application_name`, `client_addr`, `state`, `xact_start`, `state_change`, `query_start`, and wait fields for the affected query/connections. Specifically determine whether these ClientRead sessions are **idle in transaction awaiting the application's next command** versus active waiting for protocol input, and match that to time spent in the application or transport. Independently confirm sampler ticks during observation gaps. Historical pg_ash here cannot supply those missing state/client details, prove a blocker, identify row values, or justify a production SQL/index/transaction change.
