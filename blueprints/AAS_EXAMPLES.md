# pg_ash 2.0 — before / after, every reader

Companion to [AAS_API.md](AAS_API.md). One example per v1.x reader: the v1.5
call and its output, then the 2.0 equivalent. All outputs are **illustrative**
(one consistent scenario) — column contracts are exact, numbers are invented.

**The scenario used throughout:** an IO spike around `14:32` on a 1-second
sample interval; query `8231004856741017` doing `DataFileRead`, with a `Lock`
tail behind it.

Conventions that repeat below, stated once:

- v1.5 has two functions per reader: `f(p_interval, …)` and
  `f_at(p_start, p_end, …)`. 2.0 has one: `f(p_from, p_to, …)`, both
  defaulting so that a bare call means "the last hour."
- v1.5 units vary by function — `samples` (raw readers), `backend_seconds`
  (rollup readers), active sessions (chart only). 2.0 always answers in AAS
  (`avg_aas` / `peak_aas` / `p99_aas`), with `backend_seconds` as a secondary
  column and a `source` column showing where the data came from.

---

## 1. Triage

### `activity_summary()` → `ash.summary()` *(render)* and `ash.periods()` *(data)*

**Before (v1.5)** — mixed units, text values, no spike-vs-sustained signal:

```sql
select * from ash.activity_summary('1 hour');
```
```
        metric        |        value
----------------------+---------------------
 avg_active_backends  | 3.2
 peak_active_backends | 41
 peak_time            | 2026-07-04 14:32:10
 databases_active     | 2
 top_wait_1           | IO:DataFileRead
 top_query_1          | 8231004856741017
```

**After (2.0)** — the machine face is `periods()`: standard windows, typed
columns, p99 so a spike is legible without a second call:

```sql
select * from ash.periods();
```
```
 period | period_start        | source    | minutes_with_data | avg_aas | peak_aas | p99_aas
--------+---------------------+-----------+-------------------+---------+----------+---------
 1m     | 2026-07-04 14:44:00 | raw       |                 1 |    2.9  |     3.4  |    3.4
 5m     | 2026-07-04 14:40:00 | raw       |                 5 |    3.1  |     4.0  |    3.9
 1h     | 2026-07-04 13:45:00 | rollup_1m |                60 |    3.2  |    41.0  |   12.7
 1d     | 2026-07-03 14:45:00 | rollup_1m |              1440 |    2.8  |    41.0  |    6.3
 1w     | 2026-06-27 14:45:00 | rollup_1h |             10080 |    2.6  |    41.0  |    5.9
 1mo    | 2026-06-04 14:45:00 | rollup_1h |             43200 |    2.5  |    41.0  |    5.7
```

Reading: the last hour peaked at 41 while the average is 3.2 — a spike, not a
sustained shift. (`ash.summary()` renders the same picture for humans.)

## 2. Locate

### `wait_timeline(interval, bucket)` / `timeline_chart(...)` → `ash.timeline()` *(data)* / `ash.chart()` *(render)*

**Before** — unit is raw sample counts, one row per (bucket × wait_event), no
peak within bucket, silence when the sampler was off:

```sql
select * from ash.wait_timeline('30 minutes', '5 minutes');
```
```
    bucket_start     |    wait_event    | samples
---------------------+------------------+---------
 2026-07-04 14:20:00 | CPU*             |     612
 2026-07-04 14:20:00 | IO:DataFileRead  |     188
 2026-07-04 14:30:00 | IO:DataFileRead    |    9411
 2026-07-04 14:30:00 | Lock:transactionid |    2050
```

**After** — one row per bucket, AAS units, per-bucket peak + p99, explicit
no-data rows:

```sql
select * from ash.timeline(p_from => '2026-07-04 14:15', p_to => '2026-07-04 14:45');
```
```
    bucket_start     | source | data_points | avg_aas | peak_aas | p99_aas
---------------------+--------+-------------+---------+----------+---------
 2026-07-04 14:15:00 | raw    |         300 |    2.7  |     3.6  |    3.5
 2026-07-04 14:20:00 | raw    |         300 |    2.9  |     4.1  |    4.0
 2026-07-04 14:25:00 | raw    |           0 |         |          |          -- sampler was off: no data ≠ zero
 2026-07-04 14:30:00 | raw    |         300 |   24.8  |    41.0  |   39.2   -- ← the spike
 2026-07-04 14:35:00 | raw    |         300 |    6.1  |    11.4  |   10.9
 2026-07-04 14:40:00 | raw    |         300 |    3.0  |     3.8  |    3.7
```

`ash.chart()` is the human rendering of the same series (stacked by wait
class, colors optional) — unchanged in spirit from `timeline_chart`.

## 3. Drill

### `top_by_type(...)` → `ash.top('wait_event_type', …)`

**Before** — samples + presentation `bar` mixed into the data:

```sql
select * from ash.top_by_type('15 minutes');
```
```
 wait_event_type | samples |  pct  |        bar
-----------------+---------+-------+--------------------
 IO              |   14210 |  62.4 | ██████████████████
 Lock            |    4180 |  18.4 | █████
 CPU*            |    3390 |  14.9 | ████
 LWLock          |     980 |   4.3 | █
```

**After** — same drill, AAS + peak + p99 per row (a spiky class is
distinguishable from a steadily-busy one), no presentation columns:

```sql
select * from ash.top('wait_event_type', p_from => '2026-07-04 14:30', p_to => '2026-07-04 14:45');
```
```
 key    | query_text | source | avg_aas | peak_aas | p99_aas | backend_seconds |  pct
--------+------------+--------+---------+----------+---------+-----------------+-------
 IO     |            | raw    |   15.8  |    33.0  |   31.5  |           14210 |  62.4
 Lock   |            | raw    |    4.6  |    12.0  |   11.2  |            4180 |  18.4
 CPU*   |            | raw    |    3.8  |     6.0  |    5.7  |            3390 |  14.9
 LWLock |            | raw    |    1.1  |     3.0  |    2.8  |             980 |   4.3
```

### `top_waits(...)` → `ash.top('wait_event', …)`

**Before:**

```sql
select * from ash.top_waits('15 minutes', 5);
```
```
    wait_event      | samples |  pct  | bar
--------------------+---------+-------+------
 IO:DataFileRead    |   12630 |  55.4 | ████████████████
 Lock:transactionid |    3910 |  17.2 | █████
 CPU*               |    3390 |  14.9 | ████
```

**After** — and the L2 drill composes with the L1 filter instead of being a
separate function:

```sql
select * from ash.top('wait_event', p_wait_event_type => 'IO',
                      p_from => '2026-07-04 14:30', p_to => '2026-07-04 14:45');
```
```
 key              | query_text | source | avg_aas | peak_aas | p99_aas | backend_seconds |  pct
------------------+------------+--------+---------+----------+---------+-----------------+------
 IO:DataFileRead  |            | raw    |   14.0  |    31.0  |   29.8  |           12630 | 55.4
 IO:DataFileWrite |            | raw    |    1.2  |     2.0  |    1.9  |            1080 |  4.7
 IO:WALSync       |            | raw    |    0.6  |     2.0  |    1.7  |             500 |  2.3
```

### `top_queries(...)` / `top_queries_with_text(...)` → `ash.top('query_id', …)`

**Before** (two functions; `_with_text` also mixed pg_stat_statements
execution metrics into the ASH result):

```sql
select * from ash.top_queries('15 minutes', 3);
```
```
     query_id      | samples |  pct  |            query_text
-------------------+---------+-------+-----------------------------------
  8231004856741017 |   11890 |  52.2 | select o.*, c.name from orders o…
  -882290014352918 |    3020 |  13.3 | update inventory set qty = qty -…
  4411002933801220 |    1470 |   6.5 | select count(*) from events wher…
```

**After:**

```sql
select * from ash.top('query_id', p_from => '2026-07-04 14:30', p_to => '2026-07-04 14:45', p_limit => 3);
```
```
       key         |            query_text             | source | avg_aas | peak_aas | p99_aas | backend_seconds |  pct
-------------------+-----------------------------------+--------+---------+----------+---------+-----------------+------
  8231004856741017 | select o.*, c.name from orders o… | raw    |   13.2  |    30.0  |   28.7  |           11890 | 52.2
  -882290014352918 | update inventory set qty = qty -… | raw    |    3.4  |     9.0  |    8.5  |            3020 | 13.3
  4411002933801220 | select count(*) from events wher… | raw    |    1.6  |     3.0  |    2.9  |            1470 |  6.5
```

(pg_stat_statements execution metrics no longer ride along — join on
`query_id` yourself when you want them.)

## 4. Leaf (the drill v1.x could only half-do)

### `query_waits(query_id, …)` → `ash.top('wait_event', p_query_id => …)`

**Before:**

```sql
select * from ash.query_waits(8231004856741017, '15 minutes');
```
```
    wait_event     | samples |  pct  | bar
-------------------+---------+-------+--------
 IO:DataFileRead   |   10110 |  85.0 | █████████████
 CPU*              |    1300 |  10.9 | ██
```

**After:**

```sql
select * from ash.top('wait_event', p_query_id => 8231004856741017,
                      p_from => '2026-07-04 14:30', p_to => '2026-07-04 14:45');
```

Same shape as every other `top()` call — `avg_aas 11.2, peak_aas 26.0, p99_aas 24.9` for `IO:DataFileRead`, etc.

### `event_queries(event, …)` → `ash.top('query_id', p_wait_event => …)` + `ash.aas(p_wait_event => …)`

**Before** — sample counts only; no way to see whether the event itself was
spiky, and silently empty if the window predated raw retention:

```sql
select * from ash.event_queries('DataFileRead', '15 minutes');
```
```
     query_id      | samples |  pct  |            query_text
-------------------+---------+-------+-----------------------------------
  8231004856741017 |   10110 |  80.1 | select o.*, c.name from orders o…
```

**After** — the event's own avg/peak/p99 first, then its per-query split:

```sql
select avg_aas, peak_aas, p99_aas from ash.aas(p_wait_event => 'DataFileRead',
       p_from => '2026-07-04 14:30', p_to => '2026-07-04 14:45');
```
```
 avg_aas | peak_aas | p99_aas
---------+----------+---------
   14.0  |    31.0  |   29.8
```

```sql
select * from ash.top('query_id', p_wait_event => 'DataFileRead',
                      p_from => '2026-07-04 14:30', p_to => '2026-07-04 14:45');
```

And if you ask for a window whose raw samples are gone, you get an error, not
an empty table:

```
ERROR:  pg_ash: this drill needs raw samples; raw retention starts at
        2026-07-03 00:00:00+00 but requested window starts at 2026-06-28 …
HINT:   Narrow the window or drill without the query/event tie.
```

## 5. Long windows (rollup readers)

### `minute_waits(...)`, `hourly_queries(...)`, `daily_peak_backends(...)` → `ash.timeline()` / `ash.top()` with auto source

**Before** — three fixed-grain functions, `backend_seconds` units, grain baked
into the name:

```sql
select * from ash.daily_peak_backends('7 days');
```
```
    day     | peak_backends | avg_backends
------------+---------------+--------------
 2026-06-28 |            12 |          2.4
 2026-06-29 |            41 |          2.9
```

**After** — one series function; grain and source are chosen from the window
(and reported), unit is AAS everywhere:

```sql
select * from ash.timeline(p_from => now() - interval '7 days');  -- auto: 1-hour buckets, rollup_1h
```
```
    bucket_start     |  source   | data_points | avg_aas | peak_aas | p99_aas
---------------------+-----------+-------------+---------+----------+---------
 2026-06-28 00:00:00 | rollup_1h |        3600 |    2.1  |     4.0  |
 2026-06-28 01:00:00 | rollup_1h |        3600 |    2.3  |     5.0  |
 …
```

`hourly_queries('7 days')` becomes `ash.top('query_id', p_from => now() - interval '7 days')` —
same query ranking, over rollups, in AAS.

### `samples_by_database(...)` → `ash.top('database', …)`

```sql
select * from ash.top('database', p_from => now() - interval '1 hour');
```
```
   key    | query_text | source    | avg_aas | peak_aas | p99_aas | backend_seconds |  pct
----------+------------+-----------+---------+----------+---------+-----------------+------
 shop     |            | rollup_1m |    2.9  |    39.0  |   11.8  |           10440 | 90.6
 metrics  |            | rollup_1m |    0.3  |     2.0  |    1.2  |            1080 |  9.4
```

## 6. Raw evidence

### `samples(...)` / `samples_at(...)` → `ash.samples(p_from, p_to, …)`

Same decoded raw rows; single function, uniform filters:

```sql
select * from ash.samples(p_limit => 3, p_wait_event => 'DataFileRead');
```

## 7. New in 2.0 (no v1.x equivalent)

### `ash.aas()` — scalar load, any window, any filter

```sql
select * from ash.aas();   -- the last hour, one row
```
```
    period_start     |      period_end      | source    | buckets_expected | buckets_with_data | avg_aas | peak_aas | p99_aas | backend_seconds
---------------------+----------------------+-----------+------------------+-------------------+---------+----------+---------+-----------------
 2026-07-04 13:45:00 | 2026-07-04 14:45:00  | rollup_1m |               60 |                59 |    3.2  |    41.0  |   12.7  |           11520
```

### `ash.compare()` — before/after a deploy

```sql
select * from ash.compare(p_from_1 => '14:00', p_to_1 => '14:30',    -- baseline
                          p_from_2 => '14:30', p_to_2 => '15:00',    -- after deploy
                          p_dimension => 'wait_event');
```
```
        key         | avg_aas_1 | avg_aas_2 | avg_delta | peak_aas_1 | peak_aas_2 | p99_aas_1 | p99_aas_2 | pct_1 | pct_2
--------------------+-----------+-----------+-----------+------------+------------+-----------+-----------+-------+-------
 IO:DataFileRead    |      1.1  |     14.0  |    +12.9  |       2.0  |      31.0  |      1.9  |     29.8  |  34.4 |  55.4
 Lock:transactionid |      0.2  |      4.6  |     +4.4  |       1.0  |      12.0  |      0.9  |     11.2  |   6.3 |  18.4
 CPU*               |      1.7  |      3.8  |     +2.1  |       3.0  |       6.0  |      2.8  |      5.7  |  53.1 |  14.9
```

### `ash.report()` — one JSON load report per period (machine ingest)

```sql
select ash.report(p_from => '2026-07-04 00:00', p_to => '2026-07-05 00:00');
```
```json
{
  "aas_avg":     {"total": 3.2, "cpu": 1.8, "io": 0.9, "ipc": 0.2, "lock": 0.2, "lwlock": 0.1},
  "aas_worst1m": {"total": 41.0, "cpu": 6.0, "io": 31.0, "ipc": 1.0, "lock": 12.0, "lwlock": 3.0},
  "aas_p99":     {"total": 12.7, "cpu": 4.1, "io": 6.2, "ipc": 0.8, "lock": 2.3, "lwlock": 0.9},
  "aas_p999":    {"total": 38.5, "cpu": 5.8, "io": 28.9, "ipc": 1.0, "lock": 10.4, "lwlock": 2.7},
  "top_events_worst1m":   {"io": ["DataFileRead(29.8)", "DataFileWrite(1.1)"], "ipc": ["WalSyncMethodAssign(0.4)"], "lock": ["transactionid(9.7)"], "lwlock": ["WALWrite(1.9)"]},
  "top_events_p99":       {"io": ["DataFileRead(5.1)"], "ipc": [], "lock": ["transactionid(1.4)"], "lwlock": []},
  "top_events_p999":      {"io": ["DataFileRead(27.0)"], "ipc": [], "lock": ["transactionid(8.8)"], "lwlock": ["WALWrite(1.6)"]},
  "top_queryids_worst1m": {"total": ["8231004856741017(28.4)"], "io": ["8231004856741017(26.1)"], "ipc": [], "lock": ["-882290014352918(8.9)"], "lwlock": []},
  "top_queryids_p99":     {"total": ["8231004856741017(4.9)"], "io": ["8231004856741017(4.4)"], "ipc": [], "lock": [], "lwlock": []},
  "top_queryids_p999":    {"total": ["8231004856741017(24.7)"], "io": ["8231004856741017(23.0)"], "ipc": [], "lock": ["-882290014352918(8.1)"], "lwlock": []}
}
```

---

## The whole mapping at a glance

| v1.5 (each also had an `_at` twin) | 2.0 |
|---|---|
| `activity_summary` | `summary` (render) / `periods` (data) |
| `timeline_chart` | `chart` (render) / `timeline` (data) |
| `wait_timeline` | `timeline` |
| `top_by_type` | `top('wait_event_type')` |
| `top_waits` | `top('wait_event')` |
| `top_queries`, `top_queries_with_text` | `top('query_id')` |
| `query_waits(q)` | `top('wait_event', p_query_id => q)` |
| `event_queries(e)` | `top('query_id', p_wait_event => e)` |
| `samples_by_database` | `top('database')` |
| `minute_waits`, `hourly_queries`, `daily_peak_backends` | `timeline` / `top` (auto source) |
| `samples`, `samples_at` | `samples` |
| — | `aas` (new) |
| — | `compare` (new) |
| — | `report` (new) |
