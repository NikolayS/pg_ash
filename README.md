# pg_ash

Active Session History for Postgres — lightweight wait event sampling with zero bloat.

**The anti-extension.** Pure SQL/plpgsql that works on any Postgres 14+ — including RDS, Cloud SQL, AlloyDB, Supabase, Neon, and every other managed provider. No C extension, no `shared_preload_libraries`, no provider approval, no restart. Just `\i` and go.

## Why

Postgres has no built-in session history. When something was slow an hour ago, there is nothing to look at. pg_ash samples `pg_stat_activity` every second and stores the results in a compact format queryable with plain SQL.

### How it compares

| | pg_ash | pg_wait_sampling | pgsentinel | External sampling |
|---|---|---|---|---|
| Install | `\i` (pure SQL) | shared_preload_libraries | Compile + shared_preload_libraries | Separate infra |
| Works on managed (RDS, Cloud SQL, Supabase, ...) | Yes | Cloud SQL only (as of early 2026) | Not known to be supported | Yes, with effort |
| Sampling rate | 1s (via pg_cron) | 10ms (in-process) | 10ms (in-process) | 15-60s typical |
| Visibility | Inside Postgres | Inside Postgres | Inside Postgres | Outside only |
| Storage | Disk (~30 MiB/day) | Memory only | Memory only | External store |
| Historical queries | Yes (persistent) | Ring buffer (lost on restart) | Ring buffer (lost on restart) | Depends on setup |
| Pure SQL | Yes | No (C extension) | No (C extension) | No |
| Maintenance overhead | None | None | None | High |
| Requirements | pg_cron 1.5+ | shared_preload_libraries (restart required) | Compile + shared_preload_libraries (restart required) | Agent + storage |

## Quick start

```sql
-- install (just run the SQL file — works on RDS, Cloud SQL, AlloyDB, etc.)
\i ash--1.0.sql

-- start sampling (1 sample/second via pg_cron)
select ash.start('1 second');

-- wait a few minutes, then query
select * from ash.top_waits('1 hour');
select * from ash.top_queries_with_text('1 hour');
select * from ash.waits_by_type('1 hour');

-- stop sampling
select ash.stop();

-- uninstall (drops the ash schema and pg_cron jobs)
select ash.uninstall();
```

## Usage

### What hurt recently?

```sql
-- morning coffee: what happened overnight?
select * from ash.activity_summary('8 hours');
```

```
        metric        |            value
----------------------+------------------------------
 time_range           | 08:00:00
 total_samples        | 28800
 avg_active_backends  | 16.4
 peak_active_backends | 25
 peak_time            | 2026-02-14 03:17:42+00
 databases_active     | 3
 top_wait_1           | CPU* (37.33%)
 top_wait_2           | Lock:tuple (24.01%)
 top_wait_3           | LWLock:WALWrite (14.67%)
 top_query_1          | 1234567890 (40.00%)
 top_query_2          | 9876543210 (22.66%)
 top_query_3          | 5555555555 (21.33%)
```

```sql
-- top wait events in the last hour (default: top 10 + Other)
select * from ash.top_waits('1 hour');
```

```
    wait_event     | samples |  pct
-------------------+---------+-------
 CPU*              |   20160 | 37.33
 Lock:tuple        |   12965 | 24.01
 LWLock:WALWrite   |    7920 | 14.67
 IO:DataFileWrite  |    7200 | 13.33
 IO:DataFileRead   |    5760 | 10.67
 Client:ClientRead |    3600 |  6.67
 Timeout:PgSleep   |    2160 |  4.00
 LWLock:BufferIO   |    1440 |  2.67
 Lock:transactionid|     720 |  1.33
 Other             |     540 |  1.00
```

```sql
-- top queries with text from pg_stat_statements
select * from ash.top_queries_with_text('1 hour', 5);
```

```
   query_id   | samples |  pct  | calls | mean_time_ms | query_text
--------------+---------+-------+-------+--------------+-------------------------------
 1234567890   |   21600 | 40.00 |  5600 |        12.34 | select * from orders where...
 9876543210   |   12240 | 22.66 |  3200 |         8.56 | update inventory set quant...
 5555555555   |   11520 | 21.33 |  1800 |        45.67 | select count(*) from event...
```

```sql
-- breakdown by wait event type
select * from ash.waits_by_type('1 hour');
```

```
 wait_event_type | samples |  pct
-----------------+---------+-------
 CPU*            |   20160 | 37.33
 Lock            |   12965 | 24.01
 IO              |    7200 | 13.33
 LWLock          |    7920 | 14.67
 Client          |    3600 |  6.67
 Timeout         |    2160 |  4.00
```

### Investigate an incident

Use the `_at` functions with absolute timestamps to zoom into a specific time window:

```sql
-- what happened between 3:00 and 3:10 am?
select * from ash.top_waits_at('2026-02-14 03:00', '2026-02-14 03:10');

-- which queries were running during the incident?
select * from ash.top_queries_at('2026-02-14 03:00', '2026-02-14 03:10');

-- minute-by-minute timeline of the incident
select * from ash.wait_timeline_at(
    '2026-02-14 03:00',
    '2026-02-14 03:10',
    '1 minute'
);
```

### Visual histogram

```sql
select * from ash.histogram('1 hour');
```

```
     wait_event       | samples |  pct  |                    bar
----------------------+---------+-------+-------------------------------------------
 CPU*                 |   20160 | 37.33 | ████████████████████████████████████████ 37.33%
 Lock:tuple           |   12965 | 24.01 | █████████████████████████ 24.01%
 LWLock:WALWrite      |    7920 | 14.67 | ███████████████ 14.67%
 IO:DataFileWrite     |    7200 | 13.33 | ██████████████ 13.33%
 IO:DataFileRead      |    5760 | 10.67 | ███████████ 10.67%
 Client:ClientRead    |    3600 |  6.67 | ███████ 6.67%
 Timeout:PgSleep      |    2160 |  4.00 | ████ 4.00%
 LWLock:BufferIO      |    1440 |  2.67 | ██ 2.67%
 Lock:transactionid   |     720 |  1.33 | █ 1.33%
 Other                |     540 |  1.00 | █ 1.00%
```

```sql
-- histogram for a specific incident window
select * from ash.histogram_at('2026-02-14 03:00', '2026-02-14 03:10');
```

### Analyze a specific query

```sql
-- what is query 1234567890 waiting on?
select * from ash.query_waits(1234567890, '1 hour');
```

```
    wait_event    | samples |  pct
------------------+---------+-------
 Lock:tuple       |    2880 | 28.57
 IO:DataFileWrite |    2880 | 28.57
 IO:DataFileRead  |    1440 | 14.29
 CPU*             |    1440 | 14.29
 LWLock:WALWrite  |    1440 | 14.29
```

```sql
-- same, but during a specific time window
select * from ash.query_waits_at(1234567890, '2026-02-14 03:00', '2026-02-14 03:10');
```

### Check status

```sql
select * from ash.status();
```

```
           metric           |             value
----------------------------+-------------------------------
 current_slot               | 0
 sample_interval            | 00:00:01
 rotation_period            | 1 day
 samples_in_current_slot    | 86400
 last_sample_ts             | 2026-02-14 20:40:10+00
 wait_event_map_count       | 21
 query_map_count            | 200
 pg_cron_available          | yes
```

## Function reference

### Relative time (last N hours)

| Function | Description |
|----------|-------------|
| `ash.top_waits(interval, limit)` | Top wait events ranked by sample count |
| `ash.histogram(interval, limit, width)` | Visual bar chart of wait event distribution |
| `ash.top_queries(interval, limit)` | Top queries ranked by sample count |
| `ash.top_queries_with_text(interval, limit)` | Same as top_queries, with pg_stat_statements join |
| `ash.query_waits(query_id, interval)` | Wait profile for a specific query |
| `ash.waits_by_type(interval)` | Breakdown by wait event type |
| `ash.wait_timeline(interval, bucket)` | Wait events bucketed over time |
| `ash.samples_by_database(interval)` | Per-database activity |
| `ash.activity_summary(interval)` | One-call overview: samples, peak backends, top waits, top queries |
| `ash.status()` | Sampling status and partition info |

All interval-based functions default to `'1 hour'`. Limit defaults to `10` (top 9 + "Other" rollup row).

### Absolute time (incident investigation)

| Function | Description |
|----------|-------------|
| `ash.top_waits_at(start, end, limit)` | Top waits in a time range |
| `ash.histogram_at(start, end, limit, width)` | Visual bar chart in a time range |
| `ash.top_queries_at(start, end, limit)` | Top queries in a time range |
| `ash.query_waits_at(query_id, start, end)` | Query wait profile in a time range |
| `ash.waits_by_type_at(start, end)` | Breakdown by wait event type in a time range |
| `ash.wait_timeline_at(start, end, bucket)` | Wait timeline in a time range |

Start and end are `timestamptz`. Bucket defaults to `'1 minute'`.

## How it works

### Sampling

`ash.take_sample()` runs every second via pg_cron. It reads `pg_stat_activity`, groups active backends by `(wait_event_type, wait_event, state)`, and encodes the result into a single `integer[]` per database:

```
{-5, 3, 101, 102, 101, -1, 2, 103, 104, -8, 1, 105}
 │   │  │              │  │  │           │  │  │
 │   │  └─ query_ids   │  │  └─ qids     │  │  └─ qid
 │   └─ count=3        │  └─ count=2     │  └─ count=1
 └─ wait_event_id=5    └─ weid=1         └─ weid=8
```

6 active backends across 3 wait events = 1 row, 12 array elements. Full row size: 24 (tuple header) + 4 (sample_ts) + 4 (datid) + 2 (active_count) + 2 (slot) + 68 (array: 20-byte header + 12 × 4) + alignment = **106 bytes** (measured with `pg_column_size`).

### Dictionary tables

| Table | Purpose |
|-------|---------|
| `ash.wait_event_map` | Maps `(state, wait_event_type, wait_event)` to integer IDs |
| `ash.query_map` | Maps `query_id` (from `pg_stat_activity`) to integer IDs |

Dictionaries are auto-populated by the sampler. Wait events are stable (~600 entries max across all Postgres versions). Query map grows as new queries appear and is garbage-collected based on `last_seen`.

Encoding version is tracked in `ash.config.encoding_version`, not in the array itself — zero per-row overhead.

**Note on `CPU*`**: When `wait_event_type` and `wait_event` are both NULL in `pg_stat_activity`, the backend is active but not in a known wait state. This is *either* genuine CPU work *or* an uninstrumented code path where Postgres does not report a wait event. The asterisk signals this ambiguity. See [gaps.wait.events](https://gaps.wait.events) for details on uninstrumented wait events in Postgres — these gaps are being closed over time, making `CPU*` increasingly accurate.

### Rotation

Skytools PGQ-style 3-partition ring buffer. Three physical tables (`sample_0`, `sample_1`, `sample_2`) rotate on a configurable schedule (default: daily). TRUNCATE replaces the oldest partition — zero dead tuples, zero bloat, no VACUUM needed for sample tables.

Only 2 partitions hold data at any time. The third is always empty, ready for the next rotation.

```
┌──────────┐  ┌───────────┐  ┌──────────┐
│ sample_0 │  │ sample_1  │  │ sample_2 │
│ (today)  │  │(yesterday)│  │ (empty)  │
│ writing  │  │ readable  │  │ next     │
└──────────┘  └───────────┘  └──────────┘
                              ↑ TRUNCATE + rotate
```

### Reader optimization

Reader functions decode arrays inline using `generate_subscripts()` with direct array subscript access. This avoids per-row plpgsql function calls and is 9-17x faster than the `CROSS JOIN LATERAL decode_sample()` approach.

## Storage

| Active backends | Storage/day | Max on disk (2 partitions) |
|----------------|------------|---------------------------|
| 10 | 11 MiB | 22 MiB |
| 50 | 30 MiB | 60 MiB |
| 100 | 50 MiB | 100 MiB |
| 200 | 100 MiB | 200 MiB |
| 500 | 245 MiB | 490 MiB |

At 500+ backends, TOAST LZ4 compression reduces actual storage.

## Performance

Measured on Postgres 17, 50 backends, 1s sampling, `jit = off` (median of 10 runs, warm cache):

| Metric | Result |
|--------|--------|
| `top_waits('1 hour')` | 30 ms |
| `top_waits('24 hours')` | 6.1 s |
| `top_queries_with_text('1 hour')` | 31 ms |
| `take_sample()` overhead | 53 ms |
| WAL per sample | ~29 KiB (~2.4 GiB/day) |
| Rotation (1-day partition) | 9 ms |
| Dead tuples after rotation | 0 |

See [issue #1](https://github.com/NikolayS/pg_ash/issues/1) for full benchmarks — EXPLAIN ANALYZE output, backend scaling, multi-database tests, WAL analysis, and concurrency testing.

## Requirements

- Postgres 14+ (requires `query_id` in `pg_stat_activity`)
- pg_cron 1.5+ (for sub-minute scheduling)
- pg_stat_statements (optional — enables query text in `top_queries_with_text()`)

**Note on `query_id`**: The default `compute_query_id = auto` only populates `query_id` when pg_stat_statements is in `shared_preload_libraries`. If `query_id` is NULL in `pg_stat_activity`, set:

```sql
alter system set compute_query_id = 'on';
-- requires reload: select pg_reload_conf();
```

## Configuration

```sql
-- change sampling interval (default: 1 second)
select ash.stop();
select ash.start('5 seconds');

-- change rotation interval (default: 1 day)
update ash.config set rotation_period = '12 hours';

-- check current configuration
select * from ash.status();
```

## Known limitations

- **24-hour queries are slow** (~6s for full-day scan) — aggregate rollup tables are [planned](ROLLUP_DESIGN.md).
- **JIT protection built in** — all reader functions use `SET jit = off` to prevent JIT compilation overhead (which can be 10-750x slower depending on Postgres version and dataset size). No global configuration needed.
- **Single-database install** — pg_ash installs in one database and samples all databases from there. Per-database filtering works via the `datid` column.

## License

[Apache 2.0](LICENSE)
