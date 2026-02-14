# pg_ash

Active Session History for Postgres — lightweight wait event sampling with zero bloat.

Pure SQL/plpgsql. No C extension, no `shared_preload_libraries` changes. Install and go.

## Why

Postgres has no built-in session history. When something was slow an hour ago, there is nothing to look at. pg_ash samples `pg_stat_activity` every second and stores the results in a compact format queryable with plain SQL.

### How it compares

| | pg_ash | pg_wait_sampling | pgsentinel |
|---|---|---|---|
| Install | `CREATE EXTENSION` | shared_preload_libraries | Compile + shared_preload_libraries |
| Storage | Disk (~30 MiB/day) | Memory only | Memory only |
| Historical queries | Yes (persistent) | No (ring buffer) | No (ring buffer) |
| Pure SQL | Yes | No (C extension) | No (C extension) |
| Requires | pg_cron 1.5+ | — | — |

## Quick start

```sql
-- install
create extension pg_ash;

-- start sampling (1 sample/second via pg_cron)
select ash.start('1 second');

-- wait a few minutes, then query
select * from ash.top_waits('1 hour');
select * from ash.top_queries_with_text('1 hour');
select * from ash.cpu_vs_waiting('1 hour');

-- stop sampling
select ash.stop();
```

## Usage

### What hurt recently?

```sql
-- morning coffee: overnight summary
select * from ash.activity_summary('8 hours');

-- top wait events in the last hour
select * from ash.top_waits('1 hour');

-- top queries with text from pg_stat_statements
select * from ash.top_queries_with_text('1 hour');

-- CPU vs waiting breakdown
select * from ash.cpu_vs_waiting('1 hour');

-- wait events over time (1-minute buckets)
select * from ash.wait_timeline('1 hour', '1 minute');

-- which databases are active?
select * from ash.samples_by_database('1 hour');
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

-- CPU vs waiting during the incident
select * from ash.cpu_vs_waiting_at('2026-02-14 03:00', '2026-02-14 03:10');
```

### Analyze a specific query

```sql
-- what is query 12345678 waiting on?
select * from ash.query_waits(12345678, '1 hour');

-- same, but during a specific time window
select * from ash.query_waits_at(12345678, '2026-02-14 03:00', '2026-02-14 03:10');

-- top queries with pgss stats (calls, mean_exec_time, query text)
select * from ash.top_queries_with_text('1 hour');
```

Output:

```
 query_id  | samples | pct   | calls | mean_time_ms | query_text
-----------+---------+-------+-------+--------------+---------------------------
 123456789 |    1200 | 34.20 |  5600 |        12.34 | select * from orders wh...
  87654321 |     800 | 22.80 |  3200 |         8.56 | update inventory set qu...
```

### Check status

```sql
-- sampling status, partition info, data age
select * from ash.status();
```

## Function reference

### Relative time (last N hours)

| Function | Description |
|----------|-------------|
| `ash.top_waits(interval, limit)` | Top wait events ranked by sample count |
| `ash.top_queries(interval, limit)` | Top queries ranked by sample count |
| `ash.top_queries_with_text(interval, limit)` | Same as top_queries, with pg_stat_statements join |
| `ash.query_waits(query_id, interval)` | Wait profile for a specific query |
| `ash.cpu_vs_waiting(interval)` | CPU vs waiting breakdown |
| `ash.wait_timeline(interval, bucket)` | Wait events bucketed over time |
| `ash.samples_by_database(interval)` | Per-database activity |
| `ash.activity_summary(interval)` | One-call overview: samples, peak backends, top waits, top queries |
| `ash.status()` | Sampling status and partition info |

All interval-based functions default to `'1 hour'`. Limit defaults to `20`.

### Absolute time (incident investigation)

| Function | Description |
|----------|-------------|
| `ash.top_waits_at(start, end, limit)` | Top waits in a time range |
| `ash.top_queries_at(start, end, limit)` | Top queries in a time range |
| `ash.query_waits_at(query_id, start, end)` | Query wait profile in a time range |
| `ash.cpu_vs_waiting_at(start, end)` | CPU vs waiting in a time range |
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

## Configuration

```sql
-- change sampling interval (default: 1 second)
select ash.stop();
select ash.start('5 seconds');

-- change rotation interval (default: 1 day)
update ash.config set rotation_interval = '12 hours';

-- check current configuration
select * from ash.status();
```

## Known limitations

- **24-hour queries are slow** (~6s for full-day scan) — aggregate rollup tables are [planned](ROLLUP_DESIGN.md).
- **JIT must be off for OLTP** — JIT adds 10x overhead to reader queries (30 ms to 340 ms). Disable globally with `ALTER SYSTEM SET jit = off`.
- **Array building is O(n^2)** in plpgsql at high backend counts — switching to `array_agg()` is planned.
- **Single-database install** — pg_ash installs in one database and samples all databases from there. Per-database filtering works via the `datid` column.

## License

[Apache 2.0](LICENSE)
