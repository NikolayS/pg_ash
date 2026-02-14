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
select * from ash.top_queries('1 hour');
select * from ash.cpu_vs_waiting('1 hour');

-- top queries with query text (requires pg_stat_statements)
select * from ash.top_queries_with_text('1 hour');

-- what is a specific query waiting on?
select * from ash.query_waits(12345678, '1 hour');

-- morning coffee: what happened overnight?
select * from ash.activity_summary('8 hours');

-- incident investigation: what happened at 3am?
select * from ash.top_waits_at('2026-02-14 02:50', '2026-02-14 03:10');
select * from ash.wait_timeline_at('2026-02-14 02:50', '2026-02-14 03:10', '1 minute');

-- stop sampling
select ash.stop();

-- uninstall (removes all data)
select ash.uninstall();
```

## Reader functions

### Relative time (last N hours)

| Function | Description |
|----------|-------------|
| `ash.top_waits(interval, limit)` | Top wait events by sample count |
| `ash.top_queries(interval, limit)` | Top queries by sample count |
| `ash.top_queries_with_text(interval, limit)` | Top queries with text from pg_stat_statements |
| `ash.query_waits(query_id, interval)` | Wait profile for a specific query |
| `ash.cpu_vs_waiting(interval)` | CPU vs waiting breakdown |
| `ash.wait_timeline(interval, bucket)` | Wait events bucketed over time |
| `ash.samples_by_database(interval)` | Activity per database |
| `ash.activity_summary(interval)` | One-call overview: samples, peak backends, top waits/queries |
| `ash.status()` | Current sampling status and partition info |

### Absolute time (incident investigation)

| Function | Description |
|----------|-------------|
| `ash.top_waits_at(start, end, limit)` | Top waits in a specific time range |
| `ash.top_queries_at(start, end, limit)` | Top queries in a specific time range |
| `ash.query_waits_at(query_id, start, end)` | Query wait profile in a specific time range |
| `ash.cpu_vs_waiting_at(start, end)` | CPU vs waiting in a specific time range |
| `ash.wait_timeline_at(start, end, bucket)` | Wait timeline in a specific time range |

## How it works

1. **Sampler** (`ash.take_sample()`): reads `pg_stat_activity`, encodes all active backends into a single `integer[]` per database, inserts one row per tick.

2. **Encoding**: `[version, -wait_event_id, count, query_id, query_id, ..., -next_wait, count, ...]`. Dictionary tables map IDs to names. Compact — avg 326 bytes for 50 backends.

3. **Rotation**: PGQ-style 3-partition ring buffer. TRUNCATE the oldest partition — zero dead tuples, zero bloat, no VACUUM needed. Only 2 partitions hold data at any time.

4. **Readers**: inline SQL decode using `generate_subscripts()` — 9-17x faster than per-row plpgsql decoding.

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
| `take_sample()` overhead | 53 ms |
| WAL per sample | ~29 KiB (~2.4 GiB/day) |
| Rotation (1-day partition) | 9 ms |
| Dead tuples after rotation | 0 |

See [issue #1](https://github.com/NikolayS/pg_ash/issues/1) for full benchmarks — EXPLAIN ANALYZE, backend scaling, multi-database tests, and concurrency testing.

## Requirements

- Postgres 14+ (requires `query_id` in `pg_stat_activity`)
- pg_cron 1.5+ (for sub-minute scheduling)
- pg_stat_statements (optional, for query text in `top_queries_with_text()`)

## Known limitations

- **24-hour queries are slow** (~6s for full-day scan) — aggregate rollup tables planned for a future version.
- **JIT must be off for OLTP** — JIT adds 10x overhead to reader queries (30 ms to 340 ms).
- **Array building is O(n^2)** in plpgsql at high backend counts — switching to `array_agg()` is planned.

## License

[Apache 2.0](LICENSE)
