# pg_ash

**Active Session History for Postgres** — lightweight wait event sampling with zero bloat.

Pure SQL/plpgsql. No C extension, no `shared_preload_libraries` changes. Just `CREATE EXTENSION` and go.

## Why

Postgres has no built-in session history. When something was slow an hour ago, you have nothing to look at. pg_ash fixes that by sampling `pg_stat_activity` every second and storing the results in a compact format you can query with plain SQL.

### How it compares

| | pg_ash | pg_wait_sampling | pgsentinel |
|---|---|---|---|
| Install | `CREATE EXTENSION` | shared_preload_libraries | Compile + shared_preload_libraries |
| Storage | Disk (~37 MiB/day) | Memory only | Memory only |
| Historical queries | ✅ Persistent | ❌ Ring buffer | ❌ Ring buffer |
| Pure SQL | ✅ | ❌ C extension | ❌ C extension |
| Requires | pg_cron ≥ 1.5 | — | — |

## Quick start

```sql
-- Install
CREATE EXTENSION pg_ash;

-- Start sampling (1 sample/second via pg_cron)
SELECT ash.start('1 second');

-- Wait a few minutes, then query
SELECT * FROM ash.top_waits('1 hour');
SELECT * FROM ash.top_queries('1 hour');
SELECT * FROM ash.cpu_vs_waiting('1 hour');

-- Stop sampling
SELECT ash.stop();

-- Uninstall (removes all data)
SELECT ash.uninstall();
```

## Reader functions

| Function | Description |
|----------|-------------|
| `ash.top_waits(interval, limit)` | Top wait events by sample count |
| `ash.top_queries(interval, limit)` | Top queries by sample count |
| `ash.cpu_vs_waiting(interval)` | CPU vs waiting breakdown |
| `ash.wait_timeline(interval, bucket)` | Wait events bucketed over time |
| `ash.samples_by_database(interval)` | Activity per database |
| `ash.status()` | Current sampling status and partition info |

## How it works

1. **Sampler** (`ash.take_sample()`): Reads `pg_stat_activity`, encodes all active backends into a single `integer[]` per database, inserts one row per tick.

2. **Encoding**: `[version, -wait_event_id, count, query_id, query_id, ..., -next_wait, count, ...]`. Dictionary tables map IDs to names. Compact — avg 326 bytes for 50 backends.

3. **Rotation**: PGQ-style 3-partition ring buffer. TRUNCATE the oldest partition — zero dead tuples, zero bloat, no VACUUM needed. Only 2 partitions hold data at any time.

4. **Readers**: Inline SQL decode using `generate_subscripts()` — 9–17× faster than per-row plpgsql decoding.

## Storage

| Backends | Storage/day | Max on disk (2 partitions) |
|----------|------------|---------------------------|
| 10 | 11 MiB | 22 MiB |
| 50 | 30 MiB | 60 MiB |
| 100 | 50 MiB | 100 MiB |
| 200 | 100 MiB | 200 MiB |
| 500 | 245 MiB | 490 MiB |

At 500+ backends, TOAST LZ4 compression kicks in and reduces actual storage.

## Performance

Measured on Postgres 17, 50 backends, 1s sampling (median of 10 runs, warm cache):

| Metric | Result |
|--------|--------|
| `top_waits('1 hour')` | 339 ms |
| `top_waits('24 hours')` | 6.8 s |
| `take_sample()` overhead | 53 ms |
| WAL per sample | ~31 KiB (~3.7 GiB/day) |
| Rotation (1-day partition) | 9 ms |
| Dead tuples after rotation | 0 |

See [issue #1](https://github.com/NikolayS/pg_ash/issues/1) for full benchmark results including EXPLAIN ANALYZE, backend scaling, multi-database tests, and concurrency testing.

## Requirements

- Postgres 14+ (requires `query_id` in `pg_stat_activity`)
- pg_cron 1.5+ (for sub-minute scheduling)

## Known limitations

- **24h queries are slow** (~7s for full-day scan). Aggregate rollup tables would fix this — planned for a future version.
- **JIT compilation overhead**: JIT adds ~1.5s to reader queries. Consider `SET jit = off` for interactive use.
- **Array building is O(n²)** in plpgsql at high backend counts. Switching to `array_agg()` would fix this.

## License

[Apache 2.0](LICENSE)
