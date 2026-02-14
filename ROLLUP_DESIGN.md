# Rollup design for long-term storage

## Problem

Raw samples at 1s intervals: ~30 MiB/day, ~11 GiB/year. With 3-partition rotation (1-day retention), we lose all history beyond ~2 days. For trend analysis ("is the system getting slower this month?"), we need aggregated long-term storage.

## Design

Two rollup levels:

### Per-minute rollup (`ash.rollup_1m`)

| Column | Type | Description |
|--------|------|-------------|
| `ts` | `int4` | Minute-aligned sample_ts (60-second buckets) |
| `datid` | `oid` | Database OID |
| `samples` | `smallint` | Number of raw samples in this minute (normally 60) |
| `peak_backends` | `smallint` | Max active_count in this minute |
| `wait_counts` | `int4[]` | `[wait_id, count, wait_id, count, ...]` |
| `query_counts` | `int4[]` | `[query_map_id, count, query_map_id, count, ...]` |

**Retention**: 30 days (43,200 rows/db at 1-minute resolution).

### Per-hour rollup (`ash.rollup_1h`)

Same schema as `rollup_1m`, aggregated from minute rollups.

**Retention**: 5 years (43,800 rows/db/year at 1-hour resolution).

## Storage estimate

Assuming 200 distinct wait events and 500 distinct queries per hour:

| Level | Rows/year/db | Avg row size | Storage/year/db |
|-------|-------------|-------------|----------------|
| 1-minute | 525,600 | ~400 bytes | ~200 MiB |
| 1-hour | 8,760 | ~800 bytes | ~7 MiB |

5 years of hourly data: ~35 MiB per database. Negligible.

## Aggregation process

1. **Minute rollup** runs via pg_cron every minute:
   - Scans raw samples from the last minute
   - Aggregates wait counts and query counts into arrays
   - Inserts into `rollup_1m`

2. **Hourly rollup** runs via pg_cron every hour:
   - Scans `rollup_1m` rows from the last hour
   - Merges wait_counts and query_counts arrays
   - Inserts into `rollup_1h`

3. **Minute retention** runs daily:
   - Deletes `rollup_1m` rows older than 30 days

## Array encoding for rollups

Simpler than raw samples — just pairs: `[id, count, id, count, ...]`

```sql
-- wait_counts example: 
-- wait_event 5 seen 1200 times, wait_event 3 seen 800 times
{5, 1200, 3, 800}

-- query_counts example:
-- query_map_id 42 seen 500 times, query_map_id 17 seen 300 times
{42, 500, 17, 300}
```

Sorted by count descending. Top-N truncation at rollup time (keep top 100 queries per hour, all wait events).

## Reader functions

| Function | Source | Description |
|----------|--------|-------------|
| `ash.hourly_waits(start, end)` | `rollup_1h` | Wait trends over days/weeks/months |
| `ash.hourly_queries(start, end)` | `rollup_1h` | Query trends over days/weeks/months |
| `ash.daily_peak_backends(start, end)` | `rollup_1h` | Peak concurrency per day |

These read directly from pre-aggregated arrays — no array decoding of raw samples. Sub-millisecond for any time range.

## Key decisions

1. **Rollup runs independently of rotation** — even if raw data is truncated, rollups persist.
2. **No separate rollup partitions needed** — 35 MiB/year doesn't justify partition complexity. Simple DELETE for retention.
3. **Array encoding** is simpler than raw samples — just id/count pairs, no version byte, no nesting.
4. **Top-N truncation** keeps rollup rows bounded — storing every rare query for 5 years wastes space.
5. **query_map must not GC aggressively** — if a query_map entry is deleted but still referenced in rollups, the rollup reader will lose the query_id mapping. The `last_seen` column should consider rollup references.
