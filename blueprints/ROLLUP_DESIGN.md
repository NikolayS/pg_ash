# Rollup design for long-term storage

## Problem

Raw samples at 1s intervals: ~30 MiB/day. With 3-partition rotation (1-day default retention), we lose all history beyond ~2 days. For trend analysis ("is the system getting slower this month?"), we need aggregated long-term storage.

## Design

Two rollup levels, aggregated per database:

### Per-minute rollup (`ash.rollup_1m`)

| Column | Type | Description |
|--------|------|-------------|
| `ts` | `int4` | Minute-aligned sample_ts (60-second buckets) |
| `datid` | `oid` | Database OID |
| `samples` | `smallint` | Number of raw samples in this minute (normally 60) |
| `peak_backends` | `smallint` | Max active_count in this minute |
| `wait_counts` | `int4[]` | `[wait_id, count, wait_id, count, ...]` |
| `query_counts` | `int8[]` | `[query_id, count, query_id, count, ...]` |

**Retention**: 30 days (43,200 rows/db at 1-minute resolution).

**Note on `query_counts`**: Uses `int8[]` with raw `query_id` values (not `query_map_id` references). This eliminates the GC coordination problem — rollup rows are self-contained and don't depend on `query_map` entries surviving for the full rollup retention period.

### Per-hour rollup (`ash.rollup_1h`)

Same schema as `rollup_1m`, aggregated from minute rollups.

**Retention**: 5 years (43,800 rows/db/year at 1-hour resolution).

### Index and retention strategy

- B-tree index on `(ts, datid)` for both tables
- Retention via daily `DELETE WHERE ts < threshold` + autovacuum
- Trade-off: this generates dead tuples (unlike raw samples' zero-bloat TRUNCATE rotation). Acceptable because rollup tables are much smaller and autovacuum handles them easily. At ~322 MiB/year for `rollup_1m`, partition-based rotation would add complexity without meaningful benefit.

## Storage estimate

Assuming ~21 distinct active wait events per minute and ~50 distinct queries per minute:

**Minute rollup row size:**
- Tuple header: 23 bytes
- `ts` (int4): 4 bytes
- `datid` (oid): 4 bytes
- `samples` (smallint): 2 bytes
- `peak_backends` (smallint): 2 bytes
- `wait_counts` (int4[], 21 pairs): 20 (header) + 21 × 2 × 4 = **188 bytes**
- `query_counts` (int8[], 50 pairs): 20 (header) + 50 × 2 × 8 = **820 bytes**
- **Total: ~1,043 bytes/row**

**Hourly rollup row size** (~21 waits, top 100 queries):
- `wait_counts`: 188 bytes
- `query_counts` (int8[], 100 pairs): 20 + 100 × 2 × 8 = **1,620 bytes**
- **Total: ~1,843 bytes/row**

| Level | Rows/year/db | Avg row size | Storage/year/db |
|-------|-------------|-------------|----------------|
| 1-minute | 525,600 | ~1,043 bytes | ~523 MiB |
| 1-hour | 8,760 | ~1,843 bytes | ~15.4 MiB |

5 years of hourly data: ~77 MiB per database. Still negligible for any production system.

30 days of minute data: ~43 MiB per database (only 30 days retained, not full year).

## Count semantics

`count` in rollup arrays is the **sum of per-second backend counts** — i.e., **backend-seconds**. If a wait event was seen on 10 backends for each of 60 samples in a minute, its count = 600.

To convert to average backends: `count / samples`.

This is consistent with how Oracle ASH reports are computed and gives correct proportional breakdowns.

## Aggregation process

1. **Minute rollup** runs via pg_cron every minute:
   - Scans raw samples for the previous minute (per database)
   - Sums wait counts from decoded arrays into `[wait_id, total_count]` pairs
   - Collects `query_id` (from `query_map` lookup) with summed counts, sorted by count descending
   - All wait events kept; queries truncated to top 100
   - **Upsert** with `ON CONFLICT (ts, datid) DO UPDATE` for idempotency (handles pg_cron double-fires, late execution, or manual re-runs)

2. **Hourly rollup** runs via pg_cron every hour:
   - Scans `rollup_1m` rows for the previous hour (per database)
   - Merges wait_counts: sum counts for matching wait_ids
   - Merges query_counts: sum counts for matching query_ids, re-truncate to top 100
   - Upsert into `rollup_1h`

3. **Retention** runs daily:
   - `DELETE FROM ash.rollup_1m WHERE ts < now_ts - 30_days`
   - `DELETE FROM ash.rollup_1h WHERE ts < now_ts - 5_years`

## Array encoding for rollups

Simpler than raw samples — just pairs, sorted by count descending:

```
-- wait_counts: wait_event 5 seen 1200 backend-seconds, event 3 seen 800
{5, 1200, 3, 800}

-- query_counts: query_id 1234567890 seen 500 backend-seconds, 9876543210 seen 300
{1234567890, 500, 9876543210, 300}
```

No nesting, no negative markers. Wait events use `wait_event_map` ids (int4). Queries use raw `query_id` values (int8) — self-contained, no `query_map` dependency.

## Reader functions

```sql
-- Wait event trends over time (e.g., last 30 days from minute rollups)
ash.minute_waits(p_start timestamptz, p_end timestamptz, p_limit int DEFAULT 10)
RETURNS TABLE (wait_event text, samples bigint, pct numeric)

-- Query trends from hourly rollups (e.g., last 3 months)
ash.hourly_queries(p_start timestamptz, p_end timestamptz, p_limit int DEFAULT 10)
RETURNS TABLE (query_id bigint, samples bigint, pct numeric, query_text text)

-- Peak concurrency per day
ash.daily_peak_backends(p_start timestamptz, p_end timestamptz)
RETURNS TABLE (day date, peak_backends int, avg_backends numeric)
```

All readers resolve `wait_event_map` ids to names. Query text joined from `pg_stat_statements` when available. "Other" rollup row for top-N truncation (consistent with `top_waits` API).

These read directly from pre-aggregated arrays — sub-millisecond for any time range.

## Key decisions

1. **Rollup runs independently of rotation** — even if raw data is truncated, rollups persist.
2. **Simple DELETE for retention** — no partition complexity needed. Autovacuum handles dead tuples. Rollup tables are small enough that bloat is not a concern.
3. **Array encoding** is simpler than raw samples — just id/count pairs, no nesting.
4. **Top-N truncation** keeps rollup rows bounded — top 100 queries per hour. All wait events kept (bounded by Postgres source: ~600 max, ~21 active).
5. **Denormalized `query_id` in rollups** — `query_counts` stores raw `query_id` (int8), not `query_map_id`. Eliminates GC coordination: rollups don't depend on `query_map` entries surviving for years. Costs ~2× array size for queries but removes the hardest correctness problem.
6. **Per-database aggregation** — one rollup row per database per time bucket, matching the raw sample schema. Server-wide queries use `GROUP BY ts` across databases.
7. **Upsert for idempotency** — `ON CONFLICT (ts, datid) DO UPDATE` handles double-fires, late execution, and manual re-runs. No watermark tracking needed.
8. **Backend-seconds as the count unit** — consistent with Oracle ASH, gives correct proportional breakdowns, allows conversion to average backends via `count / samples`.
