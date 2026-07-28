# Rollup design for long-term storage

> **Historical design record (non-normative).** This file documents a pre-2.0
> proposal and is retained only for design history. Its schemas, function
> names, argument names, operational commands, performance figures, and
> implementation status may not match the shipped release. Do not use it as an
> install, upgrade, or API guide. Use `README.md`, `RELEASE_NOTES.md`,
> `AAS_API.md`, and the catalog comments in `sql/ash-install.sql` for current
> behavior.

## Problem

The original proposal assumed raw samples at one-second intervals and estimated
about 30 MiB/day. The shipped three-partition, one-day rotation contract
guarantees roughly one day plus the current partial period, not two complete
days. Rollups provide the longer retained history.

## Design

Two rollup levels, aggregated per database:

### Per-minute rollup (`ash.rollup_1m`)

| Column | Type | Description |
|--------|------|-------------|
| `ts` | `int4` | Minute-aligned sample_ts (60-second buckets) |
| `datid` | `oid` | Database OID |
| `samples` | `smallint` | Number of activity-bearing raw rows contributing; not successful sampler ticks |
| `peak_backends` | `smallint` | Max active_count in this minute |
| `wait_counts` | `int4[]` | `[wait_id, count, wait_id, count, ...]` |
| `query_counts` | `int8[]` | `[query_id, count, query_id, count, ...]` |

**Retention**: 30 days (up to 43,200 activity-bearing rows per database at
1-minute resolution).

**Note on `query_counts`**: Uses `int8[]` with raw `query_id` values (not `query_map_id` references). This eliminates the GC coordination problem — rollup rows are self-contained and don't depend on `query_map` entries surviving for the full rollup retention period.

### Per-hour rollup (`ash.rollup_1h`)

The hourly row carries the same aggregate columns as `rollup_1m`, plus
`minute_counts int4[]`: 60 per-minute total-activity slots used to preserve
valid minute-grain extrema. A NULL slot means no stored observation, which can
be idle time or missing coverage. Legacy/incomplete arrays are disclosed at
hour grain rather than expanded into synthetic minutes.

**Retention**: 5 years (8,760 rows/db/year, or approximately 43,800 rows over
five years, at 1-hour resolution).

### Index and retention strategy

- B-tree index on `(ts, datid)` for both tables
- Retention via daily `DELETE WHERE ts < threshold` + autovacuum
- Trade-off: this generates dead tuples, unlike raw samples' TRUNCATE rotation.
  The original proposal expected autovacuum to keep up; operators must verify
  that assumption and actual table size on their workload.

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

The historical fixture estimated about 77 MiB per database for five years of
hourly data. Actual storage depends on database count and retained array
cardinality; measure the target workload.

At this historical fixture's assumed row size and one activity-bearing row in
every minute, 30 days of minute data was estimated at about 43 MiB per
database. Actual storage is workload-dependent.

## Count semantics

`count` in rollup arrays is the sum of retained backend appearances. It equals
backend-seconds only while the sampling interval remains one second. For
example, if a wait event is retained on 10 backends in each of 60 one-second
samples, its count is 600.

To compute AAS for a wall-clock window, multiply retained appearances by the
current configured sample interval and divide by the window duration. Because
historical cadence is not persisted (#137), changing that interval rescales
retained history; the stored count alone cannot prove elapsed backend-seconds.

## Aggregation process

1. **Minute rollup** runs every minute via pg_cron or an equivalent external
   scheduler:
   - Processes completed, unprocessed minute grains up to `batch_limit`,
     scanning raw rows per grain and advancing the watermark even when a grain
     is empty
   - Sums wait counts from decoded arrays into `[wait_id, total_count]` pairs
   - Collects `query_id` (from `query_map` lookup) with summed counts, sorted by count descending
   - All wait events kept; queries truncated to top 100
   - **Upsert** with `ON CONFLICT (ts, datid) DO UPDATE` for idempotency (handles pg_cron double-fires, late execution, or manual re-runs)

2. **Hourly rollup** runs hourly through the configured scheduler:
   - Processes up to 24 completed, unprocessed hour grains, scanning
     `rollup_1m` rows per grain and advancing the watermark even when a grain
     is empty
   - Merges wait_counts: sum counts for matching wait_ids
   - Merges query_counts: sum counts for matching query_ids, re-truncate to top 100
   - Upsert into `rollup_1h`

3. **Retention** runs daily:
   - `DELETE FROM ash.rollup_1m WHERE ts < now_ts - 30_days`
   - `DELETE FROM ash.rollup_1h WHERE ts < now_ts - 5_years`

## Array encoding for rollups

Simpler than raw samples — just pairs, sorted by count descending:

```
-- wait_counts: wait_event 5 has 1200 stored appearances, event 3 has 800
{5, 1200, 3, 800}

-- query_counts: query_id 1234567890 has 500 stored appearances, 9876543210 has 300
{1234567890, 500, 9876543210, 300}
```

No nesting, no negative markers. Wait events use `wait_event_map` ids (int4). Queries use raw `query_id` values (int8) — self-contained, no `query_map` dependency.

## Reader functions

The reader signatures proposed here (`minute_waits`, `hourly_queries`, and
`daily_peak_backends`) were never the 2.0 surface. Current readers are
`ash.periods`, `ash.aas`, `ash.timeline`, `ash.top`, `ash.compare`,
`ash.samples`, `ash.report`, `ash.chart`, and `ash.summary`; aggregate readers
select raw, minute-rollup, or hourly-rollup storage as documented in
`AAS_API.md`.

Performance is hardware-, data-, retention-, and workload-dependent. Direct
rollup reads are intended to make wide windows economical, but no portable
latency guarantee is part of the 2.0 contract.

## Key decisions

1. **Rollup runs independently of rotation** — even if raw data is truncated, rollups persist.
2. **Simple DELETE for retention** — rollup tables are unpartitioned; retention
   uses `DELETE` plus autovacuum, whose table-size and bloat behavior must be
   monitored on the target workload.
3. **Array encoding** is simpler than raw samples — just id/count pairs, no nesting.
4. **Top-N truncation** keeps rollup rows bounded — top 100 queries per hour. All wait events kept (bounded by Postgres source: ~600 max, ~21 active).
5. **Denormalized `query_id` in rollups** — `query_counts` stores raw `query_id` (int8), not `query_map_id`. Eliminates GC coordination: rollups don't depend on `query_map` entries surviving for years. Costs ~2× array size for queries but removes the hardest correctness problem.
6. **Per-database aggregation** — one rollup row per database per time bucket, matching the raw sample schema. Server-wide queries use `GROUP BY ts` across databases.
7. **Transactional watermarks plus upsert** — minute/hour workers advance
   watermarks across completed grains, including empty grains; per-database
   data rows use `ON CONFLICT (ts, datid) DO UPDATE`.
8. **Stored appearances as the count unit** — counts equal backend-seconds
   only at an unchanged one-second cadence. Current-interval AAS weighting and
   its #137 limitation must remain explicit.
