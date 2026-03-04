# Configurable N partitions + rollup tables for long-term storage

## Summary

Two related enhancements:

1. **Configurable partition count** — generalize the hardcoded 3-partition ring buffer to N partitions, enabling multi-day raw sample retention (e.g., 7 partitions = 6 days of raw data + 1 empty "next" slot).

2. **Rollup tables with percentile statistics** — pre-aggregated 1-minute and 1-hour rollup tables for long-term trend analysis and root cause analysis (RCA), retaining min/max/percentile data critical for troubleshooting.

---

## Part 1: Configurable N Partitions

### Current state

The partition count is hardcoded to 3 everywhere:
- 3 physical partitions: `sample_0`, `sample_1`, `sample_2`
- 3 query map tables: `query_map_0`, `query_map_1`, `query_map_2`
- `query_map_all` view hardcodes 3 `UNION ALL` branches
- `rotate()` uses `% 3` arithmetic
- `_active_slots()` returns `array[current, (current - 1 + 3) % 3]`
- `take_sample()` has 3-way `IF/ELSIF/ELSE` for query_map inserts

This gives exactly 2 days of retention (current + previous partition, each holding 1 `rotation_period` of data).

### Goal

Add a `num_partitions` config parameter (default: 3, min: 3, max: 32) that controls:
- How many physical partitions exist
- How many are "active" (readable) — all except the one empty "next" slot
- Rotation arithmetic: `% N` instead of `% 3`

**Retention formula:** `(num_partitions - 1) × rotation_period`
- 3 partitions, 1-day rotation → 2 days (current default)
- 7 partitions, 1-day rotation → 6 days
- 4 partitions, 6-hour rotation → 18 hours (high-resolution, shorter window)

### Design

#### Config change

```sql
ALTER TABLE ash.config ADD COLUMN num_partitions smallint NOT NULL DEFAULT 3;
ALTER TABLE ash.config ADD CONSTRAINT num_partitions_range
  CHECK (num_partitions >= 3 AND num_partitions <= 32);
```

#### Partition management

**Approach: dynamic DDL via `ash.set_partitions(N)`**

A new function `ash.set_partitions(N)` handles growing/shrinking:

- **Growing (3 → 7):** Creates `sample_3..6`, `query_map_3..6`, indexes, rebuilds `query_map_all` view
- **Shrinking (7 → 3):** Only allowed if partitions being removed are empty (the "next" slots). Truncates and drops them, rebuilds view
- Updates `ash.config.num_partitions`

This avoids pre-creating 32 empty partitions and keeps the schema clean.

#### Key code changes

| Location | Current | New |
|----------|---------|-----|
| `rotate()` slot arithmetic | `% 3` | `% num_partitions` (read from config) |
| `rotate()` TRUNCATE dispatch | 3-way IF | `EXECUTE format('TRUNCATE ash.sample_%s', slot)` |
| `_active_slots()` | Returns 2-element array | Returns `(N-1)`-element array (all except "next") |
| `take_sample()` query_map insert | 3-way IF/ELSIF/ELSE | `EXECUTE format('INSERT INTO ash.query_map_%s ...', slot)` |
| `query_map_all` view | 3 hardcoded UNIONs | Rebuilt dynamically by `set_partitions()` |
| Reader functions | `slot = any(_active_slots())` | No change needed (already uses `_active_slots()`) |

#### Migration path

- Default remains 3 partitions — zero behavior change for existing installs
- `ash.set_partitions(7)` can be called at any time (creates new partitions, updates config)
- `ash.start()` / `ash.stop()` unaffected — they don't care about partition count
- Upgrade script from 1.2: add column + constraint, no partition changes

---

## Part 2: Rollup Tables for Long-Term Storage

### Problem

Raw samples at 1s intervals give ~30 MiB/day. With partition rotation, all history beyond `(N-1) × rotation_period` is lost. For trend analysis and post-incident RCA, we need aggregated long-term storage.

The existing rollup design (ROLLUP_DESIGN.md) stores only **sums** (backend-seconds) per wait event and query. This is sufficient for "what was the average load?" but loses critical information for troubleshooting.

### What's needed for RCA

When investigating incidents, the key questions are:

| Question | Required statistic | Why averages fail |
|----------|--------------------|-------------------|
| "Was it a spike or sustained?" | **min, max, percentiles** of active_count | avg=10 could be steady 10 or a 5-min burst of 120 |
| "What specifically caused the spike?" | **per-wait-event max** (peak backends per wait type) | Sum doesn't tell you which wait event peaked |
| "How bad was the worst moment?" | **p99** of active_count | Outlier detection — distinguish routine from catastrophic |
| "Is it getting worse over time?" | **p50 trend** over days/weeks | Median is more robust than mean for trend detection |
| "Was the problem locks, IO, or CPU?" | **per-wait peak + sum** | Need both: sum for proportion, max for severity |

### Recommended design: percentile-enhanced rollups

#### Minute rollup: `ash.rollup_1m`

```sql
CREATE TABLE ash.rollup_1m (
  ts               int4       NOT NULL,  -- minute-aligned sample_ts
  datid            oid        NOT NULL,
  samples          smallint   NOT NULL,  -- raw samples in this minute (normally 60)
  -- Overall active backend distribution
  active_min       smallint   NOT NULL,  -- min(active_count) across samples
  active_max       smallint   NOT NULL,  -- max(active_count) across samples
  active_p50       smallint   NOT NULL,  -- median active_count
  active_p90       smallint   NOT NULL,  -- 90th percentile
  active_p99       smallint   NOT NULL,  -- 99th percentile (worst non-outlier)
  -- Per-wait-event breakdown: [wait_id, sum, max, wait_id, sum, max, ...]
  wait_counts      int4[]     NOT NULL,
  -- Per-query breakdown: [query_id, sum, query_id, sum, ...] (int8 pairs)
  query_counts     int8[]     NOT NULL,

  CONSTRAINT rollup_1m_pk PRIMARY KEY (ts, datid)
);

CREATE INDEX ON ash.rollup_1m (ts);
```

**`wait_counts` encoding** — triplets instead of pairs:
```
[wait_id, sum_backend_seconds, max_in_single_sample, wait_id, sum, max, ...]
```

The `max_in_single_sample` field answers "how many backends were simultaneously in this wait state at the peak moment?" — essential for diagnosing lock storms, IO saturation, etc.

**`query_counts` encoding** — pairs (unchanged from original design):
```
[query_id_hi32, query_id_lo32, sum, ...]   -- or int8[] pairs: [query_id, sum, ...]
```

Per-query peak isn't useful (a query with max=1 running 60 times is the same concern as max=1 running once). Sum is what matters.

**Retention:** 30 days. ~45 MiB/database.

#### Hourly rollup: `ash.rollup_1h`

```sql
CREATE TABLE ash.rollup_1h (
  ts               int4       NOT NULL,  -- hour-aligned sample_ts
  datid            oid        NOT NULL,
  samples          smallint   NOT NULL,  -- raw samples in this hour (normally 3600)
  active_min       smallint   NOT NULL,
  active_max       smallint   NOT NULL,
  active_p50       smallint   NOT NULL,
  active_p90       smallint   NOT NULL,
  active_p99       smallint   NOT NULL,
  wait_counts      int4[]     NOT NULL,  -- [wait_id, sum, max, ...] triplets
  query_counts     int8[]     NOT NULL,  -- [query_id, sum, ...] pairs, top 200

  CONSTRAINT rollup_1h_pk PRIMARY KEY (ts, datid)
);

CREATE INDEX ON ash.rollup_1h (ts);
```

**Retention:** 5 years. ~16 MiB/database/year.

#### Percentile computation strategy

**1-minute rollup** (from raw samples):
- Runs every minute via pg_cron
- Reads 60 raw samples from `ash.sample`
- `percentile_cont(array[0.5, 0.9, 0.99]) WITHIN GROUP (ORDER BY active_count)` — exact percentiles over 60 values
- Per-wait-event `sum()` and `max()` from decoded arrays
- Top 200 queries by sum, with raw `query_id` (no query_map dependency)

**1-hour rollup** (from raw samples, NOT from minute rollups):
- Runs every hour via pg_cron
- Reads 3600 raw samples directly from `ash.sample` — still available since raw retention ≥ 1 day
- Exact percentiles via `percentile_cont()` over 3600 values
- This avoids the mathematical problem of computing percentiles from percentiles (which is incorrect)
- The minute rollup is NOT an intermediate step for the hourly — both read raw data independently

**Why not compute hourly from minute rollups?** You can't derive exact p50/p90/p99 from 60 per-minute p50/p90/p99 values. The median of medians ≠ the overall median. Computing from raw samples is simple, correct, and feasible (raw data is always available when the hourly job runs).

#### Retention management

```sql
-- Runs daily via pg_cron (added to ash.start())
DELETE FROM ash.rollup_1m WHERE ts < extract(epoch FROM now() - ash.epoch())::int4 - (30 * 86400);
DELETE FROM ash.rollup_1h WHERE ts < extract(epoch FROM now() - ash.epoch())::int4 - (5 * 365 * 86400);
```

Simple DELETE + autovacuum. Rollup tables are small enough that dead tuple bloat is negligible.

#### Reader functions

```sql
-- Wait event trends over 30 days (from minute rollups)
ash.rollup_waits(p_interval interval DEFAULT '7 days', p_limit int DEFAULT 10)
-- Returns: wait_event, total_backend_seconds, peak_concurrent, pct

-- Hourly load profile with percentile bands
ash.rollup_load_profile(p_interval interval DEFAULT '24 hours', p_bucket interval DEFAULT '1 hour')
-- Returns: bucket_start, active_min, active_p50, active_p90, active_p99, active_max

-- Long-term query trends (from hourly rollups)
ash.rollup_queries(p_interval interval DEFAULT '30 days', p_limit int DEFAULT 10)
-- Returns: query_id, total_backend_seconds, pct, query_text

-- Compare two time periods (RCA: "what changed?")
ash.rollup_compare(p_baseline_start, p_baseline_end, p_incident_start, p_incident_end)
-- Returns: wait_event, baseline_pct, incident_pct, change_pct

-- Daily peak report
ash.rollup_daily_peaks(p_interval interval DEFAULT '30 days')
-- Returns: day, peak_backends, p99_backends, p50_backends
```

The `rollup_compare()` function is the RCA workhorse — it shows exactly which wait events increased between a known-good baseline period and the incident period.

### Why this design is optimal for RCA

1. **Spike detection:** `active_max` vs `active_p50` divergence immediately reveals spiky vs sustained load. A ratio > 5× is a red flag.

2. **Wait event severity:** Per-wait `max` tells you "Lock waits peaked at 80 concurrent backends" even if the average was 3. Sum alone would show locks as 5% of total — masking that one catastrophic 30-second lock storm.

3. **Trend analysis:** p50 over weeks/months is more robust than mean (resistant to outliers). "p50 active backends grew from 5 to 15 over 3 months" = organic growth. "p50 stable at 5 but p99 jumped from 10 to 100" = new intermittent problem.

4. **Incident comparison:** `rollup_compare()` with a baseline period instantly shows "IO:DataFileRead went from 12% to 68% of samples" — the kind of signal that would take an hour to find manually.

5. **Compact storage:** Percentile columns add only 10 bytes/row (5 × smallint). Wait event triplets add ~33% over pairs. Total overhead is minimal compared to the diagnostic value gained.

### Alternatives considered

| Approach | Pros | Cons | Verdict |
|----------|------|------|---------|
| **Sum only** (original design) | Simplest | No distribution info, can't detect spikes | Insufficient for RCA |
| **Min/max only** | Simple, small | No distribution shape — can't tell "always high" from "one spike" | Incomplete |
| **Full histogram buckets** | Rich distribution | Complex to query, arbitrary bucket boundaries, larger storage | Over-engineered |
| **t-digest / DDSketch** | Mergeable percentiles | Requires C extension or complex PL/pgSQL, opaque blob storage | Against pg_ash philosophy |
| **Percentiles + per-wait max** (chosen) | Answers all key RCA questions, SQL-native, compact | Hourly must read raw samples (not minute rollups) | Best trade-off |

---

## Implementation plan

### Phase 1: Configurable partitions
1. Add `num_partitions` to `ash.config` (default 3)
2. Implement `ash.set_partitions(N)` — creates/drops partition tables + indexes, rebuilds `query_map_all` view
3. Refactor `rotate()` to use `num_partitions` and dynamic SQL
4. Refactor `take_sample()` query_map insert to use dynamic SQL
5. Refactor `_active_slots()` to return `(N-1)` active slots
6. Write upgrade script (1.2 → 1.3)
7. Tests: verify rotation with 3, 5, 7 partitions; verify set_partitions grow/shrink

### Phase 2: Rollup tables
1. Create `ash.rollup_1m` and `ash.rollup_1h` tables with percentile columns
2. Implement `ash.rollup_minute()` — aggregate raw samples into 1-min rollup with `percentile_cont()`
3. Implement `ash.rollup_hour()` — aggregate raw samples into 1-hour rollup
4. Implement `ash.rollup_retain()` — DELETE expired rows
5. Add pg_cron jobs in `ash.start()`: minute rollup, hourly rollup, daily retention
6. Implement reader functions: `rollup_waits()`, `rollup_load_profile()`, `rollup_queries()`, `rollup_compare()`, `rollup_daily_peaks()`
7. Update `ash.stop()` and `ash.uninstall()` to handle rollup jobs
8. Tests: verify rollup accuracy, retention, idempotency (upsert on re-run)

### Phase 3: Documentation + upgrade
1. Update SPEC.md and ROLLUP_DESIGN.md
2. Write 1.2-to-1.3 upgrade script
3. Update README with rollup examples
4. Update `ash.status()` to show rollup statistics

---

## Storage estimates

### Configurable partitions (raw samples)

| Partitions | rotation_period | Retention | Storage (50 backends) |
|------------|----------------|-----------|-----------------------|
| 3 (default) | 1 day | 2 days | ~66 MiB |
| 4 | 1 day | 3 days | ~99 MiB |
| 7 | 1 day | 6 days | ~198 MiB |
| 8 | 6 hours | 42 hours | ~58 MiB |

### Rollup tables (per database)

| Table | Row size | Rows/month | Storage/month | Retention |
|-------|----------|------------|---------------|-----------|
| `rollup_1m` | ~1,100 bytes | 43,200 | ~45 MiB | 30 days |
| `rollup_1h` | ~1,900 bytes | 720 | ~1.3 MiB | 5 years |

5 years of hourly data: ~78 MiB per database. Negligible.
