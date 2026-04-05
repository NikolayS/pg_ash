# Configurable N-partitions + Rollup tables

## Overview

Two features for pg_ash v1.5:

1. **Configurable N partitions** — replace hardcoded 3-partition ring buffer with user-configurable N (default 3, range 3–32). Modeled after pg-flight-recorder's `ring_buffer_slots` approach.
2. **Rollup tables for long-term storage** — minute and hourly aggregates surviving rotation, per the existing `ROLLUP_DESIGN.md`.

Both share a dependency: rotation logic must become dynamic before rollups can reference partition boundaries correctly.

---

## Part 1: Configurable N partitions

### Problem

pg_ash hardcodes 3 partitions (`sample_0/1/2`, `query_map_0/1/2`) with `% 3` modular arithmetic and a `CASE` statement in `rotate()`. This means:

- Default retention = 1 rotation period (the third partition is being truncated)
- Users who want longer raw sample retention (e.g., 7 days at 1-day rotation = 7 partitions) must edit SQL source
- The `CASE` block in `rotate()` doesn't scale

### Design

#### Config

Add `num_partitions` column to `ash.config`:

```sql
alter table ash.config
  add column num_partitions smallint not null default 3;
```

Valid range: 3–32. Minimum 3 because the ring buffer needs current + previous + one being truncated. Maximum 32 is practical — beyond that, the `query_map_all` UNION ALL view becomes unwieldy and planner overhead grows.

Constraint:

```sql
alter table ash.config
  add constraint config_num_partitions_check
  check (num_partitions between 3 and 32);
```

#### Partition naming

Keep the existing `sample_N` / `query_map_N` naming convention:

```
ash.sample_0, ash.sample_1, ..., ash.sample_{N-1}
ash.query_map_0, ash.query_map_1, ..., ash.query_map_{N-1}
```

#### Install-time creation

Fresh install creates N partitions (default 3) using a `DO` block with `generate_series`:

```sql
do $$
declare
  v_n int;
begin
  select num_partitions into v_n from ash.config where singleton;
  for i in 0..v_n-1 loop
    execute format(
      'create table if not exists ash.sample_%s '
      'partition of ash.sample for values in (%s)', i, i
    );
    execute format(
      'create table if not exists ash.query_map_%s ('
      '  id int4 generated always as identity,'
      '  query_id int8 not null,'
      '  unique (query_id)'
      ')', i
    );
    -- create indexes on sample_N
    execute format(
      'create index if not exists sample_%s_ts_idx '
      'on ash.sample_%s (sample_ts desc)', i, i
    );
  end loop;
end $$;
```

#### `query_map_all` view

Dynamic view recreation via function, called at install and after `rebuild_partitions()`:

```sql
create or replace function ash._rebuild_query_map_view()
returns void
language plpgsql
as $$
declare
  v_n int;
  v_sql text := '';
begin
  select num_partitions into v_n from ash.config where singleton;
  for i in 0..v_n-1 loop
    if i > 0 then v_sql := v_sql || ' union all '; end if;
    v_sql := v_sql || format(
      'select %s::smallint as slot, id, query_id from ash.query_map_%s', i, i
    );
  end loop;
  execute 'create or replace view ash.query_map_all as ' || v_sql;
end $$;
```

#### `current_slot()` — unchanged

Still reads from `ash.config.current_slot`. No change needed.

#### `take_sample()` — dynamic routing

Replace the 3-way `IF` for query_map inserts with dynamic SQL:

```sql
-- current approach (hardcoded):
if v_current_slot = 0 then
  insert into ash.query_map_0 ...
elsif v_current_slot = 1 then
  insert into ash.query_map_1 ...
else
  insert into ash.query_map_2 ...
end if;

-- new approach (dynamic):
execute format(
  'insert into ash.query_map_%s (query_id) '
  'select distinct sa.query_id '
  'from unnest($1) as sa(query_id) '
  'where sa.query_id <> 0 '
  'on conflict (query_id) do nothing',
  v_current_slot
) using v_query_ids;

-- with 50k cap check:
execute format(
  'select pg_catalog.pg_relation_size(oid) '
  'from pg_catalog.pg_class '
  'where oid = %L::regclass',
  'ash.query_map_' || v_current_slot
) into v_qmap_size;
```

**Performance note**: Dynamic SQL adds ~0.1ms overhead per `take_sample()` call. At 1s sampling interval, this is negligible (<0.01% overhead). The plan cache won't help with partition routing anyway since the slot changes.

#### `rotate()` — dynamic truncation

Replace the `CASE` statement with dynamic SQL:

```sql
-- current approach:
case v_truncate_slot
  when 0 then truncate ash.sample_0; truncate ash.query_map_0; ...
  when 1 then ...
  when 2 then ...
end case;

-- new approach:
v_num_partitions := (select num_partitions from ash.config where singleton);
v_truncate_slot := (v_new_slot + 1) % v_num_partitions;

execute format('truncate ash.sample_%s', v_truncate_slot);
execute format('truncate ash.query_map_%s', v_truncate_slot);
execute format(
  'alter table ash.query_map_%s alter column id restart', v_truncate_slot
);
```

Modular arithmetic changes from `% 3` to `% v_num_partitions` everywhere.

#### `rebuild_partitions(p_num int DEFAULT NULL)`

New admin function (modeled after pg-flight-recorder's `rebuild_ring_buffers`):

```sql
create or replace function ash.rebuild_partitions(p_num int default null)
returns text
language plpgsql
as $$
declare
  v_old_n int;
  v_new_n int;
begin
  select num_partitions into v_old_n from ash.config where singleton;
  v_new_n := coalesce(p_num, v_old_n);

  if v_new_n < 3 or v_new_n > 32 then
    raise exception 'num_partitions must be between 3 and 32, got: %', v_new_n;
  end if;

  if v_new_n = v_old_n then
    return format('already at %s partitions, no rebuild needed', v_old_n);
  end if;

  -- Stop sampling first (if pg_cron is available)
  if ash._pg_cron_available() then
    perform ash.stop();
  end if;

  -- Drop ALL existing sample partitions and query_maps
  for i in 0..v_old_n-1 loop
    execute format('drop table if exists ash.sample_%s', i);
    execute format('drop table if exists ash.query_map_%s', i);
  end loop;

  -- Update config
  update ash.config
  set num_partitions = v_new_n,
      current_slot = 0,
      rotated_at = now()
  where singleton;

  -- Create new partitions
  for i in 0..v_new_n-1 loop
    execute format(
      'create table ash.sample_%s partition of ash.sample for values in (%s)', i, i
    );
    execute format(
      'create table ash.query_map_%s ('
      '  id int4 generated always as identity,'
      '  query_id int8 not null,'
      '  unique (query_id))', i
    );
    -- indexes on sample_N
    execute format(
      'create index sample_%s_ts_idx on ash.sample_%s (sample_ts desc)', i, i
    );
  end loop;

  -- Rebuild the query_map_all view
  perform ash._rebuild_query_map_view();

  return format('rebuilt: %s -> %s partitions. all data cleared. restart sampling.', v_old_n, v_new_n);
end $$;
```

**WARNING**: This is destructive — all raw sample data is lost. Rollup tables (Part 2) survive.

REVOKE from PUBLIC (admin-only, like `rotate`, `start`, `stop`).

#### `status()` — add num_partitions metric

```sql
metric := 'num_partitions'; value := v_config.num_partitions::text; return next;
```

#### `uninstall()` — dynamic drop

Replace hardcoded drops with loop:

```sql
for i in 0..v_n-1 loop
  execute format('drop table if exists ash.sample_%s', i);
  execute format('drop table if exists ash.query_map_%s', i);
end loop;
```

#### Upgrade path (1.4 → 1.5)

```sql
-- Add num_partitions column (default 3 = backwards-compatible)
do $$
begin
  if not exists (
    select from information_schema.columns
    where table_schema = 'ash' and table_name = 'config'
      and column_name = 'num_partitions'
  ) then
    alter table ash.config
      add column num_partitions smallint not null default 3;
    alter table ash.config
      add constraint config_num_partitions_check
      check (num_partitions between 3 and 32);
  end if;
end $$;
```

Existing installations keep 3 partitions. `rebuild_partitions(N)` to change.

#### Retention semantics

With N partitions and rotation_period P:

- **Active data**: partitions `current_slot` (writing) and `current_slot - 1` (just completed)
- **Readable data**: N-1 partitions (all except the one being truncated next)
- **Total raw retention**: `(N - 2) * P` readable + current partial
- **Default (N=3, P=1day)**: ~1–2 days (unchanged from current behavior)
- **Example (N=9, P=1day)**: ~7 days raw retention

#### sample table default constraint

The `sample` parent table has `slot smallint not null default ash.current_slot()`. The partition `for values in (N)` constraint means PG routes inserts to the correct child. No change needed here — LIST partitioning handles it.

But we need to ensure the CHECK constraint on `slot` reflects the valid range. Currently there's no explicit CHECK — LIST partition bounds enforce it implicitly. With dynamic N, any `slot` value outside 0..N-1 would fail at insert time with "no partition of relation". This is the correct behavior.

---

## Part 2: Rollup tables for long-term storage

### Problem

Raw samples rotate away after (N-2) × rotation_period. For trend analysis ("is the system getting slower this month?"), we need aggregated long-term storage.

### Design

Per the existing `ROLLUP_DESIGN.md`, with refinements for the N-partition world.

#### Tables

```sql
create table if not exists ash.rollup_1m (
  ts         int4 not null,  -- minute-aligned, epoch offset
  datid      oid not null,
  samples    smallint not null,
  peak_backends smallint not null,
  wait_counts int4[] not null,  -- [wait_id, count, wait_id, count, ...]
  query_counts int8[] not null, -- [query_id, count, query_id, count, ...]
  primary key (ts, datid)
);

create table if not exists ash.rollup_1h (
  ts         int4 not null,  -- hour-aligned, epoch offset
  datid      oid not null,
  samples    smallint not null,
  peak_backends smallint not null,
  wait_counts int4[] not null,
  query_counts int8[] not null,
  primary key (ts, datid)
);
```

B-tree on `(ts, datid)` via PK — sufficient for all access patterns.

No partitioning on rollup tables. They're small (see storage estimates below) and `DELETE` + autovacuum handles retention fine.

#### Epoch function

Reuse pg_ash's existing epoch (2025-01-01 UTC, from `ash.config.installed_at`? No — we need a fixed, universal epoch):

```sql
create or replace function ash.epoch()
returns timestamptz
immutable
language sql
as $$
  select '2025-01-01 00:00:00+00'::timestamptz
$$;
```

`int4` overflow horizon: 2025 + 68 years ≈ 2093. Sufficient.

#### `ash.rollup_minute()`

Aggregation function, called by pg_cron every minute (or by external scheduler):

```sql
create or replace function ash.rollup_minute()
returns int  -- number of rollup rows upserted
language plpgsql
as $$
declare
  v_minute_start int4;
  v_minute_end int4;
  v_count int := 0;
begin
  -- Previous minute boundaries
  v_minute_end := extract(epoch from
    date_trunc('minute', now()) - ash.epoch())::int4;
  v_minute_start := v_minute_end - 60;

  -- For each database with samples in this minute:
  -- 1. Decode all samples in the time range
  -- 2. Aggregate wait events into [wait_id, count] pairs
  -- 3. Aggregate query_ids into [query_id, count] pairs (top 100)
  -- 4. Compute peak_backends (max active sessions in any single sample)
  -- 5. Upsert into rollup_1m

  with minute_samples as (
    select
      s.sample_ts,
      s.active_count,
      s.data,
      s.slot
    from ash.sample s
    where s.sample_ts >= v_minute_start
      and s.sample_ts < v_minute_end
  ),
  decoded as (
    select
      ms.sample_ts,
      ms.active_count,
      d.*
    from minute_samples ms
    cross join lateral ash.decode_sample(ms.data, ms.slot) d
  ),
  per_db as (
    select
      datid,
      count(distinct sample_ts)::smallint as samples,
      max(active_count)::smallint as peak_backends,
      -- wait event aggregation: sum counts per wait_event_map id
      -- (implementation detail: build array from aggregated results)
      array_agg(distinct wait_event_id order by count desc) as wait_ids,
      array_agg(distinct query_id order by count desc) as query_ids
    from decoded
    group by datid
  )
  insert into ash.rollup_1m (ts, datid, samples, peak_backends, wait_counts, query_counts)
  select
    v_minute_start,
    datid,
    samples,
    peak_backends,
    wait_counts_array,  -- built from per-wait aggregation
    query_counts_array  -- built from per-query aggregation, top 100
  from per_db
  on conflict (ts, datid) do update set
    samples = excluded.samples,
    peak_backends = excluded.peak_backends,
    wait_counts = excluded.wait_counts,
    query_counts = excluded.query_counts;

  get diagnostics v_count = row_count;
  return v_count;
end $$;
```

**Note**: The actual implementation will need to properly decode the packed sample arrays. The `decode_sample()` function already exists — it returns rows with `(datid, userid, query_map_id, wait_event_type, wait_event)`. The rollup aggregator needs to:

1. Group by `datid`
2. For waits: count occurrences of each `(wait_event_type, wait_event)` pair → look up `wait_event_map` id → build `[id, count, id, count, ...]`
3. For queries: resolve `query_map_id` → `query_id` via `query_map_all`, count occurrences → build `[query_id, count, ...]`, truncate to top 100
4. `samples` = count of distinct `sample_ts` values
5. `peak_backends` = max `active_count` across all samples in the minute

**Key invariant**: Rollup must run BEFORE rotation truncates the data. With default 1-day rotation, there's plenty of time. With very short rotation periods (e.g., 1 hour with 3 partitions), the rollup might miss data if it falls behind. The rollup function should check if data exists and emit a WARNING if the time range is empty (gap detection).

#### `ash.rollup_hour()`

```sql
create or replace function ash.rollup_hour()
returns int
language plpgsql
as $$
declare
  v_hour_start int4;
  v_hour_end int4;
  v_count int := 0;
begin
  v_hour_end := extract(epoch from
    date_trunc('hour', now()) - ash.epoch())::int4;
  v_hour_start := v_hour_end - 3600;

  insert into ash.rollup_1h (ts, datid, samples, peak_backends, wait_counts, query_counts)
  select
    v_hour_start,
    datid,
    sum(samples)::smallint,
    max(peak_backends)::smallint,
    ash._merge_wait_counts(array_agg(wait_counts)),
    ash._truncate_query_counts(
      ash._merge_query_counts(array_agg(query_counts)),
      100  -- top 100 queries per hour
    )
  from ash.rollup_1m
  where ts >= v_hour_start and ts < v_hour_end
  group by datid
  on conflict (ts, datid) do update set
    samples = excluded.samples,
    peak_backends = excluded.peak_backends,
    wait_counts = excluded.wait_counts,
    query_counts = excluded.query_counts;

  get diagnostics v_count = row_count;
  return v_count;
end $$;
```

#### Helper functions for array merging

```sql
-- Merge multiple wait_counts arrays: sum counts for matching wait_ids
create or replace function ash._merge_wait_counts(p_arrays int4[][])
returns int4[]
language plpgsql immutable
as $$
declare
  v_result jsonb := '{}';
  v_arr int4[];
  v_i int;
begin
  foreach v_arr slice 1 in array p_arrays loop
    v_i := 1;
    while v_i < array_length(v_arr, 1) loop
      -- v_arr[v_i] = wait_id, v_arr[v_i+1] = count
      v_result := jsonb_set(
        v_result,
        array[v_arr[v_i]::text],
        to_jsonb(coalesce((v_result->>v_arr[v_i]::text)::int, 0) + v_arr[v_i+1])
      );
      v_i := v_i + 2;
    end loop;
  end loop;
  -- Convert back to sorted array [id, count, id, count, ...]
  return (
    select array_agg(v order by count desc)
    from (
      select unnest(array[key::int4, value::int4]) as v,
             value::int as count
      from jsonb_each_text(v_result)
    ) sub
  );
end $$;

-- Same pattern for query_counts (int8 arrays)
create or replace function ash._merge_query_counts(p_arrays int8[][])
returns int8[]
language plpgsql immutable;

-- Truncate query_counts to top N by count
create or replace function ash._truncate_query_counts(p_arr int8[], p_top int)
returns int8[]
language plpgsql immutable;
```

**Alternative (simpler, recommended)**: Skip jsonb intermediate. Use a temp-table-free approach with `unnest` + `array_agg`:

```sql
-- For the hourly rollup, since we're aggregating at most 60 minute-rows per db:
-- Just unnest all arrays, group by id, sum counts, re-array
with pairs as (
  select
    arr[i] as id,
    arr[i+1] as count
  from unnest_minute_arrays  -- from rollup_1m rows
  cross join generate_subscripts(arr, 1) i
  where i % 2 = 1
)
select array_agg(id order by sum_count desc) || array_agg(sum_count order by sum_count desc)
from (
  select id, sum(count) as sum_count
  from pairs
  group by id
) agg;
```

The exact implementation should avoid temp tables (per pg_ash design principles — zero catalog churn).

#### Retention function

```sql
create or replace function ash.rollup_cleanup()
returns text
language plpgsql
as $$
declare
  v_1m_deleted int;
  v_1h_deleted int;
  v_1m_retention int;  -- days
  v_1h_retention int;  -- days
begin
  -- Configurable via ash.config (future), hardcoded for now
  v_1m_retention := 30;   -- 30 days of per-minute data
  v_1h_retention := 1825; -- 5 years of per-hour data

  delete from ash.rollup_1m
  where ts < extract(epoch from
    now() - (v_1m_retention || ' days')::interval - ash.epoch())::int4;
  get diagnostics v_1m_deleted = row_count;

  delete from ash.rollup_1h
  where ts < extract(epoch from
    now() - (v_1h_retention || ' days')::interval - ash.epoch())::int4;
  get diagnostics v_1h_deleted = row_count;

  return format('cleanup: deleted %s minute rows, %s hourly rows',
    v_1m_deleted, v_1h_deleted);
end $$;
```

#### Reader functions

```sql
-- Wait event trends over time from minute rollups
create or replace function ash.minute_waits(
  p_interval interval default '1 hour',
  p_limit int default 10,
  p_color bool default false
)
returns table (
  wait_event text,
  backend_seconds bigint,
  pct numeric,
  bar text
)
language plpgsql stable
set jit = off;

-- Same but with absolute timestamps
create or replace function ash.minute_waits_at(
  p_start timestamptz,
  p_end timestamptz,
  p_limit int default 10,
  p_color bool default false
)
returns table (...)
language plpgsql stable
set jit = off;

-- Query trends from hourly rollups
create or replace function ash.hourly_queries(
  p_interval interval default '1 day',
  p_limit int default 10,
  p_color bool default false
)
returns table (
  query_id bigint,
  backend_seconds bigint,
  pct numeric,
  query_text text  -- from pg_stat_statements if available
)
language plpgsql stable
set jit = off;

-- Peak concurrency per day
create or replace function ash.daily_peak_backends(
  p_interval interval default '7 days'
)
returns table (
  day date,
  peak_backends int,
  avg_backends numeric
)
language plpgsql stable
set jit = off;

-- Rollup status (for ash.status())
-- Shows: rollup_1m rows, oldest/newest, rollup_1h rows, oldest/newest
```

All readers get `_at` variants (absolute timestamps) following existing pg_ash convention.

#### pg_cron scheduling

When `ash.start()` is called and pg_cron is available:

```sql
-- Existing: take_sample every second, rotate every rotation_period
-- New:
perform cron.schedule('ash_rollup_1m', '* * * * *', 'select ash.rollup_minute()');
perform cron.schedule('ash_rollup_1h', '0 * * * *', 'select ash.rollup_hour()');
perform cron.schedule('ash_rollup_gc', '0 3 * * *', 'select ash.rollup_cleanup()');
```

When pg_cron is unavailable, emit NOTICE with external scheduler instructions (consistent with existing behavior).

#### `ash.stop()` changes

Also unschedule rollup jobs.

#### `ash.status()` additions

```
rollup_1m_rows         | 43200
rollup_1m_oldest       | 2026-03-04 00:00:00+00
rollup_1m_newest       | 2026-04-03 05:59:00+00
rollup_1h_rows         | 744
rollup_1h_oldest       | 2026-03-04 00:00:00+00
rollup_1h_newest       | 2026-04-03 05:00:00+00
```

---

## Storage estimates

### Raw samples (configurable N)

| N | rotation_period | Raw retention | Storage (est.) |
|---|----------------|---------------|----------------|
| 3 | 1 day | ~1-2 days | ~30 MiB/day |
| 5 | 1 day | ~3-4 days | ~30 MiB/day |
| 9 | 1 day | ~7 days | ~30 MiB/day |
| 3 | 1 hour | ~1-2 hours | ~1.25 MiB/hr |

N doesn't change daily storage — it changes how many days you keep.

### Rollup tables

Per `ROLLUP_DESIGN.md`:

| Level | Retention | Rows/db | Storage/db |
|-------|----------|---------|-----------|
| 1-minute | 30 days | ~43,200 | ~43 MiB |
| 1-hour | 5 years | ~43,800 | ~77 MiB |

Total: ~120 MiB per database for 5 years of trend data. Negligible.

---

## Upgrade path

### 1.4 → 1.5

```sql
-- ash-1.4-to-1.5.sql

-- 1. Add num_partitions column
alter table ash.config
  add column if not exists num_partitions smallint not null default 3;
alter table ash.config
  add constraint config_num_partitions_check
  check (num_partitions between 3 and 32);

-- 2. Create rollup tables
create table if not exists ash.rollup_1m (...);
create table if not exists ash.rollup_1h (...);

-- 3. Create epoch(), rollup functions, reader functions
-- 4. Replace rotate() with dynamic version
-- 5. Replace take_sample() with dynamic query_map routing
-- 6. Create rebuild_partitions()
-- 7. Create _rebuild_query_map_view()
-- 8. Update status() with new metrics
-- 9. Update version to 1.5
```

Existing 3-partition installations continue working unchanged. `num_partitions = 3` by default.

---

## Implementation order

1. **Phase A: Configurable partitions** (can ship independently)
   - Add `num_partitions` to config
   - Make `rotate()` dynamic
   - Make `take_sample()` query_map routing dynamic
   - Add `rebuild_partitions()`, `_rebuild_query_map_view()`
   - Update `uninstall()`, `status()`
   - Dynamic partition creation at install time
   - Upgrade script
   - CI: test with N=3 (default), N=5, N=7

2. **Phase B: Rollup tables** (depends on Phase A for dynamic rotation awareness)
   - Add `epoch()` function
   - Create `rollup_1m`, `rollup_1h` tables
   - Implement `rollup_minute()`, `rollup_hour()`, `rollup_cleanup()`
   - Array merge helpers
   - Reader functions (`minute_waits`, `hourly_queries`, `daily_peak_backends`) with `_at` variants
   - Update `start()`/`stop()` for rollup cron jobs
   - Update `status()` with rollup metrics
   - Update `uninstall()` to drop rollup tables
   - CI: test rollup correctness (insert known samples → verify aggregated values)

---

## Open questions

1. **Should `num_partitions` be changeable via a simple `UPDATE` + function call, or require `rebuild_partitions()`?** Recommendation: require `rebuild_partitions()` — changing the partition count requires DDL (create/drop tables), so it can't be a simple config change.

2. **Rollup retention configurability**: Hardcode 30d/5y initially, or add `rollup_1m_retention_days` / `rollup_1h_retention_days` to config? Recommendation: config columns from the start — trivial to add, avoids schema change later.

3. **Should rollup run even without pg_cron?** Yes — the functions exist regardless. Users without pg_cron call them from external schedulers (crontab, systemd timer). Same pattern as `take_sample()`.

4. **Gap detection**: If rollup finds no raw samples for a time range (data was rotated before rollup ran), should it insert a zero-row or skip? Recommendation: skip + emit WARNING. A zero-row would be misleading ("system was idle" vs "data was lost").

5. **Rollup during partition rebuild**: `rebuild_partitions()` clears all raw data. Rollup tables survive. This is the correct behavior — rollup preserves historical trends even when raw data is destroyed.

---

## Key decisions (inherited from ROLLUP_DESIGN.md)

1. **Denormalized `query_id` in rollups** — `query_counts` stores raw `query_id` (int8), not `query_map_id`. Eliminates GC coordination.
2. **Backend-seconds as count unit** — consistent with Oracle ASH.
3. **Upsert for idempotency** — `ON CONFLICT DO UPDATE` handles double-fires.
4. **No partitioning on rollup tables** — too small to benefit.
5. **All wait events kept, queries truncated to top 100** — waits are bounded by PG source (~600 max).

## Key decisions (new)

6. **Dynamic SQL for partition routing** — `execute format(...)` in `take_sample()` and `rotate()`. Negligible overhead at 1s interval.
7. **`rebuild_partitions()` is destructive** — requires explicit call, REVOKE'd from PUBLIC.
8. **Minimum 3 partitions** — ring buffer needs current + previous + truncate target.
9. **Maximum 32 partitions** — practical limit for UNION ALL view and planner.
10. **Config column, not config table** — `num_partitions` in `ash.config` singleton row, consistent with existing design.
