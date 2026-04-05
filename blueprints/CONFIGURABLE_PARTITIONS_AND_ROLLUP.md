# Configurable N-partitions + Rollup tables

> **Spec version**: 0.2 (2026-04-05)
> **Target**: pg_ash v1.5
> **Status**: Draft — under review
> **Issue**: [#30](https://github.com/NikolayS/pg_ash/issues/30)
> **Branch**: `feat/configurable-partitions-and-rollup`

## Changelog

| Version | Date | Changes |
|---------|------|---------|
| 0.1 | 2026-04-03 | Initial spec: configurable N-partitions + rollup tables |
| 0.2 | 2026-04-05 | Incorporated 4 expert reviews. Major changes: epoch fixed to existing 2026-01-01; watermark-based rollup execution model; hardened `rebuild_partitions()` with disable/lock/restart protocol; precise retention semantics; `peak_backends` clarified as per-database; retention config columns added from day one; `rollup_min_samples` threshold added; ts/epoch helper functions; explicit upgrade ordering; array merge helpers redesigned (jsonb approach removed); `rollup_minute()` pseudocode rewritten; reader function specs expanded; `start()`/`stop()` idempotency rules; catalog-based cleanup for `uninstall()`/`rebuild_partitions()` |

---

## Overview

Two features for pg_ash v1.5:

1. **Configurable N partitions** — replace hardcoded 3-partition ring buffer with user-configurable N (default 3, range 3–32). Modeled after pg-flight-recorder's `ring_buffer_slots` approach.
2. **Rollup tables for long-term storage** — minute and hourly aggregates surviving rotation, per the existing `ROLLUP_DESIGN.md`.

Both share a dependency: rotation logic must become dynamic before rollups can reference partition boundaries correctly.

**Version note**: Current version is 1.3. Whether this ships as v1.4 or v1.5 depends on whether other features land first. The spec uses "v1.5" as a placeholder; the upgrade script will be named accordingly at release time (e.g., `ash-1.3-to-1.4.sql` or `ash-1.4-to-1.5.sql`).

---

## Part 1: Configurable N partitions

### Problem

pg_ash hardcodes 3 partitions (`sample_0/1/2`, `query_map_0/1/2`) with `% 3` modular arithmetic and a `CASE` statement in `rotate()`. This means:

- Default retention = 1 rotation period (the third partition is being truncated)
- Users who want longer raw sample retention (e.g., 7 days at 1-day rotation) must edit SQL source
- The `CASE` block in `rotate()` doesn't scale

### Design

#### Config changes

Add columns to `ash.config`:

```sql
alter table ash.config
  add column num_partitions smallint not null default 3;

alter table ash.config
  add constraint config_num_partitions_check
  check (num_partitions between 3 and 32);
```

Valid range: 3–32.
- **Minimum 3**: the ring buffer needs current + previous + one being truncated.
- **Maximum 32**: implementation ceiling chosen to contain `query_map_all` UNION ALL planning overhead. CI must benchmark lookup paths at N=3, 9, 16, and 32. If performance degrades beyond acceptable thresholds, fall back to a real partitioned `query_map` parent rather than a generated view. This is a known scaling ceiling, not just a "practical" limit.

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

Replace the 3-way `IF` for query_map inserts with dynamic SQL. The dynamic version must preserve the existing behavior: query `pg_stat_activity` directly with the 50k cap check using `reltuples` (not `pg_relation_size`).

```sql
-- current approach (hardcoded 3-way IF, repeated query):
if v_current_slot = 0 then
  insert into ash.query_map_0 (query_id)
  select distinct sa.query_id
  from pg_stat_activity sa
  where sa.query_id is not null
    and sa.state in ('active', 'idle in transaction', 'idle in transaction (aborted)')
    and (sa.backend_type = 'client backend'
     or (v_include_bg and sa.backend_type in (...)))
    and sa.pid <> pg_backend_pid()
    and (select reltuples from pg_class
     where oid = 'ash.query_map_0'::regclass) < 50000
  on conflict (query_id) do nothing;
elsif v_current_slot = 1 then
  -- same query, different table name (bug fixes must be applied 3x)
  ...
end if;

-- new approach (single dynamic SQL, bug fixes apply once):
execute format(
  'insert into ash.query_map_%s (query_id) '
  'select distinct sa.query_id '
  'from pg_stat_activity sa '
  'where sa.query_id is not null '
  '  and sa.state in (''active'', ''idle in transaction'', '
  '    ''idle in transaction (aborted)'') '
  '  and (sa.backend_type = ''client backend'' '
  '   or ($1 and sa.backend_type in (''autovacuum worker'', '
  '     ''logical replication worker'', ''parallel worker'', '
  '     ''background worker''))) '
  '  and sa.pid <> pg_backend_pid() '
  '  and (select reltuples from pg_class '
  '   where oid = %L::regclass) < 50000 '
  'on conflict (query_id) do nothing',
  v_current_slot,
  'ash.query_map_' || v_current_slot
) using v_include_bg;
```

**Performance note**: Dynamic SQL adds ~0.1ms overhead per `take_sample()` call. At 1s sampling interval, this is negligible (<0.01% overhead). The plan cache won't help with partition routing anyway since the slot changes. Ensure `v_current_slot` is strictly typed as `smallint` before format concatenation to prevent any string coercion overhead.

#### `rotate()` — dynamic truncation

Replace the `CASE` statement with dynamic SQL:

```sql
-- read num_partitions from config (already fetched with current_slot)
v_num_partitions := v_config.num_partitions;

-- calculate new slot dynamically
v_new_slot := (v_old_slot + 1) % v_num_partitions;
v_truncate_slot := (v_new_slot + 1) % v_num_partitions;

-- advance current_slot first (before truncate)
update ash.config
set current_slot = v_new_slot,
    rotated_at = now()
where singleton;

-- dynamic truncation (replaces CASE block)
execute format('truncate ash.sample_%s', v_truncate_slot);
execute format('truncate ash.query_map_%s', v_truncate_slot);
-- ALTER COLUMN id RESTART takes ACCESS EXCLUSIVE lock on query_map.
-- Safe here because we just truncated the table — no concurrent readers.
execute format(
  'alter table ash.query_map_%s alter column id restart', v_truncate_slot
);
```

Modular arithmetic changes from `% 3` to `% v_num_partitions` everywhere.

**Rollup integration** (Phase B): `rotate()` should call `rollup_minute()` for any un-rolled-up minutes in the partition about to be truncated. This is a belt-and-suspenders safeguard — the regular cron-driven rollup handles the normal case, but this ensures no data loss even if the scheduler drifts. Safe because rollup is idempotent (upsert). See Part 2 for details.

#### `rebuild_partitions(p_num int default null)` — hardened

New admin function. **Destructive** — all raw sample data is lost. Rollup tables survive.

The v0.1 spec was too simple — it called `stop()` then immediately dropped tables. This races with in-flight `take_sample()` calls and external schedulers. The hardened protocol:

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

  -- Step 1: Mark sampling disabled in config.
  -- take_sample() checks this flag and returns early.
  update ash.config set sampling_enabled = false where singleton;

  -- Step 2: Stop pg_cron jobs if available
  if ash._pg_cron_available() then
    perform ash.stop();
  end if;

  -- Step 3: Acquire advisory lock shared by take_sample/rotate/rollup paths.
  -- Wait up to 5s for any in-flight operations to complete.
  set local lock_timeout = '5s';
  if not pg_try_advisory_lock(hashtext('ash_rebuild')) then
    -- Re-enable sampling since we failed
    update ash.config set sampling_enabled = true where singleton;
    raise exception 'rebuild_partitions: could not acquire lock — '
      'another operation is in progress';
  end if;

  begin
    -- Step 4: Brief sleep to let any in-flight take_sample() finish.
    -- take_sample() runs every 1s and completes in <100ms typically.
    perform pg_sleep(2);

    -- Step 5: Drop ALL existing sample partitions and query_maps.
    -- Use catalog enumeration (not config) to catch orphaned tables
    -- from prior failed rebuilds.
    perform ash._drop_all_partitions();

    -- Step 6: Update config
    update ash.config
    set num_partitions = v_new_n,
        current_slot = 0,
        rotated_at = now()
    where singleton;

    -- Step 7: Create new partitions
    for i in 0..v_new_n-1 loop
      execute format(
        'create table ash.sample_%s partition of ash.sample '
        'for values in (%s)', i, i
      );
      execute format(
        'create table ash.query_map_%s ('
        '  id int4 generated always as identity,'
        '  query_id int8 not null,'
        '  unique (query_id))', i
      );
      execute format(
        'create index sample_%s_ts_idx on ash.sample_%s (sample_ts desc)',
        i, i
      );
    end loop;

    -- Step 8: Rebuild the query_map_all view
    perform ash._rebuild_query_map_view();

    perform pg_advisory_unlock(hashtext('ash_rebuild'));

    -- Step 9: Leave sampling DISABLED. User must explicitly call
    -- ash.start() to resume. This is intentional — forcing explicit
    -- restart prevents accidental data collection into a fresh schema.
    return format(
      'rebuilt: %s -> %s partitions. all raw data cleared. '
      'call ash.start() to resume sampling.',
      v_old_n, v_new_n
    );

  exception when others then
    perform pg_advisory_unlock(hashtext('ash_rebuild'));
    -- Re-enable sampling on failure so the system isn't left dead
    update ash.config set sampling_enabled = true where singleton;
    raise;
  end;
end $$;
```

**Config change**: add `sampling_enabled bool not null default true` to `ash.config`. `take_sample()` checks this flag and returns 0 immediately when disabled.

REVOKE from PUBLIC (admin-only, like `rotate`, `start`, `stop`).

#### `_drop_all_partitions()` — catalog-based cleanup

Use catalog enumeration instead of trusting `num_partitions` config. This catches orphaned tables from prior failed rebuilds:

```sql
create or replace function ash._drop_all_partitions()
returns void
language plpgsql
as $$
declare
  v_rec record;
begin
  -- Drop sample partitions (children of ash.sample)
  for v_rec in
    select c.relname
    from pg_inherits i
    join pg_class c on c.oid = i.inhrelid
    join pg_namespace n on n.oid = c.relnamespace
    where i.inhparent = 'ash.sample'::regclass
      and n.nspname = 'ash'
  loop
    execute format('drop table if exists ash.%I', v_rec.relname);
  end loop;

  -- Drop query_map tables by naming pattern
  for v_rec in
    select c.relname
    from pg_class c
    join pg_namespace n on n.oid = c.relnamespace
    where n.nspname = 'ash'
      and c.relname ~ '^query_map_[0-9]+$'
      and c.relkind = 'r'
  loop
    execute format('drop table if exists ash.%I', v_rec.relname);
  end loop;
end $$;
```

#### `status()` — add num_partitions metric

```sql
metric := 'num_partitions'; value := v_config.num_partitions::text; return next;
metric := 'sampling_enabled'; value := v_config.sampling_enabled::text; return next;
```

#### `uninstall()` — catalog-based drop

Use `_drop_all_partitions()` instead of loop over config count:

```sql
-- Drop all sample partitions and query_maps (catalog-based)
perform ash._drop_all_partitions();
-- Then drop the parent table, view, etc.
```

#### Retention semantics — precise definition

With N partitions and rotation_period P:

- **Guaranteed complete-history retention**: `(N - 2) * P` — this many full rotation periods are always fully queryable, regardless of when you ask.
- **Best-case visible window**: up to `(N - 1) * P` — immediately after rotation, before truncation of the oldest, you briefly see N-1 complete periods.
- **Plus current partial period**: the active partition accumulates data throughout the current rotation cycle.
- **Default (N=3, P=1day)**: guaranteed 1 day of complete history + current partial (unchanged from current behavior).
- **Example (N=9, P=1day)**: guaranteed 7 days of complete history + current partial.

**Important**: "N-1 readable partitions" is physically true but not equivalent to "N-1 periods of useful history." The practically queryable window depends on when within the rotation cycle you ask. Documentation and `status()` output should use the conservative `(N - 2) * P` guarantee.

#### sample table partition routing

The `sample` parent table has `slot smallint not null default ash.current_slot()`. LIST partitioning routes inserts to the correct child by slot value. No change needed — any `slot` value outside 0..N-1 fails at insert time with "no partition of relation." This is the correct behavior.

---

## Part 2: Rollup tables for long-term storage

### Problem

Raw samples rotate away after `(N-2) * rotation_period`. For trend analysis ("is the system getting slower this month?"), we need aggregated long-term storage.

### Design

Per the existing `ROLLUP_DESIGN.md`, with refinements for the N-partition world and fixes from review.

#### Config additions

Add retention and rollup control columns to `ash.config`:

```sql
alter table ash.config
  add column rollup_1m_retention_days smallint not null default 30,
  add column rollup_1h_retention_days smallint not null default 1825,
  add column rollup_min_samples smallint not null default 3,
  add column last_rollup_1m_ts int4,
  add column last_rollup_1h_ts int4;
```

- `rollup_1m_retention_days`: how long to keep per-minute rollups (default 30 days).
- `rollup_1h_retention_days`: how long to keep per-hour rollups (default 1825 = 5 years).
- `rollup_min_samples`: minimum backend-seconds a query must have in an aggregation window to be stored in `query_counts` (default 3). Queries below this threshold are noise — not useful for trend analysis. See `PARTITIONED_QUERYMAP_DESIGN.md` for rationale.
- `last_rollup_1m_ts` / `last_rollup_1h_ts`: watermark timestamps for catch-up execution (see below).

#### Epoch and timestamp helpers

pg_ash already has `ash.epoch()` returning `'2026-01-01 00:00:00+00'::timestamptz` (ash-install.sql line 55). **This must not change** — it is `IMMUTABLE` and all existing `sample_ts` values are seconds since this epoch.

**Important**: The v0.1 spec proposed `2025-01-01` for rollups — this was wrong. Rollup `ts` columns use the same epoch as `sample_ts`. No new epoch function needed.

Add helper functions to centralize ts↔timestamptz conversion (every reader and rollup function needs this — a single source of truth prevents off-by-one bugs):

```sql
-- Convert timestamptz to int4 epoch offset
create or replace function ash.ts_from_timestamptz(p_ts timestamptz)
returns int4
language sql
immutable
parallel safe
as $$
  select extract(epoch from p_ts - ash.epoch())::int4
$$;

-- Convert int4 epoch offset to timestamptz
create or replace function ash.ts_to_timestamptz(p_ts int4)
returns timestamptz
language sql
immutable
parallel safe
as $$
  select ash.epoch() + p_ts * interval '1 second'
$$;
```

`int4` overflow horizon: 2026 + 68 years = 2094. Sufficient.

#### Tables

```sql
create table if not exists ash.rollup_1m (
  ts              int4 not null,     -- minute-aligned epoch offset
  datid           oid not null,
  samples         smallint not null, -- count of raw samples in this minute (max 60)
  peak_backends   smallint not null, -- max per-database active backends in any
                                     -- single sample within this minute
  wait_counts     int4[] not null,   -- [wait_id, count, wait_id, count, ...]
  query_counts    int8[] not null,   -- [query_id, count, query_id, count, ...]
  primary key (ts, datid)
);

create table if not exists ash.rollup_1h (
  ts              int4 not null,     -- hour-aligned epoch offset
  datid           oid not null,
  samples         smallint not null, -- sum of minute samples (max 3600)
  peak_backends   smallint not null, -- max per-database peak across the hour
  wait_counts     int4[] not null,
  query_counts    int8[] not null,
  primary key (ts, datid)
);
```

B-tree on `(ts, datid)` via PK — sufficient for all access patterns.

No partitioning on rollup tables. They're small (see storage estimates below) and `DELETE` + autovacuum handles retention fine. **Note for heavy multi-database deployments**: with 50+ databases, `rollup_1m` could reach 2M+ rows and the daily `DELETE` + autovacuum cycle becomes noticeable. Monthly range partitioning on `rollup_1m` could help in the future but is not needed initially.

**`peak_backends` semantics**: This is the **per-database** peak active backend count, not a cluster-wide number. Computed by counting decoded rows per `(sample_ts, datid)` and taking the max. The raw `active_count` column in `ash.sample` is also per-database (one sample row per datid per tick), so `max(active_count)` grouped by `datid` gives the correct per-database peak.

**`smallint` for `samples` and `peak_backends`**: `samples` caps at 60 per minute / 3600 per hour — well within smallint range. `peak_backends` would only overflow at 32,767 concurrent backends per database, which exceeds PostgreSQL's practical limits. Assumption documented here.

**Array encoding**: Flat `[id, count, id, count, ...]` pairs sorted by count descending. No nesting, no negative markers. Wait events use `wait_event_map` ids (int4). Queries use raw `query_id` values (int8) — self-contained, no `query_map` dependency for long-term storage.

**Array invariants** (enforced by helper functions):
- Arrays must have even length (id/count pairs)
- NULL arrays are treated as empty
- Empty result returns `'{}'::int4[]` (or `int8[]`), never NULL
- Ordering: by count DESC, then by id ASC for deterministic output
- For query counts, truncation preserves `[id, count]` pairing correctly

#### Watermark-based rollup execution model

The v0.1 spec used a fire-and-forget "previous minute only" model — `rollup_minute()` always processed exactly the last minute. This is fragile: if the scheduler misses a fire (pg_cron under load, long-running transaction blocking, clock skew), that minute is lost forever once rotation truncates the raw data.

**v0.2 uses watermark-based catch-up**:

- `ash.config.last_rollup_1m_ts` / `last_rollup_1h_ts` track the last successfully processed timestamp.
- Each rollup call processes **all unprocessed complete periods** up to a bounded batch size.
- The watermark advances transactionally with the upsert.
- This gives: catch-up after outages, deterministic gap detection, idempotent replay, less dependence on exact scheduler timing.

#### `ash.rollup_minute()`

```sql
create or replace function ash.rollup_minute()
returns int  -- total rollup rows upserted
language plpgsql
as $$
declare
  v_last_ts int4;
  v_now_minute_ts int4;
  v_minute_start int4;
  v_minute_end int4;
  v_batch_limit int := 60;  -- catch up at most 60 minutes per call
  v_total int := 0;
  v_count int;
  v_min_samples smallint;
begin
  select last_rollup_1m_ts, rollup_min_samples
  into v_last_ts, v_min_samples
  from ash.config where singleton;

  -- Current minute boundary (we only process *complete* minutes)
  v_now_minute_ts := ash.ts_from_timestamptz(date_trunc('minute', now()));

  -- Initialize watermark if NULL (first run)
  if v_last_ts is null then
    -- Find earliest sample_ts, align to minute
    select (min(sample_ts) / 60) * 60
    into v_last_ts
    from ash.sample;

    if v_last_ts is null then
      return 0;  -- no samples at all
    end if;
  end if;

  -- Process each unprocessed complete minute
  v_minute_start := v_last_ts;
  while v_minute_start < v_now_minute_ts and v_batch_limit > 0 loop
    v_minute_end := v_minute_start + 60;

    -- [PSEUDOCODE — actual implementation needs full decode/aggregate]
    -- The implementation must:
    -- 1. Select all samples where sample_ts >= v_minute_start
    --    and sample_ts < v_minute_end
    -- 2. Decode each sample via ash.decode_sample(data, slot)
    --    which returns (datid, userid, query_map_id, wait_event_type,
    --    wait_event)
    -- 3. Group by datid:
    --    a. samples = count(distinct sample_ts)
    --    b. peak_backends = computed by counting decoded rows per
    --       (sample_ts, datid), then taking max of those counts
    --    c. For waits: count occurrences of each wait_event_map id
    --       across all decoded rows → build [id, count, ...] array
    --       sorted by count desc. All wait events kept (bounded by
    --       PG source: ~600 max).
    --    d. For queries: resolve query_map_id → query_id via
    --       query_map_all (using the sample's recorded slot),
    --       count occurrences, filter to >= v_min_samples,
    --       take top 100 by count → build [query_id, count, ...]
    --       array sorted by count desc
    -- 4. Upsert into rollup_1m with ON CONFLICT (ts, datid)
    --    DO UPDATE
    --
    -- Gap detection: if no samples exist for this minute range,
    -- check whether samples exist for *later* minutes. If yes,
    -- emit WARNING (data was likely rotated before rollup ran).
    -- Do NOT insert zero-rows — that would imply an idle system
    -- rather than a missing observer.

    -- ... (full SQL implementation TBD at coding time) ...

    get diagnostics v_count = row_count;
    v_total := v_total + v_count;

    -- Advance watermark transactionally
    update ash.config
    set last_rollup_1m_ts = v_minute_end
    where singleton;

    v_minute_start := v_minute_end;
    v_batch_limit := v_batch_limit - 1;
  end loop;

  return v_total;
end $$;
```

**Key dependency**: Minute rollup must resolve `query_map_id` → `query_id` while both the sample rows and the matching `query_map_N` table for that slot still exist. If a slot is rotated/truncated before rollup runs, historical query resolution is lost. This is another reason watermark-based catch-up is critical. Tests must simulate rotation occurring near minute boundaries. Gap warnings should distinguish "no samples" from "samples exist but query_map resolution failed."

**`rotate()` integration**: As noted in Part 1, `rotate()` should call `rollup_minute()` for un-rolled-up minutes in the partition about to be truncated. This is the last-resort safeguard ensuring no data loss even when the scheduler drifts.

#### `ash.rollup_hour()`

```sql
create or replace function ash.rollup_hour()
returns int
language plpgsql
as $$
declare
  v_last_ts int4;
  v_now_hour_ts int4;
  v_hour_start int4;
  v_hour_end int4;
  v_batch_limit int := 24;  -- catch up at most 24 hours per call
  v_total int := 0;
  v_count int;
begin
  select last_rollup_1h_ts into v_last_ts
  from ash.config where singleton;

  v_now_hour_ts := ash.ts_from_timestamptz(date_trunc('hour', now()));

  if v_last_ts is null then
    select (min(ts) / 3600) * 3600 into v_last_ts from ash.rollup_1m;
    if v_last_ts is null then return 0; end if;
  end if;

  v_hour_start := v_last_ts;
  while v_hour_start < v_now_hour_ts and v_batch_limit > 0 loop
    v_hour_end := v_hour_start + 3600;

    insert into ash.rollup_1h (
      ts, datid, samples, peak_backends, wait_counts, query_counts
    )
    select
      v_hour_start,
      datid,
      sum(samples)::smallint,
      max(peak_backends)::smallint,
      ash._merge_wait_counts(array_agg(wait_counts)),
      ash._truncate_pairs(
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
    v_total := v_total + v_count;

    update ash.config
    set last_rollup_1h_ts = v_hour_end
    where singleton;

    v_hour_start := v_hour_end;
    v_batch_limit := v_batch_limit - 1;
  end loop;

  return v_total;
end $$;
```

#### Helper functions for array merging

**Design decision**: The v0.1 spec proposed a jsonb-based merge. All reviewers agreed this should be discarded — converting integer arrays to text-keyed jsonb just to increment counts is a CPU burn. Use pure set-based SQL with unnest + group by + array_agg.

The helpers take an `anyarray` aggregated from multiple rollup rows and merge id/count pairs.

```sql
-- Merge multiple wait_counts arrays: sum counts for matching wait_ids.
-- Input: array of int4[] arrays (from array_agg(wait_counts)).
-- Note: PG flattens array_agg(int4[]) into int4[] — so the input
-- is actually a single flat int4[] containing concatenated pairs.
-- The function must handle this correctly.
create or replace function ash._merge_wait_counts(p_flat int4[])
returns int4[]
language sql
immutable
parallel safe
as $$
  -- Unnest the flat array into (position, value) pairs,
  -- extract id/count pairs by position parity,
  -- group by id, sum counts, re-assemble sorted array
  with numbered as (
    select
      row_number() over () as pos,
      val
    from unnest(p_flat) as val
  ),
  pairs as (
    select
      n1.val as id,
      n2.val as cnt
    from numbered n1
    join numbered n2 on n2.pos = n1.pos + 1
    where n1.pos % 2 = 1
  ),
  merged as (
    select id, sum(cnt)::int4 as total
    from pairs
    group by id
    order by total desc, id asc
  )
  select coalesce(
    array_agg(id order by total desc, id asc)
      || array_agg(total order by total desc, id asc),
    '{}'::int4[]
  )
  from merged
$$;
```

**Wait — the above interleaving is wrong.** `array_agg(id) || array_agg(total)` produces `[id1, id2, ..., count1, count2, ...]` not `[id1, count1, id2, count2, ...]`. Correct approach:

```sql
-- Correct: produce interleaved [id, count, id, count, ...]
create or replace function ash._merge_wait_counts(p_flat int4[])
returns int4[]
language sql
immutable
parallel safe
as $$
  with numbered as (
    select row_number() over () as pos, val
    from unnest(p_flat) as val
  ),
  pairs as (
    select n1.val as id, n2.val as cnt
    from numbered n1
    join numbered n2 on n2.pos = n1.pos + 1
    where n1.pos % 2 = 1
  ),
  merged as (
    select id, sum(cnt)::int4 as total
    from pairs
    group by id
  ),
  interleaved as (
    select unnest(array[id, total]) as v,
           row_number() over (order by total desc, id asc) as grp
    from merged
  )
  select coalesce(array_agg(v order by grp, v desc), '{}'::int4[])
  from interleaved
$$;
```

**Implementation note**: The exact interleaving approach needs careful testing. The core algorithm is: unnest flat concatenated pairs → group by id → sum counts → re-interleave into `[id, count, ...]` sorted by count desc. The implementation should be prototyped and tested in isolation before building the rest of the rollup pipeline around it.

Same pattern for `_merge_query_counts(int8[])` — identical logic, different types.

```sql
-- Truncate a paired array to top N entries by count
create or replace function ash._truncate_pairs(p_arr int8[], p_top int)
returns int8[]
language sql
immutable
parallel safe
as $$
  with numbered as (
    select row_number() over () as pos, val
    from unnest(p_arr) as val
  ),
  pairs as (
    select n1.val as id, n2.val as cnt
    from numbered n1
    join numbered n2 on n2.pos = n1.pos + 1
    where n1.pos % 2 = 1
  ),
  top_n as (
    select id, cnt
    from pairs
    order by cnt desc, id asc
    limit p_top
  ),
  interleaved as (
    select unnest(array[id, cnt]) as v,
           row_number() over (order by cnt desc, id asc) as grp
    from top_n
  )
  select coalesce(array_agg(v order by grp, v desc), '{}'::int8[])
  from interleaved
$$;
```

**Array invariant enforcement**: All helper functions guarantee:
- Even-length output (id/count pairs)
- NULL input → empty array
- Empty input → empty array
- Deterministic ordering: count DESC, id ASC
- No temp tables (zero catalog churn)

#### Retention function

```sql
create or replace function ash.rollup_cleanup()
returns text
language plpgsql
as $$
declare
  v_1m_deleted int;
  v_1h_deleted int;
  v_1m_retention int;
  v_1h_retention int;
  v_cutoff_1m int4;
  v_cutoff_1h int4;
begin
  select rollup_1m_retention_days, rollup_1h_retention_days
  into v_1m_retention, v_1h_retention
  from ash.config where singleton;

  v_cutoff_1m := ash.ts_from_timestamptz(
    now() - (v_1m_retention || ' days')::interval
  );
  v_cutoff_1h := ash.ts_from_timestamptz(
    now() - (v_1h_retention || ' days')::interval
  );

  delete from ash.rollup_1m where ts < v_cutoff_1m;
  get diagnostics v_1m_deleted = row_count;

  delete from ash.rollup_1h where ts < v_cutoff_1h;
  get diagnostics v_1h_deleted = row_count;

  return format('cleanup: deleted %s minute rows, %s hourly rows',
    v_1m_deleted, v_1h_deleted);
end $$;
```

#### Reader functions

Reader functions follow existing pg_ash convention: interval-based default + `_at()` variants for absolute timestamps. All readers use `ash.ts_from_timestamptz()` / `ash.ts_to_timestamptz()` for epoch conversion.

Readers hit **rollup tables only**, not raw samples. Raw sample readers (`top_waits`, `top_queries`, etc.) already exist and cover short-term analysis. Rollup readers are for historical trends.

```sql
-- Wait event trends from minute rollups
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
set jit = off
as $$
declare
  v_start_ts int4;
  v_end_ts int4;
begin
  v_end_ts := ash.ts_from_timestamptz(now());
  v_start_ts := ash.ts_from_timestamptz(now() - p_interval);
  -- Decode wait_counts arrays from rollup_1m rows in range,
  -- join wait_event_map for names, sum counts, compute pct,
  -- format bar chart. Top p_limit by backend_seconds.
  -- (Full implementation at coding time)
  return query select null::text, null::bigint, null::numeric, null::text
  where false;  -- placeholder
end $$;

-- Absolute-time variant
create or replace function ash.minute_waits_at(
  p_start timestamptz,
  p_end timestamptz,
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

create or replace function ash.hourly_queries_at(
  p_start timestamptz,
  p_end timestamptz,
  p_limit int default 10,
  p_color bool default false
)
returns table (
  query_id bigint,
  backend_seconds bigint,
  pct numeric,
  query_text text
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

create or replace function ash.daily_peak_backends_at(
  p_start timestamptz,
  p_end timestamptz
)
returns table (
  day date,
  peak_backends int,
  avg_backends numeric
)
language plpgsql stable
set jit = off;
```

#### pg_cron scheduling — idempotent

`start()` and `stop()` must handle repeated calls and pre-existing jobs gracefully:

```sql
-- In ash.start():
-- Unschedule by known name BEFORE scheduling (idempotent).
-- This handles: repeated start() calls, name collisions from
-- older versions, or stale jobs from a prior install.
begin
  perform cron.unschedule('ash_rollup_1m');
exception when others then null;
end;
perform cron.schedule('ash_rollup_1m', '* * * * *',
  'select ash.rollup_minute()');

begin
  perform cron.unschedule('ash_rollup_1h');
exception when others then null;
end;
perform cron.schedule('ash_rollup_1h', '0 * * * *',
  'select ash.rollup_hour()');

begin
  perform cron.unschedule('ash_rollup_gc');
exception when others then null;
end;
perform cron.schedule('ash_rollup_gc', '0 3 * * *',
  'select ash.rollup_cleanup()');
```

When pg_cron is unavailable, emit NOTICE with external scheduler instructions (consistent with existing behavior for `take_sample` and `rotate`).

#### `ash.stop()` changes

Also unschedule rollup jobs. Tolerate missing jobs (idempotent):

```sql
-- In ash.stop():
begin perform cron.unschedule('ash_rollup_1m'); exception when others then null; end;
begin perform cron.unschedule('ash_rollup_1h'); exception when others then null; end;
begin perform cron.unschedule('ash_rollup_gc'); exception when others then null; end;
```

#### `ash.status()` additions

Report expected vs. actual scheduler state where possible:

```
num_partitions         | 9
sampling_enabled       | t
rollup_1m_rows         | 43200
rollup_1m_oldest       | 2026-03-04 00:00:00+00
rollup_1m_newest       | 2026-04-03 05:59:00+00
rollup_1m_retention    | 30 days
rollup_1h_rows         | 744
rollup_1h_oldest       | 2026-03-04 00:00:00+00
rollup_1h_newest       | 2026-04-03 05:00:00+00
rollup_1h_retention    | 1825 days
last_rollup_1m_ts      | 2026-04-03 05:59:00+00
last_rollup_1h_ts      | 2026-04-03 05:00:00+00
```

---
