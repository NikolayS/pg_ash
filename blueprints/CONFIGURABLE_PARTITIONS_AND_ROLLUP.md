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
