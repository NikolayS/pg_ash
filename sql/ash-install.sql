-- pg_ash: Active Session History for Postgres
-- Version: 1.4 (latest)
-- Fresh install: \i sql/ash-install.sql
-- Upgrade from 1.0: \i sql/ash-1.0-to-1.1.sql, then \i sql/ash-1.1-to-1.2.sql, then \i sql/ash-1.2-to-1.3.sql
-- Upgrade from 1.1: \i sql/ash-1.1-to-1.2.sql, then \i sql/ash-1.2-to-1.3.sql
-- Upgrade from 1.2: \i sql/ash-1.2-to-1.3.sql


-- Drop functions removed or changed in 1.1 (handled by DO block below)
-- Drop ALL overloads of functions whose signatures changed across versions.
-- Using DO block because DROP FUNCTION requires exact arg types and we can't
-- predict which stale overloads exist from prior installs.
do $$
declare
  r record;
begin
  for r in
    select p.oid::regprocedure as sig
    from pg_proc p
    join pg_namespace n on p.pronamespace = n.oid
    where n.nspname = 'ash'
      and p.proname in (
        'top_waits', 'top_waits_at',
        'histogram', 'histogram_at',
        'timeline_chart', 'timeline_chart_at',
        'query_waits', 'query_waits_at',
        'top_by_type', 'top_by_type_at',
        'waits_by_type', 'waits_by_type_at',
        'event_queries', 'event_queries_at',
        'top_queries_with_text',
        'uninstall',
        'debug_logging'
      )
  loop
    execute 'drop function if exists ' || r.sig;
  end loop;
end $$;

--------------------------------------------------------------------------------
-- STEP 1: Core schema and infrastructure
--------------------------------------------------------------------------------

-- Create schema
create schema if not exists ash;

-- Epoch function: 2026-01-01 00:00:00 UTC
-- WARNING: This value must NEVER change after installation. All sample_ts
-- values are seconds since this epoch. Changing it corrupts all timestamps.
create or replace function ash.epoch()
returns timestamptz
language sql
immutable
parallel safe
as $$
  select '2026-01-01 00:00:00+00'::timestamptz
$$;

-- Configuration singleton table
create table if not exists ash.config (
  singleton                  bool primary key default true check (singleton),
  current_slot               smallint not null default 0,
  num_partitions             smallint not null default 3
                               check (num_partitions between 3 and 32),
  sampling_enabled           bool not null default true,
  skipped_samples            int4 not null default 0,
  missed_samples             bigint not null default 0,
  sample_interval            interval not null default '1 second',
  rotation_period            interval not null default '1 day',
  include_bg_workers         bool not null default false,
  debug_logging              bool not null default false,
  encoding_version           smallint not null default 1,
  version                    text not null default '1.4',
  rotated_at                 timestamptz not null default clock_timestamp(),
  installed_at               timestamptz not null default clock_timestamp(),
  rollup_1m_retention_days   smallint not null default 30,
  rollup_1h_retention_days   smallint not null default 1825,
  rollup_min_backend_seconds smallint not null default 3,
  last_rollup_1m_ts          int4,
  last_rollup_1h_ts          int4
);

-- Insert initial row if not exists
insert into ash.config (singleton) values (true) on conflict do nothing;

-- Migration: add v1.4 columns if upgrading from pre-1.4.
-- Must run before any code reads these columns.
-- Uses per-column IF NOT EXISTS so the block is safe when some columns
-- (e.g. missed_samples from the PR #29 upgrade) were already added.
alter table ash.config
  add column if not exists num_partitions smallint not null default 3
    check (num_partitions between 3 and 32),
  add column if not exists sampling_enabled bool not null default true,
  add column if not exists skipped_samples int4 not null default 0,
  add column if not exists missed_samples bigint not null default 0,
  add column if not exists rollup_1m_retention_days smallint not null default 30,
  add column if not exists rollup_1h_retention_days smallint not null default 1825,
  add column if not exists rollup_min_backend_seconds smallint not null default 3,
  add column if not exists last_rollup_1m_ts int4,
  add column if not exists last_rollup_1h_ts int4;

-- Wait event dictionary
create table if not exists ash.wait_event_map (
  id    smallint primary key generated always as identity (start with 1),
  state text not null,
  type  text not null,
  event text not null,
  unique (state, type, event)
);

-- Query ID dictionaries — one per sample partition, TRUNCATE together.
-- Each has its own identity sequence (explicit, not LIKE INCLUDING ALL,
-- because PG14-15 shares sequences with LIKE INCLUDING ALL).
-- Created dynamically based on num_partitions (default 3).
do $$
declare
  v_n int;
begin
  select num_partitions into v_n from ash.config where singleton;

  for i in 0 .. v_n - 1 loop
    execute format(
      'create table if not exists ash.query_map_%s ('
      '  id       int4 primary key generated always as identity (start with 1),'
      '  query_id int8 not null unique'
      ')', i
    );
  end loop;
end $$;

-- Rebuild query_map_all view dynamically for N partitions.
-- Called by rebuild_partitions() after creating/dropping partition tables.
create or replace function ash._rebuild_query_map_view()
returns void
language plpgsql
as $$
declare
  v_n int;
  v_sql text := '';
begin
  select num_partitions into v_n
  from ash.config
  where singleton;

  for i in 0 .. v_n - 1 loop
    if i > 0 then
      v_sql := v_sql || ' union all ';
    end if;

    v_sql := v_sql || format(
      'select %s::smallint as slot, id, query_id from ash.query_map_%s',
      i, i
    );
  end loop;

  execute 'create or replace view ash.query_map_all as ' || v_sql;
end;
$$;

-- Unified view for readers — planner eliminates non-matching partitions
-- when slot is a constant (which it is, from s.slot in reader queries).
-- Built dynamically to support N partitions.
select ash._rebuild_query_map_view();

-- Drop all sample partitions and query_map tables (catalog-based).
-- Uses pg_inherits/pg_class instead of trusting num_partitions config,
-- catching orphaned tables from prior failed rebuilds.
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
end;
$$;

-- Current slot function
create or replace function ash.current_slot()
returns smallint
language sql
stable
parallel safe
as $$
  select current_slot from ash.config where singleton
$$;

-- Sample table (partitioned by slot)
create table if not exists ash.sample (
  sample_ts    int4 not null,
  datid        oid not null,
  active_count smallint not null,
  data         integer[] not null
         check (data[1] < 0 and array_length(data, 1) >= 3),
  slot         smallint not null default ash.current_slot()
) partition by list (slot);

-- Create partitions and indexes dynamically based on num_partitions.
do $$
declare
  v_n int;
begin
  select num_partitions into v_n from ash.config where singleton;

  for i in 0 .. v_n - 1 loop
    execute format(
      'create table if not exists ash.sample_%s '
      'partition of ash.sample for values in (%s)', i, i
    );

    -- (sample_ts) for time-range reader queries
    execute format(
      'create index if not exists sample_%s_ts_idx '
      'on ash.sample_%s (sample_ts)', i, i
    );

    -- (datid, sample_ts) for per-database time-range queries
    execute format(
      'create index if not exists sample_%s_datid_ts_idx '
      'on ash.sample_%s (datid, sample_ts)', i, i
    );
  end loop;
end $$;

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

-- Register wait event function (upsert, returns id)
create or replace function ash._register_wait(p_state text, p_type text, p_event text)
returns smallint
language plpgsql
as $$
declare
  v_id smallint;
begin
  -- Try to get existing
  select id into v_id
  from ash.wait_event_map
  where state = p_state and type = p_type and event = p_event;

  if v_id is not null then
    return v_id;
  end if;

  -- Insert new entry
  insert into ash.wait_event_map (state, type, event)
  values (p_state, p_type, p_event)
  on conflict (state, type, event) do nothing
  returning id into v_id;

  -- If insert succeeded, return it
  if v_id is not null then
    return v_id;
  end if;

  -- Race condition: another session inserted, fetch it
  select id into v_id
  from ash.wait_event_map
  where state = p_state and type = p_type and event = p_event;

  return v_id;
end;
$$;

-- (_register_query removed — bulk registration in take_sample() handles
-- query_map inserts directly via per-partition INSERT ON CONFLICT)

-- Validate data array structure
create or replace function ash._validate_data(p_data integer[])
returns boolean
language plpgsql
immutable
as $$
declare
  v_len int;
  v_idx int;
  v_count int;
  v_qid_count int;
begin
  -- Basic checks
  if p_data is null or array_length(p_data, 1) is null then
    return false;
  end if;

  v_len := array_length(p_data, 1);

  -- Minimum valid: [-wid, count, qid] = 3 elements
  if v_len < 3 then
    return false;
  end if;

  -- First element must be a negative wait_id marker
  if p_data[1] >= 0 then
    return false;
  end if;

  -- Walk the structure
  v_idx := 1;
  while v_idx <= v_len loop
    -- Expect negative marker (wait event id)
    if p_data[v_idx] >= 0 then
      return false;
    end if;

    v_idx := v_idx + 1;

    -- Expect count
    if v_idx > v_len then
      return false;
    end if;

    v_count := p_data[v_idx];
    if v_count <= 0 then
      return false;
    end if;

    v_idx := v_idx + 1;

    -- Expect exactly v_count query_ids (non-negative)
    v_qid_count := 0;
    while v_idx <= v_len and p_data[v_idx] >= 0 loop
      v_qid_count := v_qid_count + 1;
      v_idx := v_idx + 1;
    end loop;

    if v_qid_count <> v_count then
      return false;
    end if;
  end loop;

  return true;
end;
$$;


--------------------------------------------------------------------------------
-- STEP 2: Sampler and decoder
--------------------------------------------------------------------------------

-- Core sampler function (no hstore dependency)
create or replace function ash.take_sample()
returns int
language plpgsql
as $$
declare
  v_sample_ts int4;
  v_include_bg bool;
  v_debug_logging bool;
  v_sampling_enabled bool;
  v_rec record;
  v_datid_rec record;
  v_data integer[];
  v_active_count smallint;
  v_current_wait_id smallint;
  v_current_slot smallint;
  v_rows_inserted int := 0;
  v_missed_count bigint;
  v_seen_waits text[] := '{}';
begin
  -- Get config (single read for all settings)
  select sampling_enabled, include_bg_workers, debug_logging
  into v_sampling_enabled, v_include_bg, v_debug_logging
  from ash.config where singleton;

  if not v_sampling_enabled then
    update ash.config
    set skipped_samples = skipped_samples + 1
    where singleton;

    return 0;
  end if;

  -- Acquire participation lock (xact-level, auto-releases on commit/rollback).
  -- rebuild_partitions() polls pg_locks for this to drain in-flight operations.
  if not pg_try_advisory_xact_lock(0, hashtext('ash_operation')::int4) then
    update ash.config
    set skipped_samples = skipped_samples + 1
    where singleton;

    return 0;
  end if;

  -- Get sample timestamp (seconds since epoch, from now())
  v_sample_ts := extract(epoch from now() - ash.epoch())::int4;
  v_current_slot := ash.current_slot();

  -- =========================================================================
  -- Sampler: 4 pg_stat_activity reads (single-database setup).
  --   1. Wait event registration loop
  --   2. Query_map registration INSERT
  --   3. Distinct datids loop
  --   4. Per-datid encoding CTE (+ active_count)
  -- Reads 1-2 are non-atomic (separate queries) — a backend may appear in
  -- one but not the other. This is harmless: query_map gets an extra entry,
  -- or a wait event registers one tick early.
  -- No temp tables — avoids pg_class/pg_attribute catalog churn on every tick.
  -- =========================================================================

  -- ---- Read 1: Register new wait events; optionally log each sampled session ----
  -- CPU* means the backend is active with no wait event reported. This is
  -- either genuine CPU work or an uninstrumented code path in Postgres.
  -- The asterisk signals this ambiguity. See https://gaps.wait.events
  --
  -- Debug logging (when v_debug_logging = true):
  --   Uses RAISE LOG — goes to server log only, never to the client.
  --   Independent of log_min_messages and client_min_messages.
  --   Enable:  select ash.set_debug_logging(true);
  --   Disable: select ash.set_debug_logging(false);
  --
  -- Both tasks share one pg_stat_activity scan. Wait event registration skips
  -- duplicates via a seen-set (text[] + ANY check) to avoid repeated lookups.
  for v_rec in
    select
      sa.pid,
      sa.state,
      coalesce(sa.wait_event_type,
        case
          when sa.state = 'active'                  then 'CPU*'
          when sa.state like 'idle in transaction%'  then 'IdleTx'
        end
      ) as wait_type,
      coalesce(sa.wait_event,
        case
          when sa.state = 'active'                  then 'CPU*'
          when sa.state like 'idle in transaction%'  then 'IdleTx'
        end
      ) as wait_event,
      sa.backend_type,
      sa.query_id
    from pg_stat_activity sa
    where sa.state in ('active', 'idle in transaction', 'idle in transaction (aborted)')
      and (sa.backend_type = 'client backend'
       or (v_include_bg and sa.backend_type in ('autovacuum worker', 'logical replication worker', 'parallel worker', 'background worker')))
      and sa.pid <> pg_backend_pid()
  loop
    -- Register wait event if not yet seen this tick (dedup in memory, not per row lookup).
    if not (v_rec.state || '|' || v_rec.wait_type || '|' || v_rec.wait_event = any(v_seen_waits)) then
      v_seen_waits := v_seen_waits || (v_rec.state || '|' || v_rec.wait_type || '|' || v_rec.wait_event);
      if not exists (
        select from ash.wait_event_map
        where state = v_rec.state and type = v_rec.wait_type and event = v_rec.wait_event
      ) then
        perform ash._register_wait(v_rec.state, v_rec.wait_type, v_rec.wait_event);
      end if;
    end if;

    -- Debug logging: RAISE LOG goes to server log only, never to the client.
    -- Independent of log_min_messages and client_min_messages.
    if v_debug_logging then
      raise log 'ash.take_sample: pid=% state=% wait_type=% wait_event=% backend_type=% query_id=%',
        v_rec.pid, v_rec.state, v_rec.wait_type, v_rec.wait_event,
        v_rec.backend_type, coalesce(v_rec.query_id::text, '(null)');
    end if;
  end loop;

  -- ---- Read 2: Register query_ids into current slot's query_map ----
  -- Partitioned query_map: TRUNCATE resets on rotation, but between rotations
  -- PG14-15 volatile SQL comments can flood query_map. 50k hard cap per
  -- partition prevents unbounded growth. PG16+ normalizes comments.
  -- Dynamic SQL: single query template, bug fixes apply once (not 3×).
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

  -- ---- Read 2+3: Per-database encoding ----
  -- Build and insert encoded arrays — one per database.
  -- Uses CTEs instead of temp tables to avoid catalog churn.
  for v_datid_rec in
    select distinct coalesce(sa.datid, 0::oid) as datid
    from pg_stat_activity sa
    where sa.state in ('active', 'idle in transaction', 'idle in transaction (aborted)')
      and (sa.backend_type = 'client backend'
       or (v_include_bg and sa.backend_type in ('autovacuum worker', 'logical replication worker', 'parallel worker', 'background worker')))
      and sa.pid <> pg_backend_pid()
  loop
    begin
      -- Single query: snapshot → group by wait → encode → flatten
      with snapshot as (
        select
          wm.id as wait_id,
          coalesce(m.id, 0) as map_id
        from pg_stat_activity sa
        join ash.wait_event_map wm
         on wm.state = sa.state
        and wm.type = coalesce(sa.wait_event_type,
            case when sa.state = 'active' then 'CPU*'
              when sa.state like 'idle in transaction%' then 'IdleTx' end)
        and wm.event = coalesce(sa.wait_event,
            case when sa.state = 'active' then 'CPU*'
              when sa.state like 'idle in transaction%' then 'IdleTx' end)
        left join ash.query_map_all m on m.slot = v_current_slot and m.query_id = sa.query_id
        where sa.state in ('active', 'idle in transaction', 'idle in transaction (aborted)')
          and (sa.backend_type = 'client backend'
           or (v_include_bg and sa.backend_type in ('autovacuum worker', 'logical replication worker', 'parallel worker', 'background worker')))
          and sa.pid <> pg_backend_pid()
          and coalesce(sa.datid, 0::oid) = v_datid_rec.datid
      ),
      groups as (
        select
          row_number() over (order by s.wait_id) as gnum,
          array[(-s.wait_id)::integer, count(*)::integer]
            || array_agg(s.map_id::integer) as group_arr
        from snapshot s
        group by s.wait_id
      ),
      flat as (
        select array_agg(el order by g.gnum, u.ord) as data
        from groups g,
          lateral unnest(g.group_arr) with ordinality as u(el, ord)
      ),
      backend_count as (
        select count(*)::smallint as cnt from snapshot
      )
      select f.data, bc.cnt into v_data, v_active_count
      from flat f, backend_count bc;

      if v_data is not null and array_length(v_data, 1) >= 3 then
        insert into ash.sample (sample_ts, datid, active_count, data)
        values (v_sample_ts, v_datid_rec.datid, v_active_count, v_data);
        v_rows_inserted := v_rows_inserted + 1;
      end if;

    exception when others then
      raise warning 'ash.take_sample: error inserting sample for datid % [%]: %', v_datid_rec.datid, sqlstate, sqlerrm;
    end;
  end loop;

  return v_rows_inserted;

exception when query_canceled then
  -- statement_timeout (or pg_cancel_backend) fired — record the miss.
  -- NOTE: query_canceled catches both statement_timeout AND explicit
  -- pg_cancel_backend() signals. PG provides no way to distinguish them.
  -- This is intentional: either way, the sample was interrupted and the
  -- gap should be observable. If you need to hard-cancel take_sample(),
  -- use pg_terminate_backend() instead.
  update ash.config set missed_samples = missed_samples + 1
    where singleton
    returning missed_samples into v_missed_count;
  if v_missed_count is null then
    raise warning 'ash.take_sample: interrupted (config row missing — missed_samples not tracked)';
  else
    raise warning 'ash.take_sample: interrupted (missed_samples = %)', v_missed_count;
  end if;
  return -1;
end;
$$;

-- Decode sample function
-- p_slot: when provided, look up query_ids from that partition only.
-- When NULL (default), search all partitions via query_map_all view.
-- decode_sample(): return one row per backend.
-- Note: wait_id is the canonical ash.wait_event_map.id, which uniquely
-- identifies (state, type, event). Always join aggregations on wait_id
-- (not on wait_event text) — the wait_event string collapses across
-- states (e.g., ClientRead under 'active' vs 'idle in transaction'),
-- and joining on text double-counts.
create or replace function ash.decode_sample(p_data integer[], p_slot smallint default null)
returns table (
  wait_id smallint,
  wait_event text,
  query_id int8,
  count int
)
language plpgsql
stable
as $$
declare
  v_len int;
  v_idx int;
  v_wait_id smallint;
  v_count int;
  v_qid_idx int;
  v_map_id int4;
  v_type text;
  v_event text;
  v_query_id int8;
begin
  -- Basic validation
  if p_data is null or array_length(p_data, 1) is null then
    return;
  end if;

  v_len := array_length(p_data, 1);

  -- Basic structure check: first element must be negative (wait_id marker)
  if v_len < 3 or p_data[1] >= 0 then
    raise warning 'ash.decode_sample: invalid data array';
    return;
  end if;

  -- Walk the structure
  v_idx := 1;
  while v_idx <= v_len loop
    -- Get wait event id (negative marker)
    if p_data[v_idx] >= 0 then
      raise warning 'ash.decode_sample: expected negative wait_id at position %', v_idx;
      return;
    end if;

    v_wait_id := -p_data[v_idx];
    v_idx := v_idx + 1;

    -- Get count
    if v_idx > v_len then
      raise warning 'ash.decode_sample: unexpected end of array at position %', v_idx;
      return;
    end if;

    v_count := p_data[v_idx];
    v_idx := v_idx + 1;

    -- Look up wait event info
    select w.type, w.event
    into v_type, v_event
    from ash.wait_event_map w
    where w.id = v_wait_id;

    -- Process each query_id
    for v_qid_idx in 1..v_count loop
      if v_idx > v_len then
        raise warning 'ash.decode_sample: not enough query_ids for count %', v_count;
        return;
      end if;

      v_map_id := p_data[v_idx];
      v_idx := v_idx + 1;

      -- Handle sentinel (0 = NULL query_id)
      if v_map_id = 0 then
        v_query_id := null;
      elsif p_slot is not null then
        select m.query_id into v_query_id
        from ash.query_map_all m
        where m.slot = p_slot and m.id = v_map_id;
      else
        -- No slot context — search all partitions (less efficient).
        -- WARNING: after rotation, the same id may exist in multiple
        -- partitions with different query_ids (independent sequences).
        -- Result is nondeterministic. Always pass p_slot when available.
        select m.query_id into v_query_id
        from ash.query_map_all m
        where m.id = v_map_id
        limit 1;
      end if;

      wait_id := v_wait_id;
      wait_event := case when v_event = v_type then v_event else v_type || ':' || v_event end;
      query_id := v_query_id;
      count := 1;
      return next;
    end loop;
  end loop;

  return;
end;
$$;


--------------------------------------------------------------------------------
-- STEP 3: Rotation function
--------------------------------------------------------------------------------

-- Rotate partitions: advance current_slot, truncate the old previous partition
-- and its matching query_map partition (lockstep TRUNCATE — zero bloat everywhere).
-- Uses dynamic SQL with modulo-N for configurable partition count.
create or replace function ash.rotate()
returns text
language plpgsql
as $$
declare
  v_old_slot smallint;
  v_new_slot smallint;
  v_truncate_slot smallint;
  v_num_partitions smallint;
  v_rotation_period interval;
  v_rotated_at timestamptz;
  v_rotation_minutes int;
begin
  -- Advisory lock prevents concurrent rotation from pg_cron overlap.
  -- Xact-level: auto-releases on commit/rollback — no leak risk with pg_cron
  -- connection reuse. rotate() is REVOKE'd from PUBLIC — only schema owner.
  if not pg_try_advisory_xact_lock(hashtext('ash_rotate')) then
    return 'skipped: another rotation in progress';
  end if;

  begin
    -- Get current config
    select current_slot, num_partitions, rotation_period, rotated_at
    into v_old_slot, v_num_partitions, v_rotation_period, v_rotated_at
    from ash.config
    where singleton;

    -- Check if we rotated too recently (within 90% of rotation_period)
    if now() - v_rotated_at < v_rotation_period * 0.9 then
      return 'skipped: rotated too recently at ' || v_rotated_at::text;
    end if;

    -- Set lock timeout to avoid blocking on long-running queries
    set local lock_timeout = '2s';

    -- Pre-truncation rollup: process endangered minutes before they are lost.
    -- Called before config update to avoid locking contention with take_sample().
    -- Rollup is idempotent (upsert), so double-processing is safe.
    v_rotation_minutes := extract(epoch from v_rotation_period)::int / 60;

    begin
      perform ash.rollup_minute(v_rotation_minutes);
    exception when undefined_function then
      -- rollup_minute() not yet installed (upgrade in progress), skip
      null;
    when others then
      raise warning 'ash.rotate: rollup_minute failed [%]: %', sqlstate, sqlerrm;
    end;

    -- Calculate new slot dynamically (0 -> 1 -> ... -> N-1 -> 0)
    v_new_slot := (v_old_slot + 1) % v_num_partitions;

    -- The partition to truncate is the one after the new slot
    v_truncate_slot := (v_new_slot + 1) % v_num_partitions;

    -- Advance current_slot first (before truncate)
    update ash.config
    set current_slot = v_new_slot,
      rotated_at = now()
    where singleton;

    -- Lockstep TRUNCATE: sample partition + matching query_map partition.
    -- Zero bloat everywhere — no DELETE, no dead tuples, no GC needed.
    -- Dynamic SQL replaces the old CASE block for N-partition support.
    execute format('truncate ash.sample_%s', v_truncate_slot);
    execute format('truncate ash.query_map_%s', v_truncate_slot);
    -- ALTER COLUMN id RESTART takes ACCESS EXCLUSIVE lock on query_map.
    -- Safe here because we just truncated the table — no concurrent readers.
    execute format(
      'alter table ash.query_map_%s alter column id restart', v_truncate_slot
    );

    return format('rotated: slot %s -> %s, truncated slot %s (sample + query_map)',
           v_old_slot, v_new_slot, v_truncate_slot);

  exception when lock_not_available then
    return 'failed: lock timeout on partition truncate, will retry next cycle';
  when others then
    raise;
  end;
end;
$$;


-- Rebuild partitions: destructive admin function to change partition count.
-- All raw sample data is lost. Rollup tables survive.
-- WARNING: failure after acquiring lock leaves sampling_enabled = false.
-- Manual recovery: UPDATE ash.config SET sampling_enabled = true; SELECT ash.start();
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

  -- Step 1: Mark sampling disabled. take_sample() checks this and returns early.
  update ash.config set sampling_enabled = false where singleton;

  -- Step 2: Stop pg_cron jobs if available
  if ash._pg_cron_available() then
    perform ash.stop();
  end if;

  -- Step 3: Acquire rebuild exclusive lock (two-key xact-level form).
  -- Xact-level: auto-releases on commit/rollback — no manual unlock needed.
  if not pg_try_advisory_xact_lock(1, hashtext('ash_rebuild')::int4) then
    update ash.config set sampling_enabled = true where singleton;
    raise exception 'rebuild_partitions: could not acquire lock — '
      'another rebuild is in progress';
  end if;

  -- Step 4: Drain — wait up to 5s for in-flight take_sample/rollup to finish.
  -- They hold (0, hashtext('ash_operation')) xact locks; we poll pg_locks.
  for i in 1 .. 10 loop
    if not exists (
      select from pg_locks
      where locktype = 'advisory'
        and classid = 0
        and objid = hashtext('ash_operation')::int4::oid
        and granted
    ) then
      exit;
    end if;

    perform pg_sleep(0.5);
  end loop;

  -- Step 5: Drop the query_map_all view first (depends on query_map tables)
  drop view if exists ash.query_map_all;

  -- Drop ALL existing sample partitions and query_maps.
  -- Uses catalog enumeration to catch orphaned tables.
  perform ash._drop_all_partitions();

  -- Step 6: Update config
  update ash.config
  set num_partitions = v_new_n,
    current_slot = 0,
    rotated_at = now()
  where singleton;

  -- Step 7: Create new partitions
  for i in 0 .. v_new_n - 1 loop
    execute format(
      'create table ash.sample_%s '
      'partition of ash.sample for values in (%s)', i, i
    );

    execute format(
      'create table ash.query_map_%s ('
      '  id       int4 primary key generated always as identity (start with 1),'
      '  query_id int8 not null unique'
      ')', i
    );

    execute format(
      'create index sample_%s_ts_idx on ash.sample_%s (sample_ts)', i, i
    );

    execute format(
      'create index sample_%s_datid_ts_idx on ash.sample_%s (datid, sample_ts)',
      i, i
    );
  end loop;

  -- Step 8: Rebuild the query_map_all view
  perform ash._rebuild_query_map_view();

  -- Step 9: Leave sampling DISABLED. User must explicitly call
  -- ash.start() to resume. Prevents accidental data collection
  -- into a partially-built schema.
  return format(
    'rebuilt: %s -> %s partitions. all raw data cleared. '
    'call ash.start() to resume sampling.',
    v_old_n, v_new_n
  );
end;
$$;


--------------------------------------------------------------------------------
-- STEP 4: Start/stop/uninstall functions
--------------------------------------------------------------------------------

-- Check if pg_cron extension is available
create or replace function ash._pg_cron_available()
returns boolean
language sql
stable
as $$
  select exists (
    select from pg_extension where extname = 'pg_cron'
  )
$$;

-- Start sampling: create pg_cron jobs
create or replace function ash.start(p_interval interval default '1 second')
returns table (job_type text, job_id bigint, status text)
language plpgsql
as $$
declare
  v_sampler_job bigint;
  v_rotation_job bigint;
  v_cron_version text;
  v_seconds int;
  v_hours int;
  v_schedule text;
  v_skip_nodename_update boolean := false;
begin
  -- Validate interval
  v_seconds := extract(epoch from p_interval)::int;
  if v_seconds < 1 then
    job_type := 'error';
    job_id := null;
    status := format('interval must be at least 1 second, got %s', p_interval);
    return next;
    return;
  end if;

  -- If pg_cron is not available, just record the interval and advise on external scheduling
  if not ash._pg_cron_available() then
    update ash.config
    set sample_interval = p_interval,
      sampling_enabled = true,
      skipped_samples = 0
    where singleton;

    job_type := 'sampler';
    job_id := null;
    status := format('interval set to %s — schedule externally (pg_cron not available)', p_interval);
    return next;

    job_type := 'rotation';
    job_id := null;
    status := format('rotation_period is %s — schedule ash.rotate() externally', (select rotation_period from ash.config where singleton));
    return next;

    job_type := 'rollup';
    job_id := null;
    status := 'schedule ash.rollup_minute() every minute, ash.rollup_hour() every hour, ash.rollup_cleanup() daily';
    return next;

    raise notice 'pg_cron is not installed. To sample, call ash.take_sample() from an external scheduler:';
    raise notice '  system cron:    * * * * * psql -qAtX -c "select ash.take_sample()" (for per-second, use a loop)';
    raise notice '  psql:           SELECT ash.take_sample() \watch 1';
    raise notice '  any language:   execute "SELECT ash.take_sample()" in a loop with sleep';
    raise notice 'Also schedule ash.rotate() at the rotation_period interval (default: daily).';
    raise notice 'Schedule rollup: ash.rollup_minute() every minute, ash.rollup_hour() every hour, ash.rollup_cleanup() daily.';

    return;
  end if;

  -- Check pg_cron version (need >= 1.5 for sub-minute scheduling)
  select extversion into v_cron_version
  from pg_extension where extname = 'pg_cron';

  if string_to_array(regexp_replace(v_cron_version, '[^0-9.]', '', 'g'), '.')::int[] < '{1,5}'::int[] then
    if v_seconds < 60 then
      job_type := 'error';
      job_id := null;
      status := format('pg_cron version %s too old for sub-minute scheduling (need >= 1.5). Use external scheduler or upgrade pg_cron.', v_cron_version);
      return next;
      return;
    end if;
  end if;

  -- Detect whether we need to UPDATE cron.job.nodename after scheduling.
  -- Skip when cron.use_background_workers = on (nodename irrelevant)
  -- or cron.host is already '' or a socket path (cron.schedule() inherits it).
  begin
    v_skip_nodename_update :=
      coalesce(current_setting('cron.use_background_workers', true), '') = 'on'
      or coalesce(current_setting('cron.host', true), 'localhost') = ''
      or coalesce(current_setting('cron.host', true), 'localhost') like '/%';
  exception when others then
    v_skip_nodename_update := false;
  end;

  -- Convert interval to pg_cron schedule format
  -- pg_cron supports: '[1-59] seconds' for sub-minute, or cron syntax for minute+

  -- Build schedule: seconds format for <60s, cron format for 60s+
  if v_seconds <= 59 then
    v_schedule := v_seconds || ' seconds';
  elsif v_seconds < 3600 then
    -- Convert to cron: every N minutes
    if v_seconds % 60 <> 0 then
      job_type := 'error';
      job_id := null;
      status := format('interval must be exact minutes (60s, 120s, etc.), got %s', p_interval);
      return next;
      return;
    end if;
    v_schedule := '*/' || (v_seconds / 60) || ' * * * *';
  else
    -- Convert to cron: every N hours (limit to 23 hours max for step syntax)
    if v_seconds % 3600 <> 0 then
      job_type := 'error';
      job_id := null;
      status := format('interval must be exact hours (3600s, 7200s, etc., up to 23h), got %s', p_interval);
      return next;
      return;
    end if;
    v_hours := v_seconds / 3600;
    if v_hours > 23 then
      job_type := 'error';
      job_id := null;
      status := format('interval exceeds maximum 23 hours (82800s), got %s = %s hours. Use days or shorter interval.', p_interval, v_hours);
      return next;
      return;
    end if;
    if v_hours = 1 then
      v_schedule := '0 * * * *';  -- Every hour at minute 0
    else
      v_schedule := '0 */' || v_hours || ' * * *';  -- Every N hours at minute 0
    end if;
  end if;

  -- Check for existing sampler job (idempotent)
  select jobid into v_sampler_job
  from cron.job
  where jobname = 'ash_sampler';

  if v_sampler_job is not null then
    job_type := 'sampler';
    job_id := v_sampler_job;
    status := 'already exists';
    return next;
  else
    -- Create sampler job
    select cron.schedule(
      'ash_sampler',
      v_schedule,
      'set statement_timeout = ''500ms''; select ash.take_sample()'
    ) into v_sampler_job;

    -- Clear nodename so pg_cron uses Unix socket instead of TCP.
    -- cron.schedule() sets nodename from cron.host GUC (default 'localhost'),
    -- which forces TCP and fails when pg_hba.conf only allows sockets.
    -- Skipped when cron.use_background_workers = on (no libpq connections)
    -- or cron.host is already '' / a socket path (already correct).
    if not v_skip_nodename_update then
      update cron.job set nodename = '' where jobid = v_sampler_job;
    end if;

    job_type := 'sampler';
    job_id := v_sampler_job;
    status := 'created';
    return next;
  end if;

  -- Check for existing rotation job (idempotent)
  select jobid into v_rotation_job
  from cron.job
  where jobname = 'ash_rotation';

  if v_rotation_job is not null then
    job_type := 'rotation';
    job_id := v_rotation_job;
    status := 'already exists';
    return next;
  else
    -- Create rotation job (daily at midnight UTC)
    select cron.schedule(
      'ash_rotation',
      '0 0 * * *',
      'select ash.rotate()'
    ) into v_rotation_job;

    if not v_skip_nodename_update then
      update cron.job set nodename = '' where jobid = v_rotation_job;
    end if;

    job_type := 'rotation';
    job_id := v_rotation_job;
    status := 'created';
    return next;
  end if;

  -- Schedule rollup cron jobs (idempotent: unschedule first)
  -- rollup_minute: every minute
  begin
    perform cron.unschedule('ash_rollup_1m');
  exception when others then
    null;
  end;

  select cron.schedule(
    'ash_rollup_1m',
    '* * * * *',
    'select ash.rollup_minute()'
  ) into v_rotation_job; -- reuse variable for job id

  if not v_skip_nodename_update then
    update cron.job set nodename = '' where jobid = v_rotation_job;
  end if;

  job_type := 'rollup_1m';
  job_id := v_rotation_job;
  status := 'created';
  return next;

  -- rollup_hour: every hour at minute 0
  begin
    perform cron.unschedule('ash_rollup_1h');
  exception when others then
    null;
  end;

  select cron.schedule(
    'ash_rollup_1h',
    '0 * * * *',
    'select ash.rollup_hour()'
  ) into v_rotation_job;

  if not v_skip_nodename_update then
    update cron.job set nodename = '' where jobid = v_rotation_job;
  end if;

  job_type := 'rollup_1h';
  job_id := v_rotation_job;
  status := 'created';
  return next;

  -- rollup_cleanup: daily at 03:00 UTC
  begin
    perform cron.unschedule('ash_rollup_gc');
  exception when others then
    null;
  end;

  select cron.schedule(
    'ash_rollup_gc',
    '0 3 * * *',
    'select ash.rollup_cleanup()'
  ) into v_rotation_job;

  if not v_skip_nodename_update then
    update cron.job set nodename = '' where jobid = v_rotation_job;
  end if;

  job_type := 'rollup_gc';
  job_id := v_rotation_job;
  status := 'created';
  return next;

  -- Update sample_interval, enable sampling, reset skip counter
  update ash.config
  set sample_interval = p_interval,
    sampling_enabled = true,
    skipped_samples = 0
  where singleton;

  -- Warn about pg_cron run history overhead.
  -- At 1s sampling, cron.job_run_details grows ~12 MiB/day unbounded.
  -- pg_cron has no built-in purge — only cron.log_run = off (disables entirely).
  begin
    if current_setting('cron.log_run', true)::bool then
      raise notice 'hint: pg_cron logs every sample to cron.job_run_details (~12 MiB/day).';
      raise notice 'to disable: alter system set cron.log_run = off; select pg_reload_conf();';
      raise notice 'or schedule periodic cleanup: delete from cron.job_run_details where end_time < now() - interval ''1 day'';';
    end if;
  exception when others then
    null; -- GUC not available
  end;

  return;
end;
$$;

-- Stop sampling: remove pg_cron jobs, disable sampling
create or replace function ash.stop()
returns table (job_type text, job_id bigint, status text)
language plpgsql
as $$
declare
  v_job_id bigint;
begin
  -- Mark sampling as disabled
  update ash.config set sampling_enabled = false where singleton;

  -- If pg_cron is not available, just remind about external scheduler
  if not ash._pg_cron_available() then
    job_type := 'info';
    job_id := null;
    status := 'pg_cron not installed — remember to stop your external scheduler (cron, systemd timer, loop script, etc.)';
    return next;
    return;
  end if;

  -- Remove sampler job
  select jobid into v_job_id
  from cron.job
  where jobname = 'ash_sampler';

  if v_job_id is not null then
    perform cron.unschedule('ash_sampler');
    job_type := 'sampler';
    job_id := v_job_id;
    status := 'removed';
    return next;
  end if;

  -- Remove rotation job
  select jobid into v_job_id
  from cron.job
  where jobname = 'ash_rotation';

  if v_job_id is not null then
    perform cron.unschedule('ash_rotation');
    job_type := 'rotation';
    job_id := v_job_id;
    status := 'removed';
    return next;
  end if;

  -- Remove rollup jobs (idempotent — tolerate missing jobs)
  begin
    perform cron.unschedule('ash_rollup_1m');
    job_type := 'rollup_1m';
    job_id := null;
    status := 'removed';
    return next;
  exception when others then
    null;
  end;

  begin
    perform cron.unschedule('ash_rollup_1h');
    job_type := 'rollup_1h';
    job_id := null;
    status := 'removed';
    return next;
  exception when others then
    null;
  end;

  begin
    perform cron.unschedule('ash_rollup_gc');
    job_type := 'rollup_gc';
    job_id := null;
    status := 'removed';
    return next;
  exception when others then
    null;
  end;

  return;
end;
$$;

-- Enable or disable debug logging in take_sample().
-- When enabled, every sampled session emits a RAISE LOG message:
--   ash.take_sample: pid=NNN state=active wait_type=Client wait_event=ClientRead ...
--
-- RAISE LOG goes to the server log only — never to the client.
-- It is independent of log_min_messages and client_min_messages.
--
-- Usage:
--   select ash.set_debug_logging(true);   -- enable
--   select ash.set_debug_logging(false);  -- disable
--   select ash.set_debug_logging();       -- show current state
create or replace function ash.set_debug_logging(p_enabled bool default null)
returns text
language plpgsql
as $$
declare
  v_current bool;
begin
  select debug_logging into v_current from ash.config where singleton;

  if p_enabled is null then
    return 'debug_logging = ' || v_current::text;
  end if;

  update ash.config set debug_logging = p_enabled where singleton;

  if p_enabled then
    return 'debug_logging enabled — each sampled session will emit RAISE LOG';
  else
    return 'debug_logging disabled';
  end if;
end;
$$;

-- Uninstall: stop jobs and drop schema
create or replace function ash.uninstall(p_confirm text default null)
returns text
language plpgsql
as $$
declare
  v_rec record;
  v_jobs_removed int := 0;
begin
  if p_confirm is distinct from 'yes' then
    raise exception 'to uninstall pg_ash, call: select ash.uninstall(''yes'')';
  end if;

  -- Stop pg_cron jobs first
  for v_rec in select * from ash.stop() loop
    if v_rec.status = 'removed' then
      v_jobs_removed := v_jobs_removed + 1;
    end if;
  end loop;

  -- Drop the schema
  drop schema ash cascade;

  return format('uninstalled: removed %s pg_cron jobs, dropped ash schema', v_jobs_removed);
end;
$$;


--------------------------------------------------------------------------------
-- STEP 5: Reader and diagnostic functions
--------------------------------------------------------------------------------

-- Helper to get active slots (current and previous)
create or replace function ash._active_slots()
returns smallint[]
language sql
stable
as $$
  select array[
    current_slot,
    ((current_slot - 1 + num_partitions) % num_partitions)::smallint
  ]
  from ash.config
  where singleton
$$;

-- Status: diagnostic dashboard
create or replace function ash.status()
returns table (
  metric text,
  value text
)
language plpgsql
stable
set jit = off
as $$
declare
  v_config record;
  v_last_sample_ts int4;
  v_samples_current int;
  v_samples_total int;
  v_wait_events int;
  v_query_ids int;
  v_rollup_1m_rows bigint;
  v_rollup_1h_rows bigint;
  v_rollup_1m_oldest int4;
  v_rollup_1m_newest int4;
  v_rollup_1h_oldest int4;
  v_rollup_1h_newest int4;
begin
  -- Get config
  select * into v_config from ash.config where singleton;

  -- Last sample timestamp
  select max(sample_ts) into v_last_sample_ts from ash.sample;

  -- Samples in current partition
  select count(*) into v_samples_current
  from ash.sample where slot = v_config.current_slot;

  -- Total samples
  select count(*) into v_samples_total from ash.sample;

  -- Dictionary sizes
  select count(*) into v_wait_events from ash.wait_event_map;
  select count(*) into v_query_ids from ash.query_map_all;

  -- Rollup stats (handle tables not yet existing during upgrade)
  begin
    select count(*), min(ts), max(ts)
    into v_rollup_1m_rows, v_rollup_1m_oldest, v_rollup_1m_newest
    from ash.rollup_1m;

    select count(*), min(ts), max(ts)
    into v_rollup_1h_rows, v_rollup_1h_oldest, v_rollup_1h_newest
    from ash.rollup_1h;
  exception when undefined_table then
    v_rollup_1m_rows := 0;
    v_rollup_1h_rows := 0;
  end;

  metric := 'version'; value := coalesce(v_config.version, '1.0'); return next;
  metric := 'color'; value := case when ash._color_on() then 'on' else 'off' end; return next;
  metric := 'num_partitions'; value := v_config.num_partitions::text; return next;
  metric := 'sampling_enabled'; value := v_config.sampling_enabled::text; return next;
  metric := 'skipped_samples'; value := v_config.skipped_samples::text; return next;
  metric := 'current_slot'; value := v_config.current_slot::text; return next;
  metric := 'sample_interval'; value := v_config.sample_interval::text; return next;
  metric := 'rotation_period'; value := v_config.rotation_period::text; return next;
  metric := 'raw_retention'; value := ((v_config.num_partitions - 2) * v_config.rotation_period)::text || ' + current partial'; return next;
  metric := 'include_bg_workers'; value := v_config.include_bg_workers::text; return next;
  metric := 'debug_logging'; value := v_config.debug_logging::text; return next;
  metric := 'missed_samples'; value := v_config.missed_samples::text; return next;
  metric := 'installed_at'; value := v_config.installed_at::text; return next;
  metric := 'rotated_at'; value := v_config.rotated_at::text; return next;
  metric := 'time_since_rotation'; value := (now() - v_config.rotated_at)::text; return next;

  if v_last_sample_ts is not null then
    metric := 'last_sample_ts'; value := ash.ts_to_timestamptz(v_last_sample_ts)::text; return next;
    metric := 'time_since_last_sample'; value := (now() - ash.ts_to_timestamptz(v_last_sample_ts))::text; return next;
  else
    metric := 'last_sample_ts'; value := 'no samples'; return next;
  end if;

  metric := 'samples_in_current_slot'; value := v_samples_current::text; return next;
  metric := 'samples_total'; value := v_samples_total::text; return next;
  metric := 'wait_event_map_count'; value := v_wait_events::text; return next;
  metric := 'wait_event_map_utilization'; value := round(v_wait_events::numeric / 32767 * 100, 2)::text || '%'; return next;
  metric := 'query_map_count'; value := v_query_ids::text; return next;

  -- Rollup metrics
  metric := 'rollup_1m_rows'; value := coalesce(v_rollup_1m_rows, 0)::text; return next;

  if v_rollup_1m_oldest is not null then
    metric := 'rollup_1m_oldest'; value := ash.ts_to_timestamptz(v_rollup_1m_oldest)::text; return next;
    metric := 'rollup_1m_newest'; value := ash.ts_to_timestamptz(v_rollup_1m_newest)::text; return next;
  end if;

  metric := 'rollup_1m_retention'; value := v_config.rollup_1m_retention_days || ' days'; return next;
  metric := 'rollup_1h_rows'; value := coalesce(v_rollup_1h_rows, 0)::text; return next;

  if v_rollup_1h_oldest is not null then
    metric := 'rollup_1h_oldest'; value := ash.ts_to_timestamptz(v_rollup_1h_oldest)::text; return next;
    metric := 'rollup_1h_newest'; value := ash.ts_to_timestamptz(v_rollup_1h_newest)::text; return next;
  end if;

  metric := 'rollup_1h_retention'; value := v_config.rollup_1h_retention_days || ' days'; return next;

  if v_config.last_rollup_1m_ts is not null then
    metric := 'last_rollup_1m_ts'; value := ash.ts_to_timestamptz(v_config.last_rollup_1m_ts)::text; return next;
  end if;

  if v_config.last_rollup_1h_ts is not null then
    metric := 'last_rollup_1h_ts'; value := ash.ts_to_timestamptz(v_config.last_rollup_1h_ts)::text; return next;
  end if;

  -- pg_cron status if available
  if ash._pg_cron_available() then
    metric := 'pg_cron_available'; value := 'yes'; return next;
    for metric, value in
      select 'cron_job_' || jobname,
         format('id=%s, schedule=%s, active=%s', jobid, schedule, active)
      from cron.job
      where jobname in (
        'ash_sampler', 'ash_rotation',
        'ash_rollup_1m', 'ash_rollup_1h', 'ash_rollup_gc'
      )
    loop
      return next;
    end loop;
  else
    metric := 'pg_cron_available'; value := 'no (use external scheduler)'; return next;
  end if;

  return;
end;
$$;

--------------------------------------------------------------------------------
-- STEP 6: Rollup tables and functions
--------------------------------------------------------------------------------

-- Minute-level rollup: aggregated samples per minute per database.
-- Survives raw partition rotation. Retained per rollup_1m_retention_days.
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

-- Hourly rollup: aggregated from minute rollups.
-- Retained per rollup_1h_retention_days (default 5 years).
create table if not exists ash.rollup_1h (
  ts              int4 not null,     -- hour-aligned epoch offset
  datid           oid not null,
  samples         smallint not null, -- sum of minute samples (max 3600)
  peak_backends   smallint not null, -- max per-database peak across the hour
  wait_counts     int4[] not null,
  query_counts    int8[] not null,
  primary key (ts, datid)
);

-- Merge multiple wait_counts arrays: sum counts for matching wait_ids.
-- Input: flat int4[] from array_agg(wait_counts) — PG concatenates
-- the pairs into one flat array. The function extracts id/count pairs
-- by position parity, groups by id, sums counts, and re-interleaves.
-- Uses CROSS JOIN LATERAL (VALUES ...) for correct pair ordering
-- (avoids the ORDER BY v DESC bug that swaps id/count when count > id).
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
    select id, sum(cnt)::int4 as total,
           row_number() over (order by sum(cnt) desc, id asc) as rn
    from pairs
    group by id
  ),
  interleaved as (
    select v, rn, sub
    from merged
    cross join lateral (values (1, id), (2, total)) as t(sub, v)
  )
  select coalesce(
    array_agg(v order by rn, sub),
    '{}'::int4[]
  )
  from interleaved
$$;

-- Merge multiple query_counts arrays: identical logic to _merge_wait_counts
-- above, but int8 typed. Kept separate for type safety (no polymorphic overhead).
create or replace function ash._merge_query_counts(p_flat int8[])
returns int8[]
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
    select id, sum(cnt)::int8 as total,
           row_number() over (order by sum(cnt) desc, id asc) as rn
    from pairs
    group by id
  ),
  interleaved as (
    select v, rn, sub
    from merged
    cross join lateral (values (1, id), (2, total)) as t(sub, v)
  )
  select coalesce(
    array_agg(v order by rn, sub),
    '{}'::int8[]
  )
  from interleaved
$$;

-- Truncate a paired array to top N entries by count.
-- Preserves [id, count] pairing correctly.
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
    select id, cnt,
           row_number() over (order by cnt desc, id asc) as rn
    from pairs
    order by cnt desc, id asc
    limit p_top
  ),
  interleaved as (
    select v, rn, sub
    from top_n
    cross join lateral (values (1, id), (2, cnt)) as t(sub, v)
  )
  select coalesce(
    array_agg(v order by rn, sub),
    '{}'::int8[]
  )
  from interleaved
$$;

-- Rollup minute: watermark-based aggregation of raw samples into minute rollups.
-- Processes all unprocessed complete minutes up to p_batch_limit.
-- Idempotent via ON CONFLICT DO UPDATE (upsert).
create or replace function ash.rollup_minute(
  p_batch_limit int default 60  -- max minutes to catch up per call
)
returns int
language plpgsql
as $$
declare
  v_last_ts int4;
  v_now_minute_ts int4;
  v_minute_start int4;
  v_minute_end int4;
  v_batch_remaining int;
  v_total int := 0;
  v_count int;
  v_min_backend_seconds smallint;
  v_has_later_data bool;
begin
  -- Acquire participation lock (xact-level)
  if not pg_try_advisory_xact_lock(0, hashtext('ash_operation')::int4) then
    return 0;
  end if;

  v_batch_remaining := p_batch_limit;

  select last_rollup_1m_ts, rollup_min_backend_seconds
  into v_last_ts, v_min_backend_seconds
  from ash.config where singleton;

  -- Current minute boundary (only process *complete* minutes)
  v_now_minute_ts := ash.ts_from_timestamptz(date_trunc('minute', now()));

  -- Initialize watermark if NULL (first run after install or upgrade).
  if v_last_ts is null then
    select (min(sample_ts) / 60) * 60
    into v_last_ts
    from ash.sample;

    if v_last_ts is null then
      return 0;  -- no samples at all
    end if;
  end if;

  -- Process each unprocessed complete minute
  v_minute_start := v_last_ts;

  while v_minute_start < v_now_minute_ts and v_batch_remaining > 0 loop
    v_minute_end := v_minute_start + 60;

    -- Decode samples once, then aggregate wait_counts and query_counts together.
    -- Previous version decoded samples 3x per datid (outer + 2 correlated subqueries).
    insert into ash.rollup_1m (
      ts, datid, samples, peak_backends, wait_counts, query_counts
    )
    with decoded as (
      select
        s.datid,
        s.sample_ts,
        s.active_count,
        (ash.decode_sample(s.data, s.slot)).*
      from ash.sample s
      where s.sample_ts >= v_minute_start
        and s.sample_ts < v_minute_end
    ),
    base as (
      select
        datid,
        count(distinct sample_ts)::smallint as samples,
        max(active_count)::smallint as peak_backends
      from decoded
      group by datid
    ),
    wait_agg as (
      -- Aggregate by canonical wait_event_map.id (includes state).
      -- Joining on wait_event text would match multiple map rows when the
      -- same (type, event) exists under multiple states (e.g., ClientRead
      -- under both 'active' and 'idle in transaction') and double-count.
      select
        d.datid,
        d.wait_id::int4 as wait_id,
        count(*)::int4 as cnt,
        row_number() over (
          partition by d.datid
          order by count(*) desc, d.wait_id asc
        ) as rn
      from decoded d
      group by d.datid, d.wait_id
    ),
    wait_interleaved as (
      select datid, v, rn, sub
      from wait_agg
      cross join lateral (values (1, wait_id), (2, cnt)) as t(sub, v)
    ),
    wait_arrays as (
      select
        datid,
        coalesce(array_agg(v order by rn, sub), '{}'::int4[]) as wait_counts
      from wait_interleaved
      group by datid
    ),
    query_agg as (
      select
        d.datid,
        d.query_id,
        count(*)::int8 as cnt,
        row_number() over (
          partition by d.datid
          order by count(*) desc, d.query_id asc
        ) as rn
      from decoded d
      where d.query_id is not null and d.query_id <> 0
      group by d.datid, d.query_id
      having count(*) >= v_min_backend_seconds
    ),
    query_top as (
      select datid, query_id, cnt, rn
      from query_agg
      where rn <= 100
    ),
    query_interleaved as (
      select datid, v, rn, sub
      from query_top
      cross join lateral (values (1, query_id), (2, cnt)) as t(sub, v)
    ),
    query_arrays as (
      select
        datid,
        coalesce(array_agg(v order by rn, sub), '{}'::int8[]) as query_counts
      from query_interleaved
      group by datid
    )
    select
      v_minute_start,
      b.datid,
      b.samples,
      b.peak_backends,
      coalesce(wa.wait_counts, '{}'::int4[]),
      coalesce(qa.query_counts, '{}'::int8[])
    from base b
    left join wait_arrays wa on wa.datid = b.datid
    left join query_arrays qa on qa.datid = b.datid
    on conflict (ts, datid) do update set
      samples = excluded.samples,
      peak_backends = excluded.peak_backends,
      wait_counts = excluded.wait_counts,
      query_counts = excluded.query_counts;

    get diagnostics v_count = row_count;

    -- Gap detection: no samples for this minute but later data exists
    if v_count = 0 then
      select exists (
        select from ash.sample where sample_ts >= v_minute_end
      ) into v_has_later_data;

      if v_has_later_data then
        raise warning 'ash.rollup_minute: gap at minute % — no samples but later data exists (data may have rotated before rollup)',
          ash.ts_to_timestamptz(v_minute_start);
      end if;
    end if;

    v_total := v_total + v_count;

    -- Advance watermark transactionally
    update ash.config
    set last_rollup_1m_ts = v_minute_end
    where singleton;

    v_minute_start := v_minute_end;
    v_batch_remaining := v_batch_remaining - 1;
  end loop;

  return v_total;
end;
$$;

-- Rollup hour: aggregate minute rollups into hourly rollups.
-- Watermark-based, idempotent via upsert.
create or replace function ash.rollup_hour()
returns int
language plpgsql
as $$
declare
  v_last_ts int4;
  v_now_hour_ts int4;
  v_hour_start int4;
  v_hour_end int4;
  v_batch_limit int := 24;
  v_total int := 0;
  v_count int;
begin
  -- Acquire participation lock (xact-level)
  if not pg_try_advisory_xact_lock(0, hashtext('ash_operation')::int4) then
    return 0;
  end if;

  select last_rollup_1h_ts into v_last_ts
  from ash.config where singleton;

  v_now_hour_ts := ash.ts_from_timestamptz(date_trunc('hour', now()));

  if v_last_ts is null then
    select (min(ts) / 3600) * 3600 into v_last_ts from ash.rollup_1m;

    if v_last_ts is null then
      return 0;
    end if;
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
      ash._merge_wait_counts(
        array_agg(wait_counts) filter (where wait_counts <> '{}')
      ),
      ash._truncate_pairs(
        ash._merge_query_counts(
          array_agg(query_counts) filter (where query_counts <> '{}')
        ),
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
end;
$$;

-- Rollup cleanup: delete expired rows based on retention config.
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
end;
$$;

--------------------------------------------------------------------------------
-- STEP 7: Rollup reader functions
--------------------------------------------------------------------------------

-- Wait event trends from minute rollups (absolute time range — base implementation)
create or replace function ash.minute_waits_at(
  p_start timestamptz,
  p_end timestamptz,
  p_limit int default 10
)
returns table (
  wait_event text,
  backend_seconds bigint,
  pct numeric,
  bar text
)
language plpgsql
stable
set jit = off
as $$
declare
  v_start_ts int4;
  v_end_ts int4;
begin
  v_start_ts := ash.ts_from_timestamptz(p_start);
  v_end_ts := ash.ts_from_timestamptz(p_end);

  return query
  with raw_pairs as (
    select
      (row_number() over ()) as pos,
      val::int4 as val
    from ash.rollup_1m r,
      lateral unnest(r.wait_counts) as val
    where r.ts >= v_start_ts and r.ts < v_end_ts
  ),
  pairs as (
    select n1.val as wait_id, n2.val as cnt
    from raw_pairs n1
    join raw_pairs n2 on n2.pos = n1.pos + 1
    where n1.pos % 2 = 1
  ),
  summed as (
    select
      wait_id,
      sum(cnt)::bigint as total
    from pairs
    group by wait_id
  ),
  grand_total as (
    select sum(total) as gt from summed
  )
  select
    wm.state || '/' || wm.type || '/' || wm.event,
    s.total,
    round(s.total * 100.0 / nullif(gt.gt, 0), 1),
    repeat('#', (s.total * 40 / nullif(gt.gt, 0))::int)
  from summed s
  join ash.wait_event_map wm on wm.id = s.wait_id
  cross join grand_total gt
  order by s.total desc
  limit p_limit;
end;
$$;

-- Wait event trends from minute rollups (interval wrapper)
create or replace function ash.minute_waits(
  p_interval interval default '1 hour',
  p_limit int default 10
)
returns table (
  wait_event text,
  backend_seconds bigint,
  pct numeric,
  bar text
)
language sql
stable
set jit = off
as $$
  select * from ash.minute_waits_at(now() - p_interval, now(), p_limit)
$$;

-- Query trends from hourly rollups (absolute time range — base implementation)
create or replace function ash.hourly_queries_at(
  p_start timestamptz,
  p_end timestamptz,
  p_limit int default 10
)
returns table (
  query_id bigint,
  backend_seconds bigint,
  pct numeric,
  query_text text
)
language plpgsql
stable
set jit = off
as $$
declare
  v_start_ts int4;
  v_end_ts int4;
  v_has_pgss bool;
  v_rec record;
  v_qtext text;
begin
  v_start_ts := ash.ts_from_timestamptz(p_start);
  v_end_ts := ash.ts_from_timestamptz(p_end);

  select exists (select from pg_extension where extname = 'pg_stat_statements')
  into v_has_pgss;

  for v_rec in
    with raw_pairs as (
      select
        (row_number() over ()) as pos,
        val
      from ash.rollup_1h r,
        lateral unnest(r.query_counts) as val
      where r.ts >= v_start_ts and r.ts < v_end_ts
    ),
    pairs as (
      select n1.val as qid, n2.val as cnt
      from raw_pairs n1
      join raw_pairs n2 on n2.pos = n1.pos + 1
      where n1.pos % 2 = 1
    ),
    summed as (
      select
        qid,
        sum(cnt)::bigint as total
      from pairs
      group by qid
    ),
    grand_total as (
      select sum(total) as gt from summed
    )
    select
      s.qid,
      s.total,
      round(s.total * 100.0 / nullif(gt.gt, 0), 1) as p
    from summed s
    cross join grand_total gt
    order by s.total desc
    limit p_limit
  loop
    query_id := v_rec.qid;
    backend_seconds := v_rec.total;
    pct := v_rec.p;
    query_text := null;

    if v_has_pgss then
      begin
        execute 'select left(query, 80) from pg_stat_statements where queryid = $1 limit 1'
        into v_qtext using v_rec.qid;
        query_text := v_qtext;
      exception when others then
        null;
      end;
    end if;

    return next;
  end loop;
end;
$$;

-- Query trends from hourly rollups (interval wrapper)
create or replace function ash.hourly_queries(
  p_interval interval default '1 day',
  p_limit int default 10
)
returns table (
  query_id bigint,
  backend_seconds bigint,
  pct numeric,
  query_text text
)
language sql
stable
set jit = off
as $$
  select * from ash.hourly_queries_at(now() - p_interval, now(), p_limit)
$$;

-- Peak concurrency per day from hourly rollups (absolute time range — base implementation)
create or replace function ash.daily_peak_backends_at(
  p_start timestamptz,
  p_end timestamptz
)
returns table (
  day date,
  peak_backends int,
  avg_backends numeric
)
language plpgsql
stable
set jit = off
as $$
declare
  v_start_ts int4;
  v_end_ts int4;
begin
  v_start_ts := ash.ts_from_timestamptz(p_start);
  v_end_ts := ash.ts_from_timestamptz(p_end);

  return query
  select
    (ash.ts_to_timestamptz(r.ts))::date as d,
    max(r.peak_backends)::int,
    round(avg(r.peak_backends), 1)
  from ash.rollup_1h r
  where r.ts >= v_start_ts and r.ts < v_end_ts
  group by d
  order by d;
end;
$$;

-- Peak concurrency per day from hourly rollups (interval wrapper)
create or replace function ash.daily_peak_backends(
  p_interval interval default '7 days'
)
returns table (
  day date,
  peak_backends int,
  avg_backends numeric
)
language sql
stable
set jit = off
as $$
  select * from ash.daily_peak_backends_at(now() - p_interval, now())
$$;


--------------------------------------------------------------------------------
-- STEP 8: Existing reader functions (raw samples)
--------------------------------------------------------------------------------

-- Top wait events (inline SQL decode — no plpgsql per-row overhead)
-------------------------------------------------------------------------------
-- Wait event color mapping (24-bit RGB, aligned with PostgresAI monitoring)
--
--   Wait type       Color          RGB
--   ─────────────   ─────────────  ───────────────
--   CPU*            green          80, 250, 123
--   IdleTx          light yellow   241, 250, 140
--   IO              vivid blue     30, 100, 255
--   Lock            red            255, 85, 85
--   LWLock          pink           255, 121, 198
--   IPC             cyan           0, 200, 255
--   Client          yellow         255, 220, 100
--   Timeout         orange         255, 165, 0
--   BufferPin       teal           0, 210, 180
--   Activity        purple         150, 100, 255
--   Extension       light purple   190, 150, 255
--   Unknown/Other   gray           180, 180, 180
--
-- Uses 24-bit RGB escape codes (\033[38;2;R;G;Bm) for consistent rendering
-- across terminal themes (light, dark, solarized, etc.).
-- Colors: off by default. Enable per-call (p_color := true) or per-session:
--   set ash.color = on;
-- The session GUC avoids passing p_color to every function call.
-------------------------------------------------------------------------------

-- Resolve effective color state: explicit param wins, then session GUC.
create or replace function ash._color_on(p_color boolean default false)
returns boolean
language sql
stable
as $$
  select p_color or coalesce(current_setting('ash.color', true), '') in ('on', 'true', '1');
$$;

create or replace function ash._wait_color(p_event text, p_color boolean default false)
returns text
language sql
stable
as $$
  -- All escapes padded to 19 chars: \033[38;2;RRR;GGG;BBBm
  -- Uniform length prevents pspg right-border misalignment.
  select case when not ash._color_on(p_color) then '' else
    case
      when p_event like 'CPU%' then E'\033[38;2;080;250;123m'         -- green
      when p_event like 'IdleTx%' then E'\033[38;2;241;250;140m'      -- light yellow
      when p_event like 'IO:%' then E'\033[38;2;030;100;255m'         -- vivid blue
      when p_event like 'Lock:%' then E'\033[38;2;255;085;085m'       -- red
      when p_event like 'LWLock:%' then E'\033[38;2;255;121;198m'     -- pink
      when p_event like 'IPC:%' then E'\033[38;2;000;200;255m'        -- cyan
      when p_event like 'Client:%' then E'\033[38;2;255;220;100m'     -- yellow
      when p_event like 'Timeout:%' then E'\033[38;2;255;165;000m'    -- orange
      when p_event like 'BufferPin:%' then E'\033[38;2;000;210;180m'  -- teal
      when p_event like 'Activity:%' then E'\033[38;2;150;100;255m'   -- purple
      when p_event like 'Extension:%' then E'\033[38;2;190;150;255m'  -- light purple
      else E'\033[38;2;180;180;180m'                                   -- gray (unknown)
    end
  end;
$$;

-- Convenience: reset code, empty when color off
create or replace function ash._reset(p_color boolean default false)
returns text
language sql
stable
as $$
  select case when ash._color_on(p_color) then E'\033[0m' else '' end;
$$;

-- Build a bar string with fixed visible width (for pspg/column alignment).
-- Visible: [blocks padded to p_width] + ' ' + pct + '%'
-- Invisible ANSI codes don't affect visual width.
create or replace function ash._bar(
  p_event text,
  p_pct numeric,
  p_max_pct numeric,
  p_width int,
  p_color boolean default false
)
returns text
language sql
stable
as $$
  -- All color escapes are now exactly 19 chars (zero-padded RGB).
  -- reset is always 4 chars. Total invisible = 23 when color on, 0 when off.
  select ash._wait_color(p_event, p_color)
    || rpad(
         repeat('█', greatest(1, (p_pct / nullif(p_max_pct, 0) * p_width)::int)),
         p_width
       )
    || ash._reset(p_color)
    || lpad(p_pct || '%', 8);
$$;

create or replace function ash.top_waits(
  p_interval interval default '1 hour',
  p_limit int default 10,
  p_width int default 40,
  p_color boolean default false
)
returns table (
  wait_event text,
  samples bigint,
  pct numeric,
  bar text
)
language sql
stable
set jit = off
as $$
  with waits as (
    select
      case when wm.event = wm.type then wm.event else wm.type || ':' || wm.event end as wait_event,
      s.data[i + 1] as cnt
    from ash.sample s, generate_subscripts(s.data, 1) i,
         ash.wait_event_map wm
    where wm.id = (-s.data[i])::smallint
      and s.slot = any(ash._active_slots())
      and s.sample_ts >= extract(epoch from now() - p_interval - ash.epoch())::int4
      and s.data[i] < 0
  ),
  totals as (
    select w.wait_event, sum(w.cnt) as cnt
    from waits w
    group by w.wait_event
  ),
  grand_total as (
    select sum(cnt) as total from totals
  ),
  ranked as (
    select
      t.wait_event,
      t.cnt,
      row_number() over (order by t.cnt desc) as rn
    from totals t
  ),
  top_rows as (
    select wait_event, cnt, rn from ranked where rn <= p_limit
  ),
  other as (
    select 'Other'::text as wait_event, sum(cnt) as cnt, (p_limit + 1)::bigint as rn
    from ranked where rn > p_limit
    having sum(cnt) > 0
  ),
  max_pct as (
    select max(round(r.cnt::numeric / gt.total * 100, 2)) as m
    from (select * from top_rows union all select * from other) r
    cross join grand_total gt
  )
  select
    r.wait_event,
    r.cnt as samples,
    round(r.cnt::numeric / gt.total * 100, 2) as pct,
    ash._bar(r.wait_event, round(r.cnt::numeric / gt.total * 100, 2), mp.m, p_width, p_color) as bar
  from (select * from top_rows union all select * from other) r
  cross join grand_total gt
  cross join max_pct mp
  order by r.rn
$$;

-- Wait event timeline (time-bucketed breakdown, inline SQL decode)
create or replace function ash.wait_timeline(
  p_interval interval default '1 hour',
  p_bucket interval default '1 minute'
)
returns table (
  bucket_start timestamptz,
  wait_event text,
  samples bigint
)
language sql
stable
set jit = off
as $$
  with waits as (
    select
      ash.epoch() + (s.sample_ts - (s.sample_ts % extract(epoch from p_bucket)::int4)) * interval '1 second' as bucket,
      (-s.data[i])::smallint as wait_id,
      s.data[i + 1] as cnt
    from ash.sample s, generate_subscripts(s.data, 1) i
    where s.slot = any(ash._active_slots())
      and s.sample_ts >= extract(epoch from now() - p_interval - ash.epoch())::int4
      and s.data[i] < 0
  )
  select
    w.bucket as bucket_start,
    case when wm.event = wm.type then wm.event else wm.type || ':' || wm.event end as wait_event,
    sum(w.cnt) as samples
  from waits w
  join ash.wait_event_map wm on wm.id = w.wait_id
  group by w.bucket, wm.type, wm.event
  order by w.bucket, sum(w.cnt) desc
$$;

-- Top queries by wait samples (inline SQL decode)
-- Extracts individual query_map_ids from the encoded array.
-- Format: [-wid, count, qid, qid, ..., -wid, count, qid, ...]
-- A query_id position is: data[i] >= 0 AND data[i-1] >= 0 AND i > 1
-- (i > 1 guards against data[0] which is NULL in 1-indexed arrays)
create or replace function ash.top_queries(
  p_interval interval default '1 hour',
  p_limit int default 10
)
returns table (
  query_id bigint,
  samples bigint,
  pct numeric,
  query_text text
)
language plpgsql
stable
set jit = off
as $$
declare
  v_has_pgss boolean := false;
begin
  -- Probe the view directly — extension installed <> shared library loaded
  begin
    perform 1 from pg_stat_statements limit 1;
    v_has_pgss := true;
  exception when others then
    v_has_pgss := false;
  end;

  if v_has_pgss then
    return query
    with qids as (
      select s.slot, s.data[i] as map_id
      from ash.sample s, generate_subscripts(s.data, 1) i
      where s.slot = any(ash._active_slots())
        and s.sample_ts >= extract(epoch from now() - p_interval - ash.epoch())::int4
        and i > 1                   -- guard: data[0] is NULL
        and s.data[i] >= 0          -- not a wait_id marker
        and s.data[i - 1] >= 0      -- not a count (count follows negative marker)
    ),
    resolved as (
      select qm.query_id, count(*) as cnt
      from qids q
      join ash.query_map_all qm on qm.slot = q.slot and qm.id = q.map_id
      where q.map_id > 0  -- skip sentinel 0 (NULL query_id)
      group by qm.query_id
    ),
    grand_total as (
      select sum(cnt) as total from resolved
    )
    select
      r.query_id,
      r.cnt as samples,
      round(r.cnt::numeric / gt.total * 100, 2) as pct,
      left(pss.query, 100) as query_text
    from resolved r
    cross join grand_total gt
    left join pg_stat_statements pss on pss.queryid = r.query_id
    order by r.cnt desc
    limit p_limit;
  else
    raise warning 'pg_stat_statements is not installed — query_text will be NULL. Run: CREATE EXTENSION pg_stat_statements;';
    return query
    with qids as (
      select s.slot, s.data[i] as map_id
      from ash.sample s, generate_subscripts(s.data, 1) i
      where s.slot = any(ash._active_slots())
        and s.sample_ts >= extract(epoch from now() - p_interval - ash.epoch())::int4
        and i > 1
        and s.data[i] >= 0
        and s.data[i - 1] >= 0
    ),
    resolved as (
      select qm.query_id, count(*) as cnt
      from qids q
      join ash.query_map_all qm on qm.slot = q.slot and qm.id = q.map_id
      where q.map_id > 0
      group by qm.query_id
    ),
    grand_total as (
      select sum(cnt) as total from resolved
    )
    select
      r.query_id,
      r.cnt as samples,
      round(r.cnt::numeric / gt.total * 100, 2) as pct,
      null::text as query_text
    from resolved r
    cross join grand_total gt
    order by r.cnt desc
    limit p_limit;
  end if;
end;
$$;

-- Wait event type distribution
create or replace function ash.top_by_type(
  p_interval interval default '1 hour',
  p_width int default 40,
  p_color boolean default false
)
returns table (
  wait_event_type text,
  samples bigint,
  pct numeric,
  bar text
)
language sql
stable
set jit = off
as $$
  with waits as (
    select
      (-s.data[i])::smallint as wait_id,
      s.data[i + 1] as cnt
    from ash.sample s, generate_subscripts(s.data, 1) i
    where s.slot = any(ash._active_slots())
      and s.sample_ts >= extract(epoch from now() - p_interval - ash.epoch())::int4
      and s.data[i] < 0
  ),
  totals as (
    select wm.type as wait_type, sum(w.cnt) as cnt
    from waits w
    join ash.wait_event_map wm on wm.id = w.wait_id
    group by wm.type
  ),
  grand_total as (
    select sum(cnt) as total from totals
  ),
  max_pct as (
    select max(round(t.cnt::numeric / gt.total * 100, 2)) as m
    from totals t cross join grand_total gt
  )
  select
    t.wait_type as wait_event_type,
    t.cnt as samples,
    round(t.cnt::numeric / gt.total * 100, 2) as pct,
    ash._bar(t.wait_type || ':*', round(t.cnt::numeric / gt.total * 100, 2), mp.m, p_width, p_color) as bar
  from totals t, grand_total gt, max_pct mp
  order by t.cnt desc
$$;

-- Samples by database
-- Top queries with text from pg_stat_statements
-- Returns query text and pgss stats when pg_stat_statements is available,
-- NULL columns otherwise.
drop function if exists ash.top_queries_with_text(interval, int);
create or replace function ash.top_queries_with_text(
  p_interval interval default '1 hour',
  p_limit int default 10
)
returns table (
  query_id bigint,
  samples bigint,
  pct numeric,
  calls bigint,
  total_exec_time_ms numeric,
  mean_exec_time_ms numeric,
  query_text text
)
language plpgsql
stable
set jit = off
as $$
declare
  v_has_pgss boolean := false;
begin
  -- Probe the view directly — extension installed <> shared library loaded
  begin
    perform 1 from pg_stat_statements limit 1;
    v_has_pgss := true;
  exception when others then
    v_has_pgss := false;
  end;

  if v_has_pgss then
    return query
    with qids as (
      select s.slot, s.data[i] as map_id
      from ash.sample s, generate_subscripts(s.data, 1) i
      where s.slot = any(ash._active_slots())
        and s.sample_ts >= extract(epoch from now() - p_interval - ash.epoch())::int4
        and i > 1
        and s.data[i] >= 0
        and s.data[i - 1] >= 0
    ),
    resolved as (
      select qm.query_id, count(*) as cnt
      from qids q
      join ash.query_map_all qm on qm.slot = q.slot and qm.id = q.map_id
      where q.map_id > 0
      group by qm.query_id
    ),
    grand_total as (
      select sum(cnt) as total from resolved
    )
    select
      r.query_id,
      r.cnt as samples,
      round(r.cnt::numeric / gt.total * 100, 2) as pct,
      pss.calls,
      round(pss.total_exec_time::numeric, 2) as total_exec_time_ms,
      round(pss.mean_exec_time::numeric, 2) as mean_exec_time_ms,
      left(pss.query, 200) as query_text
    from resolved r
    cross join grand_total gt
    left join pg_stat_statements pss on pss.queryid = r.query_id
    order by r.cnt desc
    limit p_limit;
  else
    raise exception 'pg_stat_statements extension is not installed. Run: CREATE EXTENSION pg_stat_statements;'
      using hint = 'top_queries_with_text() requires pg_stat_statements for query text and execution metrics. Use top_queries() for sample-only data without pg_stat_statements.';
  end if;
end;
$$;

-- Wait profile for a specific query — what is this query waiting on?
-- Walks the encoded arrays, finds the query_map_id, then looks back to find
-- which wait group it belongs to (the nearest preceding negative element).
create or replace function ash.query_waits(
  p_query_id bigint,
  p_interval interval default '1 hour',
  p_width int default 40,
  p_color boolean default false
)
returns table (
  wait_event text,
  samples bigint,
  pct numeric,
  bar text
)
language plpgsql
stable
set jit = off
as $$
begin
  -- Check if this query_id exists in any partition
  if not exists (select from ash.query_map_all where query_id = p_query_id) then
    return;
  end if;

  return query
  with map_ids as (
    -- All (slot, id) pairs for this query across partitions
    select slot, id from ash.query_map_all where query_id = p_query_id
  ),
  hits as (
    -- Find every position where the query appears in a sample,
    -- then walk backwards to find the wait group marker
    select
      (select (- s.data[j])::smallint
      from generate_series(i, 1, -1) j
      where s.data[j] < 0
      limit 1
      ) as wait_id
    from ash.sample s, generate_subscripts(s.data, 1) i
    where s.slot = any(ash._active_slots())
      and s.sample_ts >= extract(epoch from now() - p_interval - ash.epoch())::int4
      and i > 1
      and s.data[i] >= 0
      and s.data[i - 1] >= 0  -- it's a query_id position, not a count
      and exists (select from map_ids m where m.slot = s.slot and m.id = s.data[i])
  ),
  named_hits as (
    select
      case when wm.event = wm.type then wm.event else wm.type || ':' || wm.event end as evt
    from hits h
    join ash.wait_event_map wm on wm.id = h.wait_id
    where h.wait_id is not null
  ),
  totals as (
    select evt, count(*) as cnt
    from named_hits
    group by evt
  ),
  grand_total as (
    select sum(cnt) as total from totals
  ),
  max_pct as (
    select max(round(t.cnt::numeric / gt.total * 100, 2)) as m
    from totals t cross join grand_total gt
  )
  select
    t.evt,
    t.cnt as samples,
    round(t.cnt::numeric / gt.total * 100, 2) as pct,
    ash._bar(t.evt, round(t.cnt::numeric / gt.total * 100, 2), mp.m, p_width, p_color) as bar
  from totals t
  cross join grand_total gt
  cross join max_pct mp
  order by t.cnt desc;
end;
$$;

create or replace function ash.samples_by_database(
  p_interval interval default '1 hour'
)
returns table (
  database_name text,
  datid oid,
  samples bigint,
  total_backends bigint
)
language sql
stable
set jit = off
as $$
  select
    coalesce(d.datname, '<background>') as database_name,
    s.datid,
    count(*) as samples,
    sum(s.active_count) as total_backends
  from ash.sample s
  left join pg_database d on d.oid = s.datid
  where s.slot = any(ash._active_slots())
    and s.sample_ts >= extract(epoch from now() - p_interval - ash.epoch())::int4
  group by s.datid, d.datname
  order by total_backends desc
$$;

-------------------------------------------------------------------------------
-- Absolute time range functions — for incident investigation
-------------------------------------------------------------------------------

-- Backward-compat alias: old upgrade scripts (1.1-to-1.2, 1.2-to-1.3) reference
-- this name in function bodies. Keep as a thin wrapper so re-applying old upgrade
-- scripts on a v1.4 database doesn't fail. New code should use ts_from_timestamptz.
create or replace function ash._to_sample_ts(p_ts timestamptz)
returns int4
language sql
immutable
parallel safe
as $$
  select ash.ts_from_timestamptz(p_ts)
$$;

-- Top waits in an absolute time range
create or replace function ash.top_waits_at(
  p_start timestamptz,
  p_end timestamptz,
  p_limit int default 10,
  p_width int default 40,
  p_color boolean default false
)
returns table (
  wait_event text,
  samples bigint,
  pct numeric,
  bar text
)
language sql
stable
set jit = off
as $$
  with waits as (
    select
      case when wm.event = wm.type then wm.event else wm.type || ':' || wm.event end as wait_event,
      s.data[i + 1] as cnt
    from ash.sample s, generate_subscripts(s.data, 1) i,
         ash.wait_event_map wm
    where wm.id = (-s.data[i])::smallint
      and s.slot = any(ash._active_slots())
      and s.sample_ts >= ash.ts_from_timestamptz(p_start)
      and s.sample_ts < ash.ts_from_timestamptz(p_end)
      and s.data[i] < 0
  ),
  totals as (
    select w.wait_event, sum(w.cnt) as cnt
    from waits w
    group by w.wait_event
  ),
  grand_total as (
    select sum(cnt) as total from totals
  ),
  ranked as (
    select
      t.wait_event,
      t.cnt,
      row_number() over (order by t.cnt desc) as rn
    from totals t
  ),
  top_rows as (
    select wait_event, cnt, rn from ranked where rn <= p_limit
  ),
  other as (
    select 'Other'::text as wait_event, sum(cnt) as cnt, (p_limit + 1)::bigint as rn
    from ranked where rn > p_limit
    having sum(cnt) > 0
  ),
  max_pct as (
    select max(round(r.cnt::numeric / gt.total * 100, 2)) as m
    from (select * from top_rows union all select * from other) r
    cross join grand_total gt
  )
  select
    r.wait_event,
    r.cnt as samples,
    round(r.cnt::numeric / gt.total * 100, 2) as pct,
    ash._bar(r.wait_event, round(r.cnt::numeric / gt.total * 100, 2), mp.m, p_width, p_color) as bar
  from (select * from top_rows union all select * from other) r
  cross join grand_total gt
  cross join max_pct mp
  order by r.rn
$$;

-- Top queries in an absolute time range
create or replace function ash.top_queries_at(
  p_start timestamptz,
  p_end timestamptz,
  p_limit int default 10
)
returns table (
  query_id bigint,
  samples bigint,
  pct numeric,
  query_text text
)
language plpgsql
stable
set jit = off
as $$
declare
  v_has_pgss boolean := false;
  v_start int4 := ash.ts_from_timestamptz(p_start);
  v_end int4 := ash.ts_from_timestamptz(p_end);
begin
  -- Probe the view directly — extension installed <> shared library loaded
  begin
    perform 1 from pg_stat_statements limit 1;
    v_has_pgss := true;
  exception when others then
    v_has_pgss := false;
  end;

  if v_has_pgss then
    return query
    with qids as (
      select s.slot, s.data[i] as map_id
      from ash.sample s, generate_subscripts(s.data, 1) i
      where s.slot = any(ash._active_slots())
        and s.sample_ts >= v_start and s.sample_ts < v_end
        and i > 1 and s.data[i] >= 0 and s.data[i - 1] >= 0
    ),
    resolved as (
      select qm.query_id, count(*) as cnt
      from qids q
      join ash.query_map_all qm on qm.slot = q.slot and qm.id = q.map_id
      where q.map_id > 0
      group by qm.query_id
    ),
    grand_total as (
      select sum(cnt) as total from resolved
    )
    select r.query_id, r.cnt, round(r.cnt::numeric / gt.total * 100, 2),
       left(pss.query, 100)
    from resolved r
    cross join grand_total gt
    left join pg_stat_statements pss on pss.queryid = r.query_id
    order by r.cnt desc limit p_limit;
  else
    raise warning 'pg_stat_statements is not installed — query_text will be NULL. Run: CREATE EXTENSION pg_stat_statements;';
    return query
    with qids as (
      select s.slot, s.data[i] as map_id
      from ash.sample s, generate_subscripts(s.data, 1) i
      where s.slot = any(ash._active_slots())
        and s.sample_ts >= v_start and s.sample_ts < v_end
        and i > 1 and s.data[i] >= 0 and s.data[i - 1] >= 0
    ),
    resolved as (
      select qm.query_id, count(*) as cnt
      from qids q
      join ash.query_map_all qm on qm.slot = q.slot and qm.id = q.map_id
      where q.map_id > 0
      group by qm.query_id
    ),
    grand_total as (
      select sum(cnt) as total from resolved
    )
    select r.query_id, r.cnt, round(r.cnt::numeric / gt.total * 100, 2),
       null::text
    from resolved r
    cross join grand_total gt
    order by r.cnt desc limit p_limit;
  end if;
end;
$$;

-- Wait timeline in an absolute time range
create or replace function ash.wait_timeline_at(
  p_start timestamptz,
  p_end timestamptz,
  p_bucket interval default '1 minute'
)
returns table (
  bucket_start timestamptz,
  wait_event text,
  samples bigint
)
language sql
stable
set jit = off
as $$
  with waits as (
    select
      ash.epoch() + (s.sample_ts - (s.sample_ts % extract(epoch from p_bucket)::int4)) * interval '1 second' as bucket,
      (-s.data[i])::smallint as wait_id,
      s.data[i + 1] as cnt
    from ash.sample s, generate_subscripts(s.data, 1) i
    where s.slot = any(ash._active_slots())
      and s.sample_ts >= ash.ts_from_timestamptz(p_start)
      and s.sample_ts < ash.ts_from_timestamptz(p_end)
      and s.data[i] < 0
  )
  select
    w.bucket as bucket_start,
    case when wm.event = wm.type then wm.event else wm.type || ':' || wm.event end as wait_event,
    sum(w.cnt) as samples
  from waits w
  join ash.wait_event_map wm on wm.id = w.wait_id
  group by w.bucket, wm.type, wm.event
  order by w.bucket, sum(w.cnt) desc
$$;

-- Wait event type distribution in an absolute time range
create or replace function ash.top_by_type_at(
  p_start timestamptz,
  p_end timestamptz,
  p_width int default 40,
  p_color boolean default false
)
returns table (
  wait_event_type text,
  samples bigint,
  pct numeric,
  bar text
)
language sql
stable
set jit = off
as $$
  with waits as (
    select (-s.data[i])::smallint as wait_id, s.data[i + 1] as cnt
    from ash.sample s, generate_subscripts(s.data, 1) i
    where s.slot = any(ash._active_slots())
      and s.sample_ts >= ash.ts_from_timestamptz(p_start)
      and s.sample_ts < ash.ts_from_timestamptz(p_end)
      and s.data[i] < 0
  ),
  totals as (
    select wm.type as wait_type, sum(w.cnt) as cnt
    from waits w join ash.wait_event_map wm on wm.id = w.wait_id
    group by wm.type
  ),
  grand_total as (
    select sum(cnt) as total from totals
  ),
  max_pct as (
    select max(round(t.cnt::numeric / gt.total * 100, 2)) as m
    from totals t cross join grand_total gt
  )
  select
    t.wait_type,
    t.cnt,
    round(t.cnt::numeric / gt.total * 100, 2),
    ash._bar(t.wait_type || ':*', round(t.cnt::numeric / gt.total * 100, 2), mp.m, p_width, p_color)
  from totals t, grand_total gt, max_pct mp
  order by t.cnt desc
$$;

-- Query waits in an absolute time range
create or replace function ash.query_waits_at(
  p_query_id bigint,
  p_start timestamptz,
  p_end timestamptz,
  p_width int default 40,
  p_color boolean default false
)
returns table (
  wait_event text,
  samples bigint,
  pct numeric,
  bar text
)
language plpgsql
stable
set jit = off
as $$
declare
  v_start int4 := ash.ts_from_timestamptz(p_start);
  v_end int4 := ash.ts_from_timestamptz(p_end);
begin
  if not exists (select from ash.query_map_all where query_id = p_query_id) then
    return;
  end if;

  return query
  with map_ids as (
    select slot, id from ash.query_map_all where query_id = p_query_id
  ),
  hits as (
    select
      (select (- s.data[j])::smallint
      from generate_series(i, 1, -1) j
      where s.data[j] < 0 limit 1
      ) as wait_id
    from ash.sample s, generate_subscripts(s.data, 1) i
    where s.slot = any(ash._active_slots())
      and s.sample_ts >= v_start and s.sample_ts < v_end
      and i > 1
      and s.data[i] >= 0 and s.data[i - 1] >= 0
      and exists (select from map_ids m where m.slot = s.slot and m.id = s.data[i])
  ),
  named_hits as (
    select
      case when wm.event = wm.type then wm.event else wm.type || ':' || wm.event end as evt
    from hits h
    join ash.wait_event_map wm on wm.id = h.wait_id
    where h.wait_id is not null
  ),
  totals as (
    select evt, count(*) as cnt from named_hits group by evt
  ),
  grand_total as (
    select sum(cnt) as total from totals
  ),
  max_pct as (
    select max(round(t.cnt::numeric / gt.total * 100, 2)) as m
    from totals t cross join grand_total gt
  )
  select
    t.evt,
    t.cnt,
    round(t.cnt::numeric / gt.total * 100, 2),
    ash._bar(t.evt, round(t.cnt::numeric / gt.total * 100, 2), mp.m, p_width, p_color)
  from totals t
  cross join grand_total gt
  cross join max_pct mp
  order by t.cnt desc;
end;
$$;

-------------------------------------------------------------------------------
-- Activity summary — the "morning coffee" function
-------------------------------------------------------------------------------

-- Activity summary — one-call overview of a time period
-- Returns key-value pairs: sample count, peak backends, top waits, top queries.
create or replace function ash.activity_summary(
  p_interval interval default '24 hours'
)
returns table (
  metric text,
  value text
)
language plpgsql
stable
set jit = off
as $$
declare
  v_total_samples bigint;
  v_total_backends bigint;
  v_peak_backends smallint;
  v_peak_ts timestamptz;
  v_databases int;
  v_min_ts int4;
  r record;
  v_rank int;
begin
  v_min_ts := extract(epoch from now() - p_interval - ash.epoch())::int4;

  -- Basic counts
  select count(*), coalesce(sum(active_count), 0), max(active_count)
  into v_total_samples, v_total_backends, v_peak_backends
  from ash.sample
  where slot = any(ash._active_slots())
    and sample_ts >= v_min_ts;

  if v_total_samples = 0 then
    return query select 'status'::text, 'no data in this time range'::text;
    return;
  end if;

  -- Peak time
  select ash.epoch() + sample_ts * interval '1 second'
  into v_peak_ts
  from ash.sample
  where slot = any(ash._active_slots())
    and sample_ts >= v_min_ts
  order by active_count desc
  limit 1;

  -- Distinct databases
  select count(distinct datid) into v_databases
  from ash.sample
  where slot = any(ash._active_slots())
    and sample_ts >= v_min_ts;

  return query select 'time_range'::text, p_interval::text;
  return query select 'total_samples', v_total_samples::text;
  return query select 'avg_active_backends', round(v_total_backends::numeric / v_total_samples, 1)::text;
  return query select 'peak_active_backends', v_peak_backends::text;
  return query select 'peak_time', v_peak_ts::text;
  return query select 'databases_active', v_databases::text;

  -- Top 3 waits
  v_rank := 0;
  for r in select tw.wait_event || ' (' || tw.pct || '%)' as desc
      from ash.top_waits(p_interval, 3) tw
      where tw.wait_event <> 'Other'
  loop
    v_rank := v_rank + 1;
    return query select 'top_wait_' || v_rank, r.desc;
  end loop;

  -- Top 3 queries
  v_rank := 0;
  for r in select tq.query_id::text || coalesce(' — ' || left(tq.query_text, 60), '') || ' (' || tq.pct || '%)' as desc
      from ash.top_queries(p_interval, 3) tq
  loop
    v_rank := v_rank + 1;
    return query select 'top_query_' || v_rank, r.desc;
  end loop;

  return;
end;
$$;

-------------------------------------------------------------------------------
-- Histogram — visual wait event distribution in your terminal
-------------------------------------------------------------------------------

-------------------------------------------------------------------------------
-- Timeline chart — stacked bar visualization of wait events over time
-- ANSI color-coded bars by wait event type (24-bit RGB):
--   Green=CPU*  Blue=IO  Red=Lock  Pink=LWLock  Cyan=IPC
--   Yellow=Client  Orange=Timeout  Teal=BufferPin  Purple=Activity
--   Light yellow=IdleTx  Gray=Other
-------------------------------------------------------------------------------

-- Map wait event type prefix to ANSI color code

create or replace function ash.timeline_chart(
  p_interval interval default '1 hour',
  p_bucket interval default '1 minute',
  p_top int default 3,
  p_width int default 40,
  p_color boolean default false
)
returns table (
  bucket_start timestamptz,
  active numeric,
  detail text,
  chart text
)
language plpgsql
stable
set jit = off
as $$
declare
  v_reset text := ash._reset(p_color);
  v_max_active numeric;
  v_start_ts int4;
  v_bucket_secs int4;
  v_rec record;
  v_bar text;
  v_legend text;
  v_char_count int;
  v_val numeric;
  v_top_events text[];
  v_event_colors text[];
  v_event_chars text[] := array['█', '▓', '░', '▒'];  -- distinct chars per rank
  v_other_color text := ash._wait_color('Other', p_color);  -- gray for Other
  v_other_char text := '·';
  v_ch text;
  v_i int;
  v_legend_len int;
begin
  v_start_ts := extract(epoch from now() - p_interval - ash.epoch())::int4;
  v_bucket_secs := extract(epoch from p_bucket)::int4;

  -- Rank by avg active sessions weighted by bucket presence
  select array_agg(t.wait_event order by t.score desc)
  into v_top_events
  from (
    select
      wait_event,
      avg_active * bucket_fraction as score
    from (
      select
        case when wm.event = wm.type then wm.event
          else wm.type || ':' || wm.event end as wait_event,
        sum(s.data[i + 1])::numeric
          / nullif(count(distinct s.sample_ts), 0) as avg_active,
        count(distinct s.sample_ts - (s.sample_ts % v_bucket_secs))::numeric
          / nullif(greatest(1, (extract(epoch from p_interval)::int4 / v_bucket_secs)), 0)
          as bucket_fraction
      from ash.sample s, generate_subscripts(s.data, 1) i,
           ash.wait_event_map wm
      where wm.id = (-s.data[i])::smallint
        and s.slot = any(ash._active_slots())
        and s.sample_ts >= v_start_ts
        and s.data[i] < 0
      group by 1
    ) sub
    order by score desc
    limit p_top
  ) t;

  if v_top_events is null then
    return;
  end if;

  -- Build color array for each event
  v_event_colors := array[]::text[];
  for v_i in 1..array_length(v_top_events, 1) loop
    v_event_colors := v_event_colors || ash._wait_color(v_top_events[v_i], p_color);
  end loop;

  -- Find max average active sessions across all buckets for bar scaling
  select max(avg_total) into v_max_active
  from (
    select
      s.sample_ts - (s.sample_ts % v_bucket_secs) as bucket,
      sum(s.data[i + 1])::numeric
        / nullif(count(distinct s.sample_ts), 0) as avg_total
    from ash.sample s, generate_subscripts(s.data, 1) i
    where s.slot = any(ash._active_slots())
      and s.sample_ts >= v_start_ts
      and s.data[i] < 0
    group by 1
  ) t;

  if v_max_active is null or v_max_active = 0 then
    return;
  end if;

  -- Emit legend header row with colored blocks (distinct chars per rank)
  v_legend := '';
  for v_i in 1..array_length(v_top_events, 1) loop
    v_ch := coalesce(v_event_chars[v_i], v_event_chars[array_length(v_event_chars, 1)]);
    if v_i > 1 then v_legend := v_legend || '  '; end if;
    v_legend := v_legend || v_event_colors[v_i] || v_ch || v_reset || ' ' || v_top_events[v_i];
  end loop;
  v_legend := v_legend || '  ' || v_other_color || v_other_char || v_reset || ' Other';
  v_legend_len := length(v_legend);
  bucket_start := null;
  active := null;
  detail := null;
  chart := v_legend;
  return next;

  -- Build chart row by row
  for v_rec in
    with buckets as (
      select
        s.sample_ts - (s.sample_ts % v_bucket_secs) as bucket_ts,
        case when wm.event = wm.type then wm.event
          else wm.type || ':' || wm.event end as wait_event,
        sum(s.data[i + 1]) as cnt
      from ash.sample s, generate_subscripts(s.data, 1) i,
           ash.wait_event_map wm
      where wm.id = (-s.data[i])::smallint
        and s.slot = any(ash._active_slots())
        and s.sample_ts >= v_start_ts
        and s.data[i] < 0
      group by 1, 2
    ),
    bucket_samples as (
      select
        s.sample_ts - (s.sample_ts % v_bucket_secs) as bucket_ts,
        count(distinct s.sample_ts) as n_samples
      from ash.sample s
      where s.slot = any(ash._active_slots())
        and s.sample_ts >= v_start_ts
      group by 1
    ),
    per_bucket as (
      select
        b.bucket_ts,
        bs.n_samples,
        round(sum(b.cnt)::numeric / nullif(bs.n_samples, 0), 1) as total,
        jsonb_object_agg(
          b.wait_event,
          round(b.cnt::numeric / nullif(bs.n_samples, 0), 1)
        ) as events
      from buckets b
      join bucket_samples bs on bs.bucket_ts = b.bucket_ts
      group by b.bucket_ts, bs.n_samples
    )
    select
      ash.epoch() + g.ts * interval '1 second' as ts,
      coalesce(pb.total, 0) as total,
      coalesce(pb.events, '{}'::jsonb) as events
    from generate_series(
      v_start_ts - (v_start_ts % v_bucket_secs) + v_bucket_secs,
      extract(epoch from now() - ash.epoch())::int4
        - ((extract(epoch from now() - ash.epoch())::int4) % v_bucket_secs),
      v_bucket_secs
    ) g(ts)
    left join per_bucket pb on pb.bucket_ts = g.ts
    order by g.ts
  loop
    v_bar := '';
    v_legend := '';

    -- Colored stacked bar for each top event (distinct char per rank)
    for v_i in 1..array_length(v_top_events, 1) loop
      v_val := coalesce((v_rec.events ->> v_top_events[v_i])::numeric, 0);
      v_ch := coalesce(v_event_chars[v_i], v_event_chars[array_length(v_event_chars, 1)]);
      if v_val > 0 then
        v_char_count := greatest(0, round(v_val / v_max_active * p_width)::int);
        if v_char_count > 0 then
          v_bar := v_bar || v_event_colors[v_i] || repeat(v_ch, v_char_count) || v_reset;
        end if;
        v_legend := v_legend || ' ' || v_top_events[v_i] || '=' || v_val;
      end if;
    end loop;

    -- "Other" bar — remainder
    v_val := greatest(v_rec.total - (
      select coalesce(sum(coalesce((v_rec.events ->> e)::numeric, 0)), 0)
      from unnest(v_top_events) e
    ), 0);
    if v_val > 0 then
      v_char_count := greatest(0, round(v_val / v_max_active * p_width)::int);
      if v_char_count > 0 then
        v_bar := v_bar || v_other_color || repeat(v_other_char, v_char_count) || v_reset;
      end if;
      v_legend := v_legend || ' Other=' || v_val;
    end if;

    -- Pad to match legend row length so psql column alignment is consistent.
    -- ANSI codes add invisible bytes that psql counts as characters.
    if length(v_bar) < v_legend_len then
      v_bar := v_bar || repeat(' ', v_legend_len - length(v_bar));
    end if;

    bucket_start := v_rec.ts;
    active := v_rec.total;
    detail := ltrim(v_legend);
    chart := v_bar;
    return next;
  end loop;
end;
$$;

-- Absolute-time variant
create or replace function ash.timeline_chart_at(
  p_start timestamptz,
  p_end timestamptz,
  p_bucket interval default '1 minute',
  p_top int default 3,
  p_width int default 40,
  p_color boolean default false
)
returns table (
  bucket_start timestamptz,
  active numeric,
  detail text,
  chart text
)
language plpgsql
stable
set jit = off
as $$
declare
  v_reset text := ash._reset(p_color);
  v_max_active numeric;
  v_start_ts int4;
  v_end_ts int4;
  v_bucket_secs int4;
  v_rec record;
  v_bar text;
  v_legend text;
  v_char_count int;
  v_val numeric;
  v_top_events text[];
  v_event_colors text[];
  v_event_chars text[] := array['█', '▓', '░', '▒'];  -- distinct chars per rank
  v_other_color text := ash._wait_color('Other', p_color);  -- gray for Other
  v_other_char text := '·';
  v_ch text;
  v_i int;
  v_legend_len int;
begin
  v_start_ts := ash.ts_from_timestamptz(p_start);
  v_end_ts := ash.ts_from_timestamptz(p_end);
  v_bucket_secs := extract(epoch from p_bucket)::int4;

  -- Rank by avg active sessions weighted by bucket presence
  select array_agg(t.wait_event order by t.score desc)
  into v_top_events
  from (
    select
      wait_event,
      avg_active * bucket_fraction as score
    from (
      select
        case when wm.event = wm.type then wm.event
          else wm.type || ':' || wm.event end as wait_event,
        sum(s.data[i + 1])::numeric
          / nullif(count(distinct s.sample_ts), 0) as avg_active,
        count(distinct s.sample_ts - (s.sample_ts % v_bucket_secs))::numeric
          / nullif(greatest(1, ((v_end_ts - v_start_ts) / v_bucket_secs)), 0)
          as bucket_fraction
      from ash.sample s, generate_subscripts(s.data, 1) i,
           ash.wait_event_map wm
      where wm.id = (-s.data[i])::smallint
        and s.slot = any(ash._active_slots())
        and s.sample_ts >= v_start_ts and s.sample_ts < v_end_ts
        and s.data[i] < 0
      group by 1
    ) sub
    order by score desc
    limit p_top
  ) t;

  if v_top_events is null then
    return;
  end if;

  -- Build color array for each event
  v_event_colors := array[]::text[];
  for v_i in 1..array_length(v_top_events, 1) loop
    v_event_colors := v_event_colors || ash._wait_color(v_top_events[v_i], p_color);
  end loop;

  select max(avg_total) into v_max_active
  from (
    select
      s.sample_ts - (s.sample_ts % v_bucket_secs) as bucket,
      sum(s.data[i + 1])::numeric
        / nullif(count(distinct s.sample_ts), 0) as avg_total
    from ash.sample s, generate_subscripts(s.data, 1) i
    where s.slot = any(ash._active_slots())
      and s.sample_ts >= v_start_ts and s.sample_ts < v_end_ts
      and s.data[i] < 0
    group by 1
  ) t;

  if v_max_active is null or v_max_active = 0 then
    return;
  end if;

  -- Emit legend header row with colored blocks (distinct chars per rank)
  v_legend := '';
  for v_i in 1..array_length(v_top_events, 1) loop
    v_ch := coalesce(v_event_chars[v_i], v_event_chars[array_length(v_event_chars, 1)]);
    if v_i > 1 then v_legend := v_legend || '  '; end if;
    v_legend := v_legend || v_event_colors[v_i] || v_ch || v_reset || ' ' || v_top_events[v_i];
  end loop;
  v_legend := v_legend || '  ' || v_other_color || v_other_char || v_reset || ' Other';
  v_legend_len := length(v_legend);
  bucket_start := null;
  active := null;
  detail := null;
  chart := v_legend;
  return next;

  for v_rec in
    with buckets as (
      select
        s.sample_ts - (s.sample_ts % v_bucket_secs) as bucket_ts,
        case when wm.event = wm.type then wm.event
          else wm.type || ':' || wm.event end as wait_event,
        sum(s.data[i + 1]) as cnt
      from ash.sample s, generate_subscripts(s.data, 1) i,
           ash.wait_event_map wm
      where wm.id = (-s.data[i])::smallint
        and s.slot = any(ash._active_slots())
        and s.sample_ts >= v_start_ts and s.sample_ts < v_end_ts
        and s.data[i] < 0
      group by 1, 2
    ),
    bucket_samples as (
      select
        s.sample_ts - (s.sample_ts % v_bucket_secs) as bucket_ts,
        count(distinct s.sample_ts) as n_samples
      from ash.sample s
      where s.slot = any(ash._active_slots())
        and s.sample_ts >= v_start_ts and s.sample_ts < v_end_ts
      group by 1
    ),
    per_bucket as (
      select
        b.bucket_ts,
        bs.n_samples,
        round(sum(b.cnt)::numeric / nullif(bs.n_samples, 0), 1) as total,
        jsonb_object_agg(
          b.wait_event,
          round(b.cnt::numeric / nullif(bs.n_samples, 0), 1)
        ) as events
      from buckets b
      join bucket_samples bs on bs.bucket_ts = b.bucket_ts
      group by b.bucket_ts, bs.n_samples
    )
    select
      ash.epoch() + g.ts * interval '1 second' as ts,
      coalesce(pb.total, 0) as total,
      coalesce(pb.events, '{}'::jsonb) as events
    from generate_series(
      v_start_ts - (v_start_ts % v_bucket_secs),
      v_end_ts - (v_end_ts % v_bucket_secs),
      v_bucket_secs
    ) g(ts)
    left join per_bucket pb on pb.bucket_ts = g.ts
    order by g.ts
  loop
    v_bar := '';
    v_legend := '';

    -- Colored stacked bar for each top event (distinct char per rank)
    for v_i in 1..array_length(v_top_events, 1) loop
      v_val := coalesce((v_rec.events ->> v_top_events[v_i])::numeric, 0);
      v_ch := coalesce(v_event_chars[v_i], v_event_chars[array_length(v_event_chars, 1)]);
      if v_val > 0 then
        v_char_count := greatest(0, round(v_val / v_max_active * p_width)::int);
        if v_char_count > 0 then
          v_bar := v_bar || v_event_colors[v_i] || repeat(v_ch, v_char_count) || v_reset;
        end if;
        v_legend := v_legend || ' ' || v_top_events[v_i] || '=' || v_val;
      end if;
    end loop;

    -- "Other" bar — remainder
    v_val := greatest(v_rec.total - (
      select coalesce(sum(coalesce((v_rec.events ->> e)::numeric, 0)), 0)
      from unnest(v_top_events) e
    ), 0);
    if v_val > 0 then
      v_char_count := greatest(0, round(v_val / v_max_active * p_width)::int);
      if v_char_count > 0 then
        v_bar := v_bar || v_other_color || repeat(v_other_char, v_char_count) || v_reset;
      end if;
      v_legend := v_legend || ' Other=' || v_val;
    end if;

    -- Pad to match legend row length so psql column alignment is consistent.
    if length(v_bar) < v_legend_len then
      v_bar := v_bar || repeat(' ', v_legend_len - length(v_bar));
    end if;

    bucket_start := v_rec.ts;
    active := v_rec.total;
    detail := ltrim(v_legend);
    chart := v_bar;
    return next;
  end loop;
end;
$$;

-------------------------------------------------------------------------------
-- Raw samples — fully decoded sample data with timestamps and query text
-------------------------------------------------------------------------------

create or replace function ash.samples(
  p_interval interval default '1 hour',
  p_limit int default 100
)
returns table (
  sample_time timestamptz,
  database_name text,
  active_backends smallint,
  wait_event text,
  query_id bigint,
  query_text text
)
language plpgsql
stable
set jit = off
as $$
declare
  v_has_pgss boolean := false;
  v_min_ts int4;
begin
  v_min_ts := extract(epoch from now() - p_interval - ash.epoch())::int4;

  begin
    perform 1 from pg_stat_statements limit 1;
    v_has_pgss := true;
  exception when others then
    v_has_pgss := false;
  end;

  if v_has_pgss then
    return query
    with decoded as (
      select
        s.sample_ts,
        s.slot,
        s.datid,
        s.active_count,
        (-s.data[i])::smallint as wait_id,
        s.data[i + 2 + gs.n] as map_id
      from ash.sample s,
        generate_subscripts(s.data, 1) i,
        generate_series(0, greatest(s.data[i + 1] - 1, -1)) gs(n)
      where s.slot = any(ash._active_slots())
        and s.sample_ts >= v_min_ts
        and s.data[i] < 0
        and i + 1 <= array_length(s.data, 1)
        and i + 2 + gs.n <= array_length(s.data, 1)
    )
    select
      ash.epoch() + make_interval(secs => d.sample_ts),
      coalesce(db.datname, '<oid:' || d.datid || '>')::text,
      d.active_count,
      case when wm.event = wm.type then wm.event
        else wm.type || ':' || wm.event end,
      qm.query_id,
      left(pgss.query, 80)
    from decoded d
    join ash.wait_event_map wm on wm.id = d.wait_id
    left join ash.query_map_all qm on qm.slot = d.slot and qm.id = d.map_id and d.map_id <> 0
    left join pg_database db on db.oid = d.datid
    left join pg_stat_statements pgss on pgss.queryid = qm.query_id
      and pgss.dbid = d.datid
    order by d.sample_ts desc, wm.type, wm.event
    limit p_limit;
  else
    raise warning 'pg_stat_statements is not installed — query_text will be NULL. Run: CREATE EXTENSION pg_stat_statements;';
    return query
    with decoded as (
      select
        s.sample_ts,
        s.slot,
        s.datid,
        s.active_count,
        (-s.data[i])::smallint as wait_id,
        s.data[i + 2 + gs.n] as map_id
      from ash.sample s,
        generate_subscripts(s.data, 1) i,
        generate_series(0, greatest(s.data[i + 1] - 1, -1)) gs(n)
      where s.slot = any(ash._active_slots())
        and s.sample_ts >= v_min_ts
        and s.data[i] < 0
        and i + 1 <= array_length(s.data, 1)
        and i + 2 + gs.n <= array_length(s.data, 1)
    )
    select
      ash.epoch() + make_interval(secs => d.sample_ts),
      coalesce(db.datname, '<oid:' || d.datid || '>')::text,
      d.active_count,
      case when wm.event = wm.type then wm.event
        else wm.type || ':' || wm.event end,
      qm.query_id,
      null::text
    from decoded d
    join ash.wait_event_map wm on wm.id = d.wait_id
    left join ash.query_map_all qm on qm.slot = d.slot and qm.id = d.map_id and d.map_id <> 0
    left join pg_database db on db.oid = d.datid
    order by d.sample_ts desc, wm.type, wm.event
    limit p_limit;
  end if;
end;
$$;

create or replace function ash.samples_at(
  p_start timestamptz,
  p_end timestamptz,
  p_limit int default 100
)
returns table (
  sample_time timestamptz,
  database_name text,
  active_backends smallint,
  wait_event text,
  query_id bigint,
  query_text text
)
language plpgsql
stable
set jit = off
as $$
declare
  v_has_pgss boolean := false;
  v_start int4;
  v_end int4;
begin
  v_start := ash.ts_from_timestamptz(p_start);
  v_end := ash.ts_from_timestamptz(p_end);

  begin
    perform 1 from pg_stat_statements limit 1;
    v_has_pgss := true;
  exception when others then
    v_has_pgss := false;
  end;

  if v_has_pgss then
    return query
    with decoded as (
      select
        s.sample_ts,
        s.slot,
        s.datid,
        s.active_count,
        (-s.data[i])::smallint as wait_id,
        s.data[i + 2 + gs.n] as map_id
      from ash.sample s,
        generate_subscripts(s.data, 1) i,
        generate_series(0, greatest(s.data[i + 1] - 1, -1)) gs(n)
      where s.slot = any(ash._active_slots())
        and s.sample_ts >= v_start and s.sample_ts < v_end
        and s.data[i] < 0
        and i + 1 <= array_length(s.data, 1)
        and i + 2 + gs.n <= array_length(s.data, 1)
    )
    select
      ash.epoch() + make_interval(secs => d.sample_ts),
      coalesce(db.datname, '<oid:' || d.datid || '>')::text,
      d.active_count,
      case when wm.event = wm.type then wm.event
        else wm.type || ':' || wm.event end,
      qm.query_id,
      left(pgss.query, 80)
    from decoded d
    join ash.wait_event_map wm on wm.id = d.wait_id
    left join ash.query_map_all qm on qm.slot = d.slot and qm.id = d.map_id and d.map_id <> 0
    left join pg_database db on db.oid = d.datid
    left join pg_stat_statements pgss on pgss.queryid = qm.query_id
      and pgss.dbid = d.datid
    order by d.sample_ts desc, wm.type, wm.event
    limit p_limit;
  else
    raise warning 'pg_stat_statements is not installed — query_text will be NULL. Run: CREATE EXTENSION pg_stat_statements;';
    return query
    with decoded as (
      select
        s.sample_ts,
        s.slot,
        s.datid,
        s.active_count,
        (-s.data[i])::smallint as wait_id,
        s.data[i + 2 + gs.n] as map_id
      from ash.sample s,
        generate_subscripts(s.data, 1) i,
        generate_series(0, greatest(s.data[i + 1] - 1, -1)) gs(n)
      where s.slot = any(ash._active_slots())
        and s.sample_ts >= v_start and s.sample_ts < v_end
        and s.data[i] < 0
        and i + 1 <= array_length(s.data, 1)
        and i + 2 + gs.n <= array_length(s.data, 1)
    )
    select
      ash.epoch() + make_interval(secs => d.sample_ts),
      coalesce(db.datname, '<oid:' || d.datid || '>')::text,
      d.active_count,
      case when wm.event = wm.type then wm.event
        else wm.type || ':' || wm.event end,
      qm.query_id,
      null::text
    from decoded d
    join ash.wait_event_map wm on wm.id = d.wait_id
    left join ash.query_map_all qm on qm.slot = d.slot and qm.id = d.map_id and d.map_id <> 0
    left join pg_database db on db.oid = d.datid
    order by d.sample_ts desc, wm.type, wm.event
    limit p_limit;
  end if;
end;
$$;

-- Migration: add version column if upgrading from older version
do $$
begin
  if not exists (
    select from information_schema.columns
    where table_schema = 'ash' and table_name = 'config' and column_name = 'version'
  ) then
    alter table ash.config add column version text not null default '1.3';
  end if;
end $$;

-- Migration: add missed_samples column if upgrading from pre-1.4
do $$
begin
  if not exists (
    select from information_schema.columns
    where table_schema = 'ash' and table_name = 'config' and column_name = 'missed_samples'
  ) then
    alter table ash.config add column missed_samples bigint not null default 0;
  end if;
end $$;

update ash.config set version = '1.4' where singleton;
alter table ash.config alter column version set default '1.4';

-------------------------------------------------------------------------------
-- Event queries — top query_ids for a specific wait event
-------------------------------------------------------------------------------

drop function if exists ash.event_queries(text, interval, int);
create or replace function ash.event_queries(
  p_event text,
  p_interval interval default '1 hour',
  p_limit int default 10,
  p_width int default 20,
  p_color boolean default false
)
returns table (
  query_id bigint,
  samples bigint,
  pct numeric,
  bar text,
  query_text text
)
language plpgsql
stable
set jit = off
as $$
declare
  v_has_pgss boolean := false;
  v_min_ts int4;
begin
  v_min_ts := extract(epoch from now() - p_interval - ash.epoch())::int4;

  begin
    perform 1 from pg_stat_statements limit 1;
    v_has_pgss := true;
  exception when others then
    v_has_pgss := false;
  end;

  if v_has_pgss then
    return query
    with matching_waits as (
      select wm.id as wait_id
      from ash.wait_event_map wm
      where case
        when p_event like '%:%' then
          wm.type || ':' || wm.event = p_event
          or (wm.event = wm.type and wm.event = p_event)
        else
          wm.type = p_event
          or wm.event = p_event
      end
    ),
    hits as (
      select
        s.slot,
        s.data[i + 2 + gs.n] as map_id
      from ash.sample s,
        generate_subscripts(s.data, 1) i,
        matching_waits mw,
        lateral generate_series(0, greatest(s.data[i + 1] - 1, -1)) gs(n)
      where s.slot = any(ash._active_slots())
        and s.sample_ts >= v_min_ts
        and s.data[i] < 0
        and (-s.data[i])::smallint = mw.wait_id
        and i + 2 + gs.n <= array_length(s.data, 1)
        and s.data[i + 2 + gs.n] >= 0
    ),
    resolved as (
      select m.query_id
      from hits h
      join ash.query_map_all m on m.slot = h.slot and m.id = h.map_id
    ),
    totals as (
      select r.query_id, count(*) as cnt
      from resolved r
      group by r.query_id
    ),
    grand_total as (
      select sum(cnt) as total from totals
    ),
    ranked as (
      select
        t.query_id,
        t.cnt as samples,
        round(t.cnt::numeric / gt.total * 100, 2) as pct
      from totals t
      cross join grand_total gt
      order by t.cnt desc
      limit p_limit
    ),
    max_pct as (
      select max(r.pct) as m from ranked r
    )
    select
      r.query_id,
      r.samples,
      r.pct,
      ash._bar(p_event, r.pct, mp.m, p_width, p_color) as bar,
      left(pgss.query, 200) as query_text
    from ranked r
    cross join max_pct mp
    left join pg_stat_statements pgss on pgss.queryid = r.query_id
    order by r.samples desc;
  else
    raise exception 'pg_stat_statements extension is not installed. Run: CREATE EXTENSION pg_stat_statements;'
      using hint = 'event_queries() requires pg_stat_statements for query text. Use top_queries() or top_waits() for sample-only data without pg_stat_statements.';
  end if;
end;
$$;

drop function if exists ash.event_queries_at(text, timestamptz, timestamptz, int);
create or replace function ash.event_queries_at(
  p_event text,
  p_start timestamptz,
  p_end timestamptz,
  p_limit int default 10,
  p_width int default 20,
  p_color boolean default false
)
returns table (
  query_id bigint,
  samples bigint,
  pct numeric,
  bar text,
  query_text text
)
language plpgsql
stable
set jit = off
as $$
declare
  v_has_pgss boolean := false;
  v_start int4 := ash.ts_from_timestamptz(p_start);
  v_end int4 := ash.ts_from_timestamptz(p_end);
begin
  begin
    perform 1 from pg_stat_statements limit 1;
    v_has_pgss := true;
  exception when others then
    v_has_pgss := false;
  end;

  if v_has_pgss then
    return query
    with matching_waits as (
      select wm.id as wait_id
      from ash.wait_event_map wm
      where case
        when p_event like '%:%' then
          wm.type || ':' || wm.event = p_event
          or (wm.event = wm.type and wm.event = p_event)
        else
          wm.type = p_event
          or wm.event = p_event
      end
    ),
    hits as (
      select
        s.slot,
        s.data[i + 2 + gs.n] as map_id
      from ash.sample s,
        generate_subscripts(s.data, 1) i,
        matching_waits mw,
        lateral generate_series(0, greatest(s.data[i + 1] - 1, -1)) gs(n)
      where s.slot = any(ash._active_slots())
        and s.sample_ts >= v_start and s.sample_ts < v_end
        and s.data[i] < 0
        and (-s.data[i])::smallint = mw.wait_id
        and i + 2 + gs.n <= array_length(s.data, 1)
        and s.data[i + 2 + gs.n] >= 0
    ),
    resolved as (
      select m.query_id
      from hits h
      join ash.query_map_all m on m.slot = h.slot and m.id = h.map_id
    ),
    totals as (
      select r.query_id, count(*) as cnt
      from resolved r
      group by r.query_id
    ),
    grand_total as (
      select sum(cnt) as total from totals
    ),
    ranked as (
      select
        t.query_id,
        t.cnt as samples,
        round(t.cnt::numeric / gt.total * 100, 2) as pct
      from totals t
      cross join grand_total gt
      order by t.cnt desc
      limit p_limit
    ),
    max_pct as (
      select max(r.pct) as m from ranked r
    )
    select
      r.query_id,
      r.samples,
      r.pct,
      ash._bar(p_event, r.pct, mp.m, p_width, p_color) as bar,
      left(pgss.query, 200) as query_text
    from ranked r
    cross join max_pct mp
    left join pg_stat_statements pgss on pgss.queryid = r.query_id
    order by r.samples desc;
  else
    raise exception 'pg_stat_statements extension is not installed. Run: CREATE EXTENSION pg_stat_statements;'
      using hint = 'event_queries_at() requires pg_stat_statements for query text. Use top_queries_at() or top_waits_at() for sample-only data without pg_stat_statements.';
  end if;
end;
$$;


-- If upgrading an existing install, update the sampler cron command to include
-- statement_timeout (observer-effect protection). New installs via ash.start()
-- already use the updated command.
do $$
declare
  v_job_id bigint;
begin
  select jobid into v_job_id
  from cron.job
  where jobname = 'ash_sampler'
    and command <> 'set statement_timeout = ''500ms''; select ash.take_sample()';

  if v_job_id is not null then
    -- Use cron.alter_job() instead of direct UPDATE for managed-service compat.
    perform cron.alter_job(
      job_id  := v_job_id,
      command := 'set statement_timeout = ''500ms''; select ash.take_sample()'
    );
  end if;
exception when others then
  null; -- pg_cron not installed or alter_job unavailable, skip
end $$;

-------------------------------------------------------------------------------
-- Security: restrict write/admin functions to the installing role.
-- Reader functions remain PUBLIC (read-only, no side effects).
-------------------------------------------------------------------------------
do $$
declare
  v_owner text := (select nspowner::regrole::text from pg_namespace where nspname = 'ash');
begin
  -- Admin functions: only the schema owner
  execute format('revoke all on function ash.start(interval) from public');
  execute format('revoke all on function ash.stop() from public');
  execute format('revoke all on function ash.uninstall(text) from public');
  execute format('revoke all on function ash.rotate() from public');
  execute format('revoke all on function ash.take_sample() from public');
  execute format('revoke all on function ash.set_debug_logging(bool) from public');
  execute format('revoke all on function ash.rebuild_partitions(int) from public');
  execute format('revoke all on function ash._drop_all_partitions() from public');
  execute format('revoke all on function ash._rebuild_query_map_view() from public');
  execute format('revoke all on function ash.rollup_minute(int) from public');
  execute format('revoke all on function ash.rollup_hour() from public');
  execute format('revoke all on function ash.rollup_cleanup() from public');
  execute format('revoke all on function ash._merge_wait_counts(int4[]) from public');
  execute format('revoke all on function ash._merge_query_counts(int8[]) from public');
  execute format('revoke all on function ash._truncate_pairs(int8[], int) from public');

  execute format('grant execute on function ash.start(interval) to %I', v_owner);
  execute format('grant execute on function ash.stop() to %I', v_owner);
  execute format('grant execute on function ash.uninstall(text) to %I', v_owner);
  execute format('grant execute on function ash.rotate() to %I', v_owner);
  execute format('grant execute on function ash.take_sample() to %I', v_owner);
  execute format('grant execute on function ash.set_debug_logging(bool) to %I', v_owner);
  execute format('grant execute on function ash.rebuild_partitions(int) to %I', v_owner);
  execute format('grant execute on function ash._drop_all_partitions() to %I', v_owner);
  execute format('grant execute on function ash._rebuild_query_map_view() to %I', v_owner);
  execute format('grant execute on function ash.rollup_minute(int) to %I', v_owner);
  execute format('grant execute on function ash.rollup_hour() to %I', v_owner);
  execute format('grant execute on function ash.rollup_cleanup() to %I', v_owner);
  execute format('grant execute on function ash._merge_wait_counts(int4[]) to %I', v_owner);
  execute format('grant execute on function ash._merge_query_counts(int8[]) to %I', v_owner);
  execute format('grant execute on function ash._truncate_pairs(int8[], int) to %I', v_owner);

  -- ts helpers: grant to PUBLIC (harmless read-only conversion, useful for Grafana panels)
  -- ts_from_timestamptz and ts_to_timestamptz are already PUBLIC by default
end $$;
