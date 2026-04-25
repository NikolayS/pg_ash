-- pg_ash: Active Session History for Postgres
-- Version: 1.4 (latest)
-- Fresh install: \i sql/ash-install.sql
-- Upgrade from 1.0: \i sql/ash-1.0-to-1.1.sql, then \i sql/ash-1.1-to-1.2.sql, then \i sql/ash-1.2-to-1.3.sql, then \i sql/ash-1.3-to-1.4.sql
-- Upgrade from 1.1: \i sql/ash-1.1-to-1.2.sql, then \i sql/ash-1.2-to-1.3.sql, then \i sql/ash-1.3-to-1.4.sql
-- Upgrade from 1.2: \i sql/ash-1.2-to-1.3.sql, then \i sql/ash-1.3-to-1.4.sql
-- Upgrade from 1.3: \i sql/ash-1.3-to-1.4.sql


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
        '_validate_data',
        'uninstall',
        'debug_logging',
        'rebuild_partitions'
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
--
-- OVERFLOW HORIZON (issue #37 INFO): sample_ts is stored as int4 seconds since
-- 2026-01-01 UTC. int4 max is 2,147,483,647 seconds (~68.1 years), so this
-- counter is exhausted at roughly 2094-01-19 03:14:07 UTC. Past that point,
-- the `::int4` cast in ash.take_sample() raises `ERROR: integer out of range`
-- and sampling hard-fails — it does NOT silently wrap. ash.status() surfaces
-- the remaining seconds as `epoch_seconds_remaining` for observability. Before
-- ~2090, a bigint migration of the sample_ts column (and all readers) is
-- required to keep sampling working. Do NOT change ash.epoch() to buy time —
-- that corrupts every historical sample. The fix is a column-type migration.
create or replace function ash.epoch()
returns timestamptz
language sql
immutable
parallel safe
set search_path = pg_catalog, ash
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
  rollup_1m_retention_days   smallint not null default 30
                               check (rollup_1m_retention_days >= 1),
  rollup_1h_retention_days   smallint not null default 1825
                               check (rollup_1h_retention_days >= 1),
  rollup_min_backend_seconds smallint not null default 3,
  last_rollup_1m_ts          int4,
  last_rollup_1h_ts          int4,
  -- M-BUG-4: rows silently dropped by take_sample()'s inner exception handler.
  insert_errors              bigint not null default 0,
  -- M-BUG-6 / H-SEC-3: _register_wait dictionary-cap hit counter.
  register_wait_cap_hits     bigint not null default 0
);

-- Insert initial row if not exists
insert into ash.config (singleton) values (true) on conflict do nothing;

-- Migration: add v1.4 columns if upgrading from pre-1.4.
-- Must run before any code reads these columns.
-- Uses per-column IF NOT EXISTS so the block is safe when some columns
-- (e.g. missed_samples from the PR #29 upgrade) were already added.
-- Also idempotent on fresh installs (the columns are already present from
-- the `create table if not exists` above with matching defaults).
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
  add column if not exists last_rollup_1h_ts int4,
  -- M-BUG-4: track rows silently dropped by take_sample()'s inner exception handler.
  add column if not exists insert_errors bigint not null default 0,
  -- M-BUG-6 / H-SEC-3: track how often _register_wait hits the dictionary cap
  -- and has to skip a new (state,type,event). Non-zero means wait-event
  -- registrations are being silently dropped for that sample (those sessions
  -- won't appear in encoded data for this tick). Surfaced by ash.status().
  add column if not exists register_wait_cap_hits bigint not null default 0;

-- Ensure retention CHECK constraints exist for both fresh and upgrade paths.
-- ADD COLUMN IF NOT EXISTS above doesn't apply CHECKs to pre-existing columns,
-- so add them explicitly here (guarded by a not-exists probe for idempotency).
do $$
begin
  if not exists (
    select from pg_constraint where conname = 'config_rollup_1m_retention_days_check'
  ) then
    alter table ash.config
      add constraint config_rollup_1m_retention_days_check
      check (rollup_1m_retention_days >= 1);
  end if;
  if not exists (
    select from pg_constraint where conname = 'config_rollup_1h_retention_days_check'
  ) then
    alter table ash.config
      add constraint config_rollup_1h_retention_days_check
      check (rollup_1h_retention_days >= 1);
  end if;
end $$;

-- Wait event dictionary
-- M-BUG-6 / H-SEC-3: id stays smallint (matches legacy upgrade scripts; a
-- widened type would require a separate, coordinated migration because
-- `create or replace function` cannot change the return type of
-- ash._register_wait on a re-apply). DoS mitigation happens via the hard
-- row cap enforced in _register_wait (same pattern as query_map).
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
set search_path = pg_catalog, ash
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
set search_path = pg_catalog, ash
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
set search_path = pg_catalog, ash
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

-- Migration (issue #49): align sample.data check across upgrade paths.
-- v1.0 shipped `array_length(data, 1) >= 2`; v1.1 tightened it to `>= 3`,
-- but no legacy upgrade script rewrote the constraint, so installs that
-- started at 1.0 still carry the looser form. Detect and fix in place.
-- Idempotent: only rewrites when the current definition is missing or
-- the old `>= 2` form. Drops on the partitioned parent cascade to all
-- partitions; ADD CONSTRAINT on the parent propagates back to children.
do $$
declare
  v_def text;
begin
  select pg_get_constraintdef(c.oid) into v_def
  from pg_constraint c
  where c.conrelid = 'ash.sample'::regclass
    and c.conname  = 'sample_data_check';

  if v_def is null or v_def !~ 'array_length\(data, 1\) >= 3' then
    if v_def is not null then
      alter table ash.sample drop constraint sample_data_check;
    end if;
    alter table ash.sample
      add constraint sample_data_check
      check (data[1] < 0 and array_length(data, 1) >= 3);
  end if;
end $$;

-- Convert timestamptz to int4 epoch offset.
--
-- Clamp to [0, INT4_MAX] so absurd inputs (pre-epoch dates, post-2094-horizon
-- dates) do not raise `integer out of range`. sample_ts is a non-negative int4
-- by construction, so:
--   * pre-epoch  -> 0           (no matching samples; readers return empty)
--   * post-INT4  -> 2147483647  (no matching samples; readers return empty)
-- This centralizes the same-class clamp pattern used by the interval readers
-- (#51 / PR #57) so every _at variant inherits the safety net (#63).
create or replace function ash.ts_from_timestamptz(p_ts timestamptz)
returns int4
language sql
immutable
parallel safe
set search_path = pg_catalog, ash
as $$
  select greatest(
           least(
             extract(epoch from p_ts - ash.epoch()),
             2147483647  -- int4 max; sample_ts can't exceed this without overflow
           ),
           0             -- sample_ts can't be negative; pre-epoch -> 0
         )::int4
$$;

-- Convert int4 epoch offset to timestamptz
create or replace function ash.ts_to_timestamptz(p_ts int4)
returns timestamptz
language sql
immutable
parallel safe
set search_path = pg_catalog, ash
as $$
  select ash.epoch() + p_ts * interval '1 second'
$$;

comment on table ash.sample is
$$Packed wait-event samples. One row per (sample_ts, datid). Do not join directly — use ash.samples() / ash.samples_at() for decoded rows, or ash.decode_sample(data, slot) if you need a single sample's contents.$$;

comment on column ash.sample.data is
$$Packed int4[] encoding the sample's wait events and their query_map ids. Layout: groups of (-wait_id, count, query_map_id_1, ..., query_map_id_count). A negative marker starts each group; wait_id is negated for the marker so a positive count cannot be mistaken for a group boundary. Decode via ash.samples(), ash.samples_at(), or ash.decode_sample(data, slot).$$;

-- Register wait event function (upsert, returns id)
-- M-BUG-6 / H-SEC-3: signature intentionally preserved (returns smallint).
-- Widening the return type would break `create or replace` on top of any
-- legacy upgrade script that shipped the smallint signature (PG raises
-- `cannot change return type of existing function`), breaking the
-- re-apply / idempotent-install path. DoS is prevented by a hard row cap
-- below (mirrors the query_map 50 000 pattern but sized to stay within
-- smallint's 32 767 range). When the cap is hit we skip the INSERT and
-- return NULL — callers (take_sample() via PERFORM) ignore the return
-- value, and the later per-datid snapshot JOIN on wait_event_map simply
-- excludes sessions whose (state,type,event) is not registered. The
-- register_wait_cap_hits counter in ash.config surfaces the drop so
-- ash.status() can alert operators instead of silently mis-attributing
-- events to whatever row happened to be id=1.
create or replace function ash._register_wait(p_state text, p_type text, p_event text)
returns smallint
language plpgsql
set search_path = pg_catalog, ash
as $$
declare
  v_id    smallint;
  v_count bigint;
begin
  -- Try to get existing
  select id into v_id
  from ash.wait_event_map
  where state = p_state and type = p_type and event = p_event;

  if v_id is not null then
    return v_id;
  end if;

  -- Enforce dictionary size cap before inserting. 32 000 stays well below
  -- smallint's 32 767 ceiling while still leaving room for genuine event
  -- diversity (real wait-event inventories measure in the hundreds).
  -- reltuples is a cheap estimate; an exact count would add a scan on
  -- every fresh wait-event observation on the sampler's hot path.
  select reltuples::bigint into v_count
  from pg_class where oid = 'ash.wait_event_map'::regclass;

  if v_count >= 32000 then
    -- Bump the cap-hit counter so ash.status() can surface the drop.
    -- Note: counts *registration drops* here — not the number of sampled
    -- backends observed for this (state,type,event). Many concurrent
    -- backends blocked on the same dropped event only bump it once per tick.
    -- Wrap in an inner block: if the UPDATE itself fails (e.g. config row
    -- missing mid-uninstall), we still want the outer WARNING to fire and
    -- the function to return NULL without aborting take_sample().
    begin
      update ash.config set register_wait_cap_hits = register_wait_cap_hits + 1
        where singleton;
    exception when others then
      null;  -- counter bump is best-effort
    end;
    raise warning 'ash._register_wait: wait_event_map at cap (>= 32 000 rows); skipping (state=%, type=%, event=%) — see ash.status()',
      p_state, p_type, p_event;
    return null;  -- caller PERFORMs this; snapshot JOIN drops the session
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

--------------------------------------------------------------------------------
-- STEP 2: Sampler and decoder
--------------------------------------------------------------------------------

-- Core sampler function (no hstore dependency)
create or replace function ash.take_sample()
returns int
language plpgsql
set search_path = pg_catalog, ash
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
  -- All ash advisory locks share classid = hashtext('pg_ash')::int4 with a
  -- per-kind objid. The sampler kind is dedicated so it does NOT contend
  -- with rollup_minute / rollup_hour / rollup_cleanup — a long catch-up
  -- rollup no longer silently bumps skipped_samples on every tick.
  -- rebuild_partitions() polls pg_locks for ANY ash lock to drain in-flight
  -- operations.
  if not pg_try_advisory_xact_lock(
       hashtext('pg_ash')::int4,
       hashtext('pg_ash_sampler')::int4
     ) then
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
  -- Dynamic SQL: single query template, bug fixes apply once (not N×).
  -- Existence probe at the 50000th row: one index lookup, and — unlike
  -- pg_class.reltuples — immediately accurate after TRUNCATE (reltuples
  -- can remain stale or be -1 until autovacuum/ANALYZE catches up).
  execute format(
    'insert into ash.query_map_%1$s (query_id) '
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
    '  and not exists (select 1 from ash.query_map_%1$s offset 49999 limit 1) '
    'on conflict (query_id) do nothing',
    v_current_slot
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
      -- M-BUG-4: previously a CHECK violation on ash.sample.data (or any
      -- other INSERT-time error) was silently swallowed with just a WARNING,
      -- dropping a row of observability data without any durable signal.
      -- Bump insert_errors so ash.status() can surface the count, and keep
      -- the warning so live log watchers still see it.
      --
      -- Nested BEGIN/EXCEPTION around the counter UPDATE: the outer
      -- `exception when others` is a terminal handler, but the UPDATE
      -- itself can fail (e.g. config row absent mid-uninstall, lock
      -- timeout) and propagate out of this block. Before widening this
      -- handler to do bookkeeping, no propagation path existed — preserve
      -- that property so a flaky UPDATE never aborts the whole sampler.
      begin
        update ash.config set insert_errors = insert_errors + 1 where singleton;
      exception when others then
        null;  -- counter bump is best-effort; don't let it abort take_sample()
      end;
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
--
-- M-BUG-9: validate the entire array shape before emitting ANY rows.
-- Previously the function interleaved validation with `return next`, so a
-- malformed trailing segment still produced one or more valid-looking rows
-- followed by a WARNING. Callers saw silently truncated, partially correct
-- output. Now: walk once to verify shape, then walk again to emit — on
-- validation failure raise a single warning and return zero rows.
create or replace function ash.decode_sample(p_data integer[], p_slot smallint default null)
returns table (
  wait_event text,
  query_id int8,
  count int
)
language plpgsql
stable
set search_path = pg_catalog, ash
as $$
declare
  v_len int;
  v_idx int;
  v_wait_id int;
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

  -- Reject pathologically large arrays. Real ash.sample.data arrays are
  -- bounded by pg_stat_activity row count (a few hundred entries even on
  -- a busy database) plus the packed query_map_id payload — the largest
  -- legitimate data we ever see is well under 10 000 elements. A larger
  -- array passed by a malicious caller would force the validator and
  -- decoder to walk it twice, sustaining backend memory pressure.
  if v_len > 100000 then
    raise warning 'ash.decode_sample: data array too large (% > 100000)', v_len;
    return;
  end if;

  -- Basic structure check: first element must be negative (wait_id marker)
  if v_len < 3 or p_data[1] >= 0 then
    raise warning 'ash.decode_sample: invalid data array';
    return;
  end if;

  -- ---- Pass 1: validate shape only, emit nothing ----
  -- Reuses the same walker logic the old code had, but exits with a single
  -- warning and RETURN (no partial rows) if anything is wrong.
  v_idx := 1;
  while v_idx <= v_len loop
    if p_data[v_idx] >= 0 then
      raise warning 'ash.decode_sample: expected negative wait_id at position %', v_idx;
      return;
    end if;
    v_idx := v_idx + 1;

    if v_idx > v_len then
      raise warning 'ash.decode_sample: unexpected end of array at position % (missing count)', v_idx;
      return;
    end if;
    v_count := p_data[v_idx];
    if v_count <= 0 then
      raise warning 'ash.decode_sample: non-positive count % at position %', v_count, v_idx;
      return;
    end if;
    v_idx := v_idx + 1;

    if v_idx + v_count - 1 > v_len then
      raise warning 'ash.decode_sample: not enough query_ids for count % at position %', v_count, v_idx;
      return;
    end if;
    for v_qid_idx in 1..v_count loop
      if p_data[v_idx] < 0 then
        raise warning 'ash.decode_sample: expected non-negative query_id at position %', v_idx;
        return;
      end if;
      v_idx := v_idx + 1;
    end loop;
  end loop;

  -- ---- Pass 2: emit rows (shape is known good) ----
  v_idx := 1;
  while v_idx <= v_len loop
    v_wait_id := -p_data[v_idx];
    v_idx := v_idx + 1;

    v_count := p_data[v_idx];
    v_idx := v_idx + 1;

    -- Look up wait event info
    select w.type, w.event
    into v_type, v_event
    from ash.wait_event_map w
    where w.id = v_wait_id;

    -- Process each query_id
    for v_qid_idx in 1..v_count loop
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

      wait_event := case when v_event = v_type then v_event else v_type || ':' || v_event end;
      query_id := v_query_id;
      count := 1;
      return next;
    end loop;
  end loop;

  return;
end;
$$;

-- Convenience overload: decode every ash.sample row whose sample_ts matches.
-- Walks all datids/slots and returns decoded rows annotated with datid so the
-- caller can distinguish them. Implemented as a SQL LATERAL JOIN over the
-- 2-arg decode_sample(data, slot) SRF (passes slot for unambiguous lookup,
-- avoiding the "search-all-partitions" branch that can return stale ids
-- after rotation).
create or replace function ash.decode_sample(p_sample_ts int4)
returns table (
  datid      oid,
  wait_event text,
  query_id   int8,
  count      int
)
language sql
stable
set search_path = pg_catalog, ash
as $$
  select s.datid, d.wait_event, d.query_id, d.count
  from ash.sample s,
       lateral ash.decode_sample(s.data, s.slot) d
  where s.sample_ts = p_sample_ts
$$;

-- Wall-clock convenience: convert timestamptz to the matching sample_ts via
-- ts_from_timestamptz() and delegate to decode_sample(int4). Same return
-- shape. Named decode_sample_at() (matching the samples_at / top_waits_at
-- naming convention) so we don't create a decode_sample(unknown) ambiguity
-- between int4 and timestamptz overloads.
--
-- Intentionally NOT routed through _active_slots_for_at() (#69): unlike the
-- range-scan _at readers, decode_sample_at is a point-lookup keyed by an
-- exact sample_ts. Restricting to the helper's "now() - 2*rotation_period
-- .. now()" active-slots window would hide rows the caller can prove exist
-- (matching sample_ts in any partition). The silent ts_from_timestamptz()
-- int4 clamp from #63 is sufficient: absurd timestamps still don't error,
-- they just miss every partition.
create or replace function ash.decode_sample_at(p_ts timestamptz)
returns table (
  datid      oid,
  wait_event text,
  query_id   int8,
  count      int
)
language sql
stable
set search_path = pg_catalog, ash
as $$
  select d.datid, d.wait_event, d.query_id, d.count
  from ash.decode_sample(ash.ts_from_timestamptz(p_ts)) d
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
set search_path = pg_catalog, ash
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
  -- Two-arg form so rebuild_partitions's drain poll (which keys on classid)
  -- can see this lock; was a single-arg form in 1.4 betas but that put rotate
  -- in a different lock namespace and rebuild couldn't drain it.
  if not pg_try_advisory_xact_lock(
       hashtext('pg_ash')::int4,
       hashtext('pg_ash_rotate')::int4
     ) then
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
    -- Single statement with RESTART IDENTITY: one AccessExclusiveLock
    -- acquisition pair per slot, and resets the query_map_N identity sequence
    -- atomically (sample_N has no identity column, so it is unaffected).
    -- Dynamic SQL for N-partition support.
    execute format(
      'truncate ash.sample_%1$s, ash.query_map_%1$s restart identity',
      v_truncate_slot
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
create or replace function ash.rebuild_partitions(
  p_num_partitions int,
  p_confirm text default null
)
returns text
language plpgsql
set search_path = pg_catalog, ash
as $$
declare
  v_old_n int;
  v_new_n int;
begin
  -- Destructive: drops all raw sample partitions. Require explicit confirmation
  -- BEFORE touching any state (sampling_enabled, pg_cron jobs, partitions).
  if p_confirm is distinct from 'yes' then
    raise exception 'rebuild_partitions is destructive — all raw sample data '
      'will be lost. To proceed, call: '
      'select ash.rebuild_partitions(%, ''yes'')', p_num_partitions;
  end if;

  select num_partitions into v_old_n from ash.config where singleton;
  v_new_n := coalesce(p_num_partitions, v_old_n);

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
  -- All ash advisory locks share classid = hashtext('pg_ash')::int4. Each kind
  -- gets its own objid. This makes the (classid, objid) pair ash-specific
  -- and harder for an unrelated extension or a hostile session to squat
  -- (using literal classid 0/1 was vulnerable). Xact-level: auto-releases
  -- on commit/rollback — no manual unlock needed.
  if not pg_try_advisory_xact_lock(
       hashtext('pg_ash')::int4,
       hashtext('pg_ash_rebuild')::int4
     ) then
    update ash.config set sampling_enabled = true where singleton;
    raise exception 'rebuild_partitions: could not acquire lock — '
      'another rebuild is in progress';
  end if;

  -- Step 4: Drain — wait up to 5s for in-flight take_sample / rollup_* /
  -- rotate to release their ash advisory locks. STRICT drain: if anyone is
  -- still holding a lock at the end of the budget, raise an exception
  -- rather than proceeding to drop partitions out from under them. The
  -- caller can retry. Drains every ash lock kind (sampler / rollup /
  -- rotate) — anything that touches raw sample partitions OR rollup
  -- tables we'd be about to invalidate.
  declare
    v_drained boolean := false;
  begin
    for i in 1 .. 10 loop
      if not exists (
        select from pg_locks
        where locktype = 'advisory'
          and classid = hashtext('pg_ash')::int4::oid
          and objid in (
            hashtext('pg_ash_sampler')::int4::oid,
            hashtext('pg_ash_rollup')::int4::oid,
            hashtext('pg_ash_rotate')::int4::oid
          )
          and granted
          and pid <> pg_backend_pid()
      ) then
        v_drained := true;
        exit;
      end if;

      perform pg_sleep(0.5);
    end loop;

    if not v_drained then
      update ash.config set sampling_enabled = true where singleton;
      raise exception 'rebuild_partitions: drain timeout — in-flight '
        'sampler / rollup / rotate operations did not release within 5 s. '
        'Retry after they complete (or call ash.stop() first to halt the '
        'sampler).';
    end if;
  end;

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
set search_path = pg_catalog, ash
as $$
  select exists (
    select from pg_extension where extname = 'pg_cron'
  )
$$;

-- Start sampling: create pg_cron jobs
create or replace function ash.start(p_interval interval default '1 second')
returns table (job_type text, job_id bigint, status text)
language plpgsql
set search_path = pg_catalog, ash
as $$
declare
  v_sampler_job bigint;
  v_rotation_job bigint;
  v_cron_version text;
  v_seconds int;
  v_hours int;
  v_schedule text;
  v_skip_nodename_update boolean := false;
  v_debug_logging boolean := false;
  v_pg_cron_available boolean;
begin
  -- Read debug_logging flag so we can trace the pg_cron detection / scheduling
  -- path when ash.start() appears to no-op. Treat an error here as "debug off"
  -- so ash.start() still works in half-installed / upgrading states.
  begin
    select debug_logging into v_debug_logging from ash.config where singleton;
  exception when others then
    v_debug_logging := false;
  end;

  -- Validate interval
  v_seconds := extract(epoch from p_interval)::int;
  if v_seconds < 1 then
    job_type := 'error';
    job_id := null;
    status := format('interval must be at least 1 second, got %s', p_interval);
    return next;
    return;
  end if;

  -- H-BUG-1: validate interval shape BEFORE branching on pg_cron availability.
  -- Previously, the no-pg_cron branch returned early (below), skipping the
  -- seconds/minutes/hours checks. Same input must produce the same accept/
  -- reject outcome regardless of whether pg_cron is installed.
  --
  -- Build schedule string here (also used later when pg_cron is available):
  -- seconds format for <60s, cron format for 60s+.
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

  -- Privilege check: without pg_read_all_stats (or superuser), query_id is
  -- hidden for activity owned by other roles and collapses to the sentinel 0,
  -- silently skewing top_queries / query_waits results.
  begin
    if not (
      (select rolsuper from pg_roles where rolname = current_user)
      or pg_has_role(current_user, 'pg_read_all_stats', 'MEMBER')
    ) then
      raise notice 'warning: role % is not a superuser and not a member of pg_read_all_stats.', current_user;
      raise notice '  query_id will be NULL for activity owned by other roles and bucketed under 0,';
      raise notice '  skewing top_queries / query_waits. Fix: grant pg_read_all_stats to %;', current_user;
    end if;
  exception when others then
    -- don't let the privilege probe block ash.start(), but surface the failure
    raise notice 'privilege probe failed: %', sqlerrm;
  end;

  v_pg_cron_available := ash._pg_cron_available();
  if v_debug_logging then
    raise log 'ash.start: pg_cron_available=% interval=% seconds=%',
      v_pg_cron_available, p_interval, v_seconds;
  end if;

  -- If pg_cron is not available, just record the interval and advise on external scheduling
  if not v_pg_cron_available then
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

  -- M-BUG-8: defend against malformed extversion. If regexp_replace() strips
  -- everything (e.g. extversion is 'dev', empty, or starts with '.'),
  -- string_to_array(...)::int[] raises 'invalid input syntax for type integer'
  -- and bubbles up as a crash. Require a leading MAJOR.MINOR pattern before
  -- parsing; if it doesn't match, assume a modern pg_cron (>= 1.5) rather
  -- than failing the call.
  if v_cron_version ~ '^\d+\.\d+' then
    begin
      if string_to_array(regexp_replace(v_cron_version, '[^0-9.]', '', 'g'), '.')::int[] < '{1,5}'::int[] then
        if v_seconds < 60 then
          job_type := 'error';
          job_id := null;
          status := format('pg_cron version %s too old for sub-minute scheduling (need >= 1.5). Use external scheduler or upgrade pg_cron.', v_cron_version);
          return next;
          return;
        end if;
      end if;
    exception when others then
      -- Unparseable version — assume modern pg_cron (>= 1.5) and proceed.
      raise notice 'ash.start: could not parse pg_cron version "%" — assuming modern (>= 1.5)', v_cron_version;
    end;
  else
    raise notice 'ash.start: unrecognized pg_cron version "%" — assuming modern (>= 1.5)', v_cron_version;
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

  -- (schedule string v_schedule already built above, before the pg_cron
  -- availability branch — see H-BUG-1 fix.)

  -- Check for existing sampler job (idempotent)
  select jobid into v_sampler_job
  from cron.job
  where jobname = 'ash_sampler';

  if v_sampler_job is not null then
    -- H-BUG-2: re-sync the pg_cron schedule when the job already exists.
    -- Previously ash.start(new_interval) updated ash.config.sample_interval
    -- (further below) but never touched cron.job.schedule, so pg_cron kept
    -- firing at the old cadence — a silent behavioral divergence between
    -- configured and actual sampling rate.
    perform cron.alter_job(job_id := v_sampler_job, schedule := v_schedule);
    job_type := 'sampler';
    job_id := v_sampler_job;
    status := format('already exists — schedule updated to %s', v_schedule);
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

    if v_debug_logging then
      raise log 'ash.start: scheduled ash_sampler jobid=% schedule=% skip_nodename_update=%',
        v_sampler_job, v_schedule, v_skip_nodename_update;
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
set search_path = pg_catalog, ash
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
set search_path = pg_catalog, ash
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
set search_path = pg_catalog, ash
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

-- Helper to get active slots (current and previous).
create or replace function ash._active_slots()
returns smallint[]
language sql
stable
set search_path = pg_catalog, ash
as $$
  select array[
    current_slot,
    ((current_slot - 1 + num_partitions) % num_partitions)::smallint
  ]
  from ash.config
  where singleton
$$;

-- Helper used by reader functions that accept a user-supplied interval.
-- Returns the same array as ash._active_slots() for in-range intervals.
-- For intervals beyond 2 * rotation_period, returns an empty array so
-- reader `slot = any(...)` JOINs naturally yield zero rows — honoring
-- the NOTICE's "older samples not available" promise (and avoiding the
-- int4-epoch underflow that would otherwise raise `integer out of range`).
-- A single NOTICE is emitted per transaction in that case so callers get
-- a clear signal instead of a silent empty set.
-- Deduplication uses a transaction-scoped GUC (ash.notice_oversized) so
-- multi-query readers (e.g. activity_summary) don't spam the log with one
-- NOTICE per partition/sub-query.
--
-- NB: distinct name (not an overload of ash._active_slots()) because the
-- upgrade scripts re-create the zero-arg form on idempotent re-apply; an
-- overloaded pair would make bare ash._active_slots() ambiguous.
create or replace function ash._active_slots_for(p_interval interval)
returns smallint[]
language plpgsql
stable
set search_path = pg_catalog, ash
as $$
declare
  v_current_slot smallint;
  v_num_partitions smallint;
  v_rotation_period interval;
  v_already text;
begin
  select current_slot, num_partitions, rotation_period
    into v_current_slot, v_num_partitions, v_rotation_period
  from ash.config
  where singleton;

  -- Negative interval is meaningless and would underflow `now() - p_interval`
  -- past the int4 horizon downstream. Treat as out-of-window.
  if p_interval is not null and p_interval < interval '0' then
    v_already := current_setting('ash.notice_oversized', true);
    if v_already is null or v_already = '' then
      raise notice
        'requested interval % is negative; nothing to retrieve.',
        p_interval;
      perform set_config('ash.notice_oversized', '1', true);
    end if;
    return array[]::smallint[];
  end if;

  if p_interval is not null and p_interval > 2 * v_rotation_period then
    -- Suppress duplicate NOTICEs within the same transaction. The GUC is
    -- set local to the current transaction and auto-resets on commit/rollback.
    v_already := current_setting('ash.notice_oversized', true);
    if v_already is null or v_already = '' then
      raise notice
        'requested interval % exceeds 2 * rotation_period (%); only the last two partitions are retained, so older samples are not available. Shorten the interval or increase rotation_period via ash.config.',
        p_interval, v_rotation_period;
      perform set_config('ash.notice_oversized', '1', true);
    end if;
    -- Honor the NOTICE: return no slots so reader JOINs (`slot = any(...)`)
    -- yield empty. Without this, an absurd interval like '1000 years' clamps
    -- v_min_ts to 0 and matches every retained sample, contradicting the
    -- "older samples not available" promise. Callers wanting all retained
    -- data should pass an interval <= 2 * rotation_period.
    return array[]::smallint[];
  end if;

  -- Slot enumeration must use the configured num_partitions (default 3,
  -- configurable in [3, 32] via rebuild_partitions). Hardcoded `% 3` would
  -- mis-enumerate on N != 3.
  return array[
    v_current_slot,
    ((v_current_slot - 1 + v_num_partitions) % v_num_partitions)::smallint
  ];
end;
$$;

-- Absolute-range counterpart to ash._active_slots_for(interval), used by every
-- _at reader (top_waits_at, samples_at, query_waits_at, etc.). Returns the
-- active slot set when the requested [p_start, p_end) range overlaps what raw
-- samples retain (~2 * rotation_period back from now()), and an empty array
-- with a NOTICE when it doesn't — restoring loud-warn symmetry with the
-- relative readers (#69). Without this, _at readers silently returned 0 rows
-- on absurd inputs (year 1000, year 3000) thanks to ts_from_timestamptz()'s
-- int4 clamp (#63), which was a UX regression vs the interval path.
--
-- Out-of-retention conditions (each emits the NOTICE and returns {}):
--   * p_end   <= now() - 2 * rotation_period   (range entirely too old)
--   * p_start >  now()                          (range entirely in the future)
--
-- Importantly, an empty range (p_start >= p_end) inside the retained window
-- is NOT flagged — callers may legitimately ask for a zero-length window and
-- a NOTICE would be noise. Likewise nulls are passed through silently; the
-- reader's own WHERE clause filters them out as unknown comparisons.
--
-- Shares the ash.notice_oversized transaction-scoped GUC with
-- _active_slots_for(interval) so multi-call readers (and the relative wrapper
-- chain delegating into _at) don't spam one NOTICE per sub-query.
--
-- Readers must invoke this helper into a local variable in plpgsql (not as a
-- predicate inside language=sql bodies) — otherwise the planner can fold the
-- accompanying time predicate to false and skip the call entirely, losing the
-- NOTICE side-effect. See top_waits_at and friends for the established
-- pattern.
create or replace function ash._active_slots_for_at(
  p_start timestamptz,
  p_end   timestamptz
)
returns smallint[]
language plpgsql
stable
set search_path = pg_catalog, ash
as $$
declare
  v_current_slot    smallint;
  v_num_partitions  smallint;
  v_rotation_period interval;
  v_now             timestamptz := now();
  v_retention_start timestamptz;
  v_already         text;
begin
  select current_slot, num_partitions, rotation_period
    into v_current_slot, v_num_partitions, v_rotation_period
  from ash.config
  where singleton;

  v_retention_start := v_now - 2 * v_rotation_period;

  -- Out-of-retention check. Skip when either bound is null (the reader's
  -- own WHERE will yield empty without us needing to NOTICE) or when the
  -- range is empty inside retention (legitimate degenerate query).
  if p_start is not null and p_end is not null
     and (p_end <= v_retention_start or p_start > v_now) then
    v_already := current_setting('ash.notice_oversized', true);
    if v_already is null or v_already = '' then
      raise notice
        'requested range [%, %) lies outside the retained window (now - 2 * rotation_period .. now, i.e. [%, %)); only the last two partitions are retained, so older or future samples are not available. Adjust the range or increase rotation_period via ash.config.',
        p_start, p_end, v_retention_start, v_now;
      perform set_config('ash.notice_oversized', '1', true);
    end if;
    return array[]::smallint[];
  end if;

  return array[
    v_current_slot,
    ((v_current_slot - 1 + v_num_partitions) % v_num_partitions)::smallint
  ];
end;
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
set search_path = pg_catalog, ash
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
  -- M-BUG-4: surface the counter of rows dropped by take_sample()'s inner
  -- exception handler (CHECK violations and similar). Non-zero = silent
  -- data loss occurred — check server log for the matching WARNINGs.
  metric := 'insert_errors'; value := v_config.insert_errors::text; return next;
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
  -- M-BUG-6 / H-SEC-3: denominator tracks the 32 000 cap enforced in
  -- _register_wait (stays within smallint's 32 767 ceiling so we don't
  -- have to widen the id column / function signature).
  metric := 'wait_event_map_utilization'; value := round(v_wait_events::numeric / 32000 * 100, 2)::text || '%'; return next;
  metric := 'register_wait_cap_hits'; value := v_config.register_wait_cap_hits::text; return next;
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

  -- Epoch overflow horizon (issue #37): sample_ts is int4 seconds since
  -- 2026-01-01 UTC and int4 is exhausted circa 2094-01-19 — at which point
  -- the ::int4 cast in take_sample() raises ERROR and sampling hard-fails
  -- (no silent wrap). Surface remaining seconds so operators can plan the
  -- bigint migration well before the horizon. Value goes negative past the
  -- horizon (by design — indicates how long ago sampling would have stopped).
  metric := 'epoch_seconds_remaining';
  value := (2147483647::bigint - extract(epoch from (now() - ash.epoch()))::bigint)::text;
  return next;

  -- pg_cron status if available
  if ash._pg_cron_available() then
    metric := 'pg_cron_available'; value := 'yes'; return next;
    -- Issue #61: cron.job is owned by the pg_cron extension and requires
    -- USAGE on schema cron + SELECT on cron.job. Monitoring roles granted
    -- only ash.* readers will hit insufficient_privilege here, which used
    -- to abort status() entirely. Catch and surface a single fallback row
    -- so operators can see *why* cron details are missing.
    begin
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
    exception when insufficient_privilege then
      metric := 'cron_jobs';
      value := format(
        '<no cron.job access; grant USAGE ON SCHEMA cron TO %I>',
        current_user
      );
      return next;
    end;
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

-- Array concatenation aggregates: flat-concatenate arrays of varying lengths.
-- PostgreSQL's built-in array_agg() on arrays requires equal dimensions and
-- produces a multi-dimensional result. These use array_cat() to produce a
-- flat 1-D result, which _merge_wait_counts/_merge_query_counts expect.
do $$
begin
  if not exists (
    select from pg_proc p
    join pg_namespace n on n.oid = p.pronamespace
    where n.nspname = 'ash' and p.proname = '_int4_array_cat_agg'
      and p.prokind = 'a'
  ) then
    create aggregate ash._int4_array_cat_agg(int4[]) (
      sfunc = array_cat,
      stype = int4[],
      initcond = '{}'
    );
  end if;

  if not exists (
    select from pg_proc p
    join pg_namespace n on n.oid = p.pronamespace
    where n.nspname = 'ash' and p.proname = '_int8_array_cat_agg'
      and p.prokind = 'a'
  ) then
    create aggregate ash._int8_array_cat_agg(int8[]) (
      sfunc = array_cat,
      stype = int8[],
      initcond = '{}'
    );
  end if;
end $$;

-- Merge multiple wait_counts arrays: sum counts for matching wait_ids.
-- Input: flat int4[] from _int4_array_cat_agg(wait_counts) — concatenated
-- pairs into one flat array. The function extracts id/count pairs
-- by position parity, groups by id, sums counts, and re-interleaves.
-- Uses CROSS JOIN LATERAL (VALUES ...) for correct pair ordering
-- (avoids the ORDER BY v DESC bug that swaps id/count when count > id).
create or replace function ash._merge_wait_counts(p_flat int4[])
returns int4[]
language sql
immutable
parallel safe
set search_path = pg_catalog, ash
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
set search_path = pg_catalog, ash
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
set search_path = pg_catalog, ash
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
set search_path = pg_catalog, ash
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
  -- Acquire rollup lock (xact-level). Distinct from the sampler lock so a
  -- long catch-up doesn't bump skipped_samples on every concurrent tick.
  -- rebuild_partitions's drain poll waits on this objid.
  if not pg_try_advisory_xact_lock(
       hashtext('pg_ash')::int4,
       hashtext('pg_ash_rollup')::int4
     ) then
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
    -- We walk the packed data[] array inline (same pattern as ash.samples())
    -- rather than calling ash.decode_sample(), so we can group directly on
    -- the canonical wait_event_map.id (negative markers in data[]). Grouping
    -- on wait_event text would collapse states (e.g., ClientRead under
    -- 'active' vs 'idle in transaction') and double-count via the unique
    -- (state, type, event) rows in wait_event_map.
    insert into ash.rollup_1m (
      ts, datid, samples, peak_backends, wait_counts, query_counts
    )
    with decoded as (
      select
        s.datid,
        s.sample_ts,
        s.active_count,
        s.slot,
        (-s.data[i])::smallint as wait_id,
        s.data[i + 2 + gs.n] as map_id
      from ash.sample s,
        generate_subscripts(s.data, 1) i,
        generate_series(0, greatest(s.data[i + 1] - 1, -1)) gs(n)
      where s.sample_ts >= v_minute_start
        and s.sample_ts < v_minute_end
        and s.data[i] < 0
        and i + 1 <= array_length(s.data, 1)
        and i + 2 + gs.n <= array_length(s.data, 1)
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
      -- Resolve map_id -> query_id via the per-slot query_map partition.
      -- map_id = 0 is the sentinel for "no query_id" and is skipped.
      select
        d.datid,
        qm.query_id,
        count(*)::int8 as cnt,
        row_number() over (
          partition by d.datid
          order by count(*) desc, qm.query_id asc
        ) as rn
      from decoded d
      join ash.query_map_all qm
        on qm.slot = d.slot and qm.id = d.map_id
      where d.map_id <> 0
        and qm.query_id is not null
      group by d.datid, qm.query_id
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
set search_path = pg_catalog, ash
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
  -- Acquire rollup lock (xact-level). Same kind as rollup_minute /
  -- rollup_cleanup so they serialize among themselves; distinct from
  -- the sampler lock.
  if not pg_try_advisory_xact_lock(
       hashtext('pg_ash')::int4,
       hashtext('pg_ash_rollup')::int4
     ) then
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
        ash._int4_array_cat_agg(wait_counts) filter (where wait_counts <> '{}')
      ),
      ash._truncate_pairs(
        ash._merge_query_counts(
          ash._int8_array_cat_agg(query_counts) filter (where query_counts <> '{}')
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
set search_path = pg_catalog, ash
as $$
declare
  v_1m_deleted int;
  v_1h_deleted int;
  v_1m_retention int;
  v_1h_retention int;
  v_cutoff_1m int4;
  v_cutoff_1h int4;
begin
  -- Acquire rollup lock (xact-level). Shares the kind with rollup_minute
  -- and rollup_hour so cleanup can't delete rows that an in-flight rollup
  -- is upserting into. Also visible to rebuild_partitions's drain poll.
  if not pg_try_advisory_xact_lock(
       hashtext('pg_ash')::int4,
       hashtext('pg_ash_rollup')::int4
     ) then
    return 'cleanup: skipped — another rollup operation in progress';
  end if;

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
set search_path = pg_catalog, ash
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
set search_path = pg_catalog, ash
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
set search_path = pg_catalog, ash, public
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
set search_path = pg_catalog, ash, public
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
set search_path = pg_catalog, ash
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
set search_path = pg_catalog, ash
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
set search_path = pg_catalog, ash
as $$
  select p_color or coalesce(current_setting('ash.color', true), '') in ('on', 'true', '1');
$$;

create or replace function ash._wait_color(p_event text, p_color boolean default false)
returns text
language sql
stable
set search_path = pg_catalog, ash
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
set search_path = pg_catalog, ash
as $$
  select case when ash._color_on(p_color) then E'\033[0m' else '' end;
$$;

-- Build a bar string with fixed visible width (for pspg/column alignment).
-- Visible: [blocks padded to p_width] + ' ' + pct + '%'
-- Invisible ANSI codes don't affect visual width.
--
-- p_width is clamped to [1, 500] to prevent reader-callable OOM via
-- unbounded `repeat()` on the █ character (a granted reader role could
-- otherwise pass p_width => 1_000_000_000 and allocate ~3 GB per row).
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
set search_path = pg_catalog, ash
as $$
  -- All color escapes are now exactly 19 chars (zero-padded RGB).
  -- reset is always 4 chars. Total invisible = 23 when color on, 0 when off.
  select ash._wait_color(p_event, p_color)
    || rpad(
         repeat('█', greatest(1, (p_pct / nullif(p_max_pct, 0) * least(greatest(p_width, 1), 500))::int)),
         least(greatest(p_width, 1), 500)
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
set search_path = pg_catalog, ash
as $$
  with waits as (
    select
      case when wm.event = wm.type then wm.event else wm.type || ':' || wm.event end as wait_event,
      s.data[i + 1] as cnt
    from ash.sample s, generate_subscripts(s.data, 1) i,
         ash.wait_event_map wm
    where wm.id = (-s.data[i])::smallint
      and s.slot = any(ash._active_slots_for(p_interval))
      and s.sample_ts >= greatest(extract(epoch from now() - p_interval - ash.epoch()), 0)::int4
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
set search_path = pg_catalog, ash
as $$
  with waits as (
    select
      ash.epoch() + (s.sample_ts - (s.sample_ts % extract(epoch from p_bucket)::int4)) * interval '1 second' as bucket,
      (-s.data[i])::smallint as wait_id,
      s.data[i + 1] as cnt
    from ash.sample s, generate_subscripts(s.data, 1) i
    where s.slot = any(ash._active_slots_for(p_interval))
      and s.sample_ts >= greatest(extract(epoch from now() - p_interval - ash.epoch()), 0)::int4
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
-- search_path includes public so unqualified pg_stat_statements resolves to
-- the view created by CREATE EXTENSION pg_stat_statements (default schema
-- public on RDS/Cloud SQL/Supabase/AlloyDB/Neon). Keep public last so ash.*
-- names still win over any user-created objects in public.
-- NOTE: If pgss is installed into a non-public schema, the trailing
-- ash._apply_pgss_search_path() call at the end of this file appends the
-- detected schema to search_path on every pgss reader. Re-run
-- select ash._apply_pgss_search_path(); after installing/moving pgss.
set search_path = pg_catalog, ash, public
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
      where s.slot = any(ash._active_slots_for(p_interval))
        and s.sample_ts >= greatest(extract(epoch from now() - p_interval - ash.epoch()), 0)::int4
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
      where s.slot = any(ash._active_slots_for(p_interval))
        and s.sample_ts >= greatest(extract(epoch from now() - p_interval - ash.epoch()), 0)::int4
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
set search_path = pg_catalog, ash
as $$
  with waits as (
    select
      (-s.data[i])::smallint as wait_id,
      s.data[i + 1] as cnt
    from ash.sample s, generate_subscripts(s.data, 1) i
    where s.slot = any(ash._active_slots_for(p_interval))
      and s.sample_ts >= greatest(extract(epoch from now() - p_interval - ash.epoch()), 0)::int4
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
-- search_path includes public for pg_stat_statements access; see top_queries.
set search_path = pg_catalog, ash, public
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
      where s.slot = any(ash._active_slots_for(p_interval))
        and s.sample_ts >= greatest(extract(epoch from now() - p_interval - ash.epoch()), 0)::int4
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
set search_path = pg_catalog, ash
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
  samples as (
    select s.ctid, s.slot, s.data
    from ash.sample s
    where s.slot = any(ash._active_slots_for(p_interval))
      and s.sample_ts >= greatest(extract(epoch from now() - p_interval - ash.epoch()), 0)::int4
  ),
  -- Unpack each sample once and use a running-group window so each
  -- element carries the nearest preceding negative wait marker. This
  -- is O(N) per sample vs. the prior O(N^2) correlated subquery that
  -- walked backwards from every qid position.
  unpacked as (
    select
      s.ctid,
      s.slot,
      i as pos,
      s.data[i] as v,
      s.data[i - 1] as prev_v
    from samples s, generate_subscripts(s.data, 1) i
  ),
  grp_ids as (
    -- Running group: bump on each negative marker. Within a group the
    -- first element is the negative wait marker, followed by the count
    -- and the qids. O(N) per sample vs. the prior O(N^2) walk-back.
    select
      ctid,
      slot,
      pos,
      v,
      prev_v,
      sum(case when v < 0 then 1 else 0 end)
        over (partition by ctid, slot order by pos
              rows between unbounded preceding and current row) as grp
    from unpacked
  ),
  grouped as (
    select
      ctid,
      slot,
      pos,
      v,
      prev_v,
      -- Group's wait marker is the (sole) negative value in the group.
      -- Negate it to get the wait_event_map id.
      -min(v) over (partition by ctid, slot, grp) as wait_id
    from grp_ids
  ),
  hits as (
    -- A qid position is data[i] >= 0 AND data[i-1] >= 0 (i.e. neither
    -- the wait marker nor the count). Restrict to this query's map_ids.
    select g.wait_id::smallint as wait_id
    from grouped g
    where g.pos > 1
      and g.v >= 0
      and g.prev_v >= 0
      and exists (
        select from map_ids m
        where m.slot = g.slot and m.id = g.v
      )
  ),
  named_hits as (
    select
      case when wm.event = wm.type then wm.event else wm.type || ':' || wm.event end as evt
    from hits h
    join ash.wait_event_map wm on wm.id = h.wait_id
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

-- NOTE: `total_backends` is sum(active_count) over returned samples
-- (i.e. backend-samples, not distinct backends). Rename deferred to
-- v2.0 breaking-change release.
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
set search_path = pg_catalog, ash
as $$
  select
    coalesce(d.datname, '<background>') as database_name,
    s.datid,
    count(*) as samples,
    sum(s.active_count) as total_backends
  from ash.sample s
  left join pg_database d on d.oid = s.datid
  where s.slot = any(ash._active_slots_for(p_interval))
    and s.sample_ts >= greatest(extract(epoch from now() - p_interval - ash.epoch()), 0)::int4
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
set search_path = pg_catalog, ash
as $$
  select ash.ts_from_timestamptz(p_ts)
$$;

-- Top waits in an absolute time range.
-- plpgsql (not sql) so the _active_slots_for_at(p_start, p_end) NOTICE side
-- effect is observable even when the time predicates fold to false (e.g.
-- p_start = p_end = 0 after ts_from_timestamptz clamps absurd dates). A
-- language=sql body would inline the helper into the WHERE clause, which
-- the planner can short-circuit out of when the rest of the predicate is
-- provably empty (#69). Pulling the call into a local first guarantees
-- evaluation regardless of plan shape.
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
language plpgsql
stable
set jit = off
set search_path = pg_catalog, ash
as $$
-- The OUT-parameter table columns (wait_event, samples, pct, bar) collide
-- with bare column references inside the embedded SQL — tell plpgsql to
-- prefer the column over the function variable in those CTEs.
#variable_conflict use_column
declare
  v_slots smallint[] := ash._active_slots_for_at(p_start, p_end);
  v_start int4       := ash.ts_from_timestamptz(p_start);
  v_end   int4       := ash.ts_from_timestamptz(p_end);
begin
  return query
  with waits as (
    select
      case when wm.event = wm.type then wm.event else wm.type || ':' || wm.event end as wait_event,
      s.data[i + 1] as cnt
    from ash.sample s, generate_subscripts(s.data, 1) i,
         ash.wait_event_map wm
    where wm.id = (-s.data[i])::smallint
      and s.slot = any(v_slots)
      and s.sample_ts >= v_start
      and s.sample_ts < v_end
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
    r.cnt::bigint as samples,
    round(r.cnt::numeric / gt.total * 100, 2) as pct,
    ash._bar(r.wait_event, round(r.cnt::numeric / gt.total * 100, 2), mp.m, p_width, p_color) as bar
  from (select * from top_rows union all select * from other) r
  cross join grand_total gt
  cross join max_pct mp
  order by r.rn;
end;
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
-- search_path includes public for pg_stat_statements access; see top_queries.
set search_path = pg_catalog, ash, public
as $$
declare
  v_has_pgss boolean := false;
  -- Pull v_slots first so the helper's NOTICE fires even when the time
  -- predicate folds to false on absurd ranges (#69).
  v_slots smallint[] := ash._active_slots_for_at(p_start, p_end);
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
      where s.slot = any(v_slots)
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
      where s.slot = any(v_slots)
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
-- plpgsql (not sql) so the _active_slots_for_at NOTICE side effect fires even
-- when the time predicate folds to false; see top_waits_at for context (#69).
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
language plpgsql
stable
set jit = off
set search_path = pg_catalog, ash
as $$
-- OUT-param columns shadow embedded SQL aliases; prefer column refs.
#variable_conflict use_column
declare
  v_slots smallint[] := ash._active_slots_for_at(p_start, p_end);
  v_start int4       := ash.ts_from_timestamptz(p_start);
  v_end   int4       := ash.ts_from_timestamptz(p_end);
begin
  return query
  with waits as (
    select
      ash.epoch() + (s.sample_ts - (s.sample_ts % extract(epoch from p_bucket)::int4)) * interval '1 second' as bucket,
      (-s.data[i])::smallint as wait_id,
      s.data[i + 1] as cnt
    from ash.sample s, generate_subscripts(s.data, 1) i
    where s.slot = any(v_slots)
      and s.sample_ts >= v_start
      and s.sample_ts < v_end
      and s.data[i] < 0
  )
  select
    w.bucket as bucket_start,
    case when wm.event = wm.type then wm.event else wm.type || ':' || wm.event end as wait_event,
    sum(w.cnt) as samples
  from waits w
  join ash.wait_event_map wm on wm.id = w.wait_id
  group by w.bucket, wm.type, wm.event
  order by w.bucket, sum(w.cnt) desc;
end;
$$;

-- Wait event type distribution in an absolute time range
-- plpgsql (not sql) so the _active_slots_for_at NOTICE side effect fires even
-- when the time predicate folds to false; see top_waits_at for context (#69).
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
language plpgsql
stable
set jit = off
set search_path = pg_catalog, ash
as $$
-- OUT-param columns shadow embedded SQL aliases; prefer column refs.
#variable_conflict use_column
declare
  v_slots smallint[] := ash._active_slots_for_at(p_start, p_end);
  v_start int4       := ash.ts_from_timestamptz(p_start);
  v_end   int4       := ash.ts_from_timestamptz(p_end);
begin
  return query
  with waits as (
    select (-s.data[i])::smallint as wait_id, s.data[i + 1] as cnt
    from ash.sample s, generate_subscripts(s.data, 1) i
    where s.slot = any(v_slots)
      and s.sample_ts >= v_start
      and s.sample_ts < v_end
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
  order by t.cnt desc;
end;
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
set search_path = pg_catalog, ash
as $$
declare
  v_start int4 := ash.ts_from_timestamptz(p_start);
  v_end int4 := ash.ts_from_timestamptz(p_end);
  v_slots smallint[];
begin
  if not exists (select from ash.query_map_all where query_id = p_query_id) then
    return;
  end if;

  -- Computed after the query_id existence short-circuit (matching the
  -- relative ash.query_waits pattern: a non-existent query_id returns
  -- silently without emitting any NOTICE about retention). Pulled into
  -- a local so the NOTICE side effect fires regardless of whether the
  -- time predicate folds out (#69).
  v_slots := ash._active_slots_for_at(p_start, p_end);

  return query
  with map_ids as (
    select slot, id from ash.query_map_all where query_id = p_query_id
  ),
  samples as (
    select s.ctid, s.slot, s.data
    from ash.sample s
    where s.slot = any(v_slots)
      and s.sample_ts >= v_start and s.sample_ts < v_end
  ),
  -- See ash.query_waits for commentary: O(N) running-group rewrite
  -- of the old O(N^2) correlated walk-back.
  unpacked as (
    select
      s.ctid,
      s.slot,
      i as pos,
      s.data[i] as v,
      s.data[i - 1] as prev_v
    from samples s, generate_subscripts(s.data, 1) i
  ),
  grp_ids as (
    select
      ctid, slot, pos, v, prev_v,
      sum(case when v < 0 then 1 else 0 end)
        over (partition by ctid, slot order by pos
              rows between unbounded preceding and current row) as grp
    from unpacked
  ),
  grouped as (
    select
      ctid, slot, pos, v, prev_v,
      -min(v) over (partition by ctid, slot, grp) as wait_id
    from grp_ids
  ),
  hits as (
    select g.wait_id::smallint as wait_id
    from grouped g
    where g.pos > 1
      and g.v >= 0
      and g.prev_v >= 0
      and exists (
        select from map_ids m
        where m.slot = g.slot and m.id = g.v
      )
  ),
  named_hits as (
    select
      case when wm.event = wm.type then wm.event else wm.type || ':' || wm.event end as evt
    from hits h
    join ash.wait_event_map wm on wm.id = h.wait_id
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
set search_path = pg_catalog, ash
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
  v_min_ts := greatest(extract(epoch from now() - p_interval - ash.epoch()), 0)::int4;

  -- Basic counts
  select count(*), coalesce(sum(active_count), 0), max(active_count)
  into v_total_samples, v_total_backends, v_peak_backends
  from ash.sample
  where slot = any(ash._active_slots_for(p_interval))
    and sample_ts >= v_min_ts;

  if v_total_samples = 0 then
    return query select 'status'::text, 'no data in this time range'::text;
    return;
  end if;

  -- Peak time
  select ash.epoch() + sample_ts * interval '1 second'
  into v_peak_ts
  from ash.sample
  where slot = any(ash._active_slots_for(p_interval))
    and sample_ts >= v_min_ts
  order by active_count desc
  limit 1;

  -- Distinct databases
  select count(distinct datid) into v_databases
  from ash.sample
  where slot = any(ash._active_slots_for(p_interval))
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
set search_path = pg_catalog, ash
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
  -- Clamp p_width to a sane visible range to prevent reader-callable OOM
  -- via unbounded `repeat()` on the bar characters. 500 is an order of
  -- magnitude beyond any realistic terminal width.
  p_width := least(greatest(p_width, 1), 500);
  v_start_ts := greatest(extract(epoch from now() - p_interval - ash.epoch()), 0)::int4;
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
          / nullif(greatest(1, (least(extract(epoch from p_interval), 2147483647)::int4 / v_bucket_secs)), 0)
          as bucket_fraction
      from ash.sample s, generate_subscripts(s.data, 1) i,
           ash.wait_event_map wm
      where wm.id = (-s.data[i])::smallint
        and s.slot = any(ash._active_slots_for(p_interval))
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
    where s.slot = any(ash._active_slots_for(p_interval))
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
        and s.slot = any(ash._active_slots_for(p_interval))
        and s.sample_ts >= v_start_ts
        and s.data[i] < 0
      group by 1, 2
    ),
    bucket_samples as (
      select
        s.sample_ts - (s.sample_ts % v_bucket_secs) as bucket_ts,
        count(distinct s.sample_ts) as n_samples
      from ash.sample s
      where s.slot = any(ash._active_slots_for(p_interval))
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
set search_path = pg_catalog, ash
as $$
declare
  v_reset text := ash._reset(p_color);
  v_max_active numeric;
  v_start_ts int4;
  v_end_ts int4;
  v_slots smallint[];
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
  -- Clamp p_width to a sane visible range to prevent reader-callable OOM
  -- via unbounded `repeat()` on the bar characters. See ash._bar comment.
  p_width := least(greatest(p_width, 1), 500);
  v_start_ts := ash.ts_from_timestamptz(p_start);
  v_end_ts := ash.ts_from_timestamptz(p_end);
  -- Pulled into a local so the NOTICE side effect fires even when the
  -- inner queries' time predicates fold to false (#69).
  v_slots := ash._active_slots_for_at(p_start, p_end);
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
        and s.slot = any(v_slots)
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
    where s.slot = any(v_slots)
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
        and s.slot = any(v_slots)
        and s.sample_ts >= v_start_ts and s.sample_ts < v_end_ts
        and s.data[i] < 0
      group by 1, 2
    ),
    bucket_samples as (
      select
        s.sample_ts - (s.sample_ts % v_bucket_secs) as bucket_ts,
        count(distinct s.sample_ts) as n_samples
      from ash.sample s
      where s.slot = any(v_slots)
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
-- search_path includes public for pg_stat_statements access; see top_queries.
set search_path = pg_catalog, ash, public
as $$
declare
  v_has_pgss boolean := false;
  v_min_ts int4;
begin
  v_min_ts := greatest(extract(epoch from now() - p_interval - ash.epoch()), 0)::int4;

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
      where s.slot = any(ash._active_slots_for(p_interval))
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
      where s.slot = any(ash._active_slots_for(p_interval))
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
-- search_path includes public for pg_stat_statements access; see top_queries.
set search_path = pg_catalog, ash, public
as $$
declare
  v_has_pgss boolean := false;
  v_start int4;
  v_end int4;
  v_slots smallint[];
begin
  v_start := ash.ts_from_timestamptz(p_start);
  v_end := ash.ts_from_timestamptz(p_end);
  -- Pulled into a local so the helper's NOTICE side effect fires even when
  -- the time predicate folds to false on absurd ranges (#69).
  v_slots := ash._active_slots_for_at(p_start, p_end);

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
      where s.slot = any(v_slots)
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
      where s.slot = any(v_slots)
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

comment on function ash.samples(interval, int) is
$$Decoded wait-event samples over the last p_interval (default '1 hour'), newest first, up to p_limit rows (default 100). Returns (sample_time, database_name, active_backends, wait_event, query_id, query_text). query_text requires pg_stat_statements; NULL otherwise.$$;

comment on function ash.samples_at(timestamptz, timestamptz, int) is
$$Decoded wait-event samples between p_start and p_end, newest first, up to p_limit rows (default 100). Same return shape as ash.samples().$$;

comment on function ash.decode_sample(integer[], smallint) is
$$Decodes a single ash.sample.data array into (wait_event, query_id, count) rows. Pass p_slot (ash.sample.slot) to resolve query_ids unambiguously; omitting it searches all query_map partitions and may return a stale id after rotation.$$;

comment on function ash.decode_sample(int4) is
$$Convenience overload: decodes every ash.sample row whose sample_ts equals p_sample_ts (across all datids/slots) and returns (datid, wait_event, query_id, count). Internally calls decode_sample(data, slot) with the row's slot, so query_id resolution is unambiguous.$$;

comment on function ash.decode_sample_at(timestamptz) is
$$Wall-clock convenience: same as decode_sample(int4) but accepts timestamptz, converting via ts_from_timestamptz() to find the matching sample_ts. Named with the _at suffix (consistent with samples_at, top_waits_at) to avoid an unknown-typed decode_sample(123) literal matching both the int4 and timestamptz overloads.$$;

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
-- search_path includes public for pg_stat_statements access; see top_queries.
set search_path = pg_catalog, ash, public
as $$
declare
  v_has_pgss boolean := false;
  v_min_ts int4;
begin
  v_min_ts := greatest(extract(epoch from now() - p_interval - ash.epoch()), 0)::int4;

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
      where s.slot = any(ash._active_slots_for(p_interval))
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
-- search_path includes public for pg_stat_statements access; see top_queries.
set search_path = pg_catalog, ash, public
as $$
declare
  v_has_pgss boolean := false;
  v_start int4 := ash.ts_from_timestamptz(p_start);
  v_end int4 := ash.ts_from_timestamptz(p_end);
  -- Pulled into a local so the helper's NOTICE side effect fires even when
  -- the time predicate folds to false on absurd ranges (#69).
  v_slots smallint[] := ash._active_slots_for_at(p_start, p_end);
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
      where s.slot = any(v_slots)
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
-- Security: restrict all ash schema access. Admin functions belong only to
-- the installing role; reader functions and underlying tables are revoked
-- from PUBLIC so tenants on shared clusters cannot see observability data
-- (query text, waits, config) unless the owner explicitly grants access.
-- The owner retains full access by virtue of owning the objects.
-------------------------------------------------------------------------------

-- Helper: detect the schema that holds the pg_stat_statements view.
-- Managed services differ: RDS/Cloud SQL/Supabase/AlloyDB/Neon default to
-- public, but self-hosted installs may use `pg_stat_statements`, `extensions`,
-- `monitoring`, or another custom schema. Returns NULL when pgss is not
-- installed.
create or replace function ash._pgss_schema()
returns text
language sql
stable
set search_path = pg_catalog
as $$
  select n.nspname::text
  from pg_extension e
  join pg_namespace n on n.oid = e.extnamespace
  where e.extname = 'pg_stat_statements'
$$;

comment on function ash._pgss_schema() is
  'Returns the schema name of the installed pg_stat_statements extension, or NULL if not installed. Used to keep reader functions portable across managed services and custom install schemas.';

-- Helper: re-apply search_path on the seven pgss reader functions using the
-- currently detected pgss schema. Run this after installing / moving
-- pg_stat_statements if it lives outside `public`. Safe to re-run.
create or replace function ash._apply_pgss_search_path()
returns text
language plpgsql
set search_path = pg_catalog, ash
as $$
declare
  v_pgss_schema text := ash._pgss_schema();
  -- B2: keep this list in sync with v_pgss_readers in sql/ash-1.3-to-1.4.sql
  -- (and any future upgrade scripts) AND with the per-function `set
  -- search_path = pg_catalog, ash, public` clauses in this file. Any reader
  -- that probes pg_stat_statements must appear here so its search_path
  -- resolves the view across managed-service and custom pgss install schemas.
  v_readers text[] := array[
    'top_queries', 'top_queries_at', 'top_queries_with_text',
    'samples', 'samples_at',
    'event_queries', 'event_queries_at',
    'hourly_queries', 'hourly_queries_at'
  ];
  v_path text;
  r record;
begin
  -- Always keep public in the path as a fallback (matches the managed-service
  -- default and preserves behavior when pgss is not yet installed). When the
  -- extension lives in a non-default schema, list THAT schema BEFORE public
  -- so an attacker who creates a `public.pg_stat_statements` view cannot
  -- shadow the real one and feed attacker-controlled query_text into
  -- monitoring dashboards. (Security review #76 finding.)
  if v_pgss_schema is null or v_pgss_schema in ('pg_catalog', 'ash', 'public') then
    v_path := 'pg_catalog, ash, public';
  else
    v_path := format('pg_catalog, ash, %I, public', v_pgss_schema);
  end if;

  for r in
    select p.proname,
           pg_catalog.pg_get_function_identity_arguments(p.oid) as args
    from pg_catalog.pg_proc p
    join pg_catalog.pg_namespace n on p.pronamespace = n.oid
    where n.nspname = 'ash'
      and p.prokind = 'f'
      and p.proname = any(v_readers)
  loop
    execute format('alter function ash.%I(%s) set search_path = %s',
                   r.proname, r.args, v_path);
  end loop;

  return v_path;
end;
$$;

comment on function ash._apply_pgss_search_path() is
  'Re-applies search_path on pgss reader functions using the currently detected pg_stat_statements schema. Run after installing pg_stat_statements if it lives outside the public schema.';

-- Apply now so installs that have pgss in a non-public schema work out of the
-- box. No-op (keeps default) when pgss is absent or lives in public.
select ash._apply_pgss_search_path();

-- Canonical "admin" function set: callers that must NOT be granted to
-- monitoring roles. Single source of truth for the REVOKE-from-PUBLIC /
-- GRANT-to-owner hardening block below and for grant_reader/revoke_reader
-- (which exclude these names from the reader EXECUTE bundle). Adding a new
-- admin entry point requires updating only this list.
create or replace function ash._admin_funcs()
returns text[]
language sql
immutable
parallel safe
as $$
  select array[
    'start', 'stop', 'uninstall', 'rotate', 'take_sample',
    'set_debug_logging', 'rebuild_partitions', 'rollup_minute',
    'rollup_hour', 'rollup_cleanup', '_drop_all_partitions',
    '_rebuild_query_map_view', '_merge_wait_counts', '_merge_query_counts',
    '_truncate_pairs', '_int4_array_cat_agg', '_int8_array_cat_agg',
    -- the helpers themselves: granting them to a reader role would let
    -- that role hand out privileges. keep them admin-only.
    'grant_reader', 'revoke_reader'
  ]::text[]
$$;

comment on function ash._admin_funcs() is
  'Canonical list of ash.* admin function names (must not be granted to monitoring roles). Single source of truth used by the REVOKE/GRANT hardening block and by grant_reader/revoke_reader.';

do $$
declare
  v_owner text := (select nspowner::regrole::text from pg_namespace where nspname = 'ash');
  v_admin_funcs constant text[] := ash._admin_funcs();
  r record;
begin
  -- Admin functions: revoke from PUBLIC and grant only to the schema owner.
  -- Resolve signatures dynamically via pg_proc so any future overload or
  -- default-argument change is picked up automatically. prokind in ('f','a')
  -- covers regular functions and aggregates (_int{4,8}_array_cat_agg).
  -- Entries in _admin_funcs() that are not yet created at this point in
  -- install order (e.g. grant_reader/revoke_reader, defined below) are
  -- skipped here and locked down by their own DO block once created.
  for r in
    select p.proname,
           pg_catalog.pg_get_function_identity_arguments(p.oid) as args
    from pg_catalog.pg_proc p
    join pg_catalog.pg_namespace n on p.pronamespace = n.oid
    where n.nspname = 'ash'
      and p.prokind in ('f', 'a')
      and p.proname::text = any(v_admin_funcs)
  loop
    execute format('revoke all on function ash.%I(%s) from public',
                   r.proname, r.args);
    execute format('grant execute on function ash.%I(%s) to %I',
                   r.proname, r.args, v_owner);
  end loop;

  -- ts helpers: grant to PUBLIC (harmless read-only conversion, useful for Grafana panels)
  -- ts_from_timestamptz and ts_to_timestamptz are already PUBLIC by default

  -- Reader/helper functions: revoke EXECUTE from PUBLIC for every non-trigger
  -- function in ash.*. Signatures are resolved dynamically via pg_proc so
  -- default arguments and future overloads do not cause drift. Admin
  -- functions above are re-revoked here (harmless: REVOKE is idempotent).
  for r in
    select p.proname,
           pg_catalog.pg_get_function_identity_arguments(p.oid) as args
    from pg_catalog.pg_proc p
    join pg_catalog.pg_namespace n on p.pronamespace = n.oid
    where n.nspname = 'ash'
      and p.prokind = 'f'
  loop
    execute format('revoke execute on function ash.%I(%s) from public',
                   r.proname, r.args);
  end loop;

  -- Re-grant EXECUTE on ts helpers to PUBLIC: these are pure, immutable
  -- timestamp <-> int4 conversion utilities with no access to sample data.
  -- Useful for Grafana panels and ad-hoc queries against rollup views.
  -- ash.epoch() must also be public since ts_from_timestamptz inlines a call to it.
  execute 'grant execute on function ash.epoch() to public';
  execute 'grant execute on function ash.ts_from_timestamptz(timestamptz) to public';
  execute 'grant execute on function ash.ts_to_timestamptz(int4) to public';

  -- Reader tables/views: revoke SELECT from PUBLIC for objects holding
  -- sample data, query text, and configuration. REVOKE on a partitioned
  -- parent does not cascade to partitions in PostgreSQL, so sample_N and
  -- query_map_N are enumerated dynamically below. Rollup tables hold
  -- aggregated wait/query data and must also be restricted.
  execute 'revoke select on table ash.sample from public';
  execute 'revoke select on table ash.query_map_all from public';
  execute 'revoke select on table ash.config from public';
  execute 'revoke select on table ash.wait_event_map from public';
  execute 'revoke select on table ash.rollup_1m from public';
  execute 'revoke select on table ash.rollup_1h from public';

  -- Per-slot partition/dictionary tables: sample_N and query_map_N.
  for r in
    select c.relname
    from pg_catalog.pg_class c
    join pg_catalog.pg_namespace n on c.relnamespace = n.oid
    where n.nspname = 'ash'
      and c.relkind in ('r', 'p')
      and (c.relname ~ '^query_map_[0-9]+$' or c.relname ~ '^sample_[0-9]+$')
  loop
    execute format('revoke select on ash.%I from public', r.relname);
  end loop;
end $$;

--------------------------------------------------------------------------------
-- STEP 7: Convenience helpers for monitoring roles
--------------------------------------------------------------------------------

-- ash.grant_reader(role) / ash.revoke_reader(role)
--
-- Convenience helpers that hand a monitoring role (Grafana, Datadog, an
-- on-call dashboard, etc.) the *minimum* privileges needed to invoke every
-- public reader function and read from the tables the readers depend on.
-- They are the inverse of the REVOKE-from-PUBLIC hardening above: instead
-- of opening up the schema globally, the operator names a specific role.
--
-- Granted set:
--   - USAGE on schema ash
--   - EXECUTE on every ash.* function EXCEPT the admin set (start, stop,
--     uninstall, rotate, take_sample, set_debug_logging, rebuild_partitions,
--     rollup_minute, rollup_hour, rollup_cleanup, _drop_all_partitions,
--     _rebuild_query_map_view, _merge_wait_counts, _merge_query_counts,
--     _truncate_pairs, _int4_array_cat_agg, _int8_array_cat_agg). Defining
--     "reader" by exclusion (rather than enumeration) keeps the helpers
--     correct as new readers and reader-internal helpers are added.
--   - SELECT on ash.sample (+ every sample_N partition), ash.query_map_all
--     (+ every query_map_N partition), ash.config, ash.wait_event_map,
--     ash.rollup_1m, ash.rollup_1h.
--
-- Both helpers are idempotent (safe to re-run), validate the role exists
-- via pg_roles, quote_ident() the role name, and emit a RAISE NOTICE
-- summarizing what was changed. revoke_reader() is the symmetric undo.
create or replace function ash.grant_reader(p_role name)
returns void
language plpgsql
set search_path = pg_catalog, ash
as $$
declare
  r record;
  v_role text;
  -- Canonical admin set lives in ash._admin_funcs(): a reader role must not
  -- receive EXECUTE on any of these (incl. grant_reader/revoke_reader, which
  -- would let the role hand out privileges).
  v_admin_funcs constant text[] := ash._admin_funcs();
  v_func_count int := 0;
  v_table_count int := 0;
begin
  if p_role is null or length(trim(p_role::text)) = 0 then
    raise exception 'ash.grant_reader: role name must not be null or empty';
  end if;

  -- Validate role exists. quote_ident() defends against SQL injection in
  -- the dynamic GRANT statements below, but a non-existent role would
  -- raise a confusing "role does not exist" from inside the loop —
  -- surface a clear error up front instead.
  if not exists (select 1 from pg_catalog.pg_roles where rolname = p_role) then
    raise exception 'ash.grant_reader: role % does not exist', quote_literal(p_role);
  end if;

  v_role := quote_ident(p_role);

  execute format('grant usage on schema ash to %s', v_role);

  -- EXECUTE on every reader function (= every ash.* function not in the
  -- admin set). Signatures are resolved dynamically via pg_proc so default
  -- arguments and future overloads do not cause drift.
  for r in
    select p.proname,
           pg_catalog.pg_get_function_identity_arguments(p.oid) as args
    from pg_catalog.pg_proc p
    join pg_catalog.pg_namespace n on p.pronamespace = n.oid
    where n.nspname = 'ash'
      and p.prokind = 'f'
      and p.proname::text <> all (v_admin_funcs)
  loop
    execute format('grant execute on function ash.%I(%s) to %s',
                   r.proname, r.args, v_role);
    v_func_count := v_func_count + 1;
  end loop;

  -- SELECT on reader tables. Partitioned-parent grants do not cascade to
  -- partitions in PostgreSQL, so sample_N and query_map_N are enumerated.
  execute format('grant select on table ash.sample to %s', v_role);
  execute format('grant select on table ash.query_map_all to %s', v_role);
  execute format('grant select on table ash.config to %s', v_role);
  execute format('grant select on table ash.wait_event_map to %s', v_role);
  execute format('grant select on table ash.rollup_1m to %s', v_role);
  execute format('grant select on table ash.rollup_1h to %s', v_role);
  v_table_count := v_table_count + 6;

  for r in
    select c.relname
    from pg_catalog.pg_class c
    join pg_catalog.pg_namespace n on c.relnamespace = n.oid
    where n.nspname = 'ash'
      and c.relkind in ('r', 'p')
      and (c.relname ~ '^query_map_[0-9]+$' or c.relname ~ '^sample_[0-9]+$')
  loop
    execute format('grant select on ash.%I to %s', r.relname, v_role);
    v_table_count := v_table_count + 1;
  end loop;

  raise notice 'ash.grant_reader: granted USAGE on schema ash, EXECUTE on % reader function(s), SELECT on % table(s) to %',
    v_func_count, v_table_count, v_role;
end;
$$;

comment on function ash.grant_reader(name) is
  'Grants the minimum privileges (USAGE on schema ash, EXECUTE on all reader functions, SELECT on reader tables incl. partitions) to a monitoring role. Idempotent. Inverse: ash.revoke_reader(name). Caveat: ash.rebuild_partitions(N, ''yes'') creates new partition tables that previously-granted readers cannot access; re-run ash.grant_reader() for each monitoring role after any rebuild_partitions() call.';

create or replace function ash.revoke_reader(p_role name)
returns void
language plpgsql
set search_path = pg_catalog, ash
as $$
declare
  r record;
  v_role text;
  -- See grant_reader: ash._admin_funcs() is the single source of truth.
  v_admin_funcs constant text[] := ash._admin_funcs();
  v_func_count int := 0;
  v_table_count int := 0;
begin
  if p_role is null or length(trim(p_role::text)) = 0 then
    raise exception 'ash.revoke_reader: role name must not be null or empty';
  end if;

  if not exists (select 1 from pg_catalog.pg_roles where rolname = p_role) then
    raise exception 'ash.revoke_reader: role % does not exist', quote_literal(p_role);
  end if;

  v_role := quote_ident(p_role);

  for r in
    select p.proname,
           pg_catalog.pg_get_function_identity_arguments(p.oid) as args
    from pg_catalog.pg_proc p
    join pg_catalog.pg_namespace n on p.pronamespace = n.oid
    where n.nspname = 'ash'
      and p.prokind = 'f'
      and p.proname::text <> all (v_admin_funcs)
  loop
    execute format('revoke execute on function ash.%I(%s) from %s',
                   r.proname, r.args, v_role);
    v_func_count := v_func_count + 1;
  end loop;

  execute format('revoke select on table ash.sample from %s', v_role);
  execute format('revoke select on table ash.query_map_all from %s', v_role);
  execute format('revoke select on table ash.config from %s', v_role);
  execute format('revoke select on table ash.wait_event_map from %s', v_role);
  execute format('revoke select on table ash.rollup_1m from %s', v_role);
  execute format('revoke select on table ash.rollup_1h from %s', v_role);
  v_table_count := v_table_count + 6;

  for r in
    select c.relname
    from pg_catalog.pg_class c
    join pg_catalog.pg_namespace n on c.relnamespace = n.oid
    where n.nspname = 'ash'
      and c.relkind in ('r', 'p')
      and (c.relname ~ '^query_map_[0-9]+$' or c.relname ~ '^sample_[0-9]+$')
  loop
    execute format('revoke select on ash.%I from %s', r.relname, v_role);
    v_table_count := v_table_count + 1;
  end loop;

  -- USAGE last so the in-flight statements above can still resolve ash.*
  -- by name even if the role had no other path to it. Idempotent.
  execute format('revoke usage on schema ash from %s', v_role);

  raise notice 'ash.revoke_reader: revoked USAGE on schema ash, EXECUTE on % reader function(s), SELECT on % table(s) from %',
    v_func_count, v_table_count, v_role;
end;
$$;

comment on function ash.revoke_reader(name) is
  'Revokes the privileges granted by ash.grant_reader(): USAGE on schema ash, EXECUTE on all reader functions, SELECT on reader tables. Idempotent. Inverse: ash.grant_reader(name). Caveat: ash.rebuild_partitions(N, ''yes'') creates new partition tables that previously-granted readers cannot access; re-run ash.grant_reader() for each monitoring role after any rebuild_partitions() call.';

-- Lock down the helpers themselves: only the schema owner may hand out
-- (or take back) privileges. PUBLIC must not be able to call them.
do $$
declare
  v_owner text := (select nspowner::regrole::text from pg_namespace where nspname = 'ash');
begin
  execute format('revoke all on function ash.grant_reader(name) from public');
  execute format('revoke all on function ash.revoke_reader(name) from public');
  execute format('grant execute on function ash.grant_reader(name) to %I', v_owner);
  execute format('grant execute on function ash.revoke_reader(name) to %I', v_owner);
end $$;
