-- pg_ash: Active Session History for Postgres
-- Version: 2.0 (development)
-- Fresh install: \i sql/ash-install.sql
-- Upgrade from 1.0: \i sql/ash-1.0-to-1.1.sql, then \i sql/ash-1.1-to-1.2.sql, then \i sql/ash-1.2-to-1.3.sql, then \i sql/ash-1.3-to-1.4.sql, then \i sql/ash-1.4-to-1.5.sql
-- Upgrade from 1.1: \i sql/ash-1.1-to-1.2.sql, then \i sql/ash-1.2-to-1.3.sql, then \i sql/ash-1.3-to-1.4.sql, then \i sql/ash-1.4-to-1.5.sql
-- Upgrade from 1.2: \i sql/ash-1.2-to-1.3.sql, then \i sql/ash-1.3-to-1.4.sql, then \i sql/ash-1.4-to-1.5.sql
-- Upgrade from 1.3: \i sql/ash-1.3-to-1.4.sql, then \i sql/ash-1.4-to-1.5.sql
-- Upgrade from 1.4: \i sql/ash-1.4-to-1.5.sql


-- Preserve function EXECUTE grants across the drop/recreate below (#107).
-- DROP FUNCTION destroys ACLs that CREATE OR REPLACE would keep, so a
-- monitoring role configured via ash.grant_reader() (or a manual GRANT)
-- would silently lose access on every upgrade or installer re-apply.
-- Snapshot every explicit non-owner EXECUTE grant on ash.* functions into
-- a temp table now; the matching block at the very end of this script
-- re-applies them to the recreated functions and drops the snapshot. The
-- argument signature is captured alongside the name so the restore can put
-- the grant back on the exact same overload and never widen a role that
-- held only one overload of a function.
-- PUBLIC (grantee oid 0, which has no pg_roles row) is intentionally
-- excluded: the hardening block below re-applies the REVOKE-from-PUBLIC
-- posture on every install.
do $$
begin
  drop table if exists pg_temp._ash_install_func_acl;
  create temp table _ash_install_func_acl (
    proname   name not null,
    args      text not null,
    grantee   name not null,
    grantable bool not null
  );
  insert into pg_temp._ash_install_func_acl (proname, args, grantee, grantable)
  select distinct p.proname,
         pg_catalog.pg_get_function_identity_arguments(p.oid),
         g.rolname, acl.is_grantable
  from pg_proc p
  join pg_namespace n on p.pronamespace = n.oid
  cross join lateral aclexplode(p.proacl) as acl
  join pg_roles g on g.oid = acl.grantee
  where n.nspname = 'ash'
    and p.prokind in ('f', 'a')
    and acl.privilege_type = 'EXECUTE'
    and acl.grantee <> p.proowner;

  -- Reader-role detection. The exact-signature restore at the end of this
  -- script can only replay grants on functions that existed BEFORE the
  -- upgrade, so a reader role would be left without EXECUTE on helpers
  -- introduced by THIS version (e.g. 2.0's _pgss_query_text and the
  -- retention helpers) and every reader call would then die mid-function.
  -- A role that held EXECUTE on the full pre-upgrade reader bundle (every
  -- non-admin ash.* function — exactly what ash.grant_reader() hands out)
  -- is recorded here and re-run through ash.grant_reader() during the
  -- restore, which grants the complete current closure. Detection uses the
  -- snapshotted explicit aclitems, not has_function_privilege(), so
  -- superusers and role-membership shortcuts never qualify; roles holding
  -- only partial manual grants keep the exact-signature restore path and
  -- are never widened. ash._admin_funcs() may not exist yet (fresh install,
  -- or upgrade from a version predating the #45 hardening — which also
  -- predates grant_reader, so there are no reader bundles to detect).
  drop table if exists pg_temp._ash_install_reader_roles;
  create temp table _ash_install_reader_roles (rolname name primary key);
  if to_regproc('ash._admin_funcs') is not null then
    insert into pg_temp._ash_install_reader_roles (rolname)
    select a.grantee
    from pg_temp._ash_install_func_acl a
    group by a.grantee
    having not exists (
      select 1
      from pg_proc p
      join pg_namespace n on p.pronamespace = n.oid
      where n.nspname = 'ash'
        and p.prokind = 'f'
        and p.proname::text <> all (ash._admin_funcs())
        and not exists (
          select 1
          from pg_temp._ash_install_func_acl a2
          where a2.grantee = a.grantee
            and a2.proname = p.proname
            and a2.args = pg_catalog.pg_get_function_identity_arguments(p.oid)
        )
    );
  end if;
end $$;

-- Drop functions removed or changed in 1.1 (handled by DO block below)
-- Drop ALL overloads of functions whose signatures changed across versions.
-- Using DO block because DROP FUNCTION requires exact arg types and we can't
-- predict which stale overloads exist from prior installs.
do $$
declare
  r record;
begin
  -- 2.0 drops the p_ prefix from every function parameter (p_from -> since,
  -- p_wait_event -> wait_event, ...). CREATE OR REPLACE cannot rename an
  -- input parameter, so every param-bearing function surviving from a
  -- pre-rename install must be dropped first (grants are preserved by the
  -- ACL snapshot/restore around this script, #107).
  --
  -- _sample_data_is_valid needs special handling: the sample_data_check
  -- constraint depends on it, so the constraint is dropped first (only when
  -- the function still carries the old p_data parameter name) and re-added
  -- by the constraint-migration block further down, which treats a missing
  -- constraint as "needs rewrite".
  if exists (
    select from pg_proc p
    join pg_namespace n on p.pronamespace = n.oid
    where n.nspname = 'ash'
      and p.proname = '_sample_data_is_valid'
      and p.proargnames[1] = 'p_data'
  ) then
    if to_regclass('ash.sample') is not null then
      alter table ash.sample drop constraint if exists sample_data_check;
    end if;
    drop function ash._sample_data_is_valid(integer[]);
  end if;

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
        'rebuild_partitions',
        -- 2.0 reader rework (issue #113): drop every removed v1.x reader, every
        -- draft aas_* function (all overloads / _at twins), and the earlier 2.0
        -- draft names that were renamed to the final surface (aas_by -> top,
        -- aas_series -> timeline, aas_compare -> compare, aas_periods -> periods,
        -- health_report -> report), plus the changed-signature name `samples`, so
        -- re-applying this installer over any prior install yields exactly the
        -- 2.0 surface (periods, aas, timeline, top, compare, samples, report,
        -- chart, summary). `aas` is a stable name but its signature changed, so
        -- it is dropped too to clear any old overload.
        'top_queries', 'top_queries_at',
        'wait_timeline', 'wait_timeline_at',
        'activity_summary',
        'samples', 'samples_at',
        'samples_by_database', 'samples_by_database_at',
        'minute_waits', 'minute_waits_at',
        'hourly_queries', 'hourly_queries_at',
        'daily_peak_backends', 'daily_peak_backends_at',
        '_to_sample_ts', '_pick_rollup_source',
        'aas', 'aas_at',
        'aas_periods',
        -- signature/return-type changes during the 2.0 development cycle:
        -- periods() renamed minutes_with_data -> buckets_with_data and gained a
        -- bucket column; top() gained order_by. Drop stale overloads so
        -- re-apply over an earlier 2.0 draft converges on the final surface.
        'periods', 'top',
        -- 2.0 parameter de-prefixing (see the note above the loop): every
        -- remaining param-bearing function whose parameter names changed.
        'ts_from_timestamptz', 'ts_to_timestamptz', '_register_wait',
        'decode_sample', 'decode_sample_at',
        'start', 'set_debug_logging',
        '_active_slots_for', '_active_slots_for_at',
        '_merge_wait_counts', '_merge_query_counts', '_truncate_pairs',
        'rollup_minute',
        '_color_on', '_wait_color', '_reset', '_bar',
        '_pick_source', '_pick_source_agg', '_raise_tie_retention',
        '_grain_counts', '_grain_by',
        'timeline', 'compare', '_pgss_query_text',
        '_hr_top_events', '_hr_top_queryids',
        'report', 'chart', 'summary',
        'grant_reader', 'revoke_reader',
        'aas_series', 'aas_by', 'aas_compare', 'health_report',
        'aas_timeline', 'aas_timeline_at',
        'aas_wait_types', 'aas_wait_types_at',
        'aas_wait_events', 'aas_wait_events_at',
        'aas_queryids', 'aas_queryids_at',
        'aas_summary', 'aas_summary_at',
        'aas_waits', 'aas_waits_at',
        'aas_queries', 'aas_queries_at'
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
  version                    text not null default '2.0',
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

-- Stamp the version on both fresh installs and upgrades. On an existing install
-- the `create table if not exists` above keeps the old row and column default,
-- so set them here explicitly (mirrors the released installer convention). This
-- also keeps the column default schema-identical between a fresh 2.0 install and
-- an upgrade chain landing on 2.0 (the CI schema-equivalence check).
update ash.config set version = '2.0' where singleton;
alter table ash.config alter column version set default '2.0';

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

-- Validate the packed ash.sample.data representation.
--
-- Layout: one or more groups of:
--   - negative wait-id marker
--   - positive backend count N
--   - exactly N non-negative query_map ids, where 0 means NULL query_id
--
-- Kept as a standalone immutable helper so both fresh installs and upgrades
-- can use one table CHECK expression. The function intentionally performs
-- shape validation only; wait/query dictionary lookups stay in readers.
create or replace function ash._sample_data_is_valid(data integer[])
returns boolean
language plpgsql
immutable
strict
parallel safe
set search_path = pg_catalog
as $$
declare
  v_len int;
  v_idx int := 1;
  v_count int;
  v_qid_idx int;
begin
  v_len := array_length(data, 1);
  if v_len is null or v_len < 3 or v_len > 100000 then
    return false;
  end if;

  while v_idx <= v_len loop
    if data[v_idx] is null or data[v_idx] >= 0 then
      return false;
    end if;
    v_idx := v_idx + 1;

    if v_idx > v_len then
      return false;
    end if;
    v_count := data[v_idx];
    if v_count is null or v_count <= 0 then
      return false;
    end if;
    v_idx := v_idx + 1;

    if v_idx + v_count - 1 > v_len then
      return false;
    end if;

    for v_qid_idx in 1..v_count loop
      if data[v_idx] is null or data[v_idx] < 0 then
        return false;
      end if;
      v_idx := v_idx + 1;
    end loop;
  end loop;

  return true;
end;
$$;

-- Sample table (partitioned by slot)
create table if not exists ash.sample (
  sample_ts    int4 not null,
  datid        oid not null,
  active_count smallint not null,
  data         integer[] not null
         check (ash._sample_data_is_valid(data)),
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

-- Migration (issues #49, #89): align sample.data check across upgrade paths.
-- v1.0 shipped `array_length(data, 1) >= 2`; v1.1 tightened it to `>= 3`;
-- v1.5 validates the full packed shape so impossible wait counts are rejected
-- at INSERT time. Detect and fix in place. Idempotent: only rewrites when the
-- current definition is missing or not using the validator helper. Drops on the
-- partitioned parent cascade to all partitions; ADD CONSTRAINT on the parent
-- propagates back to children.
do $$
declare
  v_def text;
  v_valid boolean;
  v_invalid bigint;
begin
  select pg_get_constraintdef(c.oid), c.convalidated
    into v_def, v_valid
  from pg_constraint c
  where c.conrelid = 'ash.sample'::regclass
    and c.conname  = 'sample_data_check';

  if v_def is null or v_def !~ '_sample_data_is_valid' then
    select count(*) into v_invalid
    from ash.sample
    where not ash._sample_data_is_valid(data);

    if v_invalid > 0 then
      create table if not exists ash.sample_malformed_1_5 (
        sample_ts int4 not null,
        datid oid not null,
        active_count smallint not null,
        data integer[] not null,
        slot smallint not null,
        quarantined_at timestamptz not null default clock_timestamp()
      );

      insert into ash.sample_malformed_1_5(sample_ts, datid, active_count, data, slot)
      select sample_ts, datid, active_count, data, slot
      from ash.sample
      where not ash._sample_data_is_valid(data);

      raise warning 'pg_ash upgrade: quarantined and deleted % malformed ash.sample row(s) rejected by the v1.5 data-shape validator',
        v_invalid;
      delete from ash.sample
      where not ash._sample_data_is_valid(data);
    end if;

    if v_def is not null then
      alter table ash.sample drop constraint sample_data_check;
    end if;
    alter table ash.sample
      add constraint sample_data_check
      check (ash._sample_data_is_valid(data));
  elsif not v_valid then
    alter table ash.sample validate constraint sample_data_check;
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
create or replace function ash.ts_from_timestamptz(ts timestamptz)
returns int4
language sql
immutable
parallel safe
set search_path = pg_catalog, ash
as $$
  select greatest(
           least(
             extract(epoch from ts - ash.epoch()),
             2147483647  -- int4 max; sample_ts can't exceed this without overflow
           ),
           0             -- sample_ts can't be negative; pre-epoch -> 0
         )::int4
$$;

-- Convert int4 epoch offset to timestamptz
create or replace function ash.ts_to_timestamptz(ts int4)
returns timestamptz
language sql
immutable
parallel safe
set search_path = pg_catalog, ash
as $$
  select ash.epoch() + ts * interval '1 second'
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
create or replace function ash._register_wait(state text, type text, event text)
returns smallint
language plpgsql
set search_path = pg_catalog, ash
as $$
-- Parameters share names with wait_event_map columns, so inside queries the
-- parameter side is always function-qualified (_register_wait.state, ...) and
-- the column side alias-qualified; use_column keeps the bare ON CONFLICT
-- column list below unambiguous.
#variable_conflict use_column
declare
  v_id     smallint;
  v_at_cap boolean;
begin
  -- Try to get existing
  select m.id into v_id
  from ash.wait_event_map m
  where m.state = _register_wait.state
    and m.type  = _register_wait.type
    and m.event = _register_wait.event;

  if v_id is not null then
    return v_id;
  end if;

  -- Enforce dictionary size cap before inserting. 32 000 stays well below
  -- smallint's 32 767 ceiling while still leaving room for genuine event
  -- diversity (real wait-event inventories measure in the hundreds).
  -- Use an exact existence probe for the 32 000th row instead of
  -- pg_class.reltuples: reltuples can be -1 or stale immediately after
  -- TRUNCATE/restore, which bypasses a hard cap until ANALYZE catches up.
  select exists (
    select 1 from ash.wait_event_map offset 31999 limit 1
  ) into v_at_cap;

  if v_at_cap then
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
      state, type, event;
    return null;  -- caller PERFORMs this; snapshot JOIN drops the session
  end if;

  -- Insert new entry
  insert into ash.wait_event_map (state, type, event)
  values (_register_wait.state, _register_wait.type, _register_wait.event)
  on conflict (state, type, event) do nothing
  returning id into v_id;

  -- If insert succeeded, return it
  if v_id is not null then
    return v_id;
  end if;

  -- Race condition: another session inserted, fetch it
  select m.id into v_id
  from ash.wait_event_map m
  where m.state = _register_wait.state
    and m.type  = _register_wait.type
    and m.event = _register_wait.event;

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
-- slot: when provided, look up query_ids from that partition only.
-- When NULL (default), search all partitions via query_map_all view.
--
-- M-BUG-9: validate the entire array shape before emitting ANY rows.
-- Previously the function interleaved validation with `return next`, so a
-- malformed trailing segment still produced one or more valid-looking rows
-- followed by a WARNING. Callers saw silently truncated, partially correct
-- output. Now: walk once to verify shape, then walk again to emit — on
-- validation failure raise a single warning and return zero rows.
create or replace function ash.decode_sample(data integer[], slot smallint default null)
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
  if data is null or array_length(data, 1) is null then
    return;
  end if;

  v_len := array_length(data, 1);

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
  if v_len < 3 or data[1] >= 0 then
    raise warning 'ash.decode_sample: invalid data array';
    return;
  end if;

  -- ---- Pass 1: validate shape only, emit nothing ----
  -- Reuses the same walker logic the old code had, but exits with a single
  -- warning and RETURN (no partial rows) if anything is wrong.
  v_idx := 1;
  while v_idx <= v_len loop
    if data[v_idx] >= 0 then
      raise warning 'ash.decode_sample: expected negative wait_id at position %', v_idx;
      return;
    end if;
    v_idx := v_idx + 1;

    if v_idx > v_len then
      raise warning 'ash.decode_sample: unexpected end of array at position % (missing count)', v_idx;
      return;
    end if;
    v_count := data[v_idx];
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
      if data[v_idx] < 0 then
        raise warning 'ash.decode_sample: expected non-negative query_id at position %', v_idx;
        return;
      end if;
      v_idx := v_idx + 1;
    end loop;
  end loop;

  -- ---- Pass 2: emit rows (shape is known good) ----
  v_idx := 1;
  while v_idx <= v_len loop
    v_wait_id := -data[v_idx];
    v_idx := v_idx + 1;

    v_count := data[v_idx];
    v_idx := v_idx + 1;

    -- Look up wait event info
    select w.type, w.event
    into v_type, v_event
    from ash.wait_event_map w
    where w.id = v_wait_id;

    -- Process each query_id
    for v_qid_idx in 1..v_count loop
      v_map_id := data[v_idx];
      v_idx := v_idx + 1;

      -- Handle sentinel (0 = NULL query_id)
      if v_map_id = 0 then
        v_query_id := null;
      elsif slot is not null then
        select m.query_id into v_query_id
        from ash.query_map_all m
        where m.slot = decode_sample.slot and m.id = v_map_id;
      else
        -- No slot context — search all partitions (less efficient).
        -- WARNING: after rotation, the same id may exist in multiple
        -- partitions with different query_ids (independent sequences).
        -- Result is nondeterministic. Always pass slot when available.
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
create or replace function ash.decode_sample(sample_ts int4)
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
  -- function-qualified: in a SQL-language body a bare sample_ts would resolve
  -- to the s.sample_ts column (columns take precedence), not the parameter
  where s.sample_ts = decode_sample.sample_ts
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
create or replace function ash.decode_sample_at(ts timestamptz)
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
  from ash.decode_sample(ash.ts_from_timestamptz(ts)) d
$$;

comment on function ash.decode_sample(integer[], smallint) is
$$Decodes a single ash.sample.data array into (wait_event, query_id, count) rows. Pass slot (ash.sample.slot) to resolve query_ids unambiguously; omitting it searches all query_map partitions and may return a stale id after rotation.$$;

comment on function ash.decode_sample(int4) is
$$Convenience overload: decodes every ash.sample row whose sample_ts equals sample_ts (across all datids/slots) and returns (datid, wait_event, query_id, count). Internally calls decode_sample(data, slot) with the row's slot, so query_id resolution is unambiguous.$$;

comment on function ash.decode_sample_at(timestamptz) is
$$Wall-clock convenience: same as decode_sample(int4) but accepts timestamptz, converting via ts_from_timestamptz() to find the matching sample_ts. Named with the _at suffix to avoid an unknown-typed decode_sample(123) literal matching both the int4 and timestamptz overloads.$$;


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
  v_rollup_result int;
  v_endangered_rows bigint;
  v_unrolled_groups bigint;
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

    -- Calculate new slot dynamically (0 -> 1 -> ... -> N-1 -> 0)
    v_new_slot := (v_old_slot + 1) % v_num_partitions;

    -- The partition to truncate is the one after the new slot
    v_truncate_slot := (v_new_slot + 1) % v_num_partitions;

    -- Set lock timeout to avoid blocking on long-running queries
    set local lock_timeout = '2s';

    -- Pre-truncation rollup: process endangered minutes before they are lost.
    -- This is no longer best-effort: if the endangered raw slot has rows and
    -- we cannot prove they are represented in rollup_1m, skip rotation rather
    -- than deleting the only copy of the samples (#81).
    execute format('select count(*) from ash.sample_%s', v_truncate_slot)
    into v_endangered_rows;

    if v_endangered_rows > 0 then
      if not pg_try_advisory_xact_lock(
           hashtext('pg_ash')::int4,
           hashtext('pg_ash_rollup')::int4
         ) then
        return format(
          'failed: pre-truncation rollup busy; slot %s not truncated',
          v_truncate_slot
        );
      end if;

      v_rotation_minutes := greatest(extract(epoch from v_rotation_period)::int / 60, 1);

      begin
        select ash.rollup_minute(v_rotation_minutes) into v_rollup_result;
      exception when undefined_function then
        return format(
          'failed: rollup_minute() unavailable; slot %s not truncated',
          v_truncate_slot
        );
      when others then
        raise warning 'ash.rotate: rollup_minute failed [%]: %', sqlstate, sqlerrm;
        return format(
          'failed: pre-truncation rollup failed [%s]; slot %s not truncated',
          sqlstate,
          v_truncate_slot
        );
      end;

      execute format(
        'with raw as ('
        '  select (sample_ts / 60) * 60 as ts, datid,'
        '         count(distinct sample_ts)::smallint as samples,'
        '         max(active_count)::smallint as peak_backends'
        '  from ash.sample_%1$s'
        '  group by 1, 2'
        ') '
        'select count(*) '
        'from raw '
        'left join ash.rollup_1m r on r.ts = raw.ts and r.datid = raw.datid '
        'where r.ts is null '
        '   or r.samples < raw.samples '
        '   or r.peak_backends < raw.peak_backends',
        v_truncate_slot
      ) into v_unrolled_groups;

      if v_unrolled_groups > 0 then
        return format(
          'failed: pre-truncation rollup incomplete for %s group(s) in slot %s; slot not truncated',
          v_unrolled_groups,
          v_truncate_slot
        );
      end if;
    end if;

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
  num_partitions int,
  confirm text default null
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
  if confirm is distinct from 'yes' then
    raise exception 'rebuild_partitions is destructive — all raw sample data '
      'will be lost. To proceed, call: '
      'select ash.rebuild_partitions(%, ''yes'')', num_partitions;
  end if;

  select c.num_partitions into v_old_n from ash.config c where c.singleton;
  v_new_n := coalesce(rebuild_partitions.num_partitions, v_old_n);

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
create or replace function ash.start(every interval default '1 second')
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
  if every is null then
    job_type := 'error';
    job_id := null;
    status := 'interval must not be null';
    return next;
    return;
  end if;

  v_seconds := extract(epoch from every)::int;
  if v_seconds < 1 then
    job_type := 'error';
    job_id := null;
    status := format('interval must be at least 1 second, got %s', every);
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
      status := format('interval must be exact minutes (60s, 120s, etc.), got %s', every);
      return next;
      return;
    end if;
    v_schedule := '*/' || (v_seconds / 60) || ' * * * *';
  else
    -- Convert to cron: every N hours (limit to 23 hours max for step syntax)
    if v_seconds % 3600 <> 0 then
      job_type := 'error';
      job_id := null;
      status := format('interval must be exact hours (3600s, 7200s, etc., up to 23h), got %s', every);
      return next;
      return;
    end if;
    v_hours := v_seconds / 3600;
    if v_hours > 23 then
      job_type := 'error';
      job_id := null;
      status := format('interval exceeds maximum 23 hours (82800s), got %s = %s hours. Use days or shorter interval.', every, v_hours);
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
      v_pg_cron_available, every, v_seconds;
  end if;

  -- If pg_cron is not available, just record the interval and advise on external scheduling
  if not v_pg_cron_available then
    update ash.config
    set sample_interval = every,
      sampling_enabled = true,
      skipped_samples = 0
    where singleton;

    job_type := 'sampler';
    job_id := null;
    status := format('interval set to %s — schedule externally (pg_cron not available)', every);
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
  set sample_interval = every,
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
create or replace function ash.set_debug_logging(enabled bool default null)
returns text
language plpgsql
set search_path = pg_catalog, ash
as $$
declare
  v_current bool;
begin
  select debug_logging into v_current from ash.config where singleton;

  if enabled is null then
    return 'debug_logging = ' || v_current::text;
  end if;

  update ash.config set debug_logging = enabled where singleton;

  if enabled then
    return 'debug_logging enabled — each sampled session will emit RAISE LOG';
  else
    return 'debug_logging disabled';
  end if;
end;
$$;

-- Uninstall: stop jobs and drop schema
create or replace function ash.uninstall(confirm text default null)
returns text
language plpgsql
set search_path = pg_catalog, ash
as $$
declare
  v_rec record;
  v_jobs_removed int := 0;
begin
  if confirm is distinct from 'yes' then
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
-- Returns every raw slot still retained by the configured N-partition ring.
-- For intervals beyond (num_partitions - 2) * rotation_period, returns an
-- empty array so reader `slot = any(...)` JOINs naturally yield zero rows —
-- honoring the NOTICE's "older samples not available" promise (and avoiding
-- the int4-epoch underflow that would otherwise raise `integer out of range`).
-- A single NOTICE is emitted per transaction in that case so callers get
-- a clear signal instead of a silent empty set.
-- Deduplication uses a transaction-scoped GUC (ash.notice_oversized) so
-- multi-query readers (e.g. activity_summary) don't spam the log with one
-- NOTICE per partition/sub-query.
--
-- NB: distinct name (not an overload of ash._active_slots()) because the
-- upgrade scripts re-create the zero-arg form on idempotent re-apply; an
-- overloaded pair would make bare ash._active_slots() ambiguous.
create or replace function ash._active_slots_for(lookback interval)
returns smallint[]
language plpgsql
stable
set search_path = pg_catalog, ash
as $$
declare
  v_current_slot smallint;
  v_num_partitions smallint;
  v_rotation_period interval;
  v_raw_retention interval;
  v_already text;
begin
  select current_slot, num_partitions, rotation_period
    into v_current_slot, v_num_partitions, v_rotation_period
  from ash.config
  where singleton;

  v_raw_retention := (v_num_partitions - 2) * v_rotation_period;

  -- Negative interval is meaningless and would underflow `now() - lookback`
  -- past the int4 horizon downstream. Treat as out-of-window.
  if lookback is not null and lookback < interval '0' then
    v_already := current_setting('ash.notice_oversized', true);
    if v_already is null or v_already = '' then
      raise notice
        'requested interval % is negative; nothing to retrieve.',
        lookback;
      perform set_config('ash.notice_oversized', '1', true);
    end if;
    return array[]::smallint[];
  end if;

  if lookback is not null and lookback > v_raw_retention then
    -- Suppress duplicate NOTICEs within the same transaction. The GUC is
    -- set local to the current transaction and auto-resets on commit/rollback.
    v_already := current_setting('ash.notice_oversized', true);
    if v_already is null or v_already = '' then
      raise notice
        'requested interval % exceeds raw retention (%); only % completed partition(s) plus the current partial partition are retained. Shorten the interval, increase rotation_period, or rebuild with more partitions.',
        lookback, v_raw_retention, v_num_partitions - 2;
      perform set_config('ash.notice_oversized', '1', true);
    end if;
    -- Honor the NOTICE: return no slots so reader JOINs (`slot = any(...)`)
    -- yield empty. Without this, an absurd interval like '1000 years' clamps
    -- v_min_ts to 0 and matches every retained sample, contradicting the
    -- "older samples not available" promise. Callers wanting all retained
    -- data should pass an interval <= raw_retention.
    return array[]::smallint[];
  end if;

  -- Slot enumeration must use the configured num_partitions and include every
  -- retained raw slot, not just current+previous. Readers still filter by
  -- sample_ts; the slot list only keeps the partition-pruning contract honest.
  return array(
    select ((v_current_slot - gs.i + v_num_partitions) % v_num_partitions)::smallint
    from generate_series(0, v_num_partitions - 2) as gs(i)
    order by gs.i
  );
end;
$$;

-- Absolute-range counterpart to ash._active_slots_for(interval), used by every
-- _at reader (top_waits_at, samples_at, query_waits_at, etc.). Returns the
-- active slot set when the requested [since, until) range overlaps what raw
-- samples retain ((num_partitions - 2) * rotation_period back from now()), and
-- an empty array with a NOTICE when it doesn't — restoring loud-warn symmetry
-- with the relative readers (#69). Without this, _at readers silently returned
-- 0 rows on absurd inputs (year 1000, year 3000) thanks to
-- ts_from_timestamptz()'s int4 clamp (#63), which was a UX regression vs the
-- interval path.
--
-- Out-of-retention conditions (each emits the NOTICE and returns {}):
--   * until   <= now() - raw_retention (range entirely too old)
--   * since >  now()                  (range entirely in the future)
--
-- Importantly, an empty range (since >= until) inside the retained window
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
  since timestamptz,
  until   timestamptz
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
  v_raw_retention   interval;
  v_retention_start timestamptz;
  v_already         text;
begin
  select current_slot, num_partitions, rotation_period
    into v_current_slot, v_num_partitions, v_rotation_period
  from ash.config
  where singleton;

  v_raw_retention := (v_num_partitions - 2) * v_rotation_period;
  v_retention_start := v_now - v_raw_retention;

  -- Out-of-retention check. Skip when either bound is null (the reader's
  -- own WHERE will yield empty without us needing to NOTICE) or when the
  -- range is empty inside retention (legitimate degenerate query).
  if since is not null and until is not null
     and (until <= v_retention_start or since > v_now) then
    v_already := current_setting('ash.notice_oversized', true);
    if v_already is null or v_already = '' then
      raise notice
        'requested range [%, %) lies outside the retained window (now - raw_retention .. now, i.e. [%, %)); only % completed partition(s) plus the current partial partition are retained. Adjust the range, increase rotation_period, or rebuild with more partitions.',
        since, until, v_retention_start, v_now, v_num_partitions - 2;
      perform set_config('ash.notice_oversized', '1', true);
    end if;
    return array[]::smallint[];
  end if;

  return array(
    select ((v_current_slot - gs.i + v_num_partitions) % v_num_partitions)::smallint
    from generate_series(0, v_num_partitions - 2) as gs(i)
    order by gs.i
  );
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

  -- Retention-start boundaries (2.0): the earliest timestamp each source can
  -- answer, so a caller can plan a window before querying and knows where the
  -- raw wait<->query drill stops. NULL when the source holds no data yet.
  metric := 'raw_retention_start';
  value := coalesce(ash._raw_retention_start()::text, 'no samples'); return next;
  metric := 'rollup_1m_retention_start';
  value := coalesce(ash._rollup_1m_retention_start()::text, 'no rollups'); return next;
  metric := 'rollup_1h_retention_start';
  value := coalesce(ash._rollup_1h_retention_start()::text, 'no rollups'); return next;

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
  minute_counts   int4[],            -- 60-slot per-minute total activity counts:
                                     -- element i = sum of wait_counts counts of the
                                     -- rollup_1m row at ts + (i-1)*60; NULL element =
                                     -- no rollup_1m row for that minute. Preserves the
                                     -- true minute-grain AAS distribution so long
                                     -- (rollup_1h-backed) windows report the exact
                                     -- per-minute peak_aas / p99_aas. NULL column =
                                     -- legacy pre-2.0 row (readers fall back to the
                                     -- flat hour average for such hours).
  primary key (ts, datid)
);

-- Upgrade path: pre-2.0 installs created rollup_1h without minute_counts.
alter table ash.rollup_1h
  add column if not exists minute_counts int4[];

-- Upgrade backfill: reconstruct minute_counts for legacy rollup_1h rows whose
-- hour is still covered by rollup_1m (per-minute detail survives there for
-- rollup_1m_retention_days). Older hours keep minute_counts NULL and readers
-- fall back to the flat hour average — a lower bound, never a wrong spike.
-- Idempotent: only touches rows where minute_counts is still NULL.
update ash.rollup_1h h
set minute_counts = (
  select array_agg(mt.total order by gs.idx)
  from generate_series(0, 59) gs(idx)
  left join lateral (
    select (select coalesce(sum(r.wait_counts[o + 1]), 0)::int4
            from generate_subscripts(r.wait_counts, 1) o
            where o % 2 = 1) as total
    from ash.rollup_1m r
    where r.datid = h.datid and r.ts = h.ts + gs.idx * 60
  ) mt on true
)
where h.minute_counts is null
  and exists (
    select from ash.rollup_1m r2
    where r2.datid = h.datid and r2.ts >= h.ts and r2.ts < h.ts + 3600
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
create or replace function ash._merge_wait_counts(flat int4[])
returns int4[]
language sql
immutable
parallel safe
set search_path = pg_catalog, ash
as $$
  with numbered as (
    select row_number() over () as pos, val
    from unnest(flat) as val
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
create or replace function ash._merge_query_counts(flat int8[])
returns int8[]
language sql
immutable
parallel safe
set search_path = pg_catalog, ash
as $$
  with numbered as (
    select row_number() over () as pos, val
    from unnest(flat) as val
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
create or replace function ash._truncate_pairs(arr int8[], n int)
returns int8[]
language sql
immutable
parallel safe
set search_path = pg_catalog, ash
as $$
  with numbered as (
    select row_number() over () as pos, val
    from unnest(arr) as val
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
    limit n
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
-- Processes all unprocessed complete minutes up to batch_limit.
-- Idempotent via ON CONFLICT DO UPDATE (upsert).
create or replace function ash.rollup_minute(
  batch_limit int default 60  -- max minutes to catch up per call
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
  v_sampler_lock_acquired bool := false;
begin
  -- Acquire rollup lock (xact-level). Rollup operations serialize with each
  -- other, and rebuild_partitions's drain poll waits on this objid.
  if not pg_try_advisory_xact_lock(
       hashtext('pg_ash')::int4,
       hashtext('pg_ash_rollup')::int4
     ) then
    return 0;
  end if;

  v_batch_remaining := batch_limit;

  select last_rollup_1m_ts, rollup_min_backend_seconds
  into v_last_ts, v_min_backend_seconds
  from ash.config where singleton;

  -- Drain in-flight samplers before choosing the "complete minute" boundary.
  -- take_sample() gets sample_ts from transaction-start now(), then may block
  -- before INSERT. Without this drain, rollup_minute() can advance the
  -- watermark while a sampler later commits an old-minute row, permanently
  -- excluding it from rollups (#81).
  if not pg_try_advisory_lock(
       hashtext('pg_ash')::int4,
       hashtext('pg_ash_sampler')::int4
     ) then
    return 0;
  end if;
  v_sampler_lock_acquired := true;

  begin
    -- Current minute boundary (only process *complete* minutes). This is
    -- computed after the sampler drain, so future samplers cannot commit rows
    -- with sample_ts older than this boundary.
    v_now_minute_ts := ash.ts_from_timestamptz(date_trunc('minute', now()));

    perform pg_advisory_unlock(
      hashtext('pg_ash')::int4,
      hashtext('pg_ash_sampler')::int4
    );
    v_sampler_lock_acquired := false;
  exception when others then
    if v_sampler_lock_acquired then
      perform pg_advisory_unlock(
        hashtext('pg_ash')::int4,
        hashtext('pg_ash_sampler')::int4
      );
    end if;
    raise;
  end;

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
      ts, datid, samples, peak_backends, wait_counts, query_counts,
      minute_counts
    )
    with m as (
      select datid, ts, samples, peak_backends, wait_counts, query_counts,
             -- per-minute total activity count = sum of the wait_counts counts
             (select coalesce(sum(wait_counts[o + 1]), 0)::int4
              from generate_subscripts(wait_counts, 1) o
              where o % 2 = 1) as minute_total
      from ash.rollup_1m
      where ts >= v_hour_start and ts < v_hour_end
    )
    select
      v_hour_start,
      m.datid,
      sum(m.samples)::smallint,
      max(m.peak_backends)::smallint,
      ash._merge_wait_counts(
        ash._int4_array_cat_agg(m.wait_counts) filter (where m.wait_counts <> '{}')
      ),
      ash._truncate_pairs(
        ash._merge_query_counts(
          ash._int8_array_cat_agg(m.query_counts) filter (where m.query_counts <> '{}')
        ),
        100  -- top 100 queries per hour
      ),
      -- 60-slot per-minute totals (NULL element = minute without a rollup_1m
      -- row); lets rollup_1h-backed readers keep the exact minute-grain
      -- peak_aas / p99_aas.
      (select array_agg(mm.minute_total order by gs.idx)
       from generate_series(0, 59) gs(idx)
       left join m mm on mm.datid = m.datid
                     and mm.ts = v_hour_start + gs.idx * 60)
    from m
    group by m.datid
    on conflict (ts, datid) do update set
      samples = excluded.samples,
      peak_backends = excluded.peak_backends,
      wait_counts = excluded.wait_counts,
      query_counts = excluded.query_counts,
      minute_counts = excluded.minute_counts;

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

drop function if exists ash.aas_summary(interval);
drop function if exists ash.aas_summary_at(timestamptz, timestamptz);
drop function if exists ash.periods(timestamptz);
drop function if exists ash.aas_waits(interval, text, int);
drop function if exists ash.aas_waits_at(timestamptz, timestamptz, text, int);
drop function if exists ash.aas_queries(interval, int);
drop function if exists ash.aas_queries_at(timestamptz, timestamptz, int);

-- Configured sample interval in seconds (>= a tiny floor to avoid div-by-zero).
-- Each rollup count is one sample appearance = sample_interval_secs of backend
-- time, so AAS = sum(count) * sample_interval_secs / wall_clock_seconds.
create or replace function ash._sample_interval_secs()
returns numeric
language sql
stable
set search_path = pg_catalog, ash
as $$
  select greatest(
           coalesce(extract(epoch from sample_interval)::numeric, 1),
           0.001
         )
  from ash.config
  where singleton
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
-- Colors: off by default. Enable per-call (color := true) or per-session:
--   set ash.color = on;
-- The session GUC avoids passing color to every function call.
-------------------------------------------------------------------------------

-- Resolve effective color state: explicit param wins, then session GUC.
create or replace function ash._color_on(color boolean default false)
returns boolean
language sql
stable
set search_path = pg_catalog, ash
as $$
  select color or coalesce(current_setting('ash.color', true), '') in ('on', 'true', '1');
$$;

create or replace function ash._wait_color(event text, color boolean default false)
returns text
language sql
stable
set search_path = pg_catalog, ash
as $$
  -- All escapes padded to 19 chars: \033[38;2;RRR;GGG;BBBm
  -- Uniform length prevents pspg right-border misalignment.
  select case when not ash._color_on(color) then '' else
    case
      when event like 'CPU%' then E'\033[38;2;080;250;123m'         -- green
      when event like 'IdleTx%' then E'\033[38;2;241;250;140m'      -- light yellow
      when event like 'IO:%' then E'\033[38;2;030;100;255m'         -- vivid blue
      when event like 'Lock:%' then E'\033[38;2;255;085;085m'       -- red
      when event like 'LWLock:%' then E'\033[38;2;255;121;198m'     -- pink
      when event like 'IPC:%' then E'\033[38;2;000;200;255m'        -- cyan
      when event like 'Client:%' then E'\033[38;2;255;220;100m'     -- yellow
      when event like 'Timeout:%' then E'\033[38;2;255;165;000m'    -- orange
      when event like 'BufferPin:%' then E'\033[38;2;000;210;180m'  -- teal
      when event like 'Activity:%' then E'\033[38;2;150;100;255m'   -- purple
      when event like 'Extension:%' then E'\033[38;2;190;150;255m'  -- light purple
      else E'\033[38;2;180;180;180m'                                   -- gray (unknown)
    end
  end;
$$;

-- Convenience: reset code, empty when color off
create or replace function ash._reset(color boolean default false)
returns text
language sql
stable
set search_path = pg_catalog, ash
as $$
  select case when ash._color_on(color) then E'\033[0m' else '' end;
$$;

-- Build a bar string with fixed visible width (for pspg/column alignment).
-- Visible: [blocks padded to width] + ' ' + pct + '%'
-- Invisible ANSI codes don't affect visual width.
--
-- width is clamped to [1, 500] to prevent reader-callable OOM via
-- unbounded `repeat()` on the █ character (a granted reader role could
-- otherwise pass width => 1_000_000_000 and allocate ~3 GB per row).
create or replace function ash._bar(
  event text,
  pct numeric,
  max_pct numeric,
  width int,
  color boolean default false
)
returns text
language sql
stable
set search_path = pg_catalog, ash
as $$
  -- All color escapes are now exactly 19 chars (zero-padded RGB).
  -- reset is always 4 chars. Total invisible = 23 when color on, 0 when off.
  select ash._wait_color(event, color)
    || rpad(
         repeat('█', greatest(1, (pct / nullif(max_pct, 0) * least(greatest(width, 1), 500))::int)),
         least(greatest(width, 1), 500)
       )
    || ash._reset(color)
    || lpad(pct || '%', 8);
$$;

--------------------------------------------------------------------------------
-- STEP 8: 2.0 reader / analysis API (AAS)
--------------------------------------------------------------------------------
-- The minimal AAS surface (issue #113, blueprints/AAS_API.md): seven data
-- functions (periods, aas, timeline, top, compare, samples, report) and two
-- render helpers (chart, summary). AAS = Average Active
-- Sessions. Every reader auto-selects its data source by window (raw ->
-- rollup_1m -> rollup_1h), reports it in a `source` column, and raises rather
-- than returning a silent empty result when a wait<->query drill exceeds raw
-- retention. Internal workhorses (_grain_counts / _grain_by) and the retention
-- helpers back the whole family.

-- Retention-start helpers: earliest timestamp each source can answer. Null when
-- the source holds no data. Used by source auto-selection and by the raw-drill
-- retention-boundary exception.
create or replace function ash._raw_retention_start()
returns timestamptz
language sql
stable
set search_path = pg_catalog, ash
as $$
  select ash.ts_to_timestamptz(min(sample_ts)) from ash.sample
$$;

create or replace function ash._rollup_1m_retention_start()
returns timestamptz
language sql
stable
set search_path = pg_catalog, ash
as $$
  select ash.ts_to_timestamptz(min(ts)) from ash.rollup_1m
$$;

create or replace function ash._rollup_1h_retention_start()
returns timestamptz
language sql
stable
set search_path = pg_catalog, ash
as $$
  select ash.ts_to_timestamptz(min(ts)) from ash.rollup_1h
$$;

-- Source auto-selection (the trust property, AAS_API.md §6): the finest source
-- whose retention reaches since. Raw is preferred within raw retention (most
-- accurate, and the only source that can tie wait<->query or answer while
-- rollups lag/are disabled); then rollup_1m, then rollup_1h. Returns 'none' only
-- when nothing holds data. Callers that need the tie force 'raw' and raise past
-- raw retention rather than falling back to a rollup that cannot answer.
create or replace function ash._pick_source(since timestamptz)
returns text
language sql
stable
set search_path = pg_catalog, ash
as $$
  select case
    when ash._raw_retention_start() is not null
         and since >= ash._raw_retention_start() then 'raw'
    when ash._rollup_1m_retention_start() is not null
         and since >= ash._rollup_1m_retention_start() then 'rollup_1m'
    when ash._rollup_1h_retention_start() is not null then 'rollup_1h'
    when ash._rollup_1m_retention_start() is not null then 'rollup_1m'
    when ash._raw_retention_start() is not null then 'raw'
    else 'none'
  end
$$;

-- Source selection for the AGGREGATE readers (aas / timeline / periods, and the
-- non-tie drills of top / chart). Raw and rollup_1m share per-minute grain, so
-- for anything wider than ~1 hour that rollup_1m fully covers we prefer rollup_1m
-- (a raw decode of a wide window spills hundreds of MB — the last-24h read cost
-- ~4.5s and ~500MB before this). Narrow windows still fall through to
-- _pick_source (raw preferred) so the freshest partial minute is captured, and
-- windows rollup can't cover (or where rollup is disabled/lagging) still fall to
-- raw / rollup_1h. Leaf tie-drills (top/samples) bypass this and force raw. The
-- source column stays honest — it names whatever was actually read.
create or replace function ash._pick_source_agg(since timestamptz, until timestamptz)
returns text
language sql
stable
set search_path = pg_catalog, ash
as $$
  select case
    when extract(epoch from (until - since)) > 3600
         and ash._rollup_1m_retention_start() is not null
         and since >= ash._rollup_1m_retention_start() then 'rollup_1m'
    else ash._pick_source(since)
  end
$$;

-- Raw-retention guard for the wait<->query tie drills (aas / timeline / top
-- with both a wait filter and query_id). Returns silently when raw samples
-- cover the window start; otherwise raises, with guidance split by case:
--   * window ENTIRELY past raw retention (or no raw samples at all): the tie
--     is unrecoverable — narrowing the window cannot help, so point to the
--     untied aggregate readers instead;
--   * PARTIAL overlap (window starts before raw retention but ends inside
--     it): narrowing the window to the raw-covered part recovers the drill.
-- start_ts / end_ts are the reader's minute-FLOORED window bounds (the guard
-- must reason about what actually gets queried); since is the un-floored window
-- start the caller passed, echoed in the messages so they reflect the user's
-- arguments rather than looking like pg_ash misheard them.
create or replace function ash._raise_tie_retention(
  raw_start timestamptz,
  start_ts int4,
  end_ts int4,
  since timestamptz
)
returns void
language plpgsql
stable
set search_path = pg_catalog, ash
as $$
declare
  v_next_boundary timestamptz;
begin
  if raw_start is not null
     and ash.ts_to_timestamptz(start_ts) >= raw_start then
    return;  -- raw covers the window start: the tie drill can proceed
  end if;
  if raw_start is null or ash.ts_to_timestamptz(end_ts) <= raw_start then
    raise exception 'pg_ash: this drill needs the raw wait<->query tie, but the requested window (% to %) is entirely outside raw retention (%). The tie is unrecoverable for that window — narrowing it will not help. Use the untied aggregate readers instead: drop either the wait filter or query_id (e.g. ash.aas(), ash.timeline(), ash.top() with one of the two).',
      since,
      ash.ts_to_timestamptz(end_ts),
      coalesce('raw retention starts at ' || raw_start, 'no raw samples exist');
  end if;
  -- The reader floors since to the minute BEFORE this guard runs, so a user
  -- who narrows to exactly raw_start (when it is mid-minute) re-floors below
  -- it and loops on this same error. Advise the first minute-aligned instant at
  -- or after raw retention start — a value that actually clears the guard.
  v_next_boundary := case
    when date_trunc('minute', raw_start) = raw_start then raw_start
    else date_trunc('minute', raw_start) + interval '1 minute'
  end;
  raise exception 'pg_ash: this drill needs raw samples; raw retention starts at % but the requested window starts at %. Narrow the window to start at or after % (the window end is still inside raw retention), or drill without the query/event tie.',
    raw_start, since, v_next_boundary;
end;
$$;

-- Workhorse: matching backend-count per underlying grain row (minute for raw /
-- rollup_1m, hour for rollup_1h) over [start_ts, end_ts), with uniform
-- filters. One row per grain timestamp that EXISTS in the source (cnt may be 0
-- when nothing matched) so callers can distinguish measured-zero from no-data.
-- 'raw' supports the wait<->query tie (both a wait filter and query_id);
-- the rollup sources cannot and must not be asked for it (caller routes such
-- requests to 'raw'). grain_secs is 60 (raw / rollup_1m / rollup_1h_minutes)
-- or 3600 (rollup_1h). 'rollup_1h_minutes' is the internal minute-grain view
-- of rollup_1h (unfiltered / database-filtered only) via minute_counts.
create or replace function ash._grain_counts(
  start_ts int4,
  end_ts int4,
  source text,
  wait_event_type text default null,
  wait_event text default null,
  query_id bigint default null,
  database name default null
)
returns table (
  ts int4,
  cnt numeric,
  grain_secs int4
)
language plpgsql
stable
set jit = off
set search_path = pg_catalog, ash
as $$
declare
  v_datid oid;
begin
  if database is not null then
    select d.oid into v_datid from pg_database d where d.datname = database;
    if v_datid is null then
      return;  -- unknown database name: no matching rows
    end if;
  end if;

  if source = 'raw' then
    return query
    with mins as (
      select distinct (s.sample_ts / 60) * 60 as mts
      from ash.sample s
      where s.slot = any(ash._active_slots_for_at(
                       ash.ts_to_timestamptz(start_ts),
                       ash.ts_to_timestamptz(end_ts)))
        and s.sample_ts >= start_ts and s.sample_ts < end_ts
        and (v_datid is null or s.datid = v_datid)
    ),
    expanded as (
      select (s.sample_ts / 60) * 60 as mts, s.slot, s.datid,
             (-s.data[i])::int as wait_id,
             s.data[i + 2 + gs.n] as map_id
      from ash.sample s,
        generate_subscripts(s.data, 1) i,
        lateral generate_series(0, greatest(s.data[i + 1] - 1, -1)) gs(n)
      where s.slot = any(ash._active_slots_for_at(
                       ash.ts_to_timestamptz(start_ts),
                       ash.ts_to_timestamptz(end_ts)))
        and s.sample_ts >= start_ts and s.sample_ts < end_ts
        and s.data[i] < 0
        and i + 1 <= array_length(s.data, 1)
        and i + 2 + gs.n <= array_length(s.data, 1)
        and (v_datid is null or s.datid = v_datid)
    ),
    matched as (
      select e.mts, count(*)::numeric as cnt
      from expanded e
      join ash.wait_event_map wm on wm.id = e.wait_id
      left join ash.query_map_all qm
        on qm.slot = e.slot and qm.id = e.map_id and e.map_id <> 0
      where (wait_event_type is null or wm.type = wait_event_type)
        and (wait_event is null
             or (case when wm.event = wm.type then wm.event
                      else wm.type || ':' || wm.event end) = wait_event
             or wm.event = wait_event)
        -- function-qualified: bare query_id is ambiguous against qm.query_id
        and (_grain_counts.query_id is null
             or qm.query_id = _grain_counts.query_id)
      group by e.mts
    )
    select m.mts, coalesce(mm.cnt, 0)::numeric, 60
    from mins m
    left join matched mm on mm.mts = m.mts;

  elsif source = 'rollup_1h_minutes' then
    -- Internal minute-grain view of rollup_1h via the preserved minute_counts
    -- arrays, so peak_aas / p99_aas survive the rollup_1m -> rollup_1h seam
    -- (same values a rollup_1m read of the window would produce). Totals only:
    -- wait/query filters need the hour-grain arrays, so callers route filtered
    -- reads to 'rollup_1h'. Legacy pre-2.0 rows (minute_counts is null) flatten
    -- to their hour average per covered minute — a lower bound for the peak and
    -- an "hour was flat" assumption for percentiles, never an invented spike.
    if wait_event_type is not null or wait_event is not null
       or query_id is not null then
      raise exception 'ash._grain_counts: rollup_1h_minutes supports no wait/query filters';
    end if;
    return query
    with hours as (
      select r.ts, r.datid, r.minute_counts, r.wait_counts
      from ash.rollup_1h r
      where r.ts >= start_ts - 3540 and r.ts < end_ts
        and (v_datid is null or r.datid = v_datid)
    ),
    mins as (
      select (h.ts + (u.idx - 1) * 60)::int4 as mts, u.mc::numeric as cnt
      from hours h,
        unnest(h.minute_counts) with ordinality u(mc, idx)
      where h.minute_counts is not null and u.mc is not null
      union all
      select (h.ts + gs.idx * 60)::int4,
             (select coalesce(sum(h.wait_counts[o + 1]), 0)
              from generate_subscripts(h.wait_counts, 1) o
              where o % 2 = 1)::numeric / 60.0
      from hours h,
        generate_series(0, 59) gs(idx)
      where h.minute_counts is null
    )
    select mins.mts, sum(mins.cnt)::numeric, 60
    from mins
    where mins.mts >= start_ts and mins.mts < end_ts
    group by mins.mts;

  elsif source = 'rollup_1h' then
    if query_id is not null then
      return query
      select r.ts, sum(sub.cnt)::numeric, 3600
      from ash.rollup_1h r
      cross join lateral (
        select coalesce(sum(r.query_counts[o + 1]), 0) as cnt
        from generate_subscripts(r.query_counts, 1) o
        where o % 2 = 1 and r.query_counts[o] = query_id
      ) sub
      where r.ts >= start_ts and r.ts < end_ts
        and (v_datid is null or r.datid = v_datid)
      group by r.ts;
    else
      return query
      select r.ts, sum(sub.cnt)::numeric, 3600
      from ash.rollup_1h r
      cross join lateral (
        select coalesce(sum(r.wait_counts[o + 1]), 0) as cnt
        from generate_subscripts(r.wait_counts, 1) o
        join ash.wait_event_map wm on wm.id = r.wait_counts[o]
        where o % 2 = 1
          and (wait_event_type is null or wm.type = wait_event_type)
          and (wait_event is null
               or (case when wm.event = wm.type then wm.event
                        else wm.type || ':' || wm.event end) = wait_event
               or wm.event = wait_event)
      ) sub
      where r.ts >= start_ts and r.ts < end_ts
        and (v_datid is null or r.datid = v_datid)
      group by r.ts;
    end if;

  else  -- rollup_1m
    if query_id is not null then
      return query
      select r.ts, sum(sub.cnt)::numeric, 60
      from ash.rollup_1m r
      cross join lateral (
        select coalesce(sum(r.query_counts[o + 1]), 0) as cnt
        from generate_subscripts(r.query_counts, 1) o
        where o % 2 = 1 and r.query_counts[o] = query_id
      ) sub
      where r.ts >= start_ts and r.ts < end_ts
        and (v_datid is null or r.datid = v_datid)
      group by r.ts;
    else
      return query
      select r.ts, sum(sub.cnt)::numeric, 60
      from ash.rollup_1m r
      cross join lateral (
        select coalesce(sum(r.wait_counts[o + 1]), 0) as cnt
        from generate_subscripts(r.wait_counts, 1) o
        join ash.wait_event_map wm on wm.id = r.wait_counts[o]
        where o % 2 = 1
          and (wait_event_type is null or wm.type = wait_event_type)
          and (wait_event is null
               or (case when wm.event = wm.type then wm.event
                        else wm.type || ':' || wm.event end) = wait_event
               or wm.event = wait_event)
      ) sub
      where r.ts >= start_ts and r.ts < end_ts
        and (v_datid is null or r.datid = v_datid)
      group by r.ts;
    end if;
  end if;
end;
$$;

-- ============================================================================
-- 2.0 DATA FUNCTIONS
-- ============================================================================

-- Scalar AAS load summary for one window, optionally filtered. avg_aas is the
-- window average; peak_aas / p99_aas are the max and 99th percentile of per
-- bucket AAS (zero-filled within data coverage) so a short spike is not hidden
-- by the average. backend_seconds is the absolute secondary. The window is
-- snapped to minute boundaries. Combining a wait filter with query_id needs
-- the raw wait<->query tie and raises past raw retention.
create or replace function ash.aas(
  since timestamptz default null,
  until timestamptz default null,
  wait_event_type text default null,
  wait_event text default null,
  query_id bigint default null,
  database name default null,
  bucket interval default '1 minute'
)
returns table (
  period_start timestamptz,
  period_end timestamptz,
  source text,
  buckets_expected bigint,
  buckets_with_data bigint,
  avg_aas numeric,
  peak_aas numeric,
  p99_aas numeric,
  backend_seconds numeric
)
language plpgsql
stable
set jit = off
set search_path = pg_catalog, ash
as $$
declare
  v_from timestamptz := coalesce(since, now() - interval '1 hour');
  v_to timestamptz := coalesce(until, now());
  v_start_ts int4;
  v_end_ts int4;
  v_bucket_secs int4;
  v_grain_secs int4;
  v_si numeric;
  v_source text;
  v_read_source text;
  v_tie boolean;
  v_raw_start timestamptz;
begin
  v_bucket_secs := extract(epoch from bucket)::int4;
  if v_bucket_secs is null or v_bucket_secs < 60 then
    raise exception 'bucket must be at least 1 minute, got %', bucket;
  end if;

  v_start_ts := (ash.ts_from_timestamptz(v_from) / 60) * 60;
  v_end_ts := (ash.ts_from_timestamptz(v_to) / 60) * 60;
  -- overflow-safe empty/degenerate-window guard (#63): never let v_start_ts + 60
  -- wrap past INT4_MAX near the 2094 epoch horizon.
  if v_end_ts <= v_start_ts then
    v_end_ts := least(v_start_ts::bigint + 60, 2147483647)::int4;
  end if;
  v_si := ash._sample_interval_secs();

  v_tie := query_id is not null
           and (wait_event_type is not null or wait_event is not null);

  if v_tie then
    v_source := 'raw';
    v_raw_start := ash._raw_retention_start();
    perform ash._raise_tie_retention(v_raw_start, v_start_ts, v_end_ts, v_from);
    v_grain_secs := 60;
  else
    v_source := ash._pick_source_agg(ash.ts_to_timestamptz(v_start_ts),
                                     ash.ts_to_timestamptz(v_end_ts));
    v_grain_secs := case when v_source = 'rollup_1h' then 3600 else 60 end;
  end if;
  v_read_source := v_source;

  -- rollup_1h preserves the per-minute totals (minute_counts), so the
  -- unfiltered / database-only read keeps minute grain across the
  -- rollup_1m -> rollup_1h seam: peak_aas / p99_aas stay per-minute and match
  -- what a rollup_1m-backed window of the same span reports (wider window
  -- never shrinks the peak). Wait/query filters still need the hour-grain
  -- arrays. The reported source stays 'rollup_1h' — that is what was read.
  if v_source = 'rollup_1h' and wait_event_type is null
     and wait_event is null and query_id is null then
    v_read_source := 'rollup_1h_minutes';
    v_grain_secs := 60;
  end if;

  -- peak/p99 bucket cannot be finer than the source grain
  v_bucket_secs := greatest(v_bucket_secs, v_grain_secs);

  return query
  -- Assign each grain row to its bucket arithmetically and equi-group (was a
  -- range self-join, O(buckets x grains) — a 1-month window planned as a
  -- nested-loop range join took ~38s). Only sampler-covered buckets appear in
  -- per_bucket, which is exactly the peak/p99 zero-fill frame; buckets_expected
  -- is counted arithmetically.
  -- Buckets are calendar-aligned: keyed by flooring the grain timestamp to
  -- bucket relative to ash.epoch() (midnight UTC), NOT to since's offset —
  -- a '1 hour' bucket starts on the hour, '1 day' on the UTC day. The same
  -- absolute window therefore always yields the same bucket boundaries
  -- (reproducible regardless of when the call is made). Edge buckets clipped
  -- by the window divide by their in-window coverage only.
  with grains as (
    select (g.ts / v_bucket_secs) * v_bucket_secs as bstart,
           g.cnt
    from ash._grain_counts(v_start_ts, v_end_ts, v_read_source,
           wait_event_type, wait_event, query_id, database) g
  ),
  per_bucket as (
    select bstart, count(*) as n, sum(cnt) as cnt
    from grains
    group by bstart
  ),
  bucket_aas as (
    select bstart,
           (cnt * v_si / (least(bstart + v_bucket_secs, v_end_ts)
                          - greatest(bstart, v_start_ts))) as aas
    from per_bucket
  )
  select
    ash.ts_to_timestamptz(v_start_ts),
    ash.ts_to_timestamptz(v_end_ts),
    v_source,
    (((v_end_ts - 1) / v_bucket_secs) - (v_start_ts / v_bucket_secs) + 1)::bigint,
    (select count(*) from per_bucket)::bigint,
    round((select coalesce(sum(cnt), 0) from per_bucket) * v_si
          / (v_end_ts - v_start_ts)::numeric, 2),
    coalesce(round((select max(aas) from bucket_aas), 2), 0),
    coalesce(round((select percentile_cont(0.99) within group (order by aas)
                    from bucket_aas)::numeric, 2), 0),
    round((select coalesce(sum(cnt), 0) from per_bucket) * v_si, 2);
end;
$$;

-- AAS time series: one row per bucket across the whole window (no-data buckets
-- included with data_points = 0 and null AAS). bucket => null auto-selects
-- grain by span. peak_aas is the worst underlying grain within the bucket;
-- p99_aas is the 99th percentile of the per-grain AAS. Unfiltered /
-- database-filtered reads are minute-grain on every source (rollup_1h keeps
-- per-minute totals in minute_counts); p99_aas is null only for wait/query-
-- filtered rollup_1h-backed buckets, which remain hour-grain.
create or replace function ash.timeline(
  since timestamptz default null,
  until timestamptz default null,
  bucket interval default null,
  wait_event_type text default null,
  wait_event text default null,
  query_id bigint default null,
  database name default null
)
returns table (
  bucket_start timestamptz,
  source text,
  data_points bigint,
  avg_aas numeric,
  peak_aas numeric,
  p99_aas numeric
)
language plpgsql
stable
set jit = off
set search_path = pg_catalog, ash
as $$
declare
  v_from timestamptz := coalesce(since, now() - interval '1 hour');
  v_to timestamptz := coalesce(until, now());
  v_start_ts int4;
  v_end_ts int4;
  v_span int4;
  v_bucket_secs int4;
  v_grain_secs int4;
  v_si numeric;
  v_source text;
  v_read_source text;
  v_tie boolean;
  v_raw_start timestamptz;
begin
  v_start_ts := (ash.ts_from_timestamptz(v_from) / 60) * 60;
  v_end_ts := (ash.ts_from_timestamptz(v_to) / 60) * 60;
  -- overflow-safe empty/degenerate-window guard (#63).
  if v_end_ts <= v_start_ts then
    v_end_ts := least(v_start_ts::bigint + 60, 2147483647)::int4;
  end if;
  v_span := v_end_ts - v_start_ts;

  if bucket is null then
    v_bucket_secs := case
      when v_span <= 6 * 3600 then 60
      when v_span <= 7 * 86400 then 3600
      else 86400 end;
  else
    v_bucket_secs := extract(epoch from bucket)::int4;
    if v_bucket_secs is null or v_bucket_secs < 60 then
      raise exception 'bucket must be at least 1 minute, got %', bucket;
    end if;
  end if;

  -- Bound the emitted-row count: one row per bucket, so an explicit fine bucket
  -- over a very wide window can blow up (1 minute over 10 years ~ 5M rows). Cap
  -- at 100000 and tell the caller to widen bucket.
  if (v_span::bigint / v_bucket_secs) > 100000 then
    raise exception 'ash.timeline: % buckets exceeds the 100000-row cap; use a coarser bucket (or bucket => null for auto grain)',
      (v_span::bigint / v_bucket_secs);
  end if;

  v_si := ash._sample_interval_secs();

  v_tie := query_id is not null
           and (wait_event_type is not null or wait_event is not null);
  if v_tie then
    v_source := 'raw';
    v_raw_start := ash._raw_retention_start();
    perform ash._raise_tie_retention(v_raw_start, v_start_ts, v_end_ts, v_from);
    v_grain_secs := 60;
  else
    v_source := ash._pick_source_agg(ash.ts_to_timestamptz(v_start_ts),
                                     ash.ts_to_timestamptz(v_end_ts));
    if v_source = 'rollup_1h' and wait_event_type is null
       and wait_event is null and query_id is null then
      -- rollup_1h preserves per-minute totals (minute_counts), so the
      -- unfiltered / database-only read keeps minute grain: peak_aas / p99_aas
      -- stay per-minute across the rollup_1m -> rollup_1h seam, and sub-hour
      -- buckets work. The reported source stays 'rollup_1h'.
      v_read_source := 'rollup_1h_minutes';
    elsif v_source = 'rollup_1h' and v_bucket_secs < 3600 then
      -- filtered sub-hour buckets need minute grain; the hour-grain arrays
      -- cannot supply it, so fall back to rollup_1m (older buckets simply show
      -- no data). 'none' (a truly empty window) is left as-is and reported
      -- honestly.
      v_source := 'rollup_1m';
    end if;
    v_read_source := coalesce(v_read_source, v_source);
    v_grain_secs := case when v_read_source = 'rollup_1h' then 3600 else 60 end;
  end if;
  v_read_source := coalesce(v_read_source, v_source);

  return query
  -- Arithmetic bucket-keying + equi-join (was an O(buckets x grains) range
  -- join). No-data buckets still appear via the left join from the full bucket
  -- series, with data_points = 0 and null AAS.
  -- Buckets are calendar-aligned (floored to bucket relative to ash.epoch(),
  -- midnight UTC), not anchored to since: the same absolute window always
  -- yields the same bucket_start labels, and an hour/day bucket carries its
  -- calendar hour/UTC-day label. The first bucket_start may therefore precede
  -- since; edge buckets divide by their in-window coverage only.
  with grains as (
    select (g.ts / v_bucket_secs) * v_bucket_secs as bstart,
           (g.cnt * v_si / v_grain_secs) as gaas, g.cnt
    from ash._grain_counts(v_start_ts, v_end_ts, v_read_source,
           wait_event_type, wait_event, query_id, database) g
  ),
  agg as (
    select bstart, count(*) as n, sum(cnt) as cnt, max(gaas) as peak,
           percentile_cont(0.99) within group (order by gaas) as p99
    from grains
    group by bstart
  ),
  buckets as (
    select gs.ts::int4 as bstart
    from generate_series(((v_start_ts / v_bucket_secs) * v_bucket_secs)::bigint,
                         (v_end_ts - 1)::bigint, v_bucket_secs) gs(ts)
  )
  select
    ash.ts_to_timestamptz(b.bstart),
    v_source,
    coalesce(a.n, 0)::bigint,
    case when a.n > 0 then
      round(a.cnt * v_si / (least(b.bstart + v_bucket_secs, v_end_ts)
                            - greatest(b.bstart, v_start_ts)), 2)
    end,
    case when a.n > 0 then round(a.peak, 2) end,
    -- p99 stays null only for the genuinely hour-grain read (filtered
    -- rollup_1h): a percentile over hour averages would masquerade as a
    -- minute percentile. The unfiltered rollup_1h read is minute-grain via
    -- minute_counts and reports a real per-minute p99.
    case when a.n > 0 and v_read_source <> 'rollup_1h' then
      round(a.p99::numeric, 2)
    end
  from buckets b
  left join agg a on a.bstart = b.bstart
  order by b.bstart;
end;
$$;

-- Standard trailing windows for triage: one summary row per window ending at
-- until. Each window delegates to ash.aas(), which auto-selects its source
-- (short windows may read raw for the freshest partial minute; the wide windows
-- read rollups). peak_aas/p99_aas are per-minute (worst / 99th-percentile
-- minute), which is what capacity triage wants, and buckets_with_data is a
-- true count of covered buckets at the grain named by the bucket column —
-- always '1 minute' here (unfiltered reads are minute-grain on every source,
-- incl. rollup_1h via minute_counts), exposed per row so "43200 buckets" reads
-- as "43200 @ 1 minute" without cross-referencing. After the
-- arithmetic-bucketing fix this is cheap even for the 1-month window (~90ms
-- for the whole call on a month of rollups).
create or replace function ash.periods(
  until timestamptz default null
)
returns table (
  period text,
  period_start timestamptz,
  period_end timestamptz,
  source text,
  bucket interval,
  buckets_with_data bigint,
  avg_aas numeric,
  peak_aas numeric,
  p99_aas numeric
)
language sql
stable
set jit = off
set search_path = pg_catalog, ash
as $$
  with e(end_ts) as (
    select date_trunc('minute', coalesce(until, now()))
  ),
  periods(label, span) as (
    values
      ('1m'::text,  interval '1 minute'),
      ('5m',        interval '5 minutes'),
      ('1h',        interval '1 hour'),
      ('1d',        interval '1 day'),
      ('1w',        interval '1 week'),
      ('1mo',       interval '30 days')
  )
  select
    p.label,
    a.period_start,
    a.period_end,
    a.source,
    interval '1 minute',
    a.buckets_with_data,
    a.avg_aas,
    a.peak_aas,
    a.p99_aas
  from periods p
  cross join e
  cross join lateral ash.aas(e.end_ts - p.span, e.end_ts) a
$$;

-- Per-key backend-count per grain row for a breakdown dimension, with uniform
-- filters. Companion to _grain_counts. key is the dimension value (text);
-- key_num carries the numeric query_id for the 'query_id' dimension (null
-- otherwise) so the caller can join query text. 'raw' supports the wait<->query
-- tie; rollup sources must not be asked for it.
create or replace function ash._grain_by(
  start_ts int4,
  end_ts int4,
  source text,
  dimension text,
  wait_event_type text default null,
  wait_event text default null,
  query_id bigint default null,
  database name default null
)
returns table (
  ts int4,
  key text,
  key_num bigint,
  cnt numeric
)
language plpgsql
stable
set jit = off
set search_path = pg_catalog, ash
as $$
declare
  v_datid oid;
  v_tbl text;
  v_disp constant text :=
    '(case when wm.event = wm.type then wm.event else wm.type || '':'' || wm.event end)';
begin
  if dimension not in ('wait_event_type', 'wait_event', 'query_id', 'database') then
    raise exception 'ash.top: unknown dimension %; use wait_event_type|wait_event|query_id|database', dimension;
  end if;
  if database is not null then
    select d.oid into v_datid from pg_database d where d.datname = database;
    if v_datid is null then return; end if;
  end if;

  if source = 'raw' then
    return query
    with expanded as (
      select (s.sample_ts / 60) * 60 as mts, s.slot, s.datid,
             (-s.data[i])::int as wait_id,
             s.data[i + 2 + gs.n] as map_id
      from ash.sample s,
        generate_subscripts(s.data, 1) i,
        lateral generate_series(0, greatest(s.data[i + 1] - 1, -1)) gs(n)
      where s.slot = any(ash._active_slots_for_at(
                       ash.ts_to_timestamptz(start_ts),
                       ash.ts_to_timestamptz(end_ts)))
        and s.sample_ts >= start_ts and s.sample_ts < end_ts
        and s.data[i] < 0
        and i + 1 <= array_length(s.data, 1)
        and i + 2 + gs.n <= array_length(s.data, 1)
        and (v_datid is null or s.datid = v_datid)
    ),
    dec as (
      select e.mts,
             wm.type as wet,
             (case when wm.event = wm.type then wm.event
                   else wm.type || ':' || wm.event end) as evt,
             qm.query_id as qid,
             e.datid
      from expanded e
      join ash.wait_event_map wm on wm.id = e.wait_id
      left join ash.query_map_all qm
        on qm.slot = e.slot and qm.id = e.map_id and e.map_id <> 0
      where (wait_event_type is null or wm.type = wait_event_type)
        and (wait_event is null
             or (case when wm.event = wm.type then wm.event
                      else wm.type || ':' || wm.event end) = wait_event
             or wm.event = wait_event)
        -- function-qualified: bare query_id is ambiguous against qm.query_id
        and (_grain_by.query_id is null or qm.query_id = _grain_by.query_id)
    )
    select d.mts,
           case dimension
             when 'wait_event_type' then d.wet
             when 'wait_event' then d.evt
             -- unattributed activity (no query_id captured) keeps a NULL key,
             -- not a sentinel string: callers can tell "no attribution" from a
             -- real key without parsing.
             when 'query_id' then d.qid::text
             else coalesce((select dd.datname::text from pg_database dd where dd.oid = d.datid),
                           '<oid:' || d.datid || '>')
           end,
           case when dimension = 'query_id' then d.qid else null end,
           count(*)::numeric
    from dec d
    group by 1, 2, 3;
    return;
  end if;

  v_tbl := case when source = 'rollup_1h' then 'ash.rollup_1h' else 'ash.rollup_1m' end;

  if dimension in ('wait_event_type', 'wait_event') then
    return query execute format($q$
      select r.ts,
             %s as key, null::bigint as key_num,
             sum(r.wait_counts[o + 1])::numeric as cnt
      from %s r
      cross join generate_subscripts(r.wait_counts, 1) o
      join ash.wait_event_map wm on wm.id = r.wait_counts[o]
      where o %% 2 = 1 and r.ts >= $1 and r.ts < $2
        and ($3 is null or r.datid = $3)
        and ($4 is null or wm.type = $4)
        and ($5 is null or %s = $5 or wm.event = $5)
      group by r.ts, key
    $q$,
      case when dimension = 'wait_event_type' then 'wm.type' else v_disp end,
      v_tbl, v_disp)
    using start_ts, end_ts, v_datid, wait_event_type, wait_event;

  elsif dimension = 'query_id' then
    return query execute format($q$
      select r.ts, r.query_counts[o]::text as key,
             r.query_counts[o]::bigint as key_num,
             sum(r.query_counts[o + 1])::numeric as cnt
      from %s r
      cross join generate_subscripts(r.query_counts, 1) o
      where o %% 2 = 1 and r.ts >= $1 and r.ts < $2
        and ($3 is null or r.datid = $3)
        and ($4 is null or r.query_counts[o] = $4)
      group by r.ts, r.query_counts[o]
    $q$, v_tbl)
    using start_ts, end_ts, v_datid, query_id;

  else  -- database
    if query_id is not null then
      return query execute format($q$
        select r.ts,
               coalesce(d.datname::text, '<oid:' || r.datid || '>') as key,
               null::bigint as key_num,
               (select coalesce(sum(r.query_counts[o + 1]), 0)
                from generate_subscripts(r.query_counts, 1) o
                where o %% 2 = 1 and r.query_counts[o] = $4)::numeric as cnt
        from %s r
        left join pg_database d on d.oid = r.datid
        where r.ts >= $1 and r.ts < $2 and ($3 is null or r.datid = $3)
      $q$, v_tbl)
      using start_ts, end_ts, v_datid, query_id;
    else
      return query execute format($q$
        select r.ts,
               coalesce(d.datname::text, '<oid:' || r.datid || '>') as key,
               null::bigint as key_num,
               (select coalesce(sum(r.wait_counts[o + 1]), 0)
                from generate_subscripts(r.wait_counts, 1) o
                join ash.wait_event_map wm on wm.id = r.wait_counts[o]
                where o %% 2 = 1
                  and ($4 is null or wm.type = $4)
                  and ($5 is null or %s = $5 or wm.event = $5))::numeric as cnt
        from %s r
        left join pg_database d on d.oid = r.datid
        where r.ts >= $1 and r.ts < $2 and ($3 is null or r.datid = $3)
      $q$, v_disp, v_tbl)
      using start_ts, end_ts, v_datid, wait_event_type, wait_event;
    end if;
  end if;
end;
$$;

-- The single vertical drill: AAS broken down by one dimension, every row
-- carrying avg/peak/p99 plus its share (pct) of the window total. Filters
-- compose with the dimension. Crossing the wait<->query tie (query_id
-- dimension with a wait filter, or a wait dimension with query_id) needs raw
-- samples and raises past raw retention. query_text is filled only for the
-- query_id dimension with pg_stat_statements present. order_by picks the
-- ranking metric BEFORE the top-n cut: 'avg' (default; sustained load),
-- 'peak' or 'p99' (spike-first — a query dominant for one minute of an
-- incident ranks above cosmetic baseline rows that beat it on average).
create or replace function ash.top(
  dimension text,
  since timestamptz default null,
  until timestamptz default null,
  wait_event_type text default null,
  wait_event text default null,
  query_id bigint default null,
  database name default null,
  n int default 10,
  bucket interval default '1 minute',
  order_by text default 'avg'
)
returns table (
  key text,
  query_text text,
  source text,
  avg_aas numeric,
  peak_aas numeric,
  p99_aas numeric,
  backend_seconds numeric,
  pct numeric
)
language plpgsql
stable
set jit = off
-- public in search_path for pg_stat_statements query-text lookup (see samples()).
set search_path = pg_catalog, ash, public
as $$
declare
  v_from timestamptz := coalesce(since, now() - interval '1 hour');
  v_to timestamptz := coalesce(until, now());
  v_start_ts int4;
  v_end_ts int4;
  v_bucket_secs int4;
  v_grain_secs int4;
  v_si numeric;
  v_source text;
  v_tie boolean;
  v_raw_start timestamptz;
  v_has_pgss boolean := false;
  v_key_num bigint;
begin
  if dimension not in ('wait_event_type', 'wait_event', 'query_id', 'database') then
    raise exception 'ash.top: unknown dimension %; use wait_event_type|wait_event|query_id|database', dimension;
  end if;
  if order_by not in ('avg', 'peak', 'p99') then
    raise exception 'ash.top: unknown order_by %; use avg|peak|p99', order_by;
  end if;
  v_bucket_secs := extract(epoch from bucket)::int4;
  if v_bucket_secs is null or v_bucket_secs < 60 then
    raise exception 'bucket must be at least 1 minute, got %', bucket;
  end if;

  v_start_ts := (ash.ts_from_timestamptz(v_from) / 60) * 60;
  v_end_ts := (ash.ts_from_timestamptz(v_to) / 60) * 60;
  -- overflow-safe empty/degenerate-window guard (#63).
  if v_end_ts <= v_start_ts then
    v_end_ts := least(v_start_ts::bigint + 60, 2147483647)::int4;
  end if;
  v_si := ash._sample_interval_secs();

  v_tie := (dimension in ('wait_event_type', 'wait_event') and query_id is not null)
        or (dimension = 'query_id' and (wait_event_type is not null or wait_event is not null))
        or (dimension = 'database' and query_id is not null
            and (wait_event_type is not null or wait_event is not null));

  if v_tie then
    v_source := 'raw';
    v_raw_start := ash._raw_retention_start();
    perform ash._raise_tie_retention(v_raw_start, v_start_ts, v_end_ts, v_from);
    v_grain_secs := 60;
  else
    -- non-tie breakdown is an aggregate read: prefer rollup for wide windows.
    v_source := ash._pick_source_agg(ash.ts_to_timestamptz(v_start_ts),
                                     ash.ts_to_timestamptz(v_end_ts));
    v_grain_secs := case when v_source = 'rollup_1h' then 3600 else 60 end;
  end if;
  v_bucket_secs := greatest(v_bucket_secs, v_grain_secs);

  if dimension = 'query_id' and ash._pgss_schema() is not null then
    begin
      perform 1 from pg_stat_statements limit 1;
      v_has_pgss := true;
    exception when others then
      v_has_pgss := false;
    end;
  end if;

  for key, v_key_num, source, avg_aas, peak_aas, p99_aas, backend_seconds, pct in
  -- Buckets are calendar-aligned (floored to bucket relative to ash.epoch(),
  -- midnight UTC), matching ash.aas()/ash.timeline(): the same absolute window
  -- always yields the same bucket boundaries. Edge buckets clipped by the
  -- window divide by their in-window coverage only.
  with keyed as (
    select (b.ts / v_bucket_secs) * v_bucket_secs as bstart,
           b.key, b.key_num, b.cnt
    from ash._grain_by(v_start_ts, v_end_ts, v_source, dimension,
           wait_event_type, wait_event, query_id, database) b
  ),
  -- Zero-fill frame = the sampler-covered buckets, derived from the source's
  -- grain set INDEPENDENT of the dimension/filter (#6). Deriving it from the
  -- filtered rows made a key's p99 move when OTHER keys changed and disagree
  -- with ash.aas() for the same drill. database is the only filter that
  -- legitimately restricts coverage, so it is the only one passed here.
  covered as (
    select distinct (g.ts / v_bucket_secs) * v_bucket_secs as bstart
    from ash._grain_counts(v_start_ts, v_end_ts, v_source,
           null, null, null, database) g
  ),
  keys as (
    select k.key, max(k.key_num) as key_num, sum(k.cnt) as total
    from keyed k
    group by k.key
    having sum(k.cnt) > 0
  ),
  grand as (select coalesce(sum(total), 0) as g from keys),
  -- Only the returned top-N need per-bucket peak/p99, so for the default
  -- avg ordering, limit before the cross-join zero-fill (bounds it to
  -- N x covered buckets). peak/p99 ordering must rank on the per-bucket
  -- stats themselves, so it keeps every key through the zero-fill (same
  -- cost class as compare(), which already runs top with an unbounded
  -- limit) and cuts at the final ORDER BY.
  top_keys as (
    select * from keys order by total desc
    limit case when order_by = 'avg' then greatest(n, 0)
          else 2147483647 end
  ),
  key_bucket_data as (
    select k2.key, k2.bstart, sum(k2.cnt) as bcnt
    from keyed k2 group by k2.key, k2.bstart
  ),
  key_bucket as (
    select tk.key, tk.key_num, tk.total, cb.bstart,
           coalesce(kbd.bcnt, 0) as bcnt
    from top_keys tk
    cross join covered cb
    left join key_bucket_data kbd
      on kbd.key is not distinct from tk.key and kbd.bstart = cb.bstart
  ),
  per_key as (
    select kb.key, kb.key_num, kb.total,
           round(kb.total * v_si / (v_end_ts - v_start_ts)::numeric, 2) as avg_aas,
           round(max(kb.bcnt * v_si
                     / (least(kb.bstart + v_bucket_secs, v_end_ts)
                        - greatest(kb.bstart, v_start_ts))), 2) as peak_aas,
           round(percentile_cont(0.99) within group (
                   order by kb.bcnt * v_si
                     / (least(kb.bstart + v_bucket_secs, v_end_ts)
                        - greatest(kb.bstart, v_start_ts)))::numeric, 2) as p99_aas
    from key_bucket kb
    group by kb.key, kb.key_num, kb.total
  )
  select
    pk.key,
    pk.key_num,
    v_source,
    pk.avg_aas,
    pk.peak_aas,
    pk.p99_aas,
    round(pk.total * v_si, 2),
    round(pk.total * 100.0 / nullif(g.g, 0), 2)
  from per_key pk
  cross join grand g
  order by case order_by
             when 'peak' then pk.peak_aas
             when 'p99' then pk.p99_aas
             else pk.total
           end desc, pk.total desc
  limit greatest(n, 0)
  loop
    query_text := null;
    if dimension = 'query_id' and v_has_pgss and v_key_num is not null then
      begin
        execute 'select left(query, 100) from pg_stat_statements where queryid = $1 limit 1'
          into query_text using v_key_num;
      exception when others then
        query_text := null;
      end;
    end if;
    return next;
  end loop;
end;
$$;

-- Before/after comparison of two windows (US-7). dimension => null gives one
-- overall row; a dimension gives the top rows by abs(avg_delta) via a full outer
-- join across the two windows (a key present in only one window still appears).
-- avg_delta is window-2 minus window-1.
-- Coverage honesty: a window with NO data coverage (e.g. entirely past
-- retention) reports NULL on its side — in both overall and per-dimension
-- modes — and avg_delta is NULL, never a fake zero-baseline "regression";
-- a NOTICE names the uncovered window. Within a covered window, a key absent
-- from one side is a true measured zero and the delta stands.
create or replace function ash.compare(
  since_1 timestamptz,
  until_1 timestamptz,
  since_2 timestamptz,
  until_2 timestamptz,
  dimension text default null,
  n int default 10,
  wait_event_type text default null,
  wait_event text default null,
  query_id bigint default null,
  database name default null,
  bucket interval default '1 minute'
)
returns table (
  key text,
  query_text text,
  avg_aas_1 numeric,
  avg_aas_2 numeric,
  avg_delta numeric,
  peak_aas_1 numeric,
  peak_aas_2 numeric,
  p99_aas_1 numeric,
  p99_aas_2 numeric,
  pct_1 numeric,
  pct_2 numeric
)
language plpgsql
stable
set jit = off
set search_path = pg_catalog, ash, public
as $$
declare
  a1 record;
  a2 record;
  v_cov1 boolean;
  v_cov2 boolean;
begin
  -- validate here, in compare's own frame, so the error names ash.compare
  -- and never leaks the internal delegation to ash.top.
  if dimension is not null
     and dimension not in ('wait_event_type', 'wait_event', 'query_id', 'database') then
    raise exception 'ash.compare: unknown dimension %; use wait_event_type|wait_event|query_id|database (or null for one overall row)', dimension;
  end if;

  -- Per-window coverage probe (rollup-backed, cheap). buckets_with_data = 0
  -- means the window holds no data at all — its side must read NULL, and the
  -- caller is warned: comparing against an uncovered window says nothing
  -- about a regression.
  select * into a1 from ash.aas(since_1, until_1,
    wait_event_type, wait_event, query_id, database, bucket);
  select * into a2 from ash.aas(since_2, until_2,
    wait_event_type, wait_event, query_id, database, bucket);
  v_cov1 := a1.buckets_with_data > 0;
  v_cov2 := a2.buckets_with_data > 0;
  if not v_cov1 then
    raise notice 'ash.compare: window 1 (% to %) has no data coverage — its columns and avg_delta are NULL, not zero. Check retention with ash.status() before reading a delta as a change.',
      a1.period_start, a1.period_end;
  end if;
  if not v_cov2 then
    raise notice 'ash.compare: window 2 (% to %) has no data coverage — its columns and avg_delta are NULL, not zero. Check retention with ash.status() before reading a delta as a change.',
      a2.period_start, a2.period_end;
  end if;

  if dimension is null then
    return query
    select
      'overall'::text, null::text,
      case when v_cov1 then a1.avg_aas end,
      case when v_cov2 then a2.avg_aas end,
      case when v_cov1 and v_cov2
           then round(a2.avg_aas - a1.avg_aas, 2) end,
      case when v_cov1 then a1.peak_aas end,
      case when v_cov2 then a2.peak_aas end,
      case when v_cov1 then a1.p99_aas end,
      case when v_cov2 then a2.p99_aas end,
      null::numeric, null::numeric;
    return;
  end if;

  return query
  with w1 as (
    select * from ash.top(dimension, since_1, until_1,
      wait_event_type, wait_event, query_id, database, 2147483647, bucket)
  ),
  w2 as (
    select * from ash.top(dimension, since_2, until_2,
      wait_event_type, wait_event, query_id, database, 2147483647, bucket)
  )
  select
    coalesce(w1.key, w2.key),
    coalesce(w1.query_text, w2.query_text),
    w1.avg_aas, w2.avg_aas,
    -- a key absent from a COVERED window is a true zero; an UNCOVERED window
    -- contributes NULL (no fake regression against an empty baseline).
    case when v_cov1 and v_cov2
         then round(coalesce(w2.avg_aas, 0) - coalesce(w1.avg_aas, 0), 2) end,
    w1.peak_aas, w2.peak_aas,
    w1.p99_aas, w2.p99_aas,
    w1.pct, w2.pct
  from w1
  -- NULL-safe: the unattributed-query bucket keeps a NULL key in both windows
  -- and must pair up like any other key. (FULL JOIN cannot use IS NOT DISTINCT
  -- FROM, so NULL is folded to an out-of-band sentinel for the join only.)
  full outer join w2 on coalesce(w1.key, chr(1)) = coalesce(w2.key, chr(1))
  order by abs(coalesce(w2.avg_aas, 0) - coalesce(w1.avg_aas, 0)) desc
  limit greatest(n, 0);
end;
$$;

-- Query text for a query_id from pg_stat_statements, via dynamic SQL so the
-- reference is never parsed when pgss is absent (a static reference would make
-- the caller fail to plan). Trusts pgss only when the real extension schema is
-- resolvable (#87 anti-spoof). Returns null when unavailable.
create or replace function ash._pgss_query_text(query_id bigint, maxlen int default 80)
returns text
language plpgsql
stable
set search_path = pg_catalog, ash, public
as $$
declare
  v_text text;
begin
  if query_id is null or ash._pgss_schema() is null then
    return null;
  end if;
  begin
    execute 'select left(query, $1) from pg_stat_statements where queryid = $2 limit 1'
      into v_text using maxlen, query_id;
  exception when others then
    v_text := null;
  end;
  return v_text;
end;
$$;

-- Decoded raw sample rows, newest first (2.0 conventions + uniform filters).
-- Raw evidence; reads ash.sample directly.
--
-- Split into a thin public SQL wrapper (ash.samples, below) and this plpgsql
-- workhorse: PL/pgSQL forbids an IN parameter sharing a name with a RETURNS
-- TABLE column, and samples() both filters BY and returns wait_event /
-- query_id. SQL-language functions have no such restriction, so the wrapper
-- carries the uniform public filter names (wait_event, query_id) and this
-- internal body uses _filter-suffixed names for the two that collide.
create or replace function ash._samples(
  since timestamptz default null,
  until timestamptz default null,
  n int default 100,
  wait_event_type text default null,
  wait_event_filter text default null,
  query_id_filter bigint default null,
  database name default null
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
set search_path = pg_catalog, ash, public
as $$
declare
  v_from timestamptz := coalesce(since, now() - interval '1 hour');
  v_to timestamptz := coalesce(until, now());
  v_start int4;
  v_end int4;
  v_slots smallint[];
  v_datid oid;
  v_has_pgss boolean := false;
begin
  v_start := ash.ts_from_timestamptz(v_from);
  v_end := ash.ts_from_timestamptz(v_to);
  v_slots := ash._active_slots_for_at(v_from, v_to);
  if database is not null then
    select d.oid into v_datid from pg_database d where d.datname = database;
    if v_datid is null then return; end if;
  end if;
  if ash._pgss_schema() is not null then
    begin
      perform 1 from pg_stat_statements limit 1;
      v_has_pgss := true;
    exception when others then
      v_has_pgss := false;
    end;
  end if;

  return query
  with decoded as (
    select
      s.sample_ts, s.slot, s.datid, s.active_count,
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
      and (v_datid is null or s.datid = v_datid)
  ),
  resolved as (
    select
      d.sample_ts, d.datid, d.active_count,
      case when wm.event = wm.type then wm.event
           else wm.type || ':' || wm.event end as wait_event,
      wm.type as wet,
      qm.query_id as qid
    from decoded d
    join ash.wait_event_map wm on wm.id = d.wait_id
    left join ash.query_map_all qm
      on qm.slot = d.slot and qm.id = d.map_id and d.map_id <> 0
    where (wait_event_type is null or wm.type = wait_event_type)
      and (wait_event_filter is null
           or (case when wm.event = wm.type then wm.event
                    else wm.type || ':' || wm.event end) = wait_event_filter
           or wm.event = wait_event_filter)
      and (query_id_filter is null or qm.query_id = query_id_filter)
  )
  select
    ash.epoch() + make_interval(secs => r.sample_ts),
    coalesce(db.datname, '<oid:' || r.datid || '>')::text,
    r.active_count,
    r.wait_event,
    r.qid,
    case when v_has_pgss then ash._pgss_query_text(r.qid, 80) else null end
  from resolved r
  left join pg_database db on db.oid = r.datid
  order by r.sample_ts desc, r.wait_event
  limit greatest(n, 0);
end;
$$;

-- Public wrapper carrying the uniform filter names (wait_event, query_id) —
-- see ash._samples above for why the plpgsql body cannot. SQL-language
-- functions allow an IN parameter to share a name with a RETURNS TABLE
-- column; positional $n references keep the body immune to that overlap.
create or replace function ash.samples(
  since timestamptz default null,
  until timestamptz default null,
  n int default 100,
  wait_event_type text default null,
  wait_event text default null,
  query_id bigint default null,
  database name default null
)
returns table (
  sample_time timestamptz,
  database_name text,
  active_backends smallint,
  wait_event text,
  query_id bigint,
  query_text text
)
language sql
stable
set jit = off
set search_path = pg_catalog, ash, public
as $$
  select * from ash._samples($1, $2, $3, $4, $5, $6, $7)
$$;

-- report helper: top wait events at a set of minutes for one wait class,
-- from rollup_1m, as pre-formatted "event(aas)" strings (aas = avg per-minute
-- AAS across the given minutes, 1 decimal). Empty array when nothing matched.
create or replace function ash._hr_top_events(
  type text, minutes int4[], n int, si numeric
)
returns text[]
language sql
stable
set search_path = pg_catalog, ash
as $$
  with pme as (
    select wm.event as ev, sum(r.wait_counts[o + 1])::numeric as cnt
    from ash.rollup_1m r
    cross join generate_subscripts(r.wait_counts, 1) o
    join ash.wait_event_map wm on wm.id = r.wait_counts[o]
    -- function-qualified: in a SQL-language body a bare `type` would resolve
    -- to the wm.type column (columns take precedence), not the parameter
    where o % 2 = 1 and r.ts = any(minutes) and wm.type = _hr_top_events.type
    group by wm.event
    order by 2 desc
    limit greatest(n, 0)
  )
  select coalesce(
    array_agg(ev || '(' ||
      to_char(round(cnt * si
                    / (greatest(array_length(minutes, 1), 1) * 60.0), 1),
              'FM990.0') || ')'
      order by cnt desc),
    array[]::text[])
  from pme
$$;

-- report helper: top query ids at a set of minutes, optionally within one
-- wait class (type null = across all classes = the 'total' key), read from RAW
-- samples (the wait<->query tie). "queryid(aas)" strings, int64-safe.
create or replace function ash._hr_top_queryids(
  type text, minutes int4[], n int, si numeric
)
returns text[]
language sql
stable
set jit = off
set search_path = pg_catalog, ash
as $$
  with expanded as (
    select s.slot, (-s.data[i])::int as wait_id, s.data[i + 2 + gs.n] as map_id
    from ash.sample s,
      generate_subscripts(s.data, 1) i,
      lateral generate_series(0, greatest(s.data[i + 1] - 1, -1)) gs(n)
    where s.slot = any(ash._active_slots_for_at(
                     ash.ts_to_timestamptz((select min(m) from unnest(minutes) m)),
                     ash.ts_to_timestamptz((select max(m) + 60 from unnest(minutes) m))))
      -- Sargable range bound so the (sample_ts) index prunes to the extreme
      -- minutes instead of seq-scanning the whole active partition (#perf: this
      -- runs up to 15x per report()); the exact = any(...) stays as residual.
      and s.sample_ts >= (select min(m) from unnest(minutes) m)
      and s.sample_ts < (select max(m) + 60 from unnest(minutes) m)
      and (s.sample_ts / 60) * 60 = any(minutes)
      and s.data[i] < 0
      and i + 1 <= array_length(s.data, 1)
      and i + 2 + gs.n <= array_length(s.data, 1)
  ),
  hits as (
    select qm.query_id as qid, count(*)::numeric as cnt
    from expanded e
    join ash.wait_event_map wm on wm.id = e.wait_id
    join ash.query_map_all qm on qm.slot = e.slot and qm.id = e.map_id and e.map_id <> 0
    -- function-qualified: in a SQL-language body a bare `type` would resolve
    -- to the wm.type column (columns take precedence), not the parameter
    where (_hr_top_queryids.type is null or wm.type = _hr_top_queryids.type)
    group by qm.query_id
    order by 2 desc
    limit greatest(n, 0)
  )
  select coalesce(
    array_agg(qid::text || '(' ||
      to_char(round(cnt * si
                    / (greatest(array_length(minutes, 1), 1) * 60.0), 1),
              'FM990.0') || ')'
      order by cnt desc),
    array[]::text[])
  from hits
$$;

-- Machine-readable load report (US-8): one self-contained jsonb load report for
-- the window. Per-class per-minute AAS (zero-filled) drives avg/worst1m/p99/p999;
-- top_events_* come from rollup_1m, top_queryids_* from raw samples, attributed
-- per extreme minute (a key is present iff raw samples still cover that
-- worst/percentile minute set — even when the window start predates raw
-- retention); top_queryids_available + coverage metadata make the attribution
-- state explicit. Returns null when no coverage.
create or replace function ash.report(
  since timestamptz default null,
  until timestamptz default null,
  vcpus int default null,
  n int default 3
)
returns jsonb
language plpgsql
stable
set jit = off
set search_path = pg_catalog, ash
as $$
declare
  v_from timestamptz := coalesce(since, now() - interval '1 day');
  v_to timestamptz := coalesce(until, now());
  v_start_ts int4;
  v_end_ts int4;
  v_si numeric;
  v_n int;
  v_cluster text;
  v_raw_min int4;
  v_result jsonb;
  v_class record;
  v_agg jsonb := '{}'::jsonb;
  v_avg jsonb := '{}'::jsonb;
  v_worst jsonb := '{}'::jsonb;
  v_p99 jsonb := '{}'::jsonb;
  v_p999 jsonb := '{}'::jsonb;
  v_te_w jsonb := '{}'::jsonb;
  v_te_9 jsonb := '{}'::jsonb;
  v_te_99 jsonb := '{}'::jsonb;
  v_tq_w jsonb := '{}'::jsonb;
  v_tq_9 jsonb := '{}'::jsonb;
  v_tq_99 jsonb := '{}'::jsonb;
  v_tot_avg numeric := 0;
  v_tot_worst numeric := 0;
  v_tot_p99 numeric := 0;
  v_tot_p999 numeric := 0;
  -- total-series own extreme minutes (drive total worst1m/p99/p999 and the
  -- top_queryids_*.total windows); thresholds kept unrounded for minute-set
  -- membership.
  v_tworst_min int4;
  v_t99_mins int4[];
  v_t999_mins int4[];
  v_t99_thr numeric;
  v_t999_thr numeric;
begin
  v_start_ts := (ash.ts_from_timestamptz(v_from) / 60) * 60;
  v_end_ts := (ash.ts_from_timestamptz(v_to) / 60) * 60;
  -- overflow-safe empty/degenerate-window guard (#63).
  if v_end_ts <= v_start_ts then
    v_end_ts := least(v_start_ts::bigint + 60, 2147483647)::int4;
  end if;
  v_si := ash._sample_interval_secs();

  -- Covered minutes (any rollup_1m row); null result when no coverage at all.
  select count(*) into v_n
  from (select distinct ts from ash.rollup_1m
        where ts >= v_start_ts and ts < v_end_ts) t;
  if v_n = 0 then
    return null;
  end if;

  -- top_queryids_* attribution needs the raw wait<->query tie, decided PER
  -- EXTREME MINUTE (not per window start): the default 1-day window typically
  -- starts right at the raw-retention boundary, but the worst/percentile
  -- minutes themselves are usually well inside raw retention — dropping the
  -- attribution wholesale threw away exactly the answer the report exists to
  -- give. v_raw_min is the first minute raw samples can attribute; each
  -- extreme-minute set below is filtered to covered minutes and attributed
  -- when any survive.
  -- (greatest/least in ts_from_timestamptz ignore NULLs, so a missing raw
  -- retention start must be short-circuited here, not passed through.)
  v_raw_min := case when ash._raw_retention_start() is null then null
                    else (ash.ts_from_timestamptz(ash._raw_retention_start()) / 60) * 60
               end;

  -- Per-class metrics over the zero-filled per-minute AAS series.
  for v_class in
    select * from (values
      ('cpu','CPU*'),('io','IO'),('ipc','IPC'),('lock','Lock'),('lwlock','LWLock')
    ) c(k, t)
  loop
    declare
      v_cavg numeric; v_cworst numeric; v_cp99 numeric; v_cp999 numeric;
      v_worst_min int4;
      v_p99_mins int4[];
      v_p999_mins int4[];
    begin
      with cov as (
        select distinct ts from ash.rollup_1m
        where ts >= v_start_ts and ts < v_end_ts
      ),
      pmc as (
        select r.ts, sum(r.wait_counts[o + 1])::numeric as cnt
        from ash.rollup_1m r
        cross join generate_subscripts(r.wait_counts, 1) o
        join ash.wait_event_map wm on wm.id = r.wait_counts[o]
        where o % 2 = 1 and r.ts >= v_start_ts and r.ts < v_end_ts
          and wm.type = v_class.t
        group by r.ts
      ),
      grid as (
        select cov.ts, coalesce(p.cnt, 0) * v_si / 60.0 as aas
        from cov left join pmc p on p.ts = cov.ts
      ),
      agg as (
        select
          avg(aas) as cavg, max(aas) as cworst,
          percentile_cont(0.99) within group (order by aas) as cp99,
          percentile_cont(0.999) within group (order by aas) as cp999
        from grid
      )
      select round(cavg, 2), round(cworst, 2), round(cp99::numeric, 2), round(cp999::numeric, 2)
      into v_cavg, v_cworst, v_cp99, v_cp999
      from agg;

      -- worst minute and percentile-minute sets for top_events / top_queryids
      select ts into v_worst_min from (
        select cov.ts, coalesce(p.cnt, 0) as cnt
        from (select distinct ts from ash.rollup_1m where ts >= v_start_ts and ts < v_end_ts) cov
        left join (
          select r.ts, sum(r.wait_counts[o + 1])::numeric as cnt
          from ash.rollup_1m r cross join generate_subscripts(r.wait_counts, 1) o
          join ash.wait_event_map wm on wm.id = r.wait_counts[o]
          where o % 2 = 1 and r.ts >= v_start_ts and r.ts < v_end_ts and wm.type = v_class.t
          group by r.ts
        ) p on p.ts = cov.ts
        order by cnt desc, cov.ts
        limit 1
      ) x;

      select coalesce(array_agg(ts) filter (where aas >= v_cp99 and aas > 0), array[]::int4[]),
             coalesce(array_agg(ts) filter (where aas >= v_cp999 and aas > 0), array[]::int4[])
      into v_p99_mins, v_p999_mins
      from (
        select cov.ts, coalesce(p.cnt, 0) * v_si / 60.0 as aas
        from (select distinct ts from ash.rollup_1m where ts >= v_start_ts and ts < v_end_ts) cov
        left join (
          select r.ts, sum(r.wait_counts[o + 1])::numeric as cnt
          from ash.rollup_1m r cross join generate_subscripts(r.wait_counts, 1) o
          join ash.wait_event_map wm on wm.id = r.wait_counts[o]
          where o % 2 = 1 and r.ts >= v_start_ts and r.ts < v_end_ts and wm.type = v_class.t
          group by r.ts
        ) p on p.ts = cov.ts
      ) g;

      v_avg := v_avg || jsonb_build_object(v_class.k, v_cavg);
      v_worst := v_worst || jsonb_build_object(v_class.k, v_cworst);
      v_p99 := v_p99 || jsonb_build_object(v_class.k, v_cp99);
      v_p999 := v_p999 || jsonb_build_object(v_class.k, v_cp999);
      -- avg of a sum == sum of the class avgs, so total avg is accumulated here;
      -- worst1m/p99/p999 total come from the summed series' OWN extreme (below),
      -- not the sum of each class's independent worst minute.
      v_tot_avg := v_tot_avg + coalesce(v_cavg, 0);

      -- top_events (rollup) and top_queryids (raw) for the four non-cpu classes;
      -- top_queryids 'total' is handled once, outside this loop.
      if v_class.k <> 'cpu' then
        v_te_w := v_te_w || jsonb_build_object(v_class.k,
          to_jsonb(ash._hr_top_events(v_class.t, array[v_worst_min], n, v_si)));
        v_te_9 := v_te_9 || jsonb_build_object(v_class.k,
          to_jsonb(ash._hr_top_events(v_class.t, v_p99_mins, n, v_si)));
        v_te_99 := v_te_99 || jsonb_build_object(v_class.k,
          to_jsonb(ash._hr_top_events(v_class.t, v_p999_mins, n, v_si)));
        -- attribution per extreme minute: a key appears when raw samples still
        -- cover its minute(s); percentile sets attribute over the raw-covered
        -- subset (a partially-covered set is better than none — the coverage
        -- metadata lets a consumer detect the reduced attribution window).
        if v_raw_min is not null and v_worst_min >= v_raw_min then
          v_tq_w := v_tq_w || jsonb_build_object(v_class.k,
            to_jsonb(ash._hr_top_queryids(v_class.t, array[v_worst_min], n, v_si)));
        end if;
        if v_raw_min is not null then
          v_p99_mins := array(select m from unnest(v_p99_mins) m where m >= v_raw_min);
          v_p999_mins := array(select m from unnest(v_p999_mins) m where m >= v_raw_min);
          if cardinality(v_p99_mins) > 0 then
            v_tq_9 := v_tq_9 || jsonb_build_object(v_class.k,
              to_jsonb(ash._hr_top_queryids(v_class.t, v_p99_mins, n, v_si)));
          end if;
          if cardinality(v_p999_mins) > 0 then
            v_tq_99 := v_tq_99 || jsonb_build_object(v_class.k,
              to_jsonb(ash._hr_top_queryids(v_class.t, v_p999_mins, n, v_si)));
          end if;
        end if;
      end if;
    end;
  end loop;

  -- Total = the summed per-minute series' OWN extreme (matches the platform
  -- ingestion recipe and top_queryids_*.total). Statement 1: values + unrounded
  -- thresholds + worst minute.
  with grid as (
    select cov.ts, coalesce(t.cnt, 0) * v_si / 60.0 as aas
    from (select distinct ts from ash.rollup_1m where ts >= v_start_ts and ts < v_end_ts) cov
    left join (
      select r.ts, sum(r.wait_counts[o + 1])::numeric as cnt
      from ash.rollup_1m r cross join generate_subscripts(r.wait_counts, 1) o
      join ash.wait_event_map wm on wm.id = r.wait_counts[o]
      where o % 2 = 1 and r.ts >= v_start_ts and r.ts < v_end_ts
        and wm.type in ('CPU*','IO','IPC','Lock','LWLock')
      group by r.ts
    ) t on t.ts = cov.ts
  )
  select
    round(coalesce(max(aas), 0), 2),
    round(coalesce(percentile_cont(0.99) within group (order by aas), 0)::numeric, 2),
    round(coalesce(percentile_cont(0.999) within group (order by aas), 0)::numeric, 2),
    percentile_cont(0.99) within group (order by aas),
    percentile_cont(0.999) within group (order by aas),
    (select ts from grid order by aas desc, ts limit 1)
  into v_tot_worst, v_tot_p99, v_tot_p999, v_t99_thr, v_t999_thr, v_tworst_min
  from grid;

  -- Statement 2: the p99/p999 minute sets (>= the unrounded thresholds).
  select
    coalesce(array_agg(ts) filter (where aas >= v_t99_thr and aas > 0), array[]::int4[]),
    coalesce(array_agg(ts) filter (where aas >= v_t999_thr and aas > 0), array[]::int4[])
  into v_t99_mins, v_t999_mins
  from (
    select cov.ts, coalesce(t.cnt, 0) * v_si / 60.0 as aas
    from (select distinct ts from ash.rollup_1m where ts >= v_start_ts and ts < v_end_ts) cov
    left join (
      select r.ts, sum(r.wait_counts[o + 1])::numeric as cnt
      from ash.rollup_1m r cross join generate_subscripts(r.wait_counts, 1) o
      join ash.wait_event_map wm on wm.id = r.wait_counts[o]
      where o % 2 = 1 and r.ts >= v_start_ts and r.ts < v_end_ts
        and wm.type in ('CPU*','IO','IPC','Lock','LWLock')
      group by r.ts
    ) t on t.ts = cov.ts
  ) g;

  v_avg := jsonb_build_object('total', round(v_tot_avg, 2)) || v_avg;
  v_worst := jsonb_build_object('total', v_tot_worst) || v_worst;
  v_p99 := jsonb_build_object('total', v_tot_p99) || v_p99;
  v_p999 := jsonb_build_object('total', v_tot_p999) || v_p999;

  v_result := jsonb_build_object(
    'aas_avg', v_avg,
    'aas_worst1m', v_worst,
    'aas_p99', v_p99,
    'aas_p999', v_p999,
    'top_events_worst1m', v_te_w,
    'top_events_p99', v_te_9,
    'top_events_p999', v_te_99
  );

  -- 'total' top_queryids for each window = top queries at the window's overall
  -- worst/percentile minutes (the summed-series extremes computed above), so
  -- these agree with aas_worst1m/p99/p999.total. Same per-extreme-minute
  -- attribution rule as the per-class keys above.
  if v_raw_min is not null then
    if v_tworst_min >= v_raw_min then
      v_tq_w := v_tq_w || jsonb_build_object('total',
        to_jsonb(ash._hr_top_queryids(null, array[v_tworst_min], n, v_si)));
    end if;
    v_t99_mins := array(select m from unnest(v_t99_mins) m where m >= v_raw_min);
    v_t999_mins := array(select m from unnest(v_t999_mins) m where m >= v_raw_min);
    if cardinality(v_t99_mins) > 0 then
      v_tq_9 := v_tq_9 || jsonb_build_object('total',
        to_jsonb(ash._hr_top_queryids(null, v_t99_mins, n, v_si)));
    end if;
    if cardinality(v_t999_mins) > 0 then
      v_tq_99 := v_tq_99 || jsonb_build_object('total',
        to_jsonb(ash._hr_top_queryids(null, v_t999_mins, n, v_si)));
    end if;
  end if;

  -- Each top_queryids_* object appears when it attributed at least one key
  -- (consumers MUST treat these keys as optional per the frozen contract);
  -- top_queryids_available (additive, always present) is the explicit signal
  -- so ingest can branch on a field rather than on key absence.
  if v_tq_w <> '{}'::jsonb then
    v_result := v_result || jsonb_build_object('top_queryids_worst1m', v_tq_w);
  end if;
  if v_tq_9 <> '{}'::jsonb then
    v_result := v_result || jsonb_build_object('top_queryids_p99', v_tq_9);
  end if;
  if v_tq_99 <> '{}'::jsonb then
    v_result := v_result || jsonb_build_object('top_queryids_p999', v_tq_99);
  end if;

  -- Additive metadata (frozen contract: keys only ever added): the window and
  -- coverage actually used, so a consumer can reconcile this payload against
  -- ash.aas()/ash.top() for the same window and detect degraded resolution
  -- (minutes_with_data < minutes_expected) or a reduced attribution window
  -- (raw_retention_start inside the window).
  v_result := v_result || jsonb_build_object(
    'top_queryids_available',
      (v_tq_w <> '{}'::jsonb or v_tq_9 <> '{}'::jsonb or v_tq_99 <> '{}'::jsonb),
    'coverage', jsonb_build_object(
      'from', ash.ts_to_timestamptz(v_start_ts),
      'to', ash.ts_to_timestamptz(v_end_ts),
      'source', 'rollup_1m',
      'minutes_expected', (v_end_ts - v_start_ts) / 60,
      'minutes_with_data', v_n,
      'raw_retention_start', ash._raw_retention_start()
    )
  );

  -- optional / conditional top-level keys
  if vcpus is not null then
    v_result := jsonb_build_object('vcpus', vcpus) || v_result;
  end if;
  v_cluster := current_setting('cluster_name', true);
  if v_cluster is not null and length(v_cluster) > 0 then
    v_result := jsonb_build_object('cluster_name', v_cluster) || v_result;
  end if;

  return v_result;
end;
$$;

-- Human render helper: stacked per-bucket AAS chart (2.0 port of timeline_chart).
-- Presentation-only; reads the rollup-backed AAS via _grain_by. bucket => null
-- auto-selects grain by span like ash.timeline.
create or replace function ash.chart(
  since timestamptz default null,
  until timestamptz default null,
  bucket interval default null,
  n int default 3,
  width int default 40,
  color boolean default false
)
returns table (
  bucket_start timestamptz,
  aas numeric,
  detail text,
  chart text
)
language plpgsql
stable
set jit = off
set search_path = pg_catalog, ash
as $$
declare
  v_from timestamptz := coalesce(since, now() - interval '1 hour');
  v_to timestamptz := coalesce(until, now());
  v_start_ts int4;
  v_end_ts int4;
  v_span int4;
  v_bucket_secs int4;
  v_grain_secs int4;
  v_si numeric;
  v_source text;
  v_reset text := ash._reset(color);
  v_top_events text[];
  v_event_colors text[];
  v_event_chars text[] := array['█', '▓', '░', '▒'];
  v_other_color text := ash._wait_color('Other', color);
  v_other_char text := '·';
  v_max numeric;
  v_legend text;
  v_legend_len int;
  v_rec record;
  v_bar text;
  v_val numeric;
  v_ch text;
  v_i int;
  v_char_count int;
begin
  width := least(greatest(width, 1), 500);
  v_start_ts := (ash.ts_from_timestamptz(v_from) / 60) * 60;
  v_end_ts := (ash.ts_from_timestamptz(v_to) / 60) * 60;
  -- overflow-safe empty/degenerate-window guard (#63).
  if v_end_ts <= v_start_ts then
    v_end_ts := least(v_start_ts::bigint + 60, 2147483647)::int4;
  end if;
  v_span := v_end_ts - v_start_ts;
  if bucket is null then
    v_bucket_secs := case when v_span <= 6 * 3600 then 60
                          when v_span <= 7 * 86400 then 3600 else 86400 end;
  else
    v_bucket_secs := extract(epoch from bucket)::int4;
    if v_bucket_secs is null or v_bucket_secs < 60 then
      raise exception 'bucket must be at least 1 minute, got %', bucket;
    end if;
  end if;
  v_si := ash._sample_interval_secs();
  v_source := ash._pick_source_agg(ash.ts_to_timestamptz(v_start_ts),
                                   ash.ts_to_timestamptz(v_end_ts));
  if v_source = 'none' then v_source := 'rollup_1m'; end if;
  if v_source = 'rollup_1h' and v_bucket_secs < 3600 then
    v_source := 'rollup_1m';
  end if;
  v_grain_secs := case when v_source = 'rollup_1h' then 3600 else 60 end;

  -- Legend/series events: the window-wide top-n PLUS any event that is
  -- top-1 in at least one bucket. A spike event dominant in a single bucket —
  -- the very bucket under investigation — would otherwise never make the
  -- window-wide top-3 and disappear into Other, hiding the culprit.
  with g as (
    select (b.ts / v_bucket_secs) * v_bucket_secs as bstart,
           b.key as ev, sum(b.cnt) as cnt
    from ash._grain_by(v_start_ts, v_end_ts, v_source, 'wait_event') b
    group by 1, b.key
  ),
  totals as (
    select ev, sum(cnt) as tot from g group by ev
  ),
  wtop as (
    select ev from totals order by tot desc limit greatest(n, 0)
  ),
  btop as (
    select distinct on (bstart) ev from g order by bstart, cnt desc, ev
  )
  select array_agg(t.ev order by t.tot desc)
  into v_top_events
  from totals t
  where t.ev in (select ev from wtop union select ev from btop);

  if v_top_events is null then
    return;
  end if;

  -- Calendar-aligned buckets (floored to bucket relative to ash.epoch(),
  -- midnight UTC), matching timeline(); edge buckets divide by their
  -- in-window coverage.
  select max(tot) into v_max from (
    select (b.ts / v_bucket_secs) * v_bucket_secs as bstart,
           sum(b.cnt) * v_si
           / (least((b.ts / v_bucket_secs) * v_bucket_secs + v_bucket_secs, v_end_ts)
              - greatest((b.ts / v_bucket_secs) * v_bucket_secs, v_start_ts)) as tot
    from ash._grain_by(v_start_ts, v_end_ts, v_source, 'wait_event') b
    group by 1
  ) t;
  if v_max is null or v_max = 0 then
    return;
  end if;

  v_event_colors := array[]::text[];
  for v_i in 1 .. array_length(v_top_events, 1) loop
    v_event_colors := v_event_colors || ash._wait_color(v_top_events[v_i], color);
  end loop;

  v_legend := '';
  for v_i in 1 .. array_length(v_top_events, 1) loop
    v_ch := coalesce(v_event_chars[v_i], v_event_chars[array_length(v_event_chars, 1)]);
    if v_i > 1 then v_legend := v_legend || '  '; end if;
    v_legend := v_legend || v_event_colors[v_i] || v_ch || v_reset || ' ' || v_top_events[v_i];
  end loop;
  v_legend := v_legend || '  ' || v_other_color || v_other_char || v_reset || ' Other';
  v_legend_len := length(v_legend);
  bucket_start := null; aas := null; detail := null; chart := v_legend;
  return next;

  for v_rec in
    with buckets as (
      select gs.ts::int4 as bstart
      from generate_series(((v_start_ts / v_bucket_secs) * v_bucket_secs)::bigint,
                           (v_end_ts - 1)::bigint, v_bucket_secs) gs(ts)
    ),
    per_bucket as (
      select b.bstart,
             round(coalesce(sum(t.cnt), 0) * v_si
                   / (least(b.bstart + v_bucket_secs, v_end_ts)
                      - greatest(b.bstart, v_start_ts)), 2) as total,
             coalesce(jsonb_object_agg(t.ev,
               round(t.cnt * v_si
                     / (least(b.bstart + v_bucket_secs, v_end_ts)
                        - greatest(b.bstart, v_start_ts)), 2))
               filter (where t.ev is not null), '{}'::jsonb) as events
      from buckets b
      left join (
        select (g.ts / v_bucket_secs) * v_bucket_secs as bstart,
               g.key as ev, sum(g.cnt) as cnt
        from ash._grain_by(v_start_ts, v_end_ts, v_source, 'wait_event') g
        group by 1, g.key
      ) t on t.bstart = b.bstart
      group by b.bstart
    )
    select ash.ts_to_timestamptz(bstart) as ts, total, events
    from per_bucket order by bstart
  loop
    v_bar := '';
    v_legend := '';
    for v_i in 1 .. array_length(v_top_events, 1) loop
      v_val := coalesce((v_rec.events ->> v_top_events[v_i])::numeric, 0);
      v_ch := coalesce(v_event_chars[v_i], v_event_chars[array_length(v_event_chars, 1)]);
      if v_val > 0 then
        v_char_count := greatest(0, round(v_val / v_max * width)::int);
        if v_char_count > 0 then
          v_bar := v_bar || v_event_colors[v_i] || repeat(v_ch, v_char_count) || v_reset;
        end if;
        v_legend := v_legend || ' ' || v_top_events[v_i] || '=' || v_val;
      end if;
    end loop;
    v_val := greatest(v_rec.total - (
      select coalesce(sum(coalesce((v_rec.events ->> e)::numeric, 0)), 0)
      from unnest(v_top_events) e), 0);
    if v_val > 0 then
      v_char_count := greatest(0, round(v_val / v_max * width)::int);
      if v_char_count > 0 then
        v_bar := v_bar || v_other_color || repeat(v_other_char, v_char_count) || v_reset;
      end if;
      v_legend := v_legend || ' Other=' || v_val;
    end if;
    if length(v_bar) < v_legend_len then
      v_bar := v_bar || repeat(' ', v_legend_len - length(v_bar));
    end if;
    bucket_start := v_rec.ts; aas := v_rec.total; detail := ltrim(v_legend); chart := v_bar;
    return next;
  end loop;
end;
$$;

-- Human render helper: key/value AAS overview (2.0 port of activity_summary),
-- the companion to ash.periods for one window.
create or replace function ash.summary(
  since timestamptz default null,
  until timestamptz default null
)
returns table (
  metric text,
  value text
)
language plpgsql
stable
set jit = off
set search_path = pg_catalog, ash, public
as $$
declare
  v_from timestamptz := coalesce(since, now() - interval '1 hour');
  v_to timestamptz := coalesce(until, now());
  v_a record;
  r record;
  v_rank int;
begin
  select * into v_a from ash.aas(v_from, v_to) a;
  if v_a.buckets_with_data = 0 then
    return query select 'status'::text, 'no data in this time range'::text;
    return;
  end if;

  return query select 'period_start'::text, v_a.period_start::text;
  return query select 'period_end'::text, v_a.period_end::text;
  return query select 'source'::text, v_a.source;
  return query select 'minutes_with_data'::text, v_a.buckets_with_data::text;
  return query select 'avg_aas'::text, v_a.avg_aas::text;
  return query select 'peak_aas'::text, v_a.peak_aas::text;
  return query select 'p99_aas'::text, v_a.p99_aas::text;
  return query select 'backend_seconds'::text, v_a.backend_seconds::text;

  return query
  select 'databases_active'::text,
    count(*)::text from ash.top('database', v_from, v_to, n => 2147483647);

  v_rank := 0;
  for r in
    select b.key, b.avg_aas, b.pct from ash.top('wait_event', v_from, v_to, n => 3) b
  loop
    v_rank := v_rank + 1;
    return query select 'top_wait_' || v_rank,
      r.key || ' (avg_aas ' || r.avg_aas || ', ' || r.pct || '%)';
  end loop;

  v_rank := 0;
  for r in
    select b.key, b.query_text, b.avg_aas, b.pct
    from ash.top('query_id', v_from, v_to, n => 3) b
  loop
    v_rank := v_rank + 1;
    -- a NULL key is the unattributed bucket (no query_id captured)
    return query select 'top_query_' || v_rank,
      coalesce(r.key, '(unattributed)') || coalesce(' — ' || left(r.query_text, 60), '')
      || ' (avg_aas ' || r.avg_aas || ', ' || r.pct || '%)';
  end loop;
end;
$$;

-- Catalog comments (\df+ / obj_description): every reader states its unit (AAS =
-- Average Active Sessions; avg_aas is backend-time per wall-clock second,
-- peak/p99 the max/99th-percentile of per-bucket AAS), its column contract, and
-- the recommended next call, so a human or AI agent can navigate the catalog
-- alone. On-CPU/uninstrumented work is spelled 'CPU*' everywhere user-facing.
comment on function ash.periods(timestamptz) is
$$START HERE (US-1 triage): AAS for six standard trailing windows (1m, 5m, 1h, 1d, 1w, 1mo) ending at until (default now()), one row each. Columns (period, period_start, period_end, source, bucket, buckets_with_data, avg_aas, peak_aas, p99_aas): peak/p99 vs avg distinguishes a spike from sustained load; buckets_with_data counts covered buckets at the grain named by bucket (always 1 minute here). Rollup-backed (source = rollup_1m|rollup_1h). Next: locate the spike in time with ash.timeline(), then drill with ash.top().$$;

comment on function ash.aas(timestamptz, timestamptz, text, text, bigint, name, interval) is
$$Scalar AAS summary for one window [since, until) (defaults: last 1 hour). Optional uniform filters wait_event_type/wait_event/query_id/database. Columns (period_start, period_end, source, buckets_expected, buckets_with_data, avg_aas, peak_aas, p99_aas, backend_seconds); peak/p99 are over per-bucket AAS. Buckets are calendar-aligned (floored to bucket in UTC: an hour bucket starts on the hour, a day bucket on the UTC day) — the same absolute window always produces the same buckets. Also the US-4 leaf event summary: ash.aas(wait_event => 'IO:DataFileRead'). Combining a wait filter with query_id needs raw samples and raises past raw retention. source = raw|rollup_1m|rollup_1h. Next: ash.top('query_id', wait_event => ...).$$;

comment on function ash.timeline(timestamptz, timestamptz, interval, text, text, bigint, name) is
$$AAS time series (US-2 locate / US-6 capacity): one row per bucket across [since, until). bucket => null auto-selects grain by span (<= 6h: 1 minute, <= 7d: 1 hour, else 1 day). bucket_start is calendar-aligned (floored to bucket in UTC: hour buckets start on the hour, day buckets on the UTC day), so the same absolute window always returns identical bucket_start labels; the first bucket may start before since and edge buckets average over their in-window part only. Columns (bucket_start, source, data_points, avg_aas, peak_aas, p99_aas): data_points = 0 with null AAS marks a no-data bucket (distinct from measured-zero). Order by peak_aas desc nulls last to find the worst buckets (no-data buckets carry null peak_aas, which sorts first under a bare desc and would bury the real spike), then drill that window with ash.top(). peak_aas/p99_aas are per-minute even on rollup_1h-backed windows (per-minute totals are preserved in rollup_1h.minute_counts); p99_aas is null only for wait/query-filtered rollup_1h-backed buckets (hour grain).$$;

comment on function ash.top(text, timestamptz, timestamptz, text, text, bigint, name, int, interval, text) is
$$The single vertical drill (US-3): AAS broken down by dimension in wait_event_type|wait_event|query_id|database over [since, until). Every row carries avg_aas, peak_aas, p99_aas, backend_seconds, and pct (share of window total). order_by in avg|peak|p99 (default avg) picks the ranking metric BEFORE the top-n cut — use order_by => 'peak' during incident triage so a query that spiked for one minute outranks steady background rows. For the query_id dimension, a NULL key is the unattributed bucket (activity whose query_id was not captured: not run with a queryid-reporting client, truncated, or utility/idle-in-transaction work) — it is real load, not an error. Filters compose: ash.top('wait_event', wait_event_type => 'IO') is the level-2 drill; ash.top('query_id', wait_event => 'IO:DataFileRead') is the US-4 leaf. query_text is filled for the query_id dimension when pg_stat_statements is present AND the caller can read other roles pg_stat_statements text — i.e. holds pg_read_all_stats (e.g. via membership in pg_monitor); a plain ash.grant_reader() role without it sees query_text NULL even though pgss is installed, because pgss restricts query text to the owning role. Crossing the wait<->query tie (query_id dimension + wait filter, or a wait dimension + query_id) reads raw samples and raises past raw retention. source = raw|rollup_1m|rollup_1h.$$;

comment on function ash.compare(timestamptz, timestamptz, timestamptz, timestamptz, text, int, text, text, bigint, name, interval) is
$$Before/after comparison of two windows (US-7): window 1 = [since_1, until_1), window 2 = [since_2, until_2). dimension => null gives one overall row; a dimension gives the top n keys by abs(avg_delta) via a full outer join (a key present in only one window still appears). Columns (key, query_text, avg_aas_1, avg_aas_2, avg_delta, peak_aas_1, peak_aas_2, p99_aas_1, p99_aas_2, pct_1, pct_2); avg_delta = window 2 minus window 1. A window with no data coverage (e.g. past retention) reports NULL on its side and a NULL avg_delta (plus a NOTICE) — never a fake zero baseline; within a covered window an absent key is a true zero. Use to tell whether a deploy regressed load and where.$$;

comment on function ash.samples(timestamptz, timestamptz, int, text, text, bigint, name) is
$$Decoded raw sample rows, newest first (US-5 raw evidence) over [since, until) (default last 1 hour), up to n. Uniform filters wait_event_type/wait_event/query_id/database. Columns (sample_time, database_name, active_backends, wait_event, query_id, query_text). query_text needs pg_stat_statements AND that the caller holds pg_read_all_stats (e.g. via pg_monitor membership); it is null otherwise — a plain ash.grant_reader() role without pg_read_all_stats sees null even with pgss installed, because pgss restricts query text to the owning role. Reads ash.sample directly (raw retention only).$$;

comment on function ash.report(timestamptz, timestamptz, int, int) is
$$Machine-readable load report as one jsonb (US-8) for [since, until) (default last 1 day). Per wait class (cpu=CPU*, io=IO, ipc=IPC, lock=Lock, lwlock=LWLock; total = their sum) at 1-minute resolution: aas_avg / aas_worst1m / aas_p99 / aas_p999. Plus top_events_{worst1m,p99,p999} (keys io/ipc/lock/lwlock, entries "event(aas)") and top_queryids_{worst1m,p99,p999} (keys total+the four non-cpu classes, entries "queryid(aas)"). Query attribution is decided per extreme minute: a top_queryids key appears when raw samples still cover that class's worst/percentile minute(s), even if the window start predates raw retention (percentile sets attribute over their raw-covered minutes). top_queryids_available (boolean, always present) says whether any attribution was possible — branch on it, not on key absence. coverage {from, to, source, minutes_expected, minutes_with_data, raw_retention_start} reconciles the payload against ash.aas()/ash.top() and flags degraded resolution. vcpus (echoed, never used) and cluster_name are pass-throughs. Returns null when the window has no coverage; never raises. Payload contract is frozen per 2.0 minor line (keys only added, never renamed/removed); scoring/normalization is the consumer's job.$$;

comment on function ash.chart(timestamptz, timestamptz, interval, int, int, boolean) is
$$Human render helper: stacked ASCII per-bucket AAS chart over [since, until) (default last 1 hour). Series/legend = the window-wide top n wait events PLUS any event that is top-1 in at least one bucket (so a single-bucket spike culprit is always visible), plus Other. bucket => null auto-selects grain by span; buckets are calendar-aligned like ash.timeline(). Presentation-only (columns bucket_start, aas, detail, chart); for typed data use ash.timeline(). Enable ANSI color with color => true or "set ash.color = on".$$;

comment on function ash.summary(timestamptz, timestamptz) is
$$Human render helper: key/value AAS overview for one window [since, until) (default last 1 hour) — the companion to ash.periods(). Returns (metric, value): period bounds, source, minutes_with_data, avg/peak/p99 AAS, backend_seconds, databases_active, and top waits/queries. Presentation-only; for typed data use ash.aas() and ash.top().$$;

-- Schema-level map: one obj_description() lookup orients a human or agent on
-- the whole surface (readers vs operations) before any \df spelunking.
comment on schema ash is
$$pg_ash: Active Session History for Postgres (pure SQL, no extension). Reader entry points (start with ash.periods()): periods, aas, timeline, top, compare, samples, report, chart, summary, status — every reader reports load in AAS (Average Active Sessions) and names its data source. Operations/admin (owner-only, not granted by grant_reader): start, stop, take_sample, rotate, rollup_minute, rollup_hour, rollup_cleanup, rebuild_partitions, set_debug_logging, uninstall, grant_reader, revoke_reader. Each function documents itself: select obj_description('ash.<name>(<argtypes>)'::regprocedure).$$;

-- Operational / admin surface: obj_description for every entry point, so the
-- reader-vs-ops split is legible from \df+ alone.
comment on function ash.status() is
$$Installation health snapshot (readable by monitoring roles): sampling state and interval, pg_cron job status, partition slots and sizes, rollup progress/lag, retention starts (raw_retention_start, rollup_1m_retention_start, rollup_1h_retention_start — use these to plan reader windows), error counters (insert_errors, register_wait_cap_hits, missed/skipped samples), and version. Returns (metric, value) rows. Start here when pg_ash misbehaves; readers are documented on the schema: obj_description('ash'::regnamespace).$$;

comment on function ash.start(interval) is
$$Admin: start sampling — schedules take_sample() at the given interval (every => ..., default 1 second) plus rollup_minute()/rollup_hour()/rollup_cleanup() and rotation via pg_cron when available; without pg_cron it enables sampling and prints the jobs to schedule externally. Idempotent. Inverse: ash.stop(). Check with ash.status().$$;

comment on function ash.stop() is
$$Admin: stop sampling — unschedules the pg_cron jobs created by ash.start() (or disables sampling when pg_cron is absent). Collected data is kept and remains readable. Inverse: ash.start().$$;

comment on function ash.take_sample() is
$$Admin/internal: take one wait-event sample of pg_stat_activity into the current slot (the every-second worker that ash.start() schedules). Returns the number of active backends captured. Call manually only for testing; continuous sampling belongs to ash.start().$$;

comment on function ash.rotate() is
$$Admin: rotate to the next partition slot, rolling up and truncating the oldest (the daily job scheduled by ash.start()). Controls raw retention: num_partitions x rotation_period of raw samples are kept. Safe to call manually to force a rotation.$$;

comment on function ash.rollup_minute(int) is
$$Admin: fold completed minutes of raw samples into ash.rollup_1m (the every-minute job scheduled by ash.start()); batch_limit caps catch-up minutes per call. Returns minutes processed. Readers pick rollups automatically — this only needs manual calls when pg_cron is absent.$$;

comment on function ash.rollup_hour() is
$$Admin: fold completed hours of ash.rollup_1m into ash.rollup_1h, preserving per-minute totals in minute_counts so long-window peak/p99 stay minute-grain (the hourly job scheduled by ash.start()). Returns hours processed.$$;

comment on function ash.rollup_cleanup() is
$$Admin: delete rollup rows past retention (rollup_1m_retention_days / rollup_1h_retention_days in ash.config; the daily job scheduled by ash.start()). Returns a summary of rows deleted.$$;

comment on function ash.rebuild_partitions(int, text) is
$$Admin, DESTRUCTIVE: drop and recreate the sample/query-map partitions with num_partitions slots (3-32; raw retention = slots x rotation_period). Requires confirm => 'yes'. DELETES ALL RAW SAMPLES (rollups are kept). Re-run ash.grant_reader() for every monitoring role afterwards — the new partitions carry no grants.$$;

comment on function ash.set_debug_logging(bool) is
$$Admin: toggle debug logging for the sampler and rollup jobs (null argument reports the current setting). Returns the resulting state.$$;

comment on function ash.uninstall(text) is
$$Admin, DESTRUCTIVE: remove pg_ash entirely — unschedules jobs and drops schema ash with all collected data. Requires confirm => 'yes'.$$;

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

-- Helper: re-apply search_path on the pgss reader functions using the currently
-- detected pgss schema. Run this after installing / moving pg_stat_statements if
-- it lives outside `public`. Safe to re-run.
create or replace function ash._apply_pgss_search_path()
returns text
language plpgsql
set search_path = pg_catalog, ash
as $$
declare
  v_pgss_schema text := ash._pgss_schema();
  -- The pgss readers are derived from the catalog, not a hand-maintained list
  -- (the old list named v1.x functions that no longer exist, so the #76 shadow
  -- mitigation covered nothing). Every ash.* function that must resolve
  -- pg_stat_statements carries `public` in its own search_path (see the per-
  -- function `set search_path = pg_catalog, ash, public` clauses); those are
  -- exactly the functions whose path we rewrite so the real pgss schema is
  -- listed before public. No-pgss functions have no `public` in their path and
  -- are left alone. Idempotent: the rewritten path still ends in public, so a
  -- re-run re-selects the same set.
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
      -- 'public' is one of the schemas in the function's own search_path
      -- (spaces stripped, comma-wrapped so 'publications' etc. can't match).
      and exists (
        select 1 from unnest(coalesce(p.proconfig, array[]::text[])) cfg
        where cfg like 'search_path=%'
          and (',' || replace(split_part(cfg, '=', 2), ' ', '') || ',') like '%,public,%'
      )
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
set search_path = pg_catalog
as $$
  select array[
    'start', 'stop', 'uninstall', 'rotate', 'take_sample',
    'set_debug_logging', 'rebuild_partitions', 'rollup_minute',
    'rollup_hour', 'rollup_cleanup', '_drop_all_partitions',
    '_rebuild_query_map_view', '_merge_wait_counts', '_merge_query_counts',
    '_truncate_pairs', '_int4_array_cat_agg', '_int8_array_cat_agg',
    '_register_wait',
    -- the helpers themselves: granting them to a reader role would let
    -- that role hand out privileges. keep them admin-only.
    'grant_reader', 'revoke_reader',
    -- _apply_pgss_search_path runs ALTER FUNCTION on every reader, which
    -- requires owner privilege so a non-owner call would fail anyway, but
    -- list it explicitly so grant_reader doesn't hand it to monitoring
    -- roles in the first place. _pgss_schema() is read-only and can stay
    -- generally callable.
    '_apply_pgss_search_path'
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
--     _truncate_pairs, _int4_array_cat_agg, _int8_array_cat_agg,
--     _register_wait). Defining "reader" by exclusion (rather than
--     enumeration) keeps the helpers correct as new readers and
--     reader-internal helpers are added.
--   - SELECT on ash.sample (+ every sample_N partition), ash.query_map_all
--     (+ every query_map_N partition), ash.config, ash.wait_event_map,
--     ash.rollup_1m, ash.rollup_1h.
--
-- Both helpers are idempotent (safe to re-run), validate the role exists
-- via pg_roles, quote_ident() the role name, and emit a RAISE NOTICE
-- summarizing what was changed. revoke_reader() is the symmetric undo.
create or replace function ash.grant_reader(role name)
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
  if role is null or length(trim(role::text)) = 0 then
    raise exception 'ash.grant_reader: role name must not be null or empty';
  end if;

  -- Validate role exists. quote_ident() defends against SQL injection in
  -- the dynamic GRANT statements below, but a non-existent role would
  -- raise a confusing "role does not exist" from inside the loop —
  -- surface a clear error up front instead.
  if not exists (select 1 from pg_catalog.pg_roles where rolname = role) then
    raise exception 'ash.grant_reader: role % does not exist', quote_literal(role);
  end if;

  v_role := quote_ident(role);

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

  -- query_text from top('query_id')/samples() reads other roles' pg_stat_statements
  -- text, which pgss restricts to holders of pg_read_all_stats (pg_monitor members
  -- have it). We deliberately do NOT auto-grant pg_read_all_stats (least privilege);
  -- just tell the operator so a NULL query_text is not mistaken for a bug.
  if not pg_catalog.pg_has_role(role, 'pg_read_all_stats', 'MEMBER')
     and not exists (select 1 from pg_catalog.pg_roles
                     where rolname = role and rolsuper) then
    raise notice 'ash.grant_reader: note: % lacks pg_read_all_stats (not a pg_monitor member); query_text will be NULL for it — grant pg_monitor to % if query text is needed',
      role, role;
  end if;
end;
$$;

comment on function ash.grant_reader(name) is
  'Grants the minimum privileges (USAGE on schema ash, EXECUTE on all reader functions AND the internal helpers they depend on, SELECT on reader tables incl. partitions) to a monitoring role. This is the supported way to grant reader access: reader functions are SECURITY INVOKER and call shared internal helpers (also revoked from PUBLIC), so granting EXECUTE on an individual reader function alone is not sufficient. Idempotent. Inverse: ash.revoke_reader(name). Caveat: ash.rebuild_partitions(N, ''yes'') creates new partition tables that previously-granted readers cannot access; re-run ash.grant_reader() for each monitoring role after any rebuild_partitions() call.';

create or replace function ash.revoke_reader(role name)
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
  if role is null or length(trim(role::text)) = 0 then
    raise exception 'ash.revoke_reader: role name must not be null or empty';
  end if;

  if not exists (select 1 from pg_catalog.pg_roles where rolname = role) then
    raise exception 'ash.revoke_reader: role % does not exist', quote_literal(role);
  end if;

  v_role := quote_ident(role);

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

-- Re-apply the EXECUTE grants snapshotted before the drop/recreate at the
-- top of this script (#107), keeping the restore strictly least-privilege:
--   * Admin functions (ash._admin_funcs()) are never restored. They are
--     locked to the owner by the hardening block above and are never handed
--     out by ash.grant_reader(); re-applying a stray manual grant on one
--     would silently undo that hardening on every install. Excluding them
--     here preserves the pre-#107 behaviour where DROP FUNCTION scrubbed
--     such grants.
--   * Each grant is restored to the exact same signature when it still
--     exists, so a role that held only one overload of a function is not
--     widened to its siblings. Only when the snapshotted signature is gone
--     (a genuine cross-version signature change — the case this restore
--     exists to cover) does it fall back to every current overload of the
--     name.
-- This mirrors what CREATE OR REPLACE would have preserved: function names
-- that no longer exist (removed/renamed) are skipped, roles dropped
-- mid-script are skipped, and a partially-granted role's reach is never
-- widened. Roles that held the FULL pre-upgrade reader bundle (detected in
-- the snapshot block at the top of this script) are additionally re-run
-- through ash.grant_reader() afterwards, so they can also execute helpers
-- introduced by this version — otherwise the readers they kept would fail
-- mid-call on the first new internal helper.
do $$
declare
  r record;
  v_admin_funcs constant text[] := ash._admin_funcs();
begin
  for r in
    select distinct a.grantee, a.grantable, p.proname,
           pg_catalog.pg_get_function_identity_arguments(p.oid) as args
    from pg_temp._ash_install_func_acl a
    join pg_catalog.pg_roles g on g.rolname = a.grantee
    join pg_catalog.pg_proc p on p.proname = a.proname
    join pg_catalog.pg_namespace n on n.oid = p.pronamespace
    where n.nspname = 'ash'
      and p.prokind in ('f', 'a')
      and p.proname::text <> all (v_admin_funcs)
      and (
        -- the snapshotted overload still exists: restore exactly it
        pg_catalog.pg_get_function_identity_arguments(p.oid) = a.args
        -- the snapshotted overload is gone (signature changed): fall back
        -- to restoring every current overload of the name
        or not exists (
          select 1
          from pg_catalog.pg_proc p2
          join pg_catalog.pg_namespace n2 on n2.oid = p2.pronamespace
          where n2.nspname = 'ash'
            and p2.proname = a.proname
            and pg_catalog.pg_get_function_identity_arguments(p2.oid) = a.args
        )
      )
  loop
    execute format('grant execute on function ash.%I(%s) to %I%s',
                   r.proname, r.args, r.grantee,
                   case when r.grantable then ' with grant option' else '' end);
  end loop;

  -- Preserved reader roles: grant the full CURRENT reader closure. The
  -- exact-signature loop above only replays pre-upgrade grants, so helpers
  -- new in this version would stay denied for these roles. grant_reader()
  -- excludes the admin set, so this never widens beyond reader access.
  for r in
    select t.rolname
    from pg_temp._ash_install_reader_roles t
    join pg_catalog.pg_roles g on g.rolname = t.rolname
  loop
    perform ash.grant_reader(r.rolname);
  end loop;

  drop table pg_temp._ash_install_func_acl;
  drop table pg_temp._ash_install_reader_roles;
end $$;

-- Default reader: pg_monitor.
--
-- pg_monitor is the predefined monitoring role (PG10+); its members already
-- hold pg_read_all_stats / pg_read_all_settings / pg_stat_scan_tables and can
-- therefore read pg_stat_activity and pg_stat_statements directly — the very
-- sources pg_ash samples from. Granting them pg_ash's derived data exposes
-- nothing they could not already see, and it makes monitoring tools running
-- as pg_monitor members (Grafana, exporters) work out of the box.
--
-- Opt out at any time (the grant is a plain ash.grant_reader() bundle):
--   select ash.revoke_reader('pg_monitor');
--
-- Best-effort: this must NEVER abort the install. On a locked-down managed
-- platform the installing role may not be allowed to grant on behalf of
-- pg_monitor — warn and continue; the operator can grant_reader() manually.
do $$
begin
  if exists (select from pg_catalog.pg_roles where rolname = 'pg_monitor') then
    -- Revoke first so pg_monitor's aclitem always lands LAST on every
    -- function/table, on fresh installs and re-applies alike. Without this,
    -- CREATE OR REPLACE preserves proacl across a re-apply, and the mid-file
    -- REVOKE-then-GRANT-to-PUBLIC on epoch/ts_* would leave pg_monitor in a
    -- different aclitem position than on a fresh install — same effective
    -- privileges, but a text-level proacl diff in equivalence snapshots.
    perform ash.revoke_reader('pg_monitor');
    perform ash.grant_reader('pg_monitor');
  else
    -- pg_monitor is predefined on every supported PG version; this branch is
    -- purely defensive (e.g. an exotic fork without predefined roles).
    raise notice 'pg_ash: role pg_monitor not found, skipping default reader grant';
  end if;
exception when others then
  raise warning 'pg_ash: could not grant default reader privileges to pg_monitor (%: %); '
    'run "select ash.grant_reader(''pg_monitor'')" manually, or ignore to leave pg_monitor without access',
    sqlstate, sqlerrm;
end $$;
