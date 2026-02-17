-- pg_ash: Active Session History for Postgres
-- Steps 1-2: Core schema, infrastructure, sampler, and decoder


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
  singleton          bool primary key default true check (singleton),
  current_slot       smallint not null default 0,
  sample_interval    interval not null default '1 second',
  rotation_period    interval not null default '1 day',
  include_bg_workers bool not null default false,
  encoding_version   smallint not null default 1,
  rotated_at         timestamptz not null default clock_timestamp(),
  installed_at       timestamptz not null default clock_timestamp()
);

-- Insert initial row if not exists
insert into ash.config (singleton) values (true) on conflict do nothing;

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
create table if not exists ash.query_map_0 (
  id        int4 primary key generated always as identity (start with 1),
  query_id  int8 not null unique
);
create table if not exists ash.query_map_1 (
  id        int4 primary key generated always as identity (start with 1),
  query_id  int8 not null unique
);
create table if not exists ash.query_map_2 (
  id        int4 primary key generated always as identity (start with 1),
  query_id  int8 not null unique
);

-- Unified view for readers — planner eliminates non-matching partitions
-- when slot is a constant (which it is, from s.slot in reader queries).
create or replace view ash.query_map_all as
  select 0::smallint as slot, id, query_id from ash.query_map_0
  union all
  select 1::smallint, id, query_id from ash.query_map_1
  union all
  select 2::smallint, id, query_id from ash.query_map_2;

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
         check (data[1] < 0 and array_length(data, 1) >= 2),
  slot         smallint not null default ash.current_slot()
) partition by list (slot);

-- Create partitions
create table if not exists ash.sample_0 partition of ash.sample for values in (0);
create table if not exists ash.sample_1 partition of ash.sample for values in (1);
create table if not exists ash.sample_2 partition of ash.sample for values in (2);

-- Create indexes on partitions
-- (sample_ts) for time-range reader queries
create index if not exists sample_0_ts_idx on ash.sample_0 (sample_ts);
create index if not exists sample_1_ts_idx on ash.sample_1 (sample_ts);
create index if not exists sample_2_ts_idx on ash.sample_2 (sample_ts);
-- (datid, sample_ts) for per-database time-range queries
create index if not exists sample_0_datid_ts_idx on ash.sample_0 (datid, sample_ts);
create index if not exists sample_1_datid_ts_idx on ash.sample_1 (datid, sample_ts);
create index if not exists sample_2_datid_ts_idx on ash.sample_2 (datid, sample_ts);

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

  -- Minimum: one wait group = [-wid, count, qid] = 3 elements
  -- But could be [-wid, count] = 2 if count=0... no, count > 0
  -- Actually minimum is [-wid, 1, qid] = 3
  if v_len < 2 then
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
  v_rec record;
  v_datid_rec record;
  v_data integer[];
  v_active_count smallint;
  v_current_wait_id smallint;
  v_current_slot smallint;
  v_rows_inserted int := 0;
  v_state text;
  v_type text;
  v_event text;
begin
  -- Get sample timestamp (seconds since epoch, from now())
  v_sample_ts := extract(epoch from now() - ash.epoch())::int4;

  -- Get config
  select include_bg_workers into v_include_bg from ash.config where singleton;
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

  -- ---- Read 1: Register new wait events and query_ids ----
  -- CPU* means the backend is active with no wait event reported. This is
  -- either genuine CPU work or an uninstrumented code path in Postgres.
  -- The asterisk signals this ambiguity. See https://gaps.wait.events
  for v_rec in
    select distinct
      sa.state,
      coalesce(sa.wait_event_type,
        case
          when sa.state = 'active' then 'CPU*'
          when sa.state like 'idle in transaction%' then 'IdleTx'
        end
      ) as wait_type,
      coalesce(sa.wait_event,
        case
          when sa.state = 'active' then 'CPU*'
          when sa.state like 'idle in transaction%' then 'IdleTx'
        end
      ) as wait_event
    from pg_stat_activity sa
    where sa.state in ('active', 'idle in transaction', 'idle in transaction (aborted)')
      and (sa.backend_type = 'client backend'
       or (v_include_bg and sa.backend_type in ('autovacuum worker', 'logical replication worker', 'parallel worker', 'background worker')))
      and sa.pid <> pg_backend_pid()
  loop
    if not exists (
      select from ash.wait_event_map
      where state = v_rec.state and type = v_rec.wait_type and event = v_rec.wait_event
    ) then
      perform ash._register_wait(v_rec.state, v_rec.wait_type, v_rec.wait_event);
    end if;
  end loop;

  -- ---- Read 2: Register query_ids into current slot's query_map ----
  -- Partitioned query_map: TRUNCATE resets on rotation, but between rotations
  -- PG14-15 volatile SQL comments can flood query_map. 50k hard cap per
  -- partition prevents unbounded growth. PG16+ normalizes comments.
  -- NOTE: 3-way IF for clarity on hot path. Bug fixes must be applied 3×.
  if v_current_slot = 0 then
    insert into ash.query_map_0 (query_id)
    select distinct sa.query_id
    from pg_stat_activity sa
    where sa.query_id is not null
      and sa.state in ('active', 'idle in transaction', 'idle in transaction (aborted)')
      and (sa.backend_type = 'client backend'
       or (v_include_bg and sa.backend_type in ('autovacuum worker', 'logical replication worker', 'parallel worker', 'background worker')))
      and sa.pid <> pg_backend_pid()
      and (select reltuples from pg_class
       where oid = 'ash.query_map_0'::regclass) < 50000
    on conflict (query_id) do nothing;
  elsif v_current_slot = 1 then
    insert into ash.query_map_1 (query_id)
    select distinct sa.query_id
    from pg_stat_activity sa
    where sa.query_id is not null
      and sa.state in ('active', 'idle in transaction', 'idle in transaction (aborted)')
      and (sa.backend_type = 'client backend'
       or (v_include_bg and sa.backend_type in ('autovacuum worker', 'logical replication worker', 'parallel worker', 'background worker')))
      and sa.pid <> pg_backend_pid()
      and (select reltuples from pg_class
       where oid = 'ash.query_map_1'::regclass) < 50000
    on conflict (query_id) do nothing;
  else
    insert into ash.query_map_2 (query_id)
    select distinct sa.query_id
    from pg_stat_activity sa
    where sa.query_id is not null
      and sa.state in ('active', 'idle in transaction', 'idle in transaction (aborted)')
      and (sa.backend_type = 'client backend'
       or (v_include_bg and sa.backend_type in ('autovacuum worker', 'logical replication worker', 'parallel worker', 'background worker')))
      and sa.pid <> pg_backend_pid()
      and (select reltuples from pg_class
       where oid = 'ash.query_map_2'::regclass) < 50000
    on conflict (query_id) do nothing;
  end if;

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

      if v_data is not null and array_length(v_data, 1) >= 2 then
        insert into ash.sample (sample_ts, datid, active_count, data)
        values (v_sample_ts, v_datid_rec.datid, v_active_count, v_data);
        v_rows_inserted := v_rows_inserted + 1;
      end if;

    exception when others then
      raise warning 'ash.take_sample: error inserting sample for datid %: %', v_datid_rec.datid, sqlerrm;
    end;
  end loop;

  return v_rows_inserted;
end;
$$;

-- Decode sample function
-- p_slot: when provided, look up query_ids from that partition only.
-- When NULL (default), search all partitions via query_map_all view.
create or replace function ash.decode_sample(p_data integer[], p_slot smallint default null)
returns table (
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
  if v_len < 2 or p_data[1] >= 0 then
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
create or replace function ash.rotate()
returns text
language plpgsql
as $$
declare
  v_old_slot smallint;
  v_new_slot smallint;
  v_truncate_slot smallint;
  v_rotation_period interval;
  v_rotated_at timestamptz;
begin
  -- Try to acquire advisory lock to prevent concurrent rotation
  if not pg_try_advisory_lock(hashtext('ash_rotate')) then
    return 'skipped: another rotation in progress';
  end if;

  begin
    -- Get current config
    select current_slot, rotation_period, rotated_at
    into v_old_slot, v_rotation_period, v_rotated_at
    from ash.config
    where singleton;

    -- Check if we rotated too recently (within 90% of rotation_period)
    if now() - v_rotated_at < v_rotation_period * 0.9 then
      perform pg_advisory_unlock(hashtext('ash_rotate'));
      return 'skipped: rotated too recently at ' || v_rotated_at::text;
    end if;

    -- Set lock timeout to avoid blocking on long-running queries
    set local lock_timeout = '2s';

    -- Calculate new slot (0 -> 1 -> 2 -> 0)
    v_new_slot := (v_old_slot + 1) % 3;

    -- The partition to truncate is the one that was "previous" before rotation
    -- which is (old_slot - 1 + 3) % 3, but after we advance, it becomes
    -- the "next" partition: (new_slot + 1) % 3
    v_truncate_slot := (v_new_slot + 1) % 3;

    -- Advance current_slot first (before truncate)
    update ash.config
    set current_slot = v_new_slot,
      rotated_at = now()
    where singleton;

    -- Lockstep TRUNCATE: sample partition + matching query_map partition.
    -- Zero bloat everywhere — no DELETE, no dead tuples, no GC needed.
    case v_truncate_slot
      when 0 then
        truncate ash.sample_0;
        truncate ash.query_map_0;
        alter table ash.query_map_0 alter column id restart;
      when 1 then
        truncate ash.sample_1;
        truncate ash.query_map_1;
        alter table ash.query_map_1 alter column id restart;
      when 2 then
        truncate ash.sample_2;
        truncate ash.query_map_2;
        alter table ash.query_map_2 alter column id restart;
    end case;

    perform pg_advisory_unlock(hashtext('ash_rotate'));

    return format('rotated: slot %s -> %s, truncated slot %s (sample + query_map)',
           v_old_slot, v_new_slot, v_truncate_slot);

  exception when lock_not_available then
    perform pg_advisory_unlock(hashtext('ash_rotate'));
    return 'failed: lock timeout on partition truncate, will retry next cycle';
  when others then
    perform pg_advisory_unlock(hashtext('ash_rotate'));
    raise;
  end;
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
  v_schedule text;
begin
  -- Check if pg_cron is available
  if not ash._pg_cron_available() then
    job_type := 'error';
    job_id := null;
    status := 'pg_cron extension not installed';
    return next;
    return;
  end if;

  -- Check pg_cron version (need >= 1.5 for second granularity)
  select extversion into v_cron_version
  from pg_extension where extname = 'pg_cron';

  if v_cron_version < '1.5' then
    job_type := 'error';
    job_id := null;
    status := format('pg_cron version %s too old, need >= 1.5', v_cron_version);
    return next;
    return;
  end if;

  -- Convert interval to pg_cron schedule format
  -- pg_cron expects 'N seconds' (not interval text like '00:00:01')
  v_seconds := extract(epoch from p_interval)::int;
  if v_seconds < 1 or v_seconds > 59 then
    job_type := 'error';
    job_id := null;
    status := format('interval must be 1-59 seconds, got %s', p_interval);
    return next;
    return;
  end if;
  v_schedule := v_seconds || ' seconds';

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
      'SELECT ash.take_sample()'
    ) into v_sampler_job;

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
      'SELECT ash.rotate()'
    ) into v_rotation_job;

    job_type := 'rotation';
    job_id := v_rotation_job;
    status := 'created';
    return next;
  end if;

  -- Update sample_interval in config
  update ash.config set sample_interval = p_interval where singleton;

  return;
end;
$$;

-- Stop sampling: remove pg_cron jobs
create or replace function ash.stop()
returns table (job_type text, job_id bigint, status text)
language plpgsql
as $$
declare
  v_job_id bigint;
begin
  -- Check if pg_cron is available
  if not ash._pg_cron_available() then
    job_type := 'info';
    job_id := null;
    status := 'pg_cron not installed, no jobs to remove';
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

  return;
end;
$$;

-- Uninstall: stop jobs and drop schema
create or replace function ash.uninstall()
returns text
language plpgsql
as $$
declare
  v_rec record;
  v_jobs_removed int := 0;
begin
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
    ((current_slot - 1 + 3) % 3)::smallint
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
as $$
declare
  v_config record;
  v_last_sample_ts int4;
  v_samples_current int;
  v_samples_total int;
  v_wait_events int;
  v_query_ids int;
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

  metric := 'current_slot'; value := v_config.current_slot::text; return next;
  metric := 'sample_interval'; value := v_config.sample_interval::text; return next;
  metric := 'rotation_period'; value := v_config.rotation_period::text; return next;
  metric := 'include_bg_workers'; value := v_config.include_bg_workers::text; return next;
  metric := 'installed_at'; value := v_config.installed_at::text; return next;
  metric := 'rotated_at'; value := v_config.rotated_at::text; return next;
  metric := 'time_since_rotation'; value := (now() - v_config.rotated_at)::text; return next;

  if v_last_sample_ts is not null then
    metric := 'last_sample_ts'; value := (ash.epoch() + v_last_sample_ts * interval '1 second')::text; return next;
    metric := 'time_since_last_sample'; value := (now() - (ash.epoch() + v_last_sample_ts * interval '1 second'))::text; return next;
  else
    metric := 'last_sample_ts'; value := 'no samples'; return next;
  end if;

  metric := 'samples_in_current_slot'; value := v_samples_current::text; return next;
  metric := 'samples_total'; value := v_samples_total::text; return next;
  metric := 'wait_event_map_count'; value := v_wait_events::text; return next;
  metric := 'wait_event_map_utilization'; value := round(v_wait_events::numeric / 32767 * 100, 2)::text || '%'; return next;
  metric := 'query_map_count'; value := v_query_ids::text; return next;

  -- pg_cron status if available
  if ash._pg_cron_available() then
    metric := 'pg_cron_available'; value := 'yes'; return next;
    for metric, value in
      select 'cron_job_' || jobname,
         format('id=%s, schedule=%s, active=%s', jobid, schedule, active)
      from cron.job
      where jobname in ('ash_sampler', 'ash_rotation')
    loop
      return next;
    end loop;
  else
    metric := 'pg_cron_available'; value := 'no'; return next;
  end if;

  return;
end;
$$;

-- Top wait events (inline SQL decode — no plpgsql per-row overhead)
create or replace function ash.top_waits(
  p_interval interval default '1 hour',
  p_limit int default 10
)
returns table (
  wait_event text,
  samples bigint,
  pct numeric
)
language sql
stable
set jit = off
as $$
  with waits as (
    select (-s.data[i])::smallint as wait_id, s.data[i + 1] as cnt
    from ash.sample s, generate_subscripts(s.data, 1) i
    where s.slot = any(ash._active_slots())
      and s.sample_ts >= extract(epoch from now() - p_interval - ash.epoch())::int4
      and s.data[i] < 0
  ),
  totals as (
    select w.wait_id, sum(w.cnt) as cnt
    from waits w
    group by w.wait_id
  ),
  grand_total as (
    select sum(cnt) as total from totals
  ),
  ranked as (
    select
      case when wm.event = wm.type then wm.event else wm.type || ':' || wm.event end as wait_event,
      t.cnt,
      row_number() over (order by t.cnt desc) as rn
    from totals t
    join ash.wait_event_map wm on wm.id = t.wait_id
  ),
  top_rows as (
    select wait_event, cnt, rn from ranked where rn <= p_limit
  ),
  other as (
    select 'Other'::text as wait_event, sum(cnt) as cnt, p_limit::bigint as rn
    from ranked where rn > p_limit
    having sum(cnt) > 0
  )
  select
    r.wait_event,
    r.cnt as samples,
    round(r.cnt::numeric / gt.total * 100, 2) as pct
  from (select * from top_rows union all select * from other) r
  cross join grand_total gt
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
  v_has_pg_stat_statements boolean := false;
begin
  -- Probe the view directly — extension installed <> shared library loaded
  begin
    perform 1 from pg_stat_statements limit 1;
    v_has_pg_stat_statements := true;
  exception when others then
    v_has_pg_stat_statements := false;
  end;

  if v_has_pg_stat_statements then
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
create or replace function ash.waits_by_type(
  p_interval interval default '1 hour'
)
returns table (
  wait_event_type text,
  samples bigint,
  pct numeric
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
  )
  select
    t.wait_type as wait_event_type,
    t.cnt as samples,
    round(t.cnt::numeric / gt.total * 100, 2) as pct
  from totals t, grand_total gt
  order by t.cnt desc
$$;

-- Samples by database
-- Top queries with text from pg_stat_statements
-- Returns query text and pgss stats when pg_stat_statements is available,
-- NULL columns otherwise.
create or replace function ash.top_queries_with_text(
  p_interval interval default '1 hour',
  p_limit int default 10
)
returns table (
  query_id bigint,
  samples bigint,
  pct numeric,
  calls bigint,
  mean_time_ms numeric,
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
      round(pss.mean_exec_time::numeric, 2) as mean_time_ms,
      left(pss.query, 200) as query_text
    from resolved r
    cross join grand_total gt
    left join pg_stat_statements pss on pss.queryid = r.query_id
    order by r.cnt desc
    limit p_limit;
  else
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
      null::bigint as calls,
      null::numeric as mean_time_ms,
      null::text as query_text
    from resolved r
    cross join grand_total gt
    order by r.cnt desc
    limit p_limit;
  end if;
end;
$$;

-- Wait profile for a specific query — what is this query waiting on?
-- Walks the encoded arrays, finds the query_map_id, then looks back to find
-- which wait group it belongs to (the nearest preceding negative element).
create or replace function ash.query_waits(
  p_query_id bigint,
  p_interval interval default '1 hour'
)
returns table (
  wait_event text,
  samples bigint,
  pct numeric
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
  totals as (
    select wait_id, count(*) as cnt
    from hits
    where wait_id is not null
    group by wait_id
  ),
  grand_total as (
    select sum(cnt) as total from totals
  )
  select
    case when wm.event = wm.type then wm.event else wm.type || ':' || wm.event end as wait_event,
    t.cnt as samples,
    round(t.cnt::numeric / gt.total * 100, 2) as pct
  from totals t
  join ash.wait_event_map wm on wm.id = t.wait_id
  cross join grand_total gt
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

-- Convert a timestamptz to our internal sample_ts (seconds since epoch)
create or replace function ash._to_sample_ts(p_ts timestamptz)
returns int4
language sql
immutable
as $$
  select extract(epoch from p_ts - ash.epoch())::int4
$$;

-- Top waits in an absolute time range
create or replace function ash.top_waits_at(
  p_start timestamptz,
  p_end timestamptz,
  p_limit int default 10
)
returns table (
  wait_event text,
  samples bigint,
  pct numeric
)
language sql
stable
set jit = off
as $$
  with waits as (
    select (-s.data[i])::smallint as wait_id, s.data[i + 1] as cnt
    from ash.sample s, generate_subscripts(s.data, 1) i
    where s.slot = any(ash._active_slots())
      and s.sample_ts >= ash._to_sample_ts(p_start)
      and s.sample_ts < ash._to_sample_ts(p_end)
      and s.data[i] < 0
  ),
  totals as (
    select w.wait_id, sum(w.cnt) as cnt
    from waits w
    group by w.wait_id
  ),
  grand_total as (
    select sum(cnt) as total from totals
  ),
  ranked as (
    select
      case when wm.event = wm.type then wm.event else wm.type || ':' || wm.event end as wait_event,
      t.cnt,
      row_number() over (order by t.cnt desc) as rn
    from totals t
    join ash.wait_event_map wm on wm.id = t.wait_id
  ),
  top_rows as (
    select wait_event, cnt, rn from ranked where rn <= p_limit
  ),
  other as (
    select 'Other'::text as wait_event, sum(cnt) as cnt, p_limit::bigint as rn
    from ranked where rn > p_limit
    having sum(cnt) > 0
  )
  select
    r.wait_event,
    r.cnt as samples,
    round(r.cnt::numeric / gt.total * 100, 2) as pct
  from (select * from top_rows union all select * from other) r
  cross join grand_total gt
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
  v_start int4 := ash._to_sample_ts(p_start);
  v_end int4 := ash._to_sample_ts(p_end);
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
      and s.sample_ts >= ash._to_sample_ts(p_start)
      and s.sample_ts < ash._to_sample_ts(p_end)
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
create or replace function ash.waits_by_type_at(
  p_start timestamptz,
  p_end timestamptz
)
returns table (
  wait_event_type text,
  samples bigint,
  pct numeric
)
language sql
stable
set jit = off
as $$
  with waits as (
    select (-s.data[i])::smallint as wait_id, s.data[i + 1] as cnt
    from ash.sample s, generate_subscripts(s.data, 1) i
    where s.slot = any(ash._active_slots())
      and s.sample_ts >= ash._to_sample_ts(p_start)
      and s.sample_ts < ash._to_sample_ts(p_end)
      and s.data[i] < 0
  ),
  totals as (
    select wm.type as wait_type, sum(w.cnt) as cnt
    from waits w join ash.wait_event_map wm on wm.id = w.wait_id
    group by wm.type
  ),
  grand_total as (
    select sum(cnt) as total from totals
  )
  select t.wait_type, t.cnt, round(t.cnt::numeric / gt.total * 100, 2)
  from totals t, grand_total gt
  order by t.cnt desc
$$;

-- Query waits in an absolute time range
create or replace function ash.query_waits_at(
  p_query_id bigint,
  p_start timestamptz,
  p_end timestamptz
)
returns table (
  wait_event text,
  samples bigint,
  pct numeric
)
language plpgsql
stable
set jit = off
as $$
declare
  v_start int4 := ash._to_sample_ts(p_start);
  v_end int4 := ash._to_sample_ts(p_end);
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
  totals as (
    select wait_id, count(*) as cnt from hits where wait_id is not null group by wait_id
  ),
  grand_total as (
    select sum(cnt) as total from totals
  )
  select case when wm.event = wm.type then wm.event else wm.type || ':' || wm.event end, t.cnt, round(t.cnt::numeric / gt.total * 100, 2)
  from totals t
  join ash.wait_event_map wm on wm.id = t.wait_id
  cross join grand_total gt
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
      from ash.top_waits(p_interval, 4) tw
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

create or replace function ash.histogram(
  p_interval interval default '1 hour',
  p_limit int default 10,
  p_width int default 40
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
  with data as (
    select tw.wait_event, tw.samples, tw.pct
    from ash.top_waits(p_interval, p_limit) tw
  ),
  max_pct as (
    select max(d.pct) as m from data d
  )
  select
    rpad(d.wait_event, 20) as wait_event,
    d.samples,
    d.pct,
    repeat('█', greatest(1, (d.pct / nullif(mp.m, 0) * p_width)::int))
      || ' ' || d.pct || '%' as bar
  from data d, max_pct mp
$$;

create or replace function ash.histogram_at(
  p_start timestamptz,
  p_end timestamptz,
  p_limit int default 10,
  p_width int default 40
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
  with data as (
    select tw.wait_event, tw.samples, tw.pct
    from ash.top_waits_at(p_start, p_end, p_limit) tw
  ),
  max_pct as (
    select max(d.pct) as m from data d
  )
  select
    rpad(d.wait_event, 20) as wait_event,
    d.samples,
    d.pct,
    repeat('█', greatest(1, (d.pct / nullif(mp.m, 0) * p_width)::int))
      || ' ' || d.pct || '%' as bar
  from data d, max_pct mp
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
  v_start := ash._to_sample_ts(p_start);
  v_end := ash._to_sample_ts(p_end);

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

