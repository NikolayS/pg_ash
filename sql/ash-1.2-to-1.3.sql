-- pg_ash: upgrade from 1.2 to 1.3
-- Safe to re-run (idempotent).
-- Changes:
--   - pg_cron is now optional — ash.start(), ash.stop(), ash.status()
--     gracefully handle the no-pg_cron case with external scheduling guidance.
--   - Re-creates take_sample(), rotate(), top_waits(), top_waits_at(),
--     top_queries() to match fresh install (schema parity).
--   - Fixes config.version column default and sample.data CHECK constraint.

-- Start sampling: create pg_cron jobs or advise on external scheduling
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
    update ash.config set sample_interval = p_interval where singleton;

    job_type := 'sampler';
    job_id := null;
    status := format('interval set to %s — schedule externally (pg_cron not available)', p_interval);
    return next;

    job_type := 'rotation';
    job_id := null;
    status := format('rotation_period is %s — schedule ash.rotate() externally', (select rotation_period from ash.config where singleton));
    return next;

    raise notice 'pg_cron is not installed. To sample, call ash.take_sample() from an external scheduler:';
    raise notice '  system cron:    * * * * * psql -qAtX -c "select ash.take_sample()" (for per-second, use a loop)';
    raise notice '  psql:           SELECT ash.take_sample() \\watch 1';
    raise notice '  any language:   execute "SELECT ash.take_sample()" in a loop with sleep';
    raise notice 'Also schedule ash.rotate() at the rotation_period interval (default: daily).';

    return;
  end if;

  -- Check pg_cron version (need >= 1.5 for second granularity)
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

  -- Convert interval to pg_cron schedule format
  -- pg_cron supports: '[1-59] seconds' for sub-minute, or cron syntax for minute+

  -- Build schedule: seconds format for <60s, cron format for 60s+
  if v_seconds <= 59 then
    v_schedule := v_seconds || ' seconds';
  elsif v_seconds < 3600 then
    -- Convert to cron: every N minutes
    if v_seconds % 60 != 0 then
      job_type := 'error';
      job_id := null;
      status := format('interval must be exact minutes (60s, 120s, etc.), got %s', p_interval);
      return next;
      return;
    end if;
    v_schedule := '*/' || (v_seconds / 60) || ' * * * *';
  else
    -- Convert to cron: every N hours (limit to 23 hours max for step syntax)
    if v_seconds % 3600 != 0 then
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
    -- cron.schedule() defaults nodename to 'localhost' which forces TCP
    -- and fails when pg_hba.conf only allows socket connections.
    update cron.job set nodename = '' where jobid = v_sampler_job;

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

    update cron.job set nodename = '' where jobid = v_rotation_job;

    job_type := 'rotation';
    job_id := v_rotation_job;
    status := 'created';
    return next;
  end if;

  -- Update sample_interval in config (after all validation and job creation)
  update ash.config set sample_interval = p_interval where singleton;

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

-- Stop sampling: remove pg_cron jobs
create or replace function ash.stop()
returns table (job_type text, job_id bigint, status text)
language plpgsql
as $$
declare
  v_job_id bigint;
begin
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

  return;
end;
$$;

-- Status: update pg_cron_available hint
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

  metric := 'version'; value := coalesce(v_config.version, '1.0'); return next;
  metric := 'color'; value := case when ash._color_on() then 'on' else 'off' end; return next;
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
    metric := 'pg_cron_available'; value := 'no (use external scheduler)'; return next;
  end if;

  return;
end;
$$;

-- Re-create take_sample() to match fresh install (adds sqlstate to error message)
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
end;
$$;

-- Re-create rotate() to match fresh install (expanded advisory lock comment)
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
  -- Advisory lock prevents concurrent rotation from pg_cron overlap.
  -- rotate() is already REVOKE'd from PUBLIC — only schema owner can call it.
  -- A malicious user holding this advisory lock number can delay (not prevent)
  -- rotation, but they'd need direct DB access first.
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

-- Re-create top_waits() to use ash._bar() helper (matches fresh install)
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

-- Re-create top_waits_at() to use ash._bar() helper (matches fresh install)
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
      and s.sample_ts >= ash._to_sample_ts(p_start)
      and s.sample_ts < ash._to_sample_ts(p_end)
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

-- Re-create top_queries() to match fresh install (v_has_pgss variable name)
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

-- Fix config.version column default to match fresh install
alter table ash.config alter column version set default '1.3';

-- Fix sample.data CHECK constraint to match fresh install (>= 3 instead of >= 2)
-- The constraint name from ash-1.0.sql is auto-generated; drop and re-add.
do $$
begin
  -- Drop the old check constraint (from 1.0: >= 2)
  -- Constraint name is auto-generated, so find it dynamically.
  perform 1 from pg_constraint
    where conrelid = 'ash.sample'::regclass
      and contype = 'c'
      and pg_get_constraintdef(oid) like '%array_length(data, 1) >= 2%';
  if found then
    execute (
      select format('alter table ash.sample drop constraint %I',
             c.conname)
      from pg_constraint c
      where c.conrelid = 'ash.sample'::regclass
        and c.contype = 'c'
        and pg_get_constraintdef(c.oid) like '%array_length(data, 1) >= 2%'
      limit 1
    );
    alter table ash.sample add check (data[1] < 0 and array_length(data, 1) >= 3);
  end if;
end;
$$;

-- Update version
update ash.config set version = '1.3' where singleton;
