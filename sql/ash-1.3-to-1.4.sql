-- pg_ash: upgrade from 1.3 to 1.4
-- Safe to re-run (idempotent).
-- Changes:
--   - take_sample() catches statement_timeout (query_canceled) instead of
--     silently dropping the sample. Returns -1 on timeout. (#28)
--   - New missed_samples counter in ash.config — incremented on every
--     caught timeout, visible in ash.status(). (#27, #28)

-- Add missed_samples column to config if missing
do $$
begin
  if not exists (
    select from information_schema.columns
    where table_schema = 'ash' and table_name = 'config' and column_name = 'missed_samples'
  ) then
    alter table ash.config
      add column missed_samples bigint not null default 0;
  end if;
end $$;

-- Update version (both data and column default for schema parity)
update ash.config set version = '1.4' where singleton;
alter table ash.config alter column version set default '1.4';

-- Re-create take_sample with query_canceled handler
create or replace function ash.take_sample()
returns int
language plpgsql
as $$
declare
  v_sample_ts int4;
  v_include_bg bool;
  v_debug_logging bool;
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
  -- Get sample timestamp (seconds since epoch, from now())
  v_sample_ts := extract(epoch from now() - ash.epoch())::int4;

  -- Get config
  select include_bg_workers, debug_logging
  into v_include_bg, v_debug_logging
  from ash.config where singleton;
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

-- Re-create status() with missed_samples metric
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
  metric := 'debug_logging'; value := v_config.debug_logging::text; return next;
  metric := 'missed_samples'; value := v_config.missed_samples::text; return next;
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


-- =========================================================================
-- Reader UX (#37) — mirror ash-install.sql changes.
-- =========================================================================

-- Drop dead helper (was never called; removed in install.sql).
drop function if exists ash._validate_data(integer[]);

-- Helper used by reader functions that accept a user-supplied interval.
-- Returns the same array as ash._active_slots(), but also emits a single
-- NOTICE per transaction when p_interval exceeds 2 * rotation_period — so
-- callers get a clear signal instead of a silent empty set. Never clamps
-- the caller's interval — the returned row set stays honest.
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
as $$
declare
  v_current_slot smallint;
  v_rotation_period interval;
  v_already text;
begin
  select current_slot, rotation_period
    into v_current_slot, v_rotation_period
  from ash.config
  where singleton;

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
  end if;

  return array[
    v_current_slot,
    ((v_current_slot - 1 + 3) % 3)::smallint
  ];
end;
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
      and s.slot = any(ash._active_slots_for(p_interval))
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
    where s.slot = any(ash._active_slots_for(p_interval))
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
      where s.slot = any(ash._active_slots_for(p_interval))
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
      where s.slot = any(ash._active_slots_for(p_interval))
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
    where s.slot = any(ash._active_slots_for(p_interval))
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
      where s.slot = any(ash._active_slots_for(p_interval))
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
    where s.slot = any(ash._active_slots_for(p_interval))
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
  where s.slot = any(ash._active_slots_for(p_interval))
    and s.sample_ts >= extract(epoch from now() - p_interval - ash.epoch())::int4
  group by s.datid, d.datname
  order by total_backends desc
$$;

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
