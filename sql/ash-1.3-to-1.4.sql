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
  -- Existence probe at the 50000th row: one index lookup, and — unlike
  -- pg_class.reltuples — immediately accurate after TRUNCATE (reltuples
  -- can remain stale or be -1 until autovacuum/ANALYZE catches up).
  if v_current_slot = 0 then
    insert into ash.query_map_0 (query_id)
    select distinct sa.query_id
    from pg_stat_activity sa
    where sa.query_id is not null
      and sa.state in ('active', 'idle in transaction', 'idle in transaction (aborted)')
      and (sa.backend_type = 'client backend'
       or (v_include_bg and sa.backend_type in ('autovacuum worker', 'logical replication worker', 'parallel worker', 'background worker')))
      and sa.pid <> pg_backend_pid()
      and not exists (select 1 from ash.query_map_0 offset 49999 limit 1)
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
      and not exists (select 1 from ash.query_map_1 offset 49999 limit 1)
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
      and not exists (select 1 from ash.query_map_2 offset 49999 limit 1)
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

-- Re-create rotate() with single-statement TRUNCATE ... RESTART IDENTITY
-- (PR #42 / issue #37 perf: L-BUG-12).
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
    -- Single statement with RESTART IDENTITY: one AccessExclusiveLock
    -- acquisition per slot, and resets the query_map_N identity sequence
    -- atomically (sample_N has no identity column, so it is unaffected).
    case v_truncate_slot
      when 0 then
        truncate ash.sample_0, ash.query_map_0 restart identity;
      when 1 then
        truncate ash.sample_1, ash.query_map_1 restart identity;
      when 2 then
        truncate ash.sample_2, ash.query_map_2 restart identity;
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

-- Re-create query_waits() with O(N) running-group window (PR #42 / M-BUG-7).
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
  samples as (
    select s.ctid, s.slot, s.data
    from ash.sample s
    where s.slot = any(ash._active_slots())
      and s.sample_ts >= extract(epoch from now() - p_interval - ash.epoch())::int4
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

-- Re-create query_waits_at() with the same O(N) rewrite (PR #42 / M-BUG-7).
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
  samples as (
    select s.ctid, s.slot, s.data
    from ash.sample s
    where s.slot = any(ash._active_slots())
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

