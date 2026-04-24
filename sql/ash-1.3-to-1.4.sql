-- pg_ash: upgrade from 1.3 to 1.4
-- Safe to re-run (idempotent).
-- Changes:
--   - take_sample() catches statement_timeout (query_canceled) instead of
--     silently dropping the sample. Returns -1 on timeout. (#28)
--   - New missed_samples counter in ash.config — incremented on every
--     caught timeout, visible in ash.status(). (#27, #28)
--   - status() surfaces epoch_seconds_remaining for the 2094 overflow
--     horizon of sample_ts (int4). (#37)

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
  -- TODO: verify via EXPLAIN that the uncorrelated probe subquery is
  -- hoisted to InitPlan (executed once) rather than re-evaluated per row.
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

-- ash.start() is re-created below after ash.status() — single consolidated
-- definition that mirrors sql/ash-install.sql byte-for-byte (required by
-- schema-equivalence CI which diffs md5(prosrc) between fresh install and
-- the full upgrade chain).

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
-- (PR #42 / issue #37 perf: L-BUG-12). The dynamic alter-function loop at
-- the end of this file re-applies `set search_path = pg_catalog, ash` on
-- this (and every other) function in ash.* to match ash-install.sql.
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

-- Re-create ash.start() with pg_read_all_stats privilege notice and
-- debug_logging traces. This body MUST match ash-install.sql byte-for-byte
-- (schema-equivalence CI diffs md5(prosrc) between fresh install and
-- upgrade chain).
--
-- The dynamic alter-function loop at the end of this file re-applies
-- `set search_path = pg_catalog, ash` on this function, matching
-- ash-install.sql's inline `SET search_path` clause.
--
-- Changes vs 1.3:
--   - Privilege probe (pg_read_all_stats / superuser) — #40
--   - debug_logging-gated RAISE LOG lines — PR #44
--   - search_path hardening (applied via alter-function loop) — PR #45
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
    raise notice '  psql:           SELECT ash.take_sample() \watch 1';
    raise notice '  any language:   execute "SELECT ash.take_sample()" in a loop with sleep';
    raise notice 'Also schedule ash.rotate() at the rotation_period interval (default: daily).';

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

-------------------------------------------------------------------------------
-- Privilege hardening (mirrors ash-install.sql changes in PR #45, issue #37).
-- Applies search_path = pg_catalog, ash to every ash.* function and revokes
-- SELECT/EXECUTE from PUBLIC on reader functions, underlying tables, and
-- per-slot partitions. Dynamic so it covers functions/partitions created by
-- earlier upgrade scripts. All statements are idempotent.
--
-- IMPORTANT: the v_pgss_readers list below must stay in sync with the same
-- list inside ash._apply_pgss_search_path() in ash-install.sql. If you add or
-- remove a function that probes pg_stat_statements, update both places.
-------------------------------------------------------------------------------

-- Helper: detect the schema that holds the pg_stat_statements view so reader
-- functions keep working when pgss lives outside `public` (self-hosted
-- installs, schema-isolated setups). Mirrors ash-install.sql.
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
    'event_queries', 'event_queries_at'
  ];
  v_path text;
  r record;
begin
  -- Always keep public in the path as a fallback (matches the managed-service
  -- default and preserves behavior when pgss is not yet installed). If the
  -- extension lives elsewhere, append that schema after public so ash.* still
  -- wins over any user-created objects in public/<pgss_schema>.
  if v_pgss_schema is null or v_pgss_schema in ('pg_catalog', 'ash', 'public') then
    v_path := 'pg_catalog, ash, public';
  else
    v_path := format('pg_catalog, ash, public, %I', v_pgss_schema);
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

do $$
declare
  r record;
  v_pgss_schema text := ash._pgss_schema();
  v_pgss_path text;
  -- Functions that reference pg_stat_statements unqualified need public (or
  -- the detected pgss schema) on the search_path so the view is visible.
  -- Without this, the probe "perform 1 from pg_stat_statements" fails with
  -- "relation does not exist", the catch-block sets v_has_pgss=false, and the
  -- caller either raises "pg_stat_statements extension is not installed"
  -- (top_queries_with_text, event_queries, event_queries_at) or silently
  -- degrades with a misleading warning (top_queries, top_queries_at, samples,
  -- samples_at). Keep public last so ash.* still wins for ash objects.
  -- NOTE: keep this list in sync with v_readers in ash._apply_pgss_search_path()
  -- (both in this file and in ash-install.sql).
  v_pgss_readers text[] := array[
    'top_queries', 'top_queries_at', 'top_queries_with_text',
    'samples', 'samples_at',
    'event_queries', 'event_queries_at'
  ];
begin
  if v_pgss_schema is null or v_pgss_schema in ('pg_catalog', 'ash', 'public') then
    v_pgss_path := 'pg_catalog, ash, public';
  else
    v_pgss_path := format('pg_catalog, ash, public, %I', v_pgss_schema);
  end if;

  -- Apply search_path guard to every non-trigger function in ash.*.
  -- alter function ... set search_path is idempotent.
  for r in
    select p.proname,
           pg_catalog.pg_get_function_identity_arguments(p.oid) as args
    from pg_catalog.pg_proc p
    join pg_catalog.pg_namespace n on p.pronamespace = n.oid
    where n.nspname = 'ash'
      and p.prokind = 'f'
  loop
    if r.proname = any(v_pgss_readers) then
      execute format('alter function ash.%I(%s) set search_path = %s',
                     r.proname, r.args, v_pgss_path);
    else
      execute format('alter function ash.%I(%s) set search_path = pg_catalog, ash',
                     r.proname, r.args);
    end if;
  end loop;
end $$;

do $$
declare
  v_owner text := (select nspowner::regrole::text from pg_namespace where nspname = 'ash');
  r record;
begin
  -- Admin functions: only the schema owner
  execute format('revoke all on function ash.start(interval) from public');
  execute format('revoke all on function ash.stop() from public');
  execute format('revoke all on function ash.uninstall(text) from public');
  execute format('revoke all on function ash.rotate() from public');
  execute format('revoke all on function ash.take_sample() from public');
  execute format('revoke all on function ash.set_debug_logging(bool) from public');

  execute format('grant execute on function ash.start(interval) to %I', v_owner);
  execute format('grant execute on function ash.stop() to %I', v_owner);
  execute format('grant execute on function ash.uninstall(text) to %I', v_owner);
  execute format('grant execute on function ash.rotate() to %I', v_owner);
  execute format('grant execute on function ash.take_sample() to %I', v_owner);
  execute format('grant execute on function ash.set_debug_logging(bool) to %I', v_owner);

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

  -- Reader tables/views: revoke SELECT from PUBLIC for objects holding
  -- sample data, query text, and configuration. REVOKE on a partitioned
  -- parent does not cascade to partitions in PostgreSQL, so sample_N and
  -- query_map_N are enumerated dynamically below.
  execute 'revoke select on table ash.sample from public';
  execute 'revoke select on table ash.query_map_all from public';
  execute 'revoke select on table ash.config from public';
  execute 'revoke select on table ash.wait_event_map from public';

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
