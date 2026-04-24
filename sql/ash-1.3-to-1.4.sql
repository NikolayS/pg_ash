-- pg_ash: upgrade from 1.3 to 1.4
-- Safe to re-run (idempotent).
-- Changes:
--   - take_sample() catches statement_timeout (query_canceled) instead of
--     silently dropping the sample. Returns -1 on timeout. (#28)
--   - New missed_samples counter in ash.config — incremented on every
--     caught timeout, visible in ash.status(). (#27, #28)
--   - take_sample() inner exception handler now bumps an insert_errors
--     counter in ash.config instead of silently dropping data with just a
--     WARNING. (M-BUG-4)
--   - _register_wait() enforces a 32 000-row cap on wait_event_map to prevent
--     DoS via unbounded dictionary growth; bumps register_wait_cap_hits in
--     ash.config when the cap is hit. (M-BUG-6 / H-SEC-3)
--   - status() surfaces the two new counters, plus epoch_seconds_remaining
--     for the 2094 overflow horizon of sample_ts (int4). (#37)
--   - start() validates the interval shape before branching on pg_cron
--     availability so the accept/reject outcome is independent of pg_cron.
--     (H-BUG-1)
--   - start() re-syncs cron.job.schedule on re-invocation when the sampler
--     job already exists. (H-BUG-2)
--   - start() defends against malformed pg_cron extversion. (M-BUG-8)
--
-- NOTE on CLAUDE.md: "Upgrade scripts are generated at release time, not in
-- feature PRs." This script ships in a feature PR because test.yml runs the
-- full upgrade chain on every PR and enforces schema equivalence against
-- ash-install.sql — so the upgrade script must track install.sql changes to
-- keep CI green. Revisit the policy once there's a dedicated release process.

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

-- M-BUG-4: track rows silently dropped by take_sample()'s inner exception handler.
alter table ash.config add column if not exists insert_errors bigint not null default 0;

-- M-BUG-6 / H-SEC-3: track how often _register_wait hits the dictionary cap
-- and has to skip a new (state,type,event). Non-zero means wait-event
-- registrations are being silently dropped for that sample (those sessions
-- won't appear in encoded data for this tick). Surfaced by ash.status().
alter table ash.config add column if not exists register_wait_cap_hits bigint not null default 0;

-- Update version (both data and column default for schema parity)
update ash.config set version = '1.4' where singleton;
alter table ash.config alter column version set default '1.4';

-- Re-create _register_wait with dictionary size cap (M-BUG-6 / H-SEC-3)
create or replace function ash._register_wait(p_state text, p_type text, p_event text)
returns smallint
language plpgsql
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

-- Re-create take_sample with query_canceled handler and insert_errors counter
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

-- Re-create decode_sample with two-pass validation (M-BUG-9): previously a
-- malformed trailing segment produced partial rows followed by a WARNING;
-- now validation runs first and emits zero rows on failure.
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

-- Re-create start() with pre-branch validation (H-BUG-1), schedule re-sync
-- (H-BUG-2), and defensive extversion parsing (M-BUG-8).
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
    raise notice '  psql:           SELECT ash.take_sample() \watch 1';
    raise notice '  any language:   execute "SELECT ash.take_sample()" in a loop with sleep';
    raise notice 'Also schedule ash.rotate() at the rotation_period interval (default: daily).';

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

-- Re-create status() with missed_samples, insert_errors, register_wait_cap_hits
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
  -- M-BUG-4: surface the counter of rows dropped by take_sample()'s inner
  -- exception handler (CHECK violations and similar). Non-zero = silent
  -- data loss occurred — check server log for the matching WARNINGs.
  metric := 'insert_errors'; value := v_config.insert_errors::text; return next;
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
  -- M-BUG-6 / H-SEC-3: denominator tracks the 32 000 cap enforced in
  -- _register_wait (stays within smallint's 32 767 ceiling so we don't
  -- have to widen the id column / function signature).
  metric := 'wait_event_map_utilization'; value := round(v_wait_events::numeric / 32000 * 100, 2)::text || '%'; return next;
  metric := 'register_wait_cap_hits'; value := v_config.register_wait_cap_hits::text; return next;
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
