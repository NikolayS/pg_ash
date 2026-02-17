-- pg_ash: upgrade from 1.1 to 1.2
-- Safe to re-run (idempotent).
-- Changes: bar column on query_waits/top_by_type, event_queries(),
-- chart padding for psql alignment, version tracking.


-- Add version column if missing
do $$
begin
  if not exists (
    select from information_schema.columns
    where table_schema = 'ash' and table_name = 'config' and column_name = 'version'
  ) then
    alter table ash.config add column version text not null default '1.2';
  end if;
end $$;

-- Sampler observer-effect mitigation: add statement_timeout to pg_cron command.
-- statement_timeout inside plpgsql SET clause doesn't cancel sub-statements,
-- so we set it at session level in the cron command itself.
do $$
begin
  if exists (select from cron.job where jobname = 'ash_sampler') then
    update cron.job
    set command = 'set statement_timeout = ''500ms''; select ash.take_sample()'
    where jobname = 'ash_sampler';
  end if;
exception when others then
  null; -- pg_cron not installed, skip
end $$;

-- Session-level color toggle: set ash.color = on;
-- Avoids passing p_color := true to every function call.
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

create or replace function ash._reset(p_color boolean default false)
returns text
language sql
stable
as $$
  select case when ash._color_on(p_color) then E'\033[0m' else '' end;
$$;

-- Build a bar string with fixed visible width (for pspg/column alignment).
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
  select ash._wait_color(p_event, p_color)
    || rpad(
         repeat('█', greatest(1, (p_pct / nullif(p_max_pct, 0) * p_width)::int)),
         p_width
       )
    || ash._reset(p_color)
    || lpad(p_pct || '%', 8);
$$;

-- Drop + recreate functions with changed signatures or new features.
-- top_queries_with_text: rename mean_time_ms → mean_exec_time_ms, add total_exec_time_ms
drop function if exists ash.top_queries_with_text(interval, int);
-- timeline_chart: add chart padding for psql alignment
drop function if exists ash.timeline_chart(interval, interval, int, int, boolean);
drop function if exists ash.timeline_chart_at(timestamptz, timestamptz, interval, int, int, boolean);
-- query_waits, top_by_type: add bar column + p_width/p_color params
drop function if exists ash.query_waits(bigint, interval);
drop function if exists ash.query_waits_at(bigint, timestamptz, timestamptz);
drop function if exists ash.waits_by_type(interval);
drop function if exists ash.waits_by_type_at(timestamptz, timestamptz);
drop function if exists ash.top_by_type(interval);
drop function if exists ash.top_by_type_at(timestamptz, timestamptz);

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
      null::numeric as total_exec_time_ms,
      null::numeric as mean_exec_time_ms,
      null::text as query_text
    from resolved r
    cross join grand_total gt
    order by r.cnt desc
    limit p_limit;
  end if;
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
  v_visible_width int;
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
    v_visible_width := 0;

    -- Colored stacked bar for each top event (distinct char per rank)
    for v_i in 1..array_length(v_top_events, 1) loop
      v_val := coalesce((v_rec.events ->> v_top_events[v_i])::numeric, 0);
      v_ch := coalesce(v_event_chars[v_i], v_event_chars[array_length(v_event_chars, 1)]);
      if v_val > 0 then
        v_char_count := greatest(0, round(v_val / v_max_active * p_width)::int);
        if v_char_count > 0 then
          v_bar := v_bar || v_event_colors[v_i] || repeat(v_ch, v_char_count) || v_reset;
          v_visible_width := v_visible_width + v_char_count;
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
        v_visible_width := v_visible_width + v_char_count;
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
  v_visible_width int;
  v_legend_len int;
begin
  v_start_ts := ash._to_sample_ts(p_start);
  v_end_ts := ash._to_sample_ts(p_end);
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
    v_visible_width := 0;

    -- Colored stacked bar for each top event (distinct char per rank)
    for v_i in 1..array_length(v_top_events, 1) loop
      v_val := coalesce((v_rec.events ->> v_top_events[v_i])::numeric, 0);
      v_ch := coalesce(v_event_chars[v_i], v_event_chars[array_length(v_event_chars, 1)]);
      if v_val > 0 then
        v_char_count := greatest(0, round(v_val / v_max_active * p_width)::int);
        if v_char_count > 0 then
          v_bar := v_bar || v_event_colors[v_i] || repeat(v_ch, v_char_count) || v_reset;
          v_visible_width := v_visible_width + v_char_count;
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
        v_visible_width := v_visible_width + v_char_count;
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
        lateral generate_series(0, s.data[i + 1] - 1) gs(n)
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
        lateral generate_series(0, s.data[i + 1] - 1) gs(n)
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
      null::text as query_text
    from ranked r
    cross join max_pct mp
    order by r.samples desc;
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
  v_start int4 := ash._to_sample_ts(p_start);
  v_end int4 := ash._to_sample_ts(p_end);
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
        lateral generate_series(0, s.data[i + 1] - 1) gs(n)
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
        lateral generate_series(0, s.data[i + 1] - 1) gs(n)
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
      null::text as query_text
    from ranked r
    cross join max_pct mp
    order by r.samples desc;
  end if;
end;
$$;


-------------------------------------------------------------------------------
-- Re-create start() with statement_timeout in cron command
-------------------------------------------------------------------------------
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

  if string_to_array(v_cron_version, '.')::int[] < '{1,5}'::int[] then
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

  -- Update sample_interval in config
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
-- Re-create status() with color and version metrics
-------------------------------------------------------------------------------
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
    metric := 'pg_cron_available'; value := 'no'; return next;
  end if;

  return;
end;
$$;

-- Update version (must be last)
update ash.config set version = '1.2' where singleton;

-- Uninstall confirmation guard
drop function if exists ash.uninstall();
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

  for v_rec in select * from ash.stop() loop
    if v_rec.status = 'removed' then
      v_jobs_removed := v_jobs_removed + 1;
    end if;
  end loop;

  drop schema ash cascade;
  return format('uninstalled: removed %s pg_cron jobs, dropped ash schema', v_jobs_removed);
end;
$$;

-- Security: restrict write/admin functions to the installing role.
do $$
declare
  v_owner text := (select nspowner::regrole::text from pg_namespace where nspname = 'ash');
begin
  execute format('revoke all on function ash.start(interval) from public');
  execute format('revoke all on function ash.stop() from public');
  execute format('revoke all on function ash.uninstall(text) from public');
  execute format('revoke all on function ash.rotate() from public');
  execute format('revoke all on function ash.take_sample() from public');

  execute format('grant execute on function ash.start(interval) to %I', v_owner);
  execute format('grant execute on function ash.stop() to %I', v_owner);
  execute format('grant execute on function ash.uninstall(text) to %I', v_owner);
  execute format('grant execute on function ash.rotate() to %I', v_owner);
  execute format('grant execute on function ash.take_sample() to %I', v_owner);
end $$;
