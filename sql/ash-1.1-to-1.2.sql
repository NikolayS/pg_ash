-- pg_ash: upgrade from 1.1 to 1.2
-- Safe to re-run (idempotent).

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

-- Update version
update ash.config set version = '1.2' where singleton;

-- Drop old signatures (return type changed — can't use CREATE OR REPLACE).
-- timeline_chart: swap detail/chart column order
drop function if exists ash.timeline_chart(interval, interval, int, int, boolean);
drop function if exists ash.timeline_chart_at(timestamptz, timestamptz, interval, int, int, boolean);
-- query_waits, waits_by_type: add bar column + p_width/p_color params
drop function if exists ash.query_waits(bigint, interval);
drop function if exists ash.query_waits_at(bigint, timestamptz, timestamptz);
drop function if exists ash.waits_by_type(interval);
drop function if exists ash.waits_by_type_at(timestamptz, timestamptz);

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
  chart text,
  detail text
)
language plpgsql
stable
set jit = off
as $$
declare
  v_reset text := case when p_color then E'\033[0m' else '' end;
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
  v_other_color text := case when p_color then E'\033[38;2;180;180;180m' else '' end;  -- gray for Other
  v_other_char text := '·';
  v_ch text;
  v_i int;
  v_visible_width int;
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
      ash.epoch() + pb.bucket_ts * interval '1 second' as ts,
      pb.total,
      pb.events
    from per_bucket pb
    order by pb.bucket_ts
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

    -- Pad to fixed visual width so psql column alignment works with ANSI codes
    if v_visible_width < p_width then
      v_bar := v_bar || repeat(' ', p_width - v_visible_width);
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
  chart text,
  detail text
)
language plpgsql
stable
set jit = off
as $$
declare
  v_reset text := case when p_color then E'\033[0m' else '' end;
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
  v_other_color text := case when p_color then E'\033[38;2;180;180;180m' else '' end;  -- gray for Other
  v_other_char text := '·';
  v_ch text;
  v_i int;
  v_visible_width int;
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
      ash.epoch() + pb.bucket_ts * interval '1 second' as ts,
      pb.total,
      pb.events
    from per_bucket pb
    order by pb.bucket_ts
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

    -- Pad to fixed visual width so psql column alignment works with ANSI codes
    if v_visible_width < p_width then
      v_bar := v_bar || repeat(' ', p_width - v_visible_width);
    end if;

    bucket_start := v_rec.ts;
    active := v_rec.total;
    detail := ltrim(v_legend);
    chart := v_bar;
    return next;
  end loop;
end;
$$;
create or replace function ash.waits_by_type(
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
    ash._wait_color(t.wait_type || ':*', p_color)
      || repeat('█', greatest(1, (round(t.cnt::numeric / gt.total * 100, 2) / nullif(mp.m, 0) * p_width)::int))
      || ash._reset(p_color) || ' ' || round(t.cnt::numeric / gt.total * 100, 2) || '%' as bar
  from totals t, grand_total gt, max_pct mp
  order by t.cnt desc
$$;

create or replace function ash.waits_by_type_at(
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
    ash._wait_color(t.wait_type || ':*', p_color)
      || repeat('█', greatest(1, (round(t.cnt::numeric / gt.total * 100, 2) / nullif(mp.m, 0) * p_width)::int))
      || ash._reset(p_color) || ' ' || round(t.cnt::numeric / gt.total * 100, 2) || '%'
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
    ash._wait_color(t.evt, p_color)
      || repeat('█', greatest(1, (round(t.cnt::numeric / gt.total * 100, 2) / nullif(mp.m, 0) * p_width)::int))
      || ash._reset(p_color) || ' ' || round(t.cnt::numeric / gt.total * 100, 2) || '%' as bar
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
    ash._wait_color(t.evt, p_color)
      || repeat('█', greatest(1, (round(t.cnt::numeric / gt.total * 100, 2) / nullif(mp.m, 0) * p_width)::int))
      || ash._reset(p_color) || ' ' || round(t.cnt::numeric / gt.total * 100, 2) || '%'
  from totals t
  cross join grand_total gt
  cross join max_pct mp
  order by t.cnt desc;
end;
$$;

update ash.config set version = '1.2' where singleton;

-------------------------------------------------------------------------------
-- Event queries — top query_ids for a specific wait event
-------------------------------------------------------------------------------

create or replace function ash.event_queries(
  p_event text,
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
  v_min_ts int4;
begin
  v_min_ts := extract(epoch from now() - p_interval - ash.epoch())::int4;

  begin
    perform 1 from pg_stat_statements limit 1;
    v_has_pgss := true;
  exception when others then
    v_has_pgss := false;
  end;

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
      s.data[i + 1] as cnt,
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
  )
  select
    t.query_id,
    t.cnt as samples,
    round(t.cnt::numeric / gt.total * 100, 2) as pct,
    case when v_has_pgss then (
      select left(p.query, 200)
      from pg_stat_statements p
      where p.queryid = t.query_id
      limit 1
    ) end as query_text
  from totals t
  cross join grand_total gt
  order by t.cnt desc
  limit p_limit;
end;
$$;

create or replace function ash.event_queries_at(
  p_event text,
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
  begin
    perform 1 from pg_stat_statements limit 1;
    v_has_pgss := true;
  exception when others then
    v_has_pgss := false;
  end;

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
      s.data[i + 1] as cnt,
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
  )
  select
    t.query_id,
    t.cnt as samples,
    round(t.cnt::numeric / gt.total * 100, 2) as pct,
    case when v_has_pgss then (
      select left(p.query, 200)
      from pg_stat_statements p
      where p.queryid = t.query_id
      limit 1
    ) end as query_text
  from totals t
  cross join grand_total gt
  order by t.cnt desc
  limit p_limit;
end;
$$;
