/*
 * lib/shape.sql — numeric shape assertions for the seeded window.
 *
 * "No errors" is not a quality bar. A demo can run clean and still be boring,
 * empty, or quietly wrong: rollups that refused to re-roll show as empty
 * buckets, a badly chosen calm phase shows as a lock storm, a restamp bug
 * shows as a 10x AAS. Every one of those ships silently unless something
 * asserts on the NUMBERS.
 *
 * Run by lib/seed.sh immediately after the rollups. Any RAISE here becomes
 * exit 4. Called with psql -v base_ts=... -v vmin=... etc.
 *
 * psql does not interpolate its variables inside dollar-quoted bodies, so the
 * values are handed to the DO block through session GUCs.
 */

\set ON_ERROR_STOP on

select
  set_config('ash_demo.base_ts',     :'base_ts',     false) as base_ts,
  set_config('ash_demo.vmin',        :'vmin',        false) as vmin,
  set_config('ash_demo.vmin_slack',  :'vmin_slack',  false) as vmin_slack,
  set_config('ash_demo.vmin_total',  :'vmin_total',  false) as vmin_total,
  set_config('ash_demo.ph_baseline', :'ph_baseline', false) as ph_baseline,
  set_config('ash_demo.ph_storm',    :'ph_storm',    false) as ph_storm
\gset shape_

do $shape$
declare
  v_base_ts      int4 := current_setting('ash_demo.base_ts')::int4;
  v_vmin         int  := current_setting('ash_demo.vmin')::int;
  v_slack        int  := current_setting('ash_demo.vmin_slack')::int;
  v_total        int  := current_setting('ash_demo.vmin_total')::int;
  v_ph_baseline  int  := current_setting('ash_demo.ph_baseline')::int;
  v_ph_storm     int  := current_setting('ash_demo.ph_storm')::int;

  v_since        timestamptz := ash.ts_to_timestamptz(v_base_ts + v_slack * 60);
  v_until        timestamptz := ash.ts_to_timestamptz(v_base_ts + v_total * 60);
  v_base_since   timestamptz := v_since;
  v_base_until   timestamptz := ash.ts_to_timestamptz(v_base_ts + v_ph_baseline * 60);
  v_storm_since  timestamptz := v_base_until;
  v_storm_until  timestamptz := ash.ts_to_timestamptz(
                                  v_base_ts + (v_ph_baseline + v_ph_storm) * 60);

  v_minutes      int;
  v_types        int;
  v_storm_peak   numeric;
  v_calm_median  numeric;
  v_top_event    text;
  v_top_pct      numeric;
  v_calm_event   text;
  v_query_key    text;
  v_query_pct    numeric;
  v_periods      int;
  v_periods_null int;
  v_gaps         int;
  v_chart_locks  int;
  v_rec          record;
begin
  ------------------------------------------------------------------------
  -- 1. Enough populated virtual minutes.
  ------------------------------------------------------------------------
  select count(distinct sample_row.sample_ts / 60)
  into v_minutes
  from ash.sample as sample_row
  where sample_row.sample_ts >= v_base_ts
    and sample_row.sample_ts <  v_base_ts + v_total * 60;

  if v_minutes < v_total then
    raise exception
      'shape: only % of % virtual minutes carry samples', v_minutes, v_total;
  end if;

  ------------------------------------------------------------------------
  -- 2. A believable variety of waits. One wait type is a monoculture, not a
  --    production system.
  ------------------------------------------------------------------------
  select count(*)
  into v_types
  from ash.top('wait_event_type', v_since, v_until, n => 100) as top_row;

  if v_types < 4 then
    raise exception
      'shape: only % distinct wait event types in the window (want >= 4)',
      v_types;
  end if;

  ------------------------------------------------------------------------
  -- 3. The spike is a spike. Storm peak AAS must be at least 3x the median
  --    calm minute, otherwise nobody looking at the chart sees an incident.
  ------------------------------------------------------------------------
  select aas_row.peak_aas
  into v_storm_peak
  from ash.aas(v_storm_since, v_storm_until) as aas_row;

  select percentile_cont(0.5) within group (order by tl.avg_aas)
  into v_calm_median
  from ash.timeline(v_base_since, v_base_until, interval '1 minute') as tl;

  if v_storm_peak is null or v_calm_median is null then
    raise exception 'shape: could not measure storm peak (%) or calm median (%)',
      v_storm_peak, v_calm_median;
  end if;

  if v_storm_peak < 3 * v_calm_median then
    raise exception
      'shape: storm peak_aas % is not 3x the median calm minute % — '
      'the incident will not read as an incident',
      v_storm_peak, v_calm_median;
  end if;

  ------------------------------------------------------------------------
  -- 4. The storm is a LOCK storm, and it is dominant, and it has a single
  --    identifiable guilty statement to drill into.
  ------------------------------------------------------------------------
  select top_row.key, top_row.pct
  into v_top_event, v_top_pct
  from ash.top('wait_event', v_storm_since, v_storm_until, n => 1) as top_row;

  if v_top_event is null or v_top_event not like 'Lock:%' then
    raise exception
      'shape: the storm window''s rank-1 wait event is "%" — expected a Lock:* '
      'event. The workload is not producing row contention.', v_top_event;
  end if;

  if v_top_pct < 35 then
    raise exception
      'shape: rank-1 storm wait % holds only % of the window — not dominant',
      v_top_event, v_top_pct || '%';
  end if;

  select top_row.key, top_row.pct
  into v_query_key, v_query_pct
  from ash.top('query_id', v_storm_since, v_storm_until,
               wait_event => v_top_event, n => 1) as top_row;

  if v_query_key is null then
    raise exception
      'shape: no query id is attributable to % in the storm window — '
      'the wait<->query drill has nowhere to land', v_top_event;
  end if;

  if v_query_pct < 50 then
    raise exception
      'shape: the top query for % holds only % of that wait — no single '
      'guilty statement', v_top_event, v_query_pct || '%';
  end if;

  ------------------------------------------------------------------------
  -- 5. Calm actually looks calm: the baseline window must NOT be led by a
  --    lock wait. (Default TPC-B at low scale fails this, loudly, which is
  --    the whole point of the check.)
  ------------------------------------------------------------------------
  select top_row.key
  into v_calm_event
  from ash.top('wait_event', v_base_since, v_base_until, n => 1) as top_row;

  if v_calm_event like 'Lock:%' then
    raise exception
      'shape: the calm baseline is led by % — calm must look calm', v_calm_event;
  end if;

  ------------------------------------------------------------------------
  -- 6. ash.periods() returns all six rows with a real number in each.
  --    This is what proves the rollup chain (and its watermarks) is intact:
  --    the 1h/1d/1w/1mo rows can only be answered from rollup_1h.
  ------------------------------------------------------------------------
  select count(*), count(*) filter (where period_row.avg_aas is null)
  into v_periods, v_periods_null
  from ash.periods(v_until) as period_row;

  if v_periods <> 6 then
    raise exception 'shape: ash.periods() returned % rows, expected 6', v_periods;
  end if;

  if v_periods_null > 0 then
    raise exception
      'shape: % of 6 ash.periods() rows have a NULL avg_aas — the rollups did '
      'not populate (did the rollup watermarks get reset?)', v_periods_null;
  end if;

  ------------------------------------------------------------------------
  -- 7. The FLAGSHIP CHART is legible.
  --
  --    ash.chart() ranks its series by total AAS across the whole window and
  --    draws the top n; everything else collapses into a single "Other" dot
  --    column. The incident lasts five of twenty-eight minutes, so if the calm
  --    phases are heavy enough, both Lock waits fall out of the top n and the
  --    hero image renders the storm as an anonymous row of dots.
  --
  --    That is not an error, it is not empty, and no other assertion here
  --    notices it — it is just a bad picture, which is exactly the class of
  --    defect this harness exists to make impossible. So: assert that at least
  --    two Lock:* events survive into the top four of the full window, which is
  --    what scenes.tsv's `n => 4` draws.
  ------------------------------------------------------------------------
  select count(*)
  into v_chart_locks
  from ash.top('wait_event', v_since, v_until, n => 4) as top_row
  where top_row.key like 'Lock:%';

  if v_chart_locks < 2 then
    raise exception
      'shape: only % Lock:* event(s) rank in the top 4 of the chart window — '
      'the calm phases are drowning the incident, and ash.chart(n => 4) will '
      'draw the storm as "Other". Lower ASH_LOAD_BASELINE_CLIENTS / '
      'ASH_LOAD_READIO_CLIENTS.', v_chart_locks;
  end if;

  ------------------------------------------------------------------------
  -- 8. No holes: every minute bucket in [since, until) carries data.
  ------------------------------------------------------------------------
  select count(*)
  into v_gaps
  from ash.timeline(v_since, v_until, interval '1 minute') as tl
  where tl.data_points = 0 or tl.avg_aas is null or tl.avg_aas = 0;

  if v_gaps > 0 then
    raise exception 'shape: % empty minute bucket(s) inside the window', v_gaps;
  end if;

  ------------------------------------------------------------------------
  -- Report. Printed on success so a human running `make seed` can see the
  -- story in numbers before a single pixel is rendered.
  ------------------------------------------------------------------------
  raise notice 'shape ok  window % .. %', v_since, v_until;
  raise notice 'shape ok  % virtual minutes, % wait event types', v_minutes, v_types;
  raise notice 'shape ok  calm median AAS %  ->  storm peak AAS %  (%x)',
    round(v_calm_median, 2), round(v_storm_peak, 2),
    round(v_storm_peak / nullif(v_calm_median, 0), 1);
  -- RAISE has one placeholder and no width/format specifiers; the percent
  -- signs are concatenated into the arguments rather than escaped in the
  -- format string, which is both shorter and impossible to get backwards.
  raise notice 'shape ok  storm rank-1 wait % (%), guilty query % (% of that wait)',
    v_top_event, v_top_pct || '%', v_query_key, v_query_pct || '%';
  raise notice 'shape ok  calm rank-1 wait %', v_calm_event;
  raise notice 'shape ok  chart top-4 over the window carries % Lock:* series',
    v_chart_locks;
  for v_rec in
    select period_row.period, period_row.source, period_row.avg_aas,
           period_row.peak_aas
    from ash.periods(v_until) as period_row
  loop
    -- RAISE has exactly one placeholder, `%`; rpad() does the column padding.
    raise notice 'shape ok  periods % source=% avg=% peak=%',
      rpad(v_rec.period, 4), rpad(v_rec.source, 10),
      v_rec.avg_aas, v_rec.peak_aas;
  end loop;
end
$shape$;
