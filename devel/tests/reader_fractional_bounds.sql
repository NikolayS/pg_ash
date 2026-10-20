\set ON_ERROR_STOP on

-- Real minute-rollup readers must floor timestamps before int4 rounding.
-- Keep neighboring minutes distinct so shifted windows change the results.
begin;
set local time zone 'UTC';
truncate ash.sample, ash.rollup_1m, ash.rollup_1h;
update ash.config
set sample_interval = interval '1 second', current_slot = 0
where singleton;

do $$
declare
  v_from timestamptz := date_trunc('minute', now()) - interval '10 minutes';
  v_until timestamptz := v_from + interval '2 minutes';
  v_wait smallint := ash._register_wait('active', 'CPU*', 'CPU*');
  v_query int4;
  v_offset interval;
  v_bounds record;
  v_reader text;
  v_expected jsonb;
  v_actual jsonb;
  v_report jsonb;
  v_aas record;
  v_compare record;
  v_count bigint;
begin
  insert into ash.rollup_1m (
    ts, datid, samples, peak_backends, wait_counts, query_counts
  )
  select ash.ts_from_timestamptz(v_from) + minute_no * 60,
    0::oid, 1, activity, array[v_wait, activity], '{}'::bigint[]
  from (values (0, 60), (1, 120), (2, 600), (3, 1200), (4, 2400))
    as fixture(minute_no, activity);
  insert into ash.query_map_0 (query_id) values (424243)
    on conflict (query_id) do update set query_id = excluded.query_id
    returning id into v_query;
  -- The oldest raw sample is in the second half of the extreme minute.
  -- Session-local truncation would move its attribution cutoff past that minute.
  insert into ash.sample (sample_ts, datid, active_count, data, slot)
  values (ash.ts_from_timestamptz(v_from) + 100, 0::oid, 120,
    array[-v_wait, 120] || array_fill(v_query, array[120]), 0);

  select to_jsonb(a) into v_expected from ash.aas(v_from, v_until) as a;
  assert v_expected ->> 'source' = 'rollup_1m';
  assert (v_expected ->> 'backend_seconds')::numeric = 180;
  v_report := ash.report(v_from, v_until);
  assert (v_report #>> '{aas_avg,total}')::numeric = 1.5;
  assert (v_report #>> '{coverage,minutes_expected}')::int = 2;
  assert (v_report #> '{top_queryids_worst1m,total}') ? '424243(2.0)',
    'control must attribute the extreme minute to its retained raw query';

  -- UTC grain boundaries must not shift under a second-resolution offset.
  set local time zone interval '30 seconds';
  select * into v_aas from ash.aas(
    v_from + interval '20 seconds', v_until + interval '20 seconds');
  assert v_aas.backend_seconds = 180 and v_aas.avg_aas = 1.5
    and v_aas.period_start = v_from and v_aas.period_end = v_until,
    format('session time zone shifted UTC reader bounds: %s', v_aas);
  foreach v_offset in array array[interval '20 seconds', interval '40 seconds']
  loop
    v_actual := ash.report(v_from + v_offset, v_until + v_offset);
    assert v_actual is not null, 'session time zone removed report coverage';
    assert (v_actual #>> '{coverage,from}')::timestamptz = v_from
      and (v_actual #>> '{coverage,to}')::timestamptz = v_until,
      format('session time zone shifted report bounds: %s', v_actual);
    assert (v_actual -> 'aas_avg') is not distinct from (v_report -> 'aas_avg')
      and (v_actual -> 'top_queryids_worst1m')
        is not distinct from (v_report -> 'top_queryids_worst1m'),
      format('session time zone changed report metrics or attribution: %s', v_actual);
  end loop;
  set local time zone 'UTC';

  -- Below the rounding boundary, at the boundary, and just before rollover.
  foreach v_offset in array array[
    interval '59.499 seconds', interval '59.5 seconds', interval '59.999 seconds'
  ] loop
    for v_bounds in
      select * from (values
        (v_from + v_offset, v_until),
        (v_from, v_until + v_offset),
        (v_from + v_offset, v_until + v_offset)
      ) as bounds(since, until)
    loop
      -- report already floors in timestamp space: retain it as a control.
      assert ash.report(v_bounds.since, v_bounds.until) = v_report,
        'report fractional-bound control changed';
      foreach v_reader in array array[
        'ash.aas(%1$L, %2$L)',
        'ash.timeline(%1$L, %2$L, ''1 minute'')',
        'ash.top(''wait_event_type'', %1$L, %2$L)',
        'ash.chart(%1$L, %2$L)',
        'ash.compare(%1$L, %2$L, %1$L, %2$L)',
        'ash.compare(%1$L, %2$L, %1$L, %2$L, ''wait_event_type'')'
      ] loop
        execute 'select jsonb_agg(to_jsonb(r)) from ' ||
          format(v_reader, v_from, v_until) || ' as r' into v_expected;
        execute 'select jsonb_agg(to_jsonb(r)) from ' ||
          format(v_reader, v_bounds.since, v_bounds.until) || ' as r'
          into v_actual;
        assert v_expected is not null, format('empty control: %s', v_reader);
        assert v_actual = v_expected,
          format('fractional bounds changed %s at %s: expected %s, got %s',
            v_reader, v_bounds, v_expected, v_actual);
      end loop;
    end loop;
  end loop;

  -- The epoch conversion clamps extreme dates; keep the clamped seconds
  -- minute-aligned too, rather than producing a zero-span post-horizon read.
  truncate ash.sample, ash.rollup_1m;
  for v_bounds in
    select * from (values
      ('1000-01-01'::timestamptz, '1000-01-02'::timestamptz),
      ('3000-01-01'::timestamptz, '3000-01-02'::timestamptz)
    ) as bounds(since, until)
  loop
    assert ash.report(v_bounds.since, v_bounds.until) is null,
      'extreme-date report must preserve uncovered NULL semantics';
    select * into v_aas from ash.aas(v_bounds.since, v_bounds.until);
    assert v_aas.source = 'none' and v_aas.avg_aas = 0
      and v_aas.backend_seconds = 0 and v_aas.buckets_with_data = 0
      and v_aas.period_end > v_aas.period_start,
      format('extreme-date aas must stay empty with positive span: %s', v_aas);
    select count(*) into v_count
    from ash.timeline(v_bounds.since, v_bounds.until) as t
    where t.data_points <> 0;
    assert v_count = 0, 'extreme-date timeline must have no observed data';
    select count(*) into v_count
    from ash.top('wait_event_type', v_bounds.since, v_bounds.until);
    assert v_count = 0, 'extreme-date top must be empty';
    select count(*) into v_count
    from ash.chart(v_bounds.since, v_bounds.until);
    assert v_count = 0, 'extreme-date chart must be empty';
    select * into v_compare from ash.compare(
      v_bounds.since, v_bounds.until, v_bounds.since, v_bounds.until);
    assert v_compare.avg_aas_1 is null and v_compare.avg_aas_2 is null
      and v_compare.avg_delta is null,
      'extreme-date compare must preserve uncovered NULL semantics';
    select count(*) into v_count from ash.compare(
      v_bounds.since, v_bounds.until, v_bounds.since, v_bounds.until,
      'wait_event_type');
    assert v_count = 0, 'extreme-date dimensional compare must be empty';
  end loop;
  raise notice 'reader fractional bounds PASSED';
end $$;
rollback;
