\set ON_ERROR_STOP on

-- Real minute-rollup readers must floor timestamps before int4 rounding.
-- Keep neighboring minutes distinct so shifted windows change the results.
begin;
truncate ash.sample, ash.rollup_1m, ash.rollup_1h;
update ash.config set sample_interval = interval '1 second' where singleton;

do $$
declare
  v_from timestamptz := date_trunc('minute', now()) - interval '10 minutes';
  v_until timestamptz := v_from + interval '2 minutes';
  v_wait smallint := ash._register_wait('active', 'CPU*', 'CPU*');
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

  select to_jsonb(a) into v_expected from ash.aas(v_from, v_until) as a;
  assert v_expected ->> 'source' = 'rollup_1m';
  assert (v_expected ->> 'backend_seconds')::numeric = 180;
  v_report := ash.report(v_from, v_until);
  assert (v_report #>> '{aas_avg,total}')::numeric = 1.5;
  assert (v_report #>> '{coverage,minutes_expected}')::int = 2;

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
  truncate ash.rollup_1m;
  for v_bounds in
    select * from (values
      ('1000-01-01'::timestamptz, '1000-01-02'::timestamptz),
      ('3000-01-01'::timestamptz, '3000-01-02'::timestamptz)
    ) as bounds(since, until)
  loop
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
