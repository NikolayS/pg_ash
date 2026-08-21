/* -------------------------------------------------------------------------
 * rollup_1h grain honesty and partial-hour degradation (#161, #130)
 *
 * Extracted from .github/workflows/test.yml. GitHub Actions silently refuses
 * to create a workflow run when the workflow file exceeds 512,000 bytes, and
 * test.yml sits just under that ceiling, so large test bodies live here and
 * are invoked with `psql --file` from a short workflow step. Same assertions,
 * same order; only the transport changed.
 *
 * Run standalone against a prepared database with:
 *   psql -v ON_ERROR_STOP=1 -f devel/tests/rollup_1h_grain_honesty.sql
 * ------------------------------------------------------------------------- */
-- B1a: top() must not present an hour average as a minute peak.
do $$
declare
  v_cpu smallint;
  v_io smallint;
  v_h timestamptz := date_trunc('hour', now()) - interval '45 days';
  v_aas record;
  v_aas_90 record;
  v_wait record;
  v_database record;
  v_timeline_90_rows bigint;
  v_timeline_90_points bigint;
begin
  truncate ash.sample;
  truncate ash.rollup_1m, ash.rollup_1h;
  update ash.config
  set sample_interval = interval '1 second',
      last_rollup_1m_ts = null,
      last_rollup_1h_ts = null
  where singleton;

  select ash._register_wait('active', 'CPU*', 'CPU*') into v_cpu;
  select ash._register_wait('active', 'IO', 'DataFileRead') into v_io;

  insert into ash.rollup_1h (
    ts, datid, samples, peak_backends,
    wait_counts, query_counts, minute_counts
  ) values (
    ash.ts_from_timestamptz(v_h),
    0::oid,
    3600,
    10,
    array[v_io, 2760, v_cpu, 1380]::int4[],
    '{}'::int8[],
    array[600]::int4[] || array_fill(60, array[59])
  );

  select * into v_aas
  from ash.aas(v_h, v_h + interval '1 hour');
  select * into v_wait
  from ash.top(
    'wait_event',
    v_h,
    v_h + interval '1 hour',
    n => 10,
    bucket => interval '1 minute'
  )
  where key = 'IO:DataFileRead';
  select * into v_database
  from ash.top(
    'database',
    v_h,
    v_h + interval '1 hour',
    n => 10,
    bucket => interval '1 minute'
  );
  select * into v_aas_90
  from ash.aas(
    v_h,
    v_h + interval '1 hour',
    bucket => interval '90 seconds'
  );
  select count(*), sum(data_points)
    into v_timeline_90_rows, v_timeline_90_points
  from ash.timeline(
    v_h,
    v_h + interval '1 hour',
    bucket => interval '90 seconds'
  );

  assert v_aas.avg_aas = 1.15
     and v_aas.peak_aas = 10.00
     and v_aas.p99_aas = 4.69
     and v_wait.avg_aas = 0.77
     and v_wait.peak_aas is null
     and v_wait.p99_aas is null
     and v_database.avg_aas = 1.15
     and v_database.peak_aas = 10.00
     and v_database.p99_aas = 4.69,
    format(
      'B1a aas avg=%s peak=%s p99=%s; top wait avg=%s peak=%s p99=%s; database avg=%s peak=%s p99=%s',
      v_aas.avg_aas,
      v_aas.peak_aas,
      v_aas.p99_aas,
      v_wait.avg_aas,
      v_wait.peak_aas,
      v_wait.p99_aas,
      v_database.avg_aas,
      v_database.peak_aas,
      v_database.p99_aas
    );

  assert v_aas_90.effective_bucket = interval '2 minutes'
     and v_aas_90.buckets_expected = 30
     and v_timeline_90_rows = 30
     and v_timeline_90_points = 60,
    format(
      'bucket widening effective=%s expected=%s timeline_rows=%s points=%s',
      v_aas_90.effective_bucket,
      v_aas_90.buckets_expected,
      v_timeline_90_rows,
      v_timeline_90_points
    );

  begin
    perform *
    from ash.aas(
      v_h,
      v_h + interval '1 hour',
      bucket => interval '59.9 seconds'
    );
    raise exception 'sub-minute fractional bucket was accepted';
  exception when others then
    if sqlerrm not like 'bucket must be at least 1 minute%' then
      raise;
    end if;
  end;
end $$;

-- B1b: adding a filter must disclose the surviving hour grain by
-- keeping exact totals while NULLing unsupported minute extremes.
do $$
declare
  v_cpu smallint;
  v_io smallint;
  v_h timestamptz := date_trunc('hour', now()) - interval '45 days';
  v_all record;
  v_io_minute record;
  v_io_fractional_hour record;
  v_io_hour record;
begin
  truncate ash.sample;
  truncate ash.rollup_1m, ash.rollup_1h;
  update ash.config
  set sample_interval = interval '1 second',
      last_rollup_1m_ts = null,
      last_rollup_1h_ts = null
  where singleton;

  select ash._register_wait('active', 'CPU*', 'CPU*') into v_cpu;
  select ash._register_wait('active', 'IO', 'DataFileRead') into v_io;

  insert into ash.rollup_1h (
    ts, datid, samples, peak_backends,
    wait_counts, query_counts, minute_counts
  ) values (
    ash.ts_from_timestamptz(v_h),
    0::oid,
    3600,
    21,
    array[v_io, 8280, v_cpu, 60]::int4[],
    '{}'::int8[],
    array[1260]::int4[] || array_fill(120, array[59])
  );

  select * into v_all
  from ash.aas(v_h, v_h + interval '1 hour');
  select * into v_io_minute
  from ash.aas(
    v_h,
    v_h + interval '1 hour',
    wait_event_type => 'IO',
    bucket => interval '1 minute'
  );
  select * into v_io_hour
  from ash.aas(
    v_h,
    v_h + interval '1 hour',
    wait_event_type => 'IO',
    bucket => interval '1 hour'
  );
  select * into v_io_fractional_hour
  from ash.aas(
    v_h,
    v_h + interval '1 hour',
    wait_event_type => 'IO',
    bucket => interval '3599.9 seconds'
  );

  assert v_all.avg_aas = 2.32
     and v_all.peak_aas = 21.00
     and v_all.p99_aas = 9.79
     and v_io_minute.avg_aas = 2.30
     and v_io_minute.backend_seconds = 8280.00
     and v_io_minute.peak_aas is null
     and v_io_minute.p99_aas is null
     and v_io_fractional_hour.effective_bucket = interval '1 hour'
     and v_io_fractional_hour.peak_aas is null
     and v_io_fractional_hour.p99_aas is null
     and v_io_hour.peak_aas = 2.30
     and v_io_hour.p99_aas = 2.30,
    format(
      'B1b unfiltered avg=%s peak=%s p99=%s; IO minute avg=%s seconds=%s peak=%s p99=%s; IO fractional-hour bucket=%s peak=%s p99=%s; IO hour peak=%s p99=%s',
      v_all.avg_aas,
      v_all.peak_aas,
      v_all.p99_aas,
      v_io_minute.avg_aas,
      v_io_minute.backend_seconds,
      v_io_minute.peak_aas,
      v_io_minute.p99_aas,
      v_io_fractional_hour.effective_bucket,
      v_io_fractional_hour.peak_aas,
      v_io_fractional_hour.p99_aas,
      v_io_hour.peak_aas,
      v_io_hour.p99_aas
    );
end $$;

-- B1c: byte-identical windows across retention tiers keep their
-- honest averages. Overall minute-detail extremes remain comparable;
-- dimensional hour-vs-minute extremes are suppressed as a pair.
do $$
declare
  v_io smallint;
  v_old_h timestamptz := date_trunc('hour', now()) - interval '45 days';
  v_new_h timestamptz := date_trunc('hour', now()) - interval '2 hours';
  v_overall record;
  v_dimension record;
  v_dimension_hour record;
begin
  truncate ash.sample;
  truncate ash.rollup_1m, ash.rollup_1h;
  update ash.config
  set sample_interval = interval '1 second',
      last_rollup_1m_ts = null,
      last_rollup_1h_ts = null
  where singleton;

  select ash._register_wait('active', 'IO', 'DataFileRead') into v_io;

  insert into ash.rollup_1h (
    ts, datid, samples, peak_backends,
    wait_counts, query_counts, minute_counts
  ) values (
    ash.ts_from_timestamptz(v_old_h),
    0::oid,
    3600,
    600,
    array[v_io, 39540]::int4[],
    '{}'::int8[],
    array[36000]::int4[] || array_fill(60, array[59])
  );

  insert into ash.rollup_1m (
    ts, datid, samples, peak_backends, wait_counts, query_counts
  )
  select
    ash.ts_from_timestamptz(
      v_new_h + minute_idx * interval '1 minute'
    ),
    0::oid,
    60,
    case when minute_idx = 0 then 600 else 1 end,
    array[
      v_io,
      case when minute_idx = 0 then 36000 else 60 end
    ]::int4[],
    '{}'::int8[]
  from generate_series(0, 59) as minute_idx;

  select * into v_overall
  from ash.compare(
    v_old_h,
    v_old_h + interval '1 hour',
    v_new_h,
    v_new_h + interval '1 hour'
  );
  select * into v_dimension
  from ash.compare(
    v_old_h,
    v_old_h + interval '1 hour',
    v_new_h,
    v_new_h + interval '1 hour',
    dimension => 'wait_event'
  );
  select * into v_dimension_hour
  from ash.compare(
    v_old_h,
    v_old_h + interval '1 hour',
    v_new_h,
    v_new_h + interval '1 hour',
    dimension => 'wait_event',
    bucket => interval '1 hour'
  );

  assert to_jsonb(v_overall) ->> 'source_1' = 'rollup_1h'
     and to_jsonb(v_overall) ->> 'source_2' = 'rollup_1m'
     and (to_jsonb(v_overall) ->> 'effective_bucket_1')::interval
           = interval '1 minute'
     and (to_jsonb(v_overall) ->> 'effective_bucket_2')::interval
           = interval '1 minute'
     and v_overall.avg_aas_1 = 10.98
     and v_overall.avg_aas_2 = 10.98
     and v_overall.avg_delta = 0.00
     and v_overall.peak_aas_1 = 600.00
     and v_overall.peak_aas_2 = 600.00
     and v_overall.p99_aas_1 = 246.59
     and v_overall.p99_aas_2 = 246.59
     and to_jsonb(v_dimension) ->> 'source_1' = 'rollup_1h'
     and to_jsonb(v_dimension) ->> 'source_2' = 'rollup_1m'
     and (to_jsonb(v_dimension) ->> 'effective_bucket_1')::interval
           = interval '1 hour'
     and (to_jsonb(v_dimension) ->> 'effective_bucket_2')::interval
           = interval '1 minute'
     and v_dimension.avg_aas_1 = 10.98
     and v_dimension.avg_aas_2 = 10.98
     and v_dimension.avg_delta = 0.00
     and v_dimension.peak_aas_1 is null
     and v_dimension.peak_aas_2 is null
     and v_dimension.p99_aas_1 is null
     and v_dimension.p99_aas_2 is null,
    format(
      'B1c overall source=%s/%s avg=%s/%s delta=%s peak=%s/%s p99=%s/%s; dimension source=%s/%s avg=%s/%s delta=%s peak=%s/%s p99=%s/%s',
      coalesce(to_jsonb(v_overall) ->> 'source_1', '<missing>'),
      coalesce(to_jsonb(v_overall) ->> 'source_2', '<missing>'),
      v_overall.avg_aas_1,
      v_overall.avg_aas_2,
      v_overall.avg_delta,
      v_overall.peak_aas_1,
      v_overall.peak_aas_2,
      v_overall.p99_aas_1,
      v_overall.p99_aas_2,
      coalesce(to_jsonb(v_dimension) ->> 'source_1', '<missing>'),
      coalesce(to_jsonb(v_dimension) ->> 'source_2', '<missing>'),
      v_dimension.avg_aas_1,
      v_dimension.avg_aas_2,
      v_dimension.avg_delta,
      v_dimension.peak_aas_1,
      v_dimension.peak_aas_2,
      v_dimension.p99_aas_1,
      v_dimension.p99_aas_2
    );

  -- Matching displayed buckets do not make different retained
  -- grains comparable: the old wait dimension has one hourly
  -- datum while the recent side has 60 minute observations.
  assert to_jsonb(v_dimension_hour) ->> 'source_1' = 'rollup_1h'
     and to_jsonb(v_dimension_hour) ->> 'source_2' = 'rollup_1m'
     and (
       to_jsonb(v_dimension_hour) ->> 'effective_bucket_1'
     )::interval = interval '1 hour'
     and (
       to_jsonb(v_dimension_hour) ->> 'effective_bucket_2'
     )::interval = interval '1 hour'
     and v_dimension_hour.avg_aas_1 = 10.98
     and v_dimension_hour.avg_aas_2 = 10.98
     and v_dimension_hour.avg_delta = 0.00
     and v_dimension_hour.peak_aas_1 is null
     and v_dimension_hour.peak_aas_2 is null
     and v_dimension_hour.p99_aas_1 is null
     and v_dimension_hour.p99_aas_2 is null,
    format(
      'B1c equal bucket/different grain source=%s/%s bucket=%s/%s avg=%s/%s delta=%s peak=%s/%s p99=%s/%s',
      to_jsonb(v_dimension_hour) ->> 'source_1',
      to_jsonb(v_dimension_hour) ->> 'source_2',
      to_jsonb(v_dimension_hour) ->> 'effective_bucket_1',
      to_jsonb(v_dimension_hour) ->> 'effective_bucket_2',
      v_dimension_hour.avg_aas_1,
      v_dimension_hour.avg_aas_2,
      v_dimension_hour.avg_delta,
      v_dimension_hour.peak_aas_1,
      v_dimension_hour.peak_aas_2,
      v_dimension_hour.p99_aas_1,
      v_dimension_hour.p99_aas_2
    );
end $$;

-- #130: hour-only dimensions snap partial bounds outward and expose
-- the effective window; the database dimension keeps minute precision.
do $$
declare
  v_cpu smallint;
  v_io smallint;
  v_h timestamptz := date_trunc('hour', now()) - interval '45 days';
  v_aas record;
  v_wait record;
  v_database record;
  v_rows bigint;
  v_min_aas numeric;
  v_max_aas numeric;
  v_min_source text;
  v_max_source text;
  v_points bigint;
  v_nonnull_peaks bigint;
  v_nonnull_p99s bigint;
begin
  truncate ash.sample;
  truncate ash.rollup_1m, ash.rollup_1h;
  update ash.config
  set sample_interval = interval '1 second',
      last_rollup_1m_ts = null,
      last_rollup_1h_ts = null
  where singleton;

  select ash._register_wait('active', 'CPU*', 'CPU*') into v_cpu;
  select ash._register_wait('active', 'IO', 'DataFileRead') into v_io;

  insert into ash.rollup_1h (
    ts, datid, samples, peak_backends,
    wait_counts, query_counts, minute_counts
  ) values
    (
      ash.ts_from_timestamptz(v_h),
      0::oid,
      3600,
      1,
      array[v_io, 3600]::int4[],
      '{}'::int8[],
      array_fill(60, array[60])
    ),
    (
      ash.ts_from_timestamptz(v_h + interval '1 hour'),
      0::oid,
      3600,
      1,
      array[v_cpu, 3600]::int4[],
      '{}'::int8[],
      array_fill(60, array[60])
    );

  select * into v_aas
  from ash.aas(
    v_h,
    v_h + interval '30 minutes',
    wait_event_type => 'IO'
  );
  select * into v_wait
  from ash.top(
    'wait_event_type',
    v_h + interval '30 minutes',
    v_h + interval '90 minutes',
    n => 10
  )
  where key = 'IO';
  select * into v_database
  from ash.top(
    'database',
    v_h,
    v_h + interval '30 minutes',
    n => 10
  );

  assert v_aas.period_start = v_h
     and v_aas.period_end = v_h + interval '1 hour'
     and v_aas.avg_aas = 1.00
     and v_aas.backend_seconds = 3600.00
     and v_aas.peak_aas is null
     and v_aas.p99_aas is null
     and (to_jsonb(v_wait) ->> 'period_start')::timestamptz = v_h
     and (to_jsonb(v_wait) ->> 'period_end')::timestamptz
           = v_h + interval '2 hours'
     and v_wait.avg_aas = 0.50
     and v_wait.backend_seconds = 3600.00
     and v_wait.peak_aas is null
     and v_wait.p99_aas is null
     and (to_jsonb(v_database) ->> 'period_start')::timestamptz = v_h
     and (to_jsonb(v_database) ->> 'period_end')::timestamptz
           = v_h + interval '30 minutes'
     and v_database.avg_aas = 1.00
     and v_database.backend_seconds = 1800.00
     and v_database.peak_aas = 1.00
     and v_database.p99_aas = 1.00,
    format(
      '#130 aas period=%s..%s avg=%s seconds=%s peak=%s p99=%s; wait period=%s..%s avg=%s seconds=%s peak=%s p99=%s; database period=%s..%s avg=%s seconds=%s peak=%s p99=%s',
      v_aas.period_start,
      v_aas.period_end,
      v_aas.avg_aas,
      v_aas.backend_seconds,
      v_aas.peak_aas,
      v_aas.p99_aas,
      coalesce(to_jsonb(v_wait) ->> 'period_start', '<missing>'),
      coalesce(to_jsonb(v_wait) ->> 'period_end', '<missing>'),
      v_wait.avg_aas,
      v_wait.backend_seconds,
      v_wait.peak_aas,
      v_wait.p99_aas,
      coalesce(to_jsonb(v_database) ->> 'period_start', '<missing>'),
      coalesce(to_jsonb(v_database) ->> 'period_end', '<missing>'),
      v_database.avg_aas,
      v_database.backend_seconds,
      v_database.peak_aas,
      v_database.p99_aas
    );

  select
    count(*),
    min(source),
    max(source),
    sum(data_points),
    min(avg_aas),
    max(avg_aas),
    count(peak_aas),
    count(p99_aas)
    into
      v_rows,
      v_min_source,
      v_max_source,
      v_points,
      v_min_aas,
      v_max_aas,
      v_nonnull_peaks,
      v_nonnull_p99s
  from ash.timeline(
    v_h + interval '30 minutes',
    v_h + interval '90 minutes',
    interval '30 minutes',
    wait_event_type => 'IO'
  );
  assert v_rows = 2
     and v_min_source = 'rollup_1h'
     and v_max_source = 'rollup_1h'
     and v_points = 2
     and v_min_aas = 0.00
     and v_max_aas = 1.00
     and v_nonnull_peaks = 0
     and v_nonnull_p99s = 0,
    format(
      '#130 timeline rows=%s source=%s..%s points=%s avg=%s..%s nonnull_peak=%s nonnull_p99=%s',
      v_rows,
      v_min_source,
      v_max_source,
      v_points,
      v_min_aas,
      v_max_aas,
      v_nonnull_peaks,
      v_nonnull_p99s
    );

  select
    count(*) filter (where bucket_start is not null),
    min(aas) filter (where bucket_start is not null),
    max(aas) filter (where bucket_start is not null)
    into v_rows, v_min_aas, v_max_aas
  from ash.chart(
    v_h + interval '30 minutes',
    v_h + interval '90 minutes',
    interval '30 minutes'
  );
  assert v_rows = 2 and v_min_aas = 1.00 and v_max_aas = 1.00,
    format(
      '#130 chart snapped rows=%s aas=%s..%s',
      v_rows,
      v_min_aas,
      v_max_aas
    );
end $$;

-- summary(): the minute-precise headline and widened hour-only
-- wait/query drills must disclose their distinct effective plans.
do $$
declare
  v_cpu smallint;
  v_io smallint;
  v_h timestamptz := date_trunc('hour', now()) - interval '45 days';
  v_period_start text;
  v_period_end text;
  v_avg text;
  v_drill_source text;
  v_drill_start text;
  v_drill_end text;
  v_drill_bucket text;
  v_top_wait text;
begin
  truncate ash.sample;
  truncate ash.rollup_1m, ash.rollup_1h;
  update ash.config
  set sample_interval = interval '1 second',
      last_rollup_1m_ts = null,
      last_rollup_1h_ts = null
  where singleton;

  select ash._register_wait('active', 'CPU*', 'CPU*') into v_cpu;
  select ash._register_wait('active', 'IO', 'DataFileRead') into v_io;

  /*
   * Both hours contain load only in the halves outside the requested
   * [h+30m, h+90m) window. The headline can prove measured zero from
   * minute_counts; the dimensional drills can only report the two
   * complete hours (IO 36000 / 7200 = 5.00 AAS).
   */
  insert into ash.rollup_1h (
    ts, datid, samples, peak_backends,
    wait_counts, query_counts, minute_counts
  ) values
    (
      ash.ts_from_timestamptz(v_h),
      0::oid,
      3600,
      20,
      array[v_io, 36000]::int4[],
      '{}'::int8[],
      array_fill(1200, array[30])
        || array_fill(0, array[30])
    ),
    (
      ash.ts_from_timestamptz(v_h + interval '1 hour'),
      0::oid,
      3600,
      10,
      array[v_cpu, 18000]::int4[],
      '{}'::int8[],
      array_fill(0, array[30])
        || array_fill(600, array[30])
    );

  select
    max(value) filter (where metric = 'period_start'),
    max(value) filter (where metric = 'period_end'),
    max(value) filter (where metric = 'avg_aas'),
    max(value) filter (where metric = 'drill_source'),
    max(value) filter (where metric = 'drill_period_start'),
    max(value) filter (where metric = 'drill_period_end'),
    max(value) filter (where metric = 'drill_effective_bucket'),
    max(value) filter (where metric = 'top_wait_1')
    into
      v_period_start,
      v_period_end,
      v_avg,
      v_drill_source,
      v_drill_start,
      v_drill_end,
      v_drill_bucket,
      v_top_wait
  from ash.summary(
    v_h + interval '30 minutes',
    v_h + interval '90 minutes'
  );

  assert v_period_start::timestamptz
           = v_h + interval '30 minutes'
     and v_period_end::timestamptz
           = v_h + interval '90 minutes'
     and v_avg = '0.00'
     and v_drill_source = 'rollup_1h'
     and v_drill_start::timestamptz = v_h
     and v_drill_end::timestamptz = v_h + interval '2 hours'
     and v_drill_bucket::interval = interval '1 hour'
     and v_top_wait
           = 'IO:DataFileRead (avg_aas 5.00, 66.67%)',
    format(
      'summary headline=%s..%s avg=%s; drill=%s %s..%s bucket=%s top=%s',
      v_period_start,
      v_period_end,
      v_avg,
      v_drill_source,
      v_drill_start,
      v_drill_end,
      v_drill_bucket,
      v_top_wait
    );
end $$;

-- #131 regression controls: defaults and now()-relative calls remain
-- usable when rollup_1h is the only retained source.
do $$
declare
  v_io smallint;
  v_requested_start int4;
  v_requested_end int4;
  v_effective_start int4;
  v_effective_end int4;
  v_expected_hours int;
  v_summary_rows bigint;
  v_summary_avg text;
  v_summary_drill_source text;
  v_summary_drill_start text;
  v_summary_drill_end text;
  v_summary_drill_bucket text;
  v_chart_rows bigint;
  v_chart_min numeric;
  v_chart_max numeric;
  v_timeline_rows bigint;
  v_timeline_points bigint;
  v_timeline_min numeric;
  v_timeline_max numeric;
  v_timeline_peaks bigint;
  v_timeline_p99s bigint;
  v_top record;
  v_aas record;
begin
  truncate ash.sample;
  truncate ash.rollup_1m, ash.rollup_1h;
  update ash.config
  set sample_interval = interval '1 second',
      last_rollup_1m_ts = null,
      last_rollup_1h_ts = null
  where singleton;

  select ash._register_wait('active', 'IO', 'DataFileRead') into v_io;

  v_requested_start :=
    (ash.ts_from_timestamptz(now() - interval '1 hour') / 60) * 60;
  v_requested_end :=
    (ash.ts_from_timestamptz(now()) / 60) * 60;
  v_effective_start := (v_requested_start / 3600) * 3600;
  v_effective_end :=
    ((v_requested_end::bigint + 3599) / 3600 * 3600)::int4;
  v_expected_hours := (v_effective_end - v_effective_start) / 3600;

  insert into ash.rollup_1h (
    ts, datid, samples, peak_backends,
    wait_counts, query_counts, minute_counts
  )
  select
    hour_ts::int4,
    0::oid,
    3600,
    1,
    array[v_io, 3600]::int4[],
    array[111::bigint, 3600]::int8[],
    array_fill(60, array[60])
  from generate_series(
    v_effective_start::bigint,
    (v_effective_end - 3600)::bigint,
    3600
  ) as hour_ts;

  select
    count(*),
    max(value) filter (where metric = 'avg_aas'),
    max(value) filter (where metric = 'drill_source'),
    max(value) filter (where metric = 'drill_period_start'),
    max(value) filter (where metric = 'drill_period_end'),
    max(value) filter (where metric = 'drill_effective_bucket')
    into
      v_summary_rows,
      v_summary_avg,
      v_summary_drill_source,
      v_summary_drill_start,
      v_summary_drill_end,
      v_summary_drill_bucket
  from ash.summary();
  select
    count(*) filter (where bucket_start is not null),
    min(aas) filter (where bucket_start is not null),
    max(aas) filter (where bucket_start is not null)
    into v_chart_rows, v_chart_min, v_chart_max
  from ash.chart();
  select
    count(*),
    sum(data_points),
    min(avg_aas),
    max(avg_aas),
    count(peak_aas),
    count(p99_aas)
    into
      v_timeline_rows,
      v_timeline_points,
      v_timeline_min,
      v_timeline_max,
      v_timeline_peaks,
      v_timeline_p99s
  from ash.timeline();
  select * into v_top from ash.top('wait_event_type')
  where key = 'IO';
  select * into v_aas from ash.aas(wait_event_type => 'IO');

  assert v_summary_rows = 15
     and v_summary_avg = '1.00'
     and v_summary_drill_source = 'rollup_1h'
     and v_summary_drill_start::timestamptz
           = ash.ts_to_timestamptz(v_effective_start)
     and v_summary_drill_end::timestamptz
           = ash.ts_to_timestamptz(v_effective_end)
     and v_summary_drill_bucket::interval = interval '1 hour'
     and v_chart_rows = v_expected_hours
     and v_chart_min = 1.00
     and v_chart_max = 1.00
     and v_timeline_rows = 60
     and v_timeline_points = 60
     and v_timeline_min = 1.00
     and v_timeline_max = 1.00
     and v_timeline_peaks = 60
     and v_timeline_p99s = 60
     and v_top.avg_aas = 1.00
     and v_top.peak_aas is null
     and v_top.p99_aas is null
     and v_aas.avg_aas = 1.00
     and v_aas.backend_seconds
           = (v_effective_end - v_effective_start)::numeric
     and v_aas.peak_aas is null
     and v_aas.p99_aas is null,
    format(
      '#131 defaults summary rows=%s avg=%s drill=%s %s..%s bucket=%s; chart rows=%s expected=%s aas=%s..%s; timeline rows=%s points=%s avg=%s..%s nonnull_peak=%s nonnull_p99=%s; top avg=%s peak=%s p99=%s; filtered aas avg=%s seconds=%s peak=%s p99=%s',
      v_summary_rows,
      v_summary_avg,
      v_summary_drill_source,
      v_summary_drill_start,
      v_summary_drill_end,
      v_summary_drill_bucket,
      v_chart_rows,
      v_expected_hours,
      v_chart_min,
      v_chart_max,
      v_timeline_rows,
      v_timeline_points,
      v_timeline_min,
      v_timeline_max,
      v_timeline_peaks,
      v_timeline_p99s,
      v_top.avg_aas,
      v_top.peak_aas,
      v_top.p99_aas,
      v_aas.avg_aas,
      v_aas.backend_seconds,
      v_aas.peak_aas,
      v_aas.p99_aas
    );

  truncate ash.rollup_1m, ash.rollup_1h;
  update ash.config
  set last_rollup_1m_ts = null,
      last_rollup_1h_ts = null
  where singleton;

  raise notice
    'rollup_1h grain honesty, partial-hour, and default-call tests PASSED';
end $$;
