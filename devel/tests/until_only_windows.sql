/* -------------------------------------------------------------------------
 * until-only windows and inverted-window validation (#165)
 *
 * Extracted from .github/workflows/test.yml. GitHub Actions silently refuses
 * to create a workflow run when the workflow file exceeds 512,000 bytes, and
 * test.yml sits just under that ceiling, so large test bodies live here and
 * are invoked with `psql --file` from a short workflow step. Same assertions,
 * same order; only the transport changed.
 *
 * Run standalone against a prepared database with:
 *   psql -v ON_ERROR_STOP=1 -f devel/tests/until_only_windows.sql
 * ------------------------------------------------------------------------- */
-- B5 / issue #165. Reproduce the original failure exactly:
-- an until-only call ending three hours ago used to be rewritten to
-- a one-minute window around now()-1h, where the fixture below is
-- IO:DataFileRead at 40.00 AAS / 100.00%.
begin;
update ash.config
  set current_slot = 0, sample_interval = '1 second',
      last_rollup_1m_ts = null, last_rollup_1h_ts = null
  where singleton;
truncate ash.sample_0, ash.sample_1, ash.sample_2;
truncate ash.query_map_0, ash.query_map_1, ash.query_map_2;
truncate ash.rollup_1m, ash.rollup_1h;

create temp table b5anchor as
select date_trunc('minute', now() - interval '3 hours') as requested_until,
       date_trunc('minute', now() - interval '1 hour') as wrong_minute;

do $$
declare
  v_io smallint;
  v_datid oid;
  v_until timestamptz;
  v_wrong timestamptz;
  v_intended timestamptz;
begin
  select ash._register_wait('active', 'IO', 'DataFileRead') into v_io;
  select oid into v_datid from pg_database where datname = current_database();
  select requested_until, wrong_minute
    into v_until, v_wrong from b5anchor;
  v_intended := v_until - interval '30 minutes';

  -- Intended incident: 300 backend-seconds in the hour ending at
  -- requested_until (5.00 AAS in its minute, 0.08 over the hour).
  insert into ash.sample (sample_ts, datid, active_count, data, slot)
  values (
    ash.ts_from_timestamptz(v_intended), v_datid, 300,
    array[-v_io::int, 300]
      || pg_catalog.array_fill(0, array[300]),
    0
  );
  insert into ash.rollup_1m (
    ts, datid, samples, peak_backends, wait_counts, query_counts
  ) values (
    ash.ts_from_timestamptz(v_intended), v_datid, 1, 300,
    array[v_io::int, 300], array[]::bigint[]
  );

  -- Wrong hour: this is what the old degenerate-window clamp read.
  insert into ash.sample (sample_ts, datid, active_count, data, slot)
  values (
    ash.ts_from_timestamptz(v_wrong), v_datid, 2400,
    array[-v_io::int, 2400]
      || pg_catalog.array_fill(0, array[2400]),
    0
  );
  insert into ash.rollup_1m (
    ts, datid, samples, peak_backends, wait_counts, query_counts
  ) values (
    ash.ts_from_timestamptz(v_wrong), v_datid, 1, 2400,
    array[v_io::int, 2400], array[]::bigint[]
  );
end $$;

-- Every until-only reader anchors its implicit start to until-1h.
do $$
declare
  v_until timestamptz := (select requested_until from b5anchor);
  a record;
  r record;
  j jsonb;
  v_count int;
  v_points bigint;
  v_max numeric;
  v_value text;
begin
  select * into a from ash.aas(until => v_until);
  assert a.period_start = v_until - interval '1 hour'
     and a.period_end = v_until,
    format('aas until-only bounds: got [%s,%s), want [%s,%s)',
           a.period_start, a.period_end,
           v_until - interval '1 hour', v_until);
  assert a.buckets_expected = 60 and a.buckets_with_data = 1
     and a.avg_aas = 0.08 and a.peak_aas = 5.00
     and a.p99_aas = 5.00 and a.backend_seconds = 300.00,
    format('aas until-only values: expected 60/1/0.08/5.00/5.00/300.00, '
           'got %s/%s/%s/%s/%s/%s',
           a.buckets_expected, a.buckets_with_data, a.avg_aas,
           a.peak_aas, a.p99_aas, a.backend_seconds);

  select count(*), sum(data_points), max(avg_aas)
    into v_count, v_points, v_max
  from ash.timeline(until => v_until);
  assert v_count = 60 and v_points = 1 and v_max = 5.00,
    format('timeline until-only expected rows/points/max 60/1/5.00, '
           'got %s/%s/%s', v_count, v_points, v_max);

  select * into r from ash.top('wait_event', until => v_until);
  assert r.key = 'IO:DataFileRead' and r.avg_aas = 0.08
     and r.peak_aas = 5.00 and r.p99_aas = 5.00
     and r.backend_seconds = 300.00 and r.pct = 100.00,
    format('top until-only expected IO:DataFileRead/0.08/5.00/5.00/'
           '300.00/100.00, got %s/%s/%s/%s/%s/%s',
           r.key, r.avg_aas, r.peak_aas, r.p99_aas,
           r.backend_seconds, r.pct);

  select count(*), max(aas)
    into v_count, v_max
  from ash.chart(until => v_until);
  assert v_count = 61 and v_max = 5.00,
    format('chart until-only expected legend+60/max 61/5.00, got %s/%s',
           v_count, v_max);
  assert exists (
    select from ash.chart(until => v_until)
    where bucket_start is null
      and position('IO:DataFileRead' in chart) > 0
  ), 'chart until-only legend must name IO:DataFileRead';

  select count(*) into v_count
  from ash.samples(until => v_until, n => 500);
  assert v_count = 300,
    'samples until-only expected 300 exact backend rows, got ' || v_count;
  select * into r
  from ash.samples(until => v_until, n => 500) limit 1;
  assert r.sample_time = v_until - interval '30 minutes'
     and r.wait_event = 'IO:DataFileRead',
    format('samples until-only expected intended minute/IO, got %s/%s',
           r.sample_time, r.wait_event);

  j := ash.report(until => v_until);
  assert j is not null, 'report until-only must read intended rollup_1m';
  assert (j->'coverage'->>'from')::timestamptz
           = v_until - interval '1 hour'
     and (j->'coverage'->>'to')::timestamptz = v_until
     and (j->'coverage'->>'minutes_expected')::int = 60
     and (j->'coverage'->>'minutes_with_data')::int = 1,
    'report until-only coverage must be exactly the preceding hour: '
    || (j->'coverage')::text;
  j := ash.report();
  assert (j->'coverage'->>'from')::timestamptz
           = date_trunc('minute', now() - interval '1 day')
     and (j->'coverage'->>'to')::timestamptz
           = date_trunc('minute', now())
     and (j->'coverage'->>'minutes_expected')::int = 1440
     and (j->'coverage'->>'minutes_with_data')::int = 2,
    'report() no-argument default must remain the preceding day: '
    || (j->'coverage')::text;

  select value into v_value
  from ash.summary(until => v_until) where metric = 'avg_aas';
  assert v_value = '0.08',
    'summary until-only avg_aas expected 0.08, got ' || v_value;
  select value into v_value
  from ash.summary(until => v_until) where metric = 'top_wait_1';
  assert v_value = 'IO:DataFileRead (avg_aas 0.08, 100.00%)',
    'summary until-only top_wait_1 mismatch: ' || v_value;
  select value into v_value
  from ash.summary(until => v_until) where metric = 'period_start';
  assert v_value::timestamptz = v_until - interval '1 hour',
    'summary until-only period_start mismatch: ' || v_value;

  -- periods() was already correct and is the family-wide reference.
  select * into r from ash.periods(v_until) where period = '1h';
  assert r.period_start = v_until - interval '1 hour'
     and r.period_end = v_until and r.buckets_with_data = 1
     and r.avg_aas = 0.08 and r.peak_aas = 5.00,
    format('periods reference expected anchored 1h/1/0.08/5.00, '
           'got [%s,%s)/%s/%s/%s', r.period_start, r.period_end,
           r.buckets_with_data, r.avg_aas, r.peak_aas);

  -- compare has required bounds, but NULL since_N means the same
  -- until-only default for each delegated reader window.
  select * into r
  from ash.compare(null, v_until, null, v_until);
  assert r.key = 'overall' and r.avg_aas_1 = 0.08
     and r.avg_aas_2 = 0.08 and r.avg_delta = 0.00
     and r.peak_aas_1 = 5.00 and r.peak_aas_2 = 5.00,
    format('compare NULL-since windows expected .08/.08/.00/5/5, '
           'got %s/%s/%s/%s/%s',
           r.avg_aas_1, r.avg_aas_2, r.avg_delta,
           r.peak_aas_1, r.peak_aas_2);

  raise notice 'B5 until-only reader windows PASSED';
end $$;

-- Explicit since > until must raise in every public reader's frame.
do $$
declare
  v_until timestamptz := (select requested_until from b5anchor);
  v_later timestamptz :=
    (select requested_until + interval '40 seconds' from b5anchor);
  v_earlier timestamptz :=
    (select requested_until + interval '20 seconds' from b5anchor);
  v_raised boolean;
  a record;
begin
  -- A chronological sub-minute window and an equal-bound window
  -- remain legitimate degenerate windows. Minute flooring makes
  -- each empty, then the #63 guard safely expands it to one minute.
  select * into a from ash.aas(v_earlier, v_later);
  assert a.period_start = v_until
     and a.period_end = v_until + interval '1 minute',
    'chronological sub-minute window must retain the #63 clamp';
  select * into a from ash.aas(v_until, v_until);
  assert a.period_start = v_until
     and a.period_end = v_until + interval '1 minute',
    'equal window must retain the #63 clamp';

  v_raised := false;
  begin perform * from ash.aas(v_later, v_earlier);
  exception when others then
    if sqlerrm = 'ash.aas: since must be less than or equal to until'
      then v_raised := true; else raise; end if;
  end;
  assert v_raised, 'ash.aas must reject since > until';

  v_raised := false;
  begin perform * from ash.timeline(v_later, v_earlier);
  exception when others then
    if sqlerrm = 'ash.timeline: since must be less than or equal to until'
      then v_raised := true; else raise; end if;
  end;
  assert v_raised, 'ash.timeline must reject since > until';

  v_raised := false;
  begin perform * from ash.top('wait_event', v_later, v_earlier);
  exception when others then
    if sqlerrm = 'ash.top: since must be less than or equal to until'
      then v_raised := true; else raise; end if;
  end;
  assert v_raised, 'ash.top must reject since > until';

  v_raised := false;
  begin perform ash.report(v_later, v_earlier);
  exception when others then
    if sqlerrm = 'ash.report: since must be less than or equal to until'
      then v_raised := true; else raise; end if;
  end;
  assert v_raised, 'ash.report must reject since > until';

  v_raised := false;
  begin perform * from ash.chart(v_later, v_earlier);
  exception when others then
    if sqlerrm = 'ash.chart: since must be less than or equal to until'
      then v_raised := true; else raise; end if;
  end;
  assert v_raised, 'ash.chart must reject since > until';

  v_raised := false;
  begin perform * from ash.samples(v_later, v_earlier);
  exception when others then
    if sqlerrm = 'ash.samples: since must be less than or equal to until'
      then v_raised := true; else raise; end if;
  end;
  assert v_raised, 'ash.samples must reject since > until';

  v_raised := false;
  begin perform * from ash.summary(v_later, v_earlier);
  exception when others then
    if sqlerrm = 'ash.summary: since must be less than or equal to until'
      then v_raised := true; else raise; end if;
  end;
  assert v_raised, 'ash.summary must reject since > until';

  v_raised := false;
  begin
    perform * from ash.compare(
      v_later, v_earlier, v_until - interval '1 hour', v_until);
  exception when others then
    if sqlerrm =
         'ash.compare: since_1 must be less than or equal to until_1'
      then v_raised := true; else raise; end if;
  end;
  assert v_raised, 'ash.compare must reject inverted window 1';

  v_raised := false;
  begin
    perform * from ash.compare(
      v_until - interval '1 hour', v_until, v_later, v_earlier);
  exception when others then
    if sqlerrm =
         'ash.compare: since_2 must be less than or equal to until_2'
      then v_raised := true; else raise; end if;
  end;
  assert v_raised, 'ash.compare must reject inverted window 2';

  raise notice 'B5 inverted-window validation PASSED';
end $$;

-- #63 remains effective for a chronological window whose real span
-- cannot fit in int4 seconds: conversions clamp both endpoints
-- before any int4 subtraction/cast can overflow.
truncate ash.sample_0, ash.sample_1, ash.sample_2;
truncate ash.rollup_1m, ash.rollup_1h;
do $$
declare
  a record;
  r record;
  v_count int;
  v_value text;
begin
  assert extract(epoch from (
    '3000-01-01'::timestamptz - '1000-01-01'::timestamptz
  )) > 2147483647,
    'test setup: cross-horizon span must overflow int4 seconds';

  select * into a
  from ash.aas('1000-01-01'::timestamptz,
               '3000-01-01'::timestamptz);
  assert a.period_start = ash.epoch()
     and a.period_end = ash.ts_to_timestamptz(2147483640)
     and a.source = 'none'
     and a.buckets_expected = 35791394
     and a.buckets_with_data = 0 and a.avg_aas = 0
     and a.peak_aas = 0 and a.p99_aas = 0
     and a.backend_seconds = 0,
    'cross-horizon aas must clamp safely with exact zero values';

  select count(*) into v_count
  from ash.timeline('1000-01-01'::timestamptz,
                    '3000-01-01'::timestamptz);
  assert v_count = 24856,
    'cross-horizon timeline expected 24856 safe daily buckets, got '
    || v_count;
  select count(*) into v_count
  from ash.top('wait_event', '1000-01-01'::timestamptz,
               '3000-01-01'::timestamptz);
  assert v_count = 0, 'cross-horizon top must return 0 rows';
  select count(*) into v_count
  from ash.samples('1000-01-01'::timestamptz,
                   '3000-01-01'::timestamptz);
  assert v_count = 0, 'cross-horizon samples must return 0 rows';
  assert ash.report('1000-01-01'::timestamptz,
                    '3000-01-01'::timestamptz) is null,
    'cross-horizon report must return NULL';
  select count(*) into v_count
  from ash.chart('1000-01-01'::timestamptz,
                 '3000-01-01'::timestamptz);
  assert v_count = 0, 'cross-horizon chart must return 0 rows';
  select value into v_value
  from ash.summary('1000-01-01'::timestamptz,
                   '3000-01-01'::timestamptz)
  where metric = 'status';
  assert v_value = 'no data in this time range',
    'cross-horizon summary must report no data';
  select * into r from ash.compare(
    '1000-01-01'::timestamptz, '3000-01-01'::timestamptz,
    '1000-01-01'::timestamptz, '3000-01-01'::timestamptz);
  assert r.key = 'overall' and r.avg_aas_1 is null
     and r.avg_aas_2 is null and r.avg_delta is null,
    'cross-horizon compare must preserve uncovered NULL semantics';

  raise notice 'B5 #63 cross-horizon clamp PASSED';
end $$;
rollback;
