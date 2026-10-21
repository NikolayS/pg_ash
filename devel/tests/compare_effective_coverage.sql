\set ON_ERROR_STOP on
begin;
truncate ash.sample, ash.rollup_1m, ash.rollup_1h;
update ash.config set sample_interval = interval '1 second';
select ash._register_wait('active', 'CPU*', 'CPU*');
do $$
declare
  v_h timestamptz := date_trunc('hour', now()) - interval '2 days';
  v_h_ts integer := ash.ts_from_timestamptz(v_h);
  v_cpu integer;
  v_datid oid;
  v_actual record;
  v_hours integer;
  v_dimension text;
begin
  select id into strict v_cpu from ash.wait_event_map where type = 'CPU*';
  select oid into strict v_datid from pg_database where datname = current_database();
  insert into ash.rollup_1m (ts, datid, samples, peak_backends, wait_counts, query_counts)
  values
    (v_h_ts, v_datid, 60, 1, array[v_cpu, 60], '{}'),
    (v_h_ts + 3600, v_datid, 60, 2, array[v_cpu, 120], '{}');
  update ash.config set last_rollup_1h_ts = null, last_rollup_1m_ts = v_h_ts + 7200;
  v_hours := ash.rollup_hour();
  assert v_hours = 2, 'writer must aggregate exactly two hours';
  assert (select bool_and(ash._minute_counts_valid(minute_counts, wait_counts))
          from ash.rollup_1h), 'writer detail must be valid';
  -- Model expiration of minute retention; keep writer-produced hourly rows.
  delete from ash.rollup_1m;
  select * into strict v_actual from ash.compare(
    v_h, v_h + interval '1 hour', v_h + interval '1 hour', v_h + interval '2 hours',
    dimension => 'wait_event', bucket => interval '1 hour');
  assert v_actual.avg_delta is not distinct from 0.01,
    format('whole-hour control failed: %s', row_to_json(v_actual));
  raise notice 'PASS: real writer, valid sparse minute detail, whole-hour delta 0.01';
  foreach v_dimension in array array['wait_event', 'wait_event_type', 'query_id'] loop
    select * into strict v_actual from ash.compare(
      v_h + interval '30 minutes', v_h + interval '31 minutes',
      v_h + interval '90 minutes', v_h + interval '91 minutes',
      dimension => v_dimension, bucket => interval '1 hour');
    raise notice 'narrow request actual: %', row_to_json(v_actual);
    assert v_actual.period_start_1 = v_h
       and v_actual.period_end_1 = v_h + interval '1 hour'
       and v_actual.period_start_2 = v_h + interval '1 hour'
       and v_actual.period_end_2 = v_h + interval '2 hours',
      'effective hours must match the passing control';
    assert v_actual.avg_aas_1 = 0.02 and v_actual.avg_aas_2 = 0.03
       and v_actual.avg_delta is not distinct from 0.01,
      format('BUG: same effective covered hours must yield delta 0.01, got %s',
             coalesce(v_actual.avg_delta::text, 'NULL'));
  end loop;

  -- Scalar reads keep genuine minute detail and must not widen these windows.
  select * into strict v_actual from ash.compare(
    v_h + interval '30 minutes', v_h + interval '31 minutes',
    v_h + interval '90 minutes', v_h + interval '91 minutes',
    bucket => interval '1 hour');
  assert v_actual.period_start_1 = v_h + interval '30 minutes'
     and v_actual.period_end_1 = v_h + interval '31 minutes'
     and v_actual.avg_aas_1 is null and v_actual.avg_aas_2 is null
     and v_actual.avg_delta is null, 'scalar coverage must remain narrow';
  assert not exists (
    select 1 from ash.compare(
      v_h + interval '30 minutes', v_h + interval '31 minutes',
      v_h + interval '90 minutes', v_h + interval '91 minutes',
      dimension => 'database', bucket => interval '1 hour')
  ), 'database breakdown must also preserve sparse minute detail';

  -- An actually uncovered hour is not a measured zero.
  select * into strict v_actual from ash.compare(
    v_h - interval '30 minutes', v_h - interval '29 minutes',
    v_h + interval '90 minutes', v_h + interval '91 minutes',
    dimension => 'wait_event', bucket => interval '1 hour');
  assert v_actual.avg_aas_1 is null and v_actual.avg_aas_2 = 0.03
     and v_actual.avg_delta is null, 'missing hour must retain a NULL delta';

  -- Coverage is independent of the key: a key absent in a covered hour is zero.
  update ash.rollup_1h
  set wait_counts = '{}',
      minute_counts = array[0] || array_fill(null::integer, array[59])
  where ts = v_h_ts;
  select * into strict v_actual from ash.compare(
    v_h + interval '30 minutes', v_h + interval '31 minutes',
    v_h + interval '90 minutes', v_h + interval '91 minutes',
    dimension => 'wait_event', bucket => interval '1 hour');
  assert v_actual.avg_aas_1 is null and v_actual.avg_aas_2 = 0.03
     and v_actual.avg_delta = 0.03, 'covered absent key must use a zero baseline';
  raise notice 'PASS: effective coverage, scalar bounds, missing hour, and absent key';
end;
$$;
rollback;
