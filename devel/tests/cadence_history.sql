\set ON_ERROR_STOP on

/* #137: retain the meaning of stored appearances until weighted storage exists. */
begin;
truncate ash.sample, ash.rollup_1m, ash.rollup_1h;
update ash.config
set sample_interval = interval '1 second',
  last_rollup_1m_ts = null,
  last_rollup_1h_ts = null;

do $cadence_history$
declare
  v_base timestamptz := date_trunc('hour', now()) - interval '2 hours';
  v_ts int4 := ash.ts_from_timestamptz(v_base);
  v_datid oid := (select oid from pg_database where datname = current_database());
  v_wait smallint := ash._register_wait('active', 'CPU*', 'CPU*');
  v_actual record;
  v_before jsonb;
  v_jobs_before jsonb;
  v_jobs_after jsonb;
  v_invalid interval;
begin
  insert into ash.sample (sample_ts, datid, active_count, data)
  select v_ts + tick, v_datid, 1, array[-v_wait::int, 1, 0]
  from generate_series(0, 59) as tick;

  select * into v_actual from ash.aas(v_base, v_base + interval '1 minute');
  assert v_actual.avg_aas = 1 and v_actual.peak_aas = 1
    and v_actual.backend_seconds = 60,
    format('fixture must represent one busy backend minute: %s', v_actual);
  select to_jsonb(config) into v_before from ash.config as config;

  begin
    update ash.config set sample_interval = interval '5 seconds';
    raise exception 'direct cadence change with raw history was accepted';
  exception when sqlstate '55000' then
    assert sqlerrm like '%retained history%', sqlerrm;
  end;
  assert (select to_jsonb(config) from ash.config as config) = v_before,
    'rejected config update changed config';

  if ash._pg_cron_available() then
    select coalesce(jsonb_agg(job order by jobid), '[]'::jsonb)
    into v_jobs_before from cron.job as job;
  end if;
  begin
    perform ash.start(interval '5 seconds');
    raise exception 'start changed cadence with raw history';
  exception when sqlstate '55000' then
    assert sqlerrm like '%retained history%', sqlerrm;
  end;
  assert (select to_jsonb(config) from ash.config as config) = v_before,
    'rejected start changed config';
  if ash._pg_cron_available() then
    select coalesce(jsonb_agg(job order by jobid), '[]'::jsonb)
    into v_jobs_after from cron.job as job;
    assert v_jobs_after = v_jobs_before, 'rejected start changed cron jobs';
  end if;
  select * into v_actual from ash.aas(v_base, v_base + interval '1 minute');
  assert v_actual.avg_aas = 1 and v_actual.peak_aas = 1
    and v_actual.backend_seconds = 60,
    'rejected cadence changes must preserve historical values';

  perform ash.rollup_minute();
  truncate ash.sample;
  assert exists (select from ash.rollup_1m), 'minute rollup fixture is empty';
  begin
    update ash.config set sample_interval = interval '5 seconds';
    raise exception 'raw-empty cadence change with minute history accepted';
  exception when sqlstate '55000' then
    assert sqlerrm like '%retained history%', sqlerrm;
  end;
  select * into v_actual from ash.aas(v_base, v_base + interval '1 minute');
  assert v_actual.source = 'rollup_1m' and v_actual.avg_aas = 1
    and v_actual.peak_aas = 1 and v_actual.backend_seconds = 60,
    format('minute history changed: %s', v_actual);

  perform ash.rollup_hour();
  truncate ash.rollup_1m;
  assert exists (select from ash.rollup_1h), 'hour rollup fixture is empty';
  begin
    perform ash.start(interval '5 seconds');
    raise exception 'cadence change with only hour history accepted';
  exception when sqlstate '55000' then
    assert sqlerrm like '%retained history%', sqlerrm;
  end;
  assert (select sample_interval from ash.config) = interval '1 second',
    'hour history cadence changed';

  /* A new empty collection can select another cadence explicitly. */
  truncate ash.rollup_1h;
  perform ash.start(interval '5 seconds');
  insert into ash.sample (sample_ts, datid, active_count, data)
  select v_ts + tick, v_datid, 1, array[-v_wait::int, 1, 0]
  from generate_series(0, 55, 5) as tick;
  perform ash.stop();
  perform ash.start();
  assert (select sample_interval from ash.config) = interval '5 seconds',
    'start() must resume the configured cadence';
  select * into v_actual from ash.aas(v_base, v_base + interval '1 minute');
  assert v_actual.avg_aas = 1 and v_actual.peak_aas = 1
    and v_actual.backend_seconds = 60,
    format('stop/start reweighted five-second history: %s', v_actual);

  /* Same-value updates are harmless, including unrelated config changes. */
  update ash.config set sample_interval = interval '5 seconds', debug_logging = false;
  assert (select count(*) from ash.sample) = 12, 'history was removed';
  truncate ash.sample;

  foreach v_invalid in array array[
    null::interval, interval '0 seconds', interval '-1 second',
    interval '0.6 seconds', interval '1.4 seconds', interval '59.6 seconds',
    interval '61 seconds', interval '90 seconds', interval '2 minutes',
    interval '1 hour', interval '100000000 days'
  ] loop
    select to_jsonb(config) into v_before from ash.config as config;
    select count(*) as errors into v_actual
    from ash.start(v_invalid) where job_type = 'error';
    assert v_actual.errors = 1, format('start accepted invalid interval %s', v_invalid);
    assert (select to_jsonb(config) from ash.config as config) = v_before,
      format('invalid start(%s) changed config', v_invalid);
    begin
      update ash.config set sample_interval = v_invalid;
      raise exception 'direct config accepted invalid interval %', v_invalid;
    exception when check_violation then
      assert sqlerrm like '%1 to 60 whole seconds%', sqlerrm;
    end;
    assert (select to_jsonb(config) from ash.config as config) = v_before,
      'invalid direct update changed config';
  end loop;

  for seconds in 1..60 loop
    perform ash.start(seconds * interval '1 second');
    assert (select sample_interval from ash.config) = seconds * interval '1 second',
      format('valid %s-second cadence was rejected', seconds);
  end loop;
  raise notice 'Issue #137 cadence history assertions PASSED';
end
$cadence_history$;
rollback;
