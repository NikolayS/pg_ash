\set ON_ERROR_STOP on
/* Dedicated database: install the pinned beta1 baseline before the new guard. */
\i :legacy_install_path

update ash.config set sample_interval = interval '2 minutes';
insert into ash.sample (sample_ts, datid, active_count, data)
select ash.ts_from_timestamptz(date_trunc('minute', now()) - interval '10 minutes'),
  oid, 1, array[-ash._register_wait('active', 'CPU*', 'CPU*')::int, 1, 0]
from pg_database where datname = current_database();
insert into ash.rollup_1m (ts, datid, samples, peak_backends, wait_counts, query_counts)
select sample_ts, datid, 1, 1, array[-data[1], 1], '{}'::bigint[] from ash.sample;
create temporary table cadence_legacy_before as
select (select to_jsonb(config) - 'version' from ash.config as config) as config,
  (select jsonb_agg(sample) from ash.sample as sample) as raw,
  (select jsonb_agg(minute) from ash.rollup_1m as minute) as minute;

\i :install_path
\i :install_path

do $cadence_legacy$
declare
  v_before record;
  v_actual record;
  v_reader text;
begin
  select * into strict v_before from cadence_legacy_before;
  assert (select to_jsonb(config) - 'version' from ash.config as config) @> v_before.config,
    'upgrade/re-apply must preserve unsupported legacy config verbatim';
  assert (select jsonb_agg(sample) from ash.sample as sample) = v_before.raw,
    'upgrade removed or changed raw legacy history';
  assert (select jsonb_agg(minute) from ash.rollup_1m as minute) = v_before.minute,
    'upgrade removed or changed minute legacy history';

  assert (select value from ash.status() where metric = 'sample_interval_supported') = 'false',
    'status must identify unsupported legacy cadence';
  assert ash.take_sample() = 0, 'unsupported legacy cadence continued sampling';
  call ash.run_take_sample();
  assert (select skipped_samples from ash.config) = (v_before.config->>'skipped_samples')::int + 2,
    'unsupported legacy calls must count both skipped samples';
  assert (select jsonb_agg(sample) from ash.sample as sample) = v_before.raw,
    'unsupported legacy CALL added raw samples';
  foreach v_reader in array array[
    'select * from ash.aas(now() - interval ''1 hour'', now())',
    'select * from ash.timeline(now() - interval ''1 hour'', now())',
    'select * from ash.top(''wait_event_type'', now() - interval ''1 hour'', now())',
    'select ash.report(now() - interval ''1 hour'', now())',
    'select * from ash.chart(now() - interval ''1 hour'', now())'
  ] loop
    begin
      execute v_reader;
      raise exception 'reader accepted unsupported legacy weighting: %', v_reader;
    exception when sqlstate '55000' then
      assert sqlerrm like '%unsupported legacy sample_interval%', sqlerrm;
    end;
  end loop;
  perform ash.status();
  select count(*) as errors into v_actual from ash.start() where job_type = 'error';
  assert v_actual.errors = 1, 'start() accepted unsupported legacy cadence';
  begin
    perform ash.start(interval '1 second');
    raise exception 'legacy history was reweighted to one second';
  exception when sqlstate '55000' then
    assert sqlerrm like '%retained history%', sqlerrm;
  end;

  /* Explicit fixture reset models an operator starting a new collection. */
  truncate ash.sample, ash.rollup_1m, ash.rollup_1h;
  perform ash.start(interval '1 second');
  assert (select sample_interval from ash.config) = interval '1 second',
    'empty installation could not leave unsupported legacy cadence';
  raise notice 'Issue #137 legacy cadence assertions PASSED';
end
$cadence_legacy$;
