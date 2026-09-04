\set ON_ERROR_STOP on

-- Isolated synthetic evidence for the public prompt's shape and semantics.
-- Roll back fixture data, configuration, and dictionaries after the test.
begin;
truncate ash.sample, ash.rollup_1m, ash.rollup_1h;
update ash.config set sample_interval = interval '1 second' where singleton;

do $$
declare
  v_io smallint;
  v_lock smallint;
  v_client smallint;
  v_query int4;
  v_start int4 := ash.ts_from_timestamptz(
    date_trunc('minute', now()) - interval '3 minutes');
  v_from timestamptz;
  v_until timestamptz;
  v_report jsonb;
  v_field text;
begin
  v_io := ash._register_wait('active', 'IO', 'ReportContractIO');
  v_lock := ash._register_wait('active', 'Lock', 'ReportContractLock');
  v_client := ash._register_wait('active', 'Client', 'ReportContractClient');
  insert into ash.query_map_0 (query_id) values (424242)
    on conflict (query_id) do update set query_id = excluded.query_id
    returning id into v_query;
  v_from := ash.ts_to_timestamptz(v_start);
  v_until := ash.ts_to_timestamptz(v_start + 180);

  -- Two observed minutes, one absent minute; distinct IO and Lock peaks.
  -- Client appears in storage but is outside report.total's five-class scope.
  insert into ash.rollup_1m (
    ts, datid, samples, peak_backends, wait_counts, query_counts
  ) values
    (v_start, 0::oid, 1, 720,
      array[v_io, 120, v_client, 600], array[424242, 720]::bigint[]),
    (v_start + 60, 0::oid, 1, 180,
      array[v_lock, 180], array[424242, 180]::bigint[]);
  insert into ash.sample (sample_ts, datid, active_count, data, slot)
  values
    (v_start, 0::oid, 720,
      array[-v_io, 120] || array_fill(v_query, array[120]) ||
      array[-v_client, 600] || array_fill(v_query, array[600]), 0),
    (v_start + 60, 0::oid, 180,
      array[-v_lock, 180] || array_fill(v_query, array[180]), 0);

  v_report := ash.report(v_from, v_until);
  assert v_report is not null, 'report requires the seeded minute rollups';
  assert v_report ?& array[
    'aas_avg', 'aas_worst1m', 'aas_p99', 'aas_p999',
    'top_events_worst1m', 'top_events_p99', 'top_events_p999',
    'top_queryids_available', 'coverage'
  ], 'missing a documented required report field';
  foreach v_field in array array[
    'aas_avg', 'aas_worst1m', 'aas_p99', 'aas_p999'
  ] loop
    assert jsonb_typeof(v_report -> v_field) = 'object',
      'AAS statistics must remain objects';
    assert (v_report -> v_field) ?& array[
      'total', 'cpu', 'io', 'ipc', 'lock', 'lwlock'
    ], 'missing a documented AAS class';
  end loop;
  assert (v_report -> 'coverage') ?& array[
    'from', 'to', 'source', 'minutes_expected', 'minutes_with_data',
    'raw_retention_start'
  ], 'missing a documented coverage field';
  -- Presence checks deliberately accept additive keys in the stable contract.
  assert v_report #>> '{coverage,source}' = 'rollup_1m';
  assert (v_report #>> '{coverage,minutes_expected}')::int = 3;
  assert (v_report #>> '{coverage,minutes_with_data}')::int = 2;
  assert (v_report #>> '{aas_avg,total}')::numeric = 2.5,
    'report average uses observed minutes and excludes Client';
  assert (v_report #>> '{aas_worst1m,total}')::numeric = 3;
  assert (v_report #>> '{aas_worst1m,io}')::numeric = 2;
  assert (v_report #>> '{aas_worst1m,lock}')::numeric = 3,
    'class maxima are independent, not a decomposition of total peak';
  assert (v_report ->> 'top_queryids_available')::bool,
    'raw query IDs must be available independently of pg_stat_statements';
  assert not (v_report ? 'vcpus'), 'omitted vcpus must remain absent';
  assert (ash.report(v_from, v_until, vcpus => 8) ->> 'vcpus')::int = 8,
    'vcpus is caller-supplied metadata';

  truncate ash.sample;
  v_report := ash.report(v_from, v_until);
  assert not (v_report ->> 'top_queryids_available')::bool,
    'minute rollups alone cannot provide extreme-minute raw attribution';
  assert (v_report #>> '{aas_avg,total}')::numeric = 2.5,
    'losing raw attribution must not remove minute-rollup base metrics';

  truncate ash.rollup_1m;
  assert ash.report(v_from, v_until) is null,
    'no minute-rollup observations means NULL, not a zero-load report';
  raise notice 'report prompt contract PASSED';
end $$;
rollback;
