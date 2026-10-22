-- #269: slot selection must run once per raw reader, not once per sample.
-- Small behavioral fixture, not a timing benchmark. All fixture changes roll back.
\set ON_ERROR_STOP on
begin;
set local track_functions = 'pl';

update ash.config
set sampling_enabled = false,
    current_slot = 0,
    num_partitions = 3,
    rotation_period = interval '1 day',
    rotated_at = date_trunc('minute', now()),
    sample_interval = interval '1 second',
    last_rollup_1m_ts = null,
    last_rollup_1h_ts = null
where singleton;
truncate ash.sample_0, ash.sample_1, ash.sample_2;
truncate ash.query_map_0, ash.query_map_1, ash.query_map_2;
truncate ash.rollup_1m, ash.rollup_1h;

do $$
declare
  v_start int4 := ash.ts_from_timestamptz(
    date_trunc('minute', now()) - interval '10 minutes');
  v_wait smallint;
  v_map int4;
  v_datid oid;
  v_mode text;
  v_call int4;
  v_before bigint;
  v_after bigint;
  v_result record;
begin
  v_wait := ash._register_wait('active', 'Timeout', 'PgSleep');
  insert into ash.query_map_0 (query_id)
  values (269269)
  returning id into v_map;
  select db.oid into strict v_datid
  from pg_database as db
  where db.datname = current_database();

  insert into ash.sample (sample_ts, datid, active_count, data, slot)
  select v_start + sample_second, v_datid, 2,
         array[-v_wait, 2, v_map, v_map]::int4[], 0
  from generate_series(0, 11) as fixture(sample_second);

  foreach v_mode in array array[
    'force_custom_plan', 'force_generic_plan', 'auto'
  ] loop
    perform set_config('plan_cache_mode', v_mode, true);
    -- Repeated calls exercise cached plans as well as the first invocation.
    for v_call in 1..8 loop
      select coalesce(sum(calls), 0) into v_before
      from pg_stat_xact_user_functions
      where funcid =
        'ash._active_slots_for_at(timestamptz,timestamptz)'::regprocedure;

      select * into strict v_result
      from ash.top(
        'query_id',
        since => ash.ts_to_timestamptz(v_start),
        until => ash.ts_to_timestamptz(v_start + 60),
        wait_event_type => 'Timeout',
        database => current_database(),
        n => 100
      );

      select coalesce(sum(calls), 0) into v_after
      from pg_stat_xact_user_functions
      where funcid =
        'ash._active_slots_for_at(timestamptz,timestamptz)'::regprocedure;

      assert v_result.key = '269269' and v_result.source = 'raw'
        and v_result.backend_seconds = 24 and v_result.avg_aas = 0.4,
        format('unexpected raw result in %s call %s: %s',
          v_mode, v_call, row_to_json(v_result));
      assert v_after - v_before = 2,
        format('%s call %s: expected 2 slot selections, observed %s',
          v_mode, v_call, v_after - v_before);
    end loop;
  end loop;
  raise notice 'raw slot selection call counts and results PASSED';
end;
$$;
rollback;
