\set ON_ERROR_STOP on

begin;
set local timezone = 'UTC';

do $$
declare
  v_hour int4 := ash.ts_from_timestamptz(date_trunc('hour', now() - interval '3 days'));
  v_expired int4 := ash.ts_from_timestamptz(date_trunc('hour', now() - interval '8 days'));
  v_initialized boolean;
  v_minutes int4[];
begin
  -- Cover first-run catch-up and catch-up with an existing hourly watermark.
  foreach v_initialized in array array[false, true] loop
    truncate ash.rollup_1m, ash.rollup_1h;
    update ash.config
    set rollup_1m_retention_days = 2,
      rollup_1h_retention_days = 7,
      last_rollup_1m_ts = v_hour + 3600,
      last_rollup_1h_ts = case when v_initialized then v_hour end
    where singleton;

    insert into ash.rollup_1m (ts, datid, samples, peak_backends, wait_counts, query_counts)
    values
      (v_expired, 0, 1, 1, array[1, 1], array[42, 1]::int8[]),
      (v_hour, 0, 1, 1, array[1, 1], array[42, 1]::int8[]),
      (v_hour + 60, 0, 2, 2, array[1, 3], array[42, 3]::int8[]);

    assert ash.rollup_cleanup() = 'cleanup: deleted 1 minute rows, 0 hourly rows',
      'cleanup must keep pending input within hourly retention';
    assert (select array_agg(ts order by ts) from ash.rollup_1m)
      = array[v_hour, v_hour + 60];
    assert (select count(*) from ash.rollup_1h) = 0;
    assert (select last_rollup_1h_ts from ash.config)
      is not distinct from case when v_initialized then v_hour end;

    assert ash.rollup_hour() = 1;
    v_minutes := array_fill(null::int4, array[60]);
    v_minutes[1] := 1;
    v_minutes[2] := 3;
    assert (select count(*) from ash.rollup_1h) = 1;
    assert (select ts = v_hour and samples = 3 and peak_backends = 2
      and wait_counts = array[1, 4] and query_counts = array[42, 4]::int8[]
      and minute_counts = v_minutes from ash.rollup_1h),
      'hourly catch-up must preserve exact counts and minute detail';
    assert (select last_rollup_1h_ts from ash.config) = v_hour + 3600;

    assert ash.rollup_cleanup() = 'cleanup: deleted 2 minute rows, 0 hourly rows';
    assert (select count(*) from ash.rollup_1m) = 0;
    assert (select count(*) from ash.rollup_1h) = 1;
    assert ash.rollup_cleanup() = 'cleanup: deleted 0 minute rows, 0 hourly rows';
    assert ash.rollup_hour() = 0;
  end loop;
end;
$$;

rollback;
