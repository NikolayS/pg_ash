-- Invalid reader inputs must fail with an API error before integer narrowing.
begin;
do $$
declare
  reader text;
  oversized text;
  command text;
  message text;
begin
  foreach reader in array array['aas', 'timeline', 'top', 'chart'] loop
    foreach oversized in array array['69 years', '100000 years', '2147483647.1 seconds',
        '2147483647 seconds', '2147483640.1 seconds'] loop
      command := format('select * from ash.%I(%sbucket => %L::interval)',
        reader, case when reader = 'top' then '''wait_event'', ' else '' end,
        oversized);
      begin
        execute command;
        raise exception 'accepted oversized bucket: %', command;
      exception when invalid_parameter_value then
        get stacked diagnostics message = message_text;
        assert message like 'ash.' || reader || ': bucket%', message;
      end;
    end loop;
  end loop;
  begin
    perform * from ash.top(null);
    raise exception 'accepted NULL dimension';
  exception when raise_exception then
    get stacked diagnostics message = message_text;
    assert message like 'ash.top: unknown dimension%', message;
  end;
  foreach reader in array array['aas', 'timeline', 'top', 'chart'] loop
    execute format('select * from ash.%I(%sbucket => interval ''2147483640 seconds'')',
      reader, case when reader = 'top' then '''wait_event'', ' else '' end);
  end loop;
  -- Existing automatic bucket and large, representable bucket inputs stay valid.
  perform * from ash.timeline(bucket => null);
  perform * from ash.chart(bucket => null);
end;
$$;
-- A flat hourly source has a different maximum aligned bucket.
do $$
declare
  hour_start timestamptz := date_trunc('hour', now()) - interval '45 days';
  wait_id smallint;
  reader text;
  command text;
  message text;
begin
  wait_id := ash._register_wait('active', 'CPU*', 'CPU*');
  insert into ash.rollup_1h (
    ts, datid, samples, peak_backends, wait_counts, query_counts, minute_counts
  ) values (
    ash.ts_from_timestamptz(hour_start), 0::oid, 3600, 1,
    array[wait_id, 3600]::int4[], '{}'::int8[], null
  );
  foreach reader in array array['aas', 'timeline', 'top', 'chart'] loop
    command := format('select * from ash.%I(%ssince => %L, until => %L, bucket => ',
      reader, case when reader = 'top' then '''wait_event'', ' else '' end,
      hour_start, hour_start + interval '1 hour');
    execute command || 'interval ''2147482800 seconds'')';
    begin
      execute command || 'interval ''2147482800.1 seconds'')';
      raise exception 'accepted overflowing hourly bucket: %', reader;
    exception when invalid_parameter_value then
      get stacked diagnostics message = message_text;
      assert message like 'ash.' || reader || ': bucket%', message;
    end;
  end loop;
  assert (select avg_aas = 1 and backend_seconds = 3600
          from ash.aas(hour_start, hour_start + interval '1 hour',
                       bucket => interval '2147482800 seconds'));
end;
$$;
-- A valid large bucket can end beyond int4 even when its data does not.
do $$
declare
  wait_id smallint := ash._register_wait('active', 'CPU*', 'CPU*');
  window_start timestamptz := ash.ts_to_timestamptz(1200000000);
  window_end timestamptz := ash.ts_to_timestamptz(1200000060);
  large_bucket interval := interval '1100000040 seconds';
begin
  insert into ash.rollup_1m(ts, datid, samples, peak_backends, wait_counts, query_counts)
  select 1200000000, oid, 60, 1, array[wait_id, 60], '{}'::int8[]
  from pg_database where datname = current_database();
  assert (select avg_aas = 1 and peak_aas = 1 and backend_seconds = 60
          from ash.aas(window_start, window_end, bucket => large_bucket));
  assert (select count(*) = 1 and bool_and(avg_aas = 1 and data_points = 1)
          from ash.timeline(window_start, window_end, bucket => large_bucket));
  assert (select count(*) = 1 and bool_and(avg_aas = 1 and peak_aas = 1 and pct = 100)
          from ash.top('wait_event', window_start, window_end, bucket => large_bucket));
  assert (select count(*) = 1 and bool_and(aas = 1)
          from ash.chart(window_start, window_end, bucket => large_bucket)
          where bucket_start is not null);
end;
$$;
rollback;
