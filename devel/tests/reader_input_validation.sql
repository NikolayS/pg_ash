-- Invalid reader inputs must fail with an API error before integer narrowing.
begin;
-- #307: scalar minute detail must not hide dimensional hourly overflow.
do $$
declare
  hour_start timestamptz := date_trunc('hour', now()) - interval '45 days';
  wait_id smallint := ash._register_wait('active', 'CPU*', 'CPU*');
  dimension_name text;
  message text;
  result record;
begin
  truncate ash.sample, ash.rollup_1m, ash.rollup_1h;
  insert into ash.rollup_1h (
    ts, datid, samples, peak_backends, wait_counts, query_counts, minute_counts
  )
  select ash.ts_from_timestamptz(hour_start), oid, 3600, 1,
         array[wait_id, 3600], array[307, 3600]::bigint[], array_fill(60, array[60])
  from pg_database where datname = current_database();
  assert (select ash._minute_counts_valid(minute_counts, wait_counts) from ash.rollup_1h);
  foreach dimension_name in array array['wait_event', 'wait_event_type', 'query_id'] loop
    select * into strict result from ash.compare(
      hour_start, hour_start + interval '1 hour',
      hour_start, hour_start + interval '1 hour',
      dimension => dimension_name, bucket => interval '2147482800 seconds');
    assert result.source_1 = 'rollup_1h' and result.source_2 = 'rollup_1h'
       and result.avg_aas_1 = 1 and result.avg_aas_2 = 1 and result.avg_delta = 0,
      format('large valid hourly bucket changed results: %s', row_to_json(result));
    begin
      perform * from ash.compare(
        hour_start, hour_start + interval '1 hour',
        hour_start, hour_start + interval '1 hour',
        dimension => dimension_name, bucket => interval '2147483640 seconds');
      raise exception 'accepted overflowing dimensional bucket: %', dimension_name;
    exception when invalid_parameter_value then
      get stacked diagnostics message = message_text;
      assert message like 'ash.compare: bucket exceeds the supported range after grain alignment%', message;
    end;
  end loop;
  -- Scalar and database reads retain genuine minute detail and its larger bound.
  foreach dimension_name in array array[null::text, 'database'] loop
    select * into strict result from ash.compare(
      hour_start, hour_start + interval '1 hour',
      hour_start, hour_start + interval '1 hour',
      dimension => dimension_name, bucket => interval '2147483640 seconds');
    assert result.avg_aas_1 = 1 and result.avg_aas_2 = 1 and result.avg_delta = 0,
      format('valid minute detail changed results: %s', row_to_json(result));
  end loop;
  truncate ash.rollup_1h;
end;
$$;
do $$
declare
  reader text;
  oversized text;
  command text;
  message text;
begin
  foreach reader in array array['aas', 'timeline', 'top', 'chart', 'compare'] loop
    foreach oversized in array array['69 years', '100000 years', '2147483647.1 seconds',
        '2147483647 seconds', '2147483640.1 seconds'] loop
      command := format('select * from ash.%I(%sbucket => %L::interval)',
        reader, case when reader = 'top' then '''wait_event'', '
        when reader = 'compare' then 'null, null, null, null, dimension => ''wait_event'', '
        else '' end,
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
  foreach reader in array array['aas', 'timeline', 'top', 'chart', 'compare'] loop
    execute format('select * from ash.%I(%sbucket => interval ''2147483640 seconds'')',
      reader, case when reader = 'top' then '''wait_event'', '
        when reader = 'compare' then 'null, null, null, null, dimension => ''wait_event'', '
        else '' end);
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
  foreach reader in array array['aas', 'timeline', 'top', 'chart', 'compare'] loop
    if reader = 'compare' then
      command := format('select * from ash.compare(%L, %L, %L, %L, '
        'dimension => ''wait_event'', bucket => ',
        hour_start, hour_start + interval '1 hour',
        hour_start, hour_start + interval '1 hour');
    else
      command := format('select * from ash.%I(%ssince => %L, until => %L, bucket => ',
        reader, case when reader = 'top' then '''wait_event'', ' else '' end,
        hour_start, hour_start + interval '1 hour');
    end if;
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
