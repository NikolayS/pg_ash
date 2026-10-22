-- Invalid reader inputs must fail with an API error before integer narrowing.
begin;
do $$
declare
  reader text;
  oversized text;
  command text;
  message text;
  failures text[] := array[]::text[];
begin
  foreach reader in array array['aas', 'timeline', 'top', 'chart'] loop
    foreach oversized in array array['69 years', '100000 years', '2147483647.1 seconds'] loop
      command := format('select * from ash.%I(%sbucket => %L::interval)',
        reader, case when reader = 'top' then '''wait_event'', ' else '' end,
        oversized);
      begin
        execute command;
        raise exception 'accepted oversized bucket: %', command;
      exception when invalid_parameter_value then
        get stacked diagnostics message = message_text;
        assert message like 'ash.' || reader || ': bucket%', message;
      when others then
        failures := array_append(failures, command || ': ' || sqlstate || ' ' || sqlerrm);
      end;
    end loop;
  end loop;
  begin
    perform * from ash.top(null);
    raise exception 'accepted NULL dimension';
  exception when raise_exception then
    get stacked diagnostics message = message_text;
    if message not like 'ash.top: unknown dimension%' then
      failures := array_append(failures, message);
    end if;
  end;
  assert cardinality(failures) = 0, array_to_string(failures, E'\n');
  -- Existing automatic bucket and large, representable bucket inputs stay valid.
  perform * from ash.timeline(bucket => null);
  perform * from ash.chart(bucket => null);
  perform * from ash.aas(bucket => interval '68 years');
  perform * from ash.timeline(bucket => interval '68 years');
  perform * from ash.top('wait_event', bucket => interval '68 years');
  perform * from ash.chart(bucket => interval '68 years');
end;
$$;
rollback;
