/*
 * ash.report() payload-shape contract.
 *
 * README's "LLM analysis" section ships a prompt that instructs a model to
 * read specific fields of this payload. A prompt naming fields that no longer
 * exist is worse than no prompt: the model reports on absent data instead of
 * failing loudly. These asserts pin the exact shape the prompt depends on, so
 * a contract change breaks CI rather than the prompt.
 *
 * Shape only -- the value-level asserts live with the seeded fixture in the
 * reader API test. Self-contained: seeds its own minimal sample.
 */
insert into ash.wait_event_map (state, type, event) values
  ('active', 'CPU*', 'CPU*'),
  ('active', 'IO', 'DataFileRead')
  on conflict do nothing;
insert into ash.query_map_0 (query_id) values (4242) on conflict do nothing;

do $$
declare
  v_cpu smallint;
  v_io smallint;
  v_q int4;
  v_ts int4;
  v_from timestamptz;
  j jsonb;
  v_keys text;
  v_grain text;
begin
  select id into v_cpu from ash.wait_event_map where type = 'CPU*';
  select id into v_io from ash.wait_event_map where event = 'DataFileRead';
  select id into v_q from ash.query_map_0 where query_id = 4242;

  -- Minute-aligned, 3 minutes back, so the window is covered by rollup_1m.
  v_ts := (ash.ts_from_timestamptz(
             date_trunc('minute', now() - interval '3 minutes')) / 60) * 60;
  v_from := ash.ts_to_timestamptz(v_ts);
  insert into ash.sample (sample_ts, datid, active_count, data, slot)
    values (v_ts, 1, 2, array[-v_cpu, 1, v_q, -v_io, 1, v_q]::integer[], 0);
  perform ash.rollup_minute();

  j := ash.report(v_from, now());
  assert j is not null, 'report_contract: report returned null on a seeded window';

  -- Every field the README prompt tells a model to read must be present.
  assert j ?& array[
           'aas_avg', 'aas_worst1m', 'aas_p99', 'aas_p999',
           'top_events_worst1m', 'top_events_p99', 'top_events_p999',
           'top_queryids_available', 'coverage'],
    format('report_contract: payload is missing fields the README prompt '
           'depends on; got %s',
      (select string_agg(k, ',' order by k) from jsonb_object_keys(j) k));

  -- The AAS objects are keyed by total plus the five wait classes the prompt
  -- explains. Exact key set, so an added or dropped class is caught.
  foreach v_grain in array array['aas_avg', 'aas_worst1m', 'aas_p99', 'aas_p999']
  loop
    assert jsonb_typeof(j -> v_grain) = 'object',
      format('report_contract: %s must be a jsonb object, got %s',
        v_grain, jsonb_typeof(j -> v_grain));
    select string_agg(k, ',' order by k) into v_keys
      from jsonb_object_keys(j -> v_grain) k;
    assert v_keys = 'cpu,io,ipc,lock,lwlock,total',
      format('report_contract: %s keys must be cpu,io,ipc,lock,lwlock,total, '
             'got %s', v_grain, v_keys);
  end loop;

  -- coverage is what the prompt uses to decide whether to trust a verdict.
  assert jsonb_typeof(j -> 'coverage') = 'object',
    'report_contract: coverage must be a jsonb object';
  select string_agg(k, ',' order by k) into v_keys
    from jsonb_object_keys(j -> 'coverage') k;
  assert v_keys
    = 'from,minutes_expected,minutes_with_data,raw_retention_start,source,to',
    format('report_contract: coverage keys changed, got %s', v_keys);

  -- vcpus drives the whole capacity verdict, so both directions matter: it is
  -- echoed exactly when supplied, and absent when not, so a consumer can tell
  -- "no core count given" from a fabricated one.
  assert not (j ? 'vcpus'),
    'report_contract: vcpus must be absent when it is not supplied';
  assert (ash.report(v_from, now(), vcpus => 16) ->> 'vcpus')::int = 16,
    'report_contract: vcpus must be echoed exactly when supplied';

  raise notice 'ash.report payload contract PASSED';
end;
$$;
