\set ON_ERROR_STOP on

-- PostgreSQL 19 native Buffer wait-event class compatibility:
--   * ash.report() exposes an additive "buffer" class (BufferShared,
--     BufferShareExclusive, BufferExclusive, BufferCleanup) and includes it in
--     "total" metrics and query attribution.
--   * Legacy LWLock:BufferContent stays in "lwlock"; legacy BufferPin stays
--     outside the report's selected classes.
--   * ash._wait_color() maps Buffer:* to the same teal RGB as BufferPin:*.
--   * On PostgreSQL 19+, verify the live pg_wait_events catalog exposes the
--     native Buffer wait events.
begin;
truncate ash.sample, ash.rollup_1m, ash.rollup_1h;

do $$
declare
  v_id smallint;
  v_old smallint;
  v_cleanup smallint;
  v_pin smallint;
  v_q int4;
  v_ts int4 := ash.ts_from_timestamptz(
    date_trunc('minute', now(), 'UTC') - interval '2 minutes'
  );
  v_event text;
  v_metric text;
  j jsonb;
begin
  update ash.config
  set sample_interval = interval '1 second',
      current_slot = 0,
      rotated_at = now()
  where singleton;

  insert into ash.query_map_0 (query_id) values (1900019)
    on conflict (query_id) do nothing;
  select id into v_q from ash.query_map_0 where query_id = 1900019;

  if current_setting('server_version_num')::int >= 190000 then
    foreach v_event in array array[
      'BufferCleanup', 'BufferShared', 'BufferShareExclusive', 'BufferExclusive'
    ] loop
      assert exists (
        select
        from pg_catalog.pg_wait_events
        where type = 'Buffer' and name = v_event
      ), 'PG19 pg_wait_events missing native Buffer event: ' || v_event;
    end loop;
  end if;

  v_old := ash._register_wait('active', 'LWLock', 'BufferContent');
  v_cleanup := ash._register_wait('active', 'Buffer', 'BufferCleanup');
  v_pin := ash._register_wait('active', 'BufferPin', 'BufferPin');

  foreach v_event in array array[
    'BufferShared', 'BufferShareExclusive', 'BufferExclusive'
  ] loop
    v_id := ash._register_wait('active', 'Buffer', v_event);
    truncate ash.rollup_1m, ash.sample;
    insert into ash.sample (sample_ts, datid, active_count, data, slot)
    values (
      v_ts, 1, 240,
      array[-v_id::int4, 60] || array_fill(v_q, array[60])
        || array[-v_cleanup::int4, 60] || array_fill(v_q, array[60])
        || array[-v_old::int4, 60] || array_fill(v_q, array[60])
        || array[-v_pin::int4, 60] || array_fill(v_q, array[60]),
      0
    );
    insert into ash.rollup_1m (
      ts, datid, samples, peak_backends, wait_counts, query_counts
    ) values (
      v_ts, 1, 60, 4,
      array[v_id, 60, v_old, 60, v_cleanup, 60, v_pin, 60]::int4[],
      '{}'::int8[]
    );

    j := ash.report(
      ash.ts_to_timestamptz(v_ts),
      ash.ts_to_timestamptz(v_ts + 60)
    );
    foreach v_metric in array array[
      'aas_avg', 'aas_worst1m', 'aas_p99', 'aas_p999'
    ] loop
      assert (j->v_metric->>'buffer')::numeric = 2,
        v_metric || ': buffer content waits lost';
      assert (j->v_metric->>'lwlock')::numeric = 1,
        v_metric || ': legacy LWLock changed';
      assert (j->v_metric->>'total')::numeric = 3,
        v_metric || ': total loses native buffer waits';
    end loop;

    assert j->'top_events_worst1m'->'buffer' @> jsonb_build_array(v_event || '(1.0)'),
      'native PG19 event name missing from top_events_worst1m';
    assert j->'top_events_worst1m'->'buffer' @> '["BufferCleanup(1.0)"]'::jsonb,
      'BufferCleanup missing from top_events_worst1m';
    assert j->'top_events_p99'->'buffer' @> jsonb_build_array(v_event || '(1.0)'),
      'native PG19 event name missing from top_events_p99';
    assert j->'top_events_p999'->'buffer' @> jsonb_build_array(v_event || '(1.0)'),
      'native PG19 event name missing from top_events_p999';

    foreach v_metric in array array[
      'top_queryids_worst1m', 'top_queryids_p99', 'top_queryids_p999'
    ] loop
      assert j->v_metric->'buffer' = '["1900019(2.0)"]'::jsonb,
        v_metric || ': buffer attribution mismatch';
      assert j->v_metric->'total' = '["1900019(3.0)"]'::jsonb,
        v_metric || ': total attribution mismatch';
    end loop;

    assert ash._hr_top_queryids('Buffer', array[v_ts], 3, 1) = array['1900019(2.0)'],
      'buffer query attribution helper mismatch';
    assert ash._hr_top_queryids(null, array[v_ts], 3, 1) = array['1900019(3.0)'],
      'total query attribution helper mismatch';
    assert ash._wait_color('Buffer:' || v_event, true) = E'\033[38;2;000;210;180m',
      'Buffer color mismatch';
    assert ash._wait_color('Buffer:' || v_event, false) = '',
      'Buffer color disabled mismatch';
  end loop;

  assert ash._wait_color('Buffer:BufferCleanup', true)
         = ash._wait_color('BufferPin:BufferPin', true),
    'Buffer:BufferCleanup color must match BufferPin teal';

  raise notice 'PostgreSQL 19 Buffer wait compatibility PASSED';
end $$;

rollback;
