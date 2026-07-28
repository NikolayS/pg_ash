begin;

-- When raw covers a wide aggregate window, rollup_1m may replace it
-- only if that rollup covers the requested end. Otherwise a lagging
-- worker would silently drop recent raw load. Completeness for a
-- stalled rollup older than physical raw coverage remains #122.
-- Conversely, a healthy rollup must keep the fast path when a
-- dashboard rounds until up to the next minute boundary.
do $$
declare
  v_wait smallint;
  v_start timestamptz := date_trunc('minute', now()) - interval '2 hours';
  v_end timestamptz := date_trunc('minute', now());
  v_historical_end timestamptz := v_end - interval '30 minutes';
  v_start_ts int4;
  v_recent_ts int4;
  a record;
begin
  truncate ash.sample_0, ash.sample_1, ash.sample_2;
  truncate ash.rollup_1m, ash.rollup_1h;
  update ash.config
  set current_slot = 0,
      num_partitions = 3,
      sample_interval = interval '1 second',
      rotation_period = interval '1 day',
      rollup_1m_retention_days = 30,
      rotated_at = v_end,
      last_rollup_1m_ts = ash.ts_from_timestamptz(
        v_start + interval '1 minute'
      ),
      last_rollup_1h_ts = null
  where singleton;

  select ash._register_wait('active', 'IO', 'StaleRollupProof')
  into v_wait;
  v_start_ts := ash.ts_from_timestamptz(v_start);
  v_recent_ts := ash.ts_from_timestamptz(v_end - interval '1 minute');

  insert into ash.rollup_1m (
    ts, datid, samples, peak_backends, wait_counts, query_counts
  ) values (
    v_start_ts, 0::oid, 1, 1,
    array[v_wait, 1]::int4[], '{}'::int8[]
  );
  insert into ash.sample (
    sample_ts, datid, active_count, data, slot
  ) values
    (v_start_ts, 0::oid, 1, array[-v_wait, 1, 0]::int4[], 0),
    (v_recent_ts, 0::oid, 1, array[-v_wait, 1, 0]::int4[], 0);

  select * into a from ash.aas(v_start, v_end);
  assert a.source = 'raw',
    'stale rollup must fall back to raw, got ' || a.source;
  assert a.buckets_with_data = 2,
    'raw fallback should retain both covered minutes, got '
    || a.buckets_with_data;
  assert a.backend_seconds = 2.00,
    'raw fallback should retain both backend-seconds, got '
    || a.backend_seconds;

  truncate ash.rollup_1m;
  insert into ash.rollup_1m (
    ts, datid, samples, peak_backends, wait_counts, query_counts
  )
  select
    ash.ts_from_timestamptz(minute_ts),
    0::oid,
    1,
    1,
    array[v_wait, 1]::int4[],
    '{}'::int8[]
  from generate_series(
    v_start,
    v_end - interval '1 minute',
    interval '1 minute'
  ) as minute_ts;
  update ash.config
  set last_rollup_1m_ts = ash.ts_from_timestamptz(v_historical_end)
  where singleton;

  select * into a from ash.aas(v_start, v_historical_end);
  assert a.source = 'rollup_1m',
    'healthy historical rollup must use its covered watermark, got '
    || a.source;
  assert a.buckets_with_data = 90,
    'historical rollup should cover 90 complete minutes, got '
    || a.buckets_with_data;
  assert a.backend_seconds = 90.00,
    'historical rollup should retain 90 backend-seconds, got '
    || a.backend_seconds;

  update ash.config
  set last_rollup_1m_ts = ash.ts_from_timestamptz(v_end)
  where singleton;
  select * into a
  from ash.aas(v_start, v_end + interval '1 minute');
  assert a.source = 'rollup_1m',
    'healthy rollup must keep the fast path for a rounded-up until, '
    'got ' || a.source;
  assert a.buckets_with_data = 120,
    'healthy rollup should cover 120 complete minutes, got '
    || a.buckets_with_data;
  assert a.backend_seconds = 120.00,
    'healthy rollup should retain 120 backend-seconds, got '
    || a.backend_seconds;

  raise notice 'stale/rounded-up rollup source selection PASSED';
end $$;

rollback;
