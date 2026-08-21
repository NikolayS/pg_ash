/* -------------------------------------------------------------------------
 * v1.4 rollup tables and functions
 *
 * Extracted from .github/workflows/test.yml. GitHub Actions silently refuses
 * to create a workflow run when the workflow file exceeds 512,000 bytes, and
 * test.yml sits just under that ceiling, so large test bodies live here and
 * are invoked with `psql --file` from a short workflow step. Same assertions,
 * same order; only the transport changed.
 *
 * Run standalone against a prepared database with:
 *   psql -v ON_ERROR_STOP=1 -f devel/tests/rollup_tables_v14.sql
 * ------------------------------------------------------------------------- */
do $$
declare
  v_count int;
  v_result int;
  v_result_text text;
  v_minute_ts int4;
  v_sample_ts int4;
  v_wait_id smallint;
begin
  -- === Rollup tables exist ===
  assert exists (select from pg_tables
    where schemaname = 'ash' and tablename = 'rollup_1m'),
    'rollup_1m table missing';
  assert exists (select from pg_tables
    where schemaname = 'ash' and tablename = 'rollup_1h'),
    'rollup_1h table missing';

  -- === Config columns for rollup ===
  assert (select rollup_1m_retention_days from ash.config where singleton) = 30,
    'rollup_1m_retention_days default should be 30';
  assert (select rollup_1h_retention_days from ash.config where singleton) = 1825,
    'rollup_1h_retention_days default should be 1825';
  assert (select rollup_min_backend_seconds from ash.config where singleton) = 3,
    'rollup_min_backend_seconds default should be 3';

  -- Stop any pg_cron jobs from prior tests that may inject samples
  perform ash.stop();
  -- Clear data from prior tests (rotate() calls rollup_minute internally,
  -- and rotation/sampler tests insert raw samples).
  -- TRUNCATE parent cascades to all child partitions.
  truncate ash.sample;
  truncate ash.rollup_1m;
  truncate ash.rollup_1h;
  -- Also clear query_map tables (rebuild may have reset them)
  truncate ash.query_map_0;
  truncate ash.query_map_1;
  truncate ash.query_map_2;
  update ash.config
  set last_rollup_1m_ts = null, last_rollup_1h_ts = null
  where singleton;

  -- === Rollup with no data returns 0 ===
  select ash.rollup_minute() into v_result;
  assert v_result = 0, 'rollup_minute with no data should return 0';

  select ash.rollup_hour() into v_result;
  assert v_result = 0, 'rollup_hour with no data should return 0';

  -- === Rollup cleanup with no data ===
  select ash.rollup_cleanup() into v_result_text;
  assert v_result_text like 'cleanup:%',
    'rollup_cleanup should return cleanup message';

  -- === Seed data and test rollup ===
  -- Register a wait event
  select ash._register_wait('active', 'CPU*', 'CPU*') into v_wait_id;

  -- Insert a query_map entry
  insert into ash.query_map_0 (query_id) values (12345)
  on conflict (query_id) do nothing;

  -- Get a minute-aligned ts 2 minutes ago (so it's a complete minute)
  v_minute_ts := (ash.ts_from_timestamptz(
    date_trunc('minute', now() - interval '2 minutes')
  ) / 60) * 60;

  -- Insert sample data for that minute
  for i in 0 .. 2 loop
    v_sample_ts := v_minute_ts + i * 20;  -- 3 samples, 20s apart
    insert into ash.sample (sample_ts, datid, active_count, data, slot)
    values (
      v_sample_ts,
      0::oid,
      2::smallint,
      array[-v_wait_id, 2, 1, 0]::integer[],
      0::smallint
    );
  end loop;

  -- Clear watermark to force processing from start
  update ash.config set last_rollup_1m_ts = null where singleton;

  -- Run rollup_minute
  select ash.rollup_minute() into v_result;
  assert v_result > 0,
    'rollup_minute should process seeded data, got ' || v_result;

  -- Verify rollup_1m has data
  select count(*) into v_count from ash.rollup_1m;
  assert v_count > 0,
    'rollup_1m should have rows after rollup_minute';

  -- Verify rollup_1m content — exact values
  assert exists (
    select from ash.rollup_1m
    where ts = v_minute_ts and datid = 0::oid
  ), 'rollup_1m should have row for seeded minute';

  -- Verify exact aggregated values
  declare
    v_r record;
  begin
    select * into v_r from ash.rollup_1m
    where ts = v_minute_ts and datid = 0::oid;

    assert v_r.samples = 3,
      'rollup samples should be 3, got ' || v_r.samples;
    assert v_r.peak_backends = 2,
      'rollup peak_backends should be 2, got ' || v_r.peak_backends;
    assert array_length(v_r.wait_counts, 1) >= 2,
      'wait_counts should have at least one pair';
    assert v_r.wait_counts[1] = v_wait_id,
      'wait_counts[1] should be wait_id ' || v_wait_id || ', got ' || v_r.wait_counts[1];
  end;

  -- Verify watermark advanced
  assert (select last_rollup_1m_ts from ash.config where singleton) is not null,
    'last_rollup_1m_ts should be set after rollup';

  -- === Test idempotency: re-run should upsert, not duplicate ===
  update ash.config set last_rollup_1m_ts = null where singleton;
  select ash.rollup_minute() into v_result;
  -- Should still have exactly 1 row per minute (upsert)
  select count(*) into v_count from ash.rollup_1m
  where ts = v_minute_ts and datid = 0::oid;
  assert v_count = 1,
    'rollup should be idempotent (upsert), got ' || v_count || ' rows';

  -- === Test rollup_hour ===
  -- Seed minute rollup data for an hour boundary
  declare
    v_hour_ts int4 := (ash.ts_from_timestamptz(
      date_trunc('hour', now() - interval '2 hours')
    ) / 3600) * 3600;
  begin
    insert into ash.rollup_1m (ts, datid, samples, peak_backends, wait_counts, query_counts)
    values
      (v_hour_ts,      0::oid, 60, 5, array[v_wait_id, 120]::int4[], '{12345,60}'::int8[]),
      (v_hour_ts + 60, 0::oid, 60, 3, array[v_wait_id, 100]::int4[], '{12345,40}'::int8[])
    on conflict (ts, datid) do nothing;

    update ash.config
    set last_rollup_1m_ts = v_hour_ts + 3600,
        last_rollup_1h_ts = null
    where singleton;
    select ash.rollup_hour() into v_result;
    assert v_result > 0,
      'rollup_hour should process seeded data';

    -- Verify hourly aggregation — exact values
    declare
      v_hr record;
    begin
      select * into v_hr from ash.rollup_1h
      where ts = v_hour_ts and datid = 0::oid;

      assert v_hr is not null, 'rollup_1h row missing';
      assert v_hr.samples = 120,
        'hourly samples should be 120 (60+60), got ' || v_hr.samples;
      assert v_hr.peak_backends = 5,
        'hourly peak_backends should be 5, got ' || v_hr.peak_backends;
      -- wait_counts should be merged: wait_id with count 220 (120+100)
      assert v_hr.wait_counts[1] = v_wait_id,
        'hourly wait_counts[1] should be wait_id';
      assert v_hr.wait_counts[2] = 220,
        'hourly wait_counts[2] should be 220 (120+100), got ' || v_hr.wait_counts[2];
    end;
  end;

  -- === Regression: wait event under multiple states must not be double-counted ===
  -- ClientRead can appear under both 'active' and 'idle in transaction'.
  -- wait_event_map has UNIQUE (state, type, event) so each (state, type, event)
  -- triple has its own id. rollup_minute() must aggregate by canonical wait_id
  -- (not by text wait_event) — otherwise a single decoded event would match
  -- multiple wait_event_map rows and inflate the count.
  declare
    v_active_cr_id smallint;
    v_idle_cr_id smallint;
    v_seed_wait_id smallint;
    v_cr_minute_ts int4;
    v_row record;
    v_i int;
    v_total_count int4;
  begin
    /*
     * Force [active wait_id, count] = [2, 3] while the excluded
     * idle wait_id is also 3. Pair-array assertions must inspect
     * odd ID positions only; scanning every element confuses the
     * count with an idle wait ID.
     */
    truncate ash.sample;
    truncate ash.rollup_1m;
    truncate ash.wait_event_map restart identity cascade;
    select ash._register_wait('active', 'Test', 'PairCollisionSeed')
      into v_seed_wait_id;
    select ash._register_wait('active', 'Client', 'ClientRead')
      into v_active_cr_id;
    select ash._register_wait('idle in transaction', 'Client', 'ClientRead')
      into v_idle_cr_id;

    assert v_seed_wait_id = 1
           and v_active_cr_id = 2
           and v_idle_cr_id = 3,
      format(
        'pair-collision fixture IDs mismatch: seed=%s active=%s idle=%s',
        v_seed_wait_id,
        v_active_cr_id,
        v_idle_cr_id
      );
    assert v_active_cr_id <> v_idle_cr_id,
      'active/ClientRead and idle/ClientRead must have distinct wait_ids';

    -- Clear sample/rollup and reset watermark so we rollup a fresh minute
    truncate ash.sample;
    truncate ash.rollup_1m;
    update ash.config set last_rollup_1m_ts = null where singleton;

    -- Pick a fresh complete minute 3 minutes ago
    v_cr_minute_ts := (ash.ts_from_timestamptz(
      date_trunc('minute', now() - interval '3 minutes')
    ) / 60) * 60;

    -- Seed 3 samples each referencing ONLY v_active_cr_id (one backend each).
    -- If rollup_minute's join inflates via the second wm row
    -- (same type+event, different state), the stored count would become 6.
    for v_i in 0 .. 2 loop
      insert into ash.sample (sample_ts, datid, active_count, data, slot)
      values (
        v_cr_minute_ts + v_i * 15,
        0::oid,
        1::smallint,
        array[-v_active_cr_id, 1, 0]::integer[],
        0::smallint
      );
    end loop;

    select ash.rollup_minute() into v_result;
    assert v_result > 0, 'rollup_minute should process ClientRead samples';

    select * into v_row from ash.rollup_1m
    where ts = v_cr_minute_ts and datid = 0::oid;

    assert v_row is not null, 'rollup_1m row for ClientRead minute missing';
    assert v_row.samples = 3,
      'samples should be 3, got ' || v_row.samples;

    -- wait_counts is [wait_id, cnt, wait_id, cnt, ...]. Sum every other
    -- element starting from index 2. Total must equal 3 (one backend per
    -- sample), not 6 (which is the double-counted result).
    v_total_count := 0;
    for v_i in 2 .. coalesce(array_length(v_row.wait_counts, 1), 0) by 2 loop
      v_total_count := v_total_count + v_row.wait_counts[v_i];
    end loop;
    assert v_total_count = 3,
      'wait_counts total should be 3 (not double-counted across states), got '
        || v_total_count || ' — wait_counts=' || v_row.wait_counts::text;
    assert v_row.wait_counts = array[v_active_cr_id::int4, 3],
      'wait_counts should preserve the exact ID/count collision {2,3}, got '
        || v_row.wait_counts::text;

    -- Only the 'active' wait_id should appear, never the 'idle' one
    -- (no sample referenced idle in transaction/ClientRead).
    assert not exists (
      select 1
      from generate_subscripts(
        v_row.wait_counts, 1
      ) as pair_position(pos)
      where pair_position.pos % 2 = 1
        and v_row.wait_counts[pair_position.pos] = v_idle_cr_id
    ),
      'idle-state ClientRead wait_id must not appear in wait_counts: '
        || v_row.wait_counts::text;
    assert exists (
      select 1
      from generate_subscripts(
        v_row.wait_counts, 1
      ) as pair_position(pos)
      where pair_position.pos % 2 = 1
        and v_row.wait_counts[pair_position.pos] = v_active_cr_id
    ),
      'active-state ClientRead wait_id must appear in wait_counts';

    raise notice 'ClientRead state-disambiguation regression test PASSED';
  end;

  raise notice 'Rollup tests PASSED';
end;
$$;

-- Test rollup_hour with different-length arrays
-- Regression test: array_agg() on arrays of different lengths
-- crashes with "cannot accumulate arrays of different dimensionality".
-- Fixed by using _int4_array_cat_agg / _int8_array_cat_agg.
do $$
declare
  v_wait_id1 smallint;
  v_wait_id2 smallint;
  v_hour_ts int4;
  v_result int;
  v_hr record;
begin
  select ash._register_wait('active', 'CPU*', 'CPU*') into v_wait_id1;
  select ash._register_wait('active', 'IO', 'DataFileRead') into v_wait_id2;

  truncate ash.rollup_1h;

  v_hour_ts := (ash.ts_from_timestamptz(
    date_trunc('hour', now() - interval '5 hours')
  ) / 3600) * 3600;

  -- Minute 1: 2 wait types (4-element array), 2 queries (4-element array)
  insert into ash.rollup_1m (ts, datid, samples, peak_backends, wait_counts, query_counts)
  values (v_hour_ts, 0::oid, 30, 5,
    array[v_wait_id1, 60, v_wait_id2, 40]::int4[],
    '{12345,60,67890,20}'::int8[])
  on conflict (ts, datid) do nothing;

  -- Minute 2: 1 wait type (2-element array), 1 query (2-element array)
  insert into ash.rollup_1m (ts, datid, samples, peak_backends, wait_counts, query_counts)
  values (v_hour_ts + 60, 0::oid, 30, 3,
    array[v_wait_id1, 50]::int4[],
    '{12345,40}'::int8[])
  on conflict (ts, datid) do nothing;

  update ash.config
  set last_rollup_1m_ts = v_hour_ts + 3600,
      last_rollup_1h_ts = null
  where singleton;
  select ash.rollup_hour() into v_result;
  assert v_result > 0,
    'rollup_hour should handle different-length arrays';

  select * into v_hr from ash.rollup_1h
  where ts = v_hour_ts and datid = 0::oid;

  assert v_hr.samples = 60,
    'samples should be 60 (30+30), got ' || v_hr.samples;
  assert v_hr.peak_backends = 5,
    'peak should be 5, got ' || v_hr.peak_backends;
  assert v_hr.wait_counts[1] = v_wait_id1 and v_hr.wait_counts[2] = 110,
    'CPU* merged count should be 110 (60+50), got ' || v_hr.wait_counts::text;

  raise notice 'Rollup hour different-length arrays test PASSED';
end;
$$;

/*
 * #191: return processed time grains, independently of the number of
 * per-database rows written. Exercise both the historically invisible
 * single-database case and the failing two-database case.
 */
do $rollup_return_contract$
declare
  v_anchor int4 := ash.ts_from_timestamptz(
    date_trunc('hour', statement_timestamp()) - interval '1 hour'
  );
  v_datids bigint;
  v_expected_datids int;
  v_hour_result int;
  v_hour_rows bigint;
  v_hour_watermark int4;
  v_minute_result int;
  v_minute_rows bigint;
  v_minute_watermark int4;
  v_wait_id smallint;
begin
  select ash._register_wait(
    'active',
    'Issue191',
    'ReturnContract'
  )
  into v_wait_id;

  for v_expected_datids in 1..2 loop
    truncate table ash.sample;
    truncate table ash.rollup_1m, ash.rollup_1h;
    update ash.config
    set current_slot = 0,
      sample_interval = interval '1 second',
      last_rollup_1m_ts = v_anchor,
      last_rollup_1h_ts = null
    where singleton;

    with database_ids as (
      select database_row.oid as datid
      from pg_catalog.pg_database as database_row
      order by
        (
          database_row.datname = pg_catalog.current_database()
        ) desc,
        database_row.oid
      limit v_expected_datids
    )
    insert into ash.sample (
      sample_ts,
      datid,
      active_count,
      data,
      slot
    )
    select
      v_anchor,
      database_ids.datid,
      1,
      array[-v_wait_id::int, 1, 0]::int4[],
      0
    from database_ids;

    select pg_catalog.count(distinct sample_row.datid)
    into v_datids
    from ash.sample as sample_row;
    select ash.rollup_minute(1)
    into v_minute_result;
    select pg_catalog.count(*)
    into v_minute_rows
    from ash.rollup_1m;
    select last_rollup_1m_ts
    into v_minute_watermark
    from ash.config
    where singleton;

    update ash.config
    set last_rollup_1m_ts = v_anchor + 3600,
      last_rollup_1h_ts = v_anchor
    where singleton;
    select ash.rollup_hour()
    into v_hour_result;
    select pg_catalog.count(*)
    into v_hour_rows
    from ash.rollup_1h;
    select last_rollup_1h_ts
    into v_hour_watermark
    from ash.config
    where singleton;

    assert v_datids = v_expected_datids
      and v_minute_result = 1
      and v_minute_rows = v_expected_datids
      and v_minute_watermark = v_anchor + 60
      and v_hour_result = 1
      and v_hour_rows = v_expected_datids
      and v_hour_watermark = v_anchor + 3600,
      format(
        'ash.rollup return contract (#191, %s databases): expected '
        'datids=%s minute_result=1 minute_rows=%s '
        'minute_watermark_delta=60 hour_result=1 hour_rows=%s '
        'hour_watermark_delta=3600, got datids=%s minute_result=%s '
        'minute_rows=%s minute_watermark_delta=%s hour_result=%s '
        'hour_rows=%s hour_watermark_delta=%s',
        v_expected_datids,
        v_expected_datids,
        v_expected_datids,
        v_expected_datids,
        v_datids,
        v_minute_result,
        v_minute_rows,
        v_minute_watermark - v_anchor,
        v_hour_result,
        v_hour_rows,
        v_hour_watermark - v_anchor
      );
  end loop;

  /*
   * Empty completed periods still consume a batch slot and advance
   * the watermark, so they count as processed grains despite writing
   * no per-database rows.
   */
  truncate table ash.sample;
  truncate table ash.rollup_1m, ash.rollup_1h;
  update ash.config
  set last_rollup_1m_ts = v_anchor,
    last_rollup_1h_ts = null
  where singleton;
  select ash.rollup_minute(1)
  into v_minute_result;
  select pg_catalog.count(*)
  into v_minute_rows
  from ash.rollup_1m;
  select last_rollup_1m_ts
  into v_minute_watermark
  from ash.config
  where singleton;

  update ash.config
  set last_rollup_1m_ts = v_anchor + 3600,
    last_rollup_1h_ts = v_anchor
  where singleton;
  select ash.rollup_hour()
  into v_hour_result;
  select pg_catalog.count(*)
  into v_hour_rows
  from ash.rollup_1h;
  select last_rollup_1h_ts
  into v_hour_watermark
  from ash.config
  where singleton;

  assert v_minute_result = 1
    and v_minute_rows = 0
    and v_minute_watermark = v_anchor + 60
    and v_hour_result = 1
    and v_hour_rows = 0
    and v_hour_watermark = v_anchor + 3600,
    format(
      'ash.rollup empty-grain contract (#191): expected '
      'minute_result=1 minute_rows=0 minute_watermark_delta=60 '
      'hour_result=1 hour_rows=0 hour_watermark_delta=3600, got '
      'minute_result=%s minute_rows=%s minute_watermark_delta=%s '
      'hour_result=%s hour_rows=%s hour_watermark_delta=%s',
      v_minute_result,
      v_minute_rows,
      v_minute_watermark - v_anchor,
      v_hour_result,
      v_hour_rows,
      v_hour_watermark - v_anchor
    );

  update ash.config
  set last_rollup_1m_ts = ash.ts_from_timestamptz(
    date_trunc('minute', statement_timestamp())
  )
  where singleton;
  select ash.rollup_minute(1000000)
  into v_minute_result;
  assert v_minute_result = 0,
    format(
      'already-caught-up rollup_minute(1000000) expected 0, got %s',
      v_minute_result
    );

  truncate table ash.sample;
  truncate table ash.rollup_1m, ash.rollup_1h;
  update ash.config
  set last_rollup_1m_ts = null,
    last_rollup_1h_ts = null
  where singleton;
end
$rollup_return_contract$;
