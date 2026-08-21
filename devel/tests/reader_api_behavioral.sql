/* -------------------------------------------------------------------------
 * behavioural coverage of the 2.0 reader API
 *
 * Extracted from .github/workflows/test.yml. GitHub Actions silently refuses
 * to create a workflow run when the workflow file exceeds 512,000 bytes, and
 * test.yml sits just under that ceiling, so large test bodies live here and
 * are invoked with `psql --file` from a short workflow step. Same assertions,
 * same order; only the transport changed.
 *
 * Run standalone against a prepared database with:
 *   psql -v ON_ERROR_STOP=1 -f devel/tests/reader_api_behavioral.sql
 * ------------------------------------------------------------------------- */
-- ============================================================================
-- 2.0 reader API — deterministic seed + behavioral assertions.
-- One raw sample row per (minute, datid); sample_interval pinned to 1s so
-- AAS = backend-count / window-seconds. Window W = [m0, m0+300) over minutes
-- m0..m4 anchored 20 minutes back (complete minutes, inside raw+rollup
-- retention). Every expected avg/peak/p99/backend_seconds below is computed by
-- hand from the seeded counts (see comments).
-- ============================================================================
update ash.config
  set current_slot = 0, sample_interval = '1 second',
      last_rollup_1m_ts = null, last_rollup_1h_ts = null
  where singleton;
truncate ash.sample_0, ash.sample_1, ash.sample_2;
truncate ash.query_map_0, ash.query_map_1, ash.query_map_2;
truncate ash.rollup_1m, ash.rollup_1h;
alter table ash.query_map_0 alter column id restart;

drop table if exists t2anchor;
create temp table t2anchor as
  select ((ash.ts_from_timestamptz(date_trunc('minute', now() - interval '20 minutes'))) / 60) * 60 as base_ts;

insert into ash.wait_event_map (state, type, event) values
  ('active', 'CPU*', 'CPU*'),
  ('active', 'IO', 'DataFileRead'),
  ('active', 'Lock', 'tuple'),
  ('active', 'LWLock', 'WALWrite')
on conflict do nothing;

insert into ash.query_map_0 (query_id) values (111111), (222222), (333333)
on conflict do nothing;

do $$
declare
  v_cpu smallint; v_io smallint; v_lock smallint;
  v_q1 int4; v_q2 int4; v_q3 int4;
  v_base int4; v_dp oid; v_dt oid;
  v_normal integer[];
begin
  select id into v_cpu  from ash.wait_event_map where type='CPU*' and event='CPU*';
  select id into v_io   from ash.wait_event_map where type='IO' and event='DataFileRead';
  select id into v_lock from ash.wait_event_map where type='Lock' and event='tuple';
  select id into v_q1 from ash.query_map_0 where query_id=111111;
  select id into v_q2 from ash.query_map_0 where query_id=222222;
  select id into v_q3 from ash.query_map_0 where query_id=333333;
  select base_ts into v_base from t2anchor;
  select oid into v_dp from pg_database where datname='postgres';
  select oid into v_dt from pg_database where datname='template1';

  -- Baseline minute (postgres): IO 6 (q1x3,q2x2,q3x1) + CPU 3 (unknown) = 9.
  v_normal := array[-v_io, 6, v_q1,v_q1,v_q1,v_q2,v_q2,v_q3, -v_cpu, 3, 0,0,0]::integer[];
  insert into ash.sample (sample_ts, datid, active_count, data, slot) values
    (v_base + 0,   v_dp, 9, v_normal, 0),
    (v_base + 60,  v_dp, 9, v_normal, 0),
    (v_base + 180, v_dp, 9, v_normal, 0),
    (v_base + 240, v_dp, 9, v_normal, 0);

  -- Spike minute m2 (postgres): IO 12 (q1x6,q2x4,q3x2) + CPU 3 + Lock 6 (q1x6) = 21.
  insert into ash.sample (sample_ts, datid, active_count, data, slot) values
    (v_base + 120, v_dp, 21,
      array[-v_io, 12, v_q1,v_q1,v_q1,v_q1,v_q1,v_q1,v_q2,v_q2,v_q2,v_q2,v_q3,v_q3,
            -v_cpu, 3, 0,0,0,
            -v_lock, 6, v_q1,v_q1,v_q1,v_q1,v_q1,v_q1]::integer[], 0);

  -- Spike minute m2 (template1): IO 3 (q1x3) — the second database.
  insert into ash.sample (sample_ts, datid, active_count, data, slot) values
    (v_base + 120, v_dt, 3, array[-v_io, 3, v_q1,v_q1,v_q1]::integer[], 0);
end $$;

-- Derive rollups from raw (keeps raw and rollup_1m consistent for report/periods).
select ash.rollup_minute();

-- --- ash.aas (scalar summary, raw source) -----------------------------------
do $$
declare a record; v_from timestamptz; v_to timestamptz;
begin
  select ash.ts_to_timestamptz(base_ts), ash.ts_to_timestamptz(base_ts + 300)
    into v_from, v_to from t2anchor;

  -- overall: sum backend-rows = 60 over 300s. avg 0.20; per-min buckets
  -- [9,9,24,9,9]/60 = [.15,.15,.40,.15,.15] -> peak .40, p99 .39.
  select * into a from ash.aas(v_from, v_to);
  assert a.source = 'raw', 'aas source expected raw, got ' || a.source;
  assert a.buckets_expected = 5, 'aas buckets_expected 5, got ' || a.buckets_expected;
  assert a.buckets_with_data = 5, 'aas buckets_with_data 5, got ' || a.buckets_with_data;
  assert a.avg_aas = 0.20, 'aas avg_aas 0.20, got ' || a.avg_aas;
  assert a.peak_aas = 0.40, 'aas peak_aas 0.40, got ' || a.peak_aas;
  assert a.p99_aas = 0.39, 'aas p99_aas 0.39, got ' || a.p99_aas;
  assert a.backend_seconds = 60.00, 'aas backend_seconds 60, got ' || a.backend_seconds;

  -- leaf: ash.aas(wait_event => 'IO:DataFileRead'). IO rows = 39.
  select * into a from ash.aas(v_from, v_to, wait_event => 'IO:DataFileRead');
  assert a.avg_aas = 0.13, 'aas IO avg 0.13, got ' || a.avg_aas;
  assert a.peak_aas = 0.25, 'aas IO peak 0.25, got ' || a.peak_aas;
  assert a.p99_aas = 0.24, 'aas IO p99 0.24, got ' || a.p99_aas;
  assert a.backend_seconds = 39.00, 'aas IO backend_seconds 39, got ' || a.backend_seconds;
  raise notice 'ash.aas PASSED';
end $$;

-- --- ash.top (all dimensions + leaf drill) ----------------------------------
do $$
declare r record; v_from timestamptz; v_to timestamptz; n int;
begin
  select ash.ts_to_timestamptz(base_ts), ash.ts_to_timestamptz(base_ts + 300)
    into v_from, v_to from t2anchor;

  -- wait_event_type: IO 39 (pct 65), CPU* 15 (25), Lock 6 (10). Total 60.
  select * into r from ash.top('wait_event_type', v_from, v_to) where key = 'IO';
  assert r.avg_aas = 0.13 and r.peak_aas = 0.25 and r.p99_aas = 0.24
     and r.backend_seconds = 39.00 and r.pct = 65.00,
     format('top(wait_event_type) IO mismatch: avg=%s peak=%s p99=%s bsec=%s pct=%s',
            r.avg_aas, r.peak_aas, r.p99_aas, r.backend_seconds, r.pct);
  select * into r from ash.top('wait_event_type', v_from, v_to) where key = 'CPU*';
  assert r.avg_aas = 0.05 and r.backend_seconds = 15.00 and r.pct = 25.00,
     format('top(wait_event_type) CPU* mismatch: avg=%s bsec=%s pct=%s',
            r.avg_aas, r.backend_seconds, r.pct);
  select * into r from ash.top('wait_event_type', v_from, v_to) where key = 'Lock';
  assert r.backend_seconds = 6.00 and r.pct = 10.00, 'top(wait_event_type) Lock mismatch';

  -- wait_event filtered by type (L2 drill): only IO:DataFileRead, pct 100.
  select * into r from ash.top('wait_event', v_from, v_to, wait_event_type => 'IO');
  assert r.key = 'IO:DataFileRead' and r.backend_seconds = 39.00 and r.pct = 100.00,
     format('top(wait_event|IO) mismatch: key=%s bsec=%s pct=%s', r.key, r.backend_seconds, r.pct);

  -- query_id: q1 27 (45), unattributed NULL 15 (25), q2 12 (20), q3 6 (10).
  select * into r from ash.top('query_id', v_from, v_to) where key = '111111';
  assert r.avg_aas = 0.09 and r.backend_seconds = 27.00 and r.pct = 45.00,
     format('top(query_id) q1 mismatch: avg=%s bsec=%s pct=%s', r.avg_aas, r.backend_seconds, r.pct);
  select count(*) into n from ash.top('query_id', v_from, v_to);
  assert n = 4, 'top(query_id) should return 4 keys, got ' || n;
  -- the unattributed bucket is a NULL key (not the literal 'unknown').
  assert exists (select 1 from ash.top('query_id', v_from, v_to)
                 where key is null and backend_seconds = 15.00),
     'top(query_id) NULL key (CPU has no query) should be 15';
  assert not exists (select 1 from ash.top('query_id', v_from, v_to)
                     where key = 'unknown'),
     'top(query_id) must not emit the literal string unknown';

  -- database: postgres 57 (95), template1 3 (5).
  select * into r from ash.top('database', v_from, v_to) where key = 'postgres';
  assert r.backend_seconds = 57.00 and r.pct = 95.00, 'top(database) postgres mismatch';
  select * into r from ash.top('database', v_from, v_to) where key = 'template1';
  assert r.backend_seconds = 3.00 and r.pct = 5.00, 'top(database) template1 mismatch';

  -- leaf drill (query_id + wait filter): forces raw. IO by query: q1 21, q2 12, q3 6.
  select * into r from ash.top('query_id', v_from, v_to, wait_event => 'DataFileRead')
    where key = '111111';
  assert r.source = 'raw' and r.backend_seconds = 21.00 and r.pct = 53.85,
     format('top(query_id|DataFileRead) q1 mismatch: src=%s bsec=%s pct=%s',
            r.source, r.backend_seconds, r.pct);

  -- unknown dimension raises.
  begin
    perform * from ash.top('bogus', v_from, v_to);
    raise exception 'top(bogus) should have raised';
  exception when others then
    assert sqlerrm like '%unknown dimension%', 'top(bogus) wrong error: ' || sqlerrm;
  end;
  raise notice 'ash.top PASSED';
end $$;

-- --- raw-retention exception (tie drill before raw retention) ----------------
update ash.config
set num_partitions = 3,
    rotation_period = interval '1 day',
    rotated_at = date_trunc('minute', now())
where singleton;

do $$
declare v_base int4; v_raised boolean := false;
begin
  select base_ts into v_base from t2anchor;
  begin
    perform * from ash.top('query_id',
      ash.ts_to_timestamptz(v_base - 172800),
      ash.ts_to_timestamptz(v_base - 172200),
      wait_event => 'DataFileRead');
    raise exception 'expected raw-retention exception, none raised';
  exception when others then
    -- window is ENTIRELY past raw retention: the error must say the
    -- tie is unrecoverable and point to the untied readers, and must
    -- NOT suggest narrowing (which cannot help here).
    if sqlerrm like '%entirely outside raw retention%'
       and sqlerrm like '%untied aggregate readers%'
       and sqlerrm not like '%Narrow the window%' then
      v_raised := true;
    else raise; end if;
  end;
  assert v_raised, 'raw-retention exception message mismatch';
  raise notice 'raw-retention exception PASSED';
end $$;

update ash.config
set rotation_period = interval '1 day',
    rotated_at = date_trunc('minute', now())
where singleton;

-- --- ash.timeline (per-bucket series + no-data bucket) -----------------------
do $$
declare r record; v_from timestamptz; v_to timestamptz; n int;
begin
  select ash.ts_to_timestamptz(base_ts), ash.ts_to_timestamptz(base_ts + 300)
    into v_from, v_to from t2anchor;

  select count(*) into n from ash.timeline(v_from, v_to);
  assert n = 5, 'timeline should return 5 buckets, got ' || n;
  -- every bucket has one grain (data_points = 1 for raw minute grain).
  select * into r from ash.timeline(v_from, v_to)
    where bucket_start = ash.ts_to_timestamptz((select base_ts + 120 from t2anchor));
  assert r.source = 'raw' and r.data_points = 1 and r.avg_aas = 0.40
     and r.peak_aas = 0.40 and r.p99_aas = 0.40,
     format('timeline spike bucket mismatch: dp=%s avg=%s peak=%s p99=%s',
            r.data_points, r.avg_aas, r.peak_aas, r.p99_aas);
  select * into r from ash.timeline(v_from, v_to)
    where bucket_start = ash.ts_to_timestamptz((select base_ts from t2anchor));
  assert r.avg_aas = 0.15, 'timeline m0 avg 0.15, got ' || r.avg_aas;

  -- no-data bucket: window over m5 (empty minute) -> data_points 0, null AAS.
  select * into r from ash.timeline(ash.ts_to_timestamptz((select base_ts + 300 from t2anchor)),
                                    ash.ts_to_timestamptz((select base_ts + 360 from t2anchor)));
  assert r.data_points = 0 and r.avg_aas is null and r.peak_aas is null,
     format('timeline no-data bucket mismatch: dp=%s avg=%s', r.data_points, r.avg_aas);
  raise notice 'ash.timeline PASSED';
end $$;

-- --- ash.periods (six standard windows) -------------------------------------
do $$
declare r record; v_end timestamptz; n int;
begin
  select ash.ts_to_timestamptz(base_ts + 300) into v_end from t2anchor;
  select count(*) into n from ash.periods(v_end);
  assert n = 6, 'periods should return 6 rows, got ' || n;

  -- 1m period = [m4, m4+60): only m4 (avg 0.15), raw. buckets_with_data
  -- counts covered buckets at the grain named by the bucket column
  -- (always 1 minute for periods).
  select * into r from ash.periods(v_end) where period = '1m';
  assert r.source = 'raw' and r.buckets_with_data = 1 and r.avg_aas = 0.15,
     format('periods 1m mismatch: src=%s bwd=%s avg=%s', r.source, r.buckets_with_data, r.avg_aas);
  assert r.bucket = interval '1 minute',
     'periods bucket column must be 1 minute, got ' || r.bucket;
  -- 5m period = exactly the seeded window.
  select * into r from ash.periods(v_end) where period = '5m';
  assert r.source = 'raw' and r.buckets_with_data = 5 and r.avg_aas = 0.20
     and r.peak_aas = 0.40 and r.p99_aas = 0.39,
     format('periods 5m mismatch: src=%s bwd=%s avg=%s peak=%s p99=%s',
            r.source, r.buckets_with_data, r.avg_aas, r.peak_aas, r.p99_aas);
  -- 1h period reaches before raw retention -> rollup_1m source.
  select * into r from ash.periods(v_end) where period = '1h';
  assert r.source = 'rollup_1m' and r.buckets_with_data = 5 and r.peak_aas = 0.40,
     format('periods 1h mismatch: src=%s bwd=%s peak=%s', r.source, r.buckets_with_data, r.peak_aas);
  -- every row exposes its grain: 6 rows, all 1-minute buckets.
  select count(*) into n from ash.periods(v_end) where bucket = interval '1 minute';
  assert n = 6, 'periods bucket column on all 6 rows, got ' || n;
  raise notice 'ash.periods PASSED';
end $$;

-- --- ash.compare (before/after) ---------------------------------------------
do $$
declare r record; v0 timestamptz; v2 timestamptz; v5 timestamptz;
begin
  select ash.ts_to_timestamptz(base_ts), ash.ts_to_timestamptz(base_ts + 120),
         ash.ts_to_timestamptz(base_ts + 300)
    into v0, v2, v5 from t2anchor;

  -- overall: w1 [m0,m0+120) avg 18/120=0.15; w2 [m2,m0+300) avg 42/180=0.23.
  select * into r from ash.compare(v0, v2, v2, v5);
  assert r.key = 'overall' and r.avg_aas_1 = 0.15 and r.avg_aas_2 = 0.23
     and r.avg_delta = 0.08,
     format('compare overall mismatch: k=%s a1=%s a2=%s d=%s',
            r.key, r.avg_aas_1, r.avg_aas_2, r.avg_delta);

  -- dimension: IO delta 0.05; Lock present only in w2 (avg_aas_1 null).
  select * into r from ash.compare(v0, v2, v2, v5, dimension => 'wait_event_type')
    where key = 'IO';
  assert r.avg_aas_1 = 0.10 and r.avg_aas_2 = 0.15 and r.avg_delta = 0.05,
     format('compare IO mismatch: a1=%s a2=%s d=%s', r.avg_aas_1, r.avg_aas_2, r.avg_delta);
  select * into r from ash.compare(v0, v2, v2, v5, dimension => 'wait_event_type')
    where key = 'Lock';
  assert r.avg_aas_1 is null and r.avg_aas_2 = 0.03,
     format('compare Lock (w2-only) mismatch: a1=%s a2=%s', r.avg_aas_1, r.avg_aas_2);
  raise notice 'ash.compare PASSED';
end $$;

-- --- ash.samples (raw evidence + filters) -----------------------------------
do $$
declare v_from timestamptz; v_to timestamptz; n int; r record;
begin
  select ash.ts_to_timestamptz(base_ts), ash.ts_to_timestamptz(base_ts + 300)
    into v_from, v_to from t2anchor;

  select count(*) into n from ash.samples(v_from, v_to);
  assert n = 60, 'samples should decode 60 backend-rows, got ' || n;
  select count(*) into n from ash.samples(v_from, v_to, query_id => 111111);
  assert n = 27, 'samples filtered by q1 should be 27, got ' || n;
  select count(*) into n from ash.samples(v_from, v_to, wait_event => 'IO:DataFileRead');
  assert n = 39, 'samples filtered by IO event should be 39, got ' || n;
  -- a known decoded row (spike minute, postgres, IO).
  select * into r from ash.samples(v_from, v_to, wait_event => 'IO:DataFileRead',
                                   query_id => 111111) limit 1;
  assert r.database_name = 'postgres' and r.wait_event = 'IO:DataFileRead'
     and r.query_id = 111111,
     format('samples row mismatch: db=%s ev=%s q=%s', r.database_name, r.wait_event, r.query_id);
  raise notice 'ash.samples PASSED';
end $$;

-- --- ash.report (machine JSON load report) ----------------------------------
do $$
declare j jsonb; v_from timestamptz; v_to timestamptz;
  expected_keys text[] := array[
    'aas_avg','aas_worst1m','aas_p99','aas_p999',
    'top_events_worst1m','top_events_p99','top_events_p999',
    'top_queryids_worst1m','top_queryids_p99','top_queryids_p999',
    'top_queryids_available','coverage'];
begin
  select ash.ts_to_timestamptz(base_ts), ash.ts_to_timestamptz(base_ts + 300)
    into v_from, v_to from t2anchor;
  j := ash.report(v_from, v_to);
  assert j is not null, 'report returned null on covered window';

  -- exact top-level key set.
  assert (select array_agg(k order by k) from jsonb_object_keys(j) k)
         = (select array_agg(k order by k) from unnest(expected_keys) k),
     'report top-level keys mismatch: ' || (select string_agg(k, ',' order by k) from jsonb_object_keys(j) k);

  -- aas_avg per class (cpu .05, io .13, ipc 0, lock .02, lwlock 0; total .20).
  assert (j->'aas_avg'->>'total')::numeric = 0.20, 'report aas_avg.total 0.20';
  assert (j->'aas_avg'->>'cpu')::numeric = 0.05, 'report aas_avg.cpu 0.05';
  assert (j->'aas_avg'->>'io')::numeric = 0.13, 'report aas_avg.io 0.13';
  assert (j->'aas_avg'->>'lock')::numeric = 0.02, 'report aas_avg.lock 0.02';
  assert (j->'aas_avg'->>'ipc')::numeric = 0, 'report aas_avg.ipc 0';
  assert (j->'aas_avg'->>'lwlock')::numeric = 0, 'report aas_avg.lwlock 0';

  -- worst1m / p99 / p999 totals.
  assert (j->'aas_worst1m'->>'total')::numeric = 0.40, 'report aas_worst1m.total 0.40';
  assert (j->'aas_worst1m'->>'io')::numeric = 0.25, 'report aas_worst1m.io 0.25';
  assert (j->'aas_p99'->>'total')::numeric = 0.39, 'report aas_p99.total 0.39';
  assert (j->'aas_p99'->>'io')::numeric = 0.24, 'report aas_p99.io 0.24';
  assert (j->'aas_p999'->>'total')::numeric = 0.40, 'report aas_p999.total 0.40';

  -- top_events_* must NOT carry a cpu key (CPU* has no per-event breakdown)
  -- nor a total key; io key present at the worst minute.
  assert not (j->'top_events_worst1m' ? 'cpu'), 'report top_events must not have cpu key';
  assert not (j->'top_events_worst1m' ? 'total'), 'report top_events must not have total key';
  assert (j->'top_events_worst1m'->'io')->>0 like 'DataFileRead(%',
     'report top_events_worst1m.io should lead with DataFileRead';

  -- top_queryids keys: total + four non-cpu classes; total leads with 111111.
  assert j->'top_queryids_worst1m' ? 'total', 'report top_queryids must have total key';
  assert not (j->'top_queryids_worst1m' ? 'cpu'), 'report top_queryids must not have cpu key';
  assert (j->'top_queryids_worst1m'->'total')->>0 like '111111(%',
     'report top_queryids_worst1m.total should lead with 111111';

  -- attribution/coverage metadata: available flag + coverage object
  -- reconcile the payload against ash.aas() for the same window.
  assert (j->>'top_queryids_available')::boolean is true,
     'report top_queryids_available must be true when attribution present';
  assert j->'coverage'->>'source' = 'rollup_1m', 'report coverage.source rollup_1m';
  assert (j->'coverage'->>'minutes_expected')::int = 5, 'report coverage.minutes_expected 5';
  assert (j->'coverage'->>'minutes_with_data')::int = 5, 'report coverage.minutes_with_data 5';
  assert (j->'coverage'->>'from')::timestamptz = v_from
     and (j->'coverage'->>'to')::timestamptz = v_to,
     'report coverage window must echo the minute-floored request';
  assert j->'coverage' ? 'raw_retention_start', 'report coverage.raw_retention_start present';

  -- vcpus is echoed, cpu-scoring left to the consumer.
  assert (ash.report(v_from, v_to, vcpus => 8)->>'vcpus')::int = 8,
     'report should echo vcpus';

  -- no coverage -> null (consumer skips ingestion).
  assert ash.report(ash.ts_to_timestamptz((select base_ts + 3600 from t2anchor)),
                    ash.ts_to_timestamptz((select base_ts + 7200 from t2anchor))) is null,
     'report over uncovered window should be null';
  raise notice 'ash.report PASSED';
end $$;

-- --- ash.report p99/p999 attribution for sparse low-AAS classes (#115) -------
do $$
declare
  j jsonb; v_from timestamptz; v_to timestamptz; v_class text;
  v_cpu smallint; v_lw smallint; v_q2 int4; v_m0 int4; v_dp oid;
begin
  select id into v_cpu from ash.wait_event_map where type='CPU*' and event='CPU*';
  select id into v_lw from ash.wait_event_map where type='LWLock' and event='WALWrite';
  select id into v_q2 from ash.query_map_0 where query_id=222222;
  select base_ts + 600 into v_m0 from t2anchor;
  select oid into v_dp from pg_database where datname='postgres';

  -- Fixture: 5 covered minutes [m0..m4]; LWLock only in m3 (44) and m4 (46).
  -- lwlock per-minute AAS grid = [0, 0, 0, 0.7333, 0.7667]:
  --   p99  = 0.765333... -> displays as 0.77 (ABOVE the true max 0.766667)
  --   p999 = 0.766533... -> displays as 0.77
  -- Filtering the p99/p999 minute sets on the ROUNDED 0.77 matches no minute
  -- at all, wiping out top_events_p99/p999 and top_queryids_p99/p999 for the
  -- class even though aas_p99 reports 0.77; the unrounded thresholds match m4.
  insert into ash.rollup_1m (ts, datid, samples, peak_backends, wait_counts, query_counts) values
    (v_m0,       v_dp, 1, 3,  array[v_cpu, 3],  array[0, 3]::int8[]),
    (v_m0 + 60,  v_dp, 1, 3,  array[v_cpu, 3],  array[0, 3]::int8[]),
    (v_m0 + 120, v_dp, 1, 3,  array[v_cpu, 3],  array[0, 3]::int8[]),
    (v_m0 + 180, v_dp, 1, 44, array[v_lw, 44],  array[222222, 44]::int8[]),
    (v_m0 + 240, v_dp, 1, 46, array[v_lw, 46],  array[222222, 46]::int8[]);
  -- matching raw samples for the extreme minutes (top_queryids attribution).
  insert into ash.sample (sample_ts, datid, active_count, data, slot) values
    (v_m0 + 180, v_dp, 44,
      array[-v_lw, 44]::integer[] || array_fill(v_q2, array[44]), 0),
    (v_m0 + 240, v_dp, 46,
      array[-v_lw, 46]::integer[] || array_fill(v_q2, array[46]), 0);

  v_from := ash.ts_to_timestamptz(v_m0);
  v_to := ash.ts_to_timestamptz(v_m0 + 300);
  j := ash.report(v_from, v_to);
  assert j is not null, 'report(sparse lwlock) returned null';

  -- displayed values round UP above the true per-minute max (0.766667).
  assert (j->'aas_avg'->>'lwlock')::numeric = 0.30, 'sparse lwlock aas_avg 0.30';
  assert (j->'aas_worst1m'->>'lwlock')::numeric = 0.77, 'sparse lwlock aas_worst1m 0.77';
  assert (j->'aas_p99'->>'lwlock')::numeric = 0.77, 'sparse lwlock aas_p99 0.77';
  assert (j->'aas_p999'->>'lwlock')::numeric = 0.77, 'sparse lwlock aas_p999 0.77';

  -- attribution must survive display rounding: minute-set membership is
  -- decided on the UNROUNDED thresholds, so m4 (0.7667) attributes.
  assert j->'top_events_p99'->'lwlock' = '["WALWrite(0.8)"]'::jsonb,
     'sparse lwlock top_events_p99 must attribute WALWrite, got '
     || coalesce((j->'top_events_p99'->'lwlock')::text, '<missing>');
  assert j->'top_events_p999'->'lwlock' = '["WALWrite(0.8)"]'::jsonb,
     'sparse lwlock top_events_p999 must attribute WALWrite, got '
     || coalesce((j->'top_events_p999'->'lwlock')::text, '<missing>');
  assert j->'top_queryids_p99'->'lwlock' = '["222222(0.8)"]'::jsonb,
     'sparse lwlock top_queryids_p99 must attribute 222222, got '
     || coalesce((j->'top_queryids_p99'->'lwlock')::text, '<missing>');
  assert j->'top_queryids_p999'->'lwlock' = '["222222(0.8)"]'::jsonb,
     'sparse lwlock top_queryids_p999 must attribute 222222, got '
     || coalesce((j->'top_queryids_p999'->'lwlock')::text, '<missing>');

  -- invariant across the event-attributed classes: aas_p99 > 0 must never
  -- come with an empty top_events_p99 (cpu has no per-event breakdown).
  for v_class in select unnest(array['io','ipc','lock','lwlock']) loop
    if coalesce((j->'aas_p99'->>v_class)::numeric, 0) > 0 then
      assert jsonb_array_length(coalesce(j->'top_events_p99'->v_class, '[]'::jsonb)) > 0,
         format('class %s: aas_p99 > 0 but top_events_p99 empty', v_class);
    end if;
  end loop;

  -- leave the shared fixture exactly as this block found it.
  delete from ash.rollup_1m where ts >= v_m0 and ts < v_m0 + 300;
  delete from ash.sample where sample_ts >= v_m0 and sample_ts < v_m0 + 300;
  raise notice 'ash.report sparse-class p99/p999 attribution PASSED';
end $$;

-- --- ash.summary + ash.chart (human render helpers, smoke) -------------------
do $$
declare v_from timestamptz; v_to timestamptz; n int; v_val text;
begin
  select ash.ts_to_timestamptz(base_ts), ash.ts_to_timestamptz(base_ts + 300)
    into v_from, v_to from t2anchor;

  -- summary key/value overview mirrors ash.aas + top waits/queries.
  select value into v_val from ash.summary(v_from, v_to) where metric = 'avg_aas';
  assert v_val = '0.20', 'summary avg_aas 0.20, got ' || v_val;
  select value into v_val from ash.summary(v_from, v_to) where metric = 'source';
  assert v_val = 'raw', 'summary source raw, got ' || v_val;
  select value into v_val from ash.summary(v_from, v_to) where metric = 'databases_active';
  assert v_val = '2', 'summary databases_active 2, got ' || v_val;
  select value into v_val from ash.summary(v_from, v_to) where metric = 'top_wait_1';
  assert v_val like 'IO:DataFileRead%', 'summary top_wait_1 should be IO, got ' || v_val;

  -- chart returns a legend row plus one row per bucket (1 + 5).
  select count(*) into n from ash.chart(v_from, v_to);
  assert n = 6, 'chart should return legend + 5 buckets = 6 rows, got ' || n;
  assert exists (select 1 from ash.chart(v_from, v_to)
                 where bucket_start is null and chart is not null),
     'chart should emit a legend row';
  raise notice 'ash.summary + ash.chart PASSED';
end $$;
