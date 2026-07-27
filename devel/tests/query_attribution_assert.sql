-- Minute threshold compaction: the NULL residual must reconcile the
-- query breakdown and compete for the top-N cut.
do $$
declare
  v_ts int4 := (
    ash.ts_from_timestamptz(
      date_trunc('minute', now() - interval '20 minutes')
    ) / 60
  ) * 60;
  v_from timestamptz;
  v_to timestamptz;
  v_datid oid;
  v_wait_id smallint;
  v_q1 int4;
  v_q2 int4;
  v_q3 int4;
  v_result int;
  v_aas record;
  v_named record;
  v_residual record;
  v_top1 record;
  v_compare_residual record;
  v_compare_residual_rows int;
  v_summary_value text;
  v_rows int;
  v_total numeric;
  v_pct numeric;
begin
  truncate ash.sample_0, ash.sample_1, ash.sample_2;
  truncate ash.query_map_0, ash.query_map_1, ash.query_map_2
    restart identity;
  truncate ash.rollup_1m, ash.rollup_1h;
  update ash.config
  set current_slot = 0,
      sample_interval = interval '1 second',
      rollup_min_backend_seconds = 3,
      last_rollup_1m_ts = null,
      last_rollup_1h_ts = null
  where singleton;

  select oid into v_datid
  from pg_database
  where datname = current_database();
  select ash._register_wait('active', 'CPU*', 'CPU*')
    into v_wait_id;

  insert into ash.query_map_0 (query_id)
  values (111), (222), (333);
  select id into v_q1 from ash.query_map_0 where query_id = 111;
  select id into v_q2 from ash.query_map_0 where query_id = 222;
  select id into v_q3 from ash.query_map_0 where query_id = 333;

  -- One packed sample: 26 backend appearances. Only q111 reaches
  -- the minute threshold; q222/q333 and the 20 uncaptured
  -- appearances must survive as one NULL residual.
  insert into ash.sample (
    sample_ts, datid, active_count, data, slot
  ) values (
    v_ts,
    v_datid,
    26,
    array[-v_wait_id, 26, v_q1, v_q1, v_q1, v_q2, v_q2, v_q3]
      || array_fill(0, array[20]),
    0
  );

  select ash.rollup_minute(1) into v_result;
  assert v_result = 1,
    'rollup_minute should process exactly one completed minute, got '
    || v_result;
  assert (
    select count(*)
    from ash.rollup_1m
    where ts = v_ts
  ) = 1,
    'minute fixture should create exactly one database row';
  assert (
    select query_counts
    from ash.rollup_1m
    where ts = v_ts
  ) = array[111, 3]::int8[],
    'fixture must exercise compacted query_counts';

  v_from := ash.ts_to_timestamptz(v_ts);
  v_to := ash.ts_to_timestamptz(v_ts + 60);
  truncate ash.sample_0, ash.sample_1, ash.sample_2;

  select * into v_aas from ash.aas(v_from, v_to);
  assert v_aas.source = 'rollup_1m'
         and v_aas.backend_seconds = 26.00,
    format(
      'fixture AAS mismatch: source=%s backend_seconds=%s',
      v_aas.source,
      v_aas.backend_seconds
    );

  select * into v_named
  from ash.top('query_id', v_from, v_to, n => 100)
  where key = '111';
  select * into v_residual
  from ash.top('query_id', v_from, v_to, n => 100)
  where key is null;
  select count(*), sum(backend_seconds), sum(pct)
    into v_rows, v_total, v_pct
  from ash.top('query_id', v_from, v_to, n => 100);

  assert v_named.backend_seconds = 3.00
         and v_named.pct = 11.54,
    format(
      'named compacted query mismatch: seconds=%s pct=%s',
      v_named.backend_seconds,
      v_named.pct
    );
  assert v_residual.backend_seconds = 23.00
         and v_residual.pct = 88.46,
    format(
      'NULL residual mismatch: seconds=%s pct=%s',
      v_residual.backend_seconds,
      v_residual.pct
    );
  assert v_rows = 2
         and v_total = v_aas.backend_seconds
         and v_pct = 100.00,
    format(
      'query breakdown does not reconcile: rows=%s total=%s pct=%s',
      v_rows,
      v_total,
      v_pct
    );

  select * into v_top1
  from ash.top('query_id', v_from, v_to, n => 1);
  assert v_top1.key is null
         and v_top1.source = 'rollup_1m'
         and v_top1.backend_seconds = 23.00
         and v_top1.pct = 88.46,
    format(
      'residual top-N mismatch: key=%s source=%s seconds=%s pct=%s',
      coalesce(v_top1.key, '<NULL>'),
      v_top1.source,
      v_top1.backend_seconds,
      v_top1.pct
    );

  select count(*), max(avg_delta)
    into v_compare_residual_rows, v_pct
  from ash.compare(
    v_from,
    v_to,
    v_from,
    v_to,
    dimension => 'query_id',
    n => 100
  )
  where key is null;
  select * into v_compare_residual
  from ash.compare(
    v_from,
    v_to,
    v_from,
    v_to,
    dimension => 'query_id',
    n => 100
  )
  where key is null;
  assert v_compare_residual_rows = 1
         and v_pct = 0.00
         and v_compare_residual.source_1 = 'rollup_1m'
         and v_compare_residual.source_2 = 'rollup_1m'
         and v_compare_residual.pct_1 = 88.46
         and v_compare_residual.pct_2 = 88.46,
    format(
      'compare NULL residual mismatch: rows=%s source=%s/%s delta=%s pct=%s/%s',
      v_compare_residual_rows,
      v_compare_residual.source_1,
      v_compare_residual.source_2,
      v_pct,
      v_compare_residual.pct_1,
      v_compare_residual.pct_2
    );

  select value into v_summary_value
  from ash.summary(v_from, v_to)
  where metric = 'top_query_1';
  assert v_summary_value
           = '(other / unattributed) (avg_aas 0.38, 88.46%)',
    'summary residual label/value mismatch: '
    || coalesce(v_summary_value, '<NULL>');

  raise notice
    'issue #136 rollup_1m residual GREEN: named=3.00/11.54 residual=23.00/88.46 top1=NULL compare=NULL/0.00';
end $$;

-- Hour top-set compaction: each input minute has at most 100 query
-- IDs, but rollup_hour() must compact their 200-ID union to 100.
-- The missing 100 seconds are therefore purely the hourly top-set
-- residual, not minute-threshold or uncaptured attribution.
do $$
declare
  v_hour timestamptz := date_trunc('hour', now()) - interval '2 hours';
  v_hour_ts int4;
  v_wait_id smallint;
  v_result int;
  v_stored_rows int;
  v_stored_query_total numeric;
  v_stored_wait_total numeric;
  v_rows int;
  v_total numeric;
  v_pct numeric;
  v_residual record;
  v_named record;
begin
  truncate ash.sample_0, ash.sample_1, ash.sample_2;
  truncate ash.rollup_1m, ash.rollup_1h;
  update ash.config
  set sample_interval = interval '1 second',
      last_rollup_1m_ts = null,
      last_rollup_1h_ts = null
  where singleton;
  select ash._register_wait('active', 'CPU*', 'CPU*')
    into v_wait_id;
  v_hour_ts := ash.ts_from_timestamptz(v_hour);

  insert into ash.rollup_1m (
    ts, datid, samples, peak_backends, wait_counts, query_counts
  )
  select
    v_hour_ts,
    0::oid,
    60,
    300,
    array[v_wait_id, 300]::int4[],
    (
      select array_agg(pair.elem order by query_row.qid, pair.ord)
      from generate_series(1, 100) as query_row(qid)
      cross join lateral (
        values
          (1, query_row.qid::int8),
          (2, 3::int8)
      ) as pair(ord, elem)
    )::int8[]
  union all
  select
    v_hour_ts + 60,
    0::oid,
    60,
    100,
    array[v_wait_id, 100]::int4[],
    (
      select array_agg(pair.elem order by query_row.qid, pair.ord)
      from generate_series(1001, 1100) as query_row(qid)
      cross join lateral (
        values
          (1, query_row.qid::int8),
          (2, 1::int8)
      ) as pair(ord, elem)
    )::int8[];

  update ash.config
  set last_rollup_1m_ts = v_hour_ts + 3600,
      last_rollup_1h_ts = v_hour_ts
  where singleton;
  select ash.rollup_hour() into v_result;
  assert v_result = 1,
    'rollup_hour should process exactly one hour grain, got '
    || v_result;
  assert (
    select count(*)
    from ash.rollup_1h
    where ts = v_hour_ts
  ) = 1,
    'hour fixture should create exactly one database row';

  select
    cardinality(query_counts) / 2,
    (
      select sum(query_counts[pos + 1])
      from generate_subscripts(query_counts, 1) as pos
      where pos % 2 = 1
    ),
    (
      select sum(wait_counts[pos + 1])
      from generate_subscripts(wait_counts, 1) as pos
      where pos % 2 = 1
    )
    into
      v_stored_rows,
      v_stored_query_total,
      v_stored_wait_total
  from ash.rollup_1h
  where ts = v_hour_ts
    and datid = 0::oid;
  assert v_stored_rows = 100
         and v_stored_query_total = 300
         and v_stored_wait_total = 400,
    format(
      'hourly fixture mismatch: rows=%s query_total=%s wait_total=%s',
      v_stored_rows,
      v_stored_query_total,
      v_stored_wait_total
    );

  truncate ash.rollup_1m;
  select count(*), sum(backend_seconds), sum(pct)
    into v_rows, v_total, v_pct
  from ash.top(
    'query_id',
    v_hour,
    v_hour + interval '1 hour',
    n => 101
  );
  select * into v_residual
  from ash.top(
    'query_id',
    v_hour,
    v_hour + interval '1 hour',
    n => 101
  )
  where key is null;
  select * into v_named
  from ash.top(
    'query_id',
    v_hour,
    v_hour + interval '1 hour',
    n => 101
  )
  where key = '1';

  assert v_rows = 101
         and v_total = 400.00
         and v_pct = 100.00
         and v_residual.source = 'rollup_1h'
         and v_residual.effective_bucket = interval '1 hour'
         and v_residual.backend_seconds = 100.00
         and v_residual.pct = 25.00
         and v_residual.peak_aas is null
         and v_residual.p99_aas is null
         and v_named.backend_seconds = 3.00
         and v_named.pct = 0.75,
    format(
      'rollup_1h top-100 mismatch: rows=%s total=%s pct=%s residual=%s/%s named=%s/%s source=%s bucket=%s peak=%s p99=%s',
      v_rows,
      v_total,
      v_pct,
      v_residual.backend_seconds,
      v_residual.pct,
      v_named.backend_seconds,
      v_named.pct,
      v_residual.source,
      v_residual.effective_bucket,
      v_residual.peak_aas,
      v_residual.p99_aas
    );

  raise notice
    'issue #136 rollup_1h top-100 GREEN: rows=101 total=400.00 pct=100.00 residual=100.00/25.00';
end $$;

-- Exact query filters force raw, but a young/raw-only installation
-- has no coarser pre-raw history to lose and must remain queryable.
do $$
declare
  v_ts int4 := (
    ash.ts_from_timestamptz(
      date_trunc('minute', now() - interval '5 minutes')
    ) / 60
  ) * 60;
  v_old_ts int4 := (
    ash.ts_from_timestamptz(
      date_trunc('minute', now() - interval '2 hours')
    ) / 60
  ) * 60;
  v_hour_ts int4;
  v_minute_idx int;
  v_minute_counts int4[];
  v_datid oid;
  v_wait_id smallint;
  v_q1 int4;
  v_aas record;
  v_until_aas record;
  v_top record;
  v_compare record;
  v_timeline_rows int;
  v_timeline_data_rows int;
  v_timeline_source_min text;
  v_timeline_source_max text;
  v_timeline_avg numeric;
  v_aas_error text;
  v_until_aas_error text;
  v_inverted_error text;
  v_timeline_error text;
  v_top_error text;
  v_compare_error text;
  v_other_db_error text;
begin
  truncate ash.sample_0, ash.sample_1, ash.sample_2;
  truncate ash.query_map_0, ash.query_map_1, ash.query_map_2
    restart identity;
  truncate ash.rollup_1m, ash.rollup_1h;
  update ash.config
  set current_slot = 0,
      sample_interval = interval '1 second',
      last_rollup_1m_ts = null,
      last_rollup_1h_ts = null
  where singleton;
  select oid into v_datid
  from pg_database
  where datname = current_database();
  select ash._register_wait('active', 'CPU*', 'CPU*')
    into v_wait_id;
  insert into ash.query_map_0 (query_id) values (111);
  select id into v_q1
  from ash.query_map_0
  where query_id = 111;
  insert into ash.sample (
    sample_ts, datid, active_count, data, slot
  ) values (
    v_ts,
    v_datid,
    7,
    array[-v_wait_id, 7] || array_fill(v_q1, array[7]),
    0
  );

  -- A normal young install has already rolled its first completed
  -- minute. _pick_source() names that rollup via its fallback even
  -- though it starts alongside raw and holds no older history.
  insert into ash.rollup_1m (
    ts, datid, samples, peak_backends, wait_counts, query_counts
  ) values (
    v_ts,
    v_datid,
    1,
    7,
    array[v_wait_id, 7]::int4[],
    array[111, 7]::int8[]
  );

  assert now() - interval '1 hour' < ash._raw_oldest_sample()
         and ash._rollup_1m_retention_start()
             = date_trunc('minute', ash._raw_oldest_sample())
         and ash._pick_source(now() - interval '1 hour')
             = 'rollup_1m',
    format(
      'young-install precondition mismatch: raw_oldest=%s rollup_start=%s pick=%s',
      ash._raw_oldest_sample(),
      ash._rollup_1m_retention_start(),
      ash._pick_source(now() - interval '1 hour')
    );

  begin
    select * into v_aas from ash.aas(query_id => 111);
  exception when others then
    v_aas_error := sqlerrm;
  end;
  begin
    select * into v_until_aas
    from ash.aas(
      until => date_trunc('minute', now()),
      query_id => 111
    );
  exception when others then
    v_until_aas_error := sqlerrm;
  end;
  begin
    perform * from ash.aas(
      since => date_trunc('minute', now()),
      until => date_trunc('minute', now()) - interval '1 minute',
      query_id => 111
    );
  exception when others then
    v_inverted_error := sqlerrm;
  end;
  begin
    select
      count(*),
      count(*) filter (where data_points = 1),
      min(source),
      max(source),
      max(avg_aas) filter (where data_points = 1)
      into
        v_timeline_rows,
        v_timeline_data_rows,
        v_timeline_source_min,
        v_timeline_source_max,
        v_timeline_avg
    from ash.timeline(query_id => 111);
  exception when others then
    v_timeline_error := sqlerrm;
  end;
  begin
    select * into v_top
    from ash.top('database', query_id => 111)
    where key = current_database();
  exception when others then
    v_top_error := sqlerrm;
  end;
  begin
    select * into v_compare
    from ash.compare(
      date_trunc('minute', now() - interval '10 minutes'),
      date_trunc('minute', now()),
      date_trunc('minute', now() - interval '10 minutes'),
      date_trunc('minute', now()),
      dimension => 'database',
      query_id => 111
    )
    where key = current_database();
  exception when others then
    v_compare_error := sqlerrm;
  end;

  assert v_aas_error is null
         and v_until_aas_error is null
         and v_timeline_error is null
         and v_top_error is null
         and v_compare_error is null,
    format(
      'young-install query_id errors: aas=%s until_aas=%s timeline=%s top=%s compare=%s',
      coalesce(v_aas_error, '<none>'),
      coalesce(v_until_aas_error, '<none>'),
      coalesce(v_timeline_error, '<none>'),
      coalesce(v_top_error, '<none>'),
      coalesce(v_compare_error, '<none>')
    );
  assert v_aas.source = 'raw'
         and v_aas.buckets_with_data = 1
         and v_aas.backend_seconds = 7.00,
    format(
      'young-install aas mismatch: source=%s buckets=%s seconds=%s',
      v_aas.source,
      v_aas.buckets_with_data,
      v_aas.backend_seconds
    );
  assert v_until_aas.period_start
           = date_trunc('minute', now()) - interval '1 hour'
         and v_until_aas.period_end = date_trunc('minute', now())
         and v_until_aas.source = 'raw'
         and v_until_aas.backend_seconds = 7.00,
    format(
      'young-install until-only aas mismatch: period=%s..%s source=%s seconds=%s',
      v_until_aas.period_start,
      v_until_aas.period_end,
      v_until_aas.source,
      v_until_aas.backend_seconds
    );
  assert v_inverted_error
           = 'ash.aas: since must be less than or equal to until',
    'young-install inverted-window validation mismatch: '
    || coalesce(v_inverted_error, '<none>');
  assert v_timeline_rows = 60
         and v_timeline_data_rows = 1
         and v_timeline_source_min = 'raw'
         and v_timeline_source_max = 'raw'
         and v_timeline_avg = 0.12,
    format(
      'young-install timeline mismatch: rows=%s data_rows=%s source=%s..%s avg=%s',
      v_timeline_rows,
      v_timeline_data_rows,
      v_timeline_source_min,
      v_timeline_source_max,
      v_timeline_avg
    );
  assert v_top.source = 'raw'
         and v_top.backend_seconds = 7.00
         and v_top.pct = 100.00,
    format(
      'young-install top mismatch: source=%s seconds=%s pct=%s',
      v_top.source,
      v_top.backend_seconds,
      v_top.pct
    );
  assert v_compare.source_1 = 'raw'
         and v_compare.source_2 = 'raw'
         and v_compare.avg_delta = 0.00
         and v_compare.pct_1 = 100.00
         and v_compare.pct_2 = 100.00,
    format(
      'young-install compare mismatch: source=%s/%s delta=%s pct=%s/%s',
      v_compare.source_1,
      v_compare.source_2,
      v_compare.avg_delta,
      v_compare.pct_1,
      v_compare.pct_2
    );

  -- The same rule applies to a first hourly row with valid minute
  -- detail: its hour envelope starts earlier, but a non-NULL slot
  -- only in raw's first retained minute proves no older coverage.
  v_hour_ts := (v_ts / 3600) * 3600;
  v_minute_idx := (v_ts - v_hour_ts) / 60 + 1;
  v_minute_counts := array_fill(null::int4, array[60]);
  v_minute_counts[v_minute_idx] := 7;
  truncate ash.rollup_1m;
  insert into ash.rollup_1h (
    ts,
    datid,
    samples,
    peak_backends,
    wait_counts,
    query_counts,
    minute_counts
  ) values (
    v_hour_ts,
    v_datid,
    1,
    7,
    array[v_wait_id, 7]::int4[],
    array[111, 7]::int8[],
    v_minute_counts
  );
  assert ash._pick_source(now() - interval '1 hour') = 'rollup_1h',
    'young-install hourly fallback precondition mismatch';
  v_aas_error := null;
  begin
    select * into v_aas from ash.aas(query_id => 111);
  exception when others then
    v_aas_error := sqlerrm;
  end;
  assert v_aas_error is null
         and v_aas.source = 'raw'
         and v_aas.backend_seconds = 7.00,
    format(
      'same-minute hourly query mismatch: error=%s source=%s seconds=%s',
      coalesce(v_aas_error, '<none>'),
      v_aas.source,
      v_aas.backend_seconds
    );

  -- Once a coarser source really is selected for pre-raw history,
  -- an explicit query filter must reject compacted rollup data.
  -- Keep the young raw row and put the old rollup strictly between
  -- the logical ring boundary and physical oldest sample. This
  -- proves the guard uses physical coverage when forcing raw would
  -- otherwise discard retained attribution.
  update ash.config
  set rotated_at = clock_timestamp()
  where singleton;
  assert ash._raw_retention_start()
           < ash.ts_to_timestamptz(v_old_ts)
         and ash.ts_to_timestamptz(v_old_ts + 60)
           <= date_trunc('minute', ash._raw_oldest_sample()),
    format(
      'logical/physical split precondition mismatch: logical=%s old=%s..%s physical=%s',
      ash._raw_retention_start(),
      ash.ts_to_timestamptz(v_old_ts),
      ash.ts_to_timestamptz(v_old_ts + 60),
      ash._raw_oldest_sample()
    );
  truncate ash.rollup_1m;
  insert into ash.rollup_1m (
    ts, datid, samples, peak_backends, wait_counts, query_counts
  ) values (
    v_old_ts,
    v_datid,
    60,
    2,
    array[v_wait_id, 2]::int4[],
    '{}'::int8[]
  );

  -- Coarser history for one database must not reject an exact query
  -- for a different/unknown database: no matching retained row is
  -- discarded by forcing that filtered read to raw.
  begin
    select * into v_aas from ash.aas(
      ash.ts_to_timestamptz(v_old_ts),
      ash.ts_to_timestamptz(v_old_ts + 60),
      query_id => 222,
      database => 'pgash_missing_database'
    );
  exception when others then
    v_other_db_error := sqlerrm;
  end;
  assert v_other_db_error is null
         and v_aas.source = 'raw'
         and v_aas.buckets_with_data = 0
         and v_aas.backend_seconds = 0.00,
    format(
      'database-scoped exact query mismatch: error=%s source=%s buckets=%s seconds=%s',
      coalesce(v_other_db_error, '<none>'),
      v_aas.source,
      v_aas.buckets_with_data,
      v_aas.backend_seconds
    );

  begin
    perform * from ash.aas(
      ash.ts_to_timestamptz(v_old_ts),
      ash.ts_to_timestamptz(v_old_ts + 60),
      query_id => 222
    );
    raise exception 'ash.aas query_id filter should require raw';
  exception when others then
    assert sqlerrm like '%exact raw query attribution%'
           and sqlerrm like '%entirely outside raw retention%',
      'ash.aas query_id retention error mismatch: ' || sqlerrm;
  end;
  begin
    perform * from ash.timeline(
      ash.ts_to_timestamptz(v_old_ts),
      ash.ts_to_timestamptz(v_old_ts + 60),
      query_id => 222
    );
    raise exception 'ash.timeline query_id filter should require raw';
  exception when others then
    assert sqlerrm like '%exact raw query attribution%'
           and sqlerrm like '%entirely outside raw retention%',
      'ash.timeline query_id retention error mismatch: ' || sqlerrm;
  end;
  begin
    perform * from ash.top(
      'database',
      ash.ts_to_timestamptz(v_old_ts),
      ash.ts_to_timestamptz(v_old_ts + 60),
      query_id => 222
    );
    raise exception 'ash.top query_id filter should require raw';
  exception when others then
    assert sqlerrm like '%exact raw query attribution%'
           and sqlerrm like '%entirely outside raw retention%',
      'ash.top query_id retention error mismatch: ' || sqlerrm;
  end;
  begin
    perform * from ash.compare(
      ash.ts_to_timestamptz(v_old_ts),
      ash.ts_to_timestamptz(v_old_ts + 60),
      ash.ts_to_timestamptz(v_old_ts),
      ash.ts_to_timestamptz(v_old_ts + 60),
      dimension => 'database',
      query_id => 222
    );
    raise exception 'ash.compare query_id filter should require raw';
  exception when others then
    assert sqlerrm like '%exact raw query attribution%'
           and sqlerrm like '%entirely outside raw retention%',
      'ash.compare query_id retention error mismatch: ' || sqlerrm;
  end;

  truncate ash.sample_0, ash.sample_1, ash.sample_2;
  truncate ash.rollup_1m, ash.rollup_1h;
  truncate ash.query_map_0, ash.query_map_1, ash.query_map_2;
  update ash.config
  set last_rollup_1m_ts = null,
      last_rollup_1h_ts = null
  where singleton;
  raise notice
    'issue #136 young-install GREEN: same-minute 1m/1h rollups ignored; aas=raw/7.00 until-only=raw/7.00 inverted=validated timeline=raw/0.12 top=raw/7.00 compare=raw/raw; other-db allowed; old rollup rejected';
end $$;
