/* -------------------------------------------------------------------------
 * Helper / codec surfaces
 * ------------------------------------------------------------------------- */
do $feature_helpers$
declare
  v_fixture ash_feature_context%rowtype;
  v_actual jsonb;
  v_decoded_at jsonb;
  v_decoded_row jsonb;
  v_decoded_ts jsonb;
  v_expected_decoded_ts jsonb;
begin
  select *
  into strict v_fixture
  from ash_feature_context;

  assert ash.epoch() = '2026-01-01T00:00:00+00:00'::timestamptz,
    format(
      '[%s] ash.epoch: expected the immutable 2026-01-01 UTC epoch, got %s',
      pg_catalog.current_setting('ash.feature_mode'),
      ash.epoch()
    );
  assert ash.ts_from_timestamptz(
    '2026-01-02T10:17:36+00:00'::timestamptz
  ) = 123456,
    format(
      '[%s] ash.ts_from_timestamptz: expected 123456, got %s',
      pg_catalog.current_setting('ash.feature_mode'),
      ash.ts_from_timestamptz(
        '2026-01-02T10:17:36+00:00'::timestamptz
      )
    );
  assert ash.ts_to_timestamptz(123456) =
    '2026-01-02T10:17:36+00:00'::timestamptz,
    format(
      '[%s] ash.ts_to_timestamptz: expected 2026-01-02T10:17:36Z, got %s',
      pg_catalog.current_setting('ash.feature_mode'),
      ash.ts_to_timestamptz(123456)
    );
  assert ash.current_slot() = 0,
    format(
      '[%s] ash.current_slot: expected fixture slot 0, got %s',
      pg_catalog.current_setting('ash.feature_mode'),
      ash.current_slot()
    );

  select pg_catalog.jsonb_agg(
    pg_catalog.jsonb_build_array(
      decoded.wait_event,
      decoded.query_id,
      decoded.count
    )
    order by decoded.wait_event, decoded.query_id
  )
  into v_decoded_row
  from ash.sample as sample_row
  cross join lateral ash.decode_sample(
    sample_row.data,
    sample_row.slot
  ) as decoded
  where sample_row.sample_ts =
    ash.ts_from_timestamptz(v_fixture.fixture_start);

  v_actual := pg_catalog.jsonb_build_array(
    pg_catalog.jsonb_build_array('CPU*', 10101, 1),
    pg_catalog.jsonb_build_array('IO:DataFileRead', 10101, 1),
    pg_catalog.jsonb_build_array('IO:DataFileRead', 20202, 1)
  );
  assert v_decoded_row = v_actual,
    format(
      '[%s] ash.decode_sample(data,slot): expected exact wait/query expansion %s, got %s',
      pg_catalog.current_setting('ash.feature_mode'),
      v_actual,
      v_decoded_row
    );

  select pg_catalog.jsonb_agg(
    pg_catalog.jsonb_build_array(
      decoded.datid,
      decoded.wait_event,
      decoded.query_id,
      decoded.count
    )
    order by decoded.wait_event, decoded.query_id
  )
  into v_decoded_ts
  from ash.decode_sample(
    ash.ts_from_timestamptz(v_fixture.fixture_start)
  ) as decoded;

  select pg_catalog.jsonb_agg(
    pg_catalog.jsonb_build_array(
      decoded.datid,
      decoded.wait_event,
      decoded.query_id,
      decoded.count
    )
    order by decoded.wait_event, decoded.query_id
  )
  into v_decoded_at
  from ash.decode_sample_at(v_fixture.fixture_start) as decoded;

  v_expected_decoded_ts := pg_catalog.jsonb_build_array(
    pg_catalog.jsonb_build_array(
      v_fixture.datid,
      'CPU*',
      10101,
      1
    ),
    pg_catalog.jsonb_build_array(
      v_fixture.datid,
      'IO:DataFileRead',
      10101,
      1
    ),
    pg_catalog.jsonb_build_array(
      v_fixture.datid,
      'IO:DataFileRead',
      20202,
      1
    )
  );
  assert v_decoded_ts = v_expected_decoded_ts
    and v_decoded_at = v_expected_decoded_ts,
    format(
      '[%s] ash.decode_sample(int4)/decode_sample_at: expected exact rows %s, got int4=%s timestamptz=%s',
      pg_catalog.current_setting('ash.feature_mode'),
      v_expected_decoded_ts,
      v_decoded_ts,
      v_decoded_at
    );
end
$feature_helpers$;

/* -------------------------------------------------------------------------
 * ash.aas()
 * ------------------------------------------------------------------------- */
do $feature_aas$
declare
  v_fixture ash_feature_context%rowtype;
  v_actual record;
begin
  select *
  into strict v_fixture
  from ash_feature_context;

  select *
  into strict v_actual
  from ash.aas(v_fixture.fixture_start, v_fixture.fixture_end);

  assert v_actual.period_start = v_fixture.fixture_start
    and v_actual.period_end = v_fixture.fixture_end
    and v_actual.source = 'raw'
    and v_actual.buckets_expected = 4
    and v_actual.buckets_with_data = 4
    and v_actual.avg_aas = 4.00
    and v_actual.peak_aas = 5.00
    and v_actual.p99_aas = 4.97
    and v_actual.backend_seconds = 960.00,
    format(
      '[%s] ash.aas: expected raw/4 buckets/avg 4.00/peak 5.00/p99 4.97/960 backend-seconds, got %s',
      pg_catalog.current_setting('ash.feature_mode'),
      pg_catalog.row_to_json(v_actual)
    );

  select *
  into strict v_actual
  from ash.aas(
    v_fixture.fixture_start,
    v_fixture.fixture_end,
    wait_event_type => 'IO'
  );
  assert v_actual.source = 'raw'
    and v_actual.buckets_expected = 4
    and v_actual.buckets_with_data = 4
    and v_actual.avg_aas = 1.75
    and v_actual.peak_aas = 3.00
    and v_actual.p99_aas = 2.97
    and v_actual.backend_seconds = 420.00,
    format(
      '[%s] ash.aas filter: expected exact IO metrics 1.75/3.00/2.97/420, got %s',
      pg_catalog.current_setting('ash.feature_mode'),
      pg_catalog.row_to_json(v_actual)
    );

  select *
  into strict v_actual
  from ash.aas(
    v_fixture.fixture_start,
    v_fixture.fixture_end,
    wait_event_type => 'IPC'
  );
  assert v_actual.source = 'raw'
    and v_actual.buckets_expected = 4
    and v_actual.buckets_with_data = 4
    and v_actual.avg_aas = 0
    and v_actual.peak_aas = 0
    and v_actual.p99_aas = 0
    and v_actual.backend_seconds = 0,
    format(
      '[%s] ash.aas no-match filter: expected measured coverage with exact zero metrics, got %s',
      pg_catalog.current_setting('ash.feature_mode'),
      pg_catalog.row_to_json(v_actual)
    );

  select *
  into strict v_actual
  from ash.aas(
    v_fixture.fixture_start + interval '2 minutes',
    v_fixture.fixture_start + interval '3 minutes'
  );
  assert v_actual.buckets_expected = 1
    and v_actual.buckets_with_data = 1
    and v_actual.avg_aas = 5.00
    and v_actual.peak_aas = 5.00
    and v_actual.p99_aas = 5.00
    and v_actual.backend_seconds = 300.00,
    format(
      '[%s] ash.aas single-sample window: expected exact 5.00 AAS and 300 backend-seconds, got %s',
      pg_catalog.current_setting('ash.feature_mode'),
      pg_catalog.row_to_json(v_actual)
    );

  select *
  into strict v_actual
  from ash.aas(
    v_fixture.fixture_start + interval '1 minute',
    v_fixture.fixture_start + interval '1 minute'
  );
  assert v_actual.period_end =
    v_fixture.fixture_start + interval '2 minutes'
    and v_actual.buckets_expected = 1
    and v_actual.buckets_with_data = 1
    and v_actual.avg_aas = 4.00
    and v_actual.peak_aas = 4.00
    and v_actual.p99_aas = 4.00,
    format(
      '[%s] ash.aas empty/degenerate window: expected documented one-minute expansion with exact 4.00 AAS, got %s',
      pg_catalog.current_setting('ash.feature_mode'),
      pg_catalog.row_to_json(v_actual)
    );

  select *
  into strict v_actual
  from ash.aas(
    v_fixture.fixture_start,
    v_fixture.fixture_end,
    bucket => interval '1 hour'
  );
  assert v_actual.buckets_expected = 1
    and v_actual.buckets_with_data = 1
    and v_actual.avg_aas = 4.00
    and v_actual.peak_aas = 4.00
    and v_actual.p99_aas = 4.00
    and v_actual.backend_seconds = 960.00,
    format(
      '[%s] ash.aas bucket larger than window: expected one 4.00-AAS bucket, got %s',
      pg_catalog.current_setting('ash.feature_mode'),
      pg_catalog.row_to_json(v_actual)
    );
end
$feature_aas$;

/* -------------------------------------------------------------------------
 * ash.timeline()
 * ------------------------------------------------------------------------- */
do $feature_timeline$
declare
  v_fixture ash_feature_context%rowtype;
  v_actual record;
  v_avg numeric[];
  v_buckets timestamptz[];
  v_data_points bigint[];
  v_peak numeric[];
  v_p99 numeric[];
  v_sources text[];
begin
  select *
  into strict v_fixture
  from ash_feature_context;

  select
    pg_catalog.array_agg(bucket_start order by bucket_start),
    pg_catalog.array_agg(source order by bucket_start),
    pg_catalog.array_agg(data_points order by bucket_start),
    pg_catalog.array_agg(avg_aas order by bucket_start),
    pg_catalog.array_agg(peak_aas order by bucket_start),
    pg_catalog.array_agg(p99_aas order by bucket_start)
  into
    v_buckets,
    v_sources,
    v_data_points,
    v_avg,
    v_peak,
    v_p99
  from ash.timeline(
    v_fixture.fixture_start,
    v_fixture.fixture_end,
    interval '1 minute'
  );

  assert v_buckets = array[
      v_fixture.fixture_start,
      v_fixture.fixture_start + interval '1 minute',
      v_fixture.fixture_start + interval '2 minutes',
      v_fixture.fixture_start + interval '3 minutes'
    ]::timestamptz[]
    and v_sources = array['raw', 'raw', 'raw', 'raw']::text[]
    and v_data_points = array[1, 1, 1, 1]::bigint[]
    and v_avg = array[3, 4, 5, 4]::numeric[]
    and v_peak = array[3, 4, 5, 4]::numeric[]
    and v_p99 = array[3, 4, 5, 4]::numeric[],
    format(
      '[%s] ash.timeline: expected four exact raw buckets [3,4,5,4], got buckets=%s sources=%s points=%s avg=%s peak=%s p99=%s',
      pg_catalog.current_setting('ash.feature_mode'),
      v_buckets,
      v_sources,
      v_data_points,
      v_avg,
      v_peak,
      v_p99
    );

  select
    pg_catalog.array_agg(data_points order by bucket_start),
    pg_catalog.array_agg(avg_aas order by bucket_start)
  into
    v_data_points,
    v_avg
  from ash.timeline(
    v_fixture.fixture_start,
    v_fixture.fixture_end,
    interval '1 minute',
    wait_event_type => 'IPC'
  );
  assert v_data_points = array[1, 1, 1, 1]::bigint[]
    and v_avg = array[0, 0, 0, 0]::numeric[],
    format(
      '[%s] ash.timeline no-match filter: expected four measured-zero buckets, got points=%s avg=%s',
      pg_catalog.current_setting('ash.feature_mode'),
      v_data_points,
      v_avg
    );

  select *
  into strict v_actual
  from ash.timeline(
    v_fixture.fixture_start,
    v_fixture.fixture_end,
    interval '1 hour'
  );
  assert v_actual.bucket_start =
    pg_catalog.date_trunc('hour', v_fixture.fixture_start)
    and v_actual.source = 'raw'
    and v_actual.data_points = 4
    and v_actual.avg_aas = 4.00
    and v_actual.peak_aas = 5.00
    and v_actual.p99_aas = 4.97,
    format(
      '[%s] ash.timeline bucket larger than window: expected one calendar bucket with avg/peak/p99 4.00/5.00/4.97, got %s',
      pg_catalog.current_setting('ash.feature_mode'),
      pg_catalog.row_to_json(v_actual)
    );
end
$feature_timeline$;

/* -------------------------------------------------------------------------
 * ash.top()
 * ------------------------------------------------------------------------- */
do $feature_top$
declare
  v_fixture ash_feature_context%rowtype;
  v_actual record;
  v_count bigint;
  v_metrics jsonb;
  v_pct numeric;
begin
  select *
  into strict v_fixture
  from ash_feature_context;

  select
    pg_catalog.count(*),
    pg_catalog.sum(top_row.pct),
    pg_catalog.jsonb_object_agg(
      top_row.key,
      pg_catalog.jsonb_build_array(
        top_row.source,
        top_row.avg_aas,
        top_row.peak_aas,
        top_row.p99_aas,
        top_row.backend_seconds,
        top_row.pct
      )
    )
  into
    v_count,
    v_pct,
    v_metrics
  from ash.top(
    'wait_event_type',
    v_fixture.fixture_start,
    v_fixture.fixture_end,
    n => 10
  ) as top_row;

  assert v_count = 3
    and v_pct = 100.00
    and v_metrics = pg_catalog.jsonb_build_object(
      'CPU*',
      pg_catalog.jsonb_build_array(
        'raw',
        1.00,
        1.00,
        1.00,
        240.00,
        25.00
      ),
      'IO',
      pg_catalog.jsonb_build_array(
        'raw',
        1.75,
        3.00,
        2.97,
        420.00,
        43.75
      ),
      'Lock',
      pg_catalog.jsonb_build_array(
        'raw',
        1.25,
        4.00,
        3.91,
        300.00,
        31.25
      )
    ),
    format(
      '[%s] ash.top(wait_event_type): expected exact metrics and pct sum 100.00, got count=%s pct=%s metrics=%s',
      pg_catalog.current_setting('ash.feature_mode'),
      v_count,
      v_pct,
      v_metrics
    );

  select pg_catalog.jsonb_object_agg(
    top_row.key,
    pg_catalog.jsonb_build_array(
      top_row.avg_aas,
      top_row.peak_aas,
      top_row.p99_aas,
      top_row.backend_seconds,
      top_row.pct
    )
  )
  into v_metrics
  from ash.top(
    'wait_event',
    v_fixture.fixture_start,
    v_fixture.fixture_end,
    n => 10
  ) as top_row;
  assert v_metrics = pg_catalog.jsonb_build_object(
    'CPU*',
    pg_catalog.jsonb_build_array(1.00, 1.00, 1.00, 240.00, 25.00),
    'IO:DataFileRead',
    pg_catalog.jsonb_build_array(1.75, 3.00, 2.97, 420.00, 43.75),
    'Lock:tuple',
    pg_catalog.jsonb_build_array(1.25, 4.00, 3.91, 300.00, 31.25)
  ),
    format(
      '[%s] ash.top(wait_event): expected exact event breakdown, got %s',
      pg_catalog.current_setting('ash.feature_mode'),
      v_metrics
    );

  select *
  into strict v_actual
  from ash.top(
    'wait_event_type',
    v_fixture.fixture_start,
    v_fixture.fixture_end,
    n => 1
  );
  assert v_actual.key = 'IO'
    and v_actual.avg_aas = 1.75
    and v_actual.pct = 43.75,
    format(
      '[%s] ash.top n=>1 avg rank: expected IO 1.75/43.75, got %s',
      pg_catalog.current_setting('ash.feature_mode'),
      pg_catalog.row_to_json(v_actual)
    );

  select *
  into strict v_actual
  from ash.top(
    'wait_event_type',
    v_fixture.fixture_start,
    v_fixture.fixture_end,
    n => 1,
    order_by => 'peak'
  );
  assert v_actual.key = 'Lock'
    and v_actual.peak_aas = 4.00,
    format(
      '[%s] ash.top n=>1 peak rank: expected Lock peak 4.00, got %s',
      pg_catalog.current_setting('ash.feature_mode'),
      pg_catalog.row_to_json(v_actual)
    );

  select *
  into strict v_actual
  from ash.top(
    'database',
    v_fixture.fixture_start,
    v_fixture.fixture_end,
    n => 10
  );
  assert v_actual.key = pg_catalog.current_database()
    and v_actual.source = 'raw'
    and v_actual.avg_aas = 4.00
    and v_actual.peak_aas = 5.00
    and v_actual.p99_aas = 4.97
    and v_actual.backend_seconds = 960.00
    and v_actual.pct = 100.00,
    format(
      '[%s] ash.top(database): expected current database at exact 4.00/5.00/4.97/960/100, got %s',
      pg_catalog.current_setting('ash.feature_mode'),
      pg_catalog.row_to_json(v_actual)
    );

  select pg_catalog.count(*)
  into v_count
  from ash.top(
    'wait_event',
    v_fixture.fixture_start,
    v_fixture.fixture_end,
    wait_event_type => 'IPC'
  );
  assert v_count = 0,
    format(
      '[%s] ash.top no-match filter: expected zero rows, got %s',
      pg_catalog.current_setting('ash.feature_mode'),
      v_count
    );

  select *
  into strict v_actual
  from ash.top(
    'query_id',
    v_fixture.fixture_start,
    v_fixture.fixture_end,
    wait_event => 'IO:DataFileRead',
    n => 1
  );
  assert v_actual.key = '20202'
    and v_actual.query_text is null
    and v_actual.source = 'raw'
    and v_actual.avg_aas = 1.50
    and v_actual.peak_aas = 3.00
    and v_actual.p99_aas = 2.97
    and v_actual.backend_seconds = 360.00
    and v_actual.pct = 85.71,
    format(
      '[%s] ash.top raw wait/query tie: expected qid 20202 exact metrics and null synthetic query text, got %s',
      pg_catalog.current_setting('ash.feature_mode'),
      pg_catalog.row_to_json(v_actual)
    );
end
$feature_top$;

/* -------------------------------------------------------------------------
 * ash.compare()
 * ------------------------------------------------------------------------- */
do $feature_compare$
declare
  v_fixture ash_feature_context%rowtype;
  v_actual record;
  v_count bigint;
  v_metrics jsonb;
begin
  select *
  into strict v_fixture
  from ash_feature_context;

  select *
  into strict v_actual
  from ash.compare(
    v_fixture.fixture_start,
    v_fixture.fixture_start + interval '2 minutes',
    v_fixture.fixture_start + interval '2 minutes',
    v_fixture.fixture_end
  );
  assert v_actual.key = 'overall'
    and v_actual.query_text is null
    and v_actual.avg_aas_1 = 3.50
    and v_actual.avg_aas_2 = 4.50
    and v_actual.avg_delta = 1.00
    and v_actual.peak_aas_1 = 4.00
    and v_actual.peak_aas_2 = 5.00
    and v_actual.p99_aas_1 = 3.99
    and v_actual.p99_aas_2 = 4.99
    and v_actual.pct_1 is null
    and v_actual.pct_2 is null,
    format(
      '[%s] ash.compare overall: expected exact before/after 3.50->4.50 delta 1.00, got %s',
      pg_catalog.current_setting('ash.feature_mode'),
      pg_catalog.row_to_json(v_actual)
    );

  select
    pg_catalog.count(*),
    pg_catalog.jsonb_object_agg(
      compare_row.key,
      pg_catalog.jsonb_build_array(
        compare_row.avg_aas_1,
        compare_row.avg_aas_2,
        compare_row.avg_delta,
        compare_row.peak_aas_1,
        compare_row.peak_aas_2,
        compare_row.p99_aas_1,
        compare_row.p99_aas_2,
        compare_row.pct_1,
        compare_row.pct_2
      )
    )
  into
    v_count,
    v_metrics
  from ash.compare(
    v_fixture.fixture_start,
    v_fixture.fixture_start + interval '2 minutes',
    v_fixture.fixture_start + interval '2 minutes',
    v_fixture.fixture_end,
    dimension => 'wait_event_type',
    n => 10
  ) as compare_row;
  assert v_count = 3
    and v_metrics = pg_catalog.jsonb_build_object(
      'CPU*',
      pg_catalog.jsonb_build_array(
        1.00, 1.00, 0.00, 1.00, 1.00, 1.00, 1.00, 28.57, 22.22
      ),
      'IO',
      pg_catalog.jsonb_build_array(
        2.50, 1.00, -1.50, 3.00, 2.00, 2.99, 1.98, 71.43, 22.22
      ),
      'Lock',
      pg_catalog.jsonb_build_array(
        null, 2.50, 2.50, null, 4.00, null, 3.97, null, 55.56
      )
    ),
    format(
      '[%s] ash.compare dimension: expected exact full-outer wait deltas, got count=%s metrics=%s',
      pg_catalog.current_setting('ash.feature_mode'),
      v_count,
      v_metrics
    );

  select *
  into strict v_actual
  from ash.compare(
    v_fixture.fixture_start,
    v_fixture.fixture_start + interval '2 minutes',
    v_fixture.fixture_start + interval '2 minutes',
    v_fixture.fixture_end,
    dimension => 'wait_event_type',
    n => 1
  );
  assert v_actual.key = 'Lock'
    and v_actual.avg_delta = 2.50,
    format(
      '[%s] ash.compare n=>1: expected largest absolute delta Lock=2.50, got %s',
      pg_catalog.current_setting('ash.feature_mode'),
      pg_catalog.row_to_json(v_actual)
    );
end
$feature_compare$;

/* -------------------------------------------------------------------------
 * ash.samples()
 * ------------------------------------------------------------------------- */
do $feature_samples$
declare
  v_fixture ash_feature_context%rowtype;
  v_actual record;
  v_count bigint;
  v_query_text_count bigint;
begin
  select *
  into strict v_fixture
  from ash_feature_context;

  select
    pg_catalog.count(*),
    pg_catalog.count(query_text)
  into
    v_count,
    v_query_text_count
  from ash.samples(
    v_fixture.fixture_start,
    v_fixture.fixture_end,
    n => 100
  );
  assert v_count = 16
    and v_query_text_count = 0,
    format(
      '[%s] ash.samples: expected 16 decoded rows and zero synthetic pgss texts, got rows=%s text_rows=%s',
      pg_catalog.current_setting('ash.feature_mode'),
      v_count,
      v_query_text_count
    );

  select *
  into strict v_actual
  from ash.samples(
    v_fixture.fixture_start,
    v_fixture.fixture_end,
    n => 1
  );
  assert v_actual.sample_time =
    v_fixture.fixture_start + interval '3 minutes'
    and v_actual.database_name = pg_catalog.current_database()
    and v_actual.active_backends = 4
    and v_actual.wait_event = 'CPU*'
    and v_actual.query_id is null
    and v_actual.query_text is null,
    format(
      '[%s] ash.samples n=>1: expected newest CPU* unattributed row with active_backends=4, got %s',
      pg_catalog.current_setting('ash.feature_mode'),
      pg_catalog.row_to_json(v_actual)
    );

  select pg_catalog.count(*)
  into v_count
  from ash.samples(
    v_fixture.fixture_start,
    v_fixture.fixture_end,
    n => 100,
    query_id => 30303
  );
  assert v_count = 5,
    format(
      '[%s] ash.samples query filter: expected five qid=30303 rows, got %s',
      pg_catalog.current_setting('ash.feature_mode'),
      v_count
    );

  select pg_catalog.count(*)
  into v_count
  from ash.samples(
    v_fixture.fixture_start,
    v_fixture.fixture_end,
    n => 100,
    wait_event_type => 'IPC'
  );
  assert v_count = 0,
    format(
      '[%s] ash.samples no-match filter: expected zero rows, got %s',
      pg_catalog.current_setting('ash.feature_mode'),
      v_count
    );
end
$feature_samples$;

/* -------------------------------------------------------------------------
 * ash.rollup_minute() and ash.periods()
 * ------------------------------------------------------------------------- */
do $feature_rollup_minute_and_periods$
declare
  v_fixture ash_feature_context%rowtype;
  v_periods jsonb;
  v_rollup record;
  v_rollup_rows bigint;
  v_rolled int;
begin
  select *
  into strict v_fixture
  from ash_feature_context;

  select ash.rollup_minute(5)
  into v_rolled;
  assert v_rolled = 5,
    format(
      '[%s] ash.rollup_minute return: expected five processed grains (four data-bearing plus one empty), got %s',
      pg_catalog.current_setting('ash.feature_mode'),
      v_rolled
    );

  select pg_catalog.count(*)
  into v_rollup_rows
  from ash.rollup_1m;

  assert v_rollup_rows = 4,
    format(
      '[%s] ash.rollup_minute rows: expected exactly four data-bearing rollup_1m rows, got %s',
      pg_catalog.current_setting('ash.feature_mode'),
      v_rollup_rows
    );

  select *
  into strict v_rollup
  from ash.rollup_1m
  where ts = ash.ts_from_timestamptz(v_fixture.fixture_start);
  assert v_rollup.datid = v_fixture.datid
    and v_rollup.samples = 1
    and v_rollup.peak_backends = 3
    and v_rollup.wait_counts = array[
      v_fixture.io_wait_id, 2, v_fixture.cpu_wait_id, 1
    ]::int4[]
    and v_rollup.query_counts = array[10101, 2, 20202, 1]::int8[],
    format(
      '[%s] ash.rollup_minute minute 0: expected exact samples/peak/wait/query arrays, got %s',
      pg_catalog.current_setting('ash.feature_mode'),
      pg_catalog.row_to_json(v_rollup)
    );

  select *
  into strict v_rollup
  from ash.rollup_1m
  where ts = ash.ts_from_timestamptz(
    v_fixture.fixture_start + interval '1 minute'
  );
  assert v_rollup.datid = v_fixture.datid
    and v_rollup.samples = 1
    and v_rollup.peak_backends = 4
    and v_rollup.wait_counts = array[
      v_fixture.io_wait_id, 3, v_fixture.cpu_wait_id, 1
    ]::int4[]
    and v_rollup.query_counts = array[20202, 3, 10101, 1]::int8[],
    format(
      '[%s] ash.rollup_minute minute 1: expected exact samples/peak/wait/query arrays, got %s',
      pg_catalog.current_setting('ash.feature_mode'),
      pg_catalog.row_to_json(v_rollup)
    );

  select *
  into strict v_rollup
  from ash.rollup_1m
  where ts = ash.ts_from_timestamptz(
    v_fixture.fixture_start + interval '2 minutes'
  );
  assert v_rollup.datid = v_fixture.datid
    and v_rollup.samples = 1
    and v_rollup.peak_backends = 5
    and v_rollup.wait_counts = array[
      v_fixture.lock_wait_id, 4, v_fixture.cpu_wait_id, 1
    ]::int4[]
    and v_rollup.query_counts = array[30303, 4, 10101, 1]::int8[],
    format(
      '[%s] ash.rollup_minute minute 2: expected exact samples/peak/wait/query arrays, got %s',
      pg_catalog.current_setting('ash.feature_mode'),
      pg_catalog.row_to_json(v_rollup)
    );

  select *
  into strict v_rollup
  from ash.rollup_1m
  where ts = ash.ts_from_timestamptz(
    v_fixture.fixture_start + interval '3 minutes'
  );
  assert v_rollup.datid = v_fixture.datid
    and v_rollup.samples = 1
    and v_rollup.peak_backends = 4
    and v_rollup.wait_counts = array[
      v_fixture.io_wait_id,
      2,
      v_fixture.cpu_wait_id,
      1,
      v_fixture.lock_wait_id,
      1
    ]::int4[]
    and v_rollup.query_counts = array[20202, 2, 30303, 1]::int8[],
    format(
      '[%s] ash.rollup_minute minute 3: expected exact samples/peak/wait/query arrays, got %s',
      pg_catalog.current_setting('ash.feature_mode'),
      pg_catalog.row_to_json(v_rollup)
    );

  select pg_catalog.jsonb_object_agg(
    period_row.period,
    pg_catalog.jsonb_build_array(
      period_row.source,
      period_row.bucket,
      period_row.buckets_with_data,
      period_row.avg_aas,
      period_row.peak_aas,
      period_row.p99_aas
    )
  )
  into v_periods
  from ash.periods(v_fixture.fixture_end) as period_row;

  assert v_periods = pg_catalog.jsonb_build_object(
      '1m',
      pg_catalog.jsonb_build_array(
        'raw', interval '1 minute', 1, 4.00, 4.00, 4.00
      ),
      '5m',
      pg_catalog.jsonb_build_array(
        'rollup_1m', interval '1 minute', 4, 3.20, 5.00, 4.97
      ),
      '1h',
      pg_catalog.jsonb_build_array(
        'rollup_1m', interval '1 minute', 4, 0.27, 5.00, 4.97
      ),
      '1d',
      pg_catalog.jsonb_build_array(
        'rollup_1m', interval '1 minute', 4, 0.01, 5.00, 4.97
      ),
      '1w',
      pg_catalog.jsonb_build_array(
        'rollup_1m', interval '1 minute', 4, 0.00, 5.00, 4.97
      ),
      '1mo',
      pg_catalog.jsonb_build_array(
        'rollup_1m', interval '1 minute', 4, 0.00, 5.00, 4.97
      )
    ),
    format(
      '[%s] ash.periods: expected six exact window/source/bucket/AAS rows, got %s',
      pg_catalog.current_setting('ash.feature_mode'),
      v_periods
    );
end
$feature_rollup_minute_and_periods$;

/* -------------------------------------------------------------------------
 * ash.report()
 * ------------------------------------------------------------------------- */
do $feature_report$
declare
  v_cluster_name text :=
    pg_catalog.current_setting('cluster_name', true);
  v_fixture ash_feature_context%rowtype;
  v_expected_keys text[];
  v_keys text[];
  v_report jsonb;
begin
  select *
  into strict v_fixture
  from ash_feature_context;

  select ash.report(
    v_fixture.fixture_start,
    v_fixture.fixture_end,
    vcpus => 8,
    n => 3
  )
  into strict v_report;

  select pg_catalog.array_agg(report_key order by report_key)
  into v_keys
  from pg_catalog.jsonb_object_keys(
    v_report - 'cluster_name'
  ) as report_key;
  v_expected_keys := array[
    'aas_avg',
    'aas_p99',
    'aas_p999',
    'aas_worst1m',
    'coverage',
    'top_events_p99',
    'top_events_p999',
    'top_events_worst1m',
    'top_queryids_available',
    'top_queryids_p99',
    'top_queryids_p999',
    'top_queryids_worst1m',
    'vcpus'
  ]::text[];

  assert v_keys = v_expected_keys,
    format(
      '[%s] ash.report keys: expected exact frozen payload keys %s, got %s',
      pg_catalog.current_setting('ash.feature_mode'),
      v_expected_keys,
      v_keys
    );
  assert (
      coalesce(v_cluster_name, '') = ''
      and not (v_report ? 'cluster_name')
    )
    or (
      coalesce(v_cluster_name, '') <> ''
      and v_report ->> 'cluster_name' = v_cluster_name
    ),
    format(
      '[%s] ash.report cluster_name: expected exact optional pass-through %L, got key=%s value=%L',
      pg_catalog.current_setting('ash.feature_mode'),
      v_cluster_name,
      v_report ? 'cluster_name',
      v_report ->> 'cluster_name'
    );
  assert v_report -> 'vcpus' = '8'::jsonb,
    format(
      '[%s] ash.report vcpus: expected pass-through 8, got %s',
      pg_catalog.current_setting('ash.feature_mode'),
      v_report -> 'vcpus'
    );
  assert v_report -> 'aas_avg' = '{
      "total": 4.00,
      "cpu": 1.00,
      "io": 1.75,
      "ipc": 0.00,
      "lock": 1.25,
      "lwlock": 0.00
    }'::jsonb
    and v_report -> 'aas_worst1m' = '{
      "total": 5.00,
      "cpu": 1.00,
      "io": 3.00,
      "ipc": 0.00,
      "lock": 4.00,
      "lwlock": 0.00
    }'::jsonb
    and v_report -> 'aas_p99' = '{
      "total": 4.97,
      "cpu": 1.00,
      "io": 2.97,
      "ipc": 0.00,
      "lock": 3.91,
      "lwlock": 0.00
    }'::jsonb
    and v_report -> 'aas_p999' = '{
      "total": 5.00,
      "cpu": 1.00,
      "io": 3.00,
      "ipc": 0.00,
      "lock": 3.99,
      "lwlock": 0.00
    }'::jsonb,
    format(
      '[%s] ash.report metrics: expected exact class total/avg/extreme values, got avg=%s worst=%s p99=%s p999=%s',
      pg_catalog.current_setting('ash.feature_mode'),
      v_report -> 'aas_avg',
      v_report -> 'aas_worst1m',
      v_report -> 'aas_p99',
      v_report -> 'aas_p999'
    );
  assert v_report -> 'top_events_worst1m' = '{
      "io": ["DataFileRead(3.0)"],
      "ipc": [],
      "lock": ["tuple(4.0)"],
      "lwlock": []
    }'::jsonb
    and v_report -> 'top_events_p99' = '{
      "io": ["DataFileRead(3.0)"],
      "ipc": [],
      "lock": ["tuple(4.0)"],
      "lwlock": []
    }'::jsonb
    and v_report -> 'top_events_p999' = '{
      "io": ["DataFileRead(3.0)"],
      "ipc": [],
      "lock": ["tuple(4.0)"],
      "lwlock": []
    }'::jsonb,
    format(
      '[%s] ash.report top events: expected exact extreme-minute event arrays, got worst=%s p99=%s p999=%s',
      pg_catalog.current_setting('ash.feature_mode'),
      v_report -> 'top_events_worst1m',
      v_report -> 'top_events_p99',
      v_report -> 'top_events_p999'
    );
  assert v_report -> 'top_queryids_worst1m' = '{
      "total": ["30303(4.0)", "10101(1.0)"],
      "io": ["20202(3.0)"],
      "ipc": [],
      "lock": ["30303(4.0)"],
      "lwlock": []
    }'::jsonb
    and v_report -> 'top_queryids_p99' = '{
      "total": ["30303(4.0)", "10101(1.0)"],
      "io": ["20202(3.0)"],
      "lock": ["30303(4.0)"]
    }'::jsonb
    and v_report -> 'top_queryids_p999' = '{
      "total": ["30303(4.0)", "10101(1.0)"],
      "io": ["20202(3.0)"],
      "lock": ["30303(4.0)"]
    }'::jsonb
    and v_report -> 'top_queryids_available' = 'true'::jsonb,
    format(
      '[%s] ash.report top query IDs: expected exact attributed extreme-minute arrays, got worst=%s p99=%s p999=%s available=%s',
      pg_catalog.current_setting('ash.feature_mode'),
      v_report -> 'top_queryids_worst1m',
      v_report -> 'top_queryids_p99',
      v_report -> 'top_queryids_p999',
      v_report -> 'top_queryids_available'
    );
  assert v_report -> 'coverage' = pg_catalog.jsonb_build_object(
    'from',
    v_fixture.fixture_start,
    'to',
    v_fixture.fixture_end,
    'source',
    'rollup_1m',
    'minutes_expected',
    4,
    'minutes_with_data',
    4,
    'raw_retention_start',
    v_fixture.raw_retention_start
  ),
    format(
      '[%s] ash.report coverage: expected exact four-minute rollup coverage, got %s',
      pg_catalog.current_setting('ash.feature_mode'),
      v_report -> 'coverage'
    );
end
$feature_report$;

/* -------------------------------------------------------------------------
 * ash.chart()
 * ------------------------------------------------------------------------- */
do $feature_chart$
declare
  v_fixture ash_feature_context%rowtype;
  v_aas numeric[];
  v_buckets timestamptz[];
  v_charts text[];
  v_details text[];
  v_legend text :=
    '█ IO:DataFileRead  ▓ Lock:tuple  · Other';
begin
  select *
  into strict v_fixture
  from ash_feature_context;

  select
    pg_catalog.array_agg(bucket_start order by ordinal),
    pg_catalog.array_agg(aas order by ordinal),
    pg_catalog.array_agg(detail order by ordinal),
    pg_catalog.array_agg(chart order by ordinal)
  into
    v_buckets,
    v_aas,
    v_details,
    v_charts
  from ash.chart(
    v_fixture.fixture_start,
    v_fixture.fixture_end,
    bucket => interval '1 minute',
    n => 1,
    width => 10,
    color => false
  ) with ordinality as chart_row(
    bucket_start,
    aas,
    detail,
    chart,
    ordinal
  );

  assert v_buckets = array[
      null,
      v_fixture.fixture_start,
      v_fixture.fixture_start + interval '1 minute',
      v_fixture.fixture_start + interval '2 minutes',
      v_fixture.fixture_start + interval '3 minutes'
    ]::timestamptz[]
    and v_aas = array[null, 3, 4, 5, 4]::numeric[]
    and v_details = array[
      null,
      'IO:DataFileRead=2.00 Other=1.00',
      'IO:DataFileRead=3.00 Other=1.00',
      'Lock:tuple=4.00 Other=1.00',
      'IO:DataFileRead=2.00 Lock:tuple=1.00 Other=1.00'
    ]::text[]
    and v_charts = array[
      v_legend,
      pg_catalog.rpad('████··', pg_catalog.length(v_legend)),
      pg_catalog.rpad('██████··', pg_catalog.length(v_legend)),
      pg_catalog.rpad('▓▓▓▓▓▓▓▓··', pg_catalog.length(v_legend)),
      pg_catalog.rpad('████▓▓··', pg_catalog.length(v_legend))
    ]::text[],
    format(
      '[%s] ash.chart: expected exact legend plus four numeric/detail/bar rows, got buckets=%s aas=%s details=%s charts=%s',
      pg_catalog.current_setting('ash.feature_mode'),
      v_buckets,
      v_aas,
      v_details,
      v_charts
    );
end
$feature_chart$;

/* -------------------------------------------------------------------------
 * ash.summary()
 * ------------------------------------------------------------------------- */
do $feature_summary$
declare
  v_fixture ash_feature_context%rowtype;
  v_metrics text[];
  v_values text[];
begin
  select *
  into strict v_fixture
  from ash_feature_context;

  select
    pg_catalog.array_agg(metric order by ordinal),
    pg_catalog.array_agg(value order by ordinal)
  into
    v_metrics,
    v_values
  from ash.summary(
    v_fixture.fixture_start,
    v_fixture.fixture_end
  ) with ordinality as summary_row(metric, value, ordinal);

  assert v_metrics = array[
      'period_start',
      'period_end',
      'source',
      'buckets_with_data',
      'avg_aas',
      'peak_aas',
      'p99_aas',
      'backend_seconds',
      'drill_source',
      'drill_period_start',
      'drill_period_end',
      'drill_effective_bucket',
      'databases_active',
      'top_wait_1',
      'top_wait_2',
      'top_wait_3',
      'top_query_1',
      'top_query_2',
      'top_query_3'
    ]::text[]
    and v_values = array[
      v_fixture.fixture_start::text,
      v_fixture.fixture_end::text,
      'raw',
      '4',
      '4.00',
      '5.00',
      '4.97',
      '960.00',
      'raw',
      v_fixture.fixture_start::text,
      v_fixture.fixture_end::text,
      interval '1 minute'::text,
      '1',
      'IO:DataFileRead (avg_aas 1.75, 43.75%)',
      'Lock:tuple (avg_aas 1.25, 31.25%)',
      'CPU* (avg_aas 1.00, 25.00%)',
      '20202 (avg_aas 1.50, 37.50%)',
      '30303 (avg_aas 1.25, 31.25%)',
      '10101 (avg_aas 1.00, 25.00%)'
    ]::text[],
    format(
      '[%s] ash.summary: expected exact 19-row human summary with headline and drill provenance, got metrics=%s values=%s',
      pg_catalog.current_setting('ash.feature_mode'),
      v_metrics,
      v_values
    );
end
$feature_summary$;

/* -------------------------------------------------------------------------
 * ash.status()
 * ------------------------------------------------------------------------- */
do $feature_status$
declare
  v_expected_cron boolean :=
    pg_catalog.current_setting('ash.feature_expected_cron')::boolean;
  v_fixture ash_feature_context%rowtype;
  v_status jsonb;
begin
  select *
  into strict v_fixture
  from ash_feature_context;

  select pg_catalog.jsonb_object_agg(status_row.metric, status_row.value)
  into v_status
  from ash.status() as status_row;

  assert v_status ->> 'current_slot' = '0'
    and v_status ->> 'sampling_enabled' = 'true'
    and v_status ->> 'sample_interval' = '00:01:00'
    and v_status ->> 'samples_in_current_slot' = '4'
    and v_status ->> 'samples_total' = '4'
    and v_status ->> 'wait_event_map_count' = '3'
    and v_status ->> 'query_map_count' = '3'
    and v_status ->> 'rollup_1m_rows' = '4'
    and (v_status ->> 'raw_retention_start')::timestamptz =
      v_fixture.raw_retention_start
    and (v_status ->> 'rollup_1m_retention_start')::timestamptz =
      v_fixture.fixture_start,
    format(
      '[%s] ash.status: expected exact fixture/config/count/retention side effects, got %s',
      pg_catalog.current_setting('ash.feature_mode'),
      v_status
    );
  assert (v_expected_cron and v_status ->> 'pg_cron_available' = 'yes')
    or (
      not v_expected_cron
      and v_status ->> 'pg_cron_available' =
        'no (use external scheduler)'
    ),
    format(
      '[%s] ash.status degraded scheduler state: expected pg_cron_available=%s, got %s',
      pg_catalog.current_setting('ash.feature_mode'),
      v_expected_cron,
      v_status ->> 'pg_cron_available'
    );
end
$feature_status$;

/* -------------------------------------------------------------------------
 * pg_stat_statements query-text enrichment (present and absent paths).
 * ------------------------------------------------------------------------- */
\if :expected_pgss
select 424242::int as ash_feature_pgss_marker_424242;
\endif

do $feature_pgss_text$
declare
  v_expected_pgss boolean :=
    pg_catalog.current_setting('ash.feature_expected_pgss')::boolean;
  v_fixture ash_feature_context%rowtype;
  v_marker_query text;
  v_marker_query_id bigint;
  v_pgss_schema text;
  v_sample_text text;
  v_top_text text;
begin
  select *
  into strict v_fixture
  from ash_feature_context;

  v_pgss_schema := ash._pgss_schema();
  if not v_expected_pgss then
    assert v_pgss_schema is null,
      format(
        '[%s] no-pgss enrichment: expected no catalog-resolved pgss schema, got %s',
        pg_catalog.current_setting('ash.feature_mode'),
        v_pgss_schema
      );
    return;
  end if;

  assert v_pgss_schema is not null,
    format(
      '[%s] pgss enrichment: expected the installed extension schema, got null',
      pg_catalog.current_setting('ash.feature_mode')
    );
  execute pg_catalog.format(
    'select queryid, query from %I.pg_stat_statements '
    'where query like $1 order by calls desc, queryid limit 1',
    v_pgss_schema
  )
  into strict
    v_marker_query_id,
    v_marker_query
  using '%ash_feature_pgss_marker_424242%';

  update ash.query_map_0
  set query_id = v_marker_query_id
  where query_id = 20202;

  select top_row.query_text
  into strict v_top_text
  from ash.top(
    'query_id',
    v_fixture.fixture_start,
    v_fixture.fixture_end,
    n => 10
  ) as top_row
  where top_row.key = v_marker_query_id::text;
  assert v_top_text = pg_catalog.left(v_marker_query, 100),
    format(
      '[%s] ash.top pgss enrichment: expected exact text %L, got %L',
      pg_catalog.current_setting('ash.feature_mode'),
      pg_catalog.left(v_marker_query, 100),
      v_top_text
    );

  select sample_row.query_text
  into strict v_sample_text
  from ash.samples(
    v_fixture.fixture_start,
    v_fixture.fixture_end,
    n => 100,
    query_id => v_marker_query_id
  ) as sample_row
  limit 1;
  assert v_sample_text = pg_catalog.left(v_marker_query, 80),
    format(
      '[%s] ash.samples pgss enrichment: expected exact text %L, got %L',
      pg_catalog.current_setting('ash.feature_mode'),
      pg_catalog.left(v_marker_query, 80),
      v_sample_text
    );
end
$feature_pgss_text$;

/* -------------------------------------------------------------------------
 * Documented no-data and future-window contracts across every reader.
 * ------------------------------------------------------------------------- */
do $feature_reader_edges$
declare
  v_fixture ash_feature_context%rowtype;
  v_aas record;
  v_chart_count bigint;
  v_compare record;
  v_future_end timestamptz;
  v_future_start timestamptz;
  v_report jsonb;
  v_samples_count bigint;
  v_summary jsonb;
  v_timeline record;
  v_top_count bigint;
begin
  select *
  into strict v_fixture
  from ash_feature_context;
  v_future_start :=
    pg_catalog.date_trunc('hour', v_fixture.fixture_end)
    + interval '2 hours';
  v_future_end := v_future_start + interval '1 hour';

  select *
  into strict v_aas
  from ash.aas(
    v_fixture.fixture_start - interval '2 hours',
    v_fixture.fixture_start - interval '1 hour'
  );
  assert v_aas.buckets_with_data = 0
    and v_aas.avg_aas = 0
    and v_aas.peak_aas = 0
    and v_aas.p99_aas = 0
    and v_aas.backend_seconds = 0,
    format(
      '[%s] ash.aas before-oldest window: expected zero coverage and exact zero metrics, got %s',
      pg_catalog.current_setting('ash.feature_mode'),
      pg_catalog.row_to_json(v_aas)
    );

  select *
  into strict v_timeline
  from ash.timeline(
    v_future_start,
    v_future_end,
    interval '1 hour'
  );
  assert v_timeline.data_points = 0
    and v_timeline.avg_aas is null
    and v_timeline.peak_aas is null
    and v_timeline.p99_aas is null,
    format(
      '[%s] ash.timeline future window: expected one explicit no-data bucket with null metrics, got %s',
      pg_catalog.current_setting('ash.feature_mode'),
      pg_catalog.row_to_json(v_timeline)
    );

  select pg_catalog.count(*)
  into v_top_count
  from ash.top(
    'wait_event',
    v_future_start,
    v_future_end
  );
  assert v_top_count = 0,
    format(
      '[%s] ash.top future window: expected zero rows, got %s',
      pg_catalog.current_setting('ash.feature_mode'),
      v_top_count
    );

  select pg_catalog.count(*)
  into v_samples_count
  from ash.samples(
    v_fixture.fixture_start - interval '2 hours',
    v_fixture.fixture_start - interval '1 hour'
  );
  assert v_samples_count = 0,
    format(
      '[%s] ash.samples before-oldest window: expected zero raw rows, got %s',
      pg_catalog.current_setting('ash.feature_mode'),
      v_samples_count
    );

  select ash.report(
    v_future_start,
    v_future_end
  )
  into v_report;
  assert v_report is null,
    format(
      '[%s] ash.report future window: expected SQL null, got %s',
      pg_catalog.current_setting('ash.feature_mode'),
      v_report
    );

  select pg_catalog.count(*)
  into v_chart_count
  from ash.chart(
    v_future_start,
    v_future_end
  );
  assert v_chart_count = 0,
    format(
      '[%s] ash.chart future window: expected zero rows, got %s',
      pg_catalog.current_setting('ash.feature_mode'),
      v_chart_count
    );

  select pg_catalog.jsonb_object_agg(summary_row.metric, summary_row.value)
  into v_summary
  from ash.summary(
    v_future_start,
    v_future_end
  ) as summary_row;
  assert v_summary = '{"status": "no data in this time range"}'::jsonb,
    format(
      '[%s] ash.summary future window: expected exact no-data status, got %s',
      pg_catalog.current_setting('ash.feature_mode'),
      v_summary
    );

  select *
  into strict v_compare
  from ash.compare(
    v_fixture.fixture_start,
    v_fixture.fixture_end,
    v_future_start,
    v_future_end
  );
  assert v_compare.key = 'overall'
    and v_compare.avg_aas_1 = 4.00
    and v_compare.avg_aas_2 is null
    and v_compare.avg_delta is null
    and v_compare.peak_aas_1 = 5.00
    and v_compare.peak_aas_2 is null
    and v_compare.p99_aas_1 = 4.97
    and v_compare.p99_aas_2 is null,
    format(
      '[%s] ash.compare uncovered side: expected covered window 1 and null window 2/delta, got %s',
      pg_catalog.current_setting('ash.feature_mode'),
      pg_catalog.row_to_json(v_compare)
    );
end
$feature_reader_edges$;
