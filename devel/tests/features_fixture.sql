/*
 * Four complete one-minute samples, known by construction:
 *
 * minute  total  CPU*  IO  Lock  query 10101  query 20202  query 30303  null
 *      0      3     1   2     0            2            1            0     0
 *      1      4     1   3     0            1            3            0     0
 *      2      5     1   0     4            1            0            4     0
 *      3      4     1   2     1            0            2            1     1
 *
 * sample_interval = 1 minute makes every encoded backend appearance exactly
 * one backend-minute. Over the four-minute window: avg=4, peak=5, p99=4.97,
 * backend_seconds=960.
 */
truncate table ash.sample;
truncate table
  ash.query_map_0,
  ash.query_map_1,
  ash.query_map_2
restart identity;
truncate table ash.rollup_1m, ash.rollup_1h;
truncate table ash.wait_event_map restart identity;

update ash.config
set
  current_slot = 0,
  sampling_enabled = true,
  skipped_samples = 0,
  sample_interval = interval '1 minute',
  rotation_period = interval '1 day',
  rotated_at = pg_catalog.clock_timestamp(),
  rollup_1m_retention_days = 30,
  rollup_1h_retention_days = 1825,
  rollup_min_backend_seconds = 1,
  last_rollup_1h_ts = null,
  debug_logging = false
where singleton;

insert into ash.wait_event_map (
  state,
  type,
  event
)
values
  ('active', 'CPU*', 'CPU*'),
  ('active', 'IO', 'DataFileRead'),
  ('active', 'Lock', 'tuple');

insert into ash.query_map_0 (query_id)
values
  (10101),
  (20202),
  (30303);

create temporary table ash_feature_context (
  fixture_start timestamptz not null,
  fixture_end timestamptz not null,
  raw_retention_start timestamptz not null,
  datid oid not null,
  cpu_wait_id smallint not null,
  io_wait_id smallint not null,
  lock_wait_id smallint not null,
  query_10101_id int4 not null,
  query_20202_id int4 not null,
  query_30303_id int4 not null
)
on commit preserve rows;

insert into ash_feature_context (
  fixture_start,
  fixture_end,
  raw_retention_start,
  datid,
  cpu_wait_id,
  io_wait_id,
  lock_wait_id,
  query_10101_id,
  query_20202_id,
  query_30303_id
)
select
  fixture_anchor.fixture_start,
  fixture_anchor.fixture_start + interval '4 minutes',
  ash.ts_to_timestamptz(
    ash.ts_from_timestamptz(
      pg_catalog.date_trunc(
        'minute',
        config_row.rotated_at
          - (config_row.num_partitions - 2) * config_row.rotation_period
      )
    )
  ),
  database_row.oid,
  cpu_wait.id,
  io_wait.id,
  lock_wait.id,
  query_10101.id,
  query_20202.id,
  query_30303.id
from pg_catalog.pg_database as database_row
cross join ash.config as config_row
cross join lateral (
  select
    pg_catalog.date_trunc(
      'hour',
      pg_catalog.statement_timestamp()
    ) - interval '50 minutes' as fixture_start
) as fixture_anchor
cross join lateral (
  select id
  from ash.wait_event_map
  where type = 'CPU*'
) as cpu_wait
cross join lateral (
  select id
  from ash.wait_event_map
  where type = 'IO'
) as io_wait
cross join lateral (
  select id
  from ash.wait_event_map
  where type = 'Lock'
) as lock_wait
cross join lateral (
  select id
  from ash.query_map_0
  where query_id = 10101
) as query_10101
cross join lateral (
  select id
  from ash.query_map_0
  where query_id = 20202
) as query_20202
cross join lateral (
  select id
  from ash.query_map_0
  where query_id = 30303
) as query_30303
where
  database_row.datname = pg_catalog.current_database()
  and config_row.singleton;

/*
 * Pin the minute watermark to the first data-bearing grain. The reader test
 * processes exactly five completed grains: these four fixture minutes plus
 * one trailing empty minute. This keeps processed-grain and persisted-row
 * assertions deterministic and deliberately different.
 */
update ash.config as config
set last_rollup_1m_ts = ash.ts_from_timestamptz(
  fixture.fixture_start
)
from ash_feature_context as fixture
where config.singleton;

insert into ash.sample (
  sample_ts,
  datid,
  active_count,
  data,
  slot
)
select
  ash.ts_from_timestamptz(fixture.fixture_start),
  fixture.datid,
  3,
  array[
    -fixture.cpu_wait_id,
    1,
    fixture.query_10101_id,
    -fixture.io_wait_id,
    2,
    fixture.query_10101_id,
    fixture.query_20202_id
  ]::integer[],
  0
from ash_feature_context as fixture
union all
select
  ash.ts_from_timestamptz(fixture.fixture_start + interval '1 minute'),
  fixture.datid,
  4,
  array[
    -fixture.cpu_wait_id,
    1,
    fixture.query_10101_id,
    -fixture.io_wait_id,
    3,
    fixture.query_20202_id,
    fixture.query_20202_id,
    fixture.query_20202_id
  ]::integer[],
  0
from ash_feature_context as fixture
union all
select
  ash.ts_from_timestamptz(fixture.fixture_start + interval '2 minutes'),
  fixture.datid,
  5,
  array[
    -fixture.cpu_wait_id,
    1,
    fixture.query_10101_id,
    -fixture.lock_wait_id,
    4,
    fixture.query_30303_id,
    fixture.query_30303_id,
    fixture.query_30303_id,
    fixture.query_30303_id
  ]::integer[],
  0
from ash_feature_context as fixture
union all
select
  ash.ts_from_timestamptz(fixture.fixture_start + interval '3 minutes'),
  fixture.datid,
  4,
  array[
    -fixture.cpu_wait_id,
    1,
    0,
    -fixture.io_wait_id,
    2,
    fixture.query_20202_id,
    fixture.query_20202_id,
    -fixture.lock_wait_id,
    1,
    fixture.query_30303_id
  ]::integer[],
  0
from ash_feature_context as fixture;

do $feature_fixture$
declare
  v_mode text := pg_catalog.current_setting(
    'ash.feature_mode',
    true
  );
  v_fixture ash_feature_context%rowtype;
  v_rows bigint;
  v_backends bigint;
begin
  select *
  into strict v_fixture
  from ash_feature_context;

  select
    count(*),
    sum(active_count)
  into
    v_rows,
    v_backends
  from ash.sample
  where
    sample_ts >= ash.ts_from_timestamptz(v_fixture.fixture_start)
    and sample_ts < ash.ts_from_timestamptz(v_fixture.fixture_end);

  assert v_rows = 4 and v_backends = 16,
    format(
      '[%s] fixture: expected 4 sample rows and 16 encoded backends, got rows=%s backends=%s',
      coalesce(v_mode, 'standalone'),
      v_rows,
      v_backends
    );
end
$feature_fixture$;
