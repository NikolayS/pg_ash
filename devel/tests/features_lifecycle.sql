/* -------------------------------------------------------------------------
 * ash.set_debug_logging()
 * ------------------------------------------------------------------------- */
do $feature_debug_logging$
declare
  v_actual text;
begin
  select ash.set_debug_logging()
  into v_actual;
  assert v_actual = 'debug_logging = false',
    format(
      '[%s] ash.set_debug_logging report: expected "debug_logging = false", got %L',
      pg_catalog.current_setting('ash.feature_mode'),
      v_actual
    );

  select ash.set_debug_logging(true)
  into v_actual;
  assert v_actual =
    'debug_logging enabled — each sampled session will emit RAISE LOG'
    and (
      select debug_logging
      from ash.config
      where singleton
    ),
    format(
      '[%s] ash.set_debug_logging enable: expected exact return and config=true, got return=%L config=%s',
      pg_catalog.current_setting('ash.feature_mode'),
      v_actual,
      (
        select debug_logging
        from ash.config
        where singleton
      )
    );

  select ash.set_debug_logging(false)
  into v_actual;
  assert v_actual = 'debug_logging disabled'
    and not (
      select debug_logging
      from ash.config
      where singleton
    ),
    format(
      '[%s] ash.set_debug_logging disable: expected exact return and config=false, got return=%L config=%s',
      pg_catalog.current_setting('ash.feature_mode'),
      v_actual,
      (
        select debug_logging
        from ash.config
        where singleton
      )
    );
end
$feature_debug_logging$;

/* -------------------------------------------------------------------------
 * ash.take_sample(): disabled skip and one known pg_sleep() backend.
 * ------------------------------------------------------------------------- */
begin;

do $feature_take_sample_disabled$
declare
  v_before_rows bigint;
  v_before_skips int4;
  v_result int;
begin
  select
    pg_catalog.count(*)
  into v_before_rows
  from ash.sample;
  select skipped_samples
  into strict v_before_skips
  from ash.config
  where singleton;

  update ash.config
  set sampling_enabled = false
  where singleton;
  select ash.take_sample()
  into v_result;

  assert v_result = 0
    and (
      select pg_catalog.count(*)
      from ash.sample
    ) = v_before_rows
    and (
      select skipped_samples
      from ash.config
      where singleton
    ) = v_before_skips + 1,
    format(
      '[%s] ash.take_sample disabled: expected return 0/no row/skipped+1, got return=%s rows=%s skips=%s',
      pg_catalog.current_setting('ash.feature_mode'),
      v_result,
      (
        select pg_catalog.count(*)
        from ash.sample
      ),
      (
        select skipped_samples
        from ash.config
        where singleton
      )
    );
end
$feature_take_sample_disabled$;

rollback;

begin;

do $feature_take_sample_live$
declare
  v_before_queries bigint;
  v_before_rows bigint;
  v_before_waits bigint;
  v_decoded jsonb;
  v_expected_query_id bigint;
  v_new_sample record;
  v_pgss_preloaded boolean :=
    'pg_stat_statements' = any (
      pg_catalog.string_to_array(
        pg_catalog.replace(
          pg_catalog.current_setting('shared_preload_libraries'),
          ' ',
          ''
        ),
        ','
      )
    );
  v_result int;
begin
  select pg_catalog.count(*)
  into v_before_rows
  from ash.sample;
  select pg_catalog.count(*)
  into v_before_waits
  from ash.wait_event_map;
  select pg_catalog.count(*)
  into v_before_queries
  from ash.query_map_all;
  select activity.query_id
  into strict v_expected_query_id
  from pg_catalog.pg_stat_activity as activity
  where
    activity.application_name = pg_catalog.format(
      'pg_ash_features_%s',
      pg_catalog.current_setting('ash.feature_mode')
    )
    and activity.state = 'active'
    and activity.query = 'select pg_sleep(300);';

  assert v_pgss_preloaded = (v_expected_query_id is not null),
    format(
      '[%s] ash.take_sample preload precondition: expected pg_stat_statements preload=%s to match sleeper query_id presence, got query_id=%s',
      pg_catalog.current_setting('ash.feature_mode'),
      v_pgss_preloaded,
      v_expected_query_id
    );

  update ash.config
  set
    sampling_enabled = true,
    sample_interval = interval '1 second'
  where singleton;

  select ash.take_sample()
  into v_result;

  select *
  into strict v_new_sample
  from ash.sample
  order by sample_ts desc
  limit 1;
  select pg_catalog.jsonb_agg(
    pg_catalog.jsonb_build_array(
      decoded.wait_event,
      decoded.query_id,
      decoded.count
    )
  )
  into v_decoded
  from ash.decode_sample(
    v_new_sample.data,
    v_new_sample.slot
  ) as decoded;

  assert v_result = 1
    and (
      select pg_catalog.count(*)
      from ash.sample
    ) = v_before_rows + 1
    and v_new_sample.active_count = 1
    and v_new_sample.slot = 0
    and pg_catalog.jsonb_array_length(v_decoded) = 1
    and v_decoded -> 0 ->> 0 = 'Timeout:PgSleep'
    and (v_decoded -> 0 ->> 1)::bigint
      is not distinct from v_expected_query_id,
    format(
      '[%s] ash.take_sample live: expected one inserted Timeout:PgSleep backend with exact live query_id %s, got return=%s row=%s decoded=%s total_rows=%s',
      pg_catalog.current_setting('ash.feature_mode'),
      v_expected_query_id,
      v_result,
      pg_catalog.row_to_json(v_new_sample),
      v_decoded,
      (
        select pg_catalog.count(*)
        from ash.sample
      )
    );
  assert (
      select pg_catalog.count(*)
      from ash.wait_event_map
    ) = v_before_waits + 1
    and (
      select pg_catalog.count(*)
      from ash.query_map_all
    ) = v_before_queries
      + case when v_expected_query_id is null then 0 else 1 end,
    format(
      '[%s] ash.take_sample dictionaries: expected one PgSleep wait and exactly %s real query mapping(s), got waits %s->%s queries %s->%s',
      pg_catalog.current_setting('ash.feature_mode'),
      case when v_expected_query_id is null then 0 else 1 end,
      v_before_waits,
      (
        select pg_catalog.count(*)
        from ash.wait_event_map
      ),
      v_before_queries,
      (
        select pg_catalog.count(*)
        from ash.query_map_all
      )
    );
end
$feature_take_sample_live$;

rollback;

/* -------------------------------------------------------------------------
 * ash.start() / ash.stop(): exact cron and external-scheduler behavior.
 * ------------------------------------------------------------------------- */
do $feature_start_stop$
declare
  v_expected_cron boolean :=
    pg_catalog.current_setting('ash.feature_expected_cron')::boolean;
  v_jobs jsonb;
  v_start_rows jsonb;
  v_stop_rows jsonb;
begin
  if v_expected_cron then
    select pg_catalog.jsonb_object_agg(
      start_row.job_type,
      pg_catalog.jsonb_build_array(
        start_row.job_id is not null,
        start_row.status
      )
    )
    into v_start_rows
    from ash.start(interval '2 seconds') as start_row;

    assert v_start_rows = '{
        "sampler": [true, "created"],
        "rotation": [true, "created"],
        "rollup_1m": [true, "created"],
        "rollup_1h": [true, "created"],
        "rollup_gc": [true, "created"]
      }'::jsonb,
      format(
        '[%s] ash.start cron return: expected five exact created job rows, got %s',
        pg_catalog.current_setting('ash.feature_mode'),
        v_start_rows
      );

    select pg_catalog.jsonb_object_agg(
      cron_job.jobname,
      pg_catalog.jsonb_build_array(
        cron_job.schedule,
        cron_job.command,
        cron_job.database,
        cron_job.active
      )
    )
    into v_jobs
    from cron.job as cron_job
    where cron_job.jobname like 'ash_%';
    assert v_jobs = pg_catalog.jsonb_build_object(
      'ash_sampler',
      pg_catalog.jsonb_build_array(
        '2 seconds',
        'set statement_timeout = ''500ms''; select ash.take_sample()',
        pg_catalog.current_database(),
        true
      ),
      'ash_rotation',
      pg_catalog.jsonb_build_array(
        '0 0 * * *',
        'select ash.rotate()',
        pg_catalog.current_database(),
        true
      ),
      'ash_rollup_1m',
      pg_catalog.jsonb_build_array(
        '* * * * *',
        'select ash.rollup_minute()',
        pg_catalog.current_database(),
        true
      ),
      'ash_rollup_1h',
      pg_catalog.jsonb_build_array(
        '1 * * * *',
        'select ash.rollup_hour()',
        pg_catalog.current_database(),
        true
      ),
      'ash_rollup_gc',
      pg_catalog.jsonb_build_array(
        '0 3 * * *',
        'select ash.rollup_cleanup()',
        pg_catalog.current_database(),
        true
      )
    ),
      format(
        '[%s] ash.start cron side effect: expected exact five schedules/commands/database/active states, got %s',
        pg_catalog.current_setting('ash.feature_mode'),
        v_jobs
      );

    perform * from ash.start(interval '3 seconds');
    assert (
      select schedule = '3 seconds'
      from cron.job
      where jobname = 'ash_sampler'
    )
    and (
      select sample_interval = interval '3 seconds'
        and sampling_enabled
      from ash.config
      where singleton
    ),
      format(
        '[%s] ash.start idempotent reschedule: expected cron/config 3 seconds and enabled=true',
        pg_catalog.current_setting('ash.feature_mode')
      );

    select pg_catalog.jsonb_object_agg(
      stop_row.job_type,
      stop_row.status
    )
    into v_stop_rows
    from ash.stop() as stop_row;
    assert v_stop_rows = '{
        "sampler": "removed",
        "rotation": "removed",
        "rollup_1m": "removed",
        "rollup_1h": "removed",
        "rollup_gc": "removed"
      }'::jsonb
      and not exists (
        select
        from cron.job
        where jobname like 'ash_%'
      )
      and not (
        select sampling_enabled
        from ash.config
        where singleton
      ),
      format(
        '[%s] ash.stop cron side effect: expected five removed rows/no ash jobs/config disabled, got rows=%s remaining=%s enabled=%s',
        pg_catalog.current_setting('ash.feature_mode'),
        v_stop_rows,
        (
          select pg_catalog.count(*)
          from cron.job
          where jobname like 'ash_%'
        ),
        (
          select sampling_enabled
          from ash.config
          where singleton
        )
      );
  else
    select pg_catalog.jsonb_object_agg(
      start_row.job_type,
      pg_catalog.jsonb_build_array(start_row.job_id, start_row.status)
    )
    into v_start_rows
    from ash.start(interval '2 seconds') as start_row;
    assert v_start_rows = '{
        "sampler": [
          null,
          "interval set to 00:00:02 — schedule externally (pg_cron not available)"
        ],
        "rotation": [
          null,
          "rotation_period is 1 day — schedule ash.rotate() externally"
        ],
        "rollup": [
          null,
          "schedule ash.rollup_minute() every minute, ash.rollup_hour() at minute 1 every hour, ash.rollup_cleanup() daily"
        ]
      }'::jsonb
      and (
        select sample_interval = interval '2 seconds'
          and sampling_enabled
        from ash.config
        where singleton
      ),
      format(
        '[%s] ash.start no-cron: expected exact three external-scheduler rows and enabled 2-second config, got %s',
        pg_catalog.current_setting('ash.feature_mode'),
        v_start_rows
      );

    select pg_catalog.jsonb_agg(
      pg_catalog.jsonb_build_array(
        stop_row.job_type,
        stop_row.job_id,
        stop_row.status
      )
    )
    into v_stop_rows
    from ash.stop() as stop_row;
    assert v_stop_rows = '[[
        "info",
        null,
        "pg_cron not installed — remember to stop your external scheduler (cron, systemd timer, loop script, etc.)"
      ]]'::jsonb
      and not (
        select sampling_enabled
        from ash.config
        where singleton
      ),
      format(
        '[%s] ash.stop no-cron: expected exact reminder row and config disabled, got %s',
        pg_catalog.current_setting('ash.feature_mode'),
        v_stop_rows
      );
  end if;
end
$feature_start_stop$;

/* -------------------------------------------------------------------------
 * ash.rotate(): exact slot advance and lockstep partition/query-map truncate.
 * ------------------------------------------------------------------------- */
begin;

do $feature_rotate$
declare
  v_fixture ash_feature_context%rowtype;
  v_inserted_id int4;
  v_result text;
begin
  select *
  into strict v_fixture
  from ash_feature_context;

  insert into ash.query_map_2 (query_id)
  values (909090)
  returning id
  into v_inserted_id;
  assert v_inserted_id = 1,
    format(
      '[%s] ash.rotate setup: expected fresh slot-2 identity 1, got %s',
      pg_catalog.current_setting('ash.feature_mode'),
      v_inserted_id
    );

  insert into ash.sample_2 (
    sample_ts,
    datid,
    active_count,
    data,
    slot
  )
  values (
    ash.ts_from_timestamptz(v_fixture.fixture_start),
    v_fixture.datid,
    1,
    array[-v_fixture.cpu_wait_id, 1, 0]::int4[],
    2
  );
  assert (
    select pg_catalog.count(*)
    from ash.sample_2
  ) = 1,
    format(
      '[%s] ash.rotate setup: expected one doomed slot-2 sample sentinel',
      pg_catalog.current_setting('ash.feature_mode')
    );

  update ash.config
  set
    current_slot = 0,
    rotated_at = pg_catalog.now() - interval '2 days',
    rotation_period = interval '1 day'
  where singleton;

  select ash.rotate()
  into v_result;
  assert v_result =
    'rotated: slot 0 -> 1, truncated slot 2 (sample + query_map)'
    and ash.current_slot() = 1
    and (
      select pg_catalog.count(*)
      from ash.query_map_2
    ) = 0
    and (
      select pg_catalog.count(*)
      from ash.sample_2
    ) = 0
    and (
      select pg_catalog.count(*)
      from ash.sample
    ) = 4,
    format(
      '[%s] ash.rotate: expected exact slot 0->1, empty slot-2 sample/query partitions, and four retained slot-0 samples; got result=%L slot=%s qmap2=%s sample2=%s samples=%s',
      pg_catalog.current_setting('ash.feature_mode'),
      v_result,
      ash.current_slot(),
      (
        select pg_catalog.count(*)
        from ash.query_map_2
      ),
      (
        select pg_catalog.count(*)
        from ash.sample_2
      ),
      (
        select pg_catalog.count(*)
        from ash.sample
      )
    );

  insert into ash.query_map_2 (query_id)
  values (909091)
  returning id
  into v_inserted_id;
  assert v_inserted_id = 1,
    format(
      '[%s] ash.rotate identity side effect: expected truncated slot identity restart at 1, got %s',
      pg_catalog.current_setting('ash.feature_mode'),
      v_inserted_id
    );
end
$feature_rotate$;

rollback;

/* -------------------------------------------------------------------------
 * ash.rollup_hour(): exact merged hour and minute-count preservation.
 * ------------------------------------------------------------------------- */
begin;

do $feature_rollup_hour$
declare
  v_fixture ash_feature_context%rowtype;
  v_hour record;
  v_result int;
begin
  select *
  into strict v_fixture
  from ash_feature_context;

  update ash.config
  set last_rollup_1h_ts =
    ash.ts_from_timestamptz(
      pg_catalog.date_trunc('hour', v_fixture.fixture_start)
    )
  where singleton;
  select ash.rollup_hour()
  into v_result;
  select *
  into strict v_hour
  from ash.rollup_1h
  where ts = ash.ts_from_timestamptz(
    pg_catalog.date_trunc('hour', v_fixture.fixture_start)
  );

  assert v_result = 1
    and v_hour.datid = v_fixture.datid
    and v_hour.samples = 4
    and v_hour.peak_backends = 5
    and v_hour.wait_counts = array[
      v_fixture.io_wait_id,
      7,
      v_fixture.lock_wait_id,
      5,
      v_fixture.cpu_wait_id,
      4
    ]::int4[]
    and v_hour.query_counts = array[
      20202, 6, 30303, 5, 10101, 4
    ]::int8[]
    and pg_catalog.array_length(v_hour.minute_counts, 1) = 60
    and v_hour.minute_counts[11:14] = array[3, 4, 5, 4]::int4[]
    and (
      select pg_catalog.count(*)
      from pg_catalog.unnest(v_hour.minute_counts) as minute_count
      where minute_count is not null
    ) = 4,
    format(
      '[%s] ash.rollup_hour: expected one exact merged row with [3,4,5,4] minute counts, got return=%s row=%s',
      pg_catalog.current_setting('ash.feature_mode'),
      v_result,
      pg_catalog.row_to_json(v_hour)
    );
end
$feature_rollup_hour$;

rollback;

/* -------------------------------------------------------------------------
 * ash.rollup_cleanup(): exact retention deletions and survivor counts.
 * ------------------------------------------------------------------------- */
begin;

do $feature_rollup_cleanup$
declare
  v_fixture ash_feature_context%rowtype;
  v_recent_1h_ts int4;
  v_recent_1m_ts int4;
  v_result text;
  v_stale_1h_ts int4;
  v_stale_1m_ts int4;
begin
  select *
  into strict v_fixture
  from ash_feature_context;

  v_stale_1m_ts := ash.ts_from_timestamptz(
    pg_catalog.date_trunc(
      'minute',
      pg_catalog.now() - interval '3 days'
    )
  );
  v_recent_1m_ts := ash.ts_from_timestamptz(
    pg_catalog.date_trunc('minute', pg_catalog.now())
  );
  v_stale_1h_ts := ash.ts_from_timestamptz(
    pg_catalog.date_trunc(
      'hour',
      pg_catalog.now() - interval '3 days'
    )
  );
  v_recent_1h_ts := ash.ts_from_timestamptz(
    pg_catalog.date_trunc('hour', pg_catalog.now())
  );

  update ash.config
  set
    rollup_1m_retention_days = 2,
    rollup_1h_retention_days = 1
  where singleton;
  insert into ash.rollup_1m (
    ts,
    datid,
    samples,
    peak_backends,
    wait_counts,
    query_counts
  )
  values
    (
      v_stale_1m_ts,
      v_fixture.datid,
      1,
      1,
      array[v_fixture.cpu_wait_id, 1]::int4[],
      '{}'::int8[]
    ),
    (
      v_recent_1m_ts,
      v_fixture.datid,
      1,
      1,
      array[v_fixture.cpu_wait_id, 1]::int4[],
      '{}'::int8[]
    );
  insert into ash.rollup_1h (
    ts,
    datid,
    samples,
    peak_backends,
    wait_counts,
    query_counts,
    minute_counts
  )
  values
    (
      v_stale_1h_ts,
      v_fixture.datid,
      1,
      1,
      array[v_fixture.cpu_wait_id, 1]::int4[],
      '{}'::int8[],
      array[1]::int4[]
    ),
    (
      v_recent_1h_ts,
      v_fixture.datid,
      1,
      1,
      array[v_fixture.cpu_wait_id, 1]::int4[],
      '{}'::int8[],
      array[1]::int4[]
    );

  select ash.rollup_cleanup()
  into v_result;
  assert v_result = 'cleanup: deleted 1 minute rows, 1 hourly rows'
    and not exists (
      select
      from ash.rollup_1m
      where ts = v_stale_1m_ts
    )
    and exists (
      select
      from ash.rollup_1m
      where ts = v_recent_1m_ts
    )
    and not exists (
      select
      from ash.rollup_1h
      where ts = v_stale_1h_ts
    )
    and exists (
      select
      from ash.rollup_1h
      where ts = v_recent_1h_ts
    ),
    format(
      '[%s] ash.rollup_cleanup: expected exact 1/1 deletion with recent survivors, got %L',
      pg_catalog.current_setting('ash.feature_mode'),
      v_result
    );
end
$feature_rollup_cleanup$;

rollback;

/* -------------------------------------------------------------------------
 * ash.rebuild_partitions(): destructive side effects and preserved readers.
 * ------------------------------------------------------------------------- */
begin;

do $feature_rebuild_confirmation$
declare
  v_before bigint;
  v_refused boolean := false;
begin
  select pg_catalog.count(*)
  into v_before
  from ash.sample;
  begin
    perform ash.rebuild_partitions(4);
  exception
    when others then
      v_refused := sqlerrm =
        'rebuild_partitions is destructive — all raw sample data will be lost. To proceed, call: select ash.rebuild_partitions(4, ''yes'')';
  end;
  assert v_refused
    and (
      select pg_catalog.count(*)
      from ash.sample
    ) = v_before,
    format(
      '[%s] ash.rebuild_partitions confirmation: expected exact refusal before mutation and %s retained rows, got refused=%s rows=%s',
      pg_catalog.current_setting('ash.feature_mode'),
      v_before,
      v_refused,
      (
        select pg_catalog.count(*)
        from ash.sample
      )
    );
end
$feature_rebuild_confirmation$;

do $feature_rebuild$
declare
  v_monitor_read boolean := false;
  v_reader_read boolean := false;
  v_result text;
begin
  select ash.rebuild_partitions(4, 'yes')
  into v_result;
  assert v_result =
    'rebuilt: 3 -> 4 partitions. all raw data cleared. call ash.start() to resume sampling.'
    and ash.current_slot() = 0
    and (
      select num_partitions = 4
        and not sampling_enabled
      from ash.config
      where singleton
    )
    and (
      select pg_catalog.count(*)
      from ash.sample
    ) = 0
    and (
      select pg_catalog.count(*)
      from ash.rollup_1m
    ) = 4
    and (
      select pg_catalog.count(*)
      from pg_catalog.pg_inherits as inheritance_row
      where inheritance_row.inhparent = 'ash.sample'::regclass
    ) = 4
    and (
      select pg_catalog.count(*)
      from pg_catalog.pg_class as relation_row
      inner join pg_catalog.pg_namespace as namespace_row
        on namespace_row.oid = relation_row.relnamespace
      where
        namespace_row.nspname = 'ash'
        and relation_row.relname ~ '^query_map_[0-9]+$'
        and relation_row.relkind = 'r'
    ) = 4,
    format(
      '[%s] ash.rebuild_partitions: expected exact 3->4 result, disabled slot0 config, zero raw, four retained rollups, and four sample/query partitions; got result=%L config=%s raw=%s rollups=%s sample_children=%s query_maps=%s',
      pg_catalog.current_setting('ash.feature_mode'),
      v_result,
      (
        select pg_catalog.row_to_json(config_row)
        from ash.config as config_row
      ),
      (
        select pg_catalog.count(*)
        from ash.sample
      ),
      (
        select pg_catalog.count(*)
        from ash.rollup_1m
      ),
      (
        select pg_catalog.count(*)
        from pg_catalog.pg_inherits as inheritance_row
        where inheritance_row.inhparent = 'ash.sample'::regclass
      ),
      (
        select pg_catalog.count(*)
        from pg_catalog.pg_class as relation_row
        inner join pg_catalog.pg_namespace as namespace_row
          on namespace_row.oid = relation_row.relnamespace
        where
          namespace_row.nspname = 'ash'
          and relation_row.relname ~ '^query_map_[0-9]+$'
          and relation_row.relkind = 'r'
      )
    );

  assert pg_catalog.has_table_privilege(
      'ash_feature_reader',
      'ash.sample_3',
      'SELECT'
    )
    and pg_catalog.has_table_privilege(
      'pg_monitor',
      'ash.sample_3',
      'SELECT'
    ),
    format(
      '[%s] ash.rebuild_partitions preserved ACLs: expected both reader bundles on replacement sample_3, got custom=%s pg_monitor=%s',
      pg_catalog.current_setting('ash.feature_mode'),
      pg_catalog.has_table_privilege(
        'ash_feature_reader',
        'ash.sample_3',
        'SELECT'
      ),
      pg_catalog.has_table_privilege(
        'pg_monitor',
        'ash.sample_3',
        'SELECT'
      )
    );

  execute 'set local role ash_feature_reader';
  begin
    perform pg_catalog.count(*) from ash.query_map_all;
    perform pg_catalog.count(*) from ash.sample_3;
    v_reader_read := true;
  exception
    when insufficient_privilege then
      v_reader_read := false;
  end;
  execute 'reset role';

  execute 'set local role pg_monitor';
  begin
    perform pg_catalog.count(*) from ash.query_map_all;
    perform pg_catalog.count(*) from ash.sample_3;
    v_monitor_read := true;
  exception
    when insufficient_privilege then
      v_monitor_read := false;
  end;
  execute 'reset role';

  assert v_reader_read
    and v_monitor_read,
    format(
      '[%s] ash.rebuild_partitions preserved ACL behavior: expected immediate custom/pg_monitor reads without re-grant, got custom=%s pg_monitor=%s',
      pg_catalog.current_setting('ash.feature_mode'),
      v_reader_read,
      v_monitor_read
    );
end
$feature_rebuild$;

rollback;

/*
 * rollup return contract: one time grain containing two databases must count
 * as one processed minute/hour, not two physical rollup rows (#191). Record
 * both calls now, then raise only after uninstall and role cleanup below.
 */
create temporary table ash_feature_rollup_return_contract (
  datids bigint not null,
  minute_result int not null,
  minute_rows bigint not null,
  hour_result int not null,
  hour_rows bigint not null
)
on commit preserve rows;

do $feature_rollup_return_probe$
declare
  v_anchor timestamptz :=
    pg_catalog.date_trunc(
      'hour',
      pg_catalog.statement_timestamp()
    ) - interval '1 hour';
  v_datids bigint;
  v_hour_result int;
  v_minute_result int;
begin
  truncate table ash.sample;
  truncate table ash.rollup_1m, ash.rollup_1h;
  truncate table ash.wait_event_map restart identity;

  insert into ash.wait_event_map (state, type, event)
  values ('active', 'CPU*', 'CPU*');

  update ash.config
  set
    current_slot = 0,
    sample_interval = interval '1 second',
    last_rollup_1m_ts = ash.ts_from_timestamptz(v_anchor),
    last_rollup_1h_ts = null
  where singleton;

  with database_ids as (
    select database_row.oid as datid
    from pg_catalog.pg_database as database_row
    order by
      (database_row.datname = pg_catalog.current_database()) desc,
      database_row.oid
    limit 2
  ),
  wait_id as (
    select id
    from ash.wait_event_map
    where
      state = 'active'
      and type = 'CPU*'
      and event = 'CPU*'
  )
  insert into ash.sample (
    sample_ts,
    datid,
    active_count,
    data,
    slot
  )
  select
    ash.ts_from_timestamptz(v_anchor),
    database_ids.datid,
    1,
    array[-wait_id.id::int, 1, 0]::int4[],
    0
  from database_ids
  cross join wait_id;

  select pg_catalog.count(distinct sample_row.datid)
  into v_datids
  from ash.sample as sample_row;
  select ash.rollup_minute(1)
  into v_minute_result;

  update ash.config
  set
    last_rollup_1m_ts = ash.ts_from_timestamptz(v_anchor + interval '1 hour'),
    last_rollup_1h_ts = ash.ts_from_timestamptz(v_anchor)
  where singleton;
  select ash.rollup_hour()
  into v_hour_result;

  insert into ash_feature_rollup_return_contract (
    datids,
    minute_result,
    minute_rows,
    hour_result,
    hour_rows
  )
  select
    v_datids,
    v_minute_result,
    (
      select pg_catalog.count(*)
      from ash.rollup_1m
    ),
    v_hour_result,
    (
      select pg_catalog.count(*)
      from ash.rollup_1h
    );
end
$feature_rollup_return_probe$;

/* -------------------------------------------------------------------------
 * ash.uninstall(): verify the destructive effect, then rollback for cleanup.
 * ------------------------------------------------------------------------- */
begin;

do $feature_uninstall_setup$
declare
  v_expected_cron boolean :=
    pg_catalog.current_setting('ash.feature_expected_cron')::boolean;
begin
  if v_expected_cron then
    perform *
    from ash.start(interval '5 seconds');
    assert (
      select pg_catalog.count(*)
      from cron.job
      where jobname like 'ash_%'
    ) = 5,
      format(
        '[%s] ash.uninstall setup: expected exactly five ash_* jobs for uninstall to remove, got %s',
        pg_catalog.current_setting('ash.feature_mode'),
        (
          select pg_catalog.count(*)
          from cron.job
          where jobname like 'ash_%'
        )
      );
  end if;
end
$feature_uninstall_setup$;

create temporary table ash_feature_uninstall_result
on commit drop
as
select ash.uninstall('yes') as result;

do $feature_uninstall_rollback$
declare
  v_expected_cron boolean :=
    pg_catalog.current_setting('ash.feature_expected_cron')::boolean;
  v_result text;
begin
  select result
  into strict v_result
  from ash_feature_uninstall_result;
  assert v_result = pg_catalog.format(
      'uninstalled: removed %s pg_cron jobs, dropped ash schema',
      case when v_expected_cron then 5 else 0 end
    )
    and pg_catalog.to_regnamespace('ash') is null,
    format(
      '[%s] ash.uninstall: expected exact %s-job removal result and absent schema, got result=%L schema=%s',
      pg_catalog.current_setting('ash.feature_mode'),
      case when v_expected_cron then 5 else 0 end,
      v_result,
      pg_catalog.to_regnamespace('ash')
    );
  if v_expected_cron then
    assert not exists (
      select
      from cron.job
      where jobname like 'ash_%'
    ),
      format(
        '[%s] ash.uninstall cron side effect: ash_* jobs remained',
        pg_catalog.current_setting('ash.feature_mode')
      );
  end if;
end
$feature_uninstall_rollback$;

rollback;

/*
 * Final real uninstall leaves the caller's mode isolated and lets the shell
 * assert the schema is absent before configuring the next extension mode.
 */
create temporary table ash_feature_final_uninstall
on commit preserve rows
as
select ash.uninstall('yes') as result;

do $feature_final_cleanup$
declare
  v_result text;
begin
  select result
  into strict v_result
  from ash_feature_final_uninstall;
  assert v_result =
    'uninstalled: removed 0 pg_cron jobs, dropped ash schema'
    and pg_catalog.to_regnamespace('ash') is null,
    format(
      '[%s] final ash.uninstall cleanup: expected absent schema and exact zero-job result, got result=%L schema=%s',
      pg_catalog.current_setting('ash.feature_mode'),
      v_result,
      pg_catalog.to_regnamespace('ash')
    );
end
$feature_final_cleanup$;

drop owned by ash_feature_reader;
drop role ash_feature_reader;

do $feature_rollup_return_contract$
declare
  v_probe ash_feature_rollup_return_contract%rowtype;
begin
  select *
  into strict v_probe
  from ash_feature_rollup_return_contract;

  assert v_probe.datids = 2
    and v_probe.minute_rows = 2
    and v_probe.hour_rows = 2
    and v_probe.minute_result = 1
    and v_probe.hour_result = 1,
    format(
      '[%s] ash.rollup return contract (#191): expected one processed minute/hour for two datids while writing two rows each, got datids=%s minute_result=%s minute_rows=%s hour_result=%s hour_rows=%s',
      pg_catalog.current_setting('ash.feature_mode'),
      v_probe.datids,
      v_probe.minute_result,
      v_probe.minute_rows,
      v_probe.hour_result,
      v_probe.hour_rows
    );
end
$feature_rollup_return_contract$;
