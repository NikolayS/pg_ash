/* -------------------------------------------------------------------------
 * raw retention must ignore the excluded ring slot (#128)
 *
 * Extracted from .github/workflows/test.yml. GitHub Actions silently refuses
 * to create a workflow run when the workflow file exceeds 512,000 bytes, and
 * test.yml sits just under that ceiling, so large test bodies live here and
 * are invoked with `psql --file` from a short workflow step. Same assertions,
 * same order; only the transport changed.
 *
 * Run standalone against a prepared database with:
 *   psql -v ON_ERROR_STOP=1 -f devel/tests/raw_retention_excluded_slot.sql
 * ------------------------------------------------------------------------- */
begin;

update ash.config
set num_partitions = 6,
    current_slot = 2
where singleton;

do $$
declare
  v_slots smallint[];
begin
  select ash._active_slots() into v_slots;

  assert v_slots = array[2, 1, 0, 5, 4]::smallint[],
    format(
      '_active_slots() for N=6/current=2: '
      'expected {2,1,0,5,4}, got %s',
      v_slots
    );
end
$$;

rollback;

do $$
declare
  v_wait smallint;
  v_old_ts int4 := ash.ts_from_timestamptz(
    date_trunc('minute', now()) - interval '2 days');
  v_recent_ts int4 := ash.ts_from_timestamptz(
    date_trunc('minute', now()) - interval '10 minutes');
  v_oldest timestamptz;
  v_start timestamptz;
  v_status_start timestamptz;
  v_expected_start timestamptz := date_trunc('minute', now())
    - interval '1 day';
  v_slots smallint[];
  v_raised boolean := false;
begin
  truncate ash.sample_0, ash.sample_1, ash.sample_2;
  update ash.config
  set current_slot = 0, num_partitions = 3,
      rotation_period = interval '1 day',
      rotated_at = date_trunc('minute', now())
  where singleton;
  select ash._register_wait('active', 'CPU*', 'CPU*') into v_wait;

  -- For N=3/current=0, readers retain slots {0,2}; slot 1 is the
  -- excluded oldest ring slot awaiting its next truncate.
  insert into ash.sample (
    sample_ts, datid, active_count, data, slot
  ) values
    (v_old_ts, 0::oid, 1, array[-v_wait, 1, 0]::int4[], 1),
    (v_recent_ts, 0::oid, 1, array[-v_wait, 1, 0]::int4[], 0);

  v_slots := ash._active_slots();
  assert v_slots = array[0, 2]::smallint[],
    'retained slot set should be {0,2}, got ' || v_slots::text;
  v_oldest := ash._raw_oldest_sample();
  assert v_oldest = ash.ts_to_timestamptz(v_recent_ts),
    'oldest raw sample used excluded slot 1: ' || v_oldest::text;
  v_start := ash._raw_retention_start();
  assert v_start = v_expected_start,
    'logical raw retention start mismatch: ' || v_start::text;
  select value::timestamptz into v_status_start
  from ash.status() where metric = 'raw_retention_start';
  assert v_status_start = v_start,
    'status raw_retention_start disagrees with logical boundary';

  begin
    perform * from ash.aas(
      ash.ts_to_timestamptz(v_old_ts),
      ash.ts_to_timestamptz(v_old_ts + 60),
      wait_event => 'CPU*', query_id => 128128
    );
  exception when others then
    if sqlerrm like '%entirely outside raw retention%' then
      v_raised := true;
    else
      raise;
    end if;
  end;
  assert v_raised,
    'tie drill over the excluded slot must raise past retention';

  truncate ash.sample_0, ash.sample_1, ash.sample_2;
  raise notice 'raw retention excludes the oldest ring slot PASSED';
end $$;

-- B3 / #163: a fresh default-interval install has only a partial first
-- minute. The documented five-minute tie drill must still read it,
-- status must return a reusable logical boundary, and remediation must
-- round down so it does not discard that partial minute.
begin;

create temp table b3_start_result (
  result_order smallint generated always as identity,
  job_type text,
  job_id bigint,
  status text
) on commit drop;
update ash.config
set sample_interval = interval '7 seconds',
    sampling_enabled = false
where singleton;
insert into b3_start_result (job_type, job_id, status)
select * from ash.start();

do $$
declare
  v_job_types text[];
  v_sampler_status text;
  v_cron_available boolean := ash._pg_cron_available();
begin
  assert (select sample_interval from ash.config where singleton)
    = interval '1 second',
    'B3 precondition: ash.start() default did not replace 7 seconds';
  assert (select sampling_enabled from ash.config where singleton),
    'B3 precondition: ash.start() default did not enable sampling';

  select array_agg(job_type order by result_order)
  into v_job_types
  from b3_start_result;
  select status into v_sampler_status
  from b3_start_result
  where job_type = 'sampler';

  if v_cron_available then
    assert v_job_types = array[
      'sampler', 'rotation', 'rollup_1m', 'rollup_1h', 'rollup_gc'
    ], 'B3 ash.start() cron result mismatch: '
      || v_job_types::text;
    assert not exists (
      select from b3_start_result
      where job_id is null or status is null or job_type = 'error'
    ), 'B3 ash.start() cron result was not successful';
    assert v_sampler_status in (
      'created',
      'already exists — schedule updated to 1 seconds'
    ), 'B3 ash.start() sampler status mismatch: '
      || v_sampler_status;
  else
    assert v_job_types = array['sampler', 'rotation', 'rollup'],
      'B3 ash.start() external result mismatch: '
        || v_job_types::text;
    assert (
      select array_agg(status order by result_order)
      from b3_start_result
    ) = array[
      'interval set to 00:00:01 — schedule externally '
        '(pg_cron not available)',
      (
        select format(
          'rotation_period is %s — schedule ash.rotate() externally',
          rotation_period
        )
        from ash.config
        where singleton
      ),
      'schedule ash.rollup_minute() every minute, '
        'ash.rollup_hour() at minute 1 every hour, '
        'ash.rollup_cleanup() daily'
    ], 'B3 ash.start() external statuses mismatch';
    assert not exists (
      select from b3_start_result
      where job_id is not null or status is null or job_type = 'error'
    ), 'B3 ash.start() external result was not successful';
  end if;
end
$$;

create temp table b3_anchor (
  anchor timestamptz primary key
) on commit drop;
insert into b3_anchor values (date_trunc('minute', now()));

update ash.config
set current_slot = 0,
    num_partitions = 3,
    sample_interval = interval '1 second',
    rotation_period = interval '1 day',
    rotated_at = (select anchor from b3_anchor)
      - interval '1 minute' + interval '59.75 seconds'
where singleton;
truncate ash.sample_0, ash.sample_1, ash.sample_2;
truncate ash.query_map_0, ash.query_map_1, ash.query_map_2;
alter table ash.query_map_0 alter column id restart;

do $$
declare
  v_anchor timestamptz := (select anchor from b3_anchor);
  v_wait smallint;
  v_map_id int4;
  v_datid oid;
  v_setup text[];
begin
  select ash._register_wait('active', 'Lock', 'tuple') into v_wait;
  insert into ash.query_map_0 (query_id)
  values (8231004856741017)
  returning id into v_map_id;
  select oid into v_datid
  from pg_database
  where datname = current_database();

  insert into ash.sample (
    sample_ts, datid, active_count, data, slot
  )
  select
    ash.ts_from_timestamptz(
      v_anchor - interval '1 minute'
      + (sample_offset || ' seconds')::interval
    ),
    v_datid,
    1,
    array[-v_wait, 1, v_map_id]::int4[],
    0
  from generate_series(51, 58) as sample_offset;

  select array[
    count(*)::text,
    min(ash.ts_to_timestamptz(sample_ts))::text,
    max(ash.ts_to_timestamptz(sample_ts))::text,
    (min(sample_ts) % 60)::text
  ]
  into v_setup
  from ash.sample;
  assert v_setup = array[
    '8',
    (v_anchor - interval '1 minute' + interval '51 seconds')::text,
    (v_anchor - interval '1 minute' + interval '58 seconds')::text,
    '51'
  ], 'B3 fixture mismatch: ' || v_setup::text;

  select array[
    count(*)::text,
    min(key),
    min(source),
    min(backend_seconds)::text
  ]
  into v_setup
  from ash.top(
    'wait_event',
    since => now() - interval '5 minutes'
  );
  assert v_setup = array['1', 'Lock:tuple', 'raw', '8.00'],
    'B3 untied control mismatch: ' || v_setup::text;
end
$$;

-- RED 1: README.md's flagship drill, copied verbatim into the
-- materialized result below so its error and exact row are assertable.
do $$
declare
  v_got text[];
begin
  begin
    create temp table b3_readme_result on commit drop as
    select *
    from ash.top(
      'query_id',
      since => now() - interval '5 minutes',
      wait_event => 'Lock:tuple',
      order_by => 'peak',
      n => 5
    );
  exception when others then
    v_got := array['ERROR', sqlerrm];
  end;

  if v_got is null then
    select array[
      count(*)::text,
      min(key),
      min(source),
      min(avg_aas)::text,
      min(peak_aas)::text,
      min(p99_aas)::text,
      min(backend_seconds)::text,
      min(pct)::text
    ]
    into v_got
    from b3_readme_result;
  end if;

  assert v_got = array[
    '1', '8231004856741017', 'raw', '0.03',
    '0.13', '0.13', '8.00', '100.00'
  ], 'B3 README drill mismatch: ' || v_got::text;
end
$$;

-- RED 2: status.raw_retention_start must be directly reusable.
do $$
declare
  v_anchor timestamptz := (select anchor from b3_anchor);
  v_status_start timestamptz;
  v_result text[];
  v_got text[];
begin
  select value::timestamptz into v_status_start
  from ash.status()
  where metric = 'raw_retention_start';

  begin
    select array[
      count(*)::text,
      min(key),
      min(source),
      min(avg_aas)::text,
      min(peak_aas)::text,
      min(p99_aas)::text,
      min(backend_seconds)::text,
      min(pct)::text
    ]
    into v_result
    from ash.top(
      'query_id',
      since => (
        select value::timestamptz
        from ash.status()
        where metric = 'raw_retention_start'
      ),
      wait_event => 'Lock:tuple'
    );
  exception when others then
    v_result := array['ERROR', sqlerrm];
  end;
  v_got := array[v_status_start::text] || v_result;

  assert v_got = array[
    (v_anchor - interval '1 day' - interval '1 minute')::text,
    '1', '8231004856741017', 'raw', '0.00',
    '0.13', '0.13', '8.00', '100.00'
  ], 'B3 status boundary mismatch: ' || v_got::text;
end
$$;

-- RED 3: the guard's printed boundary must round down, and following
-- that exact advice must preserve all eight partial-minute samples.
do $$
declare
  v_anchor timestamptz := (select anchor from b3_anchor);
  v_raw_start timestamptz :=
    (select min(ash.ts_to_timestamptz(sample_ts)) from ash.sample);
  v_passed timestamptz := v_anchor - interval '5 minutes'
    + interval '23 seconds';
  v_message text;
  v_advised timestamptz;
  v_direction text;
  v_result text[];
  v_got text[];
begin
  begin
    perform ash._raise_tie_retention(
      v_raw_start,
      ash.ts_from_timestamptz(v_passed) / 60 * 60,
      ash.ts_from_timestamptz(v_anchor),
      v_passed
    );
    raise exception 'B3 guard should have raised for partial overlap';
  exception when others then
    if sqlerrm like '%Narrow the window%' then
      v_message := sqlerrm;
    else
      raise;
    end if;
  end;

  v_advised := split_part(
    split_part(v_message, 'start at or after ', 2),
    ' (the window end',
    1
  )::timestamptz;
  v_direction := case
    when v_advised = date_trunc('minute', v_raw_start) then 'down'
    when v_advised = date_trunc('minute', v_raw_start)
      + interval '1 minute' then 'up'
    else 'other'
  end;

  select array[
    count(*)::text,
    min(key),
    min(source),
    min(avg_aas)::text,
    min(peak_aas)::text,
    min(p99_aas)::text,
    min(backend_seconds)::text,
    min(pct)::text
  ]
  into v_result
  from ash.top(
    'query_id',
    since => v_advised,
    until => v_anchor,
    wait_event => 'Lock:tuple'
  );
  v_got := array[v_direction] || v_result;

  assert v_got = array[
    'down', '1', '8231004856741017', 'raw', '0.13',
    '0.13', '0.13', '8.00', '100.00'
  ], 'B3 down-rounded advice mismatch: ' || v_got::text;
end
$$;

rollback;
