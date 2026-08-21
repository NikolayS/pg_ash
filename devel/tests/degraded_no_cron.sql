do $$
declare
  v_rec record;
  v_status_val text;
  v_before bigint;
  v_after bigint;
  v_wait_id smallint;
  v_datid oid;
  v_minute int4;
begin
  -- start() should work without pg_cron (no error, prints hints)
  select status into v_status_val
  from ash.start('1 second')
  where job_type = 'sampler';
  assert v_status_val like '%schedule externally%',
    'start() without pg_cron should say schedule externally, got: ' || v_status_val;

  /*
   * A preloaded pg_cron registers cron.database_name as postmaster-only even
   * after DROP EXTENSION. Exercise mutable placeholder values only on the
   * dedicated cron-off axis, where the setting starts absent.
   */
  if current_setting('cron.database_name', true) is null then
    perform set_config(
      'cron.database_name', current_database(), false
    );
    perform * from ash.start('1 second');
    perform set_config(
      'cron.database_name', 'cron_control', false
    );
    perform * from ash.start('1 second');
  end if;

  -- stop() should work without pg_cron
  select status into v_status_val
  from ash.stop()
  limit 1;
  assert v_status_val like '%external scheduler%',
    'stop() without pg_cron should mention external scheduler, got: ' || v_status_val;

  -- status() should show pg_cron_available = no
  select value into v_status_val
  from ash.status()
  where metric = 'pg_cron_available';
  assert v_status_val like '%no%',
    'status() should show pg_cron not available, got: ' || v_status_val;

  /*
   * The external-scheduler path is the only other way pg_ash is driven, so
   * the CALL surface has to be usable there too — an operator's `psql -c
   * "call ash.run_take_sample()"` loop is the documented replacement for a
   * pg_cron job. Assert the side effect with exact values rather than just
   * calling it: the fixture backend below guarantees exactly one sampled
   * database, so a correct sampler writes exactly one row into the current
   * ring slot.
   */
  assert (
    select count(*) = 1
    from pg_stat_activity
    where application_name = 'pg_ash_cronless_workload'
      and state = 'active'
  ), 'external-path fixture: expected exactly one tagged active backend';

  truncate table ash.sample;
  update ash.config set sampling_enabled = true where singleton;

  call ash.run_take_sample();
  select count(*) into v_after from ash.sample;
  assert v_after = 1,
    format(
      'external path: run_take_sample expected exactly 1 sample row, got %s',
      v_after
    );
  assert (select min(slot) from ash.sample) = ash.current_slot(),
    format(
      'external path: run_take_sample wrote slot %s, expected current slot %s',
      (select min(slot) from ash.sample), ash.current_slot()
    );

  -- The function form must remain equally usable on the external path.
  select count(*) into v_before from ash.sample;
  perform ash.take_sample();
  select count(*) into v_after from ash.sample;
  assert v_after = v_before + 1,
    format(
      'external path: take_sample expected ash.sample %s -> %s, got %s',
      v_before, v_before + 1, v_after
    );

  /*
   * The maintenance procedures must also work with no pg_cron present: an
   * external scheduler invokes exactly these. Roll up the samples just taken
   * and assert the grain landed, so this is a side-effect check rather than
   * a smoke test.
   */
  /*
   * Seed a deterministic complete minute so the rollup procedures have real
   * work to do. Asserting only "the open minute was not folded" would pass
   * even if the procedures did nothing at all, which is what the earlier
   * version of this check amounted to.
   */
  select ash._register_wait('active', 'Degraded', 'ExternalPath')
  into v_wait_id;
  select oid into v_datid from pg_database
  where datname = current_database();
  /*
   * Two hours back, so the minute AND the hour containing it are both
   * complete: rollup_minute folds the grain, and rollup_hour has a finished
   * hour to fold. A fixture in the current hour would leave rollup_hour with
   * legitimately nothing to do, which proves nothing.
   */
  v_minute := ash.ts_from_timestamptz(
    date_trunc('minute', clock_timestamp()) - interval '2 hours'
  );
  insert into ash.sample (sample_ts, datid, active_count, data, slot)
  select v_minute + offs, v_datid, 2, array[-v_wait_id::int, 2, 0, 0],
    ash.current_slot()
  from unnest(array[0, 10, 20]) as offs;
  update ash.config
  set last_rollup_1m_ts = v_minute, last_rollup_1h_ts = null
  where singleton;

  call ash.run_rollup_minute();
  assert (
    select samples = 3 and peak_backends = 2
      and wait_counts = array[v_wait_id::int, 6]
    from ash.rollup_1m
    where ts = v_minute and datid = v_datid
  ), format(
    'external path: run_rollup_minute produced the wrong grain: %s',
    (
      select row(samples, peak_backends, wait_counts)::text
      from ash.rollup_1m where ts = v_minute and datid = v_datid
    )
  );

  call ash.run_rollup_hour();
  assert (
    select count(*) = 1
    from ash.rollup_1h
    where datid = v_datid
  ), format(
    'external path: run_rollup_hour expected exactly 1 hourly row, got %s',
    (select count(*) from ash.rollup_1h where datid = v_datid)
  );

  call ash.run_rollup_cleanup();
  assert (select count(*) from ash.rollup_1m where ts = v_minute) = 1,
    'external path: run_rollup_cleanup deleted a within-retention grain';
  /*
   * Assert the invariant, not a raw count: whether the samples just taken
   * fall in a already-complete minute depends on where in the wall clock
   * the test happens to run, so a fixed row count here would flake across a
   * minute boundary. What must always hold is that the CURRENT, still-open
   * minute is never folded.
   */
  assert (
    select count(*)
    from ash.rollup_1m
    where ts >= ash.ts_from_timestamptz(
      date_trunc('minute', clock_timestamp())
    )
  ) = 0,
    format(
      'external path: rollup_minute folded the current incomplete minute, '
      'got %s such rollup_1m row(s)',
      (
        select count(*)
        from ash.rollup_1m
        where ts >= ash.ts_from_timestamptz(
          date_trunc('minute', clock_timestamp())
        )
      )
    );

  -- All 2.0 reader functions should work
  perform ash.periods();
  perform * from ash.top('wait_event', now() - interval '1 hour', now());
  perform * from ash.samples(now() - interval '1 hour', now());
  perform * from ash.chart(now() - interval '1 hour', now());

  raise notice 'All degraded-mode (no pg_cron) tests PASSED';
end;
$$;

select ash.uninstall('yes');
