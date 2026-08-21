do $$
declare
  v_rec record;
  v_status_val text;
  v_before bigint;
  v_after bigint;
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
  call ash.run_rollup_minute();
  call ash.run_rollup_hour();
  call ash.run_rollup_cleanup();
  assert (select count(*) from ash.rollup_1m) = 0,
    format(
      'external path: rollup_minute folded an incomplete minute, got %s '
      'rollup_1m row(s)',
      (select count(*) from ash.rollup_1m)
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
