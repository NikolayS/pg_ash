do $$
declare
  v_rec record;
  v_status_val text;
  v_sampled int;
  v_skipped_before bigint;
  v_skipped_after bigint;
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
   * The external-ticking trap: ash.stop() (called just above) clears
   * ash.config.sampling_enabled, and take_sample() then silently no-ops and
   * bumps skipped_samples. An integrator who ticks take_sample() from their
   * own scheduler therefore still depends on ash.start(), even with pg_cron
   * absent -- the opposite of what "you don't need start() without pg_cron"
   * suggests. Assert both sides so the contract cannot regress unnoticed.
   */
  select skipped_samples into v_skipped_before from ash.config where singleton;
  v_sampled := ash.take_sample();
  select skipped_samples into v_skipped_after from ash.config where singleton;
  assert v_sampled = 0,
    format('take_sample() must no-op while sampling is disabled, got %s',
      v_sampled);
  assert v_skipped_after = v_skipped_before + 1,
    format('disabled take_sample() must bump skipped_samples by exactly 1, '
      'got %s -> %s', v_skipped_before, v_skipped_after);

  -- Re-enabling restores collection without pg_cron.
  perform * from ash.start('1 second');
  select skipped_samples into v_skipped_before from ash.config where singleton;
  v_sampled := ash.take_sample();
  select skipped_samples into v_skipped_after from ash.config where singleton;
  /*
   * take_sample() excludes its own backend (pid <> pg_backend_pid()), so an
   * otherwise idle test database legitimately captures zero backends. The
   * exact discriminator between "sampled and found nothing" and "refused to
   * sample" is therefore skipped_samples, not the return value.
   */
  assert v_sampled is not null and v_sampled >= 0,
    format('re-enabled take_sample() must return a count, got %s', v_sampled);
  assert v_skipped_after = v_skipped_before,
    format('an accepted sample must not bump skipped_samples, got %s -> %s',
      v_skipped_before, v_skipped_after);

  -- All 2.0 reader functions should work
  perform ash.periods();
  perform * from ash.top('wait_event', now() - interval '1 hour', now());
  perform * from ash.samples(now() - interval '1 hour', now());
  perform * from ash.chart(now() - interval '1 hour', now());

  raise notice 'All degraded-mode (no pg_cron) tests PASSED';
end;
$$;

select ash.uninstall('yes');
