do $$
declare
  v_rec record;
  v_status_val text;
begin
  -- start() should work without pg_cron (no error, prints hints)
  select status into v_status_val
  from ash.start('1 second')
  where job_type = 'sampler';
  assert v_status_val like '%schedule externally%',
    'start() without pg_cron should say schedule externally, got: ' || v_status_val;

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

  -- take_sample() should work without pg_cron
  perform ash.take_sample();

  -- All 2.0 reader functions should work
  perform ash.periods();
  perform * from ash.top('wait_event', now() - interval '1 hour', now());
  perform * from ash.samples(now() - interval '1 hour', now());
  perform * from ash.chart(now() - interval '1 hour', now());

  raise notice 'All degraded-mode (no pg_cron) tests PASSED';
end;
$$;

select ash.uninstall('yes');
