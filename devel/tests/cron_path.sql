-- Clean slate: stop any prior sampler, truncate raw partitions
-- so the post-baseline count is unambiguous.
select ash.stop();
truncate ash.sample_0, ash.sample_1, ash.sample_2;

-- Capture pre-start baseline (should be 0 across all slots).
do $$
declare
  v_baseline bigint;
begin
  select count(*) into v_baseline from ash.sample;
  assert v_baseline = 0,
    format('expected 0 samples after truncate, got %s', v_baseline);
end $$;

-- Schedule sampling at 1 Hz via pg_cron.
select ash.start('1 second');

-- Sanity: cron.job has the ash row.
do $$
declare
  v_jobs int;
begin
  select count(*) into v_jobs
  from cron.job where jobname like 'ash_%';
  assert v_jobs >= 1,
    format('expected ash_* job(s) in cron.job, got %s', v_jobs);
end $$;
