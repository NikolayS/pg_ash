\set ON_ERROR_STOP on

-- Transactional roles and jobs cannot leak into later CI steps.
begin;
create role ash_visibility_owner;
create role ash_visibility_reader;
select ash.grant_reader('ash_visibility_owner');
select ash.grant_reader('ash_visibility_reader');
grant usage on schema cron to ash_visibility_owner, ash_visibility_reader;
grant select on cron.job to ash_visibility_owner, ash_visibility_reader;
grant execute on function cron.schedule(text, text, text)
  to ash_visibility_owner, ash_visibility_reader;
alter schema ash owner to ash_visibility_owner;

set local role ash_visibility_owner;
select cron.schedule('ash_sampler', '0 0 1 1 *', 'select 1');
do $$
begin
  assert row_security_active('cron.job'), 'fixture must use real pg_cron RLS';
  assert (select count(*) = 1 from cron.job where jobname = 'ash_sampler');
  assert (select count(*) = 1 from ash.status() where metric = 'cron_job_ash_sampler'),
    'schema owner must still see its job';
  assert not exists (select from ash.status() where metric = 'cron_jobs'),
    'schema owner must not receive a restricted-reader hint';
end $$;

set local role ash_visibility_reader;
do $$
declare
  v_hint text;
begin
  assert has_schema_privilege(current_user, 'cron', 'usage')
    and has_table_privilege(current_user, 'cron.job', 'select');
  assert row_security_active('cron.job');
  assert not exists (select from cron.job), 'RLS must hide the owner job';
  assert not exists (select from ash.status() where metric ~ '^cron_job_'),
    'status must not expose the hidden owner job';
  select value into v_hint from ash.status() where metric = 'cron_jobs';
  assert v_hint = '<cron.job visibility is limited by row-level security; '
    'missing rows do not mean jobs are stopped. Ask the pg_ash owner to inspect scheduling.>',
    format('missing RLS visibility explanation: %s', v_hint);
end $$;

-- A visible reader-owned job does not prove visibility of the owner's jobs.
select cron.schedule('ash_rotation', '0 0 1 1 *', 'select 1');
do $$
begin
  assert (select count(*) = 1 from ash.status() where metric ~ '^cron_job_');
  assert (select count(*) = 1 from ash.status() where metric = 'cron_job_ash_rotation');
  assert not exists (select from ash.status() where metric = 'cron_job_ash_sampler');
  assert exists (select from ash.status() where metric = 'cron_jobs'
    and value like '%row-level security%');
end $$;

reset role;
do $$
begin
  assert not row_security_active('cron.job'), 'superuser must bypass RLS';
  assert exists (select from ash.status() where metric = 'cron_job_ash_sampler');
  assert exists (select from ash.status() where metric = 'cron_job_ash_rotation');
  assert not exists (select from ash.status() where metric = 'cron_jobs'),
    'superuser must not receive a restricted-reader hint';
end $$;
rollback;
