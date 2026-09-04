#!/usr/bin/env bash
# Run against an owned disposable database with pg_cron and pg_ash installed.
set -Eeuo pipefail
IFS=$'\n\t'
export PAGER=cat

psql_base=(
  psql
  --no-psqlrc
  --host="${PGHOST:-localhost}"
  --username="${PGUSER:-postgres}"
  --dbname="${PGDATABASE:-postgres}"
  --set=ON_ERROR_STOP=1
)
locker_application="pg_ash_issue_203_lock_holder"
locker_shell_pid=""
locker_backend_pid=""

stop_locker() {
  local shell_pid="${locker_shell_pid}"
  local backend_pid="${locker_backend_pid}"

  locker_shell_pid=""
  locker_backend_pid=""
  if [[ "${backend_pid}" =~ ^[0-9]+$ ]]; then
    "${psql_base[@]}" \
      --command="
        select pg_terminate_backend(pid)
        from pg_stat_activity
        where pid = ${backend_pid}
          and application_name = '${locker_application}';
      " >/dev/null 2>&1 || true
  fi
  if [[ -n "${shell_pid}" ]]; then
    kill -TERM "${shell_pid}" >/dev/null 2>&1 || true
    wait "${shell_pid}" >/dev/null 2>&1 || true
  fi
}

cleanup_issue_203() {
  local exit_code=$?

  trap - EXIT INT TERM
  stop_locker
  "${psql_base[@]}" --quiet >/dev/null 2>&1 <<'SQL' || true
select cron.unschedule(jobid) from cron.job
where username = 'ash_issue_203_other_superuser';
drop role if exists ash_issue_203_other_superuser;
SQL
  exit "${exit_code}"
}

trap cleanup_issue_203 EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

"${psql_base[@]}" \
  --command="select * from ash.stop(); select * from ash.start((select sample_interval from ash.config where singleton));" \
  >/dev/null

"${psql_base[@]}" <<'EOF'
do $$
declare
  v_job_count int;
  v_all_active boolean;
begin
  select count(*), bool_and(active)
  into v_job_count, v_all_active
  from cron.job
  where jobname = any(array[
    'ash_sampler',
    'ash_rotation',
    'ash_rollup_1m',
    'ash_rollup_1h',
    'ash_rollup_gc'
  ])
    and username = current_user
    and database = current_database();
  assert v_job_count = 5 and v_all_active,
    format(
      'issue #203 fixture requires five active jobs, '
      'got count=%s all_active=%s',
      v_job_count,
      v_all_active
    );
  assert (
    select active and command = 'call ash.run_rollup_minute()'
    from cron.job
    where jobname = 'ash_rollup_1m'
      and username = current_user
      and database = current_database()
  ), 'issue #203 fixture requires the active minute-rollup command';
end;
$$;
EOF

PGAPPNAME="${locker_application}" \
  "${psql_base[@]}" --quiet --command="
    do \$locker\$
    declare
      v_jobname name;
    begin
      select jobname into strict v_jobname
      from cron.job
      where jobname = 'ash_rollup_1m'
        and username = current_user
        and database = current_database()
      for update;
      perform pg_sleep(300);
    end
    \$locker\$;
  " >/dev/null 2>&1 &
locker_shell_pid=$!

for attempt in $(seq 1 100); do
  locker_backend_pid="$("${psql_base[@]}" \
    --tuples-only \
    --no-align \
    --command="
      select pid
      from pg_stat_activity
      where application_name = '${locker_application}'
        and wait_event_type = 'Timeout'
        and wait_event = 'PgSleep';
    ")"
  locker_backend_pid="$(
    printf '%s' "${locker_backend_pid}" | tr -d '[:space:]'
  )"
  if [[ "${locker_backend_pid}" =~ ^[0-9]+$ ]]; then
    break
  fi
  sleep 0.1
done
if [[ ! "${locker_backend_pid}" =~ ^[0-9]+$ ]]; then
  echo "issue #203 lock holder did not reach pg_sleep" >&2
  exit 1
fi

"${psql_base[@]}" <<'EOF'
set lock_timeout = '500ms';

do $$
declare
  v_false_successes text[] := '{}'::text[];
  v_original_n smallint;
  v_original_interval interval;
  v_job_count int;
  v_all_active boolean;
  v_wait_id smallint;
  v_slot smallint;
  v_sentinel_ts int4 := ash.ts_from_timestamptz(now()) - 4242;
begin
  select num_partitions, sample_interval
  into v_original_n, v_original_interval
  from ash.config
  where singleton;
  select ash._register_wait(
    'active',
    'Issue203',
    'RebuildRollbackSentinel'
  ) into v_wait_id;
  select ash.current_slot() into v_slot;
  insert into ash.sample (
    sample_ts,
    datid,
    active_count,
    data,
    slot
  )
  values (
    v_sentinel_ts,
    0::oid,
    1,
    array[-v_wait_id, 1, 0]::int4[],
    v_slot
  );

  /*
   * The sentinel exception rolls back each subtransaction when the
   * lifecycle call falsely returns success. That lets one fixture
   * enumerate every affected caller without allowing uninstall()
   * to remove the schema before the remaining assertions execute.
   */
  begin
    perform ash.stop();
    raise exception using
      errcode = 'P0203',
      message = 'ash.stop() falsely reported success';
  exception
    when sqlstate 'P0203' then
      v_false_successes :=
        array_append(v_false_successes, 'ash.stop()');
    when lock_not_available then
      assert sqlerrm like '%ash_rollup_1m%',
        'ash.stop() error must name the job it could not unschedule';
  end;

  begin
    perform ash.start((select sample_interval from ash.config where singleton));
    raise exception using
      errcode = 'P0203',
      message = 'ash.start() falsely reported success';
  exception
    when sqlstate 'P0203' then
      v_false_successes :=
        array_append(v_false_successes, 'ash.start()');
    when lock_not_available then
      assert sqlerrm like '%ash_rollup_1m%',
        'ash.start() error must name the job it could not replace';
  end;

  begin
    perform ash.rebuild_partitions(4, 'yes');
    raise exception using
      errcode = 'P0203',
      message = 'ash.rebuild_partitions() falsely reported success';
  exception
    when sqlstate 'P0203' then
      v_false_successes :=
        array_append(v_false_successes, 'ash.rebuild_partitions()');
    when lock_not_available then
      assert sqlerrm like '%ash_rollup_1m%',
        'rebuild_partitions() error must name the surviving job';
  end;

  begin
    perform ash.uninstall('yes');
    raise exception using
      errcode = 'P0203',
      message = 'ash.uninstall() falsely reported success';
  exception
    when sqlstate 'P0203' then
      v_false_successes :=
        array_append(v_false_successes, 'ash.uninstall()');
    when lock_not_available then
      assert sqlerrm like '%ash_rollup_1m%',
        'uninstall() error must name the surviving job';
  end;

  assert to_regnamespace('ash') is not null,
    'failed uninstall must preserve the ash schema';
  assert (
    select sampling_enabled
    from ash.config
    where singleton
  ), 'failed lifecycle calls must preserve sampling_enabled=true';
  assert (
    select num_partitions
    from ash.config
    where singleton
  ) = v_original_n,
    'failed rebuild_partitions must preserve num_partitions';
  assert (
    select sample_interval
    from ash.config
    where singleton
  ) = v_original_interval,
    'failed ash.start must preserve sample_interval';
  assert exists (
    select
    from ash.sample
    where sample_ts = v_sentinel_ts
      and datid = 0::oid
      and data = array[-v_wait_id, 1, 0]::int4[]
  ), 'failed rebuild_partitions must preserve raw samples';

  select count(*), bool_and(active)
  into v_job_count, v_all_active
  from cron.job
  where jobname = any(array[
    'ash_sampler',
    'ash_rotation',
    'ash_rollup_1m',
    'ash_rollup_1h',
    'ash_rollup_gc'
  ])
    and username = current_user
    and database = current_database();
  assert v_job_count = 5 and v_all_active,
    format(
      'failed lifecycle calls must preserve five active jobs, '
      'got count=%s all_active=%s',
      v_job_count,
      v_all_active
    );
  assert (
    select active and command = 'call ash.run_rollup_minute()'
    from cron.job
    where jobname = 'ash_rollup_1m'
      and username = current_user
      and database = current_database()
  ), 'failed lifecycle calls changed the locked minute-rollup job';
  assert (
    select active
           and schedule = case when extract(epoch from v_original_interval) < 60
             then extract(epoch from v_original_interval)::int || ' seconds'
             else '*/1 * * * *' end
           and command =
             'set statement_timeout = ''500ms''; '
             'call ash.run_take_sample()'
    from cron.job
    where jobname = 'ash_sampler'
      and username = current_user
      and database = current_database()
  ), 'failed ash.start changed the sampler job';
  assert (
    select active
           and schedule = '0 0 * * *'
           and command = 'call ash.run_rotate()'
    from cron.job
    where jobname = 'ash_rotation'
      and username = current_user
      and database = current_database()
  ), 'failed lifecycle calls changed the rotation job';

  assert cardinality(v_false_successes) = 0,
    format(
      'issue #203: lifecycle functions reported success while '
      'ash_rollup_1m could not be unscheduled: %s',
      array_to_string(v_false_successes, ', ')
    );

  raise notice
    'issue #203: stop/rebuild/uninstall propagate unschedule '
    'failures atomically';
end;
$$;

reset lock_timeout;
EOF

stop_locker

"${psql_base[@]}" <<'EOF'
do $$
declare
  v_removed_count int;
  v_removed_with_ids int;
  v_all_removed boolean;
  v_second_count int;
  v_sampler_job bigint;
  v_rotation_job bigint;
  v_rollup_job bigint;
begin
  select count(*), count(job_id), bool_and(status = 'removed')
  into v_removed_count, v_removed_with_ids, v_all_removed
  from ash.stop();
  assert v_removed_count = 5
         and v_removed_with_ids = 5
         and v_all_removed,
    format(
      'successful stop must report five real removals with job IDs, '
      'got rows=%s IDs=%s all_removed=%s',
      v_removed_count,
      v_removed_with_ids,
      v_all_removed
    );

  select count(*) into v_second_count from ash.stop();
  assert v_second_count = 0,
    format(
      'stop with all jobs absent must be idempotent, got %s rows',
      v_second_count
    );

  perform ash.start((select sample_interval from ash.config where singleton));
  select jobid into strict v_sampler_job
  from cron.job
  where jobname = 'ash_sampler'
    and username = current_user
    and database = current_database();
  select jobid into strict v_rotation_job
  from cron.job
  where jobname = 'ash_rotation'
    and username = current_user
    and database = current_database();
  select jobid into strict v_rollup_job
  from cron.job
  where jobname = 'ash_rollup_1h'
    and username = current_user
    and database = current_database();
  -- Preserve all custom commands, but explicit start reactivates local jobs.
  update cron.job set command = 'select 1', active = false
  where username = current_user and database = current_database()
    and jobname in ('ash_sampler', 'ash_rotation', 'ash_rollup_1m',
                    'ash_rollup_1h', 'ash_rollup_gc');
  perform ash.start((select sample_interval from ash.config where singleton));
  assert (select count(*) = 5 and bool_and(active and command = 'select 1')
    from cron.job where username = current_user
      and database = current_database()
      and jobname in ('ash_sampler', 'ash_rotation', 'ash_rollup_1m',
                      'ash_rollup_1h', 'ash_rollup_gc')),
    'explicit start must reactivate every local job and preserve custom text';

  -- A role's named-job uniqueness spans databases. Never steal a collision.
  perform cron.alter_job(job_id := v_sampler_job, database := 'template1');
  begin
    perform ash.start((select sample_interval from ash.config where singleton));
    raise exception 'cross-database start falsely succeeded';
  exception when object_not_in_prerequisite_state then
    assert sqlerrm like '%ash_sampler%template1%',
      'collision error must name the job and target database';
  end;
  assert (select database = 'template1' and command = 'select 1'
    from cron.job where jobid = v_sampler_job),
    'failed start must preserve the foreign-database job';
  begin
    perform ash.stop();
    raise exception 'cross-database stop falsely succeeded';
  exception when object_not_in_prerequisite_state then
    null;
  end;
  assert (select sampling_enabled from ash.config where singleton),
    'collision must leave config unchanged';
  assert (select count(*) = 5 from cron.job where username = current_user
    and jobname in ('ash_sampler', 'ash_rotation', 'ash_rollup_1m',
                    'ash_rollup_1h', 'ash_rollup_gc')),
    'collision must preserve every managed job';
  perform cron.alter_job(job_id := v_sampler_job,
    database := current_database());

  raise notice
    'issue #203: absent-job idempotency and existing-job '
    'convergence verified';
end;
$$;

create role ash_issue_203_other_superuser superuser;
set role ash_issue_203_other_superuser;

do $$
declare
  v_false_successes text[] := '{}'::text[];
begin
  begin
    perform ash.start((select sample_interval from ash.config where singleton));
    raise exception using
      errcode = 'P0203',
      message = 'cross-role ash.start() falsely reported success';
  exception
    when sqlstate 'P0203' then
      v_false_successes :=
        array_append(v_false_successes, 'ash.start()');
    when insufficient_privilege then
      assert sqlerrm like '%ash schema owner postgres%',
        'cross-role ash.start error must name the schema owner';
  end;

  begin
    perform ash.stop();
    raise exception using
      errcode = 'P0203',
      message = 'cross-role ash.stop() falsely reported success';
  exception
    when sqlstate 'P0203' then
      v_false_successes :=
        array_append(v_false_successes, 'ash.stop()');
    when insufficient_privilege then
      assert sqlerrm like '%ash schema owner postgres%',
        'cross-role ash.stop error must name the schema owner';
  end;

  begin
    perform ash.rebuild_partitions(4, 'yes');
    raise exception using
      errcode = 'P0203',
      message =
        'cross-role ash.rebuild_partitions() falsely reported success';
  exception
    when sqlstate 'P0203' then
      v_false_successes :=
        array_append(
          v_false_successes,
          'ash.rebuild_partitions()'
        );
    when insufficient_privilege then
      assert sqlerrm like '%ash schema owner postgres%',
        'cross-role rebuild error must name the schema owner';
  end;

  begin
    perform ash.uninstall('yes');
    raise exception using
      errcode = 'P0203',
      message = 'cross-role ash.uninstall() falsely reported success';
  exception
    when sqlstate 'P0203' then
      v_false_successes :=
        array_append(v_false_successes, 'ash.uninstall()');
    when insufficient_privilege then
      assert sqlerrm like '%ash schema owner postgres%',
        'cross-role uninstall error must name the schema owner';
  end;

  assert cardinality(v_false_successes) = 0,
    format(
      'issue #203: cross-role lifecycle calls reported success: %s',
      array_to_string(v_false_successes, ', ')
    );
end;
$$;

select cron.schedule(
  'ash_rollup_1m',
  '0 0 1 1 *',
  'call ash.run_rollup_minute()'
);
reset role;

do $$
begin
  begin
    perform ash.start((select sample_interval from ash.config where singleton));
    raise exception using
      errcode = 'P0203',
      message = 'start ignored a foreign-owned managed job';
  exception
    when sqlstate 'P0203' then
      raise;
    when object_not_in_prerequisite_state then
      assert sqlerrm like '%ash_rollup_1m%ash_issue_203_other_superuser%',
        'start refusal must name the foreign job and owner';
  end;
  begin
    perform ash.uninstall('yes');
    raise exception using
      errcode = 'P0203',
      message = 'uninstall ignored a foreign-owned managed job';
  exception
    when sqlstate 'P0203' then
      raise;
    when object_not_in_prerequisite_state then
      assert sqlerrm like
               '%ash_rollup_1m%ash_issue_203_other_superuser%',
        'foreign-job refusal must name the job and owning role';
  end;

  assert to_regnamespace('ash') is not null,
    'foreign-owned job refusal must preserve the ash schema';
  assert (
    select sampling_enabled
    from ash.config
    where singleton
  ), 'foreign-owned job refusal must preserve sampling_enabled=true';
  assert (
    select count(*) = 5 and bool_and(active)
    from cron.job
    where jobname = any(array[
      'ash_sampler',
      'ash_rotation',
      'ash_rollup_1m',
      'ash_rollup_1h',
      'ash_rollup_gc'
    ])
      and username = current_user
  ), 'foreign-owned job refusal changed schema-owner jobs';

  raise notice
    'issue #203: cross-role lifecycle calls and foreign jobs '
    'fail closed';
end;
$$;

set role ash_issue_203_other_superuser;
select cron.unschedule('ash_rollup_1m');
reset role;
drop role ash_issue_203_other_superuser;
EOF

trap - EXIT INT TERM

"${psql_base[@]}" <<'SQL'
begin;
select * from ash.stop();
create role ash_lifecycle_owner;
grant pg_read_all_stats to ash_lifecycle_owner;
alter schema ash owner to ash_lifecycle_owner;
grant all on all tables in schema ash to ash_lifecycle_owner;
grant all on all sequences in schema ash to ash_lifecycle_owner;
grant execute on all routines in schema ash to ash_lifecycle_owner;
grant usage on schema cron to ash_lifecycle_owner;
grant select on cron.job to ash_lifecycle_owner;
set role ash_lifecycle_owner;
do $$
begin
  perform ash.start((select sample_interval from ash.config where singleton));
  assert (select count(*) = 5 and bool_and(active)
    from cron.job where username = current_user
      and database = current_database()),
    'non-superuser schema owner must create five active jobs';
  perform ash.start((select sample_interval from ash.config where singleton));
  assert (select count(*) = 5 and bool_and(active)
    from cron.job where username = current_user
      and database = current_database()),
    'non-superuser second start must preserve five active jobs';
end;
$$;
reset role;
select cron.alter_job(job_id := jobid, active := false)
from cron.job where username = 'ash_lifecycle_owner';
set role ash_lifecycle_owner;
do $$
begin
  perform ash.start((select sample_interval from ash.config where singleton));
  assert (select count(*) = 5 and bool_and(active)
    from cron.job where username = current_user
      and database = current_database()),
    'non-superuser start must reactivate all five existing jobs';
  assert (select count(*) = 5 and count(job_id) = 5 from ash.stop()),
    'non-superuser schema owner must remove its own jobs';
  assert not (select sampling_enabled from ash.config where singleton),
    'non-superuser stop must disable sampling';
  raise notice 'non-superuser schema owner lifecycle PASSED';
end;
$$;
reset role;
rollback;
SQL
