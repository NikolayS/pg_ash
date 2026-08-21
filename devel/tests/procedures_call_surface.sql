/* -------------------------------------------------------------------------
 * Issue #221: CALL-able maintenance procedures
 *
 * Behavioural coverage for ash.run_take_sample / run_rotate /
 * run_rollup_minute / run_rollup_hour / run_rollup_cleanup. Asserts exact
 * aggregate values and real side effects, proves the procedure and function
 * forms are the same code path, and checks that the two mis-resolution cases
 * and the reader-role denials raise the right SQLSTATEs.
 *
 * Extracted from .github/workflows/test.yml: GitHub Actions silently refuses
 * to create a workflow run when the workflow file exceeds 512 KiB, and
 * test.yml sits close to that ceiling. Keep large test bodies here and invoke
 * them with psql -f from a short workflow step.
 *
 * Requires an active backend tagged
 * application_name = 'pg_ash_issue_221_workload' so ash.take_sample() has
 * something to sample; the workflow step provides it.
 * ------------------------------------------------------------------------- */
truncate table ash.sample;
update ash.config
set sampling_enabled = true,
  include_bg_workers = false
where singleton;

do $take_sample$
declare
  v_after bigint;
  v_before bigint;
  v_slot smallint := ash.current_slot();
begin
  select count(*) into v_before from ash.sample;
  call ash.run_take_sample();
  select count(*) into v_after from ash.sample;

  assert v_after = v_before + 1,
    format(
      'run_take_sample expected sample rows %s -> %s, got %s -> %s',
      v_before,
      v_before + 1,
      v_before,
      v_after
    );
  assert (
    select count(*) = 1 and min(slot) = v_slot
    from ash.sample
  ), format(
    'run_take_sample expected its one new row in slot %s, got %s',
    v_slot,
    (select string_agg(slot::text, ',' order by slot)
     from ash.sample)
  );
end
$take_sample$;

do $resolution$
begin
  begin
    execute 'select ash.run_take_sample()';
    raise exception
      'select ash.run_take_sample() unexpectedly resolved';
  exception when others then
    assert sqlstate = '42809', format(
      'select ash.run_take_sample() expected 42809 '
      '(wrong_object_type), got %s: %s',
      sqlstate,
      sqlerrm
    );
  end;

  begin
    execute 'call ash.take_sample()';
    raise exception 'call ash.take_sample() unexpectedly resolved';
  exception when others then
    assert sqlstate = '42809', format(
      'call ash.take_sample() expected 42809 '
      '(wrong_object_type), got %s: %s',
      sqlstate,
      sqlerrm
    );
  end;
end
$resolution$;

do $side_effects$
declare
  v_datid oid := (
    select oid from pg_database where datname = current_database()
  );
  v_function_row ash.rollup_1m%rowtype;
  v_hour int4 := ash.ts_from_timestamptz(
    date_trunc('hour', statement_timestamp()) - interval '2 hours'
  );
  v_hour_row ash.rollup_1h%rowtype;
  v_minute int4 := ash.ts_from_timestamptz(
    date_trunc('minute', statement_timestamp())
      - interval '5 minutes'
  );
  v_num_partitions smallint;
  v_old_slot smallint;
  v_procedure_row ash.rollup_1m%rowtype;
  v_recent_ts int4 := ash.ts_from_timestamptz(
    date_trunc('hour', statement_timestamp())
  );
  v_result int;
  v_target_slot smallint;
  v_truncate_slot smallint;
  v_wait_id smallint;
begin
  select ash._register_wait(
    'active',
    'Issue221',
    'ProcedureEquivalence'
  )
  into v_wait_id;

  /*
   * Three identical complete-minute fixtures let the function
   * process the first grain and the procedure process the second.
   * The untouched third grain proves batch_limit = 1 is honored.
   */
  truncate table ash.sample, ash.rollup_1m, ash.rollup_1h;
  update ash.config
  set current_slot = 0,
    last_rollup_1m_ts = v_minute,
    last_rollup_1h_ts = null
  where singleton;

  for minute_idx in 0..2 loop
    for sample_idx in 0..2 loop
      insert into ash.sample (
        sample_ts,
        datid,
        active_count,
        data,
        slot
      ) values (
        v_minute + minute_idx * 60 + sample_idx * 10,
        v_datid,
        2,
        array[-v_wait_id::int, 2, 0, 0],
        0
      );
    end loop;
  end loop;

  select ash.rollup_minute(1) into v_result;
  assert v_result = 1, format(
    'rollup_minute(1) control expected 1 grain, got %s',
    v_result
  );
  select * into strict v_function_row
  from ash.rollup_1m
  where ts = v_minute and datid = v_datid;

  call ash.run_rollup_minute(1);
  select * into strict v_procedure_row
  from ash.rollup_1m
  where ts = v_minute + 60 and datid = v_datid;

  assert row(
    v_procedure_row.samples,
    v_procedure_row.peak_backends,
    v_procedure_row.wait_counts,
    v_procedure_row.query_counts
  ) is not distinct from row(
    v_function_row.samples,
    v_function_row.peak_backends,
    v_function_row.wait_counts,
    v_function_row.query_counts
  ), format(
    'run_rollup_minute aggregate differs from function: '
    'function=(%s,%s,%s,%s) procedure=(%s,%s,%s,%s)',
    v_function_row.samples,
    v_function_row.peak_backends,
    v_function_row.wait_counts,
    v_function_row.query_counts,
    v_procedure_row.samples,
    v_procedure_row.peak_backends,
    v_procedure_row.wait_counts,
    v_procedure_row.query_counts
  );
  assert v_procedure_row.samples = 3
    and v_procedure_row.peak_backends = 2
    and v_procedure_row.wait_counts
      = array[v_wait_id::int, 6]
    and v_procedure_row.query_counts = '{}'::int8[],
    format(
      'run_rollup_minute exact aggregate mismatch: (%s,%s,%s,%s)',
      v_procedure_row.samples,
      v_procedure_row.peak_backends,
      v_procedure_row.wait_counts,
      v_procedure_row.query_counts
    );
  assert (
    select last_rollup_1m_ts = v_minute + 120
    from ash.config where singleton
  ), format(
    'run_rollup_minute(1) expected watermark %s, got %s',
    v_minute + 120,
    (select last_rollup_1m_ts from ash.config where singleton)
  );
  assert (select count(*) from ash.rollup_1m) = 2,
    format(
      'run_rollup_minute(1) expected exactly 2 total grain rows, got %s',
      (select count(*) from ash.rollup_1m)
    );

  /*
   * Hourly wrapper must reproduce the function's exact aggregation.
   */
  truncate table ash.rollup_1m, ash.rollup_1h;
  insert into ash.rollup_1m (
    ts,
    datid,
    samples,
    peak_backends,
    wait_counts,
    query_counts
  ) values
    (
      v_hour, v_datid, 2, 3,
      array[v_wait_id::int, 3],
      array[101, 2]::int8[]
    ),
    (
      v_hour + 60, v_datid, 3, 4,
      array[v_wait_id::int, 5],
      array[101, 1, 202, 4]::int8[]
    );
  update ash.config
  set last_rollup_1m_ts = v_hour + 3600,
    last_rollup_1h_ts = v_hour
  where singleton;

  call ash.run_rollup_hour();
  select * into strict v_hour_row
  from ash.rollup_1h
  where ts = v_hour and datid = v_datid;
  assert (select count(*) from ash.rollup_1h) = 1,
    format(
      'run_rollup_hour expected exactly 1 row, got %s',
      (select count(*) from ash.rollup_1h)
    );
  assert v_hour_row.samples = 5
    and v_hour_row.peak_backends = 4
    and v_hour_row.wait_counts = array[v_wait_id::int, 8]
    and v_hour_row.query_counts = '{202,4,101,3}'::int8[]
    and cardinality(v_hour_row.minute_counts) = 60
    and v_hour_row.minute_counts[1] = 3
    and v_hour_row.minute_counts[2] = 5
    and v_hour_row.minute_counts[3] is null,
    format(
      'run_rollup_hour exact aggregate mismatch: '
      'samples=%s peak=%s waits=%s queries=%s minutes=%s',
      v_hour_row.samples,
      v_hour_row.peak_backends,
      v_hour_row.wait_counts,
      v_hour_row.query_counts,
      v_hour_row.minute_counts
    );
  assert (
    select last_rollup_1h_ts = v_hour + 3600
    from ash.config where singleton
  ), format(
    'run_rollup_hour expected watermark %s, got %s',
    v_hour + 3600,
    (select last_rollup_1h_ts from ash.config where singleton)
  );

  /* Cleanup must delete only rows outside each retention window. */
  truncate table ash.rollup_1m, ash.rollup_1h;
  update ash.config
  set rollup_1h_retention_days = 1
  where singleton;
  insert into ash.rollup_1m values
    (0, v_datid, 1, 1, array[v_wait_id::int, 1], '{}'),
    (v_recent_ts, v_datid, 1, 1,
     array[v_wait_id::int, 1], '{}');
  insert into ash.rollup_1h values
    (0, v_datid, 1, 1, array[v_wait_id::int, 1], '{}', null),
    (v_recent_ts, v_datid, 1, 1,
     array[v_wait_id::int, 1], '{}', null);

  call ash.run_rollup_cleanup();
  assert (
    select count(*) = 1 and min(ts) = v_recent_ts
    from ash.rollup_1m
  ), format(
    'run_rollup_cleanup minute post-condition mismatch: %s',
    (select array_agg(ts order by ts) from ash.rollup_1m)
  );
  assert (
    select count(*) = 1 and min(ts) = v_recent_ts
    from ash.rollup_1h
  ), format(
    'run_rollup_cleanup hour post-condition mismatch: %s',
    (select array_agg(ts order by ts) from ash.rollup_1h)
  );
  update ash.config
  set rollup_1h_retention_days = 1825
  where singleton;

  /* Rotate must advance once and truncate the following ring slot. */
  truncate table ash.sample;
  select current_slot, num_partitions
  into v_old_slot, v_num_partitions
  from ash.config where singleton;
  v_target_slot := (v_old_slot + 1) % v_num_partitions;
  v_truncate_slot := (v_target_slot + 1) % v_num_partitions;
  execute format(
    'insert into ash.sample_%1$s '
    '(sample_ts, datid, active_count, data, slot) '
    'values (0, $1, 1, $2, %1$s)',
    v_truncate_slot
  ) using v_datid, array[-v_wait_id::int, 1, 0];
  execute format(
    'insert into ash.query_map_%s (query_id) '
    'values (221221) on conflict (query_id) do nothing',
    v_truncate_slot
  );
  update ash.config
  set rotated_at = clock_timestamp() - interval '2 days'
  where singleton;

  call ash.run_rotate();
  assert ash.current_slot() = v_target_slot, format(
    'run_rotate expected current_slot %s, got %s',
    v_target_slot,
    ash.current_slot()
  );
  execute format(
    'select count(*) from ash.sample_%s', v_truncate_slot
  ) into v_result;
  assert v_result = 0, format(
    'run_rotate expected sample_%s to be empty, got %s rows',
    v_truncate_slot,
    v_result
  );
  execute format(
    'select count(*) from ash.query_map_%s', v_truncate_slot
  ) into v_result;
  assert v_result = 0, format(
    'run_rotate expected query_map_%s to be empty, got %s rows',
    v_truncate_slot,
    v_result
  );

  update ash.config
  set last_rollup_1m_ts = null,
    last_rollup_1h_ts = null
  where singleton;
end
$side_effects$;

do $role_setup$
declare
  v_public_acl_count int;
begin
  if not exists (
    select from pg_roles where rolname = 'ash_issue_221_reader'
  ) then
    create role ash_issue_221_reader;
  end if;
  perform ash.grant_reader('ash_issue_221_reader');

  /*
   * PUBLIC is grantee OID 0 (it has no pg_roles row), which aclexplode
   * reports directly. Do NOT pattern-match proacl::text for '=X/': the
   * owner's own grant renders as 'postgres=X/postgres' and matches too,
   * which would flag a correctly hardened procedure.
   *
   * A NULL proacl means default privileges, and the default for a procedure
   * is EXECUTE to PUBLIC — so a NULL here is also a failure, not a pass.
   */
  select count(*) into v_public_acl_count
  from pg_proc as proc
  join pg_namespace as nsp on nsp.oid = proc.pronamespace
  where nsp.nspname = 'ash'
    and proc.prokind = 'p'
    and proc.proname in (
      'run_take_sample',
      'run_rotate',
      'run_rollup_minute',
      'run_rollup_hour',
      'run_rollup_cleanup'
    )
    and (
      proc.proacl is null
      or exists (
        select
        from aclexplode(proc.proacl) as acl
        where acl.grantee = 0
      )
    );
  assert v_public_acl_count = 0, format(
    'issue #221 procedures executable by PUBLIC: %s of 5',
    v_public_acl_count
  );
end
$role_setup$;

set role ash_issue_221_reader;
do $reader_denials$
begin
  begin
    call ash.run_take_sample();
    raise exception 'reader unexpectedly called run_take_sample';
  exception when others then
    assert sqlstate = '42501', format(
      'reader run_take_sample expected 42501, got %s: %s',
      sqlstate,
      sqlerrm
    );
  end;
  begin
    call ash.run_rotate();
    raise exception 'reader unexpectedly called run_rotate';
  exception when others then
    assert sqlstate = '42501', format(
      'reader run_rotate expected 42501, got %s: %s',
      sqlstate,
      sqlerrm
    );
  end;
  begin
    call ash.run_rollup_minute();
    raise exception 'reader unexpectedly called run_rollup_minute';
  exception when others then
    assert sqlstate = '42501', format(
      'reader run_rollup_minute expected 42501, got %s: %s',
      sqlstate,
      sqlerrm
    );
  end;
  begin
    call ash.run_rollup_hour();
    raise exception 'reader unexpectedly called run_rollup_hour';
  exception when others then
    assert sqlstate = '42501', format(
      'reader run_rollup_hour expected 42501, got %s: %s',
      sqlstate,
      sqlerrm
    );
  end;
  begin
    call ash.run_rollup_cleanup();
    raise exception 'reader unexpectedly called run_rollup_cleanup';
  exception when others then
    assert sqlstate = '42501', format(
      'reader run_rollup_cleanup expected 42501, got %s: %s',
      sqlstate,
      sqlerrm
    );
  end;
end
$reader_denials$;
reset role;

select ash.revoke_reader('ash_issue_221_reader');
drop role ash_issue_221_reader;
select ash.stop();
do $$
begin
  raise notice 'Issue #221 CALL-able procedure tests PASSED';
end $$;
