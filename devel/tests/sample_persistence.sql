\set ON_ERROR_STOP on

/*
 * Issue #224: optional UNLOGGED persistence for the raw sample ring.
 * This file owns the full behavioral surface so test.yml needs only one
 * short psql --file step.
 */

do $fresh_state$
declare
  v_logged int;
  v_num_partitions int;
  v_total int;
  v_unlogged int;
begin
  select num_partitions
  into v_num_partitions
  from ash.config
  where singleton;

  select
    count(*),
    count(*) filter (where rel.relpersistence = 'p'),
    count(*) filter (where rel.relpersistence = 'u')
  into v_total, v_logged, v_unlogged
  from pg_catalog.pg_inherits as inh
  join pg_catalog.pg_class as rel on rel.oid = inh.inhrelid
  join pg_catalog.pg_namespace as nsp on nsp.oid = rel.relnamespace
  where inh.inhparent = 'ash.sample'::pg_catalog.regclass
    and nsp.nspname = 'ash'
    and rel.relname ~ '^sample_[0-9]+$';

  assert v_total = v_num_partitions,
    format(
      '#224 fresh ring: expected %s numeric partitions, got %s',
      v_num_partitions,
      v_total
    );
  assert v_logged = v_num_partitions,
    format(
      '#224 fresh ring: expected %s logged partitions, got %s',
      v_num_partitions,
      v_logged
    );
  assert v_unlogged = 0,
    format('#224 fresh ring: expected 0 unlogged partitions, got %s',
      v_unlogged);
  assert (
    select sample_unlogged from ash.config where singleton
  ) = false,
    '#224 fresh config: sample_unlogged must default to false';
  assert (
    select value from ash.status() where metric = 'sample_unlogged'
  ) = 'false',
    '#224 fresh status: sample_unlogged must report false';
  assert 'set_sample_persistence' = any(ash._admin_funcs()),
    '#224 admin catalog: set_sample_persistence is missing';
  assert '_apply_sample_persistence' = any(ash._admin_funcs()),
    '#224 admin catalog: _apply_sample_persistence is missing';
  assert not pg_catalog.has_function_privilege(
    'public',
    'ash.set_sample_persistence(text)',
    'EXECUTE'
  ), '#224 privileges: PUBLIC can execute set_sample_persistence';
  assert not pg_catalog.has_function_privilege(
    'public',
    'ash._apply_sample_persistence()',
    'EXECUTE'
  ), '#224 privileges: PUBLIC can execute _apply_sample_persistence';
end
$fresh_state$;

do $set_unlogged$
declare
  v_expected text;
  v_n int;
  v_result text;
  v_unlogged int;
begin
  select num_partitions into v_n from ash.config where singleton;
  v_expected := format(
    'sample persistence: unlogged; %s partitions converted. Converting '
    'populated partitions rewrites them; in unlogged mode the raw sample '
    'ring will be empty after a crash or immediate shutdown.',
    v_n
  );

  select ash.set_sample_persistence('  UnLoGgEd  ') into v_result;
  assert v_result = v_expected,
    format('#224 set unlogged: expected %L, got %L', v_expected, v_result);

  select count(*)
  into v_unlogged
  from pg_catalog.pg_inherits as inh
  join pg_catalog.pg_class as rel on rel.oid = inh.inhrelid
  join pg_catalog.pg_namespace as nsp on nsp.oid = rel.relnamespace
  where inh.inhparent = 'ash.sample'::pg_catalog.regclass
    and nsp.nspname = 'ash'
    and rel.relname ~ '^sample_[0-9]+$'
    and rel.relpersistence = 'u';

  assert v_unlogged = v_n,
    format(
      '#224 set unlogged: expected %s unlogged partitions, got %s',
      v_n,
      v_unlogged
    );
  assert (
    select relpersistence
    from pg_catalog.pg_class
    where oid = 'ash.sample'::pg_catalog.regclass
  ) = 'p', '#224 set unlogged: ash.sample parent changed persistence';
  assert (
    select relpersistence
    from pg_catalog.pg_class
    where oid = 'ash.rollup_1m'::pg_catalog.regclass
  ) = 'p', '#224 set unlogged: ash.rollup_1m changed persistence';
  assert (
    select relpersistence
    from pg_catalog.pg_class
    where oid = 'ash.rollup_1h'::pg_catalog.regclass
  ) = 'p', '#224 set unlogged: ash.rollup_1h changed persistence';
  assert (
    select value from ash.status() where metric = 'sample_unlogged'
  ) = 'true', '#224 unlogged status: expected true';
end
$set_unlogged$;

do $rotation$
declare
  v_n int;
  v_result text;
  v_unlogged int;
begin
  select num_partitions into v_n from ash.config where singleton;

  for i in 1 .. v_n loop
    update ash.config
    set rotated_at = pg_catalog.clock_timestamp() - rotation_period
    where singleton;

    select ash.rotate() into v_result;
    assert v_result like 'rotated:%',
      format('#224 rotation %s/%s failed: %s', i, v_n, v_result);
  end loop;

  select count(*)
  into v_unlogged
  from pg_catalog.pg_inherits as inh
  join pg_catalog.pg_class as rel on rel.oid = inh.inhrelid
  join pg_catalog.pg_namespace as nsp on nsp.oid = rel.relnamespace
  where inh.inhparent = 'ash.sample'::pg_catalog.regclass
    and nsp.nspname = 'ash'
    and rel.relname ~ '^sample_[0-9]+$'
    and rel.relpersistence = 'u';

  assert v_unlogged = v_n,
    format(
      '#224 rotation: expected all %s partitions unlogged, got %s',
      v_n,
      v_unlogged
    );
end
$rotation$;

do $unlogged_data$
declare
  v_active_total int;
  v_datid oid;
  v_decoded_count int;
  v_decoded_total int;
  v_io_count int;
  v_map_id int4;
  v_minute_ts int4;
  v_n int;
  v_null_query_count int;
  v_query_count int;
  v_query_id constant int8 := 224224;
  v_raw_count int;
  v_raw_timestamps int4[];
  v_rollup ash.rollup_1m%rowtype;
  v_rollup_result int;
  v_slot smallint;
  v_wait_id smallint;
begin
  truncate ash.sample;
  truncate ash.rollup_1m;
  truncate ash.rollup_1h;

  select num_partitions, current_slot
  into v_n, v_slot
  from ash.config
  where singleton;
  for i in 0 .. v_n - 1 loop
    execute format(
      'truncate ash.query_map_%s restart identity',
      i
    );
  end loop;

  update ash.config
  set last_rollup_1m_ts = null,
    last_rollup_1h_ts = null,
    rollup_min_backend_seconds = 3,
    sample_interval = interval '1 second'
  where singleton;

  select oid
  into v_datid
  from pg_catalog.pg_database
  where datname = pg_catalog.current_database();
  select ash._register_wait('active', 'IO', 'DataFileRead')
  into v_wait_id;
  execute format(
    'insert into ash.query_map_%s (query_id) values ($1) returning id',
    v_slot
  )
  into v_map_id
  using v_query_id;

  v_minute_ts := (
    ash.ts_from_timestamptz(
      pg_catalog.date_trunc(
        'minute',
        pg_catalog.now() - interval '2 minutes'
      )
    ) / 60
  ) * 60;

  /*
   * Take three deterministic raw samples. Each has two active backends on
   * the same wait: one attributed to query 224224 and one unattributed.
   */
  for i in 0 .. 2 loop
    insert into ash.sample (
      sample_ts,
      datid,
      active_count,
      data,
      slot
    )
    values (
      v_minute_ts + 5 + i * 20,
      v_datid,
      2,
      array[-v_wait_id, 2, v_map_id, 0]::int4[],
      v_slot
    );
  end loop;

  select
    count(*),
    sum(active_count),
    array_agg(sample_ts order by sample_ts)
  into v_raw_count, v_active_total, v_raw_timestamps
  from ash.sample;
  assert v_raw_count = 3,
    format('#224 unlogged raw: expected 3 rows, got %s', v_raw_count);
  assert v_active_total = 6,
    format('#224 unlogged raw: expected active total 6, got %s',
      v_active_total);
  assert v_raw_timestamps = array[
    v_minute_ts + 5,
    v_minute_ts + 25,
    v_minute_ts + 45
  ], format(
    '#224 unlogged raw: unexpected timestamps %s',
    v_raw_timestamps::text
  );

  select
    count(*),
    sum(decoded.count),
    count(*) filter (where decoded.wait_event = 'IO:DataFileRead'),
    count(*) filter (where decoded.query_id = v_query_id),
    count(*) filter (where decoded.query_id is null)
  into
    v_decoded_count,
    v_decoded_total,
    v_io_count,
    v_query_count,
    v_null_query_count
  from ash.decode_sample(v_minute_ts + 5) as decoded;
  assert v_decoded_count = 2 and v_decoded_total = 2,
    format(
      '#224 decode: expected 2 rows/count, got rows=%s count=%s',
      v_decoded_count,
      v_decoded_total
    );
  assert v_io_count = 2,
    format('#224 decode: expected 2 IO:DataFileRead rows, got %s',
      v_io_count);
  assert v_query_count = 1 and v_null_query_count = 1,
    format(
      '#224 decode: expected query/null counts 1/1, got %s/%s',
      v_query_count,
      v_null_query_count
    );

  select ash.rollup_minute() into v_rollup_result;
  assert v_rollup_result = 2,
    format(
      '#224 unlogged rollup: expected 2 processed minutes, got %s',
      v_rollup_result
    );
  select *
  into strict v_rollup
  from ash.rollup_1m
  where ts = v_minute_ts
    and datid = v_datid;
  assert v_rollup.samples = 3,
    format('#224 rollup samples: expected 3, got %s', v_rollup.samples);
  assert v_rollup.peak_backends = 2,
    format('#224 rollup peak: expected 2, got %s',
      v_rollup.peak_backends);
  assert v_rollup.wait_counts = array[v_wait_id::int4, 6],
    format('#224 rollup waits: expected {%s,6}, got %s',
      v_wait_id, v_rollup.wait_counts::text);
  assert v_rollup.query_counts = array[v_query_id, 3]::int8[],
    format('#224 rollup queries: expected {%s,3}, got %s',
      v_query_id, v_rollup.query_counts::text);
end
$unlogged_data$;

do $rebuilds$
declare
  v_total int;
  v_unlogged int;
begin
  perform ash.rebuild_partitions(5, 'yes');

  select
    count(*),
    count(*) filter (where rel.relpersistence = 'u')
  into v_total, v_unlogged
  from pg_catalog.pg_inherits as inh
  join pg_catalog.pg_class as rel on rel.oid = inh.inhrelid
  join pg_catalog.pg_namespace as nsp on nsp.oid = rel.relnamespace
  where inh.inhparent = 'ash.sample'::pg_catalog.regclass
    and nsp.nspname = 'ash'
    and rel.relname ~ '^sample_[0-9]+$';
  assert v_total = 5 and v_unlogged = 5,
    format(
      '#224 larger rebuild: expected total/unlogged 5/5, got %s/%s',
      v_total,
      v_unlogged
    );

  perform ash.rebuild_partitions(3, 'yes');

  select
    count(*),
    count(*) filter (where rel.relpersistence = 'u')
  into v_total, v_unlogged
  from pg_catalog.pg_inherits as inh
  join pg_catalog.pg_class as rel on rel.oid = inh.inhrelid
  join pg_catalog.pg_namespace as nsp on nsp.oid = rel.relnamespace
  where inh.inhparent = 'ash.sample'::pg_catalog.regclass
    and nsp.nspname = 'ash'
    and rel.relname ~ '^sample_[0-9]+$';
  assert v_total = 3 and v_unlogged = 3,
    format(
      '#224 smaller rebuild: expected total/unlogged 3/3, got %s/%s',
      v_total,
      v_unlogged
    );
  assert ash._apply_sample_persistence() = 0,
    '#224 reconcile helper: matching unlogged ring must be a no-op';
  assert (
    select sample_unlogged from ash.config where singleton
  ) = true, '#224 reconcile helper changed sample_unlogged';
end
$rebuilds$;

do $logged_and_negative$
declare
  v_before bool;
  v_expected text;
  v_message text;
  v_result text;
  v_sqlstate text;
begin
  v_expected :=
    'sample persistence: logged; 3 partitions converted. Converting '
    'populated partitions rewrites them; in unlogged mode the raw sample '
    'ring will be empty after a crash or immediate shutdown.';
  select ash.set_sample_persistence('logged') into v_result;
  assert v_result = v_expected,
    format('#224 set logged: expected %L, got %L', v_expected, v_result);
  assert (
    select count(*)
    from pg_catalog.pg_inherits as inh
    join pg_catalog.pg_class as rel on rel.oid = inh.inhrelid
    where inh.inhparent = 'ash.sample'::pg_catalog.regclass
      and rel.relpersistence = 'p'
  ) = 3, '#224 set logged: expected all 3 partitions logged';
  assert (
    select value from ash.status() where metric = 'sample_unlogged'
  ) = 'false', '#224 logged status: expected false';

  v_expected :=
    'sample persistence: logged; 0 partitions converted. Converting '
    'populated partitions rewrites them; in unlogged mode the raw sample '
    'ring will be empty after a crash or immediate shutdown.';
  select ash.set_sample_persistence('logged') into v_result;
  assert v_result = v_expected,
    format('#224 first logged no-op: expected %L, got %L',
      v_expected, v_result);
  select ash.set_sample_persistence('LOGGED') into v_result;
  assert v_result = v_expected,
    format('#224 second logged no-op: expected %L, got %L',
      v_expected, v_result);

  select sample_unlogged into v_before from ash.config where singleton;
  begin
    perform ash.set_sample_persistence('bogus');
  exception when others then
    get stacked diagnostics
      v_sqlstate = returned_sqlstate,
      v_message = message_text;
  end;
  assert v_sqlstate = '22023',
    format('#224 invalid mode: expected SQLSTATE 22023, got %s',
      coalesce(v_sqlstate, '<no exception>'));
  assert v_message =
    'ash.set_sample_persistence: mode must be logged or '
    'unlogged; got ''bogus''',
    format('#224 invalid mode: unexpected message %L', v_message);
  assert (
    select sample_unlogged from ash.config where singleton
  ) = v_before, '#224 invalid mode changed sample_unlogged';
end
$logged_and_negative$;

/* The explicit operator action must reuse the #222 recovery guard. */
begin;
create or replace function ash._in_recovery()
returns bool
language sql
stable
parallel safe
set search_path = pg_catalog
as $$
  select true
$$;

do $recovery_guard$
declare
  v_before bool;
  v_sqlstate text;
begin
  select sample_unlogged into v_before from ash.config where singleton;
  begin
    perform ash.set_sample_persistence('unlogged');
  exception when others then
    get stacked diagnostics v_sqlstate = returned_sqlstate;
  end;
  assert v_sqlstate = '25006',
    format('#224 recovery guard: expected SQLSTATE 25006, got %s',
      coalesce(v_sqlstate, '<no exception>'));
  assert (
    select sample_unlogged from ash.config where singleton
  ) = v_before, '#224 recovery guard changed sample_unlogged';
end
$recovery_guard$;
rollback;

do $before_reapply$
declare
  v_result text;
begin
  select ash.set_sample_persistence('unlogged') into v_result;
  assert v_result like
    'sample persistence: unlogged; 3 partitions converted.%',
    format('#224 re-apply setup: unexpected result %L', v_result);
  assert ash._apply_sample_persistence() = 0,
    '#224 pre-reapply helper: matching ring must be a no-op';
  assert (
    select sample_unlogged from ash.config where singleton
  ) = true, '#224 pre-reapply helper changed sample_unlogged';
end
$before_reapply$;

/*
 * True installer re-apply: the operator's unlogged choice must survive.
 * The path comes from the caller as :install_path rather than being written
 * here, because which file is the current installer depends on where the
 * release cycle stands — devel/sql/ash-install.sql during development,
 * sql/ash-install.sql once a release stamp has promoted it. The workflow
 * step passes ash_sql_chain.py fresh-install-path, the single source of
 * truth CI uses everywhere else.
 */
\i :install_path

do $after_reapply$
declare
  v_total int;
  v_unlogged int;
begin
  select
    count(*),
    count(*) filter (where rel.relpersistence = 'u')
  into v_total, v_unlogged
  from pg_catalog.pg_inherits as inh
  join pg_catalog.pg_class as rel on rel.oid = inh.inhrelid
  join pg_catalog.pg_namespace as nsp on nsp.oid = rel.relnamespace
  where inh.inhparent = 'ash.sample'::pg_catalog.regclass
    and nsp.nspname = 'ash'
    and rel.relname ~ '^sample_[0-9]+$';

  assert (
    select sample_unlogged from ash.config where singleton
  ) = true, '#224 installer re-apply reset sample_unlogged';
  assert v_total = 3 and v_unlogged = 3,
    format(
      '#224 installer re-apply: expected total/unlogged 3/3, got %s/%s',
      v_total,
      v_unlogged
    );
  assert ash._apply_sample_persistence() = 0,
    '#224 post-reapply helper: matching ring must be a no-op';
  assert (
    select value from ash.status() where metric = 'sample_unlogged'
  ) = 'true', '#224 post-reapply status: expected true';
end
$after_reapply$;

do $extra_negatives$
declare
  v_state text;
  v_msg text;
  v_before bool;
  v_converted int;
begin
  select sample_unlogged into v_before from ash.config where singleton;

  /*
   * NULL is a distinct branch from 'bogus': lower(btrim(null)) is NULL, so
   * `not in ('logged','unlogged')` alone evaluates to NULL, not false. The
   * `v_mode is null or` disjunct is what actually rejects it — without a test,
   * deleting it would let set_sample_persistence(null) fall through to a NULL
   * update while the suite stayed green.
   */
  begin
    perform ash.set_sample_persistence(null);
    raise exception '#224: set_sample_persistence(null) was accepted';
  exception when others then
    v_state := sqlstate;
    v_msg := sqlerrm;
  end;
  assert v_state = '22023', format(
    '#224: set_sample_persistence(null) expected 22023, got %s: %s',
    v_state, v_msg
  );
  assert (select sample_unlogged from ash.config where singleton) = v_before,
    '#224: set_sample_persistence(null) changed sample_unlogged';

  /*
   * Partial drift. Every other reconcile assertion is all-N or zero, so a
   * helper that only ever converted the whole ring — or nothing — would pass.
   * Drift exactly one partition and require exactly one conversion.
   */
  perform ash.set_sample_persistence('unlogged');
  alter table ash.sample_1 set logged;
  /*
   * Capture the count first. Calling the helper again inside format() would
   * run it a second time on the failure path — repairing the ring before the
   * message is built, so the assertion would always report 0 converted, and
   * would perform DDL while failing.
   */
  v_converted := ash._apply_sample_persistence();
  assert v_converted = 1, format(
    '#224: reconcile of a one-partition drift converted %s partitions, '
    'expected exactly 1',
    v_converted
  );
  assert (
    select count(*) = 0
    from pg_catalog.pg_inherits as inh
    join pg_catalog.pg_class as rel on rel.oid = inh.inhrelid
    where inh.inhparent = 'ash.sample'::pg_catalog.regclass
      and rel.relpersistence <> 'u'
  ), '#224: reconcile left a partition logged after repairing drift';

  /*
   * Only numeric children are touched: a decoy sibling in the ash schema
   * whose name is not sample_<N> must be left alone.
   */
  /*
   * The decoy has to be a REAL partition of ash.sample. A standalone table is
   * already excluded by the helper's inhparent predicate, so it would pass
   * even with the documented '^sample_[0-9]+$' name filter deleted — the very
   * claim this case is supposed to pin.
   */
  create table ash.sample_spare partition of ash.sample for values in (99);
  perform ash.set_sample_persistence('logged');
  assert (
    select relpersistence
    from pg_catalog.pg_class
    where oid = 'ash.sample_spare'::pg_catalog.regclass
  ) = 'p', '#224: reconcile altered a non-numeric partition decoy';
  perform ash.set_sample_persistence('unlogged');
  assert (
    select relpersistence
    from pg_catalog.pg_class
    where oid = 'ash.sample_spare'::pg_catalog.regclass
  ) = 'p', '#224: reconcile converted a partition whose name is not sample_<N>';
  drop table ash.sample_spare;

  raise notice '#224 negative and drift cases PASSED';
end
$extra_negatives$;

do $cleanup$
declare
  v_result text;
begin
  select ash.set_sample_persistence('logged') into v_result;
  assert v_result like
    'sample persistence: logged; 3 partitions converted.%',
    format('#224 final logged conversion: unexpected result %L', v_result);
  assert (
    select count(*)
    from pg_catalog.pg_inherits as inh
    join pg_catalog.pg_class as rel on rel.oid = inh.inhrelid
    where inh.inhparent = 'ash.sample'::pg_catalog.regclass
      and rel.relpersistence = 'p'
  ) = 3, '#224 cleanup: expected all 3 partitions logged';
  assert (
    select value from ash.status() where metric = 'sample_unlogged'
  ) = 'false', '#224 cleanup status: expected false';
  assert (
    select relpersistence
    from pg_catalog.pg_class
    where oid = 'ash.sample'::pg_catalog.regclass
  ) = 'p', '#224 cleanup: ash.sample parent is not permanent';
  assert (
    select relpersistence
    from pg_catalog.pg_class
    where oid = 'ash.rollup_1m'::pg_catalog.regclass
  ) = 'p', '#224 cleanup: ash.rollup_1m is not permanent';
  assert (
    select relpersistence
    from pg_catalog.pg_class
    where oid = 'ash.rollup_1h'::pg_catalog.regclass
  ) = 'p', '#224 cleanup: ash.rollup_1h is not permanent';

  truncate ash.sample;
  truncate ash.rollup_1m;
  truncate ash.rollup_1h;
  truncate ash.wait_event_map restart identity;
  for i in 0 .. 2 loop
    execute format(
      'truncate ash.query_map_%s restart identity',
      i
    );
  end loop;
  update ash.config
  set current_slot = 0,
    sampling_enabled = true,
    sample_interval = interval '1 second',
    include_bg_workers = false,
    rotated_at = pg_catalog.clock_timestamp(),
    last_rollup_1m_ts = null,
    last_rollup_1h_ts = null
  where singleton;

  raise notice 'Issue #224 sample persistence assertions PASSED';
end
$cleanup$;
