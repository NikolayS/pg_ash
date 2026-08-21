/* -------------------------------------------------------------------------
 * ash.grant_reader(), ash.revoke_reader(), and the default pg_monitor bundle
 * ------------------------------------------------------------------------- */
do $feature_reader_role_setup$
begin
  if not exists (
    select
    from pg_catalog.pg_roles
    where rolname = 'ash_feature_reader'
  ) then
    create role ash_feature_reader nologin noinherit;
  else
    perform ash.revoke_reader('ash_feature_reader');
  end if;
end
$feature_reader_role_setup$;

do $feature_privileges$
declare
  v_admin_denied boolean := false;
  v_aas record;
  v_direct_functions bigint;
  v_reader_signatures text[];
  v_direct_tables bigint;
  v_fixture ash_feature_context%rowtype;
  v_read_denied boolean := false;
  v_status_version text;
begin
  select *
  into strict v_fixture
  from ash_feature_context;

  begin
    execute 'set local role ash_feature_reader';
    perform * from ash.status();
    execute 'reset role';
  exception
    when insufficient_privilege then
      execute 'reset role';
      v_read_denied := true;
  end;
  assert v_read_denied,
    format(
      '[%s] ash.grant_reader precondition: ungranted role unexpectedly read ash.status()',
      pg_catalog.current_setting('ash.feature_mode')
    );

  perform ash.grant_reader('ash_feature_reader');

  select pg_catalog.count(*)
  into v_direct_functions
  from pg_catalog.pg_proc as procedure_row
  inner join pg_catalog.pg_namespace as namespace_row
    on namespace_row.oid = procedure_row.pronamespace
  cross join lateral pg_catalog.aclexplode(procedure_row.proacl) as acl
  where
    namespace_row.nspname = 'ash'
    and acl.grantee = (
      select role_row.oid
      from pg_catalog.pg_roles as role_row
      where role_row.rolname = 'ash_feature_reader'
    )
    and acl.privilege_type = 'EXECUTE';

  select pg_catalog.count(*)
  into v_direct_tables
  from pg_catalog.pg_class as relation_row
  inner join pg_catalog.pg_namespace as namespace_row
    on namespace_row.oid = relation_row.relnamespace
  cross join lateral pg_catalog.aclexplode(relation_row.relacl) as acl
  where
    namespace_row.nspname = 'ash'
    and acl.grantee = (
      select role_row.oid
      from pg_catalog.pg_roles as role_row
      where role_row.rolname = 'ash_feature_reader'
    )
    and acl.privilege_type = 'SELECT';


  /*
   * A bare count tells you the bundle changed size, not what changed. Pin the
   * exact set of signatures a reader receives, so adding a function to the
   * reader surface — or accidentally dropping one out of ash._admin_funcs() —
   * names itself in the failure instead of showing up as an off-by-one that
   * the next person "fixes" by bumping the number.
   */
  select pg_catalog.array_agg(
           granted.signature order by granted.signature
         )
  into v_reader_signatures
  from (
    select proc.proname || '('
             || pg_catalog.pg_get_function_identity_arguments(proc.oid)
             || ')' as signature
    from pg_catalog.pg_proc as proc
    join pg_catalog.pg_namespace as nsp on nsp.oid = proc.pronamespace
    cross join lateral pg_catalog.aclexplode(proc.proacl) as acl
    join pg_catalog.pg_roles as grantee_role
      on grantee_role.oid = acl.grantee
    where nsp.nspname = 'ash'
      and grantee_role.rolname = 'ash_feature_reader'
      and acl.privilege_type = 'EXECUTE'
  ) as granted;

  assert v_reader_signatures = array[
    '_active_slots()',
    '_active_slots_for(lookback interval)',
    '_active_slots_for_at(since timestamp with time zone, until timestamp with time zone)',
    '_bar(event text, pct numeric, max_pct numeric, width integer, color boolean)',
    '_color_on(color boolean)',
    '_exact_query_uses_coarser(start_ts integer, end_ts integer, database name)',
    '_grain_by(start_ts integer, end_ts integer, source text, dimension text, wait_event_type text, wait_event text, query_id bigint, database name)',
    '_grain_counts(start_ts integer, end_ts integer, source text, wait_event_type text, wait_event text, query_id bigint, database name)',
    '_hr_top_events(type text, minutes integer[], n integer, si numeric)',
    '_hr_top_queryids(type text, minutes integer[], n integer, si numeric)',
    '_in_recovery()',
    '_minute_counts_valid(minute_counts integer[], wait_counts integer[])',
    '_pg_cron_available()',
    '_pgss_query_text(query_id bigint, maxlen integer)',
    '_pgss_schema()',
    '_pick_source(since timestamp with time zone)',
    '_pick_source_agg(since timestamp with time zone, until timestamp with time zone)',
    '_raise_tie_retention(raw_start timestamp with time zone, start_ts integer, end_ts integer, since timestamp with time zone)',
    '_raw_oldest_sample()',
    '_raw_retention_start()',
    '_raw_ring_readable()',
    '_require_raw_ring(what text)',
    '_reset(color boolean)',
    '_rollup_1h_has_flat(start_ts integer, end_ts integer, database name)',
    '_rollup_1h_retention_start()',
    '_rollup_1m_retention_start()',
    '_sample_data_is_valid(data integer[])',
    '_sample_interval_secs()',
    '_samples(since timestamp with time zone, until timestamp with time zone, n integer, wait_event_type text, wait_event_filter text, query_id_filter bigint, database name)',
    '_wait_color(event text, color boolean)',
    'aas(since timestamp with time zone, until timestamp with time zone, wait_event_type text, wait_event text, query_id bigint, database name, bucket interval)',
    'chart(since timestamp with time zone, until timestamp with time zone, bucket interval, n integer, width integer, color boolean)',
    'compare(since_1 timestamp with time zone, until_1 timestamp with time zone, since_2 timestamp with time zone, until_2 timestamp with time zone, dimension text, n integer, wait_event_type text, wait_event text, query_id bigint, database name, bucket interval)',
    'current_slot()',
    'decode_sample(data integer[], slot smallint)',
    'decode_sample(sample_ts integer)',
    'decode_sample_at(ts timestamp with time zone)',
    'epoch()',
    'periods(until timestamp with time zone)',
    'report(since timestamp with time zone, until timestamp with time zone, vcpus integer, n integer)',
    'samples(since timestamp with time zone, until timestamp with time zone, n integer, wait_event_type text, wait_event text, query_id bigint, database name)',
    'status()',
    'summary(since timestamp with time zone, until timestamp with time zone)',
    'timeline(since timestamp with time zone, until timestamp with time zone, bucket interval, wait_event_type text, wait_event text, query_id bigint, database name)',
    'top(dimension text, since timestamp with time zone, until timestamp with time zone, wait_event_type text, wait_event text, query_id bigint, database name, n integer, bucket interval, order_by text)',
    'ts_from_timestamptz(ts timestamp with time zone)',
    'ts_to_timestamptz(ts integer)'
  ]::text[],
    format(
      '[%s] ash.grant_reader bundle membership changed; unexpected: %s; missing: %s',
      pg_catalog.current_setting('ash.feature_mode'),
      (select pg_catalog.array_agg(sig)
       from unnest(v_reader_signatures) as sig
       where sig <> all (array[
    '_active_slots()',
    '_active_slots_for(lookback interval)',
    '_active_slots_for_at(since timestamp with time zone, until timestamp with time zone)',
    '_bar(event text, pct numeric, max_pct numeric, width integer, color boolean)',
    '_color_on(color boolean)',
    '_exact_query_uses_coarser(start_ts integer, end_ts integer, database name)',
    '_grain_by(start_ts integer, end_ts integer, source text, dimension text, wait_event_type text, wait_event text, query_id bigint, database name)',
    '_grain_counts(start_ts integer, end_ts integer, source text, wait_event_type text, wait_event text, query_id bigint, database name)',
    '_hr_top_events(type text, minutes integer[], n integer, si numeric)',
    '_hr_top_queryids(type text, minutes integer[], n integer, si numeric)',
    '_in_recovery()',
    '_minute_counts_valid(minute_counts integer[], wait_counts integer[])',
    '_pg_cron_available()',
    '_pgss_query_text(query_id bigint, maxlen integer)',
    '_pgss_schema()',
    '_pick_source(since timestamp with time zone)',
    '_pick_source_agg(since timestamp with time zone, until timestamp with time zone)',
    '_raise_tie_retention(raw_start timestamp with time zone, start_ts integer, end_ts integer, since timestamp with time zone)',
    '_raw_oldest_sample()',
    '_raw_retention_start()',
    '_raw_ring_readable()',
    '_require_raw_ring(what text)',
    '_reset(color boolean)',
    '_rollup_1h_has_flat(start_ts integer, end_ts integer, database name)',
    '_rollup_1h_retention_start()',
    '_rollup_1m_retention_start()',
    '_sample_data_is_valid(data integer[])',
    '_sample_interval_secs()',
    '_samples(since timestamp with time zone, until timestamp with time zone, n integer, wait_event_type text, wait_event_filter text, query_id_filter bigint, database name)',
    '_wait_color(event text, color boolean)',
    'aas(since timestamp with time zone, until timestamp with time zone, wait_event_type text, wait_event text, query_id bigint, database name, bucket interval)',
    'chart(since timestamp with time zone, until timestamp with time zone, bucket interval, n integer, width integer, color boolean)',
    'compare(since_1 timestamp with time zone, until_1 timestamp with time zone, since_2 timestamp with time zone, until_2 timestamp with time zone, dimension text, n integer, wait_event_type text, wait_event text, query_id bigint, database name, bucket interval)',
    'current_slot()',
    'decode_sample(data integer[], slot smallint)',
    'decode_sample(sample_ts integer)',
    'decode_sample_at(ts timestamp with time zone)',
    'epoch()',
    'periods(until timestamp with time zone)',
    'report(since timestamp with time zone, until timestamp with time zone, vcpus integer, n integer)',
    'samples(since timestamp with time zone, until timestamp with time zone, n integer, wait_event_type text, wait_event text, query_id bigint, database name)',
    'status()',
    'summary(since timestamp with time zone, until timestamp with time zone)',
    'timeline(since timestamp with time zone, until timestamp with time zone, bucket interval, wait_event_type text, wait_event text, query_id bigint, database name)',
    'top(dimension text, since timestamp with time zone, until timestamp with time zone, wait_event_type text, wait_event text, query_id bigint, database name, n integer, bucket interval, order_by text)',
    'ts_from_timestamptz(ts timestamp with time zone)',
    'ts_to_timestamptz(ts integer)'
      ]::text[])),
      (select pg_catalog.array_agg(sig)
       from unnest(array[
    '_active_slots()',
    '_active_slots_for(lookback interval)',
    '_active_slots_for_at(since timestamp with time zone, until timestamp with time zone)',
    '_bar(event text, pct numeric, max_pct numeric, width integer, color boolean)',
    '_color_on(color boolean)',
    '_exact_query_uses_coarser(start_ts integer, end_ts integer, database name)',
    '_grain_by(start_ts integer, end_ts integer, source text, dimension text, wait_event_type text, wait_event text, query_id bigint, database name)',
    '_grain_counts(start_ts integer, end_ts integer, source text, wait_event_type text, wait_event text, query_id bigint, database name)',
    '_hr_top_events(type text, minutes integer[], n integer, si numeric)',
    '_hr_top_queryids(type text, minutes integer[], n integer, si numeric)',
    '_in_recovery()',
    '_minute_counts_valid(minute_counts integer[], wait_counts integer[])',
    '_pg_cron_available()',
    '_pgss_query_text(query_id bigint, maxlen integer)',
    '_pgss_schema()',
    '_pick_source(since timestamp with time zone)',
    '_pick_source_agg(since timestamp with time zone, until timestamp with time zone)',
    '_raise_tie_retention(raw_start timestamp with time zone, start_ts integer, end_ts integer, since timestamp with time zone)',
    '_raw_oldest_sample()',
    '_raw_retention_start()',
    '_raw_ring_readable()',
    '_require_raw_ring(what text)',
    '_reset(color boolean)',
    '_rollup_1h_has_flat(start_ts integer, end_ts integer, database name)',
    '_rollup_1h_retention_start()',
    '_rollup_1m_retention_start()',
    '_sample_data_is_valid(data integer[])',
    '_sample_interval_secs()',
    '_samples(since timestamp with time zone, until timestamp with time zone, n integer, wait_event_type text, wait_event_filter text, query_id_filter bigint, database name)',
    '_wait_color(event text, color boolean)',
    'aas(since timestamp with time zone, until timestamp with time zone, wait_event_type text, wait_event text, query_id bigint, database name, bucket interval)',
    'chart(since timestamp with time zone, until timestamp with time zone, bucket interval, n integer, width integer, color boolean)',
    'compare(since_1 timestamp with time zone, until_1 timestamp with time zone, since_2 timestamp with time zone, until_2 timestamp with time zone, dimension text, n integer, wait_event_type text, wait_event text, query_id bigint, database name, bucket interval)',
    'current_slot()',
    'decode_sample(data integer[], slot smallint)',
    'decode_sample(sample_ts integer)',
    'decode_sample_at(ts timestamp with time zone)',
    'epoch()',
    'periods(until timestamp with time zone)',
    'report(since timestamp with time zone, until timestamp with time zone, vcpus integer, n integer)',
    'samples(since timestamp with time zone, until timestamp with time zone, n integer, wait_event_type text, wait_event text, query_id bigint, database name)',
    'status()',
    'summary(since timestamp with time zone, until timestamp with time zone)',
    'timeline(since timestamp with time zone, until timestamp with time zone, bucket interval, wait_event_type text, wait_event text, query_id bigint, database name)',
    'top(dimension text, since timestamp with time zone, until timestamp with time zone, wait_event_type text, wait_event text, query_id bigint, database name, n integer, bucket interval, order_by text)',
    'ts_from_timestamptz(ts timestamp with time zone)',
    'ts_to_timestamptz(ts integer)'
      ]::text[]) as sig
       where sig <> all (v_reader_signatures))
    );
  assert pg_catalog.has_schema_privilege(
      'ash_feature_reader',
      'ash',
      'USAGE'
    )
    and v_direct_functions = 47
    and v_direct_tables = 12,
    format(
      '[%s] ash.grant_reader ACLs: expected schema USAGE, 47 direct function EXECUTEs, and 12 direct table SELECTs; got usage=%s functions=%s tables=%s',
      pg_catalog.current_setting('ash.feature_mode'),
      pg_catalog.has_schema_privilege(
        'ash_feature_reader',
        'ash',
        'USAGE'
      ),
      v_direct_functions,
      v_direct_tables
    );
  assert pg_catalog.has_function_privilege(
      'ash_feature_reader',
      'ash._exact_query_uses_coarser(integer,integer,name)',
      'EXECUTE'
    ),
    format(
      '[%s] ash.grant_reader helper bundle: _exact_query_uses_coarser() is not executable',
      pg_catalog.current_setting('ash.feature_mode')
    );
  assert not pg_catalog.has_function_privilege(
      'ash_feature_reader',
      'ash.start(interval)',
      'EXECUTE'
    )
    and not pg_catalog.has_function_privilege(
      'ash_feature_reader',
      'ash.take_sample()',
      'EXECUTE'
    )
    and not pg_catalog.has_function_privilege(
      'ash_feature_reader',
      'ash.rebuild_partitions(integer,text)',
      'EXECUTE'
    )
    and not pg_catalog.has_function_privilege(
      'ash_feature_reader',
      'ash._admin_funcs()',
      'EXECUTE'
    ),
    format(
      '[%s] ash.grant_reader least privilege: reader acquired an admin EXECUTE grant',
      pg_catalog.current_setting('ash.feature_mode')
    );

  execute 'set local role ash_feature_reader';
  select *
  into strict v_aas
  from ash.aas(v_fixture.fixture_start, v_fixture.fixture_end);
  select value
  into strict v_status_version
  from ash.status()
  where metric = 'version';
  perform pg_catalog.count(*) from ash.sample;
  perform pg_catalog.count(*) from ash.rollup_1m;
  begin
    perform * from ash.start(interval '2 seconds');
  exception
    when insufficient_privilege then
      v_admin_denied := true;
  end;
  execute 'reset role';

  assert v_aas.avg_aas = 4.00
    and v_aas.peak_aas = 5.00
    and v_aas.p99_aas = 4.97
    and v_status_version is not null,
    format(
      '[%s] ash.grant_reader actual read: expected exact AAS 4.00/5.00/4.97 and readable status, got aas=%s version=%s',
      pg_catalog.current_setting('ash.feature_mode'),
      pg_catalog.row_to_json(v_aas),
      v_status_version
    );
  assert v_admin_denied,
    format(
      '[%s] ash.grant_reader actual least privilege: role unexpectedly called ash.start()',
      pg_catalog.current_setting('ash.feature_mode')
    );

  perform ash.revoke_reader('ash_feature_reader');

  select pg_catalog.count(*)
  into v_direct_functions
  from pg_catalog.pg_proc as procedure_row
  inner join pg_catalog.pg_namespace as namespace_row
    on namespace_row.oid = procedure_row.pronamespace
  cross join lateral pg_catalog.aclexplode(procedure_row.proacl) as acl
  where
    namespace_row.nspname = 'ash'
    and acl.grantee = (
      select role_row.oid
      from pg_catalog.pg_roles as role_row
      where role_row.rolname = 'ash_feature_reader'
    )
    and acl.privilege_type = 'EXECUTE';

  select pg_catalog.count(*)
  into v_direct_tables
  from pg_catalog.pg_class as relation_row
  inner join pg_catalog.pg_namespace as namespace_row
    on namespace_row.oid = relation_row.relnamespace
  cross join lateral pg_catalog.aclexplode(relation_row.relacl) as acl
  where
    namespace_row.nspname = 'ash'
    and acl.grantee = (
      select role_row.oid
      from pg_catalog.pg_roles as role_row
      where role_row.rolname = 'ash_feature_reader'
    )
    and acl.privilege_type = 'SELECT';

  assert not pg_catalog.has_schema_privilege(
      'ash_feature_reader',
      'ash',
      'USAGE'
    )
    and v_direct_functions = 0
    and v_direct_tables = 0,
    format(
      '[%s] ash.revoke_reader ACLs: expected no schema USAGE/direct EXECUTE/direct SELECT, got usage=%s functions=%s tables=%s',
      pg_catalog.current_setting('ash.feature_mode'),
      pg_catalog.has_schema_privilege(
        'ash_feature_reader',
        'ash',
        'USAGE'
      ),
      v_direct_functions,
      v_direct_tables
    );

  v_read_denied := false;
  execute 'set local role ash_feature_reader';
  begin
    perform * from ash.status();
  exception
    when insufficient_privilege then
      v_read_denied := true;
  end;
  execute 'reset role';
  assert v_read_denied,
    format(
      '[%s] ash.revoke_reader actual read: revoked role unexpectedly read ash.status()',
      pg_catalog.current_setting('ash.feature_mode')
    );

  /*
   * Leave this role granted for rebuild_partitions() to prove that the
   * destructive rebuild preserves the real bundle on replacement children.
   */
  perform ash.grant_reader('ash_feature_reader');
end
$feature_privileges$;

do $feature_revoke_protected$
declare
  v_acl_after jsonb;
  v_acl_before jsonb;
  v_refused boolean := false;
begin
  select pg_catalog.jsonb_build_object(
    'schema',
    (
      select pg_catalog.to_jsonb(namespace_row.nspacl)
      from pg_catalog.pg_namespace as namespace_row
      where namespace_row.nspname = 'ash'
    ),
    'functions',
    (
      select pg_catalog.jsonb_object_agg(
        procedure_row.oid::text,
        pg_catalog.to_jsonb(procedure_row.proacl)
        order by procedure_row.oid
      )
      from pg_catalog.pg_proc as procedure_row
      inner join pg_catalog.pg_namespace as namespace_row
        on namespace_row.oid = procedure_row.pronamespace
      where namespace_row.nspname = 'ash'
    ),
    'relations',
    (
      select pg_catalog.jsonb_object_agg(
        relation_row.oid::text,
        pg_catalog.to_jsonb(relation_row.relacl)
        order by relation_row.oid
      )
      from pg_catalog.pg_class as relation_row
      inner join pg_catalog.pg_namespace as namespace_row
        on namespace_row.oid = relation_row.relnamespace
      where namespace_row.nspname = 'ash'
    )
  )
  into v_acl_before;

  begin
    perform ash.revoke_reader(current_user::name);
  exception
    when others then
      v_refused := sqlerrm = pg_catalog.format(
        'ash.revoke_reader: refusing protected role %s (schema owner, current user, or superuser)',
        current_user
      );
  end;

  select pg_catalog.jsonb_build_object(
    'schema',
    (
      select pg_catalog.to_jsonb(namespace_row.nspacl)
      from pg_catalog.pg_namespace as namespace_row
      where namespace_row.nspname = 'ash'
    ),
    'functions',
    (
      select pg_catalog.jsonb_object_agg(
        procedure_row.oid::text,
        pg_catalog.to_jsonb(procedure_row.proacl)
        order by procedure_row.oid
      )
      from pg_catalog.pg_proc as procedure_row
      inner join pg_catalog.pg_namespace as namespace_row
        on namespace_row.oid = procedure_row.pronamespace
      where namespace_row.nspname = 'ash'
    ),
    'relations',
    (
      select pg_catalog.jsonb_object_agg(
        relation_row.oid::text,
        pg_catalog.to_jsonb(relation_row.relacl)
        order by relation_row.oid
      )
      from pg_catalog.pg_class as relation_row
      inner join pg_catalog.pg_namespace as namespace_row
        on namespace_row.oid = relation_row.relnamespace
      where namespace_row.nspname = 'ash'
    )
  )
  into v_acl_after;

  assert v_refused
    and v_acl_after = v_acl_before,
    format(
      '[%s] ash.revoke_reader protected target: expected exact refusal before any ACL mutation, got refused=%s acl_unchanged=%s',
      pg_catalog.current_setting('ash.feature_mode'),
      v_refused,
      v_acl_after = v_acl_before
    );
end
$feature_revoke_protected$;

do $feature_default_pg_monitor$
declare
  v_admin_denied boolean := false;
  v_aas record;
  v_direct_functions bigint;
  v_direct_tables bigint;
  v_fixture ash_feature_context%rowtype;
  v_version text;
begin
  select *
  into strict v_fixture
  from ash_feature_context;

  select pg_catalog.count(*)
  into v_direct_functions
  from pg_catalog.pg_proc as procedure_row
  inner join pg_catalog.pg_namespace as namespace_row
    on namespace_row.oid = procedure_row.pronamespace
  cross join lateral pg_catalog.aclexplode(procedure_row.proacl) as acl
  where
    namespace_row.nspname = 'ash'
    and acl.grantee = (
      select role_row.oid
      from pg_catalog.pg_roles as role_row
      where role_row.rolname = 'pg_monitor'
    )
    and acl.privilege_type = 'EXECUTE';

  select pg_catalog.count(*)
  into v_direct_tables
  from pg_catalog.pg_class as relation_row
  inner join pg_catalog.pg_namespace as namespace_row
    on namespace_row.oid = relation_row.relnamespace
  cross join lateral pg_catalog.aclexplode(relation_row.relacl) as acl
  where
    namespace_row.nspname = 'ash'
    and acl.grantee = (
      select role_row.oid
      from pg_catalog.pg_roles as role_row
      where role_row.rolname = 'pg_monitor'
    )
    and acl.privilege_type = 'SELECT';

  assert pg_catalog.has_schema_privilege('pg_monitor', 'ash', 'USAGE')
    and v_direct_functions = 47
    and v_direct_tables = 12
    and not pg_catalog.has_function_privilege(
      'pg_monitor',
      'ash.start(interval)',
      'EXECUTE'
    )
    and not pg_catalog.has_function_privilege(
      'pg_monitor',
      'ash._admin_funcs()',
      'EXECUTE'
    ),
    format(
      '[%s] default pg_monitor ACLs: expected USAGE/47 reader functions/12 tables/no start() or _admin_funcs(), got usage=%s functions=%s tables=%s start=%s admin_list=%s',
      pg_catalog.current_setting('ash.feature_mode'),
      pg_catalog.has_schema_privilege('pg_monitor', 'ash', 'USAGE'),
      v_direct_functions,
      v_direct_tables,
      pg_catalog.has_function_privilege(
        'pg_monitor',
        'ash.start(interval)',
        'EXECUTE'
      ),
      pg_catalog.has_function_privilege(
        'pg_monitor',
        'ash._admin_funcs()',
        'EXECUTE'
      )
    );
  assert pg_catalog.has_function_privilege(
      'pg_monitor',
      'ash._exact_query_uses_coarser(integer,integer,name)',
      'EXECUTE'
    ),
    format(
      '[%s] default pg_monitor helper bundle: _exact_query_uses_coarser() is not executable',
      pg_catalog.current_setting('ash.feature_mode')
    );

  execute 'set local role pg_monitor';
  select *
  into strict v_aas
  from ash.aas(v_fixture.fixture_start, v_fixture.fixture_end);
  select value
  into strict v_version
  from ash.status()
  where metric = 'version';
  begin
    perform * from ash.start(interval '2 seconds');
  exception
    when insufficient_privilege then
      v_admin_denied := true;
  end;
  execute 'reset role';

  assert v_aas.avg_aas = 4.00
    and v_aas.peak_aas = 5.00
    and v_aas.p99_aas = 4.97
    and v_version is not null,
    format(
      '[%s] default pg_monitor actual read: expected exact AAS and status success, got aas=%s version=%s',
      pg_catalog.current_setting('ash.feature_mode'),
      pg_catalog.row_to_json(v_aas),
      v_version
    );
  assert v_admin_denied,
    format(
      '[%s] default pg_monitor actual least privilege: role unexpectedly called ash.start()',
      pg_catalog.current_setting('ash.feature_mode')
    );
end
$feature_default_pg_monitor$;
