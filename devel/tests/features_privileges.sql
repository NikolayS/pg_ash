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

  assert pg_catalog.has_schema_privilege(
      'ash_feature_reader',
      'ash',
      'USAGE'
    )
    and v_direct_functions = 46
    and v_direct_tables = 12,
    format(
      '[%s] ash.grant_reader ACLs: expected schema USAGE, 46 direct function EXECUTEs, and 12 direct table SELECTs; got usage=%s functions=%s tables=%s',
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
    and v_direct_functions = 46
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
      '[%s] default pg_monitor ACLs: expected USAGE/46 reader functions/12 tables/no start() or _admin_funcs(), got usage=%s functions=%s tables=%s start=%s admin_list=%s',
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
