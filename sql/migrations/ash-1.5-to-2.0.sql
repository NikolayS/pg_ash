/*
 * pg_ash: upgrade from 1.5 to 2.0 beta 1
 *
 * 2.0 is a breaking release: the reader API is redesigned (issue #113,
 * blueprints/AAS_API.md). This upgrade wrapper replays the 2.0 beta 1
 * installer, which:
 *   * snapshots existing reader-role EXECUTE grants, then drops every removed
 *     v1.x reader and draft aas_* function (all overloads / _at twins), the
 *     changed-signature name `samples`, and every param-bearing function kept
 *     from 1.x (2.0 renames all parameters: the p_ prefix is dropped, e.g.
 *     p_from -> since, p_to -> until, p_limit -> n; CREATE OR REPLACE cannot
 *     rename input parameters) via the top-of-installer drop block, so
 *     the resulting schema equals a fresh 2.0 install (CI asserts this),
 *   * creates the 2.0 reader surface (periods, aas, timeline, top, compare,
 *     samples, report, chart, summary) with catalog comments and grants,
 *   * re-applies the snapshotted reader grants to the surviving/recreated
 *     functions, and re-runs ash.grant_reader() for every role that held the
 *     full pre-upgrade reader bundle — so a configured reader role can use
 *     the whole 2.0 surface (new readers AND their new internal helpers)
 *     without manual intervention; roles holding only partial manual grants
 *     are restored by exact signature and never widened,
 *   * grants the default reader bundle to pg_monitor (best-effort, new in
 *     2.0 — see the block at the end of the installer; opt out afterwards
 *     with `select ash.revoke_reader('pg_monitor')`), and
 *   * stamps ash.config.version = '2.0-beta1' (and the column default), and
 *   * normalizes ash.config's physical column order to match a fresh 2.0
 *     install while preserving its singleton row and catalog properties.
 *
 * Sampling and storage are unchanged. Other admin/lifecycle behavior and
 * rollup scheduling remain compatible; rollup_minute()/rollup_hour() correct
 * only their return values to count time grains instead of per-database rows.
 * Re-apply-safe: the installer is idempotent (CREATE OR REPLACE / IF NOT
 * EXISTS plus the deterministic drop block), and the config normalization
 * exits without replacing the table once the canonical order is present.
 */

\set ON_ERROR_STOP on
\ir ../ash-install.sql

begin;
set local search_path = pg_catalog, pg_temp;
set local default_tablespace = '';

/*
 * v1.1 added fields to ash.config after the original columns, and subsequent
 * upgrades kept appending fields. The values and definitions converged by
 * 2.0, but the physical attnum order did not. Normalize the upgraded table to
 * the fresh-install order so row types, SELECT * consumers, and schema
 * snapshots agree.
 *
 * The swap is atomic inside this normalization transaction. ACCESS EXCLUSIVE
 * serializes it with samplers and readers. DROP RESTRICT is deliberate:
 * an unexpected external dependency aborts and rolls back the migration
 * instead of being destroyed. Shipped pg_ash functions use string-bodied SQL
 * or PL/pgSQL and do not depend on ash.config's relation OID.
 *
 * Effective grants and grant options are preserved. Historical grantor OIDs
 * are not replayed because doing so would require assuming arbitrary roles;
 * grants are replayed by the migration owner instead.
 */
do $config_ordinal_normalization$
declare
  v_canonical_columns constant text[] := array[
    'singleton',
    'current_slot',
    'num_partitions',
    'sampling_enabled',
    'skipped_samples',
    'missed_samples',
    'sample_interval',
    'rotation_period',
    'include_bg_workers',
    'debug_logging',
    'encoding_version',
    'version',
    'rotated_at',
    'installed_at',
    'rollup_1m_retention_days',
    'rollup_1h_retention_days',
    'rollup_min_backend_seconds',
    'last_rollup_1m_ts',
    'last_rollup_1h_ts',
    'insert_errors',
    'register_wait_cap_hits',
    'consecutive_rotate_failures'
  ]::text[];
  v_canonical_types constant text[] := array[
    'boolean',
    'smallint',
    'smallint',
    'boolean',
    'integer',
    'bigint',
    'interval',
    'interval',
    'boolean',
    'boolean',
    'smallint',
    'text',
    'timestamp with time zone',
    'timestamp with time zone',
    'smallint',
    'smallint',
    'smallint',
    'integer',
    'integer',
    'bigint',
    'bigint',
    'bigint'
  ]::text[];
  v_canonical_not_null constant bool[] := array[
    true,
    true,
    true,
    true,
    true,
    true,
    true,
    true,
    true,
    true,
    true,
    true,
    true,
    true,
    true,
    true,
    true,
    false,
    false,
    true,
    true,
    true
  ]::bool[];
  v_actual_columns text[];
  v_actual_columns_sorted text[];
  v_actual_types text[];
  v_actual_not_null bool[];
  v_canonical_columns_sorted text[];
  v_live_column_count int;
  v_min_attnum int;
  v_max_attnum int;
  v_has_dropped_columns bool;
  v_relation_oid oid;
  v_heap_am_oid oid;
  v_owner_oid oid;
  v_owner_name text;
  v_has_not_null_constraints bool;
  v_row_count bigint;
  v_singleton_count bigint;
  v_relation record;
  v_default record;
  v_constraint record;
  v_index record;
  v_acl record;
  v_comment record;
  v_grantee_sql text;
  v_grant_option_sql text;
begin
  lock table ash.config in access exclusive mode;
  v_relation_oid := 'ash.config'::regclass;

  select
    array_agg(
      attribute.attname::text
      order by attribute.attnum
    ),
    array_agg(
      attribute.attname::text
      order by attribute.attname
    ),
    count(*)::int,
    min(attribute.attnum)::int,
    max(attribute.attnum)::int
  into
    v_actual_columns,
    v_actual_columns_sorted,
    v_live_column_count,
    v_min_attnum,
    v_max_attnum
  from pg_catalog.pg_attribute as attribute
  where
    attribute.attrelid = v_relation_oid
    and attribute.attnum > 0
    and not attribute.attisdropped;

  select array_agg(column_name order by column_name)
  into v_canonical_columns_sorted
  from unnest(v_canonical_columns) as canonical(column_name);

  select exists (
    select
    from pg_catalog.pg_attribute as attribute
    where
      attribute.attrelid = v_relation_oid
      and attribute.attnum > 0
      and attribute.attisdropped
  )
  into v_has_dropped_columns;

  if v_actual_columns_sorted is distinct from v_canonical_columns_sorted then
    raise exception
      'cannot normalize ash.config: expected columns %, found %',
      v_canonical_columns,
      v_actual_columns;
  end if;

  if v_actual_columns = v_canonical_columns
     and v_live_column_count = cardinality(v_canonical_columns)
     and v_min_attnum = 1
     and v_max_attnum = cardinality(v_canonical_columns)
     and not v_has_dropped_columns then
    raise notice 'ash.config column order is already canonical';
    return;
  end if;

  select
    array_agg(
      pg_catalog.format_type(
        attribute.atttypid,
        attribute.atttypmod
      )
      order by canonical.ordinality
    ),
    array_agg(
      attribute.attnotnull
      order by canonical.ordinality
    )
  into
    v_actual_types,
    v_actual_not_null
  from unnest(v_canonical_columns)
    with ordinality as canonical(column_name, ordinality)
  inner join pg_catalog.pg_attribute as attribute
    on attribute.attrelid = v_relation_oid
    and attribute.attname = canonical.column_name
    and attribute.attnum > 0
    and not attribute.attisdropped;

  if v_actual_types is distinct from v_canonical_types
     or v_actual_not_null is distinct from v_canonical_not_null then
    raise exception
      'cannot normalize ash.config: unsupported column definitions (types %, not-null %)',
      v_actual_types,
      v_actual_not_null;
  end if;

  select
    count(*),
    count(*) filter (where singleton)
  into
    v_row_count,
    v_singleton_count
  from ash.config;

  if v_row_count <> 1 or v_singleton_count <> 1 then
    raise exception
      'cannot normalize ash.config: expected one singleton row, found % row(s), % singleton',
      v_row_count,
      v_singleton_count;
  end if;

  if pg_catalog.to_regclass('ash.config_ordinal_legacy') is not null then
    raise exception
      'cannot normalize ash.config: ash.config_ordinal_legacy already exists';
  end if;

  select
    relation.relkind,
    relation.relpersistence,
    relation.relispartition,
    relation.relrowsecurity,
    relation.relforcerowsecurity,
    relation.relreplident,
    relation.reltablespace,
    relation.reloptions,
    relation.reloftype,
    relation.relam,
    relation.relowner
  into strict v_relation
  from pg_catalog.pg_class as relation
  where relation.oid = v_relation_oid;

  select access_method.oid
  into strict v_heap_am_oid
  from pg_catalog.pg_am as access_method
  where access_method.amname = 'heap';

  if v_relation.relkind <> 'r'
     or v_relation.relpersistence <> 'p'
     or v_relation.relispartition
     or v_relation.relrowsecurity
     or v_relation.relforcerowsecurity
     or v_relation.relreplident <> 'd'
     or v_relation.reltablespace <> 0
     or v_relation.reloptions is not null
     or v_relation.reloftype <> 0
     or v_relation.relam <> v_heap_am_oid then
    raise exception
      'cannot normalize ash.config: unsupported table storage, security, or replication properties';
  end if;

  if exists (
    select
    from pg_catalog.pg_attribute as attribute
    inner join pg_catalog.pg_type as data_type
      on data_type.oid = attribute.atttypid
    where
      attribute.attrelid = v_relation_oid
      and attribute.attnum > 0
      and not attribute.attisdropped
      and (
        attribute.attidentity <> ''
        or attribute.attgenerated <> ''
        or attribute.attstattarget <> -1
        or attribute.attstorage <> data_type.typstorage
        or attribute.attcompression <> ''
        or attribute.attoptions is not null
        or (
          data_type.typcollation <> 0
          and attribute.attcollation <> data_type.typcollation
        )
      )
  ) then
    raise exception
      'cannot normalize ash.config: unsupported custom column storage properties';
  end if;

  if (
    select count(*)
    from pg_catalog.pg_trigger as trigger_row
    where
      trigger_row.tgrelid = v_relation_oid
      and not trigger_row.tgisinternal
  ) <> 1
     or not exists (
       select
       from pg_catalog.pg_trigger as trigger_row
       where
         trigger_row.tgrelid = v_relation_oid
         and not trigger_row.tgisinternal
         and trigger_row.tgname = 'config_validate_rotation'
         and trigger_row.tgfoid =
           'ash._validate_config_update()'::regprocedure
         and trigger_row.tgenabled = 'O'
     ) then
    raise exception
      'cannot normalize ash.config: unsupported custom triggers are present';
  end if;

  if exists (
    select
    from pg_catalog.pg_policy as policy
    where policy.polrelid = v_relation_oid
  ) then
    raise exception 'cannot normalize ash.config: row security policies are present';
  end if;

  if exists (
    select
    from pg_catalog.pg_rewrite as rule
    where rule.ev_class = v_relation_oid
  ) then
    raise exception 'cannot normalize ash.config: custom rules are present';
  end if;

  if exists (
    select
    from pg_catalog.pg_publication_rel as publication_relation
    where publication_relation.prrelid = v_relation_oid
  ) then
    raise exception 'cannot normalize ash.config: publication membership is present';
  end if;

  if exists (
    select
    from pg_catalog.pg_statistic_ext as statistic
    where statistic.stxrelid = v_relation_oid
  ) then
    raise exception 'cannot normalize ash.config: extended statistics are present';
  end if;

  if exists (
    select
    from pg_catalog.pg_seclabel as security_label
    where
      security_label.classoid = 'pg_class'::regclass
      and security_label.objoid = v_relation_oid
  ) then
    raise exception 'cannot normalize ash.config: security labels are present';
  end if;

  if exists (
    select
    from pg_catalog.pg_inherits as inheritance
    where
      inheritance.inhrelid = v_relation_oid
      or inheritance.inhparent = v_relation_oid
  ) then
    raise exception 'cannot normalize ash.config: table inheritance is present';
  end if;

  if exists (
    select
    from pg_catalog.pg_constraint as constraint_row
    where
      constraint_row.confrelid = v_relation_oid
      and constraint_row.conrelid <> v_relation_oid
  ) then
    raise exception
      'cannot normalize ash.config: another table has a foreign key to it';
  end if;

  if exists (
    select
    from pg_catalog.pg_index as index_row
    where
      index_row.indrelid = v_relation_oid
      and (
        not index_row.indisvalid
        or not index_row.indisready
        or not index_row.indislive
      )
  ) then
    raise exception 'cannot normalize ash.config: an invalid index is present';
  end if;

  v_owner_oid := v_relation.relowner;

  create temporary table ash_config_migration_defaults
  on commit drop
  as
  select
    attribute.attname::text as column_name,
    attribute.attnotnull as is_not_null,
    pg_catalog.pg_get_expr(
      default_value.adbin,
      default_value.adrelid
    ) as default_expression
  from pg_catalog.pg_attribute as attribute
  left join pg_catalog.pg_attrdef as default_value
    on default_value.adrelid = attribute.attrelid
    and default_value.adnum = attribute.attnum
  where
    attribute.attrelid = v_relation_oid
    and attribute.attnum > 0
    and not attribute.attisdropped;

  create temporary table ash_config_migration_constraints
  on commit drop
  as
  select
    constraint_row.conname::text as constraint_name,
    constraint_row.contype as constraint_type,
    pg_catalog.pg_get_constraintdef(
      constraint_row.oid,
      false
    ) as constraint_definition,
    pg_catalog.obj_description(
      constraint_row.oid,
      'pg_constraint'
    ) as constraint_comment
  from pg_catalog.pg_constraint as constraint_row
  where
    constraint_row.conrelid = v_relation_oid
  order by constraint_row.conname;

  select exists (
    select
    from ash_config_migration_constraints as constraint_row
    where constraint_row.constraint_type = 'n'
  )
  into v_has_not_null_constraints;

  create temporary table ash_config_migration_indexes
  on commit drop
  as
  select
    index_relation.relname::text as index_name,
    exists (
      select
      from pg_catalog.pg_constraint as constraint_row
      where
        constraint_row.conindid = index_row.indexrelid
        and constraint_row.contype in ('p', 'u', 'x')
    ) as is_constraint_index,
    index_row.indisclustered as is_clustered,
    pg_catalog.pg_get_indexdef(index_row.indexrelid) as index_definition,
    pg_catalog.obj_description(
      index_row.indexrelid,
      'pg_class'
    ) as index_comment
  from pg_catalog.pg_index as index_row
  inner join pg_catalog.pg_class as index_relation
    on index_relation.oid = index_row.indexrelid
  where
    index_row.indrelid = v_relation_oid
  order by index_relation.relname;

  create temporary table ash_config_migration_table_acl
  on commit drop
  as
  select
    acl.grantee,
    acl.privilege_type,
    acl.is_grantable
  from pg_catalog.pg_class as relation
  cross join lateral pg_catalog.aclexplode(relation.relacl) as acl
  where relation.oid = v_relation_oid;

  create temporary table ash_config_migration_column_acl
  on commit drop
  as
  select
    attribute.attname::text as column_name,
    acl.grantee,
    acl.privilege_type,
    acl.is_grantable
  from pg_catalog.pg_attribute as attribute
  cross join lateral pg_catalog.aclexplode(attribute.attacl) as acl
  where
    attribute.attrelid = v_relation_oid
    and attribute.attnum > 0
    and not attribute.attisdropped;

  create temporary table ash_config_migration_comments
  on commit drop
  as
  select
    null::text as column_name,
    pg_catalog.obj_description(
      v_relation_oid,
      'pg_class'
    ) as object_comment
  union all
  select
    attribute.attname::text as column_name,
    pg_catalog.col_description(
      v_relation_oid,
      attribute.attnum
    ) as object_comment
  from pg_catalog.pg_attribute as attribute
  where
    attribute.attrelid = v_relation_oid
    and attribute.attnum > 0
    and not attribute.attisdropped;

  alter table ash.config rename to config_ordinal_legacy;

  create table ash.config (
    singleton                  bool,
    current_slot               smallint,
    num_partitions             smallint,
    sampling_enabled           bool,
    skipped_samples            int4,
    missed_samples             bigint,
    sample_interval            interval,
    rotation_period            interval,
    include_bg_workers         bool,
    debug_logging              bool,
    encoding_version           smallint,
    version                    text,
    rotated_at                 timestamptz,
    installed_at               timestamptz,
    rollup_1m_retention_days   smallint,
    rollup_1h_retention_days   smallint,
    rollup_min_backend_seconds smallint,
    last_rollup_1m_ts          int4,
    last_rollup_1h_ts          int4,
    insert_errors                bigint,
    register_wait_cap_hits       bigint,
    consecutive_rotate_failures bigint
  ) using heap;

  insert into ash.config (
    singleton,
    current_slot,
    num_partitions,
    sampling_enabled,
    skipped_samples,
    missed_samples,
    sample_interval,
    rotation_period,
    include_bg_workers,
    debug_logging,
    encoding_version,
    version,
    rotated_at,
    installed_at,
    rollup_1m_retention_days,
    rollup_1h_retention_days,
    rollup_min_backend_seconds,
    last_rollup_1m_ts,
    last_rollup_1h_ts,
    insert_errors,
    register_wait_cap_hits,
    consecutive_rotate_failures
  )
  select
    singleton,
    current_slot,
    num_partitions,
    sampling_enabled,
    skipped_samples,
    missed_samples,
    sample_interval,
    rotation_period,
    include_bg_workers,
    debug_logging,
    encoding_version,
    version,
    rotated_at,
    installed_at,
    rollup_1m_retention_days,
    rollup_1h_retention_days,
    rollup_min_backend_seconds,
    last_rollup_1m_ts,
    last_rollup_1h_ts,
    insert_errors,
    register_wait_cap_hits,
    consecutive_rotate_failures
  from ash.config_ordinal_legacy;

  drop table ash.config_ordinal_legacy restrict;

  create trigger config_validate_rotation
  before insert or update of
    num_partitions,
    rotation_period,
    rollup_1m_retention_days
  on ash.config
  for each row
  execute function ash._validate_config_update();

  v_owner_name := pg_catalog.pg_get_userbyid(v_owner_oid);
  execute format(
    'alter table ash.config owner to %I',
    v_owner_name
  );

  for v_default in
    select
      column_name,
      is_not_null,
      default_expression
    from ash_config_migration_defaults
    order by column_name
  loop
    if v_default.default_expression is not null then
      execute format(
        'alter table ash.config alter column %I set default %s',
        v_default.column_name,
        v_default.default_expression
      );
    end if;
  end loop;

  /*
   * PostgreSQL 18 exposes NOT NULL declarations as named pg_constraint rows,
   * so the general constraint replay below preserves their exact names.
   * Earlier majors represent them only in pg_attribute.
   */
  if not v_has_not_null_constraints then
    for v_default in
      select column_name
      from ash_config_migration_defaults
      where is_not_null
      order by column_name
    loop
      execute format(
        'alter table ash.config alter column %I set not null',
        v_default.column_name
      );
    end loop;
  end if;

  /*
   * Standalone unique indexes can be valid foreign-key targets. Recreate
   * them before constraints so a custom self-referencing FK can bind to one.
   */
  for v_index in
    select
      index_name,
      index_definition
    from ash_config_migration_indexes
    where not is_constraint_index
    order by index_name
  loop
    execute v_index.index_definition;
  end loop;

  for v_constraint in
    select
      constraint_name,
      constraint_type,
      constraint_definition,
      constraint_comment
    from ash_config_migration_constraints
    order by
      case
        when constraint_type in ('p', 'u', 'x') then 1
        when constraint_type = 'f' then 3
        else 2
      end,
      constraint_name
  loop
    execute format(
      'alter table ash.config add constraint %I %s',
      v_constraint.constraint_name,
      v_constraint.constraint_definition
    );

    if v_constraint.constraint_comment is not null then
      execute format(
        'comment on constraint %I on ash.config is %L',
        v_constraint.constraint_name,
        v_constraint.constraint_comment
      );
    end if;
  end loop;

  for v_index in
    select
      index_name,
      index_comment
    from ash_config_migration_indexes
    where index_comment is not null
    order by index_name
  loop
    execute format(
      'comment on index ash.%I is %L',
      v_index.index_name,
      v_index.index_comment
    );
  end loop;

  for v_index in
    select index_name
    from ash_config_migration_indexes
    where is_clustered
    order by index_name
  loop
    execute format(
      'alter table ash.config cluster on %I',
      v_index.index_name
    );
  end loop;

  /*
   * CREATE TABLE applies the migration role's current default privileges.
   * Remove every generated non-owner grant before replaying the old ACL.
   */
  execute 'revoke all privileges on table ash.config from public';
  for v_acl in
    select distinct
      acl.grantee,
      case
        when acl.grantee = 0 then null
        else pg_catalog.pg_get_userbyid(acl.grantee)
      end as grantee_name
    from pg_catalog.pg_class as relation
    cross join lateral pg_catalog.aclexplode(relation.relacl) as acl
    where
      relation.oid = 'ash.config'::regclass
      and acl.grantee <> v_owner_oid
  loop
    if v_acl.grantee = 0 then
      v_grantee_sql := 'public';
    else
      v_grantee_sql := format(
        '%I',
        pg_catalog.pg_get_userbyid(v_acl.grantee)
      );
    end if;

    execute format(
      'revoke all privileges on table ash.config from %s',
      v_grantee_sql
    );
  end loop;

  for v_acl in
    select
      grantee,
      privilege_type,
      is_grantable
    from ash_config_migration_table_acl
    order by grantee, privilege_type
  loop
    if v_acl.grantee = 0 then
      v_grantee_sql := 'public';
    else
      v_grantee_sql := format(
        '%I',
        pg_catalog.pg_get_userbyid(v_acl.grantee)
      );
    end if;

    if v_acl.is_grantable then
      v_grant_option_sql := ' with grant option';
    else
      v_grant_option_sql := '';
    end if;

    execute format(
      'grant %s on table ash.config to %s%s',
      v_acl.privilege_type,
      v_grantee_sql,
      v_grant_option_sql
    );
  end loop;

  for v_acl in
    select
      column_name,
      grantee,
      privilege_type,
      is_grantable
    from ash_config_migration_column_acl
    order by column_name, grantee, privilege_type
  loop
    if v_acl.grantee = 0 then
      v_grantee_sql := 'public';
    else
      v_grantee_sql := format(
        '%I',
        pg_catalog.pg_get_userbyid(v_acl.grantee)
      );
    end if;

    if v_acl.is_grantable then
      v_grant_option_sql := ' with grant option';
    else
      v_grant_option_sql := '';
    end if;

    execute format(
      'grant %s (%I) on table ash.config to %s%s',
      v_acl.privilege_type,
      v_acl.column_name,
      v_grantee_sql,
      v_grant_option_sql
    );
  end loop;

  for v_comment in
    select
      column_name,
      object_comment
    from ash_config_migration_comments
    where object_comment is not null
    order by column_name nulls first
  loop
    if v_comment.column_name is null then
      execute format(
        'comment on table ash.config is %L',
        v_comment.object_comment
      );
    else
      execute format(
        'comment on column ash.config.%I is %L',
        v_comment.column_name,
        v_comment.object_comment
      );
    end if;
  end loop;

  select
    array_agg(
      attribute.attname::text
      order by attribute.attnum
    )
  into v_actual_columns
  from pg_catalog.pg_attribute as attribute
  where
    attribute.attrelid = 'ash.config'::regclass
    and attribute.attnum > 0
    and not attribute.attisdropped;

  if v_actual_columns is distinct from v_canonical_columns then
    raise exception
      'ash.config normalization produced unexpected columns: %',
      v_actual_columns;
  end if;

  select
    count(*),
    count(*) filter (where singleton)
  into
    v_row_count,
    v_singleton_count
  from ash.config;

  if v_row_count <> 1 or v_singleton_count <> 1 then
    raise exception
      'ash.config normalization lost the singleton row';
  end if;

  raise notice
    'ash.config normalized to canonical column order with row and catalog properties preserved';
end
$config_ordinal_normalization$;
commit;
