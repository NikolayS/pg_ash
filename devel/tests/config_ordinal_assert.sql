\set ON_ERROR_STOP on

select set_config('pg_ash.expected_version', :'expected_version', false);

do $$
declare
  v_columns text[];
  v_config ash.config%rowtype;
  v_default_diff text;
  v_owner text;
begin
  select array_agg(attribute.attname::text order by attribute.attnum)
  into v_columns
  from pg_attribute as attribute
  where
    attribute.attrelid = 'ash.config'::regclass
    and attribute.attnum > 0
    and not attribute.attisdropped;

  assert v_columns = array[
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
    'consecutive_rotate_failures',
    'sample_unlogged'
  ]::text[],
    format('ash.config column order differs: %s', v_columns);

  select *
  into strict v_config
  from ash.config
  where singleton;

  assert v_config.current_slot = 2, 'current_slot was not preserved';
  assert not v_config.sampling_enabled, 'sampling_enabled was not preserved';
  assert v_config.skipped_samples = 42, 'skipped_samples was not preserved';
  assert v_config.missed_samples = 43, 'missed_samples was not preserved';
  assert v_config.sample_interval = interval '7 seconds',
    'sample_interval was not preserved';
  assert v_config.rotation_period = interval '2 days',
    'rotation_period was not preserved';
  assert v_config.include_bg_workers, 'include_bg_workers was not preserved';
  assert v_config.debug_logging, 'debug_logging was not preserved';
  assert v_config.encoding_version = 1, 'encoding_version was not preserved';
  assert v_config.version = current_setting('pg_ash.expected_version'),
    'version was not upgraded';
  assert v_config.rotated_at = '2026-01-02T03:04:05+00'::timestamptz,
    'rotated_at was not preserved';
  assert v_config.installed_at = '2025-06-07T08:09:10+00'::timestamptz,
    'installed_at was not preserved';
  assert v_config.rollup_1m_retention_days = 31,
    'rollup_1m_retention_days was not preserved';
  assert v_config.rollup_1h_retention_days = 1830,
    'rollup_1h_retention_days was not preserved';
  assert v_config.rollup_min_backend_seconds = 4,
    'rollup_min_backend_seconds was not preserved';
  assert v_config.last_rollup_1m_ts = 111,
    'last_rollup_1m_ts was not preserved';
  assert v_config.last_rollup_1h_ts = 222,
    'last_rollup_1h_ts was not preserved';
  assert v_config.insert_errors = 0, 'insert_errors default was not applied';
  assert v_config.register_wait_cap_hits = 0,
    'register_wait_cap_hits default was not applied';
  assert v_config.consecutive_rotate_failures = 0,
    'consecutive_rotate_failures default was not applied';
  assert not v_config.sample_unlogged,
    'sample_unlogged default should be false';

  select pg_get_userbyid(relation.relowner)
  into v_owner
  from pg_class as relation
  where relation.oid = 'ash.config'::regclass;

  assert v_owner = 'config_ordinal_owner',
    format('ash.config owner changed to %s', v_owner);
  assert has_table_privilege(
    'config_ordinal_reader',
    'ash.config',
    'select with grant option'
  ), 'table SELECT WITH GRANT OPTION was not preserved';
  assert not has_table_privilege('public', 'ash.config', 'select'),
    'PUBLIC SELECT on ash.config was widened';
  assert has_column_privilege(
    'config_ordinal_reader',
    'ash.config',
    'sampling_enabled',
    'update'
  ), 'column UPDATE was not preserved';
  assert not has_column_privilege(
    'config_ordinal_reader',
    'ash.config',
    'debug_logging',
    'update'
  ), 'column UPDATE widened to debug_logging';

  assert exists (
    select
    from pg_constraint as constraint_row
    where
      constraint_row.conrelid = 'ash.config'::regclass
      and constraint_row.conname = 'config_ordinal_skipped_nonnegative'
  ), 'custom config constraint was not preserved';

  assert exists (
    select
    from pg_trigger as trigger_row
    where
      trigger_row.tgrelid = 'ash.config'::regclass
      and not trigger_row.tgisinternal
      and trigger_row.tgname = 'config_validate_rotation'
      and trigger_row.tgfoid =
        'ash._validate_config_update()'::regprocedure
      and trigger_row.tgenabled = 'O'
  ), 'config rotation validation trigger was not preserved';

  assert exists (
    select from pg_trigger as trigger_row
    where trigger_row.tgrelid = 'ash.config'::regclass
      and not trigger_row.tgisinternal
      and trigger_row.tgname = 'config_validate_sample_interval'
      and trigger_row.tgfoid =
        'ash._validate_sample_interval_update()'::regprocedure
      and trigger_row.tgenabled = 'O'
  ), 'config cadence validation trigger was not preserved';
  begin
    update ash.config set sample_interval = interval '61 seconds'
    where singleton;
    assert false, 'normalized config accepted an unsupported cadence';
  exception when check_violation then
    null;
  end;

  assert to_regclass('ash.config_ordinal_debug_idx') is not null,
    'custom config index was not preserved';

  assert to_regclass('ash.config_ordinal_slot_uidx') is not null,
    'standalone unique config index was not preserved';

  assert exists (
    select
    from pg_constraint as constraint_row
    where
      constraint_row.conrelid = 'ash.config'::regclass
      and constraint_row.conname = 'config_ordinal_slot_self_fk'
      and constraint_row.contype = 'f'
  ), 'self-referencing config FK was not preserved';

  select string_agg(
    format(
      '%s: before=%s after=%s',
      defaults_before.column_name,
      defaults_before.default_expression,
      defaults_after.default_expression
    ),
    '; '
    order by defaults_before.column_name
  )
  into v_default_diff
  from pg_temp.config_ordinal_defaults_before as defaults_before
  left join (
    select
      attribute.attname::text as column_name,
      pg_get_expr(
        default_value.adbin,
        default_value.adrelid
      ) as default_expression
    from pg_attribute as attribute
    left join pg_attrdef as default_value
      on default_value.adrelid = attribute.attrelid
      and default_value.adnum = attribute.attnum
    where
      attribute.attrelid = 'ash.config'::regclass
      and attribute.attnum > 0
      and not attribute.attisdropped
  ) as defaults_after
    using (column_name)
  where
    defaults_before.column_name <> 'version'
    and defaults_before.default_expression
      is distinct from defaults_after.default_expression;

  assert v_default_diff is null,
    format('ash.config defaults changed: %s', v_default_diff);

  raise notice
    'ash.config canonical ordinals, row, defaults, constraints, indexes, and ACLs PASSED';
end $$;
