\set ON_ERROR_STOP on

create role config_ordinal_owner nologin;
create role config_ordinal_reader nologin;

do $$
declare
  v_num_partitions_attnum smallint;
begin
  select attribute.attnum
  into v_num_partitions_attnum
  from pg_attribute as attribute
  where
    attribute.attrelid = 'ash.config'::regclass
    and attribute.attname = 'num_partitions'
    and not attribute.attisdropped;

  assert v_num_partitions_attnum <> 3,
    'upgrade fixture unexpectedly already has canonical config ordinals';
end $$;

update ash.config
set
  current_slot = 2,
  sampling_enabled = false,
  skipped_samples = 42,
  missed_samples = 43,
  sample_interval = interval '7 seconds',
  rotation_period = interval '2 days',
  include_bg_workers = true,
  debug_logging = true,
  rotated_at = '2026-01-02T03:04:05+00'::timestamptz,
  installed_at = '2025-06-07T08:09:10+00'::timestamptz,
  rollup_1m_retention_days = 31,
  rollup_1h_retention_days = 1830,
  rollup_min_backend_seconds = 4,
  last_rollup_1m_ts = 111,
  last_rollup_1h_ts = 222
where singleton;

alter table ash.config owner to config_ordinal_owner;
grant select on table ash.config to config_ordinal_reader with grant option;
grant update (sampling_enabled) on table ash.config to config_ordinal_reader;

alter table ash.config
  add constraint config_ordinal_skipped_nonnegative
  check (skipped_samples >= 0);

create index config_ordinal_debug_idx
on ash.config (debug_logging)
where debug_logging;

/*
 * PostgreSQL permits a foreign key to target a standalone non-partial unique
 * index. This exercises the replay order: the index must precede the FK.
 */
create unique index config_ordinal_slot_uidx
on ash.config (current_slot);

alter table ash.config
  add constraint config_ordinal_slot_self_fk
  foreign key (current_slot)
  references ash.config (current_slot);

create temporary table config_ordinal_defaults_before
on commit preserve rows
as
select
  attribute.attname::text as column_name,
  pg_get_expr(default_value.adbin, default_value.adrelid) as default_expression
from pg_attribute as attribute
left join pg_attrdef as default_value
  on default_value.adrelid = attribute.attrelid
  and default_value.adnum = attribute.attnum
where
  attribute.attrelid = 'ash.config'::regclass
  and attribute.attnum > 0
  and not attribute.attisdropped;
