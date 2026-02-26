-- pg_ash: upgrade from 1.2 to 1.3
-- Safe to re-run (idempotent).
-- Changes: ash.logging_level() — configurable session logging in take_sample().

-- Add logging_level column to config if missing
do $$
begin
  if not exists (
    select from information_schema.columns
    where table_schema = 'ash' and table_name = 'config' and column_name = 'logging_level'
  ) then
    alter table ash.config
      add column logging_level text not null default 'off'
        constraint config_logging_level_check
          check (logging_level in ('off', 'debug5', 'debug4', 'debug3', 'debug2', 'debug1',
                                   'debug', 'info', 'notice', 'warning', 'log'));
  end if;
end $$;

-- Update version
update ash.config set version = '1.3' where singleton and version = '1.2';
