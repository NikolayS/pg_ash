-- pg_ash: upgrade from 1.2 to 1.3
-- Safe to re-run (idempotent).
-- Changes: ash.debug_logging() — binary flag, RAISE LOG only, never to client.

-- Add debug_logging column to config if missing
do $$
begin
  if not exists (
    select from information_schema.columns
    where table_schema = 'ash' and table_name = 'config' and column_name = 'debug_logging'
  ) then
    alter table ash.config
      add column debug_logging bool not null default false;
  end if;

  -- Drop old logging_level column if it exists (renamed in 1.3)
  if exists (
    select from information_schema.columns
    where table_schema = 'ash' and table_name = 'config' and column_name = 'logging_level'
  ) then
    alter table ash.config drop column logging_level;
  end if;
end $$;

-- Update version
update ash.config set version = '1.3' where singleton and version = '1.2';
