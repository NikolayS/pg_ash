\set ON_ERROR_STOP on

/*
 * Behavioral coverage for every public pg_ash surface. The caller supplies
 * the exact optional-extension state and starts one pg_sleep() client backend
 * so take_sample() has a deterministic live session to capture.
 */
select pg_catalog.set_config(
  'ash.feature_mode',
  :'feature_mode',
  false
);
select pg_catalog.set_config(
  'ash.feature_expected_cron',
  :'expected_cron',
  false
);
select pg_catalog.set_config(
  'ash.feature_expected_pgss',
  :'expected_pgss',
  false
);

do $feature_preconditions$
declare
  v_mode text := pg_catalog.current_setting('ash.feature_mode');
  v_expected_cron boolean :=
    pg_catalog.current_setting('ash.feature_expected_cron')::boolean;
  v_expected_pgss boolean :=
    pg_catalog.current_setting('ash.feature_expected_pgss')::boolean;
  v_actual_cron boolean;
  v_actual_pgss boolean;
begin
  select exists (
    select
    from pg_catalog.pg_extension
    where extname = 'pg_cron'
  )
  into v_actual_cron;

  select exists (
    select
    from pg_catalog.pg_extension
    where extname = 'pg_stat_statements'
  )
  into v_actual_pgss;

  assert v_actual_cron = v_expected_cron,
    format(
      '[%s] feature precondition: expected pg_cron=%s, got %s',
      v_mode,
      v_expected_cron,
      v_actual_cron
    );
  assert v_actual_pgss = v_expected_pgss,
    format(
      '[%s] feature precondition: expected pg_stat_statements=%s, got %s',
      v_mode,
      v_expected_pgss,
      v_actual_pgss
    );
end
$feature_preconditions$;

\ir features_fixture.sql
\ir features_readers.sql
\ir features_privileges.sql
\ir features_lifecycle.sql
