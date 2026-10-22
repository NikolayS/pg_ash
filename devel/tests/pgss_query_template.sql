-- One constant template owns query-text attribution rules for all readers.
-- All schema and function changes roll back after this bounded fixture.
\set ON_ERROR_STOP on
begin;
set local track_functions = 'all';
create temp table pg_stat_statements (queryid bigint, dbid oid, query text);

do $$
declare
  v_sql text := format(ash._pgss_query_text_sql(), 'pg_temp');
  v_text text;
begin
  insert into pg_temp.pg_stat_statements values (42, 1, 'alpha text');
  execute v_sql into v_text using 5, 42::bigint, null::oid;
  assert v_text = 'alpha', 'unambiguous lookup must retain text and maxlen';
  insert into pg_temp.pg_stat_statements values (42, 2, 'beta text');
  execute v_sql into v_text using 80, 42::bigint, null::oid;
  assert v_text is null, 'unscoped collisions must not choose arbitrary text';
  execute v_sql into v_text using 80, 42::bigint, 2::oid;
  assert v_text = 'beta text', 'database scoping must retain the matching text';
  execute v_sql into v_text using 80, 42::bigint, 3::oid;
  assert v_text is null, 'a missing database must not fall back';
  delete from pg_temp.pg_stat_statements where dbid = 2;
  insert into pg_temp.pg_stat_statements values (null, 2, '<insufficient privilege>');
  execute v_sql into v_text using 80, 42::bigint, null::oid;
  assert v_text is null, 'hidden IDs cannot prove cross-database uniqueness';
  execute v_sql into v_text using 80, 42::bigint, 1::oid;
  assert v_text = 'alpha text', 'hidden unrelated IDs must not hide scoped text';
  execute v_sql into v_text using 80, null::bigint, 1::oid;
  assert v_text is null, 'NULL IDs must not expose hidden text';
  raise notice 'PASS: shared template attribution rules';
end;
$$;
drop table pg_temp.pg_stat_statements;

select exists (select from pg_extension where extname = 'pg_stat_statements') as has_pgss \gset
\if :has_pgss
create role ash_template_reader;
select ash.grant_reader('ash_template_reader');
set local role ash_template_reader;
select 42 as ash_pgss_template_length_a, 43 as ash_pgss_template_length_b,
       44 as ash_pgss_template_length_c, 45 as ash_pgss_template_length_d;
reset role;
truncate ash.sample, ash.rollup_1m, ash.rollup_1h;
update ash.config set sample_interval = interval '1 second';

do $$
declare
  v_schema text := ash._pgss_schema();
  v_datid oid := (select oid from pg_database where datname = current_database());
  v_slot smallint := ash.current_slot();
  v_wait smallint := ash._register_wait('active', 'CPU*', 'CPU*');
  v_from timestamptz := date_trunc('minute', now()) - interval '1 minute';
  v_qid bigint;
  v_map int;
  v_expected text;
  v_text text;
  v_template text := ash._pgss_query_text_sql();
  v_mode text;
  v_role text;
begin
  execute format('select queryid, query from %I.pg_stat_statements '
    'where dbid = $1 and userid = $2 and query like $3', v_schema)
    into strict v_qid, v_expected
    using v_datid, 'ash_template_reader'::regrole::oid, 'select $1 as ash_pgss_template_length_a,%';
  assert length(v_expected) > 100, 'fixture must exceed both public text limits';
  execute format('insert into ash.query_map_%s (query_id) values ($1) '
    'on conflict (query_id) do update set query_id = excluded.query_id returning id', v_slot)
    into v_map using v_qid;
  insert into ash.sample (sample_ts, datid, active_count, data, slot)
  values (ash.ts_from_timestamptz(v_from), v_datid, 1, array[-v_wait, 1, v_map], v_slot);

  foreach v_mode in array array['force_custom_plan', 'force_generic_plan'] loop
    perform set_config('plan_cache_mode', v_mode, true);
    foreach v_role in array array[current_user::text, 'pg_monitor', 'ash_template_reader'] loop
      execute format('set local role %I', v_role);
      assert ash._pgss_query_text(v_qid, 80, v_datid) = left(v_expected, 80);
      select query_text into strict v_text from ash.samples(v_from, v_from + interval '1 minute');
      assert v_text = left(v_expected, 80) and length(v_text) = 80,
        format('samples text differs for %s %s', v_role, v_mode);
      select query_text into strict v_text from ash.top('query_id', v_from, v_from + interval '1 minute');
      assert v_text = left(v_expected, 100) and length(v_text) = 100,
        format('top text differs for %s %s', v_role, v_mode);
      execute 'reset role';
    end loop;
  end loop;

  -- A transactional template change must reach all three execution paths.
  -- This catches a reader retaining a private copy of the attribution SQL.
  v_template := replace(v_template, 'min(query) filter (where queryid = $2)',
    '(''shared-template:'' || min(query) filter (where queryid = $2))');
  execute format('create or replace function ash._pgss_query_text_sql() returns text '
    'language sql immutable parallel safe set search_path = pg_catalog, ash as %L',
    'select ' || quote_literal(v_template));
  v_expected := 'shared-template:' || v_expected;
  assert ash._pgss_query_text(v_qid, 80, v_datid) = left(v_expected, 80);
  select query_text into strict v_text from ash.samples(v_from, v_from + interval '1 minute');
  assert v_text = left(v_expected, 80), 'samples must consume the shared template';
  select query_text into strict v_text from ash.top('query_id', v_from, v_from + interval '1 minute');
  assert v_text = left(v_expected, 100), 'top must consume the shared template';
  raise notice 'PASS: exact 80/100 limits, invoker grants, and all template consumers';
end;
$$;
\else
\echo 'SKIP: real pg_stat_statements text fixture requires the extension'
\endif
rollback;

-- Preserve existing manual helper grants when the installer adds its dependency.
\if :has_pgss
begin;
select set_config('pg_ash.install_in_migration_transaction', pg_current_xact_id()::text, true);
create role ash_template_partial;
grant usage on schema ash to ash_template_partial;
grant execute on function ash._pgss_schema(), ash._pgss_query_text(bigint,integer,oid) to ash_template_partial;
set local role ash_template_partial;
select 7911 as ash_template_partial_fixture;
reset role;
\ir ../sql/ash-install.sql
do $$
declare
  v_qid bigint;
  v_schema text := ash._pgss_schema();
  v_db oid := (select oid from pg_database where datname=current_database());
begin
  execute format('select queryid from %I.pg_stat_statements where userid=$1 and query=$2', v_schema)
    into strict v_qid using 'ash_template_partial'::regrole::oid, 'select $1 as ash_template_partial_fixture';
  execute 'set local role ash_template_partial';
  assert ash._pgss_query_text(v_qid,80,v_db) = 'select $1 as ash_template_partial_fixture',
    'partial helper grant must retain real text after reapply';
  execute 'reset role';
  assert not has_function_privilege('ash_template_partial','ash.take_sample()','execute');
  assert not has_table_privilege('ash_template_partial','ash.sample','select');
  raise notice 'PASS: partial helper grants retain exact text without unrelated privileges';
end;
$$;
rollback;
\endif
