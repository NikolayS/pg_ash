-- Real pg_stat_statements entries with equal query IDs and different SQL text.
-- Aliases do not affect these query IDs on supported Postgres versions.
\set ON_ERROR_STOP on
select current_database() as test_database \gset
create database ash_query_text_a;
create database ash_query_text_b;
\connect ash_query_text_a
select 42 as ash_reader_database_a;
select octet_length(md5('ash_reader_unique')) as ash_reader_unique;
\connect ash_query_text_b
select 42 as ash_reader_database_b;
\connect :test_database
begin;
truncate ash.sample, ash.rollup_1m, ash.rollup_1h;
update ash.config set sample_interval = interval '1 second';
do $$
declare
  v_pgss_schema text := ash._pgss_schema();
  v_database name;
  v_datid_a oid := (select oid from pg_database where datname = 'ash_query_text_a');
  v_datid_b oid := (select oid from pg_database where datname = 'ash_query_text_b');
  v_qid_a bigint;
  v_qid_b bigint;
  v_unique_qid bigint;
  v_slot smallint := ash.current_slot();
  v_map_id integer;
  v_cpu smallint := ash._register_wait('active', 'CPU*', 'CPU*');
  v_from timestamptz := date_trunc('minute', now()) - interval '1 minute';
  v_hour timestamptz := date_trunc('hour', now()) - interval '2 days';
  v_expected text;
  v_actual record;
  v_count integer;
begin
  assert v_pgss_schema is not null, 'fixture requires real pg_stat_statements';
  execute format('select queryid from %I.pg_stat_statements where dbid = $1 and query = $2', v_pgss_schema)
    into strict v_qid_a using v_datid_a, 'select $1 as ash_reader_database_a';
  execute format('select queryid from %I.pg_stat_statements where dbid = $1 and query = $2', v_pgss_schema)
    into strict v_qid_b using v_datid_b, 'select $1 as ash_reader_database_b';
  assert v_qid_a = v_qid_b, 'fixture must produce a real cross-database query ID collision';
  execute format('select queryid from %I.pg_stat_statements where dbid = $1 and query = $2', v_pgss_schema)
    into strict v_unique_qid using v_datid_a, 'select octet_length(md5($1)) as ash_reader_unique';
  assert ash._pgss_query_text(v_unique_qid) = 'select octet_length(md5($1)) as ash_reader_unique'
     and ash._pgss_query_text(v_unique_qid, 6) = 'select',
    'legacy helper signatures must preserve unambiguous text and maxlen';


  execute format('insert into ash.query_map_%s (query_id) values ($1) on conflict (query_id) do update set query_id = excluded.query_id returning id', v_slot)
    into v_map_id using v_qid_a;
  insert into ash.sample (sample_ts, datid, active_count, data, slot)
  values (ash.ts_from_timestamptz(v_from), v_datid_a, 1, array[-v_cpu, 1, v_map_id], v_slot),
         (ash.ts_from_timestamptz(v_from), v_datid_b, 1, array[-v_cpu, 1, v_map_id], v_slot);

  -- Both public sample rows must carry their own database's SQL text.
  select count(*) into v_count from ash.samples(v_from, v_from + interval '1 minute');
  assert v_count = 2, 'fixture must expose exactly two raw sample rows';
  foreach v_database in array array['ash_query_text_a', 'ash_query_text_b']::name[] loop
    v_expected := case when v_database = 'ash_query_text_a'
      then 'select $1 as ash_reader_database_a' else 'select $1 as ash_reader_database_b' end;
    select * into strict v_actual from ash.samples(v_from, v_from + interval '1 minute')
    where database_name = v_database;
    assert v_actual.query_id = v_qid_a and v_actual.query_text is not distinct from v_expected,
      format('samples returned another database SQL text: %s', row_to_json(v_actual));
    select * into strict v_actual from ash.top('query_id', v_from, v_from + interval '1 minute', database => v_database);
    assert v_actual.key = v_qid_a::text and v_actual.query_text is not distinct from v_expected,
      format('top returned another database SQL text: %s', row_to_json(v_actual));
    select * into strict v_actual from ash.compare(v_from, v_from + interval '1 minute',
      v_from, v_from + interval '1 minute', dimension => 'query_id', database => v_database);
    assert v_actual.query_text is not distinct from v_expected and v_actual.avg_delta = 0,
      'compare must preserve database-scoped query text';
  end loop;

  -- Default monitoring grants must include the database-aware helper.
  execute 'set local role pg_monitor';
  select * into strict v_actual from ash.samples(v_from, v_from + interval '1 minute',
    database => 'ash_query_text_a');
  assert v_actual.query_text = 'select $1 as ash_reader_database_a',
    'pg_monitor must execute the database-aware helper';
  execute 'reset role';
  assert ash._pgss_query_text(v_qid_a, 9, v_datid_a) = 'select $1',
    'database-aware helper must preserve maxlen';
  assert ash._pgss_query_text(v_qid_a, 80, 0::oid) is null,
    'a database without the query must not fall back to another database';

  -- A cross-database aggregate has no single database identity for this key.
  select * into strict v_actual from ash.top('query_id', v_from, v_from + interval '1 minute');
  assert v_actual.key = v_qid_a::text and v_actual.query_text is null,
    'unscoped top must not choose arbitrary text from colliding databases';
  assert ash._pgss_query_text(v_qid_a) is null,
    'legacy helper must return NULL for ambiguous database identity';

  -- Hourly readers must also carry the database filter into enrichment.
  insert into ash.rollup_1h (ts, datid, samples, peak_backends, wait_counts, query_counts)
  values (ash.ts_from_timestamptz(v_hour), v_datid_a, 60, 1, array[v_cpu, 60], array[v_qid_a, 60]),
         (ash.ts_from_timestamptz(v_hour), v_datid_b, 60, 1, array[v_cpu, 60], array[v_qid_b, 60]);
  foreach v_database in array array['ash_query_text_a', 'ash_query_text_b']::name[] loop
    v_expected := case when v_database = 'ash_query_text_a'
      then 'select $1 as ash_reader_database_a' else 'select $1 as ash_reader_database_b' end;
    select * into strict v_actual from ash.top('query_id', v_hour, v_hour + interval '1 hour', database => v_database);
    assert v_actual.source = 'rollup_1h' and v_actual.query_text is not distinct from v_expected,
      'hourly top must preserve database-scoped query text';
  end loop;
  raise notice 'PASS: real query ID collision, samples, raw/hourly top, compare, and unscoped ambiguity';
end;
$$;
rollback;
\connect :test_database
drop database ash_query_text_a;
drop database ash_query_text_b;
