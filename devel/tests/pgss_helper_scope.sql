-- Query-text schema discovery must not scale with returned rows.
-- Run on a disposable database as a superuser. All fixture changes roll back.
\set ON_ERROR_STOP on
select exists (select from pg_extension where extname = 'pg_stat_statements') as has_pgss \gset
\if :has_pgss
begin;
set local track_functions = 'all';
truncate ash.sample, ash.rollup_1m, ash.rollup_1h;
update ash.config set sample_interval = interval '1 second';
do $$
declare
  v_mode text;
  v_size integer;
  v_reader text;
  v_sql text;
  v_plan json;
  v_before bigint;
  v_after bigint;
  v_rows integer;
  v_text_rows integer;
  v_slot smallint := ash.current_slot();
  v_wait smallint := ash._register_wait('active', 'CPU*', 'CPU*');
  v_from timestamptz := date_trunc('minute', now()) - interval '2 minutes';
  v_datid oid := (select oid from pg_database where datname = current_database());
  v_map_ids integer[];
begin
  execute format(
    'insert into ash.query_map_%s (query_id) select -9000000000000000000 + i '
    'from generate_series(1, 12) as g(i) on conflict (query_id) do nothing', v_slot);
  execute format('select array_agg(id order by query_id) from ash.query_map_%s '
    'where query_id between -8999999999999999999 and -8999999999999999988', v_slot)
    into v_map_ids;
  foreach v_mode in array array['force_custom_plan', 'force_generic_plan'] loop
    perform set_config('plan_cache_mode', v_mode, true);
    foreach v_size in array array[0, 12, 48] loop
      truncate ash.sample;
      insert into ash.sample (sample_ts, datid, active_count, data, slot)
      select ash.ts_from_timestamptz(v_from) + i, v_datid, 1,
             array[-v_wait, 1, v_map_ids[1 + i % (v_size / 4)]], v_slot
      from generate_series(0, v_size - 1) as g(i);
      foreach v_reader in array array['samples', 'top'] loop
        v_sql := case when v_reader = 'samples'
          then 'select * from ash.samples($1, $2, n => 1000)'
          else 'select * from ash.top(''query_id'', $1, $2, n => 1000)' end;
        select coalesce(sum(calls), 0) into v_before
        from pg_stat_xact_user_functions where funcid = 'ash._pgss_schema()'::regprocedure;
        execute 'explain (analyze, format json) ' || v_sql
          into v_plan using v_from, v_from + interval '1 minute';
        select coalesce(sum(calls), 0) into v_after
        from pg_stat_xact_user_functions where funcid = 'ash._pgss_schema()'::regprocedure;
        assert v_after - v_before = 1,
          format('%s %s rows=%s: expected one schema lookup, got %s',
            v_reader, v_mode, v_size, v_after - v_before);
        assert (v_plan->0->'Plan'->>'Actual Rows')::numeric
          = case when v_reader = 'samples' then v_size else v_size / 4 end,
          'executed public-reader plan must return the exact fixture row count';
        execute 'select count(*), count(query_text) from (' || v_sql || ') as result'
          into v_rows, v_text_rows using v_from, v_from + interval '1 minute';
        assert v_rows = case when v_reader = 'samples' then v_size else v_size / 4 end
          and v_text_rows = 0, 'unknown query IDs must survive with null query text';
        raise notice 'PASS: % % rows=% schema_calls=% plan_rows=%',
          v_reader, v_mode, v_size, v_after - v_before,
          v_plan->0->'Plan'->>'Actual Rows';
      end loop;
    end loop;
  end loop;
end;
$$;
rollback;
\else
\echo 'SKIP: pg_stat_statements not installed on this PG major'
\endif
