-- Invariant helper budgets must not scale with sample rows or cached plans.
-- Use call counts and exact results, not wall-clock timing. Changes roll back.
\set ON_ERROR_STOP on
begin;
set local track_functions = 'all';
set local search_path = pg_catalog, ash, public;
set local jit = off;

update ash.config
set sampling_enabled = false,
    current_slot = 0,
    num_partitions = 3,
    rotation_period = interval '1 day',
    rotated_at = date_trunc('minute', now()),
    sample_interval = interval '1 second',
    last_rollup_1m_ts = null,
    last_rollup_1h_ts = null
where singleton;
truncate ash.sample, ash.query_map_0, ash.query_map_1, ash.query_map_2,
         ash.rollup_1m, ash.rollup_1h;

create function pg_temp.helper_calls(helper_name text)
returns bigint
language sql
as $$
  select coalesce(sum(calls), 0)::bigint
  from pg_stat_xact_user_functions
  where schemaname = 'ash' and funcname = helper_name
$$;

do $$
declare
  v_start int4 := ash.ts_from_timestamptz(
    date_trunc('minute', now()) - interval '10 minutes');
  v_wait smallint := ash._register_wait('active', 'IO', 'DataFileRead');
  v_map int4;
  v_datid oid;
  v_size int;
  v_mode text;
  v_call int;
  v_case record;
  v_before bigint;
  v_delta bigint;
  v_result text;
  v_slots_before bigint;
  v_oldest_before bigint;
  v_report jsonb;
  v_body text;
  v_plan jsonb;
  v_node jsonb;
  v_initplans int := 0;
  v_failures text[] := array[]::text[];
begin
  select oid into strict v_datid from pg_database
  where datname = current_database();
  insert into ash.query_map_0 (query_id) values (4242) returning id into v_map;

  foreach v_size in array array[3, 12] loop
    truncate ash.sample;
    insert into ash.sample (sample_ts, datid, active_count, data, slot)
    select v_start + i, v_datid, 2, array[-v_wait, 2, v_map, v_map]::int4[], 0
    from generate_series(0, v_size - 1) as fixture(i);

    foreach v_mode in array array['force_custom_plan', 'force_generic_plan', 'auto'] loop
      perform set_config('plan_cache_mode', v_mode, true);
      -- Eight calls cover the automatic custom-to-generic decision point.
      for v_call in 1..8 loop
        for v_case in
          select * from (values
            ('oldest',
             '_active_slots', ash.ts_to_timestamptz(v_start)::text),
            ('retention',
             '_raw_oldest_sample', (date_trunc('minute', now()) - interval '1 day')::text),
            ('source',
             '_raw_oldest_sample', 'raw'),
            ('aggregate source',
             '_pick_source', 'raw'),
            ('report query ids',
             '_active_slots_for_at', array[format('4242(%s)', round(v_size * 2 / 60.0, 1))]::text)
          ) as cases(label, helper, expected)
        loop
          v_before := pg_temp.helper_calls(v_case.helper);
          -- Direct PL/pgSQL expressions retain their cached plans across calls.
          case v_case.label
            when 'oldest' then v_result := ash._raw_oldest_sample()::text;
            when 'retention' then v_result := ash._raw_retention_start()::text;
            when 'source' then
              v_result := ash._pick_source(ash.ts_to_timestamptz(v_start));
            when 'aggregate source' then
              v_result := ash._pick_source_agg(ash.ts_to_timestamptz(v_start),
                                               ash.ts_to_timestamptz(v_start + 60));
            when 'report query ids' then
              v_result := ash._hr_top_queryids('IO', array[v_start], 2, 1)::text;
          end case;
          v_delta := pg_temp.helper_calls(v_case.helper) - v_before;
          assert v_result is not distinct from v_case.expected,
            format('%s result: expected %s, got %s', v_case.label, v_case.expected, v_result);
          if v_delta <> 1 then
            v_failures := array_append(v_failures, format(
              '%s: %s rows, %s, call %s: expected 1 %s call, got %s',
              v_case.label, v_size, v_mode, v_call, v_case.helper, v_delta));
          end if;
        end loop;
      end loop;
    end loop;
  end loop;

  -- Exercise fallback branches with rows present but outside the requested window.
  -- An EXISTS may stop early; a miss must still evaluate global state only once.
  foreach v_mode in array array['force_custom_plan', 'force_generic_plan', 'auto'] loop
    perform set_config('plan_cache_mode', v_mode, true);
    for v_call in 1..8 loop
      truncate ash.rollup_1m;
      v_before := pg_temp.helper_calls('_active_slots');
      v_report := ash.report(ash.ts_to_timestamptz(v_start - 120),
                             ash.ts_to_timestamptz(v_start - 60));
      v_delta := pg_temp.helper_calls('_active_slots') - v_before;
      assert v_report is null, 'report without minute rollups must stay NULL';
      if v_delta not between 1 and 2 then
        v_failures := array_append(v_failures, format(
          'report fallback: %s call %s: expected at most 2 active-slot calls, got %s',
          v_mode, v_call, v_delta));
      end if;

      insert into ash.rollup_1m
        (ts, datid, samples, peak_backends, wait_counts, query_counts)
      values (v_start, v_datid, 12, 2, array[v_wait, 24], array[4242, 24]);
      v_before := pg_temp.helper_calls('_pick_source');
      v_result := ash._pick_source_agg(ash.ts_to_timestamptz(v_start),
                                      ash.ts_to_timestamptz(v_start + 7200));
      v_delta := pg_temp.helper_calls('_pick_source') - v_before;
      assert v_result = 'raw', 'stale wide aggregate must preserve raw fallback';
      if v_delta <> 1 then
        v_failures := array_append(v_failures, format(
          'wide aggregate fallback: %s call %s: expected 1 source call, got %s',
          v_mode, v_call, v_delta));
      end if;

      -- _raw_retention_start and the extreme-minute coverage each need one scan.
      -- Each nonempty attribution request has its own, separate slot boundary.
      v_slots_before := pg_temp.helper_calls('_active_slots_for_at');
      v_oldest_before := pg_temp.helper_calls('_raw_oldest_sample');
      v_report := ash.report(ash.ts_to_timestamptz(v_start),
                             ash.ts_to_timestamptz(v_start + 60));
      v_delta := pg_temp.helper_calls('_raw_oldest_sample') - v_oldest_before
        - (pg_temp.helper_calls('_active_slots_for_at') - v_slots_before);
      assert (v_report #>> '{aas_avg,total}')::numeric = 0.4
        and v_report #> '{top_queryids_worst1m,total}' = '["4242(0.4)"]'::jsonb,
        format('unexpected populated report: %s', v_report);
      if v_delta <> 2 then
        v_failures := array_append(v_failures, format(
          'report coverage: %s call %s: expected 2 independent scans, got %s',
          v_mode, v_call, v_delta));
      end if;

      -- Select minute rollups, but scan raw rows to disprove omitted observations.
      insert into ash.rollup_1m
        (ts, datid, samples, peak_backends, wait_counts, query_counts)
      values (v_start - 7200, v_datid, 1, 2, array[v_wait, 2], array[4242, 2]);
      v_before := pg_temp.helper_calls('_active_slots');
      v_result := ash._pick_source_agg(ash.ts_to_timestamptz(v_start - 7200),
                                      ash.ts_to_timestamptz(v_start + 60));
      v_delta := pg_temp.helper_calls('_active_slots') - v_before;
      assert v_result = 'rollup_1m', 'partial minute source selection changed';
      if v_delta <> 2 then
        v_failures := array_append(v_failures, format(
          'omitted-raw probe: %s call %s: expected 2 slot selections, got %s',
          v_mode, v_call, v_delta));
      end if;
    end loop;
  end loop;

  -- EXPLAIN the installed SQL body, not a copied or simplified probe query.
  -- SQL functions with SET clauses are opaque in the outer caller's plan.
  -- Bind only the formal parameters; preserve every relation and expression.
  select prosrc into strict v_body from pg_proc
  where oid = 'ash._hr_top_queryids(text,integer[],integer,numeric)'::regprocedure;
  v_body := replace(v_body, '_hr_top_queryids.type', '$1');
  v_body := regexp_replace(v_body, '\mminutes\M', '$2', 'g');
  v_body := replace(v_body, 'greatest(n, 0)', 'greatest($3, 0)');
  v_body := regexp_replace(v_body, '\msi\M', '$4', 'g');
  execute 'explain (analyze, verbose, costs off, timing off, format json) ' || v_body
    into v_plan using 'IO'::text, array[v_start], 2, 1::numeric;
  for v_node in
    with recursive nodes(node) as (
      select v_plan -> 0 -> 'Plan'
      union all
      select child.node
      from nodes
      cross join lateral jsonb_array_elements(nodes.node -> 'Plans') as child(node)
    )
    select node from nodes
  loop
    assert coalesce(v_node ->> 'Filter', '') not like '%_active_slots_for_at%',
      format('row filter repeats invariant helper: %s', v_node);
    if coalesce(v_node ->> 'Output', '') like '%_active_slots_for_at%' then
      assert v_node ->> 'Parent Relationship' = 'InitPlan'
        and (v_node ->> 'Actual Loops')::int = 1,
        format('slot helper lacks a one-execution InitPlan: %s', v_node);
      v_initplans := v_initplans + 1;
    end if;
  end loop;
  assert v_initplans = 1,
    format('expected one executed slot InitPlan, found %s: %s', v_initplans, v_plan);

  -- Null, empty, absent, and excluded-slot evidence must not become attribution.
  assert ash._hr_top_queryids('IO', null, 2, 1) = array[]::text[];
  assert ash._hr_top_queryids('IO', array[]::int4[], 2, 1) = array[]::text[];
  assert ash._hr_top_queryids('IO', array[v_start - 60], 2, 1) = array[]::text[];
  assert ash._hr_top_queryids('IO', array[v_start], 0, 1) = array[]::text[];
  assert ash._hr_top_queryids(null, array[v_start], 2, 1) = array['4242(0.4)'];
  assert ash._hr_top_queryids('Lock', array[v_start], 2, 1) = array[]::text[];
  insert into ash.query_map_1 (id, query_id) overriding system value
  values (v_map, 4242);
  update ash.sample set slot = 1;
  assert ash._hr_top_queryids('IO', array[v_start], 2, 1) = array[]::text[],
    'the excluded ring slot must not contribute';
  assert ash._raw_oldest_sample() is null, 'oldest sample must exclude that slot';
  truncate ash.sample, ash.rollup_1m;
  assert ash._raw_oldest_sample() is null;
  assert ash._raw_retention_start() is null;
  assert ash._pick_source(ash.ts_to_timestamptz(v_start)) = 'none';
  assert ash._hr_top_queryids('IO', array[v_start], 2, 1) = array[]::text[];
  assert ash.report(ash.ts_to_timestamptz(v_start),
                    ash.ts_to_timestamptz(v_start + 60)) is null;

  assert cardinality(v_failures) = 0,
    array_to_string(v_failures, E'\n');
  raise notice 'invariant helper execution budgets PASSED';
end;
$$;
rollback;
