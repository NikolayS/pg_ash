-- Bound conversions and color resolution must not scale with rows or keys.
-- This fixture measures function calls, not elapsed time. Everything rolls back.
\set ON_ERROR_STOP on
begin;
set local track_functions = 'all';
set local jit = off;
set local ash.color = off;
set local search_path = pg_catalog, ash, public;
-- Bounds must also stay constant when the planner chooses sequential scans.
set local enable_indexscan = off;
set local enable_bitmapscan = off;

update ash.config
set sampling_enabled = false, current_slot = 0, num_partitions = 3,
    rotation_period = interval '1 day', rotated_at = date_trunc('minute', now()),
    sample_interval = interval '1 second',
    last_rollup_1m_ts = null, last_rollup_1h_ts = null
where singleton;
truncate ash.sample, ash.query_map_0, ash.query_map_1, ash.query_map_2,
         ash.rollup_1m, ash.rollup_1h;

create function pg_temp.invariant_calls(helper_name text)
returns bigint language sql as $$
  select coalesce(sum(calls), 0)::bigint
  from pg_stat_xact_user_functions
  where schemaname = 'ash' and funcname = helper_name
$$;

do $$
declare
  v_from timestamptz := date_trunc('minute', now()) - interval '10 minutes';
  v_to timestamptz := v_from + interval '1 minute';
  v_start int4 := ash.ts_from_timestamptz(v_from);
  v_datid oid;
  v_wait smallint := ash._register_wait('active', 'IO', 'Invariant');
  v_map int;
  v_size int;
  v_i int;
  v_warm int;
  v_mode text;
  v_case text;
  v_helper text;
  v_before bigint;
  v_calls bigint;
  v_small jsonb;
  v_rows jsonb;
  v_row jsonb;
  v_result text;
  v_guc text;
  v_color boolean;
  v_enabled boolean;
  v_expected text;
  v_failures text[] := array[]::text[];
begin
  select oid into strict v_datid from pg_database where datname = current_database();
  foreach v_mode in array array['force_custom_plan', 'force_generic_plan', 'auto'] loop
    perform set_config('plan_cache_mode', v_mode, true);
    v_small := '{}'::jsonb;
    foreach v_size in array array[0, 12, 48] loop
      truncate ash.sample, ash.query_map_0, ash.rollup_1m;
      for v_i in 1..(v_size / 4) loop
        v_wait := ash._register_wait('active', 'IO', 'Invariant' || v_i);
        insert into ash.query_map_0 (query_id) values (4200 + v_i) returning id into v_map;
        insert into ash.sample (sample_ts, datid, active_count, data, slot)
        select v_start + (v_i - 1) * 4 + i, v_datid, 1, array[-v_wait, 1, v_map], 0
        from generate_series(0, 3) as fixture(i);
      end loop;

      foreach v_case in array array['top', 'compare', 'chart', 'predicate', 'report keys'] loop
        v_helper := case v_case when 'chart' then '_color_on'
          when 'predicate' then 'ts_from_timestamptz' else 'ts_to_timestamptz' end;
        if v_case = 'predicate' then
          -- The selected old rollup forces the omitted-raw probe to scan all rows.
          insert into ash.rollup_1m
            (ts, datid, samples, peak_backends, wait_counts, query_counts)
          values (v_start - 7200, v_datid, 1, 1, array[v_wait, 1], array[4201, 1]);
          if v_size > 0 then
            insert into ash.rollup_1m
              (ts, datid, samples, peak_backends, wait_counts, query_counts)
            values (v_start, v_datid, v_size, 1, array[v_wait, v_size], array[4201, v_size]);
          end if;
        end if;
        -- Warm cached plans through the automatic generic-plan decision point.
        for v_warm in 1..9 loop
          v_before := pg_temp.invariant_calls(v_helper);
          case v_case
            when 'top' then
              select coalesce(jsonb_agg(to_jsonb(t) order by t.key), '[]') into v_rows
              from ash.top('wait_event', v_from, v_to, n => 20) as t;
            when 'compare' then
              select coalesce(jsonb_agg(to_jsonb(t) order by t.key), '[]') into v_rows
              from ash.compare(v_from, v_to, v_from, v_to, 'wait_event', 20) as t;
            when 'chart' then
              select coalesce(jsonb_agg(to_jsonb(t)), '[]') into v_rows
              from ash.chart(v_from, v_to, n => 20, color => false) as t;
            when 'predicate' then
              v_result := ash._pick_source_agg(v_from - interval '2 hours', v_to);
            when 'report keys' then
              v_result := ash._hr_top_queryids('IO', array[v_start], 20, 1)::text;
          end case;
          v_calls := pg_temp.invariant_calls(v_helper) - v_before;
        end loop;
        raise notice '% % rows=% helper=% calls=%', v_case, v_mode, v_size, v_helper, v_calls;
        if v_size = 12 then
          v_small := v_small || jsonb_build_object(v_case, v_calls);
        elsif v_size = 48 and v_calls > (v_small ->> v_case)::bigint then
          v_failures := array_append(v_failures, format(
            '%s %s grows from %s to %s calls', v_case, v_mode, v_small ->> v_case, v_calls));
        end if;
        if v_case = 'chart' and v_calls > 1 then
          v_failures := array_append(v_failures, format('chart resolves color %s times', v_calls));
        end if;
        if v_case in ('top', 'compare') then
          assert jsonb_array_length(v_rows) = v_size / 4;
          for v_row in select value from jsonb_array_elements(v_rows) loop
            if v_case = 'top' then
              assert v_row ->> 'source' = 'raw'
                and (v_row ->> 'period_start')::timestamptz = v_from
                and (v_row ->> 'period_end')::timestamptz = v_to
                and (v_row ->> 'effective_bucket')::interval = interval '1 minute'
                and (v_row ->> 'avg_aas')::numeric = 0.07
                and (v_row ->> 'peak_aas')::numeric = 0.07
                and (v_row ->> 'p99_aas')::numeric = 0.07
                and (v_row ->> 'backend_seconds')::numeric = 4
                and (v_row ->> 'pct')::numeric = round(400.0 / v_size, 2), v_row::text;
            else
              assert v_row ->> 'source_1' = 'raw' and v_row ->> 'source_2' = 'raw'
                and (v_row ->> 'period_start_1')::timestamptz = v_from
                and (v_row ->> 'period_start_2')::timestamptz = v_from
                and (v_row ->> 'period_end_1')::timestamptz = v_to
                and (v_row ->> 'period_end_2')::timestamptz = v_to
                and (v_row ->> 'avg_aas_1')::numeric = 0.07
                and (v_row ->> 'avg_aas_2')::numeric = 0.07
                and (v_row ->> 'avg_delta')::numeric = 0, v_row::text;
            end if;
          end loop;
        elsif v_case = 'predicate' then
          assert v_result = 'rollup_1m';
          truncate ash.rollup_1m;
        elsif v_case = 'report keys' then
          assert cardinality(v_result::text[]) = v_size / 4;
        elsif v_case = 'chart' then
          assert jsonb_array_length(v_rows) = case when v_size = 0 then 0 else 2 end;
        end if;
      end loop;
    end loop;
  end loop;

  -- Keep the existing false/true/NULL contract, including NULL with color off.
  foreach v_guc in array array['off', 'on', 'true', '1', 'invalid', ''] loop
    perform set_config('ash.color', v_guc, true);
    foreach v_color in array array[false, true, null::boolean] loop
      v_enabled := v_color or v_guc in ('on', 'true', '1');
      v_expected := case when not v_enabled then '' else E'\033[38;2;030;100;255m' end;
      assert ash._wait_color('IO:DataFileRead', v_color) = v_expected;
      v_expected := v_expected || '████    ' || case when v_enabled then E'\033[0m' else '' end || '     50%';
      v_before := pg_temp.invariant_calls('_color_on');
      v_result := ash._bar('IO:DataFileRead', 50, 100, 8, v_color);
      v_calls := pg_temp.invariant_calls('_color_on') - v_before;
      assert v_result = v_expected, format('bar color=%s guc=%s result=%s', v_color, v_guc, v_result);
      if v_calls > 1 then
        v_failures := array_append(v_failures, format('bar resolves color %s times', v_calls));
      end if;
      v_before := pg_temp.invariant_calls('_color_on');
      select jsonb_agg(to_jsonb(t)) into v_rows
      from ash.chart(v_from, v_to, n => 20, color => v_color) as t;
      v_calls := pg_temp.invariant_calls('_color_on') - v_before;
      if v_calls > 1 then
        v_failures := array_append(v_failures, format('chart color=%s guc=%s resolves color %s times',
          v_color, v_guc, v_calls));
      end if;
      assert (strpos(v_rows::text, '\u001b') > 0) = (v_enabled is distinct from false),
        format('chart color=%s guc=%s result=%s', v_color, v_guc, v_rows);
    end loop;
  end loop;
  assert cardinality(v_failures) = 0, array_to_string(v_failures, E'\n');
  raise notice 'reader invariant work budgets PASSED';
end;
$$;
rollback;
