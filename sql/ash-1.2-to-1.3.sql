-- pg_ash: upgrade from 1.2 to 1.3
-- Safe to re-run (idempotent).
-- Changes: ash.cost_estimate() function for overhead estimation.

-- cost_estimate: predict pg_ash storage and overhead
create or replace function ash.cost_estimate(
  p_backends int default null  -- avg active backends per sample (null = auto-detect from data)
)
returns table (
  metric text,
  value text
)
language plpgsql
stable
set jit = off
as $$
declare
  v_config record;
  v_interval_secs numeric;
  v_rotation_secs numeric;
  v_backends numeric;
  v_samples_per_day numeric;
  v_avg_row_bytes numeric;
  v_data_size_per_day numeric;
  v_index_size_per_day numeric;
  v_total_size_per_day numeric;
  v_max_on_disk numeric;
  v_wal_per_day numeric;
  v_sampler_calls_per_day numeric;
  v_actual_avg_backends numeric;
  v_actual_avg_bytes numeric;
  v_actual_samples bigint;
  v_source text;
begin
  -- Get config
  select * into v_config from ash.config where singleton;
  v_interval_secs := extract(epoch from v_config.sample_interval);
  v_rotation_secs := extract(epoch from v_config.rotation_period);

  -- Try to auto-detect from actual data
  if p_backends is null then
    select count(*),
           round(avg(active_count), 1),
           round(avg(pg_column_size(s.*)), 0)
    into v_actual_samples, v_actual_avg_backends, v_actual_avg_bytes
    from ash.sample s
    where slot = any(ash._active_slots());

    if v_actual_samples > 0 and v_actual_avg_backends > 0 then
      v_backends := v_actual_avg_backends;
      v_avg_row_bytes := v_actual_avg_bytes;
      v_source := 'measured (' || v_actual_samples || ' samples, avg '
                  || v_actual_avg_backends || ' backends)';
    else
      v_backends := 10;
      v_avg_row_bytes := null;
      v_source := 'estimated (no data yet, assuming 10 backends)';
    end if;
  else
    v_backends := p_backends;
    v_avg_row_bytes := null;
    v_source := 'user-specified (' || p_backends || ' backends)';
  end if;

  -- Estimate row size if not measured:
  -- tuple header ~23 bytes + sample_ts(4) + datid(4) + active_count(2) + slot(2) + alignment = ~40 bytes overhead
  -- array: 24 bytes header + (backends * ~1.6 elements/backend * 4 bytes/element)
  -- ~1.6 accounts for wait-event markers + counts interspersed with query_ids
  -- (50 backends → ~79 array elements in SPEC §3.1)
  if v_avg_row_bytes is null then
    v_avg_row_bytes := 40 + 24 + round(v_backends * 1.58 * 4);
  end if;

  -- Core calculations
  v_samples_per_day := 86400.0 / v_interval_secs;
  v_data_size_per_day := v_samples_per_day * v_avg_row_bytes;
  -- Index overhead: ~15% of table size (btree on sample_ts + btree on datid,sample_ts)
  v_index_size_per_day := v_data_size_per_day * 0.15;
  v_total_size_per_day := v_data_size_per_day + v_index_size_per_day;
  -- Max on disk: 2 partitions (current + previous)
  v_max_on_disk := v_total_size_per_day * (v_rotation_secs / 86400.0) * 2;
  -- WAL: ~1.5x row size per insert (heap + index entries + page headers)
  v_wal_per_day := v_samples_per_day * v_avg_row_bytes * 1.5;
  v_sampler_calls_per_day := v_samples_per_day;

  -- Output
  metric := 'source'; value := v_source; return next;
  metric := 'sample_interval'; value := v_config.sample_interval::text; return next;
  metric := 'rotation_period'; value := v_config.rotation_period::text; return next;
  metric := 'avg_backends'; value := round(v_backends, 1)::text; return next;
  metric := 'avg_row_bytes'; value := round(v_avg_row_bytes)::text || ' bytes'; return next;
  metric := 'samples_per_day'; value := round(v_samples_per_day)::text; return next;
  metric := 'sampler_calls_per_day'; value := round(v_sampler_calls_per_day)::text; return next;
  metric := 'table_size_per_day'; value := pg_size_pretty(v_data_size_per_day::bigint); return next;
  metric := 'index_size_per_day'; value := pg_size_pretty(v_index_size_per_day::bigint); return next;
  metric := 'total_size_per_day'; value := pg_size_pretty(v_total_size_per_day::bigint); return next;
  metric := 'max_on_disk'; value := pg_size_pretty(v_max_on_disk::bigint)
    || ' (2 × ' || v_config.rotation_period::text || ')'; return next;
  metric := 'wal_per_day'; value := pg_size_pretty(v_wal_per_day::bigint); return next;

  return;
end;
$$;
