-- Synthetic regression data, not a demo capture or workload measurement.
-- Model a short lock incident surrounded by longer CPU/client/IO activity.
-- Each case uses the real packed sample format and the real rollup functions.
select (ash.ts_from_timestamptz(date_trunc('hour', now()) - interval '1 hour') / 60) * 60 as base_ts
\gset
select set_config('demo_test.case', :'fixture_case', false),
       set_config('demo_test.base_ts', :'base_ts', false);
truncate ash.sample, ash.rollup_1m, ash.rollup_1h;
update ash.config
set current_slot = 0, sample_interval = interval '5 seconds',
    last_rollup_1m_ts = null, last_rollup_1h_ts = null
where singleton;
do $$
declare
  base_ts int := current_setting('demo_test.base_ts')::int;
  fixture_case text := current_setting('demo_test.case');
  datid oid := (select oid from pg_database where datname = current_database());
  events smallint[] := array[
    ash._register_wait('active', 'CPU*', 'CPU*'),
    ash._register_wait('active', 'Client', 'ClientRead'),
    ash._register_wait('active', 'IO', 'DataFileRead'),
    ash._register_wait('active', 'Lock', 'transactionid'),
    ash._register_wait('active', 'Lock', 'tuple'),
    ash._register_wait('active', 'Timeout', 'PgSleep')];
  map_id int;
  minute_idx int;
  event_idx int;
  counts int[];
  packed int[];
  active_count int;
begin
  insert into ash.query_map_0 (query_id) values (291) returning id into map_id;
  for minute_idx in 0..27 loop
    counts := array[1, 1, 1, 0, 0, 0];
    if fixture_case = 'rank4' then counts[3] := 1 - minute_idx % 2; end if;
    if minute_idx between 12 and 16 then
      counts[4] := 10;
      counts[5] := case when fixture_case = 'rank4' then 4 else 2 end;
      if fixture_case = 'one_lock' then
        counts[1] := counts[1] + counts[5];
        counts[5] := 0;
      end if;
    end if;
    if fixture_case = 'rank6' then counts[6] := 1; end if;
    packed := '{}'::int[];
    active_count := 0;
    for event_idx in 1..6 loop
      if counts[event_idx] > 0 then
        packed := packed || array[-events[event_idx], counts[event_idx]]
                  || array_fill(map_id, array[counts[event_idx]]);
        active_count := active_count + counts[event_idx];
      end if;
    end loop;
    insert into ash.sample (sample_ts, datid, active_count, data, slot)
    select base_ts + minute_idx * 60 + tick, datid, active_count, packed, 0
    from generate_series(0, 55, 5) as tick;
  end loop;
end $$;
select ash.rollup_minute(1680);
select ash.rollup_hour();
-- Prove the case's ranking independently of the demo gate's configured n.
select key, backend_seconds
from ash.top('wait_event', ash.ts_to_timestamptz(:base_ts + 240),
             ash.ts_to_timestamptz(:base_ts + 1680), n => 100);
