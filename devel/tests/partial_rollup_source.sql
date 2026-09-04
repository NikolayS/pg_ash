\set ON_ERROR_STOP on
\set VERBOSITY verbose
begin;
truncate ash.sample, ash.rollup_1m, ash.rollup_1h;
update ash.config set sample_interval = '1 second', current_slot = 0, rotated_at = now();
select date_trunc('minute', now()) - interval '2 hours' as since,
  date_trunc('minute', now()) as until \gset
select ash._register_wait('active', 'CPU*', 'CPU*') as wait_id \gset
insert into ash.rollup_1m(ts, datid, samples, peak_backends, wait_counts, query_counts)
values(ash.ts_from_timestamptz(:'since'), 0, 1, 1, array[:wait_id, 1], '{}');
insert into ash.sample(sample_ts, datid, active_count, data)
values(ash.ts_from_timestamptz(:'until'::timestamptz - interval '1 minute'),
  0, 10, array[-:wait_id, 10, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
update ash.config
set last_rollup_1m_ts = ash.ts_from_timestamptz(:'since'::timestamptz + interval '1 minute');

\echo partial-reader-aas
select 'aas', source, buckets_expected, buckets_with_data, backend_seconds
from ash.aas(:'since', :'until');
\echo partial-reader-timeline
select 'timeline', source, sum(data_points), sum(avg_aas)
from ash.timeline(:'since', :'until', '1 minute') group by source;
\echo partial-reader-top
select 'top', source, backend_seconds from ash.top('wait_event_type', :'since', :'until');
\echo partial-reader-periods
select count(*) from ash.periods(:'until');
\echo partial-reader-chart
select count(*) from ash.chart(:'since', :'until');

do $$
declare
  v_since timestamptz := date_trunc('minute', now()) - interval '2 hours';
  v_until timestamptz := date_trunc('minute', now());
  v_aas record;
begin
  select * into v_aas from ash.aas(v_since, v_until);
  assert v_aas.source = 'rollup_1m' and v_aas.buckets_expected = 120
    and v_aas.buckets_with_data = 1 and v_aas.backend_seconds = 1,
    format('documented partial-source values changed: %s', v_aas);
end $$;

\echo partial-source-null-watermark
update ash.config set last_rollup_1m_ts = null;
select ash._pick_source_agg(:'since', :'until');

\echo partial-source-negative
-- Unknown/stale watermark alone does not prove a raw grain was omitted.
insert into ash.rollup_1m(ts, datid, samples, peak_backends, wait_counts, query_counts)
select (sample_ts / 60) * 60, datid, 1, active_count,
  array[:wait_id, active_count::int], '{}'::bigint[] from ash.sample;
select ash._pick_source_agg(:'since', :'until');
delete from ash.rollup_1m where ts > ash.ts_from_timestamptz(:'since');
-- A healthy watermark may omit the current partial minute, by contract.
update ash.config set last_rollup_1m_ts = ash.ts_from_timestamptz(:'until');
select ash._pick_source_agg(:'since', :'until');
-- No observed newer activity means no claim of known omitted raw load.
truncate ash.sample;
update ash.config
set last_rollup_1m_ts = ash.ts_from_timestamptz(:'since'::timestamptz + interval '1 minute');
select ash._pick_source_agg(:'since', :'until');
-- A current partial-minute row must not trigger the completed-minute warning.
insert into ash.sample(sample_ts, datid, active_count, data)
values(ash.ts_from_timestamptz(:'until'), 0, 1, array[-:wait_id, 1, 0]);
select ash._pick_source_agg(:'since', :'until'::timestamptz + interval '1 minute');
-- Nor may rows outside the requested historical window trigger it.
select ash._pick_source_agg(:'since', :'until'::timestamptz - interval '30 minutes');
-- Full raw coverage still selects raw and includes both observations.
insert into ash.sample(sample_ts, datid, active_count, data)
values(ash.ts_from_timestamptz(:'since'), 0, 1, array[-:wait_id, 1, 0]);
select ash._pick_source_agg(:'since', :'until');
-- Unlogged-standby path must not probe the physically unreadable raw ring.
create or replace function ash._raw_ring_readable()
returns bool language sql stable as 'select false';
select ash._pick_source_agg(:'since', :'until');
rollback;
