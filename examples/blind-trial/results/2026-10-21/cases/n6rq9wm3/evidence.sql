\set ON_ERROR_STOP on
begin read only;
set local statement_timeout = '30s';
select * from ash.status();
select * from ash.aas(since=>'2026-10-21 22:11:00+00',until=>'2026-10-21 22:13:00+00');
select * from ash.timeline(since=>'2026-10-21 22:09:00+00',until=>'2026-10-21 22:15:00+00',bucket=>'1 minute');
select * from ash.top('wait_event',since=>'2026-10-21 22:11:00+00',until=>'2026-10-21 22:13:00+00',order_by=>'peak');
select * from ash.top('query_id',since=>'2026-10-21 22:12:00+00',until=>'2026-10-21 22:13:00+00',wait_event=>'Lock:relation',order_by=>'peak');
select * from ash.top('wait_event',since=>'2026-10-21 22:11:00+00',until=>'2026-10-21 22:13:00+00',query_id=>-9127212586606946335);
select * from ash.samples(since=>'2026-10-21 22:11:00+00',until=>'2026-10-21 22:13:00+00',n=>100);
with ticks as (
 select sample_time, count(*) as observations
 from ash.samples(since=>'2026-10-21 22:11:00+00',until=>'2026-10-21 22:13:00+00',n=>100)
 group by sample_time
), gaps as (
 select *, sample_time - lag(sample_time) over (order by sample_time) as gap from ticks
)
select min(sample_time) as first_sample, max(sample_time) as last_sample, count(*) as ticks, min(observations) as min_observations, max(observations) as max_observations, max(gap) as largest_gap from gaps;
select * from ash.aas(since=>'2026-10-21 22:09:00+00',until=>'2026-10-21 22:11:00+00');
commit;
