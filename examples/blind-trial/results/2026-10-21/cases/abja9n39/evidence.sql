\set ON_ERROR_STOP on
\pset pager off
begin read only;
set local statement_timeout='30s';
select * from ash.status();
select * from ash.aas(since=>'2026-10-21 21:54+00',until=>'2026-10-21 21:59+00');
select * from ash.timeline(since=>'2026-10-21 21:54+00',until=>'2026-10-21 21:59+00');
select * from ash.aas(since=>'2026-10-21 21:57+00',until=>'2026-10-21 21:59+00');
select * from ash.top('wait_event',since=>'2026-10-21 21:57+00',until=>'2026-10-21 21:59+00');
select * from ash.top('query_id',since=>'2026-10-21 21:57+00',until=>'2026-10-21 21:59+00',wait_event=>'Lock:transactionid');
select * from ash.top('wait_event',since=>'2026-10-21 21:57+00',until=>'2026-10-21 21:59+00',query_id=>-1016721775295826782);
select * from ash.samples(since=>'2026-10-21 21:57+00',until=>'2026-10-21 21:59+00',n=>12);
select wait_event,query_id,min(sample_time) as first_seen,max(sample_time) as last_seen,count(*) as observations,count(distinct sample_time) as ticks from ash.samples(since=>'2026-10-21 21:57+00',until=>'2026-10-21 21:59+00',n=>1000) group by 1,2;
with ticks as (select sample_time,count(*) as backends from ash.samples(since=>'2026-10-21 21:57+00',until=>'2026-10-21 21:59+00',n=>1000) group by 1), gaps as (select *,sample_time-lag(sample_time) over(order by sample_time) as gap from ticks) select * from gaps order by sample_time;
select * from ash.aas(since=>'2026-10-21 21:55+00',until=>'2026-10-21 21:57+00');
select pid,datname,state,wait_event_type,wait_event,pg_blocking_pids(pid) as blockers from pg_stat_activity where datname='appdb';
commit;
