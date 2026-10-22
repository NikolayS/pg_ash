\set ON_ERROR_STOP on
\pset pager off
begin read only;
set local statement_timeout='30s';
select * from ash.status();
select * from ash.periods();
select * from ash.aas(since=>'2026-10-21 21:56:00+00',until=>'2026-10-21 22:01:00+00');
select * from ash.chart(since=>'2026-10-21 21:56:00+00',until=>'2026-10-21 22:01:00+00',color=>false);
select * from ash.aas(since=>'2026-10-21 21:59:00+00',until=>'2026-10-21 22:01:00+00');
select * from ash.top('wait_event',since=>'2026-10-21 21:59:00+00',until=>'2026-10-21 22:01:00+00',order_by=>'peak');
select * from ash.top('query_id',since=>'2026-10-21 21:59:00+00',until=>'2026-10-21 22:01:00+00',wait_event=>'Client:ClientRead');
select * from ash.top('wait_event',since=>'2026-10-21 21:59:00+00',until=>'2026-10-21 22:00:00+00',query_id=>-9127212586606946335);
select * from ash.samples(since=>'2026-10-21 21:59:00+00',until=>'2026-10-21 22:01:00+00',wait_event=>'Client:ClientRead',n=>4);
select sample_time,count(*) as decoded_rows,min(active_backends) as reported_active_backends from ash.samples(since=>'2026-10-21 21:59:00+00',until=>'2026-10-21 22:01:00+00',n=>1000) group by sample_time order by sample_time;
select * from ash.aas(since=>'2026-10-21 21:57:00+00',until=>'2026-10-21 21:59:00+00');
select * from ash.chart(since=>'2026-10-21 21:57:00+00',until=>'2026-10-21 21:59:00+00',color=>false);
select * from ash.compare('2026-10-21 21:57:00+00','2026-10-21 21:59:00+00','2026-10-21 21:59:00+00','2026-10-21 22:01:00+00',dimension=>'wait_event');
select datname,application_name,state,wait_event_type,wait_event,query_id,xact_start,query_start from pg_stat_activity where backend_type='client backend' and pid<>pg_backend_pid();
commit;
