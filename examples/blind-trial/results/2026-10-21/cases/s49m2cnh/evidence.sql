\set ON_ERROR_STOP on
\pset pager off
\pset null '(null)'
begin isolation level repeatable read read only;
set local statement_timeout='30s';
set local timezone='UTC';
select * from ash.status();
select * from ash.aas(since=>'2026-10-21 21:59:00+00',until=>'2026-10-21 22:04:00+00');
select * from ash.timeline(since=>'2026-10-21 21:59:00+00',until=>'2026-10-21 22:04:00+00');
select * from ash.aas(since=>'2026-10-21 22:01:00+00',until=>'2026-10-21 22:03:00+00');
select * from ash.top('wait_event',since=>'2026-10-21 22:01:00+00',until=>'2026-10-21 22:03:00+00',order_by=>'peak');
select * from ash.top('query_id',since=>'2026-10-21 22:01:00+00',until=>'2026-10-21 22:03:00+00',wait_event=>'CPU*',order_by=>'peak');
select * from ash.top('wait_event',since=>'2026-10-21 22:01:00+00',until=>'2026-10-21 22:03:00+00',query_id=>2642533687498376244);
select * from ash.samples(since=>'2026-10-21 22:01:00+00',until=>'2026-10-21 22:03:00+00',wait_event=>'CPU*',query_id=>2642533687498376244,n=>100);
select * from ash.aas(since=>'2026-10-21 21:59:00+00',until=>'2026-10-21 22:01:00+00');
select * from ash.compare('2026-10-21 21:59:00+00','2026-10-21 22:01:00+00','2026-10-21 22:01:00+00','2026-10-21 22:03:00+00',dimension=>'wait_event');
rollback;
