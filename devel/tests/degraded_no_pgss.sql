-- Degraded mode: pg_ash freshly installed WITHOUT pg_stat_statements.
-- Every 2.0 reader must work; query_text degrades to NULL and is never spoofed
-- by a user-created public.pg_stat_statements relation (#87).
insert into ash.wait_event_map (state, type, event) values
  ('active', 'CPU*', 'CPU*') on conflict do nothing;
insert into ash.query_map_0 (query_id) values (999999) on conflict do nothing;

do $$
declare
  v_cpu smallint; v_q1 int4; v_ts int4; v_from timestamptz;
  n int; tcnt int; j jsonb;
begin
  select id into v_cpu from ash.wait_event_map where type = 'CPU*';
  select id into v_q1 from ash.query_map_0 where query_id = 999999;
  -- Minute-aligned 3 minutes back so a window from here reads raw.
  v_ts := (ash.ts_from_timestamptz(date_trunc('minute', now() - interval '3 minutes')) / 60) * 60;
  v_from := ash.ts_to_timestamptz(v_ts);
  insert into ash.sample (sample_ts, datid, active_count, data, slot)
    values (v_ts, 1, 1, array[-v_cpu, 1, v_q1]::integer[], 0);
  perform ash.rollup_minute();

  -- Every reader runs without pgss; query_text is NULL, no error, no WARNING.
  perform ash.periods();
  perform * from ash.aas(v_from, now());
  perform * from ash.timeline(v_from, now());
  perform * from ash.compare(v_from, now(), v_from, now());
  perform * from ash.summary(v_from, now());
  perform * from ash.chart(v_from, now());
  assert (select query_text from ash.top('query_id', v_from, now()) limit 1) is null,
    'top query_text should be NULL without pgss';
  assert (select query_text from ash.samples(v_from, now()) limit 1) is null,
    'samples query_text should be NULL without pgss';

  /*
   * The documented degraded-mode contract (README "Optional dependencies") is
   * that query *IDs* survive without pg_stat_statements and only the *text* is
   * lost. Asserting NULL text alone cannot distinguish "text degraded" from
   * "query attribution gone", so pin the exact surviving query_id.
   */
  assert (select key from ash.top('query_id', v_from, now()) limit 1) = '999999',
    format('top must still attribute the exact query_id without pgss, got %s',
      (select key from ash.top('query_id', v_from, now()) limit 1));
  assert (select query_id from ash.samples(v_from, now()) limit 1) = 999999,
    format('samples must still carry the exact query_id without pgss, got %s',
      (select query_id from ash.samples(v_from, now()) limit 1));

  -- report still produces query ids (they come from samples, not pgss).
  j := ash.report(v_from, now());
  assert j is not null, 'report should work without pgss';
  assert j->'top_queryids_worst1m' ? 'total', 'report top_queryids total should be present';

  perform ash.take_sample();
  perform ash.status();
  perform ash.set_debug_logging();

  -- #87: a user-made public.pg_stat_statements must NOT be trusted as the real
  -- extension — no row fan-out, no spoofed query_text.
  create table public.pg_stat_statements (
    queryid bigint, query text, calls bigint,
    total_exec_time double precision, mean_exec_time double precision, dbid oid);
  insert into public.pg_stat_statements values
    (999999, 'select same from user_a', 10, 100.0, 10.0, 1::oid),
    (999999, 'select same from user_b', 20, 400.0, 20.0, 2::oid);

  select count(*), count(query_text) into n, tcnt
    from ash.top('query_id', v_from, now()) where key = '999999';
  assert n = 1 and tcnt = 0,
    format('top must ignore spoofed pg_stat_statements, got rows=%s text_rows=%s', n, tcnt);
  select count(*), count(query_text) into n, tcnt
    from ash.samples(v_from, now()) where query_id = 999999;
  assert n = 1 and tcnt = 0,
    format('samples must ignore spoofed pg_stat_statements, got rows=%s text_rows=%s', n, tcnt);

  drop table public.pg_stat_statements;
  raise notice 'All degraded-mode (no pgss) 2.0 reader tests PASSED';
end $$;
