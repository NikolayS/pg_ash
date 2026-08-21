/* -------------------------------------------------------------------------
 * Issue #222: assertions for the standby / recovery guards.
 *
 * This file asserts how pg_ash must behave when the server is in recovery.
 * It is deliberately callable in two different contexts, so the same
 * assertions cover both:
 *
 *   1. Against a REAL streaming standby (workflow step "#222: recovery
 *      guards on a real streaming standby"), which proves the behaviour on a
 *      genuine physical replica.
 *   2. Inside a transaction on the primary with ash._in_recovery() temporarily
 *      redefined to return true, then rolled back (workflow step "#222:
 *      recovery guard coverage on every PostgreSQL version"). Every guard
 *      calls ash._in_recovery() rather than pg_is_in_recovery() directly, so
 *      this exercises the real guard branches on PG majors where standing up
 *      a standby for each job would be disproportionate.
 *
 * The two split matters: (2) covers every supported major cheaply, (1) proves
 * the predicate itself is right. Neither alone is sufficient.
 *
 * Run standalone with:
 *   psql -v ON_ERROR_STOP=1 -f devel/tests/replica_guards_assert.sql
 * ------------------------------------------------------------------------- */

do $replica_guards$
declare
  /*
   * Row counts before any guarded call. Every no-op path below must leave
   * all three tables completely untouched — that is the actual contract, not
   * just "returned a neutral value".
   */
  v_samples_before   bigint := (select count(*) from ash.sample);
  v_rollup_1m_before bigint := (select count(*) from ash.rollup_1m);
  v_rollup_1h_before bigint := (select count(*) from ash.rollup_1h);
  v_skip constant text :=
    'skipped: server is in recovery (standby); '
    'pg_ash writes only on a primary';
  v_admin_call text;
  v_int    int;
  v_text   text;
  v_state  text;
begin
  assert ash._in_recovery(),
    'ash._in_recovery() must be true in this context';

  /* ---- scheduled routines: clean no-op, never an error ---- */

  v_int := ash.take_sample();
  assert v_int = 0,
    format('take_sample() on a standby must return 0, got %s', v_int);

  v_text := ash.rotate();
  assert v_text = v_skip,
    format('rotate() on a standby must return the recovery-skip text, got %L',
           v_text);
  /*
   * Guard against the pre-#222 behaviour, where rotate() returned a
   * reassuring 'skipped: rotated too recently' on a standby and the caller
   * could not tell that nothing had happened.
   */
  assert v_text not like '%rotated too recently%',
    'rotate() on a standby must not report the ordinary too-recent skip';

  v_int := ash.rollup_minute();
  assert v_int = 0,
    format('rollup_minute() on a standby must return 0, got %s', v_int);

  v_int := ash.rollup_hour();
  assert v_int = 0,
    format('rollup_hour() on a standby must return 0, got %s', v_int);

  v_text := ash.rollup_cleanup();
  assert v_text = v_skip,
    format('rollup_cleanup() on a standby must return the recovery-skip '
           'text, got %L', v_text);

  /* ---- explicit operator actions: hard 25006, before any state change ---- */

  foreach v_admin_call in array array[
    'select ash.start()',
    'select ash.stop()',
    'select ash.rebuild_partitions(4, ''yes'')',
    'select ash.uninstall(''yes'')',
    'select ash.set_debug_logging(true)'
  ] loop
    begin
      execute v_admin_call;
      raise exception '% on a standby must raise, but succeeded', v_admin_call;
    exception when others then
      v_state := sqlstate;
      assert v_state = '25006',
        format('%s on a standby must raise 25006, got %s: %s',
               v_admin_call, v_state, sqlerrm);
    end;
  end loop;

  /* ---- reads must keep working ---- */

  /*
   * A NULL argument only reports the current setting, so it is a read and
   * must survive on a standby even though set_debug_logging(true) does not.
   */
  v_text := ash.set_debug_logging(null);
  assert v_text = 'debug_logging = '
      || (select debug_logging::text from ash.config where singleton),
    format('set_debug_logging(null) must still report state on a standby, '
           'got %L', v_text);

  perform ash.periods();

  /* ---- status() must tell the truth about node role ---- */

  assert (select value = 'true' from ash.status() where metric = 'in_recovery'),
    format('ash.status() must report in_recovery = true, got %L',
           (select value from ash.status() where metric = 'in_recovery'));

  /* ---- nothing was written by any of the above ---- */

  assert (select count(*) from ash.sample) = v_samples_before,
    format('guarded calls changed ash.sample: %s -> %s',
           v_samples_before, (select count(*) from ash.sample));
  assert (select count(*) from ash.rollup_1m) = v_rollup_1m_before,
    format('guarded calls changed ash.rollup_1m: %s -> %s',
           v_rollup_1m_before, (select count(*) from ash.rollup_1m));
  assert (select count(*) from ash.rollup_1h) = v_rollup_1h_before,
    format('guarded calls changed ash.rollup_1h: %s -> %s',
           v_rollup_1h_before, (select count(*) from ash.rollup_1h));

  raise notice 'Issue #222 recovery-guard assertions PASSED';
end
$replica_guards$;
