/* -------------------------------------------------------------------------
 * Issue #222: create pending rollup work so the guard assertions can fail.
 *
 * ash.rollup_minute() and ash.rollup_hour() both return 0 when there is
 * simply nothing to fold. Immediately after a fresh install there never is,
 * so asserting "returns 0 in recovery" would pass with the guard deleted —
 * the assertion would be measuring an empty database, not the guard.
 *
 * Seed raw samples two hours back and rewind the watermarks, so an UNGUARDED
 * rollup would have real work to do and would write rows. Then a return of 0
 * with unchanged row counts is a genuine signal.
 *
 * Run on the primary: in the seam step inside the transaction that overrides
 * ash._in_recovery(), and on the real-standby step before pg_basebackup so
 * the pending work replicates.
 * ------------------------------------------------------------------------- */

do $seed_pending_rollup$
declare
  v_wait_id smallint;
  v_datid oid := (select oid from pg_database where datname = current_database());
  v_minute int4 := ash.ts_from_timestamptz(
    date_trunc('minute', clock_timestamp()) - interval '2 hours'
  );
begin
  select ash._register_wait('active', 'Guard', 'PendingWork') into v_wait_id;
  insert into ash.sample (sample_ts, datid, active_count, data, slot)
  select v_minute + offs, v_datid, 2, array[-v_wait_id::int, 2, 0, 0],
    ash.current_slot()
  from unnest(array[0, 10, 20]) as offs;
  update ash.config
  set last_rollup_1m_ts = v_minute - 60,
    last_rollup_1h_ts = null
  where singleton;
end
$seed_pending_rollup$;
