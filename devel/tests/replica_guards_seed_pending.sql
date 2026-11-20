/* -------------------------------------------------------------------------
 * Issue #222: create pending rollup work so the guard assertions can fail.
 *
 * ash.rollup_minute() and ash.rollup_hour() both return 0 when there is
 * simply nothing to fold, so on a quiet database "returns 0 in recovery"
 * passes with the guard deleted — the assertion measures an empty database,
 * not the guard.
 *
 * The two grains need work pending at DIFFERENT times, because their
 * preconditions pull the same watermark in opposite directions:
 *
 *   rollup_minute() folds raw samples newer than last_rollup_1m_ts, so that
 *     watermark must be BEHIND some completed raw minute.
 *   rollup_hour() only seals an hour once last_rollup_1m_ts is PAST the end
 *     of that hour, so the same watermark must be AHEAD of the hour it is
 *     asked to fold.
 *
 * Satisfy both by separating them in time:
 *   - raw samples 5 minutes back      -> pending minute work
 *   - rollup_1m grains 3 hours back   -> pending hour work
 *   - last_rollup_1m_ts 1 hour back   -> behind the samples, past the hour
 *   - last_rollup_1h_ts null          -> the old hour has never been sealed
 *
 * Run on the primary: in the seam step inside the transaction that overrides
 * ash._in_recovery(), and on the real-standby step before pg_basebackup so
 * the pending work replicates.
 * ------------------------------------------------------------------------- */

do $seed_pending_rollup$
declare
  v_wait_id smallint;
  v_datid oid := (select oid from pg_database where datname = current_database());
  v_recent_minute int4 := ash.ts_from_timestamptz(
    date_trunc('minute', clock_timestamp()) - interval '5 minutes'
  );
  v_old_hour int4 := ash.ts_from_timestamptz(
    date_trunc('hour', clock_timestamp()) - interval '3 hours'
  );
begin
  select ash._register_wait('active', 'Guard', 'PendingWork') into v_wait_id;

  -- Pending MINUTE work: raw samples in a completed minute.
  insert into ash.sample (sample_ts, datid, active_count, data, slot)
  select v_recent_minute + offs, v_datid, 2,
    array[-v_wait_id::int, 2, 0, 0], ash.current_slot()
  from unnest(array[0, 10, 20]) as offs;

  -- Pending HOUR work: minute grains inside a long-completed hour.
  insert into ash.rollup_1m (
    ts, datid, samples, peak_backends, wait_counts, query_counts
  )
  select v_old_hour + offs, v_datid, 2, 2,
    array[v_wait_id::int, 4], '{}'::int8[]
  from unnest(array[0, 60]) as offs
  on conflict do nothing;

  update ash.config
  set last_rollup_1m_ts = ash.ts_from_timestamptz(
        date_trunc('minute', clock_timestamp()) - interval '1 hour'
      ),
    last_rollup_1h_ts = null
  where singleton;
end
$seed_pending_rollup$;
