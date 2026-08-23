/* -------------------------------------------------------------------------
 * Issue #222: undo replica_guards_seed_pending.sql on the primary.
 *
 * The seam step seeds inside a transaction it rolls back, so it needs no
 * cleanup. The real-standby step cannot: the pending work has to be COMMITTED
 * on the primary in order to replicate. That leaves seeded samples, minute
 * grains and rewound watermarks behind for every later step in the job — and
 * a later step asserting exact pre-rebuild state then fails for reasons that
 * have nothing to do with it.
 *
 * Restore the pristine post-install state.
 * ------------------------------------------------------------------------- */

truncate table ash.sample;
truncate table ash.rollup_1m;
truncate table ash.rollup_1h;

do $seed_cleanup$
declare
  v_slots int;
begin
  select num_partitions into v_slots from ash.config where singleton;
  for i in 0 .. v_slots - 1 loop
    execute format('truncate table ash.query_map_%s restart identity', i);
  end loop;

  /*
   * The seed interns a wait event via ash._register_wait('active', 'Guard',
   * 'PendingWork'), which writes a row to ash.wait_event_map. Truncating the
   * sample and rollup tables does not remove it, so "restore the pristine
   * post-install state" was not true: a synthetic wait event outlived the
   * step and showed up in ash.wait_event_map for everything after it.
   *
   * Delete that one row by its identity rather than truncating the table:
   * real wait events interned by earlier steps' sampling are legitimate state
   * and must survive.
   */
  delete from ash.wait_event_map
  where state = 'active'
    and type = 'Guard'
    and event = 'PendingWork';

  update ash.config
  set last_rollup_1m_ts = null,
    last_rollup_1h_ts = null,
    current_slot = 0,
    rotated_at = pg_catalog.clock_timestamp()
  where singleton;

  perform ash._rebuild_query_map_view();

  raise notice 'Issue #222 primary seed cleaned up';
end
$seed_cleanup$;
