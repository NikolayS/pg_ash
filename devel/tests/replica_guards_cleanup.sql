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
