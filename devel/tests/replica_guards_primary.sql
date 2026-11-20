/* -------------------------------------------------------------------------
 * Issue #222: the guards must be invisible on a primary.
 *
 * Run after the standby assertions (and after the seam transaction is rolled
 * back) to prove two things: the recovery predicate reports false here, and
 * a temporary override cannot leak out of its transaction and quietly turn
 * sampling into a no-op for every later test.
 *
 * Requires an active backend tagged application_name = 'ash_222_primary'
 * so ash.take_sample() has something to sample; the workflow step provides it.
 *
 * Run standalone with:
 *   psql -v ON_ERROR_STOP=1 -f devel/tests/replica_guards_primary.sql
 * ------------------------------------------------------------------------- */

do $replica_guards_primary$
declare
  v_before bigint := (select count(*) from ash.sample);
  v_rows   int;
  v_after  bigint;
begin
  assert exists (
    select
    from pg_stat_activity
    where application_name = 'ash_222_primary'
      and state = 'active'
  ), 'fixture: the tagged primary workload backend is not active';

  assert not ash._in_recovery(),
    'ash._in_recovery() must be false on the primary — a seam override leaked';

  v_rows := ash.take_sample();
  assert v_rows > 0,
    format('take_sample() on the primary must record rows, got %s', v_rows);

  v_after := (select count(*) from ash.sample);
  assert v_after > v_before,
    format('primary sampling did not write: ash.sample %s -> %s',
           v_before, v_after);

  assert (select value = 'false' from ash.status() where metric = 'in_recovery'),
    format('ash.status() must report in_recovery = false on the primary, '
           'got %L',
           (select value from ash.status() where metric = 'in_recovery'));

  raise notice 'Issue #222 primary regression assertions PASSED';
end
$replica_guards_primary$;
