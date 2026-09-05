/*
 * lib/seed.sql — the ash_demo helper schema used by lib/seed.sh.
 *
 * ===========================================================================
 * HONESTY BOUNDARY  (restated verbatim in demos/README.md)
 * ===========================================================================
 * The harness shapes WHICH real samples exist and WHEN they are considered to
 * have been taken. Every number in every asset is pg_ash aggregating its own
 * stored samples, written by ash.take_sample() from real pg_stat_activity over
 * real pgbench backends. No reader output is edited. The time compression
 * uses virtual-minute weights; elapsed time varies (see demos/README.md).
 * ASH_REAL_TIME=1 skips the restamp and runs in real time.
 *
 * Concretely, the only liberty taken is the UPDATE in ash_demo.restamp():
 * it rewrites ash.sample.sample_ts. It never invents a row, never edits an
 * active_count, and never touches the packed data[] array — the wait events
 * and query ids in every sample are exactly what pg_stat_activity reported.
 * ===========================================================================
 *
 * Style note: this file follows the project SQL style guide (lower case
 * keywords, explicit aliases, no `select *` in shipped queries).
 */

create schema if not exists ash_demo;

comment on schema ash_demo is
  'pg_ash demo harness scratch schema. Created by demos/lib/seed.sql; '
  'dropped with the demo database. Not part of pg_ash.';

/* ---------------------------------------------------------------------------
 * ash_demo.batch(n, delay)
 *
 * Take `n` real samples through pg_ash's own sampler, committing between each
 * so ash.take_sample() sees a fresh now() (it timestamps from transaction
 * start, not clock_timestamp) and so its transaction-level advisory lock is
 * released between ticks.
 *
 * The calling session is invisible to its own samples: take_sample() filters
 * `pid <> pg_backend_pid()`. That is what keeps the seeder out of the picture
 * and is why the harness must sample from exactly ONE session — a second
 * sampler shows up in the first one's data as Timeout:PgSleep and pollutes
 * the whole story.
 * ------------------------------------------------------------------------ */
create or replace procedure ash_demo.batch(n int, delay numeric)
language plpgsql
as $$
declare
  v_i int;
begin
  for v_i in 1 .. n loop
    perform ash.take_sample();
    commit;
    if delay > 0 then
      perform pg_sleep(delay);
    end if;
  end loop;
end;
$$;

/* ---------------------------------------------------------------------------
 * ash_demo.restamp(watermark, base_ts, idx)
 *
 * Move every sample written since `watermark` into virtual minute `idx`
 * (1-based) of the seeded history that starts at `base_ts`, spread evenly
 * across that minute's 60 seconds.
 *
 * `watermark` is the epoch-offset of clock_timestamp() taken immediately
 * BEFORE the batch. Every already-restamped row sits in the past (the seeded
 * window ends at the top of the current hour, always <= now), so
 * `sample_ts >= watermark` selects exactly the rows this batch wrote.
 *
 * Ordering is row_number() over (sample_ts, datid, active_count) — and it MUST
 * be row_number(), not dense_rank(). A tight sampling loop routinely fires
 * several ticks inside the same wall-clock second; dense_rank() collapses
 * those onto one virtual second, so the minute reports fewer distinct samples
 * than it holds backend counts for and AAS inflates by the sampling multiple.
 * A prototype hit exactly this and measured a 10x error.
 * ------------------------------------------------------------------------ */
create or replace procedure ash_demo.restamp(
  watermark int4,
  base_ts int4,
  idx int
)
language plpgsql
as $$
declare
  v_minute_start int4 := base_ts + (idx - 1) * 60;
  v_moved int;
  v_own_datid oid := (select database.oid
                      from pg_database as database
                      where database.datname = current_database());
begin
  /*
   * pg_ash samples the whole CLUSTER: ash.take_sample() writes one row per
   * database that had an active backend at that instant. On a developer
   * machine that quietly folds every other database on the box into the demo,
   * and the numbers stop being reproducible. Keep only this run's own
   * database. (This is "shaping which samples exist", the declared liberty —
   * it drops rows, it never edits one.)
   */
  delete from ash.sample as foreign_row
  where foreign_row.sample_ts >= watermark
    and foreign_row.datid <> v_own_datid;

  update ash.sample as target
  set sample_ts = fresh.new_ts
  from (
    select
      raw.ctid as row_id,
      raw.slot as row_slot,
      /*
       * Even spread: with ASH_SPM samples and sample_interval = 60/ASH_SPM,
       * offsets land exactly on the sampling grid, which is what makes
       * backend_seconds = samples x interval exact.
       * Integer division on purpose.
       */
      (v_minute_start
       + ((row_number() over (order by raw.sample_ts, raw.datid,
                              raw.active_count) - 1) * 60
          / count(*) over ()))::int4 as new_ts
    from ash.sample as raw
    where raw.sample_ts >= watermark
  ) as fresh
  /*
   * ctid is unique only within a partition, so the slot must be part of the
   * join key. (In practice a single run writes one slot, but a join that is
   * only accidentally correct is not correct.)
   */
  where target.ctid = fresh.row_id
    and target.slot = fresh.row_slot
    and target.sample_ts >= watermark;

  get diagnostics v_moved = row_count;

  if v_moved = 0 then
    raise exception
      'ash_demo.restamp: virtual minute % captured no samples at all — '
      'no qualifying activity was stored', idx
      using errcode = 'no_data_found',
        hint = 'Check ash.status() for enabled sampling, supported cadence, '
               'and skipped/missed/error counters. Short queries can be idle '
               'at sampling instants over TCP. Increase ASH_READ_SPAN_CALM '
               'and ASH_READ_SPAN_TAIL as documented in demos/README.md.';
  end if;
end;
$$;

/* ---------------------------------------------------------------------------
 * ash_demo.phase(base_ts, start_idx, n_minutes, spm, delay, restamp_on)
 *
 * One workload phase: n_minutes virtual minutes, each spm real samples,
 * restamped into place as soon as it is complete.
 *
 * Called once per phase from lib/seed.sh, with the phase's pgbench load
 * already running in the background. One psql session for the whole phase
 * keeps the "exactly one sampler" invariant trivially true and avoids paying
 * connection setup 28 times.
 * ------------------------------------------------------------------------ */
create or replace procedure ash_demo.phase(
  base_ts int4,
  start_idx int,
  n_minutes int,
  spm int,
  delay numeric,
  restamp_on bool default true
)
language plpgsql
as $$
declare
  v_i int;
  v_watermark int4;
begin
  for v_i in 0 .. n_minutes - 1 loop
    -- Watermark BEFORE the batch: everything at or after this is ours.
    v_watermark := extract(epoch from clock_timestamp() - ash.epoch())::int4;
    commit;

    call ash_demo.batch(spm, delay);

    if restamp_on then
      call ash_demo.restamp(v_watermark, base_ts, start_idx + v_i);
      commit;
    end if;
  end loop;
end;
$$;

/* ---------------------------------------------------------------------------
 * ash_demo.reset_state()
 *
 * Pre-seed hygiene, in the order that matters.
 *
 * The rollup watermark nulls are NOT optional. `delete from ash.rollup_1m`
 * without `last_rollup_1m_ts = null` leaves rollup_minute() convinced it has
 * already processed those minutes; it refuses to re-roll, the readers then
 * silently prefer the (now empty) rollup source for wide windows, and the
 * demo ships buckets full of nothing with no error anywhere. This is the
 * single highest-value line in the file.
 * ------------------------------------------------------------------------ */
create or replace procedure ash_demo.reset_state(sample_interval_secs numeric)
language plpgsql
as $$
begin
  -- Terminate any straggler sampler from a previous run, server-side.
  -- `pkill -f` was measured unreliable: it races the spawning shell and
  -- matches unrelated psql processes.
  -- Scoped to THIS database: on a shared cluster the harness must never reach
  -- into somebody else's session, whatever it happens to be called.
  perform pg_terminate_backend(activity.pid)
  from pg_stat_activity as activity
  where activity.application_name in ('ash_demo_sampler', 'ash_demo_load')
    and activity.datname = current_database()
    and activity.pid <> pg_backend_pid();

  delete from ash.sample;
  delete from ash.rollup_1m;
  delete from ash.rollup_1h;

  update ash.config
  set sampling_enabled   = true,
      sample_interval    = make_interval(secs => sample_interval_secs),
      last_rollup_1m_ts  = null,   -- <- see the comment above; not optional
      last_rollup_1h_ts  = null,
      skipped_samples    = 0,
      missed_samples     = 0,
      insert_errors      = 0
  where singleton;
end;
$$;
