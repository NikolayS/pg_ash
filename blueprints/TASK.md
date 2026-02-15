# Task: Implement pg_ash Steps 3-7 + CI

You already completed Steps 1-2. The schema is installed on PG17 at localhost:5433 user postgres.

Read SPEC.md for all design details. Add to the existing sql/ash--1.0.sql file.

## Step 3: Rotation function
- `ash.rotate()` — advance current_slot, TRUNCATE recycled partition
- pg_try_advisory_lock to prevent concurrent rotation
- Check rotated_at vs rotation_period — skip if too recent
- SET LOCAL lock_timeout = '2s'
- Truncate only the OLD previous partition by explicit name
- Update rotated_at in config
- Automatic query_map GC: DELETE WHERE last_seen < min sample_ts in live partitions

## Step 4: Start/stop/uninstall
- `ash.start(interval default '1 second')` — create pg_cron jobs. Idempotent: if jobs exist, return existing IDs. Check pg_cron >= 1.5.
- `ash.stop()` — remove pg_cron jobs, return removed IDs
- `ash.uninstall()` — calls stop() then DROP SCHEMA ash CASCADE
- NOTE: pg_cron is probably NOT available on this test server. Write the functions but handle the case where pg_cron extension doesn't exist gracefully. Test what you can.

## Step 5: Reader + diagnostic functions
- `ash.status()` — last sample ts, samples in current partition, current slot, time since rotation, dictionary utilization
- `ash.top_waits(interval, int)` — top wait events with percentages
- `ash.wait_timeline(interval, interval)` — time-bucketed breakdown  
- `ash.top_queries(interval, int)` — top queries by wait samples, LEFT JOIN pg_stat_statements if available
- `ash.waits_by_type(interval)` — wait event type distribution
- All reader functions: add WHERE slot IN (current, previous) for partition pruning. Handle datid=0 with LEFT JOIN pg_database.

## Step 7: Install/uninstall script cleanup
- Make sure ash--1.0.sql is a clean single-file install
- Dropping schema should be clean via ash.uninstall() or DROP SCHEMA ash CASCADE

## CI: GitHub Actions
- Create .github/workflows/test.yml
- Test matrix: Postgres 14, 15, 16, 17, 18
- Install pg_cron extension in CI
- Install pg_ash, run take_sample(), verify data, test decode_sample(), test rotation, test reader functions
- All tests should pass

## Testing
After implementing each step, test on psql -h localhost -p 5433 -U postgres -d postgres.
Drop and reinstall schema if needed.
Generate background activity with pg_sleep for testing.
Verify reader functions produce reasonable output.
Test rotation: insert samples, rotate, verify data survives in previous partition, verify truncated partition is empty.

When fully working with CI green, say DONE.
