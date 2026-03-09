# pg_ash 1.3 release notes

23 commits since v1.2. Upgrade from 1.2: `\i sql/ash-1.2-to-1.3.sql`. Upgrade from 1.1: `\i sql/ash-1.1-to-1.2.sql` then `\i sql/ash-1.2-to-1.3.sql`. Fresh install: `\i sql/ash-install.sql`.

## What changed

### New: debug_logging

`ash.debug_logging()` — enable per-session logging in the server log during sampling. When enabled, every sampled session emits a `RAISE LOG` message with pid, state, wait type, wait event, backend type, and query ID.

```sql
select ash.debug_logging(true);   -- enable
select ash.debug_logging(false);  -- disable
select ash.debug_logging();       -- show current state
```

Each message looks like:

```
LOG:  ash.take_sample: pid=12345 state=active wait_type=CPU* wait_event=CPU* backend_type=client backend query_id=1234567890
```

`RAISE LOG` goes directly to the server log — it is independent of `log_min_messages` and `client_min_messages`, and never appears in the psql client. Zero overhead when disabled (default).

Useful for diagnosing connection pooler issues where `Client:ClientRead` appears on pooler-parked backends (refs [#4](https://github.com/NikolayS/pg_ash/issues/4)).

### Fixed: Azure / managed-service compatibility

On Azure PostgreSQL Flexible Server, direct DML against `cron.job` is blocked — only pg_cron API functions are permitted. pg_ash used `UPDATE cron.job SET nodename = ''` which failed with `ERROR: permission denied for table job`.

Fixed: use `cron.alter_job()` for command updates, and skip the `nodename` UPDATE when it's unnecessary:

- `cron.use_background_workers = on` — nodename is irrelevant (no libpq connections)
- `cron.host = ''` or a socket path — `cron.schedule()` already inherits the correct value

Fixes [#3](https://github.com/NikolayS/pg_ash/issues/3).

### Fixed: non-standard pg_cron version strings

Azure reports pg_cron version as `'4-1'` instead of `'1.5'`. The version comparison now strips non-numeric characters before parsing, so `'4-1'` parses as `[4, 1]` which passes the `>= 1.5` check.

### New: minute and hour intervals in ash.start()

`ash.start()` now accepts intervals beyond 1–59 seconds:

```sql
select ash.start('5 minutes');   -- cron: */5 * * * *
select ash.start('1 hour');      -- cron: 0 * * * *
select ash.start('6 hours');     -- cron: 0 */6 * * *
```

Intervals must be exact minutes (60s, 120s, ...) or exact hours (3600s, 7200s, ..., up to 23 hours). Non-round intervals are rejected with a clear error message.

Fixes [#2](https://github.com/NikolayS/pg_ash/issues/2).

### Improved: take_sample() Read 1 loop

The first `pg_stat_activity` scan in `take_sample()` no longer uses `DISTINCT`. Instead, an in-memory `text[]` seen-set deduplicates wait events without an implicit sort. One fewer catalog operation per tick.

### Improved: status() shows debug_logging

`ash.status()` now includes `debug_logging` in its output.

### Improved: upgrade path — full schema parity

The 1.2→1.3 upgrade script re-creates all 38 functions (not just the ones that changed). This guarantees identical function bodies between fresh install and any upgrade path (1.0→1.1→1.2→1.3). Schema equivalence is verified in CI.

### Improved: CI

- pg_cron installed inside Docker container for reliable interval format testing
- Comprehensive interval format tests (seconds, minutes, hours, edge cases)
- Schema equivalence test: fresh install vs full upgrade path must produce identical schemas
- All upgrade paths (1.0→1.1→1.2→1.3, 1.1→1.2→1.3, idempotent re-apply) tested
- Tested on PostgreSQL 14, 15, 16, 17, and 18

## Functions (38 total)

| Function | Description |
|---|---|
| `debug_logging(enabled)` | **New** — enable/disable per-session RAISE LOG in take_sample() |

All other functions unchanged from 1.2. See README for the full reference.

## Open issues

- [#4](https://github.com/NikolayS/pg_ash/issues/4) — Idle state included causing false info with connection pooler. `debug_logging()` helps investigate; user testing in progress.

## What's next (v1.4)

- **pg_cron optional** — `ash.start()` without pg_cron, with instructions for external scheduling (system cron, systemd timer, psql `\watch`, loop script). PR [#7](https://github.com/NikolayS/pg_ash/pull/7) in progress.
- **Rollup tables** — per-minute and per-hour aggregation for long-term trends (designed in `blueprints/ROLLUP_DESIGN.md`).
