# pg_ash 1.4 release notes

Upgrade from 1.3: `\i sql/ash-1.3-to-1.4.sql`. Fresh install or upgrade from any version: `\i sql/ash-install.sql`. The upgrade script is idempotent and safe to re-run.

## Breaking changes

### `rebuild_partitions()` now requires a `'yes'` confirmation token

`ash.rebuild_partitions(N)` drops every raw sample partition and is irreversible. To prevent accidental data loss, the function takes a second argument and refuses to proceed unless it equals `'yes'`. Wording mirrors `ash.uninstall('yes')`.

```sql
-- before: silently destructive
select ash.rebuild_partitions(9);

-- now: explicit confirmation required
select ash.rebuild_partitions(9, 'yes');
```

Calling `ash.rebuild_partitions(N)` without the `'yes'` token raises an error and changes nothing — `sampling_enabled`, pg_cron jobs, and partition tables are all left untouched. The argument-validation runs before any destructive action. (#53)

## What's new

### Long-term storage: rollup tables

Two new tables — `ash.rollup_1m` (per-minute) and `ash.rollup_1h` (per-hour) — aggregate raw samples into compact summaries that survive raw-partition rotation. Watermark-based functions populate them:

- `ash.rollup_minute()` — incrementally rolls up new raw samples into `rollup_1m`
- `ash.rollup_hour()` — rolls minutes into hours, using a custom `_int4_array_cat_agg` / `_int8_array_cat_agg` to handle minutes with different-length wait/query arrays
- `ash.rollup_cleanup()` — enforces `rollup_1m_retention_days` (default 30) and `rollup_1h_retention_days` (default 1825 = ~5 years)

New rollup readers expose the data: `ash.minute_waits`, `ash.minute_waits_at`, `ash.hourly_queries`, `ash.hourly_queries_at`, `ash.daily_peak_backends`, `ash.daily_peak_backends_at`.

`ash.start()` schedules `rollup_minute` (every minute), `rollup_hour` (every hour), and `rollup_cleanup` (daily at 3am) alongside the sampler when pg_cron is available.

### Configurable N partitions

`ash.config.num_partitions` is now configurable in `[3, 32]`. Default is 3 (unchanged). `ash.rebuild_partitions(N, 'yes')` rebuilds the raw `sample_*` and `query_map_*` partition tables; `take_sample()` and `rotate()` use dynamic SQL that walks `num_partitions` instead of the hardcoded 3.

### Privilege hardening (REVOKE from PUBLIC)

Every reader function gets a `SET search_path = pg_catalog, ash, public[, <pgss_schema>]` guard that resists shadow attacks via session search_path. Every `ash.*` function has `EXECUTE` revoked from `PUBLIC`; every reader table has `SELECT` revoked. Helpers `ash._pgss_schema()` and `ash._apply_pgss_search_path()` detect the schema where `pg_stat_statements` lives and fold it into the search_path of pgss-aware readers. Hardcoded utilities (`ash.epoch()`, `ash.ts_from_timestamptz()`, `ash.ts_to_timestamptz()`) are re-granted to `PUBLIC` since they don't access sample data. (#45)

#### Privilege provisioning helpers

`ash.grant_reader(role)` and `ash.revoke_reader(role)` provision a monitoring role (Grafana, Datadog, etc.) with the minimum privileges to call every public reader. Owner-only, idempotent, symmetric undo. (#52)

```sql
create role grafana login;
select ash.grant_reader('grafana');
-- ...later
select ash.revoke_reader('grafana');
```

The admin function set is centralized in `ash._admin_funcs()` — a single source of truth for the REVOKE-from-PUBLIC hardening block and the grant/revoke helpers. (#67)

> **Note:** After `ash.rebuild_partitions(N, 'yes')`, previously-granted reader roles lose access to the new partition tables. Re-run `ash.grant_reader(...)` for each monitoring role.

### Decoded-sample convenience overloads

`ash.decode_sample(p_sample_ts int4)` and `ash.decode_sample_at(p_ts timestamptz)` decode every row at a given timestamp without requiring the caller to fetch the packed `data` array first. The original `decode_sample(integer[], smallint)` is unchanged. (#54)

### NOTICE on out-of-window queries (relative AND absolute)

When the requested time range falls outside the retained window (older than `2 * rotation_period`, or in the future), readers emit a `NOTICE` and return empty:

- Relative-interval readers (`top_waits`, `top_queries`, etc.) — via `ash._active_slots_for(p_interval)`
- Absolute-time `_at` readers (`top_waits_at`, `samples_at`, etc.) — via the new `ash._active_slots_for_at(p_start, p_end)` helper, mirroring the relative-interval behavior. (#69)

### New observability counters

Three new bigint columns on `ash.config`, all surfaced by `ash.status()`:

- `missed_samples` — bumped when `take_sample()` catches `query_canceled` (statement_timeout or pg_cancel_backend). The sampler emits a `WARNING` and returns `-1` so callers can observe the miss. (#27, #28)
- `insert_errors` — bumped when `take_sample()`'s inner `EXCEPTION WHEN OTHERS` swallows a non-cancel insert error instead of silently dropping data. (M-BUG-4)
- `register_wait_cap_hits` — bumped when `_register_wait` skips a new `(state, type, event)` because `wait_event_map` has hit its 32 000-row cap. Counter reveals silently-dropped wait registrations. (M-BUG-6 / H-SEC-3)

`status()` also reports `epoch_seconds_remaining` and the year-2094 horizon when `sample_ts` (int4 epoch offset) overflows — sampling hard-fails with `integer out of range` past that point, NOT silently wraps. (#37)

### Misc

- `sampling_enabled` flag and `skipped_samples` counter on `ash.config`
- `ts_from_timestamptz()` / `ts_to_timestamptz()` epoch ↔ int4 helpers
- `start()` validates the interval shape before branching on pg_cron availability, re-syncs `cron.job.schedule` on re-invocation, and defends against malformed `pg_cron` extversion strings (H-BUG-1, H-BUG-2, M-BUG-8)
- Pre-truncation rollup in `rotate()` so the slot we're about to discard contributes to `rollup_1m` first
- Advisory lock protocol around `take_sample()` ↔ `rotate()` to prevent overlapping samples landing in a slot mid-rotation

## Fixes

- **No more `integer out of range` on absurd reader inputs.** Both the relative-interval readers (`top_waits('1000 years')`, etc.) and the absolute-timestamp `_at` readers (`top_waits_at('1000-01-01', …)`, etc.) now return empty rows cleanly via clamps in the readers and in `ts_from_timestamptz`. (#51, #63)
- **`sample_data_check` constraint aligned across upgrade paths.** Installs upgraded from 1.0 had a looser `array_length(data, 1) >= 2` check that 1.1 silently failed to tighten because of `create table if not exists`. An idempotent DO block in install.sql now drops + re-adds the `>= 3` form. (#49)
- **`ash.status()` no longer errors for non-superuser monitoring roles when pg_cron is loaded.** A nested `EXCEPTION WHEN insufficient_privilege` substitutes a fallback row pointing at the missing `GRANT USAGE ON SCHEMA cron`. (#61)

## CI / infra

- End-to-end pg_cron firing test passes on every PG 14–18 cron-enabled job. Provisions a `root` PG role + database to satisfy peer auth in the GHA service container; asserts via `ash.sample` row growth with a real `pg_sleep` workload. (#46)
- Schema-equivalence CI now diffs `pg_constraint` between fresh-install and chain-upgrade snapshots — catches the class of divergence behind #49. (#66)
- **Hot-path perf** improvements in `query_waits` / `query_waits_at` (window-based dedup via `named_hits` CTE), `rotate()` (single-statement `truncate ash.sample_N, ash.query_map_N restart identity`), and the `_register_wait` cap probe (`offset 49999 limit 1` replacing stale `pg_class.reltuples`). (#42)
- **Supply-chain hardening** for the GitHub Actions used to drive review and CI: every third-party action pinned to a 40-char commit SHA, per-event `author_association` gates on `claude.yml`, least-privilege `permissions:` blocks, and explicit `pull_request` (not `pull_request_target`) triggers. (#40)

## Demo

README's hero visual is a [short animated GIF](demos/) of pg_ash investigating a row-lock spike on Postgres 18. Reproducible via `make -C demos record`. Human-paced typing, colored bar charts, vanilla psql in tmux. (PRs #64, #68)

## Functions

This release adds a substantial number of functions; the README's Function reference table is the canonical list. Highlights:

| New function | Purpose |
|---|---|
| `ash.rebuild_partitions(N, p_confirm)` | Reconfigure to N raw partitions (destructive — requires `'yes'`) |
| `ash.rollup_minute()` / `rollup_hour()` / `rollup_cleanup()` | Long-term rollup pipeline |
| `ash.minute_waits` / `hourly_queries` / `daily_peak_backends` (+ `_at`) | Read rollup data |
| `ash.grant_reader(role)` / `ash.revoke_reader(role)` | Provision monitoring roles |
| `ash.decode_sample(int4)` / `decode_sample_at(timestamptz)` | Convenience overloads |
| `ash._admin_funcs()` | Canonical admin function list |
| `ash._active_slots_for_at(start, end)` | Helper for `_at` reader retention warnings |
| `ash.ts_from_timestamptz` / `ts_to_timestamptz` / `epoch` | Epoch helpers (granted to PUBLIC) |
| `ash._pgss_schema()` / `_apply_pgss_search_path()` | pgss-schema-aware search_path management |

---

# pg_ash 1.3 release notes

32 commits since v1.2. Upgrade from 1.2: `\i sql/ash-1.2-to-1.3.sql`. Fresh install or upgrade from any version: `\i sql/ash-install.sql`.

## What changed

### New: pg_cron now optional

`ash.start()` works without pg_cron — records the interval and prints `NOTICE` instructions for external scheduling (system cron, systemd timer, psql `\watch`, Python loop). `ash.stop()` reminds you to stop your external scheduler. `ash.status()` shows `no (use external scheduler)` when pg_cron is absent. (#7)

### New: set_debug_logging()

`ash.set_debug_logging(true)` enables per-session `RAISE LOG` in `take_sample()` — each sampled backend emits a log line with pid, state, wait event, backend type, and query_id. Useful for diagnosing connection pooler behavior. Goes to server log only, independent of `client_min_messages`. (#8, refs #4)

### New: pgss-dependent functions fail fast

`top_queries_with_text()`, `event_queries()`, and `event_queries_at()` now raise a clear `EXCEPTION` with a `HINT` when `pg_stat_statements` is not installed, instead of silently returning NULLs. (#14, refs #10)

### New: pgss-optional functions warn

`top_queries()`, `top_queries_at()`, `samples()`, and `samples_at()` emit a `WARNING` when `pg_stat_statements` is missing — query_text returns NULL but the function still returns sample data. (#17)

### Fixed: Azure compatibility

`cron.alter_job()` API used instead of direct `UPDATE cron.job` — works on Azure Flexible Server where the table is read-only. Nodename update skipped when unnecessary (background workers mode or socket connection). (#6, fixes #3)

### Fixed: minute and hour intervals

`ash.start('5 minutes')` and `ash.start('6 hours')` now work. Sub-minute still requires pg_cron ≥ 1.5. Non-round intervals (e.g. 90 seconds) and intervals exceeding 23 hours are rejected with clear errors. (fixes #2)

### Fixed: Azure pg_cron version strings

pg_cron version `'4-1'` (Azure format) is now parsed correctly via `regexp_replace`. Previously caused a version check failure.

### Improved: take_sample() optimization

Removed `DISTINCT` from the Read 1 `pg_stat_activity` scan. Wait event dedup uses an in-memory `text[]` seen-set instead of per-row existence checks. (#8)

### Improved: schema parity

Upgrade script (`ash-1.2-to-1.3.sql`) re-creates all 38 functions, ensuring identical function bodies between fresh install and upgrade path. CI schema equivalence test validates this on every push. (#9, #13)

### Improved: CI

PG18 added to standard test matrix (dropped pgxn-tools workaround). `pg_stat_statements` loaded in CI for pgss-dependent testing. Degraded mode tests (without pgss, without pg_cron). Shell-level `WARNING` verification via stderr capture. SQL keywords lowercased in test file. (#14, #15, #17, #18)

## Functions (38 total)

| Function | Description |
|---|---|
| `set_debug_logging(bool)` | **New** — enable/disable per-session RAISE LOG in take_sample() |

All other functions unchanged from 1.2. See README for the full reference.

---

# pg_ash 1.2 release notes

51 commits since v1.1. Upgrade from 1.1: `\i sql/ash-1.1-to-1.2.sql`. Fresh install or upgrade from any version: `\i sql/ash-install.sql`.

## What changed

### New: event_queries

`event_queries()` and `event_queries_at()` — find which queries are responsible for a specific wait event. Flexible matching: `'Lock:tuple'` (exact event), `'IO'` (all events of a type), or `'CPU*'` (synthetic). Includes bar column.

```sql
-- which queries are causing Lock:tuple?
select * from ash.event_queries('Lock:tuple', '1 hour');

-- all IO-related queries in a time window
select * from ash.event_queries_at('IO', '2026-02-17 14:00', '2026-02-17 14:05');
```

### New: session-level color toggle

Enable ANSI colors for the whole session without passing `p_color` to every call:

```sql
set ash.color = on;
select * from ash.top_waits('1 hour');
select * from ash.timeline_chart('1 hour');
```

Uses a custom GUC — works on any Postgres 9.2+ without `postgresql.conf` changes. `ash.status()` now shows the current color state.

### New: bar visualization on all wait-showing functions

Every function that shows `pct` now has a `bar` column — `query_waits`, `top_by_type`, `event_queries` (plus all `_at` variants). Bar width controlled by `p_width` (default 20), colors by `p_color` or `set ash.color = on`.

### Changed: waits_by_type renamed to top_by_type

Consistent naming with `top_waits`, `top_queries`. The old name is dropped on install/upgrade.

### Changed: top_queries_with_text columns

`mean_time_ms` renamed to `mean_exec_time_ms`. New column `total_exec_time_ms` added.

### Improved: pspg-compatible bar alignment

ANSI color escapes are zero-padded to uniform 19-byte length (`\033[38;2;080;250;123m` not `\033[38;2;80;250;123m`). The `_bar()` helper pads the full bar string to a fixed raw byte length, preventing right-border misalignment in pspg and similar tools.

### Improved: timeline_chart shows empty buckets

Quiet periods now show rows with `active = 0` instead of being omitted, so you can see the full time range.

### Improved: observer-effect protection

The sampler pg_cron command now includes `SET statement_timeout = '500ms'` to prevent `take_sample()` from becoming a problem on overloaded servers. The 500ms cap gives 10× headroom over normal ~50ms execution. Existing cron jobs are updated automatically on upgrade.

### Improved: version tracking

`ash.config` now has a `version` column. `ash.status()` shows it as the first metric. The install and migration files set it automatically.

### Improved: dynamic overload cleanup

`ash-install.sql` discovers and drops all stale function overloads by name, making it safe to run on any prior version without "function is not unique" errors.

### Fixed: event_queries crash without pg_stat_statements

Both `event_queries()` and `event_queries_at()` would crash on Postgres instances without `pg_stat_statements` loaded — the table reference was validated at plan time even in the `false` branch. Split into two `RETURN QUERY` branches.

### Fixed: IdleTx color in top_by_type

`top_by_type()` passed `'IdleTx:*'` to `_wait_color()` which used exact match. Changed to `LIKE 'IdleTx%'`.

### Fixed: upgrade path completeness

`start()` and `status()` are now re-created in the 1.1→1.2 migration, ensuring upgraded installs get the statement_timeout protection and color/version metrics.

### Security: admin functions restricted

`start()`, `stop()`, `uninstall()`, `rotate()`, and `take_sample()` are now `REVOKE`d from PUBLIC and `GRANT`ed only to the schema owner. Reader functions remain PUBLIC.

### Security: uninstall confirmation guard

`ash.uninstall()` now requires a confirmation parameter: `ash.uninstall('yes')`. Calling without the argument raises an error with usage instructions.

### Fixed: status() missing SET jit = off

`status()` was the only reader function without JIT protection. Fixed.

## Functions (37 total)

| Function | Description |
|---|---|
| `event_queries(event, interval, limit, width, color)` | **New** — top queries for a wait event |
| `event_queries_at(event, start, end, limit, width, color)` | **New** — absolute-time variant |
| `_bar(event, pct, max_pct, width, color)` | **New** — shared bar rendering helper |
| `_color_on(color)` | **New** — resolve effective color state (param or session GUC) |
| `top_by_type(interval, width, color)` | **Renamed** from `waits_by_type` — now with bar |
| `top_by_type_at(start, end, width, color)` | **Renamed** from `waits_by_type_at` — now with bar |
| `query_waits(query_id, interval, width, color)` | **Enhanced** — added bar column |
| `query_waits_at(query_id, start, end, width, color)` | **Enhanced** — added bar column |
| `top_queries_with_text(interval, limit)` | **Enhanced** — renamed/added columns |
| `start(interval)` | **Enhanced** — statement_timeout in cron command |
| `status()` | **Enhanced** — shows version, color state |

All other functions unchanged from 1.1. See README for the full reference.

---

# pg_ash 1.1 release notes

Upgrade from 1.0: `\i sql/ash-1.1.sql` — safe to run on top of a running 1.0 installation.

## What changed

### New: timeline chart

`timeline_chart()` and `timeline_chart_at()` — stacked bar chart of wait events over time, showing average active sessions per bucket. Each rank gets a distinct character — `█` (rank 1), `▓` (rank 2), `░` (rank 3), `▒` (rank 4+), `·` (Other) — so the breakdown is visible without color. ANSI colors are available as an experimental feature via `p_color => true` — green = CPU\*, blue = IO, red = Lock, pink = LWLock, cyan = IPC, yellow = Client, orange = Timeout, teal = BufferPin, purple = Activity, light purple = Extension, light yellow = IdleTx. Colors are aligned with PostgresAI monitoring. Note: psql's table formatter escapes ANSI codes; colors work in pgcli, DataGrip, unaligned mode, or piped output.

```sql
select * from ash.timeline_chart('1 hour', '5 minutes');
select * from ash.timeline_chart_at('2026-02-14 19:50', '2026-02-14 20:10', '1 minute', 5, 50);
```

Output: 4 columns — `bucket_start | active | detail | chart`. First row is a legend. `p_top` controls how many events get individual bars (the rest roll into "Other"). Default `p_top = 3`.

### Changed: histogram folded into top_waits

`histogram()` and `histogram_at()` removed. The `bar` column is now part of `top_waits()` and `top_waits_at()`, controlled by the `p_width` parameter (default 40). Same visualization, fewer functions.

### Changed: ANSI colors (experimental, off by default)

Color scheme aligned with PostgresAI monitoring: green = CPU\*, blue = IO, red = Lock, pink = LWLock, cyan = IPC, yellow = Client, orange = Timeout, teal = BufferPin, purple = Activity, light purple = Extension, light yellow = IdleTx. All colors use 24-bit RGB escape codes for consistent rendering across terminal themes.

### Fixed: pg_cron version comparison

Version check now uses `string_to_array()::int[]` comparison instead of lexicographic string comparison. The old code would incorrectly reject pg_cron 1.10+.

### Fixed: check constraint tightened

Minimum valid encoded array is 3 elements (`[-wid, count, qid]`), not 2. The CHECK constraint and sampler guard now enforce `array_length >= 3`.

### Improved: 100% test coverage

CI expanded from 16 assertions to 151. All 32 functions are directly tested across Postgres 14–18.

## Functions (32 total)

| Function | Description |
|---|---|
| `timeline_chart(interval, bucket, top, width, color)` | **New** — stacked bar chart (ANSI colors opt-in) |
| `timeline_chart_at(start, end, bucket, top, width)` | **New** — absolute-time variant |
| `_wait_color(event, color)` | **New** — ANSI color mapper (experimental) |
| `top_waits(interval, limit, width)` | Top wait events with bar chart (was without `width` in 1.0) |
| `top_waits_at(start, end, limit, width)` | Absolute-time variant with bar chart |

All other functions unchanged from 1.0. See README for the full reference.

---

# pg_ash 1.0 release notes

The first release of pg_ash — active session history for Postgres.

## What it does

pg_ash samples `pg_stat_activity` every second via pg_cron and stores wait events, query IDs, and session state in a compact encoded format. The data is queryable with plain SQL through 17 built-in reader functions covering daily review, incident investigation, and trend analysis.

## Design philosophy

**The anti-extension.** pg_ash is pure SQL + PL/pgSQL — no C code, no `shared_preload_libraries`, no restart required. Install with `\i sql/ash-1.0.sql` on any Postgres 14+ instance with pg_cron 1.5+, including managed providers: RDS, Cloud SQL, AlloyDB, Supabase, Neon.

Key design decisions:

- **Skytools PGQ-style 3-partition ring buffer** — TRUNCATE-based rotation, zero bloat, zero vacuum overhead on sample data
- **Partitioned query_map** — three per-partition dictionary tables that TRUNCATE in lockstep with sample partitions, eliminating all GC logic
- **Encoded integer arrays** — wait events and query IDs packed into `integer[]` columns (~106 bytes per row for 6 active backends), with TOAST LZ4 compression
- **Inline SQL decode** — reader functions use `generate_subscripts` and array subscript access instead of plpgsql loops, achieving ~30 ms response times on 1-hour windows
- **Per-function `set jit = off`** — prevents 10x overhead from JIT compilation on OLTP servers without affecting other workloads

## Reader functions

17 functions organized into relative-time (interval) and absolute-time (timestamptz range) variants:

| Function | Description |
|---|---|
| `top_waits(interval, limit)` | Top wait events with "Other" rollup row |
| `top_queries(interval, limit)` | Top queries by wait sample count |
| `top_queries_with_text(interval, limit)` | Same, with query text from pg_stat_statements |
| `query_waits(query_id, interval)` | Wait event profile for a specific query |
| `waits_by_type(interval)` | Wait event type distribution |
| `wait_timeline(interval, bucket)` | Time-bucketed wait event breakdown |
| `samples_by_database(interval)` | Per-database sample counts |
| `activity_summary(interval)` | One-call overview — peak backends, top waits, top queries |
| `histogram(interval, limit, width)` | Visual bar chart of wait event distribution |
| `samples(interval, limit)` | Fully decoded raw sample browser |

Absolute-time variants (`_at` suffix): `top_waits_at`, `top_queries_at`, `query_waits_at`, `waits_by_type_at`, `wait_timeline_at`, `histogram_at`, `samples_at`.

## Examples

```sql
-- what happened overnight?
select * from ash.activity_summary('8 hours');
```

```
        metric        |            value
----------------------+------------------------------
 time_range           | 08:00:00
 total_samples        | 28800
 avg_active_backends  | 16.4
 peak_active_backends | 25
 peak_time            | 2026-02-14 03:17:42+00
 databases_active     | 3
 top_wait_1           | CPU* (35.00%)
 top_wait_2           | Lock:tuple (20.00%)
 top_wait_3           | LWLock:WALWrite (13.00%)
 top_query_1          | 1234567890 (36.00%)
 top_query_2          | 9876543210 (22.00%)
 top_query_3          | 5555555555 (19.00%)
```

```sql
-- visual wait event distribution
select * from ash.top_waits('1 hour');
```

```
     wait_event       | samples |  pct  |                    bar
----------------------+---------+-------+-------------------------------------------
 CPU*                 |   18900 | 35.00 | █████████████████████████████████████ 35.00%
 Lock:tuple           |   10800 | 20.00 | █████████████████████ 20.00%
 LWLock:WALWrite      |    7020 | 13.00 | ██████████████ 13.00%
 IO:DataFileWrite     |    5940 | 11.00 | ████████████ 11.00%
 IO:DataFileRead      |    4590 |  8.50 | █████████ 8.50%
 Client:ClientRead    |    2700 |  5.00 | █████ 5.00%
 Timeout:PgSleep      |    1890 |  3.50 | ████ 3.50%
 LWLock:BufferIO      |    1080 |  2.00 | ██ 2.00%
 Lock:transactionid   |     648 |  1.20 | █ 1.20%
 Other                |     432 |  0.80 | █ 0.80%
```

```sql
-- decoded raw samples with query text
select * from ash.samples('10 minutes', 20);
```

```
       sample_time        | database_name | active_backends |   wait_event    |  query_id  |                query_text
--------------------------+---------------+-----------------+-----------------+------------+------------------------------------------
 2026-02-14 20:40:10+00   | mydb          |              12 | CPU*            | 1234567890 | select * from orders where created_at > ...
 2026-02-14 20:40:10+00   | mydb          |              12 | IO:DataFileRead | 9876543210 | update inventory set quantity = quantit...
 2026-02-14 20:40:10+00   | mydb          |              12 | Lock:tuple      | 5555555555 | insert into events (type, payload) valu...
```

## Lifecycle functions

| Function | Description |
|---|---|
| `start(interval)` | Start sampling — creates pg_cron jobs |
| `stop()` | Stop sampling — removes pg_cron jobs |
| `status()` | Show configuration, slot state, and storage metrics |
| `uninstall()` | Stop sampling and drop the ash schema |

## Storage characteristics

Measured with representative workloads (see [benchmarks](https://github.com/NikolayS/pg_ash/issues/1)):

- **Row size:** ~106 bytes for 6 active backends (measured with `pg_column_size`)
- **Daily storage:** ~30 MiB for typical workloads at 1-second sampling
- **Production max:** ~60 MiB active (2 partitions x 30 MiB/day for 50 backends)
- **WAL:** ~29 KiB per sample steady state (~2.4 GiB/day), dominated by full-page writes

## Encoding format

Each sample row contains an `integer[]` column with the format:

```
[-wait_id, count, qid, qid, ..., -next_wait_id, count, qid, ...]
```

Negative values are wait event dictionary IDs (markers), followed by a backend count, then query_map IDs for each backend in that wait state. The encoding version is tracked in `ash.config.encoding_version`.

## Synthetic wait types

- **`CPU*`** — active backend with no wait event reported. The asterisk signals ambiguity: either genuine CPU work or an uninstrumented code path. See [gaps.wait.events](https://gaps.wait.events) for details.
- **`IdleTx`** — idle-in-transaction backend with no wait event. These hold locks and block vacuum — always sampled.

## Requirements

- Postgres 14+ (requires `query_id` in `pg_stat_activity`)
- pg_cron 1.5+ for sub-minute scheduling (most managed providers ship this)
- Optional: pg_stat_statements for `top_queries_with_text()`
- `compute_query_id = on` (default since Postgres 14)

## Schema

All objects live in the `ash` schema:

- `ash.config` — singleton configuration table
- `ash.wait_event_map` — wait event dictionary (~600 entries max)
- `ash.query_map_0`, `query_map_1`, `query_map_2` — per-partition query ID dictionaries
- `ash.query_map_all` — unified view (planner eliminates non-matching partitions)
- `ash.sample` — partitioned sample table (3 partitions, ring buffer)
- Indexes on `sample_ts` per partition for time-range queries

## Design documents

Detailed design blueprints are in the [`blueprints/`](blueprints/) directory:

- **[SPEC.md](blueprints/SPEC.md)** — full specification: goals, problem statement, design decisions, encoding format, sampling strategy
- **[STORAGE_BRAINSTORM.md](blueprints/STORAGE_BRAINSTORM.md)** — storage format benchmarks comparing 8 approaches (flat rows, JSONB, hstore, `smallint[]`, `integer[]`, and more) with measured bytes/row on Postgres 17
- **[PARTITIONED_QUERYMAP_DESIGN.md](blueprints/PARTITIONED_QUERYMAP_DESIGN.md)** — per-partition query_map design: why single-table GC was replaced with lockstep TRUNCATE, edge cases, trade-offs
- **[ROLLUP_DESIGN.md](blueprints/ROLLUP_DESIGN.md)** — two-level aggregation design (per-minute 30-day, per-hour 5-year) for long-term trend analysis — planned for v1.1

Benchmark results are published in [issue #1](https://github.com/NikolayS/pg_ash/issues/1).

## What is not in 1.0

- **Rollup tables** — per-minute and per-hour aggregation for long-term trends (designed in `blueprints/ROLLUP_DESIGN.md`, implementation planned for 1.1)
- **Cross-database query text** — sampling covers all databases (via `pg_stat_activity.datid`), but `top_queries_with_text()` can only resolve query text from pg_stat_statements in the database where pg_ash is installed
- **Parallel query attribution** — parallel workers are sampled but not linked to their leader

