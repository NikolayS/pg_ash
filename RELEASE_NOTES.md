# pg_ash 1.1 release notes

Upgrade from 1.0: `\i sql/ash--1.1.sql` — safe to run on top of a running 1.0 installation.

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

**The anti-extension.** pg_ash is pure SQL + PL/pgSQL — no C code, no `shared_preload_libraries`, no restart required. Install with `\i sql/ash--1.0.sql` on any Postgres 14+ instance with pg_cron 1.5+, including managed providers: RDS, Cloud SQL, AlloyDB, Supabase, Neon.

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

