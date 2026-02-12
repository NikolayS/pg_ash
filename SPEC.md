# pg_ash — Active Session History for Postgres

## 1. Goal

Provide lightweight, always-on wait event history for Postgres — the equivalent of Oracle's ASH (Active Session History) — using only pure SQL and `pg_cron`. No C extensions, no `shared_preload_libraries` changes, no external agents.

Target: small-to-medium Postgres clusters running on the primary, where installing `pg_wait_sampling` or similar C extensions is impractical or not allowed.

## 2. Problem Statement

Postgres's `pg_stat_activity` is a point-in-time snapshot. It shows what's happening *right now*, but the moment you look away, the data is gone. This makes it nearly impossible to answer basic observability questions:

- **"What was the database waiting on at 3am?"** — You can't know unless you were watching.
- **"Which queries cause the most lock contention?"** — `pg_stat_statements` gives you timing totals but no wait event breakdown per query.
- **"Is IO or CPU the bottleneck?"** — Requires continuous sampling to see the ratio over time.
- **"Did something change after Tuesday's deploy?"** — No historical baseline to compare against.

Existing solutions:
- **pg_wait_sampling** — Excellent, but requires a C extension and `shared_preload_libraries` restart. Many managed environments (RDS, Cloud SQL) don't allow it.
- **pgsentinel** — Similar C extension limitations.
- **External monitoring agents** — Pull `pg_stat_activity` from outside, but add network overhead, require separate infrastructure, and often store data externally.

pg_ash solves this entirely inside Postgres itself.

## 3. Design Decisions

### 3.1 One row per database per sample (encoded `smallint[]`)

Instead of one row per active session, we store all active sessions for a database
in a single row using a compact encoded `smallint[]`:

```
sample_ts   │ 3628080    (seconds since 2026-01-01 = 2026-02-12 03:48:00 UTC)
data        │ {-5, 3, 101, 102, 101, -1, 2, 103, 104, -0, 1, 105}
```

Encoding: `[-wait_event_id, count, query_id, query_id, ..., -next_wait, ...]`

- Negative value → wait event id (negated), from `ash.wait_event_map`
- Next value → number of backends with this wait event
- Following N values → dictionary-encoded query_ids (from `ash.query_map`)
- `wait_id = 0` (encoded as `0`) means running on CPU (no wait event)

6 active backends across 3 wait events → **1 row, 12 array elements**.

Reconstruct timestamp: `ash.epoch() + sample_ts * interval '1 second'`.

See [STORAGE_BRAINSTORM.md](STORAGE_BRAINSTORM.md) for the full design exploration
(8 approaches benchmarked on Postgres 17, 50 backends, 8,640 samples).

### 3.2 Dictionary encoding (wait events + query_ids)

**Wait events** stored as `smallint` (2 bytes) referencing `ash.wait_event_map`.
~200 known wait events in Postgres. Saves ~20 bytes per element vs text.

```sql
create table ash.wait_event_map (
  id    smallint primary key generated always as identity,
  type  text not null,   -- 'LWLock', 'IO', 'Lock', ...
  event text not null,   -- 'LockManager', 'DataFileRead', ...
  unique (type, event)
);
/* id=0 reserved for CPU (running, no wait event) */
```

**Query IDs** stored as `smallint` (2 bytes) referencing `ash.query_map`.
Most systems have 50–200 distinct query_ids. Saves 6 bytes per element vs raw `int8`.

```sql
create table ash.query_map (
  id       smallint primary key generated always as identity,
  query_id int8 not null unique
);
```

Both dictionaries are seeded on first encounter — the sampler auto-registers
unknown wait events and query_ids.

### 3.3 One install per database

pg_ash is installed per database. The `ash.sample` table stores data only for
the database it lives in — no `datid` column. If you need ASH on multiple
databases, install pg_ash in each one separately.

This saves 4 bytes/row, simplifies the sampler (no `GROUP BY datid`), and makes
reader functions cleaner.

### 3.4 Only active sessions

We sample only `state = 'active'` (and optionally `idle in transaction`). Idle connections aren't interesting for wait event profiling and would bloat the arrays. This is what makes the storage manageable: we capture *work*, not *idle connections*.

### 3.5 Compact timestamps (4 bytes)

Timestamps stored as `int4` — seconds since custom epoch `2026-01-01 00:00:00 UTC`. Good until 2094. Saves 4 bytes/row vs `timestamptz`.

```sql
-- Epoch constant
create function ash.epoch() returns timestamptz immutable language sql as
    $$select '2026-01-01 00:00:00+00'::timestamptz$$;

-- Store (in sampler)
extract(epoch from now() - ash.epoch())::int

-- Reconstruct (in queries)
ash.epoch() + (sample_ts * interval '1 second')
```

Uses `now()` (transaction time) — all rows from the same sample tick share the exact same value. Consistent and slightly cheaper than `clock_timestamp()`.

### 3.6 PGQ-style 3-partition rotation (zero bloat)

Inspired by Skype's PGQ. Three partitions rotate through roles:

```
┌─────────────┐  ┌─────────────┐  ┌─────────────┐
│  Partition 0 │  │  Partition 1 │  │  Partition 2 │
│  (previous)  │  │  (current)   │  │  (next)      │
│  READ-ONLY   │  │  INSERTING   │  │  TRUNCATED   │
└─────────────┘  └─────────────┘  └─────────────┘
```

At rotation (default: daily at midnight):
1. Advance `current_slot` → the "next" partition (already truncated, ready for writes)
2. `TRUNCATE` the old "previous" partition (becomes the new "next")

**Why this works:**
- `TRUNCATE` is instantaneous, generates minimal WAL
- Zero dead tuples, zero vacuum pressure, zero bloat — ever
- "Previous" partition always queryable (yesterday's data)
- Predictable, bounded storage — you always know exactly how much space ASH uses
- Indexes are rebuilt instantly on the empty partition after `TRUNCATE`

**Partition key:** A synthetic `slot` column (smallint, values 0/1/2) set by `ash.current_slot()`. Partitioned by `LIST (slot)`.

### 3.7 No query text storage

Query text is not stored. That's what `pg_stat_statements` is for. We store `query_id` (bigint) which joins to it. Storing query text would 10–100× the storage cost.

### 3.8 Postgres 14+ minimum

`query_id` was introduced in Postgres 14 (`compute_query_id` GUC). Without it, the core value proposition (correlating wait events to specific queries) is lost.

## 4. Schema

### 4.1 Tables

```sql
create schema ash;

/* configuration (singleton row) */
create table ash.config (
  singleton          bool primary key default true check (singleton),
  current_slot       smallint not null default 0,
  sample_interval    interval not null default '10 seconds',
  rotation_period    interval not null default '1 day',
  include_idle_xact  bool not null default false,
  include_bg_workers bool not null default false,
  rotated_at         timestamptz not null default clock_timestamp(),
  installed_at       timestamptz not null default clock_timestamp()
);

/* wait event dictionary */
create table ash.wait_event_map (
  id    smallint primary key generated always as identity,
  type  text not null,
  event text not null,
  unique (type, event)
);

/* query_id dictionary */
create table ash.query_map (
  id       smallint primary key generated always as identity,
  query_id int8 not null unique
);

/* sample data (partitioned, one table per database) */
create table ash.sample (
  sample_ts int        not null default extract(epoch from now() - ash.epoch())::int,
  data      smallint[] not null,  /* encoded: [-wait, count, qid, qid, ...] */
  slot      smallint   not null default ash.current_slot()
) partition by list (slot);

create table ash.sample_0 partition of ash.sample for values in (0);
create table ash.sample_1 partition of ash.sample for values in (1);
create table ash.sample_2 partition of ash.sample for values in (2);
```

### 4.2 Indexes

```sql
create index on ash.sample_0 (sample_ts);
create index on ash.sample_1 (sample_ts);
create index on ash.sample_2 (sample_ts);
```

### 4.3 Functions

| Function | Purpose |
|----------|---------|
| `ash.epoch()` | Returns the custom epoch (`2026-01-01 00:00:00 UTC`) |
| `ash.current_slot()` | Returns the active partition slot (0, 1, or 2) |
| `ash.take_sample()` | Snapshots `pg_stat_activity` into `ash.sample` |
| `ash._register_wait(type, event)` | Auto-inserts unknown wait events, returns id |
| `ash._register_query(int8)` | Auto-inserts unknown query_ids, returns smallint id |
| `ash.rotate()` | Advances the current slot and truncates the recycled partition |
| `ash.start(interval)` | Creates pg_cron jobs for sampling and rotation |
| `ash.stop()` | Removes pg_cron jobs |

### 4.4 Convenience Views

| View | Purpose |
|------|---------|
| `ash.top_waits` | Top wait events in the last hour |
| `ash.wait_timeline` | Wait events bucketed by minute |
| `ash.top_queries` | Queries with most wait samples |
| `ash.cpu_vs_waiting` | CPU vs waiting breakdown |

## 5. Storage Estimates

At 10-second sampling, 1 database, encoded `smallint[]` format:

| Active backends | Rows/day | Size/day | Size with 2 partitions |
|-----------------|----------|----------|------------------------|
| 5 | 8,640 | ~0.5 MiB | ~1 MiB |
| 20 | 8,640 | ~1.0 MiB | ~2.0 MiB |
| 50 | 8,640 | ~1.8 MiB | ~3.6 MiB |
| 100 | 8,640 | ~3.4 MiB | ~6.8 MiB |

At 1-second sampling, multiply size by 10× (rows become 86,400/day).

Row count depends only on sampling frequency, not backend count. Size scales with array length.

## 6. Implementation Plan

### Step 1: Core schema and infrastructure
- Create `ash` schema
- `ash.epoch()` — immutable function returning `2026-01-01 00:00:00 UTC`
- `ash.config` singleton table (current_slot, sample_interval, rotation_period, flags)
- `ash.wait_event_map` dictionary seeded with Postgres 14+ wait events (~200 entries), `id=0` = CPU
- `ash._register_wait(type, event)` — auto-inserts unknown events, returns id
- `ash.current_slot()` — returns active partition slot from config
- `ash.sample` partitioned by `LIST (slot)` with 3 child partitions + indexes

### Step 2: Sampler function (`ash.take_sample()`)
- Snapshot `pg_stat_activity` → one row per sample tick
- Group by wait event, encode into `data smallint[]` format:
  `[-wait_id, count, qid, qid, ..., -next_wait, ...]`
- Wait event lookup via `ash.wait_event_map` + `_register_wait()` fallback
- Query ID lookup via `ash.query_map` + `_register_query()` fallback
- Filter: `state = 'active'` (+ optionally `idle in transaction`),
  `backend_type = 'client backend'` (+ optionally background workers)
- Respect config flags: `include_idle_xact`, `include_bg_workers`

### Step 3: Rotation function (`ash.rotate()`)
- Advance `current_slot` to next partition (already truncated)
- `TRUNCATE` the old previous partition (recycle it as the new "next")
- Advisory lock or check to prevent concurrent rotation
- Log rotation event (optional: insert marker row or raise notice)

### Step 4: Start/stop functions
- `ash.start(interval default '10 seconds')` — schedule pg_cron jobs (sampler + rotation)
- `ash.stop()` — unschedule pg_cron jobs
- Validate pg_cron is installed before attempting schedule

### Step 5: Reader functions (human/LLM-readable output)
- `ash.top_waits(interval default '1 hour', int default 20)` — top wait events with %, human-readable
- `ash.wait_timeline(interval default '1 hour', interval default '1 minute')` — time-bucketed wait event breakdown
- `ash.top_queries(interval default '1 hour', int default 20)` — queries with most wait samples, joined to `pg_stat_statements` for query text
- `ash.cpu_vs_waiting(interval default '1 hour')` — CPU vs waiting ratio
- `ash.report(interval default '1 hour')` — full text report combining all of the above, Oracle ASHREPORT-style
- All functions return `SETOF record` or `TABLE(...)` for easy `\x` display or programmatic consumption
- All translate `int4` timestamps to human-readable `timestamptz` and `smallint` wait_ids to `type:event` text

### Step 6: Benchmarks — simulated long-running production
Simulate realistic production workloads without waiting real time:

**Data generation:**
- 50 active backends, 10s sampling, 1 database
- Realistic wait event distribution (not uniform — weight toward IO:DataFileRead, LWLock:*, CPU)
- ~20 distinct query_ids with realistic repetition patterns (zipf-like)
- Generate directly via `INSERT ... SELECT generate_series()`

**Scenarios:**
- **1 day:** 8,640 rows, ~1.9 MiB — baseline
- **1 month:** 259,200 rows, ~57 MiB — measure query latency on reader functions
- **1 year:** 3,153,600 rows, ~680 MiB — measure query latency, verify index effectiveness
- **10 years:** 31,536,000 rows, ~6.6 GiB — stress test, verify reader functions still perform

**What to measure:**
- Table + index size at each scale
- `ash.top_waits('1 hour')` query time
- `ash.top_queries('1 hour')` query time
- `ash.wait_timeline('1 hour')` query time
- `ash.report('24 hours')` query time
- Full sequential scan time (worst case)
- `TRUNCATE` time on a full partition (should be <1ms regardless of size)
- Index scan performance for time-range queries

**Rotation simulation:**
- Fill partition 0 with 1 year of data
- Call `ash.rotate()` — verify partition 1 becomes current, partition 2 gets truncated
- Verify partition 0 data survives (now "previous")
- Call `ash.rotate()` again — verify partition 0 gets truncated (instant, regardless of 1 year of data)
- Verify zero bloat after repeated rotations

**Note:** All "1 year" / "10 year" data lives in a single partition (no rotation during generation). This tests the worst case — a partition that never got rotated. In production, each partition only holds 1 rotation period (1 day by default).

### Step 7: Install/uninstall scripts
- `ash--1.0.sql` — single-file install (schema + tables + functions + seed data)
- Document `ash.start()` / `ash.stop()` / `drop schema ash cascade`

### Step 8: Testing
- Test on Postgres 14, 15, 16, 17, 18
- Verify sampling under load (pgbench)
- Verify rotation doesn't lose data or create gaps
- Verify storage matches estimates
- Verify unknown wait events are auto-registered
- Test config flags (`include_idle_xact`, `include_bg_workers`)
- Test `ash.stop()` + `ash.start()` cycling

### Step 9: Documentation
- README with quick start
- Example queries for common scenarios
- Grafana dashboard queries
- Storage planning guide

### Step 10: CI
- GitHub Actions: install pg_cron, install pg_ash, run pgbench, verify samples, verify rotation
- Test matrix: Postgres 14, 15, 16, 17, 18

## 7. Limitations

- **1s minimum sampling** — pg_cron limit. Sub-second requires `pg_wait_sampling` (C extension).
- **Primary only** — `pg_stat_activity` on replicas doesn't show replica query wait events in the same way.
- **No per-PID tracking** — aggregated by database per sample. Can't trace one backend's journey across time. (By design — keeps storage tiny.)
- **No query text** — join `query_id` to `pg_stat_statements`.
- **Requires `compute_query_id = on`** — default since Postgres 14, but can be turned off.

## 8. Future Ideas

- **`ash.report(interval)`** — text-based ASH report function, like Oracle's `ASHREPORT`
- **Grafana dashboard JSON** — wait event heatmap, top queries, CPU vs waiting over time
- **Aggregate rollup** — per-minute summaries kept for 30+ days (tiny storage)
- **Multi-day retention** — more partitions (e.g., 7 days = 9 partitions in 3 rotation groups)
- **Per-PID mode** — optional flat-row table for detailed per-backend tracking at higher storage cost
