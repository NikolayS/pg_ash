# pg_ash — Active Session History for Postgres

## 1. Goal

Provide lightweight, always-on wait event history for Postgres — the equivalent of Oracle's ASH (Active Session History) — using only pure SQL and `pg_cron`. No C extensions, no `shared_preload_libraries` changes, no external agents.

Target: small-to-medium Postgres clusters running on the primary, where installing `pg_wait_sampling` or similar C extensions is impractical or not allowed.

## 2. Problem Statement

Postgres's `pg_stat_activity` shows what's happening *right now*, but the moment you look away, the data is gone. (Technically, it reads from shared memory row by row — not an atomic snapshot — but it's the best we have.) This makes it nearly impossible to answer basic observability questions:

- **"What was the database waiting on at 3am?"** — You can't know unless you were watching.
- **"Which queries cause the most lock contention?"** — `pg_stat_statements` gives you timing totals but no wait event breakdown per query.
- **"Is IO or CPU the bottleneck?"** — Requires continuous sampling to see the ratio over time.
- **"Did something change after Tuesday's deploy?"** — No historical baseline to compare against.

Existing solutions:
- **pg_wait_sampling** — Excellent, but requires a C extension and `shared_preload_libraries` restart. Many managed environments (RDS, Cloud SQL) don't allow it.
- **pgsentinel** — Similar C extension limitations.
- **External monitoring agents** — Pull `pg_stat_activity` from outside, but network round-trip limits sampling frequency and adds latency. Typically 10–60s intervals at best.

pg_ash solves the sampling problem entirely inside Postgres itself:
1. **Higher frequency** — 1s sampling with zero network RTT (in-process SQL)
2. **Self-diagnostics** — the database can analyze its own wait event history without external infrastructure

External monitoring systems are still valuable for long-term storage and trend analysis. pg_ash keeps only "today" and "yesterday" (2 rotation periods). Monitoring agents can periodically export pg_ash data for longer retention — getting the best of both worlds: high-frequency local sampling + long-term external storage.

## 3. Design Decisions

### 3.1 One row per database per sample tick (encoded `integer[]`)

Instead of one row per active session, we store all active sessions for a database
in a single row using a compact encoded `integer[]` (int4):

```
sample_ts   │ 3628080    (seconds since 2026-01-01 = 2026-02-12 03:48:00 UTC)
datid       │ 16384
data        │ {-5, 3, 101, 102, 101, -1, 2, 103, 104, -1, 1, 105}
```

Encoding: `[-wait_event_id, count, query_map_id, query_map_id, ..., -next_wait, ...]`

- Negative value → wait event id (negated), from `ash.wait_event_map`
- Next value → number of backends with this wait event
- Following N values → dictionary-encoded query_ids (from `ash.query_map`, int4 PK)
- Wait event IDs start at 1 (`id=1` = CPU). This avoids the `-0 = 0` ambiguity
  in the encoding — every wait marker is strictly negative.
- `0` in a query_id position = unknown/NULL query_id (sentinel)

6 active backends across 3 wait events → **1 row, 12 array elements**.

Reconstruct timestamp: `ash.epoch() + sample_ts * interval '1 second'`.

**Why `integer[]` not `smallint[]`:** `smallint` caps at 32,767 distinct
query_map entries. Busy systems with ORMs, ad-hoc SQL, and schema changes can
approach this over months. `integer` (int4) gives 2 billion entries —
effectively unlimited. Cost: ~390 bytes/row vs 221 for smallint[] (50 backends).
At 1s sampling: ~33 MiB/day. Still very manageable.

**Structural validation:** The encoding is positional with no framing — a
malformed array silently produces garbage downstream. `ash._validate_data()`
walks the array and checks structural integrity (negative marker → positive
count → N query_ids → next marker). Used in reader functions and optionally
as a `CHECK` constraint.

See [STORAGE_BRAINSTORM.md](STORAGE_BRAINSTORM.md) for the full design exploration
(8 approaches benchmarked on Postgres 17, 50 backends, 8,640 samples).

### 3.2 Dictionary encoding (wait events + query_ids)

**Wait events** stored as `smallint` (2 bytes) referencing `ash.wait_event_map`.
The dictionary key is `(state, type, event)` — so `active|IO:DataFileRead` and
`idle in transaction|IO:DataFileRead` get separate IDs. ~200 wait events × 3 states
= ~600 entries max, fits comfortably in `smallint`.

```sql
create table ash.wait_event_map (
  id    smallint primary key generated always as identity,
  state text not null,   -- 'active', 'idle in transaction', 'idle in transaction (aborted)'
  type  text not null,   -- 'LWLock', 'IO', 'Lock', ...
  event text not null,   -- 'LockManager', 'DataFileRead', ...
  unique (state, type, event)
);
/* IDs start at 1 to avoid -0 = 0 ambiguity in the encoding */
```

**Query IDs** stored as `int4` (4 bytes) referencing `ash.query_map`.
Maps `int8` query_ids to compact `int4` dictionary IDs. 2 billion capacity —
effectively unlimited.

```sql
create table ash.query_map (
  id       int4 primary key generated always as identity,
  query_id int8 not null unique
);
/* id=0 reserved: sentinel for NULL/unknown query_id */
```

The rotation function optionally garbage-collects `query_map` entries not
referenced in any live partition, to keep the table small and lookups fast.

Both dictionaries are populated on first encounter via
`INSERT ... ON CONFLICT DO NOTHING` + CTE fallback to `SELECT` existing ID.
This is concurrency-safe — if two `take_sample()` calls race (manual + pg_cron),
neither blocks and no sample is lost.

**Sampler NULL handling for `wait_event_map`:**

```sql
/* in the sampler query */
coalesce(sa.wait_event_type, '') as type,
coalesce(sa.wait_event,
  case
    when sa.state = 'active' then 'CPU'
    when sa.state like 'idle in transaction%' then 'IDLE'
  end
) as event
```

This maps `active` + no wait event → `active|:CPU` and
`idle in transaction` + no wait event → `idle in transaction|:IDLE`.

### 3.3 Single install, all databases

pg_ash is installed once (in the pg_cron database, typically `postgres`) and
samples all active backends across all databases. Each sample row includes `datid`
so you can filter by database or view server-wide load.

This is essential for "is the server overloaded?" analysis — you need all backends
in one place, not scattered across per-database installs.

### 3.4 Active + idle-in-transaction sessions

We sample `state in ('active', 'idle in transaction', 'idle in transaction (aborted)')`.

- **Active** — currently executing, the core of wait event analysis.
- **Idle in transaction** — not executing, but holding locks and blocking vacuum. These are silent killers — invisible in wait event profiles but causing real damage. Capturing them answers "why did autovacuum stall at 3am?"

Purely `idle` connections are excluded — they're just sleeping and don't affect anything. This keeps the arrays manageable: we capture *work and held resources*, not *idle connections*.

Background workers (autovacuum, WAL sender, etc.) are optionally included via `include_bg_workers` config flag (off by default).

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

Uses `now()` (transaction time) — gives one consistent timestamp per sample tick, even though `pg_stat_activity` itself is not an atomic snapshot (each row is read from shared memory at slightly different times). Good enough for 1s resolution observability.

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

`query_id` was introduced in Postgres 14 (`compute_query_id` GUC). Without it,
the core value proposition (correlating wait events to specific queries) is lost.

### 3.9 Required privileges

The sampler role needs `pg_read_all_stats` (or superuser) to see all backends
in `pg_stat_activity`. Without it, you only see your own sessions — useless
for server-wide observability.

Joining to `pg_stat_statements` for query text also requires
`pg_read_all_stats` or membership in `pg_monitor`.

### 3.10 NULL handling

- **`query_id` is NULL:** Common for utility commands, DDL, or when
  `compute_query_id = off`. Stored as `0` in the encoded array (sentinel
  meaning "unknown query"). Does not consume a `query_map` entry.
- **`datid` is NULL:** Possible for background workers without a database
  context. Stored as `0::oid`. Only relevant when `include_bg_workers = true`.
- **`wait_event` is NULL with `state = 'active'`:** Backend is running on CPU.
  Mapped to `active|CPU` in `wait_event_map`.
- **`wait_event` is NULL with `state = 'idle in transaction'`:** Backend is
  idle in a transaction, not waiting on anything specific. Mapped to
  `idle in transaction|IDLE` in `wait_event_map`.

### 3.11 pg_cron version and UTC

Requires pg_cron >= 1.5 for second-granularity schedules (`'1 second'`).
Some managed providers may ship older versions — check `select * from
pg_available_extensions where name = 'pg_cron'`.

pg_cron interprets cron expressions in UTC. The midnight rotation
(`0 0 * * *`) fires at midnight UTC, not local time. This is fine for
pg_ash — rotation boundaries don't need to align with local midnight.

## 4. Schema

### 4.1 Tables

```sql
create schema ash;

/* configuration (singleton row) */
create table ash.config (
  singleton          bool primary key default true check (singleton),
  current_slot       smallint not null default 0,
  sample_interval    interval not null default '1 second',
  rotation_period    interval not null default '1 day',
  include_bg_workers bool not null default false,
  rotated_at         timestamptz not null default clock_timestamp(),
  installed_at       timestamptz not null default clock_timestamp()
);

/* wait event dictionary — keyed by (state, type, event) */
create table ash.wait_event_map (
  id    smallint primary key generated always as identity,
  state text not null,  -- 'active', 'idle in transaction', ...
  type  text not null,  -- 'LWLock', 'IO', 'Lock', ...
  event text not null,  -- 'LockManager', 'DataFileRead', ...
  unique (state, type, event)
);

/* query_id dictionary */
create table ash.query_map (
  id       int4 primary key generated always as identity,
  query_id int8 not null unique
);

/* sample data (partitioned) */
create table ash.sample (
  sample_ts int       not null default extract(epoch from now() - ash.epoch())::int,
  datid     oid       not null,
  data      integer[] not null,  /* encoded: [-wait, count, qid, qid, ...] */
  slot      smallint  not null default ash.current_slot()
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
| `ash._register_wait(state, type, event)` | Auto-inserts unknown wait events, returns id |
| `ash._register_query(int8)` | Auto-inserts unknown query_ids, returns int4 id |
| `ash._validate_data(integer[])` | Validates encoded array structure, returns bool |
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

Default: **1-second sampling** (matches Oracle ASH). Encoded `integer[]` format, 1 database:

| Active backends | Rows/day | Size/day | Size with 2 partitions |
|-----------------|----------|----------|------------------------|
| 5 | 86,400 | ~8 MiB | ~16 MiB |
| 20 | 86,400 | ~18 MiB | ~36 MiB |
| 50 | 86,400 | ~33 MiB | ~66 MiB |
| 100 | 86,400 | ~62 MiB | ~124 MiB |

At 10-second sampling, divide by 10×.

Row count depends only on sampling frequency, not backend count. Size scales with array length.

## 6. Implementation Plan

### Step 1: Core schema and infrastructure
- Create `ash` schema
- `ash.epoch()` — immutable function returning `2026-01-01 00:00:00 UTC`
- `ash.config` singleton table (current_slot, sample_interval, rotation_period, flags)
- `ash.wait_event_map` dictionary keyed by `(state, type, event)` — seeded on first encounter, `id=1` = `active|CPU`
- `ash._register_wait(state, type, event)` — auto-inserts unknown events, returns id
- `ash.current_slot()` — returns active partition slot from config
- `ash.sample` partitioned by `LIST (slot)` with 3 child partitions + indexes

### Step 2: Sampler function (`ash.take_sample()`)
- Snapshot `pg_stat_activity` → one row per database per sample tick
- Group by `datid` and wait event, encode into `data smallint[]` format:
  `[-wait_id, count, qid, qid, ..., -next_wait, ...]`
- Wait event lookup via `ash.wait_event_map` + `_register_wait()` fallback
- Query ID lookup via `ash.query_map` + `_register_query()` fallback
- Filter: `state in ('active', 'idle in transaction', 'idle in transaction (aborted)')`,
  `backend_type = 'client backend'` (+ optionally background workers)
- Respect config flag: `include_bg_workers`
- Performance: select only needed columns, filter early on `state` and
  `backend_type` to minimize shared memory reads
- Dictionary registration via `INSERT ... ON CONFLICT DO NOTHING` + `RETURNING`
  (concurrency-safe, no locks)

### Step 3: Rotation function (`ash.rotate()`)
- Advance `current_slot` to next partition (already truncated)
- `TRUNCATE` the old previous partition (recycle it as the new "next")
- Optionally garbage-collect `ash.query_map`: delete entries not referenced in
  any live partition (keeps table small, lookups fast)
- Advisory lock to prevent concurrent rotation
- **Edge case:** If `take_sample()` is mid-flight during rotation (read
  `current_slot`, then rotation advances, then insert), the sample lands in
  the old slot. This is harmless for data correctness but means the "previous"
  partition may briefly have samples newer than "current"'s oldest. Reader
  functions must query by `sample_ts` range, not by partition identity.
- Log rotation event (optional: raise notice)

### Step 4: Start/stop functions
- `ash.start(interval default '1 second')` — schedule pg_cron jobs (sampler + rotation)
- `ash.stop()` — unschedule pg_cron jobs
- Validate pg_cron is installed before attempting schedule
- Install pg_ash in the same database as pg_cron (typically `postgres`).
  The sampler reads `pg_stat_activity` which shows all backends across all databases.

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
- 50 active backends, 1s sampling, 1 database
- Realistic wait event distribution (not uniform — weight toward IO:DataFileRead, LWLock:*, CPU)
- ~20 distinct query_ids with realistic repetition patterns (zipf-like)
- Generate directly via `INSERT ... SELECT generate_series()`

**Scenarios (50 active backends, 1s sampling, 1 database):**
- **1 day:** 86,400 rows, ~33 MiB — baseline
- **1 month:** 2,592,000 rows, ~1 GiB — measure query latency on reader functions
- **1 year:** 31,536,000 rows, ~12 GiB — measure query latency, verify index effectiveness
- **10 years:** 315,360,000 rows, ~120 GiB — stress test, verify reader functions still perform

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
- Test config flag `include_bg_workers`
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
- **Requires pg_cron >= 1.5** — for second-granularity scheduling. Some managed providers ship older versions.
- **Requires `pg_read_all_stats`** — sampler role must see all backends in `pg_stat_activity`.

## 8. Future Ideas

- **Grafana dashboard JSON** — wait event heatmap, top queries, CPU vs waiting over time
- **Aggregate rollup** — per-minute summaries kept for 30+ days (tiny storage)
- **Multi-day retention** — more partitions (e.g., 7 days = 9 partitions in 3 rotation groups)
- **Per-PID mode** — optional flat-row table for detailed per-backend tracking at higher storage cost
