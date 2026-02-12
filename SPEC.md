# pg_ash — Active Session History for PostgreSQL

## 1. Goal

Provide lightweight, always-on wait event history for PostgreSQL — the equivalent of Oracle's ASH (Active Session History) — using only pure SQL and `pg_cron`. No C extensions, no `shared_preload_libraries` changes, no external agents.

Target: small-to-medium PostgreSQL clusters running on the primary, where installing `pg_wait_sampling` or similar C extensions is impractical or not allowed.

## 2. Problem Statement

PostgreSQL's `pg_stat_activity` is a point-in-time snapshot. It shows what's happening *right now*, but the moment you look away, the data is gone. This makes it nearly impossible to answer basic observability questions:

- **"What was the database waiting on at 3am?"** — You can't know unless you were watching.
- **"Which queries cause the most lock contention?"** — `pg_stat_statements` gives you timing totals but no wait event breakdown per query.
- **"Is IO or CPU the bottleneck?"** — Requires continuous sampling to see the ratio over time.
- **"Did something change after Tuesday's deploy?"** — No historical baseline to compare against.

Existing solutions:
- **pg_wait_sampling** — Excellent, but requires a C extension and `shared_preload_libraries` restart. Many managed environments (RDS, Cloud SQL) don't allow it.
- **pgsentinel** — Similar C extension limitations.
- **External monitoring agents** — Pull `pg_stat_activity` from outside, but add network overhead, require separate infrastructure, and often store data externally.

pg_ash solves this entirely inside PostgreSQL itself.

## 3. Design Decisions

### 3.1 One row per database per sample (parallel arrays)

Instead of one row per active session, we store all active sessions for a database in a single row using parallel arrays:

```
sample_ts   │ 2026-02-12 03:48:00+00
datid       │ 16384
wait_ids    │ {5, 5, 1, 0, 0, 9, 1}
query_ids   │ {-617046263269, 482910384756, 771629384756, -617046263269, 923847561029, NULL, 482910384756}
```

Position `i` in both arrays = one active backend. `wait_ids[i]` is a smallint FK to a dictionary table; `query_ids[i]` joins to `pg_stat_statements`.

**Why not JSONB?** Benchmarked on PG17 with 1 day of realistic data (8,640 samples, 20 backends/sample):

| Approach | Rows/day | Size/day | Bytes/row |
|----------|----------|----------|-----------|
| **Parallel arrays** | **8,640** | **2.5 MB** | **307** |
| JSONB grouped | 8,640 | 5.4 MB | 634 |
| Flat per-session | 172,800 | 10.2 MB | 60 |

Arrays are 2× more compact than JSONB (native binary, no per-key text framing) and 4× more compact than flat rows (20× fewer tuples, less header overhead).

**Why not flat rows?** 20× more rows means 20× more tuple headers (23 bytes each), 20× more index entries, and 20× more vacuum work. The array approach eliminates all of that.

### 3.2 Wait event dictionary (smallint IDs)

Wait events are stored as `smallint` (2 bytes) referencing a dictionary table, not as text. There are ~200 known wait events in PostgreSQL. This saves ~20 bytes per array element vs storing `"LWLock:LockManager"` as text.

```sql
create table ash.wait_event_map (
    id       smallint primary key generated always as identity,
    type     text not null,   -- 'LWLock', 'IO', 'Lock', ...
    event    text not null,   -- 'LockManager', 'DataFileRead', ...
    unique (type, event)
);
-- id=0 reserved for CPU (running, no wait event)
```

Seeded at install time from a hardcoded set of PG14+ wait events. Unknown events are auto-registered on first encounter (future-proof).

### 3.3 Only active sessions

We sample only `state = 'active'` (and optionally `idle in transaction`). Idle connections aren't interesting for wait event profiling and would bloat the arrays. This is what makes the storage manageable: we capture *work*, not *idle connections*.

### 3.4 `now()` for timestamps

`now()` (transaction time) instead of `clock_timestamp()`. All rows from the same sample tick share the exact same timestamp. Consistent, slightly cheaper, and precision within a tick doesn't matter for 1–10s sampling.

### 3.5 PGQ-style 3-partition rotation (zero bloat)

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

### 3.6 No query text storage

Query text is not stored. That's what `pg_stat_statements` is for. We store `query_id` (bigint) which joins to it. Storing query text would 10–100× the storage cost.

### 3.7 PostgreSQL 14+ minimum

`query_id` was introduced in PG14 (`compute_query_id` GUC). Without it, the core value proposition (correlating wait events to specific queries) is lost.

## 4. Schema

### 4.1 Tables

```sql
create schema ash;

-- Configuration (singleton row)
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

-- Wait event dictionary
create table ash.wait_event_map (
    id       smallint primary key generated always as identity,
    type     text not null,
    event    text not null,
    unique (type, event)
);

-- Sample data (partitioned)
create table ash.sample (
    sample_ts   timestamptz not null default now(),
    datid       oid         not null,
    wait_ids    smallint[]  not null,
    query_ids   bigint[],
    slot        smallint    not null default ash.current_slot()
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
| `ash.current_slot()` | Returns the active partition slot (0, 1, or 2) |
| `ash.take_sample()` | Snapshots `pg_stat_activity` into `ash.sample` |
| `ash._register_wait(type, event)` | Auto-inserts unknown wait events, returns id |
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

At 10-second sampling, 1 database:

| Active backends | Rows/day | Size/day | Size with 2 partitions |
|-----------------|----------|----------|------------------------|
| 5 | 8,640 | ~1 MB | ~2 MB |
| 20 | 8,640 | ~2.5 MB | ~5 MB |
| 50 | 8,640 | ~5 MB | ~10 MB |
| 100 | 8,640 | ~9 MB | ~18 MB |

At 1-second sampling, multiply size by 10× (rows become 86,400/day).

Row count depends only on sampling frequency, not backend count. Size scales with array length.

## 6. Implementation Plan

### Step 1: Schema and dictionary
- Create `ash` schema
- Create `ash.config` table with singleton constraint
- Create `ash.wait_event_map` and seed with PG14+ wait events (~200 entries)
- `id=0` reserved for CPU (no wait event)
- Create `ash._register_wait()` for auto-inserting unknown events

### Step 2: Partitioned sample table
- Create `ash.sample` partitioned by `LIST (slot)`
- Create 3 child partitions (`sample_0`, `sample_1`, `sample_2`)
- Create `ash.current_slot()` function reading from `ash.config`
- Create `sample_ts` indexes on each partition

### Step 3: Sampler function
- Create `ash.take_sample()` — snapshots active sessions from `pg_stat_activity`
- Group by `datid`, aggregate `wait_ids` and `query_ids` as parallel arrays
- Handle the wait event lookup with LEFT JOIN + `_register_wait()` fallback
- Respect `include_idle_xact` and `include_bg_workers` config flags

### Step 4: Rotation function
- Create `ash.rotate()` — advances `current_slot`, truncates recycled partition
- Must be safe to call concurrently (idempotent or locked)

### Step 5: Start/stop functions
- Create `ash.start(interval)` — schedules pg_cron jobs for sampling and rotation
- Create `ash.stop()` — unschedules pg_cron jobs
- Validate pg_cron is available before scheduling

### Step 6: Convenience views
- `ash.top_waits` — top wait events, last hour
- `ash.wait_timeline` — per-minute wait event breakdown
- `ash.top_queries` — queries with most wait samples, joined to `pg_stat_statements`
- `ash.cpu_vs_waiting` — CPU vs waiting ratio

### Step 7: Install/uninstall scripts
- `ash--1.0.sql` — single-file install (schema + tables + functions + views + seed data)
- Document `ash.start()` / `ash.stop()` / `drop schema ash cascade`

### Step 8: Testing
- Test on PG14, 15, 16, 17, 18
- Verify sampling under load (pgbench)
- Verify rotation doesn't lose data or create gaps
- Verify storage matches estimates
- Verify unknown wait events are auto-registered
- Test with `include_idle_xact = true` and `include_bg_workers = true`
- Test `ash.stop()` + `ash.start()` cycling

### Step 9: Documentation and examples
- README with quick start
- Example queries for common scenarios
- Grafana dashboard queries
- Storage planning guide

### Step 10: CI
- GitHub Actions: install pg_cron, install pg_ash, run pgbench, verify samples, verify rotation
- Test matrix: PG14, 15, 16, 17, 18

## 7. Limitations

- **1s minimum sampling** — pg_cron limit. Sub-second requires `pg_wait_sampling` (C extension).
- **Primary only** — `pg_stat_activity` on replicas doesn't show replica query wait events in the same way.
- **No per-PID tracking** — aggregated by database per sample. Can't trace one backend's journey across time. (By design — keeps storage tiny.)
- **No query text** — join `query_id` to `pg_stat_statements`.
- **Requires `compute_query_id = on`** — default since PG14, but can be turned off.

## 8. Future Ideas

- **`ash.report(interval)`** — text-based ASH report function, like Oracle's `ASHREPORT`
- **Grafana dashboard JSON** — wait event heatmap, top queries, CPU vs waiting over time
- **Aggregate rollup** — per-minute summaries kept for 30+ days (tiny storage)
- **Multi-day retention** — more partitions (e.g., 7 days = 9 partitions in 3 rotation groups)
- **Per-PID mode** — optional flat-row table for detailed per-backend tracking at higher storage cost
