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
data        │ {1, -5, 3, 101, 102, 101, -1, 2, 103, 104, -1, 1, 105}
```

First element `1` = format version. Rest follows the encoding rules above.

Encoding: `[version, -wait_event_id, count, query_map_id, ..., -next_wait, ...]`

- `data[1]` = format version (`1` for v1). Enables future evolution
  (new fields, different grouping) without breaking reader functions.
  Reader functions check version and dispatch accordingly. If a reader
  encounters an unknown version, it must `RAISE WARNING` and return zero
  rows for that sample — not crash. This matters when v2 is deployed but
  old reader functions haven't been upgraded yet.
- Negative value → wait event id (negated), from `ash.wait_event_map`
- Next positive value → number of backends with this wait event
- Following N values → dictionary-encoded query_ids (from `ash.query_map`, int4 PK)
- **Invariant:** `count` MUST equal the number of following query_id elements
  before the next negative marker (or end of array). The encoder guarantees
  this; `_validate_data()` checks it. `count` exists for fast summation without
  walking individual elements.
- Wait event IDs start at 1 (`id=1` = CPU). This avoids the `-0 = 0` ambiguity
  in the encoding — every wait marker is strictly negative.
- `0` in a query_id position = unknown/NULL query_id (sentinel)

6 active backends across 3 wait events → **1 row, 13 array elements** (1 version + 12 data).

Reconstruct timestamp: `ash.epoch() + sample_ts * interval '1 second'`.

**Why `integer[]` not `smallint[]`:** `smallint` caps at 32,767 distinct
query_map entries. Busy systems with ORMs, ad-hoc SQL, and schema changes can
approach this over months. `integer` (int4) gives 2 billion entries —
effectively unlimited. Cost: ~390 bytes/row vs 221 for smallint[] (50 backends).
At 1s sampling: ~33 MiB/day. Still very manageable.

**Structural validation:** The encoding is positional with no framing — a
malformed array silently produces garbage downstream. Two levels of validation:

- **Cheap CHECK constraint** on the table: `data[1] = 1 AND array_length(data, 1) >= 3`.
  Catches gross corruption without adding CPU on the hot 1s insert path.
- **`ash._validate_data()`** — full structural walk (negative marker → positive
  count → N query_ids → next marker). Used in reader functions and `ash.status()`
  diagnostics. Never on the insert path.

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

The `query_map` table grows monotonically — no garbage collection. See §Step 3
for rationale.

Both dictionaries are populated on first encounter via
`INSERT ... ON CONFLICT DO NOTHING` + CTE fallback to `SELECT` existing ID.
This is concurrency-safe — if two `take_sample()` calls race (manual + pg_cron),
neither blocks and no sample is lost.

**Sampler NULL handling for `wait_event_map`:**

```sql
/* in the sampler query */
coalesce(sa.wait_event_type,
  case
    when sa.state = 'active' then 'CPU'
    when sa.state like 'idle in transaction%' then 'IDLE'
  end
) as type,
coalesce(sa.wait_event,
  case
    when sa.state = 'active' then 'CPU'
    when sa.state like 'idle in transaction%' then 'IDLE'
  end
) as event
```

This maps:
- `active` + no wait event → `(state='active', type='CPU', event='CPU')`
- `idle in transaction` + no wait event → `(state='idle in transaction', type='IDLE', event='IDLE')`

The `type` field is `'CPU'` / `'IDLE'` (not empty string) so decoded output
is self-describing. `coalesce(wait_event_type, '')` in the sampler query is
an intermediate step; the dictionary stores the clean synthetic type.

**Decoder sentinel handling:** `decode_sample()` must not join `query_map`
on `id = 0` (the sentinel for NULL/unknown `query_id`). Return `NULL` for
`query_id` when the encoded value is `0`.

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
extract(epoch from now() - ash.epoch())::int4

-- Reconstruct (in queries)
ash.epoch() + (sample_ts * interval '1 second')
```

Uses `now()` (transaction start time). Each pg_cron job runs in its own
transaction, so `now()` gives one consistent timestamp per sample tick.
This is the right choice: if two samples somehow race (manual + cron),
they get distinct transaction timestamps. And `now()` is cheaper than
`clock_timestamp()` — no syscall per invocation.

**Manual invocation warning:** `SELECT ash.take_sample()` must be called as
a standalone statement, not inside a longer transaction. Repeated calls
within the same transaction get the same `now()` → duplicate timestamps.
Document this in README.

**Precision note:** `extract(epoch ...)` returns `double precision`.
Casting to `int4` truncates sub-second fractions — this is intentional.
We sample at 1s resolution; sub-second precision would waste storage and
complicate queries. The `int4` range (2,147,483,647 seconds from epoch)
covers until 2094 — well beyond any reasonable planning horizon.

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
1. `pg_try_advisory_lock(hashtext('ash_rotate'))` — if false, another rotation is
   in progress, return immediately
2. Check `rotated_at` vs `rotation_period` — if rotated too recently, skip
   (prevents double-rotation from duplicate cron fires or manual calls)
3. `SET LOCAL lock_timeout = '2s'` — if a long analytics query holds a read lock
   on the partition being truncated, abort and retry next cron tick rather than
   blocking the sampler
4. Advance `current_slot` → the "next" partition (already truncated, ready for writes)
5. `TRUNCATE` the old "previous" partition (becomes the new "next")
6. Update `rotated_at = now()` in `ash.config`

**Why this works:**
- `TRUNCATE` is instantaneous, generates minimal WAL
- Zero dead tuples, zero vacuum pressure, zero bloat — ever
- "Previous" partition always queryable (yesterday's data)
- Predictable, bounded storage — you always know exactly how much space ASH uses
- Indexes are rebuilt instantly on the empty partition after `TRUNCATE`

**Partition key:** A synthetic `slot` column (smallint, values 0/1/2) set by `ash.current_slot()`. Partitioned by `LIST (slot)`.

**Double-rotation safety:** What if rotation fires twice before the sampler
runs? (pg_cron hiccup, long-running sampler, clock skew.) The second rotation
would advance the slot again, and the sampler's in-flight insert (using the
stale `current_slot` it read before the first rotation) would land in a
partition that just got truncated.

Defense: the `rotated_at` check in step 2 is the primary guard — it rejects
any rotation that fires within `rotation_period * 0.9` of the last one. The
advisory lock (step 1) prevents concurrent rotation. Together, these make
double-rotation effectively impossible in normal operation. In the pathological
case where the system clock jumps forward by >1 day, the admin will need to
manually verify data integrity — but that's a broken clock problem, not a
pg_ash problem.

**Failed TRUNCATE recovery:** If step 5 (TRUNCATE) fails due to `lock_timeout`,
`current_slot` has already been advanced (step 4). This means:
- `take_sample()` inserts into the correct new partition — fine.
- The old "previous" partition wasn't truncated — 3 partitions have data.
- Next rotation: the un-truncated partition is now two rotations old. It becomes
  the new "next" and gets truncated at that point.
- Self-healing after one extra cycle. During that cycle you have 3 partitions
  of data instead of 2. Benign — slightly more storage, no data loss.

### 3.7 No query text storage

Query text is not stored. That's what `pg_stat_statements` is for. We store `query_id` (bigint) which joins to it. Storing query text would 10–100× the storage cost.

### 3.8 Postgres 14+ minimum

`query_id` was introduced in Postgres 14 (`compute_query_id` GUC). Without it,
the core value proposition (correlating wait events to specific queries) is lost.

### 3.9 Required privileges

The sampler role needs `pg_read_all_stats` (or superuser) to see all backends
in `pg_stat_activity`. Without it, you only see your own sessions — useless
for server-wide observability.

**Managed providers:** Many managed Postgres services (RDS, Cloud SQL) don't
grant `pg_read_all_stats` to arbitrary roles. The sampler typically needs to
run as the managed "admin" role (e.g., `rds_superuser` on RDS, `cloudsqlsuperuser`
on Cloud SQL). Document provider-specific setup in README.

**pg_stat_statements is optional.** Reader functions that join `query_id` to
`pg_stat_statements` for query text need `pg_read_all_stats` or `pg_monitor`
membership. But all reader functions must work without it — show `query_id`
only when pg_stat_statements is not available or the user lacks privileges.
This keeps pg_ash useful even in locked-down environments.

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

### 3.11 pg_cron >= 1.5 required

Requires pg_cron >= 1.5 for second-granularity scheduling. This version
introduced interval-based scheduling (`'1 second'`) and
`cron.schedule_in_database()`. Available on most managed providers
(RDS, Cloud SQL, AlloyDB, Crunchy Bridge, Neon, Supabase).

`ash.start()` checks the installed pg_cron version and raises an error
if < 1.5.

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

/* sample data (partitioned)
 *
 * sample_ts is int4 (seconds since ash.epoch()), not timestamptz —
 * see §3.5 for why. config uses timestamptz for human-readable operational
 * timestamps; sample uses int4 for compact storage at scale.
 */
create table ash.sample (
  sample_ts      int4      not null,  /* seconds since ash.epoch(), from now() */
  datid          oid       not null,
  active_count   smallint  not null,  /* sum of counts in data[]; denormalized for
                                      * fast filtering: WHERE active_count > 50
                                      * without decoding the array */
  data           integer[] not null
                   check (data[1] = 1 and array_length(data, 1) >= 3),
  slot           smallint  not null default ash.current_slot()
) partition by list (slot);

create table ash.sample_0 partition of ash.sample for values in (0);
create table ash.sample_1 partition of ash.sample for values in (1);
create table ash.sample_2 partition of ash.sample for values in (2);
```

### 4.2 Indexes

```sql
/* composite index — most queries filter by datid + time range */
create index on ash.sample_0 (datid, sample_ts);
create index on ash.sample_1 (datid, sample_ts);
create index on ash.sample_2 (datid, sample_ts);
```

For single-database deployments, the `datid` leading column costs nothing
(all values are the same → index degenerates to `sample_ts` scan). For
multi-database servers, it enables efficient per-database filtering.

Alternative: BRIN on `sample_ts` — tiny index, fast for sequential time
range scans. Worth benchmarking at Step 6, but B-tree is the safe default.

### 4.3 Functions

| Function | Purpose |
|----------|---------|
| `ash.epoch()` | Returns the custom epoch (`2026-01-01 00:00:00 UTC`) |
| `ash.current_slot()` | Returns the active partition slot (0, 1, or 2) |
| `ash.take_sample()` | Snapshots `pg_stat_activity` into `ash.sample` |
| `ash._register_wait(state, type, event)` | Auto-inserts unknown wait events, returns id |
| `ash._register_query(int8)` | Auto-inserts unknown query_ids, returns int4 id |
| `ash._validate_data(integer[])` | Validates encoded array structure, returns bool |
| `ash.decode_sample(integer[])` | Decodes array → `TABLE(state text, type text, event text, query_id int8, count int)` |
| `ash.rotate()` | Advances the current slot and truncates the recycled partition |
| `ash.start(interval)` | Creates pg_cron jobs, returns job IDs |
| `ash.stop()` | Removes pg_cron jobs, returns removed job IDs |
| `ash.uninstall()` | Calls `stop()` then `DROP SCHEMA ash CASCADE` |
| `ash.status()` | Diagnostic dashboard: last sample ts, samples in current partition, current slot, time since last rotation, pg_cron job status, dictionary utilization (wait_event_map count vs smallint max, query_map count) |

### 4.4 Reader functions

All readers are **functions** (not views) — they take time range and limit
parameters. Views with hardcoded `'1 hour'` are less useful than they look.
See §4.3 for the full function list.

**Partition scanning note:** Since `ash.sample` is partitioned by `slot`
(not by `sample_ts`), Postgres's partition pruning cannot eliminate
partitions based on `WHERE sample_ts >= X`. However, reader functions can
add `WHERE slot IN (current, previous)` (computed from `ash.current_slot()`)
to prune the empty "next" partition. Marginal gain, but it's one line of
code and conceptually clean. The `(datid, sample_ts)` B-tree index does
the real filtering work.

**`datid = 0` handling:** When `include_bg_workers = true`, background
workers without a database context produce `datid = 0::oid`. This doesn't
join to `pg_database`. Reader functions that show database names must use
`LEFT JOIN pg_database ON datid = oid` and display `'<background>'` or
similar for `datid = 0`.

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
- `sample_ts` derived from `now()` — one consistent timestamp per tick
  (each pg_cron invocation is its own transaction)
- Snapshot `pg_stat_activity` → one row per database per sample tick
- Group by `datid` and wait event, encode into `data integer[]` format:
  `[1, -wait_id, count, qid, qid, ..., -next_wait, ...]` (leading `1` = format v1)
- **Dictionary strategy — two different approaches:**
  - **wait_event_map**: cache fully at function start (~600 rows max). Tiny,
    fits in a plpgsql hstore. Per-backend lookups hit local memory.
  - **query_map**: do NOT cache fully (can be millions of rows in ORM-heavy
    workloads). Instead: (1) collect distinct `query_id`s seen in this tick
    into a temp table, (2) bulk `INSERT ... ON CONFLICT DO NOTHING` into
    `ash.query_map`, (3) join back to get the `int4` IDs. This is O(distinct
    query_ids per tick), not O(total query_map size). Use `ON COMMIT DROP`
    temp table — pg_cron job runs in its own transaction.
- Wait event NULL handling: `active` + no wait → `active||CPU`;
  `idle in transaction` + no wait → `idle in transaction||IDLE`
- Filter: `state in ('active', 'idle in transaction', 'idle in transaction (aborted)')`,
  `backend_type = 'client backend'` (+ optionally background workers)
- Respect config flag: `include_bg_workers`
- Store `active_count` per row — total sampled backends for this datid+tick
- Performance: select only needed columns, filter early on `state` and
  `backend_type` to minimize shared memory reads
- **Error handling:** Per-row `BEGIN ... EXCEPTION WHEN OTHERS` around
  encoding. If a backend has unexpected values (new `backend_type` in
  Postgres 18, NULL `datid` when `include_bg_workers = false` leaks through),
  `RAISE WARNING` and skip the row. Losing one problematic backend is far
  better than losing an entire tick of all backends.

### Step 3: Rotation function (`ash.rotate()`)
- `pg_try_advisory_lock(hashtext('ash_rotate'))` — if false, return (another
  rotation already in progress)
- Check `rotated_at` vs `rotation_period` — if rotated too recently, skip
  (prevents double-rotation from duplicate cron fires or manual calls)
- `SET LOCAL lock_timeout = '2s'` — if a long analytics query holds a lock
  on the partition being truncated, abort gracefully and retry next cron tick
- Advance `current_slot` to next partition (already truncated)
- `TRUNCATE` the old previous partition (recycle it as the new "next")
- Update `rotated_at = now()` in `ash.config`
- **No query_map garbage collection.** GC would require scanning/unnesting all
  `integer[]` arrays in live partitions to build a "referenced" set — expensive
  analytics query just to save a few MiB of dictionary. `query_map` grows
  monotonically but slowly (one row per distinct `query_id` ever seen). Even
  pathological ORM churn produces manageable table sizes. If it ever becomes
  a problem, a manual `TRUNCATE ash.query_map` + re-seeding is the escape hatch.
- **Critical rule:** `take_sample()` must NEVER read `current_slot` and pass
  it explicitly to INSERT. Always rely on `DEFAULT ash.current_slot()` evaluated
  at row insert time. If the sampler caches the slot and rotation fires mid-flight,
  the insert lands in a partition that `rotate()` is about to truncate → data loss.
  With DEFAULT, after rotation the insert goes to the new current partition.
- `rotate()` must truncate only the partition derived from the *old* slot value
  (the one that was "previous" before the advance), never the new current.
- Log rotation event (optional: raise notice)

### Step 4: Start/stop/uninstall functions
- `ash.start(interval default '1 second')` — schedule pg_cron jobs (sampler + rotation).
  Returns the pg_cron job IDs so the user can verify in `cron.job`.
  Checks pg_cron version >= 1.5 and raises error if not met.
- `ash.stop()` — unschedule pg_cron jobs. Returns which job IDs were removed.
- `ash.uninstall()` — calls `ash.stop()` then `DROP SCHEMA ash CASCADE`.
  Plain `DROP SCHEMA` without `ash.stop()` first leaves orphaned pg_cron jobs
  that fire errors every second — `ash.uninstall()` prevents this.
- Validate pg_cron is installed before attempting schedule
- Install pg_ash in the same database as pg_cron (typically `postgres`).
  The sampler reads `pg_stat_activity` which shows all backends across all databases.

### Step 5: Reader + diagnostic functions
- `ash.status()` — first thing you run when debugging "why isn't ASH working?"
  Returns: last sample timestamp, samples in current partition, current slot,
  time since last rotation, pg_cron job status (joined to `cron.job`),
  dictionary utilization (wait_event_map rows / 32767, query_map rows)
- `ash.decode_sample(integer[])` — set-returning function, turns encoded array
  into human-readable `TABLE(state, type, event, query_id, count)`. Isolates
  the encoding format so users/views never reimplement it.
- `ash.top_waits(interval default '1 hour', int default 20)` — top wait events with %, human-readable
- `ash.wait_timeline(interval default '1 hour', interval default '1 minute')` — time-bucketed wait event breakdown
- `ash.top_queries(interval default '1 hour', int default 20)` — queries with most wait samples, joined to `pg_stat_statements` for query text
- `ash.cpu_vs_waiting(interval default '1 hour')` — CPU vs waiting ratio
- `ash.report(interval default '1 hour')` — full text report combining all of the above, Oracle ASHREPORT-style
- All functions return `SETOF record` or `TABLE(...)` for easy `\x` display or programmatic consumption
- All translate `int4` timestamps to human-readable `timestamptz` and dictionary IDs to human-readable text

### Step 6: Benchmarks — simulated long-running production
Simulate realistic production workloads without waiting real time:

**Data generation:**
- 50 active backends, 1s sampling, 1 database
- Realistic wait event distribution (not uniform — weight toward IO:DataFileRead, LWLock:*, CPU)
- ~20 distinct query_ids with realistic repetition patterns (zipf-like)
- Generate directly via `INSERT ... SELECT generate_series()`

**Scenarios (50 active backends, 1s sampling, 1 database):**
- **1 day:** 86,400 rows, ~33 MiB — the realistic production scenario. This is
  what one partition actually holds. Reader functions must be **sub-100ms** here.
- **1 month:** 2,592,000 rows, ~1 GiB — stress test for long-retention configs.
  Reader functions should still be <500ms for 1-hour windows (index-backed).

The "1 year" and "10 year" scenarios are not realistic — no single partition
would ever hold that much data in normal operation. They're only useful to
verify that `TRUNCATE` is instant regardless of partition size (it will be —
`TRUNCATE` doesn't depend on row count).

**Sampler performance benchmark:** Measure `take_sample()` execution time
with 50, 100, 200, 500 active backends. Target: <100ms for 200 backends.
If it creeps toward 500ms+ (unlikely but possible with dictionary inserts
on first encounter), that's a signal to optimize the caching strategy.

**What to measure:**
- Table + index size at each scale
- `ash.top_waits('1 hour')` query time (target: sub-100ms for 1-day partition)
- `ash.top_queries('1 hour')` query time
- `ash.wait_timeline('1 hour', '1 minute')` query time
- `ash.report('24 hours')` query time
- `take_sample()` execution time at various backend counts
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
- `ash.uninstall()` — clean removal: stops cron jobs then drops schema
- Document: install → `ash.start()` → `ash.status()` → `ash.uninstall()`

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
- **Requires pg_cron >= 1.5** — for second-granularity scheduling. Most managed providers ship this or newer.
- **Requires `pg_read_all_stats`** — sampler role must see all backends in `pg_stat_activity`.

## 8. Future Ideas

- **Grafana dashboard JSON** — wait event heatmap, top queries, CPU vs waiting over time
- **Aggregate rollup** — per-minute summaries kept for 30+ days (tiny storage)
- **Multi-day retention** — generalizes naturally: N = `retention_days + 1` partitions.
  Keep "current + (retention-1) history + next". The 3-slot PGQ design is just the
  `retention_days=1` case. Document how to configure 7-day, 30-day retention.
- **Per-PID mode** — optional flat-row table for detailed per-backend tracking at higher storage cost
- **Lightweight blocked count** — per-tick count of backends with `wait_event_type = 'Lock'`.
  Full `pg_blocking_pids()` is too expensive for 1s sampling, but a simple count
  is nearly free and answers "was there lock contention at 3am?"
