# pg_ash — Active Session History for PostgreSQL

## Overview

**pg_ash** samples `pg_stat_activity` at a configurable frequency (1–60s) and stores wait event history in a bloat-free rotating partition scheme inspired by Skype's PGQ.

No extensions required beyond `pg_cron`. Pure SQL. Runs on the primary. Designed for small-to-medium clusters where installing custom C extensions isn't practical or allowed.

**Compatibility:** PostgreSQL 14+.

## Why

PostgreSQL has `pg_stat_activity` but it's a point-in-time snapshot — blink and you miss it. Oracle has ASH (Active Session History). `pg_wait_sampling` exists but requires a C extension and `shared_preload_libraries`. pg_ash gives you the same observability with zero operational overhead: just SQL + pg_cron.

## Architecture

### Sample Row Format

One row per sample per database. Parallel arrays — each index position = one active backend.

```
sample_ts   │ 2026-02-12 03:48:00+00
datid       │ 16384
wait_ids    │ {5, 5, 1, 0, 0, 9, 1}
query_ids   │ {-617046263269, 482910384756, 771629384756, -617046263269, 923847561029, NULL, 482910384756}
```

- `wait_ids[i]` → lookup in `ash.wait_event_map` for the wait event name
- `query_ids[i]` → join to `pg_stat_statements` for query text
- `wait_id = 0` means running on CPU (no wait event)
- `query_ids` element is NULL when `query_id` is not set (e.g., `compute_query_id = off`)

7 active backends across 3 distinct wait events → **1 row**.

### Wait Event Dictionary

```sql
create table ash.wait_event_map (
    id       smallint primary key generated always as identity,
    type     text not null,  -- 'LWLock', 'IO', 'Lock', etc.
    event    text not null,  -- 'LockManager', 'DataFileRead', etc.
    unique (type, event)
);

-- id=0 is reserved: CPU (running, no wait event)
insert into ash.wait_event_map (id, type, event)
    overriding system value
    values (0, '', 'CPU');
```

Populated at install time from a known set of PG14+ wait events (~200 entries). The sampler function auto-inserts unknown wait events on first encounter (future-proof for new PG versions).

On PG17+, we could also seed from `pg_wait_events`, but we don't require it.

### Sample Table

```sql
create table ash.sample (
    sample_ts   timestamptz not null default now(),
    datid       oid         not null,
    wait_ids    smallint[]  not null,
    query_ids   bigint[]
) partition by list (slot);
```

**Why `now()` not `clock_timestamp()`:** All rows from the same sample tick share the same timestamp. `now()` is transaction-time — perfectly consistent and slightly cheaper. Precision within a sampling tick doesn't matter.

**Why parallel arrays over JSONB:**

Benchmarked with 1 day of data (8,640 samples, 20 backends):

| Approach | Size/day | Bytes/row |
|----------|----------|-----------|
| **Parallel arrays** | **2.5 MB** | 307 |
| JSONB grouped | 5.4 MB | 634 |
| Flat per-session | 10.2 MB | 60 (but 20× more rows) |

Arrays are 2× more compact than JSONB and native binary — no per-key text overhead.

### Storage Math

At 10-second sampling, 1 database:

| Active backends | Rows/day | Size/day |
|-----------------|----------|----------|
| 5 | 8,640 | ~1 MB |
| 20 | 8,640 | ~2.5 MB |
| 50 | 8,640 | ~5 MB |
| 100 | 8,640 | ~9 MB |

Row count is always 8,640/day regardless of backend count (1 row per sample). Size scales with array length.

At 1-second sampling, multiply by 10.

With 2 live partitions (current + previous day), worst case 100 backends at 1s: ~180 MB total. Still trivial.

### Bloat-Free Storage: PGQ-Style 3-Partition Rotation

Three partitions rotating through roles:

```
┌─────────────┐  ┌─────────────┐  ┌─────────────┐
│  Partition 0 │  │  Partition 1 │  │  Partition 2 │
│  (previous)  │  │  (current)   │  │  (next)      │
│  READ-ONLY   │  │  INSERTING   │  │  TRUNCATED   │
└─────────────┘  └─────────────┘  └─────────────┘
```

At rotation (default: daily at midnight):
1. Advance `current_slot` to the "next" partition (already truncated, ready)
2. `TRUNCATE` the old "previous" partition (now becomes "next")

**Why this works:**
- `TRUNCATE` is instantaneous, generates minimal WAL
- No dead tuples, no vacuum pressure, no bloat — ever
- "Previous" partition always available for queries (yesterday's data)
- Predictable, bounded storage

### Partition Key

```sql
create table ash.sample (
    ...
    slot smallint not null default ash.current_slot()
) partition by list (slot);

create table ash.sample_0 partition of ash.sample for values in (0);
create table ash.sample_1 partition of ash.sample for values in (1);
create table ash.sample_2 partition of ash.sample for values in (2);
```

### Indexes

```sql
-- Primary access pattern: time-range queries
create index on ash.sample_0 (sample_ts);
create index on ash.sample_1 (sample_ts);
create index on ash.sample_2 (sample_ts);
```

Indexes are rebuilt instantly after `TRUNCATE`. No index bloat.

GIN indexes on the arrays are possible but likely unnecessary — most queries filter by `sample_ts` first, then scan the arrays. The sample table is compact enough.

## Sampling

### The Sampler Function

```sql
create or replace function ash.take_sample()
returns void as $$
    insert into ash.sample (datid, wait_ids, query_ids)
    select
        datid,
        array_agg(coalesce(w.id, ash._register_wait(sa.wait_event_type, sa.wait_event))),
        array_agg(query_id)
    from pg_stat_activity sa
    left join ash.wait_event_map w
        on w.type = coalesce(sa.wait_event_type, '')
        and w.event = coalesce(sa.wait_event, 'CPU')
    where sa.pid != pg_backend_pid()
        and sa.state = 'active'
        and sa.backend_type = 'client backend'
    group by sa.datid;
$$ language sql;
```

**Only `state = 'active'`** — idle and idle-in-transaction sessions aren't interesting for wait event profiling. This is what keeps storage tiny.

The `_register_wait()` fallback auto-inserts any wait event not in the dictionary (handles new PG versions without manual updates).

### pg_cron Scheduling

```sql
-- Sample every 10 seconds (default)
select cron.schedule('ash_sampler', '10 seconds', 'select ash.take_sample()');

-- Rotation daily at midnight
select cron.schedule('ash_rotate', '0 0 * * *', 'select ash.rotate()');
```

Minimum interval: 1 second (pg_cron limit). Oracle ASH also samples at 1s.

### The Rotate Function

```sql
create or replace function ash.rotate()
returns void as $$
declare
    v_current smallint;
    v_next smallint;
    v_old_prev smallint;
begin
    select current_slot into v_current from ash.config;
    v_next := (v_current + 1) % 3;
    v_old_prev := (v_current + 2) % 3;

    -- Advance: start writing to the next slot (already empty)
    update ash.config set
        current_slot = v_next,
        rotated_at = clock_timestamp();

    -- Truncate the old previous (now becomes "next", to be used next rotation)
    execute format('truncate ash.sample_%s', v_old_prev);
end;
$$ language plpgsql;
```

## Configuration

```sql
create table ash.config (
    singleton       bool primary key default true check (singleton),
    current_slot    smallint not null default 0,
    sample_interval interval not null default '10 seconds',
    rotation_period interval not null default '1 day',
    include_idle_xact bool not null default false,
    include_bg_workers bool not null default false,
    rotated_at      timestamptz not null default clock_timestamp(),
    installed_at    timestamptz not null default clock_timestamp()
);
```

Options:
- `include_idle_xact`: also capture `idle in transaction` sessions (off by default)
- `include_bg_workers`: also capture autovacuum, WAL sender, etc. (off by default)

## Query Examples

### Top wait events in the last hour

```sql
select
    w.type || ':' || w.event as wait_event,
    count(*) as samples,
    round(100.0 * count(*) / sum(count(*)) over (), 1) as pct
from ash.sample s,
     unnest(s.wait_ids) as wid
join ash.wait_event_map w on w.id = wid
where s.sample_ts > now() - interval '1 hour'
    and wid != 0  -- exclude CPU
group by 1
order by 2 desc
limit 20;
```

### Wait event timeline (minute buckets)

```sql
select
    date_trunc('minute', s.sample_ts) as minute,
    w.type || ':' || w.event as wait_event,
    count(*) as samples
from ash.sample s,
     unnest(s.wait_ids) as wid
join ash.wait_event_map w on w.id = wid
where s.sample_ts > now() - interval '1 hour'
    and wid != 0
group by 1, 2
order by 1, 3 desc;
```

### Top queries by wait time

```sql
select
    qid as query_id,
    count(*) as wait_samples,
    array_agg(distinct w.type || ':' || w.event) as wait_events
from ash.sample s,
     unnest(s.wait_ids, s.query_ids) as u(wid, qid)
join ash.wait_event_map w on w.id = wid
where s.sample_ts > now() - interval '1 hour'
    and wid != 0
    and qid is not null
group by 1
order by 2 desc
limit 20;
```

### CPU vs waiting breakdown

```sql
select
    case when wid = 0 then 'CPU' else 'Waiting' end as state,
    count(*) as samples,
    round(100.0 * count(*) / sum(count(*)) over (), 1) as pct
from ash.sample s,
     unnest(s.wait_ids) as wid
where s.sample_ts > now() - interval '1 hour'
group by 1;
```

## Installation

```sql
create extension if not exists pg_cron;
\i ash--1.0.sql
select ash.start();           -- 10s sampling (default)
-- or: select ash.start('1 second');
```

## Uninstall

```sql
select ash.stop();    -- removes pg_cron jobs
drop schema ash cascade;
```

## Files

```
pg_ash/
├── README.md
├── SPEC.md               (this file)
├── LICENSE                (Apache 2.0)
├── ash--1.0.sql           (full install script)
└── sql/
    └── views/
        ├── top_waits.sql
        ├── wait_timeline.sql
        ├── top_queries.sql
        └── cpu_vs_waiting.sql
```

## Limitations

- **Sampling resolution:** 1s minimum (pg_cron limit). Sub-second needs `pg_wait_sampling` (C extension).
- **Primary only:** designed for primary, `pg_stat_activity` on replicas shows limited info.
- **No per-PID tracking:** aggregated by database. Can't trace individual backend journeys. (By design — keeps storage tiny.)
- **No query text:** use `pg_stat_statements` and join on `query_id`.

## Future Ideas

- Grafana dashboard JSON (wait event heatmap, top queries, CPU vs waiting)
- `ash.report()` — text-based ASH report function (like Oracle's `ASHREPORT`)
- Wait event dictionary compression: many events will never appear; could lazy-populate
- Aggregate rollup table (per-minute summaries kept for 30+ days)
- Multi-day retention via more partitions (e.g., 7×3 = 21 partitions for weekly rotation)
