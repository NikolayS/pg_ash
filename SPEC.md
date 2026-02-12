# pg_ash — Active Session History for PostgreSQL

## Overview

**pg_ash** samples `pg_stat_activity` at a configurable frequency (1–60s) and stores wait event history in a bloat-free rotating partition scheme inspired by Skype's PGQ.

No extensions required beyond `pg_cron`. Pure SQL. Runs on the primary. Designed for small-to-medium clusters where installing custom C extensions isn't practical or allowed.

## Why

PostgreSQL has `pg_stat_activity` but it's a point-in-time snapshot — blink and you miss it. Oracle has ASH (Active Session History). `pg_wait_sampling` exists but requires a C extension and `shared_preload_libraries`. pg_ash gives you the same observability with zero operational overhead: just SQL + pg_cron.

## Architecture

### Storage: PGQ-Style 3-Partition Rotation

Three partitions rotating through roles:

```
┌─────────────┐  ┌─────────────┐  ┌─────────────┐
│  Partition A │  │  Partition B │  │  Partition C │
│  (previous)  │  │  (current)   │  │  (next)      │
│  yesterday   │  │  today       │  │  tomorrow    │
│  READ-ONLY   │  │  INSERTING   │  │  TRUNCATED   │
└─────────────┘  └─────────────┘  └─────────────┘
```

At rotation time (configurable, default: daily at midnight):
1. The "next" partition was already `TRUNCATE`d (instant, no vacuum)
2. Roles shift: previous→(drop/truncate), current→previous, next→current
3. The newly freed partition becomes "next" and gets `TRUNCATE`d immediately

**Why this works:**
- `TRUNCATE` is instantaneous and generates minimal WAL
- No dead tuples, no vacuum pressure, no bloat — ever
- The "previous" partition is always available for queries (yesterday's data)
- Predictable, bounded storage: you always know exactly how much space ASH uses

### Rotation Period

Default rotation period: **1 day**. Configurable. With 3 partitions you always have:
- Full data for the current period
- Full data for the previous period
- The next partition pre-truncated and ready

For shorter retention needs, you could rotate every N hours.

### Sample Row

```sql
create table pg_ash.sample (
    sample_ts       timestamptz     not null default clock_timestamp(),
    datid           oid,
    pid             int             not null,
    leader_pid      int,
    usesysid        oid,
    backend_type    text,
    state           text,
    wait_event_type text,
    wait_event      text,
    query_id        bigint,
    xact_age_ms     int,            -- milliseconds since xact_start
    query_age_ms    int,            -- milliseconds since query_start
    backend_xid     xid,
    backend_xmin    xid
) partition by list (/* partition key — see below */);
```

**Design decisions:**
- **No `query` text** — that's what `pg_stat_statements` is for. We store `query_id` which joins to it. Huge space savings.
- **No `datname`/`usename`** — store `datid`/`usesysid` (oids), join to `pg_database`/`pg_authid` at query time. 4 bytes instead of variable-length text per row.
- **No `client_addr`/`client_hostname`/`client_port`** — not relevant for wait event analysis. Can be added as an option if needed.
- **No `application_name`** — variable-length, high cardinality. Optional column if wanted.
- **`xact_age_ms`/`query_age_ms`** — pre-computed as int4 (milliseconds). Avoids storing two additional timestamptz (16 bytes) per row. Max ~24 days at int4, plenty.
- **`backend_xid`/`backend_xmin`** — useful for detecting long transactions and snapshot bloat.

### Estimated Row Size

| Column | Type | Bytes |
|--------|------|-------|
| sample_ts | timestamptz | 8 |
| datid | oid | 4 |
| pid | int | 4 |
| leader_pid | int | 4 |
| usesysid | oid | 4 |
| backend_type | text | ~12 avg (short, low cardinality) |
| state | text | ~8 avg |
| wait_event_type | text | ~8 avg |
| wait_event | text | ~16 avg |
| query_id | bigint | 8 |
| xact_age_ms | int | 4 |
| query_age_ms | int | 4 |
| backend_xid | xid | 4 |
| backend_xmin | xid | 4 |
| **tuple header** | | 23 |
| **alignment** | | ~5 |
| **Total** | | **~118 bytes/row** |

### Storage Math

At 1-second sampling with 50 active backends:

```
50 backends × 1 sample/s × 86400 s/day × 118 bytes ≈ 490 MB/day
```

At 10-second sampling (more typical):

```
50 backends × 0.1 sample/s × 86400 s/day × 118 bytes ≈ 49 MB/day
```

At 10s sampling with 10 backends (small DB):

```
10 × 0.1 × 86400 × 118 ≈ 10 MB/day
```

With 2 partitions of live data (current + previous), that's ≤ 1 GB for a 50-backend system at 10s sampling. Totally fine.

### Partition Key Strategy

Use a synthetic partition key based on the rotation slot:

```sql
create table pg_ash.sample (
    ...
    slot smallint not null default pg_ash.current_slot()
) partition by list (slot);

create table pg_ash.sample_0 partition of pg_ash.sample for values in (0);
create table pg_ash.sample_1 partition of pg_ash.sample for values in (1);
create table pg_ash.sample_2 partition of pg_ash.sample for values in (2);
```

`pg_ash.current_slot()` returns 0, 1, or 2 based on the current rotation state (tracked in a small config table).

### Indexes

```sql
-- Primary access pattern: time-range queries
create index on pg_ash.sample_0 (sample_ts);
create index on pg_ash.sample_1 (sample_ts);
create index on pg_ash.sample_2 (sample_ts);

-- For query-specific drill-down
create index on pg_ash.sample_0 (query_id, sample_ts) where query_id is not null;
create index on pg_ash.sample_1 (query_id, sample_ts) where query_id is not null;
create index on pg_ash.sample_2 (query_id, sample_ts) where query_id is not null;
```

Indexes are rebuilt instantly after `TRUNCATE` (empty table). No index bloat.

## Sampling

### The Sampler Function

```sql
create or replace function pg_ash.take_sample()
returns void as $$
insert into pg_ash.sample (
    datid, pid, leader_pid, usesysid, backend_type,
    state, wait_event_type, wait_event, query_id,
    xact_age_ms, query_age_ms, backend_xid, backend_xmin
)
select
    datid, pid, leader_pid, usesysid, backend_type,
    state, wait_event_type, wait_event, query_id,
    extract(epoch from clock_timestamp() - xact_start)::int * 1000,
    extract(epoch from clock_timestamp() - query_start)::int * 1000,
    backend_xid, backend_xmin
from pg_stat_activity
where
    pid != pg_backend_pid()         -- exclude self
    and state != 'idle'             -- only active/active-in-transaction/etc.
    and backend_type = 'client backend';  -- skip bg workers, autovacuum, etc.
$$ language sql;
```

**Only active sessions** — idle connections aren't interesting for ASH. This is what makes the storage manageable: you're sampling work, not idle connections.

### Optional: Include Background Workers

A config flag to also capture autovacuum workers, WAL sender, etc. Off by default.

### pg_cron Scheduling

```sql
-- Sample every 10 seconds (default)
select cron.schedule('pg_ash_sampler', '10 seconds', 'select pg_ash.take_sample()');

-- Sample every 1 second (max frequency)
select cron.schedule('pg_ash_sampler', '1 second', 'select pg_ash.take_sample()');
```

**pg_cron limitation:** Minimum interval is 1 second. That's fine — Oracle ASH also samples at 1s.

### Rotation via pg_cron

```sql
-- Rotate daily at midnight
select cron.schedule('pg_ash_rotate', '0 0 * * *', 'select pg_ash.rotate()');
```

### The Rotate Function

```sql
create or replace function pg_ash.rotate()
returns void as $$
declare
    v_current smallint;
    v_next smallint;
    v_prev smallint;
begin
    select current_slot into v_current from pg_ash.config;
    v_next := (v_current + 1) % 3;
    v_prev := (v_current + 2) % 3;  -- the one that was "previous", now being recycled

    -- Truncate the slot we're about to start writing to
    -- (it was "next" and should already be empty, but be safe)
    execute format('truncate pg_ash.sample_%s', v_next);

    -- Advance the current slot
    update pg_ash.config set
        current_slot = v_next,
        rotated_at = clock_timestamp();
end;
$$ language plpgsql;
```

After rotation:
- New current = was "next" (empty, ready for inserts)
- New previous = was "current" (yesterday's full data, queryable)
- New next = was "previous" (will be truncated on *next* rotation)

**Key insight:** We truncate the slot we're *about to write to*, not the one we just finished. This means the "previous" data survives for a full rotation period after it stops receiving writes. You always have 1–2 full periods of history.

## Configuration

```sql
create table pg_ash.config (
    singleton       bool primary key default true check (singleton),
    current_slot    smallint not null default 0,
    sample_interval interval not null default '10 seconds',
    rotation_period interval not null default '1 day',
    include_bg_workers bool not null default false,
    rotated_at      timestamptz not null default clock_timestamp(),
    installed_at    timestamptz not null default clock_timestamp()
);
```

## Query Examples

### Top wait events in the last hour

```sql
select
    wait_event_type,
    wait_event,
    count(*) as samples,
    round(100.0 * count(*) / sum(count(*)) over (), 1) as pct
from pg_ash.sample
where sample_ts > now() - interval '1 hour'
    and wait_event is not null
group by 1, 2
order by 3 desc
limit 20;
```

### Wait event timeline (ASH-style)

```sql
select
    date_trunc('minute', sample_ts) as minute,
    wait_event_type,
    wait_event,
    count(*) as samples
from pg_ash.sample
where sample_ts > now() - interval '1 hour'
    and wait_event is not null
group by 1, 2, 3
order by 1, 4 desc;
```

### Top queries by wait time

```sql
select
    s.query_id,
    ss.query,
    count(*) as wait_samples,
    array_agg(distinct s.wait_event) filter (where s.wait_event is not null) as wait_events
from pg_ash.sample s
left join pg_stat_statements ss using (query_id)
where s.sample_ts > now() - interval '1 hour'
    and s.wait_event is not null
group by 1, 2
order by 3 desc
limit 20;
```

### Long-running transactions

```sql
select
    pid,
    usesysid,
    max(xact_age_ms) as max_xact_ms,
    count(*) as samples_in_xact,
    array_agg(distinct wait_event) filter (where wait_event is not null) as wait_events
from pg_ash.sample
where sample_ts > now() - interval '1 hour'
    and xact_age_ms > 60000  -- xact older than 1 minute
group by 1, 2
order by 3 desc;
```

## Installation

```sql
-- Requires pg_cron
create extension if not exists pg_cron;

-- Install pg_ash
\i pg_ash--1.0.sql

-- Start sampling (10s default)
select pg_ash.start();

-- Or with custom interval
select pg_ash.start('1 second');
```

## Uninstall

```sql
select pg_ash.stop();    -- removes pg_cron jobs
drop schema pg_ash cascade;
```

## Files

```
pg_ash/
├── README.md
├── SPEC.md               (this file)
├── LICENSE                (Apache 2.0)
├── pg_ash--1.0.sql        (full install script)
├── pg_ash_start.sql       (create pg_cron jobs)
├── pg_ash_stop.sql        (remove pg_cron jobs)
└── sql/
    ├── views/
    │   ├── top_wait_events.sql
    │   ├── wait_timeline.sql
    │   ├── top_queries.sql
    │   └── long_transactions.sql
    └── examples/
        └── grafana_queries.sql
```

## Compatibility

- **PostgreSQL 13+** (query_id requires PG14+; on PG13, query_id will be NULL)
- **pg_cron 1.5+** (for sub-minute scheduling with seconds)
- Works with `pg_stat_statements` for query text correlation
- No `shared_preload_libraries` changes (beyond pg_cron which you likely already have)

## Limitations

- **Sampling resolution:** 1 second minimum (pg_cron limit). Sub-second events may be missed. For sub-second sampling, you'd need `pg_wait_sampling` (C extension).
- **Primary only:** `pg_stat_activity` on replicas doesn't show replica queries the same way. This is designed for the primary.
- **pg_cron dependency:** pg_cron must be installed and running. It's the most widely available PG extension though.
- **No query text storage:** By design. Use `pg_stat_statements` for that.
- **Clock skew in `xact_age_ms`:** Computed at sample time, not exact. Good enough for observability.

## Future Ideas

- Built-in Grafana dashboard JSON
- `pg_ash.report()` function for text-based ASH report (like Oracle's)
- Compression: store `wait_event_type`/`wait_event`/`backend_type`/`state` as smallint FKs to a dictionary table (saves ~30 bytes/row)
- Configurable retention (e.g., 7-day rotation with 9 partitions: 3 sets of 3)
- Aggregate rollup table (per-minute summaries kept for 30 days)
