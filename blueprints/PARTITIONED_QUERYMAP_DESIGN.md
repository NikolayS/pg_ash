# Partitioned query_map design

> **Historical design record (non-normative).** This file documents a pre-2.0
> proposal and is retained only for design history. Its schemas, function
> names, argument names, operational commands, performance figures, and
> implementation status may not match the shipped release. Do not use it as an
> install, upgrade, or API guide. Use `README.md`, `RELEASE_NOTES.md`,
> `AAS_API.md`, and the catalog comments in `sql/ash-install.sql` for current
> behavior.

## Problem

The current `query_map` is a single table with DELETE-based GC. This creates dead tuples — the only source of bloat in pg_ash. On PG14-15, volatile SQL comments can flood query_map with unique query_ids, hitting the 50k hard cap.

## Design: per-partition query_map

Align query_map partitions with sample partitions. TRUNCATE together — zero bloat everywhere.

### Schema

```sql
-- Three query_map partitions, matching sample_0/1/2
CREATE TABLE ash.query_map_0 (
    id       int4 GENERATED ALWAYS AS IDENTITY,
    query_id int8 NOT NULL,
    UNIQUE (query_id)
);
CREATE TABLE ash.query_map_1 (LIKE ash.query_map_0 INCLUDING ALL);
CREATE TABLE ash.query_map_2 (LIKE ash.query_map_0 INCLUDING ALL);
```

Each partition has its own identity sequence. Map IDs are partition-local — `id=42` in `query_map_0` is unrelated to `id=42` in `query_map_1`.

### Sampler changes

`take_sample()` registers queries into the **current** partition's query_map:

```sql
-- Current slot determines which query_map to use
v_slot := ash.current_slot();
-- Register into query_map_{v_slot}
-- Use dynamic SQL or 3-way IF for partition routing
```

No `last_seen` column needed — the entire partition is truncated on rotation.

### Rotation changes

`rotate()` truncates both tables in lockstep:

```sql
-- Advance slot: 0 → 1
-- TRUNCATE ash.sample_2;       (already done)
-- TRUNCATE ash.query_map_2;    (new: truncate matching query_map)
-- Reset identity sequence for query_map_2
```

Zero dead tuples. Zero bloat. No GC logic needed.

### Reader changes: view-based abstraction

Create a unified view over all three query_map partitions:

```sql
CREATE VIEW ash.query_map_all AS
SELECT 0::smallint as slot, id, query_id FROM ash.query_map_0
UNION ALL
SELECT 1::smallint, id, query_id FROM ash.query_map_1
UNION ALL
SELECT 2::smallint, id, query_id FROM ash.query_map_2;
```

Readers join on `(slot, id)` — same single-query pattern as today:

```sql
-- Before (single query_map):
LEFT JOIN ash.query_map qm ON qm.id = map_id

-- After (partitioned, via view):
LEFT JOIN ash.query_map_all qm ON qm.slot = s.slot AND qm.id = map_id
```

The planner eliminates scans of non-matching partitions. No UNION ALL in each reader function — the view handles routing. **10 functions** join query_map and all follow this same pattern change.

### `decode_sample()` compatibility

`decode_sample(p_data integer[])` has no slot context. Add optional slot parameter:

```sql
-- Backward-compatible: searches all partitions when slot is NULL
ash.decode_sample(p_data integer[], p_slot smallint DEFAULT NULL)
```

When `p_slot` is provided, join only that partition. When NULL, join the view (all partitions). Callers with slot context (like `samples()`) pass it for efficiency.

### Edge cases

**Query spanning rotation boundary**: Gets different map IDs in each partition. Readers handle this naturally — the view join resolves per-slot. `top_queries()` aggregates by `query_id` (the real ID from `query_map.query_id`), not by `map_id`.

**Same query_id in both partitions**: Different map_ids, same query_id. After the reader joins, GROUP BY query_id merges them. Correct.

**Identity sequence reset on TRUNCATE**: Use `ALTER TABLE ... ALTER COLUMN id RESTART` after TRUNCATE to keep IDs compact. Not strictly necessary — int4 overflow at 1s sampling would take ~27,000 years even without reset — but keeps things clean.

### Complexity cost

- Sampler: routing INSERT to the correct partition (3-way IF or dynamic SQL)
- Readers: change `JOIN query_map` → `JOIN query_map_all ON (slot, id)` in 10 functions
- `decode_sample()`: add optional `p_slot` parameter, default NULL searches all
- Rotation: one extra TRUNCATE + `ALTER COLUMN id RESTART`
- No more GC function, no `last_seen` column, no hard cap needed

### Trade-off

| | Current (single table) | Partitioned |
|---|---|---|
| Bloat | Dead tuples from DELETE | Zero |
| GC logic | DELETE + 2× rotation threshold | None (TRUNCATE) |
| Hard cap needed | Yes (PG14-15 comment spam) | No (TRUNCATE resets) |
| Reader complexity | Single JOIN | JOIN via view (same pattern) |
| Sampler complexity | Single INSERT | Per-slot INSERT |
| Functions to modify | — | 10 (query_map join) + decode_sample |

The partitioned approach is cleaner. The view abstraction keeps reader changes minimal — same JOIN pattern, just add `slot` to the join condition.

---

## Minute-rollup query filtering: 3+ stored appearances threshold

The minute rollup retains a query ID after at least the configured number of
stored appearances in that minute. The configured column is named
`rollup_min_backend_seconds`, but appearances equal backend-seconds only at an
unchanged one-second sampling interval. They do not prove query runtime when
cadence differs.

### Why 3+

- **1 appearance**: The query was observed in one stored backend appearance in
  the minute; it is omitted at the default threshold.
- **2 appearances**: The query was observed in two stored backend appearances
  in the minute; it is omitted at the default threshold.
- **3+ appearances**: The query qualifies for retained rollup attribution by
  default. At a fixed one-second cadence this corresponds to at least three
  backend-seconds; at another cadence no elapsed-runtime claim follows.

### Implementation in minute rollup

```sql
-- During rollup_1m aggregation:
WITH query_totals AS (
    SELECT query_id, count(*) as total
    FROM decoded_samples
    GROUP BY query_id
    HAVING count(*) >= 3      -- stored-appearance threshold
    ORDER BY total DESC
    LIMIT 100                  -- ← top-N truncation
)
-- Encode into query_counts array
```

Queries below the threshold are omitted from retained query attribution. Their
stored appearances remain represented in `wait_counts` (which is by wait
event), so the retained total is preserved; interpreting that total as elapsed
backend-seconds still requires an unchanged one-second cadence.

### Impact on rollup storage

On a system with 5,000 distinct query_ids per minute:
- Without filter: top 100 stored = 100 × 2 × 8 = 1,600 bytes
- With 3+ filter: maybe 200-500 qualify, top 100 stored = same 1,600 bytes

The filter does not change worst-case array size because top-N already bounds
it. It changes which query IDs compete for the top 100 by excluding IDs below
the configured per-minute appearance threshold.

### Impact on hourly rollup

Hourly rollup merges the query counts already retained by the minute rollup
and re-truncates them to the top 100. It does not apply a new
three-appearance threshold across the hour.

### Configurable threshold

```sql
-- In ash.config:
rollup_min_backend_seconds smallint DEFAULT 3
```

Users can set it to 1 (keep every query ID observed at least once, subject to
the top-100 cap) or higher (stricter filtering in each minute). Despite the
historical column name, interpret it as stored appearances unless sampling
stayed at one second.

---

## Combined architecture

```
┌─────────────────────────────────────────────────┐
│                Raw samples (1-2 days)            │
│  sample_0 ←→ query_map_0                        │
│  sample_1 ←→ query_map_1                        │
│  sample_2 ←→ query_map_2  (empty, next)         │
│  TRUNCATE together on rotation. Zero bloat.      │
├─────────────────────────────────────────────────┤
│              Minute rollup (30 days)             │
│  rollup_1m: wait_counts (all events)             │
│             query_counts (top 100, 3+ stored)    │
│  Uses raw query_id (int8). Self-contained.       │
│  DELETE retention; monitor autovacuum and bloat. │
├─────────────────────────────────────────────────┤
│              Hourly rollup (5 years)             │
│  rollup_1h: wait_counts (all events)             │
│             query_counts (merged minute top 100) │
│  Uses raw query_id (int8). Self-contained.       │
│  DELETE retention; monitor autovacuum and bloat. │
└─────────────────────────────────────────────────┘
```

Raw layer: maximum compression (int4 map_ids), zero bloat (partitioned
everything), and roughly one completed day plus the current partial period at
the default geometry.

Rollup layer: self-contained (int8 query_ids), appearance-threshold-filtered,
bounded (top-N), long retention.

No GC coordination between layers. Each is independent.
