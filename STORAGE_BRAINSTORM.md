# Storage Brainstorm

Design exploration for pg_ash sample row format. All benchmarks run on Postgres 17,
8,640 samples (1 day at 10s), 50 active backends per sample, 15 distinct wait events.

## Approaches Tested

### 1. Flat per-session rows (baseline)

One row per active backend per sample tick.

```sql
create table sample_flat (
  sample_ts   timestamptz not null,
  datid       oid not null,
  pid         int not null,
  wait_event  smallint,
  query_id    bigint
);
```

172,800 rows/day. Each row ~60 bytes, but 23 bytes of tuple header overhead per row
dominates at this scale.

**Result: 10.2 MiB/day**

### 2. Parallel arrays — `smallint[]` + `bigint[]`

One row per database per sample. Two parallel arrays — position `i` is one backend.

```sql
create table sample_arrays (
  sample_ts   int not null,
  datid       oid not null,
  wait_ids    smallint[] not null,
  query_ids   bigint[] not null
);
```

8,640 rows/day. Eliminates per-backend tuple headers. But `bigint[]` with 50 elements
is 50 × 8 = 400 bytes + 24 bytes array header.

**Result: 630 bytes/row, 5.3 MiB/day**

### 3. JSONB grouped by wait event

```sql
create table sample_jsonb (
  sample_ts   int not null,
  datid       oid not null,
  data        jsonb not null
  /* {"5": [101, 102], "1": [103], "0": [104, 105]} */
);
```

Natural grouping, self-describing. But JSONB keys are always text, structural
overhead per key is significant.

**Result: 910 bytes/row, 7.5 MiB/day**

### 4. JSONB → `bytea` (pglz-compressed)

Same JSONB, but `convert_to(data::text, 'UTF8')` stored as `bytea`. Postgres
applies pglz TOAST compression automatically.

**Result: 381 bytes/row, 3.1 MiB/day**

Good compression ratio (58% smaller than raw JSONB), but every read requires
decompression + JSON parsing. Kills query performance.

### 5. Dictionary-encoded query_ids — `smallint[]` + `smallint[]`

Map `query_id` (bigint) → `smallint` via a dictionary table. Most systems
have 50–200 distinct query_ids. Each element drops from 8 bytes to 2 bytes.

```sql
create table query_map (
  id       smallint primary key generated always as identity,
  query_id int8 not null unique
);

create table sample_dict (
  sample_ts   int not null,
  datid       oid not null,
  wait_ids    smallint[] not null,
  query_ids   smallint[] not null  /* dictionary-encoded */
);
```

**Result: 292 bytes/row, 2.5 MiB/day**

Halves the storage vs `bigint[]`. The dictionary adds one join at read time,
but the table is tiny (~200 rows).

### 6. Interleaved single `smallint[]`

Both arrays merged: `{wait1, qid1, wait2, qid2, ...}`. Saves one array
header (24 bytes).

**Result: 264 bytes/row, 2.2 MiB/day**

Marginal win. Harder to work with — need stride-2 `unnest` logic.

### 7. Encoded `smallint[]` — `[-wait, count, qid, qid, ...]`

**Winner.** Run-length encode by wait event in a single array:

```
{-5, 3, 101, 102, 101, -1, 2, 103, 104, -0, 1, 105}
```

- Negative value = wait event id (negated)
- Next value = count of backends with this wait event
- Following `count` values = dictionary-encoded query_ids

Benefits:
- One array instead of two (saves 24-byte header)
- Groups by wait event naturally (like JSONB, but compact)
- ~79 elements instead of 100 for 50 backends (15 wait events × 2 header elements + 50 qids)

```sql
create table sample_encoded (
  sample_ts   int not null,
  datid       oid not null,
  data        smallint[] not null
);
```

**Result: 221 bytes/row, 1.9 MiB/day**

### 8. Encoded `bytea` — compact binary

Same encoding as #7, but packed into raw `bytea`:
- 1 byte per wait event id (0..255, plenty for ~200 events)
- 1 byte per count (0..255 backends per event)
- 2 bytes per query_id (dictionary-encoded smallint)

Saves array header overhead (24 bytes → 4 bytes varlena).

**Result: 190 bytes/row, 1.6 MiB/day**

14% smaller than encoded `smallint[]`. But requires custom encode/decode
functions for all operations — loses native array operators (`unnest`,
`array_agg`, `@>`, etc.).

## Summary

| # | Approach | Bytes/row | MiB/day | Notes |
|---|----------|-----------|---------|-------|
| 1 | Flat per-session | 60 (×20 rows) | 10.2 | Baseline, naive |
| 3 | JSONB grouped | 910 | 7.5 | Self-describing but bloated |
| 2 | `smallint[]` + `bigint[]` | 630 | 5.3 | Simple parallel arrays |
| 4 | JSONB → `bytea` (pglz) | 381 | 3.1 | Good ratio, bad read perf |
| 5 | `smallint[]` + `smallint[]` (dict) | 292 | 2.5 | Dictionary-encoded qids |
| 6 | Interleaved `smallint[]` | 264 | 2.2 | Single array, stride-2 |
| **7** | **Encoded `smallint[]`** | **221** | **1.9** | **Best balance** |
| 8 | Encoded `bytea` (compact) | 190 | 1.6 | Smallest, hardest to query |

## Decision: Encoded `smallint[]` (#7)

**221 bytes/row. 1.9 MiB/day for 50 backends at 10s sampling.**

Rationale:
- **5.4× compression** vs flat rows
- **Native array operations** — can still use `unnest()`, `array_length()`, etc.
- **Grouped by wait event** — reader functions get natural structure without extra work
- **One array, one column** — simpler schema, one array header
- **Only 14% larger than `bytea`** — not worth the decode complexity

The `bytea` approach (#8) is left as a future optimization if storage becomes
a real concern at extreme scale.

## Additional Optimizations

### LZ4 TOAST compression (Postgres 14+)

Default TOAST uses `pglz`. Setting `lz4` gives faster decompression:

```sql
alter table ash.sample_0 alter column data set compression lz4;
```

Only kicks in when array exceeds ~2 KiB (TOAST threshold) — roughly 100+ backends.
Free to set, no cost when arrays are small.

### `int4` timestamp (custom epoch)

Store `sample_ts` as `int4` (seconds since `2026-01-01 00:00:00+00`) instead of
`timestamptz`. Saves 8 bytes/row due to alignment:

```
timestamptz (8) + oid (4) → 12 bytes + 4 padding = 16 bytes
int4 (4) + oid (4)        → 8 bytes, no padding
```

Verified via `pageinspect`: 288 vs 280 bytes per tuple (8 bytes saved).

Good until 2094.

### Dictionary-encoded `query_id`

Map `query_id` (int8, 8 bytes) → `smallint` (2 bytes) via a lookup table.
Most systems have 50–200 distinct query_ids. This is already built into the
encoded format (#7) — the `query_ids` in the array are dictionary references.

### What about `datid`?

Dropping `datid` saves only 4 bytes/row (186 vs 190). Not worth losing
multi-database support. Most systems have 1–3 databases, so the column
compresses well if it ever TOASTs.
