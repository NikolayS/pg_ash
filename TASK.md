# Task: Implement pg_ash Steps 1-2

Read SPEC.md carefully. Implement Steps 1-2 from section 6 (Implementation Plan).

Write everything to: `sql/ash--1.0.sql`

## Step 1: Core schema + infrastructure
- `ash` schema
- `ash.epoch()` immutable function
- `ash.config` singleton table with initial row  
- `ash.wait_event_map` dictionary (smallint PK, state/type/event unique)
- `ash.query_map` dictionary (int4 PK, query_id int8 unique, last_seen int4)
- `ash._register_wait(state, type, event)` — upsert, returns id
- `ash._register_query(int8)` — upsert, returns int4 id
- `ash.current_slot()` — reads config
- `ash.sample` partitioned by LIST(slot), 3 children, (datid, sample_ts) indexes
- `ash._validate_data(integer[])` — structural validation
- CHECK: `data[1] = 1 AND array_length(data, 1) >= 3`

## Step 2: Sampler + decoder
- `ash.take_sample()` — the core sampler
  - sample_ts from `now()` not clock_timestamp
  - Filter: state IN (active, idle in transaction, idle in transaction (aborted))
  - backend_type = 'client backend' (+ bg workers if config says so)
  - Encode: `[1, -wait_id, count, qid, ..., -next_wait, ...]`
  - Cache wait_event_map fully at start (small)
  - query_map: collect per-tick distinct query_ids, bulk upsert, bulk update last_seen, join back
  - NULL wait_event: active->CPU/CPU, idle-in-xact->IDLE/IDLE
  - active_count per row
  - NEVER set slot explicitly — use DEFAULT ash.current_slot()
  - Per-row error handling: RAISE WARNING, skip bad rows
  - Sentinel 0 for NULL query_id
- `ash.decode_sample(integer[])` — returns TABLE(state, type, event, query_id, count)
  - Skip query_map join for sentinel 0 (return NULL query_id)
  - Unknown version -> RAISE WARNING, return zero rows

## Testing
Connect to local PG17: `psql -h localhost -p 5433 -U postgres -d postgres`
pg_cron probably not available — test manually with SELECT ash.take_sample().
Install schema, call take_sample() a few times, verify arrays, test decode_sample().

When fully working and tested, say DONE.
