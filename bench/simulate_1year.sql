-- pg_ash 1-Year Simulation Benchmark
-- Generates 365 days of realistic sample data, measures storage,
-- reader performance, rotation correctness, and bloat.
--
-- Run: psql -h localhost -p 5433 -U postgres -f bench/simulate_1year.sql

\timing on
\pset pager off

-- Clean slate: reinstall
DROP SCHEMA IF EXISTS ash CASCADE;
\i sql/ash--1.0.sql

-- ============================================================
-- Phase 1: Seed dictionaries
-- ============================================================

\echo ''
\echo '========================================'
\echo '  Phase 1: Seed dictionaries'
\echo '========================================'

INSERT INTO ash.wait_event_map (state, type, event) VALUES
    ('active', 'CPU*', 'CPU*'),
    ('active', 'IO', 'DataFileRead'),
    ('active', 'IO', 'DataFileWrite'),
    ('active', 'IO', 'WALWrite'),
    ('active', 'IO', 'WALSync'),
    ('active', 'IO', 'BufFileRead'),
    ('active', 'LWLock', 'WALWriteLock'),
    ('active', 'LWLock', 'BufferContent'),
    ('active', 'LWLock', 'BufferMapping'),
    ('active', 'LWLock', 'ProcArray'),
    ('active', 'LWLock', 'XactSLRU'),
    ('active', 'LWLock', 'SubtransSLRU'),
    ('active', 'LWLock', 'lock_manager'),
    ('active', 'Lock', 'relation'),
    ('active', 'Lock', 'tuple'),
    ('active', 'Lock', 'transactionid'),
    ('active', 'Lock', 'virtualxid'),
    ('active', 'Lock', 'advisory'),
    ('active', 'Client', 'ClientRead'),
    ('active', 'Client', 'ClientWrite'),
    ('idle in transaction', 'IdleTx', 'IdleTx')
ON CONFLICT (state, type, event) DO NOTHING;

-- Seed 200 query_ids
INSERT INTO ash.query_map (query_id, last_seen)
SELECT (1000000 + i * 12347)::int8, 0
FROM generate_series(1, 200) i
ON CONFLICT (query_id) DO NOTHING;

SELECT count(*) as wait_events FROM ash.wait_event_map;
SELECT count(*) as query_ids FROM ash.query_map;

-- ============================================================
-- Phase 2: Generate template sample arrays
-- ============================================================

\echo ''
\echo '========================================'
\echo '  Phase 2: Build template samples'
\echo '========================================'

-- Build templates using int8 arithmetic to avoid overflow
CREATE OR REPLACE FUNCTION ash._bench_make_templates(
    p_count int DEFAULT 500,
    p_backends int DEFAULT 50
)
RETURNS SETOF integer[]
LANGUAGE plpgsql AS $$
DECLARE
    v_data integer[];
    v_remaining int;
    v_wait_ids smallint[];
    v_num_waits int;
    v_query_ids int4[];
    v_num_queries int;
    v_seed int8;  -- int8 to avoid overflow
    v_wait_idx int;
    v_count int;
    v_i int;
    v_template int;
BEGIN
    v_wait_ids := ARRAY(SELECT id FROM ash.wait_event_map ORDER BY id);
    v_num_waits := array_length(v_wait_ids, 1);
    v_query_ids := ARRAY(SELECT id FROM ash.query_map ORDER BY id LIMIT 200);
    v_num_queries := array_length(v_query_ids, 1);

    FOR v_template IN 1..p_count LOOP
        v_data := ARRAY[]::integer[];
        v_remaining := p_backends;
        v_seed := v_template * 7919::int8;

        WHILE v_remaining > 0 LOOP
            -- Pick wait event
            v_seed := (v_seed * 48271::int8) % 2147483647::int8;
            v_wait_idx := 1 + (v_seed % v_num_waits)::int;

            -- Pick count (1-8)
            v_seed := (v_seed * 48271::int8) % 2147483647::int8;
            v_count := 1 + (v_seed % least(v_remaining, 8))::int;
            IF v_count > v_remaining THEN v_count := v_remaining; END IF;

            v_data := v_data || (-v_wait_ids[v_wait_idx])::integer || v_count;

            FOR v_i IN 1..v_count LOOP
                v_seed := (v_seed * 48271::int8) % 2147483647::int8;
                v_data := v_data || v_query_ids[1 + (v_seed % v_num_queries)::int];
            END LOOP;

            v_remaining := v_remaining - v_count;
        END LOOP;

        RETURN NEXT v_data;
    END LOOP;
END;
$$;

CREATE TEMP TABLE _templates AS
SELECT row_number() OVER () as id, t as data
FROM ash._bench_make_templates(500, 50) t;

SELECT count(*) as templates,
       avg(array_length(data, 1))::int as avg_array_len,
       pg_size_pretty(avg(pg_column_size(data))::bigint) as avg_row_size
FROM _templates;

-- Verify templates are valid
SELECT count(*) as valid_templates
FROM _templates
WHERE data[1] = 1 AND array_length(data, 1) >= 3;

-- ============================================================
-- Phase 3: 1 day (86,400 rows)
-- ============================================================

\echo ''
\echo '========================================'
\echo '  Phase 3: Insert 1 day (86,400 rows)'
\echo '========================================'

INSERT INTO ash.sample (sample_ts, datid, active_count, data, slot)
SELECT
    (extract(epoch FROM now() - ash.epoch())::int4 - 86400 + s),
    16384::oid,
    50::smallint,
    t.data,
    0::smallint
FROM generate_series(0, 86399) s
JOIN _templates t ON t.id = 1 + (s % 500);

\echo ''
\echo '--- 1-day storage ---'
SELECT
    to_char((SELECT count(*) FROM ash.sample_0), 'FM999,999,999') as rows,
    pg_size_pretty(pg_table_size('ash.sample_0')) as table_size,
    pg_size_pretty(pg_indexes_size('ash.sample_0')) as index_size,
    pg_size_pretty(pg_total_relation_size('ash.sample_0')) as total_size,
    pg_table_size('ash.sample_0') / NULLIF((SELECT count(*) FROM ash.sample_0), 0) as bytes_per_row;

-- ============================================================
-- Phase 4: Reader benchmarks on 1 day
-- ============================================================

\echo ''
\echo '========================================'
\echo '  Phase 4: Reader benchmarks (1 day)'
\echo '========================================'

\echo '--- top_waits(1 hour) ---'
SELECT * FROM ash.top_waits('1 hour', 10);

\echo ''
\echo '--- top_queries(1 hour) ---'
SELECT * FROM ash.top_queries('1 hour', 10);

\echo ''
\echo '--- waits_by_type(1 hour) ---'
SELECT * FROM ash.waits_by_type('1 hour');

\echo ''
\echo '--- wait_timeline(1 hour, 5 min) ---'
SELECT count(*) as timeline_buckets FROM ash.wait_timeline('1 hour', '5 minutes');

\echo ''
\echo '--- samples_by_database(1 hour) ---'
SELECT * FROM ash.samples_by_database('1 hour');

\echo ''
\echo '--- top_waits(24 hours) — full day ---'
SELECT * FROM ash.top_waits('24 hours', 10);

\echo ''
\echo '--- status() ---'
SELECT * FROM ash.status();

-- ============================================================
-- Phase 5: 1 month (31 days, ~2.68M rows)
-- ============================================================

\echo ''
\echo '========================================'
\echo '  Phase 5: 1 month (2.68M rows)'
\echo '========================================'

TRUNCATE ash.sample_0;

INSERT INTO ash.sample (sample_ts, datid, active_count, data, slot)
SELECT
    (extract(epoch FROM now() - ash.epoch())::int4 - 31 * 86400 + s),
    16384::oid,
    50::smallint,
    t.data,
    0::smallint
FROM generate_series(0, 31 * 86400 - 1) s
JOIN _templates t ON t.id = 1 + (s % 500);

\echo ''
\echo '--- 1-month storage ---'
SELECT
    to_char((SELECT count(*) FROM ash.sample_0), 'FM999,999,999') as rows,
    pg_size_pretty(pg_table_size('ash.sample_0')) as table_size,
    pg_size_pretty(pg_indexes_size('ash.sample_0')) as index_size,
    pg_size_pretty(pg_total_relation_size('ash.sample_0')) as total_size,
    round(pg_table_size('ash.sample_0')::numeric / NULLIF((SELECT count(*) FROM ash.sample_0), 0), 1) as bytes_per_row;

\echo ''
\echo '--- 1-month readers ---'

\echo '--- top_waits(1 hour) ---'
SELECT * FROM ash.top_waits('1 hour', 5);

\echo '--- top_waits(24 hours) ---'
SELECT * FROM ash.top_waits('24 hours', 5);

-- ============================================================
-- Phase 6: 1 YEAR (365 days, ~31.5M rows)
-- ============================================================

\echo ''
\echo '========================================'
\echo '  Phase 6: 1 YEAR (31.5M rows)'
\echo '  Inserting in 30-day chunks...'
\echo '========================================'

TRUNCATE ash.sample_0;

DO $$
DECLARE
    v_chunk int;
    v_start int;
    v_end int;
    v_base int4;
    v_rows bigint;
    v_t timestamptz;
BEGIN
    v_base := extract(epoch FROM now() - ash.epoch())::int4 - 365 * 86400;

    FOR v_chunk IN 0..11 LOOP
        v_t := clock_timestamp();
        v_start := v_chunk * 30 * 86400;
        IF v_chunk = 11 THEN
            v_end := 365 * 86400 - 1;
        ELSE
            v_end := (v_chunk + 1) * 30 * 86400 - 1;
        END IF;

        INSERT INTO ash.sample (sample_ts, datid, active_count, data, slot)
        SELECT
            v_base + s,
            16384::oid,
            50::smallint,
            t.data,
            0::smallint
        FROM generate_series(v_start, v_end) s
        JOIN _templates t ON t.id = 1 + (s % 500);

        GET DIAGNOSTICS v_rows = ROW_COUNT;
        RAISE NOTICE 'Chunk %/12: % rows (% s)',
            v_chunk + 1, v_rows,
            round(extract(epoch FROM clock_timestamp() - v_t)::numeric, 1);
    END LOOP;
END;
$$;

\echo ''
\echo '--- 1-year storage ---'
SELECT
    to_char((SELECT count(*) FROM ash.sample_0), 'FM999,999,999') as rows,
    pg_size_pretty(pg_table_size('ash.sample_0')) as table_size,
    pg_size_pretty(pg_indexes_size('ash.sample_0')) as index_size,
    pg_size_pretty(pg_total_relation_size('ash.sample_0')) as total_size,
    round(pg_table_size('ash.sample_0')::numeric / NULLIF((SELECT count(*) FROM ash.sample_0), 0), 1) as bytes_per_row;

-- ============================================================
-- Phase 7: Reader performance on 1-year dataset
-- ============================================================

\echo ''
\echo '========================================'
\echo '  Phase 7: Readers on 1-year dataset'
\echo '========================================'

\echo '--- top_waits(1 hour) ---'
SELECT * FROM ash.top_waits('1 hour', 5);

\echo '--- top_waits(24 hours) ---'
SELECT * FROM ash.top_waits('24 hours', 5);

\echo '--- top_queries(1 hour) ---'
SELECT * FROM ash.top_queries('1 hour', 5);

\echo '--- waits_by_type(1 hour) ---'
SELECT * FROM ash.waits_by_type('1 hour');

\echo '--- wait_timeline(1 hour, 5 min) ---'
SELECT count(*) as buckets FROM ash.wait_timeline('1 hour', '5 minutes');

\echo '--- top_waits(7 days) — wide window ---'
SELECT * FROM ash.top_waits('7 days', 5);

-- ============================================================
-- Phase 8: Rotation on massive partition
-- ============================================================

\echo ''
\echo '========================================'
\echo '  Phase 8: Rotation (31.5M rows)'
\echo '========================================'

\echo '--- Before rotation ---'
SELECT 'sample_0' as part, pg_size_pretty(pg_total_relation_size('ash.sample_0')) as size,
       to_char((SELECT count(*) FROM ash.sample_0), 'FM999,999,999') as rows
UNION ALL SELECT 'sample_1', pg_size_pretty(pg_total_relation_size('ash.sample_1')),
       to_char((SELECT count(*) FROM ash.sample_1), 'FM999,999,999')
UNION ALL SELECT 'sample_2', pg_size_pretty(pg_total_relation_size('ash.sample_2')),
       to_char((SELECT count(*) FROM ash.sample_2), 'FM999,999,999');

-- Rotation #1: 0->1, truncate empty slot 2
UPDATE ash.config SET rotated_at = now() - interval '2 days';
\echo '--- Rotation #1: 0 -> 1, truncate 2 (empty) ---'
SELECT ash.rotate();

\echo '--- After #1 ---'
SELECT 'sample_0' as part, pg_size_pretty(pg_total_relation_size('ash.sample_0')) as size,
       to_char((SELECT count(*) FROM ash.sample_0), 'FM999,999,999') as rows
UNION ALL SELECT 'sample_1', pg_size_pretty(pg_total_relation_size('ash.sample_1')),
       to_char((SELECT count(*) FROM ash.sample_1), 'FM999,999,999')
UNION ALL SELECT 'sample_2', pg_size_pretty(pg_total_relation_size('ash.sample_2')),
       to_char((SELECT count(*) FROM ash.sample_2), 'FM999,999,999');

-- Rotation #2: 1->2, TRUNCATE slot 0 (THE BIG ONE)
UPDATE ash.config SET rotated_at = now() - interval '2 days';
\echo ''
\echo '--- Rotation #2: 1 -> 2, TRUNCATE slot 0 (31.5M rows!) ---'
SELECT ash.rotate();

\echo '--- After #2 ---'
SELECT 'sample_0' as part, pg_size_pretty(pg_total_relation_size('ash.sample_0')) as size,
       to_char((SELECT count(*) FROM ash.sample_0), 'FM999,999,999') as rows
UNION ALL SELECT 'sample_1', pg_size_pretty(pg_total_relation_size('ash.sample_1')),
       to_char((SELECT count(*) FROM ash.sample_1), 'FM999,999,999')
UNION ALL SELECT 'sample_2', pg_size_pretty(pg_total_relation_size('ash.sample_2')),
       to_char((SELECT count(*) FROM ash.sample_2), 'FM999,999,999');

-- ============================================================
-- Phase 9: Bloat check
-- ============================================================

\echo ''
\echo '========================================'
\echo '  Phase 9: Bloat after rotation'
\echo '========================================'

ANALYZE ash.sample_0;
ANALYZE ash.sample_1;
ANALYZE ash.sample_2;

SELECT relname, n_live_tup, n_dead_tup,
       pg_size_pretty(pg_total_relation_size('ash.' || relname)) as total
FROM pg_stat_user_tables
WHERE schemaname = 'ash'
ORDER BY relname;

-- ============================================================
-- Phase 10: Dictionary overhead
-- ============================================================

\echo ''
\echo '========================================'
\echo '  Phase 10: Dictionaries'
\echo '========================================'

SELECT 'wait_event_map' as dict, count(*) as rows,
       pg_size_pretty(pg_total_relation_size('ash.wait_event_map')) as size
FROM ash.wait_event_map
UNION ALL SELECT 'query_map', count(*),
       pg_size_pretty(pg_total_relation_size('ash.query_map'))
FROM ash.query_map
UNION ALL SELECT 'config', count(*),
       pg_size_pretty(pg_total_relation_size('ash.config'))
FROM ash.config;

-- ============================================================
-- Summary
-- ============================================================

\echo ''
\echo '================================================'
\echo '  BENCHMARK COMPLETE'
\echo '================================================'
\echo ''
\echo 'Targets:'
\echo '  bytes/row: ~390 (50 backends)'
\echo '  1h reader: <100ms'
\echo '  24h reader: <500ms'
\echo '  TRUNCATE: <1ms (regardless of size)'
\echo '  Dead tuples after rotation: 0'
\echo '  1-year storage: ~12 GiB (50 backends, 1 db)'

-- Cleanup
DROP FUNCTION IF EXISTS ash._bench_make_templates(int, int);
