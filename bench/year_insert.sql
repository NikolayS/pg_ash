\timing on
\pset pager off

-- Clean slate
DROP SCHEMA IF EXISTS ash CASCADE;
\i sql/ash--1.0.sql

-- Seed dicts
INSERT INTO ash.wait_event_map (state, type, event) VALUES
    ('active', 'CPU', 'CPU'), ('active', 'IO', 'DataFileRead'),
    ('active', 'IO', 'DataFileWrite'), ('active', 'IO', 'WALWrite'),
    ('active', 'IO', 'WALSync'), ('active', 'IO', 'BufFileRead'),
    ('active', 'LWLock', 'WALWriteLock'), ('active', 'LWLock', 'BufferContent'),
    ('active', 'LWLock', 'BufferMapping'), ('active', 'LWLock', 'ProcArray'),
    ('active', 'LWLock', 'XactSLRU'), ('active', 'LWLock', 'SubtransSLRU'),
    ('active', 'LWLock', 'lock_manager'), ('active', 'Lock', 'relation'),
    ('active', 'Lock', 'tuple'), ('active', 'Lock', 'transactionid'),
    ('active', 'Lock', 'virtualxid'), ('active', 'Lock', 'advisory'),
    ('active', 'Client', 'ClientRead'), ('active', 'Client', 'ClientWrite'),
    ('idle in transaction', 'IDLE', 'IDLE')
ON CONFLICT DO NOTHING;
INSERT INTO ash.query_map (query_id, last_seen)
SELECT (1000000 + i * 12347)::int8, 0 FROM generate_series(1, 200) i ON CONFLICT DO NOTHING;

-- Build templates
CREATE OR REPLACE FUNCTION ash._bt(p_n int DEFAULT 500, p_b int DEFAULT 50)
RETURNS SETOF integer[] LANGUAGE plpgsql AS $$
DECLARE d integer[]; r int; ws smallint[]; nw int; qs int4[]; nq int; s int8; wi int; c int; j int; t int;
BEGIN
  ws := ARRAY(SELECT id FROM ash.wait_event_map ORDER BY id); nw := array_length(ws, 1);
  qs := ARRAY(SELECT id FROM ash.query_map ORDER BY id LIMIT 200); nq := array_length(qs, 1);
  FOR t IN 1..p_n LOOP
    d := ARRAY[1]; r := p_b; s := t * 7919::int8;
    WHILE r > 0 LOOP
      s := (s * 48271::int8) % 2147483647::int8; wi := 1 + (s % nw)::int;
      s := (s * 48271::int8) % 2147483647::int8; c := 1 + (s % least(r, 8))::int;
      IF c > r THEN c := r; END IF;
      d := d || (-ws[wi])::integer || c;
      FOR j IN 1..c LOOP s := (s * 48271::int8) % 2147483647::int8; d := d || qs[1 + (s % nq)::int]; END LOOP;
      r := r - c;
    END LOOP;
    RETURN NEXT d;
  END LOOP;
END; $$;

CREATE TEMP TABLE _t AS SELECT row_number() OVER () as id, t as data FROM ash._bt() t;

\echo '=== Templates ready ==='
SELECT count(*) as n, avg(array_length(data,1))::int as avg_len,
       pg_size_pretty(avg(pg_column_size(data))::bigint) as avg_sz FROM _t;

-- ==========================================
-- 1 YEAR: 365 days × 86,400 = 31,536,000 rows
-- Insert in 30-day chunks
-- ==========================================
\echo ''
\echo '=========================================='
\echo '  INSERTING 1 YEAR (31.5M rows)'
\echo '  12 chunks of ~30 days each'
\echo '=========================================='

DO $$
DECLARE
    v_chunk int;
    v_start int;
    v_end int;
    v_base int4;
    v_rows bigint;
    v_t timestamptz;
    v_total bigint := 0;
    v_total_start timestamptz;
BEGIN
    v_base := extract(epoch FROM now() - ash.epoch())::int4 - 365 * 86400;
    v_total_start := clock_timestamp();

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
        JOIN _t t ON t.id = 1 + (s % 500);

        GET DIAGNOSTICS v_rows = ROW_COUNT;
        v_total := v_total + v_rows;
        RAISE NOTICE 'Chunk %/12: % rows (% s) — total so far: %',
            v_chunk + 1, v_rows,
            round(extract(epoch FROM clock_timestamp() - v_t)::numeric, 1),
            v_total;
    END LOOP;

    RAISE NOTICE 'ALL DONE: % rows in % s',
        v_total,
        round(extract(epoch FROM clock_timestamp() - v_total_start)::numeric, 1);
END;
$$;

-- ==========================================
-- STORAGE
-- ==========================================
\echo ''
\echo '=========================================='
\echo '  1-YEAR STORAGE'
\echo '=========================================='

SELECT
    to_char((SELECT count(*) FROM ash.sample_0), 'FM999,999,999') as rows,
    pg_size_pretty(pg_table_size('ash.sample_0')) as heap,
    pg_size_pretty(pg_indexes_size('ash.sample_0')) as indexes,
    pg_size_pretty(pg_total_relation_size('ash.sample_0')) as total,
    round(pg_table_size('ash.sample_0')::numeric / NULLIF((SELECT count(*) FROM ash.sample_0), 0), 1) as bytes_per_row;

-- ==========================================
-- READERS ON 1-YEAR DATASET
-- ==========================================
\echo ''
\echo '=========================================='
\echo '  READERS ON 1-YEAR DATASET'
\echo '=========================================='

\echo '--- top_waits(1h) ---'
SELECT * FROM ash.top_waits('1 hour', 5);
\echo '--- top_waits(24h) ---'
SELECT * FROM ash.top_waits('24 hours', 5);
\echo '--- top_queries(1h) ---'
SELECT * FROM ash.top_queries('1 hour', 5);
\echo '--- cpu_vs_waiting(1h) ---'
SELECT * FROM ash.cpu_vs_waiting('1 hour');
\echo '--- wait_timeline(1h, 5min) ---'
SELECT count(*) as buckets FROM ash.wait_timeline('1 hour', '5 minutes');
\echo '--- top_waits(7 days) ---'
SELECT * FROM ash.top_waits('7 days', 5);

-- ==========================================
-- ROTATION ON 1-YEAR PARTITION
-- ==========================================
\echo ''
\echo '=========================================='
\echo '  ROTATION ON 1-YEAR PARTITION'
\echo '=========================================='

\echo '--- Before ---'
SELECT 'sample_0' as p, pg_size_pretty(pg_total_relation_size('ash.sample_0')) as sz,
       to_char((SELECT count(*) FROM ash.sample_0), 'FM999,999,999') as rows
UNION ALL SELECT 'sample_1', pg_size_pretty(pg_total_relation_size('ash.sample_1')),
       to_char((SELECT count(*) FROM ash.sample_1), 'FM999,999,999')
UNION ALL SELECT 'sample_2', pg_size_pretty(pg_total_relation_size('ash.sample_2')),
       to_char((SELECT count(*) FROM ash.sample_2), 'FM999,999,999');

-- #1: 0->1, truncate 2 (empty)
UPDATE ash.config SET rotated_at = now() - interval '2 days';
\echo '--- Rotation #1: 0->1 ---'
SELECT ash.rotate();

-- #2: 1->2, TRUNCATE slot 0 (31.5M rows!)
UPDATE ash.config SET rotated_at = now() - interval '2 days';
\echo '--- Rotation #2: 1->2, TRUNCATE slot 0 (31.5M rows!) ---'
SELECT ash.rotate();

\echo '--- After both rotations ---'
SELECT 'sample_0' as p, pg_size_pretty(pg_total_relation_size('ash.sample_0')) as sz,
       to_char((SELECT count(*) FROM ash.sample_0), 'FM999,999,999') as rows
UNION ALL SELECT 'sample_1', pg_size_pretty(pg_total_relation_size('ash.sample_1')),
       to_char((SELECT count(*) FROM ash.sample_1), 'FM999,999,999')
UNION ALL SELECT 'sample_2', pg_size_pretty(pg_total_relation_size('ash.sample_2')),
       to_char((SELECT count(*) FROM ash.sample_2), 'FM999,999,999');

-- ==========================================
-- BLOAT
-- ==========================================
\echo ''
\echo '=== BLOAT CHECK ==='
ANALYZE ash.sample_0;
SELECT relname, n_live_tup, n_dead_tup
FROM pg_stat_user_tables WHERE schemaname = 'ash' AND relname LIKE 'sample_%' ORDER BY relname;

-- ==========================================
-- DONE
-- ==========================================
\echo ''
\echo '=========================================='
\echo '  1-YEAR BENCHMARK COMPLETE'
\echo '=========================================='

DROP FUNCTION IF EXISTS ash._bt(int, int);
