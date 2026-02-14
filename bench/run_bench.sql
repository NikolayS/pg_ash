\timing on
\pset pager off

-- Clean and install
DROP SCHEMA IF EXISTS ash CASCADE;
\i sql/ash--1.0.sql

-- Seed dictionaries
INSERT INTO ash.wait_event_map (state, type, event) VALUES
    ('active', 'CPU*', 'CPU*'), ('active', 'IO', 'DataFileRead'),
    ('active', 'IO', 'DataFileWrite'), ('active', 'IO', 'WALWrite'),
    ('active', 'IO', 'WALSync'), ('active', 'IO', 'BufFileRead'),
    ('active', 'LWLock', 'WALWriteLock'), ('active', 'LWLock', 'BufferContent'),
    ('active', 'LWLock', 'BufferMapping'), ('active', 'LWLock', 'ProcArray'),
    ('active', 'LWLock', 'XactSLRU'), ('active', 'LWLock', 'SubtransSLRU'),
    ('active', 'LWLock', 'lock_manager'), ('active', 'Lock', 'relation'),
    ('active', 'Lock', 'tuple'), ('active', 'Lock', 'transactionid'),
    ('active', 'Lock', 'virtualxid'), ('active', 'Lock', 'advisory'),
    ('active', 'Client', 'ClientRead'), ('active', 'Client', 'ClientWrite'),
    ('idle in transaction', 'IdleTx', 'IdleTx')
ON CONFLICT DO NOTHING;
INSERT INTO ash.query_map (query_id, last_seen)
SELECT (1000000 + i * 12347)::int8, extract(epoch FROM now() - ash.epoch())::int4 FROM generate_series(1, 200) i ON CONFLICT DO NOTHING;

-- Build 500 templates (50 backends each)
CREATE OR REPLACE FUNCTION ash._bench_templates(p_n int DEFAULT 500, p_b int DEFAULT 50)
RETURNS SETOF integer[] LANGUAGE plpgsql AS $$
DECLARE d integer[]; r int; ws smallint[]; nw int; qs int4[]; nq int; s int8; wi int; c int; j int; t int;
BEGIN
  ws := ARRAY(SELECT id FROM ash.wait_event_map ORDER BY id); nw := array_length(ws, 1);
  qs := ARRAY(SELECT id FROM ash.query_map ORDER BY id LIMIT 200); nq := array_length(qs, 1);
  FOR t IN 1..p_n LOOP
    d := ARRAY[]::integer[]; r := p_b; s := t * 7919::int8;
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

CREATE TEMP TABLE _t AS SELECT row_number() OVER () as id, t as data FROM ash._bench_templates() t;

\echo '=== Templates ==='
SELECT count(*) as n, avg(array_length(data,1))::int as avg_len,
       pg_size_pretty(avg(pg_column_size(data))::bigint) as avg_sz FROM _t;

-- ==========================================
-- INSERT 1 DAY (86,400 rows)
-- ==========================================
\echo ''
\echo '=========================================='
\echo '  INSERT: 1 day (86,400 rows)'
\echo '=========================================='
INSERT INTO ash.sample (sample_ts, datid, active_count, data, slot)
SELECT (extract(epoch FROM now() - ash.epoch())::int4 - 86400 + s),
       16384::oid, 50::smallint, t.data, 0::smallint
FROM generate_series(0, 86399) s JOIN _t t ON t.id = 1 + (s % 500);

\echo '--- Storage ---'
SELECT (SELECT count(*) FROM ash.sample_0) as rows,
       pg_size_pretty(pg_table_size('ash.sample_0')) as heap,
       pg_size_pretty(pg_indexes_size('ash.sample_0')) as indexes,
       pg_size_pretty(pg_total_relation_size('ash.sample_0')) as total,
       pg_table_size('ash.sample_0') / NULLIF((SELECT count(*) FROM ash.sample_0), 0) as bytes_per_row;

-- ==========================================
-- READERS: 1 hour (the common case)
-- ==========================================
\echo ''
\echo '=========================================='
\echo '  READERS: 1 hour window on 1-day dataset'
\echo '=========================================='

\echo '--- top_waits(1h) ---'
SELECT * FROM ash.top_waits('1 hour', 10);
\echo '--- top_queries(1h) ---'
SELECT * FROM ash.top_queries('1 hour', 10);
\echo '--- waits_by_type(1h) ---'
SELECT * FROM ash.waits_by_type('1 hour');
\echo '--- wait_timeline(1h, 5min) ---'
SELECT count(*) as buckets FROM ash.wait_timeline('1 hour', '5 minutes');
\echo '--- samples_by_database(1h) ---'
SELECT * FROM ash.samples_by_database('1 hour');

-- ==========================================
-- READERS: 24 hours (full day scan)
-- ==========================================
\echo ''
\echo '=========================================='
\echo '  READERS: 24 hour window (full day scan)'
\echo '=========================================='

\echo '--- top_waits(24h) ---'
SELECT * FROM ash.top_waits('24 hours', 10);
\echo '--- top_queries(24h) ---'
SELECT * FROM ash.top_queries('24 hours', 10);
\echo '--- waits_by_type(24h) ---'
SELECT * FROM ash.waits_by_type('24 hours');
\echo '--- wait_timeline(24h, 1h) ---'
SELECT count(*) as buckets FROM ash.wait_timeline('24 hours', '1 hour');

-- ==========================================
-- ROTATION TEST
-- ==========================================
\echo ''
\echo '=========================================='
\echo '  ROTATION (86,400 rows in slot 0)'
\echo '=========================================='

\echo '--- Before ---'
SELECT 'sample_0' as p, pg_size_pretty(pg_total_relation_size('ash.sample_0')) as sz,
       (SELECT count(*) FROM ash.sample_0) as rows
UNION ALL SELECT 'sample_1', pg_size_pretty(pg_total_relation_size('ash.sample_1')),
       (SELECT count(*) FROM ash.sample_1)
UNION ALL SELECT 'sample_2', pg_size_pretty(pg_total_relation_size('ash.sample_2')),
       (SELECT count(*) FROM ash.sample_2);

-- Rotation #1: 0->1, truncate 2 (empty)
UPDATE ash.config SET rotated_at = now() - interval '2 days';
\echo '--- Rotation #1: 0->1 ---'
SELECT ash.rotate();

-- Add data to slot 1
INSERT INTO ash.sample (sample_ts, datid, active_count, data, slot)
SELECT (extract(epoch FROM now() - ash.epoch())::int4 - 100 + s),
       16384::oid, 50::smallint, t.data, 1::smallint
FROM generate_series(0, 999) s JOIN _t t ON t.id = 1 + (s % 500);

\echo '--- After #1 (1k rows in slot 1) ---'
SELECT 'sample_0' as p, pg_size_pretty(pg_total_relation_size('ash.sample_0')) as sz,
       (SELECT count(*) FROM ash.sample_0) as rows
UNION ALL SELECT 'sample_1', pg_size_pretty(pg_total_relation_size('ash.sample_1')),
       (SELECT count(*) FROM ash.sample_1)
UNION ALL SELECT 'sample_2', pg_size_pretty(pg_total_relation_size('ash.sample_2')),
       (SELECT count(*) FROM ash.sample_2);

-- Rotation #2: 1->2, TRUNCATE slot 0 (86,400 rows!)
UPDATE ash.config SET rotated_at = now() - interval '2 days';
\echo '--- Rotation #2: 1->2, TRUNCATE slot 0 (86,400 rows!) ---'
SELECT ash.rotate();

\echo '--- After #2 ---'
SELECT 'sample_0' as p, pg_size_pretty(pg_total_relation_size('ash.sample_0')) as sz,
       (SELECT count(*) FROM ash.sample_0) as rows
UNION ALL SELECT 'sample_1', pg_size_pretty(pg_total_relation_size('ash.sample_1')),
       (SELECT count(*) FROM ash.sample_1)
UNION ALL SELECT 'sample_2', pg_size_pretty(pg_total_relation_size('ash.sample_2')),
       (SELECT count(*) FROM ash.sample_2);

-- Verify slot 0 data is gone, slot 1 survives
SELECT current_slot FROM ash.config;

-- ==========================================
-- BLOAT & DICTS
-- ==========================================
\echo ''
\echo '=========================================='
\echo '  BLOAT & DICTIONARY OVERHEAD'
\echo '=========================================='
ANALYZE ash.sample_0; ANALYZE ash.sample_1; ANALYZE ash.sample_2;
SELECT relname, n_live_tup, n_dead_tup FROM pg_stat_user_tables WHERE schemaname = 'ash' ORDER BY relname;

SELECT 'wait_event_map' as d, count(*) as rows, pg_size_pretty(pg_total_relation_size('ash.wait_event_map')) as sz FROM ash.wait_event_map
UNION ALL SELECT 'query_map', count(*), pg_size_pretty(pg_total_relation_size('ash.query_map')) FROM ash.query_map;

-- ==========================================
-- STATUS
-- ==========================================
\echo ''
SELECT * FROM ash.status();

-- Cleanup
DROP FUNCTION IF EXISTS ash._bench_templates(int, int);

\echo ''
\echo '=========================================='
\echo '  DONE'
\echo '=========================================='
