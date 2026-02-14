#!/bin/bash
# 1-year insert using separate transactions per day to avoid OOM
# Each day = 86,400 rows = ~33 MiB, well within memory limits
set -e

export PGPASSWORD=test123
PG="psql -h localhost -p 5433 -U postgres -d postgres -tA"

echo "=== Clean and install ==="
$PG -f /tmp/pg_ash/sql/ash--1.0.sql 2>&1 | tail -1

echo "=== Seed dictionaries ==="
$PG << 'SQL'
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
SQL

echo "=== Build persistent template table ==="
$PG << 'SQL'
-- Use a real table (not temp) so it persists across connections
CREATE TABLE IF NOT EXISTS ash._bench_templates (id int PRIMARY KEY, data integer[]);
TRUNCATE ash._bench_templates;

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

INSERT INTO ash._bench_templates (id, data)
SELECT row_number() OVER (), t FROM ash._bt() t;

SELECT count(*) || ' templates, avg ' || avg(array_length(data,1))::int || ' elements'
FROM ash._bench_templates;
SQL

echo ""
echo "=========================================="
echo "  INSERTING 1 YEAR (365 days, 31.5M rows)"
echo "  One transaction per 10 days"
echo "=========================================="

START=$(date +%s)
TOTAL=0
BASE=$($PG -c "SELECT extract(epoch FROM now() - ash.epoch())::int4 - 365 * 86400;")

for chunk in $(seq 0 36); do
    chunk_start=$((chunk * 10 * 86400))
    if [ $chunk -eq 36 ]; then
        chunk_end=$((365 * 86400 - 1))
    else
        chunk_end=$(((chunk + 1) * 10 * 86400 - 1))
    fi
    
    t_start=$(date +%s)
    
    rows=$($PG -c "
        INSERT INTO ash.sample (sample_ts, datid, active_count, data, slot)
        SELECT
            $BASE + s, 16384::oid, 50::smallint, t.data, 0::smallint
        FROM generate_series($chunk_start, $chunk_end) s
        JOIN ash._bench_templates t ON t.id = 1 + (s % 500);
        SELECT count(*) FROM ash.sample WHERE slot = 0 AND sample_ts >= $BASE + $chunk_start AND sample_ts <= $BASE + $chunk_end;
    ")
    
    t_end=$(date +%s)
    elapsed=$((t_end - t_start))
    TOTAL=$((TOTAL + chunk_end - chunk_start + 1))
    
    days_start=$((chunk * 10))
    days_end=$(( (chunk + 1) * 10 ))
    [ $days_end -gt 365 ] && days_end=365
    
    heap=$($PG -c "SELECT pg_size_pretty(pg_table_size('ash.sample_0'));")
    echo "Days ${days_start}-${days_end}: ${elapsed}s — total ${TOTAL} rows, heap: ${heap}"
done

END=$(date +%s)
echo ""
echo "=== INSERT COMPLETE: $TOTAL rows in $((END - START))s ==="

echo ""
echo "=========================================="
echo "  1-YEAR STORAGE"
echo "=========================================="
$PG << 'SQL'
SELECT
    to_char(count(*), 'FM999,999,999') as rows,
    pg_size_pretty(pg_table_size('ash.sample_0')) as heap,
    pg_size_pretty(pg_indexes_size('ash.sample_0')) as indexes,
    pg_size_pretty(pg_total_relation_size('ash.sample_0')) as total,
    round(pg_table_size('ash.sample_0')::numeric / count(*), 1) as bytes_per_row
FROM ash.sample_0;
SQL

echo ""
echo "=========================================="
echo "  READERS ON 1-YEAR DATASET"
echo "=========================================="

echo "--- top_waits(1h) ---"
$PG -c "\timing on" -c "SELECT * FROM ash.top_waits('1 hour', 5);"

echo "--- top_waits(24h) ---"
$PG -c "\timing on" -c "SELECT * FROM ash.top_waits('24 hours', 5);"

echo "--- top_queries(1h) ---"
$PG -c "\timing on" -c "SELECT * FROM ash.top_queries('1 hour', 5);"

echo "--- cpu_vs_waiting(1h) ---"
$PG -c "\timing on" -c "SELECT * FROM ash.cpu_vs_waiting('1 hour');"

echo "--- wait_timeline(1h, 5min) ---"
$PG -c "\timing on" -c "SELECT count(*) as buckets FROM ash.wait_timeline('1 hour', '5 minutes');"

echo "--- top_waits(7d) ---"
$PG -c "\timing on" -c "SELECT * FROM ash.top_waits('7 days', 5);"

echo ""
echo "=========================================="
echo "  ROTATION (31.5M rows)"
echo "=========================================="

echo "--- Before ---"
$PG << 'SQL'
SELECT 'sample_0' as p, pg_size_pretty(pg_total_relation_size('ash.sample_0')) as sz,
       to_char((SELECT count(*) FROM ash.sample_0), 'FM999,999,999') as rows
UNION ALL SELECT 'sample_1', pg_size_pretty(pg_total_relation_size('ash.sample_1')),
       to_char((SELECT count(*) FROM ash.sample_1), 'FM999,999,999')
UNION ALL SELECT 'sample_2', pg_size_pretty(pg_total_relation_size('ash.sample_2')),
       to_char((SELECT count(*) FROM ash.sample_2), 'FM999,999,999');
SQL

echo "--- Rotation #1: 0->1, truncate 2 ---"
$PG -c "UPDATE ash.config SET rotated_at = now() - interval '2 days';"
$PG -c "\timing on" -c "SELECT ash.rotate();"

echo "--- Rotation #2: 1->2, TRUNCATE slot 0 (31.5M rows!) ---"
$PG -c "UPDATE ash.config SET rotated_at = now() - interval '2 days';"
$PG -c "\timing on" -c "SELECT ash.rotate();"

echo "--- After ---"
$PG << 'SQL'
SELECT 'sample_0' as p, pg_size_pretty(pg_total_relation_size('ash.sample_0')) as sz,
       to_char((SELECT count(*) FROM ash.sample_0), 'FM999,999,999') as rows
UNION ALL SELECT 'sample_1', pg_size_pretty(pg_total_relation_size('ash.sample_1')),
       to_char((SELECT count(*) FROM ash.sample_1), 'FM999,999,999')
UNION ALL SELECT 'sample_2', pg_size_pretty(pg_total_relation_size('ash.sample_2')),
       to_char((SELECT count(*) FROM ash.sample_2), 'FM999,999,999');
SQL

echo ""
echo "=== BLOAT CHECK ==="
$PG -c "ANALYZE ash.sample_0;"
$PG -c "SELECT relname, n_live_tup, n_dead_tup FROM pg_stat_user_tables WHERE schemaname = 'ash' AND relname LIKE 'sample_%' ORDER BY relname;"

echo ""
echo "=========================================="
echo "  1-YEAR BENCHMARK COMPLETE"
echo "=========================================="

# Cleanup
$PG -c "DROP TABLE IF EXISTS ash._bench_templates; DROP FUNCTION IF EXISTS ash._bt(int, int);"
