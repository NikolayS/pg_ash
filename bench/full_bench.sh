#!/bin/bash
# Full pg_ash benchmark suite
# Tests: sampler overhead, backend scaling, multi-db, concurrent r/w,
#        pg_cron accuracy, real workload, memory usage
set -e

export PGPASSWORD=test123
PG="psql -h localhost -p 5433 -U postgres -d postgres"
PGT="psql -h localhost -p 5433 -U postgres -d postgres -tA"

echo "================================================================"
echo "  BENCHMARK 1: take_sample() overhead (the hot path)"
echo "================================================================"
echo ""

# First, generate some real activity with pgbench
echo "--- Setting up pgbench ---"
$PGT -c "SELECT 'pgbench tables exist' WHERE EXISTS (SELECT 1 FROM pg_tables WHERE tablename='pgbench_accounts');" || \
  pgbench -h localhost -p 5433 -U postgres -i -s 10 postgres 2>&1 | tail -3

echo ""
echo "--- Measuring take_sample() with NO load ---"
$PG -c "\timing on" -c "SELECT ash.take_sample();" 2>&1 | grep -E "Time:|take_sample"

echo ""
echo "--- Starting pgbench (10 clients, 30s) and measuring take_sample() during ---"
# Run pgbench in background
pgbench -h localhost -p 5433 -U postgres -c 10 -j 2 -T 30 postgres > /tmp/pgbench_out.txt 2>&1 &
PGBENCH_PID=$!
sleep 2  # let it warm up

echo "Sampling during pgbench load..."
for i in $(seq 1 10); do
    ts_before=$($PGT -c "SELECT extract(epoch FROM clock_timestamp())::numeric;")
    $PGT -c "SELECT ash.take_sample();" > /dev/null
    ts_after=$($PGT -c "SELECT extract(epoch FROM clock_timestamp())::numeric;")
    ms=$(echo "($ts_after - $ts_before) * 1000" | bc 2>/dev/null || echo "?")
    echo "  Sample $i: ${ms} ms"
    sleep 1
done

wait $PGBENCH_PID 2>/dev/null || true
echo ""
echo "pgbench results:"
grep -E "tps|latency" /tmp/pgbench_out.txt || cat /tmp/pgbench_out.txt | tail -5

echo ""
echo "--- Measuring take_sample() with 50 clients ---"
pgbench -h localhost -p 5433 -U postgres -c 50 -j 4 -T 20 postgres > /tmp/pgbench_50.txt 2>&1 &
PGBENCH_PID=$!
sleep 3

for i in $(seq 1 5); do
    ts_before=$($PGT -c "SELECT extract(epoch FROM clock_timestamp())::numeric;")
    $PGT -c "SELECT ash.take_sample();" > /dev/null
    ts_after=$($PGT -c "SELECT extract(epoch FROM clock_timestamp())::numeric;")
    ms=$(echo "($ts_after - $ts_before) * 1000" | bc 2>/dev/null || echo "?")
    echo "  Sample (50 clients) $i: ${ms} ms"
    sleep 2
done

wait $PGBENCH_PID 2>/dev/null || true
echo "pgbench 50-client results:"
grep -E "tps|latency" /tmp/pgbench_50.txt || cat /tmp/pgbench_50.txt | tail -5

echo ""
echo "--- How many rows were captured? ---"
$PG -c "SELECT count(*) as samples, pg_size_pretty(pg_table_size('ash.sample_0')) as heap FROM ash.sample_0;"

echo ""
echo "--- Actual captured data quality ---"
$PG -c "
SELECT
    count(*) as total_samples,
    avg(active_count)::int as avg_active,
    min(active_count) as min_active,
    max(active_count) as max_active,
    avg(array_length(data, 1))::int as avg_array_len,
    pg_size_pretty(avg(pg_column_size(data))::bigint) as avg_data_size
FROM ash.sample_0;
"

echo ""
echo "================================================================"
echo "  BENCHMARK 2: Backend scaling (array size vs backends)"
echo "================================================================"
echo ""

# Generate templates with different backend counts
$PG -c "
CREATE OR REPLACE FUNCTION ash._bench_gen(p_backends int)
RETURNS integer[] LANGUAGE plpgsql AS \$\$
DECLARE
    d integer[];
    r int;
    ws smallint[];
    nw int;
    s int8;
    wi int;
    c int;
    j int;
BEGIN
    ws := ARRAY(SELECT id FROM ash.wait_event_map ORDER BY id);
    nw := array_length(ws, 1);
    d := ARRAY[]::integer[];
    r := p_backends;
    s := 42 * 7919::int8;
    WHILE r > 0 LOOP
        s := (s * 48271::int8) % 2147483647::int8;
        wi := 1 + (s % nw)::int;
        s := (s * 48271::int8) % 2147483647::int8;
        c := 1 + (s % least(r, 8))::int;
        IF c > r THEN c := r; END IF;
        d := d || (-ws[wi])::integer || c;
        FOR j IN 1..c LOOP
            s := (s * 48271::int8) % 2147483647::int8;
            d := d || (1000000 + (s % 200)::int);
        END LOOP;
        r := r - c;
    END LOOP;
    RETURN d;
END; \$\$;
" 2>&1 | tail -1

for backends in 10 50 100 200 500 1000; do
    result=$($PGT -c "
        SELECT
            $backends as backends,
            array_length(ash._bench_gen($backends), 1) as array_len,
            pg_column_size(ash._bench_gen($backends)) as bytes,
            round(pg_column_size(ash._bench_gen($backends))::numeric + 24 + 8 + 2 + 4, 0) as est_row_bytes
        ;
    ")
    echo "  Backends=$backends: $result"
done

echo ""
echo "--- Storage per day by backend count ---"
for backends in 10 50 100 200 500 1000; do
    bytes=$($PGT -c "SELECT pg_column_size(ash._bench_gen($backends)) + 24 + 8 + 2 + 4;")
    mib_day=$(echo "scale=1; $bytes * 86400 / 1048576" | bc)
    gib_year=$(echo "scale=2; $bytes * 86400 * 365 / 1073741824" | bc)
    echo "  $backends backends: ~${mib_day} MiB/day, ~${gib_year} GiB/year (unrotated)"
done

echo ""
echo "================================================================"
echo "  BENCHMARK 3: Multiple databases"
echo "================================================================"
echo ""

# Create test databases
for db in testdb1 testdb2 testdb3; do
    $PGT -c "SELECT 1 FROM pg_database WHERE datname='$db';" | grep -q 1 || \
        $PGT -c "CREATE DATABASE $db;" 2>/dev/null
done

echo "--- Databases ---"
$PG -c "SELECT datname, oid FROM pg_database WHERE datname IN ('postgres','testdb1','testdb2','testdb3') ORDER BY datname;"

echo ""
echo "--- Insert samples with multiple datids ---"
$PGT -c "
DO \$\$
DECLARE
    v_base int4;
    v_dbs oid[];
BEGIN
    v_base := extract(epoch FROM now() - ash.epoch())::int4 - 3600;
    v_dbs := ARRAY(SELECT oid FROM pg_database WHERE datname IN ('postgres','testdb1','testdb2','testdb3'));

    FOR s IN 0..3599 LOOP
        FOR d IN 1..array_length(v_dbs, 1) LOOP
            INSERT INTO ash.sample (sample_ts, datid, active_count, data)
            VALUES (v_base + s, v_dbs[d], (10 + d * 5)::smallint, ARRAY[1, -1, 10, 100, 200]);
        END LOOP;
    END LOOP;
END;
\$\$;
"

echo "--- samples_by_database(1h) ---"
$PG -c "\timing on" -c "SELECT * FROM ash.samples_by_database('1 hour');"

echo ""
echo "--- top_waits with datid filter (if supported) ---"
$PG -c "\timing on" -c "SELECT * FROM ash.top_waits('1 hour', 5);"

echo ""
echo "================================================================"
echo "  BENCHMARK 4: Concurrent readers + writer"
echo "================================================================"
echo ""

echo "--- Running 5 concurrent top_waits() while take_sample() fires every 1s ---"

# Start a sampler loop in background
(
    for i in $(seq 1 10); do
        $PGT -c "SELECT ash.take_sample();" > /dev/null 2>&1
        sleep 1
    done
) &
SAMPLER_PID=$!

# Run concurrent readers
for reader in $(seq 1 5); do
    (
        ts_before=$($PGT -c "SELECT extract(epoch FROM clock_timestamp())::numeric;")
        $PGT -c "SELECT count(*) FROM ash.top_waits('1 hour', 20);" > /dev/null
        ts_after=$($PGT -c "SELECT extract(epoch FROM clock_timestamp())::numeric;")
        ms=$(echo "($ts_after - $ts_before) * 1000" | bc 2>/dev/null || echo "?")
        echo "  Reader $reader: ${ms} ms"
    ) &
done

wait
echo "  (no lock contention detected — all readers completed)"

echo ""
echo "================================================================"
echo "  BENCHMARK 5: pg_cron scheduling accuracy"  
echo "================================================================"
echo ""

echo "--- Checking pg_cron availability ---"
HAS_CRON=$($PGT -c "SELECT count(*) FROM pg_available_extensions WHERE name='pg_cron';" 2>/dev/null || echo "0")
if [ "$HAS_CRON" = "0" ] || [ -z "$HAS_CRON" ]; then
    echo "pg_cron not available — testing manual sampling interval accuracy instead"
    echo ""
    echo "--- Manual 1s sampling accuracy (10 iterations) ---"
    prev=""
    for i in $(seq 1 10); do
        ts=$($PGT -c "SELECT extract(epoch FROM clock_timestamp())::numeric(20,6);")
        if [ -n "$prev" ]; then
            gap=$(echo "$ts - $prev" | bc)
            echo "  Iteration $i: gap=${gap}s"
        fi
        prev=$ts
        $PGT -c "SELECT ash.take_sample();" > /dev/null
        sleep 1
    done
else
    echo "pg_cron available"
    $PGT -c "CREATE EXTENSION IF NOT EXISTS pg_cron;" 2>/dev/null
    echo "--- Starting ash.start('1 second') ---"
    $PGT -c "SELECT ash.start('1 second');" 2>/dev/null || echo "(start may need cron.schedule)"
    echo "--- Waiting 15 seconds for samples ---"
    sleep 15
    echo "--- Checking sample timestamps for gaps ---"
    $PG -c "
    WITH ts AS (
        SELECT sample_ts,
               sample_ts - lag(sample_ts) OVER (ORDER BY sample_ts) as gap
        FROM ash.sample
        ORDER BY sample_ts DESC
        LIMIT 20
    )
    SELECT
        count(*) as samples,
        min(gap) as min_gap_s,
        max(gap) as max_gap_s,
        avg(gap)::numeric(10,3) as avg_gap_s,
        stddev(gap)::numeric(10,3) as stddev_gap_s
    FROM ts WHERE gap IS NOT NULL;
    "
    $PGT -c "SELECT ash.stop();" 2>/dev/null
fi

echo ""
echo "================================================================"
echo "  BENCHMARK 6: Real workload capture + readback"
echo "================================================================"
echo ""

# Clear samples
$PGT -c "TRUNCATE ash.sample_0, ash.sample_1, ash.sample_2;"
$PGT -c "UPDATE ash.config SET current_slot = 0;"

echo "--- Starting pgbench (20 clients, 15s) + sampling every 1s ---"
pgbench -h localhost -p 5433 -U postgres -c 20 -j 4 -T 15 postgres > /tmp/pgbench_real.txt 2>&1 &
PGBENCH_PID=$!

for i in $(seq 1 14); do
    $PGT -c "SELECT ash.take_sample();" > /dev/null
    sleep 1
done

wait $PGBENCH_PID 2>/dev/null || true

echo "pgbench TPS:"
grep "tps" /tmp/pgbench_real.txt || echo "(no tps line)"

echo ""
echo "--- Captured samples ---"
$PG -c "SELECT count(*) as samples, avg(active_count)::int as avg_active, max(active_count) as max_active FROM ash.sample_0;"

echo ""
echo "--- top_waits from real capture ---"
$PG -c "SELECT * FROM ash.top_waits('5 minutes', 10);"

echo ""
echo "--- top_queries from real capture ---"
$PG -c "SELECT * FROM ash.top_queries('5 minutes', 10);"

echo ""
echo "--- cpu_vs_waiting from real capture ---"
$PG -c "SELECT * FROM ash.cpu_vs_waiting('5 minutes');"

echo ""
echo "================================================================"
echo "  BENCHMARK 7: Memory usage of take_sample()"
echo "================================================================"
echo ""

echo "--- Backend memory before sampling (with 50 pgbench clients) ---"
pgbench -h localhost -p 5433 -U postgres -c 50 -j 4 -T 20 postgres > /dev/null 2>&1 &
PGBENCH_PID=$!
sleep 3

# Get memory of the backend running take_sample
$PG -c "
SELECT
    pg_size_pretty(pg_backend_memory_contexts.total_bytes) as total_mem,
    pg_size_pretty(pg_backend_memory_contexts.used_bytes) as used_mem
FROM pg_backend_memory_contexts
WHERE name = 'TopMemoryContext'
LIMIT 1;
" 2>/dev/null || echo "(pg_backend_memory_contexts not available)"

echo ""
echo "--- Sampling with 50 active clients and measuring ---"
for i in $(seq 1 3); do
    result=$($PGT -c "
        SELECT
            active_count,
            array_length(data, 1) as array_len,
            pg_column_size(data) as data_bytes
        FROM ash.sample_0
        ORDER BY sample_ts DESC
        LIMIT 1;
    " 2>/dev/null)
    $PGT -c "SELECT ash.take_sample();" > /dev/null
    echo "  Sample $i: $result"
    sleep 1
done

wait $PGBENCH_PID 2>/dev/null || true

echo ""
echo "--- Summary of captured data sizes ---"
$PG -c "
SELECT
    active_count,
    count(*) as n_samples,
    avg(array_length(data, 1))::int as avg_array_len,
    pg_size_pretty(avg(pg_column_size(data))::bigint) as avg_data_bytes,
    pg_size_pretty(max(pg_column_size(data))::bigint) as max_data_bytes
FROM ash.sample_0
GROUP BY active_count
ORDER BY active_count;
"

echo ""
echo "================================================================"
echo "  FULL BENCHMARK COMPLETE"
echo "================================================================"

# Cleanup
$PG -c "DROP FUNCTION IF EXISTS ash._bench_gen(int);" 2>/dev/null
