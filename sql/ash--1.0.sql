-- pg_ash: Active Session History for Postgres
-- Steps 1-2: Core schema, infrastructure, sampler, and decoder

--------------------------------------------------------------------------------
-- STEP 1: Core schema and infrastructure
--------------------------------------------------------------------------------

-- Create schema
CREATE SCHEMA IF NOT EXISTS ash;

-- Epoch function: 2026-01-01 00:00:00 UTC
CREATE OR REPLACE FUNCTION ash.epoch()
RETURNS timestamptz
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
AS $$
    SELECT '2026-01-01 00:00:00+00'::timestamptz
$$;

-- Configuration singleton table
CREATE TABLE IF NOT EXISTS ash.config (
    singleton          bool PRIMARY KEY DEFAULT true CHECK (singleton),
    current_slot       smallint NOT NULL DEFAULT 0,
    sample_interval    interval NOT NULL DEFAULT '1 second',
    rotation_period    interval NOT NULL DEFAULT '1 day',
    include_bg_workers bool NOT NULL DEFAULT false,
    rotated_at         timestamptz NOT NULL DEFAULT clock_timestamp(),
    installed_at       timestamptz NOT NULL DEFAULT clock_timestamp()
);

-- Insert initial row if not exists
INSERT INTO ash.config (singleton) VALUES (true) ON CONFLICT DO NOTHING;

-- Wait event dictionary
CREATE TABLE IF NOT EXISTS ash.wait_event_map (
    id    smallint PRIMARY KEY GENERATED ALWAYS AS IDENTITY (START WITH 1),
    state text NOT NULL,
    type  text NOT NULL,
    event text NOT NULL,
    UNIQUE (state, type, event)
);

-- Query ID dictionary with last_seen for GC
CREATE TABLE IF NOT EXISTS ash.query_map (
    id        int4 PRIMARY KEY GENERATED ALWAYS AS IDENTITY (START WITH 1),
    query_id  int8 NOT NULL UNIQUE,
    last_seen int4 NOT NULL DEFAULT extract(epoch FROM now() - ash.epoch())::int4
);

-- Current slot function
CREATE OR REPLACE FUNCTION ash.current_slot()
RETURNS smallint
LANGUAGE sql
STABLE
PARALLEL SAFE
AS $$
    SELECT current_slot FROM ash.config WHERE singleton = true
$$;

-- Sample table (partitioned by slot)
CREATE TABLE IF NOT EXISTS ash.sample (
    sample_ts    int4 NOT NULL,
    datid        oid NOT NULL,
    active_count smallint NOT NULL,
    data         integer[] NOT NULL
                   CHECK (data[1] = 1 AND array_length(data, 1) >= 3),
    slot         smallint NOT NULL DEFAULT ash.current_slot()
) PARTITION BY LIST (slot);

-- Create partitions
CREATE TABLE IF NOT EXISTS ash.sample_0 PARTITION OF ash.sample FOR VALUES IN (0);
CREATE TABLE IF NOT EXISTS ash.sample_1 PARTITION OF ash.sample FOR VALUES IN (1);
CREATE TABLE IF NOT EXISTS ash.sample_2 PARTITION OF ash.sample FOR VALUES IN (2);

-- Create indexes on partitions
CREATE INDEX IF NOT EXISTS sample_0_datid_ts_idx ON ash.sample_0 (datid, sample_ts);
CREATE INDEX IF NOT EXISTS sample_1_datid_ts_idx ON ash.sample_1 (datid, sample_ts);
CREATE INDEX IF NOT EXISTS sample_2_datid_ts_idx ON ash.sample_2 (datid, sample_ts);

-- Register wait event function (upsert, returns id)
CREATE OR REPLACE FUNCTION ash._register_wait(p_state text, p_type text, p_event text)
RETURNS smallint
LANGUAGE plpgsql
AS $$
DECLARE
    v_id smallint;
BEGIN
    -- Try to get existing
    SELECT id INTO v_id
    FROM ash.wait_event_map
    WHERE state = p_state AND type = p_type AND event = p_event;

    IF v_id IS NOT NULL THEN
        RETURN v_id;
    END IF;

    -- Insert new entry
    INSERT INTO ash.wait_event_map (state, type, event)
    VALUES (p_state, p_type, p_event)
    ON CONFLICT (state, type, event) DO NOTHING
    RETURNING id INTO v_id;

    -- If insert succeeded, return it
    IF v_id IS NOT NULL THEN
        RETURN v_id;
    END IF;

    -- Race condition: another session inserted, fetch it
    SELECT id INTO v_id
    FROM ash.wait_event_map
    WHERE state = p_state AND type = p_type AND event = p_event;

    RETURN v_id;
END;
$$;

-- Register query function (upsert, returns int4 id)
CREATE OR REPLACE FUNCTION ash._register_query(p_query_id int8)
RETURNS int4
LANGUAGE plpgsql
AS $$
DECLARE
    v_id int4;
    v_now_ts int4;
BEGIN
    v_now_ts := extract(epoch FROM now() - ash.epoch())::int4;

    -- Try to get existing
    SELECT id INTO v_id
    FROM ash.query_map
    WHERE query_id = p_query_id;

    IF v_id IS NOT NULL THEN
        -- Update last_seen
        UPDATE ash.query_map SET last_seen = v_now_ts WHERE query_id = p_query_id;
        RETURN v_id;
    END IF;

    -- Insert new entry
    INSERT INTO ash.query_map (query_id, last_seen)
    VALUES (p_query_id, v_now_ts)
    ON CONFLICT (query_id) DO UPDATE SET last_seen = v_now_ts
    RETURNING id INTO v_id;

    RETURN v_id;
END;
$$;

-- Validate data array structure
CREATE OR REPLACE FUNCTION ash._validate_data(p_data integer[])
RETURNS boolean
LANGUAGE plpgsql
IMMUTABLE
AS $$
DECLARE
    v_len int;
    v_idx int;
    v_count int;
    v_qid_count int;
BEGIN
    -- Basic checks
    IF p_data IS NULL OR array_length(p_data, 1) IS NULL THEN
        RETURN false;
    END IF;

    v_len := array_length(p_data, 1);

    -- Must have version marker
    IF v_len < 3 THEN
        RETURN false;
    END IF;

    -- Version check
    IF p_data[1] != 1 THEN
        RETURN false;
    END IF;

    -- Walk the structure
    v_idx := 2;
    WHILE v_idx <= v_len LOOP
        -- Expect negative marker (wait event id)
        IF p_data[v_idx] >= 0 THEN
            RETURN false;
        END IF;

        v_idx := v_idx + 1;

        -- Expect count
        IF v_idx > v_len THEN
            RETURN false;
        END IF;

        v_count := p_data[v_idx];
        IF v_count <= 0 THEN
            RETURN false;
        END IF;

        v_idx := v_idx + 1;

        -- Expect exactly v_count query_ids (non-negative)
        v_qid_count := 0;
        WHILE v_idx <= v_len AND p_data[v_idx] >= 0 LOOP
            v_qid_count := v_qid_count + 1;
            v_idx := v_idx + 1;
        END LOOP;

        IF v_qid_count != v_count THEN
            RETURN false;
        END IF;
    END LOOP;

    RETURN true;
END;
$$;


--------------------------------------------------------------------------------
-- STEP 2: Sampler and decoder
--------------------------------------------------------------------------------

-- Core sampler function (no hstore dependency)
CREATE OR REPLACE FUNCTION ash.take_sample()
RETURNS int
LANGUAGE plpgsql
AS $$
DECLARE
    v_sample_ts int4;
    v_include_bg bool;
    v_rec record;
    v_datid_rec record;
    v_data integer[];
    v_active_count smallint;
    v_current_wait_id smallint;
    v_last_wait_id smallint;
    v_count_pos int;  -- Position of count in array for current wait event group
    v_rows_inserted int := 0;
    v_state text;
    v_type text;
    v_event text;
    v_qid int4;
BEGIN
    -- Get sample timestamp (seconds since epoch, from now())
    v_sample_ts := extract(epoch FROM now() - ash.epoch())::int4;

    -- Get config
    SELECT include_bg_workers INTO v_include_bg FROM ash.config WHERE singleton = true;

    -- Create temp table for wait event cache
    CREATE TEMP TABLE IF NOT EXISTS _ash_wait_cache (
        state text,
        type text,
        event text,
        id smallint,
        PRIMARY KEY (state, type, event)
    ) ON COMMIT DROP;

    TRUNCATE _ash_wait_cache;

    -- Cache all wait events (small table, ~600 rows max)
    INSERT INTO _ash_wait_cache (state, type, event, id)
    SELECT w.state, w.type, w.event, w.id
    FROM ash.wait_event_map w;

    -- Create temp table for query_ids seen this tick
    CREATE TEMP TABLE IF NOT EXISTS _ash_qids (
        query_id int8 PRIMARY KEY,
        map_id int4
    ) ON COMMIT DROP;

    TRUNCATE _ash_qids;

    -- Collect all distinct query_ids from active sessions
    INSERT INTO _ash_qids (query_id)
    SELECT DISTINCT sa.query_id
    FROM pg_stat_activity sa
    WHERE sa.query_id IS NOT NULL
      AND sa.state IN ('active', 'idle in transaction', 'idle in transaction (aborted)')
      AND (sa.backend_type = 'client backend'
           OR (v_include_bg AND sa.backend_type IN ('autovacuum worker', 'logical replication worker', 'parallel worker', 'background worker')))
    ON CONFLICT DO NOTHING;

    -- Bulk insert new query_ids into query_map
    INSERT INTO ash.query_map (query_id, last_seen)
    SELECT q.query_id, v_sample_ts
    FROM _ash_qids q
    WHERE NOT EXISTS (SELECT 1 FROM ash.query_map m WHERE m.query_id = q.query_id)
    ON CONFLICT (query_id) DO UPDATE SET last_seen = v_sample_ts;

    -- Bulk update last_seen for existing query_ids
    UPDATE ash.query_map m
    SET last_seen = v_sample_ts
    FROM _ash_qids q
    WHERE m.query_id = q.query_id;

    -- Get map_ids for all query_ids
    UPDATE _ash_qids q
    SET map_id = m.id
    FROM ash.query_map m
    WHERE q.query_id = m.query_id;

    -- Register any new wait events we encounter
    FOR v_rec IN
        SELECT DISTINCT
            sa.state,
            COALESCE(sa.wait_event_type,
                CASE
                    WHEN sa.state = 'active' THEN 'CPU'
                    WHEN sa.state LIKE 'idle in transaction%' THEN 'IDLE'
                END
            ) as wait_type,
            COALESCE(sa.wait_event,
                CASE
                    WHEN sa.state = 'active' THEN 'CPU'
                    WHEN sa.state LIKE 'idle in transaction%' THEN 'IDLE'
                END
            ) as wait_event
        FROM pg_stat_activity sa
        WHERE sa.state IN ('active', 'idle in transaction', 'idle in transaction (aborted)')
          AND (sa.backend_type = 'client backend'
               OR (v_include_bg AND sa.backend_type IN ('autovacuum worker', 'logical replication worker', 'parallel worker', 'background worker')))
          AND sa.pid != pg_backend_pid()
    LOOP
        BEGIN
            -- Check if already cached
            IF NOT EXISTS (
                SELECT 1 FROM _ash_wait_cache
                WHERE state = v_rec.state AND type = v_rec.wait_type AND event = v_rec.wait_event
            ) THEN
                -- Register and cache
                v_current_wait_id := ash._register_wait(v_rec.state, v_rec.wait_type, v_rec.wait_event);
                INSERT INTO _ash_wait_cache (state, type, event, id)
                VALUES (v_rec.state, v_rec.wait_type, v_rec.wait_event, v_current_wait_id)
                ON CONFLICT DO NOTHING;
            END IF;
        EXCEPTION WHEN OTHERS THEN
            RAISE WARNING 'ash.take_sample: error registering wait event: %', SQLERRM;
        END;
    END LOOP;

    -- Process each database
    FOR v_datid_rec IN
        SELECT DISTINCT COALESCE(sa.datid, 0::oid) as datid
        FROM pg_stat_activity sa
        WHERE sa.state IN ('active', 'idle in transaction', 'idle in transaction (aborted)')
          AND (sa.backend_type = 'client backend'
               OR (v_include_bg AND sa.backend_type IN ('autovacuum worker', 'logical replication worker', 'parallel worker', 'background worker')))
          AND sa.pid != pg_backend_pid()
    LOOP
        BEGIN
            -- Build complete data array for this datid
            v_data := ARRAY[1];  -- Version marker
            v_active_count := 0;
            v_last_wait_id := NULL;
            v_count_pos := NULL;

            FOR v_rec IN
                SELECT
                    sa.state,
                    COALESCE(sa.wait_event_type,
                        CASE
                            WHEN sa.state = 'active' THEN 'CPU'
                            WHEN sa.state LIKE 'idle in transaction%' THEN 'IDLE'
                        END
                    ) as wait_type,
                    COALESCE(sa.wait_event,
                        CASE
                            WHEN sa.state = 'active' THEN 'CPU'
                            WHEN sa.state LIKE 'idle in transaction%' THEN 'IDLE'
                        END
                    ) as wait_event,
                    sa.query_id
                FROM pg_stat_activity sa
                WHERE sa.state IN ('active', 'idle in transaction', 'idle in transaction (aborted)')
                  AND (sa.backend_type = 'client backend'
                       OR (v_include_bg AND sa.backend_type IN ('autovacuum worker', 'logical replication worker', 'parallel worker', 'background worker')))
                  AND sa.pid != pg_backend_pid()
                  AND COALESCE(sa.datid, 0::oid) = v_datid_rec.datid
                ORDER BY sa.state, wait_type, wait_event
            LOOP
                -- Get wait event id from cache
                SELECT id INTO v_current_wait_id
                FROM _ash_wait_cache
                WHERE state = v_rec.state AND type = v_rec.wait_type AND event = v_rec.wait_event;

                -- Get query map id (0 for NULL query_id - sentinel)
                IF v_rec.query_id IS NULL THEN
                    v_qid := 0;
                ELSE
                    SELECT map_id INTO v_qid FROM _ash_qids WHERE query_id = v_rec.query_id;
                    IF v_qid IS NULL THEN
                        v_qid := 0;
                    END IF;
                END IF;

                -- Check if we need to start a new wait event group
                IF v_last_wait_id IS NULL OR v_last_wait_id != v_current_wait_id THEN
                    -- New wait event: add marker, count=1, first qid
                    v_data := v_data || ARRAY[-v_current_wait_id::integer, 1, v_qid];
                    v_count_pos := array_length(v_data, 1) - 1;  -- Position of count (before qid)
                    v_last_wait_id := v_current_wait_id;
                ELSE
                    -- Same wait event: increment count at tracked position, add qid
                    v_data[v_count_pos] := v_data[v_count_pos] + 1;
                    v_data := v_data || v_qid;
                END IF;

                v_active_count := v_active_count + 1;
            END LOOP;

            -- Insert if we have data
            IF array_length(v_data, 1) > 1 THEN
                INSERT INTO ash.sample (sample_ts, datid, active_count, data)
                VALUES (v_sample_ts, v_datid_rec.datid, v_active_count, v_data);
                v_rows_inserted := v_rows_inserted + 1;
            END IF;

        EXCEPTION WHEN OTHERS THEN
            RAISE WARNING 'ash.take_sample: error inserting sample for datid %: %', v_datid_rec.datid, SQLERRM;
        END;
    END LOOP;

    RETURN v_rows_inserted;
END;
$$;

-- Decode sample function
CREATE OR REPLACE FUNCTION ash.decode_sample(p_data integer[])
RETURNS TABLE (
    state text,
    type text,
    event text,
    query_id int8,
    count int
)
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
    v_len int;
    v_idx int;
    v_wait_id smallint;
    v_count int;
    v_qid_idx int;
    v_map_id int4;
    v_state text;
    v_type text;
    v_event text;
    v_query_id int8;
BEGIN
    -- Basic validation
    IF p_data IS NULL OR array_length(p_data, 1) IS NULL THEN
        RETURN;
    END IF;

    v_len := array_length(p_data, 1);

    -- Version check
    IF v_len < 3 OR p_data[1] != 1 THEN
        RAISE WARNING 'ash.decode_sample: unknown version or invalid data';
        RETURN;
    END IF;

    -- Walk the structure
    v_idx := 2;
    WHILE v_idx <= v_len LOOP
        -- Get wait event id (negative marker)
        IF p_data[v_idx] >= 0 THEN
            RAISE WARNING 'ash.decode_sample: expected negative wait_id at position %', v_idx;
            RETURN;
        END IF;

        v_wait_id := -p_data[v_idx];
        v_idx := v_idx + 1;

        -- Get count
        IF v_idx > v_len THEN
            RAISE WARNING 'ash.decode_sample: unexpected end of array at position %', v_idx;
            RETURN;
        END IF;

        v_count := p_data[v_idx];
        v_idx := v_idx + 1;

        -- Look up wait event info
        SELECT w.state, w.type, w.event
        INTO v_state, v_type, v_event
        FROM ash.wait_event_map w
        WHERE w.id = v_wait_id;

        -- Process each query_id
        FOR v_qid_idx IN 1..v_count LOOP
            IF v_idx > v_len THEN
                RAISE WARNING 'ash.decode_sample: not enough query_ids for count %', v_count;
                RETURN;
            END IF;

            v_map_id := p_data[v_idx];
            v_idx := v_idx + 1;

            -- Handle sentinel (0 = NULL query_id)
            IF v_map_id = 0 THEN
                v_query_id := NULL;
            ELSE
                SELECT m.query_id INTO v_query_id
                FROM ash.query_map m
                WHERE m.id = v_map_id;
            END IF;

            state := v_state;
            type := v_type;
            event := v_event;
            query_id := v_query_id;
            count := 1;
            RETURN NEXT;
        END LOOP;
    END LOOP;

    RETURN;
END;
$$;
