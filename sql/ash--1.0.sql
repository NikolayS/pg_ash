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
    encoding_version   smallint NOT NULL DEFAULT 1,
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
                   CHECK (data[1] < 0 AND array_length(data, 1) >= 2),
    slot         smallint NOT NULL DEFAULT ash.current_slot()
) PARTITION BY LIST (slot);

-- Create partitions
CREATE TABLE IF NOT EXISTS ash.sample_0 PARTITION OF ash.sample FOR VALUES IN (0);
CREATE TABLE IF NOT EXISTS ash.sample_1 PARTITION OF ash.sample FOR VALUES IN (1);
CREATE TABLE IF NOT EXISTS ash.sample_2 PARTITION OF ash.sample FOR VALUES IN (2);

-- Create indexes on partitions
-- (sample_ts) for time-range reader queries
CREATE INDEX IF NOT EXISTS sample_0_ts_idx ON ash.sample_0 (sample_ts);
CREATE INDEX IF NOT EXISTS sample_1_ts_idx ON ash.sample_1 (sample_ts);
CREATE INDEX IF NOT EXISTS sample_2_ts_idx ON ash.sample_2 (sample_ts);
-- (datid, sample_ts) for per-database time-range queries
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

    -- Minimum: one wait group = [-wid, count, qid] = 3 elements
    -- But could be [-wid, count] = 2 if count=0... no, count > 0
    -- Actually minimum is [-wid, 1, qid] = 3
    IF v_len < 2 THEN
        RETURN false;
    END IF;

    -- First element must be a negative wait_id marker
    IF p_data[1] >= 0 THEN
        RETURN false;
    END IF;

    -- Walk the structure
    v_idx := 1;
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
    v_rows_inserted int := 0;
    v_state text;
    v_type text;
    v_event text;
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
    -- CPU* means the backend is active with no wait event reported. This is
    -- either genuine CPU work or an uninstrumented code path in Postgres.
    -- The asterisk signals this ambiguity. See https://gaps.wait.events
    FOR v_rec IN
        SELECT DISTINCT
            sa.state,
            COALESCE(sa.wait_event_type,
                CASE
                    WHEN sa.state = 'active' THEN 'CPU*'
                END
            ) as wait_type,
            COALESCE(sa.wait_event,
                CASE
                    WHEN sa.state = 'active' THEN 'CPU*'
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
            -- Build encoded array using array_agg — O(n) instead of O(n²)
            -- 1. Group backends by wait_id, collect qids
            -- 2. Build per-group sub-arrays: [-wait_id, count] || qids
            -- 3. Concatenate all sub-arrays into one flat result

            -- Step 1+2: build per-group sub-arrays into a temp table
            CREATE TEMP TABLE IF NOT EXISTS _ash_groups (
                gnum int, group_arr integer[]
            ) ON COMMIT DROP;
            TRUNCATE _ash_groups;

            INSERT INTO _ash_groups (gnum, group_arr)
            SELECT
                row_number() OVER () as gnum,
                ARRAY[(-wc.id)::integer, count(*)::integer] || array_agg(COALESCE(qc.map_id, 0)::integer)
            FROM pg_stat_activity sa
            JOIN _ash_wait_cache wc
              ON wc.state = sa.state
             AND wc.type = COALESCE(sa.wait_event_type,
                    CASE WHEN sa.state = 'active' THEN 'CPU*' END)
             AND wc.event = COALESCE(sa.wait_event,
                    CASE WHEN sa.state = 'active' THEN 'CPU*' END)
            LEFT JOIN _ash_qids qc ON qc.query_id = sa.query_id
            WHERE sa.state IN ('active', 'idle in transaction', 'idle in transaction (aborted)')
              AND (sa.backend_type = 'client backend'
                   OR (v_include_bg AND sa.backend_type IN ('autovacuum worker', 'logical replication worker', 'parallel worker', 'background worker')))
              AND sa.pid != pg_backend_pid()
              AND COALESCE(sa.datid, 0::oid) = v_datid_rec.datid
            GROUP BY wc.id;

            -- Step 3: flatten all group arrays into one
            IF EXISTS (SELECT 1 FROM _ash_groups) THEN
                SELECT array_agg(el ORDER BY g.gnum, u.ord)
                INTO v_data
                FROM _ash_groups g,
                     LATERAL unnest(g.group_arr) WITH ORDINALITY AS u(el, ord);

                SELECT count(*)::smallint INTO v_active_count
                FROM pg_stat_activity sa
                WHERE sa.state IN ('active', 'idle in transaction', 'idle in transaction (aborted)')
                  AND (sa.backend_type = 'client backend'
                       OR (v_include_bg AND sa.backend_type IN ('autovacuum worker', 'logical replication worker', 'parallel worker', 'background worker')))
                  AND sa.pid != pg_backend_pid()
                  AND COALESCE(sa.datid, 0::oid) = v_datid_rec.datid;

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
    wait_event text,
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
    v_type text;
    v_event text;
    v_query_id int8;
BEGIN
    -- Basic validation
    IF p_data IS NULL OR array_length(p_data, 1) IS NULL THEN
        RETURN;
    END IF;

    v_len := array_length(p_data, 1);

    -- Basic structure check: first element must be negative (wait_id marker)
    IF v_len < 2 OR p_data[1] >= 0 THEN
        RAISE WARNING 'ash.decode_sample: invalid data array';
        RETURN;
    END IF;

    -- Walk the structure
    v_idx := 1;
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
        SELECT w.type, w.event
        INTO v_type, v_event
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

            wait_event := CASE WHEN v_event LIKE v_type || '%' THEN v_event ELSE v_type || ':' || v_event END;
            query_id := v_query_id;
            count := 1;
            RETURN NEXT;
        END LOOP;
    END LOOP;

    RETURN;
END;
$$;


--------------------------------------------------------------------------------
-- STEP 3: Rotation function
--------------------------------------------------------------------------------

-- Rotate partitions: advance current_slot, truncate the old previous partition
CREATE OR REPLACE FUNCTION ash.rotate()
RETURNS text
LANGUAGE plpgsql
AS $$
DECLARE
    v_old_slot smallint;
    v_new_slot smallint;
    v_truncate_slot smallint;
    v_rotation_period interval;
    v_rotated_at timestamptz;
    v_min_sample_ts int4;
    v_deleted_queries int;
BEGIN
    -- Try to acquire advisory lock to prevent concurrent rotation
    IF NOT pg_try_advisory_lock(hashtext('ash_rotate')) THEN
        RETURN 'skipped: another rotation in progress';
    END IF;

    BEGIN
        -- Get current config
        SELECT current_slot, rotation_period, rotated_at
        INTO v_old_slot, v_rotation_period, v_rotated_at
        FROM ash.config
        WHERE singleton = true;

        -- Check if we rotated too recently (within 90% of rotation_period)
        IF now() - v_rotated_at < v_rotation_period * 0.9 THEN
            PERFORM pg_advisory_unlock(hashtext('ash_rotate'));
            RETURN 'skipped: rotated too recently at ' || v_rotated_at::text;
        END IF;

        -- Set lock timeout to avoid blocking on long-running queries
        SET LOCAL lock_timeout = '2s';

        -- Calculate new slot (0 -> 1 -> 2 -> 0)
        v_new_slot := (v_old_slot + 1) % 3;

        -- The partition to truncate is the one that was "previous" before rotation
        -- which is (old_slot - 1 + 3) % 3, but after we advance, it becomes
        -- the "next" partition: (new_slot + 1) % 3
        v_truncate_slot := (v_new_slot + 1) % 3;

        -- Advance current_slot first (before truncate)
        UPDATE ash.config
        SET current_slot = v_new_slot,
            rotated_at = now()
        WHERE singleton = true;

        -- Truncate the recycled partition by explicit name
        CASE v_truncate_slot
            WHEN 0 THEN TRUNCATE ash.sample_0;
            WHEN 1 THEN TRUNCATE ash.sample_1;
            WHEN 2 THEN TRUNCATE ash.sample_2;
        END CASE;

        -- Automatic query_map garbage collection
        -- Find minimum sample_ts across all live partitions (current and previous)
        SELECT min(sample_ts) INTO v_min_sample_ts
        FROM ash.sample
        WHERE slot IN (v_new_slot, v_old_slot);

        -- Delete query_map entries older than the oldest surviving sample
        IF v_min_sample_ts IS NOT NULL THEN
            DELETE FROM ash.query_map
            WHERE last_seen < v_min_sample_ts;
            GET DIAGNOSTICS v_deleted_queries = ROW_COUNT;
        ELSE
            v_deleted_queries := 0;
        END IF;

        PERFORM pg_advisory_unlock(hashtext('ash_rotate'));

        RETURN format('rotated: slot %s -> %s, truncated slot %s, gc''d %s query_map entries',
                      v_old_slot, v_new_slot, v_truncate_slot, v_deleted_queries);

    EXCEPTION WHEN lock_not_available THEN
        PERFORM pg_advisory_unlock(hashtext('ash_rotate'));
        RETURN 'failed: lock timeout on partition truncate, will retry next cycle';
    WHEN OTHERS THEN
        PERFORM pg_advisory_unlock(hashtext('ash_rotate'));
        RAISE;
    END;
END;
$$;


--------------------------------------------------------------------------------
-- STEP 4: Start/stop/uninstall functions
--------------------------------------------------------------------------------

-- Check if pg_cron extension is available
CREATE OR REPLACE FUNCTION ash._pg_cron_available()
RETURNS boolean
LANGUAGE sql
STABLE
AS $$
    SELECT EXISTS (
        SELECT 1 FROM pg_extension WHERE extname = 'pg_cron'
    )
$$;

-- Start sampling: create pg_cron jobs
CREATE OR REPLACE FUNCTION ash.start(p_interval interval DEFAULT '1 second')
RETURNS TABLE (job_type text, job_id bigint, status text)
LANGUAGE plpgsql
AS $$
DECLARE
    v_sampler_job bigint;
    v_rotation_job bigint;
    v_cron_version text;
BEGIN
    -- Check if pg_cron is available
    IF NOT ash._pg_cron_available() THEN
        job_type := 'error';
        job_id := NULL;
        status := 'pg_cron extension not installed';
        RETURN NEXT;
        RETURN;
    END IF;

    -- Check pg_cron version (need >= 1.5 for second granularity)
    SELECT extversion INTO v_cron_version
    FROM pg_extension WHERE extname = 'pg_cron';

    IF v_cron_version < '1.5' THEN
        job_type := 'error';
        job_id := NULL;
        status := format('pg_cron version %s too old, need >= 1.5', v_cron_version);
        RETURN NEXT;
        RETURN;
    END IF;

    -- Check for existing sampler job (idempotent)
    SELECT jobid INTO v_sampler_job
    FROM cron.job
    WHERE jobname = 'ash_sampler';

    IF v_sampler_job IS NOT NULL THEN
        job_type := 'sampler';
        job_id := v_sampler_job;
        status := 'already exists';
        RETURN NEXT;
    ELSE
        -- Create sampler job
        SELECT cron.schedule(
            'ash_sampler',
            p_interval::text,
            'SELECT ash.take_sample()'
        ) INTO v_sampler_job;

        job_type := 'sampler';
        job_id := v_sampler_job;
        status := 'created';
        RETURN NEXT;
    END IF;

    -- Check for existing rotation job (idempotent)
    SELECT jobid INTO v_rotation_job
    FROM cron.job
    WHERE jobname = 'ash_rotation';

    IF v_rotation_job IS NOT NULL THEN
        job_type := 'rotation';
        job_id := v_rotation_job;
        status := 'already exists';
        RETURN NEXT;
    ELSE
        -- Create rotation job (daily at midnight UTC)
        SELECT cron.schedule(
            'ash_rotation',
            '0 0 * * *',
            'SELECT ash.rotate()'
        ) INTO v_rotation_job;

        job_type := 'rotation';
        job_id := v_rotation_job;
        status := 'created';
        RETURN NEXT;
    END IF;

    -- Update sample_interval in config
    UPDATE ash.config SET sample_interval = p_interval WHERE singleton = true;

    RETURN;
END;
$$;

-- Stop sampling: remove pg_cron jobs
CREATE OR REPLACE FUNCTION ash.stop()
RETURNS TABLE (job_type text, job_id bigint, status text)
LANGUAGE plpgsql
AS $$
DECLARE
    v_job_id bigint;
BEGIN
    -- Check if pg_cron is available
    IF NOT ash._pg_cron_available() THEN
        job_type := 'info';
        job_id := NULL;
        status := 'pg_cron not installed, no jobs to remove';
        RETURN NEXT;
        RETURN;
    END IF;

    -- Remove sampler job
    SELECT jobid INTO v_job_id
    FROM cron.job
    WHERE jobname = 'ash_sampler';

    IF v_job_id IS NOT NULL THEN
        PERFORM cron.unschedule('ash_sampler');
        job_type := 'sampler';
        job_id := v_job_id;
        status := 'removed';
        RETURN NEXT;
    END IF;

    -- Remove rotation job
    SELECT jobid INTO v_job_id
    FROM cron.job
    WHERE jobname = 'ash_rotation';

    IF v_job_id IS NOT NULL THEN
        PERFORM cron.unschedule('ash_rotation');
        job_type := 'rotation';
        job_id := v_job_id;
        status := 'removed';
        RETURN NEXT;
    END IF;

    RETURN;
END;
$$;

-- Uninstall: stop jobs and drop schema
CREATE OR REPLACE FUNCTION ash.uninstall()
RETURNS text
LANGUAGE plpgsql
AS $$
DECLARE
    v_rec record;
    v_jobs_removed int := 0;
BEGIN
    -- Stop pg_cron jobs first
    FOR v_rec IN SELECT * FROM ash.stop() LOOP
        IF v_rec.status = 'removed' THEN
            v_jobs_removed := v_jobs_removed + 1;
        END IF;
    END LOOP;

    -- Drop the schema
    DROP SCHEMA ash CASCADE;

    RETURN format('uninstalled: removed %s pg_cron jobs, dropped ash schema', v_jobs_removed);
END;
$$;


--------------------------------------------------------------------------------
-- STEP 5: Reader and diagnostic functions
--------------------------------------------------------------------------------

-- Helper to get active slots (current and previous)
CREATE OR REPLACE FUNCTION ash._active_slots()
RETURNS smallint[]
LANGUAGE sql
STABLE
AS $$
    SELECT ARRAY[
        current_slot,
        ((current_slot - 1 + 3) % 3)::smallint
    ]
    FROM ash.config
    WHERE singleton = true
$$;

-- Status: diagnostic dashboard
CREATE OR REPLACE FUNCTION ash.status()
RETURNS TABLE (
    metric text,
    value text
)
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
    v_config record;
    v_last_sample_ts int4;
    v_samples_current int;
    v_samples_total int;
    v_wait_events int;
    v_query_ids int;
BEGIN
    -- Get config
    SELECT * INTO v_config FROM ash.config WHERE singleton = true;

    -- Last sample timestamp
    SELECT max(sample_ts) INTO v_last_sample_ts FROM ash.sample;

    -- Samples in current partition
    SELECT count(*) INTO v_samples_current
    FROM ash.sample WHERE slot = v_config.current_slot;

    -- Total samples
    SELECT count(*) INTO v_samples_total FROM ash.sample;

    -- Dictionary sizes
    SELECT count(*) INTO v_wait_events FROM ash.wait_event_map;
    SELECT count(*) INTO v_query_ids FROM ash.query_map;

    metric := 'current_slot'; value := v_config.current_slot::text; RETURN NEXT;
    metric := 'sample_interval'; value := v_config.sample_interval::text; RETURN NEXT;
    metric := 'rotation_period'; value := v_config.rotation_period::text; RETURN NEXT;
    metric := 'include_bg_workers'; value := v_config.include_bg_workers::text; RETURN NEXT;
    metric := 'installed_at'; value := v_config.installed_at::text; RETURN NEXT;
    metric := 'rotated_at'; value := v_config.rotated_at::text; RETURN NEXT;
    metric := 'time_since_rotation'; value := (now() - v_config.rotated_at)::text; RETURN NEXT;

    IF v_last_sample_ts IS NOT NULL THEN
        metric := 'last_sample_ts'; value := (ash.epoch() + v_last_sample_ts * interval '1 second')::text; RETURN NEXT;
        metric := 'time_since_last_sample'; value := (now() - (ash.epoch() + v_last_sample_ts * interval '1 second'))::text; RETURN NEXT;
    ELSE
        metric := 'last_sample_ts'; value := 'no samples'; RETURN NEXT;
    END IF;

    metric := 'samples_in_current_slot'; value := v_samples_current::text; RETURN NEXT;
    metric := 'samples_total'; value := v_samples_total::text; RETURN NEXT;
    metric := 'wait_event_map_count'; value := v_wait_events::text; RETURN NEXT;
    metric := 'wait_event_map_utilization'; value := round(v_wait_events::numeric / 32767 * 100, 2)::text || '%'; RETURN NEXT;
    metric := 'query_map_count'; value := v_query_ids::text; RETURN NEXT;

    -- pg_cron status if available
    IF ash._pg_cron_available() THEN
        metric := 'pg_cron_available'; value := 'yes'; RETURN NEXT;
        FOR metric, value IN
            SELECT 'cron_job_' || jobname,
                   format('id=%s, schedule=%s, active=%s', jobid, schedule, active)
            FROM cron.job
            WHERE jobname IN ('ash_sampler', 'ash_rotation')
        LOOP
            RETURN NEXT;
        END LOOP;
    ELSE
        metric := 'pg_cron_available'; value := 'no'; RETURN NEXT;
    END IF;

    RETURN;
END;
$$;

-- Top wait events (inline SQL decode — no plpgsql per-row overhead)
CREATE OR REPLACE FUNCTION ash.top_waits(
    p_interval interval DEFAULT '1 hour',
    p_limit int DEFAULT 10
)
RETURNS TABLE (
    wait_event text,
    samples bigint,
    pct numeric
)
LANGUAGE sql
STABLE
AS $$
    WITH waits AS (
        SELECT (-s.data[i])::smallint as wait_id, s.data[i + 1] as cnt
        FROM ash.sample s, generate_subscripts(s.data, 1) i
        WHERE s.slot = ANY(ash._active_slots())
          AND s.sample_ts >= extract(epoch FROM now() - p_interval - ash.epoch())::int4
          AND s.data[i] < 0
    ),
    totals AS (
        SELECT w.wait_id, sum(w.cnt) as cnt
        FROM waits w
        GROUP BY w.wait_id
    ),
    grand_total AS (
        SELECT sum(cnt) as total FROM totals
    ),
    ranked AS (
        SELECT
            CASE WHEN wm.event LIKE wm.type || '%' THEN wm.event ELSE wm.type || ':' || wm.event END as wait_event,
            t.cnt,
            row_number() OVER (ORDER BY t.cnt DESC) as rn
        FROM totals t
        JOIN ash.wait_event_map wm ON wm.id = t.wait_id
    ),
    top_rows AS (
        SELECT wait_event, cnt, rn FROM ranked WHERE rn < p_limit
    ),
    other AS (
        SELECT 'Other'::text as wait_event, sum(cnt) as cnt, p_limit::bigint as rn
        FROM ranked WHERE rn >= p_limit
        HAVING sum(cnt) > 0
    )
    SELECT
        r.wait_event,
        r.cnt as samples,
        round(r.cnt::numeric / gt.total * 100, 2) as pct
    FROM (SELECT * FROM top_rows UNION ALL SELECT * FROM other) r
    CROSS JOIN grand_total gt
    ORDER BY r.rn
$$;

-- Wait event timeline (time-bucketed breakdown, inline SQL decode)
CREATE OR REPLACE FUNCTION ash.wait_timeline(
    p_interval interval DEFAULT '1 hour',
    p_bucket interval DEFAULT '1 minute'
)
RETURNS TABLE (
    bucket_start timestamptz,
    wait_event text,
    samples bigint
)
LANGUAGE sql
STABLE
AS $$
    WITH waits AS (
        SELECT
            ash.epoch() + (s.sample_ts - (s.sample_ts % extract(epoch FROM p_bucket)::int4)) * interval '1 second' as bucket,
            (-s.data[i])::smallint as wait_id,
            s.data[i + 1] as cnt
        FROM ash.sample s, generate_subscripts(s.data, 1) i
        WHERE s.slot = ANY(ash._active_slots())
          AND s.sample_ts >= extract(epoch FROM now() - p_interval - ash.epoch())::int4
          AND s.data[i] < 0
    )
    SELECT
        w.bucket as bucket_start,
        CASE WHEN wm.event LIKE wm.type || '%' THEN wm.event ELSE wm.type || ':' || wm.event END as wait_event,
        sum(w.cnt) as samples
    FROM waits w
    JOIN ash.wait_event_map wm ON wm.id = w.wait_id
    GROUP BY w.bucket, wm.type, wm.event
    ORDER BY w.bucket, sum(w.cnt) DESC
$$;

-- Top queries by wait samples (inline SQL decode)
-- Extracts individual query_map_ids from the encoded array.
-- Format: [ver, -wid, count, qid, qid, ..., -wid, count, qid, ...]
-- Extracts individual query_map_ids from the encoded array.
-- Format: [-wid, count, qid, qid, ..., -wid, count, qid, ...]
-- A query_id position is: data[i] >= 0 AND data[i-1] >= 0 AND i > 1
-- (i > 1 guards against data[0] which is NULL in 1-indexed arrays)
CREATE OR REPLACE FUNCTION ash.top_queries(
    p_interval interval DEFAULT '1 hour',
    p_limit int DEFAULT 20
)
RETURNS TABLE (
    query_id bigint,
    samples bigint,
    pct numeric,
    query_text text
)
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
    v_has_pg_stat_statements boolean;
BEGIN
    SELECT EXISTS (
        SELECT 1 FROM pg_extension WHERE extname = 'pg_stat_statements'
    ) INTO v_has_pg_stat_statements;

    IF v_has_pg_stat_statements THEN
        RETURN QUERY
        WITH qids AS (
            SELECT s.data[i] as map_id
            FROM ash.sample s, generate_subscripts(s.data, 1) i
            WHERE s.slot = ANY(ash._active_slots())
              AND s.sample_ts >= extract(epoch FROM now() - p_interval - ash.epoch())::int4
              AND i > 1                   -- guard: data[0] is NULL
              AND s.data[i] >= 0          -- not a wait_id marker
              AND s.data[i - 1] >= 0      -- not a count (count follows negative marker)
        ),
        totals AS (
            SELECT map_id, count(*) as cnt
            FROM qids
            WHERE map_id > 0  -- skip sentinel 0 (NULL query_id)
            GROUP BY map_id
        ),
        grand_total AS (
            SELECT sum(cnt) as total FROM totals
        )
        SELECT
            qm.query_id,
            t.cnt as samples,
            round(t.cnt::numeric / gt.total * 100, 2) as pct,
            left(pss.query, 100) as query_text
        FROM totals t
        JOIN ash.query_map qm ON qm.id = t.map_id
        CROSS JOIN grand_total gt
        LEFT JOIN pg_stat_statements pss ON pss.queryid = qm.query_id
        ORDER BY t.cnt DESC
        LIMIT p_limit;
    ELSE
        RETURN QUERY
        WITH qids AS (
            SELECT s.data[i] as map_id
            FROM ash.sample s, generate_subscripts(s.data, 1) i
            WHERE s.slot = ANY(ash._active_slots())
              AND s.sample_ts >= extract(epoch FROM now() - p_interval - ash.epoch())::int4
              AND i > 1
              AND s.data[i] >= 0
              AND s.data[i - 1] >= 0
        ),
        totals AS (
            SELECT map_id, count(*) as cnt
            FROM qids
            WHERE map_id > 0
            GROUP BY map_id
        ),
        grand_total AS (
            SELECT sum(cnt) as total FROM totals
        )
        SELECT
            qm.query_id,
            t.cnt as samples,
            round(t.cnt::numeric / gt.total * 100, 2) as pct,
            NULL::text as query_text
        FROM totals t
        JOIN ash.query_map qm ON qm.id = t.map_id
        CROSS JOIN grand_total gt
        ORDER BY t.cnt DESC
        LIMIT p_limit;
    END IF;
END;
$$;

-- Wait event type distribution
CREATE OR REPLACE FUNCTION ash.waits_by_type(
    p_interval interval DEFAULT '1 hour'
)
RETURNS TABLE (
    wait_event_type text,
    samples bigint,
    pct numeric
)
LANGUAGE sql
STABLE
AS $$
    WITH waits AS (
        SELECT
            (-s.data[i])::smallint as wait_id,
            s.data[i + 1] as cnt
        FROM ash.sample s, generate_subscripts(s.data, 1) i
        WHERE s.slot = ANY(ash._active_slots())
          AND s.sample_ts >= extract(epoch FROM now() - p_interval - ash.epoch())::int4
          AND s.data[i] < 0
    ),
    totals AS (
        SELECT wm.type as wait_type, sum(w.cnt) as cnt
        FROM waits w
        JOIN ash.wait_event_map wm ON wm.id = w.wait_id
        GROUP BY wm.type
    ),
    grand_total AS (
        SELECT sum(cnt) as total FROM totals
    )
    SELECT
        t.wait_type as wait_event_type,
        t.cnt as samples,
        round(t.cnt::numeric / gt.total * 100, 2) as pct
    FROM totals t, grand_total gt
    ORDER BY t.cnt DESC
$$;

-- Samples by database
-- Top queries with text from pg_stat_statements
-- Returns query text and pgss stats when pg_stat_statements is available,
-- NULL columns otherwise.
CREATE OR REPLACE FUNCTION ash.top_queries_with_text(
    p_interval interval DEFAULT '1 hour',
    p_limit int DEFAULT 20
)
RETURNS TABLE (
    query_id bigint,
    samples bigint,
    pct numeric,
    calls bigint,
    mean_time_ms numeric,
    query_text text
)
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
    v_has_pgss boolean;
BEGIN
    SELECT EXISTS (
        SELECT 1 FROM pg_extension WHERE extname = 'pg_stat_statements'
    ) INTO v_has_pgss;

    IF v_has_pgss THEN
        RETURN QUERY
        WITH qids AS (
            SELECT s.data[i] as map_id
            FROM ash.sample s, generate_subscripts(s.data, 1) i
            WHERE s.slot = ANY(ash._active_slots())
              AND s.sample_ts >= extract(epoch FROM now() - p_interval - ash.epoch())::int4
              AND i > 1
              AND s.data[i] >= 0
              AND s.data[i - 1] >= 0
        ),
        totals AS (
            SELECT map_id, count(*) as cnt
            FROM qids
            WHERE map_id > 0
            GROUP BY map_id
        ),
        grand_total AS (
            SELECT sum(cnt) as total FROM totals
        )
        SELECT
            qm.query_id,
            t.cnt as samples,
            round(t.cnt::numeric / gt.total * 100, 2) as pct,
            pss.calls,
            round(pss.mean_exec_time::numeric, 2) as mean_time_ms,
            left(pss.query, 200) as query_text
        FROM totals t
        JOIN ash.query_map qm ON qm.id = t.map_id
        CROSS JOIN grand_total gt
        LEFT JOIN pg_stat_statements pss ON pss.queryid = qm.query_id
        ORDER BY t.cnt DESC
        LIMIT p_limit;
    ELSE
        RETURN QUERY
        WITH qids AS (
            SELECT s.data[i] as map_id
            FROM ash.sample s, generate_subscripts(s.data, 1) i
            WHERE s.slot = ANY(ash._active_slots())
              AND s.sample_ts >= extract(epoch FROM now() - p_interval - ash.epoch())::int4
              AND i > 1
              AND s.data[i] >= 0
              AND s.data[i - 1] >= 0
        ),
        totals AS (
            SELECT map_id, count(*) as cnt
            FROM qids
            WHERE map_id > 0
            GROUP BY map_id
        ),
        grand_total AS (
            SELECT sum(cnt) as total FROM totals
        )
        SELECT
            qm.query_id,
            t.cnt as samples,
            round(t.cnt::numeric / gt.total * 100, 2) as pct,
            NULL::bigint as calls,
            NULL::numeric as mean_time_ms,
            NULL::text as query_text
        FROM totals t
        JOIN ash.query_map qm ON qm.id = t.map_id
        CROSS JOIN grand_total gt
        ORDER BY t.cnt DESC
        LIMIT p_limit;
    END IF;
END;
$$;

-- Wait profile for a specific query — what is this query waiting on?
-- Walks the encoded arrays, finds the query_map_id, then looks back to find
-- which wait group it belongs to (the nearest preceding negative element).
CREATE OR REPLACE FUNCTION ash.query_waits(
    p_query_id bigint,
    p_interval interval DEFAULT '1 hour'
)
RETURNS TABLE (
    wait_event text,
    samples bigint,
    pct numeric
)
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
    v_map_id int4;
BEGIN
    SELECT id INTO v_map_id FROM ash.query_map WHERE query_id = p_query_id;
    IF v_map_id IS NULL THEN
        RETURN;
    END IF;

    RETURN QUERY
    WITH hits AS (
        -- Find every position where the query appears in a sample,
        -- then walk backwards to find the wait group marker
        SELECT
            (SELECT (- s.data[j])::smallint
             FROM generate_series(i, 1, -1) j
             WHERE s.data[j] < 0
             LIMIT 1
            ) as wait_id
        FROM ash.sample s, generate_subscripts(s.data, 1) i
        WHERE s.slot = ANY(ash._active_slots())
          AND s.sample_ts >= extract(epoch FROM now() - p_interval - ash.epoch())::int4
          AND i > 1
          AND s.data[i] = v_map_id
          AND s.data[i] >= 0
          AND s.data[i - 1] >= 0  -- it's a query_id position, not a count
    ),
    totals AS (
        SELECT wait_id, count(*) as cnt
        FROM hits
        WHERE wait_id IS NOT NULL
        GROUP BY wait_id
    ),
    grand_total AS (
        SELECT sum(cnt) as total FROM totals
    )
    SELECT
        CASE WHEN wm.event LIKE wm.type || '%' THEN wm.event ELSE wm.type || ':' || wm.event END as wait_event,
        t.cnt as samples,
        round(t.cnt::numeric / gt.total * 100, 2) as pct
    FROM totals t
    JOIN ash.wait_event_map wm ON wm.id = t.wait_id
    CROSS JOIN grand_total gt
    ORDER BY t.cnt DESC;
END;
$$;

CREATE OR REPLACE FUNCTION ash.samples_by_database(
    p_interval interval DEFAULT '1 hour'
)
RETURNS TABLE (
    database_name text,
    datid oid,
    samples bigint,
    total_backends bigint
)
LANGUAGE sql
STABLE
AS $$
    SELECT
        COALESCE(d.datname, '<background>') as database_name,
        s.datid,
        count(*) as samples,
        sum(s.active_count) as total_backends
    FROM ash.sample s
    LEFT JOIN pg_database d ON d.oid = s.datid
    WHERE s.slot = ANY(ash._active_slots())
      AND s.sample_ts >= extract(epoch FROM now() - p_interval - ash.epoch())::int4
    GROUP BY s.datid, d.datname
    ORDER BY total_backends DESC
$$;

-------------------------------------------------------------------------------
-- Absolute time range functions — for incident investigation
-------------------------------------------------------------------------------

-- Convert a timestamptz to our internal sample_ts (seconds since epoch)
CREATE OR REPLACE FUNCTION ash._to_sample_ts(p_ts timestamptz)
RETURNS int4
LANGUAGE sql
IMMUTABLE
AS $$
    SELECT extract(epoch FROM p_ts - ash.epoch())::int4
$$;

-- Top waits in an absolute time range
CREATE OR REPLACE FUNCTION ash.top_waits_at(
    p_start timestamptz,
    p_end timestamptz,
    p_limit int DEFAULT 10
)
RETURNS TABLE (
    wait_event text,
    samples bigint,
    pct numeric
)
LANGUAGE sql
STABLE
AS $$
    WITH waits AS (
        SELECT (-s.data[i])::smallint as wait_id, s.data[i + 1] as cnt
        FROM ash.sample s, generate_subscripts(s.data, 1) i
        WHERE s.slot = ANY(ash._active_slots())
          AND s.sample_ts >= ash._to_sample_ts(p_start)
          AND s.sample_ts < ash._to_sample_ts(p_end)
          AND s.data[i] < 0
    ),
    totals AS (
        SELECT w.wait_id, sum(w.cnt) as cnt
        FROM waits w
        GROUP BY w.wait_id
    ),
    grand_total AS (
        SELECT sum(cnt) as total FROM totals
    ),
    ranked AS (
        SELECT
            CASE WHEN wm.event LIKE wm.type || '%' THEN wm.event ELSE wm.type || ':' || wm.event END as wait_event,
            t.cnt,
            row_number() OVER (ORDER BY t.cnt DESC) as rn
        FROM totals t
        JOIN ash.wait_event_map wm ON wm.id = t.wait_id
    ),
    top_rows AS (
        SELECT wait_event, cnt, rn FROM ranked WHERE rn < p_limit
    ),
    other AS (
        SELECT 'Other'::text as wait_event, sum(cnt) as cnt, p_limit::bigint as rn
        FROM ranked WHERE rn >= p_limit
        HAVING sum(cnt) > 0
    )
    SELECT
        r.wait_event,
        r.cnt as samples,
        round(r.cnt::numeric / gt.total * 100, 2) as pct
    FROM (SELECT * FROM top_rows UNION ALL SELECT * FROM other) r
    CROSS JOIN grand_total gt
    ORDER BY r.rn
$$;

-- Top queries in an absolute time range
CREATE OR REPLACE FUNCTION ash.top_queries_at(
    p_start timestamptz,
    p_end timestamptz,
    p_limit int DEFAULT 20
)
RETURNS TABLE (
    query_id bigint,
    samples bigint,
    pct numeric,
    query_text text
)
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
    v_has_pgss boolean;
    v_start int4 := ash._to_sample_ts(p_start);
    v_end int4 := ash._to_sample_ts(p_end);
BEGIN
    SELECT EXISTS (
        SELECT 1 FROM pg_extension WHERE extname = 'pg_stat_statements'
    ) INTO v_has_pgss;

    IF v_has_pgss THEN
        RETURN QUERY
        WITH qids AS (
            SELECT s.data[i] as map_id
            FROM ash.sample s, generate_subscripts(s.data, 1) i
            WHERE s.slot = ANY(ash._active_slots())
              AND s.sample_ts >= v_start AND s.sample_ts < v_end
              AND i > 1 AND s.data[i] >= 0 AND s.data[i - 1] >= 0
        ),
        totals AS (
            SELECT map_id, count(*) as cnt FROM qids WHERE map_id > 0 GROUP BY map_id
        ),
        grand_total AS (
            SELECT sum(cnt) as total FROM totals
        )
        SELECT qm.query_id, t.cnt, round(t.cnt::numeric / gt.total * 100, 2),
               left(pss.query, 100)
        FROM totals t
        JOIN ash.query_map qm ON qm.id = t.map_id
        CROSS JOIN grand_total gt
        LEFT JOIN pg_stat_statements pss ON pss.queryid = qm.query_id
        ORDER BY t.cnt DESC LIMIT p_limit;
    ELSE
        RETURN QUERY
        WITH qids AS (
            SELECT s.data[i] as map_id
            FROM ash.sample s, generate_subscripts(s.data, 1) i
            WHERE s.slot = ANY(ash._active_slots())
              AND s.sample_ts >= v_start AND s.sample_ts < v_end
              AND i > 1 AND s.data[i] >= 0 AND s.data[i - 1] >= 0
        ),
        totals AS (
            SELECT map_id, count(*) as cnt FROM qids WHERE map_id > 0 GROUP BY map_id
        ),
        grand_total AS (
            SELECT sum(cnt) as total FROM totals
        )
        SELECT qm.query_id, t.cnt, round(t.cnt::numeric / gt.total * 100, 2),
               NULL::text
        FROM totals t
        JOIN ash.query_map qm ON qm.id = t.map_id
        CROSS JOIN grand_total gt
        ORDER BY t.cnt DESC LIMIT p_limit;
    END IF;
END;
$$;

-- Wait timeline in an absolute time range
CREATE OR REPLACE FUNCTION ash.wait_timeline_at(
    p_start timestamptz,
    p_end timestamptz,
    p_bucket interval DEFAULT '1 minute'
)
RETURNS TABLE (
    bucket_start timestamptz,
    wait_event text,
    samples bigint
)
LANGUAGE sql
STABLE
AS $$
    WITH waits AS (
        SELECT
            ash.epoch() + (s.sample_ts - (s.sample_ts % extract(epoch FROM p_bucket)::int4)) * interval '1 second' as bucket,
            (-s.data[i])::smallint as wait_id,
            s.data[i + 1] as cnt
        FROM ash.sample s, generate_subscripts(s.data, 1) i
        WHERE s.slot = ANY(ash._active_slots())
          AND s.sample_ts >= ash._to_sample_ts(p_start)
          AND s.sample_ts < ash._to_sample_ts(p_end)
          AND s.data[i] < 0
    )
    SELECT
        w.bucket as bucket_start,
        CASE WHEN wm.event LIKE wm.type || '%' THEN wm.event ELSE wm.type || ':' || wm.event END as wait_event,
        sum(w.cnt) as samples
    FROM waits w
    JOIN ash.wait_event_map wm ON wm.id = w.wait_id
    GROUP BY w.bucket, wm.type, wm.event
    ORDER BY w.bucket, sum(w.cnt) DESC
$$;

-- Wait event type distribution in an absolute time range
CREATE OR REPLACE FUNCTION ash.waits_by_type_at(
    p_start timestamptz,
    p_end timestamptz
)
RETURNS TABLE (
    wait_event_type text,
    samples bigint,
    pct numeric
)
LANGUAGE sql
STABLE
AS $$
    WITH waits AS (
        SELECT (-s.data[i])::smallint as wait_id, s.data[i + 1] as cnt
        FROM ash.sample s, generate_subscripts(s.data, 1) i
        WHERE s.slot = ANY(ash._active_slots())
          AND s.sample_ts >= ash._to_sample_ts(p_start)
          AND s.sample_ts < ash._to_sample_ts(p_end)
          AND s.data[i] < 0
    ),
    totals AS (
        SELECT wm.type as wait_type, sum(w.cnt) as cnt
        FROM waits w JOIN ash.wait_event_map wm ON wm.id = w.wait_id
        GROUP BY wm.type
    ),
    grand_total AS (
        SELECT sum(cnt) as total FROM totals
    )
    SELECT t.wait_type, t.cnt, round(t.cnt::numeric / gt.total * 100, 2)
    FROM totals t, grand_total gt
    ORDER BY t.cnt DESC
$$;

-- Query waits in an absolute time range
CREATE OR REPLACE FUNCTION ash.query_waits_at(
    p_query_id bigint,
    p_start timestamptz,
    p_end timestamptz
)
RETURNS TABLE (
    wait_event text,
    samples bigint,
    pct numeric
)
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
    v_map_id int4;
    v_start int4 := ash._to_sample_ts(p_start);
    v_end int4 := ash._to_sample_ts(p_end);
BEGIN
    SELECT id INTO v_map_id FROM ash.query_map WHERE query_id = p_query_id;
    IF v_map_id IS NULL THEN RETURN; END IF;

    RETURN QUERY
    WITH hits AS (
        SELECT
            (SELECT (- s.data[j])::smallint
             FROM generate_series(i, 1, -1) j
             WHERE s.data[j] < 0 LIMIT 1
            ) as wait_id
        FROM ash.sample s, generate_subscripts(s.data, 1) i
        WHERE s.slot = ANY(ash._active_slots())
          AND s.sample_ts >= v_start AND s.sample_ts < v_end
          AND i > 1 AND s.data[i] = v_map_id
          AND s.data[i] >= 0 AND s.data[i - 1] >= 0
    ),
    totals AS (
        SELECT wait_id, count(*) as cnt FROM hits WHERE wait_id IS NOT NULL GROUP BY wait_id
    ),
    grand_total AS (
        SELECT sum(cnt) as total FROM totals
    )
    SELECT CASE WHEN wm.event LIKE wm.type || '%' THEN wm.event ELSE wm.type || ':' || wm.event END, t.cnt, round(t.cnt::numeric / gt.total * 100, 2)
    FROM totals t
    JOIN ash.wait_event_map wm ON wm.id = t.wait_id
    CROSS JOIN grand_total gt
    ORDER BY t.cnt DESC;
END;
$$;

-------------------------------------------------------------------------------
-- Activity summary — the "morning coffee" function
-------------------------------------------------------------------------------

-- Activity summary — one-call overview of a time period
-- Returns key-value pairs: sample count, peak backends, top waits, top queries.
CREATE OR REPLACE FUNCTION ash.activity_summary(
    p_interval interval DEFAULT '24 hours'
)
RETURNS TABLE (
    metric text,
    value text
)
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
    v_total_samples bigint;
    v_total_backends bigint;
    v_peak_backends smallint;
    v_peak_ts timestamptz;
    v_databases int;
    v_min_ts int4;
    r record;
    v_rank int;
BEGIN
    v_min_ts := extract(epoch FROM now() - p_interval - ash.epoch())::int4;

    -- Basic counts
    SELECT count(*), COALESCE(sum(active_count), 0), max(active_count)
    INTO v_total_samples, v_total_backends, v_peak_backends
    FROM ash.sample
    WHERE slot = ANY(ash._active_slots())
      AND sample_ts >= v_min_ts;

    IF v_total_samples = 0 THEN
        RETURN QUERY SELECT 'status'::text, 'no data in this time range'::text;
        RETURN;
    END IF;

    -- Peak time
    SELECT ash.epoch() + sample_ts * interval '1 second'
    INTO v_peak_ts
    FROM ash.sample
    WHERE slot = ANY(ash._active_slots())
      AND sample_ts >= v_min_ts
    ORDER BY active_count DESC
    LIMIT 1;

    -- Distinct databases
    SELECT count(DISTINCT datid) INTO v_databases
    FROM ash.sample
    WHERE slot = ANY(ash._active_slots())
      AND sample_ts >= v_min_ts;

    RETURN QUERY SELECT 'time_range'::text, p_interval::text;
    RETURN QUERY SELECT 'total_samples', v_total_samples::text;
    RETURN QUERY SELECT 'avg_active_backends', round(v_total_backends::numeric / v_total_samples, 1)::text;
    RETURN QUERY SELECT 'peak_active_backends', v_peak_backends::text;
    RETURN QUERY SELECT 'peak_time', v_peak_ts::text;
    RETURN QUERY SELECT 'databases_active', v_databases::text;

    -- Top 3 waits
    v_rank := 0;
    FOR r IN SELECT tw.wait_event || ' (' || tw.pct || '%)' as desc
             FROM ash.top_waits(p_interval, 3) tw
    LOOP
        v_rank := v_rank + 1;
        RETURN QUERY SELECT 'top_wait_' || v_rank, r.desc;
    END LOOP;

    -- Top 3 queries
    v_rank := 0;
    FOR r IN SELECT tq.query_id::text || COALESCE(' — ' || left(tq.query_text, 60), '') || ' (' || tq.pct || '%)' as desc
             FROM ash.top_queries(p_interval, 3) tq
    LOOP
        v_rank := v_rank + 1;
        RETURN QUERY SELECT 'top_query_' || v_rank, r.desc;
    END LOOP;

    RETURN;
END;
$$;
