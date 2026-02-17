-- pg_ash: upgrade from 1.0 to 1.1
-- ash-1.1.sql is safe to run on top of 1.0 (IF NOT EXISTS / CREATE OR REPLACE)
-- This file simply loads it.
\i sql/ash-1.1.sql
