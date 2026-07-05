-- pg_ash: upgrade from 1.5 to 2.0 (development)
--
-- 2.0 is a breaking release: the reader API is redesigned (issue #113,
-- blueprints/AAS_API.md). This development wrapper replays the in-progress 2.0
-- installer, which:
--   * snapshots existing reader-role EXECUTE grants, then drops every removed
--     v1.x reader and draft aas_* function (all overloads / _at twins) and the
--     changed-signature name `samples` via the top-of-installer drop block, so
--     the resulting schema equals a fresh 2.0 install (CI asserts this),
--   * creates the 2.0 reader surface (periods, aas, timeline, top, compare,
--     samples, report, chart, summary) with catalog comments and grants,
--   * re-applies the snapshotted reader grants to the surviving/recreated
--     functions, and re-runs ash.grant_reader() for every role that held the
--     full pre-upgrade reader bundle — so a configured reader role can use
--     the whole 2.0 surface (new readers AND their new internal helpers)
--     without manual intervention; roles holding only partial manual grants
--     are restored by exact signature and never widened,
--   * grants the default reader bundle to pg_monitor (best-effort, new in
--     2.0 — see the block at the end of the installer; opt out afterwards
--     with `select ash.revoke_reader('pg_monitor')`), and
--   * stamps ash.config.version = '2.0' (and the column default).
--
-- Sampling, storage, rollups, and admin/lifecycle functions are unchanged.
-- Re-apply-safe: the installer is idempotent (CREATE OR REPLACE / IF NOT EXISTS
-- plus the deterministic drop block), so running this script again is a no-op.

\set ON_ERROR_STOP on
begin;
\ir ash-install.sql
commit;
