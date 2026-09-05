-- lib/workload_read.sql — the read load for the calm and tail phases.
--
-- Why not `pgbench -b select-only`: that is a single indexed point lookup,
-- roughly 50 microseconds of server work per round trip. Over a Unix socket
-- the backends are still active often enough to sample; over TCP to a VM or a
-- container the round trip dwarfs the work and EVERY backend reads as `idle`
-- at every sampling instant. Measured on the docker backend: six clients,
-- 100% idle, zero samples, and a seed that failed at virtual minute 1 with a
-- failure because no qualifying activity was captured.
--
-- So the read load does a real range aggregate instead. A few thousand rows
-- through the primary key increases real server work per request, with CPU
-- and possible IO:DataFileRead waits. The active fraction still depends on
-- hardware and transport. If the seed captures no samples, inspect sampler
-- health and increase the span using the profile in demos/README.md.
--
-- :span is supplied per phase with pgbench -D span=N, so the same script gives
-- a light calm baseline and a heavier read tail.
\set aid random(1, 100000 * :scale)
SELECT sum(abalance) FROM pgbench_accounts WHERE aid BETWEEN :aid AND :aid + :span;
