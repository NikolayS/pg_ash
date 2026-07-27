-- lib/workload_lock.sql — the row-lock storm, as a pgbench script.
--
-- NO pg_sleep. Anywhere. A prototype faked contention with pg_sleep and shipped
-- `Timeout:PgSleep` as the demo's #1 wait event at 27.6% — a demo that teaches
-- the wrong lesson is worse than no demo.
--
-- The shape: every client updates the SAME row, then does genuine work while
-- still holding that row lock. The lock holder's `sum(abalance)` scan is what
-- turns a momentary conflict into a measurable queue, and it is real work, so
-- the holder itself shows up as CPU*/IO rather than as an artificial sleep.
--
-- What the sampler sees (this is the actual Postgres locking protocol, not a
-- contrivance): one waiter holds the tuple lock and waits on the holder's
-- transaction id (Lock:transactionid); every other waiter queues behind that
-- tuple lock (Lock:tuple). So a 12-client storm produces a large Lock:tuple
-- population with a Lock:transactionid alongside it, plus the holder's own
-- CPU*/IO:DataFileRead — a textbook row-contention signature.
BEGIN;
UPDATE pgbench_accounts SET abalance = abalance + 1 WHERE aid = 1;
SELECT sum(abalance) FROM pgbench_accounts WHERE bid = 1;
END;
