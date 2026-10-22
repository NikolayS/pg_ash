# Agentic pg_ash examples

Start with a task, not a sequence of SQL to copy:

- [Investigate a completed incident](llm-prompt.md#task-1-investigate-a-completed-incident):
  the agent executes read-only queries, chooses drill bounds and filters from
  returned evidence, and changes direction or stops when history is insufficient.
- [Verify a transaction-scope change](llm-prompt.md#task-2-verify-a-transaction-scope-change):
  the agent inspects the workload, predicts the effect, runs an isolated lab,
  and independently checks the recorded before/after evidence.

Use your existing tool-enabled coding or database agent. No provider-specific
SDK or model service is built into pg_ash. An offline model that only receives
an exported report cannot perform the follow-up queries in these tasks.

[llm-investigation.sql](llm-investigation.sql) is a read-only reference and replay
script. [lock-lab.py](lock-lab.py) is a deterministic workload fixture. Neither
script is an autonomous agent. The captures below verify these SQL/workload
fixtures; they are not transcripts of agent runs.

## Captured run

[The complete output](llm-investigation-output.txt) was captured against real
Postgres 18 in Docker with pg_stat_statements, without pg_cron. The test held
one transaction open while three updates waited, then took seven real samples
at one-second deadlines. It waited for the minute to complete before running
rollups. No timestamps, counts, query results, or recordings were rewritten.
This short demonstration does not measure production overhead or sustained
one-minute concurrency.

| Step | Captured observation | Supported interpretation |
|---|---|---|
| Headline | 28 backend-seconds over two minutes; average 0.23 AAS, peak minute 0.47 AAS | Activity is concentrated in one observed minute; the other minute has no stored observation |
| Waits | Lock:tuple 14 backend-seconds, Lock:transactionid 7, Timeout:PgSleep 7 | Lock waiting dominates the sampled active time; the sleep is the fixture's open transaction |
| Timeline | The selected tuple-lock wait appears at 22:25 UTC on 2026-09-04 | Use that minute for the query drill |
| Query | `update public.pgash_llm_demo_orders set status = $1 where id = $2` carries all 14 tuple-lock backend-seconds | This statement experienced the selected wait |
| Raw evidence | The query appears with both tuple and transaction-ID lock waits | Investigate transaction scope and the blocking transaction; the stored sample alone does not identify it |

The report's `aas_avg.total` is 0.35, while `ash.aas().avg_aas` is 0.23.
This is expected: report `total` excludes Timeout, and the report averages
across the one activity-bearing minute. `ash.aas()` includes that wait class
and uses the disclosed two-minute window. Treating these as interchangeable
metrics would misread the evidence.

A supported finding from this evidence would be:

> The selected update experienced tuple and transaction-ID lock waits during
> the 22:25 UTC minute. This supports lock contention as the next investigation
> target. Inspect the blocking transaction's duration and scope; this history
> does not establish which session blocked it or which row values conflicted.
> The unobserved minute cannot establish scheduler health or idle activity.

Query IDs and timestamps vary between runs. The raw display is capped at 20
rows; it is a sample of the selected query's evidence, not an exhaustive export.
SQL text is current best-effort pg_stat_statements text rather than archived
historical SQL. Without pg_stat_statements the same investigation retains IDs
and waits with NULL text.

## Reproduce in a disposable database

Install the development candidate in a disposable database on an otherwise
quiet Postgres instance: sampling observes every database on the server. Then
run the live test. It creates and drops only its fixture table, starts
sampling, and leaves the collected history available for inspection. Use
standard libpq environment variables for the connection. Query attribution
requires `compute_query_id = on` (or `auto` with a module that computes query
IDs, such as preloaded pg_stat_statements). The explicit session setting below
also works without a preloaded optional module:

```bash
export PGHOST=127.0.0.1 PGPORT=5432 PGUSER=postgres PGDATABASE=ash_example
export PGOPTIONS='-c compute_query_id=on'
createdb "$PGDATABASE"
psql -X -v ON_ERROR_STOP=1 -f devel/sql/ash-install.sql
python3 devel/tests/llm_example_live.py --output /tmp/llm-investigation-output.txt
```

For the text variant, install pg_stat_statements in that database on a server
where it is preloaded, then run `select ash._apply_pgss_search_path();` before
the live test. The test also works without the extension and checks that no
fixture SQL text appears. It never installs optional extensions itself. CI validates the no-extension
variant; the SQL-text variant above was also verified manually.

Preserve NOTICE diagnostics alongside exported rows and JSON. In particular,
`pg_ash partial source:` means newer completed raw observations were omitted
from the selected minute rollup. Such results cannot establish complete-window
load; absence of that notice also does not prove completeness. The capture
helper combines psql stdout and stderr so diagnostics remain in its output.

The test takes about 10–75 seconds because it waits for an actual minute
boundary. Its synthetic counterpart, `devel/tests/report_contract.sql`, tests
required report keys and exact semantic relationships inside a transaction
that rolls back all fixture changes. Required-key checks accept additive keys.

<a id="beforeafter-lock-lab"></a>

## Agent-led lab: investigate, shorten the transaction, verify

Give your agent [Task 2](llm-prompt.md#task-2-verify-a-transaction-scope-change).
The commands below are its reproducible setup and can also be run manually.

Run [lock-lab.py](lock-lab.py) on a quiet, disposable server with Python 3.10+ and `psql`.
This low-load lab uses one row, one lock holder, and three waiting updates.
It does not require `pg_stat_statements` or `pg_cron`.

The first run holds a transaction open during eight seconds of client-side work.
The script closes every workload connection before it investigates the stored history.
It ranks waits, locates lock waiting, attributes query IDs, and prints raw evidence.
Then it moves the client-side work after `commit` and repeats the same four updates.
In that phase, all four updates finish before sampling starts; the holder
stays connected but idle while the client-side work continues.
The after ticks therefore contain no sampled workload backends.
The script checks committed work and sampled lock observations in both phases.
It never uses `pg_sleep()` to create the incident.

From the repository root, create an isolated instance:

```bash
export LAB_CONTAINER="ash_demo_lock_lab_$(date +%s)"
export POSTGRES_PASSWORD="$(python3 -c 'import secrets; print(secrets.token_urlsafe(24))')"
docker run -d --name "$LAB_CONTAINER" --memory=256m --cpus=1 \
  -e POSTGRES_PASSWORD -e POSTGRES_HOST_AUTH_METHOD=scram-sha-256 \
  -p 127.0.0.1::5432 postgres:18 -c log_connections=on
export PGHOST=127.0.0.1 PGUSER=postgres PGDATABASE=ash_demo_lock_lab
export PGPORT="$(docker port "$LAB_CONTAINER" 5432/tcp | cut -d: -f2)"
export PGPASSWORD="$POSTGRES_PASSWORD"
# Wait for the published TCP endpoint, not the temporary initialization server.
for attempt in $(seq 1 30); do
  pg_isready -q -d postgres && break
  sleep 1
done
createdb "$PGDATABASE"
psql -X -v ON_ERROR_STOP=1 -f sql/ash-install.sql
python3 examples/lock-lab.py
```

The container publishes Postgres only on host loopback.
Its listener accepts the Docker port forward; remote clients cannot use that host binding.
Use `postgres:17` to repeat the lab on Postgres 17.
The script requires an empty pg_ash history and an `ash_demo*` database.
Connect as both the database owner and the `ash` schema owner.
Use a database without `pg_cron`; the script owns these manual sample calls.
Do not run another sampler or workload on this instance.

The lab takes about two minutes because it waits for real minute boundaries.
It prints a new evidence directory under `/tmp/pgash-lock-lab-*`.
The directory contains fixed-window SQL, reader output, session logs, and absolute sample-call timestamps.
Use `--output /path/to/new-directory` to choose a location; existing paths are refused.
For example, replay the investigation while the database still exists:

```bash
psql -X -v ON_ERROR_STOP=1 -f /tmp/pgash-lock-lab-<run-id>/before.sql
psql -X -v ON_ERROR_STOP=1 -f /tmp/pgash-lock-lab-<run-id>/after.sql
```

Each phase uses eight one-second sample ticks and a separate, completed 60-second reader window.
The windows are half-open: `[since, until)`.
They never overlap; timestamps and stored observations remain unchanged.
The reader's minute AAS includes the full minute denominator, not only the eight sampled ticks.
Do not interpret it as continuous minute coverage.

A passing run reports 24 sampled Lock backend-observations before the change and zero afterward.
Both phases must commit four updates.
At the one-second configured interval, 24 backend-observations represent 24 estimated lock-wait backend-seconds.
This supports the transaction-lifetime change in this controlled fixture,
not a comparison under equal concurrent load during sampling.
It does not establish latency, throughput, production impact, or absence of sub-second waits.
Missing stored rows alone prove neither idle activity nor scheduler health.
The separate tick log records the actual manual sample calls.
Historical query IDs identify statements that waited, not the blocking session or row values.
Optional SQL text comes from current `pg_stat_statements`, not an archived query-text store.

### Verified functional results

Both final runs used the same script on 2026-10-21 UTC, without optional extensions.
Each pair used equal, non-overlapping 60-second reader windows.

| Postgres | Before window (UTC) | After window (UTC) | Ticks per phase | Committed updates per phase | Sampled Lock observations, before → after |
|---|---|---|---|---|---|
| 17 | 19:35–19:36 | 19:36–19:37 | 8 | 4 | 24 → 0 |
| 18 | 19:37–19:38 | 19:38–19:39 | 8 | 4 | 24 → 0 |

[The Postgres 18 capture](lock-lab-output.txt) includes unchanged reader output, sample-call timestamps, and sampler counters.
Both runs left sampler error counters unchanged and removed their fixture tables.
These are functional results for this fixture, not a production performance claim.

### Cleanup

The script drops only its uniquely named fixture table and leaves history for inspection.
Remove the disposable container after you inspect the results:

```bash
docker rm -f "$LAB_CONTAINER"
unset PGPASSWORD POSTGRES_PASSWORD
```
