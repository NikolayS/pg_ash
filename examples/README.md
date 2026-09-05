# LLM investigation example

[llm-investigation.sql](llm-investigation.sql) is a read-only psql investigation
of an existing pg_ash installation. It freezes a window, ranks waits, selects
an observed minute, attributes the selected wait to query IDs, and retrieves
raw evidence. [llm-prompt.md](llm-prompt.md) explains the evidence to any LLM.

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

A supported LLM response would be:

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
