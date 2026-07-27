# pg_ash 2.0 — Reader / Analysis API

Decided design for the 2.0 reader surface. Supersedes the per-function growth
of v1.x and the intermediate `aas_*` drafts (PR #106, PR #112 first iteration).
Design discussion: issue #113. User stories: [AAS_USER_STORIES.md](AAS_USER_STORIES.md).

**2.0 is a breaking release.** The reader API is redesigned; v1.x readers are
removed (see §8). Sampling, storage, rollups, and admin/lifecycle functions
(`take_sample`, `rotate`, `rollup_*`, `start`/`stop`, `status`,
`grant_reader`/`revoke_reader`, `uninstall`) are unchanged.

---

## 1. Principles

1. **AAS is the only load unit.** Every reader reports `avg_aas`, `peak_aas`,
   `p99_aas` (numeric, in average-active-sessions), with `backend_seconds` as
   a secondary absolute column. The v1.x units `samples` and bare
   `backend_seconds`-as-primary disappear.
2. **One time convention.** Every reader takes
   `since timestamptz default null` → `now() - interval '1 hour'` and
   `until timestamptz default null` → `now()`.
   The `f(interval)` / `f_at(start, end)` twin pattern is dropped — that alone
   halves the surface. "Last 24 hours" is `since => now() - interval '24 hours'`.
3. **Dimensions are parameters, not function names.** Group-by is an argument
   of `top`; filters (`wait_event_type`, `wait_event`, `query_id`,
   `database`) are uniform named parameters across all readers.
4. **Data ≠ presentation.** Data functions return typed columns only. ASCII
   bars/charts/colors live in exactly two human helpers (§5).
5. **Source honesty (the trust property).** Aggregate readers auto-select their
   data source by window (raw → `rollup_1m` → `rollup_1h`). `periods`, `aas`,
   `timeline`, and `top` report it in `source`; `compare` reports
   `source_1` / `source_2`; `report` embeds it in `coverage.source`; and the
   presentation-only `chart` emits a `NOTICE` when hour grain widens its plan.
   `samples` is raw-only by definition, while `summary` includes a `source`
   metric. `rollup_1h_flat` marks legacy/incomplete detail only when a
   minute-capable plan must degrade to hour grain. When a request *cannot* be
   answered (the event↔query tie needs raw samples but the window exceeds raw
   retention), the reader raises a clear exception naming the boundary—never a
   silent empty result.
6. **Self-describing.** Every function carries a catalog `comment` stating the
   unit, the column contract, and the recommended next call, so an AI agent
   can navigate via `\df+` / `obj_description()` alone.
7. **No redundant prefixes.** The schema already says `ash.`; function names
   don't repeat the domain (`ash.top`, not `ash.aas_top`). The name `aas`
   survives only on the one function whose job is to return the metric
   itself: `ash.aas()`.

## 2. The surface

Seven data functions, two render helpers. (v1.5 shipped ~25 readers × 2 forms.)

| # | Function | Question answered |
|---|---|---|
| 1 | `ash.periods` | "Is it bad right now? Spike or sustained?" — triage entry point |
| 2 | `ash.aas` | "How much load in this window?" — scalar summary |
| 3 | `ash.timeline` | "When did it spike?" — time series |
| 4 | `ash.top` | "What is it? Who is it?" — breakdown / drill-down |
| 5 | `ash.compare` | "Did the deploy change load?" — two-window diff |
| 6 | `ash.samples` | "Show me the raw evidence." |
| 7 | `ash.report` | Machine ingest: one JSON load report for external monitoring / health platforms |
| 8 | `ash.chart` | Human: stacked ASCII timeline (replaces `timeline_chart`) |
| 9 | `ash.summary` | Human: key/value overview (replaces `activity_summary`) |

### Common parameters (uniform everywhere they appear)

```
since           timestamptz default null   -- null → now() - '1 hour'
until           timestamptz default null   -- null → now()
wait_event_type text        default null   -- filter, e.g. 'IO', 'Lock', 'CPU*'
wait_event      text        default null   -- filter, e.g. 'DataFileRead'
query_id        bigint      default null   -- filter
database        name        default null   -- filter
bucket          interval    default '1 minute'  -- sub-bucket grain for peak/p99
```

`peak_aas` = max per-`bucket` AAS over the window; `p99_aas` =
`percentile_cont(0.99)` over the same per-bucket AAS values, **zero-filled**
for stored buckets with no matching activity. When retained grain is coarser
than the requested `bucket`, both extrema are **NULL** rather than presenting
an hour average as a minute peak/percentile. A missing stored row does not say
why it is missing: `take_sample()` writes no row when no backend qualifies, so
a sampled-idle minute and an uncovered minute are indistinguishable until
sampling cadence/coverage is persisted by issue #137.

### 2.1 `ash.periods(until timestamptz default null)`

One row per standard trailing window (1m, 5m, 1h, 1d, 1w, 1mo) requested to
end at `until` (default `now()`). The zero-argument "start here" call.

Returns:
`(period text, period_start timestamptz, period_end timestamptz, source text,
bucket interval, buckets_with_data bigint, avg_aas numeric, peak_aas numeric,
p99_aas numeric)`

`period_start` / `period_end` are the effective returned bounds. They normally
match the requested trailing window, but an hour-only plan snaps them outward,
so `period_end` can be later than `until`. `buckets_with_data` (renamed from
`minutes_with_data`, to match `aas()`) counts covered buckets at the effective
grain named by `bucket`. Genuine `rollup_1h.minute_counts` keeps unfiltered
reads at one minute. If a minute-capable plan encounters legacy/incomplete
detail, the row reports `source = 'rollup_1h_flat'`, `bucket = '1 hour'`, and
NULL sub-hour extrema instead of synthesizing 60 measured minutes.

### 2.2 `ash.aas(since, until, wait_event_type, wait_event, query_id, database, bucket)`

Scalar load summary for one effective window, optionally filtered. This is
also the US-4 "leaf summary":
`ash.aas(wait_event => 'DataFileRead')` returns that event's avg/peak/p99.

Returns one row:
`(period_start timestamptz, period_end timestamptz, source text,
effective_bucket interval,
buckets_expected bigint, buckets_with_data bigint,
avg_aas numeric, peak_aas numeric, p99_aas numeric, backend_seconds numeric)`

The per-`bucket` peak/p99 buckets are **calendar-aligned** (§2.3): floored to
`bucket` on UTC/epoch boundaries, not anchored to `since`. `effective_bucket`
reports the bucket actually used after retained-grain widening. A bucket that
is not a whole multiple of retained grain is rounded **up** to the next
multiple, so the effective bucket is never finer than the request. On
`rollup_1h`, wait/query-filtered reads have only hour arrays; partial bounds
snap outward to complete hours and are disclosed by `period_start` /
`period_end`. Their average/backend seconds are exact for that disclosed
effective window, while
peak/p99 requested below one hour are NULL. Unfiltered/database-only reads use
per-`(ts, datid)` `minute_counts` when valid. Legacy/incomplete detail reports
`rollup_1h_flat` and follows the honest hour-grain behavior.

### 2.3 `ash.timeline(since, until, bucket interval default null, filters…)`

Time series. `bucket => null` auto-selects grain by span (≤ 6 h → 1 minute,
≤ 7 d → 1 hour, else 1 day) and is always safely bounded. Emits a row for
**every** effective bucket in the window. `data_points = 0` with NULL AAS
means there is no stored observation. It cannot distinguish sampled-idle from
uncovered time because both states store no row; issue #137 tracks the
coverage/cadence architecture needed to do that. When ranking buckets to find
a spike, use `order by peak_aas desc nulls last` — no-observation buckets carry
NULL `peak_aas`, which sorts first under a bare `desc` and would hide the spike. An **explicit**
`bucket` that would emit more than 100 000 buckets (e.g. `'1 minute'` over a
year) raises rather than materialize an unbounded result — pass `null` for
auto-grain or a coarser bucket.

An explicit bucket that is not a whole multiple of retained grain widens to
the next multiple (for example, 90 seconds over minute data becomes 2
minutes); it never rounds down to a finer bucket.

**Calendar-aligned buckets.** `bucket_start` is floored to `bucket` on
UTC/epoch boundaries — a 1-minute bucket starts on the minute, a 1-hour bucket
on the hour, a 1-day bucket on the UTC day — **not** anchored to `since`.
Consequences: the same absolute window returns the **same** `bucket_start`
labels on every call (reproducible plots, stable joins); the first
`bucket_start` may **precede** `since`; and edge buckets clipped by the window
average over their **in-window coverage only** (raw- and `rollup_1m`-backed
reads of one window therefore agree on the bucketed peak).

Returns:
`(bucket_start timestamptz, source text, data_points bigint,
avg_aas numeric, peak_aas numeric, p99_aas numeric)`

`peak_aas` and `p99_aas` stay per-minute on a `rollup_1h`-backed window only
when valid `minute_counts` preserves that detail. Wait/query-filtered reads and
legacy/incomplete `rollup_1h_flat` reads have hour grain. Their partial bounds
and sub-hour buckets widen outward to complete hours instead of falling
through to unavailable `rollup_1m`; both extrema are NULL when the requested
bucket is finer than that retained grain. `data_points` counts retained-grain
rows contributing to the effective bucket (for example, about 60 minute rows
in a covered one-hour bucket), not one-second samples.

### 2.4 `ash.top(dimension text, since, until, filters…, n int default 10, bucket, order_by text default 'avg')`

The single vertical drill. `dimension` ∈
`'wait_event_type' | 'wait_event' | 'query_id' | 'database'`.
Filters compose with the dimension, giving every v1.x drill as one grammar:

```sql
select * from ash.top('wait_event_type');                                   -- L1
select * from ash.top('wait_event', wait_event_type => 'IO');             -- L2
select * from ash.top('query_id');                                          -- top queries
select * from ash.top('wait_event', query_id => 123456789);               -- query → waits
select * from ash.top('query_id', wait_event => 'DataFileRead');          -- US-4 leaf
```

Returns:
`(key text, query_text text, source text,
period_start timestamptz, period_end timestamptz, effective_bucket interval,
avg_aas numeric, peak_aas numeric, p99_aas numeric,
backend_seconds numeric, pct numeric)`

- Every row carries exact avg/backend seconds and `pct`, plus effective bounds
  and bucket. `peak_aas` / `p99_aas` are NULL when retained grain exceeds the
  requested bucket.
- **`order_by` ∈ `'avg' | 'peak' | 'p99'` (default `'avg'`)** picks the
  ranking metric applied **before** the `n` cut. This is how you surface a
  spiky-but-low-average row: `order_by => 'peak'` ranks by the per-bucket
  spike, so a query that spiked for one minute outranks steady background rows
  that a mean would keep on top (the spike-first triage recipe). If the
  requested extreme is unavailable, ordering falls back to average/total
  rather than ranking on an undisclosed hourly surrogate. An unknown value
  raises `ash.top: unknown order_by <v>; use avg|peak|p99`.
- `query_text` is non-null only for `dimension = 'query_id'` with
  pg_stat_statements present **and** a caller that can read other roles'
  pgss text — i.e. holding `pg_read_all_stats` (e.g. via `pg_monitor`
  membership). A plain `grant_reader` role without it sees null even with
  pgss installed, because pgss restricts query text to the owning role.
  Degrades to null, never errors.
- For `dimension = 'query_id'`, the unattributed bucket is a **NULL `key`**
  (not the literal string `'unknown'`): activity whose `query_id` was not
  captured (client not reporting a queryid, truncated, or utility /
  idle-in-transaction work). It is real load, not an error; `compare()` pairs
  NULL keys and `summary()` renders it as `(unattributed)`.
- Combinations requiring the event↔query association
  (`'query_id'` + `wait_event`/`wait_event_type`, or `'wait_event'` +
  `query_id`) are answerable **only from raw samples**. Past raw retention the
  function raises, and the message now splits by how the window sits against the
  boundary:
  - **Window entirely past raw retention** — the tie is unrecoverable and
    narrowing cannot help; the message says so and points at the untied
    aggregate readers: *"…is entirely outside raw retention (raw retention
    starts at `<ts>`). The tie is unrecoverable for that window — narrowing it
    will not help. Use the untied aggregate readers instead: drop either the
    wait filter or query_id (e.g. ash.aas(), ash.timeline(), ash.top() with
    one of the two)."*
  - **Partial overlap** (window end still inside retention) — the message names
    the exact boundary to move to: *"…raw retention starts at `<ts>` but the
    requested window starts at `<ts>`. Narrow the window to start at or after
    `<ts>` … or drill without the query/event tie."*
- On a `rollup_1h`-backed window, wait/query dimensions and any database
  breakdown carrying a wait/query filter have hour grain. Partial bounds snap
  outward, and minute-requested extrema are NULL. Plain
  `dimension = 'database'` is the exception: `minute_counts` is stored per
  `(ts, datid)`, so it retains minute precision. Legacy/incomplete database
  detail reports `rollup_1h_flat`.

### 2.5 `ash.compare(since_1, until_1, since_2, until_2, dimension text default null, n int default 10, filters…, bucket)`

Before/after (US-7). With `dimension => null`: one overall row. With a
dimension: top rows by `abs(avg_delta)` (full outer across the two windows —
a wait/query present in only one window still appears).

Returns:
`(key text, query_text text, source_1 text, source_2 text,
period_start_1 timestamptz, period_end_1 timestamptz,
period_start_2 timestamptz, period_end_2 timestamptz,
effective_bucket_1 interval, effective_bucket_2 interval,
avg_aas_1 numeric, avg_aas_2 numeric, avg_delta numeric,
peak_aas_1 numeric, peak_aas_2 numeric,
p99_aas_1 numeric, p99_aas_2 numeric,
pct_1 numeric, pct_2 numeric)`

(With `dimension => null` the single row's `key` is `overall`.)

- **Zero-coverage honesty.** A window with no data coverage (e.g. entirely past
  retention) reports **NULL** on its side and a **NULL `avg_delta`** — never a
  fake `0.00` baseline that would read as "no change" — plus a `NOTICE` naming
  the empty window and pointing at `ash.status()`. This is consistent across
  **both** modes: the overall row and every per-dimension row. Within a
  *covered* window, an absent key is a true zero.
- **Grain visibility.** `source_1` / `source_2`, effective bounds, and
  `effective_bucket_1` / `_2` are constant per window. If the two retained
  read grains differ, both peak values and both p99 values are NULL as
  incomparable—even when an explicit coarse request makes the two effective
  bucket labels equal. Exact averages and `avg_delta` remain available.
  Different physical sources can still be comparable—for example valid
  `rollup_1h.minute_counts` and `rollup_1m` are both minute grain.
- **Validation from `compare`'s own frame.** An unknown `dimension` raises
  `ash.compare: unknown dimension <v>; use wait_event_type|wait_event|query_id|database (or null for one overall row)` —
  named `ash.compare`, not `ash.top`.

### 2.6 `ash.samples(since, until, n int default 100, filters…)`

Decoded raw sample rows, newest first. Role unchanged from v1.x `samples`,
re-parameterized to the 2.0 conventions.

### 2.7 `ash.report(...)` — see §4.

## 3. Wait-class taxonomy

pg_ash records active-with-no-wait-event samples with the literal
`wait_event_type = 'CPU*'` — a convention shared by other wait-event
samplers, so external consumers need no mapping layer.
**The asterisk is load-bearing and must never be "cleaned
up" to `CPU`:** such a sample is *either* genuine on-CPU work *or* an
uninstrumented code path in Postgres (a wait that Postgres does not report).
`CPU*` is the
user-facing spelling everywhere — function outputs, filters, chart legends,
catalog comments. Only the `report` JSON payload uses a fixed
lowercase key `cpu` (§4), and its docs carry the same caveat.

For `report` (and documented for all readers):

- **Classes:** `cpu` = `CPU*`, `io` = `IO`, `ipc` = `IPC`, `lock` = `Lock`,
  `lwlock` = `LWLock`.
- **`total` combines the five classes** (`cpu + io + ipc + lock + lwlock`).
  For an **average** this is unambiguous — the sum of per-class averages
  equals the average of the summed series — so `aas_avg.total` is that sum.
  For an **extreme** (`aas_worst1m` / `aas_p99` / `aas_p999`) the sum of each
  class's *independent* worst minute would describe a minute that never
  occurred; instead `total` is the extreme of the **summed per-minute series**
  (its own worst minute / percentile). This matches how `top_queryids_*.total`
  is computed and how downstream consumers derive a total-load series.
  `Activity`, `Client`, `Timeout`, `Extension`, `BufferPin` are *excluded from
  `total`* (idle internal workers, client waits, and timeout artifacts are not
  real load) but still visible in `top('wait_event_type')`, which reports every
  recorded type.

## 4. `ash.report` — machine-readable load report (JSON)

```
ash.report(
  since  timestamptz default null,   -- null → now() - '1 day'
  until    timestamptz default null,   -- null → now()
  vcpus int         default null,   -- optional; normalization is the platform's job
  n   int         default 3       -- top-N events/queryids per window
) returns jsonb
```

Returns one self-contained `jsonb` load report for the window — designed so
an external monitoring or health-assessment platform can ingest pg_ash as a
data source with a single call and no further queries.

Shape produced:

```json
{
  "vcpus": 96,                          // only when vcpus given
  "cluster_name": "main",               // current_setting('cluster_name'), omitted if empty
  "aas_avg":     {"total": N, "cpu": N, "io": N, "ipc": N, "lock": N, "lwlock": N},
  "aas_worst1m": {"total": N, "cpu": N, "io": N, "ipc": N, "lock": N, "lwlock": N},
  "aas_p99":     {"total": N, "cpu": N, "io": N, "ipc": N, "lock": N, "lwlock": N},
  "aas_p999":    {"total": N, "cpu": N, "io": N, "ipc": N, "lock": N, "lwlock": N},
  "top_events_worst1m": {"io": ["DataFileRead(89.0)", "…"], "ipc": [], "lock": [], "lwlock": []},
  "top_events_p99":     {"io": [], "ipc": [], "lock": [], "lwlock": []},
  "top_events_p999":    {"io": [], "ipc": [], "lock": [], "lwlock": []},
  "top_queryids_worst1m": {"total": ["123456789(12.3)", "…"], "io": [], "ipc": [], "lock": [], "lwlock": []},
  "top_queryids_p99":     {"…": []},
  "top_queryids_p999":    {"…": []},
  "top_queryids_available": true,       // ALWAYS present — branch on this, not on key absence
  "coverage": {
    "from": "2026-07-04T00:00:00+00:00",
    "to":   "2026-07-05T00:00:00+00:00",
    "source": "rollup_1m",              // report reads rollup_1m only; coverage.source is always this
    "minutes_expected": 1440,
    "minutes_with_data": 1438,
    "raw_retention_start": "2026-07-04T18:11:00+00:00"
  }
}
```

The `top_queryids_*` objects are present only when they attributed at least one
key (consumers MUST treat them as optional), so a scraper reads
`top_queryids_available` — additive and **always present** — instead of probing
for key absence. `coverage` (also additive, always present) lets a consumer
reconcile the payload against `ash.aas()` / `ash.top()` for the same window and
detect degraded resolution (`minutes_with_data < minutes_expected`) or a reduced
attribution window (`raw_retention_start` inside the window).

**Boundary:** pg_ash is **only the data source**. Consumer-side scoring —
thresholds, health zoning, normalization against vCPU counts, display labels,
ingestion pipelines — is entirely the consumer's concern. pg_ash emits raw
AAS numbers in the payload shape above and knows nothing about how they are
scored. This payload contract is frozen per 2.0 minor line: keys are only
ever added, never renamed or removed.

Semantics:

- **1-minute resolution.** All per-class series are per-minute AAS,
  zero-filled over the coverage of the window; hence the `…1m` key names.
  pg_ash's seconds-grade sampling makes these more accurate than
  coarse-scrape (e.g. 5-min) monitoring pipelines — peaks will read equal or
  higher than such sources, which is expected.
- `aas_worst1m.<class>` = max per-minute AAS of that class;
  `aas_p99` / `aas_p999` = `percentile_cont(0.99 / 0.999)` over the
  zero-filled per-minute series. Per-class extremes are computed
  independently (each class's own worst minute).
- `top_events_*`: per class, top-`n` wait events **at that class's
  extreme minute(s)** — worst1m: the single worst minute; p99/p999: minutes
  at or above that percentile. Entries are pre-formatted strings
  `"<event>(<aas>)"`, AAS rounded to 1 decimal. **No `cpu` key** (`CPU*` has
  no per-event breakdown by definition) and **no `total` key**.
- `top_queryids_*`: same windows, entries `"<query_id>(<aas>)"` with
  `query_id` rendered as text (int64-safe). Keys: `total` + the four
  non-cpu classes. Requires the event↔query tie → **raw samples**, and
  attribution is now decided **per extreme minute**, not by the window start: a
  `top_queryids` key appears when raw samples still cover **that class's**
  worst / percentile minute(s), *even if the window start predates raw
  retention* (the default 1-day window typically starts right at the boundary
  while the worst minute is well inside it — dropping attribution wholesale used
  to throw away exactly the answer the report exists to give). Percentile sets
  attribute over their **raw-covered subset**. `top_queryids_available`
  (boolean, **always present**) says whether any attribution was possible —
  branch on it, not on key absence.
- `coverage` (always present): `{from, to, source, minutes_expected,
  minutes_with_data, raw_retention_start}`. `source` is always `rollup_1m`
  (report is exclusively rollup-backed at 1-minute resolution).
- Never raises for missing data: classes with no samples report `0`; if
  the whole window has no coverage at all, returns `null` (consumers should
  skip ingestion for the period).
- Callable by the `grant_reader` role; degrades without pg_stat_statements
  (query ids still come from samples; only `query_text` is pgss-dependent and
  is not part of this payload anyway).

`vcpus` is a pass-through convenience only (echoed into the payload);
pg_ash never uses it in computation.

## 5. Human render helpers

- `ash.chart(since, until, bucket default null, n int default 3, width int default 40, color boolean default false)` —
  the stacked per-bucket AAS chart (v1.x `timeline_chart`, rebuilt on the 2.0
  internals and time convention). Returns a composite/set, so **`select *` is
  required** — a bare `select ash.chart(...)` collapses every column into one
  composite `chart` column. The legend/series is the window-wide top-`n`
  wait events **UNION any event that is top-1 in at least one bucket**, plus
  `Other` — so a single-bucket spike culprit always appears in the legend even
  if it never makes the window-wide top-N. Buckets are calendar-aligned exactly
  like `ash.timeline()` (§2.3). When only hour arrays can answer a partial
  window, the chart snaps the bounds outward and the bucket up to hour grain,
  then emits a `NOTICE` with the effective source, bounds, and bucket instead
  of silently falling through to missing minute data.
- `ash.summary(since, until)` — key/value overview (v1.x `activity_summary`,
  AAS units, plus top waits/queries), the human companion to `periods`.

Both are presentation-only: they may format, color, truncate. No data
function does any of that.

## 6. Source selection & retention metadata

- **Aggregate readers** (`aas`, `timeline`, `periods`, and non-tie `top` /
  `chart`) prefer `rollup_1m` for any window wider than ~1 hour that
  `rollup_1m` fully covers — even when that window is still within raw
  retention. Rollups are far cheaper for wide windows and just as accurate at
  minute grain, so triage never pays the cost of decoding raw samples. `raw`
  is used only for narrow (≤ ~1 h) windows, where it is both cheap and
  freshest, and for windows rollups cannot cover.
- **Leaf tie-drills** — any drill that needs the `wait_event ↔ query_id`
  association (`top('query_id', wait_event/​wait_event_type => …)`,
  `top('wait_event', query_id => …)`) and `samples` — force `raw`, because
  rollups don't preserve that association; past raw retention they raise (§1
  rule 5) rather than return empty.
- Typed aggregate readers report `source`
  (`raw` | `rollup_1m` | `rollup_1h` | `rollup_1h_flat` | `none`);
  `compare` reports `source_1` / `source_2`, `report` uses JSON coverage,
  `summary` uses a key/value metric, `samples` is raw-only, and `chart`
  discloses hour-grain widening by `NOTICE`. `rollup_1h_flat` is a
  conservative window-level marker for a minute-capable plan: at least one
  contributing legacy or incomplete hour lacks trustworthy minute detail.
  Scalar readers and `top` pick a single source/effective grain per result
  (never mixing grains under one undisclosed label). A typed aggregate window
  with no data reports `source = 'none'`.
- `ash.status()` gains rows for `raw_retention_start`,
  `rollup_1m_retention_start`, `rollup_1h_retention_start` so callers can
  plan windows before querying.

## 7. Cross-cutting requirements

Unchanged from [AAS_USER_STORIES.md §6](AAS_USER_STORIES.md) except:

- **Time addressing (revised):** the `f(interval)` + `f_at(start, end)` twin
  convention is **replaced** by the single `since`/`until` convention above.
  2.0 breaks compat; consistency-with-v1.x is no longer a constraint.
- **Named args dropped the `p_` prefix.** Every function parameter is now
  spelled bare: `p_from` → `since`, `p_to` → `until`, `p_limit`/`p_top` → `n`,
  `ash.start(p_interval => …)` → `ash.start(every => …)`, and the remaining
  filters lose their prefix too (`p_wait_event` → `wait_event`, `p_query_id`
  → `query_id`, …). Positional calls are unaffected; only callers passing
  named arguments need to update. This is a breaking change for v1.x named-arg
  callers.
- **Performance budgets:** rollup-backed reads (including `report`) < 100 ms
  for a 1-day window; raw-backed US-4 leaf drills over 1 hour < 1 s on a
  default-config instance.

## 8. Removed in 2.0

All of these are dropped by the 1.5 → 2.0 upgrade script (and absent from the
fresh installer). Replacements:

| Removed (incl. `_at` twin where it existed) | Replacement |
|---|---|
| `top_waits`, `top_by_type` | `top('wait_event')`, `top('wait_event_type')` |
| `top_queries`, `top_queries_with_text` | `top('query_id')` |
| `query_waits(q)` | `top('wait_event', query_id => q)` |
| `event_queries(e)` | `top('query_id', wait_event => e)` |
| `wait_timeline` | `timeline(...)` (+ `top` on the spike window) |
| `timeline_chart` | `ash.chart` |
| `activity_summary` | `ash.summary` |
| `samples_by_database` | `top('database')` |
| `minute_waits`, `hourly_queries`, `daily_peak_backends` | `timeline` / `top` with auto source |
| `samples_at` | `samples(since, until)` |
| draft `aas_at`, `aas_timeline(_at)`, `aas_wait_types(_at)`, `aas_wait_events(_at)`, `aas_queryids(_at)`, `aas_periods(until, bucket)` | §2 equivalents |

`decode_sample*` (debug) and all admin/lifecycle functions remain.

## 9. Traceability

| Story (AAS_USER_STORIES.md) | 2.0 function(s) |
|---|---|
| US-1 Triage | `periods` |
| US-2 Locate | `timeline` |
| US-3 Drill (avg+peak+p99 per row) | `top` |
| US-4 Leaf (event → queries) | `aas(wait_event=>…)` + `top('query_id', wait_event=>…)` |
| US-5 Programmatic honesty | `source` column + retention rows in `status()` + exception rule |
| US-6 Capacity | `timeline` (auto `rollup_1h`, per-bucket peak/p99) |
| US-7 Before/after | `compare` |
| US-8 Machine load-report ingest | `report` |
