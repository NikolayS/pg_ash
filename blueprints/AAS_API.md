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
   `p_from timestamptz default null` → `now() - interval '1 hour'` and
   `p_to timestamptz default null` → `now()`.
   The `f(interval)` / `f_at(start, end)` twin pattern is dropped — that alone
   halves the surface. "Last 24 hours" is `p_from => now() - interval '24 hours'`.
3. **Dimensions are parameters, not function names.** Group-by is an argument
   of `top`; filters (`wait_event_type`, `wait_event`, `query_id`,
   `database`) are uniform named parameters across all readers.
4. **Data ≠ presentation.** Data functions return typed columns only. ASCII
   bars/charts/colors live in exactly two human helpers (§5).
5. **Source honesty (the trust property).** Every reader auto-selects its data
   source by window (raw → `rollup_1m` → `rollup_1h`) and reports it in a
   `source` column. When a request *cannot* be answered (the event↔query tie
   needs raw samples but the window exceeds raw retention), the reader raises
   a clear exception naming the boundary — never a silent empty result.
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
p_from            timestamptz default null   -- null → now() - '1 hour'
p_to              timestamptz default null   -- null → now()
p_wait_event_type text        default null   -- filter, e.g. 'IO', 'Lock', 'CPU*'
p_wait_event      text        default null   -- filter, e.g. 'DataFileRead'
p_query_id        bigint      default null   -- filter
p_database        name        default null   -- filter
p_bucket          interval    default '1 minute'  -- sub-bucket grain for peak/p99
```

`peak_aas` = max per-`p_bucket` AAS over the window; `p99_aas` =
`percentile_cont(0.99)` over the same per-bucket AAS values, **zero-filled**
for buckets with no activity within data coverage. Buckets with *no data*
(sampler off) are excluded from percentiles and reported via coverage columns.

### 2.1 `ash.periods(p_end timestamptz default null)`

One row per standard trailing window (1m, 5m, 1h, 1d, 1w, 1mo) ending at
`p_end` (default `now()`). The zero-argument "start here" call.

Returns:
`(period text, period_start timestamptz, period_end timestamptz, source text,
minutes_with_data bigint, avg_aas numeric, peak_aas numeric, p99_aas numeric)`

### 2.2 `ash.aas(p_from, p_to, p_wait_event_type, p_wait_event, p_query_id, p_database, p_bucket)`

Scalar load summary for one window, optionally filtered. This is also the
US-4 "leaf summary": `ash.aas(p_wait_event => 'DataFileRead')` returns that
event's avg/peak/p99.

Returns one row:
`(period_start timestamptz, period_end timestamptz, source text,
buckets_expected bigint, buckets_with_data bigint,
avg_aas numeric, peak_aas numeric, p99_aas numeric, backend_seconds numeric)`

### 2.3 `ash.timeline(p_from, p_to, p_bucket interval default null, filters…)`

Time series. `p_bucket => null` auto-selects grain by span (≤ 6 h → 1 minute,
≤ 7 d → 1 hour, else 1 day) and is always safely bounded. Emits a row for
**every** bucket in the window: `data_points = 0` with null AAS marks
"no data", distinguishing it from measured-zero load. An **explicit**
`p_bucket` that would emit more than 100 000 buckets (e.g. `'1 minute'` over a
year) raises rather than materialize an unbounded result — pass `null` for
auto-grain or a coarser bucket.

Returns:
`(bucket_start timestamptz, source text, data_points bigint,
avg_aas numeric, peak_aas numeric, p99_aas numeric)`

`p99_aas` is per-bucket over `1 minute` sub-buckets and is returned (not
null) whenever the source grain allows it (raw or `rollup_1m`-backed buckets;
null for `rollup_1h`-backed buckets) — serves US-6 capacity review.

### 2.4 `ash.top(p_dimension text, p_from, p_to, filters…, p_limit int default 10, p_bucket)`

The single vertical drill. `p_dimension` ∈
`'wait_event_type' | 'wait_event' | 'query_id' | 'database'`.
Filters compose with the dimension, giving every v1.x drill as one grammar:

```sql
select * from ash.top('wait_event_type');                                   -- L1
select * from ash.top('wait_event', p_wait_event_type => 'IO');             -- L2
select * from ash.top('query_id');                                          -- top queries
select * from ash.top('wait_event', p_query_id => 123456789);               -- query → waits
select * from ash.top('query_id', p_wait_event => 'DataFileRead');          -- US-4 leaf
```

Returns:
`(key text, query_text text, source text,
avg_aas numeric, peak_aas numeric, p99_aas numeric,
backend_seconds numeric, pct numeric)`

- **Every row carries avg + peak + p99** (US-3). `pct` is the row's share of
  the window's total AAS.
- `query_text` is non-null only for `p_dimension = 'query_id'` with
  pg_stat_statements present (degrades to null, never errors).
- Combinations requiring the event↔query association
  (`'query_id'` + `p_wait_event`/`p_wait_event_type`, or `'wait_event'` +
  `p_query_id`) are answerable **only from raw samples**. If the requested
  window exceeds raw retention, the function raises:
  `pg_ash: this drill needs raw samples; raw retention starts at <ts> but requested window starts at <ts>. Narrow the window or drill without the query/event tie.`

### 2.5 `ash.compare(p_from_1, p_to_1, p_from_2, p_to_2, p_dimension text default null, p_limit int default 10, filters…, p_bucket)`

Before/after (US-7). With `p_dimension => null`: one overall row. With a
dimension: top rows by `abs(avg_delta)` (full outer across the two windows —
a wait/query present in only one window still appears).

Returns:
`(key text, query_text text,
avg_aas_1 numeric, avg_aas_2 numeric, avg_delta numeric,
peak_aas_1 numeric, peak_aas_2 numeric,
p99_aas_1 numeric, p99_aas_2 numeric,
pct_1 numeric, pct_2 numeric)`

### 2.6 `ash.samples(p_from, p_to, p_limit int default 100, filters…)`

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
  p_from  timestamptz default null,   -- null → now() - '1 day'
  p_to    timestamptz default null,   -- null → now()
  p_vcpus int         default null,   -- optional; normalization is the platform's job
  p_top   int         default 3       -- top-N events/queryids per window
) returns jsonb
```

Returns one self-contained `jsonb` load report for the window — designed so
an external monitoring or health-assessment platform can ingest pg_ash as a
data source with a single call and no further queries.

Shape produced:

```json
{
  "vcpus": 96,                          // only when p_vcpus given
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
  "top_queryids_p999":    {"…": []}
}
```

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
- `top_events_*`: per class, top-`p_top` wait events **at that class's
  extreme minute(s)** — worst1m: the single worst minute; p99/p999: minutes
  at or above that percentile. Entries are pre-formatted strings
  `"<event>(<aas>)"`, AAS rounded to 1 decimal. **No `cpu` key** (`CPU*` has
  no per-event breakdown by definition) and **no `total` key**.
- `top_queryids_*`: same windows, entries `"<query_id>(<aas>)"` with
  `query_id` rendered as text (int64-safe). Keys: `total` + the four
  non-cpu classes. Requires the event↔query tie → **raw samples**: windows
  (or extreme minutes) outside raw retention simply omit the
  `top_queryids_*` keys — consumers MUST treat these keys as optional.
- Never raises for missing data: classes with no samples report `0`; if
  the whole window has no coverage at all, returns `null` (consumers should
  skip ingestion for the period).
- Callable by the `grant_reader` role; degrades without pg_stat_statements
  (query ids still come from samples; only `query_text` is pgss-dependent and
  is not part of this payload anyway).

`p_vcpus` is a pass-through convenience only (echoed into the payload);
pg_ash never uses it in computation.

## 5. Human render helpers

- `ash.chart(p_from, p_to, p_bucket default null, p_top int default 3, p_width int default 40, p_color boolean default false)` —
  the stacked per-bucket AAS chart (v1.x `timeline_chart`, rebuilt on the 2.0
  internals and time convention).
- `ash.summary(p_from, p_to)` — key/value overview (v1.x `activity_summary`,
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
  association (`top('query_id', p_wait_event/​p_wait_event_type => …)`,
  `top('wait_event', p_query_id => …)`) and `samples` — force `raw`, because
  rollups don't preserve that association; past raw retention they raise (§1
  rule 5) rather than return empty.
- Each reader reports the source it used in the `source` column
  (`raw` | `rollup_1m` | `rollup_1h` | `none`). Scalar readers and `top` pick a
  single source per result (never mixing — no double-counting); `timeline`
  reports its source per bucket, so a long series may show different sources
  across rows. A window with no data at all reports `source = 'none'`
  uniformly across readers.
- `ash.status()` gains rows for `raw_retention_start`,
  `rollup_1m_retention_start`, `rollup_1h_retention_start` so callers can
  plan windows before querying.

## 7. Cross-cutting requirements

Unchanged from [AAS_USER_STORIES.md §6](AAS_USER_STORIES.md) except:

- **Time addressing (revised):** the `f(interval)` + `f_at(start, end)` twin
  convention is **replaced** by the single `p_from`/`p_to` convention above.
  2.0 breaks compat; consistency-with-v1.x is no longer a constraint.
- **Performance budgets:** rollup-backed reads < 100 ms for a 1-day window;
  raw-backed reads (incl. `report` over 1 day and US-4 leaf drills
  over 1 hour) < 1 s on a default-config instance.

## 8. Removed in 2.0

All of these are dropped by the 1.5 → 2.0 upgrade script (and absent from the
fresh installer). Replacements:

| Removed (incl. `_at` twin where it existed) | Replacement |
|---|---|
| `top_waits`, `top_by_type` | `top('wait_event')`, `top('wait_event_type')` |
| `top_queries`, `top_queries_with_text` | `top('query_id')` |
| `query_waits(q)` | `top('wait_event', p_query_id => q)` |
| `event_queries(e)` | `top('query_id', p_wait_event => e)` |
| `wait_timeline` | `timeline(...)` (+ `top` on the spike window) |
| `timeline_chart` | `ash.chart` |
| `activity_summary` | `ash.summary` |
| `samples_by_database` | `top('database')` |
| `minute_waits`, `hourly_queries`, `daily_peak_backends` | `timeline` / `top` with auto source |
| `samples_at` | `samples(p_from, p_to)` |
| draft `aas_at`, `aas_timeline(_at)`, `aas_wait_types(_at)`, `aas_wait_events(_at)`, `aas_queryids(_at)`, `aas_periods(p_end, p_bucket)` | §2 equivalents |

`decode_sample*` (debug) and all admin/lifecycle functions remain.

## 9. Traceability

| Story (AAS_USER_STORIES.md) | 2.0 function(s) |
|---|---|
| US-1 Triage | `periods` |
| US-2 Locate | `timeline` |
| US-3 Drill (avg+peak+p99 per row) | `top` |
| US-4 Leaf (event → queries) | `aas(p_wait_event=>…)` + `top('query_id', p_wait_event=>…)` |
| US-5 Programmatic honesty | `source` column + retention rows in `status()` + exception rule |
| US-6 Capacity | `timeline` (auto `rollup_1h`, per-bucket peak/p99) |
| US-7 Before/after | `compare` |
| US-8 Machine load-report ingest | `report` |
