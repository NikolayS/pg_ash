# pg_ash — AAS Analysis API: User Stories

Companion to [SPEC.md](SPEC.md). This document captures the **user stories** that
the AAS (Average Active Sessions) analysis / reader API must serve. Acceptance
criteria here are deliberately concrete: they are the source of truth we polish
the API against.

- **AAS** = Average Active Sessions — the average number of backends actively
  running (Oracle/ASH term of art). `avg_aas` is backend-time per wall-clock
  second; `peak_aas` / `p99_aas` are the max / 99th-percentile of per-bucket AAS.
  All values scale by the configured sample interval.
- Related work: decided 2.0 API in **[AAS_API.md](AAS_API.md)**, design
  discussion in **issue #113**; earlier drafts: PR #106, PR #112.

## Status legend (per-story coverage of the 2.0 API design)

- ✅ **Covered** — a function already satisfies the story.
- 🟡 **Partial** — a function exists but misses part of the acceptance criteria.
- ❌ **Missing** — no function yet satisfies the story.

---

## 1. Personas

| Persona | Audience | Context |
|---|---|---|
| **On-call engineer** | Human (psql) | Paged for slowness; needs an answer in seconds. |
| **Performance engineer** | Human (psql) | Tuning; attributes load to waits and queries. |
| **Capacity planner** | Human (psql / export) | Trends, peak hours, growth over weeks. |
| **Monitoring system** | Machine (`grant_reader`) | Grafana/Datadog scraping AAS metrics for plots + alerts. |
| **AI agent** | Machine | Autonomous broad→locate→drill investigation. |

## 2. The investigation arc

The core stories form one connected narrative — **broad → locate → drill → leaf**.
Each stage constrains a different part of the API:

```
US-1 Triage         broad   "is it bad? spike or sustained?"      → periods
   ↓
US-2 Locate         when    "find the spike in time"              → timeline
   ↓
US-3 Drill          what    "type → event → query, with p99"      → top(dimension, filters…)
   ↓
US-4 Leaf           who     "for THIS event, which queries?"       → aas(event…) + top('query_id', event…)
```

US-5 (programmatic consumers), US-6 (capacity/trend), US-7 (before/after),
and US-8 (machine load-report ingest) are cross-cutting or extension stories.

---

## 3. Core stories

### US-1 — Triage: spike vs sustained

> **As an** on-call engineer responding to a slowness alert,
> **I want** AAS across standard trailing windows (1m, 5m, 1h, 1d, 1w, 1mo) in a single call,
> **so that** I can immediately judge whether load is elevated vs normal, and whether it is a momentary spike or a sustained problem.

- **Trigger:** paged, or a user reports "it's slow right now."
- **Acceptance criteria:**
  1. One call returns one row per standard window.
  2. Each row exposes `avg_aas`, `peak_aas`, **and** `p99_aas`, so spike-vs-sustained is legible without a second query.
  3. Works across full rollup retention: the wide windows (1h and up) answer from rollups — no raw dependency — while the narrow 1m/5m windows read raw (cheap and freshest at that size). Aggregate readers prefer `rollup_1m` for any >1h window it fully covers, so triage never pays raw-decode cost.
  4. Meets the performance budget for a rollup read (see §6).
- **Primary API:** `ash.periods(until)`.
- **Coverage:** ✅ Covered (adds a `source` column in 2.0).

### US-2 — Locate: find when it spiked

> **As an** engineer investigating a past incident ("slow around 2am"),
> **I want** a time series of AAS across a broad window with a peak per bucket,
> **so that** I can pinpoint exactly when load spiked and select a precise window to drill into.

- **Trigger:** a vague time reference for a past problem.
- **Acceptance criteria:**
  1. Returns one row per time bucket (bucket size configurable).
  2. Includes `peak_aas` per bucket (not just `avg_aas`), so short spikes are not averaged away; orderable by peak to surface the worst buckets.
  3. Auto-selects rollup granularity by span — per-minute for short spans, per-hour for long spans — and reaches back across the relevant rollup retention.
  4. Marks buckets with no stored observation; sampled-idle and uncovered
     time remain indistinguishable until issue #137 persists cadence/coverage.
- **Primary API:** `ash.timeline(since, until, bucket)`.
- **Coverage:** ✅ Covered by design ([AAS_API.md §2.3](AAS_API.md)); adds per-bucket `p99_aas` and explicit no-data rows.

### US-3 — Drill to culprit: type → event → query, with p99

> **As an** engineer with an identified spike window,
> **I want** to drill from wait_event_type → wait_event → query, with avg, peak, and p99 for each row,
> **so that** I can see both what the database was waiting on and whether a specific wait or query was itself spiky, and then act (add an index, kill a session, tune a query).

- **Trigger:** a spike window identified via US-1/US-2.
- **Acceptance criteria:**
  1. Breakdown by `wait_event_type` over an absolute window, with each member's `pct` of total activity.
  2. Breakdown by `wait_event`, filterable to a single `wait_event_type` (the drill-in from level 1).
  3. Breakdown by `query_id`, with `query_text` when pg_stat_statements is present (and degrading to `query_id` only otherwise).
  4. **Every breakdown row carries `avg_aas`, `peak_aas`, AND `p99_aas`** — not avg alone — so a spiky member is distinguishable from a steadily-busy one.
  5. The doc/comment is explicit that the deeper event→query tie is NOT recoverable from rollups (see US-4).
- **Primary API:** `ash.top(dimension, …)` — one function; the drill levels are `top('wait_event_type')` → `top('wait_event', wait_event_type => …)` → `top('query_id', …)`.
- **Coverage:** ✅ Covered by design — every `top` row carries `avg_aas`, `peak_aas`, `p99_aas` ([AAS_API.md §2.4](AAS_API.md)).

### US-4 — Leaf: for a specific wait event, which queries?

> **As an** engineer who has identified the dominant wait event (e.g. `IO:DataFileRead`),
> **I want** that event's avg and 1-minute p99 AAS, and the same metrics broken down per query_id contributing to that event,
> **so that** I can attribute the spiky wait to the specific queries responsible.

- **Trigger:** a specific `wait_event` is the suspect.
- **Acceptance criteria:**
  1. Given a `wait_event` and a window, return `avg_aas` + `p99_aas` (+ `peak_aas`) for that event.
  2. Return, per `query_id` contributing to that event, `avg_aas` + `p99_aas` (+ `peak_aas`), with `query_text` when available.
  3. Because rollups do not preserve the wait_event ↔ query_id association, this **reads raw samples** within raw retention.
  4. **MUST signal clearly** when the requested window exceeds raw retention, so the result is never silently empty or partial (see US-5 criterion 3).
- **Primary API:** `ash.aas(wait_event => …)` for the event summary; `ash.top('query_id', wait_event => …)` for the per-query breakdown (raw-samples-backed; raises past raw retention).
- **Coverage:** ✅ Covered by design ([AAS_API.md §2.2/§2.4](AAS_API.md)); no dedicated leaf function needed — the filter grammar expresses it.

---

## 4. Consumer & extension stories

### US-5 — Programmatic consumers: typed, stable, honest output

> **As a** monitoring system (Grafana/Datadog via `ash.grant_reader`) **or** an AI agent,
> **I want** stable typed metric columns and an explicit signal of data source/availability,
> **so that** I can plot and threshold reliably and run automated broad→locate→drill loops without ever silently getting empty or misleading results.

- **Trigger:** scheduled metric scrape, or an autonomous investigation prompt.
- **Acceptance criteria:**
  1. All readers return typed columns with no ASCII/presentation columns in the data path (any human rendering lives in a separate helper).
  2. A documented, stable column contract (names + types) suitable for BI tools.
  3. An explicit `source` (`raw` | `rollup`) and/or coverage indicator, plus discoverable retention metadata, so a caller can tell when a drill is unavailable for a given window.
  4. Self-describing catalog comments (`obj_description`) covering the term, the columns, and the recommended next call.
  5. Callable by the least-privilege reader role, and degrades gracefully when pg_stat_statements is absent.
- **Primary API:** cross-cutting across the whole family.
- **Coverage:** ✅ Covered by design — typed aggregate readers expose `source`
  fields, while `report` uses JSON coverage, `summary` has separate headline
  and wait/query drill provenance metrics, `chart` a planning `NOTICE`, and
  `samples` is raw-only; retention rows live in
  `ash.status()`, with a raise-don't-return-empty rule for unanswerable drills
  ([AAS_API.md §5–§6](AAS_API.md)).
- **Drivable from the catalog alone.** pg_ash self-documents in-DB, which is what
  lets an agent navigate it without external docs: `COMMENT ON SCHEMA ash` names
  the reader entry points and the reader-vs-ops split, and **every** function —
  the readers *and* the owner-only ops functions (`start`, `stop`,
  `take_sample`, `rotate`, `rollup_*`, `rebuild_partitions`,
  `set_debug_logging`, `uninstall`, `grant_reader`, `revoke_reader`) — carries an
  `obj_description` comment. One lookup orients the caller:
  `select obj_description('ash'::regnamespace)`, then
  `select obj_description('ash.<name>(<argtypes>)'::regprocedure)` per function.

### US-6 — Capacity & trend

> **As a** capacity planner,
> **I want** AAS aggregated by hour and day over weeks, including peak (and p99) per bucket,
> **so that** I can see growth trends and identify recurring peak hours for resource planning.

- **Trigger:** periodic capacity review.
- **Acceptance criteria:**
  1. Long-window timeline (weeks) reads `rollup_1h` efficiently.
  2. Per-bucket `peak_aas` (and ideally `p99_aas`) preserved at hour/day grain.
  3. Works to the limit of `rollup_1h` retention and signals that horizon.
- **Primary API:** `ash.timeline` over long spans.
- **Coverage:** ✅ Covered — auto `rollup_1h` selection; valid
  `rollup_1h.minute_counts` preserves unfiltered/database-only minute totals.
  Wait/query dimensions and legacy/incomplete `rollup_1h_flat` data retain
  hour grain and NULL extrema requested below that grain.

### US-7 — Before/after comparison

> **As an** engineer who just deployed a change,
> **I want** to compare AAS (overall and by wait type / by query) between two windows — e.g. the hour before vs the hour after,
> **so that** I can tell whether the change regressed database load and where.

- **Trigger:** a deploy, config change, or parameter change.
- **Acceptance criteria:**
  1. Accept two windows (baseline, comparison); return AAS for each plus the delta.
  2. Support overall and at least one breakdown dimension (wait type and/or query).
  3. Sort/highlight the largest regressions by delta.
- **Primary API:** `ash.compare(since_1, until_1, since_2, until_2, dimension, …)`.
- **Coverage:** ✅ Covered by design ([AAS_API.md §2.5](AAS_API.md)).

### US-8 — Machine load-report ingest

> **As an** external monitoring / health-assessment platform,
> **I want** one call that returns a complete load report — per-wait-class avg, worst-1-minute, p99, p99.9 AAS plus top wait events and top query ids per extreme window — as a single JSON document,
> **so that** a lightweight collector can ingest pg_ash as a data source with one query per period, with pg_ash being the only thing installed on the target database.

- **Trigger:** periodic collection (per assessment run or scheduled).
- **Acceptance criteria:**
  1. One call returns a single `jsonb` matching the documented payload contract exactly: `aas_avg` / `aas_worst1m` / `aas_p99` / `aas_p999` keyed by `total, cpu, io, ipc, lock, lwlock`; `top_events_*` (no `cpu`/`total` keys, entries `"Event(aas)"`); `top_queryids_*` (int64-safe string entries).
  2. `total` = cpu+io+ipc+lock+lwlock; `Activity`/`Client`/`Timeout` excluded (not real load).
  3. All series at 1-minute resolution, zero-filled.
  4. The payload carries raw AAS only — scoring/normalization (e.g. against vCPUs) is the consumer's job; a caller-supplied `vcpus` is echoed, never used.
  5. Degrades honestly: `top_queryids_*` omitted when the needed raw samples are gone; `null` when the window has no coverage at all; callable by `grant_reader`.
- **Primary API:** `ash.report(since, until, vcpus, n)` ([AAS_API.md §4](AAS_API.md)).
- **Coverage:** ✅ Covered by design.

---

## 5. Traceability matrix

| Story | Primary function(s) | Coverage | Gap to close |
|---|---|---|---|
| US-1 Triage | `periods` | ✅ | — |
| US-2 Locate | `timeline` | ✅ | — |
| US-3 Drill | `top(dimension, filters…)` | ✅ | — |
| US-4 Leaf | `aas(event…)` + `top('query_id', event…)` | ✅ | — |
| US-5 Programmatic | whole family (explicit provenance, retention in `status()`) | ✅ | — |
| US-6 Capacity | `timeline` (long span) | ✅ | — |
| US-7 Before/after | `compare` | ✅ | — |
| US-8 Load report | `report` | ✅ | — |

Coverage above is **by design** ([AAS_API.md](AAS_API.md)); implementation
tracks in the 2.0 branch.

## 6. Cross-cutting (non-functional) requirements

These apply to every story above.

- **Unit consistency.** AAS is the primary unit (`avg_aas` / `peak_aas` / `p99_aas`); `backend_seconds` may appear as a secondary column. No third unit.
- **Percentile definition.** `p99_aas` = the 99th percentile of per-sub-bucket AAS. The sub-bucket grain is a parameter (default `'1 minute'`). `peak_aas` is the max over the same sub-buckets.
- **Raw-vs-rollup honesty (trust property).** Aggregate readers select or
  declare their source by window; when a requested drill or window cannot be
  answered (rollup can't tie event→query; window exceeds retention), it signals
  this explicitly rather than returning a silent empty/partial result.
- **Time addressing convention (2.0).** Every reader takes `since timestamptz default null` (→ `now() - '1 hour'`) and `until timestamptz default null` (→ `now()`). The v1.x `f(p_interval)` / `f_at(p_start, p_end)` twin convention is dropped — 2.0 breaks compat, and the single convention halves the surface. ([AAS_API.md §1](AAS_API.md))
- **Naming.** Function and column names use full domain terms — no abbreviations in user-facing names. Drill dimensions and filters spell out `wait_event_type` / `wait_event` / `query_id` / `database`, as the `dimension` values and parameter names of `top`. The on-CPU class is spelled `CPU*` — the asterisk marks "on CPU *or* uninstrumented wait" and must not be dropped.
- **Dual audience.** Data functions are typed and presentation-free; ASCII bars/charts live only in dedicated rendering helpers.
- **Privileges & degradation.** Every reader is callable by the `grant_reader` role and degrades gracefully without pg_stat_statements (show `query_id`, NULL `query_text`) and without pg_cron.
- **Performance budgets.** Align with [SPEC.md §6](SPEC.md): rollup-backed reads target sub-100ms for a 1-day window; raw-backed leaf reads (US-4) stay within the budget for a 1-hour window on a 1-day partition.

## 7. Out of scope (for now)

- Per-PID / per-session journey tracking (SPEC §7 limitation — by design).
- Query text storage (join `query_id` to pg_stat_statements instead).
- Sub-second sampling resolution.
- Replica-side wait analysis.
