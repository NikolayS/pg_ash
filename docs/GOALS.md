# pg_ash goals

What pg_ash is for, who it is for, and which constraints are
non-negotiable. This is the "why" behind the design decisions;
`docs/RELEASE_PROCESS.md` covers the "how" of shipping them.

## The problem

When a self-managed Postgres instance has an incident, the operator usually has
nothing to look at. Managed platforms answer this with a proprietary active
session dashboard, but that answer does not travel. Self-hosted, on-prem, and
vendor-managed installs are left guessing, and the usual guess is to
over-provision hardware rather than to diagnose the actual wait.

pg_ash exists to make active session history available everywhere Postgres
runs, with no privileged install step.

## Who it is for

1. **Operators of a single Postgres instance** who want to answer "what was the
   database doing at 03:14?" without buying a platform.
2. **Application vendors embedding observability in their product**, shipping a
   lightweight active-session view to customers who run the database
   themselves. This case sets most of the hard constraints below, because a
   vendor cannot ask every customer's DBA for privileges.

## Goals

### G1. Embeddable by an application vendor

pg_ash must install the way an application's own migrations install: plain SQL
into a dedicated `ash` schema, no `CREATE EXTENSION`, no `.control` file, no
`shared_preload_libraries`, no restart. This is what lets it run on RDS, Cloud
SQL, Supabase, AlloyDB, and Neon, and what lets a vendor ship it as an ordinary
migration rather than as a support ticket to the customer's DBA.

Everything else on this page is downstream of this constraint.

### G2. Safe to enable by default, for everyone

The question that matters is not "does it work" but "can it be on by default,
or must it be opt-in?" Opt-in observability is absent exactly when it is
needed, because nobody enables it before their first incident.

The working budget: pg_ash adds **one transaction** to whatever throughput the
database already serves. Sample volume tracks the number of *active sessions*,
not database size, so a busy 100-session instance produces kilobytes per
second, compressed — megabytes per day, not gigabytes.

That budget is currently an estimate. It should not appear in user-facing
documentation until it is measured, including the case that actually warrants
caution: an instance that is **already saturated** — few cores, many active
sessions — where the marginal cost of sampling is least predictable and most
consequential. Tracked in #226.

### G3. AAS as a machine-readable health metric

A dashboard is for humans. The harder and more valuable problem is deciding
*programmatically* whether a database is healthy, so that rules, monitoring
systems, and language models can act without a person squinting at a chart.

Average Active Sessions is that metric: the Linux load-average idea applied to
Postgres — how many sessions were concurrently active or waiting, on average,
over a window — made judgeable by normalizing against vCPU count. The concept
is not ours; it was popularized by managed-platform performance tooling.
pg_ash's contribution is bringing it outside any single vendor's platform.

The analysis ladder:

1. AAS average **and peak** for the window;
2. broken down by wait event **type** (cpu, io, ipc, lock, lwlock, …);
3. broken down by specific wait **event** — with **query ID** as a separate
   dimension of the same level, not a fourth rung.

Averages alone lie, and the design must keep peaks first-class. For example, a
16-vCPU cluster averaging 4.9 AAS over a week reads as comfortably healthy at
30.6% of capacity, while a worst one-minute peak of 295 (1844% of capacity) is
the incident anyone would actually care about.

`ash.report()` exists to serve this goal: one JSONB payload, stable key
contract, honest `coverage` so a consumer can tell "quiet" from "no data".

### G4. A stable interface

2.0 reduced and cleaned up the function set, and it is a breaking interface
change. It is intended to be the **last** one on this line. Integrators write
code against these names; interface churn costs them more than the cleanup
gains them.

The data schema is deliberately preserved across the 1.5 -> 2.0 upgrade, so the
worst case during an upgrade is a brief gap in query access, never data loss.

Corollary: renames known to be needed must land **before** the 2.0 tag or not
at all on this line (#223).

### G5. A minimal, honest dependency surface

Both optional dependencies are *soft*, and the degraded behavior is part of the
contract rather than an accident:

- **pg_stat_statements** — without it, pg_ash still captures query **IDs**
  (they come from `pg_stat_activity`); only query **text** is unavailable. This
  distinction is load-bearing for vendors: on vendor-managed Postgres the
  application's own database user cannot install extensions, so a hard
  dependency would make the product uninstallable for a large share of users.
- **pg_cron** — third-party and not available everywhere. An external scheduler
  must remain a first-class path, not a documented consolation prize, because
  some integrators will deliberately keep scheduling under their own control.

Sampling cadence guidance follows the same honesty rule: 1 second is the
practical target, 15 seconds is workable, 1 minute is too coarse and will miss
spikes outright. And even 1-second sampling cannot resolve the sub-second
ordering questions that decide some incidents — for instance, whether a lock
manager wave preceded a buffer mapping wave or followed it. Wait-event
*tracing*, not sampling, is the tool for that; pg_ash should say so rather than
imply a precision it does not have. Tracked in #230.

## Non-goals

- **Replacing a monitoring platform.** pg_ash is a focused active session
  history, not a metrics stack, not an alerting system, not a UI.
- **Being a Postgres extension.** The anti-extension design is the whole point;
  a C extension would be more capable and would not reach the users this
  project targets.
- **Multi-node aggregation.** pg_ash was built for single-node clusters, which
  is an increasingly common shape. Standbys are a known gap, addressed
  narrowly: pg_ash must not silently write on a replica, and must not let a
  reader believe replicated primary data describes the standby they are
  connected to (#222, #227).

## Known gaps

Open, tracked, and listed here so this page stays honest about where the
product does not yet meet its own goals:

| Gap | Goal at risk | Issue |
|---|---|---|
| Replica behavior is undefined: writes can be attempted on a standby, and readers can present replicated primary data as the standby's own | G1, G5 | #222, #227 |
| `ash.start()` is named for a daemon it does not start, and external tickers wrongly conclude they can skip it | G4 | #223 |
| The LLM-analysis path has no documented prompt, so an `ash.report()` payload is handed to a model that does not know what AAS is | G3 | #225 |
| Overhead is estimated, not measured, especially on an already-saturated instance | G2 | #226 |
| Incident data is unreachable exactly when the database is down | G3 | #229 |
| The degraded-mode contract and cadence guidance are not stated in one place | G5 | #230 |
| Sub-minute sampling without pg_cron needs a `CALL`-able procedure surface with real transaction control | G5 | #221, #228 |
| Samples are persistent and therefore replicated and backed up, whether or not the operator wants that | G2 | #224 |
