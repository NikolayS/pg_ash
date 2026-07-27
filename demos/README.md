# pg_ash demo harness

Every image in `README.md` and `docs/`, plus the animated reel, regenerated from
**real** pg_ash query output against a **real** PostgreSQL, with one command:

```sh
make -C demos all
```

No Docker required. No browser required. No hand-cropping, and nothing that only
works on one laptop.

---

## What it produces

| Artifact | Made by | Notes |
|---|---|---|
| `assets/<scene>.svg` | `make stills` | the primary still. Vector, byte-deterministic, crisp at any zoom |
| `assets/<scene>.png` | `make stills` | `ASH_SCALE`× raster of the same SVG (default 2×) |
| `assets/ash_demo.gif` | `make demo` | the reel |
| `assets/ash_demo.mp4` | `make demo` | same render, ~40% smaller; what docs sites want |

The scene list is data, not code: `scenes/scenes.tsv`. Adding a documentation
image means adding one line to that file.

---

## Honesty boundary

Read this before you look at a single number.

> The harness shapes **which** real samples exist and **when** they are
> considered to have been taken. Every number in every asset is pg_ash
> aggregating its own stored samples, written by `ash.take_sample()` from real
> `pg_stat_activity` over real pgbench backends. No reader output is edited.

Concretely, the seeder runs a real workload, samples it with pg_ash's real
sampler, and then rewrites `ash.sample.sample_ts` so that one real second of
load is filed as one virtual minute of history. It never invents a row, never
edits a backend count, and never touches the packed wait/query array. That is
the whole of the liberty taken, and it lives in one `UPDATE` in
`lib/seed.sql` (`ash_demo.restamp`).

**Time compression: 1 real second = 1 virtual minute.** 28 minutes of history
arrives in about 20 seconds. The exact ratio in force for a given run is
recorded in `out/window.env` as `ASH_COMPRESSION`.

`ASH_REAL_TIME=1` turns compression off entirely: the seeder then samples at the
declared interval in real wall-clock time and skips the restamp. It takes about
28 minutes and the assets are indistinguishable, which is the point — the switch
exists so that claim can be checked rather than believed.

The seeder also keeps only samples for its own database. pg_ash samples the
whole cluster by design; on a developer machine that would quietly fold every
other database on the box into the demo and the numbers would stop being
reproducible.

---

## pg_cron is not required — and this demo deliberately does not use it

The harness drives `ash.take_sample()` itself, from an ordinary session. That is
the **external scheduler** path, and it is the default here for two reasons:

1. It is what pg_ash users on RDS, Cloud SQL, Supabase, AlloyDB and Neon
   actually run, because those platforms do not give you pg_cron.
2. It is the only path that works on an arbitrary CI runner.

So the degraded no-cron mode is not a compromise in this harness — it is the
mainline. `ash.status()` in the `status` scene says so on screen: the demo shows
`pg_cron_available` as `no (use external scheduler)`, because that is the truth
of how it was collected.

If you want a cluster with real pg_cron, use `ASH_BACKEND=docker` with an image
that has the extension. It is never on the critical path.

---

## Running it

```sh
make -C demos doctor     # what is installed, what is missing, which backends work
make -C demos seed       # ~22s: a frozen incident window + its shape assertions
make -C demos stills     # assets/*.svg (+ *.png)
make -C demos demo       # assets/ash_demo.gif + .mp4
make -C demos all        # both, from ONE seed and ONE frozen window
make -C demos check      # no database at all: renderer regression gate
make -C demos down       # remove only a database/container this run created
```

`make help` prints the same list plus every knob.

Measured wall-clock on an M-series MacBook against a local PostgreSQL 18.3:
`seed` 22 s, `stills` 23 s, `demo` 155 s (75 s of that is the recording itself,
which runs in real time by construction), `all` about 3 minutes, `check` 1 s.

### Prerequisites, honestly

**Tier 1 — stills. This is the whole list.**

| Need | Why | Install |
|---|---|---|
| `python3` ≥ 3.8 | every renderer, and every width measurement | already on macOS and every CI image |
| `fontTools` | subsets the font into the SVG | `python3 -m pip install fonttools` |
| `brotli` | woff2 compression for that subset | `python3 -m pip install brotli` |
| `psql` | runs the scenes | `brew install libpq` / `apt install postgresql-client` |
| `pgbench` | drives the workload the sampler samples | ships with the client packages |
| a reachable PostgreSQL ≥ 14 | somewhere to install pg_ash | local cluster, Docker, or remote |

No Docker. No browser. No `ALTER SYSTEM`. No restart. **No pg_cron** — see the
section above.

**Tier 2 — PNG.** Any SVG rasteriser: `resvg` (`cargo install resvg`, or a
release binary), `rsvg-convert`, or any Chromium-family browser, including
`/Applications/Google Chrome.app`, which `make doctor` finds by absolute path.
Set `ASH_CHROME=/path/to/binary` to pin one. `ASH_SVG_ONLY=1` skips this tier
entirely — the SVG is the primary artifact and loses nothing.

**Tier 3 — the reel.** `tmux`, `asciinema`, `agg`, `ffmpeg`, `gifsicle` and
Pillow.

```sh
# macOS
brew install tmux ffmpeg gifsicle asciinema
brew install agg            # or fetch the static binary from its GitHub release
python3 -m pip install pillow

# Debian / Ubuntu / GitHub Actions
sudo apt-get install -y tmux ffmpeg gifsicle
python3 -m pip install asciinema pillow
# agg ships as a single static binary; take it from
# https://github.com/asciinema/agg/releases
```

### The font

JetBrains Mono is **vendored** in `fonts/` (OFL-1.1, redistributable, licence
included as `fonts/OFL.txt`). There is nothing to install and nothing to
configure.

That is a deliberate design decision, not convenience. Both renderers consume
the font *by path* — `agg --font-dir demos/fonts`, `ansi2svg.py --font
demos/fonts/JetBrainsMono-Regular.ttf` — so fontconfig is never consulted and a
system-installed "JetBrains Mono" of a different version cannot win. A missing
or unreadable face is a hard exit 6, never a silent substitution: the previous
generation of this harness was built with VHS, which quietly fell back to a
serif face on a machine without the font and produced a demo that looked wrong
in a way nobody could reproduce.

`bin/record-demo.sh` goes one step further and *proves* the vendored file was
the one used: it renames a copy of `fonts/JetBrainsMono-Regular.ttf` to a family
name that exists nowhere on the machine, asks `agg` for that family, and
requires a byte-identical raster.

### The fast iteration loop

```sh
ASH_SKIP_SEED=1 make -C demos stills   # ~23s, no reseed
make -C demos render                   # ~80s, re-render the last recording
```

`ASH_SKIP_SEED=1` reuses the warm database and the existing `out/window.env`.
Whether that is still valid is decided by the *data*, not by a clock:
`window_env_check_fresh` compares the `max(sample_ts)` the seeder recorded
against what is in the table right now, so a reseed, a stray sampler or a
dropped database all fail it with exit 3.

`make render` skips the 75-second tmux recording and redoes everything from
`agg` onwards against the cast already in `out/`. That is the loop for anything
to do with the theme, the window chrome, the palette or the size budget.

### Running the harness from outside the repo

`lib/env.sh` sources `demos/env.local` if it exists — a gitignored file for
machine-specific paths, chiefly `ASH_REPO_ROOT` (where `sql/ash-install.sql` is
read from) and `ASH_ASSETS` (where the deliverables are written to). In a normal
checkout the file is absent and both resolve from `demos/`'s own location.

---

## Backends

| `ASH_BACKEND` | What it is |
|---|---|
| `local` (default) | whatever cluster the ambient `PG*` settings already reach. Needs only `psql` + `pgbench`. No Docker, no image pull, no `ALTER SYSTEM`, no restart. This is also the CI path. |
| `docker` | optional; for pinning a major version or getting a real pg_cron. The port is probed free from 5500-5599, never hardcoded. |
| `remote` | standard `PG*` variables, with two guardrails that cannot be switched off: the target database name must match `ash_demo*`, and the harness refuses to seed on top of an `ash.sample` table it did not fill. `make down` never drops a remote database. |

House rules are enforced in code, not in this document: `backend_down` asserts
the safe `ash_demo*` / `ash_demo_*` name forms and consults an ownership ledger
before it drops or removes anything. Reused local databases, every remote
database, and containers without a `created` ledger are left in place. A failed
owned-resource teardown stays loud and retains the ledger for a retry.

---

## The story the seed tells

28 virtual minutes, of which 24 are the query window and 4 are slack in front of
it (so the raw-retention guardrail in the drill readers cannot trip as the seed
ages between `make seed` and `make demo`):

| Virtual minutes | Phase | Load |
|---|---|---|
| 1–4 | calm (slack) | read-only range aggregates |
| 5–12 | calm baseline | read-only range aggregates |
| 13–17 | **the incident** | 12 clients contending on one row + 3 write clients |
| 18–20 | recovery | mixed read/write |
| 21–28 | busy tail | heavier reads |

The incident is a genuine row-lock storm: every client updates the same row and
then does real work while still holding it. That is the actual Postgres locking
protocol, so the sampler sees `Lock:transactionid` and `Lock:tuple` alongside the
holder's own `CPU*` and IO — with a single identifiable statement behind all of
it. There is no `pg_sleep` anywhere in the workload; an earlier prototype used
one and shipped `Timeout:PgSleep` as the demo's number-one wait event, which
would have taught the wrong lesson to everyone who watched it.

The calm phases are read-only on purpose. Default TPC-B at a low scale produces
several AAS of lock contention all by itself, because every client fights over
the handful of `pgbench_branches` rows — and then "calm" looks exactly like the
incident.

### The shape is asserted, not hoped for

`lib/shape.sql` runs after every seed and fails the build (exit 4) unless:

- every one of the 28 virtual minutes carries samples;
- at least 4 distinct wait event types are present;
- the storm's `peak_aas` is at least **3×** the median calm minute;
- the storm's rank-1 wait event is a `Lock:*` event holding ≥ 35% of the window;
- one query id owns ≥ 50% of that wait, so the drill has somewhere to land;
- the calm baseline is **not** led by a lock wait;
- `ash.periods()` returns all 6 rows with no NULL `avg_aas` — which can only
  happen if the rollup chain and its watermarks are intact;
- at least **two `Lock:*` events rank in the top four** of the whole chart
  window. That one is purely about the picture: `ash.chart()` ranks its series
  over the entire window and folds the rest into a single "Other" dot column, so
  a calm phase heavy enough to outrank both lock waits would render the
  five-minute incident as anonymous dots. Nothing else here would notice — it is
  not an error, it is not empty, it is just a bad hero image. Now it fails the
  seed and names the two knobs to turn.

That last one is worth its own note. Deleting `ash.rollup_1m` without also
setting `last_rollup_1m_ts = null` leaves `ash.rollup_minute()` convinced it has
already processed those minutes. It then refuses to re-roll, the wide readers
silently prefer the empty rollup source, and the demo ships buckets full of
nothing with no error anywhere. `ash_demo.reset_state()` nulls the watermarks.

---

## The frozen window

`lib/seed.sh` writes `out/window.env` as its last action:

```sh
ASH_SINCE='2026-07-26 22:36:00-07'
ASH_UNTIL='2026-07-26 23:00:00-07'
ASH_STORM_SINCE='2026-07-26 22:44:00-07'
ASH_STORM_UNTIL='2026-07-26 22:49:00-07'
ASH_STORM_EVENT='Lock:transactionid'
ASH_COMPRESSION='1 real second = 1 virtual minute'
...
```

Both capture paths source it, and both refuse to run (exit 3) if it is missing
or stale. **No scene SQL may call `now()`.** That is what lets the stills pass
and the animation pass run minutes apart and still agree on every digit — without
coupling them into a single recording.

`ASH_STORM_EVENT` is *measured* from the seeded data rather than assumed, so
scene SQL and marker assertions bind to what the storm actually produced.

---

## Verification

There is no way to ship a broken picture quietly.

- **Preflight (§ hard gate).** Before any renderer or recorder starts, every
  scene is executed scripted and checked: non-empty, no `ERROR:`/`FATAL:`/
  `PANIC:`, every marker present, and **every line within the 100-column
  budget**. Markers are what separate "the query ran" from "the query showed the
  incident" — they are what stops an empty result set shipping as a pretty
  picture of nothing.
- **Width is measured in Python**, with East-Asian-width awareness and escape
  sequences counted as zero. Never `awk length`: that counts UTF-8 bytes and
  reported 393 for a 131-column table.
- **Post-render pixel sampling** asserts the exact wait-class RGB values from
  `docs/COLOR_SCHEME.md` survive into the artifact unquantised.
- **`make check`** re-renders the committed fixtures with no database at all and
  byte-compares against `fixtures/expected/`. It proves the *renderer* still
  works on frozen input. It **cannot** notice that the input no longer reflects
  what pg_ash does — only the nightly re-capture catches that.

---

## Embedding the results

GitHub's markdown sanitiser strips inline `<svg>`. Use the image form:

```markdown
![AAS by wait event](assets/chart.svg)
```

or `<img src="assets/chart.svg">`. Both render in GitHub's secure-static mode,
and the subsetted font travels inside the file as a data URI, so the picture
looks the same for everyone.

Do **not** pin a `width=`. The SVG is exactly 100 columns wide and GitHub scales
it down to the article column; pinning a pixel width only makes it smaller.

Caption the chart. `ash.chart()` varies the **glyph** per series (`█ ▓ ░ ▒ ·`) as
well as the colour, so the ranking is readable without relying on colour vision.

`report.svg` is correct and useful but roughly 3000 px tall — it belongs on a
documentation page, not in the README landing area.

`make stills` writes `out/embed.md` with a ready-to-paste block for every scene.

---

## Layout

```
demos/
  Makefile               every target; the only entry point
  bin/ash-demo           subcommand orchestrator
  bin/capture-stills.sh  scripted capture -> ansi -> svg/png   [--capture-only]
  bin/record-demo.sh     tmux record -> agg -> chrome -> gif/mp4  [--render-only]
  bin/check-fixtures.sh  the no-database renderer gate (make check)
  bin/make-fixtures.sh   refresh fixtures/ from the current capture
  lib/env.sh             resolves every ASH_* knob; sources demos/env.local
  lib/doctor.sh          dependency probe by tier
  lib/backend.sh         local | docker | remote, and the house-rule guardrails
  lib/seed.sh            the workload phases, the restamp, the frozen window
  lib/seed.sql           ash_demo.batch / restamp / phase / reset_state
  lib/workload_lock.sql  the row-lock storm (no pg_sleep)
  lib/workload_read.sql  the read load (real range aggregates, not point lookups)
  lib/shape.sql          numeric shape assertions
  lib/scenes.sh          parse/validate/expand scenes.tsv
  lib/verify.sh          the shared assertion vocabulary (vfy_*)
  lib/driver.sh          tmux terminal driver (drv_*)
  lib/psqlrc.still       psql settings for the scripted path
  lib/psqlrc.demo        psql settings + the OSC prompt sentinel for the reel
  render/dwidth.py       THE display-width oracle; every gate calls it
  render/ansitable.py    ANSI-aware table formatter (alignment psql cannot do)
  render/ansi2svg.py     ANSI -> SVG, with block-glyph promotion
  render/chrome.py       the window-chrome plate for the reel composite
  render/verify_pixels.py post-render colour + non-triviality gate
  scenes/scenes.tsv      THE scene list
  scenes/captions.tsv    optional prose for out/embed.md
  theme/pg_ash.json      THE style file — the only source of colour and geometry
  fonts/                 vendored JetBrains Mono (OFL-1.1)
  fixtures/              frozen capture bytes + expected renderer output
  out/                   gitignored working directory
```

### Two things that look like duplication and are not

`render/ansi2svg.py` and `render/chrome.py` both draw the window chrome, from
the same numbers in `theme/pg_ash.json`, because the stills path emits SVG
primitives and the reel path needs a raster plate for `ffmpeg` to composite
onto. They must stay in step; a divergence shows up as a still and a frame of
the reel being visibly different windows. (One did: the plate used to paint the
title bar twelve pixels past its hairline.)

`bin/capture-stills.sh` and `bin/record-demo.sh` both run every scene scripted
before doing anything expensive. That is not a redundant check — the still path
verifies what it is about to render, and the reel path verifies in two seconds
what would otherwise cost a 75-second recording to discover.

Exit codes are uniform across every script: `1` usage/config, `2` missing
dependency, `3` backend or frozen-window failure, `4` seed assertion, `5` capture
verification, `6` render, `7` animation sync.
