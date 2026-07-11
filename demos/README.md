# pg_ash demo recording

This directory contains the experimental animated GIF recorder for pg_ash demos.
The generated GIF is not embedded in the top-level README until its rendering is
readable on GitHub desktop and mobile.

| File | What it is |
|------|-----------|
| `ash_demo.gif` | The rendered GIF (committed for iteration; not embedded in the top-level README) |
| `ash_demo.cast` | asciinema v3 cast file — source of truth for the GIF |
| `record.sh` | End-to-end recorder: Docker → pg_ash install → workload → tmux/asciinema → agg |
| `smoke.sh` | Short Docker-only check: install → sampler job → live sample |
| `Dockerfile` | Pre-baked `postgres:${PG_MAJOR}` image with pg_cron + `shared_preload_libraries` compiled in — so the container boots preloaded, no runtime apt-get + restart |
| `container-entrypoint.sh` | Runs inside the container — creates DB, installs pg_ash, starts sampling, launches workload |
| `workload.sh` | Three-phase mixed workload: baseline pgbench → row-lock spike → tail |
| `Makefile` | Thin wrapper: `make smoke`, `make record`, `make clean`, `make open` |

## What it shows

The demo reproduces the investigation sequence from the README's **LLM-assisted
investigation** section on the 2.0 reader API, against a real spike (not canned
output). Every reader answers in AAS (average active sessions):

1. `ash.status()` — sampling active, current release version, pg_cron wired up
2. `ash.periods()` — triage: last-minute `peak_aas` >> `avg_aas` = a spike, not sustained
3. `ash.chart(since => now() - interval '5 minutes', bucket => '1 minute', color => true)` — colored stacked timeline: when it landed + which wait class (`Lock` in red)
4. `ash.top('wait_event', ...)` — drill: `Lock:tuple` dominates (AAS + peak + p99 per row)
5. `ash.top('query_id', wait_event => 'Lock:tuple', ...)` — the leaf: the guilty UPDATE
6. `ash.top('wait_event', query_id => <top_query_id>, ...)` — full wait profile of that query, closing the loop
7. Closing frame (held ~3s) so the GIF loops gracefully in the README

`ash.chart` is the only step that colors: in 2.0 the data readers (`periods`,
`top`, `timeline`) return typed columns only. `ash.chart` is the sole reader
that emits ANSI color; `ash.summary` is a render helper too but returns plain
key/value text.

## The spike

Five concurrent `UPDATE pgbench_accounts WHERE aid = 42` workers contend
against one "holder" transaction that grabs the same row and `pg_sleep()`s for
three seconds at a time. Every contender queues on `Lock:tuple` (with a smaller
`Lock:transactionid` tail) behind the holder — guaranteed, reproducible, no
host-level privileges required.

Runs inside a plain `postgres:18` container; no kernel tweaks, no cgroup
tricks, no custom Postgres build.

## Prerequisites

| Tool | Minimum | Install (macOS / Homebrew) |
|------|--------|---------------------------|
| Docker | any recent | [docker.com](https://docs.docker.com/get-docker/) |
| tmux | 3.x | `brew install tmux` |
| asciinema | 2.x or 3.x | `brew install asciinema` |
| agg | 1.5+ (truecolor GIF renderer for asciinema casts) | `brew install agg` |
| gifsicle | 1.90+ (optional, halves the output GIF size) | `brew install gifsicle` |
| python3 | 3.8+ (post-processes the `.cast` to drop the blank initial frame) | ships with macOS 12+ / Linux |
| GNU make | 3.81+ | ships with macOS / Linux |

Pinned versions used to produce the committed GIF:

- Docker 29.0.1
- tmux 3.6a
- asciinema 2.4.0
- agg 1.7.0
- gifsicle 1.96
- python3 3.9+

On Linux, use your distro's packages (`apt install tmux gifsicle`, `cargo
install asciinema`, release tarball for
[agg](https://github.com/asciinema/agg)).

## Reproduce the GIF

```bash
cd demos
make smoke      # short Docker-only setup/sampling check; no recording tools
make record     # ~8 minutes end-to-end (5.5 min warmup so the AAS windows have
                # enough history — see WARMUP_SEC below — plus a one-time build
                # of the pre-baked demos/Dockerfile image on first run)
make open       # open the produced gif
```

That's it. The container is torn down on exit; the only artifacts kept are
`ash_demo.cast` and `ash_demo.gif`.

### Tuning knobs

Override via environment variables:

| Var | Default | What it controls |
|-----|---------|-----------------|
| `COLS` / `ROWS` | 168 / 32 | Terminal geometry — wide enough for 2.0 `select *` output without wrapping |
| `AGG_FONT_SIZE` | 10 | Pixel font-size passed to `agg`; lower keeps the wider terminal near 1000 px |
| `TYPE_MIN_MS` / `TYPE_MAX_MS` | 30 / 120 | Per-character keystroke jitter range (ms) — see "Typing pacing" below |
| `TYPE_PUNCT_MS` | 180 | Extra pause after `, ; . ( )` characters |
| `WARMUP_SEC` | 330 | Seconds of workload before recording starts. Long (5.5 min) so the 2.0 readers' 5-minute windows sit inside raw retention — raw retention is data-limited (it starts at the oldest sample), and the leaf drills cross the wait↔query tie, which reads raw and raises if the window predates it |
| `BASELINE_SEC` | 120 | Phase-1 pgbench duration inside the container — 2 min so the baseline→spike transition falls inside the trailing 5-minute chart window |
| `SPIKE_SEC` | 480 | Phase-2 lock-contention duration — kept long enough that the spike outlives WARMUP + the ~110 s recording (~440 s) so the closing leaf drills still see fresh `Lock:tuple` samples in their 5-minute window |
| `TAIL_SEC` | 30 | Phase-3 quiet pgbench coda |
| `LOCK_WORKERS` | 5 | Contender count — more = more lock waits |
| `KEEP_CONTAINER` | 0 | Set `1` to leave the container running after recording (for re-takes) |
| `PG_MAJOR` | 18 | Postgres major version — sets both the base image (`postgres:$PG_MAJOR`) and the pre-baked image tag/build arg |
| `SETUP_TIMEOUT_SEC` | 120 | Maximum wait, in seconds, for install, pgbench initialization, sampler start, and workload launch |

Example — slower pacing and a larger spike:

```bash
WARMUP_SEC=360 SPIKE_SEC=540 LOCK_WORKERS=8 make record
```

### Re-running without recapturing the container

The `.cast` file is the source of truth — once you have one you like, re-render
the GIF without touching Docker:

```bash
agg --font-size 10 --theme monokai --speed 1.0 --fps-cap 15 \
  ash_demo.cast ash_demo.gif
```

### Typing pacing

The recorder simulates a human at the keyboard rather than pasting commands
instantly. The `human_type_and_send` helper in `record.sh` walks each command
string one character at a time, calling `tmux send-keys -l` per character and
sleeping a randomised interval between keystrokes.

| Region | Delay |
|--------|-------|
| Letters / digits | `TYPE_MIN_MS`–`TYPE_MAX_MS` ms (default 30–120 ms) |
| Spaces | 30–70 ms (slightly faster — words flow) |
| Punctuation `, ; . ( )` | base + `TYPE_PUNCT_MS` (default +180 ms) — clause-boundary pause |

Bash's `RANDOM` is reseeded from `/dev/urandom` at the start of each run so
the pacing is non-deterministic. The aggregate effect is roughly 60 cps —
brisk touch-typing, with visible "thinking" beats at punctuation.

Want it even slower (more cinematic) or faster (shorter GIF)?

```bash
TYPE_MIN_MS=60 TYPE_MAX_MS=200 TYPE_PUNCT_MS=300 make record   # slower, more deliberate
TYPE_MIN_MS=10 TYPE_MAX_MS=40  TYPE_PUNCT_MS=80  make record   # faster, breezier
```

## Design notes

- **Pre-baked image (`Dockerfile`):** pg_cron and the
  `shared_preload_libraries` / `cron.*` config are baked into a
  `postgres:${PG_MAJOR}` derivative (the config is appended to
  `postgresql.conf.sample` so a fresh `initdb` comes up preloaded). The
  container therefore boots with the extensions already active — no runtime
  `apt-get install …-cron` and no container restart at record time. This
  removes the record-time dependency on the PGDG apt mirror (which occasionally
  lags) and shaves the install + restart round-trip off every run.
  `record.sh` builds the image automatically when `Dockerfile` is present and
  falls back to the old runtime-install path if the build fails.
- **Geometry (168 × 32):** wider than typical README embeds so long wait
  event names like `Lock:transactionid` / `Client:ClientRead` and the colored
  bar charts fit on a single line. Compensated by `agg --font-size 10` so the
  rendered GIF stays near 1000 px and remains readable at GitHub's embed width.
- **Theme:** `monokai` — dark background lets the pg_ash `_wait_color()` ANSI
  palette (cyan / red / yellow / pink / purple) pop.
- **Colors on by default:** `set ash.color = on` is set in the demo's
  `~/.psqlrc`, *and* the one colored step — `ash.chart(...)` — passes
  `color => true` so its `chart` column comes back with ANSI codes. (In 2.0
  the data readers `periods` / `top` / `timeline` are presentation-free;
  `ash.chart` is the only reader that emits color, and `ash.summary` — a render
  helper too — returns plain key/value text.) The `:color` psql
  variable (mirroring the README pattern) re-runs the previous query through
  `sed` to convert psql's literalised `\x1B` back into real ESC bytes — without
  this step psql's aligned formatter would mangle the codes. We omit `less -R`
  from the README pattern here because the recorder cannot drive an interactive
  pager.
- **Human-paced typing:** commands are typed one character at a time via
  `tmux send-keys -l` with a 30–120 ms jitter and an extra ~180 ms beat at
  punctuation, so the recording feels like a real session. See
  [Typing pacing](#typing-pacing).
- **Pacing:** ~1.0–1.2 s between `\echo` banners and commands, ~4–5 s after
  each result so the viewer can read the colored table output without
  pausing.
- **First frame:** the splash banner is set to `t = 0`, so the GitHub still
  preview shows the colored banner rather than an empty prompt.
- **Loop:** closes on a held summary frame instead of a terminal exit line, so
  the auto-loop flows cleanly into the next opening banner.
- **No faking:** every table comes from `ash.*` reader functions against real
  samples collected from the live spike. You can set `KEEP_CONTAINER=1`, exec
  in, and re-run the same queries yourself.

## Troubleshooting

**pre-baked build failed** — `record.sh` builds `demos/Dockerfile` (which
`apt`-installs `postgresql-$PG_MAJOR-cron` from the PGDG mirror) once at the
start of a run. If that mirror is unreachable the build fails and the recorder
logs a warning and falls back to the plain `postgres:$PG_MAJOR` base with the
old runtime install + restart — so recording still proceeds. If the runtime
fallback cannot fetch the package either, the recorder exits; retry when the
PGDG mirror is available or choose a Postgres major with a published pg_cron
package. Remove `demos/Dockerfile` to force the fallback path directly.

**`agg: unknown option --last-frame-duration`** — upgrade to `agg` 1.7+
(`brew upgrade agg`).

**Colors look washed out** — ensure your terminal / renderer is truecolor.
`agg` always emits truecolor in the GIF; if re-rendering locally, pass
`--theme monokai` (default in our script) for the best contrast with the
pg_ash palette.

**GIF too large for README** — drop the font size (`--font-size 14`) or the
FPS cap (`--fps-cap 10`) when invoking `agg`. The target for this repo is
≤ 3 MiB.

---

Copyright 2026 PostgresAI
