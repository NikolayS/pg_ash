# pg_ash demo recording

This directory produces the animated GIF embedded in the top-level README — a
short walkthrough of the **LLM-assisted investigation** flow from the
main README, driven against a live Postgres 18 container.

| File | What it is |
|------|-----------|
| `ash_demo.gif` | The rendered GIF (committed; used by the top-level README) |
| `ash_demo.cast` | asciinema v3 cast file — source of truth for the GIF |
| `record.sh` | End-to-end recorder: Docker → pg_ash install → workload → tmux/asciinema → agg |
| `container-entrypoint.sh` | Runs inside the `postgres:18` container — creates DB, installs pg_ash, starts sampling, launches workload |
| `workload.sh` | Three-phase mixed workload: baseline pgbench → row-lock spike → tail |
| `Makefile` | Thin wrapper: `make record`, `make clean`, `make open` |

## What it shows

The demo reproduces the investigation sequence from the README's **LLM-assisted
investigation** section, against a real spike (not canned output):

1. `ash.status()` — sampling active, version 1.4, pg_cron wired up
2. `ash.top_waits('1 minute')` — colored bars; `Lock:tuple` leads
3. `ash.timeline_chart('1 minute', '10 seconds')` — stacked-bar view of when the spike landed
4. `ash.event_queries('Lock:tuple', '1 minute')` — the guilty UPDATE
5. `ash.query_waits(<top_query_id>, '1 minute')` — full wait profile of the top guilty query
6. Closing frame (held ~3s) so the GIF loops gracefully in the README

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
| asciinema | **3.x** (v3 cast format) | `brew install asciinema` |
| agg | 1.5+ (truecolor GIF renderer for asciinema casts) | `brew install agg` |
| gifsicle | 1.90+ (optional, halves the output GIF size) | `brew install gifsicle` |
| python3 | 3.8+ (post-processes the `.cast` to drop the blank initial frame) | ships with macOS 12+ / Linux |
| GNU make | 3.81+ | ships with macOS / Linux |

Pinned versions used to produce the committed GIF:

- Docker 29.0.1
- tmux 3.6a
- asciinema 3.1.0
- agg 1.7.0
- gifsicle 1.96
- python3 3.9+

On Linux, use your distro's packages (`apt install tmux gifsicle`, `cargo
install asciinema`, release tarball for
[agg](https://github.com/asciinema/agg)).

## Reproduce the GIF

```bash
cd demos
make record     # ~2 minutes end-to-end (pulls postgres:18, installs pg_cron)
make open       # open the produced gif
```

That's it. The container is torn down on exit; the only artifacts kept are
`ash_demo.cast` and `ash_demo.gif`.

### Tuning knobs

Override via environment variables:

| Var | Default | What it controls |
|-----|---------|-----------------|
| `COLS` / `ROWS` | 140 / 32 | Terminal geometry — wider so `Lock:transactionid` / `Client:ClientRead` rows don't wrap |
| `AGG_FONT_SIZE` | 12 | Pixel font-size passed to `agg`; lower keeps the wider terminal under ~1100 px |
| `TYPE_MIN_MS` / `TYPE_MAX_MS` | 30 / 120 | Per-character keystroke jitter range (ms) — see "Typing pacing" below |
| `TYPE_PUNCT_MS` | 180 | Extra pause after `, ; . ( )` characters |
| `WARMUP_SEC` | 30 | Seconds of workload before recording starts |
| `BASELINE_SEC` | 15 | Phase-1 pgbench duration inside the container |
| `SPIKE_SEC` | 120 | Phase-2 lock-contention duration — kept long enough that the spike outlives the ~90 s recording so the final `\gset` step still sees fresh `Lock:tuple` samples |
| `TAIL_SEC` | 30 | Phase-3 quiet pgbench coda |
| `LOCK_WORKERS` | 5 | Contender count — more = more lock waits |
| `KEEP_CONTAINER` | 0 | Set `1` to leave the container running after recording (for re-takes) |

Example — slower pacing and a larger spike:

```bash
WARMUP_SEC=40 SPIKE_SEC=150 LOCK_WORKERS=8 make record
```

### Re-running without recapturing the container

The `.cast` file is the source of truth — once you have one you like, re-render
the GIF without touching Docker:

```bash
agg --font-size 12 --theme monokai --speed 1.0 --fps-cap 15 \
  demos/ash_demo.cast demos/ash_demo.gif
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

- **Geometry (140 × 32):** wider than typical README embeds so long wait
  event names like `Lock:transactionid` / `Client:ClientRead` and the colored
  bar charts fit on a single line. Compensated by `agg --font-size 12` so the
  rendered GIF stays under ~1100 px and remains readable at GitHub's ~800 px
  embed width.
- **Theme:** `monokai` — dark background lets the pg_ash `_wait_color()` ANSI
  palette (cyan / red / yellow / pink / purple) pop.
- **Colors on by default:** `set ash.color = on` is set in the demo's
  `~/.psqlrc`, *and* every reader call passes `p_color => true` (or the
  positional `true` argument) so the bars come back with ANSI codes. The
  `:color` psql variable (mirroring the README pattern) re-runs the previous
  query through `sed` to convert psql's literalised `\x1B` back into real ESC
  bytes — without this step psql's aligned formatter would mangle the codes.
  We omit `less -R` from the README pattern here because the recorder cannot
  drive an interactive pager.
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

**`pg_cron install failed`** — the container's `apt` couldn't fetch the PGDG
package for this major. Either use a different `IMAGE` or temporarily set
`ASH_CRON_OPTIONAL=1` (pg_ash supports a no-cron mode; the demo will skip
`ash.start()` and rely on manual `ash.take_sample()` calls).

**`agg: unknown option --last-frame-duration`** — upgrade to `agg` 1.7+
(`brew upgrade agg`).

**Colors look washed out** — ensure your terminal / renderer is truecolor.
`agg` always emits truecolor in the GIF; if re-rendering locally, pass
`--theme monokai` (default in our script) for the best contrast with the
pg_ash palette.

**GIF too large for README** — drop the font size (`--font-size 14`) or the
FPS cap (`--fps-cap 10`) when invoking `agg`. The target for this repo is
≤ 3 MB.

---

Copyright 2026 Postgres.ai
