# pg_ash demo recording

This directory produces the animated GIF embedded in the top-level README — a
~60-second walkthrough of the **LLM-assisted investigation** flow from the
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
2. `ash.activity_summary('1 minute')` — big picture: backends spiked, lock wait dominates
3. `ash.top_waits('1 minute')` — colored bars; `Lock:transactionid` leads
4. `ash.timeline_chart('1 minute', '5 seconds')` — stacked-bar view of when the spike landed
5. `ash.event_queries('Lock:transactionid', '1 minute')` — the guilty UPDATE
6. Closing frame (held ~3s) so the GIF loops gracefully in the README

## The spike

Five concurrent `UPDATE pgbench_accounts WHERE aid = 42` workers contend
against one "holder" transaction that grabs the same row and `pg_sleep()`s for
three seconds at a time. Every contender queues on `Lock:transactionid` behind
the holder — guaranteed, reproducible, no host-level privileges required.

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
| `COLS` / `ROWS` | 92 / 28 | Terminal geometry (shrink for tighter embeds) |
| `WARMUP_SEC` | 35 | Seconds of workload before recording starts |
| `BASELINE_SEC` | 25 | Phase-1 pgbench duration inside the container |
| `SPIKE_SEC` | 35 | Phase-2 lock-contention duration |
| `LOCK_WORKERS` | 5 | Contender count — more = more `Lock:transactionid` |
| `KEEP_CONTAINER` | 0 | Set `1` to leave the container running after recording (for re-takes) |

Example — slower pacing and a larger spike:

```bash
WARMUP_SEC=50 LOCK_WORKERS=8 make record
```

### Re-running without recapturing the container

The `.cast` file is the source of truth — once you have one you like, re-render
the GIF without touching Docker:

```bash
agg --font-size 16 --theme monokai --speed 1.0 --fps-cap 15 \
  demos/ash_demo.cast demos/ash_demo.gif
```

## Design notes

- **Geometry (92 × 28):** narrower than typical terminals so the embedded GIF
  stays legible at GitHub's rendered ~800 px width.
- **Theme:** `monokai` — dark background lets the pg_ash `_wait_color()` ANSI
  palette (cyan / red / yellow / pink / purple) pop.
- **Pacing:** ~1.2 s between `echo` banners and commands, ~3–4 s between
  commands so the viewer can read table output without pausing.
- **First frame:** `ash.status()` output is already visible by ~3 s, so the
  GitHub still preview shows a populated pg_ash pane rather than an empty
  prompt.
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
