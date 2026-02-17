# Color scheme

pg_ash uses 24-bit RGB ANSI escape codes for colored terminal output.
Colors are aligned with [PostgresAI monitoring](https://gitlab.com/postgres-ai/postgresai)
Grafana dashboards (Dashboard 4 — Wait Sampling).

## Color mapping

| Wait type | Color | RGB | ANSI code |
|---|---|---|---|
| CPU* | green | 80, 250, 123 | `\033[38;2;80;250;123m` |
| IdleTx | light yellow | 241, 250, 140 | `\033[38;2;241;250;140m` |
| IO | vivid blue | 30, 100, 255 | `\033[38;2;30;100;255m` |
| Lock | red | 255, 85, 85 | `\033[38;2;255;85;85m` |
| LWLock | pink | 255, 121, 198 | `\033[38;2;255;121;198m` |
| IPC | cyan | 0, 200, 255 | `\033[38;2;0;200;255m` |
| Client | yellow | 255, 220, 100 | `\033[38;2;255;220;100m` |
| Timeout | orange | 255, 165, 0 | `\033[38;2;255;165;0m` |
| BufferPin | teal | 0, 210, 180 | `\033[38;2;0;210;180m` |
| Activity | purple | 150, 100, 255 | `\033[38;2;150;100;255m` |
| Extension | light purple | 190, 150, 255 | `\033[38;2;190;150;255m` |
| Unknown/Other | gray | 180, 180, 180 | `\033[38;2;180;180;180m` |

Reset code: `\033[0m`

## Why 24-bit RGB

Standard 4-bit ANSI colors (e.g. `\033[31m` for red) render differently depending on
terminal theme. Dark themes, light themes, and solarized themes all remap the base 16
colors — "red" can look pink, orange, or even brown.

24-bit RGB (`\033[38;2;R;G;Bm`) bypasses the theme's palette entirely and specifies
the exact color. The result looks the same in every terminal that supports truecolor
(virtually all modern terminals: iTerm2, WezTerm, Kitty, Alacritty, Windows Terminal,
GNOME Terminal, VS Code integrated terminal, etc.).

## Why these specific colors

- **Aligned with PostgresAI monitoring** — same semantic mapping as the Grafana
  dashboards, so muscle memory transfers between terminal and GUI
- **Visually distinct** — no two adjacent wait types share similar hues
- **CPU\* is green** — the happy color. Active CPU work is what you want to see.
- **Lock is red** — danger. Lock contention is bad.
- **IO is blue** — calm but notable. IO waits are normal under load.
- **IdleTx is light yellow** — warning. Idle-in-transaction holds locks and blocks vacuum.

## Block characters

In `timeline_chart()`, each rank gets a distinct block character so the breakdown
is visible even without color:

| Rank | Character | Name |
|---|---|---|
| 1 | █ | Full block |
| 2 | ▓ | Dark shade |
| 3 | ░ | Light shade |
| 4+ | ▒ | Medium shade |
| Other | · | Middle dot |

## psql and ANSI codes

psql's aligned table formatter (`\pset format aligned`, the default) counts ANSI
escape codes as characters when computing column widths. This causes misalignment
when colors are on. pg_ash pads chart strings to a consistent `length()` to minimize
this effect.

For best results with colored output, pipe through sed:

```
\set color '\\g | sed ''s/\\\\x1B/\\x1b/g'' | less -R'
```

Then use `:color` after queries:

```sql
select * from ash.top_waits('1 hour', p_color => true) :color
```

Colors also render natively in pgcli, DataGrip, and other clients that pass raw bytes.

## Implementation

Colors are implemented in `ash._wait_color(p_event text, p_color boolean)`.
The function is `IMMUTABLE` and `language sql` — Postgres inlines it into the
calling query. When `p_color = false` (the default), it returns an empty string
with zero overhead.

Functions that support color:

- `top_waits()` / `top_waits_at()` — bar column
- `query_waits()` / `query_waits_at()` — bar column
- `waits_by_type()` / `waits_by_type_at()` — bar column
- `timeline_chart()` / `timeline_chart_at()` — chart column
