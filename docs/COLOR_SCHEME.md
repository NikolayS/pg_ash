# Color scheme

pg_ash uses 24-bit RGB ANSI escape codes for colored terminal output.
Colors are aligned with [PostgresAI monitoring](https://gitlab.com/postgres-ai/postgresai)
Grafana dashboards (Dashboard 4 — Wait Sampling).

## Color mapping

| Wait event type | Color | Hex | RGB | ANSI code |
|---|---|---|---|---|
| CPU* | $\color{#50FA7B}{\rule{40pt}{12pt}}$ green | `#50FA7B` | 80, 250, 123 | `\033[38;2;80;250;123m` |
| IdleTx | $\color{#F1FA8C}{\rule{40pt}{12pt}}$ light yellow | `#F1FA8C` | 241, 250, 140 | `\033[38;2;241;250;140m` |
| IO | $\color{#1E64FF}{\rule{40pt}{12pt}}$ vivid blue | `#1E64FF` | 30, 100, 255 | `\033[38;2;30;100;255m` |
| Lock | $\color{#FF5555}{\rule{40pt}{12pt}}$ red | `#FF5555` | 255, 85, 85 | `\033[38;2;255;85;85m` |
| LWLock | $\color{#FF79C6}{\rule{40pt}{12pt}}$ pink | `#FF79C6` | 255, 121, 198 | `\033[38;2;255;121;198m` |
| IPC | $\color{#00C8FF}{\rule{40pt}{12pt}}$ cyan | `#00C8FF` | 0, 200, 255 | `\033[38;2;0;200;255m` |
| Client | $\color{#FFDC64}{\rule{40pt}{12pt}}$ yellow | `#FFDC64` | 255, 220, 100 | `\033[38;2;255;220;100m` |
| Timeout | $\color{#FFA500}{\rule{40pt}{12pt}}$ orange | `#FFA500` | 255, 165, 0 | `\033[38;2;255;165;0m` |
| BufferPin | $\color{#00D2B4}{\rule{40pt}{12pt}}$ teal | `#00D2B4` | 0, 210, 180 | `\033[38;2;0;210;180m` |
| Activity | $\color{#9664FF}{\rule{40pt}{12pt}}$ purple | `#9664FF` | 150, 100, 255 | `\033[38;2;150;100;255m` |
| Extension | $\color{#BE96FF}{\rule{40pt}{12pt}}$ light purple | `#BE96FF` | 190, 150, 255 | `\033[38;2;190;150;255m` |
| Unknown/Other | $\color{#B4B4B4}{\rule{40pt}{12pt}}$ gray | `#B4B4B4` | 180, 180, 180 | `\033[38;2;180;180;180m` |

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

## Comparison with other tools

Color assignments vary across tools. CPU = green is the only universal convention.
All hex codes below are from source code or pixel-sampled from published screenshots.

### PostgreSQL tools

| Type | pg_ash | PASH-Viewer¹ | PostgresAI² | Pigsty | pg_profile | AWS PI³ | AlloyDB⁴ |
|---|---|---|---|---|---|---|---|
| CPU | $\color{#50FA7B}{\rule{30pt}{10pt}}$ | $\color{#00CC00}{\rule{30pt}{10pt}}$ | $\color{#73BF69}{\rule{30pt}{10pt}}$ | $\color{#C8F2C2}{\rule{30pt}{10pt}}$ | — | $\color{#2CA02C}{\rule{30pt}{10pt}}$ | $\color{#579B11}{\rule{30pt}{10pt}}$ |
| IO | $\color{#1E64FF}{\rule{30pt}{10pt}}$ | $\color{#004AE7}{\rule{30pt}{10pt}}$ | $\color{#5794F2}{\rule{30pt}{10pt}}$ | $\color{#F7CB67}{\rule{30pt}{10pt}}$ | $\color{#FFAA4D}{\rule{30pt}{10pt}}$ | $\color{#FF7F0E}{\rule{30pt}{10pt}}$ | $\color{#F9023D}{\rule{30pt}{10pt}}$ |
| Lock | $\color{#FF5555}{\rule{30pt}{10pt}}$ | $\color{#C02800}{\rule{30pt}{10pt}}$ | $\color{#F2495C}{\rule{30pt}{10pt}}$ | $\color{#E02F44}{\rule{30pt}{10pt}}$ | $\color{#F2495C}{\rule{30pt}{10pt}}$ | $\color{#C49C94}{\rule{30pt}{10pt}}$ | $\color{#0451ED}{\rule{30pt}{10pt}}$ |
| LWLock | $\color{#FF79C6}{\rule{30pt}{10pt}}$ | $\color{#8B1A00}{\rule{30pt}{10pt}}$ | $\color{#C4162A}{\rule{30pt}{10pt}}$ | $\color{#CC4637}{\rule{30pt}{10pt}}$ | $\color{#C48C2D}{\rule{30pt}{10pt}}$ | $\color{#E377C2}{\rule{30pt}{10pt}}$ | $\color{#178D95}{\rule{30pt}{10pt}}$ |
| IPC | $\color{#00C8FF}{\rule{30pt}{10pt}}$ | $\color{#F06EAA}{\rule{30pt}{10pt}}$ | $\color{#FADE2A}{\rule{30pt}{10pt}}$ | $\color{#5B9CD5}{\rule{30pt}{10pt}}$ | — | — | — |
| Client | $\color{#FFDC64}{\rule{30pt}{10pt}}$ | $\color{#9F9371}{\rule{30pt}{10pt}}$ | $\color{#FFF899}{\rule{30pt}{10pt}}$ | $\color{#3E668F}{\rule{30pt}{10pt}}$ | $\color{#FADE2A}{\rule{30pt}{10pt}}$ | — | — |
| Timeout | $\color{#FFA500}{\rule{30pt}{10pt}}$ | $\color{#54381C}{\rule{30pt}{10pt}}$ | $\color{#6F450C}{\rule{30pt}{10pt}}$ | $\color{#7F7F7F}{\rule{30pt}{10pt}}$ | $\color{#3400E6}{\rule{30pt}{10pt}}$ | — | — |
| BufferPin | $\color{#00D2B4}{\rule{30pt}{10pt}}$ | $\color{#00A1E6}{\rule{30pt}{10pt}}$ | $\color{#FF9830}{\rule{30pt}{10pt}}$ | — | $\color{#49F841}{\rule{30pt}{10pt}}$ | — | — |
| Activity | $\color{#9664FF}{\rule{30pt}{10pt}}$ | $\color{#FFA500}{\rule{30pt}{10pt}}$ | $\color{#B877D9}{\rule{30pt}{10pt}}$ | — | $\color{#B877D9}{\rule{30pt}{10pt}}$ | — | — |
| Extension | $\color{#BE96FF}{\rule{30pt}{10pt}}$ | $\color{#007B14}{\rule{30pt}{10pt}}$ | $\color{#CA95E5}{\rule{30pt}{10pt}}$ | $\color{#7771A4}{\rule{30pt}{10pt}}$ | $\color{#00AF4A}{\rule{30pt}{10pt}}$ | — | — |

¹ [PASH-Viewer](https://github.com/dbacvetkov/PASH-Viewer) /
[ASH-Viewer](https://github.com/akardapolov/ASH-Viewer) — identical PG palettes,
common codebase ancestry. Colors from `EventColors.java` / `Options.java`.

² [PostgresAI](https://gitlab.com/postgres-ai/postgresai) Grafana dashboards
(Dashboard 4 — Wait Sampling). pg_ash's semantic mapping (CPU=green, Lock=red, etc.)
is aligned with these dashboards. Hex values are Grafana dark-theme interpretations
of named colors (`green` → `#73BF69`, `red` → `#F2495C`, etc.).

³ **AWS Performance Insights** uses the D3.js Category20 palette with **dynamic**
positional assignment — colors depend on rank by AAS, not on the wait event type.
CPU reliably gets green (`#2CA02C`) because it is almost always the top contributor.
Hex codes pixel-sampled from
[AWS blog](https://aws.amazon.com/blogs/database/analyzing-amazon-rds-database-workload-with-performance-insights/)
screenshots.

⁴ **AlloyDB Advanced Query Insights** — pixel-sampled from
[Google Cloud blog](https://cloud.google.com/blog/products/databases/new-query-insights-capabilities-for-cloud-sql-enterprise-plus)
GIF legend swatches. The basic Cloud SQL Query Insights view only shows three
categories (CPU = green, IO Wait = blue, Lock Wait = orange).

Other sources:
[Pigsty](https://github.com/Vonng/pigsty) `pgsql-session.json`,
[pg_profile](https://github.com/zubkov-andrei/pg_profile) `pg_profile_activity.json`.

"—" = not defined or not observed.

### Oracle ASH (the origin)

Oracle's Active Session History introduced the color conventions that inspired all
PostgreSQL ASH tools. The palette changed significantly between OEM 10g/11g
(Grid Control) and EMCC 13.5 (Cloud Control).

| Wait class | EMCC 13.5 | OEM 10g/11g | PG equivalent |
|---|---|---|---|
| CPU Used | $\color{#35C387}{\rule{30pt}{10pt}}$ teal-green | $\color{#00B300}{\rule{30pt}{10pt}}$ green | CPU |
| CPU Wait | $\color{#A9F89C}{\rule{30pt}{10pt}}$ light green | $\color{#85FF85}{\rule{30pt}{10pt}}$ light green | — |
| User I/O | $\color{#0072CA}{\rule{30pt}{10pt}}$ blue | $\color{#002EE6}{\rule{30pt}{10pt}}$ deep blue | IO |
| System I/O | $\color{#04DEDE}{\rule{30pt}{10pt}}$ cyan | $\color{#00A1E6}{\rule{30pt}{10pt}}$ sky blue | IO |
| Concurrency | $\color{#8B60C9}{\rule{30pt}{10pt}}$ purple | $\color{#993333}{\rule{30pt}{10pt}}$ dark red | LWLock |
| Application | $\color{#FF5C38}{\rule{30pt}{10pt}}$ red-orange | $\color{#C24747}{\rule{30pt}{10pt}}$ red | Lock |
| Commit | $\color{#FFB146}{\rule{30pt}{10pt}}$ amber | $\color{#C28547}{\rule{30pt}{10pt}}$ brown | — |
| Configuration | $\color{#FAF37D}{\rule{30pt}{10pt}}$ yellow | $\color{#54381C}{\rule{30pt}{10pt}}$ dark brown | Timeout |
| Administrative | $\color{#FFCC48}{\rule{30pt}{10pt}}$ gold | $\color{#54541D}{\rule{30pt}{10pt}}$ olive | — |
| Network | $\color{#00C0F0}{\rule{30pt}{10pt}}$ sky blue | $\color{#9C9D6C}{\rule{30pt}{10pt}}$ olive-gray | Client |
| Queueing | $\color{#C5B79B}{\rule{30pt}{10pt}}$ tan | $\color{#757575}{\rule{30pt}{10pt}}$ gray | IPC |
| Cluster | $\color{#CBC2AF}{\rule{30pt}{10pt}}$ beige | $\color{#E8E8E8}{\rule{30pt}{10pt}}$ light gray | — |
| Other | $\color{#F76AAE}{\rule{30pt}{10pt}}$ pink | $\color{#FF578F}{\rule{30pt}{10pt}}$ hot pink | Activity |

EMCC 13.5 hex codes from [modb.pro](https://www.modb.pro/db/188688) (pixel-sampled
from the interface). OEM 10g/11g hex codes from
[ASH-Viewer](https://github.com/akardapolov/ASH-Viewer) source
(`Options.java` → `loadTopActivityColorOracle()`).

### Notable patterns

- **CPU = green** is universal — every tool uses some shade of green for CPU
- **IO**: pg_ash, PASH-Viewer, PostgresAI use blue (following Oracle User I/O);
  AWS PI, Pigsty, pg_profile use orange/yellow; AlloyDB uses red
- **Lock = red** is common (pg_ash, PostgresAI, Pigsty, pg_profile) but not universal
  — AlloyDB uses blue, AWS PI assigns dynamically
- **Lock vs LWLock**: PASH-Viewer uses two similar dark reds (`#C02800` / `#8B1A00`),
  making them hard to distinguish. pg_ash uses red vs pink for clear separation
- **Oracle's palette shifted dramatically** between OEM 10g/11g and EMCC 13.5 — e.g.
  Concurrency went from dark red to purple, Configuration from dark brown to yellow
