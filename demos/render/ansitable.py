#!/usr/bin/env python3
"""
ansitable.py -- ANSI-aware table formatter for psql unaligned output.

Why this exists
---------------
psql's own ALIGNED formatter escapes 0x1B to the four-character text "\\x1B" and
then measures column widths from the escaped text. A cell carrying three
24-bit colour runs is therefore padded as if it were ~60 characters wider than
it draws, and every row with a different number of colour runs lands at a
different visible width. All three prototypes hit this independently; one
measured a 390-column border rule under a 100-column table.

So we ask psql for UNALIGNED output, which passes raw ESC bytes through
untouched (`psql -A -F <sep> -P footer=off`), and own the alignment here,
measuring DISPLAY width via render/dwidth.py.

Two jobs beyond alignment:

  * rstrip_cell -- ash.chart() right-pads its bar string to a constant
    length() as its own workaround for the same psql bug. We align on display
    width, so that padding is dead weight: it would inflate the still by ~70
    columns. Strip it, but keep any SGR bytes that were interleaved with it so
    the colour state machine stays balanced.

  * wrapping -- ash.summary() emits a 108-column `value` and ash.top('query_id')
    emits full statement text. Truncating would be editing the reader's output.
    Wrapping is what a terminal does, and it keeps every character. When the
    table exceeds --max-width, the widest column is folded and continuation
    lines are drawn with a dim gutter.

Input : psql -A -F $'\\x1f' -P footer=off  (header line + data lines)
Output: box-drawn table with real ESC codes preserved, LF endings.
"""

import argparse
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from dwidth import strip_ansi, width as vis  # noqa: E402

ESC_RE = re.compile(r"\x1b\[[0-9;:]*[A-Za-z]")

# psql's own "unicode" linestyle, single border.
BOX = dict(tl="┌", tm="┬", tr="┐",
           ml="├", mm="┼", mr="┤",
           bl="└", bm="┴", br="┘",
           h="─", v="│")

NUMERIC = re.compile(r"^-?[0-9][0-9,]*(\.[0-9]+)?%?$")

# Trailing run of blanks, possibly interleaved with escapes.
TAIL_RE = re.compile(r"(?:\x1b\[[0-9;:]*[A-Za-z]|[ \t])*$")


def pad(s, w, right=False):
    n = w - vis(s)
    if n <= 0:
        return s
    return (" " * n + s) if right else (s + " " * n)


def rstrip_cell(s):
    """Drop trailing blanks, preserving any escapes they were interleaved with."""
    m = TAIL_RE.search(s)
    if not m or m.start() == len(s):
        return s
    return s[:m.start()] + "".join(ESC_RE.findall(m.group(0)))


def sgr_state(s):
    """Return the SGR sequence in effect at the end of `s`, or '' if reset.

    Needed when a coloured cell is folded: the continuation line has to re-open
    whatever colour was active at the fold point, or the rest of the cell paints
    in the default foreground.
    """
    state = ""
    for m in ESC_RE.finditer(s):
        seq = m.group(0)
        if seq in ("\x1b[0m", "\x1b[m"):
            state = ""
        else:
            state = seq
    return state


# Block-drawing glyphs used by chart bars and legends.
BLOCK_GLYPHS = "\u2588\u2593\u2592\u2591"
BAR_GLYPHS = BLOCK_GLYPHS + "\u00b7"


def is_bar_cell(s):
    """Return True only for chart data, not for a labelled chart legend."""
    plain = strip_ansi(s)
    return (
        any(glyph in plain for glyph in BAR_GLYPHS)
        and all(char.isspace() or char in BAR_GLYPHS for char in plain)
    )


def chart_legend_entries(s):
    """Return ANSI-preserving legend entries, or None when `s` is not a legend.

    ash.chart() separates entries with two spaces and starts each entry with
    its swatch. Keeping that delimiter semantic matters: an ordinary word fold
    can leave the swatch at the end of one line and its label at the start of
    the next.
    """
    entries = re.split(r" {2,}", s.strip())
    if len(entries) < 2:
        return None
    for entry in entries:
        plain = strip_ansi(entry)
        if (
            len(plain) < 3
            or plain[0] not in BAR_GLYPHS
            or plain[1] != " "
            or not plain[2:].strip()
        ):
            return None
    return entries


def fold_chart_legend(entries, budget):
    """Pack whole chart-legend entries into lines within `budget`."""
    out = []
    line = ""
    for entry in entries:
        candidate = entry if not line else line + "  " + entry
        if line and vis(candidate) > budget:
            out.append(line)
            line = entry
        else:
            line = candidate
    if line:
        out.append(line)
    return out


def fold(s, budget):
    """Split a possibly-coloured string into chunks of <= budget display columns.

    Prefers a syntactic boundary in the last third of the chunk so words
    survive. Escapes are zero-width and always travel with the character that
    follows.

    A chart data cell is returned UNFOLDED, deliberately. Wrapping a bar onto a
    second line would turn one bucket into two stacked segments. A chart legend
    can contain the same glyphs, but its labels make it prose and safe to fold.
    This matters because ash.chart() keeps per-bucket leaders in addition to its
    top-n series, so the legend's width is data-dependent.
    """
    if vis(s) <= budget:
        return [s]
    if is_bar_cell(s):
        return [s]
    legend_entries = chart_legend_entries(s)
    if legend_entries is not None:
        return fold_chart_legend(legend_entries, budget)
    out = []
    buf = ""
    used = 0
    i = 0
    while i < len(s):
        m = ESC_RE.match(s, i)
        if m:
            buf += m.group(0)
            i = m.end()
            continue
        ch = s[i]
        w = vis(ch)
        if used + w > budget:
            # Back off to a syntactic boundary in the final third of the chunk
            # so a fold lands between tokens rather than inside `sample_i /
            # nterval`. A space is the first choice; a comma is the second,
            # because a long IN-list of quoted literals has no spaces at all.
            plain = strip_ansi(buf)
            cut = plain.rfind(" ")
            comma = plain.rfind(",")
            if comma > cut:
                cut = comma          # keep the comma on the line it ends
            if cut >= int(budget * 0.66):
                keep, rest = split_at_visible(buf, cut + 1)
                out.append(keep.rstrip())
                carry = sgr_state(keep)
                buf = carry + rest.lstrip()
                used = vis(buf)
            else:
                out.append(buf)
                buf = sgr_state(buf)
                used = 0
            continue
        buf += ch
        used += w
        i += 1
    if strip_ansi(buf).strip():
        out.append(buf)
    return out


def split_at_visible(s, n):
    """Split `s` after n visible characters, returning (head, tail)."""
    seen = 0
    i = 0
    while i < len(s) and seen < n:
        m = ESC_RE.match(s, i)
        if m:
            i = m.end()
            continue
        seen += 1
        i += 1
    return s[:i], s[i:]


def render(lines, sep, dim, no_header=False, keep_pad=False, max_width=None,
           auto_plain=True):
    # A result with exactly one column has nothing to align, and boxing it row
    # by row is actively wrong: ash.report() returns one jsonb_pretty() value
    # whose embedded newlines psql emits verbatim, so a box would draw a rule
    # between every line of one JSON document. Emit such a capture as plain
    # screen text instead, dropping psql's header (a bare column name).
    if auto_plain and not any(sep in ln for ln in lines):
        body = lines[1:] if (lines and not no_header) else lines
        return "\n".join(body) + "\n" if body else ""

    rows = [ln.split(sep) for ln in lines if ln != ""]
    if not keep_pad:
        rows = [[rstrip_cell(c) for c in r] for r in rows]
    if not rows:
        return ""
    ncol = max(len(r) for r in rows)
    for r in rows:
        r += [""] * (ncol - len(r))

    head = None if no_header else rows[0]
    data = rows if no_header else rows[1:]

    widths = [max(vis(r[c]) for r in rows) for c in range(ncol)]

    # Right-align a column when every non-blank data cell in it looks numeric.
    right = [bool(data) and all(NUMERIC.match(strip_ansi(r[c]).strip())
                                for r in data if strip_ansi(r[c]).strip())
             for c in range(ncol)]

    # --- fit to budget by folding the widest column ------------------------
    # Table width = sum(col + 2 padding) + (ncol + 1) vertical rules.
    def table_width(ws):
        return sum(w + 2 for w in ws) + ncol + 1

    folded = [[[c] for c in r] for r in rows]
    if max_width is not None:
        guard = 0
        while table_width(widths) > max_width and guard < ncol:
            guard += 1
            over = table_width(widths) - max_width
            victim = widths.index(max(widths))
            budget = max(widths[victim] - over, 12)
            body = range(1, len(rows)) if head is not None else range(len(rows))
            for ri in body:
                folded[ri][victim] = fold(rows[ri][victim], budget)
            # The column is now as wide as its widest fragment (the header,
            # which is never folded, still sets a floor).
            candidates = [vis(head[victim])] if head is not None else []
            for ri in body:
                candidates += [vis(p) for p in folded[ri][victim]]
            widths[victim] = max(candidates or [0])

    D, R = dim, "\x1b[0m" if dim else ""
    bar = D + BOX["v"] + R

    def rule(l, m, r):
        return D + l + m.join(BOX["h"] * (w + 2) for w in widths) + r + R

    out = [rule(BOX["tl"], BOX["tm"], BOX["tr"])]

    if head is not None:
        cells = []
        for c, h in enumerate(head):
            t = h.strip()
            free = widths[c] - vis(t)
            lft = free // 2
            cells.append(" " * lft + t + " " * (free - lft))   # centred, like psql
        out.append(bar + bar.join(" %s " % c for c in cells) + bar)
        out.append(rule(BOX["ml"], BOX["mm"], BOX["mr"]))

    start = 0 if head is None else 1
    for ri in range(start, len(rows)):
        parts = folded[ri]
        nsub = max(len(p) for p in parts)
        for k in range(nsub):
            cells = []
            for c in range(ncol):
                piece = parts[c][k] if k < len(parts[c]) else ""
                cells.append(pad(piece, widths[c], right[c] and k == 0))
            out.append(bar + bar.join(" %s " % c for c in cells) + bar)

    out.append(rule(BOX["bl"], BOX["bm"], BOX["br"]))
    return "\n".join(out) + "\n"


def sgr(hexcolor):
    """#RRGGBB -> a 24-bit foreground SGR sequence."""
    h = hexcolor.lstrip("#")
    return "\x1b[38;2;%d;%d;%dm" % (int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16))


def prompt_line(sql, theme, cols, hang=6):
    """Render the first body line: `ash ▸ <templated sql>`, wrapped to `cols`.

    The SQL shown is the TEMPLATED form (`since => $SINCE`), never the expanded
    ISO timestamp. Expanded literals are noise: they are 25 characters of
    irrelevance that push the interesting part of the call off the right edge,
    and they make the image look stale the day after it was generated.

    Wrapping happens HERE rather than in the renderer so that every line of the
    .ansi already satisfies the width gate -- the gate and the picture then
    agree by construction.
    """
    ui = theme["ui"]
    head = sgr(ui["prompt_accent"]) + "ash " + sgr(ui["prompt_dim"]) + "▸ " \
        + sgr(ui["fg"])
    # "ash ▸ " is 6 display columns and the hanging indent is 6 spaces, so every
    # line -- first or continuation -- gets the same budget.
    budget = cols - hang
    lines = fold(sql, budget) or [""]
    out = [head + lines[0] + "\x1b[0m"]
    for extra in lines[1:]:
        out.append(sgr(ui["fg"]) + " " * hang + extra + "\x1b[0m")
    return "\n".join(out) + "\n"


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("-F", "--sep", default="\x1f",
                    help="field separator used by psql -A -F")
    ap.add_argument("--prompt", help="templated SQL to show as the prompt line")
    ap.add_argument("--theme", help="theme json (required with --prompt)")
    ap.add_argument("--cols", type=int, default=100,
                    help="column budget for the prompt line")
    ap.add_argument("--dim", default="\x1b[38;2;46;67;73m",
                    help="SGR for the box rules (theme ui.rule)")
    ap.add_argument("--no-header", action="store_true")
    ap.add_argument("--keep-pad", action="store_true",
                    help="do not strip ash.chart()'s constant-length padding")
    ap.add_argument("--no-auto-plain", dest="auto_plain", action="store_false",
                    default=True,
                    help="box single-column output too (diagnostic)")
    ap.add_argument("--max-width", type=int,
                    help="fold the widest column until the table fits")
    ap.add_argument("-o", "--out")
    a = ap.parse_args(argv)

    data = sys.stdin.buffer.read().decode("utf-8", "replace")
    lines = data.split("\n")
    while lines and lines[-1] == "":
        lines.pop()
    txt = render(lines, a.sep, a.dim, a.no_header, a.keep_pad, a.max_width,
                 a.auto_plain)

    if a.prompt:
        if not a.theme:
            sys.stderr.write("ansitable: --prompt requires --theme\n")
            return 1
        import json
        with open(a.theme, encoding="utf-8") as fh:
            theme = json.load(fh)
        txt = prompt_line(a.prompt, theme, a.cols) + "\n" + txt

    if a.out:
        with open(a.out, "w", encoding="utf-8") as fh:
            fh.write(txt)
    else:
        sys.stdout.write(txt)
    return 0


if __name__ == "__main__":
    sys.exit(main())
