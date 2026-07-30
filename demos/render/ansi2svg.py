#!/usr/bin/env python3
"""
ansi2svg.py -- render captured terminal bytes (text + 24-bit ANSI SGR) to SVG.

This is the file that turns "psql output" into "a picture worth putting on a
landing page". Four decisions carry the whole result:

1. BLOCK PROMOTION (§3.5).  U+2588/2593/2591/2592 are never drawn as text.
   Each maximal horizontal run of the same (glyph, colour) becomes one <rect>,
   overdrawn by +0.6px horizontally and +0.4px vertically so adjacent runs and
   adjacent rows fuse with no sub-pixel seams. A stacked ash.chart() then reads
   as continuous coloured area -- a chart -- rather than as a grid of glyphs.

2. METRIC-INDEPENDENT TEXT (§3.6).  Every text run carries textLength and
   lengthAdjust="spacing". If the embedded font ever fails to load, glyph
   shapes change but column positions do not, so the table never shears.

3. TRUECOLOR PASSTHROUGH.  pg_ash emits the docs/COLOR_SCHEME.md palette itself
   as 24-bit SGR. This renderer parses those bytes and writes the same RGB into
   the SVG. It never assigns a wait colour of its own -- if it did, the image
   would stop being evidence of what pg_ash actually prints.

4. DETERMINISM (§3.7).  Subset codepoints sorted, head.created/modified forced
   to 0, fixed-precision number formatting, no ids, no timestamps, no
   randomness. Same .ansi in => byte-identical .svg out, verified with cmp.

Usage
  ansi2svg.py IN.ansi -o OUT.svg --theme theme/pg_ash.json \\
              --font fonts/JetBrainsMono-Regular.ttf --title "..." [--cols 100]
"""

import argparse
import base64
import hashlib
import io
import json
import os
import re
import sys
import warnings
import logging

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from dwidth import char_width  # noqa: E402

EXIT_RENDER = 6  # §2.6: render failure

CSI = re.compile(r"\x1b\[([0-9;:?]*)([@-~])")
OSC = re.compile(r"\x1b\][^\x07\x1b]*(?:\x07|\x1b\\)")

# --------------------------------------------------------------------------
# theme
# --------------------------------------------------------------------------


def load_theme(path):
    """Read theme/pg_ash.json. The ONLY source of colour and geometry."""
    with open(path, encoding="utf-8") as fh:
        t = json.load(fh)
    for key in ("font", "still", "chrome", "ui", "ansi", "shade"):
        if key not in t:
            sys.stderr.write("ansi2svg: theme is missing '%s'\n" % key)
            sys.exit(EXIT_RENDER)
    return t


# Literal ink coverage (1.00/0.75/0.50/0.25) renders #FF5555 at rank 3 as dark
# maroon: the rank ordering survives but the palette identity does not, and the
# whole point of the colour scheme is that Lock is unmistakably red. The theme
# ships a compressed curve instead. --ink restores the literal values for
# debugging a chart.
BLOCKS_INK = {"█": 1.00, "▓": 0.75, "░": 0.50, "▒": 0.25}


# --------------------------------------------------------------------------
# terminal model
# --------------------------------------------------------------------------

class Cell(object):
    __slots__ = ("ch", "fg", "bg", "bold", "ul")

    def __init__(self, ch, fg, bg, bold, ul):
        self.ch, self.fg, self.bg, self.bold, self.ul = ch, fg, bg, bold, ul

    def style(self):
        return (self.fg, self.bg, self.bold, self.ul)


class Screen(object):
    """A minimal, deliberately dumb terminal: SGR, newline, carriage return, tab.

    No cursor addressing, no scrolling, no alternate screen. The scripted stills
    path never produces any of that -- psql writes a table and stops -- and
    supporting it would only create ways for a still to disagree with the bytes.
    """

    def __init__(self, theme):
        self.theme = theme
        self.rows = []
        self.reset_sgr()

    def reset_sgr(self):
        self.fg = None
        self.bg = None
        self.bold = False
        self.ul = False
        self.inv = False

    def _row(self, r):
        while len(self.rows) <= r:
            self.rows.append([])
        return self.rows[r]

    def put(self, r, c, ch):
        row = self._row(r)
        while len(row) <= c:
            row.append(Cell(" ", None, None, False, False))
        fg, bg = self.fg, self.bg
        if self.inv:
            fg, bg = (bg or self.theme["ui"]["bg"]), (fg or self.theme["ui"]["fg"])
        row[c] = Cell(ch, fg, bg, self.bold, self.ul)

    def sgr(self, params):
        pal = self.theme["ansi"]
        if not params:
            params = "0"
        p = [int(x) if x.isdigit() else 0
             for x in params.replace(":", ";").split(";")]
        i = 0
        while i < len(p):
            v = p[i]
            if v == 0:
                self.reset_sgr()
            elif v == 1:
                self.bold = True
            elif v in (2, 21, 22):
                self.bold = False
            elif v == 4:
                self.ul = True
            elif v == 7:
                self.inv = True
            elif v == 24:
                self.ul = False
            elif v == 27:
                self.inv = False
            elif 30 <= v <= 37:
                self.fg = pal[v - 30]
            elif 90 <= v <= 97:
                self.fg = pal[v - 90 + 8]
            elif 40 <= v <= 47:
                self.bg = pal[v - 40]
            elif 100 <= v <= 107:
                self.bg = pal[v - 100 + 8]
            elif v == 39:
                self.fg = None
            elif v == 49:
                self.bg = None
            elif v in (38, 48):
                target = "fg" if v == 38 else "bg"
                if i + 1 < len(p) and p[i + 1] == 2 and i + 4 < len(p):
                    setattr(self, target, "#%02X%02X%02X"
                            % (p[i + 2] & 255, p[i + 3] & 255, p[i + 4] & 255))
                    i += 4
                elif i + 1 < len(p) and p[i + 1] == 5 and i + 2 < len(p):
                    setattr(self, target, xterm256(p[i + 2], pal))
                    i += 2
            i += 1


def xterm256(n, pal):
    if n < 16:
        return pal[n]
    if n < 232:
        n -= 16
        lv = [0, 95, 135, 175, 215, 255]
        return "#%02X%02X%02X" % (lv[n // 36], lv[(n // 6) % 6], lv[n % 6])
    g = 8 + (n - 232) * 10
    return "#%02X%02X%02X" % (g, g, g)


def parse(data, theme, cols=None, hang=6):
    """bytes/str -> Screen.

    `cols` wraps overlong lines at the column budget with a `hang`-space
    hanging indent, which is what makes a long prompt line legible instead of
    silently clipped.
    """
    if isinstance(data, bytes):
        data = data.decode("utf-8", "replace")
    data = OSC.sub("", data)
    scr = Screen(theme)
    r = c = 0
    i = 0
    n = len(data)
    indent = 0
    while i < n:
        ch = data[i]
        if ch == "\x1b":
            m = CSI.match(data, i)
            if m:
                if m.group(2) == "m":
                    scr.sgr(m.group(1))
                i = m.end()
                continue
            i += 1
            continue
        if ch == "\n":
            r += 1
            c = 0
            indent = 0
            scr._row(r)
            i += 1
            continue
        if ch == "\r":
            c = 0
            i += 1
            continue
        if ch == "\t":
            c = (c // 8 + 1) * 8
            i += 1
            continue
        if ch < " ":
            i += 1
            continue
        w = char_width(ch)
        if cols and c + w > cols:
            r += 1
            indent = hang
            c = indent
        scr.put(r, c, ch)
        c += max(w, 1)
        i += 1
    while scr.rows and not any(x.ch != " " for x in scr.rows[-1]):
        scr.rows.pop()
    return scr


# --------------------------------------------------------------------------
# font subsetting
# --------------------------------------------------------------------------

def subset_font(ttf_path, codepoints):
    """Return (family, base64 woff2) or (None, None) if it cannot be done."""
    try:
        from fontTools import subset
        from fontTools.ttLib import TTFont
    except ImportError:
        return None, None
    if not ttf_path or not os.path.isfile(ttf_path):
        return None, None

    opts = subset.Options()
    opts.flavor = "woff2"
    opts.desubroutinize = True
    opts.layout_features = []
    opts.name_IDs = []
    opts.notdef_outline = True
    opts.recalc_bounds = False
    opts.recalc_timestamp = False

    # fontTools reports the forced zero timestamp as "'created' timestamp seems
    # very low" through BOTH the warnings module and its own logger. Silence
    # both: it is expected, it is the point, and it would otherwise appear on
    # stderr for every single render.
    logging.getLogger("fontTools").setLevel(logging.ERROR)
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            font = TTFont(ttf_path, recalcBBoxes=False, recalcTimestamp=False)
            # Determinism: without this the woff2 carries the build time and two
            # renders of the same input differ.
            font["head"].created = 0
            font["head"].modified = 0
            s = subset.Subsetter(options=opts)
            s.populate(unicodes=sorted(codepoints))
            s.subset(font)
            buf = io.BytesIO()
            font.flavor = "woff2"
            font.save(buf)
    except Exception as exc:
        # A corrupt or truncated TTF must be a clean exit 6 through the
        # --require-font path, not a fontTools traceback and exit 1. The caller
        # decides whether an unembeddable font is fatal; this function only
        # reports that it could not be embedded.
        sys.stderr.write("ansi2svg: cannot subset %s: %s: %s\n"
                         % (ttf_path, type(exc).__name__, exc))
        return None, None
    return "AshMono", base64.b64encode(buf.getvalue()).decode("ascii")


# --------------------------------------------------------------------------
# SVG emission
# --------------------------------------------------------------------------

def esc(s):
    return (s.replace("&", "&amp;").replace("<", "&lt;")
             .replace(">", "&gt;").replace('"', "&quot;"))


def fmt(x):
    """Fixed-precision, locale-free, deterministic number formatting."""
    s = "%.3f" % x
    s = s.rstrip("0").rstrip(".")
    return s if s not in ("", "-0") else "0"


class Renderer(object):
    def __init__(self, theme, font_path=None, bold_path=None,
                 promote=True, ink=False, metrics="still"):
        self.t = theme
        self.font_path = font_path
        self.bold_path = bold_path
        self.promote = promote
        self.blocks = BLOCKS_INK if ink else theme["shade"]
        m = theme[metrics]
        self.fs = float(m["font_size"])
        self.cw = self.fs * float(theme["font"]["advance_em"])
        self.lh = self.fs * float(m["line_height"])

    # -- geometry (§3.2) ---------------------------------------------------
    def geometry(self, scr, cols=None):
        # Width follows the CONTENT, not the budget. Pinning every still to
        # ASH_COLS would pad ash.status() with 40 columns of empty card.
        # `cols`, when given, is only a floor-free upper bound already enforced
        # by the width gate.
        c = self.t["chrome"]
        ncols = max((len(r) for r in scr.rows), default=0)
        if cols:
            ncols = min(ncols, cols)
        nrows = len(scr.rows)
        inner_w = ncols * self.cw
        inner_h = nrows * self.lh
        card_w = inner_w + 2 * c["pad_x"]
        card_h = c["titlebar_h"] + 2 * c["pad_y"] + inner_h
        svg_w = card_w + 2 * c["margin"]
        svg_h = card_h + 2 * c["margin"]
        return dict(cols=ncols, rows=nrows, inner_w=inner_w, inner_h=inner_h,
                    card_w=card_w, card_h=card_h, svg_w=svg_w, svg_h=svg_h)

    def codepoints(self, scr, title):
        cps = set()
        for row in scr.rows:
            for cell in row:
                if self.promote and cell.ch in self.blocks:
                    continue     # drawn as <rect>, never as a glyph
                cps.add(ord(cell.ch))
        for chx in title:
            cps.add(ord(chx))
        cps |= set(range(0x20, 0x7F))
        return cps

    # -- chrome (§3.3) -----------------------------------------------------
    def chrome(self, g):
        c = self.t["chrome"]
        ui = self.t["ui"]
        m = c["margin"]
        r = c["radius"]
        w, h = g["card_w"], g["card_h"]
        tb = c["titlebar_h"]
        parts = []

        # 1. margin fill -- the off-background shade that makes the card read
        #    as a card rather than as a full-bleed screenshot.
        parts.append('<rect x="0" y="0" width="%s" height="%s" fill="%s"/>'
                     % (fmt(g["svg_w"]), fmt(g["svg_h"]), ui["marginfill"]))
        # 2. card
        parts.append('<rect x="%s" y="%s" width="%s" height="%s" rx="%s" '
                     'fill="%s" stroke="%s" stroke-width="%s"/>'
                     % (fmt(m + 0.5), fmt(m + 0.5), fmt(w - 1), fmt(h - 1),
                        fmt(r), ui["bg"], ui["border"], fmt(c["border"])))
        # 3. title bar: rounded on top, square at the bottom, plus a hairline
        parts.append(
            '<path d="M%s %s V %s a %s %s 0 0 1 %s -%s H %s a %s %s 0 0 1 %s %s '
            'V %s Z" fill="%s"/>'
            % (fmt(m + 0.5), fmt(m + tb), fmt(m + r + 0.5), fmt(r), fmt(r),
               fmt(r), fmt(r), fmt(m + w - 0.5 - r), fmt(r), fmt(r),
               fmt(r), fmt(r), fmt(m + tb), ui["titlebar"]))
        parts.append('<line x1="%s" y1="%s" x2="%s" y2="%s" stroke="%s" '
                     'stroke-width="%s"/>'
                     % (fmt(m + 0.5), fmt(m + tb), fmt(m + w - 0.5), fmt(m + tb),
                        ui["border"], fmt(c["border"])))
        # 4. traffic lights, vertically centred in the title bar. Derived from
        #    titlebar_h, not from dot_x[0]: those are the same number (19) for a
        #    38px bar, and render/chrome.py used to conflate them, which is the
        #    kind of coincidence that silently desyncs the reel from the stills
        #    the day somebody changes one of them.
        cy = m + tb / 2.0
        for k, col in enumerate(ui["dots"]):
            parts.append('<circle cx="%s" cy="%s" r="%s" fill="%s"/>'
                         % (fmt(m + c["dot_x"][k]), fmt(cy), fmt(c["dot_r"]), col))
        # 5. title
        if self.title:
            size = self.fs * c["title_size_em"]
            parts.append('<text x="%s" y="%s" text-anchor="middle" fill="%s" '
                         'font-size="%s">%s</text>'
                         % (fmt(m + w / 2.0), fmt(cy + size * 0.36),
                            ui["dim"], fmt(size), esc(self.title)))
        return "".join(parts)

    # -- body --------------------------------------------------------------
    def body(self, scr, g):
        c = self.t["chrome"]
        x0 = c["margin"] + c["pad_x"]
        y0 = c["margin"] + c["titlebar_h"] + c["pad_y"]
        rects, texts = [], []

        for ri, row in enumerate(scr.rows):
            y = y0 + ri * self.lh

            # (a) cell backgrounds and promoted block glyphs, coalesced into
            #     maximal horizontal runs.
            i = 0
            while i < len(row):
                cell = row[i]
                blk = self.promote and cell.ch in self.blocks
                key = (cell.fg, self.blocks[cell.ch]) if blk else (cell.bg, None)
                if (blk and cell.fg) or (not blk and cell.bg):
                    j = i + 1
                    while j < len(row):
                        c2 = row[j]
                        b2 = self.promote and c2.ch in self.blocks
                        k2 = (c2.fg, self.blocks[c2.ch]) if b2 else (c2.bg, None)
                        if b2 != blk or k2 != key:
                            break
                        j += 1
                    color, shade = key
                    rects.append(
                        '<rect x="%s" y="%s" width="%s" height="%s" fill="%s"%s/>'
                        % (fmt(x0 + i * self.cw), fmt(y),
                           fmt((j - i) * self.cw + 0.6), fmt(self.lh + 0.4),
                           color,
                           "" if shade in (None, 1.0)
                           else ' fill-opacity="%s"' % fmt(shade)))
                    i = j
                    continue
                i += 1

            # (b) text runs: one <text> per contiguous (fg, bold, underline).
            i = 0
            while i < len(row):
                cell = row[i]
                if (self.promote and cell.ch in self.blocks) or cell.ch == " ":
                    i += 1
                    continue
                st = cell.style()
                j = i
                chars = []
                while j < len(row):
                    c2 = row[j]
                    if c2.style() != st or (self.promote and c2.ch in self.blocks):
                        break
                    chars.append(c2.ch)
                    j += 1
                while chars and chars[-1] == " ":
                    chars.pop()
                    j -= 1
                if not chars:
                    i += 1
                    continue
                run = "".join(chars)
                attrs = ['x="%s"' % fmt(x0 + i * self.cw),
                         'y="%s"' % fmt(y + self.lh * 0.76),
                         'textLength="%s"' % fmt(len(run) * self.cw),
                         'lengthAdjust="spacing"']
                if st[0]:
                    attrs.append('fill="%s"' % st[0])
                if st[2]:
                    attrs.append('font-weight="700"')
                if st[3]:
                    attrs.append('text-decoration="underline"')
                texts.append("<text %s>%s</text>" % (" ".join(attrs), esc(run)))
                i = j

        return '<g>%s</g><g fill="%s">%s</g>' % (
            "".join(rects), self.t["ui"]["fg"], "".join(texts))

    # -- document ----------------------------------------------------------
    def document(self, scr, title, cols=None, require_font=True):
        self.title = title
        g = self.geometry(scr, cols)
        ui = self.t["ui"]

        cps = self.codepoints(scr, title)
        fam, b64 = subset_font(self.font_path, cps)
        bfam, bb64 = (None, None)
        if self.bold_path and any(cell.bold for row in scr.rows for cell in row):
            bfam, bb64 = subset_font(self.bold_path, cps)
        if require_font and not b64:
            sys.stderr.write(
                "ansi2svg: font embedding is required but failed for %r.\n"
                "  A silent fallback to a serif face is exactly the bug this "
                "harness exists to prevent.\n" % (self.font_path,))
            sys.exit(EXIT_RENDER)

        stack = ('"%s", ' % fam if fam else "") + \
            'ui-monospace, "JetBrains Mono", "SFMono-Regular", Menlo, ' \
            'Consolas, "DejaVu Sans Mono", monospace'

        css = []
        if b64:
            css.append("@font-face{font-family:'%s';font-style:normal;"
                       "font-weight:400;src:url(data:font/woff2;base64,%s) "
                       "format('woff2');}" % (fam, b64))
        if bb64:
            css.append("@font-face{font-family:'%s';font-style:normal;"
                       "font-weight:700;src:url(data:font/woff2;base64,%s) "
                       "format('woff2');}" % (fam, bb64))
        # NOTE: no `fill` in this rule, deliberately. A CSS declaration beats a
        # presentation attribute at every specificity, so `text{fill:...}` here
        # would silently override the per-run fill="#FF5555" that carries the
        # whole colour scheme -- measured: every wait colour collapsed to the
        # default foreground while the block rects stayed correct, which is a
        # very convincing-looking wrong picture. The default colour is set as a
        # presentation attribute on the text group instead, where a per-run
        # attribute can win.
        css.append("text{font-family:%s;font-size:%spx;white-space:pre;"
                   "dominant-baseline:auto;}" % (stack, fmt(self.fs)))

        out = []
        out.append('<svg xmlns="http://www.w3.org/2000/svg" width="%s" '
                   'height="%s" viewBox="0 0 %s %s" font-size="%s">'
                   % (fmt(g["svg_w"]), fmt(g["svg_h"]),
                      fmt(g["svg_w"]), fmt(g["svg_h"]), fmt(self.fs)))
        out.append("<style>%s</style>" % "".join(css))
        out.append(self.chrome(g))
        out.append(self.body(scr, g))
        out.append("</svg>")
        return "".join(out) + "\n", g


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("input")
    ap.add_argument("-o", "--out", required=True)
    ap.add_argument("--theme", required=True)
    ap.add_argument("--title", default="")
    ap.add_argument("--font")
    ap.add_argument("--bold-font")
    ap.add_argument("--cols", type=int,
                    help="column budget: wraps overlong lines and pins width")
    ap.add_argument("--metrics", default="still", choices=["still", "reel"])
    ap.add_argument("--no-require-font", dest="require_font",
                    action="store_false", default=True)
    ap.add_argument("--no-blocks", action="store_true",
                    help="draw block glyphs as text (diagnostic)")
    ap.add_argument("--ink", action="store_true",
                    help="literal terminal ink coverage for shade glyphs "
                         "(diagnostic; the theme curve is the shipping one)")
    ap.add_argument("--quiet", action="store_true")
    a = ap.parse_args(argv)

    theme = load_theme(a.theme)
    r = Renderer(theme, a.font, a.bold_font,
                 promote=not a.no_blocks, ink=a.ink, metrics=a.metrics)

    # A clean exit 1, not a traceback: a missing input is a usage error, and a
    # Python stack trace in the middle of a make run tells the reader nothing
    # about which scene went missing or why.
    if not os.path.isfile(a.input):
        sys.stderr.write("ansi2svg: no such capture: %s\n" % a.input)
        return 1
    with open(a.input, "rb") as fh:
        scr = parse(fh.read(), theme, a.cols)
    if not scr.rows:
        sys.stderr.write("ansi2svg: %s produced an empty screen\n" % a.input)
        return EXIT_RENDER

    doc, g = r.document(scr, a.title, a.cols, a.require_font)
    with open(a.out, "w", encoding="utf-8") as fh:
        fh.write(doc)
    if not a.quiet:
        sys.stderr.write(
            "ansi2svg: %s  %dx%d px  %d cols x %d rows  %d bytes  sha256=%s\n"
            % (a.out, int(g["svg_w"]), int(g["svg_h"]), g["cols"], g["rows"],
               len(doc.encode()), hashlib.sha256(doc.encode()).hexdigest()[:12]))
    return 0


if __name__ == "__main__":
    sys.exit(main())
