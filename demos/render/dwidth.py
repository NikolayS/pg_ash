#!/usr/bin/env python3
"""
dwidth.py -- the display-width oracle. One implementation, used by everything.

Why this is its own file
------------------------
Three independent things need to answer "how many terminal columns does this
line occupy": the scripted stills preflight, the table formatter, and the
animation path's pane verification. They must agree exactly, or the width gate
passes on one path and the other ships a wrapped, garbled frame.

And it must not be `awk length`. awk counts UTF-8 *bytes*: measured 393 for a
131-column table full of box-drawing glyphs. It must not be `len(s)` on the raw
string either: a 24-bit SGR run is 19 characters of zero display width, and
ash.chart(color => true) is nothing but SGR runs -- measured 154 characters for
a 131-column line.

Rules implemented here:
  * CSI sequences (ESC [ ... final)      -> width 0
  * OSC sequences (ESC ] ... BEL | ST)   -> width 0
  * other two-byte ESC sequences         -> width 0
  * East Asian Wide / Fullwidth          -> width 2
  * combining marks (Mn/Me)              -> width 0
  * everything else printable            -> width 1

CLI
  dwidth.py FILE...                  print "width<TAB>path:line" for every line
  dwidth.py --max N FILE...          exit 5 naming the first line over budget
  dwidth.py --max N --quiet FILE...  same, but silent when everything fits
  dwidth.py --report FILE...         print ONE number: the widest line. This is
                                     what the preflight table prints, so the
                                     number a human reads and the number the
                                     gate enforces come from the same code.
"""

import argparse
import re
import sys
import unicodedata

# CSI: ESC [ <params> <final>.  OSC: ESC ] ... (BEL | ESC \).
# Anything else starting with ESC: swallow ESC plus one byte.
_ANSI = re.compile(
    r"\x1b\[[0-9;:?<>=]*[@-~]"      # CSI
    r"|\x1b\][^\x07\x1b]*(?:\x07|\x1b\\)"  # OSC
    r"|\x1b[@-Z\\-_]"               # two-byte escapes
)

EXIT_WIDTH = 5  # §2.6: capture verification failure


def strip_ansi(s):
    """Remove every escape sequence, leaving only the characters a terminal draws."""
    return _ANSI.sub("", s)


def char_width(ch):
    if unicodedata.combining(ch):
        return 0
    cat = unicodedata.category(ch)
    if cat in ("Mn", "Me", "Cf"):
        return 0
    if ch < " " or ch == "\x7f":
        return 0
    return 2 if unicodedata.east_asian_width(ch) in ("W", "F") else 1


def width(s):
    """Display width in terminal columns of a string that may contain SGR."""
    return sum(char_width(c) for c in strip_ansi(s))


def measure_file(path):
    """Yield (lineno, width, text) for each line, 1-indexed."""
    with open(path, "rb") as fh:
        data = fh.read().decode("utf-8", "replace")
    if data.endswith("\n"):
        data = data[:-1]
    for i, line in enumerate(data.split("\n"), 1):
        yield i, width(line), line


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.strip().split("\n")[0])
    ap.add_argument("files", nargs="+")
    ap.add_argument("--max", type=int, help="fail (exit 5) above this width")
    ap.add_argument("--quiet", action="store_true")
    ap.add_argument("--report", action="store_true",
                    help="print only the widest line width, for a caller's table")
    a = ap.parse_args(argv)

    worst = 0
    bad = []
    for p in a.files:
        for lineno, w, line in measure_file(p):
            worst = max(worst, w)
            if a.max is not None and w > a.max:
                bad.append((p, lineno, w, strip_ansi(line)))
            elif a.max is None and not a.report:
                sys.stdout.write("%d\t%s:%d\n" % (w, p, lineno))

    if a.report and a.max is None:
        sys.stdout.write("%d\n" % worst)
        return 0
    if a.max is None:
        return 0
    if bad:
        for p, lineno, w, text in bad:
            sys.stderr.write(
                "width: %s line %d is %d columns, budget is %d\n  %s\n"
                % (p, lineno, w, a.max, text[:120]))
        return EXIT_WIDTH
    if not a.quiet:
        sys.stderr.write("width: ok, widest line %d of %d columns\n" % (worst, a.max))
    return 0


if __name__ == "__main__":
    sys.exit(main())
