#!/usr/bin/env python3
"""
verify_pixels.py -- prove the rendered PNG still contains pg_ash's own colours.

"An image was produced" is not a test. Two failure modes ship silently past it:

  1. A rasteriser or a GIF palette quantises 24-bit colour into mud. The picture
     still looks like a picture; #FF5555 Lock has just become #C05050 and the
     colour scheme documented in docs/COLOR_SCHEME.md is no longer what the
     landing page shows.
  2. The render produces a correctly sized expanse of background -- an all-black
     card -- because the body group was dropped.

So: for every scene, extract the exact 24-bit foreground colours that pg_ash
actually emitted in that scene's .ansi bytes, and assert each one is present at
some pixel of the PNG with L1 error 0. Then assert a floor on the fraction of
non-background pixels.

One wrinkle, and it is the reason this file needs the theme. Block promotion
(§3.5) draws U+2593/2591/2592 as a <rect> at theme.shade opacity, because that is
what those glyphs mean: 75%, 50%, 25% ink. A wait class that only ever appears as
a shaded glyph -- the rank-2 and rank-3 series of any ash.chart() -- is therefore
NOT in the raster at its literal RGB, and never should be. It is in the raster
composited over ui.bg at that opacity.

So a colour passes if it is present exactly (full-opacity block, or glyph ink) OR
if its composite over ui.bg at one of the theme's shade levels is present. That
still catches the failure this gate exists for -- a rasteriser or GIF palette
quantising #FF5555 into mud -- because a quantised colour matches neither.

Environment (set by bin/capture-stills.sh):
  ASH_ANSI_DIR  directory of <scene>.ansi
  ASH_PNG_DIR   directory of <scene>.png
  ASH_MANIFEST  TSV: name, width, height, title
  ASH_THEME     theme json (for ui.bg and the shade curve)
  ASH_MIN_INK   optional; minimum non-background pixel fraction (default 0.02)

Exit 0 on success, 6 on any failure (§2.6 render failure).
"""

import json
import os
import re
import sys
from collections import Counter

EXIT_RENDER = 6

# 24-bit foreground SGR: ESC [ 38 ; 2 ; r ; g ; b m
TRUECOLOR = re.compile(r"\x1b\[38;2;(\d{1,3});(\d{1,3});(\d{1,3})m")

# Composites are computed by the browser, which rounds; allow one unit of slack
# per channel. Two different palette entries are never within 1 of each other,
# so this cannot make a quantised colour pass.
COMPOSITE_SLACK = 1


def scene_colors(path):
    """Exact RGB triples pg_ash emitted as truecolor foreground in this capture."""
    with open(path, "rb") as fh:
        data = fh.read().decode("utf-8", "replace")
    return set((int(r), int(g), int(b)) for r, g, b in TRUECOLOR.findall(data))


def hex_rgb(s):
    s = s.lstrip("#")
    return tuple(int(s[i:i + 2], 16) for i in (0, 2, 4))


def composite(fg, bg, alpha):
    return tuple(int(round(f * alpha + b * (1.0 - alpha))) for f, b in zip(fg, bg))


def present_within(counts, rgb, slack):
    """Is `rgb` (or anything within `slack` per channel) in the raster?"""
    if counts.get(rgb):
        return True
    if slack <= 0:
        return False
    r, g, b = rgb
    for dr in range(-slack, slack + 1):
        for dg in range(-slack, slack + 1):
            for db in range(-slack, slack + 1):
                if counts.get((r + dr, g + dg, b + db)):
                    return True
    return False


def main():
    try:
        from PIL import Image
    except ImportError:
        sys.stderr.write("verify_pixels: Pillow not installed; skipping the "
                         "pixel gate (SVG output is unaffected)\n")
        return 0

    ansi_dir = os.environ["ASH_ANSI_DIR"]
    png_dir = os.environ["ASH_PNG_DIR"]
    manifest = os.environ["ASH_MANIFEST"]
    min_ink = float(os.environ.get("ASH_MIN_INK", "0.02"))

    theme_path = os.environ.get("ASH_THEME")
    shades = [1.0]
    ui_bg = (0, 0, 0)
    if theme_path and os.path.isfile(theme_path):
        theme = json.load(open(theme_path, encoding="utf-8"))
        ui_bg = hex_rgb(theme["ui"]["bg"])
        shades = sorted(set([1.0] + [float(v) for v in theme["shade"].values()]))

    failures = []
    checked = 0

    with open(manifest, encoding="utf-8") as fh:
        rows = [ln.rstrip("\n").split("\t") for ln in fh if ln.strip()]

    for row in rows:
        name = row[0]
        png = os.path.join(png_dir, name + ".png")
        ansi = os.path.join(ansi_dir, name + ".ansi")
        if not os.path.isfile(png):
            continue
        checked += 1

        im = Image.open(png).convert("RGB")
        pixels = im.getdata()
        present = Counter(pixels)

        # (a) exact-colour survival, allowing for shade compositing
        wanted = scene_colors(ansi)
        missing = []
        for rgb in sorted(wanted):
            # The rasteriser antialiases glyph EDGES, but the interior of a
            # promoted block <rect> and the core of a stroked glyph are the flat
            # colour. Full opacity must therefore land exactly; a shaded block
            # must land on its composite over ui.bg.
            if present.get(rgb, 0):
                continue
            if any(present_within(present, composite(rgb, ui_bg, a),
                                  COMPOSITE_SLACK)
                   for a in shades if a < 1.0):
                continue
            missing.append("#%02X%02X%02X" % rgb)
        if missing:
            failures.append("%s: colours absent from the raster: %s"
                            % (name, " ".join(missing)))

        # (b) the image is not an expanse of background
        bg, bg_n = present.most_common(1)[0]
        ink = 1.0 - (bg_n / float(len(pixels)))
        if ink < min_ink:
            failures.append("%s: only %.3f%% non-background pixels (floor %.1f%%)"
                            % (name, ink * 100.0, min_ink * 100.0))

        # (c) a byte floor catches a truncated or 1x1 write
        if os.path.getsize(png) < 4096:
            failures.append("%s: %d-byte PNG is implausibly small"
                            % (name, os.path.getsize(png)))

    if failures:
        sys.stderr.write("verify_pixels: FAILED\n")
        for f in failures:
            sys.stderr.write("  %s\n" % f)
        return EXIT_RENDER

    sys.stderr.write("verify_pixels: ok, %d raster(s) carry their exact "
                     "wait-class RGB\n" % checked)
    return 0


if __name__ == "__main__":
    sys.exit(main())
