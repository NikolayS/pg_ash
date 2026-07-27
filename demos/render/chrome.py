#!/usr/bin/env python3
"""
chrome.py -- emit the window-chrome plate PNG for the animation composite.

The stills path draws its chrome as SVG primitives inside ansi2svg.py. The
animation path cannot: agg renders a bare terminal to frames, so the chrome has
to be a separate raster that ffmpeg composites the frames onto. Same theme file,
same construction order, same numbers (§3.3) -- so a still and a frame of the
reel are visually the same window.

The plate is the BACKGROUND, not an overlay: ffmpeg's

    -i frames -i chrome_plate.png -filter_complex "[1][0]overlay=x=..:y=.."

puts the plate first and paints the opaque terminal frames on top of it, at the
body origin. The plate therefore has no transparent hole to cut; it simply
paints the whole card, and the frames cover the body area.

record-demo.sh must MEASURE the agg output with ffprobe and pass those numbers
here. Nothing about the reel's pixel size may be hardcoded -- agg's idea of how
many pixels 100x30 cells occupy depends on the font it loaded.

Usage
  chrome.py --theme theme/pg_ash.json --inner 1400x630 -o out/chrome_plate.png
            [--title "pg_ash 2.0"] [--font fonts/JetBrainsMono-Regular.ttf]
            [--scale 1] [--print-offsets]

Prints, on stdout, a sourceable summary so the caller never recomputes geometry:

  ASH_PLATE_W=1456 ASH_PLATE_H=734 ASH_PLATE_X=28 ASH_PLATE_Y=60
"""

import argparse
import json
import sys

EXIT_RENDER = 6


def load_theme(path):
    with open(path, encoding="utf-8") as fh:
        return json.load(fh)


def build(theme, inner_w, inner_h, title, font_path, scale, metrics="reel"):
    try:
        from PIL import Image, ImageDraw, ImageFont
    except ImportError:
        sys.stderr.write("chrome.py: Pillow is required for the animation "
                         "chrome plate (pip install pillow)\n")
        sys.exit(EXIT_RENDER)

    c = theme["chrome"]
    ui = theme["ui"]
    s = float(scale)

    def px(v):
        return int(round(v * s))

    card_w = inner_w + 2 * px(c["pad_x"])
    card_h = px(c["titlebar_h"]) + 2 * px(c["pad_y"]) + inner_h
    w = card_w + 2 * px(c["margin"])
    h = card_h + 2 * px(c["margin"])

    m = px(c["margin"])
    r = px(c["radius"])
    tb = px(c["titlebar_h"])

    # 1. margin fill
    img = Image.new("RGBA", (w, h), ui["marginfill"])
    d = ImageDraw.Draw(img)

    # 2. card
    d.rounded_rectangle([m, m, m + card_w - 1, m + card_h - 1], radius=r,
                        fill=ui["bg"], outline=ui["border"],
                        width=max(1, px(c["border"])))

    # 3. title bar: rounded on top, square at the bottom, then the hairline.
    #    Same shape the SVG path in ansi2svg.chrome() describes, and it must
    #    stay that way — a still and a frame of the reel are supposed to be the
    #    same window.
    #
    #    The rounded rect is drawn to y = m + tb - 1 (NOT m + tb + r). An earlier
    #    version used the latter, which painted the title-bar shade twelve pixels
    #    PAST the hairline: pixel-sampled at x=480, ui.titlebar filled rows 25..74
    #    where titlebar_h is 38, so every frame of the reel carried a visible
    #    lighter band across the top of the card that the stills did not have.
    #    The square patch below then squares off the rounded BOTTOM corners the
    #    rounded rect leaves behind.
    d.rounded_rectangle([m, m, m + card_w - 1, m + tb - 1], radius=r,
                        fill=ui["titlebar"])
    d.rectangle([m, m + tb - r, m + card_w - 1, m + tb - 1], fill=ui["titlebar"])
    d.line([m, m + tb, m + card_w - 1, m + tb], fill=ui["border"],
           width=max(1, px(c["border"])))
    # restore the card's rounded top corners that the square fill above squared
    d.rounded_rectangle([m, m, m + card_w - 1, m + card_h - 1], radius=r,
                        outline=ui["border"], width=max(1, px(c["border"])))

    # 4. traffic lights — vertically centred in the title bar (spec: cy =
    #    margin + 19 for a 38px bar), NOT "margin + dot_x[0]", which was the
    #    same number by coincidence and would have drifted the moment either
    #    the bar height or the first dot's x changed.
    cy = m + tb // 2
    dot_r = px(c["dot_r"])
    for k, col in enumerate(ui["dots"]):
        cx = m + px(c["dot_x"][k])
        d.ellipse([cx - dot_r, cy - dot_r, cx + dot_r, cy + dot_r], fill=col)

    # 5. title
    if title:
        size = theme[metrics]["font_size"] * c["title_size_em"] * s
        font = None
        if font_path:
            try:
                font = ImageFont.truetype(font_path, int(round(size)))
            except OSError:
                font = None
        if font is None:
            sys.stderr.write(
                "chrome.py: could not load %r. Refusing to fall back to a "
                "bitmap default -- the reel title would not match the stills.\n"
                % (font_path,))
            sys.exit(EXIT_RENDER)
        tw = d.textlength(title, font=font)
        d.text((m + card_w / 2.0 - tw / 2.0, cy - size * 0.62),
               title, font=font, fill=ui["dim"])

    body_x = m + px(c["pad_x"])
    body_y = m + tb + px(c["pad_y"])
    return img, body_x, body_y


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--theme", required=True)
    ap.add_argument("--inner", required=True,
                    help="WxH of the terminal raster, from ffprobe")
    ap.add_argument("-o", "--out", required=True)
    ap.add_argument("--title", default="pg_ash 2.0")
    ap.add_argument("--font")
    ap.add_argument("--scale", type=float, default=1.0)
    ap.add_argument("--metrics", default="reel", choices=["still", "reel"])
    a = ap.parse_args(argv)

    try:
        iw, ih = (int(x) for x in a.inner.lower().split("x"))
    except ValueError:
        sys.stderr.write("chrome.py: --inner must look like 1400x630\n")
        return 1

    theme = load_theme(a.theme)
    img, bx, by = build(theme, iw, ih, a.title, a.font, a.scale, a.metrics)
    img.save(a.out)
    sys.stdout.write("ASH_PLATE_W=%d ASH_PLATE_H=%d ASH_PLATE_X=%d ASH_PLATE_Y=%d\n"
                     % (img.width, img.height, bx, by))
    return 0


if __name__ == "__main__":
    sys.exit(main())
