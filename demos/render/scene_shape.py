#!/usr/bin/env python3
"""
scene_shape.py -- reduce a raw scene capture to its CONTRACT fingerprint.

Why a fingerprint and not a diff
--------------------------------
`make check` proves the renderer still turns known bytes into the same SVG. It
cannot notice that pg_ash's output moved underneath it: rename a reader, drop a
column, change a header, and the committed fixtures keep passing forever while
the live capture quietly becomes something else. That is how the landing page
went on advertising a reader 2.0 had removed.

The obvious alarm -- re-capture and `git diff --exit-code demos/fixtures` -- does
not work, and it looks like it should. Every number in a capture is a real
measurement of a real workload, so a fresh seed legitimately changes almost
every byte, the diff is red on every run, and a check that is always red is
ignored. A first attempt at masking (digits -> #, collapse glyph runs) failed
for the same reason one level up: WHICH wait event ranks fourth, and WHICH
statement ranks third, are also measurements. They moved between two runs
fifteen minutes apart on the same machine.

So the fingerprint is only the part of a capture that is a contract:

  cols=N                 how many fields the reader projects
  head=a|b|c             what they are called, in order
  keys=x,y,z             for a jsonb capture: its top-level keys, sorted
  labels=x,y,z           for a (metric, value) or (period, ...) reader: the
                         first column's values, sorted -- those are a fixed
                         vocabulary, not data. `key` columns are excluded
                         precisely because they ARE data.

What this catches: a renamed or reordered column, a reader that gained or lost
one, a report key that appeared or vanished, a summary metric that disappeared.
What it deliberately does not catch: any change in a measured value, including
which event or statement came top. Those are the demo working, not rotting.

Usage:  scene_shape.py --sep $'\\x1f' capture.raw
"""

import argparse
import re
import sys

SGR = re.compile(r'\x1b\[[0-9;:?]*[@-~]|\x1b\][^\x07\x1b]*(?:\x07|\x1b\\)')
JSON_KEY = re.compile(r'^\s*"([^"]+)"\s*:', re.MULTILINE)

# First-column names whose values are a fixed vocabulary and therefore contract.
# `key` is excluded on purpose: in ash.top() and ash.compare() it holds whatever
# the workload happened to produce.
LABEL_COLUMNS = ('metric', 'period')


def strip(s):
    return SGR.sub('', s)


def fingerprint(text, sep):
    text = strip(text)
    lines = [l for l in text.split('\n')]
    while lines and not lines[-1].strip():
        lines.pop()
    if not lines:
        return 'empty'

    # A capture with no separator anywhere is a single-column result: either
    # jsonb_pretty() or a chart legend. jsonb_pretty is the interesting case.
    if not any(sep in l for l in lines):
        keys = sorted(set(JSON_KEY.findall(text)))
        if keys:
            return 'cols=1 keys=' + ','.join(keys)
        return 'cols=1 lines=%d' % len(lines)

    rows = [l.split(sep) for l in lines]
    head = rows[0]
    parts = ['cols=%d' % len(head), 'head=' + '|'.join(h.strip() for h in head)]

    if head and head[0].strip() in LABEL_COLUMNS:
        labels = sorted(set(r[0].strip() for r in rows[1:] if r and r[0].strip()))
        parts.append('labels=' + ','.join(labels))
    return ' '.join(parts)


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument('capture')
    ap.add_argument('-F', '--sep', default='\x1f')
    a = ap.parse_args(argv)
    with open(a.capture, 'rb') as fh:
        text = fh.read().decode('utf-8', 'replace')
    sys.stdout.write(fingerprint(text, a.sep) + '\n')
    return 0


if __name__ == '__main__':
    sys.exit(main())
