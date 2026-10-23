#!/usr/bin/env python3
"""Bind reviewed demo artifacts to their capture and rendering inputs."""
import argparse
import hashlib
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
MANIFEST = ROOT / 'demos/fixtures/assets.json'


def fingerprints():
    paths = {ROOT / 'devel/sql/ash-install.sql'}
    for directory in ('assets', 'demos/scenes', 'demos/lib', 'demos/render',
                      'demos/theme', 'demos/fonts', 'demos/fixtures'):
        paths.update(p for p in (ROOT / directory).rglob('*')
                     if p.is_file() and '__pycache__' not in p.parts
                     and p.suffix != '.pyc' and p != MANIFEST)
    for name in ('ash-demo', 'capture-stills.sh', 'record-demo.sh', 'make-fixtures.sh'):
        paths.add(ROOT / 'demos/bin' / name)
    return {str(p.relative_to(ROOT)): hashlib.sha256(p.read_bytes()).hexdigest()
            for p in sorted(paths)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--record', action='store_true',
                        help='record hashes after regeneration and visual review')
    args = parser.parse_args()
    actual = fingerprints()
    if args.record:
        MANIFEST.write_text(json.dumps({'format': 1, 'sha256': actual}, indent=2) + '\n')
        print('Recorded demo inputs and artifacts; review the manifest diff.')
        return 0
    if not MANIFEST.exists():
        print('Demo asset manifest missing; regenerate and review the artifacts.', file=sys.stderr)
        return 1
    expected = json.loads(MANIFEST.read_text())
    if expected.get('format') != 1:
        print('Unsupported demo asset manifest format.', file=sys.stderr)
        return 1
    old = expected['sha256']
    changed = sorted(name for name in old.keys() | actual.keys()
                     if old.get(name) != actual.get(name))
    if changed:
        print('Demo artifacts or their inputs changed since the reviewed capture:', file=sys.stderr)
        print('\n'.join(changed), file=sys.stderr)
        print('Run make -C demos all fixtures, inspect the results, then make -C demos asset-manifest.',
              file=sys.stderr)
        return 1
    print('Committed demo artifacts match their recorded capture inputs.')
    return 0


if __name__ == '__main__':
    sys.exit(main())
