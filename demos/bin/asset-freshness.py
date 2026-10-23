#!/usr/bin/env python3
"""Bind reviewed demo artifacts to their capture and rendering inputs."""
import argparse
import hashlib
import json
import re
import subprocess
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
MANIFEST = ROOT / 'demos/fixtures/assets.json'


def fingerprints():
    source_dirs = ('demos/bin', 'demos/scenes', 'demos/lib', 'demos/render',
                   'demos/theme', 'demos/fonts', 'demos/fixtures')
    fixed = ('demos/Makefile', 'devel/sql/ash-install.sql',
             'sql/ash-install.sql', 'devel/scripts/ash_sql_chain.py')
    artifacts = {'assets/ash_demo.gif', 'assets/ash_demo.mp4'}
    scenes = ROOT / 'demos/scenes/scenes.tsv'
    if scenes.exists():
        for line in scenes.read_text().splitlines():
            name = line.split('\t', 1)[0]
            if re.fullmatch(r'[a-z0-9_]+', name):
                artifacts.update(f'assets/{name}.{suffix}' for suffix in ('svg', 'png'))
    # Git checkouts verify committed inputs, including staged new files.
    # Source archives use the same scope without requiring Git metadata.
    try:
        top = subprocess.run(['git', '-C', str(ROOT), 'rev-parse', '--show-toplevel'],
                             text=True, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
        in_git = top.returncode == 0 and Path(top.stdout.strip()).resolve() == ROOT
    except FileNotFoundError:
        in_git = False
    if in_git:
        names = subprocess.check_output(
            ['git', '-C', str(ROOT), 'ls-files', '-z', '--',
             *source_dirs, *fixed, *sorted(artifacts)]).decode().split('\0')
        paths = {ROOT / name for name in names if name}
    else:
        paths = {ROOT / name for name in (*fixed, *artifacts)}
        for directory in source_dirs:
            paths.update((ROOT / directory).rglob('*'))
    paths = {p for p in paths if p.is_file() and p != MANIFEST
             and '__pycache__' not in p.parts
             and not any(part.startswith('.') for part in p.relative_to(ROOT).parts)
             and p.suffix not in ('.pyc', '.swp', '.swo', '.tmp')
             and not p.name.endswith('~')}
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
