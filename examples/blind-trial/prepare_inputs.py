#!/usr/bin/env python3
"""Rebuild the initial trial's exact SQL and documentation from public commits."""
import argparse
import hashlib
from pathlib import Path
import subprocess
import tempfile

BASE = 'db96e305939cb1e9375e8b2029de27020fa57743'
HEADS = ['4e190a0f60248553630378f55e21e40be8a3d256', '4b43e056cab48dd704dd3c8b421a5414fa57f3da', '74879d9897a01835c3541c190c7c0a40096a94e1', 'cb4e1527a630d83d6685e6d64f11d3fe326919db']
DOCS = '95e1ecf83ce68ab9423af2568af678b87c8d9e2f'
SQL_SHA = '94b3d80b6864999922bd9d70794dac4073e8e10e5d11f918d2a0947adf6b6444'


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repo', type=Path, required=True, help='pg_ash checkout with fetched PR refs')
    parser.add_argument('--out', type=Path, required=True, help='new input directory')
    args = parser.parse_args()
    args.out.mkdir(parents=True, exist_ok=False)
    def git(*command):
        return subprocess.check_output(['git', '-C', str(args.repo), *command])
    with tempfile.TemporaryDirectory(prefix='pgash-inputs-') as tmp:
        target = Path(tmp) / 'devel/sql/ash-install.sql'
        target.parent.mkdir(parents=True)
        target.write_bytes(git('show', BASE + ':devel/sql/ash-install.sql'))
        for head in HEADS:
            patch = git('diff', BASE, head, '--', 'devel/sql/ash-install.sql')
            subprocess.run(['git', 'apply', '-'], input=patch, cwd=tmp, check=True)
        contents = target.read_bytes()
    if hashlib.sha256(contents).hexdigest() != SQL_SHA:
        raise RuntimeError('reconstructed SQL differs from the frozen executable')
    (args.out / 'ash-install.sql').write_bytes(contents)
    for relative in ('README.md', 'docs/REFERENCE.md', 'examples/llm-prompt.md',
                     'examples/llm-investigation.sql'):
        path = args.out / 'docs' / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(git('show', DOCS + ':' + relative))
    print('SQL SHA256 ' + SQL_SHA)


if __name__ == '__main__':
    main()
