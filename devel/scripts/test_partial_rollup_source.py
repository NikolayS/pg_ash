#!/usr/bin/env python3
"""Verify partial-source NOTICEs as emitted to a real psql client (#122)."""
import os
from pathlib import Path
import subprocess

script = Path(__file__).resolve().parent.parent / 'tests' / 'partial_rollup_source.sql'
result = subprocess.run(
    [os.environ.get('PSQL', 'psql'), '-X', '-v', 'ON_ERROR_STOP=1', '-f', str(script)],
    stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, check=False,
)
print(result.stdout, end='')
assert result.returncode == 0, 'partial-source SQL assertions failed'
markers = [f'partial-reader-{reader}' for reader in ('aas', 'timeline', 'top', 'periods', 'chart')]
markers += ['partial-source-null-watermark', 'partial-source-negative']
for marker, following in zip(markers, markers[1:]):
    section = result.stdout.split(marker + '\n', 1)[1].split(following + '\n', 1)[0]
    assert 'NOTICE:  01000: pg_ash partial source:' in section, f'{marker}: missing diagnostic'
    assert 'newer raw observations are omitted' in section, f'{marker}: omission not explained'
negative = result.stdout.split('partial-source-negative\n', 1)[1]
assert 'pg_ash partial source:' not in negative, 'partial-source diagnostic has a false positive'
print('Issue #122 partial-source disclosure assertions PASSED')
