#!/usr/bin/env python3
"""Read-only investigator psql adapter; installed beside connection.json/.pgpass."""
import datetime
import json
import os
from pathlib import Path
import subprocess
import sys
import time

root = Path(__file__).resolve().parent
connection = json.loads((root / 'connection.json').read_text())
os.environ.update(PGHOST='127.0.0.1', PGPORT=str(connection['port']),
                  PGDATABASE='appdb', PGUSER=connection['role'],
                  PGPASSFILE=str(root / '.pgpass'), PGAPPNAME='observer',
                  PGOPTIONS='-c default_transaction_read_only=on -c statement_timeout=10000 -c lock_timeout=2000')
args = sys.argv[1:]
script_files = {}
script_file_errors = {}
file_names = []
has_command = False
options = iter(args)
for arg in options:
    if arg in ('-c', '--command', '-f', '--file'):
        value = next(options, None)
        has_command = True
        if arg in ('-f', '--file') and value is not None:
            file_names.append(value)
    elif arg.startswith(('--file=', '-f')):
        has_command = True
        file_names.append(arg[7:] if arg.startswith('--file=') else arg[2:])
    elif arg.startswith(('--command=', '-c')):
        has_command = True
stdin = (sys.stdin.read() if '-' in file_names
         or (not has_command and not sys.stdin.isatty()) else None)
for name in file_names:
    if name == '-':
        continue
    try:
        script_files[name] = Path(name).read_text()
    except (OSError, UnicodeError) as error:
        # Let psql return its own error, but retain the failed audit read.
        script_file_errors[name] = str(error)
started = datetime.datetime.now(datetime.timezone.utc).isoformat()
anchor = time.monotonic()
result = subprocess.run(['psql', '-X', '-v', 'ON_ERROR_STOP=1'] + args,
                        input=stdin, text=True, capture_output=True)
with (root / 'audit.jsonl').open('a') as out:
    out.write(json.dumps(dict(started=started,
              ended=datetime.datetime.now(datetime.timezone.utc).isoformat(),
              elapsed_seconds=time.monotonic() - anchor, args=args, stdin=stdin,
              script_files=script_files, script_file_errors=script_file_errors,
              stdout=result.stdout,
              stderr=result.stderr, returncode=result.returncode)) + '\n')
sys.stdout.write(result.stdout)
sys.stderr.write(result.stderr)
sys.exit(result.returncode)
