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
script_files = {args[i + 1]: Path(args[i + 1]).read_text()
                for i in range(len(args) - 1) if args[i] in ('-f', '--file')}
stdin = None if (any(a in ('-c', '--command', '-f', '--file')
                     or a.startswith(('-c', '--command=', '-f', '--file=')) for a in args)
                 or sys.stdin.isatty()) else sys.stdin.read()
for arg in args:
    if arg.startswith('--file='):
        script_files[arg[7:]] = Path(arg[7:]).read_text()
    elif arg.startswith('-f') and len(arg) > 2:
        script_files[arg[2:]] = Path(arg[2:]).read_text()
started = datetime.datetime.now(datetime.timezone.utc).isoformat()
anchor = time.monotonic()
result = subprocess.run(['psql', '-X', '-v', 'ON_ERROR_STOP=1'] + args,
                        input=stdin, text=True, capture_output=True)
with (root / 'audit.jsonl').open('a') as out:
    out.write(json.dumps(dict(started=started,
              ended=datetime.datetime.now(datetime.timezone.utc).isoformat(),
              elapsed_seconds=time.monotonic() - anchor, args=args, stdin=stdin,
              script_files=script_files, stdout=result.stdout,
              stderr=result.stderr, returncode=result.returncode)) + '\n')
sys.stdout.write(result.stdout)
sys.stderr.write(result.stderr)
sys.exit(result.returncode)
