#!/usr/bin/env python3
"""Run the recorder's actual launcher in a PTY; reject startup banner noise."""
import os
from pathlib import Path
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[2]


def main():
    source = (ROOT / 'demos/bin/record-demo.sh').read_text()
    body = source.split('cat > "$LAUNCH" <<LAUNCHEOF\n', 1)[1].split('\nLAUNCHEOF', 1)[0]
    with tempfile.TemporaryDirectory(prefix='ash-startup-') as directory:
        root = Path(directory)
        splash = root / 'splash.txt'
        splash.write_text('Demo startup regression\n')
        launcher = root / 'launch.sh'
        env = {**os.environ, 'LAUNCH': str(launcher), 'SPLASH_TXT': str(splash),
               'ASH_DEMO_DIR': str(ROOT / 'demos'), 'TRANSCRIPT': str(root / 'transcript')}
        for key in ('SINCE', 'UNTIL', 'BASE_SINCE', 'BASE_UNTIL', 'STORM_SINCE', 'STORM_UNTIL'):
            env['ASH_' + key] = '2026-01-01 00:00:00+00'
        env['ASH_STORM_EVENT'] = 'Lock:tuple'
        subprocess.run(['bash', '-c', 'cat > "$LAUNCH" <<LAUNCHEOF\n' + body + '\nLAUNCHEOF\n'],
                       env=env, check=True)
        result = subprocess.run(
            ['python3', str(ROOT / 'devel/scripts/run_with_pty.py'),
             '--timeout', '10', '--ready', 'ashrdy-', 'sh', str(launcher)],
            env=env, input="select 101 + 202 as visible_result;\nselect 1 / 0;\n\\q\n",
            text=True, capture_output=True, timeout=15)
        output = result.stdout
        assert result.returncode == 0, output + result.stderr
        assert 'Demo startup regression' in output and 'ashrdy-' in output, output
        assert 'psql (' not in output and 'SSL connection' not in output, output
        assert '303' in output and 'division by zero' in output, output
        print('PASS launcher: no banner; prompt, query results, and errors remain visible')

if __name__ == '__main__':
    main()
