#!/usr/bin/env python3
"""Exercise gate step execution and cron-helper stdin without Docker."""
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest

SCRIPTS = Path(__file__).resolve().parent


class GateExecutionTests(unittest.TestCase):
    def run_steps(self, bodies):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            scripts = root / 'devel/scripts'
            scripts.mkdir(parents=True)
            shutil.copy2(SCRIPTS / 'ci_step_script.py', scripts)
            # Run the actual gate functions, replacing only the CLI entry point.
            source = (SCRIPTS / 'release_gate.sh').read_text()
            entry = '\nmain "$@"\n'
            self.assertTrue(source.endswith(entry))
            script = scripts / 'release_gate.sh'
            script.write_text(source[:-len(entry)] + '\n'
                              'surface_temp_dir="$TEST_DIR"\n'
                              'run_ci_selection --workflow "$TEST_WORKFLOW" range First Last\n')
            workflow = root / 'workflow.yml'
            workflow.write_text('jobs:\n  test:\n    steps:\n' + ''.join(
                '      - name: ' + name + '\n        run: |\n' + ''.join(
                    '          ' + line + '\n' for line in body.splitlines())
                for name, body in bodies))
            marker = root / 'executed'
            env = dict(os.environ, TEST_DIR=str(root), TEST_WORKFLOW=str(workflow),
                       TEST_MARKER=str(marker))
            result = subprocess.run(['bash', str(script)], env=env, text=True,
                                    capture_output=True, timeout=15)
            return result, marker.read_text().splitlines() if marker.exists() else []

    def test_stdin_reader_cannot_skip_following_steps(self):
        result, executed = self.run_steps([
            ('First', 'cat >/dev/null\necho first >>"$TEST_MARKER"'),
            ('Middle', 'echo middle >>"$TEST_MARKER"'),
            ('Last', 'echo last >>"$TEST_MARKER"'),
        ])
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(executed, ['first', 'middle', 'last'])

    def test_incomplete_execution_is_an_error(self):
        result, executed = self.run_steps([
            ('First', ': >"$TEST_DIR/ci-selection.bin"\necho first >>"$TEST_MARKER"'),
            ('Last', 'echo last >>"$TEST_MARKER"'),
        ])
        self.assertEqual(executed, ['first'])
        self.assertNotEqual(result.returncode, 0, 'truncated selection reported success')
        self.assertIn('executed 1 of 2 selected steps', result.stderr)

    def test_failing_step_stops_execution(self):
        result, executed = self.run_steps([
            ('First', 'echo first >>"$TEST_MARKER"\nexit 7'),
            ('Last', 'echo last >>"$TEST_MARKER"'),
        ])
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(executed, ['first'])
        self.assertIn('canonical CI step failed: First', result.stderr)


class CronHelperInputTests(unittest.TestCase):
    def test_only_explicit_sql_reaches_docker_stdin(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            docker = root / 'docker'
            docker.write_text('''#!/usr/bin/env python3
import json, os, sys
from pathlib import Path
args = sys.argv[1:]
source = sys.stdin.read() if '-i' in args else ''
with Path(os.environ['DOCKER_TRACE']).open('a') as out:
    out.write(json.dumps({'args': args, 'source': source}) + '\\n')
if 'mktemp' in args:
    print('/tmp/fixture-private-cluster')
elif 'psql' in args:
    # Stop before any real cluster or product SQL is needed.
    sys.exit(17)
''')
            docker.chmod(0o755)
            trace = root / 'trace.jsonl'
            env = dict(os.environ, PATH=str(root) + os.pathsep + os.environ['PATH'],
                       CONTAINER_ID='owned-fixture', DOCKER_TRACE=str(trace))
            result = subprocess.run([
                'python3', str(SCRIPTS / 'test_owner_cron_settings.py'),
                '--installer', str(root / 'unused.sql')],
                env=env, input='following gate selections must not be consumed\n',
                text=True, capture_output=True, timeout=15)
            self.assertNotEqual(result.returncode, 0)
            calls = [json.loads(line) for line in trace.read_text().splitlines()]
            sql_calls = [call for call in calls if 'psql' in call['args']]
            self.assertEqual(len(sql_calls), 1, calls)
            self.assertEqual(sql_calls[0]['source'],
                             'create extension pg_cron; create extension pg_stat_statements;')
            for call in calls:
                if 'psql' not in call['args']:
                    self.assertEqual(call['source'], '', call)


if __name__ == '__main__':
    unittest.main()
