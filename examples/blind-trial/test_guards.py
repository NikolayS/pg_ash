"""Offline checks for cleanup isolation; never starts Docker or a workload."""
import copy
import fcntl
import importlib.util
import json
import os
import shutil
import subprocess
import tempfile
import sys
from unittest import mock
from pathlib import Path
import unittest

spec = importlib.util.spec_from_file_location('trial', Path(__file__).with_name('trial.py'))
trial = importlib.util.module_from_spec(spec)
spec.loader.exec_module(trial)


class OwnershipTests(unittest.TestCase):
    def setUp(self):
        self.manifest = {'prefix': 'pgash_blind_test', 'run_id': 'unique-run'}
        self.case = {'container': 'pgash_blind_test_a', 'container_id': 'owned-id'}
        self.inspected = {'Id': 'owned-id', 'Name': '/pgash_blind_test_a',
                          'Config': {'Labels': {'pgash-blind-run': 'unique-run'}}}

    def test_owned_container_is_accepted(self):
        trial.validate_owned(self.case, self.inspected, self.manifest)

    def test_name_alone_cannot_authorize_deletion(self):
        for field, replacement in [('Id', 'other-id'), ('Name', '/other')]:
            inspected = copy.deepcopy(self.inspected)
            inspected[field] = replacement
            with self.assertRaises(RuntimeError):
                trial.validate_owned(self.case, inspected, self.manifest)

    def test_other_run_label_is_rejected(self):
        for labels in ({}, {'pgash-blind-run': 'different-run'}, None):
            inspected = copy.deepcopy(self.inspected)
            inspected['Config']['Labels'] = labels
            with self.assertRaises(RuntimeError):
                trial.validate_owned(self.case, inspected, self.manifest)

    def test_similar_prefix_is_not_owned(self):
        self.case['container'] = 'pgash_blind_testx_a'
        with self.assertRaises(RuntimeError):
            trial.validate_owned(self.case, self.inspected, self.manifest)

    def test_unscoped_prefix_is_rejected(self):
        self.manifest['prefix'] = 'postgres'
        with self.assertRaises(RuntimeError):
            trial.validate_owned(self.case, self.inspected, self.manifest)


    def test_cleanup_validates_all_before_removing_anything(self):
        other = {'container': 'pgash_blind_test_b', 'container_id': 'second-id'}
        manifest = dict(self.manifest, cases=[self.case, other])
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / 'manifest.json').write_text(json.dumps(manifest))
            with mock.patch.object(trial, 'PRIVATE', root), mock.patch.object(trial, 'sh') as shell:
                # The second resource reuses our name but has a different identity.
                shell.side_effect = [json.dumps([self.inspected]), json.dumps([self.inspected])]
                with self.assertRaises(RuntimeError):
                    trial.cleanup()
                self.assertEqual([call.args[0][1:3] for call in shell.call_args_list],
                                 [['container', 'inspect'], ['container', 'inspect']])

    def test_failed_setup_cleanup_ignores_never_created_cases(self):
        created = dict(self.case, id='a')
        manifest = dict(self.manifest, cases=[created, {'container': 'pgash_blind_test_b'}])
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / 'a').mkdir()
            (root / 'manifest.json').write_text(json.dumps(manifest))
            logs = subprocess.CompletedProcess([], 0, stdout='server output', stderr='server errors')
            with mock.patch.object(trial, 'PRIVATE', root), mock.patch.object(trial, 'sh') as shell, \
                    mock.patch.object(trial.subprocess, 'run', return_value=logs):
                shell.side_effect = [json.dumps([self.inspected]), '']
                trial.cleanup()
                self.assertEqual(shell.call_args_list[-1].args[0],
                                 ['docker', 'rm', '-f', '-v', 'owned-id'])
                self.assertEqual((root / 'a/container.stderr.log').read_text(), 'server errors')



    def test_reused_name_is_never_looked_up_or_removed(self):
        manifest = dict(self.manifest, cases=[dict(self.case, id='a')])
        absent = subprocess.CalledProcessError(1, [], stderr=
                    'Error response from daemon: No such container: owned-id')
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / 'manifest.json').write_text(json.dumps(manifest))
            with mock.patch.object(trial, 'PRIVATE', root), \
                    mock.patch.object(trial, 'sh', side_effect=absent) as shell, \
                    mock.patch.object(trial.subprocess, 'run') as logs:
                # An unrelated replacement may now own the original name.
                trial.cleanup()
                shell.assert_called_once_with(['docker', 'container', 'inspect', 'owned-id'])
                logs.assert_not_called()
                self.assertEqual(json.loads((root / 'cleanup.json').read_text())[0]['state'],
                                 'already_absent')

    def test_daemon_failure_is_not_misclassified_as_absence(self):
        failure = subprocess.CalledProcessError(1, [], stderr='Cannot connect to the Docker daemon')
        with mock.patch.object(trial, 'sh', side_effect=failure):
            with self.assertRaises(subprocess.CalledProcessError):
                trial.inspect_for_cleanup(self.case, self.manifest)

    def test_partial_cleanup_is_recorded_and_retry_removes_only_remaining_id(self):
        other = {'container': 'pgash_blind_test_b', 'container_id': 'second-id', 'id': 'b'}
        inspected_b = copy.deepcopy(self.inspected)
        inspected_b.update(Id='second-id', Name='/pgash_blind_test_b')
        manifest = dict(self.manifest, cases=[dict(self.case, id='a'), other])
        absent = subprocess.CalledProcessError(1, [], stderr=
                    'Error response from daemon: No such container: owned-id')
        logs = subprocess.CompletedProcess([], 0, stdout='logs', stderr='')
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / 'a').mkdir()
            (root / 'b').mkdir()
            (root / 'manifest.json').write_text(json.dumps(manifest))
            with mock.patch.object(trial, 'PRIVATE', root), mock.patch.object(trial, 'sh') as shell, \
                    mock.patch.object(trial.subprocess, 'run', return_value=logs):
                shell.side_effect = [json.dumps([self.inspected]), json.dumps([inspected_b]),
                                     '', RuntimeError('interrupted second removal')]
                with self.assertRaises(RuntimeError):
                    trial.cleanup()
                first = json.loads((root / 'cleanup.json').read_text())
                self.assertEqual([entry['container_id'] for entry in first], ['owned-id'])
                shell.reset_mock()
                shell.side_effect = [absent, json.dumps([inspected_b]), '']
                trial.cleanup()
                removals = [call.args[0] for call in shell.call_args_list
                            if call.args[0][1] == 'rm']
                self.assertEqual(removals, [['docker', 'rm', '-f', '-v', 'second-id']])
                self.assertEqual(len(json.loads((root / 'cleanup.json').read_text())), 2)



    def test_second_process_cannot_enter_a_locked_run(self):
        with tempfile.TemporaryDirectory() as tmp:
            with (Path(tmp) / '.controller.lock').open('a') as lock:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                result = subprocess.run([sys.executable, str(Path(trial.__file__)),
                                         'setup', '--run-dir', tmp],
                                        text=True, capture_output=True)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn('another controller owns this run directory', result.stderr)
                self.assertFalse((Path(tmp) / 'supervisor').exists())


class SeedTests(unittest.TestCase):
    def setup_manifest(self, root, seed=None):
        docs = root / 'docs'
        for name in ('README.md', 'docs/REFERENCE.md', 'examples/llm-prompt.md',
                     'examples/llm-investigation.sql'):
            path = docs / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text('frozen input')
        installer = root / 'install.sql'
        installer.write_text('select 1;')
        run = root / 'run'
        argv = ['trial.py', 'setup', '--run-dir', str(run), '--prefix',
                'pgash_blind_test', '--sql-file', str(installer), '--docs-dir', str(docs)]
        if seed is not None:
            argv += ['--seed', str(seed)]
        with mock.patch.object(sys, 'argv', argv), \
                mock.patch.object(trial.signal, 'signal'), \
                mock.patch.object(trial, 'sh', side_effect=['image-id', RuntimeError('stop before Docker')]):
            with self.assertRaisesRegex(RuntimeError, 'stop before Docker'):
                trial.main()
        return json.loads((run / 'supervisor/manifest.json').read_text())

    def test_default_seed_is_private_entropy_and_replayable(self):
        with tempfile.TemporaryDirectory() as tmp, \
                mock.patch.object(trial.secrets, 'randbits', side_effect=[123456789, 987654321]) as entropy:
            root = Path(tmp)
            first = self.setup_manifest(root / 'first')
            second = self.setup_manifest(root / 'second')
            replay = self.setup_manifest(root / 'replay', first['seed'])
            self.assertEqual(entropy.call_args_list, [mock.call(128), mock.call(128)])
            self.assertEqual([first['seed'], second['seed']], [123456789, 987654321])
            mapping = lambda m: [(c['id'], c['mechanism']) for c in m['cases']]
            self.assertNotEqual(mapping(first), mapping(second))
            self.assertEqual(mapping(first), mapping(replay))
            self.assertEqual((root / 'first/run/supervisor/manifest.json').stat().st_mode & 0o777, 0o600)

    def test_explicit_zero_seed_does_not_request_entropy(self):
        with tempfile.TemporaryDirectory() as tmp, \
                mock.patch.object(trial.secrets, 'randbits') as entropy:
            first = self.setup_manifest(Path(tmp) / 'first', 0)
            second = self.setup_manifest(Path(tmp) / 'second', 0)
            entropy.assert_not_called()
            self.assertEqual(first['seed'], 0)
            self.assertEqual([(c['id'], c['mechanism']) for c in first['cases']],
                             [(c['id'], c['mechanism']) for c in second['cases']])


class ReaderAuditTests(unittest.TestCase):
    def run_reader(self, args, sql=''):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            shutil.copyfile(Path(trial.__file__).with_name('reader.py'), root / 'reader.py')
            (root / 'connection.json').write_text(json.dumps({'port': 1, 'role': 'observer'}))
            # A process boundary verifies stdin forwarding and returned failures.
            psql = root / 'psql'
            psql.write_text('#!/usr/bin/env python3\nimport sys\n'
                            'if "missing.sql" in " ".join(sys.argv):\n'
                            '    sys.stderr.write("missing.sql: No such file\\n"); sys.exit(1)\n'
                            'sys.stdout.write(sys.stdin.read())\n')
            psql.chmod(0o700)
            env = dict(os.environ, PATH=str(root) + os.pathsep + os.environ['PATH'])
            result = subprocess.run([sys.executable, str(root / 'reader.py'), *args],
                                    input=sql, text=True, capture_output=True, env=env)
            audit = root / 'audit.jsonl'
            self.assertTrue(audit.exists(), result.stderr)
            entries = [json.loads(line) for line in audit.read_text().splitlines()]
            self.assertEqual(len(entries), 1)
            return result, entries[0]

    def test_stdin_file_forms_are_forwarded_and_audited(self):
        for args in (['-f', '-'], ['--file', '-'], ['-f-'], ['--file=-'], []):
            with self.subTest(args=args):
                result, record = self.run_reader(args, 'select 42;\n')
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(result.stdout, 'select 42;\n')
                self.assertEqual(record['stdin'], 'select 42;\n')
                self.assertEqual(record['returncode'], 0)

    def test_missing_file_failure_is_audited(self):
        for args in (['-f', 'missing.sql'], ['--file=missing.sql'], ['-fmissing.sql']):
            with self.subTest(args=args):
                result, record = self.run_reader(args)
                self.assertEqual(result.returncode, 1)
                self.assertEqual(record['returncode'], 1)
                self.assertIn('missing.sql', record['stderr'])
                self.assertIn('missing.sql', record['script_file_errors'])


if __name__ == '__main__':
    unittest.main()
