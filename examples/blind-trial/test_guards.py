"""Offline checks for cleanup isolation; never starts Docker or a workload."""
import copy
import fcntl
import importlib.util
import json
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


if __name__ == '__main__':
    unittest.main()
