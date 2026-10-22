#!/usr/bin/env python3
"""Exercise origin selection and the database-free help contract."""
import importlib.util
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / 'devel/tests/promoted_upgrade_paths.py'
spec = importlib.util.spec_from_file_location('promoted', SCRIPT)
promoted = importlib.util.module_from_spec(spec)
spec.loader.exec_module(promoted)


class PromotedCliTests(unittest.TestCase):
    def test_help_does_not_invoke_psql(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            marker = root / 'called'
            fake = root / 'psql'
            fake.write_text('#!/bin/sh\ntouch "$PSQL_MARKER"\nexit 97\n')
            fake.chmod(0o755)
            result = subprocess.run(['python3', str(SCRIPT), '--help'],
                env={**os.environ, 'PATH': str(root) + ':' + os.environ['PATH'],
                     'PSQL_MARKER': str(marker)}, capture_output=True, text=True)
            self.assertFalse(marker.exists(), 'help invoked psql')
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn('usage:', result.stdout)

    def test_alpha_does_not_fall_back_to_stable_payload(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / 'sql').mkdir()
            (root / 'sql/ash-install.sql').write_text('-- stable baseline')
            with self.assertRaisesRegex(RuntimeError, 'alpha.*installer'):
                promoted.tagged_installer(root, 'v2.0-alpha1')

    def test_actual_tagged_payloads(self):
        for tag in ('v1.0', 'v1.5', 'v2.0-alpha1', 'v2.0-alpha5',
                    'v2.0-beta1', 'v2.0-rc1'):
            with self.subTest(tag=tag), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                archive = root / 'tag.tar'
                subprocess.run(['git', 'archive', '-o', str(archive), tag],
                               cwd=ROOT, check=True)
                subprocess.run(['tar', '-xf', str(archive), '-C', str(root)], check=True)
                chosen = promoted.tagged_installer(root, tag)
                if '-alpha' in tag:
                    self.assertEqual(chosen, root / 'devel/sql/ash-install.sql')
                    self.assertNotEqual(chosen.read_bytes(),
                                        (root / 'sql/ash-install.sql').read_bytes())
                else:
                    self.assertEqual(chosen.parent, root / 'sql')
                self.assertTrue(chosen.is_file())


if __name__ == '__main__':
    unittest.main()
