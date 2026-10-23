#!/usr/bin/env python3
"""Exercise the public no-database gate with stale committed demo artifacts."""
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]


class AssetFreshnessTests(unittest.TestCase):
    def test_committed_artifacts_and_sources(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for name in ('demos', 'assets', 'devel/sql'):
                shutil.copytree(ROOT / name, root / name,
                                ignore=shutil.ignore_patterns('out', '__pycache__'))
            env = dict(os.environ, ASH_SVG_ONLY='1')

            def check():
                return subprocess.run(['make', '-C', str(root / 'demos'), 'check'],
                                      env=env, stdout=subprocess.PIPE,
                                      stderr=subprocess.STDOUT, text=True)

            clean = check()
            self.assertEqual(clean.returncode, 0, clean.stdout)
            for name in ('assets/chart.svg', 'assets/status.png',
                         'assets/ash_demo.gif', 'demos/scenes/scenes.tsv',
                         'devel/sql/ash-install.sql'):
                with self.subTest(path=name):
                    path = root / name
                    original = path.read_bytes()
                    path.write_bytes(original + b'\nSTALE-ARTIFACT-REGRESSION\n')
                    try:
                        result = check()
                        self.assertNotEqual(result.returncode, 0,
                                            'make check accepted changed ' + name)
                    finally:
                        path.write_bytes(original)


if __name__ == '__main__':
    unittest.main()
