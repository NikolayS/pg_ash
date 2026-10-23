#!/usr/bin/env python3
"""Exercise the public no-database gate with stale committed demo artifacts."""
import json
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

            def assert_changed(name):
                result = check()
                self.assertNotEqual(result.returncode, 0,
                                    'make check accepted changed ' + name)
                self.assertIn('changed since the reviewed capture', result.stdout)
                self.assertIn(name, result.stdout)

            clean = check()
            self.assertEqual(clean.returncode, 0, clean.stdout)
            for name in ('assets/chart.svg', 'assets/status.png',
                         'assets/ash_demo.gif', 'demos/scenes/scenes.tsv',
                         'devel/sql/ash-install.sql', 'demos/Makefile',
                         'demos/bin/check-fixtures.sh',
                         'demos/bin/asset-freshness.py'):
                with self.subTest(path=name):
                    path = root / name
                    original = path.read_bytes()
                    # Keep shell/Python/Makefile syntax valid. Failure must
                    # come from freshness validation, not a parser error.
                    path.write_bytes(original + b'\n# freshness mutation\n')
                    try:
                        assert_changed(name)
                    finally:
                        path.write_bytes(original)

            name = 'demos/lib/freshness-probe.txt'
            with self.subTest(added=name):
                path = root / name
                path.write_text('new capture input\n')
                try:
                    assert_changed(name)
                finally:
                    path.unlink()

            name = 'demos/fixtures/chart.ansi'
            with self.subTest(deleted=name):
                path = root / name
                original = path.read_bytes()
                path.unlink()
                try:
                    assert_changed(name)
                finally:
                    path.write_bytes(original)

            manifest = root / 'demos/fixtures/assets.json'
            original = manifest.read_bytes()
            with self.subTest(missing_manifest=True):
                manifest.unlink()
                try:
                    result = check()
                    self.assertNotEqual(result.returncode, 0)
                    self.assertIn('Demo asset manifest missing', result.stdout)
                finally:
                    manifest.write_bytes(original)
            with self.subTest(unsupported_format=True):
                data = json.loads(original)
                data['format'] = 2
                manifest.write_text(json.dumps(data))
                try:
                    result = check()
                    self.assertNotEqual(result.returncode, 0)
                    self.assertIn('Unsupported demo asset manifest format', result.stdout)
                finally:
                    manifest.write_bytes(original)


if __name__ == '__main__':
    unittest.main()
