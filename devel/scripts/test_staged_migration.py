#!/usr/bin/env python3
"""The current prerelease migration is replaceable without rewriting releases."""
import contextlib
import io
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import ash_sql_chain as chain


def installer(path, version):
    path.write_text(
        f"version text not null default '{version}'\n"
        f"update ash.config set version = '{version}' where singleton;\n"
        f"alter table ash.config alter column version set default '{version}';\n"
    )


class StagedMigrationTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.released = self.root / 'sql/migrations'
        self.dev = self.root / 'devel/sql'
        self.released.mkdir(parents=True)
        self.dev.mkdir(parents=True)
        (self.root / 'sql/ash-1.0.sql').write_text('-- historical installer')
        for name in ('ash-1.0-to-1.5.sql', 'ash-1.5-to-2.0.sql'):
            (self.released / name).write_text('-- released migration')
            (self.root / 'sql' / name).write_text('\\ir migrations/' + name)
        installer(self.root / 'sql/ash-install.sql', '2.0-beta1')
        installer(self.dev / 'ash-install.sql', '2.0-beta1')
        (self.dev / 'ash-1.5-to-2.0.sql').write_text('\\ir ash-install.sql')
        for name, value in [('ROOT', self.root),
                            ('UPGRADE_DIRS', (self.released, self.dev))]:
            patcher = patch.object(chain, name, value)
            patcher.start()
            self.addCleanup(patcher.stop)

    def output(self, fn, *args):
        result = io.StringIO()
        with contextlib.redirect_stdout(result):
            fn(*args)
        return result.getvalue().splitlines()

    def test_staged_current_line_drives_upgrade_and_reapply(self):
        self.assertEqual(self.output(chain.emit_full_upgrade_chain, '1.0'), [
            r'\i sql/ash-1.0.sql',
            r'\i sql/migrations/ash-1.0-to-1.5.sql',
            r'\i devel/sql/ash-1.5-to-2.0.sql',
        ])
        self.assertEqual(self.output(chain.emit_pinned_upgrade_chain, '1.5'),
                         [r'\i devel/sql/ash-1.5-to-2.0.sql'])
        self.assertEqual(self.output(chain.emit_reapply_chain),
                         [r'\i devel/sql/ash-1.5-to-2.0.sql'])

    def test_finalized_migration_cannot_be_overridden(self):
        installer(self.root / 'sql/ash-install.sql', '2.0')
        with self.assertRaisesRegex(SystemExit, 'duplicate upgrade'):
            chain.upgrade_chain_paths('1.0')

    def test_older_migration_cannot_be_overridden(self):
        (self.dev / 'ash-1.0-to-1.5.sql').write_text('-- forbidden')
        with self.assertRaisesRegex(SystemExit, 'duplicate upgrade'):
            chain.upgrade_chain_paths('1.0')

    def test_override_cannot_target_another_release_line(self):
        installer(self.dev / 'ash-install.sql', '2.1-alpha1')
        with self.assertRaisesRegex(SystemExit, 'duplicate upgrade'):
            chain.upgrade_chain_paths('1.0')

    def test_promoted_rc_returns_to_public_wrapper_and_baseline_overlay(self):
        (self.dev / 'ash-1.5-to-2.0.sql').unlink()
        installer(self.root / 'sql/ash-install.sql', '2.0-rc1')
        installer(self.dev / 'ash-install.sql', '2.0-rc1')
        self.assertEqual(self.output(chain.emit_pinned_upgrade_chain, '1.5'), [
            r'\i sql/ash-1.5-to-2.0.sql', r'\i devel/sql/ash-install.sql'])
        self.assertEqual(self.output(chain.emit_reapply_chain),
                         [r'\i devel/sql/ash-install.sql'])


if __name__ == '__main__':
    unittest.main()
