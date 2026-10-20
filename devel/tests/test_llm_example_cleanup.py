#!/usr/bin/env python3
"""Live failure cleanup regression; needs an owned disposable pg_ash database."""
import os
import subprocess
import sys
import time
import unittest
from unittest.mock import patch

import llm_example_live as example


class CleanupTests(unittest.TestCase):
    def test_failed_readiness_releases_server_transactions(self):
        original = example.wait_for_count
        communicate = subprocess.Popen.communicate
        for failure_at, inject_timeout in ((1, False), (3, False), (1, True)):
            with self.subTest(failure_at=failure_at, inject_timeout=inject_timeout):
                timed_out = False

                def communicate_with_timeout(process, *args, **kwargs):
                    nonlocal timed_out
                    if inject_timeout and not timed_out and any(
                        str(arg).startswith("set application_name = 'pgash_llm_blocker_")
                        for arg in process.args
                    ):
                        timed_out = True
                        raise subprocess.TimeoutExpired(process.args, 5)
                    return communicate(process, *args, **kwargs)

                def fail_after_ready(application, expected):
                    original(application, expected)
                    if expected == failure_at:
                        raise RuntimeError('injected readiness failure')

                # Bound the broken implementation too: a disconnected client
                # can leave pg_sleep holding a lock until this server timeout.
                options = os.environ.get('PGOPTIONS', '') + ' -c statement_timeout=8000'
                start = time.monotonic()
                with patch.dict(os.environ, PGOPTIONS=options), \
                        patch.object(sys, 'argv', ['llm_example_live.py']), \
                        patch.object(subprocess.Popen, 'communicate', communicate_with_timeout), \
                        patch.object(example, 'wait_for_count', fail_after_ready):
                    with self.assertRaisesRegex(RuntimeError, 'injected readiness failure'):
                        example.main()
                self.assertEqual(timed_out, inject_timeout)
                elapsed = time.monotonic() - start
                self.assertEqual(example.sql(
                    "select count(*) from pg_stat_activity "
                    "where datname=current_database() "
                    "and application_name like 'pgash_llm_%'"), '0')
                self.assertEqual(example.sql(
                    "select to_regclass('public.pgash_llm_demo_orders')"), '')
                self.assertLess(elapsed, 5, 'cleanup waited for the server statement timeout')


if __name__ == '__main__':
    unittest.main()
