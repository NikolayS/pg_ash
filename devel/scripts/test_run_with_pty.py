#!/usr/bin/env python3
"""Prove the interactive test runner supplies a TTY and propagates failures."""

from pathlib import Path
import subprocess
import sys
import unittest


class PtyRunnerTests(unittest.TestCase):
    def test_terminal_input_and_exit_status(self):
        runner = Path(__file__).with_name("run_with_pty.py")
        for code in (0, 7):
            with self.subTest(code=code):
                result = subprocess.run(
                    [sys.executable, str(runner), sys.executable, "-c",
                     "import sys; assert sys.stdin.isatty(); "
                     "assert input() == 'probe'; print('TTY_OK'); "
                     f"raise SystemExit({code})"],
                    input=b"probe\n", capture_output=True, timeout=10,
                )
                self.assertEqual(result.returncode, code, result.stderr)
                self.assertIn(b"TTY_OK", result.stdout)


if __name__ == "__main__":
    unittest.main()
