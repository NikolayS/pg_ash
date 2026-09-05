#!/usr/bin/env python3
"""Exercise native terminal setup, EOF, backpressure, status, and time bounds."""

from pathlib import Path
import subprocess
import sys
import unittest

RUNNER = Path(__file__).with_name("run_with_pty.py")


def run_child(code: str, source: bytes = b"", timeout: str = "5", ready=None):
    return subprocess.run(
        [sys.executable, str(RUNNER), "--timeout", timeout,
         *(["--ready", ready] if ready else []), sys.executable, "-c", code],
        input=source, capture_output=True, timeout=10,
    )


class PtyRunnerTests(unittest.TestCase):
    def test_terminal_input_and_exit_status(self):
        for code in (0, 7):
            with self.subTest(code=code):
                result = run_child(
                    "import sys; assert sys.stdin.isatty(); "
                    "assert input() == 'probe'; print('TTY_OK'); "
                    f"raise SystemExit({code})", b"probe\n"
                )
                self.assertEqual(result.returncode, code, result.stderr)
                self.assertIn(b"TTY_OK", result.stdout)

    def test_readiness_survives_terminal_initialization_flush(self):
        result = run_child(
            "import sys, termios, time; time.sleep(.1); "
            "termios.tcflush(sys.stdin.fileno(), termios.TCIFLUSH); "
            "print('READY>', flush=True); assert input() == 'probe'; "
            "print('READY_OK')", b"probe\n", ready="READY>"
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn(b"READY_OK", result.stdout)

    def test_eof_ends_a_reader_without_an_explicit_exit_command(self):
        for source in (b"probe\n", b"probe"):
            with self.subTest(source=source):
                result = run_child(
                    "import sys; print('EOF_OK', len(sys.stdin.read()))", source
                )
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertIn(f"EOF_OK {len(source)}".encode(), result.stdout)

    def test_large_chatty_input_does_not_deadlock(self):
        result = run_child(
            "import sys; n = 0\n"
            "for line in sys.stdin:\n"
            " n += 1; print(line, end='', flush=True)\n"
            "print('ALL_INPUT', n)", (b"x" * 100 + b"\n") * 2000
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn(b"ALL_INPUT 2000", result.stdout)

    def test_early_child_failure_preserves_status(self):
        result = run_child("raise SystemExit(9)", b"unused\n" * 10000)
        self.assertEqual(result.returncode, 9, result.stderr)
        self.assertNotIn(b"Traceback", result.stderr)

    def test_timeout_is_bounded_and_keeps_output(self):
        result = run_child("import time; print('BEFORE_WAIT', flush=True); "
                           "time.sleep(30)", timeout="0.3")
        self.assertEqual(result.returncode, 124, result.stderr)
        self.assertIn(b"BEFORE_WAIT", result.stdout)
        self.assertIn(b"timed out", result.stderr)


if __name__ == "__main__":
    unittest.main()
