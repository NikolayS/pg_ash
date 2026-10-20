#!/usr/bin/env python3
"""Adversarial client-output checks; no database required."""
import contextlib
import io
from pathlib import Path
import runpy
import subprocess
import unittest
from unittest.mock import patch

CHECKER = Path(__file__).with_name('test_partial_rollup_source.py')
NOTICE = 'NOTICE:  01000: pg_ash partial source: newer raw observations are omitted\n'
READERS = ('aas', 'timeline', 'top', 'periods', 'chart')


def transcript(missing=None, negative=False):
    result = ''.join('partial-reader-' + name + '\n' + ('' if name == missing else NOTICE)
                     for name in READERS)
    # This additional aas call checks values, not the preceding chart's NOTICE.
    result += 'partial-source-value-check\n' + NOTICE + 'DO\n'
    result += 'partial-source-null-watermark\n' + NOTICE
    result += 'partial-source-negative\n' + (NOTICE if negative else '') + 'ROLLBACK\n'
    return result


def check(output, code=0):
    with patch('subprocess.run', return_value=subprocess.CompletedProcess([], code, stdout=output)), \
            contextlib.redirect_stdout(io.StringIO()):
        runpy.run_path(str(CHECKER), run_name='__main__')


class PartialReaderCheckerTests(unittest.TestCase):
    def test_complete_transcript(self):
        check(transcript())

    def test_each_reader_must_emit_its_own_notice(self):
        for reader in READERS:
            with self.subTest(reader=reader):
                with self.assertRaisesRegex(AssertionError, f'partial-reader-{reader}: missing diagnostic'):
                    check(transcript(missing=reader))

    def test_negative_section_must_remain_silent(self):
        with self.assertRaisesRegex(AssertionError, 'false positive'):
            check(transcript(negative=True))

    def test_sql_failure_cannot_pass(self):
        with self.assertRaisesRegex(AssertionError, 'SQL assertions failed'):
            check(transcript(), code=1)


if __name__ == '__main__':
    unittest.main()
