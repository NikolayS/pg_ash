#!/usr/bin/env python3
"""Run a finite input script on a pseudo-terminal, preserving command status."""

import errno
import os
import pty
import subprocess
import sys


if __name__ == "__main__":
    if len(sys.argv) < 2:
        raise SystemExit("usage: run_with_pty.py command [argument ...]")
    # Fixture input is finite and ends with the command's explicit exit command.
    # Avoid pty.spawn: its stdin relay can hang on a pipe with BSD terminals.
    source = sys.stdin.buffer.read()
    master, slave = pty.openpty()
    child = subprocess.Popen(sys.argv[1:], stdin=slave, stdout=slave, stderr=slave)
    os.close(slave)
    try:
        while source:
            source = source[os.write(master, source):]
        while True:
            try:
                output = os.read(master, 65536)
            except OSError as error:
                if error.errno == errno.EIO:
                    break
                raise
            if not output:
                break
            sys.stdout.buffer.write(output)
            sys.stdout.buffer.flush()
    finally:
        os.close(master)
    raise SystemExit(child.wait())
