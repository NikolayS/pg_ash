#!/usr/bin/env python3
"""Run a bounded interactive fixture, preserving terminal output and status."""

import argparse
import errno
import os
import pty
import select
import signal
import subprocess
import sys
import termios
import time


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--timeout", type=float, default=120)
    parser.add_argument("--ready", help="wait for this output before sending input")
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    if not args.command or args.timeout <= 0:
        parser.error("a command and a positive timeout are required")

    source = sys.stdin.buffer.read()
    master, slave = pty.openpty()
    # Canonical terminals need VEOF; closing the master would hang up the
    # child before its final output/status could be collected.
    eof = termios.tcgetattr(slave)[6][termios.VEOF]
    source += 2 * (eof if isinstance(eof, bytes) else bytes([eof]))
    child = subprocess.Popen(args.command, stdin=slave, stdout=slave, stderr=slave,
                             start_new_session=True)
    os.close(slave)
    os.set_blocking(master, False)
    ready = not args.ready
    marker = args.ready.encode() if args.ready else b""
    recent = b""
    deadline = time.monotonic() + args.timeout
    timed_out = False
    try:
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                timed_out = True
                break
            readable, writable, _ = select.select(
                [master], [master] if ready and source else [], [], min(remaining, 0.1)
            )
            if readable:
                try:
                    output = os.read(master, 65536)
                except OSError as error:
                    if error.errno == errno.EIO:
                        break
                    if error.errno == errno.EAGAIN:
                        continue
                    raise
                if not output:
                    break
                sys.stdout.buffer.write(output)
                sys.stdout.buffer.flush()
                if not ready:
                    recent += output
                    ready = marker in recent
                    recent = recent[-len(marker):]
            if writable:
                try:
                    source = source[os.write(master, source[:4096]):]
                except OSError as error:
                    if error.errno == errno.EIO:
                        source = b""  # Child exited early; collect its status below.
                    elif error.errno != errno.EAGAIN:
                        raise
    finally:
        os.close(master)
    if not timed_out:
        try:
            return child.wait(timeout=max(0.01, deadline - time.monotonic()))
        except subprocess.TimeoutExpired:
            timed_out = True
    print(f"interactive fixture timed out after {args.timeout:g}s; "
          "output above is preserved", file=sys.stderr, flush=True)
    if child.poll() is None:
        os.killpg(child.pid, signal.SIGTERM)
    try:
        child.wait(timeout=2)
    except subprocess.TimeoutExpired:
        os.killpg(child.pid, signal.SIGKILL)
        child.wait()
    return 124


if __name__ == "__main__":
    raise SystemExit(main())
