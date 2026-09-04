#!/usr/bin/env python3
"""Fail closed on missing mandatory CI evidence and select live demo inputs."""

import argparse
import json
import os
import sys


def require_jobs(needs: object) -> None:
    """Both mandatory jobs must exist and have completed successfully."""
    if not isinstance(needs, dict):
        raise ValueError("required-job evidence must be an object")
    for name in ("docs-lint", "test"):
        job = needs.get(name)
        if not isinstance(job, dict) or job.get("result") != "success":
            raise ValueError(f"mandatory job {name!r} did not succeed: {job!r}")


def demo_needed(event: str, paths: list[str]) -> bool:
    """Changes to the candidate SQL and its consumers need a fresh capture."""
    if event in ("schedule", "workflow_dispatch"):
        return True
    if event not in ("push", "pull_request"):
        raise ValueError(f"unexpected demo event: {event!r}")
    prefixes = ("devel/", "demos/", "sql/", "examples/", "assets/")
    exact = {"README.md", ".github/workflows/demo.yml"}
    return any(path in exact or path.startswith(prefixes) for path in paths)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("required-jobs", "demo-needed"))
    parser.add_argument("--event", default=os.environ.get("GITHUB_EVENT_NAME", ""))
    args = parser.parse_args()
    try:
        if args.command == "required-jobs":
            require_jobs(json.loads(os.environ.get("NEEDS_JSON", "null")))
            print("All mandatory jobs succeeded")
        else:
            paths = sys.stdin.read().splitlines()
            print("yes" if demo_needed(args.event, paths) else "no")
    except (ValueError, TypeError) as error:
        print(f"CI evidence rejected: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
