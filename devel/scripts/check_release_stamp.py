#!/usr/bin/env python3
"""Check a release tag against the discovered pg_ash payload stamp."""

from __future__ import annotations

import argparse
import re
from pathlib import Path

import ash_sql_chain


RELEASE_TAG_RE = re.compile(
    r"v(?:0|[1-9][0-9]*)"
    r"\.(?:0|[1-9][0-9]*)"
    r"(?:-(?:alpha|beta|rc)[1-9][0-9]*)?$"
)


def check_release_stamp(tag: str, payload: Path) -> str:
    if not tag.startswith("v"):
        raise ValueError(f"release tag {tag!r} must start with 'v'")
    expected_version = tag[1:]
    if not expected_version:
        raise ValueError("release tag 'v' does not contain a version")

    payload_version = ash_sql_chain.install_version(payload)
    if payload_version != expected_version:
        raise ValueError(
            f"release tag {tag!r} names version {expected_version!r}, but "
            f"{payload.as_posix()} stamps ash.config.version="
            f"{payload_version!r}"
        )

    if not RELEASE_TAG_RE.fullmatch(tag):
        raise ValueError(
            f"release tag {tag!r} must use vX.Y or "
            "vX.Y-{alpha,beta,rc}N"
        )

    return (
        f"release stamp OK: {tag} matches {payload.as_posix()} "
        f"(ash.config.version={payload_version})"
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Verify that a git release tag matches ash.config.version."
    )
    parser.add_argument("--tag", required=True, help="git tag, including leading v")
    parser.add_argument(
        "--payload",
        type=Path,
        help="installer to inspect (defaults to sql/ash-install.sql)",
    )
    args = parser.parse_args()

    payload = args.payload or ash_sql_chain.ROOT / "sql" / "ash-install.sql"
    try:
        result = check_release_stamp(args.tag, payload)
    except (OSError, ValueError) as error:
        raise SystemExit(f"release stamp mismatch: {error}") from error
    print(result)


if __name__ == "__main__":
    main()
