#!/usr/bin/env python3
"""Discover pg_ash SQL install and upgrade chains for CI."""

from __future__ import annotations

import argparse
import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
SQL_DIRS = (ROOT / "sql", ROOT / "devel" / "sql")
INSTALL_RE = re.compile(r"ash-(\d+)\.(\d+)\.sql$")
UPGRADE_RE = re.compile(r"ash-(\d+\.\d+)-to-(\d+\.\d+)\.sql$")


def version_key(version: str) -> tuple[int, int]:
    major, minor = version.split(".", 1)
    return int(major), int(minor)


def rel(path: Path) -> str:
    return path.relative_to(ROOT).as_posix()


def installers() -> dict[str, Path]:
    found: dict[str, Path] = {}
    for path in (ROOT / "sql").glob("ash-*.sql"):
        match = INSTALL_RE.fullmatch(path.name)
        if match:
            version = f"{match.group(1)}.{match.group(2)}"
            found[version] = path
    if not found:
        raise SystemExit("no released ash-X.Y.sql installer found")
    return found


def upgrades(
    *, include_devel: bool = True, required: bool = True
) -> dict[str, tuple[str, Path]]:
    found: dict[str, tuple[str, Path]] = {}
    directories = SQL_DIRS if include_devel else (ROOT / "sql",)
    for directory in directories:
        if not directory.exists():
            continue
        for path in directory.glob("ash-*-to-*.sql"):
            match = UPGRADE_RE.fullmatch(path.name)
            if not match:
                continue
            src, dst = match.group(1), match.group(2)
            if src in found:
                prev = rel(found[src][1])
                raise SystemExit(f"duplicate upgrade from {src}: {prev}, {rel(path)}")
            found[src] = (dst, path)
    if required and not found:
        raise SystemExit("no ash-X.Y-to-A.B.sql upgrade scripts found")
    return found


def oldest_version() -> str:
    return min(installers(), key=version_key)


def second_oldest_version() -> str:
    versions = sorted(installers(), key=version_key)
    if len(versions) < 2:
        raise SystemExit("need at least two released installers")
    return versions[1]


def latest_released_version() -> str:
    versions = set(installers())
    for src, (dst, _path) in upgrades(include_devel=False, required=False).items():
        versions.add(src)
        versions.add(dst)
    return max(versions, key=version_key)


def fresh_install_path() -> str:
    dev_install = ROOT / "devel" / "sql" / "ash-install.sql"
    if dev_install.exists():
        return rel(dev_install)
    return rel(ROOT / "sql" / "ash-install.sql")


def emit_psql_include(path: Path) -> None:
    print(rf"\i {rel(path)}")


def emit_upgrade_chain(start: str) -> None:
    current = start
    seen: set[str] = set()
    by_source = upgrades()
    while current in by_source:
        if current in seen:
            raise SystemExit(f"cycle in upgrade chain at {current}")
        seen.add(current)
        nxt, path = by_source[current]
        emit_psql_include(path)
        current = nxt


def emit_full_upgrade_chain(start: str) -> None:
    install = installers().get(start)
    if install is None:
        raise SystemExit(f"no released installer for {start}")
    emit_psql_include(install)
    emit_upgrade_chain(start)


def emit_reapply_chain() -> None:
    by_source = upgrades()
    current = second_oldest_version()
    seen: set[str] = set()
    while current in by_source:
        if current in seen:
            raise SystemExit(f"cycle in reapply chain at {current}")
        seen.add(current)
        nxt, path = by_source[current]
        emit_psql_include(path)
        current = nxt


def main() -> None:
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("fresh-install-path")
    sub.add_parser("oldest-install-path")
    sub.add_parser("second-oldest-install-path")
    sub.add_parser("latest-released-version")
    sub.add_parser("upgrade-chain-from-oldest")
    sub.add_parser("upgrade-chain-from-second-oldest")
    sub.add_parser("full-upgrade-chain")
    sub.add_parser("reapply-chain")
    args = parser.parse_args()

    if args.command == "fresh-install-path":
        print(fresh_install_path())
    elif args.command == "oldest-install-path":
        print(rel(installers()[oldest_version()]))
    elif args.command == "second-oldest-install-path":
        print(rel(installers()[second_oldest_version()]))
    elif args.command == "latest-released-version":
        print(latest_released_version())
    elif args.command == "upgrade-chain-from-oldest":
        emit_upgrade_chain(oldest_version())
    elif args.command == "upgrade-chain-from-second-oldest":
        emit_upgrade_chain(second_oldest_version())
    elif args.command == "full-upgrade-chain":
        emit_full_upgrade_chain(oldest_version())
    elif args.command == "reapply-chain":
        emit_reapply_chain()


if __name__ == "__main__":
    main()
