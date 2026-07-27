#!/usr/bin/env python3
"""Discover pg_ash SQL install and upgrade chains for CI."""

from __future__ import annotations

import argparse
import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
UPGRADE_DIRS = (ROOT / "sql" / "migrations", ROOT / "devel" / "sql")
INSTALL_RE = re.compile(r"ash-(\d+)\.(\d+)\.sql$")
UPGRADE_RE = re.compile(r"ash-(\d+\.\d+)-to-(\d+\.\d+)\.sql$")
VERSION_DEFAULT_RE = re.compile(
    r"^\s*version\s+text\s+not\s+null\s+default\s+'([^']+)'",
    re.IGNORECASE | re.MULTILINE,
)
VERSION_UPDATE_RE = re.compile(
    r"^\s*update\s+ash\.config\s+set\s+version\s*=\s*'([^']+)'",
    re.IGNORECASE | re.MULTILINE,
)
VERSION_ALTER_DEFAULT_RE = re.compile(
    r"^\s*alter\s+table\s+ash\.config\s+alter\s+column\s+version\s+"
    r"set\s+default\s+'([^']+)'",
    re.IGNORECASE | re.MULTILINE,
)


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
    directories = UPGRADE_DIRS if include_devel else (ROOT / "sql" / "migrations",)
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
    dev_install = development_install_path()
    if dev_install.exists():
        return rel(dev_install)
    return rel(ROOT / "sql" / "ash-install.sql")


def development_install_path() -> Path:
    return ROOT / "devel" / "sql" / "ash-install.sql"


def install_version(path: Path) -> str:
    text = path.read_text()
    stamp_patterns = {
        "column default": VERSION_DEFAULT_RE,
        "singleton update": VERSION_UPDATE_RE,
        "altered default": VERSION_ALTER_DEFAULT_RE,
    }
    stamps: dict[str, str] = {}
    for label, pattern in stamp_patterns.items():
        matches = pattern.findall(text)
        if len(matches) != 1:
            raise SystemExit(
                f"expected one ash.config {label} version stamp in "
                f"{path.as_posix()}, found {len(matches)}"
            )
        stamps[label] = matches[0]

    versions = set(stamps.values())
    if len(versions) != 1:
        details = ", ".join(
            f"{label}={version!r}" for label, version in stamps.items()
        )
        raise SystemExit(
            f"inconsistent ash.config version stamps in {path.as_posix()}: "
            f"{details}"
        )
    return versions.pop()


def fresh_install_version() -> str:
    return install_version(ROOT / fresh_install_path())


def emit_psql_include(path: Path) -> None:
    print(rf"\i {rel(path)}")


def emit_development_overlay(*, development_migration_seen: bool) -> None:
    dev_install = development_install_path()
    if dev_install.exists() and not development_migration_seen:
        emit_psql_include(dev_install)


def emit_upgrade_chain(start: str) -> None:
    current = start
    seen: set[str] = set()
    by_source = upgrades()
    development_migration_seen = False
    while current in by_source:
        if current in seen:
            raise SystemExit(f"cycle in upgrade chain at {current}")
        seen.add(current)
        nxt, path = by_source[current]
        emit_psql_include(path)
        if path.parent == ROOT / "devel" / "sql":
            development_migration_seen = True
        current = nxt
    emit_development_overlay(
        development_migration_seen=development_migration_seen
    )


def emit_full_upgrade_chain(start: str) -> None:
    install = installers().get(start)
    if install is None:
        raise SystemExit(f"no released installer for {start}")
    emit_psql_include(install)
    emit_upgrade_chain(start)


def emit_reapply_chain() -> None:
    # Only the in-progress development upgrade script(s) — latest released
    # version up to the dev head — are guaranteed re-apply-safe (lockstep
    # policy). Finalized legacy scripts are immutable and idempotent only on
    # the version just below: once a later release removes the surface they
    # recreate (e.g. 2.0 drops the 1.x readers and ash._to_sample_ts),
    # re-applying them on a current install fails by design.
    by_source = upgrades()
    current = latest_released_version()
    seen: set[str] = set()
    development_migration_seen = False
    while current in by_source:
        if current in seen:
            raise SystemExit(f"cycle in reapply chain at {current}")
        seen.add(current)
        nxt, path = by_source[current]
        emit_psql_include(path)
        if path.parent == ROOT / "devel" / "sql":
            development_migration_seen = True
        current = nxt
    emit_development_overlay(
        development_migration_seen=development_migration_seen
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("fresh-install-path")
    sub.add_parser("fresh-install-version")
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
    elif args.command == "fresh-install-version":
        print(fresh_install_version())
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
