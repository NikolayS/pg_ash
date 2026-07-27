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


def trace_upgrade_chain(
    start: str,
    by_source: dict[str, tuple[str, Path]],
    *,
    label: str,
) -> tuple[list[Path], str]:
    current = start
    seen: set[str] = set()
    paths: list[Path] = []
    while current in by_source:
        if current in seen:
            raise SystemExit(f"cycle in {label} chain at {current}")
        seen.add(current)
        nxt, path = by_source[current]
        paths.append(path)
        current = nxt

    return paths, current


def upgrade_graph_head(
    by_source: dict[str, tuple[str, Path]],
    installer_versions: set[str],
) -> str:
    versions = set(installer_versions)
    for src, (dst, _path) in by_source.items():
        versions.add(src)
        versions.add(dst)
    return max(versions, key=version_key)


def validate_upgrade_graph(
    by_source: dict[str, tuple[str, Path]],
    *,
    label: str,
) -> str:
    installer_versions = set(installers())
    graph_head = upgrade_graph_head(by_source, installer_versions)
    reachable_paths: set[Path] = set()
    for start in sorted(installer_versions, key=version_key):
        paths, current = trace_upgrade_chain(start, by_source, label=label)
        reachable_paths.update(paths)
        if current != graph_head:
            raise SystemExit(
                f"disconnected {label} chain from {start}: stopped at "
                f"{current}, expected to reach {graph_head}"
            )

    unreachable_paths = sorted(
        (
            path
            for _src, (_dst, path) in by_source.items()
            if path not in reachable_paths
        ),
        key=rel,
    )
    if unreachable_paths:
        details = ", ".join(rel(path) for path in unreachable_paths)
        raise SystemExit(
            f"disconnected {label} graph: not reachable from a released "
            f"installer: {details}"
        )

    return graph_head


def upgrade_chain_paths(start: str, *, label: str = "upgrade") -> list[Path]:
    released_by_source = upgrades(include_devel=False, required=False)
    released_head = validate_upgrade_graph(
        released_by_source,
        label="released upgrade",
    )
    _released_paths, released_end = trace_upgrade_chain(
        start,
        released_by_source,
        label=f"released {label}",
    )
    if (
        version_key(start) <= version_key(released_head)
        and released_end != released_head
    ):
        raise SystemExit(
            f"disconnected released upgrade chain from {start}: stopped at "
            f"{released_end}, expected to reach {released_head}"
        )

    by_source = upgrades()
    graph_head = validate_upgrade_graph(by_source, label=label)
    paths, current = trace_upgrade_chain(start, by_source, label=label)
    if current != graph_head:
        raise SystemExit(
            f"disconnected upgrade chain from {start}: stopped at {current}, "
            f"expected to reach {graph_head}"
        )

    return paths


def emit_upgrade_paths(paths: list[Path]) -> None:
    for path in paths:
        emit_psql_include(path)
    emit_development_overlay(
        development_migration_seen=any(
            path.parent == development_install_path().parent for path in paths
        )
    )


def emit_upgrade_chain(start: str) -> None:
    emit_upgrade_paths(upgrade_chain_paths(start))


def emit_pinned_upgrade_chain(start: str) -> None:
    paths = upgrade_chain_paths(start, label="pinned upgrade")
    if not paths:
        raise SystemExit(f"no released upgrade from {start}")

    released_migration_dir = ROOT / "sql" / "migrations"
    first_path = paths[0]
    if first_path.parent != released_migration_dir:
        raise SystemExit(
            f"pinned upgrade from {start} must begin with a released migration, "
            f"found {rel(first_path)}"
        )

    public_wrapper = ROOT / "sql" / first_path.name
    if not public_wrapper.exists():
        raise SystemExit(
            f"no public upgrade wrapper for {start}: {rel(public_wrapper)}"
        )

    emit_upgrade_paths([public_wrapper, *paths[1:]])


def emit_full_upgrade_chain(start: str) -> None:
    install = installers().get(start)
    if install is None:
        raise SystemExit(f"no released installer for {start}")
    paths = upgrade_chain_paths(start)
    emit_psql_include(install)
    emit_upgrade_paths(paths)


def emit_reapply_chain() -> None:
    # Only the in-progress development upgrade script(s) — latest released
    # version up to the dev head — are guaranteed re-apply-safe (lockstep
    # policy). Finalized legacy scripts are immutable and idempotent only on
    # the version just below: once a later release removes the surface they
    # recreate (e.g. 2.0 drops the 1.x readers and ash._to_sample_ts),
    # re-applying them on a current install fails by design.
    current = latest_released_version()
    emit_upgrade_paths(upgrade_chain_paths(current, label="reapply"))


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
    pinned_upgrade = sub.add_parser("pinned-upgrade-chain")
    pinned_upgrade.add_argument("start")
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
    elif args.command == "pinned-upgrade-chain":
        emit_pinned_upgrade_chain(args.start)
    elif args.command == "full-upgrade-chain":
        emit_full_upgrade_chain(oldest_version())
    elif args.command == "reapply-chain":
        emit_reapply_chain()


if __name__ == "__main__":
    main()
