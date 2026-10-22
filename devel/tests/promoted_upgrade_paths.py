#!/usr/bin/env python3
"""Rehearse public upgrade paths using a temporary promoted candidate tree.

Released SQL is never edited. Run against an owned disposable PostgreSQL
server with CREATEDB privileges; database names are unique to this process.
"""
from __future__ import annotations

import argparse
import os
import re
import sys
import shutil
import subprocess
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "devel/scripts"))
import ash_sql_chain as chain


def tagged_origins(target_line: str) -> list[str]:
    """Include immutable historical tags and any subsequently published RCs."""
    names = subprocess.check_output(["git", "tag", "--list", "v*"],
                                    cwd=ROOT, text=True).splitlines()
    found = []
    for tag in names:
        match = chain.PAYLOAD_VERSION_RE.fullmatch(tag[1:])
        if match is None:
            continue
        line = match.group("release_line")
        if chain.version_key(line) > chain.version_key(target_line):
            continue
        stage = match.group("stage")
        counter = int(re.search(r"(\d+)$", tag).group(1)) if stage else 0
        found.append(((chain.version_key(line),
                       {"alpha": 0, "beta": 1, "rc": 2, None: 3}[stage], counter), tag))
    if not found:
        raise RuntimeError("no supported immutable SQL release tags discovered")
    return [tag for _key, tag in sorted(found)]



def tagged_installer(archived: Path, tag: str) -> Path:
    version = chain.PAYLOAD_VERSION_RE.fullmatch(tag[1:])
    source_line = version.group("release_line")
    if version.group("stage") == "alpha":
        candidate = archived / "devel/sql/ash-install.sql"
        if not candidate.is_file():
            raise RuntimeError(f"{tag}: missing alpha development installer")
        return candidate
    candidates = [archived / "sql" / name for name in (
        "ash-install.sql", f"ash--{source_line}.sql", f"ash-{source_line}.sql"
    )]
    source_install = next((path for path in candidates if path.exists()), None)
    if source_install is None:
        raise RuntimeError(f"{tag}: no recognized tagged SQL installer")
    return source_install


def main() -> None:
    argparse.ArgumentParser(description=__doc__).parse_args()
    with tempfile.TemporaryDirectory(prefix="ash-promoted-") as directory:
        temp = Path(directory)
        promoted = temp / "candidate"
        shutil.copytree(ROOT / "sql", promoted / "sql")
        candidate_install = ROOT / "devel/sql/ash-install.sql"
        if not candidate_install.exists():
            candidate_install = ROOT / "sql/ash-install.sql"
        shutil.copy2(candidate_install, promoted / "sql/ash-install.sql")
        migrations = chain.upgrades()
        target_line = chain.validate_upgrade_graph(migrations, label="promoted")
        for staged in (ROOT / "devel/sql").glob("ash-*-to-*.sql"):
            migration = staged.read_text().replace(
                r"\ir ash-install.sql", r"\ir ../ash-install.sql"
            )
            (promoted / "sql/migrations" / staged.name).write_text(migration)
            (promoted / "sql" / staged.name).write_text(
                r"\ir migrations/" + staged.name + "\n"
            )
        incoming = [path for dst, path in migrations.values() if dst == target_line]
        if len(incoming) != 1:
            raise RuntimeError("expected one cumulative migration to the candidate")
        wrapper = promoted / "sql" / incoming[0].name
        canonical = promoted / "sql/migrations" / incoming[0].name
        database = f"ash_promoted_{os.getpid()}"
        maintenance = os.environ.get("PGDATABASE", "postgres")
        env = {**os.environ, "PGDATABASE": database, "PAGER": "cat"}
        psql = ["psql", "--no-psqlrc", "--set=ON_ERROR_STOP=1",
                "--host=" + os.environ.get("PGHOST", "localhost"),
                "--username=" + os.environ.get("PGUSER", "postgres"),
                "--no-align", "--tuples-only", "--quiet"]

        def run(args: list[str], *, db: str = database, cwd: Path = ROOT) -> str:
            result = subprocess.run(psql + ["--dbname=" + db] + args,
                                    env=env, cwd=cwd, text=True,
                                    stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            if result.returncode:
                raise RuntimeError(
                    f"psql failed ({result.returncode}): {args}\n"
                    + result.stdout[-3000:] + result.stderr[-6000:]
                )
            return result.stdout

        def reset() -> None:
            run(["--command", f"drop database if exists {database} with (force)"],
                db=maintenance)
            run(["--command", f"create database {database}"], db=maintenance)

        def install(path: Path, *, cwd: Path = ROOT) -> None:
            run(["--file", str(path)], cwd=cwd)

        def snapshot() -> str:
            return run(["--file", str(ROOT / "devel/tests/schema_snapshot.sql")])

        def routines() -> dict[str, tuple[str, str]]:
            rows = run(["--command", "select oid, oid::regprocedure, "
                        "pg_get_function_result(oid) from pg_proc "
                        "where pronamespace = 'ash'::regnamespace "
                        "and prokind = 'f'"]).splitlines()
            return {signature: (oid, result)
                    for oid, signature, result in (row.split("|", 2) for row in rows)}

        def dependencies() -> dict[str, tuple[str, str]]:
            before = routines()
            # These signatures have unchanged results in beta1 and rc1.
            run(["--command", """
                create view public.ash_dependency_time as
                  select ash.ts_to_timestamptz(60) as ts;
                create view public.ash_dependency_timeline as
                  select * from ash.timeline('2026-01-01 00:00Z',
                                             '2026-01-01 00:01Z');
                create view public.ash_dependency_periods as
                  select * from ash.periods('2026-01-01 00:01Z');
                create function public.ash_dependency_atomic()
                returns timestamptz language sql
                begin atomic
                  select ash.ts_to_timestamptz(60);
                end;
            """])
            top = next(sig for sig in before if sig.startswith("ash.top("))
            if before[top][1] == expected_routines[top][1]:
                run(["--command", "create view public.ash_dependency_top as "
                     "select * from ash.top('wait_event')"])
            return before

        def check_dependencies(before: dict[str, tuple[str, str]]) -> None:
            after = routines()
            for sig, (oid, result) in before.items():
                # Changed return rows require DROP RESTRICT, not replacement.
                if sig in expected_routines and result == expected_routines[sig][1]:
                    assert after[sig][0] == oid, f"unchanged routine replaced: {sig}"
            assert run(["--command", "select ts = '2026-01-01 00:01Z'::timestamptz "
                        "and public.ash_dependency_atomic() = ts "
                        "from public.ash_dependency_time"]).strip() == "t"
            assert run(["--command", "select count(*) = 1 and "
                        "bool_and(data_points = 0 and avg_aas is null) "
                        "from public.ash_dependency_timeline"]).strip() == "t"

            assert run(["--command", "select count(*) from public.ash_dependency_periods"]).strip() == "6"
            if run(["--command", "select to_regclass('public.ash_dependency_top') is not null"]).strip() == "t":
                assert run(["--command", "select count(*) from public.ash_dependency_top"]).strip() == "0"

        def incompatible_dependency(tag: str) -> None:
            call = ("ash.top('wait_event')" if tag == "v2.0-beta1" else
                    "ash._grain_by(0, 60, 'raw', 'wait_event')")
            run(["--command", "create view public.ash_incompatible as select * from " + call])
            before_schema, before_routines = snapshot(), routines()
            before_version = run(["--command", "select version from ash.config"])
            try:
                install(wrapper)
            except RuntimeError as error:
                assert "cannot drop function" in str(error), str(error)
                assert "view ash_incompatible depends" in str(error), str(error)
            else:
                raise AssertionError(f"{tag}: incompatible dependency did not block upgrade")
            assert snapshot() == before_schema, f"{tag}: failed upgrade changed schema/ACLs"
            assert routines() == before_routines, f"{tag}: failed upgrade replaced routines"
            assert run(["--command", "select version from ash.config"]) == before_version
            run(["--command", "select * from public.ash_incompatible; "
                 "drop view public.ash_incompatible"])

        try:
            reset()
            install(promoted / "sql/ash-install.sql")
            expected_schema = snapshot()
            expected_routines = routines()
            before = dependencies()
            expected_version = run(["--command",
                "select version from ash.config where singleton"]).strip()
            # A public migration must also safely accept a fresh candidate.
            install(canonical)
            install(wrapper)
            check_dependencies(before)
            assert snapshot() == expected_schema, "fresh candidate wrapper changed schema"
            print("fresh promoted install/canonical/wrapper reapply PASS", flush=True)

            for tag in tagged_origins(target_line):
                reset()
                archived = temp / tag
                archived.mkdir()
                archive_file = temp / (tag + ".tar")
                subprocess.run(["git", "archive", "--format=tar",
                                "--output", str(archive_file), tag],
                               cwd=ROOT, check=True)
                subprocess.run(["tar", "-xf", str(archive_file), "-C", str(archived)],
                               check=True)
                source_line = chain.PAYLOAD_VERSION_RE.fullmatch(tag[1:]).group("release_line")
                source_install = tagged_installer(archived, tag)
                install(source_install, cwd=archived)
                before = None
                if tag in ("v2.0-beta1", "v2.0-rc1"):
                    before = dependencies()
                    incompatible_dependency(tag)
                run(["--command", "update ash.config set sample_interval = interval '5 seconds', "
                     "include_bg_workers = true where singleton"])
                # Preserve the actual tagged origin before applying finalized
                # historical edges and the rehearsed public candidate wrapper.
                origin_paths, _ = chain.trace_upgrade_chain(
                    source_line, migrations, label=f"{tag} promotion"
                )
                for path in origin_paths:
                    if path.name != canonical.name:
                        install(promoted / "sql/migrations" / path.name)
                install(wrapper)
                assert run(["--command", "select version from ash.config where singleton"]).strip() == expected_version, tag
                assert run(["--command", "select sample_interval = interval '5 seconds' "
                            "and include_bg_workers and not sample_unlogged "
                            "from ash.config where singleton"]).strip() == "t", tag
                assert snapshot() == expected_schema, f"{tag}: fresh/upgrade schema differs"
                oid = run(["--command", "select 'ash.config'::regclass::oid"])
                install(canonical)
                install(wrapper)
                assert run(["--command", "select 'ash.config'::regclass::oid"]) == oid, tag
                assert snapshot() == expected_schema, f"{tag}: reapply changed schema"
                if before is not None:
                    check_dependencies(before)
                # Reapply must preserve the operator's opt-in and physical ring.
                run(["--command", "select ash.set_sample_persistence('unlogged')"])
                install(wrapper)
                assert run(["--command", "select sample_unlogged from ash.config where singleton"]).strip() == "t", tag
                assert run(["--command", "select bool_and(c.relpersistence = 'u') "
                            "from pg_class c join pg_namespace n on n.oid=c.relnamespace "
                            "where n.nspname='ash' and c.relname ~ '^sample_[0-9]+$'"]).strip() == "t", tag
                print(f"{tag} -> promoted public wrapper; schema/config/reapply/persistence PASS", flush=True)
        finally:
            run(["--command", f"drop database if exists {database} with (force)"],
                db=maintenance)


if __name__ == "__main__":
    main()
