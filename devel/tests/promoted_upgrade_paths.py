#!/usr/bin/env python3
"""Rehearse public upgrade paths using a temporary promoted candidate tree.

Released SQL is never edited. Run against an owned disposable PostgreSQL
server with CREATEDB privileges; database names are unique to this process.
"""
from __future__ import annotations

import os
import shutil
import subprocess
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
TAGS = [f"v1.{n}" for n in range(6)] + [
    f"v2.0-alpha{n}" for n in range(1, 6)
] + ["v2.0-beta1"]


def main() -> None:
    with tempfile.TemporaryDirectory(prefix="ash-promoted-") as directory:
        temp = Path(directory)
        promoted = temp / "candidate"
        shutil.copytree(ROOT / "sql", promoted / "sql")
        candidate_install = ROOT / "devel/sql/ash-install.sql"
        if not candidate_install.exists():
            candidate_install = ROOT / "sql/ash-install.sql"
        shutil.copy2(candidate_install, promoted / "sql/ash-install.sql")
        staged = ROOT / "devel/sql/ash-1.5-to-2.0.sql"
        if staged.exists():
            migration = staged.read_text().replace(
                r"\ir ash-install.sql", r"\ir ../ash-install.sql"
            )
            (promoted / "sql/migrations/ash-1.5-to-2.0.sql").write_text(migration)
        wrapper = promoted / "sql/ash-1.5-to-2.0.sql"
        canonical = promoted / "sql/migrations/ash-1.5-to-2.0.sql"
        database = f"ash_promoted_{os.getpid()}"
        maintenance = os.environ.get("PGDATABASE", "postgres")
        env = {**os.environ, "PGDATABASE": database, "PAGER": "cat"}
        psql = ["psql", "--no-psqlrc", "--set=ON_ERROR_STOP=1",
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

        try:
            reset()
            install(promoted / "sql/ash-install.sql")
            expected_schema = snapshot()
            expected_version = run(["--command",
                "select version from ash.config where singleton"]).strip()
            # A public migration must also safely accept a fresh candidate.
            install(canonical)
            install(wrapper)
            assert snapshot() == expected_schema, "fresh candidate wrapper changed schema"
            print("fresh promoted install/canonical/wrapper reapply PASS", flush=True)

            for tag in TAGS:
                reset()
                archived = temp / tag
                archived.mkdir()
                archive_file = temp / (tag + ".tar")
                subprocess.run(["git", "archive", "--format=tar",
                                "--output", str(archive_file), tag, "sql"],
                               cwd=ROOT, check=True)
                subprocess.run(["tar", "-xf", str(archive_file), "-C", str(archived)],
                               check=True)
                source_name = {"v1.0": "ash--1.0.sql", "v1.1": "ash--1.1.sql"}.get(
                    tag, "ash-install.sql")
                install(archived / "sql" / source_name, cwd=archived)
                run(["--command", "update ash.config set sample_interval = interval '5 seconds', "
                     "include_bg_workers = true where singleton"])
                # Preserve the actual tagged origin before applying finalized
                # historical edges and the rehearsed public candidate wrapper.
                if tag.startswith("v1."):
                    for minor in range(int(tag.split('.')[1]), 5):
                        install(promoted / f"sql/migrations/ash-1.{minor}-to-1.{minor+1}.sql")
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
