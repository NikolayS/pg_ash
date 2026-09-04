#!/usr/bin/env python3
"""Rehearse public upgrade paths using a temporary promoted candidate tree.

Released SQL is never edited. Run against an owned disposable PostgreSQL
server with CREATEDB privileges; database names are unique to this process.
"""
from __future__ import annotations

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



def main() -> None:
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

            for tag in tagged_origins(target_line):
                reset()
                archived = temp / tag
                archived.mkdir()
                archive_file = temp / (tag + ".tar")
                subprocess.run(["git", "archive", "--format=tar",
                                "--output", str(archive_file), tag, "sql"],
                               cwd=ROOT, check=True)
                subprocess.run(["tar", "-xf", str(archive_file), "-C", str(archived)],
                               check=True)
                source_line = chain.PAYLOAD_VERSION_RE.fullmatch(tag[1:]).group("release_line")
                candidates = [archived / "sql" / name for name in (
                    "ash-install.sql", f"ash--{source_line}.sql", f"ash-{source_line}.sql"
                )]
                source_install = next((path for path in candidates if path.exists()), None)
                if source_install is None:
                    raise RuntimeError(f"{tag}: no recognized tagged SQL installer")
                install(source_install, cwd=archived)
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
