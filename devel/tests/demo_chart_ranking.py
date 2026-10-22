#!/usr/bin/env python3
"""Exercise the real demo shape gate and chart scene on deterministic samples.

This is synthetic regression coverage, not evidence of captured workload.
Use libpq PG* variables for a server where this role can create databases.
Only the uniquely named database created by this test is modified or removed.
"""
import csv
from pathlib import Path
import subprocess
import uuid

ROOT = Path(__file__).resolve().parents[2]


def run(command, *, sql=None, check=True):
    result = subprocess.run(command, input=sql, text=True, capture_output=True,
                            timeout=90)
    if check and result.returncode:
        raise RuntimeError(result.stdout + result.stderr)
    return result


def main():
    database = "ash_demo_rank_" + uuid.uuid4().hex[:12]
    run(["createdb", "--template=template0", database])
    try:
        psql = ["psql", "-X", "-A", "-t", "-v", "ON_ERROR_STOP=1",
                "--dbname=" + database]
        installer = run(["python3", str(ROOT / "devel/scripts/ash_sql_chain.py"),
                         "fresh-install-path"]).stdout.strip()
        run(psql, sql=(ROOT / installer).read_text())
        fixture = (ROOT / "devel/tests/demo_chart_ranking_fixture.sql").read_text()
        shape = (ROOT / "demos/lib/shape.sql").read_text()
        rows = csv.reader((ROOT / "demos/scenes/scenes.tsv").read_text().splitlines(),
                          delimiter="\t")
        chart = next(row[5] for row in rows if row and row[0] == "chart")
        chart = chart.replace("$SINCE", "ash.ts_to_timestamptz(:base_ts + 240)")
        chart = chart.replace("$UNTIL", "ash.ts_to_timestamptz(:base_ts + 1680)")
        settings = r"""\set vmin 24
\set vmin_slack 4
\set vmin_total 28
\set ph_baseline 12
\set ph_storm 5
"""
        for case, rank, should_pass in [("rank4", 4, True), ("rank5", 5, True),
                                       ("rank6", 6, False), ("one_lock", None, False)]:
            setup = "begin;\n\\set fixture_case " + case + "\n" + fixture
            # Run the unchanged production scene separately, even for failures.
            captured = run(psql, sql=setup + "\\echo CHART_BEGIN\n" + chart
                           + "\n\\echo CHART_END\nrollback;\n").stdout
            ranking, output = captured.split("CHART_BEGIN\n", 1)
            output = output.split("CHART_END\n", 1)[0]
            keys = [line.split("|")[0] for line in ranking.splitlines()
                    if "|" in line and line.split("|")[0] in
                    ("CPU*", "Client:ClientRead", "IO:DataFileRead",
                     "Lock:transactionid", "Lock:tuple", "Timeout:PgSleep")]
            actual_rank = keys.index("Lock:tuple") + 1 if "Lock:tuple" in keys else None
            assert actual_rank == rank, (case, keys)
            result = run(psql, sql=setup + settings + shape + "\nrollback;\n", check=False)
            if should_pass:
                assert result.returncode == 0, result.stdout + result.stderr
                assert "shape ok  chart top-" in result.stderr, result.stderr
            else:
                assert result.returncode != 0, case + " wrongly passed"
                assert "shape: only 1 Lock:* event(s) rank in the top" in result.stderr, result.stderr
            chart_rows = [line.split("|", 2) for line in output.splitlines()
                          if len(line.split("|", 2)) == 3
                          and len(line.split("|", 1)[0]) == 5
                          and line[2] == ":"]
            assert len(chart_rows) > 1, output
            assert len({len(row[0]) + len(row[1]) for row in chart_rows}) == 1, (
                "chart bars must start in the same column", chart_rows)
            assert "Lock:transactionid" in output, (case, output)
            assert ("Lock:tuple" in output) == should_pass, (case, output)
            print(f"PASS {case}: tuple rank={rank}, chart and full shape gate agree")
    finally:
        run(["dropdb", database])


if __name__ == "__main__":
    main()
