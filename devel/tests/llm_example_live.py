#!/usr/bin/env python3
"""Exercise the documented reader flow over real lock waits in a disposable DB.

Uses psql connection environment variables. Installs no extensions and changes
no timestamps/counts. Requires an installed candidate and an otherwise quiet
DB. Leaves collected history for inspection; drops only its own fixture table.
"""

import argparse
import pathlib
import subprocess
import time

ROOT = pathlib.Path(__file__).resolve().parents[2]
PSQL = ["psql", "-X", "-v", "ON_ERROR_STOP=1"]


def sql(statement: str) -> str:
    result = subprocess.run(
        PSQL + ["-qAt", "-c", statement], capture_output=True, text=True, check=True
    )
    return result.stdout.strip()


def wait_for_count(application: str, expected: int) -> None:
    deadline = time.monotonic() + 8
    while time.monotonic() < deadline:
        count = sql(
            "select count(*) from pg_stat_activity "
            f"where application_name = '{application}' and state = 'active' "
            "and wait_event_type in ('Lock', 'Timeout')"
        )
        if int(count) == expected:
            return
        time.sleep(0.1)
    raise RuntimeError(f"workload {application} did not become ready")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=pathlib.Path)
    args = parser.parse_args()
    processes = []
    created_fixture = False
    try:
        sql(
            "create table public.pgash_llm_demo_orders "
            "(id int primary key, status text); "
            "insert into public.pgash_llm_demo_orders values (1, 'new')"
        )
        created_fixture = True
        sql("select * from ash.start('1 second')")
        blocker = subprocess.Popen(
            PSQL + ["-qAt", "-c", "set application_name = 'pgash_llm_blocker'; "
                    "begin; update public.pgash_llm_demo_orders "
                    "set status = 'processing' where id = 1; "
                    "select pg_sleep(12); commit"],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        )
        processes.append(blocker)
        wait_for_count("pgash_llm_blocker", 1)
        for _ in range(3):
            processes.append(subprocess.Popen(
                PSQL + ["-qAt", "-c", "set application_name = 'pgash_llm_waiter'; "
                        "update public.pgash_llm_demo_orders "
                        "set status = 'shipped' where id = 1"],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
            ))
        wait_for_count("pgash_llm_waiter", 3)
        deadline = time.monotonic()
        for _ in range(7):
            time.sleep(max(0, deadline - time.monotonic()))
            assert int(sql("select ash.take_sample()")) >= 1
            deadline += 1
        for process in processes:
            _, stderr = process.communicate(timeout=20)
            if process.returncode:
                raise RuntimeError(stderr)

        # Minute-rollup readers need completed minutes. Wait for real time;
        # never restamp observations or edit generated reader output.
        bounds = sql(
            "select date_trunc('minute', min(ash.ts_to_timestamptz(sample_ts))) "
            "- interval '1 minute', "
            "date_trunc('minute', max(ash.ts_to_timestamptz(sample_ts))) "
            "+ interval '1 minute' from ash.sample"
        )
        since, until = bounds.split("|")
        remaining = float(sql(
            "select greatest(0, extract(epoch from "
            f"'{until}'::timestamptz - clock_timestamp()))"
        ))
        print(f"Waiting {remaining:.1f}s for the sampled minute to complete", flush=True)
        time.sleep(remaining + 0.1)
        sql("select ash.rollup_minute()")
        result = subprocess.run(
            PSQL + ["-P", "format=unaligned", "-v", f"since={since}",
                    "-v", f"until={until}",
                    "-f", str(ROOT / "examples/llm-investigation.sql")],
            capture_output=True, text=True, check=True,
        )
        assert "error:" not in result.stderr.lower(), result.stderr
        assert "Step 5: raw samples" in result.stdout, result.stdout
        assert "Lock:transactionid" in result.stdout, result.stdout
        assert '"top_queryids_available": true' in result.stdout, result.stdout
        pgss = sql("select exists(select from pg_extension "
                   "where extname = 'pg_stat_statements')") == "t"
        if pgss:
            assert "UPDATE public.pgash_llm_demo_orders".lower() in result.stdout.lower()
        else:
            assert "pgash_llm_demo_orders" not in result.stdout
        if args.output:
            args.output.write_text(result.stdout)
        print(f"LLM example PASSED (pg_stat_statements={pgss}); window {bounds}")
    finally:
        for process in processes:
            if process.poll() is None:
                process.terminate()
                process.communicate(timeout=5)
        if created_fixture:
            sql("drop table public.pgash_llm_demo_orders")


if __name__ == "__main__":
    main()
