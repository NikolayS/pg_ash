#!/usr/bin/env python3
"""Exercise #137 locking against a disposable installed database via PG* env."""

import os
import subprocess
import time
import uuid


PSQL = os.environ.get("PSQL", "psql")
BASE = [PSQL, "-X", "-qAt", "-v", "ON_ERROR_STOP=1", "-v", "VERBOSITY=verbose"]


def sql(statement, expected_state=None):
    result = subprocess.run(
        BASE + ["-c", statement], capture_output=True, text=True, check=False
    )
    if expected_state is None:
        assert result.returncode == 0, result.stderr
    else:
        assert result.returncode != 0, "statement unexpectedly succeeded"
        assert expected_state in result.stderr, result.stderr
    return result.stdout.strip()


def with_holder(statement, probe):
    app_name = "ash_cadence_" + uuid.uuid4().hex
    env = dict(os.environ, PGAPPNAME=app_name)
    holder = subprocess.Popen(
        BASE + ["-c", "begin; " + statement + "; select pg_sleep(30); rollback;"],
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        deadline = time.monotonic() + 10
        while True:
            if holder.poll() is not None:
                raise AssertionError(holder.stderr.read())
            ready = sql(
                "select exists (select from pg_stat_activity "
                f"where application_name = '{app_name}' and wait_event = 'PgSleep')"
            )
            if ready == "t":
                break
            assert time.monotonic() < deadline, "holder never reached its barrier"
            time.sleep(0.05)
        probe()
    finally:
        sql(
            "select pg_terminate_backend(pid) from pg_stat_activity "
            f"where application_name = '{app_name}'"
        )
        holder.communicate(timeout=10)


def main():
    sql(
        "truncate ash.sample, ash.rollup_1m, ash.rollup_1h; "
        "update ash.config set sample_interval = interval '1 second';"
    )
    insert = (
        "insert into ash.sample(sample_ts, datid, active_count, data) "
        "values (1, 0, 1, array[-1, 1, 0])"
    )
    change = "update ash.config set sample_interval = interval '5 seconds'"

    # Uncommitted rows are invisible to EXISTS but their write locks are visible.
    with_holder(insert, lambda: sql(change, "55P03"))
    assert sql("select sample_interval from ash.config") == "00:00:01"
    assert sql("select count(*) from ash.sample") == "0"

    with_holder(
        "select pg_advisory_xact_lock(hashtext('pg_ash'), hashtext('pg_ash_sampler'))",
        lambda: sql(change, "55P03"),
    )
    assert sql("select sample_interval from ash.config") == "00:00:01"

    # A successful empty-history check protects the gap until the caller commits.
    with_holder(
        change,
        lambda: sql("set lock_timeout = '250ms'; " + insert, "55P03"),
    )
    assert sql("select sample_interval from ash.config") == "00:00:01"
    assert sql("select count(*) from ash.sample") == "0"

    for isolation in ("repeatable read", "serializable"):
        sql("begin isolation level " + isolation + "; " + change, "55000")
    assert sql("select sample_interval from ash.config") == "00:00:01"
    print("Issue #137 cadence concurrency assertions PASSED")


if __name__ == "__main__":
    main()
