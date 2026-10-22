#!/usr/bin/env python3
"""Exercise real streaming recovery, promotion and crash recovery in private clusters.

Usage: PG_BIN=/path/to/postgresql/bin python3 devel/scripts/test_physical_runtime.py
Optional PG_ASH_SOURCE points at a candidate checkout. Requires a non-root OS user
and matching initdb/pg_ctl/psql/pg_basebackup binaries. Never uses an existing DB.
"""
import os
from pathlib import Path
import shutil
import socket
import subprocess
import tempfile
import time

SOURCE = Path(os.environ.get("PG_ASH_SOURCE", Path(__file__).resolve().parents[2]))
BIN = Path(os.environ.get("PG_BIN", Path(shutil.which("pg_ctl") or "/missing/pg_ctl").parent))


def run(name, *args):
    return subprocess.run([str(BIN / name), *map(str, args)], check=True,
                          text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT).stdout


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def main():
    print("Source:", subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=SOURCE, text=True).strip(), flush=True)
    print(run("postgres", "--version"), end="", flush=True)
    # Short /tmp path avoids Unix-domain socket length limits on macOS.
    with tempfile.TemporaryDirectory(prefix="ash-physical-", dir="/tmp") as directory:
        root = Path(directory)
        primary, replica = root / "primary", root / "replica"
        ports = {primary: free_port(), replica: free_port()}
        while ports[primary] == ports[replica]:
            ports[replica] = free_port()
        running = set()

        def start(cluster):
            run("pg_ctl", "-D", cluster, "-l", root / (cluster.name + ".log"),
                "-o", f"-p {ports[cluster]} -k {root}", "-w", "start")
            running.add(cluster)

        def stop(cluster, mode):
            run("pg_ctl", "-D", cluster, "-m", mode, "-w", "stop")
            running.discard(cluster)

        def sql(cluster, statement):
            return run("psql", "-XAt", "-v", "ON_ERROR_STOP=1", "-h", root,
                       "-p", ports[cluster], "-d", "postgres", "-c", statement).strip()

        def assert_sql(cluster, statement, expected):
            actual = sql(cluster, statement)
            assert actual == expected, (statement, expected, actual)

        def caught_up():
            lsn = sql(primary, "select pg_current_wal_lsn()")
            deadline = time.monotonic() + 30
            while time.monotonic() < deadline:
                if sql(replica, f"select coalesce(pg_last_wal_replay_lsn() >= '{lsn}'::pg_lsn, false)") == "t":
                    return
                time.sleep(.1)
            raise AssertionError("streaming replica did not catch up")

        try:
            run("initdb", "-D", primary, "--auth=trust", "--no-locale")
            start(primary)
            run("psql", "-X", "-v", "ON_ERROR_STOP=1", "-h", root, "-p", ports[primary],
                "-d", "postgres", "-f", SOURCE / "devel/sql/ash-install.sql")
            sql(primary, """
                select ash.stop();
                select ash._register_wait('active', 'CPU*', 'CPU*');
                insert into ash.sample(sample_ts, datid, active_count, data)
                values(ash.ts_from_timestamptz(date_trunc('minute', now()) - interval '1 minute'), 0, 1, array[-ash._register_wait('active', 'CPU*', 'CPU*')::int, 1, 0]);
                insert into ash.rollup_1m(ts, datid, samples, peak_backends, wait_counts, query_counts)
                select ash.ts_from_timestamptz(date_trunc('minute', now()) - interval '2 hours'),
                    0, 1, 1, array[ash._register_wait('active', 'CPU*', 'CPU*')::int, 1], '{}'::bigint[];
                update ash.config set last_rollup_1m_ts = null;
            """)
            run("pg_basebackup", "-h", root, "-p", ports[primary], "-D", replica,
                "-R", "-X", "stream", "--checkpoint=fast")
            start(replica)
            caught_up()
            assert_sql(replica, "select pg_is_in_recovery(), ash._raw_ring_readable(), count(*) from ash.sample", "t|t|1")
            print("PASS logged raw reaches streaming standby", flush=True)

            sql(primary, "select ash.set_sample_persistence('unlogged')")
            caught_up()
            assert_sql(replica, "select pg_is_in_recovery(), ash._raw_ring_readable()", "t|f")
            assert_sql(replica, "select count(*) from ash.rollup_1m", "1")
            # These execute actual raw guards on a physical standby. In particular
            # the partial-source diagnostic must not probe the unreadable ring.
            for statement in (
                "select count(*) from ash.status()",
                "select count(*) from ash.aas(now() - interval '3 hours', now())",
                "select count(*) from ash.timeline(now() - interval '3 hours', now(), '1 minute')",
                "select count(*) from ash.top('wait_event_type', now() - interval '3 hours', now())",
                "select count(*) from ash.periods()",
                "select count(*) from ash.chart(now() - interval '3 hours', now())",
            ):
                sql(replica, statement)
            print("PASS unlogged standby status and aggregate readers remain usable", flush=True)

            # Isolate promotion before allowing writes on the promoted server.
            stop(primary, "fast")
            run("pg_ctl", "-D", replica, "-w", "promote")
            assert_sql(replica, "select pg_is_in_recovery(), ash._raw_ring_readable(), count(*) from ash.sample", "f|t|0")
            assert_sql(replica, "select count(*) from ash.rollup_1m", "1")
            print("PASS promotion exposes empty readable raw ring and retained rollups", flush=True)
            sql(replica, "insert into ash.sample(sample_ts, datid, active_count, data) values(ash.ts_from_timestamptz(now()), 0, 1, array[-ash._register_wait('active', 'CPU*', 'CPU*')::int, 1, 0])")
            stop(replica, "fast")
            start(replica)
            assert_sql(replica, "select (select count(*) from ash.sample), (select count(*) from ash.rollup_1m)", "1|1")
            print("PASS clean restart preserves unlogged raw and logged history", flush=True)
            stop(replica, "immediate")
            start(replica)
            assert_sql(replica, "select (select count(*) from ash.sample), (select count(*) from ash.rollup_1m)", "0|1")
            print("PASS immediate shutdown recovery loses unlogged raw and retains rollups", flush=True)
            sql(replica, "select ash.set_sample_persistence('logged'); insert into ash.sample(sample_ts, datid, active_count, data) values(ash.ts_from_timestamptz(now()), 0, 1, array[-ash._register_wait('active', 'CPU*', 'CPU*')::int, 1, 0])")
            stop(replica, "immediate")
            start(replica)
            assert_sql(replica, "select (select count(*) from ash.sample), (select count(*) from ash.rollup_1m)", "1|1")
            print("PASS logged raw and rollups survive immediate shutdown recovery", flush=True)
        except BaseException:
            for log in root.glob("*.log"):
                print(log.name, log.read_text()[-12000:], flush=True)
            raise
        finally:
            for cluster in list(running):
                stop(cluster, "immediate")


if __name__ == "__main__":
    main()
