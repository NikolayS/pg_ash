#!/usr/bin/env python3
"""Run a small lock-wait investigation in an empty ash_demo* database.

Requires Python 3.10+, psql, and an installed pg_ash 2.0 candidate.
Uses standard libpq connection variables. No extensions are required.
"""

import argparse
import json
import os
import pathlib
import subprocess
import time
import uuid

TICKS = 8
WAITERS = 3
PSQL = ["psql", "-X", "-v", "ON_ERROR_STOP=1", "-P", "pager=off"]


class Lab:
    def __init__(self, output: pathlib.Path) -> None:
        self.output = output
        self.output.mkdir(parents=True, exist_ok=False)
        self.token = uuid.uuid4().hex[:16]
        self.table = f"public.ash_lock_lab_{self.token}"
        self.processes: list[subprocess.Popen] = []
        self.handles = []
        self.apps: list[str] = []
        self.created = False
        self.env = dict(os.environ)
        # Set this for every connection, including the actual update sessions.
        self.env["PGOPTIONS"] = self.env.get("PGOPTIONS", "") + (
            " -c compute_query_id=on -c timezone=UTC -c statement_timeout=20000"
            " -c idle_in_transaction_session_timeout=30000"
        )

    def say(self, message: str) -> None:
        print(message, flush=True)
        with (self.output / "run.log").open("a") as stream:
            stream.write(message + "\n")

    def sql(self, statement: str) -> str:
        with (self.output / "commands.sql").open("a") as stream:
            stream.write(statement + ";\n")
        result = subprocess.run(
            PSQL + ["-qAt", "-c", statement], env=self.env,
            capture_output=True, text=True, timeout=25,
        )
        if result.stderr:
            with (self.output / "diagnostics.log").open("a") as stream:
                stream.write(result.stderr)
        if result.returncode:
            raise RuntimeError(result.stderr)
        return result.stdout.strip()

    def spawn(self, application: str, statement: str) -> subprocess.Popen:
        self.apps.append(application)
        stream = (self.output / f"{application}_{len(self.processes)}.log").open("w")
        self.handles.append(stream)
        process = subprocess.Popen(
            PSQL + ["-qAt"], env={**self.env, "PGAPPNAME": application},
            stdin=subprocess.PIPE, stdout=stream, stderr=subprocess.STDOUT,
            text=True,
        )
        self.processes.append(process)
        process.stdin.write(statement + "\n")
        process.stdin.flush()
        return process

    def wait_state(self, application: str, state: str, expected: int) -> None:
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            count = self.sql(
                "select count(*) from pg_stat_activity "
                f"where application_name = '{application}' and {state}"
            )
            if int(count) == expected:
                return
            time.sleep(0.1)
        raise RuntimeError(f"Timed out waiting for {application}: {state}")

    def finish(self, process: subprocess.Popen) -> None:
        process.communicate(timeout=10)
        if process.returncode:
            raise RuntimeError("Workload failed; inspect the per-session logs")

    def preflight(self) -> None:
        if not self.sql("select current_database()").startswith("ash_demo"):
            raise RuntimeError("Use a disposable database whose name starts with ash_demo")
        if self.sql("select to_regnamespace('ash') is not null") != "t":
            raise RuntimeError("Install the pg_ash 2.0 candidate in this database first")
        info = json.loads(self.sql("""
select json_build_object(
    'version', version(), 'database', current_database(),
    'database_owner', db.datdba = role.oid,
    'ash_owner', nsp.nspowner = role.oid,
    'query_ids', current_setting('compute_query_id'),
    'pg_cron', exists(select from pg_extension where extname = 'pg_cron'),
    'pg_stat_statements', exists(
        select from pg_extension where extname = 'pg_stat_statements')
)
from pg_database as db
inner join pg_roles as role on role.rolname = current_user
inner join pg_namespace as nsp on nsp.nspname = 'ash'
where db.datname = current_database()
"""))
        if not info["database_owner"] or not info["ash_owner"]:
            raise RuntimeError("Connect as the database owner and ash schema owner")
        if info["pg_cron"]:
            raise RuntimeError("Use a separate database without pg_cron for manual sampling")
        if self.sql("""
select sampling_enabled and sample_interval = interval '1 second'
    and not exists(select from ash.sample)
    and not exists(select from ash.rollup_1m)
    and not exists(select from ash.rollup_1h)
from ash.config
""") != "t":
            raise RuntimeError("Use a fresh install with enabled one-second sampling")
        (self.output / "environment.json").write_text(json.dumps(info, indent=2) + "\n")
        self.initial_counters = self.counters()
        self.say(f"Database: {info['database']}; output: {self.output}")
        self.sql(f"create table {self.table} (id int primary key, changes int not null)")
        self.created = True
        self.sql(f"insert into {self.table} values (1, 0)")

    def counters(self) -> dict:
        return json.loads(self.sql("""
select json_build_object(
    'skipped_samples', skipped_samples, 'missed_samples', missed_samples,
    'insert_errors', insert_errors, 'register_wait_cap_hits', register_wait_cap_hits
)
from ash.config
"""))

    def minute_window(self) -> tuple[str, str]:
        # Leave ample space for startup, eight ticks, and waiter completion.
        remaining = float(self.sql("""
select case when extract(second from clock_timestamp()) > 35
    then extract(epoch from date_trunc('minute', clock_timestamp())
        + interval '1 minute' - clock_timestamp()) + 0.1
    else 0 end
"""))
        if remaining:
            self.say(f"Waiting {remaining:.1f}s for a fresh minute")
            time.sleep(remaining)
        return tuple(self.sql("""
select date_trunc('minute', clock_timestamp()),
    date_trunc('minute', clock_timestamp()) + interval '1 minute'
""").split("|"))

    def phase(self, name: str, early_commit: bool) -> dict:
        since, until = self.minute_window()
        self.sql(f"update {self.table} set changes = 0 where id = 1")
        holder_app = f"ash_lab_{self.token}_{name}_holder"
        waiter_app = f"ash_lab_{self.token}_{name}_waiter"
        update = f"update {self.table} set changes = changes + 1 where id = 1;"
        holder = self.spawn(holder_app, "begin;\n" + update)
        self.wait_state(holder_app, "state = 'idle in transaction'", 1)
        if early_commit:
            holder.stdin.write("commit;\n")
            holder.stdin.flush()
            self.wait_state(holder_app, "state = 'idle'", 1)
        waiters = [self.spawn(waiter_app, update) for _ in range(WAITERS)]
        if early_commit:
            for waiter in waiters:
                self.finish(waiter)
        else:
            self.wait_state(waiter_app, "state = 'active' and wait_event_type = 'Lock'", WAITERS)
        self.say(f"{name}: window [{since}, {until}); {TICKS} manual sample ticks")
        deadline = time.monotonic()
        ticks = []
        for index in range(TICKS):
            time.sleep(max(0, deadline + index - time.monotonic()))
            tick = json.loads(self.sql("""
select json_build_object(
    'sample_call_started_at', clock_timestamp(),
    'sample_second', ash.ts_to_timestamptz(
        extract(epoch from now() - ash.epoch())::int4),
    'inserted_database_rows', ash.take_sample(),
    'sample_call_finished_at', clock_timestamp()
)
"""))
            tick.update(phase=name, tick=index + 1)
            ticks.append(tick)
            with (self.output / "ticks.jsonl").open("a") as stream:
                stream.write(json.dumps(tick) + "\n")
        # Both clients spend eight seconds here. Only before holds a row lock.
        time.sleep(max(0, deadline + TICKS - time.monotonic()))
        if not early_commit:
            holder.stdin.write("commit;\n")
            holder.stdin.flush()
        self.finish(holder)
        for waiter in waiters:
            if not early_commit:
                self.finish(waiter)
        completed = int(self.sql(f"select changes from {self.table} where id = 1"))
        if completed != WAITERS + 1:
            raise RuntimeError(f"Expected four committed increments, got {completed}")
        if len({tick["sample_second"] for tick in ticks}) != TICKS:
            raise RuntimeError("Sample ticks collided; do not use this run for comparison")
        if self.sql(f"select clock_timestamp() < '{until}'::timestamptz") != "t":
            raise RuntimeError("Work crossed the minute boundary; comparison is invalid")
        self.say(f"{name}: all four updates committed; all workload connections exited")
        remaining = float(self.sql(
            "select greatest(0, extract(epoch from "
            f"'{until}'::timestamptz - clock_timestamp()))"
        ))
        self.say(f"{name}: waiting {remaining:.1f}s for this minute to complete")
        time.sleep(remaining + 0.1)
        self.sql("select ash.rollup_minute()")
        self.investigate(name, since, until)
        counts = json.loads(self.sql(f"""
select json_build_object(
    'lock_backend_observations', count(*),
    'lock_sample_seconds', count(distinct sample_time),
    'query_ids', coalesce(json_agg(distinct query_id), '[]'::json)
)
from ash.samples(
    since => '{since}', until => '{until}', n => 10000,
    wait_event_type => 'Lock', database => current_database()::name
)
"""))
        phase = dict(phase=name, since=since, until=until, window_seconds=60,
                     manual_sample_ticks=TICKS, committed_updates=completed, **counts)
        (self.output / f"{name}.json").write_text(json.dumps(phase, indent=2) + "\n")
        self.say(f"{name}: {counts['lock_backend_observations']} sampled Lock backend-observations")
        return phase

    def investigate(self, name: str, since: str, until: str) -> None:
        statement = f"""\\set ON_ERROR_STOP on
\\pset pager off
\\pset null '(null)'
-- Workload connections already exited. These are fixed, half-open windows.
-- SQL text is current best-effort text, not archived historical SQL.
begin transaction isolation level repeatable read read only;
set local timezone = 'UTC';
\\echo {name}: workload has ended; inspect stored load and bounds
select * from ash.aas(since => '{since}', until => '{until}',
    database => current_database()::name);
\\echo Rank stored waits
select * from ash.top('wait_event', since => '{since}', until => '{until}',
    database => current_database()::name);
\\echo Locate lock waits in time
select * from ash.timeline(since => '{since}', until => '{until}',
    bucket => '1 minute', wait_event_type => 'Lock',
    database => current_database()::name);
\\echo Attribute lock waits to historical query IDs
select * from ash.top('query_id', since => '{since}', until => '{until}',
    wait_event_type => 'Lock', database => current_database()::name);
\\echo Inspect raw lock evidence; one row per sampled backend observation
select * from ash.samples(since => '{since}', until => '{until}', n => 10000,
    wait_event_type => 'Lock', database => current_database()::name);
rollback;
"""
        path = self.output / f"{name}.sql"
        path.write_text(statement)
        result = subprocess.run(
            PSQL + ["-f", str(path)], env=self.env, stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT, text=True, timeout=25,
        )
        (self.output / f"{name}.txt").write_text(result.stdout)
        if result.returncode:
            raise RuntimeError(result.stdout)
        self.say(result.stdout)

    def cleanup(self) -> None:
        try:
            if self.apps:
                apps = ",".join(f"'{app}'" for app in self.apps)
                self.sql(
                    "select pg_terminate_backend(pid, 5000) from pg_stat_activity "
                    "where datname = current_database() and usename = session_user "
                    f"and application_name in ({apps})"
                )
        finally:
            for process in self.processes:
                if process.poll() is None:
                    process.terminate()
                    try:
                        process.communicate(timeout=5)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.communicate()
            for stream in self.handles:
                stream.close()
            if self.created:
                self.sql(f"drop table {self.table}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=pathlib.Path,
                        help="New evidence directory; existing paths are refused")
    args = parser.parse_args()
    output = args.output or pathlib.Path(f"/tmp/pgash-lock-lab-{uuid.uuid4().hex[:12]}")
    lab = Lab(output.resolve())
    try:
        lab.preflight()
        before = lab.phase("before", early_commit=False)
        lab.say("Change: commit before client-side work; keep that work outside the transaction")
        after = lab.phase("after", early_commit=True)
        if before["until"] > after["since"]:
            raise RuntimeError("Comparison windows overlap")
        if (before["lock_backend_observations"] != TICKS * WAITERS
                or before["lock_sample_seconds"] != TICKS
                or after["lock_backend_observations"] != 0):
            raise RuntimeError("Unexpected lock evidence; inspect this run, do not relabel it")
        if not before["query_ids"] or None in before["query_ids"]:
            raise RuntimeError("Missing historical query attribution")
        final_counters = lab.counters()
        (lab.output / "sampler-counters.json").write_text(
            json.dumps(dict(before=lab.initial_counters, after=final_counters), indent=2)
            + "\n"
        )
        if final_counters != lab.initial_counters:
            raise RuntimeError("Sampler diagnostics changed; inspect this run")
        lab.say(f"Verified: sampled Lock observations {TICKS * WAITERS} -> 0; "
                f"{TICKS} manual ticks and four committed updates in each phase")
        lab.say("This is sampled evidence, not a latency or throughput benchmark. "
                "Missing stored rows do not prove idle time or scheduler health. "
                "The tick log independently records these manual calls.")
    finally:
        lab.cleanup()


if __name__ == "__main__":
    main()
