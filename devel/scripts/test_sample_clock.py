#!/usr/bin/env python3
"""Check old transactions and late sampler commits in a disposable database."""

import os
import subprocess


class Session:
    def __enter__(self):
        self.process = subprocess.Popen(
            [os.environ.get("PSQL", "psql"), "-XqAt", "-v", "ON_ERROR_STOP=1"],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True,
        )
        self.sql("set statement_timeout = '75s'")
        return self

    def sql(self, statement):
        self.process.stdin.write(statement + ";\n\\echo command_done\n")
        self.process.stdin.flush()
        rows = []
        while True:
            line = self.process.stdout.readline()
            assert line, "psql exited before the command finished"
            if line.strip() == "command_done":
                return "\n".join(rows)
            rows.append(line.strip())

    def __exit__(self, *args):
        self.process.stdin.close()
        self.process.wait(timeout=10)
        self.process.stdout.close()


def main():
    with Session() as sampler, Session() as observer:
        observer.sql(
            "truncate ash.sample, ash.rollup_1m, ash.rollup_1h; "
            "update ash.config set sampling_enabled = true, include_bg_workers = false, "
            "sample_interval = interval '1 second'"
        )
        # Start before the boundary, without taking the sampler lock yet.
        sampler.sql("begin")
        old_minute = int(sampler.sql(
            "select ash.ts_from_timestamptz(date_trunc('minute', now()))"
        ))
        observer.sql(f"update ash.config set last_rollup_1m_ts = {old_minute}")
        observer.sql(
            f"select pg_sleep_until(ash.ts_to_timestamptz({old_minute + 60}) "
            "+ interval '100 milliseconds')"
        )
        assert observer.sql("select ash.rollup_minute(1)") == "1"
        assert observer.sql("select last_rollup_1m_ts from ash.config") == str(old_minute + 60)

        # The idle observer supplies exactly one sampled backend.
        observer.sql("begin")
        assert sampler.sql("select ash.take_sample()") == "1"
        sampler.sql(f"""
            do $$ begin
              assert (select count(*) from ash.sample) = 1;
              assert (select active_count from ash.sample) = 1;
              assert (select sample_ts >= {old_minute + 60} from ash.sample),
                'sample timestamp must not precede the completed-minute watermark';
            end $$
        """)
        observer.sql("commit")

        # Keep the sample uncommitted across its own minute boundary.
        observer.sql(
            f"select pg_sleep_until(ash.ts_to_timestamptz({old_minute + 120}) "
            "+ interval '100 milliseconds')"
        )
        assert observer.sql("select ash.rollup_minute(1)") == "0"
        assert observer.sql("select last_rollup_1m_ts from ash.config") == str(old_minute + 60)
        assert observer.sql("select count(*) from ash.sample") == "0"
        sampler.sql("commit")
        assert observer.sql("select ash.rollup_minute(1)") == "1"
        observer.sql(f"""
            do $$ begin
              assert (select count(*) from ash.rollup_1m) = 1;
              assert (select ts from ash.rollup_1m) = {old_minute + 60};
              assert (select samples from ash.rollup_1m) = 1;
              assert (select peak_backends from ash.rollup_1m) = 1;
              assert (select wait_counts[2] from ash.rollup_1m) = 1;
              assert (select cardinality(wait_counts) from ash.rollup_1m) = 2;
              assert (select last_rollup_1m_ts from ash.config) = {old_minute + 120};
            end $$
        """)
        assert observer.sql("select ash.rollup_minute(1)") == "0"
    print("Sample wall clock and late-commit drain assertions PASSED")


if __name__ == "__main__":
    main()
