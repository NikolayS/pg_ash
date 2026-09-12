#!/usr/bin/env python3
"""Live exit-trap ownership checks in an owned disposable pg_ash/pg_cron database."""
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest
import uuid

PSQL = shutil.which('psql')
SCRIPT = Path(__file__).with_name('test_lifecycle_atomicity.sh')
ROLE = 'ash_issue_203_other_superuser'


def sql(statement):
    return subprocess.check_output(
        [PSQL, '-XAt', '-h', os.environ.get('PGHOST', 'localhost'),
         '-U', os.environ.get('PGUSER', 'postgres'),
         '-d', os.environ.get('PGDATABASE', 'postgres'),
         '-v', 'ON_ERROR_STOP=1', '-c', statement], text=True,
    ).strip()


class LifecycleCleanupTests(unittest.TestCase):
    def test_early_failure_preserves_unowned_resources(self):
        self.check_failure('first-command-failure')

    def test_role_collision_preserves_unowned_resources(self):
        self.check_failure('role-collision')

    def test_failure_after_creation_cleans_owned_role(self):
        if sql(f"select count(*) from pg_roles where rolname='{ROLE}'") != '0':
            self.fail('fixture role already exists; refusing to alter it')
        with tempfile.TemporaryDirectory() as tmp:
            wrapper = Path(tmp) / 'psql'
            wrapper.write_text(
                '#!/usr/bin/env python3\nimport os,sys,subprocess\n'
                + 'real=' + repr(PSQL) + '\n'
                "if any(a.startswith('--command=') for a in sys.argv):\n"
                " os.execv(real,[real]+sys.argv[1:])\n"
                "body=sys.stdin.read()\n"
                "if body.startswith('set role ash_issue_203_other_superuser;'):\n"
                " print('injected post-create failure',file=sys.stderr);sys.exit(65)\n"
                "sys.exit(subprocess.run([real]+sys.argv[1:],input=body,text=True).returncode)\n"
            )
            wrapper.chmod(0o700)
            env = dict(os.environ, PATH=tmp + os.pathsep + os.environ['PATH'])
            result = subprocess.run(['bash', str(SCRIPT)], env=env,
                                    text=True, capture_output=True, timeout=90)
        self.assertEqual(result.returncode, 65, result.stderr)
        self.assertIn('injected post-create failure', result.stderr)
        self.assertEqual(sql(f"select count(*) from pg_roles where rolname='{ROLE}'"), '0',
                         'exit trap leaked its own role')

    def check_failure(self, mode):
        # Never adopt or delete a role left by another invocation.
        if sql(f"select count(*) from pg_roles where rolname='{ROLE}'") != '0':
            self.fail('fixture role already exists; refusing to alter it')
        created = False
        job = None
        try:
            sql(f'create role {ROLE} login superuser')
            created = True
            job = sql("select cron.schedule_in_database('cleanup_" + uuid.uuid4().hex
                      + "','* * * * *','select 1','template1','" + ROLE + "',false)")
            with tempfile.TemporaryDirectory() as tmp:
                env = os.environ.copy()
                if mode == 'first-command-failure':
                    wrapper = Path(tmp) / 'psql'
                    wrapper.write_text(
                        '#!/usr/bin/env python3\nimport os,sys\n'
                        "if any('select * from ash.stop(); select * from ash.start(' in a for a in sys.argv):\n"
                        " print('injected first-command failure',file=sys.stderr);sys.exit(65)\n"
                        + 'os.execv(' + repr(PSQL) + ', [' + repr(PSQL) + ']+sys.argv[1:])\n'
                    )
                    wrapper.chmod(0o700)
                    env['PATH'] = tmp + os.pathsep + env['PATH']
                result = subprocess.run(['bash', str(SCRIPT)], env=env,
                                        text=True, capture_output=True, timeout=90)
                self.assertNotEqual(result.returncode, 0, result.stdout)
                expected = ('injected first-command failure' if mode == 'first-command-failure'
                            else f'role "{ROLE}" already exists')
                self.assertIn(expected, result.stderr)
                self.assertEqual(sql(f"select count(*) from pg_roles where rolname='{ROLE}'"), '1',
                                 'exit trap deleted a role it did not create')
                self.assertEqual(sql(f"select count(*) from cron.job where jobid={int(job)} "
                                     f"and username='{ROLE}' and database='template1' and not active"), '1',
                                 'exit trap changed a foreign-database job it did not create')
        finally:
            if job is not None:
                sql(f'select cron.unschedule(jobid) from cron.job where jobid={int(job)}')
            if created:
                sql(f'drop role if exists {ROLE}')


if __name__ == '__main__':
    unittest.main()
