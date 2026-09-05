#!/usr/bin/env python3
"""Exercise owner cron privileges in a private libpq cluster inside the CI container.

CONTAINER_ID identifies an already-owned disposable test container. The main
server is untouched; a private data directory and port use the same binaries.
"""
import argparse
import hashlib
import os
from pathlib import Path
import random
import subprocess
import time

ROOT = Path(__file__).resolve().parents[2]
OWNER = 'ash_cron_settings_owner'


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--installer', type=Path)
    args = parser.parse_args()
    container = os.environ['CONTAINER_ID']
    installer = args.installer or ROOT / subprocess.check_output(
        ['python3', str(ROOT / 'devel/scripts/ash_sql_chain.py'), 'fresh-install-path'],
        cwd=ROOT, text=True).strip()

    def execute(command, source=None, timeout=45):
        result = subprocess.run(['docker', 'exec', '-i', '-u', 'postgres',
                                 container, *command], input=source, text=True,
                                stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                timeout=timeout)
        if result.returncode:
            print(result.stdout, flush=True)
            raise subprocess.CalledProcessError(result.returncode, command)
        return result.stdout.strip()

    directory = execute(['mktemp', '-d', '/tmp/ash-owner-cron.XXXXXXXX'])
    port = random.randint(20000, 60000)
    running = False
    workload = None

    def sql(statement):
        return execute(['psql', '-XAt', '-h', '/var/run/postgresql', '-p', str(port),
                        '-U', 'postgres', '-d', 'postgres', '-v', 'ON_ERROR_STOP=1'],
                       statement)

    def assert_sql(statement, expected):
        actual = sql(statement)
        assert actual == expected, (statement, expected, actual)

    def wait_fired(job_id):
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            if sql(f"select exists(select from cron.job_run_details where jobid={job_id} and status='succeeded')") == 't':
                return
            time.sleep(.25)
        raise AssertionError('LOGIN sampler did not fire successfully: ' + sql(
            f"select row_to_json(d) from cron.job_run_details d where jobid={job_id}"))

    try:
        execute(['initdb', '-D', directory + '/data', '--auth=trust', '--no-locale'])
        # Empty cron nodename uses libpq's compiled default socket directory.
        # A unique port keeps its socket distinct from the main CI server.
        options = (f'-p {port} -k /var/run/postgresql -h 127.0.0.1 '
                   '-c shared_preload_libraries=pg_cron,pg_stat_statements '
                   '-c cron.database_name=postgres '
                   '-c cron.use_background_workers=off -c cron.host=localhost')
        execute(['pg_ctl', '-D', directory + '/data', '-l', directory + '/server.log',
                 '-o', options, '-w', 'start'])
        running = True
        sql('create extension pg_cron; create extension pg_stat_statements;')
        print(sql("select version(), current_setting('cron.use_background_workers'), current_setting('cron.host')"), flush=True)
        payload = installer.read_text()
        print('Installer SHA256:', hashlib.sha256(payload.encode()).hexdigest(), flush=True)
        sql(payload)
        sql(f'''
            create role {OWNER} login;
            grant pg_monitor to {OWNER};
            alter schema ash owner to {OWNER};
            grant all on all tables in schema ash to {OWNER};
            grant all on all sequences in schema ash to {OWNER};
            grant execute on all routines in schema ash to {OWNER};
            grant usage on schema cron to {OWNER};
            grant select on cron.job to {OWNER};
        ''')
        assert_sql(f"select has_table_privilege('{OWNER}', 'cron.job', 'UPDATE'), has_column_privilege('{OWNER}', 'cron.job', 'nodename', 'UPDATE')", 'f|f')
        workload = subprocess.Popen(
            ['docker', 'exec', '-u', 'postgres', container, 'env',
             'PGAPPNAME=ash_owner_cron_fixture', 'psql', '-X', '-h',
             '/var/run/postgresql', '-p', str(port), '-U', 'postgres', '-d',
             'postgres', '-c', 'select pg_sleep(120)'],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        for _ in range(100):
            if sql("select count(*) from pg_stat_activity where application_name='ash_owner_cron_fixture' and state='active'") == '1':
                break
            time.sleep(.1)
        else:
            raise AssertionError('private tagged workload did not become active')
        # pg_monitor includes pg_read_all_settings; visibility is not UPDATE.
        print(sql(f"set role {OWNER}; select current_setting('cron.host'); select * from ash.start('1 second'); select * from ash.start('1 second'); reset role;"), flush=True)
        assert_sql(f"select count(*)=5 and bool_and(active) and bool_and(nodename='localhost') from cron.job where username='{OWNER}'", 't')
        first_job = int(sql(f"select jobid from cron.job where username='{OWNER}' and jobname='ash_sampler'"))
        wait_fired(first_job)
        assert_sql('select count(*)>0 from ash.sample', 't')
        print('PASS LOGIN owner with pg_monitor, no UPDATE: first/repeated start and actual libpq execution', flush=True)

        # Do not grant cron.alter_job: its absent EXECUTE permission selects
        # the reschedule fallback. Only the needed column can be updated.
        sql(f"grant update(nodename) on cron.job to {OWNER};")
        assert_sql(f"select has_column_privilege('{OWNER}', 'cron.job', 'nodename', 'UPDATE'), has_function_privilege('{OWNER}', 'cron.alter_job(bigint,text,text,text,text,boolean)', 'EXECUTE')", 't|f')
        sql(f"select cron.alter_job(jobid, active=>false, command=>command || ' /* owner privilege fixture */') from cron.job where username='{OWNER}';")
        commands_before = sql(f"select json_object_agg(jobname,command order by jobname) from cron.job where username='{OWNER}'")
        print(sql(f"set role {OWNER}; select * from ash.start('1 second'); reset role;"), flush=True)
        assert_sql(f"select count(*)=5 and bool_and(active) and bool_and(nodename='') from cron.job where username='{OWNER}'", 't')
        assert sql(f"select json_object_agg(jobname,command order by jobname) from cron.job where username='{OWNER}'") == commands_before
        second_job = int(sql(f"select jobid from cron.job where username='{OWNER}' and jobname='ash_sampler'"))
        assert first_job != second_job, 'inactive fallback did not recreate sampler'
        wait_fired(second_job)
        assert_sql('select count(*)>0 from ash.sample', 't')
        print('PASS column-only UPDATE: all five inactive custom commands preserved, socket defaults and real firing', flush=True)

        # An active custom endpoint must not be overwritten on another start.
        sql(f"update cron.job set nodename='localhost' where username='{OWNER}' and jobname in ('ash_sampler','ash_rotation');")
        sql(f"set role {OWNER}; select * from ash.start('1 second'); reset role;")
        assert_sql(f"select bool_and(nodename='localhost') from cron.job where username='{OWNER}' and jobname in ('ash_sampler','ash_rotation')", 't')
        print('PASS active custom sampler/rotation endpoints preserved', flush=True)

        # Stop and remove this private fixture's jobs before changing cadence.
        # No history is silently purged by the API; fixture reset is explicit.
        sql(f"set role {OWNER}; select ash.stop(); reset role; select cron.unschedule(jobid) from cron.job where username='{OWNER}';")
        time.sleep(1)
        sql('truncate ash.sample, ash.rollup_1m, ash.rollup_1h; update ash.config set sample_interval=interval \'60 seconds\';')
        # pg_cron 1.3 supports names but predates alter_job (introduced in1.4).
        # Rename inside one transaction, then roll back the compatibility seam.
        print(sql(f'''
            begin;
            update pg_extension set extversion='1.3' where extname='pg_cron';
            alter function cron.alter_job(bigint,text,text,text,text,boolean)
                rename to ash_fixture_hidden_alter_job;
            set role {OWNER};
            select * from ash.start('60 seconds');
            select * from ash.start('60 seconds');
            do $$ begin
              assert (select count(*)=5 and bool_and(active) from cron.job where username=current_user),
                'absent alter_job fallback lost managed jobs';
            end $$;
            reset role;
            rollback;
        '''), flush=True)
        print('PASS old named-job version with absent alter_job uses fallback', flush=True)
        print('PASS owner cron settings privilege regression suite', flush=True)
    finally:
        if running:
            execute(['pg_ctl', '-D', directory + '/data', '-m', 'immediate', '-w', 'stop'])
        if workload is not None:
            try:
                workload.wait(timeout=5)
            except subprocess.TimeoutExpired:
                workload.kill()
                workload.wait()
        # This directory came from mktemp above; never remove the CI PGDATA.
        execute(['rm', '-rf', '--', directory])


if __name__ == '__main__':
    main()
