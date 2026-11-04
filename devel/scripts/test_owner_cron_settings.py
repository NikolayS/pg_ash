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

    def sql(statement, database='postgres'):
        return execute(['psql', '-XAt', '-h', '/var/run/postgresql', '-p', str(port),
                        '-U', 'postgres', '-d', database, '-v', 'ON_ERROR_STOP=1'],
                       statement)

    def assert_sql(statement, expected):
        actual = sql(statement)
        assert actual == expected, (statement, expected, actual)

    def wait_fired(job_id, after_run_id=0):
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            if sql(f"select exists(select from cron.job_run_details where jobid={job_id} and runid>{after_run_id} and status='succeeded')") == 't':
                return
            time.sleep(.25)
        raise AssertionError('LOGIN job did not fire successfully: ' + sql(
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
        sql(f'create role {OWNER} login; create schema ash authorization {OWNER};')
        sql('create role ash_owner_outsider; create role ash_owner_reader; create role ash_owner_delegate; create role ash_owner_successor;')
        # Role names requiring identifier quoting must survive both grant blocks.
        quoted_owner = 'ash owner-quoted'
        quoted_database = 'ash_owner_quoted'
        sql(f'create role "{quoted_owner}";')
        sql(f'create database {quoted_database};')
        sql(f'create schema ash authorization "{quoted_owner}";', quoted_database)
        for quoted_pass in ('fresh', 're-apply'):
            sql(payload, quoted_database)
            sql("grant usage on schema ash to ash_owner_outsider; "
                "select ash.grant_reader('ash_owner_reader');", quoted_database)
            sql(f"\\set owner_role '{quoted_owner}'\n\\set identity_start 1\n" +
                (ROOT / 'devel/tests/owner_rotation.sql').read_text(), quoted_database)
            print(f'PASS quoted schema owner: {quoted_pass} rotation and access', flush=True)
        # A new helper can first appear under a different upgrade administrator.
        # Keep the released payload's original owner, including managed-role installs.
        previous_payload = (ROOT / 'sql/ash-install.sql').read_text()
        for kind in ('super', 'plain', 'plain_helper'):
            origin = 'ash_origin_' + kind
            database = 'ash_owner_upgrade_' + kind
            attributes = 'superuser' if kind == 'super' else 'nosuperuser'
            sql(f'create role {origin} {attributes};')
            sql(f'create database {database};')
            sql(f'grant create on database {database} to {origin};')
            sql(f'create schema ash authorization {origin};', database)
            origin_payload = payload if kind == 'plain_helper' else previous_payload
            sql(f'set role {origin};\n' + origin_payload + '\nreset role;', database)
            sql(f'alter schema ash owner to {OWNER};', database)
            for upgrade_pass in ('first', 're-apply'):
                sql(payload, database)
                helper_owner = origin if kind == 'plain_helper' else 'postgres'
                expected = f'{helper_owner}|{origin}|{origin}'
                actual = sql("select pg_get_userbyid(p.proowner), "
                    "pg_get_userbyid(c.relowner), pg_get_userbyid(q.relowner) "
                    "from pg_proc p, pg_class c, pg_class q "
                    "where p.oid='ash._restart_query_map_identity(int)'::regprocedure "
                    "and c.oid='ash.config'::regclass and q.oid='ash.query_map_2'::regclass", database)
                assert actual == expected, (kind, upgrade_pass, actual, expected)
                sql("grant usage on schema ash to ash_owner_outsider; "
                    "select ash.grant_reader('ash_owner_reader');", database)
                sql(f"\\set owner_role {OWNER}\n\\set identity_start 1\n" +
                    (ROOT / 'devel/tests/owner_rotation.sql').read_text(), database)
                print(f'PASS {kind} original installer, {upgrade_pass} upgrade by another admin: rotation and ownership', flush=True)
            sql('alter schema ash owner to ash_owner_successor;', database)
            sql(payload, database)
            # Ownership transfer does not revoke ordinary explicit grants.
            assert sql(f"select has_table_privilege('{OWNER}', 'ash.config', 'UPDATE') "
                f"and has_function_privilege('{OWNER}', 'ash.take_sample()', 'EXECUTE')", database) == 't'
            sql(f"""set role {OWNER};
                do $$ begin
                  begin
                    perform ash._restart_query_map_identity(2);
                    raise exception 'former schema owner retained identity helper access';
                  exception when insufficient_privilege then null;
                  end;
                end $$;
                reset role;
            """, database)
            sql("\\set owner_role ash_owner_successor\n\\set identity_start 1\n" +
                (ROOT / 'devel/tests/owner_rotation.sql').read_text(), database)
            print(f'PASS {kind} original installer: transferred owner rotates; former owner denied private helper', flush=True)
        for installation in ('fresh', 're-apply', 'second re-apply'):
            if installation != 'fresh':
                # Re-apply must still remove legacy admin access from readers.
                sql('grant execute on function ash._admin_funcs(), ash._restart_query_map_identity(int) to ash_owner_reader;')
            sql(payload)
            sql("grant usage on schema ash to ash_owner_outsider; select ash.grant_reader('ash_owner_reader');")
            # Test delegation separately from lifecycle DML rights. Grant options
            # are explicit prerequisites, not part of the installer's contract.
            # Never grant EXECUTE on admin helpers here: that would hide the bug.
            sql(f"""begin;
                grant select on all tables in schema ash to {OWNER} with grant option;
                do $$ declare f record; begin
                  for f in
                    select p.oid::regprocedure as signature
                    from pg_proc as p
                    join pg_namespace as n on n.oid=p.pronamespace
                    where n.nspname='ash' and p.prokind='f'
                      and p.proname::text <> all (ash._admin_funcs())
                  loop
                    execute format('grant execute on function %s to %I with grant option',
                                   f.signature, '{OWNER}');
                  end loop;
                end $$;
                set role {OWNER};
                select ash.grant_reader('ash_owner_delegate');
                reset role;
                set role ash_owner_delegate;
                do $$ begin
                  assert (select count(*) from ash.config)=1,
                    'delegated reader cannot read config';
                  perform * from ash.status();
                  begin
                    perform ash._admin_funcs();
                    raise exception 'reader gained admin helper access';
                  exception when insufficient_privilege then null;
                  end;
                end $$;
                reset role;
                set role {OWNER};
                select ash.revoke_reader('ash_owner_delegate');
                reset role;
                set role ash_owner_delegate;
                do $$ begin
                  begin
                    perform * from ash.config;
                    raise exception 'revoked reader still reads config';
                  exception when insufficient_privilege then null;
                  end;
                  begin
                    perform * from ash.status();
                    raise exception 'revoked reader still calls status';
                  exception when insufficient_privilege then null;
                  end;
                end $$;
                reset role;
                rollback;
            """)
            print(f'PASS {installation}: schema owner grants/revokes readers; admin helper stays private', flush=True)
            # Use real denied operations, not only ACL catalog checks.
            for role in ('ash_owner_outsider', 'ash_owner_reader'):
                commands = ["perform ash._admin_funcs()",
                            "update ash.config set sampling_enabled=false",
                            "delete from ash.sample"]
                if role == 'ash_owner_outsider':
                    commands += ["select * from ash.config", "select * from ash.sample"]
                for command in commands:
                    sql(f"""set role {role};
                        do $$ begin
                          {command};
                          raise exception 'unexpected access: {role}: {command}';
                        exception when insufficient_privilege then null;
                        end $$;
                    """)
            print(f'PASS {installation}: unrelated role denied reads/writes; reader denied writes', flush=True)
            sql(f"""set role {OWNER};
                create function ash.owner_trigger_probe() returns trigger
                language plpgsql as $$ begin return new; end $$;
                do $$ begin
                  create trigger owner_trigger_probe before update on ash.config
                    for each row execute function ash.owner_trigger_probe();
                  raise exception 'unexpected TRIGGER privilege on installer-owned config';
                exception when insufficient_privilege then null;
                end $$;
                do $$ begin
                  create table ash.owner_reference_probe (
                    singleton boolean references ash.config(singleton));
                  raise exception 'unexpected REFERENCES privilege on installer-owned config';
                exception when insufficient_privilege then null;
                end $$;
                drop function ash.owner_trigger_probe();
                reset role;
            """)
            print(f'PASS {installation}: schema owner denied TRIGGER and REFERENCES on installer-owned tables', flush=True)
            identity_start = 7 if installation == 're-apply' else 1
            sql(f"\\set owner_role {OWNER}\n\\set identity_start {identity_start}\n" +
                (ROOT / 'devel/tests/owner_rotation.sql').read_text())
            print(f'PASS {installation}: forced rotation, rollback, identity and helper security', flush=True)

        sql(f'''
            grant pg_monitor to {OWNER};
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
        assert_sql(f"set role {OWNER}; select ash.take_sample() > 0;", 'SET\nt')
        assert_sql(f"set role {OWNER}; select value from ash.status() where metric='sampling_enabled';", 'SET\ntrue')
        print('PASS split-owner take_sample and status after re-apply', flush=True)
        # pg_monitor includes pg_read_all_settings; visibility is not UPDATE.
        print(sql(f"set role {OWNER}; select current_setting('cron.host'); select * from ash.start('1 second'); select * from ash.start('1 second'); reset role;"), flush=True)
        assert_sql(f"select count(*)=5 and bool_and(active) and bool_and(nodename='localhost') from cron.job where username='{OWNER}'", 't')
        first_job = int(sql(f"select jobid from cron.job where username='{OWNER}' and jobname='ash_sampler'"))
        wait_fired(first_job)
        assert_sql('select count(*)>0 from ash.sample', 't')
        print('PASS LOGIN owner with pg_monitor, no UPDATE: first/repeated start and actual libpq execution', flush=True)

        rotation_job = int(sql(f"select jobid from cron.job where username='{OWNER}' and jobname='ash_rotation'"))
        prior_run = int(sql(f"select coalesce(max(runid),0) from cron.job_run_details where jobid={rotation_job}"))
        sql("truncate ash.sample_2, ash.query_map_2 restart identity; "
            "insert into ash.query_map_2(query_id) values (909090),(909091); "
            "update ash.config set current_slot=0, rotated_at=now()-interval '2 days';")
        sql(f"select cron.alter_job({rotation_job}, schedule=>'1 second');")
        wait_fired(rotation_job, prior_run)
        assert_sql('select current_slot from ash.config', '1')
        assert_sql('select count(*) from ash.sample_2', '0')
        assert_sql('select count(*) from ash.query_map_2', '0')
        assert_sql(f"set role {OWNER}; insert into ash.query_map_2(query_id) values (909092) returning id;", 'SET\n1\nINSERT 0 1')
        sql(f"select cron.alter_job({rotation_job}, schedule=>'0 0 * * *');")
        print('PASS scheduled split-owner CALL rotation advances slot and resets identity', flush=True)

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
        sql(f"set role {OWNER}; select ash.stop(); reset role;")
        assert_sql(f"set role {OWNER}; select value from ash.status() where metric='sampling_enabled';", 'SET\nfalse')
        assert_sql(f"select count(*) from cron.job where username='{OWNER}'", '0')
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
        sql("""begin;
            alter schema ash owner to postgres;
            update ash.config set rollup_1m_retention_days=32;
            select ash.rebuild_partitions(32, 'yes');
            update ash.config set current_slot=29, rotated_at=now()-interval '2 days';
            insert into ash.query_map_31(query_id) values (909090),(909091);
            do $$ declare result text; next_id int; begin
              result := ash.rotate();
              assert result='rotated: slot 29 -> 30, truncated slot 31 (sample + query_map)', result;
              assert ash.current_slot()=30, 'maximum ring rotation did not advance';
              assert (select count(*) from ash.query_map_31)=0, 'maximum ring dictionary survived';
              insert into ash.query_map_31(query_id) values (909092) returning id into next_id;
              assert next_id=1, 'maximum ring identity did not restart';
            end $$;
            rollback;
        """)
        print('PASS maximum 32-slot ring rotates slot 31 and restarts identity', flush=True)
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
