#!/usr/bin/env python3
"""Parent-owned replacement of only the invalid fourth fixture; no rubric changes."""
import hashlib
import json
import os
from pathlib import Path
import secrets
import shutil
import signal
import subprocess
import threading
import time

import psycopg
from psycopg import sql
import harness as h

OLD_ID = 'ssfjwgh2'
NEW_ID = 'n6rq9wm3'
ROOT = h.ROOT
PRIVATE = h.PRIVATE
PUBLIC = h.PUBLIC
SOURCE = ROOT / 'artifacts/ash-install.sql'
PATH = PRIVATE / NEW_ID
QUERY = 'select payload from records where id = %s'


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def replace_setup():
    manifest = json.loads((PRIVATE / 'manifest.json').read_text())
    old = next(c for c in manifest['cases'] if c['id'] == OLD_ID)
    assert sha(SOURCE) == manifest['sql_sha256']
    assert manifest['docs_commit'] == '95e1ecf83ce68ab9423af2568af678b87c8d9e2f'
    PATH.mkdir(exist_ok=False)
    role = 'observer_' + NEW_ID
    case = dict(id=NEW_ID, mechanism='relation', container=h.PREFIX + '_' + NEW_ID,
                replaces=OLD_ID, passwords={u: secrets.token_urlsafe(24)
                                           for u in ('postgres', 'service', role)})
    env = dict(os.environ, POSTGRES_PASSWORD=case['passwords']['postgres'])
    case['container_id'] = h.sh([
        'docker', 'run', '-d', '--name', case['container'],
        '--label', 'trial=pgash-blind-20261021', '--memory=192m', '--memory-swap=192m',
        '--cpus=0.25', '--pids-limit=96', '--shm-size=32m', '-p', '127.0.0.1::5432',
        '-e', 'POSTGRES_PASSWORD', '-e', 'POSTGRES_DB=appdb',
        '-e', 'POSTGRES_INITDB_ARGS=--auth-host=scram-sha-256 --auth-local=scram-sha-256',
        manifest['image_id'], '-c', 'shared_preload_libraries=pg_stat_statements',
        '-c', 'compute_query_id=on', '-c', 'shared_buffers=16MB', '-c', 'max_connections=16',
        '-c', 'work_mem=1MB', '-c', 'maintenance_work_mem=16MB', '-c', 'log_connections=on',
        '-c', 'log_timezone=UTC', '-c', 'timezone=UTC', '-c', 'jit=off'], env=env).strip()
    info = json.loads(h.sh(['docker', 'inspect', case['container']]))[0]
    binding = info['NetworkSettings']['Ports']['5432/tcp'][0]
    assert binding['HostIp'] == '127.0.0.1'
    assert info['HostConfig']['Memory'] == 192 * 1024 * 1024
    assert info['HostConfig']['NanoCpus'] == 250000000
    case['port'] = int(binding['HostPort'])
    h.dump(PATH / 'case-private.json', case)
    os.chmod(PATH / 'case-private.json', 0o600)
    for _ in range(60):
        try:
            conn = h.connect(case)
            break
        except psycopg.Error:
            time.sleep(1)
    else:
        raise RuntimeError('Replacement did not become ready')
    with conn:
        conn.execute('create extension pg_stat_statements')
        installenv = dict(os.environ, PGPASSWORD=case['passwords']['postgres'])
        (PATH / 'install.log').write_text(h.sh([
            'psql', '-X', '-v', 'ON_ERROR_STOP=1', '-h', '127.0.0.1', '-p', str(case['port']),
            '-U', 'postgres', '-d', 'appdb', '-f', str(SOURCE)], env=installenv))
        for user in ('service', role):
            conn.execute(sql.SQL('create role {} login password {}').format(
                sql.Identifier(user), sql.Literal(case['passwords'][user])))
        for setting, value in [('default_transaction_read_only', 'on'),
                               ('statement_timeout', '10s'), ('lock_timeout', '2s')]:
            conn.execute(sql.SQL('alter role {} set {} = {}').format(
                sql.Identifier(role), sql.Identifier(setting), sql.Literal(value)))
        conn.execute(sql.SQL('grant pg_read_all_stats to {}').format(sql.Identifier(role)))
        conn.execute('select ash.grant_reader(%s)', [role])
        conn.execute('create table records (id integer primary key, value integer not null, payload text not null)')
        conn.execute("insert into records select i, 0, repeat('x', 80) from generate_series(1, 20) as i")
        conn.execute('grant select, update on records to service')
        h.dump(PATH / 'setup.json', {
            'at': h.now(), 'server_version': conn.execute('show server_version').fetchone(),
            'hba': conn.execute('select line_number, type, address, auth_method, error from pg_hba_file_rules').fetchall(),
            'limits': manifest['limits'], 'sql_sha256': sha(SOURCE), 'docs_commit': manifest['docs_commit'],
            'frozen_rubric_sha256': sha(PRIVATE / 'rubric-frozen.json')})
    # Verify real permissions separately, before exposing the wrapper to an investigator.
    checks = []
    with h.connect(case, role, 'permission_check') as reader:
        reader.execute('set default_transaction_read_only = off')
        reader.execute('select * from ash.status()').fetchall()
        for statement in ['select * from records limit 1', 'update records set value=1 where id=1',
                          'create table public.permission_probe (x integer)',
                          'update ash.config set sampling_enabled=false', 'select ash.stop()']:
            try:
                reader.execute(statement)
                raise RuntimeError('Unexpected permission: ' + statement)
            except psycopg.errors.InsufficientPrivilege as exc:
                checks.append({'sql': statement, 'denied': True, 'sqlstate': exc.sqlstate})
    h.dump(PATH / 'permission-check.json', {'at': h.now(), 'checks': checks, 'all_denied': True})
    idir = PUBLIC / NEW_ID
    idir.mkdir(exist_ok=False)
    pgpass = idir / '.pgpass'
    pgpass.write_text(f"127.0.0.1:{case['port']}:appdb:{role}:{case['passwords'][role]}\n")
    os.chmod(pgpass, 0o600)
    wrapper = (PUBLIC / OLD_ID / 'psql').read_text()
    wrapper = wrapper.replace(OLD_ID, NEW_ID).replace(f"PGPORT='{old['port']}'", f"PGPORT='{case['port']}'")
    (idir / 'psql').write_text(wrapper)
    os.chmod(idir / 'psql', 0o700)
    h.dump(idir / 'handoff.json', {'case_id': NEW_ID, 'psql': str(idir / 'psql'),
                                 'database': 'appdb', 'docs': str(PUBLIC / 'docs')})
    smoke = subprocess.run([str(idir / 'psql'), '-Atc', 'select * from ash.status()'],
                           input='', text=True, capture_output=True, check=True)
    assert smoke.stdout
    h.dump(PATH / 'wrapper-final.json', {'sha256': sha(idir / 'psql'),
                                       'smoke_audit': str(idir / 'audit.jsonl')})
    # Preserve the invalid attempt and stop only its exact, labelled container.
    oldinfo = json.loads(h.sh(['docker', 'inspect', old['container']]))[0]
    assert oldinfo['Id'] == old['container_id']
    assert oldinfo['Config']['Labels']['trial'] == 'pgash-blind-20261021'
    assert old['container'].startswith(h.PREFIX + '_')
    h.sh(['docker', 'stop', '--time', '10', old['container']])
    archive = PRIVATE / (OLD_ID + '-invalid-attempt1')
    shutil.copytree(PRIVATE / OLD_ID, archive)
    logs = subprocess.run(['docker', 'logs', '--timestamps', old['container']],
                          text=True, capture_output=True, check=True)
    (archive / 'container.stdout.log').write_text(logs.stdout)
    (archive / 'container.stderr.log').write_text(logs.stderr)
    h.dump(archive / 'archive-hashes.json', {p.name: sha(p) for p in archive.iterdir() if p.is_file()})
    # Re-read before replacing only our entry; leave all other case entries intact.
    current = json.loads((PRIVATE / 'manifest.json').read_text())
    index = next(i for i, c in enumerate(current['cases']) if c['id'] == OLD_ID)
    assert current['cases'][index]['container_id'] == old['container_id']
    current['cases'][index] = case
    current.setdefault('invalid_containers', []).append({
        'case_id': OLD_ID, 'container': old['container'], 'container_id': old['container_id'],
        'port': old['port'], 'state': 'stopped; preserve until cleanup authorized',
        'reason': 'Frozen historical query-attribution criterion failed', 'archive': str(archive)})
    temp = PRIVATE / 'manifest.fourth-replacement.tmp'
    h.dump(temp, current)
    os.chmod(temp, 0o600)
    os.replace(temp, PRIVATE / 'manifest.json')
    return case


def prepared_spike(case, seconds, index):
    # Preparing/executing before the blocker allows extended-protocol Bind to
    # report the SELECT's real query ID before reacquiring its relation lock.
    # Reference: REL_18_STABLE postgres.c exec_bind_message, before GetCachedPlan.
    conns = []
    threads = []
    errors = []
    start = h.now()
    try:
        for i in (1, 2):
            conn = h.connect(case, 'service', 'service')
            conns.append(conn)
            conn.execute("set statement_timeout = '45s'")
            conn.execute(QUERY, [i], prepare=True).fetchall()
        blocker = h.connect(case, 'service', 'service')
        conns.append(blocker)
        blocker.execute('begin')
        blocker.execute('lock table records in access exclusive mode')

        def wait(conn, i):
            try:
                conn.execute(QUERY, [i], prepare=True).fetchall()
            except Exception as exc:
                errors.append(str(exc))

        for i, conn in enumerate(conns[:2], 1):
            thread = threading.Thread(target=wait, args=(conn, i))
            thread.start()
            threads.append(thread)
        time.sleep(seconds)
        blocker.execute('rollback')
        for thread in threads:
            thread.join(timeout=10)
            if thread.is_alive():
                raise RuntimeError('Prepared workload did not finish')
        if errors:
            raise RuntimeError(errors)
    finally:
        for conn in reversed(conns):
            conn.close()
    end = h.now()
    h.log(PATH / 'controller.jsonl', {'phase': 'spike', 'index': index, 'start': start,
                                    'end': end, 'seconds_requested': seconds,
                                    'preparation': 'same SELECT executed with prepare=True before blocker',
                                    'errors': errors})
    return {'index': index, 'start': start, 'end': end}


def main():
    signal.signal(signal.SIGTERM, lambda *_: h.STOP.set())
    signal.signal(signal.SIGINT, lambda *_: h.STOP.set())
    case = replace_setup()
    with h.connect(case) as conn:
        assert conn.execute('select count(*) as n from ash.sample').fetchone()['n'] == 0
        conn.execute("select ash.start('1 second')")
    h.dump(PATH / 'method-frozen.json', {
        'at': h.now(), 'replaces': OLD_ID, 'controller_sha256': sha(Path(__file__)),
        'harness_sha256': sha(ROOT / 'harness.py'), 'rubric_sha256': sha(PRIVATE / 'rubric-frozen.json'),
        'sql_sha256': sha(SOURCE), 'docs_commit': h.DOCS,
        'change': 'Fresh case; warm same SELECT via extended prepared execution before blocker. No criteria, ASH rows, or timestamps changed.',
        'source': 'https://raw.githubusercontent.com/postgres/postgres/REL_18_STABLE/src/backend/tcop/postgres.c',
        'parent_clearance': 'Fifth workload certified and ended at 22:09:11 UTC before this replacement started.'})
    thread = threading.Thread(target=h.scheduler, args=(case,), daemon=True)
    thread.start()
    try:
        started = h.now()
        h.dump(PATH / 'pre-run.json', {'started': started})
        h.calm(case, 35, 'baseline')
        windows = [prepared_spike(case, 14, 1)]
        h.calm(case, 20, 'intermission')
        windows.append(prepared_spike(case, 18, 2))
        h.calm(case, 35, 'recovery')
        # Finish the actual minute so minute rollup evidence is meaningful.
        with h.connect(case) as conn:
            remaining = conn.execute("select extract(epoch from date_trunc('minute', clock_timestamp()) + interval '1 minute' - clock_timestamp()) as seconds").fetchone()['seconds']
        h.STOP.wait(float(remaining) + 0.1)
        h.certify(case, windows, started)
        h.dump(PATH / 'delivery-private.json', {
            'at': h.now(), 'handoff': str(PUBLIC / NEW_ID / 'handoff.json'),
            'handoff_sha256': sha(PUBLIC / NEW_ID / 'handoff.json'),
            'certificate_sha256': sha(PATH / 'answer-sealed.json'),
            'controller_sha256': sha(Path(__file__)), 'sampler_pid': os.getpid()})
        print(json.dumps({'ready': NEW_ID, 'handoff': str(PUBLIC / NEW_ID / 'handoff.json')}), flush=True)
        while not h.STOP.wait(1):
            pass
    finally:
        h.STOP.set()
        thread.join(timeout=5)


if __name__ == '__main__':
    main()
