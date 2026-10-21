#!/usr/bin/env python3
"""Supervisor-only reproducible functional fixtures. Not investigator documentation."""
import argparse
import fcntl
import datetime as dt
import hashlib
import json
import os
from pathlib import Path
import random
import re
import shutil
import uuid
import secrets
import signal
import subprocess
import threading
import time

import psycopg
from psycopg.rows import dict_row
from psycopg import sql

HERE = Path(__file__).resolve().parent
ROOT = PRIVATE = PUBLIC = None
PREFIX = None
SOURCE = 'fe3e7b4dc56f5be9977624de7c549165bc180882'
DOCS = '95e1ecf83ce68ab9423af2568af678b87c8d9e2f'
STOP = threading.Event()


def now():
    return dt.datetime.now(dt.timezone.utc).isoformat()


def dump(path, obj):
    path.write_text(json.dumps(obj, indent=2, default=str) + '\n')


def log(path, obj):
    with path.open('a') as out:
        out.write(json.dumps(obj, default=str) + '\n')


def sh(args, **kwargs):
    return subprocess.run(args, check=True, text=True, capture_output=True, **kwargs).stdout


def connect(case, user='postgres', app='monitor'):
    return psycopg.connect(host='127.0.0.1', port=case['port'], dbname='appdb',
                          user=user, password=case['passwords'][user],
                          application_name=app, autocommit=True,
                          connect_timeout=5, row_factory=dict_row)


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def validate_owned(case, inspected, manifest):
    """Fail closed before any cleanup mutation; names alone never prove ownership."""
    prefix = manifest['prefix']
    if not re.fullmatch(r'pgash_blind_[a-z0-9_]+', prefix):
        raise RuntimeError('invalid ownership prefix')
    if not case['container'].startswith(prefix + '_'):
        raise RuntimeError('ownership mismatch')
    labels = inspected['Config'].get('Labels') or {}
    if (inspected['Id'] != case['container_id']
            or inspected['Name'] != '/' + case['container']
            or labels.get('pgash-blind-run') != manifest['run_id']):
        raise RuntimeError('container identity mismatch')


def setup(args):
    if (PRIVATE / 'manifest.json').exists():
        raise RuntimeError('run directory already initialized; use a new run directory')
    if not args.sql_file or not args.docs_dir or not args.prefix:
        raise RuntimeError('setup requires --sql-file, --docs-dir and --prefix')
    if not re.fullmatch(r'pgash_blind_[a-z0-9_]+', args.prefix):
        raise RuntimeError('prefix must match pgash_blind_[a-z0-9_]+')
    for relative in ('README.md', 'docs/REFERENCE.md', 'examples/llm-prompt.md',
                     'examples/llm-investigation.sql'):
        if not (args.docs_dir / relative).is_file():
            raise RuntimeError('missing documentation: ' + relative)
    PRIVATE.mkdir(mode=0o700, parents=True, exist_ok=True)
    PUBLIC.mkdir(mode=0o700, parents=True, exist_ok=True)
    (ROOT / 'artifacts').mkdir()
    shutil.copyfile(args.sql_file, ROOT / 'artifacts/ash-install.sql')
    for relative in ('README.md', 'docs/REFERENCE.md', 'examples/llm-prompt.md',
                     'examples/llm-investigation.sql'):
        target = PUBLIC / 'docs' / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(args.docs_dir / relative, target)
    seed = args.seed
    rng = random.Random(seed)
    mechanisms = ['transactionid', 'relation', 'cpu', 'sleep', 'idletx']
    rng.shuffle(mechanisms)
    cases = []
    for mechanism in mechanisms:
        cid = ''.join(rng.choices('abcdefghjkmnpqrstuvwxyz23456789', k=8))
        case = dict(id=cid, mechanism=mechanism, container=PREFIX + '_' + cid,
                    port=None, passwords={'postgres': secrets.token_urlsafe(24),
                                         'service': secrets.token_urlsafe(24),
                                         'observer_' + cid: secrets.token_urlsafe(24)})
        cases.append(case)
    manifest = dict(created=now(), seed=seed, model=args.model,
                    sql_commit=args.sql_revision, docs_commit=args.docs_revision,
                    sql_sha256=sha(ROOT / 'artifacts/ash-install.sql'),
                    image_id=sh(['docker', 'image', 'inspect', args.image, '--format', '{{.Id}}']).strip(),
                    prefix=args.prefix, run_id=uuid.uuid4().hex,
                    limits={'memory': '192m', 'cpus': '0.25', 'shared_buffers': '16MB', 'max_connections': 16},
                    retention='Default raw retention: one day plus current partial; unchanged.',
                    blind_limit='Procedural isolation on shared filesystem, not a security boundary.', cases=cases)
    dump(PRIVATE / 'manifest.json', manifest)
    os.chmod(PRIVATE / 'manifest.json', 0o600)
    rubric = json.loads((HERE / 'rubric.json').read_text())
    rubric['frozen'] = now()
    dump(PRIVATE / 'rubric-frozen.json', rubric)
    dump(PRIVATE / 'inputs-frozen.json', {
        'at': now(), 'controller_sha256': sha(Path(__file__)),
        'wrapper_sha256': sha(HERE / 'reader.py'),
        'rubric_sha256': sha(PRIVATE / 'rubric-frozen.json'),
        'sql_sha256': manifest['sql_sha256'],
        'docs': {str(p.relative_to(PUBLIC / 'docs')): sha(p)
                 for p in (PUBLIC / 'docs').rglob('*') if p.is_file()}})
    for case in cases:
        cdir = PRIVATE / case['id']
        cdir.mkdir(exist_ok=True)
        if (cdir / 'setup.json').exists():
            continue
        role = 'observer_' + case['id']
        env = os.environ.copy()
        env['POSTGRES_PASSWORD'] = case['passwords']['postgres']
        if not case.get('container_id'):
            out = sh(['docker', 'run', '-d', '--name', case['container'], '--label', 'pgash-blind-run=' + manifest['run_id'],
                      '--memory=192m', '--memory-swap=192m', '--cpus=0.25', '--pids-limit=96', '--shm-size=32m',
                      '-p', '127.0.0.1::5432', '-e', 'POSTGRES_PASSWORD', '-e', 'POSTGRES_DB=appdb',
                      '-e', 'POSTGRES_INITDB_ARGS=--auth-host=scram-sha-256 --auth-local=scram-sha-256',
                      manifest['image_id'], '-c', 'shared_preload_libraries=pg_stat_statements', '-c', 'compute_query_id=on',
                      '-c', 'shared_buffers=16MB', '-c', 'max_connections=16', '-c', 'work_mem=1MB',
                      '-c', 'maintenance_work_mem=16MB', '-c', 'log_connections=on', '-c', 'log_timezone=UTC',
                      '-c', 'timezone=UTC', '-c', 'jit=off'], env=env).strip()
            case['container_id'] = out
            dump(PRIVATE / 'manifest.json', manifest)
        mapping = json.loads(sh(['docker', 'inspect', case['container']]))[0]
        validate_owned(case, mapping, manifest)
        binding = mapping['NetworkSettings']['Ports']['5432/tcp'][0]
        if (binding['HostIp'] != '127.0.0.1'
                or mapping['HostConfig']['Memory'] != 192 * 1024 * 1024
                or mapping['HostConfig']['NanoCpus'] != 250000000):
            raise RuntimeError('container binding/resource contract mismatch')
        case['port'] = int(binding['HostPort'])
        dump(PRIVATE / 'manifest.json', manifest)
        for attempt in range(90):
            try:
                conn = connect(case)
                break
            except psycopg.Error:
                time.sleep(1)
        else:
            raise RuntimeError('Postgres did not become ready: ' + case['id'])
        with conn:
            conn.execute('create extension if not exists pg_stat_statements')
            installenv = os.environ.copy()
            installenv['PGPASSWORD'] = case['passwords']['postgres']
            (cdir / 'install.log').write_text(sh(['psql', '-X', '-v', 'ON_ERROR_STOP=1', '-h', '127.0.0.1', '-p', str(case['port']), '-U', 'postgres', '-d', 'appdb', '-f', str(ROOT / 'artifacts/ash-install.sql')], env=installenv))
            conn.execute('set pg_stat_statements.track_utility = off')
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
            conn.execute('set pg_stat_statements.track_utility = on')
            conn.execute("select ash.start('1 second')")
            dump(cdir / 'setup.json', {'at': now(), 'server_version': conn.execute('show server_version').fetchone(),
                                     'status': conn.execute('select * from ash.status()').fetchall(),
                                     'hba': conn.execute('select line_number, type, address, auth_method, error from pg_hba_file_rules').fetchall()})
        idir = PUBLIC / case['id']
        idir.mkdir(exist_ok=True)
        pgpass = idir / '.pgpass'
        pgpass.write_text(f"127.0.0.1:{case['port']}:appdb:{role}:{case['passwords'][role]}\n")
        os.chmod(pgpass, 0o600)
        wrapper = idir / 'psql'
        shutil.copyfile(HERE / 'reader.py', wrapper)
        os.chmod(wrapper, 0o700)
        dump(idir / 'connection.json', {'port': case['port'], 'role': role})
        checks = []
        with connect(case, user=role, app='permission_check') as reader:
            reader.execute('select * from ash.status()').fetchall()
            # Disable the advisory setting to test actual ACL enforcement.
            reader.execute('set default_transaction_read_only = off')
            for statement in ('select * from records limit 1',
                              'update records set value=1 where id=1',
                              'create table public.permission_probe (x integer)',
                              'update ash.config set sampling_enabled=false', 'select ash.stop()'):
                try:
                    reader.execute(statement)
                except psycopg.errors.InsufficientPrivilege as exc:
                    checks.append({'sql': statement, 'sqlstate': exc.sqlstate, 'denied': True})
                else:
                    raise RuntimeError('reader unexpectedly permitted: ' + statement)
        dump(cdir / 'permission-check.json', {'at': now(), 'checks': checks, 'all_denied': True})
        dump(idir / 'handoff.json', {'case_id': case['id'], 'psql': str(wrapper),
                                   'database': 'appdb', 'docs': str(PUBLIC / 'docs')})
    dump(PRIVATE / 'manifest.json', manifest)
    print(json.dumps({'setup': 'complete', 'case_ids': [x['id'] for x in cases]}), flush=True)


def scheduler(case):
    path = PRIVATE / case['id']
    with connect(case, app='collector') as conn:
        conn.execute("set statement_timeout = '2s'")
        anchor = time.monotonic()
        tick = 0
        while not STOP.is_set():
            deadline = anchor + tick
            delay = deadline - time.monotonic()
            if delay > 0:
                STOP.wait(delay)
            if STOP.is_set():
                break
            late = max(0, time.monotonic() - deadline)
            started = now()
            try:
                result = conn.execute('select ash.take_sample() as rows').fetchone()
                observations = conn.execute("""select clock_timestamp() as observed_at, pid, state, wait_event_type,
                       wait_event, query_id, query, xact_start, query_start,
                       pg_blocking_pids(pid) as blocking_pids
                    from pg_stat_activity where application_name = 'service' order by pid""").fetchall()
                log(path / 'observations.jsonl', {'at': now(), 'sessions': observations})
                if any(row['blocking_pids'] for row in observations):
                    locks = conn.execute("""select clock_timestamp() as observed_at, pid, locktype,
                        relation::regclass::text, transactionid, classid, objid, objsubid, mode, granted
                        from pg_locks where pid = any(%s) order by pid, locktype, granted""",
                                         [[row['pid'] for row in observations]]).fetchall()
                    log(path / 'locks.jsonl', {'at': now(), 'locks': locks})
                if tick % 15 == 0:
                    conn.execute('select ash.rollup_minute()')
                log(path / 'sampler.jsonl', {'started': started, 'ended': now(), 'tick': tick, 'late_seconds': late, **result})
            except Exception as exc:
                log(path / 'sampler.jsonl', {'started': started, 'ended': now(), 'tick': tick, 'error': str(exc)})
            tick += 1
            if time.monotonic() > anchor + tick + 1:
                skipped = int(time.monotonic() - anchor) - tick
                log(path / 'sampler.jsonl', {'at': now(), 'skipped_deadlines': skipped})
                tick = int(time.monotonic() - anchor) + 1


def spike(case, seconds, index):
    path = PRIVATE / case['id']
    start = now()
    conns = []
    threads = []
    errors = []
    mechanism = case['mechanism']

    def waiter(conn, query, params):
        try:
            conn.execute(query, params, prepare=(mechanism == 'relation')).fetchall() if query.startswith('select') else conn.execute(query, params)
        except Exception as exc:
            errors.append(str(exc))

    try:
        if mechanism in ('transactionid', 'relation'):
            if mechanism == 'relation':
                query = 'select payload from records where id = %s'
                # Warm the exact extended-protocol prepared statement before
                # blocking Bind's relation-lock acquisition (PG18).
                for i in (1, 2):
                    conn = connect(case, 'service', 'service')
                    conns.append(conn)
                    conn.execute("set statement_timeout = '45s'")
                    conn.execute(query, [i], prepare=True).fetchall()
                blocker = connect(case, 'service', 'service')
                conns.append(blocker)
                blocker.execute('begin')
                blocker.execute('lock table records in access exclusive mode')
                waiters = conns[:2]
            else:
                blocker = connect(case, 'service', 'service')
                conns.append(blocker)
                blocker.execute('begin')
                blocker.execute('update records set value = value + 1 where id in (1, 2)')
                query = 'update records set value = value + 1 where id = %s'
                waiters = []
                for _ in (1, 2):
                    conn = connect(case, 'service', 'service')
                    conns.append(conn)
                    conn.execute("set statement_timeout = '45s'")
                    waiters.append(conn)
            for i, conn in enumerate(waiters, 1):
                thread = threading.Thread(target=waiter, args=(conn, query, [i]))
                thread.start()
                threads.append(thread)
            time.sleep(seconds)
            blocker.execute('rollback')
        elif mechanism == 'sleep':
            for _ in range(2):
                conn = connect(case, 'service', 'service')
                conns.append(conn)
                thread = threading.Thread(target=waiter, args=(conn, 'select pg_sleep(%s)', [seconds]))
                thread.start()
                threads.append(thread)
        elif mechanism == 'cpu':
            conn = connect(case, 'service', 'service')
            conns.append(conn)
            conn.execute("set statement_timeout = '5s'")
            deadline = time.monotonic() + seconds
            while time.monotonic() < deadline:
                conn.execute('select sum(length(md5(i::text))) from generate_series(1, 250000) as i').fetchall()
        else:
            for i in (1, 2):
                conn = connect(case, 'service', 'service')
                conns.append(conn)
                conn.execute('begin')
                conn.execute('select payload from records where id = %s', [i]).fetchall()
            time.sleep(seconds)
            for conn in conns:
                conn.execute('rollback')
        for thread in threads:
            thread.join(timeout=seconds + 5)
            if thread.is_alive():
                raise RuntimeError('workload did not exit')
        if errors:
            raise RuntimeError(errors)
    finally:
        for conn in reversed(conns):
            conn.close()
    end = now()
    log(path / 'controller.jsonl', {'phase': 'spike', 'index': index, 'start': start, 'end': end, 'seconds_requested': seconds, 'errors': errors})
    return {'index': index, 'start': start, 'end': end}


def calm(case, seconds, label):
    start = now()
    if STOP.wait(seconds):
        raise RuntimeError('collection interrupted')
    log(PRIVATE / case['id'] / 'controller.jsonl', {'phase': label, 'start': start, 'end': now(), 'seconds': seconds, 'sessions': 0})


def certify(case, windows, started):
    path = PRIVATE / case['id']
    rubric = json.loads((PRIVATE / 'rubric-frozen.json').read_text())
    expected = rubric['expected'][case['mechanism']]
    if STOP.is_set():
        raise RuntimeError('collection interrupted')
    obs = [json.loads(line) for line in (path / 'observations.jsonl').read_text().splitlines()]
    with connect(case) as conn:
        conn.execute('select ash.rollup_minute()')
        samples = conn.execute('select * from ash.samples(%s::timestamptz, clock_timestamp(), 100000)', [started]).fetchall()
        dump(path / 'samples-certified.json', samples)
        pgss = conn.execute('select queryid, query, calls, total_exec_time from pg_stat_statements order by queryid').fetchall()
        dump(path / 'pgss-certified.json', pgss)
        status = conn.execute('select * from ash.status()').fetchall()
        dump(path / 'status-certified.json', status)
        rollups = conn.execute('select * from ash.rollup_1m order by ts').fetchall()
        dump(path / 'rollups-certified.json', rollups)
        live = conn.execute("select pid from pg_stat_activity where application_name = 'service'").fetchall()
    results = []
    for window in windows:
        within = lambda stamp: window['start'] <= stamp <= window['end']
        # Datetimes have PostgreSQL's UTC format; normalize before comparison.
        normalize = lambda stamp: dt.datetime.fromisoformat(str(stamp)).isoformat()
        matching_samples = [row for row in samples if within(normalize(row['sample_time'])) and row['wait_event'] == expected['event']]
        independent = [row for batch in obs if within(batch['at']) for row in batch['sessions']
                       if (f"{row['wait_event_type']}:{row['wait_event']}" == expected['event']
                           or (expected['event'] == 'CPU*' and row['state'] == 'active'
                               and row['wait_event_type'] is None and row['wait_event'] is None))]
        if case['mechanism'] == 'idletx':
            independent = [row for row in independent if row['state'] == 'idle in transaction']
        result = {'window': window, 'ash_matching_rows': len(matching_samples), 'independent_matching_observations': len(independent),
                  'query_ids': sorted(set(row['query_id'] for row in matching_samples if row['query_id'] is not None))}
        result['valid'] = len(matching_samples) >= 8 and len(independent) >= 8 and bool(result['query_ids'])
        results.append(result)
    ticks = [json.loads(line) for line in (path / 'sampler.jsonl').read_text().splitlines()]
    errors = [x for x in ticks if 'error' in x or 'skipped_deadlines' in x]
    answer = {'case_id': case['id'], 'frozen_rubric_sha256': hashlib.sha256((PRIVATE / 'rubric-frozen.json').read_bytes()).hexdigest(),
              'mechanism': case['mechanism'], 'expected': expected, 'observed_windows': results,
              'certified_at': now(), 'service_sessions_remaining': live,
              'sampler_errors_or_skips': errors, 'rollup_rows': len(rollups),
              'certified': all(x['valid'] for x in results) and not live and not errors and bool(rollups)}
    dump(path / 'answer-sealed.json', answer)
    if not answer['certified']:
        raise RuntimeError('INVALID fixture ' + case['id'] + ': ' + json.dumps(answer))
    dump(PRIVATE / case['id'] / 'ready.json', {'case_id': case['id'], 'ready': now(), 'database': 'appdb',
          'psql': str(PUBLIC / case['id'] / 'psql'), 'docs': str(PUBLIC / 'docs')})
    print(json.dumps({'READY': case['id'], 'handoff': str(PUBLIC / case['id'] / 'handoff.json')}), flush=True)


def run(keep):
    manifest = json.loads((PRIVATE / 'manifest.json').read_text())
    threads = []
    for case in manifest['cases']:
        thread = threading.Thread(target=scheduler, args=(case,), daemon=True)
        thread.start()
        threads.append(thread)
    try:
        for case in manifest['cases']:
            if (PRIVATE / case['id'] / 'ready.json').exists():
                continue
            if (PRIVATE / case['id'] / 'pre-run.json').exists():
                raise RuntimeError('incomplete fixture retained; start a new run, not a silent retry')
            started = now()
            dump(PRIVATE / case['id'] / 'pre-run.json', {'started': started, 'expected': json.loads((PRIVATE / 'rubric-frozen.json').read_text())['expected'][case['mechanism']]})
            calm(case, 35, 'baseline')
            windows = [spike(case, 14, 1)]
            calm(case, 20, 'intermission')
            windows.append(spike(case, 18, 2))
            calm(case, 35, 'recovery')
            # Only closed minutes roll up; retain real time, never restamp.
            with connect(case) as conn:
                remaining = conn.execute("select extract(epoch from date_trunc('minute', clock_timestamp()) + interval '1 minute' - clock_timestamp()) as seconds").fetchone()['seconds']
            STOP.wait(float(remaining) + 0.1)
            if STOP.is_set():
                raise RuntimeError('collection interrupted before certification')
            certify(case, windows, started)
        print(json.dumps({'all_ready': now(), 'sampler_pid': os.getpid()}), flush=True)
        if keep:
            while not STOP.wait(1):
                pass
    finally:
        STOP.set()
        for thread in threads:
            thread.join(timeout=5)


def cleanup():
    manifest = json.loads((PRIVATE / 'manifest.json').read_text())
    cases = [case for case in manifest['cases'] if case.get('container_id')]
    # Validate every resource first; a mismatch must remove nothing.
    for case in cases:
        inspected = json.loads(sh(['docker', 'inspect', case['container']]))[0]
        validate_owned(case, inspected, manifest)
    outcomes = []
    for case in cases:
        dest = PRIVATE / case['id']
        captured = subprocess.run(['docker', 'logs', '--timestamps', case['container']],
                                  check=True, text=True, capture_output=True)
        (dest / 'container.stdout.log').write_text(captured.stdout)
        (dest / 'container.stderr.log').write_text(captured.stderr)
        sh(['docker', 'rm', '-f', '-v', case['container']])
        outcomes.append({'container': case['container'], 'removed': now()})
    dump(PRIVATE / 'cleanup.json', outcomes)


def main():
    global ROOT, PRIVATE, PUBLIC, PREFIX
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=['setup', 'run', 'manifest', 'cleanup'])
    parser.add_argument('--run-dir', type=Path, required=True)
    parser.add_argument('--prefix', help='unique pgash_blind_[a-z0-9_]+ name, required for setup')
    parser.add_argument('--sql-file', type=Path)
    parser.add_argument('--docs-dir', type=Path)
    parser.add_argument('--sql-revision', default=SOURCE)
    parser.add_argument('--docs-revision', default=DOCS)
    parser.add_argument('--image', default='postgres:18', help='locally available PG18 image')
    parser.add_argument('--model', default='gpt-6.1-sol', help='investigator model recorded in manifest')
    parser.add_argument('--seed', type=int, default=202610210285)
    parser.add_argument('--keep-sampling', action='store_true')
    args = parser.parse_args()
    os.umask(0o077)
    ROOT = args.run_dir.resolve()
    PRIVATE, PUBLIC = ROOT / 'supervisor', ROOT / 'investigators'
    PREFIX = args.prefix
    if args.action != 'setup':
        manifest = json.loads((PRIVATE / 'manifest.json').read_text())
        PREFIX = manifest['prefix']
    signal.signal(signal.SIGTERM, lambda *_: STOP.set())
    signal.signal(signal.SIGINT, lambda *_: STOP.set())
    ROOT.mkdir(mode=0o700, parents=True, exist_ok=True)
    with (ROOT / '.controller.lock').open('a') as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise RuntimeError('another controller owns this run directory') from None
        if args.action == 'setup':
            setup(args)
        elif args.action == 'run':
            run(args.keep_sampling)
        elif args.action == 'manifest':
            for case in manifest['cases']:
                del case['passwords']
            print(json.dumps(manifest, indent=2))
        else:
            cleanup()


if __name__ == '__main__':
    main()
