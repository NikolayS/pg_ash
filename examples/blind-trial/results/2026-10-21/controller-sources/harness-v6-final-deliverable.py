#!/usr/bin/env python3
"""Supervisor-only reproducible functional fixtures. Not investigator documentation."""
import argparse
import datetime as dt
import hashlib
import json
import os
from pathlib import Path
import random
import secrets
import signal
import subprocess
import threading
import time

import psycopg
from psycopg.rows import dict_row

ROOT = Path(__file__).resolve().parent
PRIVATE = ROOT / 'supervisor'
PUBLIC = ROOT / 'investigators'
PREFIX = 'pgash_blind_20261021'
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


def setup(seed):
    PRIVATE.mkdir(parents=True, exist_ok=True)
    PUBLIC.mkdir(parents=True, exist_ok=True)
    existing = (PRIVATE / 'manifest.json').exists()
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
    manifest = dict(created=now(), seed=seed, model='gpt-6.1-sol', sql_commit=SOURCE,
                    docs_commit=DOCS, sql_sha256=hashlib.sha256((ROOT / 'artifacts/ash-install.sql').read_bytes()).hexdigest(),
                    image_id=sh(['docker', 'image', 'inspect', 'postgres:18', '--format', '{{.Id}}']).strip(),
                    limits={'memory': '192m', 'cpus': '0.25', 'shared_buffers': '16MB', 'max_connections': 16},
                    retention='Default 3 slots x 1 day: ~1 day raw, 30 days minute; unchanged, comfortably exceeds trial.',
                    blind_limit='Procedural isolation on shared filesystem, not a security boundary.', cases=cases)
    if existing:
        manifest = json.loads((PRIVATE / 'manifest.json').read_text())
        cases = manifest['cases']
    dump(PRIVATE / 'manifest.json', manifest)
    os.chmod(PRIVATE / 'manifest.json', 0o600)
    rubric = dict(frozen=now(), issue=285, phase_seconds=[35, 14, 20, 18, 35],
                  scoring={
                      'window': '2: both observed spikes or enclosing window with calm/recovery; 1: one spike or loose correct window; 0: wrong window.',
                      'mechanism': '2: observed specific event/mechanism; 1: correct broad class; 0: wrong.',
                      'query': '2: affected sampled query ID/text when present; 1: partial correct attribution or explicitly justified limitation; 0: unsupported/wrong.',
                      'evidence': '2: separates observation from unproven root cause; 1: minor overclaim; 0: invented blocker, CPU utilization or causal assertion.',
                      'next_check': '2: targeted read-only next check; 1: generic reasonable check; 0: unsupported destructive/remediation claim.'},
                  certification='Before handoff each spike must have >=8 matching independent activity observations and >=8 matching actual ASH samples; statement attribution available; no remaining service sessions. Failure invalidates fixture; no weakened thresholds.',
                  expected={
                      'transactionid': {'event': 'Lock:transactionid', 'query': 'update records set value = value + 1 where id = $1', 'acceptable': 'Rows blocked waiting for a transaction to finish; blocker identity and transaction intent cannot be recovered from ASH alone.', 'unsupported': 'Specific blocking PID, owner, long transaction root cause or query plan problem proved by ASH alone.'},
                      'relation': {'event': 'Lock:relation', 'query': 'select payload from records where id = $1', 'acceptable': 'Heavyweight relation-lock contention on the sampled select; inspect live pg_locks/pg_blocking_pids if recurring.', 'unsupported': 'Exact blocker SQL or exact lock mode identified from historical ASH alone.'},
                      'cpu': {'event': 'CPU*', 'query': 'select sum(length(md5(i::text))) from generate_series($1, $2) as i', 'acceptable': 'Active backend with no reported wait, consistent with bounded expression work. CPU* is not measured CPU utilization; container CPU quota may throttle execution.', 'unsupported': 'CPU percentage, host saturation or a proven CPU bottleneck from CPU* alone.'},
                      'sleep': {'event': 'Timeout:PgSleep', 'query': 'select pg_sleep($1)', 'acceptable': 'Explicit server-side sleep dominates sampled active time; elapsed active time is not CPU utilization.', 'unsupported': 'Disk, lock, CPU saturation or production reason for sleep proved.'},
                      'idletx': {'event': 'Client:ClientRead', 'query': 'select payload from records where id = $1', 'acceptable': 'ClientRead with independently retained idle-in-transaction state; historical client wait alone cannot prove client/network pathology. Last query is not necessarily executing.', 'unsupported': 'Active slow SELECT or network failure proved from ClientRead alone.'}})
    if not (PRIVATE / 'rubric-frozen.json').exists():
        dump(PRIVATE / 'rubric-frozen.json', rubric)
    for case in cases:
        cdir = PRIVATE / case['id']
        cdir.mkdir(exist_ok=True)
        if (cdir / 'setup.json').exists():
            continue
        role = 'observer_' + case['id']
        env = os.environ.copy()
        env['POSTGRES_PASSWORD'] = case['passwords']['postgres']
        if not case.get('container_id'):
            out = sh(['docker', 'run', '-d', '--name', case['container'], '--label', 'trial=pgash-blind-20261021',
                      '--memory=192m', '--memory-swap=192m', '--cpus=0.25', '--pids-limit=96', '--shm-size=32m',
                      '-p', '127.0.0.1::5432', '-e', 'POSTGRES_PASSWORD', '-e', 'POSTGRES_DB=appdb',
                      '-e', 'POSTGRES_INITDB_ARGS=--auth-host=scram-sha-256 --auth-local=scram-sha-256',
                      'postgres:18', '-c', 'shared_preload_libraries=pg_stat_statements', '-c', 'compute_query_id=on',
                      '-c', 'shared_buffers=16MB', '-c', 'max_connections=16', '-c', 'work_mem=1MB',
                      '-c', 'maintenance_work_mem=16MB', '-c', 'log_connections=on', '-c', 'log_timezone=UTC',
                      '-c', 'timezone=UTC', '-c', 'jit=off'], env=env).strip()
            case['container_id'] = out
        mapping = json.loads(sh(['docker', 'inspect', case['container']]))[0]
        case['port'] = int(mapping['NetworkSettings']['Ports']['5432/tcp'][0]['HostPort'])
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
            conn.execute('create role service login password %s' % ("'" + case['passwords']['service'] + "'"))
            conn.execute('create role ' + role + ' login password ' + "'" + case['passwords'][role] + "'")
            conn.execute('alter role ' + role + " set default_transaction_read_only = on")
            conn.execute('alter role ' + role + " set statement_timeout = '10s'")
            conn.execute('alter role ' + role + " set lock_timeout = '2s'")
            conn.execute('grant pg_read_all_stats to ' + role)
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
        wrapper.write_text("#!/usr/bin/env python3\n" + "import os, sys, subprocess, json, time, datetime\n" +
                           f"os.environ.update(PGHOST='127.0.0.1', PGPORT='{case['port']}', PGDATABASE='appdb', PGUSER='{role}', PGPASSFILE={str(pgpass)!r}, PGAPPNAME='observer', PGOPTIONS='-c default_transaction_read_only=on -c statement_timeout=10000 -c lock_timeout=2000')\n" +
                           "args=sys.argv[1:]\n" +
                           "script_files={args[i+1]:open(args[i+1]).read() for i in range(len(args)-1) if args[i] in ('-f','--file')}\n" +
                           "stdin=None if ('-c' in args or '-f' in args or sys.stdin.isatty()) else sys.stdin.read()\n" +
                           "started=datetime.datetime.now(datetime.timezone.utc).isoformat(); t=time.monotonic()\n" +
                           "result=subprocess.run(['/usr/bin/psql','-X','-v','ON_ERROR_STOP=1']+args,input=stdin,text=True,capture_output=True)\n" +
                           f"with open({str(idir / 'audit.jsonl')!r},'a') as out: out.write(json.dumps(dict(started=started,ended=datetime.datetime.now(datetime.timezone.utc).isoformat(),elapsed_seconds=time.monotonic()-t,args=args,stdin=stdin,script_files=script_files,stdout=result.stdout,stderr=result.stderr,returncode=result.returncode))+'\\n')\n" +
                           "sys.stdout.write(result.stdout);sys.stderr.write(result.stderr);sys.exit(result.returncode)\n")
        os.chmod(wrapper, 0o700)
        with connect(case, user=role, app='observer') as reader:
            assert reader.execute('show default_transaction_read_only').fetchone()['default_transaction_read_only'] == 'on'
            reader.execute('select * from ash.status()').fetchall()
        dump(idir / 'handoff.json', {'case_id': case['id'], 'psql': str(wrapper), 'database': 'appdb', 'docs': str(PUBLIC / 'docs')})
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
            conn.execute(query, params).fetchall() if query.startswith('select') else conn.execute(query, params)
        except Exception as exc:
            errors.append(str(exc))

    try:
        if mechanism in ['transactionid', 'relation', 'advisory']:
            blocker = connect(case, 'service', 'service')
            conns.append(blocker)
            if mechanism == 'transactionid':
                blocker.execute('begin')
                blocker.execute('update records set value = value + 1 where id in (1, 2)')
                query = 'update records set value = value + 1 where id = %s'
            elif mechanism == 'relation':
                blocker.execute('begin')
                blocker.execute('lock table records in access exclusive mode')
                query = 'select payload from records where id = %s'
            else:
                blocker.execute('select pg_advisory_lock(71423)')
                query = 'select pg_advisory_lock(%s)'
            for i in (1, 2):
                conn = connect(case, 'service', 'service')
                conns.append(conn)
                conn.execute("set statement_timeout = '45s'")
                thread = threading.Thread(target=waiter, args=(conn, query, [71423 if mechanism == 'advisory' else i]))
                thread.start()
                threads.append(thread)
            time.sleep(seconds)
            if mechanism == 'advisory':
                blocker.execute('select pg_advisory_unlock_all()')
                # Release each newly acquired session lock so the next waiter can finish.
                while any(t.is_alive() for t in threads):
                    for conn, thread in zip(conns[1:], threads):
                        if not thread.is_alive():
                            conn.execute('select pg_advisory_unlock_all()')
                    time.sleep(0.05)
            else:
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
        for conn in conns:
            conn.close()
    end = now()
    log(path / 'controller.jsonl', {'phase': 'spike', 'index': index, 'start': start, 'end': end, 'seconds_requested': seconds, 'errors': errors})
    return {'index': index, 'start': start, 'end': end}


def calm(case, seconds, label):
    start = now()
    time.sleep(seconds)
    log(PRIVATE / case['id'] / 'controller.jsonl', {'phase': label, 'start': start, 'end': now(), 'seconds': seconds, 'sessions': 0})


def certify(case, windows, started):
    path = PRIVATE / case['id']
    rubric = json.loads((PRIVATE / 'rubric-frozen.json').read_text())
    expected = rubric['expected'][case['mechanism']]
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
            started = now()
            dump(PRIVATE / case['id'] / 'pre-run.json', {'started': started, 'expected': json.loads((PRIVATE / 'rubric-frozen.json').read_text())['expected'][case['mechanism']]})
            calm(case, 35, 'baseline')
            windows = [spike(case, 14, 1)]
            calm(case, 20, 'intermission')
            windows.append(spike(case, 18, 2))
            calm(case, 35, 'recovery')
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
    outcomes = []
    for case in manifest['cases'] + manifest.get('invalid_containers', []):
        if not case['container'].startswith(PREFIX + '_'):
            raise RuntimeError('ownership mismatch')
        inspected = json.loads(sh(['docker', 'inspect', case['container']]))[0]
        if inspected['Id'] != case['container_id'] or inspected['Config']['Labels'].get('trial') != 'pgash-blind-20261021':
            raise RuntimeError('identity mismatch')
        dest = PRIVATE / case.get('id', case.get('case_id'))
        captured = subprocess.run(['docker', 'logs', '--timestamps', case['container']], check=True, text=True, capture_output=True)
        (dest / (case['container'] + '.stdout.log')).write_text(captured.stdout)
        (dest / (case['container'] + '.stderr.log')).write_text(captured.stderr)
        sh(['docker', 'rm', '-f', '-v', case['container']])
        outcomes.append({'container': case['container'], 'removed': now()})
    dump(PRIVATE / 'cleanup.json', outcomes)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('action', choices=['setup', 'run', 'manifest', 'cleanup'])
    parser.add_argument('--seed', type=int, default=202610210285)
    parser.add_argument('--keep-sampling', action='store_true')
    args = parser.parse_args()
    signal.signal(signal.SIGTERM, lambda *_: STOP.set())
    signal.signal(signal.SIGINT, lambda *_: STOP.set())
    if args.action == 'setup':
        setup(args.seed)
    elif args.action == 'run':
        run(args.keep_sampling)
    elif args.action == 'manifest':
        data = json.loads((PRIVATE / 'manifest.json').read_text())
        for case in data['cases']:
            del case['passwords']
        print(json.dumps(data, indent=2))
    else:
        cleanup()
