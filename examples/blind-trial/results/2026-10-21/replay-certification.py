#!/usr/bin/env python3
"""Offline evidence audit only. No database, network or workload operations."""
import datetime as dt
import json
from pathlib import Path
ROOT = Path(__file__).resolve().parent

def read(path):
    return json.loads(path.read_text())

def lines(path):
    return [json.loads(line) for line in path.read_text().splitlines()]

def recalculate(case, independent, normalize_cpu=True):
    answer = read(case / 'answer-sealed.json')
    samples = read(case / 'samples-certified.json')
    observations = lines(independent / 'observations.jsonl')
    results = []
    for frozen in answer['observed_windows']:
        window = frozen['window']
        # Preserve the frozen controller comparison semantics exactly.
        within = lambda stamp: window['start'] <= stamp <= window['end']
        event = answer['expected']['event']
        matching = [row for row in samples
                    if within(dt.datetime.fromisoformat(row['sample_time']).isoformat())
                    and row['wait_event'] == event]
        observed = [row for batch in observations if within(batch['at'])
                    for row in batch['sessions']
                    if (f"{row['wait_event_type']}:{row['wait_event']}" == event
                        or (normalize_cpu and event == 'CPU*'
                            and row['state'] == 'active'
                            and row['wait_event_type'] is None
                            and row['wait_event'] is None))]
        if answer['mechanism'] == 'idletx':
            observed = [row for row in observed if row['state'] == 'idle in transaction']
        ids = sorted({row['query_id'] for row in matching if row['query_id'] is not None})
        results.append(dict(window=window, ash_matching_rows=len(matching),
                            independent_matching_observations=len(observed), query_ids=ids,
                            valid=len(matching) >= 8 and len(observed) >= 8 and bool(ids)))
    return answer, results

results = []
for case in sorted((ROOT / 'cases').iterdir()):
    answer, observed = recalculate(case, case / 'independent')
    assert observed == answer['observed_windows'], case.name
    assert all(row['valid'] for row in observed), case.name
    ticks = lines(case / 'independent/sampler.jsonl')
    # Logs continued beyond certification; do not reclassify a frozen certificate with future events.
    ticks = [row for row in ticks if row['started'] <= answer['certified_at']]
    errors = [row for row in ticks if 'error' in row or 'skipped_deadlines' in row]
    assert errors == answer['sampler_errors_or_skips'], case.name
    results.append(dict(case=case.name, frozen_window_counts_reproduced=True,
                        observed_windows=observed, sampler_errors_before_certification=len(errors)))
relation = ROOT / 'invalid-attempts/relation-no-query-id'
answer, observed = recalculate(relation, relation)
assert observed == answer['observed_windows']
assert all(not row['valid'] and not row['query_ids'] for row in observed)
results.append(dict(case='original-relation-rejected', frozen_window_counts_reproduced=True,
                    observed_windows=observed))
cpu = ROOT / 'cases/s49m2cnh'
_, original = recalculate(cpu, cpu / 'independent', normalize_cpu=False)
assert original == read(ROOT / 'method/cpu-original-validator-result.json')['observed_windows']
results.append(dict(case='cpu-original-validator-false-negative',
                    original_counts_reproduced=True, observed_windows=original))
sleep = ROOT / 'invalid-attempts/sleep-incomplete'
assert 'RuntimeError: workload did not exit' in (sleep / 'run.log').read_text()
assert all(row['phase'] != 'spike' for row in lines(sleep / 'controller.jsonl'))
results.append(dict(case='original-sleep-rejected', failure_log_present=True,
                    completed_spike_records=0,
                    retained_decoded_sample_rows=len(read(sleep / 'samples.json')),
                    note='The controller raised before completing/certifying its first spike; this is not a fabricated failed certificate.'))
print(json.dumps(results, indent=2))
