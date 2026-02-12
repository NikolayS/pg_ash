# pg_ash

**Active Session History for PostgreSQL** — wait event sampling with zero bloat.

Inspired by Oracle ASH. Requires only `pg_cron`. Pure SQL, no C extensions, no `shared_preload_libraries` changes.

## What it does

Samples `pg_stat_activity` at a configurable frequency (1–60s) and stores wait events using parallel arrays (one row per database per sample tick) in a PGQ-style 3-partition rotation — zero vacuum, zero bloat, bounded storage.

**Sample row:**
```
sample_ts │ 2026-02-12 03:48:00+00
datid     │ 16384
wait_ids  │ {5, 5, 1, 0, 0, 9, 1}
query_ids │ {-617046263269, 482910384756, 771629384, -617046263269, 923847561029, NULL, 482910384}
```

7 active backends captured in 1 row. `wait_ids` map to a dictionary table; `query_ids` join to `pg_stat_statements`.

## Storage

~2.5 MB/day with 20 active backends at 10s sampling. See [SPEC.md](SPEC.md) for full benchmarks.

## Requirements

- PostgreSQL 14+
- `pg_cron` 1.5+ (for sub-minute scheduling)

## Status

**Design phase** — see [SPEC.md](SPEC.md) for the full specification.

## License

[Apache 2.0](LICENSE)
