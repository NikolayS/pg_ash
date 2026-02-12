# pg_ash

**Active Session History for Postgres** — wait event sampling with zero bloat.

Inspired by Oracle ASH. Requires only `pg_cron`. Pure SQL, no C extensions, no `shared_preload_libraries` changes.

## What it does

Samples `pg_stat_activity` at a configurable frequency (1–60s) and stores wait events in a compact encoded `integer[]` format (one row per database per sample tick) with PGQ-style 3-partition rotation — zero vacuum, zero bloat, bounded storage.

**Sample row:**
```
sample_ts │ 3628080    (seconds since 2026-01-01 = 2026-02-12 03:48:00 UTC)
datid     │ 16384
data      │ {-5, 3, 101, 102, 101, -1, 2, 103, 104, -1, 1, 105}
```

Encoding: `[-wait_event_id, count, query_id, query_id, ...]`. 6 active backends across 3 wait events → 1 row, ~12 array elements. Wait event IDs and query IDs reference dictionary tables.

## Storage

~33 MiB/day with 50 active backends at 1s sampling (default). See [SPEC.md](SPEC.md) for full benchmarks and [STORAGE_BRAINSTORM.md](STORAGE_BRAINSTORM.md) for design exploration.

## Requirements

- Postgres 14+
- `pg_cron` 1.5+ (for sub-minute scheduling)

## Status

**Design phase** — see [SPEC.md](SPEC.md) for the full specification.

## License

[Apache 2.0](LICENSE)
