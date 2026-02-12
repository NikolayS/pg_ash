# pg_ash

**Active Session History for PostgreSQL** — wait event sampling with zero bloat.

Inspired by Oracle ASH. Requires only `pg_cron`. Pure SQL, no C extensions, no `shared_preload_libraries` changes.

## What it does

Samples `pg_stat_activity` at a configurable frequency (1–60s) and stores wait event history using a PGQ-style 3-partition rotation scheme — zero vacuum, zero bloat, bounded storage.

## Quick start

```sql
create extension if not exists pg_cron;
\i pg_ash--1.0.sql
select pg_ash.start();  -- starts sampling every 10s
```

## Storage

~10 MB/day per 10 active backends at 10s sampling. See [SPEC.md](SPEC.md) for full math.

## Status

**Design phase** — see [SPEC.md](SPEC.md) for the full specification.

## License

[Apache 2.0](LICENSE)
