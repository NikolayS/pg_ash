# Security Policy

## Supported Versions

| Version | Supported |
| ------- | --------- |
| v1.4.x  | ✅        |
| < v1.4  | ❌        |

## Scope

pg_ash is a pure-SQL extension that samples `pg_stat_activity`. Relevant security concerns include:

- SQL injection in dynamic query construction
- Unintended data exposure from `pg_stat_activity` (query text, client addresses)
- Privilege escalation via installed functions

## Known limitations

### Advisory-lock squat (DoS)

pg_ash uses session-known advisory-lock keys for sampler / rotate / rebuild / rollup coordination. The keys are derived deterministically from string literals in `ash-install.sql` (`hashtext('pg_ash')::int4`, `hashtext('pg_ash_<kind>')::int4`), so any user with `LOGIN` and `EXECUTE` on `pg_advisory_xact_lock` can compute and squat them, halting `take_sample`, `rotate`, `rollup_*`, or `rebuild_partitions` for the duration of their transaction. This is equivalent to the prior literal `(0, …)` / `(1, …)` design — choosing a hashed, ash-scoped namespace improves accidental cross-extension collision hygiene but does not stop a hostile session.

Mitigation: revoke EXECUTE on `pg_advisory_xact_lock` from `PUBLIC` (Postgres default grants it broadly), or run pg_ash in a dedicated database where untrusted roles cannot connect.

## Reporting a Vulnerability

Use GitHub's [private vulnerability reporting](https://github.com/NikolayS/pg_ash/security/advisories/new) to report security issues privately.

Do **not** open a public issue for security bugs.

You can also reach the maintainer at **nik@samokhvalov.com**.

We will acknowledge your report within 7 days and provide a fix or mitigation as quickly as possible. There is no fixed SLA — we treat security reports as high priority.
