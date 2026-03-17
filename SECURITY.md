# Security Policy

## Supported Versions

| Version | Supported |
| ------- | --------- |
| v1.3.x  | ✅        |
| < v1.3  | ❌        |

## Scope

pg_ash is a pure-SQL extension that samples `pg_stat_activity`. Relevant security concerns include:

- SQL injection in dynamic query construction
- Unintended data exposure from `pg_stat_activity` (query text, client addresses)
- Privilege escalation via installed functions

## Reporting a Vulnerability

Use GitHub's [private vulnerability reporting](https://github.com/NikolayS/pg_ash/security/advisories/new) to report security issues privately.

Do **not** open a public issue for security bugs.

You can also reach the maintainer at **nik@samokhvalov.com**.

We will acknowledge your report within 7 days and provide a fix or mitigation as quickly as possible. There is no fixed SLA — we treat security reports as high priority.
