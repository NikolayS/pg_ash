# pg_ash SQL migrations

Version-to-version transition scripts live here.

Fresh installs use:

```sql
\i sql/ash-install.sql
```

Upgrades use the cumulative chain under this directory, for example:

```sql
\i sql/migrations/ash-1.4-to-1.5.sql
\i sql/migrations/ash-1.5-to-2.0.sql
```

The old root-level `sql/ash-X.Y-to-A.B.sql` paths remain as compatibility
wrappers for existing documentation, scripts, and operators' muscle memory.
