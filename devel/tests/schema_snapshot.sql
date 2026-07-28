\pset tuples_only on
\pset format unaligned
select 'FUNC', p.proname,
       pg_get_function_identity_arguments(p.oid),
       pg_get_function_result(p.oid),
       md5(p.prosrc)
from pg_proc p
join pg_namespace n on n.oid = p.pronamespace
where n.nspname = 'ash'
order by p.proname, pg_get_function_identity_arguments(p.oid);

select 'COL', c.relname, a.attnum, a.attname,
       pg_catalog.format_type(a.atttypid, a.atttypmod),
       a.attnotnull,
       pg_get_expr(d.adbin, d.adrelid)
from pg_class c
join pg_namespace n on n.oid = c.relnamespace
join pg_attribute a on a.attrelid = c.oid
left join pg_attrdef d on d.adrelid = a.attrelid and d.adnum = a.attnum
where n.nspname = 'ash' and c.relkind in ('r','p') and a.attnum > 0 and not a.attisdropped
order by c.relname, a.attnum;

select 'IDX', c.relname, pg_get_indexdef(i.indexrelid)
from pg_index i
join pg_class c on c.oid = i.indexrelid
join pg_namespace n on n.oid = c.relnamespace
where n.nspname = 'ash'
order by c.relname;

select 'VIEW', viewname, md5(definition)
from pg_views
where schemaname = 'ash'
order by viewname;

-- Issue #66: include CHECK / NOT NULL / PK / UNIQUE / FK constraints.
-- Covers parent partitioned tables AND their partitions (relkind r,p).
-- pg_get_constraintdef gives a stable canonical text form, so any
-- divergence (e.g. sample_data_check `>= 2` vs `>= 3` from #49)
-- surfaces here.
select 'CON', c.relname, con.conname, pg_get_constraintdef(con.oid)
from pg_constraint con
join pg_class c on c.oid = con.conrelid
join pg_namespace n on n.oid = c.relnamespace
where n.nspname = 'ash' and c.relkind in ('r','p')
order by c.relname, con.conname;

-- Issue #107: privilege state must be equivalent too. proacl is
-- NULL until the first GRANT/REVOKE touches a function; the
-- installer's REVOKE-from-PUBLIC hardening always runs, so a fresh
-- install and the full upgrade chain must converge to identical
-- explicit function ACLs.
select 'FACL', p.proname,
       pg_get_function_identity_arguments(p.oid),
       coalesce(p.proacl::text, '<default>')
from pg_proc p
join pg_namespace n on n.oid = p.pronamespace
where n.nspname = 'ash'
order by p.proname, pg_get_function_identity_arguments(p.oid);
