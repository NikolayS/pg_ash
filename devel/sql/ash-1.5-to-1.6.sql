-- pg_ash: upgrade from 1.5 to 1.6 (development)
--
-- Development wrapper. Replays the in-progress 1.6 installer so the upgrade
-- path stays schema-equivalent with fresh development installs.

\set ON_ERROR_STOP on
begin;
\ir ash-install.sql
commit;
