-- pg_ash: upgrade from 1.4 to 1.5
--
-- Bug-fix release. Replays the finalized 1.5 installer so the 1.4-to-1.5
-- upgrade path stays schema-equivalent with fresh 1.5 installs.

\ir ash-install.sql
