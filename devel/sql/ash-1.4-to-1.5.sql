-- pg_ash development upgrade from 1.4 toward 1.5.
--
-- WIP: do not bump ash.config.version here. The version is finalized when
-- 1.5 is stamped.
--
-- During the development cycle, keep the in-progress upgrade path in lockstep
-- with the in-progress final installer. At release stamp time this file moves
-- to sql/ next to the promoted ash-install.sql.

\ir ash-install.sql
