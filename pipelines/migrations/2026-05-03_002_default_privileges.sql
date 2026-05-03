-- 2026-05-03_002_default_privileges
--
-- Permanent fix for "permission denied for table X" after adding a new table
-- via migration. Sets DEFAULT PRIVILEGES so any future table/sequence that
-- derekg creates in `public` automatically grants SELECT/INSERT/UPDATE to
-- the appuser role used by the API container.
--
-- Without this, every new migration would need a paired GRANT statement,
-- which is easy to forget — and the failure mode is silent (table works in
-- psql as derekg but breaks the API at runtime, exactly as we just hit on
-- /admin/diagnostics).
--
-- Note: this only affects tables created AFTER this migration. The existing
-- 17 tables that needed grants were granted manually on 2026-05-03 prior to
-- writing this migration.
--
-- Apply via `pipelines/migrations/migrate.py`.

BEGIN;

ALTER DEFAULT PRIVILEGES FOR ROLE derekg IN SCHEMA public
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO appuser;

ALTER DEFAULT PRIVILEGES FOR ROLE derekg IN SCHEMA public
    GRANT USAGE, SELECT ON SEQUENCES TO appuser;

-- Same for prices/research/notifications schemas in case migrations land there.
ALTER DEFAULT PRIVILEGES FOR ROLE derekg IN SCHEMA prices
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO appuser;
ALTER DEFAULT PRIVILEGES FOR ROLE derekg IN SCHEMA research
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO appuser;
ALTER DEFAULT PRIVILEGES FOR ROLE derekg IN SCHEMA notifications
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO appuser;

COMMIT;
