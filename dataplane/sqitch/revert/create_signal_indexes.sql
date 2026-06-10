-- Revert pyrrho_dataplane:create_signal_indexes from pg

BEGIN;

DROP INDEX IF EXISTS idx_signal_definitions_class;
DROP INDEX IF EXISTS idx_signal_definitions_active_owner;
DROP INDEX IF EXISTS idx_signal_observations_run;
DROP INDEX IF EXISTS idx_signal_observations_ingested;
DROP INDEX IF EXISTS idx_signal_observations_ticker_date;

COMMIT;
