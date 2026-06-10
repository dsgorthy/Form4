-- Revert pyrrho_dataplane:create_signal_observations from pg

BEGIN;

DROP TABLE IF EXISTS signal_observations;

COMMIT;
