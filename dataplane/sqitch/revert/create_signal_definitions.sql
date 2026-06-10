-- Revert pyrrho_dataplane:create_signal_definitions from pg

BEGIN;

DROP TABLE IF EXISTS signal_definitions;

COMMIT;
