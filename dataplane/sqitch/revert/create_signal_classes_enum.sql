-- Revert pyrrho_dataplane:create_signal_classes_enum from pg

BEGIN;

DROP TYPE IF EXISTS signal_class CASCADE;

COMMIT;
