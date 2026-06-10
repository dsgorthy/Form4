-- Verify pyrrho_dataplane:create_signal_definitions on pg
--
-- Assert: the table exists with the expected columns, primary key, and
-- check constraints. A bare SELECT 1 wouldn't catch a wrong-shape column.

BEGIN;

DO $$
DECLARE
    n int;
BEGIN
    -- Table exists
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.tables
         WHERE table_schema = 'public' AND table_name = 'signal_definitions'
    ) THEN
        RAISE EXCEPTION 'signal_definitions table missing';
    END IF;

    -- All required columns present (catches partial DDL)
    SELECT COUNT(*) INTO n
      FROM information_schema.columns
     WHERE table_schema = 'public' AND table_name = 'signal_definitions'
       AND column_name IN (
           'signal_id', 'version', 'signal_class', 'description', 'owner',
           'output_schema', 'upstream', 'sla_hours', 'business_hours_only',
           'status', 'registered_at', 'last_modified_at'
       );
    IF n != 12 THEN
        RAISE EXCEPTION
            'signal_definitions has % of 12 expected columns', n;
    END IF;

    -- Primary key on (signal_id, version)
    SELECT COUNT(*) INTO n
      FROM pg_constraint c
      JOIN pg_class t ON c.conrelid = t.oid
     WHERE t.relname = 'signal_definitions' AND c.contype = 'p';
    IF n != 1 THEN
        RAISE EXCEPTION
            'signal_definitions PRIMARY KEY count = %; expected 1', n;
    END IF;

    -- Check constraints present (catches partial application)
    SELECT COUNT(*) INTO n
      FROM pg_constraint c
      JOIN pg_class t ON c.conrelid = t.oid
     WHERE t.relname = 'signal_definitions' AND c.contype = 'c'
       AND conname IN (
           'signal_definitions_status_chk',
           'signal_definitions_sla_positive',
           'signal_definitions_upstream_is_array',
           'signal_definitions_output_schema_is_object'
       );
    IF n != 4 THEN
        RAISE EXCEPTION
            'signal_definitions has % of 4 expected CHECKs', n;
    END IF;
END $$;

ROLLBACK;
