-- Verify pyrrho_dataplane:create_signal_observations on pg

BEGIN;

DO $$
DECLARE
    n int;
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.tables
         WHERE table_schema = 'public' AND table_name = 'signal_observations'
    ) THEN
        RAISE EXCEPTION 'signal_observations table missing';
    END IF;

    -- Required columns
    SELECT COUNT(*) INTO n
      FROM information_schema.columns
     WHERE table_schema = 'public' AND table_name = 'signal_observations'
       AND column_name IN (
           'signal_id', 'ticker', 'as_of_date', 'value', 'confidence',
           'source_run_id', 'ingested_at', 'metadata'
       );
    IF n != 8 THEN
        RAISE EXCEPTION
            'signal_observations has % of 8 expected columns', n;
    END IF;

    -- Primary key
    SELECT COUNT(*) INTO n
      FROM pg_constraint c
      JOIN pg_class t ON c.conrelid = t.oid
     WHERE t.relname = 'signal_observations' AND c.contype = 'p';
    IF n != 1 THEN
        RAISE EXCEPTION
            'signal_observations PRIMARY KEY count = %; expected 1', n;
    END IF;

    -- Three check constraints
    SELECT COUNT(*) INTO n
      FROM pg_constraint c
      JOIN pg_class t ON c.conrelid = t.oid
     WHERE t.relname = 'signal_observations' AND c.contype = 'c'
       AND conname IN (
           'signal_observations_confidence_range',
           'signal_observations_value_present',
           'signal_observations_metadata_is_object'
       );
    IF n != 3 THEN
        RAISE EXCEPTION
            'signal_observations has % of 3 expected CHECKs', n;
    END IF;
END $$;

ROLLBACK;
