-- Verify pyrrho_dataplane:create_signal_indexes on pg

BEGIN;

DO $$
DECLARE
    expected text[] := ARRAY[
        'idx_signal_observations_ticker_date',
        'idx_signal_observations_ingested',
        'idx_signal_observations_run',
        'idx_signal_definitions_active_owner',
        'idx_signal_definitions_class'
    ];
    idx_name text;
BEGIN
    FOREACH idx_name IN ARRAY expected LOOP
        IF NOT EXISTS (
            SELECT 1 FROM pg_indexes
             WHERE schemaname = 'public' AND indexname = idx_name
        ) THEN
            RAISE EXCEPTION 'Missing index: %', idx_name;
        END IF;
    END LOOP;
END $$;

ROLLBACK;
