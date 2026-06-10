-- Verify pyrrho_dataplane:create_signal_classes_enum on pg
--
-- Assert: the signal_class enum exists with exactly the 11 expected values.

BEGIN;

DO $$
DECLARE
    n        int;
    expected int := 11;
BEGIN
    SELECT COUNT(*) INTO n FROM unnest(enum_range(NULL::signal_class));
    IF n IS NULL OR n != expected THEN
        RAISE EXCEPTION
            'signal_class enum has % values; expected %', n, expected;
    END IF;

    -- Spot-check the headline classes exist (catches a typo'd rename)
    IF NOT EXISTS (
        SELECT 1 FROM unnest(enum_range(NULL::signal_class)) e
         WHERE e::text IN ('insider', 'options_flow', 'composite')
    ) THEN
        RAISE EXCEPTION 'signal_class enum is missing core classes';
    END IF;
END $$;

ROLLBACK;
