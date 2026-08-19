-- trade_narrative.catalysts / .risks were storing Postgres ARRAY literals.
--
-- WHY
--
-- The prompt in scripts/demo_narratives.py asks the model for "1-3 SPECIFIC
-- catalysts", so it returns a list. That list was handed straight to psycopg2
-- for a TEXT column; psycopg2 adapts a Python list to an ARRAY literal, and
-- the cast to text preserved the literal verbatim:
--
--   {"Q2 2026 earnings on August 11, 2026, showed double-digit adjusted
--    EBITDA growth...","CEO Sabo retires year-end..."}
--
-- The filing page printed that to the reader, braces and quotes included.
-- 1,897 of 4,503 catalysts rows and 1,527 risks rows.
--
-- THE CONVERSION
--
-- Postgres parses its own array literal correctly, which matters here: these
-- strings are full of commas ("August 11, 2026, showed...") and any naive
-- split would shred them mid-sentence. Cast to text[] and re-emit as JSON,
-- which is what the writer stores from now on.
--
-- Rows that merely start and end with braces but are not valid array literals
-- are left alone rather than guessed at — the reader handles prose already.
--
-- Applied via: psql -d form4 -f migrations/2026-08-18_narrative_array_literals.sql

BEGIN;

CREATE OR REPLACE FUNCTION pg_temp.array_literal_to_json(v text)
RETURNS text LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE
    parsed text[];
BEGIN
    IF v IS NULL OR v !~ '^\{.*\}$' THEN
        RETURN v;
    END IF;
    BEGIN
        parsed := v::text[];
    EXCEPTION WHEN others THEN
        RETURN v;              -- not a real array literal; leave it
    END;
    IF array_length(parsed, 1) IS NULL THEN
        RETURN NULL;           -- '{}' carried no content
    END IF;
    RETURN to_json(parsed)::text;
END $$;

UPDATE trade_narrative
   SET catalysts = pg_temp.array_literal_to_json(catalysts)
 WHERE catalysts LIKE '{%}';

UPDATE trade_narrative
   SET risks = pg_temp.array_literal_to_json(risks)
 WHERE risks LIKE '{%}';

DO $$
DECLARE
    c_left INTEGER;
    r_left INTEGER;
BEGIN
    SELECT count(*) FILTER (WHERE catalysts LIKE '{%}'),
           count(*) FILTER (WHERE risks LIKE '{%}')
      INTO c_left, r_left
      FROM trade_narrative;
    RAISE NOTICE 'array literals remaining: catalysts=%, risks=%', c_left, r_left;
END $$;

COMMIT;
