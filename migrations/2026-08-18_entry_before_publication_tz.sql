-- form4_entry_before_publication — the timezone was wrong, so the guard was
-- inverted.
--
-- WHY
--
-- 2026-08-17_entry_before_publication.sql put the pre-publication rule in a
-- trigger precisely so it could not fork across six writers. The rule itself
-- then shipped with a broken conversion:
--
--     (t.filed_at::timestamptz AT TIME ZONE 'America/New_York')
--
-- filed_at is TEXT holding a UTC wall clock. ::timestamptz makes Postgres read
-- a naive string in the SERVER timezone, and Studio runs America/Los_Angeles,
-- so the value was interpreted as Pacific and then shifted a further three
-- hours into Eastern. Every filing read seven hours earlier than it happened.
--
-- The effect is not a near miss in one direction, it is an inversion:
--
--   real 20:00 ET (after the bell) -> reads 03:00 -> "public before close"
--                                  -> floor is filing_date -> NOT flagged
--   real 10:30 ET (before the bell) -> reads 17:30 -> "after close"
--                                  -> floor is filing_date+1 -> FLAGGED
--
-- So it passed genuine look-aheads and flagged legitimate mid-session entries.
-- On the rebuilt books it was excluding 35 of quality_notrend's 141 closed
-- positions and 20 of quality_momentum's 55 — all of them real — while the
-- actual pre-publication fills went through.
--
-- PROOF THE COLUMN IS UTC
--
-- For filed_at hours 00, 01 and 02 the filed_at DATE is later than filing_date
-- in 23,519 of 23,519 rows. That is the evening-Eastern rollover, and it only
-- happens if the stored clock is UTC.
--
-- THE CANONICAL RULE ALREADY EXISTED AND WAS RIGHT
--
-- framework/decision/entry_timing.filed_before_close does this correctly, and
-- its docstring names this exact failure: "a naive UTC hour test is wrong for
-- half the year, which is the bug this function exists to avoid." The SQL copy
-- drifted from the Python it says it mirrors — which is the same
-- two-implementations problem the trigger was introduced to end, one layer up.
--
-- Applied via: psql -d form4 -f migrations/2026-08-18_entry_before_publication_tz.sql

CREATE OR REPLACE FUNCTION form4_entry_before_publication(
    p_trade_id   BIGINT,
    p_entry_date TEXT
) RETURNS BOOLEAN
LANGUAGE sql
STABLE
AS $$
    SELECT CASE
        WHEN p_trade_id IS NULL OR p_entry_date IS NULL THEN NULL
        ELSE (
            SELECT p_entry_date < CASE
                -- Read as UTC, compare in Eastern. Going through timestamptz
                -- this way is what makes DST correct; the previous version
                -- depended on the server's timezone, which is not a property
                -- of the data.
                WHEN ((t.filed_at::timestamp AT TIME ZONE 'UTC')
                          AT TIME ZONE 'America/New_York')::time < TIME '16:00'
                    THEN t.filing_date
                -- After the bell, or no timestamp at all: not before the next
                -- calendar day. Deliberately a calendar day rather than a
                -- trading day — this is a floor, and any real entry lands on a
                -- session at or after it.
                ELSE to_char(t.filing_date::date + 1, 'YYYY-MM-DD')
            END
              FROM trades t WHERE t.trade_id = p_trade_id
        )
    END
$$;

COMMENT ON FUNCTION form4_entry_before_publication(BIGINT, TEXT) IS
    'TRUE when a position was opened before its filing was public. filed_at is UTC. Mirrors framework.decision.entry_timing.filed_before_close.';

-- Recompute every row the old rule judged.
UPDATE strategy_portfolio
   SET entry_before_publication = form4_entry_before_publication(trade_id, entry_date)
 WHERE trade_id IS NOT NULL;

DO $$
DECLARE
    flagged INTEGER;
    total   INTEGER;
BEGIN
    SELECT count(*) FILTER (WHERE entry_before_publication), count(*)
      INTO flagged, total
      FROM strategy_portfolio WHERE trade_id IS NOT NULL;
    RAISE NOTICE 'entry_before_publication: % flagged of % rows', flagged, total;
END $$;
