-- form4_entry_before_publication: filed_at is Eastern now, stop converting it.
--
-- This is the third correction to the same rule in two days, which is the
-- argument for it not living in SQL at all:
--
--   2026-08-17  created, converting filed_at::timestamptz -> ET. That read a
--               naive string in the SERVER timezone (Pacific), so every
--               filing landed seven hours early.
--   2026-08-18  fixed to read it as UTC, which was right for the data as it
--               then stood.
--   2026-08-19  filed_at normalised to Eastern throughout, so reading it as
--               UTC now subtracts four hours that are not there.
--
-- The rule's home is framework.decision.entry_timing, which the simulator now
-- calls directly rather than reimplementing in its query. This function stays
-- only as a write-time backstop: any writer, from any language, that tries to
-- record an entry before its filing was public gets caught.
--
-- IT IS A FLOOR, NOT A REPLICA. The Python models a five-minute polling
-- pickup with jitter, and SQL should not try to reproduce that — two
-- implementations of a stochastic rule cannot agree by construction. This
-- asks only the question SQL can answer definitively: is this entry date
-- earlier than the earliest session anyone could have traded? Anything
-- subtler belongs upstream.
--
-- Applied via: psql -d form4 -f migrations/2026-08-19_entry_guard_eastern.sql

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
                -- filed_at is EASTERN. No conversion. See
                -- migrations/2026-08-19_filed_at_normalize_eastern.sql.
                WHEN t.filed_at IS NOT NULL
                     AND t.filed_at::timestamp::time < TIME '16:00'
                     AND t.filed_at::timestamp::date::text = t.filing_date
                    THEN t.filing_date
                -- After the bell, or no timestamp: not before the next
                -- calendar day. A calendar day rather than a trading day on
                -- purpose — this is a floor, and any real entry lands on a
                -- session at or after it.
                ELSE to_char(t.filing_date::date + 1, 'YYYY-MM-DD')
            END
              FROM trades t WHERE t.trade_id = p_trade_id
        )
    END
$$;

COMMENT ON FUNCTION form4_entry_before_publication(BIGINT, TEXT) IS
    'TRUE when a position was opened before its filing was public. filed_at is EASTERN. A floor check only — the authoritative rule, including the polling-pickup model, is framework.decision.entry_timing.';
