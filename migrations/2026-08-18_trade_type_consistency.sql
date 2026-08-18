-- trades.trade_type must agree with trades.trans_code.
--
-- WHY
--
-- 661 rows say both things at once:
--
--     trans_code   trade_type   rows          value
--     S            buy           393   $3,366,588,908
--     P            sell          268   $2,986,153,548
--
-- trans_code is the SEC's own field — P is an open-market purchase, S is a
-- sale. trade_type is ours, stored separately, and where they disagree the
-- product tells the reader a purchase was a sale.
--
-- It also makes a profile page contradict itself, which is how this surfaced.
-- Sylebra Capital's page reported "15 insider transactions" in the summary,
-- "Total Filings 14" in the stat grid and "15 total" over the table, because
-- one 2020-11-10 row carries trans_code='S' with trade_type='buy' and each
-- count groups by a different one of the two columns. The same row renders as
-- a BUY and a SELL of $12.2M at $31.09 on the same day.
--
-- TWO POPULATIONS, TWO TREATMENTS
--
-- 313 of the 661 collide with an existing row on idx_trades_dedup_v2 once
-- trade_type is corrected — (insider_id, ticker, trade_date, trade_type,
-- value, trans_code) already exists. A row that becomes an exact duplicate of
-- a correctly-typed twin was never a separate event: it is the same trade
-- stored twice, once right and once wrong. Those are marked is_duplicate
-- rather than flipped, which is what that column is for.
--
-- The remaining 348 have no twin and are simply mislabelled. They get the
-- label trans_code implies.
--
-- Only P and S are touched. A, M, F, G and X carry their own meanings for
-- trade_type and there is no contradiction to resolve.
--
-- Nothing in 2026 is affected — the current ingest path is consistent — so
-- this is a historical repair plus a guard against it recurring.
--
-- Applied via: psql -d form4 -f migrations/2026-08-18_trade_type_consistency.sql

-- ---------------------------------------------------------------------------
-- 1. The duplicates. Marked before the flip, or the flip would fail on the
--    unique index they collide with.
-- ---------------------------------------------------------------------------
WITH bad AS (
    SELECT trade_id, insider_id, ticker, trade_date, value, trans_code,
           CASE WHEN trans_code = 'P' THEN 'buy' ELSE 'sell' END AS correct_type
      FROM trades
     WHERE trans_code IN ('P','S') AND is_derivative = 0 AND superseded_by IS NULL
       AND trade_type <> CASE WHEN trans_code = 'P' THEN 'buy' ELSE 'sell' END
)
UPDATE trades t
   SET is_duplicate = 1,
       suspect_reason = COALESCE(t.suspect_reason || ' | ', '')
                        || 'duplicate of correctly-typed row (trans_code '
                        || t.trans_code || ' vs trade_type ' || t.trade_type || ')'
  FROM bad b
 WHERE t.trade_id = b.trade_id
   AND EXISTS (
       SELECT 1 FROM trades o
        WHERE o.insider_id = b.insider_id AND o.ticker = b.ticker
          AND o.trade_date = b.trade_date AND o.trade_type = b.correct_type
          AND o.value = b.value
          AND COALESCE(o.trans_code,'') = COALESCE(b.trans_code,'')
          AND o.trade_id <> b.trade_id
   );

-- ---------------------------------------------------------------------------
-- 2. The mislabelled remainder.
-- ---------------------------------------------------------------------------
UPDATE trades
   SET trade_type = CASE WHEN trans_code = 'P' THEN 'buy' ELSE 'sell' END
 WHERE trans_code IN ('P','S') AND is_derivative = 0 AND superseded_by IS NULL
   AND (is_duplicate = 0 OR is_duplicate IS NULL)
   AND trade_type <> CASE WHEN trans_code = 'P' THEN 'buy' ELSE 'sell' END;

-- ---------------------------------------------------------------------------
-- 3. Keep them in step. Same pattern as filing_key, signal_class and
--    value_suspect: the rule runs on every write regardless of which ingest
--    path performed it, because a convention that lives only in Python is the
--    thing that rotted here.
--
--    Deliberately limited to P and S. Silently rewriting trade_type for a
--    grant or an option exercise would be a different and larger claim.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION trades_set_trade_type() RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.trans_code = 'P' AND NEW.trade_type <> 'buy' THEN
        NEW.trade_type := 'buy';
    ELSIF NEW.trans_code = 'S' AND NEW.trade_type <> 'sell' THEN
        NEW.trade_type := 'sell';
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_trades_trade_type ON trades;
CREATE TRIGGER trg_trades_trade_type
    BEFORE INSERT OR UPDATE OF trans_code, trade_type
    ON trades
    FOR EACH ROW
    EXECUTE FUNCTION trades_set_trade_type();

-- ---------------------------------------------------------------------------
-- 4. Prove it.
-- ---------------------------------------------------------------------------
DO $$
DECLARE
    remaining INTEGER;
BEGIN
    SELECT count(*) INTO remaining
      FROM trades
     WHERE trans_code IN ('P','S') AND is_derivative = 0 AND superseded_by IS NULL
       AND (is_duplicate = 0 OR is_duplicate IS NULL)
       AND trade_type <> CASE WHEN trans_code = 'P' THEN 'buy' ELSE 'sell' END;

    RAISE NOTICE 'contradictory live rows remaining: %', remaining;
    IF remaining > 0 THEN
        RAISE EXCEPTION 'trade_type repair incomplete: % rows', remaining;
    END IF;
END $$;
