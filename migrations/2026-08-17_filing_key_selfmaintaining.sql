-- trades.filing_key — restore it, and stop it rotting again.
--
-- WHY
--
-- filing_key groups the rows of one Form 4 into one event. A fund selling on
-- 30 separate lots files ONE Form 4; without the grouping the UI shows 30
-- rows, and with a broken grouping it shows one row for five months of
-- activity.
--
-- It stopped being written on 2026-03-26 and nothing noticed:
--
--     2026-01     4,100 rows        0 null
--     2026-02     5,128 rows        0 null
--     2026-03    16,328 rows    3,305 null   <- cutover mid-month
--     2026-04    17,689 rows   17,689 null
--     2026-05    26,719 rows   26,719 null
--     2026-06    25,511 rows   25,511 null
--     2026-07    15,376 rows   15,376 null
--     2026-08    10,947 rows   10,947 null
--
-- 96,242 rows with no key. Every surface that groups on it — the insider
-- trade table, the company trade table, the filings feed — collapsed all of
-- them into a single NULL bucket. Magnetar Financial's profile reported 833
-- sales and rendered 11 table rows; 585 of those sales, across 33 separate
-- SEC accessions, were sharing one empty key.
--
-- No writer exists. Not in Python, not in a migration, not a column default,
-- not a trigger. Whatever populated it historically is gone from the repo, so
-- restoring the value alone would only reset the clock.
--
-- ON THE OLD VALUES
--
-- Where filing_key IS set on older rows it is usually a DATE, not an
-- accession, despite api/filters.py documenting "accession when available".
-- That over-groups too: Magnetar's 2025-09-17 key covers three distinct
-- accessions. Those rows are rewritten to the accession as well, so the whole
-- column means one thing.
--
-- Applied via: psql -d form4 -f migrations/2026-08-17_filing_key_selfmaintaining.sql

-- ---------------------------------------------------------------------------
-- 1. The rule. accession identifies exactly one SEC filing; trade_date is the
--    fallback for the ~2% of rows that have never carried one.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION form4_filing_key(p_accession TEXT, p_trade_date TEXT)
RETURNS TEXT
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
AS $$
    SELECT COALESCE(NULLIF(TRIM(COALESCE(p_accession, '')), ''), p_trade_date)
$$;

COMMENT ON FUNCTION form4_filing_key(TEXT, TEXT) IS
    'One Form 4 = one key. Accession when present, else trade_date. Single source of truth for trades.filing_key.';

-- ---------------------------------------------------------------------------
-- 2. Backfill, as a queue drain. insider-fetch writes every five minutes and
--    an unordered batch UPDATE deadlocked against it earlier today.
-- ---------------------------------------------------------------------------
DO $$
DECLARE
    n    INTEGER;
    idle INTEGER := 0;
BEGIN
    LOOP
        UPDATE trades t
           SET filing_key = form4_filing_key(t.accession, t.trade_date)
          FROM (
              SELECT trade_id FROM trades
               WHERE filing_key IS DISTINCT FROM form4_filing_key(accession, trade_date)
               ORDER BY trade_id
               LIMIT 20000
                 FOR UPDATE SKIP LOCKED
          ) s
         WHERE t.trade_id = s.trade_id;
        GET DIAGNOSTICS n = ROW_COUNT;
        COMMIT;
        IF n = 0 THEN
            idle := idle + 1;
            EXIT WHEN idle >= 3;
            PERFORM pg_sleep(2);
        ELSE
            idle := 0;
        END IF;
    END LOOP;
END $$;

-- ---------------------------------------------------------------------------
-- 3. Maintain it. Same pattern as signal_class and value_suspect: the rule
--    runs on every write regardless of which ingest path performed it, which
--    is the property the vanished Python writer never had.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION trades_set_filing_key() RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    NEW.filing_key := form4_filing_key(NEW.accession, NEW.trade_date);
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_trades_filing_key ON trades;
CREATE TRIGGER trg_trades_filing_key
    BEFORE INSERT OR UPDATE OF accession, trade_date
    ON trades
    FOR EACH ROW
    EXECUTE FUNCTION trades_set_filing_key();

CREATE INDEX IF NOT EXISTS idx_trades_filing_key ON trades (filing_key);
