-- strategy_portfolio.filing_date and .trade_date held the entry date.
--
-- WHY
--
-- simulate_strategy_portfolio built its INSERT with
--
--     c.company, c.entry_date, c.entry_date,
--
-- against a column list of `company, filing_date, trade_date`. Both date
-- columns therefore received the day we opened the position, and the filing's
-- own dates were never written — even though the candidate query had been
-- selecting `t.filing_date::text, t.trade_date::text` all along. They were
-- fetched, then dropped on the floor.
--
-- These are three different days and the product presents all three:
--
--   trade_date   when the insider actually dealt
--   filing_date  when EDGAR received the Form 4
--   entry_date   the first close we could have bought at
--
-- Worked example — BFLY, Larry Robbins, trade_id 54969, the row that surfaced
-- this. EDGAR: period of report 2025-11-19, filed 2025-11-21, accepted
-- 17:35:57 ET. A Friday, after the bell, so the first tradeable close is
-- Monday 2025-11-24 and the entry date is right. But the trade detail page
-- read "Filing Date 2025-11-24 / Trade Date 2025-11-24" directly above a link
-- to the SEC filing that says 11-21 and 11-19. We were contradicting our own
-- source document on the page that cites it.
--
-- SCOPE
--
-- All 381 simulated rows had filing_date = entry_date. 348 also had the wrong
-- trade_date; the other 33 coincide because same-day-tradeable filings do
-- happen. 210 had a wrong filing_date, the rest coinciding the same way.
--
-- The fix is a straight copy from trades, which is the source of truth. Only
-- rows with a trade_id can be repaired, and only those are touched.
--
-- Applied via: psql -d form4 -f migrations/2026-08-18_portfolio_filing_dates.sql

BEGIN;

UPDATE strategy_portfolio sp
   SET filing_date = t.filing_date,
       trade_date  = t.trade_date
  FROM trades t
 WHERE t.trade_id = sp.trade_id
   AND (sp.filing_date IS DISTINCT FROM t.filing_date
        OR sp.trade_date IS DISTINCT FROM t.trade_date);

DO $$
DECLARE
    wrong INTEGER;
    total INTEGER;
    ordering INTEGER;
BEGIN
    SELECT count(*) FILTER (WHERE sp.filing_date IS DISTINCT FROM t.filing_date
                               OR sp.trade_date  IS DISTINCT FROM t.trade_date),
           count(*)
      INTO wrong, total
      FROM strategy_portfolio sp JOIN trades t ON t.trade_id = sp.trade_id;

    -- trade_date <= filing_date <= entry_date must hold for every row. An
    -- entry before the filing is the look-ahead this codebase has already been
    -- bitten by once; assert it here rather than trust it.
    SELECT count(*) INTO ordering
      FROM strategy_portfolio
     WHERE trade_id IS NOT NULL
       AND (trade_date > filing_date OR filing_date > entry_date);

    RAISE NOTICE 'date mismatches remaining: % of %', wrong, total;
    RAISE NOTICE 'rows violating trade_date <= filing_date <= entry_date: %', ordering;

    IF wrong > 0 THEN
        RAISE EXCEPTION 'backfill incomplete: % rows still disagree with trades', wrong;
    END IF;
END $$;

COMMIT;
