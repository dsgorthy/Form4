-- trades.value_suspect — close three holes the first version left open.
--
-- WHY
--
-- 2026-08-17_trades_value_suspect.sql gave the "we could not parse this" case
-- its own name, which was the right move. The rule it shipped with misses
-- rows that are obviously wrong:
--
-- 1. THE BRK.A EXEMPTION IS TOO BROAD. Berkshire A shares really do trade at
--    $695,417, so the ticker is exempted from the price test — but the
--    exemption is written above the value test, so it exempts that too. A
--    BRK.A row reporting 300,000 shares sold for $208,625,295,000 is not
--    flagged. There are roughly 550,000 A shares in existence and Berkshire's
--    whole market capitalisation is around $1T, so a single insider moving
--    $208B of it is not a transaction, it is a parse failure.
--
-- 2. FUTURE TRADE DATES ARE RECORDED BUT NOT FLAGGED. Nine rows carry
--    suspect_reason = 'future_trade_date' and value_suspect = false:
--
--      STRA  traded 2029-05-04, filed 2020-05-05
--      SHYF  traded 2029-04-01, filed 2024-04-02
--      TMUS  traded 2028-05-24, filed 2024-05-28   ($23.2M)
--
--    A year typo sorts to the top of every date-ordered surface and stays
--    there for years. The diagnosis was already written down; nothing acted
--    on it.
--
-- 3. THE RULE COULD NOT SEE THE DATES. form4_value_suspect took
--    (price, value, ticker), so 2 was not expressible. It takes trade_date
--    and filing_date now, which also covers the 163 rows reporting a trade
--    that happened after it was filed.
--
-- WHAT THIS DOES NOT DO
--
-- It does not touch the far larger price-accuracy problem: 40,049
-- non-derivative P/S rows report a price more than 10x away from that day's
-- close, because `price` sometimes holds the total value rather than the
-- per-share price (CNTM: price 65,122, qty 73,680, actual close $0.86, so
-- value = price x qty = $4.8B for a $65K trade). That needs a repair pass
-- against prices.daily_prices, not a flag — strategies/insider_catalog/
-- price_validator.py is the module for it, and it is still pointed at the
-- retired SQLite database.
--
-- Applied via: psql -d form4 -f migrations/2026-08-18_value_suspect_gaps.sql

-- ---------------------------------------------------------------------------
-- 1. The rule. Order matters: the value test runs BEFORE the ticker
--    exemption, so an exempt ticker can still fail on an impossible total.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION form4_value_suspect(
    p_price       DOUBLE PRECISION,
    p_value       DOUBLE PRECISION,
    p_ticker      TEXT,
    p_trade_date  TEXT,
    p_filing_date TEXT
)
RETURNS BOOLEAN
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
AS $$
    SELECT CASE
        -- Applies to every ticker, exemptions included.
        WHEN COALESCE(p_value, 0) > 5000000000            THEN TRUE
        -- Section 16 gives an insider two business days to report AFTER the
        -- transaction, so filing_date < trade_date is impossible rather than
        -- merely unusual. Comparing the row against itself keeps the function
        -- IMMUTABLE: a fixed future cutoff would not catch a 2029 typo filed
        -- in 2020, and now() would make the answer drift with the clock.
        WHEN p_trade_date IS NOT NULL AND p_filing_date IS NOT NULL
             AND p_trade_date > p_filing_date             THEN TRUE
        WHEN upper(COALESCE(p_ticker, '')) IN ('BRK-A', 'BRK.A') THEN FALSE
        WHEN COALESCE(p_price, 0) > 100000                THEN TRUE
        ELSE FALSE
    END
$$;

COMMENT ON FUNCTION form4_value_suspect(DOUBLE PRECISION, DOUBLE PRECISION, TEXT, TEXT, TEXT) IS
    'TRUE when price/value/date cannot be believed. Single source of truth for trades.value_suspect.';

-- ---------------------------------------------------------------------------
-- 2. Maintain it, including on trade_date changes.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION trades_set_value_suspect() RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    NEW.value_suspect := form4_value_suspect(
        NEW.price, NEW.value, NEW.ticker, NEW.trade_date, NEW.filing_date
    );
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_trades_value_suspect ON trades;
CREATE TRIGGER trg_trades_value_suspect
    BEFORE INSERT OR UPDATE OF price, value, ticker, trade_date, filing_date
    ON trades
    FOR EACH ROW
    EXECUTE FUNCTION trades_set_value_suspect();

DROP FUNCTION IF EXISTS form4_value_suspect(DOUBLE PRECISION, DOUBLE PRECISION, TEXT);

-- ---------------------------------------------------------------------------
-- 3. Backfill, as a queue drain — insider-fetch writes every five minutes
--    and an unordered batch UPDATE deadlocked against it on 2026-08-17.
-- ---------------------------------------------------------------------------
DO $$
DECLARE
    n    INTEGER;
    idle INTEGER := 0;
BEGIN
    LOOP
        UPDATE trades t
           SET value_suspect = form4_value_suspect(t.price, t.value, t.ticker,
                                                   t.trade_date, t.filing_date)
          FROM (
              SELECT trade_id FROM trades
               WHERE value_suspect IS DISTINCT FROM
                     form4_value_suspect(price, value, ticker, trade_date, filing_date)
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
            PERFORM pg_sleep(1);
        ELSE
            idle := 0;
        END IF;
    END LOOP;
END $$;

-- ---------------------------------------------------------------------------
-- 4. Prove it.
-- ---------------------------------------------------------------------------
DO $$
DECLARE
    unflagged_future INTEGER;
    unflagged_huge   INTEGER;
BEGIN
    SELECT count(*) INTO unflagged_future
      FROM trades WHERE trade_date > filing_date AND NOT COALESCE(value_suspect, FALSE);
    SELECT count(*) INTO unflagged_huge
      FROM trades WHERE value > 5000000000 AND NOT COALESCE(value_suspect, FALSE);

    RAISE NOTICE 'unflagged future-dated trades: %', unflagged_future;
    RAISE NOTICE 'unflagged trades over $5B:     %', unflagged_huge;

    IF unflagged_future > 0 OR unflagged_huge > 0 THEN
        RAISE EXCEPTION 'value_suspect backfill incomplete';
    END IF;
END $$;
