-- trades.filing_key — a missing accession must not merge unrelated companies.
--
-- WHY
--
-- 2026-08-17_filing_key_selfmaintaining.sql restored the column and gave it a
-- trigger, which fixed the NULL bucket. It left the other half of the rule
-- intact: when accession is missing the key falls back to the bare trade_date.
-- A bare date is not unique to a filing. It is not unique to an insider or
-- even to a company.
--
--     filing_key    rows   insiders   tickers
--     2016-08-15     331        240       170
--     2016-08-05     303        187       148
--     2016-08-01     290        195       153
--     2019-03-04     228         60        46
--
-- Anything doing GROUP BY filing_key therefore reports one "filing" that is
-- actually 240 people trading 170 different companies. 47,388 rows carry a
-- date-shaped key; 1,703 keys span more than one insider and 1,058 span more
-- than one ticker.
--
-- The surfaces that group on filing_key ALONE are the filings feed
-- (api/filters.filing_group_by), pipelines/portfolio_simulator.py:242,
-- pipelines/render_video.py:108 and generate_daily_content.py:251. The ones
-- that group by (insider_id, ticker, trade_type, filing_key) were already
-- safe, which is why this stayed invisible: the daily content pipelines
-- happen to be in the second group.
--
-- Concentrated in 2016-2019 (46,900 of 47,388), so the live feed was not
-- affected. Company and insider history pages, /explore with a date filter,
-- and any backtest that walks the simulator were.
--
-- THE FIX
--
-- Namespace the fallback by the things that actually identify the event. The
-- "nofiling:" prefix keeps it obviously distinct from an accession, so a key
-- that looks like a date can never come back without someone noticing.
--
-- Applied via: psql -d form4 -f migrations/2026-08-18_filing_key_namespace.sql

-- ---------------------------------------------------------------------------
-- 1. The rule. Accession identifies exactly one SEC filing and stays the key
--    whenever it is present — which is 97.3% of rows, and effectively all of
--    them since 2020.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION form4_filing_key(
    p_accession  TEXT,
    p_trade_date TEXT,
    p_insider_id BIGINT,
    p_ticker     TEXT
)
RETURNS TEXT
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
AS $$
    SELECT COALESCE(
        NULLIF(TRIM(COALESCE(p_accession, '')), ''),
        'nofiling:' || COALESCE(p_insider_id::text, '?')
                    || ':' || COALESCE(NULLIF(TRIM(COALESCE(p_ticker, '')), ''), '?')
                    || ':' || COALESCE(p_trade_date, '?')
    )
$$;

COMMENT ON FUNCTION form4_filing_key(TEXT, TEXT, BIGINT, TEXT) IS
    'One Form 4 = one key. Accession when present, else nofiling:<insider>:<ticker>:<date>. Single source of truth for trades.filing_key.';

-- ---------------------------------------------------------------------------
-- 2. Maintain it. Trigger updated BEFORE the backfill so that anything
--    insider-fetch writes mid-drain already carries the new rule, and the
--    drain never has to revisit it.
--
--    insider_id and ticker join the UPDATE OF list: they are inputs now, and
--    a trigger that ignores its own inputs is how the column rotted the first
--    time.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION trades_set_filing_key() RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    NEW.filing_key := form4_filing_key(
        NEW.accession, NEW.trade_date, NEW.insider_id, NEW.ticker
    );
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_trades_filing_key ON trades;
CREATE TRIGGER trg_trades_filing_key
    BEFORE INSERT OR UPDATE OF accession, trade_date, insider_id, ticker
    ON trades
    FOR EACH ROW
    EXECUTE FUNCTION trades_set_filing_key();

-- The two-argument rule has no callers left and must not acquire one: two
-- functions with the same name and different answers is the ambiguity this
-- migration exists to remove.
DROP FUNCTION IF EXISTS form4_filing_key(TEXT, TEXT);

-- ---------------------------------------------------------------------------
-- 3. Backfill, as a queue drain — insider-fetch writes every five minutes and
--    an unordered batch UPDATE deadlocked against it on 2026-08-17.
-- ---------------------------------------------------------------------------
DO $$
DECLARE
    n    INTEGER;
    idle INTEGER := 0;
BEGIN
    LOOP
        UPDATE trades t
           SET filing_key = form4_filing_key(t.accession, t.trade_date,
                                             t.insider_id, t.ticker)
          FROM (
              SELECT trade_id FROM trades
               WHERE filing_key IS DISTINCT FROM
                     form4_filing_key(accession, trade_date, insider_id, ticker)
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
-- 4. Prove it. Both counts must be zero afterwards.
-- ---------------------------------------------------------------------------
DO $$
DECLARE
    bad_multi INTEGER;
    bad_shape INTEGER;
BEGIN
    SELECT count(*) INTO bad_multi FROM (
        SELECT filing_key FROM trades
         GROUP BY filing_key
        HAVING count(DISTINCT insider_id) > 1 OR count(DISTINCT ticker) > 1
    ) q;
    SELECT count(*) INTO bad_shape FROM trades
     WHERE filing_key ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$';

    RAISE NOTICE 'filing_keys spanning >1 insider or ticker: %', bad_multi;
    RAISE NOTICE 'date-shaped filing_keys remaining: %', bad_shape;

    IF bad_shape > 0 THEN
        RAISE EXCEPTION 'backfill incomplete: % date-shaped keys remain', bad_shape;
    END IF;
END $$;
