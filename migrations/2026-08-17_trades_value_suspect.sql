-- trades.value_suspect — stop filing "we could not parse this" under "derivative".
--
-- WHY
--
-- _classify_is_derivative() in backfill_live.py sets is_derivative=1 for two
-- unrelated reasons: the security really is a derivative, OR the price/value
-- is impossible and the row needs to be kept out of every dollar aggregate.
-- That was a reasonable quarantine — a single flag that all ~20 aggregate
-- queries already filtered on — and it worked. It also made the two cases
-- indistinguishable. Of 11,017 flagged rows:
--
--   9,820  (89%)  derivative security title, sane price and value
--     880  ( 8%)  derivative security title, impossible value
--     317  ( 3%)  NOT a derivative at all — common stock, impossible value
--
-- Two costs, both real:
--
--   1. Those 317 are ordinary common-stock filings that disappear from every
--      surface as "derivatives". SVRE is a $3.45 purchase with a quantity of
--      2.5 billion shares; BRK.A is a correct $695,417 price that tripped the
--      $5B value floor. They are dirty, but they are not derivatives, and
--      calling them derivatives means nobody will ever go look.
--
--   2. The 9,820 genuine derivative trades are discarded wholesale because
--      they share a flag with garbage. An insider buying calls is plausibly
--      informative and we currently cannot separate those rows from parse
--      failures well enough to find out.
--
-- This is the same shape as the trans_code problem signal_class fixed: one
-- overloaded flag standing in for a distinction nobody wrote down.
--
-- WHAT THIS DOES NOT DO
--
-- It does not re-parse anything and it does not change is_derivative. Every
-- existing `WHERE is_derivative = 0` keeps behaving exactly as before, and the
-- CHECK constraint in backfill.py still holds. This only adds a name for the
-- second meaning so queries that care can tell them apart:
--
--   genuine derivative     is_derivative = 1 AND NOT value_suspect
--   unparseable common     value_suspect AND security_title not derivative-ish
--
-- Repairing the underlying numbers means re-fetching from EDGAR and handling
-- retroactive reverse-split adjustment on microcaps. That is a real project
-- for 0.07% of rows, and it cannot even be scoped until the affected set is
-- addressable — which is what this column provides.
--
-- Applied via: psql -d form4 -f migrations/2026-08-17_trades_value_suspect.sql

-- ---------------------------------------------------------------------------
-- 1. The predicate, mirroring _classify_is_derivative's numeric rules.
--
--    Note the allowlist now guards BOTH rules. In the Python it guards only
--    the price test, so a legitimate high-priced common stock could clear the
--    price check and then be caught by the value floor anyway — 10,000 BRK.A
--    shares is $6.9B of genuinely real common stock. Fixed there too.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION form4_value_suspect(
    p_price  DOUBLE PRECISION,
    p_value  DOUBLE PRECISION,
    p_ticker TEXT
) RETURNS BOOLEAN
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
AS $$
    SELECT CASE
        WHEN upper(COALESCE(p_ticker, '')) IN ('BRK-A', 'BRK.A') THEN FALSE
        WHEN COALESCE(p_price, 0) > 100000        THEN TRUE
        WHEN COALESCE(p_value, 0) > 5000000000    THEN TRUE
        ELSE FALSE
    END
$$;

COMMENT ON FUNCTION form4_value_suspect(DOUBLE PRECISION, DOUBLE PRECISION, TEXT) IS
    'True when price or value is outside anything a real filing can express. Mirrors _classify_is_derivative numeric rules; single source of truth for trades.value_suspect.';

-- ---------------------------------------------------------------------------
-- 2. The column. Nullable add, no rewrite.
-- ---------------------------------------------------------------------------
ALTER TABLE trades ADD COLUMN IF NOT EXISTS value_suspect BOOLEAN;

COMMENT ON COLUMN trades.value_suspect IS
    'Price or value is unparseable. Orthogonal to is_derivative — a suspect row may be common stock. Maintained by trg_trades_value_suspect.';

-- ---------------------------------------------------------------------------
-- 3. Backfill in batches, as a queue drain rather than a plain loop.
--
--    The first attempt at this deadlocked against insider-fetch, which writes
--    to trades every five minutes. An unordered `LIMIT 50000` picks an
--    arbitrary set, so the two transactions took row locks in opposite orders
--    and Postgres killed one of them. Two changes make that impossible:
--
--      ORDER BY trade_id     both writers now take locks in the same order
--      FOR UPDATE SKIP LOCKED  never wait on a row the fetcher holds; leave it
--                              for the next pass instead of blocking on it
--
--    Because SKIP LOCKED can leave stragglers, the loop exits on three
--    consecutive empty passes rather than the first one, with a short pause
--    between so a live transaction has time to commit.
-- ---------------------------------------------------------------------------
DO $$
DECLARE
    n    INTEGER;
    idle INTEGER := 0;
BEGIN
    LOOP
        UPDATE trades t
           SET value_suspect = form4_value_suspect(t.price, t.value, t.ticker)
          FROM (
              SELECT trade_id
                FROM trades
               WHERE value_suspect IS NULL
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
-- 4. Keep it true. Scoped to the three source columns so the routine batch
--    writes (career_grade, returns, indicators) never fire it.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION trades_set_value_suspect() RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    NEW.value_suspect := form4_value_suspect(NEW.price, NEW.value, NEW.ticker);
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_trades_value_suspect ON trades;
CREATE TRIGGER trg_trades_value_suspect
    BEFORE INSERT OR UPDATE OF price, value, ticker
    ON trades
    FOR EACH ROW
    EXECUTE FUNCTION trades_set_value_suspect();

-- ---------------------------------------------------------------------------
-- 5. Partial index — the suspect set is tiny (~1,200 of 1.75M) and the queries
--    that want it want only it.
-- ---------------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_trades_value_suspect
    ON trades (filing_date DESC) WHERE value_suspect;
