-- trades.signal_class — one label for "what kind of transaction is this",
-- so that "meaningful trade" is a single filter instead of a convention.
--
-- WHY
--
-- The grading path reads `trade_type = 'buy'`, which is 40% option exercises
-- (M) and 34% grants (A). Only 24.8% are actual open-market purchases. Every
-- browsing surface already compensates by hard-coding `trans_code IN ('P','S')`
-- in ~20 separate places; the scoring path is the one that forgot, and nothing
-- structural was ever going to catch that. Measured over 2,500 graded rows:
-- the average career grade draws 29.2% of its evidence from real purchases,
-- and 62.4% of grades rest on none at all.
--
-- A convention repeated in 20 files is not a contract. This column makes the
-- classification explicit, computed once, in one place.
--
-- WHAT COUNTS AS MEANINGFUL (measured 2016-2026, SPY-adjusted, non-derivative)
--
--   class                 n         7d      30d      90d
--   discretionary_buy     112,656  -0.20   +1.06    +0.62   <- signal
--   discretionary_sell    606,261  -0.48   -0.39    -0.35   <- signal
--   planned_sell          113,262  +0.38   +0.86    +1.00   <- INVERTS
--   option_exercise       192,110  -0.60   -0.18    -0.84
--   compensation          164,849  -0.68    0.00    -0.76
--   tax_withholding       452,730  +0.21   -0.17    -0.32
--
-- planned_sell is the reason 10b5-1 gets its own class rather than a
-- down-weight: at +0.86% over 30 days it points the opposite direction from a
-- discretionary sale, so merging the two does not dilute the sell signal, it
-- cancels it.
--
-- DESIGN: TRIGGER, NOT GENERATED COLUMN
--
-- signal_class is a pure function of four columns on its own row, which makes
-- `GENERATED ALWAYS AS ... STORED` the obvious expression of intent. It is not
-- worth it here: adding a stored generated column rewrites the whole table
-- under ACCESS EXCLUSIVE, and trades is 1.75M rows / 4 GB of heap, written
-- every 5 minutes by insider-fetch and read by a live site with a 5-minute
-- alert SLA. A nullable add plus a batched backfill plus a trigger reaches the
-- same invariant with no rewrite and no long lock.
--
-- The trigger fires `BEFORE INSERT OR UPDATE OF` only the four source columns,
-- so the large batch updates that touch this table (career_grade, returns,
-- indicators) never pay for it.
--
-- form4_signal_class() is the single source of truth. Nothing in Python
-- reimplements this mapping — application code reads the column. That is what
-- keeps the SQL and the app from drifting, which is the failure mode that put
-- option exercises into the grade in the first place.
--
-- ON `inconsistent`
--
-- 636 rows disagree with themselves: 398 are trans_code='S' (a sale) flagged
-- trans_acquired_disp='A' (acquired), and 238 are 'P' flagged 'D'. We cannot
-- know which field is right without re-parsing the filing, so they are labelled
-- rather than silently coerced into a direction. They are excluded from the
-- meaningful default and stay fully queryable.
--
-- ON NULLS
--
-- 95,841 rows predate trans_acquired_disp being parsed (all of them P or S,
-- through 2026-03). trans_code alone resolves direction for those, so they
-- classify normally instead of falling into `other`.
--
-- 87,066 rows have is_10b5_1 NULL. Treated as unplanned: the Form 4 checkbox
-- is only affirmatively set when a trade IS under a plan, so absence reads as
-- "not planned or not reported", never as "planned".
--
-- Applied via: psql -d form4 -f migrations/2026-08-17_trades_signal_class.sql

-- ---------------------------------------------------------------------------
-- 1. The classifier. IMMUTABLE so it can be used in index expressions and so
--    the planner may fold it; every input is on the row itself, which is what
--    makes look-ahead structurally impossible here.
-- ---------------------------------------------------------------------------
-- Parameter types mirror the column types exactly (is_10b5_1 and is_derivative
-- are BIGINT on trades). Postgres will not implicitly narrow bigint to integer
-- when resolving a function call, so a signature that merely looks right fails
-- at the call site rather than at definition.
DROP FUNCTION IF EXISTS form4_signal_class(TEXT, INTEGER, TEXT, INTEGER);

CREATE OR REPLACE FUNCTION form4_signal_class(
    p_trans_code    TEXT,
    p_is_10b5_1     BIGINT,
    p_acquired_disp TEXT,
    p_is_derivative BIGINT
) RETURNS TEXT
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
AS $$
    SELECT CASE
        -- Derivative instruments settle on a different security than the one
        -- trade_returns prices, so they can never join the equity signal. Kept
        -- as their own class rather than dropped.
        WHEN COALESCE(p_is_derivative, 0) = 1 THEN 'derivative'

        -- Direction disagrees with itself; see header.
        WHEN p_trans_code = 'P' AND p_acquired_disp = 'D' THEN 'inconsistent'
        WHEN p_trans_code = 'S' AND p_acquired_disp = 'A' THEN 'inconsistent'

        WHEN p_trans_code = 'P' AND COALESCE(p_is_10b5_1, 0) = 0 THEN 'discretionary_buy'
        WHEN p_trans_code = 'P'                                   THEN 'planned_buy'
        WHEN p_trans_code = 'S' AND COALESCE(p_is_10b5_1, 0) = 0 THEN 'discretionary_sell'
        WHEN p_trans_code = 'S'                                   THEN 'planned_sell'

        WHEN p_trans_code IN ('M', 'X') THEN 'option_exercise'
        WHEN p_trans_code = 'A'         THEN 'compensation'
        WHEN p_trans_code = 'F'         THEN 'tax_withholding'
        WHEN p_trans_code = 'G'         THEN 'gift'

        ELSE 'other'
    END
$$;

COMMENT ON FUNCTION form4_signal_class(TEXT, BIGINT, TEXT, BIGINT) IS
    'Single source of truth for trades.signal_class. Do not reimplement in application code — read the column.';

-- ---------------------------------------------------------------------------
-- 2. The column. Nullable with no default: instant in PG11+, no rewrite.
-- ---------------------------------------------------------------------------
ALTER TABLE trades ADD COLUMN IF NOT EXISTS signal_class TEXT;

COMMENT ON COLUMN trades.signal_class IS
    'Transaction nature, from form4_signal_class(). Meaningful default is discretionary_buy + discretionary_sell. Maintained by trigger trg_trades_signal_class.';

-- ---------------------------------------------------------------------------
-- 3. Backfill in batches so the table is never locked for long. 1.75M rows at
--    50k per pass is ~35 statements.
-- ---------------------------------------------------------------------------
DO $$
DECLARE
    n INTEGER;
BEGIN
    LOOP
        UPDATE trades
           SET signal_class = form4_signal_class(trans_code, is_10b5_1, trans_acquired_disp, is_derivative)
         WHERE trade_id IN (
             SELECT trade_id FROM trades WHERE signal_class IS NULL LIMIT 50000
         );
        GET DIAGNOSTICS n = ROW_COUNT;
        EXIT WHEN n = 0;
        COMMIT;
    END LOOP;
END $$;

-- ---------------------------------------------------------------------------
-- 4. Keep it true. `UPDATE OF <cols>` fires only when one of the four source
--    columns is in the UPDATE's SET list, so routine batch writes to
--    career_grade / returns / indicators are unaffected.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION trades_set_signal_class() RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    NEW.signal_class := form4_signal_class(
        NEW.trans_code, NEW.is_10b5_1, NEW.trans_acquired_disp, NEW.is_derivative
    );
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_trades_signal_class ON trades;
CREATE TRIGGER trg_trades_signal_class
    BEFORE INSERT OR UPDATE OF trans_code, is_10b5_1, trans_acquired_disp, is_derivative
    ON trades
    FOR EACH ROW
    EXECUTE FUNCTION trades_set_signal_class();

-- ---------------------------------------------------------------------------
-- 5. Indexes. The feed filters signal_class and orders by filing_date, and the
--    scoring path walks one insider's history — both want the class first.
-- ---------------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_trades_signal_class_filing
    ON trades (signal_class, filing_date DESC);

CREATE INDEX IF NOT EXISTS idx_trades_insider_signal_class
    ON trades (insider_id, signal_class, trade_date);
