-- strategy_portfolio.entry_before_publication — was this position opened at a
-- price the filing had not reached yet?
--
-- WHY A TRIGGER AND NOT A CHECK IN EACH WRITER
--
-- Six modules update P&L on this table: portfolio_simulator (x2),
-- simulate_portfolio_intraday, cw_runner (x4), alpaca_stream_listener (x2).
-- Putting the rule in each of them is exactly what produced the bug it is
-- meant to catch — three of five position-openers had independently decided
-- that "the filing day's close" was a legal fill, because the rule lived in
-- each of them instead of in one place.
--
-- A CHECK constraint cannot express this: the answer depends on trades.filed_at,
-- and CHECK cannot reference another table. A trigger can, and it runs no
-- matter which of the six writers touched the row.
--
-- WHAT IT MEANS
--
-- TRUE  = entry_date is earlier than the first session that could actually have
--         been traded. The gain/loss on this row is measured from a price that
--         was not obtainable. Do not report it as a result.
-- FALSE = entry is at or after the first tradeable session.
-- NULL  = no trade_id to check against (hand-seeded or aggregate rows).
--
-- EDGAR accepts Form 4 until 22:00 ET and 43.5% of A+/A filings land after the
-- 16:00 bell, so "entry_date = filing_date" is legal for some rows and a
-- look-ahead for others. Only filed_at separates them.
--
-- Mirrors framework/decision/entry_timing.filed_before_close, which is the
-- Python side of the same rule. A missing filed_at resolves to after-close in
-- both, so an unknown timestamp is never treated as permission.
--
-- Applied via: psql -d form4 -f migrations/2026-08-17_entry_before_publication.sql

-- ---------------------------------------------------------------------------
-- 1. The predicate. STABLE, not IMMUTABLE — it reads trades.
-- ---------------------------------------------------------------------------
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
                -- Public before the bell: that session's close was tradeable.
                WHEN (t.filed_at::timestamptz AT TIME ZONE 'America/New_York')::time
                     < TIME '16:00'
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
    'TRUE when a position was opened before its filing was public. Mirrors framework.decision.entry_timing.filed_before_close.';

-- ---------------------------------------------------------------------------
-- 2. The column.
-- ---------------------------------------------------------------------------
-- Blocking DDL queues at the head of the lock queue and stalls every later
-- read on the table (form4.app outage, 2026-08-27). Abort instead of queueing.
SET lock_timeout = '3s';

ALTER TABLE strategy_portfolio
    ADD COLUMN IF NOT EXISTS entry_before_publication BOOLEAN;

COMMENT ON COLUMN strategy_portfolio.entry_before_publication IS
    'TRUE = opened at a price the filing had not reached. Its pnl is not a real result. Maintained by trg_sp_entry_before_publication.';

-- ---------------------------------------------------------------------------
-- 3. Backfill. Small table, no batching needed.
-- ---------------------------------------------------------------------------
UPDATE strategy_portfolio
   SET entry_before_publication = form4_entry_before_publication(trade_id, entry_date)
 WHERE entry_before_publication IS DISTINCT FROM
       form4_entry_before_publication(trade_id, entry_date);

-- ---------------------------------------------------------------------------
-- 4. Keep it true on every write, from any of the six writers. Fires on the
--    two columns the verdict depends on; a pure pnl_pct update inherits the
--    verdict already stamped on the row.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION sp_set_entry_before_publication() RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    NEW.entry_before_publication :=
        form4_entry_before_publication(NEW.trade_id, NEW.entry_date);
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_sp_entry_before_publication ON strategy_portfolio;
CREATE TRIGGER trg_sp_entry_before_publication
    BEFORE INSERT OR UPDATE OF trade_id, entry_date
    ON strategy_portfolio
    FOR EACH ROW
    EXECUTE FUNCTION sp_set_entry_before_publication();

-- ---------------------------------------------------------------------------
-- 5. Partial index — consumers want "the rows that are real results".
-- ---------------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_sp_entry_before_publication
    ON strategy_portfolio (strategy, status)
    WHERE entry_before_publication;
