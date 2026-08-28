-- trade_returns.abnormal_{N}td_from_filing — the only labels anyone can trade.
--
-- WHY
--
-- Every existing abnormal_* column is anchored to trade_date. Nobody can trade
-- on trade_date: the filing is not public until filing_date, median two days
-- later, and 7.7% of discretionary buys are filed a week or more after the fact
-- so their entire abnormal_7d window has closed before anyone could act.
--
-- The consequence is not theoretical. A trade model fitted on abnormal_7d
-- scored a +6.85pp walk-forward decile spread, positive in 10 of 10 years, and
-- had NO ranking power on filing-anchored returns — the bottom decile beat the
-- top at almost every horizon. It had learned to predict a move that had
-- already happened.
--
-- Right now the untradeable label is the only one in the database, so every
-- analysis reaches for it by default. That is the actual defect. These columns
-- make the correct label the easy one.
--
-- DEFINITION
--
--   entry  = close of the first session that could have been traded
--            (same day when filed_at < 16:00 ET, else the next session —
--            framework.decision.entry_timing, and the trg_trades_signal_class
--            sibling rule)
--   exit   = close N trading days later, on the SPY calendar
--   value  = (exit/entry - 1) - (SPY exit/SPY entry - 1), winsorized ±100pp
--
-- Trading days, not calendar days, so a long weekend cannot silently shorten a
-- hold. Winsorized at the same ±100pp as fit_trade_model.py: unclipped, the
-- holding curve peaked at +1.20% and that turned out to be +10.37% in 2026
-- alone against ≤+0.75% every other year, on a 46.6% win rate.
--
-- HORIZONS
--
-- 3/5/7/10/21/42. The short end is where the measured edge lives (the grade's
-- A+/A vs B/C/D spread is +2.90pp at 7d and +0.05pp by 60d); 42 matches the
-- production hold so strategy work and research share one definition.
--
-- SCOPE
--
-- Populated for discretionary_buy and discretionary_sell only. The other eight
-- signal classes are compensation mechanics and tax administration — nobody
-- trades them, and computing 740k rows is already the expensive part.
--
-- Applied via: psql -d form4 -f migrations/2026-08-17_returns_from_filing.sql
-- Then backfilled by: pipelines/insider_study/backfill_returns_from_filing.py
-- Blocking DDL queues at the head of the lock queue and stalls every later
-- read on the table (form4.app outage, 2026-08-27). Abort instead of queueing.
SET lock_timeout = '3s';


ALTER TABLE trade_returns
    ADD COLUMN IF NOT EXISTS abnormal_3td_from_filing  DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS abnormal_5td_from_filing  DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS abnormal_7td_from_filing  DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS abnormal_10td_from_filing DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS abnormal_21td_from_filing DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS abnormal_42td_from_filing DOUBLE PRECISION,
    -- Which session the entry was taken on. Without it a NULL return is
    -- ambiguous: no price, or not enough forward history yet?
    ADD COLUMN IF NOT EXISTS entry_date_from_filing    TEXT;

COMMENT ON COLUMN trade_returns.abnormal_7td_from_filing IS
    'SPY-adjusted return, entry at the first tradeable close AFTER the filing, 7 trading days. The tradeable label — prefer over abnormal_7d, which is anchored to trade_date and includes days that had already passed when the filing appeared.';

COMMENT ON COLUMN trade_returns.entry_date_from_filing IS
    'Session the from_filing returns are measured from. NULL = no usable entry price.';

CREATE INDEX IF NOT EXISTS idx_trade_returns_from_filing
    ON trade_returns (trade_id) WHERE abnormal_7td_from_filing IS NOT NULL;
