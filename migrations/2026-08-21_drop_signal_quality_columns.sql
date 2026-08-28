-- Drop trades.signal_quality and trades.signal_category.
--
-- WHY
--
-- Both are non-null on exactly 20,313 of the 126,521 filings since 2026-01-01
-- (16.1%) — the same population, so one writer stamps both and covers a slice.
-- Neither is read by any user-facing surface.
--
-- signal_quality is worse than merely unused. CLAUDE.md lists api/signal_quality.py
-- as red flag #1 in the signal registry: sell_win_rate_7d reads the full track
-- record, so the score is not point-in-time. tests/unit/test_pit_validation.py
-- already forbids active code importing that module — but the COLUMN it wrote
-- stayed, and pipelines/insider_study/shares_backtest.py was still feeding it
-- into a backtest as a scoring factor at weight 0.20.
--
-- A partially-populated, PIT-dirty score left lying in the schema is a trap:
-- the next person to find it has no way to know either of those things. The
-- module is archived and the readers are removed in the same commit.
--
-- NOT TO BE CONFUSED WITH strategy_portfolio.signal_quality, which is the
-- strategy's own conviction for a position, is listed in
-- api.ratings.INTERNAL_ONLY_FIELDS, and is untouched here.
--
-- Recovery: scripts/backup_databases.sh dumps form4 nightly at 03:15 PT.
--   pg_restore -d form4 -t trades <dump>   (then re-add the columns)

BEGIN;

DROP INDEX IF EXISTS idx_trades_signal_category;
-- Blocking DDL queues at the head of the lock queue and stalls every later
-- read on the table (form4.app outage, 2026-08-27). Abort instead of queueing.
SET lock_timeout = '3s';


ALTER TABLE trades DROP COLUMN IF EXISTS signal_quality;
ALTER TABLE trades DROP COLUMN IF EXISTS signal_category;

COMMIT;
