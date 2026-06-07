-- Consolidate position state back to a single canonical table.
--
-- Stage 2 of the 2026-05-22 architectural refactor created
-- sim_portfolio, paper_trades, live_trades, backtest_archive as a
-- structural split of strategy_portfolio. The cutover never landed —
-- every writer kept writing to strategy_portfolio with execution_source
-- as the discriminator, and only the drift detector ever read the new
-- tables (compared the now-stale sim_portfolio vs paper_trades copies
-- and produced 100% false-positive drift events).
--
-- The admin and user-facing portfolio views both read strategy_portfolio
-- directly. Collapsing onto it removes the divergence-by-design that
-- caused confusion and the drift-audit noise that came with it.
--
-- Tables removed:
--   sim_portfolio        — stale copy of strategy_portfolio
--                          WHERE execution_source='simulated'
--   paper_trades         — frozen at 5-19 manual seed, never re-written
--   live_trades          — empty; reserved for hypothetical live mode
--   backtest_archive     — frozen historical backtest results, no
--                          consumer; can be re-archived from
--                          strategy_portfolio WHERE
--                          execution_source LIKE 'backtest%' if needed
--   strategy_drift_audit — recorded the false-positive drift events
--                          above. Without sim_portfolio/paper_trades
--                          to compare against, this table has no
--                          producer either.
--
-- Canonical source going forward: strategy_portfolio with these
-- discriminators (already present and used today):
--   is_live boolean                — false for non-money rows
--   execution_source text          — backtest | simulated | alert |
--                                    paper | live
--
-- Safety: every row in the dropped tables is a one-shot backfill copy
-- of rows that still live in strategy_portfolio. No new data lost.

BEGIN;

DROP TABLE IF EXISTS strategy_drift_audit;
DROP TABLE IF EXISTS sim_portfolio;
DROP TABLE IF EXISTS paper_trades;
DROP TABLE IF EXISTS live_trades;
DROP TABLE IF EXISTS backtest_archive;

-- Sanity check: the canonical table still exists.
DO $$
BEGIN
    PERFORM 1 FROM strategy_portfolio LIMIT 1;
EXCEPTION
    WHEN undefined_table THEN
        RAISE EXCEPTION 'FATAL: strategy_portfolio missing — refusing to commit drop migration';
END $$;

COMMIT;
