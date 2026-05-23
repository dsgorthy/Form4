-- =============================================================================
-- Stage 2: Split strategy_portfolio into purpose-specific tables.
--
-- Before:
--   strategy_portfolio (799 rows) discriminated by (execution_source, is_live)
--     execution_source='backtest'  is_live=false  →  749 rows  (legacy archive)
--     execution_source='paper'     is_live=false  →   13 rows  (paper-account fills)
--     execution_source='simulated' is_live=false  →   24 rows  (walk-forward sim, current)
--     execution_source='live'      is_live=true   →    0 rows  (real-money — none yet)
--
-- After (this migration creates the new tables additively — does NOT drop the
-- old table yet, and does NOT migrate any writers/readers):
--   sim_portfolio       — walk-forward simulator output (what /portfolio shows)
--   paper_trades        — fills against Alpaca paper accounts (cw_runner output)
--   live_trades         — fills against Alpaca real-money accounts
--   backtest_archive    — frozen legacy backtest rows (749 rows, historical only)
--
-- A future migration will:
--   1. Replace strategy_portfolio with a VIEW that UNIONs the new tables (for
--      backward compatibility during reader rollout)
--   2. Cut writers over to write to the new tables directly
--   3. Cut readers over (cw_runner first, then API, then scripts)
--   4. Drop the strategy_portfolio view after 1 week clean
--
-- ROLLBACK: DROP TABLE sim_portfolio, paper_trades, live_trades, backtest_archive;
-- =============================================================================

BEGIN;

-- ----------------------------------------------------------------------------
-- New tables — identical column shape to strategy_portfolio except no is_live
-- (table name encodes that) and no execution_source (also encoded). Created
-- via LIKE so we inherit defaults + constraints but NOT indexes (indexes would
-- collide on name). Indexes recreated below with table-prefixed names.
-- ----------------------------------------------------------------------------

CREATE TABLE sim_portfolio (LIKE strategy_portfolio INCLUDING DEFAULTS INCLUDING CONSTRAINTS);
ALTER TABLE sim_portfolio DROP COLUMN is_live;
ALTER TABLE sim_portfolio DROP COLUMN execution_source;
ALTER TABLE sim_portfolio ADD PRIMARY KEY (id);
ALTER TABLE sim_portfolio ALTER COLUMN id SET DEFAULT nextval('strategy_portfolio_id_seq');

CREATE TABLE paper_trades (LIKE strategy_portfolio INCLUDING DEFAULTS INCLUDING CONSTRAINTS);
ALTER TABLE paper_trades DROP COLUMN is_live;
ALTER TABLE paper_trades DROP COLUMN execution_source;
ALTER TABLE paper_trades ADD PRIMARY KEY (id);
ALTER TABLE paper_trades ALTER COLUMN id SET DEFAULT nextval('strategy_portfolio_id_seq');

CREATE TABLE live_trades (LIKE strategy_portfolio INCLUDING DEFAULTS INCLUDING CONSTRAINTS);
ALTER TABLE live_trades DROP COLUMN is_live;
ALTER TABLE live_trades DROP COLUMN execution_source;
ALTER TABLE live_trades ADD PRIMARY KEY (id);
ALTER TABLE live_trades ALTER COLUMN id SET DEFAULT nextval('strategy_portfolio_id_seq');

CREATE TABLE backtest_archive (LIKE strategy_portfolio INCLUDING DEFAULTS INCLUDING CONSTRAINTS);
ALTER TABLE backtest_archive DROP COLUMN is_live;
ALTER TABLE backtest_archive DROP COLUMN execution_source;
ALTER TABLE backtest_archive ADD PRIMARY KEY (id);
ALTER TABLE backtest_archive ALTER COLUMN id SET DEFAULT nextval('strategy_portfolio_id_seq');

-- ----------------------------------------------------------------------------
-- Indexes — mirror strategy_portfolio's index set, with new prefixed names.
-- ----------------------------------------------------------------------------

CREATE INDEX idx_sim_portfolio_date         ON sim_portfolio   (entry_date);
CREATE INDEX idx_sim_portfolio_status       ON sim_portfolio   (status);
CREATE INDEX idx_sim_portfolio_strategy     ON sim_portfolio   (strategy);

CREATE INDEX idx_paper_trades_date          ON paper_trades    (entry_date);
CREATE INDEX idx_paper_trades_status        ON paper_trades    (status);
CREATE INDEX idx_paper_trades_strategy      ON paper_trades    (strategy);
-- Dedup guard: cw_runner should never write the same (strategy, trade_id, entry_date)
-- twice. Once writers are cut over, this becomes the structural protection
-- against the dup-row class of bug that hit sim_portfolio on 2026-05-22.
CREATE UNIQUE INDEX idx_paper_trades_dedup  ON paper_trades    (strategy, trade_id, entry_date)
    WHERE trade_id IS NOT NULL;

CREATE INDEX idx_live_trades_date           ON live_trades     (entry_date);
CREATE INDEX idx_live_trades_status         ON live_trades     (status);
CREATE INDEX idx_live_trades_strategy       ON live_trades     (strategy);
CREATE UNIQUE INDEX idx_live_trades_dedup   ON live_trades     (strategy, trade_id, entry_date)
    WHERE trade_id IS NOT NULL;

CREATE INDEX idx_backtest_archive_strategy  ON backtest_archive (strategy);

-- ----------------------------------------------------------------------------
-- Backfill from strategy_portfolio. The four partitions:
--   - 'simulated' rows  → sim_portfolio
--   - 'paper'     rows  → paper_trades
--   - is_live = true    → live_trades   (none today; included for completeness)
--   - 'backtest'  rows  → backtest_archive
--
-- Explicit column list (NO is_live, NO execution_source) so the SELECT matches
-- the new table shape and PostgreSQL doesn't try to map by position.
-- ----------------------------------------------------------------------------

INSERT INTO sim_portfolio (
    id, strategy, trade_id, ticker, trade_type, direction,
    entry_date, entry_price, exit_date, exit_price,
    hold_days, target_hold, stop_pct, stop_hit,
    pnl_pct, pnl_dollar, position_size, portfolio_value, equity_after,
    insider_name, insider_pit_n, insider_pit_wr, signal_quality,
    exit_reason, status, created_at, portfolio_id,
    is_estimated, slippage_applied, actual_fill_price,
    entry_reasoning, exit_reasoning, company, insider_title,
    filing_date, trade_date, trade_value, signal_grade,
    is_csuite, holdings_pct_change, is_rare_reversal,
    is_cluster, cluster_size, peak_return,
    shares, dollar_amount,
    instrument, option_expiration, option_strike, option_right,
    option_entry_price, option_exit_price, planned_exit_date
)
SELECT
    id, strategy, trade_id, ticker, trade_type, direction,
    entry_date, entry_price, exit_date, exit_price,
    hold_days, target_hold, stop_pct, stop_hit,
    pnl_pct, pnl_dollar, position_size, portfolio_value, equity_after,
    insider_name, insider_pit_n, insider_pit_wr, signal_quality,
    exit_reason, status, created_at, portfolio_id,
    is_estimated, slippage_applied, actual_fill_price,
    entry_reasoning, exit_reasoning, company, insider_title,
    filing_date, trade_date, trade_value, signal_grade,
    is_csuite, holdings_pct_change, is_rare_reversal,
    is_cluster, cluster_size, peak_return,
    shares, dollar_amount,
    instrument, option_expiration, option_strike, option_right,
    option_entry_price, option_exit_price, planned_exit_date
FROM strategy_portfolio
WHERE execution_source = 'simulated' AND COALESCE(is_live, false) = false;

INSERT INTO paper_trades (
    id, strategy, trade_id, ticker, trade_type, direction,
    entry_date, entry_price, exit_date, exit_price,
    hold_days, target_hold, stop_pct, stop_hit,
    pnl_pct, pnl_dollar, position_size, portfolio_value, equity_after,
    insider_name, insider_pit_n, insider_pit_wr, signal_quality,
    exit_reason, status, created_at, portfolio_id,
    is_estimated, slippage_applied, actual_fill_price,
    entry_reasoning, exit_reasoning, company, insider_title,
    filing_date, trade_date, trade_value, signal_grade,
    is_csuite, holdings_pct_change, is_rare_reversal,
    is_cluster, cluster_size, peak_return,
    shares, dollar_amount,
    instrument, option_expiration, option_strike, option_right,
    option_entry_price, option_exit_price, planned_exit_date
)
SELECT
    id, strategy, trade_id, ticker, trade_type, direction,
    entry_date, entry_price, exit_date, exit_price,
    hold_days, target_hold, stop_pct, stop_hit,
    pnl_pct, pnl_dollar, position_size, portfolio_value, equity_after,
    insider_name, insider_pit_n, insider_pit_wr, signal_quality,
    exit_reason, status, created_at, portfolio_id,
    is_estimated, slippage_applied, actual_fill_price,
    entry_reasoning, exit_reasoning, company, insider_title,
    filing_date, trade_date, trade_value, signal_grade,
    is_csuite, holdings_pct_change, is_rare_reversal,
    is_cluster, cluster_size, peak_return,
    shares, dollar_amount,
    instrument, option_expiration, option_strike, option_right,
    option_entry_price, option_exit_price, planned_exit_date
FROM strategy_portfolio
WHERE execution_source = 'paper' AND COALESCE(is_live, false) = false;

-- backfill backtest archive (749 rows of legacy historical research)
INSERT INTO backtest_archive (
    id, strategy, trade_id, ticker, trade_type, direction,
    entry_date, entry_price, exit_date, exit_price,
    hold_days, target_hold, stop_pct, stop_hit,
    pnl_pct, pnl_dollar, position_size, portfolio_value, equity_after,
    insider_name, insider_pit_n, insider_pit_wr, signal_quality,
    exit_reason, status, created_at, portfolio_id,
    is_estimated, slippage_applied, actual_fill_price,
    entry_reasoning, exit_reasoning, company, insider_title,
    filing_date, trade_date, trade_value, signal_grade,
    is_csuite, holdings_pct_change, is_rare_reversal,
    is_cluster, cluster_size, peak_return,
    shares, dollar_amount,
    instrument, option_expiration, option_strike, option_right,
    option_entry_price, option_exit_price, planned_exit_date
)
SELECT
    id, strategy, trade_id, ticker, trade_type, direction,
    entry_date, entry_price, exit_date, exit_price,
    hold_days, target_hold, stop_pct, stop_hit,
    pnl_pct, pnl_dollar, position_size, portfolio_value, equity_after,
    insider_name, insider_pit_n, insider_pit_wr, signal_quality,
    exit_reason, status, created_at, portfolio_id,
    is_estimated, slippage_applied, actual_fill_price,
    entry_reasoning, exit_reasoning, company, insider_title,
    filing_date, trade_date, trade_value, signal_grade,
    is_csuite, holdings_pct_change, is_rare_reversal,
    is_cluster, cluster_size, peak_return,
    shares, dollar_amount,
    instrument, option_expiration, option_strike, option_right,
    option_entry_price, option_exit_price, planned_exit_date
FROM strategy_portfolio
WHERE execution_source LIKE 'backtest%';

-- live_trades intentionally empty — no rows match WHERE is_live=true today.
-- That table is ready for the first real-money insert post-Stage 4.

-- ----------------------------------------------------------------------------
-- Verification — row count parity. Roll back if anything is off.
-- ----------------------------------------------------------------------------

DO $$
DECLARE
    src_sim       BIGINT;
    src_paper     BIGINT;
    src_live      BIGINT;
    src_backtest  BIGINT;
    dst_sim       BIGINT;
    dst_paper     BIGINT;
    dst_live      BIGINT;
    dst_backtest  BIGINT;
BEGIN
    SELECT COUNT(*) INTO src_sim FROM strategy_portfolio WHERE execution_source='simulated' AND COALESCE(is_live,false)=false;
    SELECT COUNT(*) INTO src_paper FROM strategy_portfolio WHERE execution_source='paper' AND COALESCE(is_live,false)=false;
    SELECT COUNT(*) INTO src_live FROM strategy_portfolio WHERE COALESCE(is_live,false)=true;
    SELECT COUNT(*) INTO src_backtest FROM strategy_portfolio WHERE execution_source='backtest' OR execution_source LIKE 'backtest%';

    SELECT COUNT(*) INTO dst_sim       FROM sim_portfolio;
    SELECT COUNT(*) INTO dst_paper     FROM paper_trades;
    SELECT COUNT(*) INTO dst_live      FROM live_trades;
    SELECT COUNT(*) INTO dst_backtest  FROM backtest_archive;

    IF src_sim <> dst_sim OR src_paper <> dst_paper OR src_live <> dst_live OR src_backtest <> dst_backtest THEN
        RAISE EXCEPTION 'Row count mismatch: sim=%/%  paper=%/%  live=%/%  backtest=%/%',
            src_sim, dst_sim, src_paper, dst_paper, src_live, dst_live, src_backtest, dst_backtest;
    END IF;

    RAISE NOTICE 'Backfill verified: sim=% paper=% live=% backtest=%',
        dst_sim, dst_paper, dst_live, dst_backtest;
END $$;

COMMIT;
