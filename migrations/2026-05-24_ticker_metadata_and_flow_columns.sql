-- =============================================================================
-- 2026-05-24: ticker_metadata table + 2 new PIT-clean columns on trades.
--
-- Supports the CEO Watcher validation experiment: company-level and industry-
-- level net-flow signals. See plan at
-- /Users/derekg/.claude/plans/velvety-waddling-sifakis.md and the architectural
-- audit memo project_2026-05-22_architectural_refactor.md for context.
--
-- Both new columns are nullable — PIT semantics require NULL for trades on
-- tickers with insufficient history (<3 years of activity for the baseline,
-- or no sector classification yet for the industry signal). NULL means
-- "we don't know," NOT "no signal."
--
-- ROLLBACK:
--   DROP TABLE ticker_metadata;
--   ALTER TABLE trades DROP COLUMN net_buyer_flow_90d;
--   ALTER TABLE trades DROP COLUMN industry_buy_pct_90d;
-- =============================================================================

BEGIN;

CREATE TABLE IF NOT EXISTS ticker_metadata (
    ticker            TEXT PRIMARY KEY,
    sector            TEXT,
    industry          TEXT,
    -- yfinance / polygon / alpaca-premium / manual. yfinance for now.
    source            TEXT NOT NULL DEFAULT 'yfinance',
    last_refreshed    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    refresh_attempts  INTEGER NOT NULL DEFAULT 0,
    last_error        TEXT
);
CREATE INDEX IF NOT EXISTS idx_ticker_metadata_sector   ON ticker_metadata (sector);
CREATE INDEX IF NOT EXISTS idx_ticker_metadata_industry ON ticker_metadata (industry);

ALTER TABLE trades ADD COLUMN IF NOT EXISTS net_buyer_flow_90d   DOUBLE PRECISION;
ALTER TABLE trades ADD COLUMN IF NOT EXISTS industry_buy_pct_90d DOUBLE PRECISION;

COMMENT ON COLUMN trades.net_buyer_flow_90d IS
    'PIT: (distinct buyers in prior-90d) - (distinct sellers in prior-90d), minus '
    'the trailing-3y rolling-90d median for this ticker. Positive = unusually heavy '
    'net buying. NULL if <3y of history. Populated by compute_company_net_flow.py. '
    'CRITICAL: window is [F-90d, F) strictly — never includes the current trade.';

COMMENT ON COLUMN trades.industry_buy_pct_90d IS
    'PIT: percentage of this ticker''s industry peers with >=1 unscheduled buy '
    'in [F-90d, F), minus the trailing-3y rolling-90d median for the same industry. '
    'NULL if ticker has no sector classification in ticker_metadata or industry '
    'has insufficient history. Populated by compute_industry_net_flow.py.';

COMMIT;
