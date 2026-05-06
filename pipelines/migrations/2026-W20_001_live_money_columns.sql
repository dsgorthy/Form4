-- 2026-W20_001_live_money_columns
--
-- Adds `is_live` to strategy_portfolio + order_audit so the same code path
-- can carry both paper and real-money state, distinguished by a boolean.
-- Customer-facing API filters out is_live=true rows by default until the
-- live portfolio surface is built; admin can opt in.
--
-- Default false on existing rows — every position prior to this migration
-- is paper.
--
-- Apply via `pipelines/migrations/migrate.py`.

BEGIN;

ALTER TABLE strategy_portfolio
    ADD COLUMN IF NOT EXISTS is_live BOOLEAN NOT NULL DEFAULT false;

ALTER TABLE order_audit
    ADD COLUMN IF NOT EXISTS is_live BOOLEAN NOT NULL DEFAULT false;

ALTER TABLE alpaca_position_snapshots
    ADD COLUMN IF NOT EXISTS is_live BOOLEAN NOT NULL DEFAULT false;

ALTER TABLE alpaca_reconciliation
    ADD COLUMN IF NOT EXISTS is_live BOOLEAN NOT NULL DEFAULT false;

CREATE INDEX IF NOT EXISTS idx_strategy_portfolio_live_status
    ON strategy_portfolio (strategy, is_live, status);

CREATE INDEX IF NOT EXISTS idx_order_audit_live
    ON order_audit (strategy, is_live, decided_at DESC);

COMMENT ON COLUMN strategy_portfolio.is_live IS
    'true = real-money trade in a live Alpaca account; false = paper trading. Customer-facing portfolio filters this out by default.';
COMMENT ON COLUMN order_audit.is_live IS
    'true = order placed against a live Alpaca account.';

COMMIT;
