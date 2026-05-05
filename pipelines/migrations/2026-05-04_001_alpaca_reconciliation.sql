-- 2026-05-04_001_alpaca_reconciliation
--
-- Two tables that make strategy↔Alpaca drift observable. The architecture:
-- strategy_portfolio is the canonical state (what the strategy says we hold);
-- order_audit captures every Alpaca submission attempt + fill outcome; this
-- migration adds the snapshot + divergence layer that the daily reconciler
-- writes to.
--
--   alpaca_position_snapshots  ─ rolling capture of "what does Alpaca actually
--                                hold for this strategy right now?". One row
--                                per (strategy, ticker, captured_at).
--   alpaca_reconciliation       ─ persistent divergence records. One row per
--                                detected drift; resolved_at flips on cleanup.
--
-- Apply via `pipelines/migrations/migrate.py`.

BEGIN;

CREATE TABLE IF NOT EXISTS alpaca_position_snapshots (
    id              bigserial PRIMARY KEY,
    strategy        text NOT NULL,
    ticker          text NOT NULL,
    qty             numeric NOT NULL,
    avg_entry_price numeric,
    market_value    numeric,
    current_price   numeric,
    unrealized_pl   numeric,
    captured_at     timestamptz NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_alpaca_snap_latest
    ON alpaca_position_snapshots (strategy, captured_at DESC);

CREATE INDEX IF NOT EXISTS idx_alpaca_snap_ticker
    ON alpaca_position_snapshots (ticker, captured_at DESC);

COMMENT ON TABLE alpaca_position_snapshots IS
    'Rolling capture of Alpaca paper-account positions per strategy. The reconciler appends here on each run; latest snapshot is what `alpaca_reconciliation` diffs against.';


CREATE TABLE IF NOT EXISTS alpaca_reconciliation (
    id              bigserial PRIMARY KEY,
    strategy        text NOT NULL,
    ticker          text NOT NULL,
    issue_type      text NOT NULL CHECK (issue_type IN (
        'missing_in_alpaca',   -- DB says open, Alpaca has no position
        'orphan_in_alpaca',    -- Alpaca holds, no open DB row tracking it
        'qty_mismatch',        -- both hold but share counts diverge
        'price_mismatch'       -- entry_price vs avg_entry_price divergence > threshold
    )),
    severity        text NOT NULL CHECK (severity IN ('info','warn','critical')),
    db_qty          numeric,
    alpaca_qty      numeric,
    db_entry_price  numeric,
    alpaca_avg_cost numeric,
    db_status       text,
    portfolio_id    bigint REFERENCES strategy_portfolio (id) ON DELETE SET NULL,
    detail          text,
    detected_at     timestamptz NOT NULL DEFAULT NOW(),
    resolved_at     timestamptz,
    resolution      text
);

CREATE INDEX IF NOT EXISTS idx_alpaca_recon_unresolved
    ON alpaca_reconciliation (strategy, ticker, detected_at DESC)
    WHERE resolved_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_alpaca_recon_strategy
    ON alpaca_reconciliation (strategy, detected_at DESC);

COMMENT ON TABLE alpaca_reconciliation IS
    'Persistent record of strategy↔Alpaca divergences. resolved_at IS NULL means the divergence is still active. The daily reconciler opens new rows for new drift and resolves rows whose divergence has cleared.';

COMMIT;
