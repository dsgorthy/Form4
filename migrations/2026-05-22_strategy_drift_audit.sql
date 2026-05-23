-- =============================================================================
-- Stage 5: strategy_drift_audit — record where sim and live disagree.
--
-- One row per (strategy, ticker, entry_date) divergence, written by the
-- daily drift detector. Surfaces in /admin/strategies/[name] so we can
-- see "the sim says we should be holding X but paper account doesn't"
-- (or vice versa) without manually diffing tables every morning.
-- =============================================================================

BEGIN;

CREATE TABLE IF NOT EXISTS strategy_drift_audit (
    id              BIGSERIAL PRIMARY KEY,
    ts              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    run_uuid        UUID,                       -- correlates to pipeline_runs.run_uuid
    strategy        TEXT NOT NULL,
    ticker          TEXT NOT NULL,
    entry_date      TEXT NOT NULL,
    drift_type      TEXT NOT NULL CHECK (drift_type IN (
        'sim_only',         -- sim has it, paper doesn't
        'paper_only',       -- paper has it, sim doesn't
        'size_delta',       -- both have it but dollar_amount differs >5%
        'price_delta',      -- both have it but entry_price differs >2%
        'exit_delta'        -- both closed but on different dates / prices
    )),
    sim_trade_id        BIGINT,
    paper_trade_id      BIGINT,
    sim_status          TEXT,
    paper_status        TEXT,
    sim_entry_price     DOUBLE PRECISION,
    paper_entry_price   DOUBLE PRECISION,
    sim_dollar_amount   DOUBLE PRECISION,
    paper_dollar_amount DOUBLE PRECISION,
    sim_pnl_pct         DOUBLE PRECISION,
    paper_pnl_pct       DOUBLE PRECISION,
    severity            TEXT NOT NULL DEFAULT 'info'
                            CHECK (severity IN ('info', 'warn', 'critical')),
    notes               TEXT
);

CREATE INDEX IF NOT EXISTS idx_drift_audit_strategy_ts ON strategy_drift_audit (strategy, ts DESC);
CREATE INDEX IF NOT EXISTS idx_drift_audit_drift_type  ON strategy_drift_audit (drift_type, ts DESC);
CREATE INDEX IF NOT EXISTS idx_drift_audit_run_uuid    ON strategy_drift_audit (run_uuid);

COMMIT;
