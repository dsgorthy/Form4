-- 2026-05-02_003_trade_decision_audit
--
-- Per-stage audit trail for every candidate the runner evaluates. One row
-- per (run, candidate, filter stage). Captures rejected candidates as well
-- as accepted ones, so we can reconcile "why didn't the strategy buy this
-- insider's filing?" forever.
--
-- The April outage would have been visible Day 1 in this table: thousands
-- of `stage='sma', passed=false, reason='null_input:above_sma50'` rows.
--
-- Apply via `pipelines/migrations/migrate.py`.

BEGIN;

CREATE TABLE IF NOT EXISTS trade_decision_audit (
    id                BIGSERIAL PRIMARY KEY,
    ts                timestamptz NOT NULL DEFAULT NOW(),
    run_id            uuid NOT NULL,                -- one per scan_signals invocation
    strategy          text NOT NULL,
    ticker            text,
    trade_id          bigint,
    filing_date       text,                          -- YYYY-MM-DD
    thesis            text,
    stage             text NOT NULL,                 -- 'recency'|'sma'|'dip'|'pit'|'conviction'|'capacity'|'duplicate'
    passed            boolean NOT NULL,
    reason            text,                          -- 'null_input:above_sma50' | 'conviction:4.2 < 5.0' | ...
    pit_grade         text,
    conviction        numeric(5,2),
    feature_snapshot  jsonb                          -- every feature the decision considered
);

CREATE INDEX IF NOT EXISTS idx_decision_audit_strategy_ts
    ON trade_decision_audit (strategy, ts DESC);

CREATE INDEX IF NOT EXISTS idx_decision_audit_run_id
    ON trade_decision_audit (run_id);

CREATE INDEX IF NOT EXISTS idx_decision_audit_ticker_filing
    ON trade_decision_audit (ticker, filing_date DESC);

CREATE INDEX IF NOT EXISTS idx_decision_audit_stage_passed
    ON trade_decision_audit (stage, passed, ts DESC);

COMMENT ON TABLE trade_decision_audit IS
    'Per-stage filter outcomes for every candidate. Enables "why did/did-not we trade X?" forever.';

COMMIT;
