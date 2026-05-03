-- 2026-05-02_002_order_audit
--
-- Single-source-of-truth audit log for every order placed by any strategy
-- runner. Closes the audit gap permanently: every order traces back to the
-- exact signal inputs, conviction score, and config version that drove it.
--
-- Side effect: setting `client_order_id = order_id` (where Alpaca dedups by
-- this field) eliminates the "Studio + Mini both submit the same order"
-- risk that was flagged in CLAUDE.md. The same change closes a known race.
--
-- Apply via `pipelines/migrations/migrate.py`.

BEGIN;

CREATE TABLE IF NOT EXISTS order_audit (
    order_id              text PRIMARY KEY,         -- our UUID, also Alpaca client_order_id
    strategy              text NOT NULL,
    alpaca_order_id       text,                     -- back-fill on submit response
    ticker                text NOT NULL,
    side                  text NOT NULL,            -- 'buy' | 'sell'
    qty                   numeric NOT NULL,
    order_type            text NOT NULL DEFAULT 'market',
    conviction_score      numeric(5,2),
    pit_grade             text,
    signal_inputs_json    jsonb NOT NULL,           -- snapshot of every input at decision time
    decision_rationale    text,                     -- short explanation from the strategy
    config_version_sha    text NOT NULL,            -- repo HEAD at decision time
    config_yaml_sha       text NOT NULL,            -- sha256 of the strategy yaml at decision time
    decided_at            timestamptz NOT NULL,
    submitted_at          timestamptz,
    fill_status           text,                     -- 'pending' | 'filled' | 'partial' | 'rejected' | 'cancelled'
    fill_price            numeric,
    fill_qty              numeric,
    filled_at             timestamptz,
    rejection_reason      text,
    created_at            timestamptz NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_order_audit_strategy_decided
    ON order_audit (strategy, decided_at DESC);

CREATE INDEX IF NOT EXISTS idx_order_audit_ticker_decided
    ON order_audit (ticker, decided_at DESC);

CREATE INDEX IF NOT EXISTS idx_order_audit_fill_status
    ON order_audit (fill_status, decided_at DESC);

CREATE INDEX IF NOT EXISTS idx_order_audit_alpaca_id
    ON order_audit (alpaca_order_id) WHERE alpaca_order_id IS NOT NULL;

COMMENT ON TABLE order_audit IS
    'Every order placed by a strategy runner — full provenance. Required for compliance reconciliation.';

COMMIT;
