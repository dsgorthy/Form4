-- 2026-05-03_001_decision_audit_source
--
-- Add source discriminator to trade_decision_audit so we can distinguish
-- between rows produced by:
--   'live'        — the cw_runner running on Studio in real time (post-deploy)
--   'simulation'  — a deterministic replay of the strategy from its start date
--                   (Phase 1.4 backfill — populates the lifetime history)
--
-- In theory: for any (strategy, ticker, filing_date), the live and simulation
-- decisions should agree (the strategy is deterministic). Discrepancies
-- indicate either a non-determinism bug or an input drift between the two
-- runs. The admin view can show both side-by-side for reconciliation.
--
-- Apply via `pipelines/migrations/migrate.py`.

BEGIN;

ALTER TABLE trade_decision_audit
    ADD COLUMN IF NOT EXISTS source text NOT NULL DEFAULT 'live';

CREATE INDEX IF NOT EXISTS idx_decision_audit_source_strategy
    ON trade_decision_audit (source, strategy, ts DESC);

COMMENT ON COLUMN trade_decision_audit.source IS
    'live = real cw_runner; simulation = walk-forward replay for backfill';

COMMIT;
