-- =============================================================================
-- Stage 2.5: pipeline_runs — central observability for batch jobs.
--
-- Every scheduled/triggered batch job records one row per invocation so we
-- get a queryable history: when it ran, how long, what it touched, whether
-- it succeeded. The dup-row bug from 2026-05-22 would have been visible
-- within 24h here ("strategy_simulator wrote 19 rows yesterday, 26 today,
-- 33 the day after — something is wrong"). Today the only signal of trouble
-- was the user noticing the chart looked weird.
--
-- This is NOT an Airflow / Dagster replacement. It is a 1-table observability
-- layer that surfaces in /admin/pipelines. If we outgrow it, the data we
-- collect here maps cleanly to a real orchestrator's flow_runs table.
-- =============================================================================

BEGIN;

CREATE TABLE IF NOT EXISTS pipeline_runs (
    id              BIGSERIAL PRIMARY KEY,
    service         TEXT NOT NULL,              -- e.g., 'strategy_simulator', 'daily_prices'
    started_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    ended_at        TIMESTAMPTZ,
    duration_ms     BIGINT,
    status          TEXT NOT NULL DEFAULT 'running'
                        CHECK (status IN ('running', 'ok', 'failed', 'timeout', 'partial')),
    exit_code       INTEGER,
    rows_written    BIGINT,
    rows_deleted    BIGINT,
    error_message   TEXT,
    -- Free-form per-service detail: {"strategy_results": {"QM": {"closed": 5}}, "stale_exits": 4}
    metadata        JSONB,
    host            TEXT NOT NULL DEFAULT 'studio',
    log_path        TEXT,
    -- run_id for cross-correlating with trade_decision_audit etc.
    run_uuid        UUID DEFAULT gen_random_uuid()
);

CREATE INDEX IF NOT EXISTS idx_pipeline_runs_service ON pipeline_runs (service, started_at DESC);
CREATE INDEX IF NOT EXISTS idx_pipeline_runs_status  ON pipeline_runs (status, started_at DESC);
CREATE INDEX IF NOT EXISTS idx_pipeline_runs_uuid    ON pipeline_runs (run_uuid);

COMMIT;
