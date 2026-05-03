-- 2026-05-02_001_signal_freshness
--
-- Tracks the last successful compute time for every analytical column.
-- Read by `framework/contracts/freshness.py:assert_fresh()`. Written by every
-- compute pipeline (compute_cw_indicators, build_pit_scores, daily-prices,
-- compute_signals) on completion.
--
-- Why this matters: pre-rebuild, the only freshness signal we had was
-- `MAX(filing_date)` of the source table — which tells you when the latest
-- ROW landed, not when the COMPUTE ran. The April outage went undetected for
-- 21 days because compute hadn't run, but rows kept landing via insider-fetch.
--
-- Apply via `pipelines/migrations/migrate.py`.

BEGIN;

CREATE TABLE IF NOT EXISTS signal_freshness (
    source            text NOT NULL,            -- 'public' / 'prices' / 'research' (PG schema)
    table_name        text NOT NULL,            -- 'trades' / 'daily_prices' / etc.
    column_name       text NOT NULL,            -- 'above_sma50' / 'pit_grade' / etc.
    last_computed_at  timestamptz NOT NULL,
    n_rows_affected   bigint,                   -- how many trades the compute touched
    run_id            uuid,                     -- pipeline run identifier
    populated_by      text,                     -- script path that wrote this row
    PRIMARY KEY (source, table_name, column_name, last_computed_at)
);

CREATE INDEX IF NOT EXISTS idx_signal_freshness_lookup
    ON signal_freshness (source, table_name, column_name, last_computed_at DESC);

CREATE INDEX IF NOT EXISTS idx_signal_freshness_recent
    ON signal_freshness (last_computed_at DESC);

COMMENT ON TABLE signal_freshness IS
    'Per-column last-computed-at tracking. Source of truth for freshness contracts.';

COMMIT;
