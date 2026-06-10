-- Deploy pyrrho_dataplane:create_signal_indexes to pg
-- requires: create_signal_observations
--
-- Query patterns the dataplane needs:
--   1. By ticker across time (Claude: "show me NVDA signals last 30d")
--   2. By time range (backfill audits, Parquet rollover)
--   3. By source_run_id (PIT replay traceability)
--   4. Active signals by owner (admin catalog page)

BEGIN;

-- Pattern 1: ticker + recent time
CREATE INDEX IF NOT EXISTS idx_signal_observations_ticker_date
    ON signal_observations (ticker, as_of_date DESC);

-- Pattern 2: by ingested_at — used during Parquet rollover to find rows
-- to archive, and during PIT replay to walk by ingest order
CREATE INDEX IF NOT EXISTS idx_signal_observations_ingested
    ON signal_observations (ingested_at);

-- Pattern 3: rows produced by a specific run (debugging, idempotent
-- backfill re-runs)
CREATE INDEX IF NOT EXISTS idx_signal_observations_run
    ON signal_observations (source_run_id);

-- Pattern 4: catalog filter — active signals by owner
CREATE INDEX IF NOT EXISTS idx_signal_definitions_active_owner
    ON signal_definitions (owner)
    WHERE status = 'active';

-- Pattern 5: catalog filter — signals by class (for grouped UI)
CREATE INDEX IF NOT EXISTS idx_signal_definitions_class
    ON signal_definitions (signal_class)
    WHERE status = 'active';

COMMIT;
