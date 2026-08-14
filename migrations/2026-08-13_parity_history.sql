-- parity_history — daily record of candidate-vs-baseline signal agreement.
--
-- The cutover gate for retiring the form4 bridge is "parity >= 99.5% held for
-- N consecutive days". A one-off CLI reading cannot answer that: it says what
-- parity is right now, not whether it has been stable, and a single good day
-- is exactly what you would see from a lucky run.
--
-- One row per (candidate, baseline, measured day). Re-running a day overwrites
-- it, so a late-arriving filing that improves an earlier day is reflected
-- rather than double-counted.
--
-- Lives in the dataplane DB (pyrrho_data_dev) alongside signal_observations,
-- which is what it measures.
--
-- Applied via: psql -d pyrrho_data_dev -f migrations/2026-08-13_parity_history.sql

CREATE TABLE IF NOT EXISTS parity_history (
    signal_a     TEXT        NOT NULL,   -- candidate (the new ingestor)
    signal_b     TEXT        NOT NULL,   -- baseline (what it must match)
    trade_day    DATE        NOT NULL,   -- the day compared
    count_a      INTEGER     NOT NULL,
    count_b      INTEGER     NOT NULL,
    matched      INTEGER     NOT NULL,
    only_in_a    INTEGER     NOT NULL,
    only_in_b    INTEGER     NOT NULL,
    -- Recall against the baseline: matched / count_b. This is the number the
    -- cutover gate reads. coverage_a is recorded too, but a candidate finding
    -- MORE than the baseline is not a defect — the native EDGAR reader sees
    -- filings the bridge never had.
    coverage_b   NUMERIC(6,3),
    coverage_a   NUMERIC(6,3),
    measured_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (signal_a, signal_b, trade_day)
);

CREATE INDEX IF NOT EXISTS idx_parity_history_day
    ON parity_history (trade_day DESC);
