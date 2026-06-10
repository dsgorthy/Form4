-- Deploy pyrrho_dataplane:create_signal_observations to pg
-- requires: create_signal_definitions
--
-- The canonical output: every signal computation lands as a row here.
-- (signal_id, ticker, as_of_date) is the unique key. ingested_at is
-- wallclock — used by the PIT validator to hide rows that hadn't landed
-- at the historical as_of being simulated.
--
-- Not partitioned in this initial cut. Partition by month (RANGE on
-- as_of_date) when row count justifies — sized as a follow-up migration.

BEGIN;

CREATE TABLE IF NOT EXISTS signal_observations (
    signal_id        text        NOT NULL,
    ticker           text        NOT NULL,
    as_of_date       timestamptz NOT NULL,
    value            jsonb       NOT NULL,
    confidence       real,
    source_run_id    uuid        NOT NULL,
    ingested_at      timestamptz NOT NULL DEFAULT now(),
    metadata         jsonb       NOT NULL DEFAULT '{}'::jsonb,
    PRIMARY KEY (signal_id, ticker, as_of_date),
    CONSTRAINT signal_observations_confidence_range
        CHECK (confidence IS NULL OR (confidence >= 0.0 AND confidence <= 1.0)),
    CONSTRAINT signal_observations_value_present
        CHECK (jsonb_typeof(value) IN ('object', 'number', 'string', 'boolean')),
    CONSTRAINT signal_observations_metadata_is_object
        CHECK (jsonb_typeof(metadata) = 'object')
);

COMMENT ON TABLE signal_observations IS 'Universal signal output. Every (signal_id, ticker, as_of_date) tuple is unique. Downstream consumers query this table only — they never touch raw ingestion tables.';

COMMENT ON COLUMN signal_observations.signal_id IS 'References signal_definitions.signal_id. Includes version suffix, e.g. "insider.career_grade.v3", so v3 and v4 rows coexist.';
COMMENT ON COLUMN signal_observations.as_of_date IS 'The point in time at which this value was computable. PIT pin. NEVER set this to wallclock — always to the historical or current moment the compute function targets.';
COMMENT ON COLUMN signal_observations.ingested_at IS 'Wallclock when row was written. Used by the PIT validator to hide rows whose ingested_at > (current_as_of - upstream.pit_lag) when replaying historical compute. Catches "works today, would not have worked then" bugs.';
COMMENT ON COLUMN signal_observations.value IS 'JSON payload. Schema described in signal_definitions.output_schema.';
COMMENT ON COLUMN signal_observations.confidence IS 'Optional [0,1] confidence score. Composite/convergence signals use this; raw observations leave it NULL.';

COMMIT;
