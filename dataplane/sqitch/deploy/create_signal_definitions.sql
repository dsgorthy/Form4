-- Deploy pyrrho_dataplane:create_signal_definitions to pg
-- requires: create_signal_classes_enum
--
-- The signal catalog. Every signal that lands rows in signal_observations
-- MUST first register itself here. The catalog is the contract — signal_id,
-- version, owner, upstream deps, SLA, lifecycle status.
--
-- Why this matters: when a downstream consumer (Claude / strategy / backtest)
-- asks "what signals exist for ticker X?", it queries this table. When a
-- signal SLA is breached, the alert references this row. When a signal is
-- deprecated, status flips here.

BEGIN;

CREATE TABLE IF NOT EXISTS signal_definitions (
    signal_id            text         NOT NULL,
    version              text         NOT NULL,
    signal_class         signal_class NOT NULL,
    description          text         NOT NULL DEFAULT '',
    owner                text         NOT NULL,
    output_schema        jsonb        NOT NULL DEFAULT '{}'::jsonb,
    upstream             jsonb        NOT NULL DEFAULT '[]'::jsonb,
    sla_hours            real         NOT NULL,
    business_hours_only  boolean      NOT NULL DEFAULT true,
    status               text         NOT NULL DEFAULT 'active',
    registered_at        timestamptz  NOT NULL DEFAULT now(),
    last_modified_at     timestamptz  NOT NULL DEFAULT now(),
    PRIMARY KEY (signal_id, version),
    CONSTRAINT signal_definitions_status_chk
        CHECK (status IN ('active', 'deprecated', 'archived')),
    CONSTRAINT signal_definitions_sla_positive
        CHECK (sla_hours > 0),
    CONSTRAINT signal_definitions_upstream_is_array
        CHECK (jsonb_typeof(upstream) = 'array'),
    CONSTRAINT signal_definitions_output_schema_is_object
        CHECK (jsonb_typeof(output_schema) = 'object')
);

COMMENT ON TABLE signal_definitions IS 'Signal catalog. Every row in signal_observations references a (signal_id, version) tuple registered here.';

COMMENT ON COLUMN signal_definitions.signal_id IS 'Namespaced identifier, e.g. "insider.career_grade". The version column qualifies further; together they form the primary key.';
COMMENT ON COLUMN signal_definitions.version IS 'Semver-ish: "v3.0.1". Old versions stay alongside new ones.';
COMMENT ON COLUMN signal_definitions.upstream IS 'JSON array of upstream dependencies, each {signal_id, pit_lag_seconds}. PIT lag is the SLA delay between an upstream observation becoming true in the world and becoming queryable in our data plane.';
COMMENT ON COLUMN signal_definitions.sla_hours IS 'Max staleness before runner halts or alert fires. Business-hours-aware when business_hours_only=true (default).';
COMMENT ON COLUMN signal_definitions.status IS 'active = currently writing rows. deprecated = kept readable, no new writes. archived = rows rolled to Parquet, table-level purged.';

COMMIT;
