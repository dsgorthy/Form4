-- Deploy pyrrho_dataplane:create_signal_classes_enum to pg
--
-- Signal taxonomy. Every signal_id is namespaced as <class>.<name>.<version>;
-- the class is constrained to this enum so the catalog stays disciplined.
--
-- Idempotent: re-running this migration is a no-op.

BEGIN;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'signal_class') THEN
        CREATE TYPE signal_class AS ENUM (
            'price',           -- raw price observations (prices.daily.close, prices.minute_bars)
            'volume',          -- raw volume (stock + options)
            'insider',         -- SEC insider activity (insider.career_grade, insider.recent_form)
            'options_flow',    -- options activity (options.unusual_volume, options.flow_imbalance)
            'fundamental',     -- company fundamentals (fundamentals.quality_score)
            'analyst',         -- sell-side analyst (analyst.target, analyst.rating, analyst.revision)
            'sentiment',       -- consumer / news sentiment (sentiment.retail, sentiment.news)
            'congress',        -- congressional trading (congress.trades, congress.politician_alpha)
            'earnings',        -- earnings (earnings.transcript, earnings.estimate, earnings.surprise)
            'macro',           -- macro indicators (macro.fred.*, macro.calendar)
            'composite'        -- derived from multiple classes (convergence.v1)
        );
        COMMENT ON TYPE signal_class IS 'Top-level taxonomy. Adding a class is a schema change; adding a signal within a class is a code change only.';
    END IF;
END $$;

COMMIT;
