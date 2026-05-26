-- =============================================================================
-- 2026-05-25: trade_narrative — LLM-generated "why this trade matters" per
-- high-signal insider purchase.
--
-- Only populated for the ~50-200/week trades that pass the high-signal
-- filter (see scripts/demo_narratives.py for the predicate). Routine
-- 10b5-1 / tax / recurring trades get NULL.
--
-- ROLLBACK: DROP TABLE trade_narrative;
-- =============================================================================

BEGIN;

CREATE TABLE IF NOT EXISTS trade_narrative (
    trade_id            BIGINT PRIMARY KEY,
    generated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    -- Hash of (insider, ticker, dollar_amount, news headlines, P/E, ...).
    -- Lets us detect when inputs have changed and regenerate instead of
    -- re-running every refresh cycle on unchanged data.
    inputs_sha          TEXT NOT NULL,

    -- Structured narrative — the LLM produces these as a JSON object
    -- and we unpack into columns for easy display + querying.
    summary             TEXT,     -- 2 sentences: what the trade is + why it matters
    price_context       TEXT,     -- "Down X% YTD, near 52w low after Y catalyst"
    catalysts           TEXT,     -- 1-3 specific catalysts in the next 90 days
    risks               TEXT,     -- 1-2 specific risks to the thesis

    -- Reproducibility / debugging
    input_data          JSONB,    -- the structured payload we sent to the LLM
    model_name          TEXT,     -- "glm-4.7-flash" / "claude-sonnet-4-6" / etc.
    generation_ms       INTEGER,
    error               TEXT      -- if generation failed, what happened
);

CREATE INDEX IF NOT EXISTS idx_trade_narrative_generated_at
    ON trade_narrative (generated_at DESC);

COMMENT ON TABLE trade_narrative IS
    'Per-trade LLM-generated narrative for high-signal insider buys. Only '
    'populated for trades that pass the high-signal filter (C-suite OR rare '
    'reversal OR largest-ever OR cluster>=3 OR A+/A grade, AND not 10b5-1 '
    'OR recurring OR tax-sale OR routine, AND >=$10k). NULL for the ~95% '
    'of trades that are routine.';

COMMIT;
