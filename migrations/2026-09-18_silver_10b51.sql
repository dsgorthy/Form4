\set ON_ERROR_STOP on
-- Silver learns whether a filing was made under a Rule 10b5-1 plan.
--
-- `trades.is_10b5_1` is set by the live parser from TEXT: the filing's
-- remarks, or any footnote, containing "10b5" (backfill_live.py). Since
-- 2023 the form also carries a checkbox, <aff10b5One>1</aff10b5One>, at the
-- filing level. Silver's parser captured neither, so Gold classified every
-- open-market sale as a decision and the first parity report put AAPL at
-- 473 filings against 261 (the other 318 are planned sells in trades).
--
-- The flag is FILING-level, stored on every line of the filing, NULL until
-- the backfill has looked (pipelines/silver/backfill_10b51.py; resumable by
-- quarter through the progress table). Applied by hand on Studio:
--   psql -d form4 -f migrations/2026-09-18_silver_10b51.sql
SET lock_timeout = '5s';

ALTER TABLE silver.form4_transaction ADD COLUMN IF NOT EXISTS aff_10b5_1 boolean;
COMMENT ON COLUMN silver.form4_transaction.aff_10b5_1 IS
  'Filing made under a 10b5-1 plan: the aff10b5One checkbox (2023+) or "10b5" in remarks/footnotes, the same rule trades.is_10b5_1 uses. NULL = not yet backfilled.';

CREATE TABLE IF NOT EXISTS silver.backfill_10b51_progress (
    quarter      text PRIMARY KEY,           -- bronze.edgar_index.quarter, e.g. 2024QTR1
    submissions  integer NOT NULL,
    flagged      integer NOT NULL,           -- filings set true
    done_at      timestamptz NOT NULL DEFAULT now()
);
