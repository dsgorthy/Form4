-- A failed fetch must be a retry, not a permanent hole.
--
-- The work list is "no bronze row", so the first non-200 written for an
-- accession excluded it from the corpus forever. Measured 2026-09-08: 575 of
-- 1,230,705 rows were failures, 522 of them HTTP 404 — and SIX SAMPLED 404s
-- ALL RETURN 200 when re-fetched at the same URL with the CIK from the index.
-- They are SEC hiccups under sustained load, not absent documents. The rate is
-- steady at ~0.1% of each hour rather than one burst, which extrapolates to
-- ~2,000 poisoned filings across the full 4.1M corpus.
--
-- `attempts` lets the work list re-offer a failure a bounded number of times,
-- so a transient 404 heals itself while a genuinely absent document stops
-- being retried instead of looping forever.
ALTER TABLE bronze.edgar_submission
    ADD COLUMN IF NOT EXISTS attempts INT NOT NULL DEFAULT 1;

-- Drives the retry arm of the work list. Partial: successes are ~99.9% of the
-- table and never need re-offering.
CREATE INDEX IF NOT EXISTS idx_bronze_retryable
    ON bronze.edgar_submission (attempts, fetched_at)
 WHERE http_status <> 200;
