\set ON_ERROR_STOP on

-- BRONZE, second attempt: the corpus is SEC's, not ours.
--
-- WHAT WAS WRONG WITH THE FIRST VERSION
--
-- scripts/fetch_bronze.py derived its work list from `trades`. An independent
-- audit measured `trades` against SEC's own quarterly index and found it holds
-- only ~82% of Form 4 filings:
--
--     2021 Q1   SEC 66,015   trades 54,611   82.7%
--     2024 Q2   SEC 49,647   trades 40,466   81.5%
--
-- corroborated a third way: research.derivative_trades carries 305,111
-- accessions absent from `trades` entirely, and two spot-checks confirmed they
-- are real Form 4s (derivative-only RSU awards, which the parser drops).
--
-- So an archive built from `trades` inherits an 18% hole -- roughly 850,000
-- documents -- and "we will never need to refetch" would have been false the
-- day it finished. An archive is only as complete as the list it works from,
-- and the only authoritative list is SEC's.
--
-- form.idx per quarter is one request and names every Form 4 EDGAR published
-- in it. 83 quarters covers 2006-01-01 to today.
SET lock_timeout = '5s';

CREATE TABLE IF NOT EXISTS bronze.edgar_index (
    accession    TEXT PRIMARY KEY,
    form_type    TEXT NOT NULL,          -- '4' or '4/A'
    cik          TEXT,                   -- the filer named in the index
    company_name TEXT,
    filing_date  DATE,
    quarter      TEXT NOT NULL,          -- '2021QTR1', the source file
    discovered_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_edgar_index_quarter ON bronze.edgar_index (quarter);

COMMENT ON TABLE bronze.edgar_index IS
    'Every Form 4 accession SEC published, from full-index/{Y}/{Q}/form.idx. '
    'THIS is the corpus -- bronze.edgar_submission is measured against it, not '
    'against `trades`, which holds only ~82% of filings.';

-- Track which quarters have been enumerated, so the crawl is resumable and
-- the open quarter can be re-read as it fills.
CREATE TABLE IF NOT EXISTS bronze.edgar_index_progress (
    quarter      TEXT PRIMARY KEY,
    accessions   INTEGER NOT NULL,
    fetched_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    complete     BOOLEAN NOT NULL DEFAULT TRUE   -- false for the current quarter
);

-- ── strengthen the submission CHECK ───────────────────────────────────────
--
-- The first constraint claimed to stop a "half-failed fetch looking identical
-- to a successful one" and did not: it admitted http_status=200 with an EMPTY
-- string, with a NULL byte_len, or with byte_len disagreeing with the stored
-- content by any amount -- which IS the truncated body it was meant to catch.
-- It also allowed a failure row with no explanation at all.
ALTER TABLE bronze.edgar_submission
    DROP CONSTRAINT IF EXISTS bronze_content_matches_status;

ALTER TABLE bronze.edgar_submission
    ADD CONSTRAINT bronze_content_matches_status CHECK (
        (http_status = 200
             AND content IS NOT NULL AND length(content) > 0
             AND sha256 IS NOT NULL
             AND byte_len = octet_length(content))
        OR
        (http_status <> 200 AND content IS NULL AND last_error IS NOT NULL)
    );
