\set ON_ERROR_STOP on
SET lock_timeout = '3s';

-- Earnings announcements, from EDGAR.
--
-- An 8-K carrying Item 2.02 ("Results of Operations and Financial Condition")
-- IS the earnings release. 10-Q/10-K are the periodic reports that follow and
-- are kept as a fallback for issuers that do not file a 2.02.
--
-- acceptance_datetime is stored because the same discipline applies here as
-- everywhere else: an announcement accepted at 16:31 ET is not tradeable until
-- the next session, and treating the date as the moment is the mistake that
-- produced four look-aheads in one week.
CREATE TABLE IF NOT EXISTS issuer_earnings (
    cik                 TEXT NOT NULL,
    announce_date       TEXT NOT NULL,
    acceptance_datetime TEXT,
    form                TEXT NOT NULL,
    items               TEXT,
    accession           TEXT,
    source              TEXT NOT NULL DEFAULT 'edgar_8k_202',
    fetched_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (cik, announce_date, form)
);
CREATE INDEX IF NOT EXISTS idx_issuer_earnings_cik_date
    ON issuer_earnings (cik, announce_date);

CREATE TABLE IF NOT EXISTS issuer_earnings_status (
    cik          TEXT PRIMARY KEY,
    status       TEXT NOT NULL,
    n_rows       INTEGER NOT NULL DEFAULT 0,
    attempts     INTEGER NOT NULL DEFAULT 0,
    last_error   TEXT,
    last_attempt TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
