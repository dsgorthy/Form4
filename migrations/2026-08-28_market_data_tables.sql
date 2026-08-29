-- Shares outstanding, and the long-horizon return labels.
--
-- Blocking DDL queues at the head of the lock queue and stalls every later
-- read on the table (form4.app outage, 2026-08-27). Abort instead of queueing.
SET lock_timeout = '3s';

-- ---------------------------------------------------------------------------
-- Shares outstanding, point-in-time.
--
-- From EDGAR XBRL dei:EntityCommonStockSharesOutstanding, reported on the cover
-- page of every 10-Q and 10-K. Free, keyless, same host as the SIC backfill.
--
-- TWO DATES, AND THE DIFFERENCE MATTERS. `as_of_date` is what the count is a
-- count of; `filed_date` is when the filing carrying it appeared. They differ
-- by months -- TSLA's 2026-01-23 figure was filed 2026-04-30. Every PIT join
-- must use filed_date <= filing_date. Joining on as_of_date is a look-ahead of
-- exactly the class that put 37 look-ahead entries in the books when filed_at
-- was read as UTC.
CREATE TABLE IF NOT EXISTS issuer_shares_outstanding (
    cik           TEXT        NOT NULL,
    as_of_date    TEXT        NOT NULL,   -- XBRL "end"
    filed_date    TEXT        NOT NULL,   -- XBRL "filed" -- USE THIS TO JOIN
    shares        BIGINT      NOT NULL,
    form          TEXT,                   -- 10-Q, 10-K, 10-K/A ...
    accession     TEXT,
    source        TEXT        NOT NULL DEFAULT 'edgar_xbrl',
    fetched_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (cik, as_of_date, filed_date)
);
CREATE INDEX IF NOT EXISTS idx_iso_cik_filed
    ON issuer_shares_outstanding (cik, filed_date);

-- Per-issuer fetch bookkeeping, so a resumed run skips what it already has and
-- a permanently-absent issuer is not retried forever.
CREATE TABLE IF NOT EXISTS issuer_shares_status (
    cik           TEXT PRIMARY KEY,
    status        TEXT NOT NULL,          -- ok | empty | error
    n_rows        INTEGER NOT NULL DEFAULT 0,
    attempts      INTEGER NOT NULL DEFAULT 0,
    last_error    TEXT,
    last_attempt  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ---------------------------------------------------------------------------
-- Long-horizon filing-anchored labels.
--
-- HORIZONS stopped at 42 trading days (~2 months), which is why the cluster
-- question is unanswerable: we measure cluster harm at 21d while the literature
-- ties 3+ insider clusters to above-market TWELVE-MONTH returns. Both can be
-- true. 63/126/189/252td are 3/6/9/12 months.
--
-- The most recent ~12 months of filings will hold NULL at 252td by
-- construction. That is maturity, not missingness -- check_attribute_coverage
-- exempts the current year for windows that cannot have matured.
ALTER TABLE trade_returns ADD COLUMN IF NOT EXISTS abnormal_63td_from_filing  DOUBLE PRECISION;
ALTER TABLE trade_returns ADD COLUMN IF NOT EXISTS abnormal_126td_from_filing DOUBLE PRECISION;
ALTER TABLE trade_returns ADD COLUMN IF NOT EXISTS abnormal_189td_from_filing DOUBLE PRECISION;
ALTER TABLE trade_returns ADD COLUMN IF NOT EXISTS abnormal_252td_from_filing DOUBLE PRECISION;

-- ---------------------------------------------------------------------------
-- short_metrics already exists with the right shape and zero rows. Only the
-- provenance and uniqueness guarantees are missing.
ALTER TABLE short_metrics ADD COLUMN IF NOT EXISTS avg_daily_volume BIGINT;
ALTER TABLE short_metrics ADD COLUMN IF NOT EXISTS fetched_at TIMESTAMPTZ DEFAULT NOW();
CREATE UNIQUE INDEX IF NOT EXISTS idx_short_metrics_ticker_date
    ON short_metrics (ticker, date);
