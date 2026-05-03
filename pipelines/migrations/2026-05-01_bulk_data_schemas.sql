-- ─────────────────────────────────────────────────────────────────────────────
-- Bulk-data schema additions (Areca rack / Phase 1 + Phase 3)
-- Plan: ~/.claude/plans/here-s-the-housing-thanks-cached-sifakis.md
-- Authored: 2026-05-01
--
-- Apply on Mac Studio against the form4 database:
--   ssh derekg@100.78.9.66 "psql form4 -f -" < pipelines/migrations/2026-05-01_bulk_data_schemas.sql
--
-- All statements are additive (IF NOT EXISTS) and safe to re-run.
-- 1-min equity bars and TAQ are intentionally NOT in PG — they live as
-- Parquet on the array. Only summarized features get loaded into PG.
-- ─────────────────────────────────────────────────────────────────────────────

BEGIN;

-- ── EDGAR archive ───────────────────────────────────────────────────────────
-- Master table of every filing we've fetched. content_url points at SEC's
-- archive URL; raw text/HTML lives on disk under /Volumes/data/form4/edgar/.

CREATE TABLE IF NOT EXISTS edgar_filings (
    accession_number   text PRIMARY KEY,           -- e.g. 0001193125-24-123456
    cik                text NOT NULL,
    company_name       text,
    form_type          text NOT NULL,              -- 8-K, 10-K, 10-Q, 13F-HR, SC 13D, SC 13G, 5, 144
    filed_date         text NOT NULL,              -- YYYY-MM-DD
    period_of_report   text,                       -- YYYY-MM-DD (form-dependent)
    primary_doc_url    text,                       -- canonical URL on SEC.gov
    file_path          text,                       -- relative to /Volumes/data/form4/edgar/
    parse_status       text NOT NULL DEFAULT 'pending',  -- pending | parsed | failed
    parse_error        text,
    fetched_at         timestamptz DEFAULT NOW(),
    parsed_at          timestamptz
);
CREATE INDEX IF NOT EXISTS idx_edgar_filings_cik         ON edgar_filings (cik);
CREATE INDEX IF NOT EXISTS idx_edgar_filings_form_date   ON edgar_filings (form_type, filed_date);
CREATE INDEX IF NOT EXISTS idx_edgar_filings_filed_date  ON edgar_filings (filed_date);
CREATE INDEX IF NOT EXISTS idx_edgar_filings_parse_status ON edgar_filings (parse_status) WHERE parse_status != 'parsed';

-- ── 8-K event details ───────────────────────────────────────────────────────
-- Joins to edgar_filings on accession_number. ticker resolved post-fetch
-- from cik via insiders/issuer mapping.

CREATE TABLE IF NOT EXISTS event_8k (
    accession_number   text PRIMARY KEY REFERENCES edgar_filings(accession_number) ON DELETE CASCADE,
    cik                text NOT NULL,
    ticker             text,                       -- nullable — some filers have no ticker
    filing_date        text NOT NULL,
    event_date         text,                       -- when the underlying event occurred
    item_codes         text[],                     -- ['1.01','5.02', ...]
    summary            text                        -- short extracted summary (first paragraph after item heading)
);
CREATE INDEX IF NOT EXISTS idx_event_8k_ticker_date    ON event_8k (ticker, filing_date);
CREATE INDEX IF NOT EXISTS idx_event_8k_filing_date    ON event_8k (filing_date);
CREATE INDEX IF NOT EXISTS idx_event_8k_item_codes_gin ON event_8k USING gin (item_codes);

-- ── 13F institutional holdings ──────────────────────────────────────────────
-- Quarterly. Period is the 13F-HR period_of_report (quarter end).

CREATE TABLE IF NOT EXISTS form13f_holdings (
    id                 BIGSERIAL PRIMARY KEY,
    accession_number   text NOT NULL REFERENCES edgar_filings(accession_number) ON DELETE CASCADE,
    filer_cik          text NOT NULL,
    filer_name         text,
    period_of_report   text NOT NULL,
    cusip              text NOT NULL,
    ticker             text,                       -- resolved post-fetch
    company_name       text,
    shares             bigint,
    value_usd          numeric(18,2),
    put_call           text                        -- 'PUT' | 'CALL' | NULL for stock
);
CREATE INDEX IF NOT EXISTS idx_13f_filer_period   ON form13f_holdings (filer_cik, period_of_report);
CREATE INDEX IF NOT EXISTS idx_13f_ticker_period  ON form13f_holdings (ticker, period_of_report);
CREATE INDEX IF NOT EXISTS idx_13f_cusip          ON form13f_holdings (cusip);

-- ── 13D / 13G activist + beneficial ownership ───────────────────────────────

CREATE TABLE IF NOT EXISTS form13dg_positions (
    id                 BIGSERIAL PRIMARY KEY,
    accession_number   text NOT NULL REFERENCES edgar_filings(accession_number) ON DELETE CASCADE,
    filer_cik          text NOT NULL,
    filer_name         text,
    schedule_type      text NOT NULL,              -- '13D' | '13G' | '13D/A' | '13G/A'
    subject_cik        text NOT NULL,
    ticker             text,
    shares             bigint,
    percent_owned      numeric(7,4),
    filing_date        text NOT NULL,
    event_date         text                        -- date triggering 13D/13G (purpose change, threshold cross)
);
CREATE INDEX IF NOT EXISTS idx_13dg_filer        ON form13dg_positions (filer_cik);
CREATE INDEX IF NOT EXISTS idx_13dg_ticker_date  ON form13dg_positions (ticker, filing_date);
CREATE INDEX IF NOT EXISTS idx_13dg_schedule     ON form13dg_positions (schedule_type, filing_date);

-- ── Form 144 planned-sale schedules ─────────────────────────────────────────
-- Used by tenb51_surprise strategy: Form 144 announces planned sales by affiliates.

CREATE TABLE IF NOT EXISTS form144_schedules (
    id                 BIGSERIAL PRIMARY KEY,
    accession_number   text NOT NULL REFERENCES edgar_filings(accession_number) ON DELETE CASCADE,
    seller_cik         text NOT NULL,
    seller_name        text,
    issuer_cik         text NOT NULL,
    ticker             text,
    planned_shares     bigint,
    planned_value_usd  numeric(18,2),
    planned_date       text,                       -- "approximate date of sale"
    filing_date        text NOT NULL,
    rule_10b5_1        boolean DEFAULT FALSE       -- box checked on form
);
CREATE INDEX IF NOT EXISTS idx_144_ticker_date    ON form144_schedules (ticker, filing_date);
CREATE INDEX IF NOT EXISTS idx_144_seller         ON form144_schedules (seller_cik);
CREATE INDEX IF NOT EXISTS idx_144_planned_date   ON form144_schedules (planned_date);

-- ── Short interest + borrow rate history ────────────────────────────────────
-- short_interest from FINRA Reg SHO; borrow_rate from IBKR/IBoxx if subscribed.
-- One row per ticker per date. Reporting cadence varies (bi-monthly SI; daily borrow).

CREATE TABLE IF NOT EXISTS short_metrics (
    ticker             text NOT NULL,
    date               text NOT NULL,              -- YYYY-MM-DD
    short_interest     bigint,                     -- shares short (bi-monthly)
    days_to_cover      numeric(8,3),
    short_pct_float    numeric(7,4),               -- short_interest / float
    borrow_rate        numeric(7,4),               -- annualized %, may be NULL
    borrow_available   bigint,                     -- shares available to borrow, may be NULL
    source             text NOT NULL,              -- 'finra_si' | 'ibkr' | 'iboxx'
    PRIMARY KEY (ticker, date, source)
);

-- Daily short volume (separate stream from bi-monthly short interest).
-- FINRA publishes decimals because some venues aggregate fractional algo
-- executions; numeric(20,4) preserves what they send. The `feed` column
-- discriminates between FINRA's two relevant streams:
--   'CNMS' — Consolidated NMS (default; NYSE/Nasdaq/AMEX/ARCA-listed)
--   'FORF' — OTC Reporting Facility (OTC equity, micro-caps)
CREATE TABLE IF NOT EXISTS regsho_daily (
    date                 text NOT NULL,            -- YYYY-MM-DD
    symbol               text NOT NULL,
    feed                 text NOT NULL DEFAULT 'CNMS',
    short_volume         numeric(20,4) NOT NULL,
    short_exempt_volume  numeric(20,4) NOT NULL,
    total_volume         numeric(20,4) NOT NULL,
    market               text,
    PRIMARY KEY (date, symbol, feed)
);
CREATE INDEX IF NOT EXISTS idx_regsho_daily_symbol ON regsho_daily (symbol);
CREATE INDEX IF NOT EXISTS idx_regsho_daily_date   ON regsho_daily (date);
CREATE INDEX IF NOT EXISTS idx_regsho_daily_feed   ON regsho_daily (feed, date);
CREATE INDEX IF NOT EXISTS idx_short_metrics_date   ON short_metrics (date);
CREATE INDEX IF NOT EXISTS idx_short_metrics_ticker ON short_metrics (ticker);

-- ── ETF holdings + flows ────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS etf_holdings (
    etf_ticker         text NOT NULL,
    date               text NOT NULL,              -- YYYY-MM-DD (as-of)
    holding_ticker     text NOT NULL,
    holding_cusip      text,
    weight_pct         numeric(8,5),               -- % of fund AUM
    shares             bigint,
    market_value       numeric(18,2),
    PRIMARY KEY (etf_ticker, date, holding_ticker)
);
CREATE INDEX IF NOT EXISTS idx_etf_holdings_holding ON etf_holdings (holding_ticker, date);
CREATE INDEX IF NOT EXISTS idx_etf_holdings_date    ON etf_holdings (date);

CREATE TABLE IF NOT EXISTS etf_flows (
    etf_ticker         text NOT NULL,
    date               text NOT NULL,
    aum_usd            numeric(18,2),
    net_flow_usd       numeric(18,2),              -- daily creation/redemption $
    shares_outstanding bigint,
    PRIMARY KEY (etf_ticker, date)
);
CREATE INDEX IF NOT EXISTS idx_etf_flows_date ON etf_flows (date);

-- ── Earnings call transcripts ───────────────────────────────────────────────
-- Embedding stored as bytea for now; can convert to pgvector later if extension installed.

CREATE TABLE IF NOT EXISTS earnings_transcripts (
    call_id            text PRIMARY KEY,           -- vendor-supplied or hash(ticker,fq,date)
    ticker             text NOT NULL,
    fiscal_period      text,                       -- '2024Q3'
    call_date          text NOT NULL,
    call_type          text,                       -- 'earnings' | 'guidance' | 'investor_day'
    transcript_text    text NOT NULL,
    embedding          bytea,                      -- pgvector candidate; bytea for now
    embedding_model    text,                       -- 'voyage-3-lite' etc.
    source             text NOT NULL,              -- 'seekingalpha' | 'alphasense' | 'scraped'
    fetched_at         timestamptz DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_transcripts_ticker_date ON earnings_transcripts (ticker, call_date);
CREATE INDEX IF NOT EXISTS idx_transcripts_call_date   ON earnings_transcripts (call_date);

-- ── News corpus ─────────────────────────────────────────────────────────────
-- A single news item may reference multiple tickers; we denormalize via tickers[] array.

CREATE TABLE IF NOT EXISTS news (
    news_id            text PRIMARY KEY,           -- vendor-supplied or hash(url,published_at)
    headline           text NOT NULL,
    body               text,
    tickers            text[] NOT NULL DEFAULT '{}',
    published_at       timestamptz NOT NULL,
    source             text NOT NULL,              -- 'benzinga' | 'reuters' | 'pr_newswire' | 'scraped'
    url                text,
    sentiment          numeric(5,4),               -- nullable; vendor-supplied if available
    fetched_at         timestamptz DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_news_published      ON news (published_at);
CREATE INDEX IF NOT EXISTS idx_news_tickers_gin    ON news USING gin (tickers);
CREATE INDEX IF NOT EXISTS idx_news_source_pub     ON news (source, published_at);

-- ── Generic per-event pull-status pattern ───────────────────────────────────
-- Mirrors option_pull_status. Reusable across pullers (1-min bars, TAQ,
-- EDGAR by quarter, etc.). dataset is the puller's name.

CREATE TABLE IF NOT EXISTS pull_status (
    dataset            text NOT NULL,              -- 'equity_1min' | 'taq_event_window' | 'edgar_8k_q' | ...
    item_key           text NOT NULL,              -- e.g. 'AAPL|2024-03-15' or '0001234567-24-001234'
    status             text NOT NULL,              -- 'done' | 'failed' | 'in_progress'
    attempt_count      integer NOT NULL DEFAULT 0,
    rows_written       bigint,
    bytes_written      bigint,
    error_message      text,
    started_at         timestamptz,
    completed_at       timestamptz,
    PRIMARY KEY (dataset, item_key)
);
CREATE INDEX IF NOT EXISTS idx_pull_status_dataset_status ON pull_status (dataset, status);
CREATE INDEX IF NOT EXISTS idx_pull_status_completed_at   ON pull_status (completed_at);

-- ── Dataset manifest ────────────────────────────────────────────────────────
-- Single-row-per-dataset summary. Updated by puller scripts after each batch.

CREATE TABLE IF NOT EXISTS dataset_manifest (
    dataset            text PRIMARY KEY,           -- 'equity_1min' | 'options_eod_full' | 'edgar_8k' | ...
    storage_root       text NOT NULL,              -- e.g. '/Volumes/data/form4/equity/1min'
    description        text,
    item_count         bigint NOT NULL DEFAULT 0,  -- files / rows depending on puller
    bytes_on_disk      bigint NOT NULL DEFAULT 0,
    last_item_pulled   text,                       -- most recent item_key
    last_updated_at    timestamptz DEFAULT NOW(),
    notes              text
);

COMMIT;
