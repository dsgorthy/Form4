\set ON_ERROR_STOP on
-- SILVER: the parsed, typed, JUDGED layer derived from Bronze bytes.
--
-- Bronze (bronze.edgar_submission.content) holds the SEC submission exactly as
-- served. Silver parses it faithfully -- every transaction line, every value
-- as filed, nothing dropped, nothing corrected -- and puts its judgement of
-- the numbers in a SEPARATE column. See docs/silver_layer_model.md for why a
-- flag and not an edit: price_validator.apply_correction overwrote price and
-- value on `trades` and on 2026-09-04 destroyed 29 real GOOG/AMZN/CMG rows
-- because the identity price/qty fired on coincidence.
--
-- Grain is one row per transaction line per reporting owner -- the grain of
-- a Form 4 and of `trades`, so parity is a join, not a mapping.
--
-- Two empty tables in a new schema. Touches nothing that exists. Applied by
-- hand with `psql -d form4 -f migrations/2026-09-13_silver.sql` on Studio.
SET lock_timeout = '5s';

CREATE SCHEMA IF NOT EXISTS silver;

CREATE TABLE IF NOT EXISTS silver.form4_transaction (
  accession             text        NOT NULL,   -- bronze.edgar_submission.accession
  rptowner_cik          text        NOT NULL,
  line_no               smallint    NOT NULL,   -- document order: non-derivative table, then derivative
  is_derivative         boolean     NOT NULL,
  -- filing-level, repeated so the row stands alone
  issuer_cik            text,
  ticker                text,
  period_of_report      date,
  filed_at              timestamp,              -- ACCEPTANCE-DATETIME from the SEC-HEADER. Eastern, naive, AS FILED
                                                -- (the filed_at-is-Eastern rule: never convert)
  document_type         text,                   -- 4 / 4/A / 5 / 5/A
  is_amendment          boolean     NOT NULL DEFAULT false,
  rptowner_name         text,
  rptowner_is_director  boolean,
  rptowner_is_officer   boolean,
  rptowner_is_10pct     boolean,
  rptowner_is_other     boolean,
  rptowner_title        text,
  -- the transaction, EXACTLY AS FILED. NULL means the filing did not say.
  security_title        text,
  trans_date            date,
  deemed_execution_date date,
  trans_form_type       text,
  trans_code            text,
  equity_swap           boolean,
  trans_acquired_disp   text,                   -- A / D
  shares                numeric(24,6),          -- transactionShares/value, never adjusted
  price_per_share       numeric(24,6),          -- transactionPricePerShare/value, never adjusted
  value                 numeric(30,6) GENERATED ALWAYS AS (shares * price_per_share) STORED,
  shares_owned_after    numeric(24,6),
  direct_indirect       text,                   -- D / I
  nature_of_ownership   text,
  -- derivative-only, as filed
  exercise_price        numeric(24,6),
  exercise_date         date,
  expiration_date       date,
  underlying_title      text,
  underlying_shares     numeric(24,6),
  footnote_ids          text[],
  -- the judgement, SEPARATE from the facts. NULL = not yet assessed.
  price_quality         text CHECK (price_quality IN ('ok','outside_band','implausible','no_reference')),
  price_quality_note    text,
  -- provenance
  bronze_sha256         text        NOT NULL,   -- the bytes this row was parsed from
  parser_version        text        NOT NULL,
  parsed_at             timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (accession, rptowner_cik, line_no)
);
COMMENT ON TABLE silver.form4_transaction IS
  'Form 4/5 transaction lines parsed from bronze.edgar_submission, one row per line per reporting owner, values exactly as filed. price_quality is an assessment, never an edit.';
CREATE INDEX IF NOT EXISTS silver_form4_transaction_ticker_date
  ON silver.form4_transaction (ticker, trans_date);
CREATE INDEX IF NOT EXISTS silver_form4_transaction_implausible
  ON silver.form4_transaction (ticker) WHERE price_quality = 'implausible';
CREATE INDEX IF NOT EXISTS silver_form4_transaction_unassessed
  ON silver.form4_transaction (accession) WHERE price_quality IS NULL AND price_per_share IS NOT NULL;

-- One receipt per accession attempted, INCLUDING those that yielded zero
-- rows (a holdings-only Form 4 is legal) and those that failed to parse. The
-- builder's work list is "bronze rows with no receipt", so it self-heals and
-- never re-parses what it has already judged, and a parse failure is a row
-- someone can query rather than a log line nobody reads.
CREATE TABLE IF NOT EXISTS silver.form4_parse (
  accession       text PRIMARY KEY,
  bronze_sha256   text NOT NULL,
  status          text NOT NULL CHECK (status IN ('ok','no_document','parse_error')),
  rows_written    integer NOT NULL DEFAULT 0,
  error           text,
  parser_version  text NOT NULL,
  parsed_at       timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS silver_form4_parse_status ON silver.form4_parse (status) WHERE status <> 'ok';
