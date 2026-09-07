\set ON_ERROR_STOP on

-- A QUALITY ASSESSMENT, NOT A CORRECTION.
--
-- WHAT THE SOURCE ACTUALLY SAYS
--
-- The "$231B of phantom value" on AMMA, CNTM and IHT was diagnosed for months
-- as a parse failure -- price_validator's correction is literally named
-- `price_is_total_value`. With Bronze in place that was tested against the
-- source document for the first time on 2026-09-06:
--
--     IHT 0001493152-25-015819
--       trades:  price 22,625  qty 12,500  value $282,812,500
--       XML:     <transactionPricePerShare><value>22625</value>
--                <transactionShares><value>12500</value>
--
-- 62 of 64 suspect rows covered by Bronze match the XML exactly. The parser is
-- faithful. The FILER entered 22,625 as a per-share price for a $1.50 stock.
--
-- So there is nothing to re-parse and nothing to correct. What is needed is to
-- say, in a column, that we do not believe the number -- and to keep
-- publishing what SEC holds on the filing's own page, because the filing is
-- real.
--
-- WHY NOT OVERWRITE price/value
--
-- price_validator.apply_correction does exactly that, moving the original into
-- price_as_filed. That is a derived layer rewriting a source fact, and on
-- 2026-09-04 it destroyed 29 real GOOG/AMZN/CMG trades: the `price/qty`
-- identity fired on coincidence because our price history is split-adjusted
-- and filings are not. A flag is recomputable and reversible; an edit is not.
-- It also does not compose -- $36.5B of the phantom total is still live
-- because a correction landed on one copy of a duplicated filing.
--
-- WHERE THE THRESHOLD COMES FROM, measured 2026-09-06 against a per-ticker
-- monthly price band:
--
--     ratio        rows       tickers   dollars
--     >=10000x       49            10   $48.5B
--     1000-10000x    89            31   $14.8B
--     100-1000x      90            48    $3.8B
--     20-100x    12,343            39   $33.3B     <-- split artifacts
--     3-20x      60,485           170  $228.8B     <-- splits, ADRs, volatility
--
-- 100x is where the population changes character. Below it, 12,343 rows sit on
-- 39 tickers -- 316 filings each, the signature of a SPLIT, which moves every
-- pre-split filing by the same factor. Above it the concentration collapses to
-- ~2 rows per ticker, every affected ticker trades under $10, and the ratios
-- run to 66,441x. No corporate action moves a price 100x in a month; the
-- largest split in the data is 50:1.
SET lock_timeout = '5s';

ALTER TABLE trades ADD COLUMN IF NOT EXISTS price_quality TEXT;

COMMENT ON COLUMN trades.price_quality IS
    'Assessment of the FILED price, never a replacement for it. '
    'NULL = unassessed (no price reference available). '
    '''ok'' = within a plausible band. '
    '''implausible'' = >=100x the ticker''s monthly band; excluded from dollar '
    'aggregates but still shown on the filing page, labelled, because the '
    'filing is real and a reader looking it up must see what SEC holds.';

-- Only two reads: "is this row trustworthy" on a filing, and "exclude the
-- untrustworthy" in an aggregate. Partial, because ok/NULL is the vast
-- majority and never needs the index.
CREATE INDEX IF NOT EXISTS idx_trades_price_implausible
    ON trades (ticker) WHERE price_quality = 'implausible';
