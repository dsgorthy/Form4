-- \set ON_ERROR_STOP on
--
-- Without this, psql -f runs every statement, prints the errors, and EXITS 0.
-- On 2026-08-28 the lock_timeout guard below correctly aborted every ALTER in
-- this file while a Dagster run held AccessShareLock on trades -- and the
-- migration reported success. Half-applied schema that claims to be applied is
-- worse than a failure.
\set ON_ERROR_STOP on

-- Derived trade features. All computable from data we already hold.
--
-- Blocking DDL queues at the head of the lock queue and stalls every later
-- read on the table (form4.app outage, 2026-08-27). Abort instead of queueing.
SET lock_timeout = '3s';

-- Conviction, from the filing itself. shares_owned_after is 95.8% populated
-- (99%+ since 2020), so how much an insider grew their OWN stake needs no new
-- source. value_owned_after is the empty column (194 rows) and is just
-- shares x price, so it is not used.
ALTER TABLE trades ADD COLUMN IF NOT EXISTS pct_of_prior_holding DOUBLE PRECISION;

-- Disclosure timing. trans_timeliness is populated on 427 of 317,901 rows and
-- is unusable, but the lag is directly computable. Values outside [0, 365] are
-- left NULL: 48 rows have a filing_date BEFORE the trade_date and one is
-- 730,485 days, which are data errors rather than slow filers.
ALTER TABLE trades ADD COLUMN IF NOT EXISTS filing_lag_days INTEGER;

-- Momentum the buyer could see, measured to the FILING date, which is when we
-- could act. The literature's strongest single feature is distance from the
-- 52-week high (36% of importance in the microcap study), and purchases
-- disclosed into strength outperform those into weakness.
ALTER TABLE trades ADD COLUMN IF NOT EXISTS ret_20d_pre_filing  DOUBLE PRECISION;
ALTER TABLE trades ADD COLUMN IF NOT EXISTS ret_60d_pre_filing  DOUBLE PRECISION;
ALTER TABLE trades ADD COLUMN IF NOT EXISTS pct_off_52w_high    DOUBLE PRECISION;

-- The move between transaction and disclosure. The insider saw it; we ignore
-- it today. A large run-up between trade and filing changes what the signal is
-- worth by the time anyone can act on it.
ALTER TABLE trades ADD COLUMN IF NOT EXISTS ret_trade_to_filing DOUBLE PRECISION;

-- Size normalised by liquidity. The absolute-dollar decile curve is HUMP
-- shaped -- the bottom two deciles (under $3,024) return a third of the middle
-- while the top decile is worse than the eighth -- so raw dollars is the wrong
-- variable. Trade value over 20-day average dollar volume is the standard
-- normalisation and prices.daily_prices already carries volume.
ALTER TABLE trades ADD COLUMN IF NOT EXISTS value_pct_of_adv DOUBLE PRECISION;

CREATE INDEX IF NOT EXISTS idx_trades_filing_lag ON trades (filing_lag_days)
    WHERE filing_lag_days IS NOT NULL;
