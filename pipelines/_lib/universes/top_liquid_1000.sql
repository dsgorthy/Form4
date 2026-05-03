-- Universe: top_liquid_1000
-- Scope: Phase 1 #1 priority order; Phase 2 #7 TAQ event windows
--
-- Definition: top 1,000 underlyings by 90-day median dollar volume on the
-- most recent 6 months of daily price data. Median (not mean) so a single
-- short-squeeze day doesn't pull a thinly-traded name into the universe.
--
-- Why "median over 90 days" not raw volume:
--   * Median rejects outlier days
--   * 90-day window smooths around earnings / re-rating events
--   * 6-month lookback ensures we use only recent regime data
--
-- Tune AS_OF and LOOKBACK as the array fills with newer prices. Currently
-- daily_prices runs to whatever update_daily_prices last filled.
--
-- Output: ticker (text), median_dollar_volume (numeric), latest_close (numeric)
--   psql form4 -At -F'|' -f pipelines/_lib/universes/top_liquid_1000.sql \
--     -o /Volumes/data/form4/manifest/universe_top_liquid_1000.txt

WITH window_bounds AS (
    SELECT
        (CURRENT_DATE - INTERVAL '180 days')::date AS lookback_start,
        CURRENT_DATE::date AS as_of
),
recent AS (
    SELECT
        dp.ticker,
        dp.close,
        dp.volume,
        dp.date,
        dp.close * dp.volume AS dollar_volume
    FROM prices.daily_prices dp, window_bounds w
    WHERE dp.date::date >= w.lookback_start
      AND dp.volume > 0
      AND dp.close > 0
),
ranked AS (
    SELECT
        ticker,
        percentile_cont(0.5) WITHIN GROUP (ORDER BY dollar_volume) AS median_dollar_volume,
        MAX(date::date) AS last_seen,
        COUNT(*) AS day_count
    FROM recent
    GROUP BY ticker
    HAVING COUNT(*) >= 90  -- need at least 90 trading days of data
)
SELECT
    r.ticker,
    ROUND(r.median_dollar_volume::numeric, 0) AS median_dollar_volume,
    r.day_count,
    r.last_seen
FROM ranked r
ORDER BY r.median_dollar_volume DESC
LIMIT 1000;
