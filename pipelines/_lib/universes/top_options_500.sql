-- Universe: top_options_500
-- Scope: Phase 2 #6 — ThetaData 1-min options for liquid underlyings
--
-- Definition: top 500 underlyings by total option contract volume in the
-- last 12 months of EOD option data. Uses option_prices.volume (per-contract
-- daily volume from ThetaData EOD). Excludes index products since SPX work
-- is deferred per the plan.
--
-- Why volume not open interest: open interest captures stale positioning;
-- volume reflects active intraday flow, which is what 1-min data is for.
--
-- Output: ticker (text), total_option_volume (bigint), contract_count (int)
--   psql form4 -At -F'|' -f pipelines/_lib/universes/top_options_500.sql \
--     -o /Volumes/data/form4/manifest/universe_top_options_500.txt

WITH bounds AS (
    SELECT (CURRENT_DATE - INTERVAL '365 days')::date AS lookback_start
),
volume_agg AS (
    SELECT
        op.ticker,
        SUM(COALESCE(op.volume, 0))::bigint AS total_option_volume,
        COUNT(DISTINCT (op.expiration, op.strike, op."right")) AS contract_count,
        MAX(op.trade_date::date) AS last_seen
    FROM option_prices op, bounds b
    WHERE op.trade_date::date >= b.lookback_start
      AND op.ticker NOT IN ('SPX','VIX','RUT','NDX','XSP')  -- index/SPX deferred
    GROUP BY op.ticker
    HAVING SUM(COALESCE(op.volume, 0)) > 0
)
SELECT
    ticker,
    total_option_volume,
    contract_count,
    last_seen
FROM volume_agg
ORDER BY total_option_volume DESC
LIMIT 500;
