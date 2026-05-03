-- Universe: insider_active
-- Scope: Phase 1 #3 — ThetaData EOD options expansion
--
-- Definition: every ticker that has had at least one insider open-market
-- trade (buy or sell, P/S/A/D) reported in the `trades` table since 2016.
-- This is the natural population for options EOD coverage because we never
-- need contract data for a ticker we'll never trade or backtest against.
--
-- ~26% of these will not have listed options (OTC, micro-cap) — the puller
-- will mark those as 'no-data' in option_pull_status; they are not retried.
--
-- Output: ticker (text). Save to file for the puller to consume:
--   psql form4 -At -f pipelines/_lib/universes/insider_active.sql \
--     -o /Volumes/data/form4/manifest/universe_insider_active.txt

SELECT DISTINCT t.ticker
FROM trades t
WHERE t.filing_date >= '2016-01-01'
  AND t.ticker IS NOT NULL
  AND t.ticker !~ '\s'                          -- drop malformed
  AND length(t.ticker) BETWEEN 1 AND 6
ORDER BY t.ticker;
