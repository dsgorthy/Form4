-- Drop two dead indexes and give the planner statistics it has never had.
--
-- (1) TWO INDEXES ON DEPRECATED COLUMNS
--
-- Over seven days of uninterrupted stats — Postgres has not restarted since
-- 2026-08-11 and pg_stat has never been reset — these two were scanned zero
-- times, while `trades` took a write every five minutes from insider-fetch.
-- Every one of those writes maintained them.
--
--   idx_trades_signal_quality  328 MB  on signal_quality
--   idx_trades_pit             274 MB  on (pit_n_trades, pit_win_rate_7d)
--
-- Both index columns the codebase has already retired. signal_quality.py is
-- documented as orphaned and carries a known PIT violation; pit_n_trades and
-- pit_win_rate_7d are the insider_track_records fallback chain, which nothing
-- ranks on any more after 2026-08-18.
--
-- NOT dropping idx_trades_amend_match (131 MB) despite also showing zero
-- scans: it covers (rptowner_cik, ticker, trade_date, trans_code), which is
-- the amendment-matching path during ingest. Amendments are rare enough that
-- seven days is not conclusive, and a slow ingest is worse than 131 MB.
--
-- Reversible — the CREATE statements are above. Rebuilding either takes a few
-- minutes on 1.7M rows.
--
-- (2) TABLES THE PLANNER IS BLIND TO
--
-- Several large tables have never been ANALYZEd, so Postgres has no row
-- counts or column distributions for them and plans queries by guessing.
-- pg_stat_user_tables reported them as having ZERO rows, which is how stale
-- these statistics are — prices.option_prices alone holds 23.5M.
--
-- prices.daily_prices matters most: the simulator and the portfolio API read
-- it constantly.
--
-- Applied via: psql -d form4 -f migrations/2026-08-19_index_cleanup_and_analyze.sql

DROP INDEX IF EXISTS public.idx_trades_signal_quality;
DROP INDEX IF EXISTS public.idx_trades_pit;

ANALYZE prices.daily_prices;
ANALYZE prices.option_prices;
ANALYZE research.filing_footnotes;
ANALYZE research.derivative_trades;
ANALYZE public.trade_context;
ANALYZE public.score_history;
ANALYZE public.trade_decision_audit;
ANALYZE public.processed_filings;
ANALYZE public.trades;

DO $$
DECLARE idx_total text;
BEGIN
    SELECT pg_size_pretty(sum(pg_relation_size(indexrelid)))
      INTO idx_total FROM pg_stat_user_indexes WHERE relname = 'trades';
    RAISE NOTICE 'trades index total now: %', idx_total;
END $$;
