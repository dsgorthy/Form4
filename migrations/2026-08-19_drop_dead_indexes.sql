-- Two more indexes on columns no query touches.
--
-- Following 2026-08-19_index_cleanup_and_analyze.sql, ten indexes on `trades`
-- still showed zero scans over seven days. Zero scans alone is not enough to
-- drop one — it can mean the planner currently prefers another path, and a
-- rare or seasonal query might still need it.
--
-- The decisive test is different: is the indexed COLUMN referenced by any
-- query in the codebase at all? An index on a column nothing filters, joins
-- or sorts by cannot be used by any plan, now or later.
--
--   idx_trades_signal_category  96 MB  signal_category — 0 references
--   idx_trades_timeliness       54 MB  timeliness      — 0 references
--
-- The other eight all index columns that ARE queried (signal_grade 57
-- references, is_routine 21, rptowner_cik 13, relationship 12, txn_group_id 6,
-- issuer_cik 5, document_type 3) plus idx_trades_amend_match on the ingest
-- path. They stay.
--
-- Deliberately NOT dropping them on this pass. The ANALYZE in the previous
-- migration gave the planner real statistics for the first time — it believed
-- prices.daily_prices held 280k rows when it holds 13.1M — so plans are being
-- chosen differently from today. Index-usage counts gathered BEFORE that
-- change do not predict behaviour after it. Re-check idx_scan in a week of
-- normal operation and decide then, on fresh evidence.
--
-- Applied via: psql -d form4 -f migrations/2026-08-19_drop_dead_indexes.sql

DROP INDEX IF EXISTS public.idx_trades_signal_category;
DROP INDEX IF EXISTS public.idx_trades_timeliness;

DO $$
DECLARE total text; n integer;
BEGIN
    SELECT pg_size_pretty(sum(pg_relation_size(indexrelid))), count(*)
      INTO total, n FROM pg_stat_user_indexes WHERE relname = 'trades';
    RAISE NOTICE 'trades: % indexes, % total', n, total;
END $$;
