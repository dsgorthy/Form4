\set ON_ERROR_STOP on

-- Performance indexes, 2026-09-02. Every one built CONCURRENTLY.
--
-- CONCURRENTLY is not optional here: a plain CREATE INDEX takes ACCESS
-- EXCLUSIVE on the table, and an ALTER queueing for that lock on `trades` is
-- what took form4.app down on 2026-08-27. CONCURRENTLY cannot run inside a
-- transaction block, so this file must be run with psql -f, not wrapped.

-- 1. The feed's date window.
--
-- The feed filters on the last N days. Written as
-- COALESCE(filed_at, filing_date) >= X it is unindexable in practice: Postgres
-- estimated 705,719 rows for a 14-day window against a true 6,069 and chose a
-- sequential scan, reading 1,653,440 buffers to return 25 rows.
--
-- An expression index on that COALESCE, even with statistics raised to 2000,
-- did not move the estimate. The application query was rewritten as a
-- disjunction on the two underlying columns instead, which these back.
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_trades_effective_ts
    ON trades ((COALESCE(filed_at, filing_date)) DESC)
 WHERE superseded_by IS NULL AND is_derivative = 0;

-- 2. Search. ILIKE '%term%' cannot use a btree at all, so search was scanning
-- 7.1M trades rows and 212k insiders per keystroke. GIN trigram indexes are
-- the standard answer and took search from 7.8s to 0.5s.
CREATE EXTENSION IF NOT EXISTS pg_trgm;

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_trades_ticker_trgm
    ON trades USING gin (ticker gin_trgm_ops);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_trades_company_trgm
    ON trades USING gin (company gin_trgm_ops);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_insiders_name_trgm
    ON insiders USING gin (name gin_trgm_ops);

ANALYZE trades;
ANALYZE insiders;
