-- The ticker fan-out path had no index.
--
-- watchlist has idx_watchlist_insider on (insider_id) WHERE insider_id IS NOT
-- NULL, and a unique index on (user_id, ticker, insider_id) — which leads with
-- user_id and therefore cannot serve "who follows AAPL?". Every ticker match
-- was a sequential scan. Irrelevant at 3 rows; at 20,000 it is the hot path of
-- the notification pipeline running without an index.
--
-- Partial, matching the existing insider index, because a row is one or the
-- other.
--
-- Apply: psql -d form4 -f migrations/2026-08-24_watchlist_ticker_index.sql

CREATE INDEX IF NOT EXISTS idx_watchlist_ticker
    ON notifications.watchlist (ticker)
    WHERE ticker IS NOT NULL;
