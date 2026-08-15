-- Let the watchlist hold an insider, not only a ticker.
--
-- The table's primary key was (user_id, ticker), so ticker could not be
-- nullable and an insider-only row was unrepresentable. Following a person is
-- the more natural subscription for this product now that insider pages,
-- slugs and search all exist — "tell me when Jim Farley trades" rather than
-- "tell me when F moves".
--
-- Safe to restructure: the table holds 3 rows.
--
-- Applied via: psql -d form4 -f migrations/2026-08-15_watchlist_insider_pk.sql

ALTER TABLE notifications.watchlist DROP CONSTRAINT IF EXISTS watchlist_pkey;

ALTER TABLE notifications.watchlist
    ADD COLUMN IF NOT EXISTS watchlist_id BIGSERIAL PRIMARY KEY;

ALTER TABLE notifications.watchlist ALTER COLUMN ticker DROP NOT NULL;

-- NULLS NOT DISTINCT (PG15+) so a user cannot add the same ticker twice, nor
-- the same insider twice, even though one side of the pair is NULL each time.
-- Without it Postgres treats every NULL as unique and the guard does nothing.
DROP INDEX IF EXISTS notifications.idx_watchlist_unique_target;
CREATE UNIQUE INDEX idx_watchlist_unique_target
    ON notifications.watchlist (user_id, ticker, insider_id) NULLS NOT DISTINCT;
