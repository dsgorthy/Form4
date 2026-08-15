-- User-defined alert filters, and watching an insider rather than only a ticker.
--
-- Today a user gets exactly one implicit filter: the columns on
-- notification_preferences (min_trade_value, min_insider_tier, plus a boolean
-- per event type). That cannot express "career grade A+ in my watchlist
-- tickers" and it cannot express two different rules at once.
--
-- Modelling filters as rows rather than columns gives all three tiers with no
-- migration between them:
--   choose one     one filter, one condition
--   apply several  several filters, OR'd
--   composite      one filter, several conditions, AND'd
--
-- Conditions AND within a filter; filters OR across. "A+ anywhere" and "any
-- grade in my tickers" become two rows, not a feature request.
--
-- notification_preferences is left untouched and keeps working as the default
-- filter, so nobody's current settings change. A user with no rows here
-- behaves exactly as before.
--
-- Applied via: psql -d form4 -f migrations/2026-08-15_alert_filters.sql

CREATE TABLE IF NOT EXISTS notifications.alert_filters (
    filter_id   BIGSERIAL PRIMARY KEY,
    user_id     TEXT        NOT NULL,
    name        TEXT        NOT NULL,
    event_type  TEXT,                      -- NULL = applies to every event type
    enabled     BOOLEAN     NOT NULL DEFAULT TRUE,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_alert_filters_user
    ON notifications.alert_filters (user_id) WHERE enabled;

-- One row per condition. `field` names a column the scanner can evaluate;
-- `op` and `value` are text so a numeric threshold and a ticker set share the
-- same shape. The scanner validates `field` against an allow-list — this table
-- is user input and must never reach a query as an identifier unchecked.
CREATE TABLE IF NOT EXISTS notifications.alert_filter_conditions (
    condition_id BIGSERIAL PRIMARY KEY,
    filter_id    BIGINT NOT NULL REFERENCES notifications.alert_filters(filter_id) ON DELETE CASCADE,
    field        TEXT   NOT NULL,          -- career_grade | pit_grade | value | ticker | trans_code | ...
    op           TEXT   NOT NULL,          -- gte | lte | eq | in | is_true | is_false
    value        TEXT   NOT NULL           -- 'A', '50000', 'NVDA,AAPL'
);

CREATE INDEX IF NOT EXISTS idx_alert_filter_conditions_filter
    ON notifications.alert_filter_conditions (filter_id);

-- Follow a person, not just a company. The insider pages, slugs and search all
-- exist now, and "tell me when this person trades" is the more natural
-- subscription for this product than "tell me when this ticker moves".
-- Nullable both ways: a row watches a ticker, an insider, or both.
ALTER TABLE notifications.watchlist
    ADD COLUMN IF NOT EXISTS insider_id INTEGER;

ALTER TABLE notifications.watchlist
    ALTER COLUMN ticker DROP NOT NULL;

CREATE INDEX IF NOT EXISTS idx_watchlist_insider
    ON notifications.watchlist (insider_id) WHERE insider_id IS NOT NULL;

-- A row that watches nothing is meaningless and would silently match everything.
ALTER TABLE notifications.watchlist
    DROP CONSTRAINT IF EXISTS watchlist_target_present;
ALTER TABLE notifications.watchlist
    ADD CONSTRAINT watchlist_target_present
    CHECK (ticker IS NOT NULL OR insider_id IS NOT NULL);
