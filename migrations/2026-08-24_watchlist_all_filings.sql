-- Per-user opt-out from the meaningful-filings default on watchlist alerts.
--
-- 71.6% of Form 4s are mechanical — 10b5-1 plans, compensation grants, tax
-- withholding, option exercises. Watching one active ticker unfiltered is
-- roughly three alerts for every one worth reading, so the default is
-- api.filters.MEANINGFUL_CLASSES (discretionary buys and sells) and this
-- column turns the filter off for users who want the raw stream.
--
-- Default 0 = filtered. Existing rows inherit the new default, which is a
-- behaviour change for the 6 current users and is the intent.
--
-- Apply: psql -d form4 -f migrations/2026-08-24_watchlist_all_filings.sql

ALTER TABLE notifications.notification_preferences
    ADD COLUMN IF NOT EXISTS watchlist_all_filings smallint NOT NULL DEFAULT 0;
