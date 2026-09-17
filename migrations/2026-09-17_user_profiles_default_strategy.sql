\set ON_ERROR_STOP on
-- The onboarding form's one real question -- which strategy book to open
-- with -- was never stored: the API demanded three fields the form stopped
-- sending and 422'd every submit (api/routers/onboarding.py, 2026-09-17).
-- One nullable column; the table has 2 rows. Applied by hand on Studio:
--   psql -d form4 -f migrations/2026-09-17_user_profiles_default_strategy.sql
SET lock_timeout = '5s';
ALTER TABLE notifications.user_profiles ADD COLUMN IF NOT EXISTS default_strategy text;
