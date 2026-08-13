-- Insider slug aliases — retire a slug without breaking its URL.
--
-- Slugs were write-once because regenerating them from names would silently
-- rewrite live URLs and throw away accumulated ranking. That rule protected
-- URLs at the cost of freezing mistakes: correcting "Prestridge III John R"
-- to "John R. Prestridge III" left the page permanently at
-- /insider/iii-john-r-prestridge.
--
-- This table changes the invariant from "never change a slug" to "never break
-- a URL". A retired slug is recorded here and keeps resolving (301 -> current
-- slug), so name corrections are free from now on.
--
-- Applied via: psql -d form4 -f migrations/2026-08-14_insider_slug_aliases.sql

CREATE TABLE IF NOT EXISTS insider_slug_aliases (
    old_slug    TEXT PRIMARY KEY,
    insider_id  INTEGER NOT NULL REFERENCES insiders(insider_id) ON DELETE CASCADE,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- The resolver looks up by old_slug (PK, covered). This index serves the
-- reverse question — "what URLs used to point here?" — for auditing.
CREATE INDEX IF NOT EXISTS idx_insider_slug_aliases_insider
    ON insider_slug_aliases (insider_id);
