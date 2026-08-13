-- insiders.slug — stable, human-readable URL segment.
--
-- Insider URLs were /insider/{name-slug}-{sqid}. The sqid suffix guarantees
-- uniqueness but is noise on an SEO surface. Measured collision rate on the
-- name alone is only 0.71% (128,824 insiders, 127,904 distinct slugs), so
-- 99.3% can have a clean /insider/roger-s-penske.
--
-- THE SLUG IS WRITE-ONCE. It is assigned here and must never be regenerated
-- from the name. SEC filings spell the same person inconsistently
-- ("Roger S. Penske" vs "PENSKE ROGER S"), so a future name-normalisation
-- pass would silently rewrite live URLs and discard whatever ranking they had
-- accumulated. Renames get a redirect, never a rewrite. The partial index
-- below only covers rows where slug IS NOT NULL so new inserts do not fail.
--
-- Conflict rule: lowest insider_id wins the clean slug; the rest keep the
-- disambiguated {name}-{sqid} form. Ordering on insider_id (immutable) rather
-- than trade count (which changes) means today's clean URL stays clean even
-- if a more prominent namesake files tomorrow.
--
-- Note: many apparent collisions are duplicate records for ONE entity
-- ("BlueMountain Capital Management, LLC" x7), not distinct people — an
-- entity-resolution artifact. Those resolve to suffixed slugs, which is
-- harmless; entity resolution would reduce the count further.
--
-- Apply:  psql -d form4 -f migrations/2026-08-13_insider_slugs.sql

BEGIN;

ALTER TABLE public.insiders ADD COLUMN IF NOT EXISTS slug text;

CREATE UNIQUE INDEX IF NOT EXISTS insiders_slug_key
    ON public.insiders (slug)
    WHERE slug IS NOT NULL;

CREATE INDEX IF NOT EXISTS insiders_slug_lookup
    ON public.insiders (slug)
    WHERE slug IS NOT NULL;

COMMIT;
