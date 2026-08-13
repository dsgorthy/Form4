-- politicians.politician_id sequence was never advanced after the bulk load.
--
-- politician_id defaults to nextval('politicians_politician_id_seq'), but the
-- rows were migrated in with explicit IDs, which does NOT move the sequence.
-- Result: max(politician_id)=409 while the sequence still sat at 1, so the
-- very next INSERT that relies on the default fails with
--   duplicate key value violates unique constraint "politicians_pkey"
--   DETAIL: Key (politician_id)=(1) already exists.
--
-- This is a latent trap for ANY writer, not just the dataplane congress sync
-- that surfaced it — nothing had inserted a politician since the migration,
-- so it sat unnoticed. congress_trades' own sequence was checked and is
-- healthy (max 88049, seq 88335); only this one drifted.
--
-- setval with is_called=true means the NEXT nextval() returns max+1.
--
-- Apply:  psql -d form4 -f migrations/2026-08-12_fix_politicians_sequence.sql

BEGIN;

SELECT setval(
    'politicians_politician_id_seq',
    COALESCE((SELECT max(politician_id) FROM public.politicians), 1),
    true
);

COMMIT;
