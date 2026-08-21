-- Data-model consolidation, pass 1: an exact duplicate and seven empty shells.
--
-- CONTEXT
--
-- form4 is 25 GB. An inventory of all 63 tables against their access counters
-- found roughly 12 GB of it is never read. This migration takes only the part
-- that is unambiguous; the judgement calls are written up separately.
--
-- ── 1. research.filing_footnotes — 1,862 MB, an exact duplicate ─────────────
--
-- public.filing_footnotes and research.filing_footnotes both hold exactly
-- 4,646,211 rows. They are the same SEC footnote corpus stored twice, with
-- different denormalisation:
--
--   public    accession, footnote_id, footnote_text, ticker, filing_date
--   research  footnote_id_pk, accession, footnote_id, footnote_text, created_at
--
-- public is the live one, on three independent pieces of evidence:
--
--   * search_path is "$user", public — so every unqualified reference in the
--     codebase resolves to public.
--   * Every WRITER is unqualified: reparse_bulk.py, backfill_sec_fields.py and
--     strategies/insider_catalog/schema.sql all say `filing_footnotes`.
--   * The ONLY occurrence of `research.filing_footnotes` anywhere in the repo
--     is the ANALYZE line added yesterday in
--     2026-08-19_index_cleanup_and_analyze.sql. Nothing reads it.
--
-- The `research` schema is a migration artefact. backfill.py still carries
-- `RESEARCH_DB = DB_PATH.parent / "research.db"` from the SQLite era; the
-- schema was created by importing that file and the copy was never removed.
--
-- ── 2. Empty duplicate shells ───────────────────────────────────────────────
--
-- public.derivative_trades and public.nonderiv_holdings hold ZERO rows while
-- their research counterparts hold 1,162,052 and 616,459. They are empty
-- shells created by the same import, and an unqualified query against either
-- silently returns nothing — which is a worse failure than a missing table.
--
-- ── 3. Seven tables that are empty and referenced nowhere ───────────────────
--
-- Speculative schema from abandoned feature work. Each has zero rows, zero
-- index scans, zero inserts, and zero references anywhere in api/, frontend/,
-- pipelines/, strategies/, framework/, scripts/ or dataplane/. Other empty
-- tables (news, edgar_filings, short_metrics, pull_status, dataset_manifest,
-- insider_market_sentiment, event_8k, regsho_daily, deploys) are DELIBERATELY
-- LEFT: a bare-word grep matches them in comments and prompt text, and at
-- 24-48 kB each the space is not worth a false positive.
--
-- ── RECOVERY ────────────────────────────────────────────────────────────────
--
-- scripts/backup_databases.sh dumps form4 nightly at 03:15 PT to
-- /Users/derekg/backups/postgres/ and rsyncs off-box to the Mini. Four daily
-- dumps exist, 1.4 GB each. To restore one of these:
--
--   pg_restore -d form4 -t filing_footnotes -n research \
--     /Users/derekg/backups/postgres/form4_20260820_031504.dump
--
-- Applied via: psql -d form4 -f migrations/2026-08-20_drop_duplicate_and_dead_tables.sql

BEGIN;

DO $$
DECLARE
    pub_n bigint;
    res_n bigint;
BEGIN
    -- Refuse to drop the duplicate unless it really is one. If public has
    -- fewer rows than research, the assumption above is wrong and dropping
    -- research would lose data.
    SELECT count(*) INTO pub_n FROM public.filing_footnotes;
    SELECT count(*) INTO res_n FROM research.filing_footnotes;
    IF pub_n < res_n THEN
        RAISE EXCEPTION
            'public.filing_footnotes has % rows against research %. research is NOT redundant — aborting.',
            pub_n, res_n;
    END IF;
    RAISE NOTICE 'filing_footnotes: public=%, research=% — dropping research', pub_n, res_n;
END $$;

DROP TABLE IF EXISTS research.filing_footnotes;

-- Empty shells whose real data lives in the research schema.
DROP TABLE IF EXISTS public.derivative_trades;
DROP TABLE IF EXISTS public.nonderiv_holdings;

-- Empty, unscanned, unreferenced.
DROP TABLE IF EXISTS public.form144_schedules;
DROP TABLE IF EXISTS public.form13f_holdings;
DROP TABLE IF EXISTS public.form13dg_positions;
DROP TABLE IF EXISTS public.earnings_transcripts;
DROP TABLE IF EXISTS public.solo_insider_signals;
DROP TABLE IF EXISTS public.etf_holdings;
DROP TABLE IF EXISTS public.etf_flows;

DO $$
DECLARE
    sz text;
    n  integer;
BEGIN
    SELECT pg_size_pretty(pg_database_size(current_database())) INTO sz;
    SELECT count(*) INTO n FROM pg_stat_user_tables;
    RAISE NOTICE 'form4 is now %, across % tables', sz, n;
END $$;

COMMIT;
