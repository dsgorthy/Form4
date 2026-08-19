-- trades.filed_at held two timezones in one column. Normalise to Eastern.
--
-- WHAT DEREK FOUND
--
-- CDNL, accession 0001213900-26-090850. SEC's filing index says
-- "Accepted 2026-08-17 17:37:34" — after the bell. We booked the entry on
-- 2026-08-17 at that day's close of $39.34. The first price anyone could
-- actually have paid was 2026-08-18's open, $41.74: a 6.1% head start we did
-- not have.
--
-- THE TWO POPULATIONS
--
-- filed_at is TEXT and the ingest path changed at the turn of the year:
--
--   filing_date <= 2025-12-31   UTC. Bulk/historical ingest. 72-93% of rows
--                               fall in hours 20-23 + 00-03, which is ET
--                               16:00-22:00 shifted forward.
--   filing_date >= 2026-01-01   EASTERN. backfill_live.fetch_form4_xml
--                               scrapes the "Accepted" field out of EDGAR's
--                               index HTML, which SEC publishes in ET, and
--                               stores the string verbatim. 63-70% of rows
--                               land in hours 15-18, EDGAR's close rush.
--
-- The boundary is sharp: 2025-12-31 is 72% UTC-shaped, 2026-01-02 is 27%,
-- and by 2026-01-08 it is 2%.
--
-- Every reader assumed UTC throughout — the simulator's tradeable_same_day,
-- form4_entry_before_publication, and framework.decision.entry_timing. On a
-- 2026 filing that subtracts four hours it should not, so 17:37 reads as
-- 13:37, the guard says "before the close", and the entry books against a
-- price that had already printed.
--
-- 37 of 278 published positions were entered a day early because of this.
--
-- WHY NORMALISE RATHER THAN BRANCH IN EVERY READER
--
-- There are at least four readers and they have already drifted from each
-- other once — the 2026-08-18 migration fixed an inverted copy of this same
-- rule in SQL while the Python was correct. A column that means one thing is
-- the only version that cannot drift. After this, filed_at is Eastern, always,
-- and every reader can drop its conversion.
--
-- DST: ET is UTC-4 from the second Sunday in March to the first Sunday in
-- November, UTC-5 otherwise. Computed per row from the actual timestamp
-- rather than approximated by month, because a filing on 2026-03-07 and one
-- on 2026-03-09 differ by an hour.
--
-- Guarded by schema_migrations: running twice would shift 2025 rows a second
-- time and silently move a decade of filings into the afternoon.
--
-- Applied via: psql -d form4 -f migrations/2026-08-19_filed_at_normalize_eastern.sql

BEGIN;

DO $$
DECLARE
    already boolean;
    n_before integer;
    n_after  integer;
BEGIN
    SELECT EXISTS (SELECT 1 FROM schema_migrations
                    WHERE version = '2026-08-19_001_filed_at_eastern')
      INTO already;
    IF already THEN
        RAISE NOTICE 'already applied — skipping';
        RETURN;
    END IF;

    SELECT count(*) INTO n_before FROM trades
     WHERE filed_at IS NOT NULL AND filing_date < '2026-01-01'
       AND (EXTRACT(HOUR FROM filed_at::timestamp) >= 20
            OR EXTRACT(HOUR FROM filed_at::timestamp) <= 3);

    -- Convert the UTC era to Eastern. AT TIME ZONE 'UTC' reads the naive
    -- string as UTC; AT TIME ZONE 'America/New_York' renders it as naive
    -- Eastern, applying the correct DST offset for that instant.
    UPDATE trades
       SET filed_at = to_char(
             ((filed_at::timestamp AT TIME ZONE 'UTC') AT TIME ZONE 'America/New_York'),
             'YYYY-MM-DD HH24:MI:SS')
     WHERE filed_at IS NOT NULL
       AND filing_date < '2026-01-01';

    SELECT count(*) INTO n_after FROM trades
     WHERE filed_at IS NOT NULL AND filing_date < '2026-01-01'
       AND (EXTRACT(HOUR FROM filed_at::timestamp) >= 20
            OR EXTRACT(HOUR FROM filed_at::timestamp) <= 3);

    RAISE NOTICE 'pre-2026 rows in UTC-shaped hours: % -> %', n_before, n_after;

    INSERT INTO schema_migrations (version, applied_by, checksum)
    VALUES ('2026-08-19_001_filed_at_eastern', current_user,
            'filed_at-utc-to-eastern-pre-2026');
END $$;

COMMIT;

COMMENT ON COLUMN trades.filed_at IS
    'EDGAR acceptance timestamp, EASTERN time, naive text YYYY-MM-DD HH24:MI:SS. Normalised 2026-08-19 — pre-2026 rows were UTC. Do NOT apply a timezone conversion when reading.';
