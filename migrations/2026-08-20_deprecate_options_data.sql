-- Deprecate the options dataset. 7.4 GB, 29% of the database, read 196 times.
--
-- WHY NOW
--
-- ThetaData was cancelled 2026-06-07 and the pull has been dormant since
-- 2026-04-09. Options performance was removed from the product on 2026-08-13 —
-- api/routers/filings.py still carries the comment recording it. Nothing in
-- api/, frontend/ or any scheduled job reads these tables; the only frontend
-- matches for "option" are <option> elements.
--
--   prices.option_prices       23,507,271 rows   7,324 MB   196 index scans
--   prices.option_pull_status     314,026 rows      41 MB     0 index scans
--
-- THE DATA IS NOT BEING LOST
--
-- It is unre-fetchable now the subscription is gone, so it is not being
-- discarded — it is being moved from a live table to cold storage it already
-- occupies. scripts/backup_databases.sh takes a compressed custom-format
-- pg_dump of form4 nightly at 03:15 PT. Verified against
-- form4_20260820_031504.dump that BOTH tables are present as TABLE DATA, not
-- schema-only. Four daily dumps exist on Studio and three are rsynced off-box
-- to the Mini.
--
-- To bring either back:
--
--   pg_restore -d form4 -n prices -t option_prices \
--     /Users/derekg/backups/postgres/form4_20260820_031504.dump
--
-- Note the dumps rotate on RETENTION_DAYS (default 7). If this data is wanted
-- indefinitely, copy one dump somewhere outside the rotation.
--
-- Applied via: psql -d form4 -f migrations/2026-08-20_deprecate_options_data.sql

BEGIN;

DO $$
DECLARE n_p bigint; n_s bigint;
BEGIN
    SELECT count(*) INTO n_p FROM prices.option_prices;
    SELECT count(*) INTO n_s FROM prices.option_pull_status;
    RAISE NOTICE 'dropping option_prices (% rows) and option_pull_status (% rows)', n_p, n_s;
END $$;

DROP TABLE IF EXISTS prices.option_prices;
DROP TABLE IF EXISTS prices.option_pull_status;

DO $$
DECLARE sz text;
BEGIN
    SELECT pg_size_pretty(pg_database_size(current_database())) INTO sz;
    RAISE NOTICE 'form4 is now %', sz;
END $$;

COMMIT;
