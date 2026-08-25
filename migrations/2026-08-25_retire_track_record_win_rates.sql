-- Retire the insider_track_records win-rate family.
--
-- These columns backed the Buy/Sell Track Record block on the insider page.
-- They were wrong three ways at once:
--
--   1. They count execution LOTS. The header above them counts FILINGS, so a
--      single table row carried two denominators -- Romano Gianluca (27782)
--      rendered "Filings 19" beside an accuracy computed over 154 lots.
--   2. They apply no signal_class filter, so 10b5-1 plan sales, tax
--      withholding and option exercises scored as timing decisions. 81.9% of
--      insiders with a scored sell record contain them; 23.3% are entirely
--      mechanical.
--   3. Nothing has refreshed them since February 2026. The daily writer
--      (pit_scoring.sync_to_track_records, refresh_features_daily.sh step 7)
--      writes score, counts and dates and never these columns; the only
--      writer that does is the legacy SQLite backfill.compute_track_records,
--      which is not in the chain. wr30 coverage by record start: 2026-01
--      92%, 2026-02 28%, 2026-03 2.2%, 2026-07 0.0%.
--
-- The API now computes all three windows on the fly, filing-grouped and
-- discretionary-only, in api/routers/insiders.py. NULLing rather than
-- dropping: `SELECT * FROM insider_track_records` is how the profile route
-- reads this table, and a NULL is the honest value for a number nobody
-- computes.
--
-- The old values are copied to insider_track_records_retired_win_rates first.
-- Do NOT rely on "rerun the backfill to restore": the only writer of these
-- columns is backfill.compute_track_records, which takes a
-- sqlite3.Connection and cannot run against this database at all. Without
-- the copy below this migration would be one-way.
--
-- best_window and recent_win_rate_7d are deliberately NOT nulled: they are
-- separate surfaces (a StatBox and an unread column) and retiring them is a
-- product decision, not part of this fix.

-- 1) Keep what we are about to erase.
CREATE TABLE IF NOT EXISTS insider_track_records_retired_win_rates AS
SELECT insider_id,
       buy_win_rate_7d, buy_win_rate_30d, buy_win_rate_90d,
       buy_avg_return_7d, buy_avg_return_30d, buy_avg_return_90d,
       buy_avg_abnormal_7d, buy_avg_abnormal_30d, buy_avg_abnormal_90d,
       buy_median_return_7d,
       sell_win_rate_7d, sell_win_rate_30d, sell_win_rate_90d,
       sell_avg_return_7d, sell_avg_return_30d, sell_avg_return_90d,
       sell_avg_abnormal_7d, sell_avg_abnormal_30d, sell_avg_abnormal_90d,
       NOW()::text AS retired_at
  FROM insider_track_records;

-- 2) Retire them.
UPDATE insider_track_records SET
    buy_win_rate_7d      = NULL,
    buy_win_rate_30d     = NULL,
    buy_win_rate_90d     = NULL,
    buy_avg_return_7d    = NULL,
    buy_avg_return_30d   = NULL,
    buy_avg_return_90d   = NULL,
    buy_avg_abnormal_7d  = NULL,
    buy_avg_abnormal_30d = NULL,
    buy_avg_abnormal_90d = NULL,
    buy_median_return_7d = NULL,
    sell_win_rate_7d     = NULL,
    sell_win_rate_30d    = NULL,
    sell_win_rate_90d    = NULL,
    sell_avg_return_7d   = NULL,
    sell_avg_return_30d  = NULL,
    sell_avg_return_90d  = NULL,
    sell_avg_abnormal_7d = NULL,
    sell_avg_abnormal_30d= NULL,
    sell_avg_abnormal_90d= NULL
WHERE buy_win_rate_7d      IS NOT NULL OR buy_win_rate_30d    IS NOT NULL
   OR buy_win_rate_90d     IS NOT NULL OR buy_avg_return_7d   IS NOT NULL
   OR buy_avg_return_30d   IS NOT NULL OR buy_avg_return_90d  IS NOT NULL
   OR buy_avg_abnormal_7d  IS NOT NULL OR buy_avg_abnormal_30d IS NOT NULL
   OR buy_avg_abnormal_90d IS NOT NULL OR buy_median_return_7d IS NOT NULL
   OR sell_win_rate_7d     IS NOT NULL OR sell_win_rate_30d   IS NOT NULL
   OR sell_win_rate_90d    IS NOT NULL OR sell_avg_return_7d  IS NOT NULL
   OR sell_avg_return_30d  IS NOT NULL OR sell_avg_return_90d IS NOT NULL
   OR sell_avg_abnormal_7d IS NOT NULL OR sell_avg_abnormal_30d IS NOT NULL
   OR sell_avg_abnormal_90d IS NOT NULL;
