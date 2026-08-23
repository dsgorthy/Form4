-- Drop trades.career_grade_grouped.
--
-- It existed to hold filing-grouped career grades while nothing published read
-- them, so the tranche A/B could run against real data without restating
-- anything. career_grade now holds those same values — verified identical on
-- all 196,008 rows in the shadow's window, and 0 differences overall — so the
-- column is a duplicate of the thing it was measuring.
--
-- Removing it on principle rather than for space: a second copy of a published
-- grade is exactly the drift this day's audit was about. The next reader
-- should find one career_grade, not two columns that agree today and might not
-- tomorrow.
--
-- Recoverable by re-running the scorer (~28 min for 400,460 keys) or from the
-- nightly pg_dump.

DROP INDEX IF EXISTS idx_trades_cgg;
ALTER TABLE trades DROP COLUMN IF EXISTS career_grade_grouped;
