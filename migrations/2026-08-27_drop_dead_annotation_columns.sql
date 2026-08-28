-- Drop three annotation columns that no longer mean anything.
--
-- sma20_rel        Removed from compute_cw_indicators in Phase 1, with
--                  tests/unit/test_pit_validation.py::test_sma20_rel_removed
--                  enforcing that it stays removed. Nobody dropped the COLUMN,
--                  so 1.56M stale values sat there reading like a feature.
--                  Zero non-test references anywhere in the codebase.
--
-- recurring_period Written only by compute_cw_indicators and read by nothing.
--                  0.4% populated against is_recurring's 56%, so even where it
--                  exists it is unusable. prog_median_interval_days from
--                  api/programmatic.py is the well-defined replacement: a
--                  number of days rather than a bucket label.
--
-- is_routine       NOTHING has ever written it. compute_trade_grade deducted
--                  5 points for it, which meant the same filing scored
--                  differently depending on when it was ingested -- stale
--                  residue on old rows, NULL on everything from the SEC
--                  reload. Its two jobs already have owners: signal_class /
--                  is_discretionary() answers "was this a decision", and
--                  is_programmatic + prog_median_interval_days answer "is this
--                  insider on a schedule, and how often".
--
-- Reversible: these are re-addable and re-computable, and nothing reads them.

-- LOCK GUARD -- do not remove.
--
-- On 2026-08-27 this migration took form4.app down for ~30 minutes. A
-- 220-minute diagnostic query held AccessShareLock on trades; these ALTERs
-- queued for AccessExclusiveLock behind it; and because Postgres grants lock
-- requests IN ORDER, every API read that arrived afterwards queued behind the
-- ALTER -- even though those reads only needed AccessShare and were compatible
-- with the query actually holding the table. 86 requests piled up, exhausted
-- max_connections, and the API could no longer get a connection to start.
--
-- lock_timeout makes that failure mode impossible: if the lock is not free in
-- 3 seconds this aborts instead of queueing. Re-run it when the table is idle.
SET lock_timeout = '3s';

ALTER TABLE trades DROP COLUMN IF EXISTS sma20_rel;
ALTER TABLE trades DROP COLUMN IF EXISTS recurring_period;
ALTER TABLE trades DROP COLUMN IF EXISTS is_routine;
