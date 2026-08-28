-- trades.ingested_at — a real timestamp for "when did we write this row".
--
-- The 5-minute alert SLA needs an incremental predicate: every tick must ask
-- "what landed since my watermark". Today the only candidates are TEXT:
--
--   filing_date  DATE-valued text, no intraday component at all
--   created_at   TEXT, unindexed, and its format varies with whatever wrote
--                the row (with/without microseconds, with/without offset)
--   filed_at     TEXT, indexed — SEC acceptance, not our ingest
--
-- Comparing TEXT timestamps happens to work while every row carries the same
-- offset, and stops working quietly when one does not. That exact pattern
-- produced a false 7-hour staleness page on 2026-08-14: Postgres renders a
-- whole-hour offset as "-07", Python 3.9's fromisoformat rejects it, the
-- fallback dropped the offset, and Pacific was read as UTC. Building a
-- user-facing SLA on the same footing is not worth the 30 minutes it saves.
--
-- ingested_at is the watermark column: it answers "did we write this row
-- since the last scan", which is what makes the scan lossless. A filing
-- accepted at 10:00 but ingested at 14:00 must still be picked up, so the
-- watermark cannot key off acceptance time. filed_at remains the right clock
-- for MEASURING latency (acceptance -> alert); ingested_at is for finding work.
--
-- Applied via: psql -d form4 -f migrations/2026-08-15_trades_ingested_at.sql

-- Instant in PG11+: a nullable add with no default rewrites nothing.
-- Blocking DDL queues at the head of the lock queue and stalls every later
-- read on the table (form4.app outage, 2026-08-27). Abort instead of queueing.
SET lock_timeout = '3s';

ALTER TABLE trades ADD COLUMN IF NOT EXISTS ingested_at TIMESTAMPTZ;

-- Backfill in batches so the table is never locked for long. created_at is
-- text with a mixed format; ::timestamptz parses every variant we store,
-- including the bare "-07" that broke the Python side.
DO $$
DECLARE
    n INTEGER;
BEGIN
    LOOP
        UPDATE trades
           SET ingested_at = created_at::timestamptz
         WHERE trade_id IN (
             SELECT trade_id FROM trades
              WHERE ingested_at IS NULL AND created_at IS NOT NULL
              LIMIT 50000
         );
        GET DIAGNOSTICS n = ROW_COUNT;
        EXIT WHEN n = 0;
        COMMIT;
    END LOOP;
END $$;

-- New rows get it for free, so no writer has to remember.
ALTER TABLE trades ALTER COLUMN ingested_at SET DEFAULT NOW();

-- The index the 5-minute scan rides on. A tick reads ~10 rows out of 1.74M;
-- without this it is a sequential scan every five minutes, forever.
CREATE INDEX IF NOT EXISTS idx_trades_ingested_at ON trades (ingested_at DESC);

-- Watermarks move from a DATE to a real timestamp. The old column could not
-- express "processed through 10:37 today", which is why user notifications
-- ran a day behind however often the scanner was invoked.
ALTER TABLE notifications.scan_watermarks
    ADD COLUMN IF NOT EXISTS last_processed_at TIMESTAMPTZ;

-- Seed from the date watermark, interpreted as end-of-day in the database's
-- local zone. Erring late would skip filings; end-of-day for an already
-- processed date is the boundary the old scanner actually reached.
UPDATE notifications.scan_watermarks
   SET last_processed_at = (last_processed_date::date + INTERVAL '1 day')
                           AT TIME ZONE 'America/Los_Angeles'
 WHERE last_processed_at IS NULL
   AND last_processed_date IS NOT NULL;
