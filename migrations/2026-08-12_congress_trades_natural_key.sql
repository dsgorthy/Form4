-- congress_trades: add a natural-key unique index.
--
-- The table had only its serial PK (congress_trade_id), so nothing stopped a
-- writer from inserting the same disclosure twice. That was survivable while
-- exactly one scraper wrote to it; it stops being survivable now that the
-- dataplane sync (congress.trades.raw.v1 -> form4.congress_trades) re-runs
-- over overlapping windows and must be idempotent.
--
-- Verified clean before adding: 40,493 rows, 40,493 distinct keys, 0 dupes.
--
-- Apply:  psql -d form4 -f migrations/2026-08-12_congress_trades_natural_key.sql

BEGIN;

-- COALESCE the nullable components: NULLs are distinct from each other in a
-- unique index, so without this two rows that differ only by a NULL owner or
-- value_low would both be admitted and the constraint would not bind.
CREATE UNIQUE INDEX IF NOT EXISTS congress_trades_natural_key
    ON public.congress_trades (
        politician_id,
        ticker,
        trade_date,
        filing_date,
        COALESCE(value_low, -1),
        COALESCE(owner, ''),
        COALESCE(trade_type, '')
    );

COMMIT;
