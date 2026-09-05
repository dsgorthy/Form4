\set ON_ERROR_STOP on

-- PHASE 1 of the data-layer plan: give a transaction a real identity.
--
-- THE PROBLEM
--
-- Uniqueness on `trades` is enforced by
--
--     UNIQUE (insider_id, ticker, trade_date, trade_type, value)
--
-- where `value` is DOUBLE PRECISION. $3,952,018,935.87 and $3,952,018,936.00
-- are different keys, so the 2026-08-26 reload inserted filings that already
-- existed: 458,314 accessions ingested more than once, 402,118 excess rows.
--
-- Ingest uses INSERT OR IGNORE (-> ON CONFLICT DO NOTHING), so that constraint
-- IS the idempotency guarantee. A key that does not hold means re-ingest
-- duplicates, every time.
--
-- THE KEY THAT SHOULD HAVE BEEN THERE
--
-- A Form 4 transaction's identity is its position in its filing:
-- (accession, line_no). The parser already walks transactions in document
-- order -- ElementTree preserves it -- and discards the index. Capturing it is
-- one enumerate().
--
-- WHY THE INDEX IS PARTIAL, AND WHY THE BACKFILL SKIPS DUPLICATED ACCESSIONS
--
-- Assigning line_no by row_number across an accession that was ingested TWICE
-- numbers a 5-line filing 1..10 and calls it ten distinct lines -- unique, and
-- a lie. Those accessions are left NULL until Phase 2 retires the duplicates,
-- and the index is partial so NULLs are simply out of scope.
--
-- Everything else -- the large majority -- gets its real line_no now, and
-- becomes idempotent on re-ingest immediately.
SET lock_timeout = '5s';

-- Instant in PG11+: a nullable column with no default does not rewrite the
-- table, which matters at 28 GB.
ALTER TABLE trades ADD COLUMN IF NOT EXISTS line_no INTEGER;

COMMENT ON COLUMN trades.line_no IS
    'Zero-based position of this transaction within its Form 4 document. '
    'With accession this is the natural key. NULL means the accession was '
    'ingested more than once and awaits Phase 2 dedup -- never assign a '
    'row_number across copies, it manufactures distinct lines that are not.';
