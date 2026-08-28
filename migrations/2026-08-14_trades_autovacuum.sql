-- Tune autovacuum for `trades`, the hottest table in the product.
--
-- Postgres defaults trigger an autovacuum at 20% dead tuples. On a 1.74M-row
-- table that is ~350k dead rows before anything happens, and `trades` takes
-- writes every 5 minutes. Measured 2026-08-14: last autovacuum 2026-08-12,
-- 121k dead tuples outstanding, and the index-only scan behind /api/v1/filings
-- doing 154,191 heap fetches because the visibility map had gone stale. A
-- manual VACUUM ANALYZE took that endpoint from 4.9s to 2.5s.
--
-- 2% of 1.74M is ~35k dead tuples, so the table gets vacuumed roughly daily
-- instead of every few days, and the visibility map stays fresh enough for
-- index-only scans to actually be index-only.
--
-- cost_delay 2ms (default 2ms in PG12+, stated here explicitly) keeps the
-- vacuum from competing with the 5-minute ingest.
--
-- Applied via: psql -d form4 -f migrations/2026-08-14_trades_autovacuum.sql
-- Blocking DDL queues at the head of the lock queue and stalls every later
-- read on the table (form4.app outage, 2026-08-27). Abort instead of queueing.
SET lock_timeout = '3s';


ALTER TABLE trades SET (
    autovacuum_vacuum_scale_factor  = 0.02,
    autovacuum_vacuum_threshold     = 5000,
    autovacuum_analyze_scale_factor = 0.01,
    autovacuum_analyze_threshold    = 5000,
    autovacuum_vacuum_cost_delay    = 2
);

-- Same reasoning, smaller table: trade_returns is rewritten nightly.
ALTER TABLE trade_returns SET (
    autovacuum_vacuum_scale_factor  = 0.05,
    autovacuum_analyze_scale_factor = 0.02
);
