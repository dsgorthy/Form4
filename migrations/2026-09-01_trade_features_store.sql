-- \set ON_ERROR_STOP on
--
-- Without this, psql -f runs every statement, prints the errors, and EXITS 0.
\set ON_ERROR_STOP on

-- Blocking DDL queues at the head of the lock queue and stalls every later
-- read on the table (form4.app outage, 2026-08-27). Abort instead of queueing.
SET lock_timeout = '3s';

-- Long-format feature store.
--
-- `trades` carries 102 columns. Every feature added there is a migration, a
-- lock on the table the API reads, and its own coverage question -- and the
-- 2026-08-28 migration was refused four times by an unrelated Dagster run
-- before it applied. Generating features in bulk does not survive that.
--
-- Long, not wide: adding a feature is an INSERT and removing a leaky one is a
-- DELETE. Nothing is ever ALTERed. 318k trades x ~30 features is ~10M rows,
-- which Postgres does not notice. Readers that want a matrix pivot on the way
-- out; nothing needs a matrix in the database.
CREATE TABLE IF NOT EXISTS trade_features (
    trade_id     BIGINT           NOT NULL,
    feature      TEXT             NOT NULL,
    value        DOUBLE PRECISION,
    computed_at  TIMESTAMPTZ      NOT NULL DEFAULT NOW(),
    PRIMARY KEY (trade_id, feature)
);

-- Reading one feature across the corpus is the common access pattern
-- (screening, percentile generation, clustering). Include value so the
-- index answers it without touching the heap.
CREATE INDEX IF NOT EXISTS idx_trade_features_feature
    ON trade_features (feature) INCLUDE (trade_id, value);

-- Provenance for each build, so a result can be traced to the code that made
-- it. A feature whose definition changed silently is indistinguishable from
-- one that did not, which is how a leak survives a rebuild.
CREATE TABLE IF NOT EXISTS trade_feature_runs (
    run_id       BIGSERIAL PRIMARY KEY,
    started_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at  TIMESTAMPTZ,
    git_sha      TEXT,
    n_features   INTEGER,
    n_rows       BIGINT,
    since_date   TEXT,
    notes        TEXT
);
