\set ON_ERROR_STOP on

-- Related insiders, 2026-09-03.
--
-- A precomputed top-K neighbour list per insider. It is a NAVIGATION aid, not
-- a ranking: see scripts/insider_similarity.py for why this table must never
-- be read as a statement about quality.
--
-- This is a brand-new table, so the CREATE takes a lock on nothing and needs
-- no CONCURRENTLY. lock_timeout is still set because the file is run with
-- psql -f against the live database and a later ALTER here would inherit the
-- session; leaving it unset is how 2026-08-27 happened.
SET lock_timeout = '5s';

CREATE TABLE IF NOT EXISTS insider_similarity (
    insider_id          INTEGER  NOT NULL,
    related_insider_id  INTEGER  NOT NULL,
    rank                SMALLINT NOT NULL,

    -- The blend actually used for ordering, in [0,1].
    score               REAL     NOT NULL,

    -- The three components, stored separately so the UI can say WHY two
    -- insiders are related and so a later reader can re-weight without
    -- recomputing. A score with no decomposition is unauditable.
    co_investment       REAL     NOT NULL,  -- Jaccard over ticker sets
    sector_overlap      REAL     NOT NULL,  -- Jaccard over sector sets
    profile_sim         REAL     NOT NULL,  -- behavioural, standardised space

    shared_tickers      INTEGER  NOT NULL DEFAULT 0,
    -- Up to three symbols, comma-separated, purely so the card can render
    -- "Also files on ABT, PG" without a second query.
    shared_ticker_list  TEXT,

    computed_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    PRIMARY KEY (insider_id, related_insider_id)
);

-- The only read pattern: "give me insider X's neighbours in order".
CREATE INDEX IF NOT EXISTS idx_insider_similarity_lookup
    ON insider_similarity (insider_id, rank);

COMMENT ON TABLE insider_similarity IS
    'Top-K similar insiders per insider. Similarity only -- NOT a quality or '
    'performance signal. The behavioural clustering underneath was tested '
    'against forward returns and failed (permutation p=0.208). Rebuilt by '
    'scripts/insider_similarity.py via the Dagster insider_similarity asset.';
