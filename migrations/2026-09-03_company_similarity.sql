\set ON_ERROR_STOP on

-- Related companies, 2026-09-03.
--
-- Company pages are the second-most-crawled surface (1,482 Googlebot requests
-- in 7 days against 508 for insider pages) and had ZERO company-to-company
-- links: 20 outbound links to insiders, none to a peer. No topical navigation
-- for a reader, and no sector signal for a crawler.
--
-- Precomputed rather than live because the live query does not bound. Shared
-- insiders on AAPL runs in 50ms and on BRK.A in 8ms, but on OPK -- 13,761
-- discretionary rows -- it takes 6.9 SECONDS. A self-join over an insider set
-- that large is not something to put behind a page render.
SET lock_timeout = '5s';

CREATE TABLE IF NOT EXISTS company_similarity (
    ticker           TEXT     NOT NULL,
    related_ticker   TEXT     NOT NULL,
    rank             SMALLINT NOT NULL,
    score            REAL     NOT NULL,

    -- Which relation put this row here. Same reasoning as insider_similarity:
    -- the two are not equally strong and the card has to say which it is.
    --   shared_insiders  people file on both. Concrete and countable.
    --   sector_peer      same sector, ranked by recent insider buying.
    reason           TEXT     NOT NULL,
    shared_insiders  INTEGER  NOT NULL DEFAULT 0,
    same_sector      BOOLEAN  NOT NULL DEFAULT FALSE,
    recent_buys      INTEGER  NOT NULL DEFAULT 0,

    computed_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (ticker, related_ticker)
);

CREATE INDEX IF NOT EXISTS idx_company_similarity_lookup
    ON company_similarity (ticker, rank);

COMMENT ON TABLE company_similarity IS
    'Top-K related companies per ticker. Navigation and topical linking only '
    '-- NOT a quality, correlation or trading signal. Rebuilt weekly by '
    'scripts/insider_similarity.py via the Dagster insider_similarity asset.';
