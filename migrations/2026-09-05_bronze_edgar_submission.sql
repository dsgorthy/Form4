\set ON_ERROR_STOP on

-- BRONZE: the bytes SEC published, kept forever.
--
-- WHY THIS EXISTS
--
-- A Form 4 transaction's identity is its position in its own filing. That
-- position exists only in the XML, and we have never stored a single Form 4
-- document -- 96.5% of `trades` was loaded from quarterly TSV exports that
-- discard document order. An audit on 2026-09-04 proved the consequence: an
-- attempt to reconstruct line numbers from insert order mismatched real
-- document order on 13 of 13 filings tested against live EDGAR.
--
-- No SQL recovers information that was never stored. So it gets fetched once,
-- and then it is ours.
--
-- WHAT A ROW IS
--
-- One accession, one complete submission text file, byte for byte, with a
-- checksum. This is the file at
--     /Archives/edgar/data/{cik}/{accession_nodash}/{accession}.txt
-- which is a single request and contains the whole <ownershipDocument>.
-- Verified 2026-09-05: 6.6 KB for a 2026 filing, 25-45 KB for 2006.
--
-- WHY POSTGRES AND NOT THE FILESYSTEM
--
-- ~38 GB raw. Postgres TOAST compresses `text` automatically, and a table
-- inherits the nightly verified pg_dump that already rsyncs off-box to the
-- Mini (scripts/backup_databases.sh). A parallel filesystem tree would need
-- its own backup, its own verification and its own restore story -- three new
-- things that can silently not happen. The whole point of this layer is that
-- it cannot be lost.
--
-- IMMUTABLE. Insert only. A filing is amended by SEC issuing a NEW accession,
-- never by rewriting an old one, so there is nothing here to update.
SET lock_timeout = '5s';

CREATE SCHEMA IF NOT EXISTS bronze;

CREATE TABLE IF NOT EXISTS bronze.edgar_submission (
    accession    TEXT PRIMARY KEY,

    -- Which CIK resolved the URL. rptowner_cik works; the accession's own
    -- prefix CIK 404s on roughly half of filings (tested), so it is only a
    -- fallback and worth recording which one won.
    cik_used     TEXT,
    source_url   TEXT,

    http_status  INTEGER NOT NULL,
    byte_len     INTEGER,
    sha256       TEXT,

    -- The document. NULL only when http_status <> 200.
    content      TEXT,

    fetched_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    attempts     INTEGER NOT NULL DEFAULT 1,
    last_error   TEXT,

    -- A stored row must either carry its bytes or explain why it does not.
    -- Without this a fetch that half-failed looks identical to one that
    -- succeeded and returned nothing.
    CONSTRAINT bronze_content_matches_status CHECK (
        (http_status = 200 AND content IS NOT NULL AND sha256 IS NOT NULL)
        OR (http_status <> 200 AND content IS NULL)
    )
);

-- The only two read patterns: "what is left to fetch" and "what failed".
CREATE INDEX IF NOT EXISTS idx_bronze_status
    ON bronze.edgar_submission (http_status);

COMMENT ON TABLE bronze.edgar_submission IS
    'BRONZE tier. The complete SEC submission text for one accession, byte '
    'for byte, checksummed. Immutable and insert-only. Silver and Gold are '
    'pure functions of this table -- any parser, classification or feature '
    'change is fixed by re-deriving from here, with no network access. This '
    'is the layer that makes a refetch unnecessary forever.';
