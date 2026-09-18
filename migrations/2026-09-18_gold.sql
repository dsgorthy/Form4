\set ON_ERROR_STOP on
-- GOLD v0: Silver, made product-shaped. Read-only over Silver and trades;
-- touches nothing the product reads today.
--
-- Silver (silver.form4_transaction) is every Form 4/5 line as filed, one
-- row per line per reporting owner, with a price judgement in a separate
-- column. The product reads `trades`, whose rows are a lossy, hand-patched
-- projection of the same filings (the 48.6% ingestion loss of 2026-08-26,
-- price_validator's overwrites, the 48,000 orphans repaired 2026-09-18).
-- Gold is the bridge: Silver's facts plus the four things `trades` adds that
-- the product depends on --
--
--   insider_id       the identity the product keys on. From the CIK ->
--                    insider_id map trades already carries (87% of Silver's
--                    filers after the orphan repair; the rest are people who
--                    never reached trades at all).
--   is_joint_copy    a filing with several reporting owners repeats each
--                    line once per owner; trades marks the copies
--                    is_duplicate. One line per (accession, line_no) counts.
--   superseded_by    a 4/A for the same issuer, owner and period replaces the
--                    original. trades marks 1,906 rows; Silver holds ~250k
--                    amendment lines, so expect the parity report to move here.
--   signal_class     the same SQL function trades' trigger uses, fed
--                    Silver's aff_10b5_1 (migrations/2026-09-18_silver_10b51.sql,
--                    trades' own rule: the checkbox or "10b5" in remarks or
--                    footnotes). NULL until the backfill has run a quarter.
--
-- Materialized, because it is 11.6M rows with two window/join passes and the
-- parity script queries it per ticker. Refresh: REFRESH MATERIALIZED VIEW
-- gold.form4_line (minutes). Applied by hand on Studio:
--   psql -d form4 -f migrations/2026-09-18_gold.sql
SET lock_timeout = '5s';

CREATE SCHEMA IF NOT EXISTS gold;

-- The CIK -> insider_id map the product already uses, taken from trades.
-- 4,171 CIKs map to more than one insider_id (name-order variants); the
-- most frequent wins, which is the one the product's pages are built on.
DROP TABLE IF EXISTS gold.insider_by_cik;
CREATE TABLE gold.insider_by_cik AS
SELECT rptowner_cik AS cik, insider_id
  FROM (SELECT rptowner_cik, insider_id,
               row_number() OVER (PARTITION BY rptowner_cik ORDER BY count(*) DESC, insider_id) AS rn
          FROM trades
         WHERE rptowner_cik IS NOT NULL
         GROUP BY rptowner_cik, insider_id) x
 WHERE rn = 1;
ALTER TABLE gold.insider_by_cik ADD PRIMARY KEY (cik);
COMMENT ON TABLE gold.insider_by_cik IS 'CIK -> insider_id as trades has it; the majority insider_id per CIK. Rebuilt with the view.';

DROP MATERIALIZED VIEW IF EXISTS gold.form4_line;
CREATE MATERIALIZED VIEW gold.form4_line AS
WITH hdr AS (
    SELECT accession,
           min(issuer_cik)       AS issuer_cik,
           min(document_type)    AS document_type,
           min(period_of_report) AS period_of_report,
           min(filed_at)         AS filed_at,
           min(rptowner_cik)     AS first_owner
      FROM silver.form4_transaction
     GROUP BY accession
),
amend AS (
    -- The earliest amendment that supersedes each original: same issuer,
    -- same first reporting owner, same period, form type + '/A', filed later.
    SELECT o.accession AS original, min(a.accession) AS amendment
      FROM hdr o
      JOIN hdr a ON a.issuer_cik = o.issuer_cik
                AND a.period_of_report = o.period_of_report
                AND a.first_owner = o.first_owner
                AND a.document_type = o.document_type || '/A'
                AND a.filed_at > o.filed_at
     WHERE o.document_type IN ('4', '5')
     GROUP BY o.accession
)
SELECT s.accession, s.rptowner_cik, s.line_no, s.is_derivative,
       s.issuer_cik, s.ticker, s.period_of_report, s.filed_at, s.document_type, s.is_amendment,
       s.rptowner_name, s.rptowner_title,
       s.security_title, s.trans_date, s.trans_code, s.trans_acquired_disp,
       s.shares, s.price_per_share, s.value, s.shares_owned_after, s.direct_indirect,
       s.price_quality, s.aff_10b5_1, s.bronze_sha256, s.parser_version,
       m.insider_id,
       (row_number() OVER (PARTITION BY s.accession, s.line_no ORDER BY s.rptowner_cik)) > 1 AS is_joint_copy,
       am.amendment AS superseded_by,
       form4_signal_class(s.trans_code,
                          (CASE WHEN s.aff_10b5_1 THEN 1 ELSE 0 END)::bigint,
                          s.trans_acquired_disp,
                          (CASE WHEN s.is_derivative THEN 1 ELSE 0 END)::bigint) AS signal_class
  FROM silver.form4_transaction s
  LEFT JOIN gold.insider_by_cik m ON m.cik = s.rptowner_cik
  LEFT JOIN amend am ON am.original = s.accession;

CREATE INDEX gold_form4_line_ticker_date ON gold.form4_line (ticker, trans_date);
CREATE INDEX gold_form4_line_insider     ON gold.form4_line (insider_id);
CREATE INDEX gold_form4_line_accession   ON gold.form4_line (accession);
COMMENT ON MATERIALIZED VIEW gold.form4_line IS
  'Silver lines plus insider_id, is_joint_copy, superseded_by and signal_class (10b5-1 unknown: gap). Read-only bridge for the parity report before any cutover.';
ANALYZE gold.form4_line;
