# Silver build plan — from Bronze bytes to a faithful, judged table

Written 2026-09-11. Design only; nothing here has been applied to Studio.
Model it implements: `docs/silver_layer_model.md`. Every fact below was read
from the code or measured read-only on Studio the same day.

## What exists, verified

**Bronze.** `bronze.edgar_submission(accession, cik_used, source_url,
http_status, byte_len, sha256, content, fetched_at, attempts, last_error)`,
plus `bronze.edgar_index` (the corpus, 4,137,834 accessions) and
`edgar_index_progress`. 2,549,052 documents stored at 16:31 PT (61.6%),
heaviest in 2014–2022 (~190–203k each); 9 accessions are HTTP 404 at SEC.
The fetcher (`scripts/fetch_bronze.py`, PID 53889) holds
`pg_try_advisory_lock(hashtext('bronze_fetch'))` — session-level, on that key
only. **A reader never contends with it**: advisory locks do not block
SELECT, and a builder that never calls `pg_try_advisory_lock('bronze_fetch')`
never touches it. Reading Bronze while the fetch runs is safe; it costs IO.

**`content` is the SGML submission, not bare XML.** A row begins
`<SEC-DOCUMENT>0001209191-13-023830.txt : 20130503 <SEC-HEADER>…` and the
ownership XML sits inside `<TEXT><XML>…<ownershipDocument>…</ownershipDocument>
…</XML></TEXT>`. Silver must unwrap before it parses.

**The unwrap already exists**: `pipelines/insider_study/download_edgar_data.py`
:195–215 takes a `<TEXT>` section, regex-extracts `<ownershipDocument…
</ownershipDocument>`, and falls back to stripping the inner `<?xml?>`
declaration that `ET.fromstring` rejects. Lift it into a shared helper rather
than copy it.

**The parser already exists**: `strategies/insider_catalog/backfill_live.py`
`parse_form4_xml(...)` at :501 (non-derivative purchases, the OpenInsider row
shape) and `parse_form4_xml_full(...)` at :846 (everything the live ingest
writes). Silver's builder calls `parse_form4_xml_full`. It must NOT call
`price_validator.apply_correction` on the way in — that is the overwrite the
model doc exists to stop.

**`price_quality` already exists on `trades`** (migration
`2026-09-06_price_quality.sql`, 228 rows, the migration's own UPDATE is the
only writer). Its criterion — per-share price ≥100× the ticker's monthly
price band — is the one Silver reuses. Silver does not replace that column on
`trades`; it is where the judgement lives for rows derived from Bronze, and a
later cutover (out of scope here) would make `trades` a view or a projection.

**Nothing reads Bronze content today** except the fetcher and
`verify_bronze.py`. Silver is the first consumer.

## The table

One row per transaction line, per reporting owner — that is the grain of a
Form 4 and of `trades`. Names follow `trades` so parity is a join, not a map.

```sql
-- migrations/2026-09-12_silver.sql  (apply with psql -f on Studio: DDL, Derek's call)
CREATE SCHEMA IF NOT EXISTS silver;

CREATE TABLE IF NOT EXISTS silver.form4_transaction (
  accession            text        NOT NULL,   -- FK-by-convention to bronze.edgar_submission
  rptowner_cik         text        NOT NULL,
  line_no              smallint    NOT NULL,   -- position within the filing's transaction tables
  is_derivative        boolean     NOT NULL,
  -- filing-level, repeated per row so the table stands alone
  issuer_cik           text,
  ticker               text,
  period_of_report     date,
  filed_at             timestamptz,            -- ACCEPTANCE-DATETIME from the SEC-HEADER, Eastern as filed
  document_type        text,                   -- 4 / 4/A / 5 / 5/A
  is_amendment         boolean     NOT NULL DEFAULT false,
  rptowner_name        text,
  rptowner_relationship text,
  -- the transaction, EXACTLY AS FILED
  security_title       text,
  trans_date           date,
  deemed_execution_date date,
  trans_code           text,
  trans_form_type      text,
  equity_swap          boolean,
  trans_acquired_disp  text,                   -- A / D
  shares               numeric(20,4),          -- transactionShares, never adjusted
  price_per_share      numeric(20,6),          -- transactionPricePerShare, never adjusted
  value                numeric(24,4) GENERATED ALWAYS AS (shares * price_per_share) STORED,
  shares_owned_after   numeric(20,4),
  direct_indirect      text,                   -- D / I
  nature_of_ownership  text,
  is_10b5_1            boolean,
  footnote_ids         text[],
  -- the judgement, SEPARATE from the facts
  price_quality        text CHECK (price_quality IN ('ok','outside_band','implausible','no_reference')),
  price_quality_note   text,
  -- provenance
  bronze_sha256        text        NOT NULL,   -- which bytes this row was parsed from
  parsed_at            timestamptz NOT NULL DEFAULT now(),
  parser_version       text        NOT NULL,
  PRIMARY KEY (accession, rptowner_cik, line_no)
);
CREATE INDEX IF NOT EXISTS silver_form4_transaction_ticker_date
  ON silver.form4_transaction (ticker, trans_date);
CREATE INDEX IF NOT EXISTS silver_form4_transaction_implausible
  ON silver.form4_transaction (ticker) WHERE price_quality = 'implausible';

-- accessions parsed, including ones that yielded zero rows (holdings-only
-- filings are legal), so the work list is "bronze rows not here", not
-- "bronze rows with no transaction".
CREATE TABLE IF NOT EXISTS silver.form4_parse (
  accession      text PRIMARY KEY,
  bronze_sha256  text NOT NULL,
  rows_written   integer NOT NULL,
  status         text NOT NULL CHECK (status IN ('ok','no_document','parse_error')),
  error          text,
  parsed_at      timestamptz NOT NULL DEFAULT now(),
  parser_version text NOT NULL
);
```

`value` is GENERATED because a filing carries no total; storing one invites
the two to drift (model doc). `bronze_sha256` on every row is what makes a
re-parse honest: if Bronze's bytes for an accession ever change (they should
not — an amendment is a new accession), the row says which bytes it came from.

## The builder — `scripts/build_silver.py`

Work list is DERIVED, so it self-heals exactly as `ops_bronze_topup` does:

```sql
SELECT s.accession, s.sha256, s.content
  FROM bronze.edgar_submission s
 WHERE s.http_status = 200
   AND NOT EXISTS (SELECT 1 FROM silver.form4_parse p WHERE p.accession = s.accession)
 ORDER BY s.fetched_at
 LIMIT %(batch)s
```

Per accession: unwrap → `parse_form4_xml_full` → rows; assess `price_quality`
(below); one transaction per batch: INSERT rows `ON CONFLICT DO NOTHING`, then
INSERT the `form4_parse` receipt. A parse failure writes a `parse_error`
receipt with the exception text and moves on — it must not stall the batch,
and it must be visible (the receipt is the visibility).

- `--batch 1000`, `--limit N`, `--accession X` (one, for debugging),
  `--dry-run` (parse, print, write nothing).
- Wrapped in `framework.observability.pipeline_run("build_silver")` so it
  shows on `/admin/pipelines` and the watchdog's status check sees it.
- Idempotent by construction: re-running never duplicates (PK + receipt).
- `parser_version` = a constant bumped when parse logic changes; a re-parse
  is `DELETE FROM silver.form4_parse WHERE parser_version < 'X'` and re-run.
- Reads Bronze in `fetched_at` order so the oldest-archived filings land
  first and the parity sample (below) is stable between runs.

**price_quality assessment** reuses the migration's criterion verbatim: for a
row with `price_per_share` and a reference band (the ticker's monthly
min/max from `prices.daily_prices` for `trans_date`'s month), `implausible`
at ≥100× the band's max, `outside_band` between 3× and 100×, `ok` inside, and
`no_reference` when the ticker has no prices for that month. The note carries
the band and the ratio in words. This is a SEPARATE pass, `--assess`, run
after the parse, so a parse without prices is still a complete parse.

## Parity — `scripts/parity_silver_vs_trades.py`

The point of the exercise. For every accession in `silver.form4_parse` with
status `ok`, join `silver.form4_transaction` to `trades` on
`(accession, rptowner_cik, trans_date, trans_code, security_title)` and,
where `trades.line_no` is not NULL, on `line_no` too.

Must match exactly: `trans_code`, `trans_date`, `trans_acquired_disp`,
`security_title`, `shares` = `trades.qty`, `shares_owned_after`,
`direct_indirect`, `is_derivative`, `ticker`, `issuer_cik`.

Expected to differ, and how to read it:

| Silver | trades | reading |
|---|---|---|
| `price_per_share` | `price` where `price_as_filed IS NULL` | must be equal |
| `price_per_share` | `price_as_filed` where NOT NULL | **must be equal** — Silver has the filed number, `trades.price` was overwritten by `price_validator`; this is the check that proves the layer model right |
| rows present | no `trades` row | a filing the live path dropped — the ingestion-loss class; count and list |
| no Silver row | `trades` row exists | a `trades` row Bronze cannot account for — duplicates, pre-2013 sources, hand fixes; count and list |
| `line_no` | NULL | Silver recovers it; report how many |

Output: a markdown report with counts per bucket and 20 examples each, plus
a CSV of every mismatch. The report is the deliverable Derek reads before
any cutover conversation.

## Sequencing that never touches `trades` and never cuts over

1. **Locally, no Studio changes.** Write the migration file, the shared
   unwrap helper, the builder, the assessor, the parity script. Unit tests
   against a fixture of three Bronze documents pulled read-only —
   `0001493152-25-015819` (IHT, the 22,625 filer error), one of the 17 GOOG
   rows `price_validator` "corrected", and one ordinary filing — asserting
   the parse reproduces the filed numbers byte-for-byte and that `value` is
   the product. Test that the builder never imports `price_validator`.
   *~5h. No approval needed.*
2. **Derek applies the DDL** on Studio: `psql -d form4 -f
   migrations/2026-09-12_silver.sql`, with `lock_timeout` set. Creates two
   empty tables in a new schema; touches nothing existing. *5 min, his call.*
3. **Smoke build**: `build_silver.py --limit 10000` on Studio from the
   checkout, while the fetcher runs. Measure rows/s and IO. Then
   `parity_silver_vs_trades.py` on those 10,000. *~1h.*
4. **Full build** of what Bronze holds (2.55M today, 4.14M at completion).
   At a conservative 200 accessions/s that is ~3.5h for today's Bronze; run
   it as a Dagster asset with a schedule so it keeps up with the fetcher and
   with live ingest thereafter (registered in `scheduled_work.yaml`; the
   Dagster restart to register it is Derek's). *~1h to write, hours to run.*
5. **`--assess`** over the built rows once prices are joined. *~1h.*
6. **Parity report** over the full set. Read it. Then, and only then, the
   cutover conversation — which this plan deliberately does not design.

**Needs Derek:** step 2 (DDL on Studio); registering the Dagster asset
(daemon restart); any decision about `trades` afterwards.

**Total, excluding run time:** ~8h of build. Steps 1 is safe for an
unattended session; nothing after it is.

## Unverified

- `parse_form4_xml_full`'s exact return shape (which fields, how derivative
  rows and holdings are represented) — read at :846 but not exercised here.
  The builder's first task is a fixture test that pins it.
- Whether `filed_at` should come from the SEC-HEADER's ACCEPTANCE-DATETIME or
  from `bronze.edgar_index.filed_at`; they should agree, and parity should
  check that they do. The `filed_at`-is-Eastern rule in memory applies.
- Throughput. 200/s is a guess; the smoke build measures it.
