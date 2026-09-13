#!/usr/bin/env python3
"""Bronze rows with no receipt -> silver.form4_transaction.

Self-healing by construction, the same shape as ops_bronze_topup: the work
list is "stored submissions that silver.form4_parse has not seen", so newly
fetched filings, filings the previous run did not reach, and filings a future
bug drops are all the same case. Idempotent: the primary key and the receipt
mean a re-run cannot duplicate.

A parse failure is a receipt with status='parse_error' and the exception
text, and the batch continues. It must not stall the batch, and it must be
queryable -- a log line nobody tails is how notification_scanner failed 3,811
times unseen.

Never calls price_validator. Never takes the fetcher's advisory lock
(reads do not contend with it). Never writes to trades.

    python3 pipelines/silver/build.py --limit 10000          # smoke
    python3 pipelines/silver/build.py                        # everything pending
    python3 pipelines/silver/build.py --accession 0001493152-25-015819 --dry-run
"""
from __future__ import annotations

import argparse
import logging
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from config.database import get_connection  # noqa: E402
from framework.observability.pipeline_runner import pipeline_run  # noqa: E402
from pipelines.silver.parse import (  # noqa: E402
    PARSER_VERSION, NoDocument, parse_submission,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("build_silver")

WORK_SQL = """
    SELECT s.accession, s.sha256, s.content
      FROM bronze.edgar_submission s
     WHERE s.http_status = 200
       AND NOT EXISTS (SELECT 1 FROM silver.form4_parse p WHERE p.accession = s.accession)
     ORDER BY s.fetched_at
     LIMIT ?
"""

INSERT_ROW = """
    INSERT INTO silver.form4_transaction (
        accession, rptowner_cik, line_no, is_derivative,
        issuer_cik, ticker, period_of_report, filed_at, document_type, is_amendment,
        rptowner_name, rptowner_is_director, rptowner_is_officer, rptowner_is_10pct,
        rptowner_is_other, rptowner_title,
        security_title, trans_date, deemed_execution_date, trans_form_type, trans_code,
        equity_swap, trans_acquired_disp, shares, price_per_share, shares_owned_after,
        direct_indirect, nature_of_ownership,
        exercise_price, exercise_date, expiration_date, underlying_title, underlying_shares,
        footnote_ids, bronze_sha256, parser_version
    ) VALUES (?,?,?,?, ?,?,?,?,?,?, ?,?,?,?,?,?, ?,?,?,?,?, ?,?,?,?,?, ?,?, ?,?,?,?,?, ?,?,?)
    ON CONFLICT (accession, rptowner_cik, line_no) DO NOTHING
"""

RECEIPT = """
    INSERT INTO silver.form4_parse (accession, bronze_sha256, status, rows_written, error, parser_version)
    VALUES (?,?,?,?,?,?)
    ON CONFLICT (accession) DO NOTHING
"""


def rows_for(accession: str, sha: str, content: str) -> tuple[str, list[tuple], str | None]:
    """(status, rows, error). Pure -- no database."""
    try:
        header, filing = parse_submission(content)
    except NoDocument as exc:
        return "no_document", [], str(exc)
    except Exception as exc:  # ET.ParseError and anything else the XML throws
        return "parse_error", [], f"{type(exc).__name__}: {exc}"[:500]

    doc_type = filing.document_type or header.submission_type
    is_amend = bool(doc_type and doc_type.endswith("/A"))
    por = filing.period_of_report or header.period_of_report
    rows = []
    for o in filing.owners:
        for ln in filing.lines:
            rows.append((
                accession, o.cik, ln.line_no, ln.is_derivative,
                filing.issuer_cik, filing.ticker, por, header.acceptance_datetime, doc_type, is_amend,
                o.name, o.is_director, o.is_officer, o.is_10pct, o.is_other, o.officer_title,
                ln.security_title, ln.trans_date, ln.deemed_execution_date, ln.trans_form_type,
                ln.trans_code, ln.equity_swap, ln.trans_acquired_disp, ln.shares, ln.price_per_share,
                ln.shares_owned_after, ln.direct_indirect, ln.nature_of_ownership,
                ln.exercise_price, ln.exercise_date, ln.expiration_date, ln.underlying_title,
                ln.underlying_shares, ln.footnote_ids or None, sha, PARSER_VERSION,
            ))
    return "ok", rows, None


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=None, help="accessions this run (default: all pending)")
    ap.add_argument("--batch", type=int, default=500)
    ap.add_argument("--accession", help="one accession, for debugging")
    ap.add_argument("--dry-run", action="store_true", help="parse and print, write nothing")
    args = ap.parse_args()

    with pipeline_run("build_silver") as run:
        conn = get_connection()
        t0 = time.monotonic()
        done = written = failed = nodoc = 0
        remaining = args.limit
        while True:
            if args.accession:
                batch = conn.execute(
                    "SELECT accession, sha256, content FROM bronze.edgar_submission "
                    "WHERE accession = ? AND http_status = 200", (args.accession,)).fetchall()
            else:
                n = args.batch if remaining is None else max(0, min(args.batch, remaining))
                if n == 0:
                    break
                batch = conn.execute(WORK_SQL, (n,)).fetchall()
            if not batch:
                break
            for r in batch:
                status, rows, err = rows_for(r["accession"], r["sha256"], r["content"])
                if args.dry_run:
                    print(r["accession"], status, len(rows), err or "")
                    for row in rows:
                        print("  ", row[1], row[2], row[20], row[23], row[24], row[16])
                else:
                    for row in rows:
                        conn.execute(INSERT_ROW, row)
                    conn.execute(RECEIPT, (r["accession"], r["sha256"], status, len(rows), err, PARSER_VERSION))
                done += 1
                written += len(rows)
                failed += status == "parse_error"
                nodoc += status == "no_document"
            if not args.dry_run:
                conn.commit()
            if remaining is not None:
                remaining -= len(batch)
            el = time.monotonic() - t0
            logger.info("%d accessions, %d rows, %d no-document, %d parse errors | %.1f acc/s",
                        done, written, nodoc, failed, done / el if el else 0)
            if args.accession:
                break
        run.set_rows_written(written)
        run.set_metadata({"accessions": done, "parse_errors": failed, "no_document": nodoc,
                          "seconds": round(time.monotonic() - t0, 1), "dry_run": args.dry_run})
    return 0


if __name__ == "__main__":
    sys.exit(main())
