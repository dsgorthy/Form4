#!/usr/bin/env python3
"""Enumerate every Form 4 SEC ever published, from the quarterly full index.

WHY THIS EXISTS

The first Bronze fetcher took its work list from `trades`. An audit measured
`trades` against this very index and found it holds ~82% of Form 4 filings
(2021Q1: 54,611 of 66,015). An archive built from it inherits an 18% hole, so
"we will never refetch" would have been false the day it finished.

full-index/{Y}/{Q}/form.idx is ONE request per quarter and names every filing
EDGAR published in it. 83 quarters covers 2006 to today. That is the corpus.

RESUMABLE AND RE-READABLE. Completed past quarters are skipped; the CURRENT
quarter is always re-read, because it is still filling.

Usage:
    python3 scripts/fetch_sec_index.py            # crawl all quarters
    python3 scripts/fetch_sec_index.py --status
"""
from __future__ import annotations

import argparse
import datetime as dt
import logging
import re
import sys
import time
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from config.database import get_connection  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

USER_AGENT = "Form4/1.0 dsgorthy@hotmail.com"
DELAY = 0.15
START_YEAR = 2006

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": USER_AGENT, "Accept-Encoding": "gzip, deflate"})


def quarters():
    today = dt.date.today()
    for y in range(START_YEAR, today.year + 1):
        for q in (1, 2, 3, 4):
            if y == today.year and q > (today.month - 1) // 3 + 1:
                break
            yield y, q


#: FORMS 4 AND 5, both with amendments.
#:
#: Form 5 is the annual statement of changes in beneficial ownership -- real
#: insider transaction data, and `trades` already carries 148,348 rows from
#: 96,598 Form 5 accessions (2.07%). A Form-4-only index therefore leaves
#: filings that back our own data unarchived, which the verifier caught on its
#: first run as 56 "orphaned" documents whose &lt;documentType&gt; was 5.
#:
#: Form 3 is deliberately excluded: it is an initial statement of ownership
#: and reports no transactions, so nothing in `trades` derives from one.
INDEX_FORMS = ("4", "4/A", "5", "5/A")


def parse_idx(text: str, quarter: str) -> list:
    """form.idx is fixed-width-ish; the form type is the FIRST column.

    Startswith on the padded column, not a substring search: `type=4` style
    matching pulls in 424B2, 40-F and friends. The daily-index parser learned
    this and the live-queue reader learned it again.
    """
    rows, seen = [], set()
    for line in text.splitlines():
        form = line.split(" ", 1)[0] if " " in line else ""
        if form not in INDEX_FORMS:
            continue
        parts = line.split()
        if len(parts) < 4:
            continue
        path, filed, cik = parts[-1], parts[-2], parts[-3]
        m = re.search(r"(\d{10}-\d{2}-\d{6})", path)
        # THE QUARTERLY INDEX HYPHENATES THE DATE. form.idx per quarter emits
        # "2021-01-05" (10 chars); the DAILY form.YYYYMMDD.idx emits
        # "20210105" (8). The first version of this parser reused the daily
        # parser's `len(filed) != 8` guard and rejected every row in every
        # quarter -- 0 filings from 83 files, reported as success.
        if not m or not cik.isdigit():
            continue
        if len(filed) == 8 and filed.isdigit():
            filed = f"{filed[:4]}-{filed[4:6]}-{filed[6:]}"
        elif not re.fullmatch(r"\d{4}-\d{2}-\d{2}", filed):
            continue
        acc = m.group(1)
        if acc in seen:
            continue
        seen.add(acc)
        rows.append((acc, form, cik,
                     " ".join(parts[1:-3]).strip()[:300], filed, quarter))
    return rows


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--status", action="store_true")
    args = ap.parse_args()

    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SET statement_timeout = '600s'")

    cur.execute("SELECT count(*) AS n FROM bronze.edgar_index")
    logger.info("index holds %d accessions", cur.fetchone()["n"])
    if args.status:
        return 0

    cur.execute("SELECT quarter FROM bronze.edgar_index_progress WHERE complete")
    done = {r["quarter"] for r in cur.fetchall()}
    today = dt.date.today()
    current_q = f"{today.year}QTR{(today.month - 1) // 3 + 1}"

    total_new = 0
    for y, q in quarters():
        qtr = f"{y}QTR{q}"
        if qtr in done and qtr != current_q:
            continue
        url = f"https://www.sec.gov/Archives/edgar/full-index/{y}/QTR{q}/form.idx"
        try:
            r = SESSION.get(url, timeout=120)
            time.sleep(DELAY)
        except requests.RequestException as exc:
            logger.error("%s unreachable: %s", qtr, exc)
            continue
        if r.status_code != 200:
            logger.error("%s returned HTTP %s", qtr, r.status_code)
            continue

        rows = parse_idx(r.text, qtr)
        for i in range(0, len(rows), 5000):
            chunk = rows[i:i + 5000]
            ph = ",".join(["(?,?,?,?,?,?)"] * len(chunk))
            cur.execute(
                f"""INSERT INTO bronze.edgar_index
                      (accession, form_type, cik, company_name, filing_date, quarter)
                    VALUES {ph} ON CONFLICT (accession) DO NOTHING""",
                [v for row in chunk for v in row])
            total_new += cur.rowcount or 0
        cur.execute("""INSERT INTO bronze.edgar_index_progress (quarter, accessions, complete)
                       VALUES (?, ?, ?)
                       ON CONFLICT (quarter) DO UPDATE
                         SET accessions = EXCLUDED.accessions,
                             fetched_at = NOW(), complete = EXCLUDED.complete""",
                    (qtr, len(rows), qtr != current_q))
        conn.commit()
        logger.info("  %s: %d filings in index", qtr, len(rows))

    cur.execute("""SELECT count(*) AS n,
                          min(filing_date)::text AS lo, max(filing_date)::text AS hi
                     FROM bronze.edgar_index""")
    r = cur.fetchone()
    logger.info("DONE: index holds %d accessions (%s .. %s), %d new this run",
                r["n"], r["lo"], r["hi"], total_new)
    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
