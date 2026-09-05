#!/usr/bin/env python3
"""Fetch every Form 4 submission SEC ever published into bronze.edgar_submission.

THE ONE-TIME COST THAT ENDS THE PROBLEM

A transaction's identity is its position in its filing. That position lives
only in the XML, and 96.5% of `trades` came from quarterly TSV exports that
discard it. An audit on 2026-09-04 proved insert order is not document order:
13 of 13 filings tested against live EDGAR mismatched.

So the documents come back once -- ~3.2M filings, ~89 hours at SEC's rate
limit -- and after that every parser fix, classification change or dedup
decision is a local re-derivation. No refetch, ever.

DESIGN NOTES

Resumable by construction: the work list is "accessions in trades with no
bronze row", so a re-run after any interruption picks up exactly where it
stopped. Nothing is held in memory between batches.

RATE LIMITED AS ONE SHARED BUDGET. SEC allows 10 req/s per client, not per
thread. A token bucket shared across the pool enforces that globally; giving
each worker its own sleep would multiply the rate by the worker count and get
the IP blocked.

FAILURES ARE RECORDED, NOT SKIPPED. A non-200 writes a row with the status and
the error. "We could not get this one" then lives in a table that can be
queried and retried, rather than as a silent gap -- which is the failure mode
that let 48.6% of the record go missing once already.

Usage:
    python3 scripts/fetch_bronze.py                  # run to completion
    python3 scripts/fetch_bronze.py --limit 500      # a taste
    python3 scripts/fetch_bronze.py --retry-failed   # re-attempt non-200s
    python3 scripts/fetch_bronze.py --status         # progress only
"""
from __future__ import annotations

import argparse
import hashlib
import logging
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from config.database import get_connection  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

USER_AGENT = "Form4/1.0 dsgorthy@hotmail.com"   # same declared UA as the rest of the pipeline
RATE_PER_SEC = 8.0        # SEC permits 10; leave headroom rather than find the edge
WORKERS = 4
BATCH = 500               # accessions claimed per DB round trip
TIMEOUT = 45


class TokenBucket:
    """One shared budget for the whole process, not one per thread."""

    def __init__(self, rate: float):
        self.rate = rate
        self.allowance = rate
        self.last = time.monotonic()
        self.lock = threading.Lock()

    def take(self) -> None:
        with self.lock:
            now = time.monotonic()
            self.allowance = min(self.rate, self.allowance + (now - self.last) * self.rate)
            self.last = now
            if self.allowance < 1.0:
                wait = (1.0 - self.allowance) / self.rate
                time.sleep(wait)
                self.allowance = 0.0
                self.last = time.monotonic()
            else:
                self.allowance -= 1.0


BUCKET = TokenBucket(RATE_PER_SEC)
SESSION = requests.Session()
SESSION.headers.update({"User-Agent": USER_AGENT, "Accept-Encoding": "gzip, deflate"})


def submission_url(accession: str, cik: str) -> str:
    return (f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/"
            f"{accession.replace('-', '')}/{accession}.txt")


def fetch_one(job: tuple) -> dict:
    """Fetch one submission. Never raises -- a failure is a row, not a crash."""
    accession, ciks = job
    last_err = None
    for cik in ciks:
        if not cik:
            continue
        url = submission_url(accession, cik)
        BUCKET.take()
        try:
            r = SESSION.get(url, timeout=TIMEOUT)
        except requests.RequestException as exc:
            last_err = f"{type(exc).__name__}: {exc}"[:400]
            continue
        if r.status_code == 200:
            body = r.text
            return {"accession": accession, "cik_used": str(int(cik)), "source_url": url,
                    "http_status": 200, "byte_len": len(r.content),
                    "sha256": hashlib.sha256(r.content).hexdigest(),
                    "content": body, "last_error": None}
        last_err = f"HTTP {r.status_code}"
        # 404 on one CIK just means that CIK is not a party; try the next.
    return {"accession": accession, "cik_used": None, "source_url": None,
            "http_status": 0 if last_err and "HTTP" not in last_err else 404,
            "byte_len": None, "sha256": None, "content": None,
            "last_error": last_err or "no candidate cik"}


#: `trades` has no cik column of its own -- the reporting owner's CIK lives in
#: rptowner_cik, and `insiders.cik` is the second candidate. The accession's
#: own prefix is a third, added by the caller; it 404s on roughly half of
#: filings (tested 2026-09-05) so it is strictly a fallback.
TODO_SQL = """
SELECT t.accession,
       MAX(NULLIF(t.rptowner_cik, '')) AS owner_cik,
       MAX(NULLIF(i.cik, ''))          AS insider_cik
  FROM trades t
  LEFT JOIN insiders i ON i.insider_id = t.insider_id
  LEFT JOIN bronze.edgar_submission b ON b.accession = t.accession
 WHERE t.accession IS NOT NULL AND t.accession <> ''
   AND b.accession IS NULL
 GROUP BY t.accession
 LIMIT ?
"""


def progress(conn) -> tuple:
    cur = conn.cursor()
    cur.execute("""
        SELECT (SELECT count(DISTINCT accession) FROM trades
                 WHERE accession IS NOT NULL AND accession <> '') AS total,
               (SELECT count(*) FROM bronze.edgar_submission) AS have,
               (SELECT count(*) FROM bronze.edgar_submission WHERE http_status = 200) AS ok
    """)
    r = cur.fetchone()
    return r["total"], r["have"], r["ok"]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, help="stop after N submissions")
    ap.add_argument("--retry-failed", action="store_true")
    ap.add_argument("--status", action="store_true")
    args = ap.parse_args()

    conn = get_connection()
    cur = conn.cursor()
    total, have, ok = progress(conn)
    logger.info("bronze: %d/%d accessions stored (%d ok, %d failed)",
                have, total, ok, have - ok)
    if args.status:
        return 0

    if args.retry_failed:
        cur.execute("DELETE FROM bronze.edgar_submission WHERE http_status <> 200")
        conn.commit()
        logger.info("cleared %d failed rows for retry", cur.rowcount or 0)

    done = t0 = 0
    t0 = time.monotonic()
    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        while True:
            take = BATCH if not args.limit else min(BATCH, args.limit - done)
            if take <= 0:
                break
            cur.execute(TODO_SQL, (take,))
            rows = cur.fetchall()
            if not rows:
                break
            jobs = [(r["accession"],
                     [r["owner_cik"], r["insider_cik"], r["accession"][:10]])
                    for r in rows]

            results = list(pool.map(fetch_one, jobs))
            for res in results:
                cur.execute("""
                    INSERT INTO bronze.edgar_submission
                      (accession, cik_used, source_url, http_status, byte_len,
                       sha256, content, last_error)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT (accession) DO NOTHING
                """, (res["accession"], res["cik_used"], res["source_url"],
                      res["http_status"], res["byte_len"], res["sha256"],
                      res["content"], res["last_error"]))
            conn.commit()
            done += len(results)
            el = time.monotonic() - t0
            nok = sum(1 for r in results if r["http_status"] == 200)
            logger.info("  +%d (%d ok) | %d this run | %.1f/s | %.1f%% of corpus",
                        len(results), nok, done, done / el if el else 0,
                        100.0 * (have + done) / total)

    total, have, ok = progress(conn)
    logger.info("DONE this run. bronze: %d/%d stored, %d ok, %d failed",
                have, total, ok, have - ok)
    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
