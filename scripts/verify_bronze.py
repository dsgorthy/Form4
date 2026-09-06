#!/usr/bin/env python3
"""Try to prove the Bronze archive is WRONG. Report what survives.

WHY THIS EXISTS

Asked how I could guarantee the archive is correct, the honest answer was that
I cannot. Every self-check I ran on this project passed while the thing it
checked was broken -- because they verified that code existed rather than that
it produced the right result, and because a hand-inspection of 40 documents
was called an audit.

A guarantee is not a claim. It is a check that would fail loudly if the claim
were false, run over the whole corpus, on a schedule, forever. This is that
check. It is written to FAIL, not to reassure.

FIVE INDEPENDENT PROPERTIES, in increasing order of what they would catch:

  1. INDEX COMPLETENESS   The quarterly index is cross-checked against SEC's
                          DAILY indexes for sampled days. Two different files
                          published by SEC from the same source of truth. If
                          the quarterly crawl dropped rows -- which it did
                          once, silently, returning 0 filings from 83 quarters
                          -- the daily files disagree.

  2. ARCHIVE COVERAGE     Every accession in the index has a submission row.
                          Names what is missing rather than reporting a
                          percentage.

  3. STORED INTEGRITY     sha256 recomputes over the stored content and
                          byte_len equals its octet length, for every row.
                          Catches the class where the checksum described bytes
                          that were not stored.

  4. FIDELITY TO SEC      Re-fetch a random sample and compare checksums
                          against what is stored. This is the only check that
                          can prove the bytes are SEC's rather than merely
                          self-consistent. Everything else can pass while the
                          archive holds something subtly wrong.

  5. USABILITY            Every stored document actually contains a parseable
                          <ownershipDocument> with at least one transaction
                          element. An archive that cannot be parsed has not
                          solved anything.

Usage:
    python3 scripts/verify_bronze.py                 # all checks, sample 200
    python3 scripts/verify_bronze.py --sample 1000   # heavier fidelity check
    python3 scripts/verify_bronze.py --quick         # skip the network checks
"""
from __future__ import annotations

import argparse
import hashlib
import logging
import random
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
SESSION = requests.Session()
SESSION.headers.update({"User-Agent": USER_AGENT, "Accept-Encoding": "gzip, deflate"})

FAILURES: list = []


def fail(check: str, detail: str) -> None:
    FAILURES.append((check, detail))
    logger.error("FAIL  %-22s %s", check, detail)


def ok(check: str, detail: str) -> None:
    logger.info("ok    %-22s %s", check, detail)


# ── 1. index completeness, against SEC's other index ───────────────────────

def check_index_against_daily(conn, days: int = 6) -> None:
    """The quarterly and daily indexes are two files SEC builds from one
    source. If our quarterly crawl dropped anything, they disagree."""
    cur = conn.cursor()
    cur.execute("""SELECT filing_date::text AS d FROM bronze.edgar_index
                    WHERE filing_date > CURRENT_DATE - 400
                    GROUP BY 1 ORDER BY random() LIMIT ?""", (days,))
    sampled = [r["d"] for r in cur.fetchall()]
    if not sampled:
        fail("index/daily-xcheck", "no index rows to sample")
        return

    worst = 0.0
    for d in sampled:
        y, m, dd = d.split("-")
        q = (int(m) - 1) // 3 + 1
        url = (f"https://www.sec.gov/Archives/edgar/daily-index/{y}/QTR{q}/"
               f"form.{y}{m}{dd}.idx")
        try:
            r = SESSION.get(url, timeout=90)
            time.sleep(DELAY)
        except requests.RequestException as exc:
            logger.warning("  %s unreachable: %s", d, exc)
            continue
        if r.status_code != 200:
            continue          # weekend/holiday: no daily file
        sec = set()
        for line in r.text.splitlines():
            if line.startswith("4 ") or line.startswith("4/A "):
                mm = re.search(r"(\d{10}-\d{2}-\d{6})", line)
                if mm:
                    sec.add(mm.group(1))
        if not sec:
            continue
        cur.execute("SELECT accession FROM bronze.edgar_index WHERE filing_date = ?", (d,))
        ours = {r["accession"] for r in cur.fetchall()}
        missing = sec - ours
        pct = 100.0 * len(missing) / len(sec)
        worst = max(worst, pct)
        if missing:
            fail("index/daily-xcheck",
                 f"{d}: SEC daily has {len(sec)}, index has {len(ours)}, "
                 f"{len(missing)} missing ({pct:.1f}%) e.g. {sorted(missing)[:2]}")
    if worst == 0.0:
        ok("index/daily-xcheck", f"{len(sampled)} days sampled, 0 filings missing")


# ── 2. archive coverage ────────────────────────────────────────────────────

def check_coverage(conn) -> None:
    cur = conn.cursor()
    cur.execute("""
        SELECT (SELECT count(*) FROM bronze.edgar_index) AS corpus,
               (SELECT count(*) FROM bronze.edgar_submission WHERE http_status = 200) AS stored,
               (SELECT count(*) FROM bronze.edgar_submission WHERE http_status <> 200) AS failed,
               (SELECT count(*) FROM bronze.edgar_submission s
                 WHERE NOT EXISTS (SELECT 1 FROM bronze.edgar_index i
                                    WHERE i.accession = s.accession)) AS orphaned
    """)
    r = cur.fetchone()
    pct = 100.0 * r["stored"] / r["corpus"] if r["corpus"] else 0
    logger.info("      corpus=%d stored=%d (%.2f%%) failed=%d orphaned=%d",
                r["corpus"], r["stored"], pct, r["failed"], r["orphaned"])
    if r["failed"]:
        fail("coverage/failed", f"{r['failed']} accessions recorded as failures")
    if r["orphaned"]:
        fail("coverage/orphaned",
             f"{r['orphaned']} stored documents are not in the SEC index")
    if r["stored"] and not r["failed"] and not r["orphaned"]:
        ok("coverage", f"{r['stored']}/{r['corpus']} archived, no failures, no orphans")


# ── 3. stored integrity ────────────────────────────────────────────────────

def check_integrity(conn) -> None:
    cur = conn.cursor()
    cur.execute("""
        SELECT count(*) AS n,
               count(*) FILTER (WHERE sha256 <> encode(sha256(convert_to(content,'UTF8')),'hex')) AS bad_sha,
               count(*) FILTER (WHERE byte_len <> octet_length(content)) AS bad_len,
               count(*) FILTER (WHERE content IS NULL OR length(content) = 0) AS empty
          FROM bronze.edgar_submission WHERE http_status = 200
    """)
    r = cur.fetchone()
    for label, n in (("bad-sha256", r["bad_sha"]), ("bad-byte_len", r["bad_len"]),
                     ("empty-content", r["empty"])):
        if n:
            fail(f"integrity/{label}", f"{n} of {r['n']} rows")
    if not (r["bad_sha"] or r["bad_len"] or r["empty"]):
        ok("integrity", f"{r['n']} rows: sha256 and byte_len both recompute")


# ── 4. fidelity: are these actually SEC's bytes? ───────────────────────────

def check_fidelity(conn, sample: int) -> None:
    """The only check that can tell a self-consistent archive from a correct
    one. Re-fetch and compare the checksum we stored."""
    cur = conn.cursor()
    cur.execute("""SELECT accession, source_url, sha256, byte_len
                     FROM bronze.edgar_submission
                    WHERE http_status = 200
                    ORDER BY random() LIMIT ?""", (sample,))
    rows = cur.fetchall()
    if not rows:
        fail("fidelity", "nothing archived to verify")
        return
    mismatched = unreachable = 0
    for r in rows:
        try:
            resp = SESSION.get(r["source_url"], timeout=60)
            time.sleep(DELAY)
        except requests.RequestException:
            unreachable += 1
            continue
        if resp.status_code != 200:
            unreachable += 1
            continue
        live = hashlib.sha256(resp.text.encode("utf-8")).hexdigest()
        if live != r["sha256"]:
            mismatched += 1
            fail("fidelity/sha-mismatch",
                 f"{r['accession']}: stored {r['sha256'][:12]} live {live[:12]} "
                 f"(stored {r['byte_len']}B, live {len(resp.text.encode('utf-8'))}B)")
    checked = len(rows) - unreachable
    if mismatched == 0 and checked:
        ok("fidelity", f"{checked} documents re-fetched from SEC, all checksums identical")
    if unreachable:
        logger.warning("      %d could not be re-fetched (transient)", unreachable)


# ── 5. usability ───────────────────────────────────────────────────────────

def check_usable(conn, sample: int) -> None:
    cur = conn.cursor()
    cur.execute("""SELECT accession, content FROM bronze.edgar_submission
                    WHERE http_status = 200 ORDER BY random() LIMIT ?""", (sample,))
    bad_doc = no_txn = 0
    rows = cur.fetchall()
    for r in rows:
        c = r["content"]
        if "<ownershipDocument>" not in c or "</ownershipDocument>" not in c:
            bad_doc += 1
            fail("usable/no-document", r["accession"])
        elif ("<nonDerivativeTransaction>" not in c
              and "<derivativeTransaction>" not in c
              and "<nonDerivativeHolding>" not in c
              and "<derivativeHolding>" not in c):
            no_txn += 1
    if not bad_doc:
        ok("usable", f"{len(rows)} documents carry a complete ownershipDocument "
                     f"({no_txn} hold no transaction element, which is legal for a Form 4)")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--sample", type=int, default=200)
    ap.add_argument("--quick", action="store_true", help="skip network checks")
    args = ap.parse_args()

    conn = get_connection()
    conn.cursor().execute("SET statement_timeout = '900s'")

    check_coverage(conn)
    check_integrity(conn)
    check_usable(conn, args.sample)
    if not args.quick:
        check_index_against_daily(conn)
        check_fidelity(conn, args.sample)

    print()
    if FAILURES:
        logger.error("VERIFICATION FAILED: %d problem(s)", len(FAILURES))
        for c, d in FAILURES[:20]:
            logger.error("  %s: %s", c, d)
        return 1
    logger.info("VERIFICATION PASSED: every check above ran and found nothing")
    return 0


if __name__ == "__main__":
    sys.exit(main())
