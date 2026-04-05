"""
Backfill filed_at (acceptanceDateTime) from SEC submissions API.

For each unique issuer_cik in trades, fetches the full filing history
from data.sec.gov/submissions and populates filed_at by matching on
accession number. Auto-resumes — skips CIKs where all trades already
have filed_at.

Usage:
    python3 pipelines/insider_study/backfill_filed_at.py [--dry-run] [--limit N]

Rate limit: 10 req/s to SEC EDGAR (we use 8/s with backoff).
Estimated runtime: ~20 min for 8,400 CIKs + pagination.
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from pipelines.insider_study.db_lock import db_write_lock

DB_PATH = Path(__file__).resolve().parents[2] / "strategies" / "insider_catalog" / "insiders.db"
SEC_BASE = "https://data.sec.gov"
USER_AGENT = "Form4App derek@sidequestgroup.com"
REQ_DELAY = 0.125  # 8 req/s


def _fetch_json(url: str) -> dict | None:
    """Fetch JSON from SEC with retry on 429/5xx."""
    for attempt in range(4):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
            resp = urllib.request.urlopen(req, timeout=30)
            return json.loads(resp.read())
        except urllib.error.HTTPError as e:
            if e.code == 429 or e.code >= 500:
                wait = 2 ** attempt
                print(f"  HTTP {e.code}, retrying in {wait}s...", flush=True)
                time.sleep(wait)
                continue
            if e.code == 404:
                return None
            raise
        except (urllib.error.URLError, TimeoutError):
            time.sleep(2 ** attempt)
            continue
    return None


def _parse_acceptance_dt(s: str) -> str:
    """Convert '2026-03-30T17:10:20.000Z' → '2026-03-30 17:10:20'."""
    s = s.replace("Z", "").replace("T", " ")
    if "." in s:
        s = s.split(".")[0]
    return s


def _extract_accession_datetimes(data: dict) -> list[tuple[str, str]]:
    """Extract (accession, datetime) pairs from a submissions response's recent filings."""
    recent = data.get("filings", {}).get("recent", {})
    accessions = recent.get("accessionNumber", [])
    datetimes = recent.get("acceptanceDateTime", [])
    return [
        (acc, _parse_acceptance_dt(dt))
        for acc, dt in zip(accessions, datetimes)
        if acc and dt
    ]


def main():
    parser = argparse.ArgumentParser(description="Backfill filed_at from SEC submissions API")
    parser.add_argument("--dry-run", action="store_true", help="Don't write to DB")
    parser.add_argument("--limit", type=int, default=0, help="Limit CIKs to process (0=all)")
    args = parser.parse_args()

    db = sqlite3.connect(str(DB_PATH))
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA busy_timeout=30000")  # 30s wait for locks
    cur = db.cursor()

    # Only query CIKs that still have trades needing filed_at (auto-resume)
    cur.execute("""
        SELECT DISTINCT issuer_cik
        FROM trades
        WHERE issuer_cik IS NOT NULL
          AND (filed_at IS NULL OR filed_at = '')
          AND accession IS NOT NULL
        ORDER BY issuer_cik
    """)
    ciks = [r[0] for r in cur.fetchall()]
    print(f"CIKs remaining: {len(ciks):,}", flush=True)

    if args.limit > 0:
        ciks = ciks[:args.limit]
        print(f"Limited to first {args.limit}", flush=True)

    total_updated = 0
    total_api_calls = 0
    start_time = time.time()

    for i, cik in enumerate(ciks):
        cik_padded = cik.lstrip("0").zfill(10)
        url = f"{SEC_BASE}/submissions/CIK{cik_padded}.json"
        time.sleep(REQ_DELAY)
        data = _fetch_json(url)
        total_api_calls += 1

        if not data:
            continue

        # Collect all (accession, datetime) pairs from main page
        pairs = _extract_accession_datetimes(data)

        # Follow pagination for older filings
        for f in data.get("filings", {}).get("files", []):
            time.sleep(REQ_DELAY)
            page_data = _fetch_json(f"{SEC_BASE}/submissions/{f['name']}")
            total_api_calls += 1
            if page_data:
                accessions = page_data.get("accessionNumber", [])
                datetimes = page_data.get("acceptanceDateTime", [])
                pairs.extend(
                    (acc, _parse_acceptance_dt(dt))
                    for acc, dt in zip(accessions, datetimes)
                    if acc and dt
                )

        # Batch UPDATE under exclusive write lock
        if pairs and not args.dry_run:
            with db_write_lock():
                cur.executemany(
                    "UPDATE trades SET filed_at = ? WHERE accession = ? AND (filed_at IS NULL OR filed_at = '')",
                    [(dt, acc) for acc, dt in pairs],
                )
                total_updated += cur.rowcount
                db.commit()

        # Free memory
        del pairs

        # Progress every 200 CIKs
        if (i + 1) % 200 == 0 or (i + 1) == len(ciks):
            elapsed = time.time() - start_time
            rate = (i + 1) / elapsed if elapsed > 0 else 0
            eta = (len(ciks) - i - 1) / rate if rate > 0 else 0
            print(
                f"[{i+1}/{len(ciks)}] "
                f"updated={total_updated:,} "
                f"api={total_api_calls:,} "
                f"rate={rate:.1f}/s "
                f"ETA={eta/60:.1f}m",
                flush=True,
            )

    elapsed = time.time() - start_time
    print(f"\nDone in {elapsed/60:.1f} min", flush=True)
    print(f"API calls: {total_api_calls:,}", flush=True)
    print(f"Rows updated: {total_updated:,}", flush=True)

    if not args.dry_run:
        cur.execute("SELECT COUNT(*) FROM trades WHERE filed_at IS NOT NULL AND filed_at != ''")
        has = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM trades")
        total = cur.fetchone()[0]
        print(f"filed_at coverage: {has:,} / {total:,} ({100*has/total:.1f}%)", flush=True)

    db.close()


if __name__ == "__main__":
    main()
