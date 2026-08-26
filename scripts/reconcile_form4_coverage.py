#!/usr/bin/env python3
"""Do we hold every Form 4 EDGAR published? Ask EDGAR, not ourselves.

WHY THIS EXISTS

Between April and August 2026 the fetcher discarded roughly one filing in
eight and recorded every one of them as processed, so nothing in the system
disagreed with itself: the job was green, the heartbeat was fresh, the row
counts looked normal, and `processed_filings` said the work was done. It was
found because a person noticed Zillow had no 2020 filings.

Every check we had asked our own database whether it was happy. This one asks
the publisher. It is the only kind of check that could have caught it.

  EDGAR daily index      https://www.sec.gov/Archives/edgar/daily-index/{Y}/QTR{q}/form.{YYYYMMDD}.idx
  EDGAR quarterly index  https://www.sec.gov/Archives/edgar/full-index/{Y}/QTR{q}/form.idx

Both are complete and uncapped. Do NOT use EDGAR full-text search (EFTS) as
ground truth: it silently caps at 10,000 hits and reports the cap as the
total, which is how the historical backfill came to hold 48.6% of the record
while believing it was finished.

A Form 4 appears once per FILER in the index — issuer and each reporting
owner — so count DISTINCT accessions, never rows. The inflation is ~2.07x.

Usage:
  python3 scripts/reconcile_form4_coverage.py --days 7
  python3 scripts/reconcile_form4_coverage.py --date 2026-08-14 --list-missing
  python3 scripts/reconcile_form4_coverage.py --days 30 --min-coverage 99.0
  python3 scripts/reconcile_form4_coverage.py --days 30 --requeue
"""
from __future__ import annotations

import argparse
import re
import sys
import time
import urllib.error
import urllib.request
from datetime import date, datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from config.database import get_connection  # noqa: E402

USER_AGENT = "Form4 reconciliation (derek.gorthy@gmail.com)"
ACC_RE = re.compile(r"(\d{10}-\d{2}-\d{6})")
# EDGAR asks for <=10 req/s. We make one request per day checked.
REQUEST_DELAY = 0.15


def edgar_accessions(day: date) -> set[str] | None:
    """Every Form 4 accession EDGAR published on `day`, or None if no index.

    Weekends and holidays have no daily index; that is a None, not an empty
    set, so the caller can tell "nothing was published" from "we looked and
    found nothing".
    """
    q = (day.month - 1) // 3 + 1
    url = (f"https://www.sec.gov/Archives/edgar/daily-index/{day.year}/QTR{q}/"
           f"form.{day:%Y%m%d}.idx")
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            body = resp.read().decode("latin-1")
    except urllib.error.HTTPError as exc:
        if exc.code in (403, 404):
            return None
        raise
    finally:
        time.sleep(REQUEST_DELAY)

    out = set()
    for line in body.splitlines():
        # The form-type column is left-aligned and padded; "4" and "4/A" are
        # different documents and only plain 4s are in scope here.
        if line.startswith("4 "):
            m = ACC_RE.search(line)
            if m:
                out.add(m.group(1))
    return out


def held_accessions(conn, day: date) -> tuple[set[str], set[str]]:
    """(accessions with rows in trades, accessions recorded as read)."""
    d = day.isoformat()
    rows = conn.execute(
        "SELECT DISTINCT accession FROM trades "
        "WHERE filing_date = ? AND accession IS NOT NULL", (d,)).fetchall()
    in_trades = {r[0] for r in rows}
    try:
        rows = conn.execute(
            "SELECT accession FROM processed_filings "
            "WHERE filing_date = ? AND status IN ('ok', 'empty')", (d,)).fetchall()
        read_ok = {r[0] for r in rows}
    except Exception:
        read_ok = set()
    return in_trades, read_ok


def requeue(conn, days_missing) -> int:
    """Hand missing filings back to the fetcher by marking them `failed`.

    The fetcher skips anything already in processed_filings, and every filing
    lost between April and August 2026 has a row there — written by the bug,
    with trade_count 0 and a NULL status. So fixing the fetcher does not on
    its own recover them: they look processed.

    This does not fetch anything. It sets status='failed', attempts=0 and
    records the cik the retry needs, which puts the filing back in
    get_retryable() and lets the normal 5-minute run drain it. Deliberately
    the same path a fresh failure takes, so recovery and steady state cannot
    drift.

    Only touches accessions with NO rows in `trades`. A filing we read and
    found genuinely empty is left alone.
    """
    import urllib.request as _u
    queued = 0
    for day, missing in days_missing:
        # The daily index carries the CIK; processed_filings may not.
        q = (day.month - 1) // 3 + 1
        url = (f"https://www.sec.gov/Archives/edgar/daily-index/{day.year}/QTR{q}/"
               f"form.{day:%Y%m%d}.idx")
        req = _u.Request(url, headers={"User-Agent": USER_AGENT})
        try:
            with _u.urlopen(req, timeout=60) as resp:
                body = resp.read().decode("latin-1")
        except Exception:
            continue
        time.sleep(REQUEST_DELAY)
        cik_of = {}
        for line in body.splitlines():
            if line.startswith("4 ") or line.startswith("4/A "):
                parts = line.split()
                m = ACC_RE.search(parts[-1]) if parts else None
                if m and m.group(1) not in cik_of and parts[-3].isdigit():
                    cik_of[m.group(1)] = (parts[-3], " ".join(parts[1:-3]).strip())
        for acc in sorted(missing):
            cik, company = cik_of.get(acc, (None, None))
            if not cik:
                continue
            conn.execute(
                """INSERT INTO processed_filings
                       (accession, filing_date, trade_count, status, attempts,
                        last_error, last_attempt_at, cik, company)
                   VALUES (?, ?, 0, 'failed', 0, 'requeued by reconciliation',
                           NULL, ?, ?)
                   ON CONFLICT (accession) DO UPDATE SET
                       status     = 'failed',
                       attempts   = 0,
                       last_error = 'requeued by reconciliation',
                       cik        = excluded.cik,
                       company    = COALESCE(excluded.company, processed_filings.company)""",
                (acc, day.isoformat(), cik, company),
            )
            queued += 1
        conn.commit()
    return queued


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=7,
                    help="how many days back from today (or --date) to check")
    ap.add_argument("--date", help="end date, YYYY-MM-DD (default: yesterday)")
    ap.add_argument("--min-coverage", type=float, default=99.0,
                    help="exit non-zero below this %% of EDGAR's filings")
    ap.add_argument("--list-missing", action="store_true")
    ap.add_argument("--requeue", action="store_true",
                    help="hand every missing filing back to the fetcher's retry queue")
    args = ap.parse_args()

    end = (datetime.strptime(args.date, "%Y-%m-%d").date() if args.date
           else date.today() - timedelta(days=1))
    conn = get_connection()

    tot_edgar = tot_held = 0
    worst = []
    print(f"{'date':12s} {'EDGAR':>7s} {'held':>7s} {'read':>7s} {'coverage':>9s}")
    for i in range(args.days):
        day = end - timedelta(days=i)
        pub = edgar_accessions(day)
        if pub is None:
            continue                      # weekend / holiday: nothing published
        in_trades, read_ok = held_accessions(conn, day)
        # A filing we read and found genuinely empty is covered, not missing.
        covered = pub & (in_trades | read_ok)
        missing = pub - (in_trades | read_ok)
        tot_edgar += len(pub)
        tot_held += len(covered)
        pct = len(covered) / len(pub) * 100 if pub else 100.0
        flag = "" if pct >= args.min_coverage else "  <-- BELOW THRESHOLD"
        print(f"{day.isoformat():12s} {len(pub):>7,} {len(in_trades):>7,} "
              f"{len(read_ok):>7,} {pct:>8.1f}%{flag}")
        if missing:
            worst.append((day, missing))

    if not tot_edgar:
        print("\nNo EDGAR index found for any day in range (weekend/holiday?)")
        return 0

    overall = tot_held / tot_edgar * 100
    print(f"\n{'TOTAL':12s} {tot_edgar:>7,} {tot_held:>7,} {'':>7s} {overall:>8.1f}%")
    total_missing = sum(len(m) for _, m in worst)
    if total_missing:
        print(f"\n{total_missing:,} filing(s) published by EDGAR are absent here.")
        if args.list_missing:
            for day, missing in worst:
                for acc in sorted(missing):
                    print(f"   {day.isoformat()}  {acc}")
        else:
            print("   re-run with --list-missing to enumerate them")

    if args.requeue and worst:
        queued = requeue(conn, worst)
        print(f"\nQueued {queued:,} filing(s) for the fetcher to re-drive.")
        print("  insider-fetch drains RETRY_SWEEP_LIMIT per 5-minute run.")

    if overall < args.min_coverage:
        print(f"\nFAIL: {overall:.1f}% < {args.min_coverage}% required")
        return 1
    print(f"\nOK: {overall:.1f}% coverage")
    return 0


if __name__ == "__main__":
    sys.exit(main())
