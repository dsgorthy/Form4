#!/usr/bin/env python3
"""
Fix corrupt trade_date values in the trades table.

Known issues:
  - 0024-* should be 2024-*
  - 0025-* should be 2025-*
  - 2033-* should be 2023-*

Cross-references filing_date (which is clean) to verify corrections.

Usage:
    python3 strategies/insider_catalog/fix_bad_dates.py [--dry-run]
"""

import argparse
import logging
import sqlite3
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-8s %(message)s", datefmt="%H:%M:%S")
logger = logging.getLogger(__name__)

DB_PATH = Path(__file__).resolve().parent / "insiders.db"

# Date fix rules: (pattern, replacement_prefix)
DATE_FIXES = [
    ("0024-", "2024-"),
    ("0025-", "2025-"),
    ("2033-", "2023-"),
]


def find_bad_dates(conn: sqlite3.Connection) -> list:
    """Find all trades with corrupt dates."""
    bad_trades = []
    for pattern, fix_prefix in DATE_FIXES:
        rows = conn.execute("""
            SELECT trade_id, insider_id, ticker, trade_date, filing_date, value
            FROM trades
            WHERE trade_date LIKE ?
        """, (f"{pattern}%",)).fetchall()
        for row in rows:
            fixed_date = fix_prefix + row[3][len(pattern):]
            bad_trades.append({
                "trade_id": row[0],
                "insider_id": row[1],
                "ticker": row[2],
                "trade_date": row[3],
                "filing_date": row[4],
                "value": row[5],
                "fixed_date": fixed_date,
                "pattern": pattern,
            })
    return bad_trades


def fix_dates(conn: sqlite3.Connection, dry_run: bool = False) -> dict:
    """Fix corrupt dates. Returns stats dict."""
    bad_trades = find_bad_dates(conn)

    if not bad_trades:
        logger.info("No corrupt dates found.")
        return {"total_bad": 0, "fixed": 0, "skipped": 0}

    logger.info("Found %d trades with corrupt dates:", len(bad_trades))

    fixed = 0
    skipped = 0

    for bt in bad_trades:
        # Verify fix by checking filing_date is close to fixed_date
        filing_year = bt["filing_date"][:4] if bt["filing_date"] else None
        fixed_year = bt["fixed_date"][:4]

        # Filing date should be in the same year or year+1 of trade
        if filing_year and abs(int(filing_year) - int(fixed_year)) <= 1:
            logger.info("  FIX: trade_id=%d %s %s → %s (filing: %s)",
                       bt["trade_id"], bt["ticker"], bt["trade_date"],
                       bt["fixed_date"], bt["filing_date"])
            if not dry_run:
                conn.execute(
                    "UPDATE trades SET trade_date = ? WHERE trade_id = ?",
                    (bt["fixed_date"], bt["trade_id"]),
                )
            fixed += 1
        else:
            logger.warning("  SKIP: trade_id=%d %s %s — filing_date %s doesn't match fixed year %s",
                          bt["trade_id"], bt["ticker"], bt["trade_date"],
                          bt["filing_date"], fixed_year)
            skipped += 1

    if not dry_run:
        conn.commit()

    logger.info("Fixed %d dates, skipped %d", fixed, skipped)
    return {"total_bad": len(bad_trades), "fixed": fixed, "skipped": skipped}


def main():
    parser = argparse.ArgumentParser(description="Fix corrupt trade dates")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--db-path", type=str, default=str(DB_PATH))
    args = parser.parse_args()

    conn = sqlite3.connect(args.db_path)
    conn.execute("PRAGMA journal_mode=WAL")

    stats = fix_dates(conn, dry_run=args.dry_run)

    print(f"\n{'='*50}")
    print(f"DATE FIX {'(DRY RUN) ' if args.dry_run else ''}RESULTS")
    print(f"{'='*50}")
    print(f"Bad dates found: {stats['total_bad']}")
    print(f"Fixed:           {stats['fixed']}")
    print(f"Skipped:         {stats['skipped']}")
    print(f"{'='*50}")

    # Verify
    remaining = conn.execute("""
        SELECT COUNT(*) FROM trades
        WHERE trade_date < '2000-01-01' OR trade_date > '2030-12-31'
    """).fetchone()[0]
    print(f"Remaining out-of-range dates: {remaining}")

    conn.close()


if __name__ == "__main__":
    main()
