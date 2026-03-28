#!/usr/bin/env python3
"""
Deduplicate congress_trades table.

The original UNIQUE(politician_id, ticker, trade_type, trade_date, value_low) constraint
allows unlimited duplicates when value_low IS NULL, because NULL != NULL in SQL.

Fix: recreate table with a generated column that COALESCEs NULL to -1 for uniqueness.

Usage:
    python3 strategies/insider_catalog/congress_dedup.py [--dry-run]
"""

import argparse
import logging
import sqlite3
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-8s %(message)s", datefmt="%H:%M:%S")
logger = logging.getLogger(__name__)

DB_PATH = Path(__file__).resolve().parent / "insiders.db"


def count_duplicates(conn: sqlite3.Connection) -> int:
    """Count duplicate rows that would be removed."""
    return conn.execute("""
        SELECT COUNT(*) FROM (
            SELECT congress_trade_id,
                   ROW_NUMBER() OVER (
                       PARTITION BY politician_id, ticker, trade_type, trade_date, COALESCE(value_low, -1)
                       ORDER BY congress_trade_id ASC
                   ) AS rn
            FROM congress_trades
        ) WHERE rn > 1
    """).fetchone()[0]


def deduplicate(conn: sqlite3.Connection, dry_run: bool = False) -> dict:
    """Deduplicate congress_trades. Returns stats dict."""
    stats = {}

    # Count before
    total_before = conn.execute("SELECT COUNT(*) FROM congress_trades").fetchone()[0]
    dup_count = count_duplicates(conn)
    stats["total_before"] = total_before
    stats["duplicates"] = dup_count

    logger.info("Congress trades before: %d", total_before)
    logger.info("Duplicates to remove: %d", dup_count)

    if dup_count == 0:
        logger.info("No duplicates found. Nothing to do.")
        stats["total_after"] = total_before
        return stats

    if dry_run:
        logger.info("DRY RUN — no changes made")
        stats["total_after"] = total_before - dup_count
        return stats

    # Step 1: Create new table with COALESCE-safe uniqueness
    logger.info("Recreating congress_trades with COALESCE-safe unique constraint...")

    conn.executescript("""
        -- Create new table
        CREATE TABLE IF NOT EXISTS congress_trades_new (
            congress_trade_id INTEGER PRIMARY KEY AUTOINCREMENT,
            politician_id INTEGER NOT NULL REFERENCES politicians(politician_id),
            ticker TEXT NOT NULL,
            company TEXT,
            asset_type TEXT NOT NULL DEFAULT 'stock',
            trade_type TEXT NOT NULL,
            trade_date TEXT NOT NULL,
            trade_date_start TEXT,
            trade_date_end TEXT,
            value_low INTEGER,
            value_high INTEGER,
            value_estimate INTEGER,
            filing_date TEXT,
            filing_delay_days INTEGER,
            owner TEXT,
            report_url TEXT,
            source TEXT NOT NULL DEFAULT 'senate_watcher',
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            -- Generated column for COALESCE-safe uniqueness
            value_low_dedup INTEGER GENERATED ALWAYS AS (COALESCE(value_low, -1)) STORED,
            UNIQUE(politician_id, ticker, trade_type, trade_date, value_low_dedup)
        );
    """)

    # Step 2: Insert deduplicated rows (keep earliest congress_trade_id per group)
    conn.execute("""
        INSERT OR IGNORE INTO congress_trades_new
            (politician_id, ticker, company, asset_type, trade_type,
             trade_date, trade_date_start, trade_date_end,
             value_low, value_high, value_estimate,
             filing_date, filing_delay_days, owner, report_url, source, created_at)
        SELECT politician_id, ticker, company, asset_type, trade_type,
               trade_date, trade_date_start, trade_date_end,
               value_low, value_high, value_estimate,
               filing_date, filing_delay_days, owner, report_url, source, created_at
        FROM congress_trades
        ORDER BY congress_trade_id ASC
    """)

    new_count = conn.execute("SELECT COUNT(*) FROM congress_trades_new").fetchone()[0]
    logger.info("New table has %d rows (removed %d duplicates)", new_count, total_before - new_count)

    # Step 3: Swap tables
    conn.execute("DROP TABLE congress_trades")
    conn.execute("ALTER TABLE congress_trades_new RENAME TO congress_trades")

    # Step 4: Recreate indexes
    conn.execute("CREATE INDEX IF NOT EXISTS idx_congress_trades_ticker ON congress_trades(ticker)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_congress_trades_politician ON congress_trades(politician_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_congress_trades_date ON congress_trades(trade_date DESC)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_congress_trades_filing ON congress_trades(filing_date DESC)")

    conn.commit()

    total_after = conn.execute("SELECT COUNT(*) FROM congress_trades").fetchone()[0]
    stats["total_after"] = total_after

    logger.info("Deduplication complete: %d → %d trades", total_before, total_after)

    # Verify no remaining duplicates
    remaining_dupes = conn.execute("""
        SELECT politician_id, ticker, trade_date, COUNT(*) as n
        FROM congress_trades
        GROUP BY politician_id, ticker, trade_type, trade_date, COALESCE(value_low, -1)
        HAVING n > 1
    """).fetchall()

    if remaining_dupes:
        logger.warning("WARNING: %d duplicate groups remain!", len(remaining_dupes))
    else:
        logger.info("Verified: zero duplicate groups remain")

    return stats


def main():
    parser = argparse.ArgumentParser(description="Deduplicate congress_trades")
    parser.add_argument("--dry-run", action="store_true", help="Count duplicates without removing")
    parser.add_argument("--db-path", type=str, default=str(DB_PATH), help="Path to insiders.db")
    args = parser.parse_args()

    conn = sqlite3.connect(args.db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")

    stats = deduplicate(conn, dry_run=args.dry_run)

    print(f"\n{'='*50}")
    print(f"CONGRESS DEDUP RESULTS")
    print(f"{'='*50}")
    print(f"Before:     {stats['total_before']:,}")
    print(f"Duplicates: {stats['duplicates']:,}")
    print(f"After:      {stats['total_after']:,}")
    print(f"{'='*50}")

    conn.close()


if __name__ == "__main__":
    main()
