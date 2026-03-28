#!/usr/bin/env python3
"""
Backfill non-P/S transaction codes (A, M, F, G, X, V) into the trades table.

Previously, only P (Purchase) and S (Sale) nonderiv transactions were inserted
into trades. This script reads cached SEC quarterly ZIPs via parse_quarter_full()
and inserts A/M/F/G/X/V nonderiv transactions as new rows.

Trans code → trade_type mapping:
    A (Award/Grant)      → buy   (shares acquired)
    M (Option Exercise)  → buy   (shares acquired via exercise)
    F (Tax Withholding)  → sell  (shares disposed for taxes)
    G (Gift)             → sell  (shares given away / disposed)
    X (RSU Exercise)     → buy   (shares acquired via RSU vesting)
    V (Voluntary Report) → buy   (shares acquired, voluntary disclosure)

Dedup: Uses INSERT OR IGNORE with expanded unique constraint that includes
trans_code to prevent collisions between e.g. A-code grants and P-code buys
with the same value on the same day.

Usage:
    python3 pipelines/insider_study/backfill_all_codes.py
    python3 pipelines/insider_study/backfill_all_codes.py --start 2020-Q1 --end 2025-Q4
    python3 pipelines/insider_study/backfill_all_codes.py --dry-run
"""

from __future__ import annotations

import argparse
import logging
import sqlite3
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from pipelines.insider_study.download_sec_bulk import parse_quarter_full, quarter_range
from strategies.insider_catalog.backfill import (
    DB_PATH,
    get_or_create_insider,
    is_csuite,
    get_title_weight,
    normalize_name,
    normalize_ticker,
    validate_trade_date,
    migrate_schema,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

CACHE_DIR = PROJECT_ROOT / "pipelines" / "insider_study" / "data" / "sec_bulk_cache"

# Trans codes to backfill (everything except P and S, which are already in trades)
BACKFILL_CODES = {"A", "M", "F", "G", "X", "V"}

# Trans code → trade_type mapping
CODE_TO_TRADE_TYPE = {
    "A": "buy",   # Award/Grant — shares acquired
    "M": "buy",   # Option Exercise — shares acquired
    "F": "sell",  # Tax Withholding — shares disposed
    "G": "sell",  # Gift — shares disposed
    "X": "buy",   # RSU Exercise — shares acquired
    "V": "buy",   # Voluntary Report — shares acquired
}


def add_dedup_index(conn: sqlite3.Connection):
    """Add expanded unique index that includes trans_code for dedup."""
    logger.info("Creating expanded dedup index (if not exists)...")
    conn.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_trades_dedup_v2
        ON trades(insider_id, ticker, trade_date, trade_type, value, COALESCE(trans_code, ''))
    """)
    conn.commit()
    logger.info("Dedup index ready")


def backfill_quarter(conn: sqlite3.Connection, zip_path: Path, dry_run: bool = False) -> dict:
    """Parse one quarterly ZIP and insert non-P/S nonderiv transactions into trades."""
    zip_bytes = zip_path.read_bytes()
    data = parse_quarter_full(zip_bytes)

    stats = {"scanned": 0, "inserted": 0, "skipped_dup": 0, "skipped_bad": 0}

    for t in data["nonderiv_trans"]:
        code = t["trans_code"]
        if code not in BACKFILL_CODES:
            continue

        stats["scanned"] += 1

        ticker = normalize_ticker(t["ticker"])
        if not ticker:
            stats["skipped_bad"] += 1
            continue

        trade_date = validate_trade_date(t["trade_date"], t["filing_date"])
        if not trade_date:
            stats["skipped_bad"] += 1
            continue

        price = t["price"]
        shares = t["shares"]
        value = t["value"]

        # For awards/exercises, price or shares may be 0 — that's OK, value matters
        if value <= 0 and price <= 0:
            stats["skipped_bad"] += 1
            continue

        # If value is 0 but we have shares, set value from shares (at $0 price)
        if value <= 0:
            value = abs(shares) * price if price > 0 else 0

        trade_type = CODE_TO_TRADE_TYPE.get(code, "buy")
        insider_name = t["insider_name"]
        if not insider_name:
            stats["skipped_bad"] += 1
            continue

        if dry_run:
            stats["inserted"] += 1
            continue

        insider_id = get_or_create_insider(conn, insider_name)
        title = t["title"]
        csuite = is_csuite(title)
        title_wt = get_title_weight(title)

        try:
            changes_before = conn.total_changes
            conn.execute("""
                INSERT OR IGNORE INTO trades
                    (insider_id, ticker, company, title, trade_type, trade_date,
                     filing_date, price, qty, value, is_csuite, title_weight,
                     source, accession, trans_code, trans_acquired_disp,
                     direct_indirect, shares_owned_after, value_owned_after,
                     nature_of_ownership, equity_swap, is_10b5_1,
                     security_title, deemed_execution_date, trans_form_type,
                     rptowner_cik)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'edgar_bulk', ?, ?, ?,
                        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                insider_id, ticker, t["company"], title, trade_type, trade_date,
                t["filing_date"],
                price if price > 0 else 0,
                int(abs(shares)) if shares != 0 else 0,
                value,
                1 if csuite else 0,
                title_wt,
                t["accession"] or None,
                code,
                t["trans_acquired_disp"] or None,
                t["direct_indirect"] or None,
                t["shares_owned_after"] if t["shares_owned_after"] > 0 else None,
                t["value_owned_after"] if t["value_owned_after"] > 0 else None,
                t["nature_of_ownership"] or None,
                t["equity_swap"],
                t["is_10b5_1"],
                t["security_title"] or None,
                t["deemed_execution_date"] or None,
                t["trans_form_type"] or None,
                t["rptowner_cik"] or None,
            ))
            if conn.total_changes > changes_before:
                stats["inserted"] += 1
            else:
                stats["skipped_dup"] += 1
        except sqlite3.IntegrityError:
            stats["skipped_dup"] += 1

    return stats


def main():
    parser = argparse.ArgumentParser(description="Backfill non-P/S trans codes into trades table")
    parser.add_argument("--start", default="2016-Q1", help="Start quarter (default: 2016-Q1)")
    parser.add_argument("--end", default="2025-Q4", help="End quarter (default: 2025-Q4)")
    parser.add_argument("--db", type=Path, default=DB_PATH, help="Path to insiders.db")
    parser.add_argument("--dry-run", action="store_true", help="Count without inserting")
    args = parser.parse_args()

    conn = sqlite3.connect(str(args.db))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")

    # Ensure schema is up to date
    migrate_schema(conn)

    # Get pre-backfill counts
    pre_count = conn.execute("SELECT COUNT(*) AS c FROM trades").fetchone()["c"]
    pre_by_code = conn.execute("""
        SELECT COALESCE(trans_code, 'NULL') AS code, COUNT(*) AS c
        FROM trades GROUP BY trans_code ORDER BY c DESC
    """).fetchall()
    logger.info("Pre-backfill: %d total trades", pre_count)
    for row in pre_by_code:
        logger.info("  trans_code=%s: %d", row["code"], row["c"])

    # Add expanded dedup index BEFORE inserts
    if not args.dry_run:
        add_dedup_index(conn)

    # Process quarters
    quarters = list(quarter_range(args.start, args.end))
    total_stats = {"scanned": 0, "inserted": 0, "skipped_dup": 0, "skipped_bad": 0}

    for year, quarter in quarters:
        zip_path = CACHE_DIR / f"{year}q{quarter}_form345.zip"
        if not zip_path.exists():
            logger.debug("No cached ZIP for %dQ%d, skipping", year, quarter)
            continue

        stats = backfill_quarter(conn, zip_path, args.dry_run)
        for k in total_stats:
            total_stats[k] += stats[k]

        if not args.dry_run:
            conn.commit()

        logger.info(
            "%dQ%d: scanned=%d inserted=%d dup=%d bad=%d",
            year, quarter, stats["scanned"], stats["inserted"],
            stats["skipped_dup"], stats["skipped_bad"],
        )

    # Post-backfill stats
    if not args.dry_run:
        post_count = conn.execute("SELECT COUNT(*) AS c FROM trades").fetchone()["c"]
        post_by_code = conn.execute("""
            SELECT COALESCE(trans_code, 'NULL') AS code, COUNT(*) AS c
            FROM trades GROUP BY trans_code ORDER BY c DESC
        """).fetchall()
        logger.info("")
        logger.info("=== Backfill Complete ===")
        logger.info("Pre:  %d trades", pre_count)
        logger.info("Post: %d trades (+%d new)", post_count, post_count - pre_count)
        logger.info("")
        logger.info("Post-backfill breakdown by trans_code:")
        for row in post_by_code:
            logger.info("  trans_code=%s: %d", row["code"], row["c"])
    else:
        logger.info("")
        logger.info("=== DRY RUN Complete ===")

    logger.info("")
    logger.info("Totals: scanned=%d inserted=%d dup=%d bad=%d",
                total_stats["scanned"], total_stats["inserted"],
                total_stats["skipped_dup"], total_stats["skipped_bad"])

    conn.close()


if __name__ == "__main__":
    main()
