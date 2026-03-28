#!/usr/bin/env python3
"""Compute Point-in-Time Cohen routine classification for all trades.

Cohen et al. (2012): An insider's trade in month M of year Y is "routine"
if that insider traded the same ticker in month M for 3+ consecutive years
STRICTLY BEFORE year Y.

This is PIT — at the time of each trade, we only look backward. No future data.

Usage:
    python3 pipelines/insider_study/compute_cohen_pit.py           # full compute
    python3 pipelines/insider_study/compute_cohen_pit.py --since 2025-01-01
    python3 pipelines/insider_study/compute_cohen_pit.py --stats   # just show stats
"""
from __future__ import annotations

import argparse
import logging
import sqlite3
import sys
import time
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

DB_PATH = Path(__file__).resolve().parent.parent.parent / "strategies" / "insider_catalog" / "insiders.db"
N_CONSECUTIVE_YEARS = 3
BATCH_SIZE = 50_000


def compute_cohen_pit(conn: sqlite3.Connection, since: str | None = None) -> int:
    """Compute cohen_routine for all trades, point-in-time.

    Algorithm:
    1. Build lookup: {(insider_id, ticker, month): sorted list of years}
    2. For each trade, check if the insider has 3+ consecutive years
       of trading that ticker in that month STRICTLY BEFORE the trade's year.

    Returns number of trades updated.
    """
    t0 = time.time()

    # Step 1: Build the full history lookup from ALL trades
    # We need the complete history to do PIT lookups for any trade
    logger.info("Building trade history index...")
    rows = conn.execute("""
        SELECT insider_id, ticker,
               CAST(strftime('%m', filing_date) AS INTEGER) AS month,
               CAST(strftime('%Y', filing_date) AS INTEGER) AS year
        FROM trades
        WHERE filing_date IS NOT NULL
          AND insider_id IS NOT NULL
    """).fetchall()

    # {(insider_id, ticker, month): set of years}
    history: dict[tuple, set[int]] = defaultdict(set)
    for r in rows:
        key = (r[0], r[1], r[2])
        history[key].add(r[3])

    logger.info("Index built: %d unique (insider, ticker, month) combos from %d trades (%.1fs)",
                len(history), len(rows), time.time() - t0)

    # Pre-sort years and check for consecutive runs
    # Cache: {(insider_id, ticker, month): {year: bool}} — is it routine at that year?
    routine_cache: dict[tuple, dict[int, bool]] = {}
    for key, year_set in history.items():
        years = sorted(year_set)
        year_routine: dict[int, bool] = {}
        for y in years:
            # PIT: only look at years strictly before y
            prior = [yy for yy in years if yy < y]
            if len(prior) < N_CONSECUTIVE_YEARS:
                year_routine[y] = False
                continue
            # Check for N consecutive years in prior history
            found = False
            for i in range(len(prior) - N_CONSECUTIVE_YEARS + 1):
                if prior[i + N_CONSECUTIVE_YEARS - 1] - prior[i] == N_CONSECUTIVE_YEARS - 1:
                    found = True
                    break
            year_routine[y] = found
        routine_cache[key] = year_routine

    logger.info("Routine cache built (%.1fs)", time.time() - t0)

    # Step 2: Fetch trades to update
    where = ""
    params: list = []
    if since:
        where = "AND filing_date >= ?"
        params = [since]

    trades = conn.execute(f"""
        SELECT trade_id, insider_id, ticker,
               CAST(strftime('%m', filing_date) AS INTEGER) AS month,
               CAST(strftime('%Y', filing_date) AS INTEGER) AS year
        FROM trades
        WHERE filing_date IS NOT NULL
          AND insider_id IS NOT NULL
          {where}
    """, params).fetchall()

    logger.info("Computing cohen_routine for %d trades...", len(trades))

    # Step 3: Batch update
    updates: list[tuple[int, int]] = []  # (cohen_routine, trade_id)
    routine_count = 0
    opportunistic_count = 0

    for r in trades:
        trade_id, insider_id, ticker, month, year = r
        key = (insider_id, ticker, month)
        is_routine = routine_cache.get(key, {}).get(year, False)
        val = 1 if is_routine else 0
        updates.append((val, trade_id))
        if is_routine:
            routine_count += 1
        else:
            opportunistic_count += 1

    # Batch write
    logger.info("Writing %d updates (routine=%d, opportunistic=%d)...",
                len(updates), routine_count, opportunistic_count)

    for i in range(0, len(updates), BATCH_SIZE):
        batch = updates[i:i + BATCH_SIZE]
        conn.executemany(
            "UPDATE trades SET cohen_routine = ? WHERE trade_id = ?",
            batch,
        )
        conn.commit()
        logger.info("  batch %d-%d written", i, min(i + BATCH_SIZE, len(updates)))

    elapsed = time.time() - t0
    logger.info(
        "Done: %d trades updated in %.1fs (%.1f%% routine, %.1f%% opportunistic)",
        len(updates), elapsed,
        routine_count / len(updates) * 100 if updates else 0,
        opportunistic_count / len(updates) * 100 if updates else 0,
    )
    return len(updates)


def show_stats(conn: sqlite3.Connection) -> None:
    """Show current cohen_routine distribution."""
    row = conn.execute("""
        SELECT
            COUNT(*) as total,
            SUM(CASE WHEN cohen_routine IS NOT NULL THEN 1 ELSE 0 END) as computed,
            SUM(CASE WHEN cohen_routine = 1 THEN 1 ELSE 0 END) as routine,
            SUM(CASE WHEN cohen_routine = 0 THEN 1 ELSE 0 END) as opportunistic,
            SUM(CASE WHEN cohen_routine IS NULL THEN 1 ELSE 0 END) as missing
        FROM trades
    """).fetchone()
    print(f"Total trades:    {row[0]:,}")
    print(f"With cohen:      {row[1]:,} ({row[1]/row[0]*100:.1f}%)")
    print(f"  Routine:       {row[2]:,} ({row[2]/row[0]*100:.1f}%)")
    print(f"  Opportunistic: {row[3]:,} ({row[3]/row[0]*100:.1f}%)")
    print(f"  Missing:       {row[4]:,}")

    # Year breakdown
    print("\nBy year:")
    for r in conn.execute("""
        SELECT strftime('%Y', filing_date) as yr,
               COUNT(*) as total,
               SUM(CASE WHEN cohen_routine = 1 THEN 1 ELSE 0 END) as routine,
               SUM(CASE WHEN cohen_routine = 0 THEN 1 ELSE 0 END) as opportunistic
        FROM trades
        WHERE filing_date IS NOT NULL AND cohen_routine IS NOT NULL
        GROUP BY yr
        ORDER BY yr
    """).fetchall():
        pct = r[2] / r[1] * 100 if r[1] else 0
        print(f"  {r[0]}: {r[1]:>7,} trades ({r[2]:>6,} routine = {pct:.1f}%)")


def main() -> None:
    parser = argparse.ArgumentParser(description="Compute PIT Cohen routine classification")
    parser.add_argument("--since", help="Only recompute trades filed on/after this date")
    parser.add_argument("--stats", action="store_true", help="Show current stats and exit")
    args = parser.parse_args()

    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=wal")

    try:
        if args.stats:
            show_stats(conn)
        else:
            compute_cohen_pit(conn, since=args.since)
            print()
            show_stats(conn)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
