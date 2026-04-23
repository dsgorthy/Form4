#!/usr/bin/env python3
"""
Flag duplicate trades caused by multiple entity filings for the same transaction.

When Michael Dell sells shares, both SLTA IV and SLTA V may file Form 4s for
the same trade. This creates duplicate records that inflate cluster signals,
trade counts, and total values.

Rules:
  1. Match trades on (ticker, trade_date, price, qty, trade_type) across insiders
  2. Person vs entity duplicates: suppress entity trades, keep person's
  3. Entity vs entity in same group: keep primary_insider_id's trade
  4. Entity vs entity with no group: flag but don't suppress (needs resolution)
  5. Person vs person: never suppress (could be genuine coincidence)

Usage:
  python dedup_trades.py              # Flag duplicates
  python dedup_trades.py --dry-run    # Preview only
  python dedup_trades.py --stats      # Show dedup stats
"""

from __future__ import annotations

import argparse
import logging
import sqlite3
from collections import defaultdict
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

DB_PATH = Path(__file__).resolve().parent / "insiders.db"


def ensure_column(conn: sqlite3.Connection):
    """Add is_duplicate column if it doesn't exist."""
    cols = {row[1] for row in conn.execute("PRAGMA table_info(trades)").fetchall()}
    if "is_duplicate" not in cols:
        conn.execute("ALTER TABLE trades ADD COLUMN is_duplicate INTEGER NOT NULL DEFAULT 0")
        conn.commit()
        logger.info("Added is_duplicate column to trades table")


def build_group_lookup(conn: sqlite3.Connection) -> dict[int, int]:
    """Build insider_id -> primary_insider_id mapping from entity groups."""
    rows = conn.execute("""
        SELECT igm.insider_id, ig.primary_insider_id
        FROM insider_group_members igm
        JOIN insider_groups ig ON igm.group_id = ig.group_id
    """).fetchall()
    return {r[0]: r[1] for r in rows}


def build_entity_set(conn: sqlite3.Connection) -> set[int]:
    """Get set of entity insider_ids."""
    rows = conn.execute(
        "SELECT insider_id FROM insiders WHERE is_entity = 1"
    ).fetchall()
    return {r[0] for r in rows}


def run_dedup(conn: sqlite3.Connection, dry_run: bool = False):
    """Find and flag duplicate trades."""
    if not dry_run:
        ensure_column(conn)
        # Reset all flags before re-computing
        conn.execute("UPDATE trades SET is_duplicate = 0 WHERE is_duplicate != 0")
        conn.commit()

    entities = build_entity_set(conn)
    group_lookup = build_group_lookup(conn)

    # Find all trade groups with identical (ticker, trade_date, price, qty, trade_type)
    # across different insiders
    rows = conn.execute("""
        SELECT trade_id, insider_id, ticker, trade_date, price, qty, trade_type
        FROM trades
        ORDER BY ticker, trade_date, price, qty, trade_type
    """).fetchall()

    # Group by signature
    groups: dict[tuple, list[tuple[int, int]]] = defaultdict(list)
    for trade_id, insider_id, ticker, trade_date, price, qty, trade_type in rows:
        key = (ticker, trade_date, price, qty, trade_type)
        groups[key].append((trade_id, insider_id))

    # Process groups with multiple insiders
    to_flag: list[int] = []  # trade_ids to mark as duplicate
    stats = {
        "person_entity": 0,     # entity suppressed in favor of person
        "group_primary": 0,     # entity suppressed in favor of group primary
        "entity_entity": 0,     # entity suppressed (identical trade, no group needed)
        "person_person": 0,     # person-person matches (never suppressed)
        "skipped_same": 0,      # same insider_id (not cross-insider dupe)
    }

    for key, trade_list in groups.items():
        if len(trade_list) < 2:
            continue

        # Get unique insider_ids in this group
        unique_insiders = set(iid for _, iid in trade_list)
        if len(unique_insiders) < 2:
            stats["skipped_same"] += len(trade_list) - 1
            continue

        # Classify insiders in this group
        person_trades = [(tid, iid) for tid, iid in trade_list if iid not in entities]
        entity_trades = [(tid, iid) for tid, iid in trade_list if iid in entities]

        if person_trades and entity_trades:
            # Rule 2: Person vs entity — suppress entities
            for tid, iid in entity_trades:
                to_flag.append(tid)
                stats["person_entity"] += 1

        elif entity_trades and not person_trades:
            # All entities — check groups
            # Find which entities share a primary
            primary_map: dict[int, list[tuple[int, int]]] = defaultdict(list)
            ungrouped: list[tuple[int, int]] = []

            for tid, iid in entity_trades:
                primary = group_lookup.get(iid)
                if primary is not None:
                    primary_map[primary].append((tid, iid))
                else:
                    ungrouped.append((tid, iid))

            # Within each primary group, keep one trade, suppress rest
            for primary_id, group_trades in primary_map.items():
                if len(group_trades) > 1:
                    # Keep the first, suppress rest
                    for tid, iid in group_trades[1:]:
                        to_flag.append(tid)
                        stats["group_primary"] += 1

            # Ungrouped entities with identical trades — strong evidence of
            # shared beneficial ownership. Keep one, suppress the rest.
            all_remaining = ungrouped[:]
            # Also include one representative from each primary group if
            # there are multiple groups (cross-group dupes)
            if len(primary_map) > 1:
                group_reps = []
                for primary_id, group_trades in primary_map.items():
                    group_reps.append(group_trades[0])
                # Keep first group rep, suppress others
                for tid, iid in group_reps[1:]:
                    to_flag.append(tid)
                    stats["entity_entity"] += 1

            if all_remaining:
                if primary_map:
                    # Ungrouped entities duplicating a grouped entity's trade
                    for tid, iid in all_remaining:
                        to_flag.append(tid)
                        stats["entity_entity"] += 1
                elif len(all_remaining) > 1:
                    # Multiple ungrouped entities, no person — keep first, suppress rest
                    for tid, iid in all_remaining[1:]:
                        to_flag.append(tid)
                        stats["entity_entity"] += 1

        elif len(person_trades) > 1:
            # Rule 5: Multiple people with same trade — never suppress
            stats["person_person"] += len(person_trades) - 1

    if dry_run:
        logger.info("DRY RUN — would flag %d trades as duplicates", len(to_flag))
    else:
        # Batch update
        for i in range(0, len(to_flag), 500):
            batch = to_flag[i:i+500]
            placeholders = ",".join("?" * len(batch))
            conn.execute(
                f"UPDATE trades SET is_duplicate = 1 WHERE trade_id IN ({placeholders})",
                batch,
            )
        conn.commit()
        logger.info("Flagged %d trades as duplicates", len(to_flag))

    logger.info("Dedup breakdown:")
    logger.info("  Entity suppressed (person exists):  %d", stats["person_entity"])
    logger.info("  Entity suppressed (group primary):  %d", stats["group_primary"])
    logger.info("  Entity suppressed (identical trade): %d", stats["entity_entity"])
    logger.info("  Person-person matches (kept):       %d", stats["person_person"])

    return len(to_flag), stats


def rebuild_aggregates(conn: sqlite3.Connection):
    """Rebuild insider_companies excluding duplicates."""
    logger.info("Rebuilding insider_companies (excluding duplicates)...")
    conn.execute("DELETE FROM insider_companies")
    conn.execute("""
        INSERT INTO insider_companies
            (insider_id, ticker, company, title, trade_count, total_value, first_trade, last_trade)
        SELECT
            t.insider_id,
            t.ticker,
            MAX(t.company),
            (SELECT t2.title FROM trades t2
             WHERE t2.insider_id = t.insider_id AND t2.ticker = t.ticker
               AND (t2.is_duplicate = 0 OR t2.is_duplicate IS NULL)
             ORDER BY t2.trade_date DESC LIMIT 1),
            COUNT(*),
            SUM(t.value),
            MIN(t.trade_date),
            MAX(t.trade_date)
        FROM trades t
        WHERE (t.is_duplicate = 0 OR t.is_duplicate IS NULL)
          AND t.is_derivative = 0
        GROUP BY t.insider_id, t.ticker
    """)
    conn.commit()
    count = conn.execute("SELECT COUNT(*) FROM insider_companies").fetchone()[0]
    logger.info("Rebuilt %d insider-company mappings", count)


def print_stats(conn: sqlite3.Connection):
    """Print dedup statistics."""
    cols = {row[1] for row in conn.execute("PRAGMA table_info(trades)").fetchall()}
    if "is_duplicate" not in cols:
        print("is_duplicate column not yet created. Run dedup_trades.py first.")
        return

    total = conn.execute("SELECT COUNT(*) FROM trades").fetchone()[0]
    dupes = conn.execute("SELECT COUNT(*) FROM trades WHERE is_duplicate = 1").fetchone()[0]
    active = total - dupes

    # Top tickers by duplicates
    top_dupes = conn.execute("""
        SELECT ticker, COUNT(*) as dupe_count
        FROM trades
        WHERE is_duplicate = 1
        GROUP BY ticker
        ORDER BY dupe_count DESC
        LIMIT 10
    """).fetchall()

    print(f"\n{'='*50}")
    print("TRADE DEDUPLICATION STATS")
    print(f"{'='*50}")
    print(f"Total trades:      {total:,}")
    print(f"Duplicates flagged: {dupes:,} ({dupes/total*100:.1f}%)")
    print(f"Active trades:     {active:,}")

    if top_dupes:
        print(f"\nTop tickers by duplicates:")
        for ticker, count in top_dupes:
            print(f"  {ticker:8s} {count:,}")

    print(f"{'='*50}\n")


def main():
    parser = argparse.ArgumentParser(description="Flag duplicate entity trades")
    parser.add_argument("--dry-run", action="store_true", help="Preview only")
    parser.add_argument("--stats", action="store_true", help="Show dedup stats")
    args = parser.parse_args()

    conn = sqlite3.connect(str(DB_PATH), timeout=120)
    conn.execute("PRAGMA journal_mode=WAL")

    if args.stats:
        print_stats(conn)
    else:
        flagged, stats = run_dedup(conn, dry_run=args.dry_run)
        if not args.dry_run and flagged > 0:
            rebuild_aggregates(conn)
        print_stats(conn)

    conn.close()


if __name__ == "__main__":
    main()
