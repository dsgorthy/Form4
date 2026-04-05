"""
Compute PIT-safe cluster sizes for each trade.

For each trade, counts how many OTHER insiders filed trades on the same
ticker+trans_code in the 30 days up to and including the trade's filing_date.

This is the backward-looking version: the first insider in a cluster gets
pit_cluster_size=0. Only the 2nd+ insider sees prior filers.

Usage:
    python3 pipelines/insider_study/compute_pit_clusters.py
"""

from __future__ import annotations

import sqlite3
import sys
import time
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from pipelines.insider_study.db_lock import db_write_lock

DB_PATH = Path(__file__).resolve().parents[2] / "strategies" / "insider_catalog" / "insiders.db"
WINDOW_DAYS = 30
BATCH_SIZE = 10000


def main():
    db = sqlite3.connect(str(DB_PATH))
    db.execute("PRAGMA busy_timeout=30000")
    db.execute("PRAGMA journal_mode=WAL")

    print("Loading trades...", flush=True)
    t0 = time.time()

    # Load all trades grouped by (ticker, trans_code), sorted by filing_date
    rows = db.execute("""
        SELECT trade_id, insider_id, ticker, trans_code, filing_date
        FROM trades
        WHERE trans_code IN ('P', 'S')
          AND filing_date IS NOT NULL
        ORDER BY ticker, trans_code, filing_date, trade_id
    """).fetchall()

    print(f"  {len(rows):,} trades loaded ({time.time()-t0:.1f}s)", flush=True)

    # Group by (ticker, trans_code)
    groups: dict[tuple[str, str], list[tuple[int, int, str]]] = defaultdict(list)
    for trade_id, insider_id, ticker, trans_code, filing_date in rows:
        groups[(ticker, trans_code)].append((trade_id, insider_id, filing_date))

    print(f"  {len(groups):,} ticker+trans_code groups", flush=True)

    # For each group, sliding window to count distinct insiders in prior 30 days
    updates: list[tuple[int, int]] = []  # (cluster_size, trade_id)
    total = 0

    for (ticker, tc), trades in groups.items():
        # trades sorted by filing_date
        # For each trade, count distinct other insiders with filing_date in [fd-30d, fd]
        # Use a deque-like approach: maintain a window of (filing_date, insider_id)
        window: list[tuple[str, int]] = []  # (filing_date, insider_id)

        for trade_id, insider_id, filing_date in trades:
            # Compute cutoff: 30 days before this filing_date
            # Simple string comparison works for YYYY-MM-DD dates
            from datetime import datetime, timedelta
            cutoff = (datetime.strptime(filing_date, "%Y-%m-%d") - timedelta(days=WINDOW_DAYS)).strftime("%Y-%m-%d")

            # Trim window: remove entries older than cutoff
            window = [(fd, iid) for fd, iid in window if fd >= cutoff]

            # Count distinct OTHER insiders in window (filed BEFORE or ON same day but different insider)
            # Only count insiders who filed strictly before this trade's filing_date
            # OR on the same filing_date but with a lower trade_id (filed earlier that day)
            other_insiders = {iid for fd, iid in window if iid != insider_id and fd <= filing_date}
            cluster_size = len(other_insiders)

            updates.append((cluster_size, trade_id))
            total += 1

            # Add this trade to the window AFTER counting (so it doesn't count itself)
            window.append((filing_date, insider_id))

            if len(updates) >= BATCH_SIZE:
                with db_write_lock():
                    db.executemany("UPDATE trades SET pit_cluster_size = ? WHERE trade_id = ?", updates)
                    db.commit()
                updates = []
                if total % 100000 == 0:
                    print(f"  {total:,} trades processed ({time.time()-t0:.1f}s)", flush=True)

    # Final batch
    if updates:
        with db_write_lock():
            db.executemany("UPDATE trades SET pit_cluster_size = ? WHERE trade_id = ?", updates)
            db.commit()

    elapsed = time.time() - t0
    print(f"\nDone: {total:,} trades in {elapsed:.1f}s", flush=True)

    # Distribution
    dist = db.execute("""
        SELECT pit_cluster_size, COUNT(*)
        FROM trades WHERE pit_cluster_size IS NOT NULL
        GROUP BY pit_cluster_size ORDER BY pit_cluster_size
    """).fetchall()
    print("\nCluster size distribution:")
    for size, count in dist[:15]:
        print(f"  {size}: {count:,}")

    # How many qualify as cluster (2+ other insiders)?
    n_cluster = db.execute("SELECT COUNT(*) FROM trades WHERE pit_cluster_size >= 2").fetchone()[0]
    n_buy_cluster = db.execute("SELECT COUNT(*) FROM trades WHERE pit_cluster_size >= 2 AND trans_code = 'P'").fetchone()[0]
    print(f"\nCluster trades (pit_cluster_size >= 2): {n_cluster:,} total, {n_buy_cluster:,} buys")

    db.close()


if __name__ == "__main__":
    main()
