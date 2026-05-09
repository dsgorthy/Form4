"""
Compute PIT-safe cluster sizes for each trade.

For each trade, counts how many OTHER insiders filed trades on the same
ticker+trans_code in the 30 days up to and including the trade's filing_date.

Only counts insiders who filed strictly before this trade (or on the same
filing_date but with a lower trade_id — i.e., earlier within that day).
The first insider in a cluster gets pit_cluster_size=0.

Usage:
    python3 pipelines/insider_study/compute_pit_clusters.py [--since YYYY-MM-DD]

Migrated 2026-05-08 from SQLite (insiders.db) to PostgreSQL (form4) — the
previous SQLite-backed version had been orphaned since the 2026-04-07
SQLite→PG migration, leaving pit_cluster_size NULL on every trade filed
since 2026-04-01. See docs/postmortems/2026-04-07_21d_silent_outage.md for
the sibling failure mode.
"""

from __future__ import annotations

import argparse
import sys
import time
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from config.database import get_connection

WINDOW_DAYS = 30
BATCH_SIZE = 10000


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--since",
        help="Only recompute trades with filing_date >= YYYY-MM-DD. "
             "Omit to recompute all trades.",
    )
    args = parser.parse_args()

    conn = get_connection()
    cur = conn.cursor()

    print("Loading trades...", flush=True)
    t0 = time.time()

    # We need ALL trades for the per-(ticker, trans_code) sliding window
    # context, even if we're only writing back a subset. Filtering applied
    # later when collecting updates.
    cur.execute(
        """
        SELECT trade_id, insider_id, ticker, trans_code, filing_date
        FROM trades
        WHERE trans_code IN ('P', 'S')
          AND filing_date IS NOT NULL
        ORDER BY ticker, trans_code, filing_date, trade_id
        """
    )
    rows = cur.fetchall()

    print(f"  {len(rows):,} trades loaded ({time.time()-t0:.1f}s)", flush=True)

    write_since: datetime | None = None
    if args.since:
        write_since = datetime.strptime(args.since, "%Y-%m-%d")
        print(f"  Will write updates for filing_date >= {args.since}", flush=True)

    # Group by (ticker, trans_code)
    groups: dict[tuple[str, str], list[tuple[int, int, str]]] = defaultdict(list)
    for trade_id, insider_id, ticker, trans_code, filing_date in rows:
        # filing_date may come back as date or string depending on driver
        fd_str = filing_date if isinstance(filing_date, str) else filing_date.strftime("%Y-%m-%d")
        groups[(ticker, trans_code)].append((trade_id, insider_id, fd_str))

    print(f"  {len(groups):,} ticker+trans_code groups", flush=True)

    updates: list[tuple[int, int]] = []  # (cluster_size, trade_id)
    total = 0
    written = 0

    for (ticker, tc), trades in groups.items():
        window: list[tuple[str, int]] = []  # (filing_date_str, insider_id)

        for trade_id, insider_id, filing_date in trades:
            cutoff = (datetime.strptime(filing_date, "%Y-%m-%d") - timedelta(days=WINDOW_DAYS)).strftime("%Y-%m-%d")
            window = [(fd, iid) for fd, iid in window if fd >= cutoff]

            other_insiders = {
                iid for fd, iid in window
                if iid != insider_id and fd <= filing_date
            }
            cluster_size = len(other_insiders)

            total += 1

            should_write = (
                write_since is None
                or datetime.strptime(filing_date, "%Y-%m-%d") >= write_since
            )
            if should_write:
                updates.append((cluster_size, trade_id))
                written += 1

            window.append((filing_date, insider_id))

            if len(updates) >= BATCH_SIZE:
                cur.executemany(
                    "UPDATE trades SET pit_cluster_size = %s WHERE trade_id = %s",
                    updates,
                )
                conn.commit()
                updates = []
                if written % 100000 == 0:
                    print(f"  {written:,} writes / {total:,} processed ({time.time()-t0:.1f}s)", flush=True)

    if updates:
        cur.executemany(
            "UPDATE trades SET pit_cluster_size = %s WHERE trade_id = %s",
            updates,
        )
        conn.commit()

    # Freshness contract for trades.pit_cluster_size
    if written > 0:
        from framework.contracts.freshness_writer import write_freshness
        write_freshness(
            conn,
            table="trades",
            column="pit_cluster_size",
            n_rows_affected=written,
            populated_by="pipelines/insider_study/compute_pit_clusters.py",
        )
        conn.commit()

    elapsed = time.time() - t0
    print(f"\nDone: {total:,} processed, {written:,} written in {elapsed:.1f}s", flush=True)

    cur.execute(
        """
        SELECT pit_cluster_size, COUNT(*)
        FROM trades WHERE pit_cluster_size IS NOT NULL
        GROUP BY pit_cluster_size ORDER BY pit_cluster_size
        """
    )
    dist = cur.fetchall()
    print("\nCluster size distribution:")
    for size, count in dist[:15]:
        print(f"  {size}: {count:,}")

    cur.execute("SELECT COUNT(*) FROM trades WHERE pit_cluster_size >= 2")
    n_cluster = cur.fetchone()[0]
    cur.execute(
        "SELECT COUNT(*) FROM trades WHERE pit_cluster_size >= 2 AND trans_code = 'P'"
    )
    n_buy_cluster = cur.fetchone()[0]
    print(f"\nCluster trades (pit_cluster_size >= 2): {n_cluster:,} total, {n_buy_cluster:,} buys")

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
