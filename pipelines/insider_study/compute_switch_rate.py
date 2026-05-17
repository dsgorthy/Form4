#!/usr/bin/env python3
"""Compute insider_switch_rate and is_rare_reversal — PostgreSQL recurring writer.

Replaces the old SQLite implementation that was hard-pinned to
`strategies/insider_catalog/insiders.db`. That file was the only writer for
`is_rare_reversal`, and it stopped running once the data layer moved to PG —
silently silenced `reversal_dip` for ~8 weeks until the 2026-05-16 audit.

Point-in-time: for each trade event, only considers the insider's PRIOR
events. Groups by filing_key to avoid counting lot-splits as separate events.

Window discipline: the count walks the FULL event history per insider
(required for correct switch_rate and is_rare_reversal). Only UPDATEs
trades whose filing_date >= --since.

Usage:
    python3 pipelines/insider_study/compute_switch_rate.py --since 2026-04-01
"""
from __future__ import annotations

import argparse
import logging
import sys
import time
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from config.database import get_connection  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

BATCH_SIZE = 5000
DEFAULT_SINCE = "2016-01-01"
RARE_REVERSAL_STREAK = 5  # 5+ same-direction events, then opposite


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--since", default=DEFAULT_SINCE,
                        help="Only UPDATE trades with filing_date >= this date (YYYY-MM-DD)")
    args = parser.parse_args()

    conn = get_connection()

    logger.info("Loading full trade history (PIT counting needs full prior set)...")
    t0 = time.time()
    rows = conn.execute("""
        SELECT trade_id, insider_id, trade_type, filing_date, filing_key
          FROM trades
         WHERE filing_date >= ?
         ORDER BY insider_id, filing_date, trade_id
    """, (DEFAULT_SINCE,)).fetchall()
    logger.info("Loaded %d trades in %.1fs", len(rows), time.time() - t0)

    by_insider: dict[int, list[tuple]] = defaultdict(list)
    for row in rows:
        by_insider[row[1]].append(row)
    logger.info("Unique insiders: %d", len(by_insider))

    updates: list[tuple] = []  # (switch_rate, is_rare_reversal, trade_id)
    processed = 0
    rare_count = 0
    t0 = time.time()

    for insider_id, trades in by_insider.items():
        # 1) Dedupe by filing_key to derive events_order
        seen: dict[str, tuple[str, str]] = {}
        events_order: list[tuple[str, str, str]] = []
        for trade_id, _, trade_type, filing_date, filing_key in trades:
            key = filing_key if filing_key else f"_nofk_{trade_id}"
            if key not in seen:
                direction = "buy" if trade_type == "buy" else "sell"
                seen[key] = (direction, filing_date)
                events_order.append((key, direction, filing_date))

        key_to_idx = {k: i for i, (k, _, _) in enumerate(events_order)}

        # 2) PIT walk
        for trade_id, _, trade_type, filing_date, filing_key in trades:
            if filing_date < args.since:
                continue  # Out of UPDATE window
            key = filing_key if filing_key else f"_nofk_{trade_id}"
            event_idx = key_to_idx[key]
            current_dir = "buy" if trade_type == "buy" else "sell"
            prior = events_order[:event_idx]

            if not prior:
                updates.append((None, 0, trade_id))
                processed += 1
                continue

            prior_dirs = [d for _, d, _ in prior]
            n = len(prior_dirs)
            if n < 2:
                switch_rate = 0.0
            else:
                switches = sum(
                    1 for i in range(1, n) if prior_dirs[i] != prior_dirs[i - 1]
                )
                switch_rate = switches / (n - 1)

            is_rare = 0
            if n >= RARE_REVERSAL_STREAK:
                last_dir = prior_dirs[-1]
                streak = 0
                for d in reversed(prior_dirs):
                    if d == last_dir:
                        streak += 1
                    else:
                        break
                if streak >= RARE_REVERSAL_STREAK and current_dir != last_dir:
                    is_rare = 1
                    rare_count += 1

            updates.append((switch_rate, is_rare, trade_id))
            processed += 1

        if len(updates) >= BATCH_SIZE * 4:
            _flush(conn, updates)
            updates = []

    _flush(conn, updates)
    logger.info("Computed and wrote %d trades in %.1fs (rare_reversal=%d)",
                processed, time.time() - t0, rare_count)

    try:
        from framework.contracts.freshness_writer import write_freshness
        write_freshness(
            conn, table="trades", column="is_rare_reversal",
            n_rows_affected=processed,
            populated_by="pipelines/insider_study/compute_switch_rate.py",
        )
        write_freshness(
            conn, table="trades", column="insider_switch_rate",
            n_rows_affected=processed,
            populated_by="pipelines/insider_study/compute_switch_rate.py",
        )
        conn.commit()
    except Exception as e:
        logger.warning("freshness write failed: %s", e)

    conn.close()
    return 0


def _flush(conn, updates: list[tuple]) -> None:
    if not updates:
        return
    conn.executemany(
        "UPDATE trades SET insider_switch_rate = ?, is_rare_reversal = ? WHERE trade_id = ?",
        updates,
    )
    conn.commit()


if __name__ == "__main__":
    sys.exit(main())
