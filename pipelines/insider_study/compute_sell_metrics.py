#!/usr/bin/env python3
"""
Compute sell-side metrics in insider_track_records.

For each insider with sell trades:
  - Join trades (trade_type='sell') with trade_returns
  - For sells, a "win" = stock DROPS after selling (return is negative)
  - Compute: sell_win_rate_7d, sell_avg_return_7d
  - Also compute 30d and 90d if columns exist (adds them if not)

Usage:
  python compute_sell_metrics.py
  python compute_sell_metrics.py --dry-run
"""

from __future__ import annotations

import argparse
import logging
import sqlite3
import statistics
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

CATALOG_DIR = Path(__file__).resolve().parent.parent.parent / "strategies" / "insider_catalog"
DB_PATH = CATALOG_DIR / "insiders.db"


def ensure_columns(conn: sqlite3.Connection):
    """Add sell-side 30d/90d columns if they don't exist."""
    existing = {row[1] for row in conn.execute("PRAGMA table_info(insider_track_records)").fetchall()}

    new_cols = [
        ("sell_win_rate_30d", "REAL"),
        ("sell_avg_return_30d", "REAL"),
        ("sell_avg_abnormal_7d", "REAL"),
        ("sell_avg_abnormal_30d", "REAL"),
        ("sell_win_rate_90d", "REAL"),
        ("sell_avg_return_90d", "REAL"),
        ("sell_avg_abnormal_90d", "REAL"),
    ]

    for col_name, col_type in new_cols:
        if col_name not in existing:
            conn.execute(f"ALTER TABLE insider_track_records ADD COLUMN {col_name} {col_type}")
            logger.info("Added column: %s", col_name)

    conn.commit()


def main():
    parser = argparse.ArgumentParser(description="Compute sell-side metrics")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    if not DB_PATH.exists():
        logger.error("DB not found at %s", DB_PATH)
        return

    conn = sqlite3.connect(str(DB_PATH))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")

    ensure_columns(conn)

    # Get all insiders with sell trades that have return data
    insiders = conn.execute("""
        SELECT DISTINCT t.insider_id
        FROM trades t
        JOIN trade_returns tr ON t.trade_id = tr.trade_id
        WHERE t.trade_type = 'sell'
          AND (tr.return_7d IS NOT NULL OR tr.return_30d IS NOT NULL OR tr.return_90d IS NOT NULL)
    """).fetchall()

    logger.info("Found %d insiders with sell trades having return data", len(insiders))

    updated = 0
    skipped_no_record = 0

    for i, (insider_id,) in enumerate(insiders):
        if i % 5000 == 0 and i > 0:
            logger.info("  Progress: %d/%d (%.0f%%)", i, len(insiders), 100 * i / len(insiders))

        # Check if they have a track record row
        exists = conn.execute(
            "SELECT 1 FROM insider_track_records WHERE insider_id = ?", (insider_id,)
        ).fetchone()
        if not exists:
            skipped_no_record += 1
            continue

        # Pull sell returns
        sell_returns = conn.execute("""
            SELECT tr.return_7d, tr.abnormal_7d,
                   tr.return_30d, tr.abnormal_30d,
                   tr.return_90d, tr.abnormal_90d
            FROM trades t
            JOIN trade_returns tr ON t.trade_id = tr.trade_id
            WHERE t.insider_id = ? AND t.trade_type = 'sell'
        """, (insider_id,)).fetchall()

        if not sell_returns:
            continue

        # 7d: for sells, a "win" means stock dropped (return < 0)
        ret_7d = [r[0] for r in sell_returns if r[0] is not None]
        abn_7d = [r[1] for r in sell_returns if r[1] is not None]
        wr_7d = sum(1 for r in ret_7d if r < 0) / len(ret_7d) if ret_7d else None
        avg_7d = statistics.mean(ret_7d) if ret_7d else None
        avg_abn_7d = statistics.mean(abn_7d) if abn_7d else None

        # 30d
        ret_30d = [r[2] for r in sell_returns if r[2] is not None]
        abn_30d = [r[3] for r in sell_returns if r[3] is not None]
        wr_30d = sum(1 for r in ret_30d if r < 0) / len(ret_30d) if ret_30d else None
        avg_30d = statistics.mean(ret_30d) if ret_30d else None
        avg_abn_30d = statistics.mean(abn_30d) if abn_30d else None

        # 90d
        ret_90d = [r[4] for r in sell_returns if r[4] is not None]
        abn_90d = [r[5] for r in sell_returns if r[5] is not None]
        wr_90d = sum(1 for r in ret_90d if r < 0) / len(ret_90d) if ret_90d else None
        avg_90d = statistics.mean(ret_90d) if ret_90d else None
        avg_abn_90d = statistics.mean(abn_90d) if abn_90d else None

        if not args.dry_run:
            conn.execute("""
                UPDATE insider_track_records
                SET sell_win_rate_7d = ?,
                    sell_avg_return_7d = ?,
                    sell_avg_abnormal_7d = ?,
                    sell_win_rate_30d = ?,
                    sell_avg_return_30d = ?,
                    sell_avg_abnormal_30d = ?,
                    sell_win_rate_90d = ?,
                    sell_avg_return_90d = ?,
                    sell_avg_abnormal_90d = ?,
                    computed_at = datetime('now')
                WHERE insider_id = ?
            """, (
                round(wr_7d, 6) if wr_7d is not None else None,
                round(avg_7d, 6) if avg_7d is not None else None,
                round(avg_abn_7d, 6) if avg_abn_7d is not None else None,
                round(wr_30d, 6) if wr_30d is not None else None,
                round(avg_30d, 6) if avg_30d is not None else None,
                round(avg_abn_30d, 6) if avg_abn_30d is not None else None,
                round(wr_90d, 6) if wr_90d is not None else None,
                round(avg_90d, 6) if avg_90d is not None else None,
                round(avg_abn_90d, 6) if avg_abn_90d is not None else None,
                insider_id,
            ))

        updated += 1

        if updated % 1000 == 0 and not args.dry_run:
            conn.commit()

    if not args.dry_run:
        conn.commit()

    logger.info("=" * 60)
    logger.info("SELL-SIDE METRICS COMPLETE")
    logger.info("=" * 60)
    logger.info("  Insiders updated: %d", updated)
    logger.info("  Skipped (no track record row): %d", skipped_no_record)

    # Summary stats
    for window in ["7d", "30d", "90d"]:
        wr_col = f"sell_win_rate_{window}"
        avg_col = f"sell_avg_return_{window}"

        row = conn.execute(f"""
            SELECT COUNT(*),
                   AVG({wr_col}) * 100,
                   AVG({avg_col}) * 100
            FROM insider_track_records
            WHERE {wr_col} IS NOT NULL
        """).fetchone()

        count = row[0] or 0
        avg_wr = row[1] or 0
        avg_ret = row[2] or 0
        logger.info("  %s: %d insiders | Avg sell win rate: %.1f%% | Avg return after sell: %+.2f%%",
                     window, count, avg_wr, avg_ret)

    # Top 10 best sell-signal insiders (highest sell win rate at 7d, with enough trades)
    top_sellers = conn.execute("""
        SELECT i.name, tr.sell_count, tr.sell_win_rate_7d, tr.sell_avg_return_7d,
               tr.sell_avg_abnormal_7d, tr.primary_title, tr.primary_ticker
        FROM insider_track_records tr
        JOIN insiders i ON tr.insider_id = i.insider_id
        WHERE tr.sell_count >= 5 AND tr.sell_win_rate_7d IS NOT NULL
        ORDER BY tr.sell_win_rate_7d DESC
        LIMIT 10
    """).fetchall()

    logger.info("\nTop 10 sell-signal insiders (7d win rate, 5+ sells):")
    for name, count, wr, avg_ret, avg_abn, title, ticker in top_sellers:
        logger.info("  %s (%s @ %s) — %d sells | WR: %.0f%% | Avg ret: %+.1f%% | Avg abn: %+.1f%%",
                     name, title or "?", ticker or "?", count,
                     (wr or 0) * 100, (avg_ret or 0) * 100, (avg_abn or 0) * 100)

    conn.close()


if __name__ == "__main__":
    main()
