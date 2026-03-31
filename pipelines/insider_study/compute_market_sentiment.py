#!/usr/bin/env python3
"""
Compute daily insider market sentiment indicator based on rolling buy/sell ratios.

Uses point-in-time filing_date (not trade_date) to count 30-day rolling windows
of P-code buys and S-code sells, then assigns a sentiment label by percentile rank.

Creates/populates insider_market_sentiment table in insiders.db.

Usage:
    python3 pipelines/insider_study/compute_market_sentiment.py
    python3 pipelines/insider_study/compute_market_sentiment.py --start 2024-01-01 --end 2026-03-31
"""

from __future__ import annotations

import argparse
import logging
import sqlite3
import time
from datetime import datetime, timedelta
from pathlib import Path

try:
    from pipelines.insider_study.price_utils import load_prices, PRICES_DIR
except ModuleNotFoundError:
    from price_utils import load_prices, PRICES_DIR

DB_PATH = Path(__file__).resolve().parents[2] / "strategies" / "insider_catalog" / "insiders.db"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

BATCH_SIZE = 5_000


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS insider_market_sentiment (
    date TEXT PRIMARY KEY,
    buy_count_30d INTEGER,
    sell_count_30d INTEGER,
    buy_sell_ratio REAL,
    buy_value_30d REAL,
    sell_value_30d REAL,
    value_ratio REAL,
    cluster_count_30d INTEGER,
    percentile REAL,
    sentiment_label TEXT
);
"""


# ---------------------------------------------------------------------------
# Trading day calendar from SPY price data
# ---------------------------------------------------------------------------

def get_trading_days(start: str, end: str) -> list[str]:
    """Return sorted list of trading days from SPY price file in [start, end]."""
    spy_prices = load_prices("SPY")
    if not spy_prices:
        logger.error("No SPY price data found in %s", PRICES_DIR)
        return []
    all_dates = sorted(spy_prices.keys())
    return [d for d in all_dates if start <= d <= end]


# ---------------------------------------------------------------------------
# Core computation
# ---------------------------------------------------------------------------

def compute_sentiment(conn: sqlite3.Connection, start: str, end: str) -> int:
    """Compute sentiment for each trading day in [start, end]."""
    trading_days = get_trading_days(start, end)
    if not trading_days:
        logger.error("No trading days found for range %s to %s", start, end)
        return 0

    logger.info("Computing sentiment for %d trading days: %s to %s",
                len(trading_days), trading_days[0], trading_days[-1])

    # Prefetch all buy/sell trades with filing_date in the relevant window.
    # We need 30 days before the first trading day for the rolling window.
    window_start = (datetime.strptime(trading_days[0], "%Y-%m-%d") - timedelta(days=35)).strftime("%Y-%m-%d")
    window_end = trading_days[-1]

    buy_trades = conn.execute("""
        SELECT filing_date, value
        FROM trades
        WHERE trans_code = 'P'
          AND filing_date BETWEEN ? AND ?
          AND filing_date IS NOT NULL
        ORDER BY filing_date
    """, (window_start, window_end)).fetchall()

    sell_trades = conn.execute("""
        SELECT filing_date, value
        FROM trades
        WHERE trans_code = 'S'
          AND filing_date BETWEEN ? AND ?
          AND filing_date IS NOT NULL
        ORDER BY filing_date
    """, (window_start, window_end)).fetchall()

    # Prefetch cluster counts (top_trade signals) by filing_date
    cluster_trades = conn.execute("""
        SELECT t.filing_date
        FROM trade_signals ts
        JOIN trades t ON ts.trade_id = t.trade_id
        WHERE ts.signal_type = 'top_trade'
          AND t.filing_date BETWEEN ? AND ?
          AND t.filing_date IS NOT NULL
        ORDER BY t.filing_date
    """, (window_start, window_end)).fetchall()

    # Build sorted lists for sliding window
    buy_dates = [(r["filing_date"], r["value"] or 0) for r in buy_trades]
    sell_dates = [(r["filing_date"], r["value"] or 0) for r in sell_trades]
    cluster_dates = [r["filing_date"] for r in cluster_trades]

    # Compute per-day metrics using two-pointer sliding window
    rows = []
    t0 = time.time()

    for day in trading_days:
        day_dt = datetime.strptime(day, "%Y-%m-%d")
        window_lo = (day_dt - timedelta(days=30)).strftime("%Y-%m-%d")

        # Count buys in window
        buy_count = 0
        buy_value = 0.0
        for fd, val in buy_dates:
            if fd < window_lo:
                continue
            if fd > day:
                break
            buy_count += 1
            buy_value += val

        # Count sells in window
        sell_count = 0
        sell_value = 0.0
        for fd, val in sell_dates:
            if fd < window_lo:
                continue
            if fd > day:
                break
            sell_count += 1
            sell_value += val

        # Count clusters in window
        cluster_count = 0
        for fd in cluster_dates:
            if fd < window_lo:
                continue
            if fd > day:
                break
            cluster_count += 1

        # Ratios with div-by-zero protection
        buy_sell_ratio = buy_count / sell_count if sell_count > 0 else (float(buy_count) if buy_count > 0 else 0.0)
        value_ratio = buy_value / sell_value if sell_value > 0 else (buy_value if buy_value > 0 else 0.0)

        rows.append((
            day, buy_count, sell_count, buy_sell_ratio,
            buy_value, sell_value, value_ratio,
            cluster_count,
        ))

    elapsed = time.time() - t0
    logger.info("Computed %d days in %.1fs", len(rows), elapsed)

    # Compute percentile rank for buy_sell_ratio
    ratios = sorted(r[3] for r in rows)
    n = len(ratios)
    ratio_to_percentile = {}
    for i, val in enumerate(ratios):
        ratio_to_percentile[val] = (i / (n - 1)) * 100 if n > 1 else 50.0

    # For ties, we want the highest percentile for that ratio value
    # Re-scan to handle duplicates properly: use rank / total
    ratio_rank = {}
    for i, val in enumerate(ratios):
        ratio_rank[val] = i  # last occurrence wins, giving higher rank to ties

    def percentile_for(ratio: float) -> float:
        if n <= 1:
            return 50.0
        rank = ratio_rank.get(ratio, 0)
        return (rank / (n - 1)) * 100

    def label_for(pct: float) -> str:
        if pct >= 80:
            return "very_bullish"
        if pct >= 60:
            return "bullish"
        if pct >= 40:
            return "neutral"
        if pct >= 20:
            return "bearish"
        return "very_bearish"

    # Insert results
    inserts = []
    for row in rows:
        day, buy_count, sell_count, bsr, buy_val, sell_val, vr, cc = row
        pct = percentile_for(bsr)
        lbl = label_for(pct)
        inserts.append((day, buy_count, sell_count, bsr, buy_val, sell_val, vr, cc, pct, lbl))

    # Batch insert
    for i in range(0, len(inserts), BATCH_SIZE):
        batch = inserts[i:i + BATCH_SIZE]
        conn.executemany("""
            INSERT OR REPLACE INTO insider_market_sentiment
            (date, buy_count_30d, sell_count_30d, buy_sell_ratio,
             buy_value_30d, sell_value_30d, value_ratio,
             cluster_count_30d, percentile, sentiment_label)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, batch)
    conn.commit()

    logger.info("Inserted %d sentiment rows", len(inserts))
    return len(inserts)


# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------

def print_summary(conn: sqlite3.Connection):
    """Print summary statistics."""
    total = conn.execute("SELECT COUNT(*) FROM insider_market_sentiment").fetchone()[0]
    print(f"\n=== Market Sentiment Summary ({total:,} days) ===\n")

    # Label distribution
    label_rows = conn.execute("""
        SELECT sentiment_label, COUNT(*) AS cnt,
               AVG(buy_sell_ratio) AS avg_bsr,
               AVG(buy_count_30d) AS avg_buys,
               AVG(sell_count_30d) AS avg_sells
        FROM insider_market_sentiment
        GROUP BY sentiment_label
        ORDER BY avg_bsr DESC
    """).fetchall()

    print(f"{'Label':<15} {'Count':>7} {'Avg BSR':>10} {'Avg Buys':>10} {'Avg Sells':>10}")
    print("-" * 55)
    for r in label_rows:
        print(f"{r['sentiment_label']:<15} {r['cnt']:>7,} {r['avg_bsr']:>10.4f} "
              f"{r['avg_buys']:>10.1f} {r['avg_sells']:>10.1f}")

    # Most recent 5 days
    recent = conn.execute("""
        SELECT date, buy_sell_ratio, sentiment_label, buy_count_30d, sell_count_30d
        FROM insider_market_sentiment
        ORDER BY date DESC LIMIT 5
    """).fetchall()
    print(f"\nMost recent:")
    for r in recent:
        print(f"  {r['date']}  BSR={r['buy_sell_ratio']:.4f}  "
              f"{r['sentiment_label']:<14} buys={r['buy_count_30d']} sells={r['sell_count_30d']}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Compute daily insider market sentiment indicator")
    parser.add_argument("--start", default="2016-01-01", help="Start date (default: 2016-01-01)")
    parser.add_argument("--end", default=datetime.now().strftime("%Y-%m-%d"),
                        help="End date (default: today)")
    parser.add_argument("--db", default=str(DB_PATH), help="Path to insiders.db")
    args = parser.parse_args()

    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA cache_size=-200000")

    logger.info("Database: %s", args.db)

    # Create table
    conn.execute(CREATE_TABLE_SQL)
    conn.commit()

    t0 = time.time()
    n = compute_sentiment(conn, args.start, args.end)
    elapsed = time.time() - t0

    print_summary(conn)
    logger.info("Done: %d rows in %.1fs", n, elapsed)
    conn.close()


if __name__ == "__main__":
    main()
