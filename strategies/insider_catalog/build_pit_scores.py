#!/usr/bin/env python3
"""
Walk-forward PIT score builder.

Phase C3 of the Data Quality & Scoring Redesign.

Processes all trades chronologically by filing_date. For each trade,
computes the PIT score for that insider+ticker using ONLY prior data,
then stores it. Maintains running aggregates in memory to avoid
re-querying the entire history for each trade.

Expected runtime: ~30-60 minutes for 804K trades.

Usage:
    python strategies/insider_catalog/build_pit_scores.py
    python strategies/insider_catalog/build_pit_scores.py --start 2020-01-01 --end 2024-12-31
    python strategies/insider_catalog/build_pit_scores.py --buy-only  # only score buy trades
"""

from __future__ import annotations

import argparse
import logging
import math
import sqlite3
import statistics
import sys
import time
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from backfill import DB_PATH, migrate_schema
from pit_scoring import _score_window, upsert_score

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

# How many days after a trade before we consider the 7d return "observable"
RETURN_OBSERVABLE_LAG = 10


class RunningAggregates:
    """
    Maintains per-insider and per-insider-per-ticker running stats in memory.

    Avoids re-querying the DB for the full trade history on every score computation.
    Tracks returns that are "observable" — i.e., enough time has passed for the
    7d forward return to have materialized.
    """

    def __init__(self):
        # Per insider: list of (trade_date, ticker, abnormal_7d_or_None)
        self.insider_trades: dict[int, list[tuple]] = defaultdict(list)
        # Per insider+ticker: list of (trade_date, abnormal_7d_or_None)
        self.insider_ticker_trades: dict[tuple[int, str], list[tuple]] = defaultdict(list)
        # Role lookup: (insider_id, ticker) → title
        self.roles: dict[tuple[int, str], str] = {}
        # Primary company: insider_id → ticker with most trades
        self.primary_ticker: dict[int, str] = {}
        self.ticker_counts: dict[int, dict[str, int]] = defaultdict(lambda: defaultdict(int))

    def add_trade(self, insider_id: int, ticker: str, trade_date: str,
                  abnormal_7d: float | None, title: str | None):
        """Record a new trade."""
        self.insider_trades[insider_id].append((trade_date, ticker, abnormal_7d))
        self.insider_ticker_trades[(insider_id, ticker)].append((trade_date, abnormal_7d))

        if title:
            self.roles[(insider_id, ticker)] = title

        self.ticker_counts[insider_id][ticker] += 1
        # Update primary ticker
        counts = self.ticker_counts[insider_id]
        self.primary_ticker[insider_id] = max(counts, key=counts.get)

    def get_observable_returns(self, insider_id: int, ticker: str | None,
                               as_of_date: str) -> list[float]:
        """
        Get abnormal returns observable as of as_of_date.

        A return is observable if the trade happened at least RETURN_OBSERVABLE_LAG
        days before as_of_date.
        """
        from datetime import datetime, timedelta
        cutoff_dt = datetime.strptime(as_of_date, "%Y-%m-%d") - timedelta(days=RETURN_OBSERVABLE_LAG)
        cutoff = cutoff_dt.strftime("%Y-%m-%d")

        if ticker is None:
            # Global
            trades = self.insider_trades.get(insider_id, [])
            return [abn for td, _, abn in trades if td <= cutoff and abn is not None]
        else:
            # Ticker-specific
            trades = self.insider_ticker_trades.get((insider_id, ticker), [])
            return [abn for td, abn in trades if td <= cutoff and abn is not None]

    def compute_score(self, insider_id: int, ticker: str, as_of_date: str,
                      min_ticker_trades: int = 2, min_global_trades: int = 3) -> dict:
        """
        Compute PIT score from in-memory running aggregates.
        """
        # Ticker-specific
        ticker_returns = self.get_observable_returns(insider_id, ticker, as_of_date)
        ticker_n = len(ticker_returns)
        if ticker_returns:
            ticker_wr = sum(1 for r in ticker_returns if r > 0) / ticker_n
            ticker_avg = statistics.mean(ticker_returns)
        else:
            ticker_wr = None
            ticker_avg = None
        ticker_score_raw = _score_window(ticker_wr, ticker_avg, ticker_n) * 3.0

        # Global
        global_returns = self.get_observable_returns(insider_id, None, as_of_date)
        global_n = len(global_returns)
        if global_returns:
            global_wr = sum(1 for r in global_returns if r > 0) / global_n
            global_avg = statistics.mean(global_returns)
        else:
            global_wr = None
            global_avg = None
        global_score_raw = _score_window(global_wr, global_avg, global_n) * 3.0

        sufficient_data = 1 if global_n >= min_global_trades else 0

        # Trade counts (all trades, not just those with returns)
        ticker_trade_count = len(self.insider_ticker_trades.get((insider_id, ticker), []))
        global_trade_count = len(self.insider_trades.get(insider_id, []))

        # Blending
        if ticker_n < min_ticker_trades:
            gw, tw = 1.0, 0.0
        elif ticker_n < 5:
            gw, tw = 0.70, 0.30
        elif ticker_n < 10:
            gw, tw = 0.50, 0.50
        else:
            gw, tw = 0.30, 0.70

        # Ticker outperformance adjustment
        if (ticker_wr is not None and global_wr is not None
                and ticker_wr - global_wr > 0.10
                and ticker_n >= min_ticker_trades):
            shift = min(0.10, gw)
            gw -= shift
            tw += shift

        blended = global_score_raw * gw + ticker_score_raw * tw

        # Role adjustment
        role = self.roles.get((insider_id, ticker))
        is_primary = 1 if self.primary_ticker.get(insider_id) == ticker else 0
        role_weight = 1.0

        if role and is_primary:
            rl = role.lower()
            if any(kw in rl for kw in ("ceo", "chief exec", "chairman")):
                role_weight = 1.15
            elif any(kw in rl for kw in ("cfo", "president")):
                role_weight = 1.10
            elif any(kw in rl for kw in ("coo", "evp", "svp")):
                role_weight = 1.05

        blended = min(3.0, max(0.0, blended * role_weight))

        return {
            "insider_id": insider_id,
            "ticker": ticker,
            "as_of_date": as_of_date,
            "ticker_trade_count": ticker_trade_count,
            "ticker_win_rate_7d": round(ticker_wr, 4) if ticker_wr is not None else None,
            "ticker_avg_abnormal_7d": round(ticker_avg, 6) if ticker_avg is not None else None,
            "ticker_score": round(ticker_score_raw, 4),
            "global_trade_count": global_trade_count,
            "global_win_rate_7d": round(global_wr, 4) if global_wr is not None else None,
            "global_avg_abnormal_7d": round(global_avg, 6) if global_avg is not None else None,
            "global_score": round(global_score_raw, 4),
            "blended_score": round(blended, 4),
            "role_at_ticker": role,
            "role_weight": round(role_weight, 4),
            "is_primary_company": is_primary,
            "sufficient_data": sufficient_data,
        }


def build_walkforward_scores(
    conn: sqlite3.Connection,
    start_date: str = "2016-01-01",
    end_date: str = "2026-12-31",
    buy_only: bool = True,
    batch_size: int = 10000,
):
    """
    Build walk-forward PIT scores for all trades.

    Processes trades ordered by filing_date ASC. For each buy trade:
    1. Add the trade to running aggregates
    2. Compute PIT score for that insider+ticker at filing_date
    3. Store in insider_ticker_scores and score_history
    """
    logger.info("Building walk-forward PIT scores (%s to %s)...", start_date, end_date)

    # Load all trades with their returns, ordered by filing_date
    trade_type_filter = "AND t.trade_type = 'buy'" if buy_only else ""
    trades = conn.execute(f"""
        SELECT t.trade_id, t.insider_id, t.ticker, t.trade_date, t.filing_date,
               t.title, t.trade_type,
               tr.abnormal_7d
        FROM trades t
        LEFT JOIN trade_returns tr ON t.trade_id = tr.trade_id
        WHERE t.filing_date >= ? AND t.filing_date <= ?
              {trade_type_filter}
        ORDER BY t.filing_date ASC, t.trade_date ASC
    """, (start_date, end_date)).fetchall()

    logger.info("Processing %d trades...", len(trades))

    agg = RunningAggregates()
    scored = 0
    start_time = time.monotonic()

    for i, row in enumerate(trades):
        trade_id, insider_id, ticker, trade_date, filing_date, title, trade_type, abnormal_7d = row

        # Add trade to running aggregates
        agg.add_trade(insider_id, ticker, trade_date, abnormal_7d, title)

        # Compute PIT score at the filing date
        score = agg.compute_score(insider_id, ticker, filing_date)
        upsert_score(conn, score, trigger_trade_id=trade_id)
        scored += 1

        # Batch commit
        if scored % batch_size == 0:
            conn.commit()
            elapsed = time.monotonic() - start_time
            rate = scored / elapsed
            eta = (len(trades) - scored) / rate if rate > 0 else 0
            logger.info(
                "  %d/%d scored (%.0f/sec, ETA %.0f min) | date: %s",
                scored, len(trades), rate, eta / 60, filing_date,
            )

    conn.commit()
    elapsed = time.monotonic() - start_time
    logger.info(
        "Walk-forward scoring complete: %d scores in %.1f min (%.0f/sec)",
        scored, elapsed / 60, scored / max(elapsed, 1),
    )


def verify_no_leakage(conn: sqlite3.Connection):
    """Verify that no score uses future data."""
    # For every score, the as_of_date should be <= the filing_date of the trade it scores
    leaky = conn.execute("""
        SELECT COUNT(*)
        FROM score_history sh
        JOIN trades t ON sh.trigger_trade_id = t.trade_id
        WHERE sh.as_of_date > t.filing_date
    """).fetchone()[0]

    if leaky > 0:
        logger.error("DATA LEAKAGE DETECTED: %d scores have as_of_date > trade filing_date", leaky)
    else:
        logger.info("No data leakage detected (all as_of_date <= filing_date)")

    return leaky == 0


def print_summary(conn: sqlite3.Connection):
    """Print scoring summary statistics."""
    total = conn.execute("SELECT COUNT(*) FROM insider_ticker_scores").fetchone()[0]
    with_data = conn.execute(
        "SELECT COUNT(*) FROM insider_ticker_scores WHERE sufficient_data = 1"
    ).fetchone()[0]
    history = conn.execute("SELECT COUNT(*) FROM score_history").fetchone()[0]

    # Score distribution
    dist = conn.execute("""
        SELECT
            CASE
                WHEN blended_score >= 2.0 THEN 'high (2.0+)'
                WHEN blended_score >= 1.0 THEN 'medium (1.0-2.0)'
                WHEN blended_score >= 0.5 THEN 'low (0.5-1.0)'
                ELSE 'minimal (<0.5)'
            END as bucket,
            COUNT(*)
        FROM insider_ticker_scores
        WHERE sufficient_data = 1
        GROUP BY bucket
        ORDER BY MIN(blended_score) DESC
    """).fetchall()

    print(f"\n{'='*60}")
    print("PIT SCORING SUMMARY")
    print(f"{'='*60}")
    print(f"Total insider-ticker-date scores: {total:,}")
    print(f"With sufficient data:             {with_data:,}")
    print(f"Score history entries:             {history:,}")
    print(f"\nScore distribution (sufficient data only):")
    for bucket, count in dist:
        print(f"  {bucket:<25} {count:>8,}")
    print(f"{'='*60}\n")


def main():
    parser = argparse.ArgumentParser(description="Build walk-forward PIT scores")
    parser.add_argument("--start", default="2016-01-01", help="Start date (default: 2016-01-01)")
    parser.add_argument("--end", default="2026-12-31", help="End date (default: 2026-12-31)")
    parser.add_argument("--buy-only", action="store_true", default=True,
                        help="Only score buy trades (default: True)")
    parser.add_argument("--all-types", action="store_true",
                        help="Score all trade types, not just buys")
    parser.add_argument("--clear", action="store_true",
                        help="Clear existing scores before building")
    args = parser.parse_args()

    conn = sqlite3.connect(str(DB_PATH))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")

    migrate_schema(conn)

    if args.clear:
        logger.info("Clearing existing scores...")
        conn.execute("DELETE FROM insider_ticker_scores")
        conn.execute("DELETE FROM score_history")
        conn.commit()

    buy_only = not args.all_types
    build_walkforward_scores(conn, args.start, args.end, buy_only=buy_only)

    verify_no_leakage(conn)
    print_summary(conn)

    conn.close()
    logger.info("Done.")


if __name__ == "__main__":
    main()
