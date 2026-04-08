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
from config.database import get_connection
import sys
import time
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from backfill import migrate_schema
from pit_scoring import upsert_score

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

    v2: stores both 7d and 30d returns, returns (trade_date, value) tuples
    for recency weighting in BayesianScorerV2.
    """

    def __init__(self):
        # Per insider: list of (trade_date, ticker, abnormal_7d, abnormal_30d, abnormal_90d)
        self.insider_trades: dict[int, list[tuple]] = defaultdict(list)
        # Per insider+ticker: list of (trade_date, abnormal_7d, abnormal_30d, abnormal_90d)
        self.insider_ticker_trades: dict[tuple[int, str], list[tuple]] = defaultdict(list)
        # Role lookup: (insider_id, ticker) → title
        self.roles: dict[tuple[int, str], str] = {}
        # Primary company: insider_id → ticker with most trades
        self.primary_ticker: dict[int, str] = {}
        self.ticker_counts: dict[int, dict[str, int]] = defaultdict(lambda: defaultdict(int))

    def add_trade(self, insider_id: int, ticker: str, trade_date: str,
                  abnormal_7d: float | None, abnormal_30d: float | None,
                  abnormal_90d: float | None, title: str | None):
        """Record a new trade with 7d, 30d, and 90d returns."""
        self.insider_trades[insider_id].append((trade_date, ticker, abnormal_7d, abnormal_30d, abnormal_90d))
        self.insider_ticker_trades[(insider_id, ticker)].append((trade_date, abnormal_7d, abnormal_30d, abnormal_90d))

        if title:
            self.roles[(insider_id, ticker)] = title

        self.ticker_counts[insider_id][ticker] += 1
        counts = self.ticker_counts[insider_id]
        self.primary_ticker[insider_id] = max(counts, key=counts.get)

    def get_observable_returns(self, insider_id: int, ticker: str | None,
                               as_of_date: str, window: str = "7d"
                               ) -> list[tuple[str, float]]:
        """
        Get observable returns as (trade_date, abnormal_return) tuples.

        Returns tuples for recency weighting in BayesianScorerV2.
        window: "7d" (lag=10 days) or "30d" (lag=40 days)
        """
        from datetime import datetime, timedelta
        lag = {"7d": RETURN_OBSERVABLE_LAG, "30d": 40, "90d": 100}.get(window, RETURN_OBSERVABLE_LAG)
        cutoff_dt = datetime.strptime(as_of_date, "%Y-%m-%d") - timedelta(days=lag)
        cutoff = cutoff_dt.strftime("%Y-%m-%d")

        # Index into the tuple: (trade_date, [ticker,] abnormal_7d, abnormal_30d, abnormal_90d)
        field_idx = {"7d": 2, "30d": 3, "90d": 4}[window]

        if ticker is None:
            trades = self.insider_trades.get(insider_id, [])
            return [(td, t[field_idx]) for t in trades
                    if (td := t[0]) <= cutoff and t[field_idx] is not None]
        else:
            trades = self.insider_ticker_trades.get((insider_id, ticker), [])
            # ticker_trades don't have the ticker field, so index is field_idx - 1
            return [(td, t[field_idx - 1]) for t in trades
                    if (td := t[0]) <= cutoff and t[field_idx - 1] is not None]

    # Legacy compatibility: return flat list of floats
    def get_observable_returns_flat(self, insider_id: int, ticker: str | None,
                                    as_of_date: str) -> list[float]:
        """Legacy: flat list of 7d abnormal returns (no dates)."""
        return [r for _, r in self.get_observable_returns(insider_id, ticker, as_of_date, "7d")]

    def compute_score_v2(self, insider_id: int, ticker: str, as_of_date: str) -> "ScoringResult":
        """Compute PIT score using BayesianScorerV2."""
        from pit_scoring import BayesianScorerV2, ScoringContext

        ctx = ScoringContext(
            insider_id=insider_id,
            ticker=ticker,
            as_of_date=as_of_date,
            ticker_returns_7d=self.get_observable_returns(insider_id, ticker, as_of_date, "7d"),
            ticker_returns_30d=self.get_observable_returns(insider_id, ticker, as_of_date, "30d"),
            ticker_returns_90d=self.get_observable_returns(insider_id, ticker, as_of_date, "90d"),
            global_returns_7d=self.get_observable_returns(insider_id, None, as_of_date, "7d"),
            global_returns_30d=self.get_observable_returns(insider_id, None, as_of_date, "30d"),
            global_returns_90d=self.get_observable_returns(insider_id, None, as_of_date, "90d"),
            role_at_ticker=self.roles.get((insider_id, ticker)),
            is_primary_company=(self.primary_ticker.get(insider_id) == ticker),
        )
        return BayesianScorerV2().score(ctx)



def build_walkforward_scores(
    conn: object,
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
               tr.abnormal_7d, tr.abnormal_30d, tr.abnormal_90d
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
        trade_id, insider_id, ticker, trade_date, filing_date, title, trade_type, abnormal_7d, abnormal_30d, abnormal_90d = row

        # Add trade to running aggregates (v2: includes 30d and 90d returns)
        agg.add_trade(insider_id, ticker, trade_date, abnormal_7d, abnormal_30d, abnormal_90d, title)

        # Compute PIT score using Bayesian v2 scorer
        score = agg.compute_score_v2(insider_id, ticker, filing_date)
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


def verify_no_leakage(conn: object):
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


def print_summary(conn: object):
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

    conn = get_connection()

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
