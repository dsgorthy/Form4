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
from typing import TYPE_CHECKING

sys.path.insert(0, str(Path(__file__).resolve().parent))
from backfill import migrate_schema
from pit_scoring import upsert_score, MEANINGFUL_BUY_CLASSES, MEANINGFUL_CLASSES

if TYPE_CHECKING:
    # compute_score_v2 annotates -> "ScoringResult" but only imported
    # BayesianScorerV2 and ScoringContext inside the function body, so the
    # name resolved nowhere. Harmless at runtime -- string annotations are
    # not evaluated -- but it is a dangling reference and F821 is right to
    # flag it.
    from pit_scoring import ScoringResult

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
        # Per insider: list of (trade_date, filing_date, ticker, abnormal_7d, abnormal_30d, abnormal_90d)
        self.insider_trades: dict[int, list[tuple]] = defaultdict(list)
        # Per insider+ticker: list of (trade_date, filing_date, abnormal_7d, abnormal_30d, abnormal_90d)
        self.insider_ticker_trades: dict[tuple[int, str], list[tuple]] = defaultdict(list)
        # Role lookup: (insider_id, ticker) → title
        self.roles: dict[tuple[int, str], str] = {}
        # Primary company: insider_id → ticker with most filings
        self.primary_ticker: dict[int, str] = {}
        self.ticker_counts: dict[int, dict[str, int]] = defaultdict(lambda: defaultdict(int))
        # ONE OBSERVATION PER FILING. A Form 4 filled in five tranches is one
        # decision; every tranche carries the same abnormal return, and
        # counting the ladder made one lucky outcome look like a track record
        # (the 2026-08-22 tranche correction, applied to the career scorer in
        # pit_scoring._get_returns but never to this walk-forward copy).
        self._seen_filings: set[tuple[int, str, str]] = set()

    def add_trade(self, insider_id: int, ticker: str, trade_date: str,
                  abnormal_7d: float | None, abnormal_30d: float | None,
                  abnormal_90d: float | None, title: str | None,
                  filing_date: str | None = None,
                  filing_key: str | None = None) -> bool:
        """Record a filing with its 7d, 30d, and 90d returns.

        Returns False when `filing_key` was already recorded for this
        insider+ticker — a later execution lot of a filing already counted —
        in which case nothing is added. Role bookkeeping still runs.
        """
        if title:
            self.roles[(insider_id, ticker)] = title

        fd = str(filing_date)[:10] if filing_date else str(trade_date)[:10]
        key = (insider_id, ticker, str(filing_key) if filing_key else f"{trade_date}")
        if key in self._seen_filings:
            return False
        self._seen_filings.add(key)

        td = str(trade_date)[:10]
        self.insider_trades[insider_id].append((td, fd, ticker, abnormal_7d, abnormal_30d, abnormal_90d))
        self.insider_ticker_trades[(insider_id, ticker)].append((td, fd, abnormal_7d, abnormal_30d, abnormal_90d))

        self.ticker_counts[insider_id][ticker] += 1
        counts = self.ticker_counts[insider_id]
        self.primary_ticker[insider_id] = max(counts, key=counts.get)
        return True

    def get_observable_returns(self, insider_id: int, ticker: str | None,
                               as_of_date: str, window: str = "7d"
                               ) -> list[tuple[str, float]]:
        """
        Get observable returns as (trade_date, abnormal_return) tuples.

        Returns tuples for recency weighting in BayesianScorerV2.
        window: "7d" (lag=10 days) or "30d" (lag=40 days)

        TWO GUARDS, BOTH REQUIRED. `trade_date <= as_of - lag` makes the
        forward return observable at all. `filing_date < as_of_date` — STRICT —
        is what keeps a trade out of its own grade: the score is stamped as_of
        the trade's own filing_date, so a Form 4 lodged 124 days after
        execution clears every maturity cutoff and, without this guard, grades
        itself on its own realised return. pit_scoring._get_returns got this
        guard on 2026-08-30 (measured: late-filed A+/A/B rows carried a 90d
        return of +36.59% against −6.94% for C/D, a 43.5-point gap that clean
        rows do not show); this walk-forward copy did not, and it is the copy
        that produces `pit_grade`, which conviction reads. Strict `<` also
        drops same-day siblings, which is right: a filing published in the
        same session cannot inform the score used to judge it.
        """
        from datetime import datetime, timedelta
        lag = {"7d": RETURN_OBSERVABLE_LAG, "30d": 40, "90d": 100}.get(window, RETURN_OBSERVABLE_LAG)
        cutoff_dt = datetime.strptime(as_of_date, "%Y-%m-%d") - timedelta(days=lag)
        cutoff = cutoff_dt.strftime("%Y-%m-%d")

        # Index into the tuple: (trade_date, filing_date, [ticker,] abnormal_7d, abnormal_30d, abnormal_90d)
        field_idx = {"7d": 3, "30d": 4, "90d": 5}[window]

        if ticker is None:
            trades = self.insider_trades.get(insider_id, [])
            return [(td, t[field_idx]) for t in trades
                    if (td := t[0]) <= cutoff and t[1] < as_of_date
                    and t[field_idx] is not None]
        else:
            trades = self.insider_ticker_trades.get((insider_id, ticker), [])
            # ticker_trades don't have the ticker field, so index is field_idx - 1
            return [(td, t[field_idx - 1]) for t in trades
                    if (td := t[0]) <= cutoff and t[1] < as_of_date
                    and t[field_idx - 1] is not None]

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

    # Load all filings with their returns, ordered by filing_date.
    #
    # A GRADE MEASURES DECISIONS, NOT COMPENSATION. This gated on
    # `trade_type = 'buy'` until 2026-09-21, which admits 184k compensation
    # grants and 221k option exercises — the population the 2026-08-25 fix
    # removed from the career scorer (pit_scoring._get_returns) and that
    # CLAUDE.md says never to gate on trade_type. signal_class is derived,
    # never typed, and the three hygiene predicates match every other reader.
    classes = tuple(MEANINGFUL_BUY_CLASSES if buy_only else MEANINGFUL_CLASSES)
    cls_ph = ", ".join("?" for _ in classes)
    trades = conn.execute(f"""
        SELECT t.trade_id, t.insider_id, t.ticker, t.trade_date, t.filing_date,
               t.title, t.trade_type,
               tr.abnormal_7d, tr.abnormal_30d, tr.abnormal_90d,
               COALESCE(t.filing_key, t.accession, t.trade_date::text) AS filing_key
        FROM trades t
        LEFT JOIN trade_returns tr ON t.trade_id = tr.trade_id
        WHERE t.filing_date >= ? AND t.filing_date <= ?
          AND t.signal_class IN ({cls_ph})
          AND t.superseded_by IS NULL
          AND t.is_derivative = 0
          AND (t.is_duplicate = 0 OR t.is_duplicate IS NULL)
        ORDER BY t.filing_date ASC, t.trade_date ASC
    """, (start_date, end_date, *classes)).fetchall()

    logger.info("Processing %d trades...", len(trades))

    # FAIL CLOSED: refuse to "succeed" with 0 scored if the input window
    # had no trades at all. The April 2026 outage analog: a successful
    # 0-score run looks identical to "scoring is healthy" downstream, but
    # produces no `insider_ticker_scores` rows for the window — which then
    # silently demotes every fresh trade's pit_grade to NULL → "C".
    if len(trades) == 0:
        msg = (
            f"build_pit_scores: 0 trades found in window {start_date}..{end_date}. "
            "Likely an upstream data gap. Refusing to 'succeed' on empty input."
        )
        logger.error(msg)
        with __import__("contextlib").suppress(Exception):
            from framework.alerts.log import alert
            alert.critical("build_pit_scores", msg,
                           start_date=start_date, end_date=end_date)
        raise RuntimeError(msg)

    agg = RunningAggregates()
    scored = 0
    start_time = time.monotonic()

    for i, row in enumerate(trades):
        (trade_id, insider_id, ticker, trade_date, filing_date, title, trade_type,
         abnormal_7d, abnormal_30d, abnormal_90d, filing_key) = row
        filing_date = str(filing_date)[:10]

        # Add the filing to the running aggregates. A second execution lot of
        # a filing already recorded adds nothing and is not scored again: the
        # score at (insider, ticker, filing_date) is one number per filing.
        is_new = agg.add_trade(insider_id, ticker, trade_date,
                               abnormal_7d, abnormal_30d, abnormal_90d, title,
                               filing_date=filing_date, filing_key=filing_key)
        if not is_new:
            continue

        # Compute PIT score using Bayesian v2 scorer. The filing just added is
        # excluded from its own score by the strict filing_date guard.
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

    # Freshness contract: write signal_freshness row so the runner's
    # preflight check knows insider_ticker_scores.blended_score is current.
    # Only on a successful non-zero run — see freshness_writer docstring.
    if scored > 0:
        from framework.contracts.freshness_writer import write_freshness
        write_freshness(
            conn,
            table="insider_ticker_scores",
            column="blended_score",
            n_rows_affected=scored,
            populated_by="strategies/insider_catalog/build_pit_scores.py",
        )
        conn.commit()


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
    parser.add_argument("--skip-migrate", action="store_true",
                        help="Skip migrate_schema (already applied on PG; "
                             "the SQLite-era schema.sql doesn't translate cleanly).")
    args = parser.parse_args()

    conn = get_connection()

    if not args.skip_migrate:
        migrate_schema(conn)

    if args.clear:
        # Reset the V2 walk-forward columns in the window WITHOUT deleting the
        # rows. compute_career_grades owns career_blended_score / career_grade
        # on these same rows and takes four hours to write them; a DELETE here
        # erased that work whenever this job ran after it, and forced the
        # rebuild chain into an order it does not otherwise need. Rows the new
        # population no longer triggers keep their career columns and read as
        # unscored (sufficient_data = 0, blended_score NULL), which every V2
        # reader already treats as "no opinion". score_history is this job's
        # own log and is cleared outright.
        logger.info("Resetting V2 scores in %s..%s (career columns preserved)...",
                    args.start, args.end)
        conn.execute("""
            UPDATE insider_ticker_scores
               SET ticker_trade_count = NULL, ticker_win_rate_7d = NULL,
                   ticker_avg_abnormal_7d = NULL, ticker_score = NULL,
                   global_trade_count = NULL, global_win_rate_7d = NULL,
                   global_avg_abnormal_7d = NULL, global_score = NULL,
                   blended_score = NULL, sufficient_data = 0
             WHERE as_of_date >= ? AND as_of_date <= ?
        """, (args.start, args.end))
        conn.execute(
            "DELETE FROM score_history WHERE as_of_date >= ? AND as_of_date <= ?",
            (args.start, args.end))
        conn.commit()

    buy_only = not args.all_types
    build_walkforward_scores(conn, args.start, args.end, buy_only=buy_only)

    verify_no_leakage(conn)
    print_summary(conn)

    conn.close()
    logger.info("Done.")


if __name__ == "__main__":
    main()
